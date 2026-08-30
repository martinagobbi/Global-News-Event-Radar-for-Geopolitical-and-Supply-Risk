<#
================================================================================
Silver snapshot (Windows) — export the silver layer to Parquet, or restore it.

Windows PowerShell 5.1 port of bootstrap/silver_snapshot.sh. Same behaviour,
same subcommands, same environment-variable overrides — see that file for the
full rationale behind each design choice; the comments here only repeat what
is specific to the Windows port.

  .\bootstrap\silver_snapshot.ps1 export    # silver volume    -> data\silver_seed
  .\bootstrap\silver_snapshot.ps1 restore   # data\silver_seed -> silver volume
  .\bootstrap\silver_snapshot.ps1 wipe      # empty the silver tables
  .\bootstrap\silver_snapshot.ps1 recreate
  .\bootstrap\silver_snapshot.ps1 trim {seed|<YYYYMMDDHHMMSS>}

Requires the stores tier to be running, and Docker Desktop's `docker` CLI on
PATH. Run this on the machine hosting s1r1 — see _Resolve-ChContainer below.

WHY THIS IS A SEPARATE FILE RATHER THAN "JUST RUN THE .sh": PowerShell has no
native `<`/`>` redirection to an external process that preserves bytes
unchanged — piping a native command's stdout through the PowerShell pipeline
(or using PowerShell's own `>`) re-encodes it as text, which corrupts a
Parquet file. The two operations that move Parquet bytes (export's SELECT
... FORMAT Parquet, restore's INSERT ... FORMAT Parquet) are therefore
delegated to cmd.exe, whose `<`/`>` redirection is a raw OS-level file
handle duplication and stays binary-safe. Every other call here just moves
short text (row counts, EXISTS TABLE, DDL) and uses plain PowerShell capture.
================================================================================
#>

param(
    [Parameter(Position = 0)]
    [string]$Action,

    [Parameter(Position = 1)]
    [string]$Cutoff
)

$ErrorActionPreference = 'Stop'

function Get-EnvOrDefault {
    param([string]$Name, [string]$Default)
    $val = [Environment]::GetEnvironmentVariable($Name)
    if ([string]::IsNullOrEmpty($val)) { return $Default }
    return $val
}

# Which container to run clickhouse-client in. Single-machine mode uses plain
# Compose, where the container is named exactly this. Intended mode runs the
# stores as a SWARM STACK, and Swarm ignores `container_name` — the task is
# called something like radar-stores_clickhouse-s1r1.1.<taskid>, with a
# different id every time it is rescheduled, so the name cannot be
# hard-coded. Resolve it by service label instead, and fall back to the
# Compose name.
#
# Run this on the machine hosting s1r1: `docker exec` is local to one
# daemon, so the container has to be here. (Which machine that is, is fixed
# by the placement constraint in docker-stack.stores.yml.)
function _Resolve-ChContainer {
    $explicit = [Environment]::GetEnvironmentVariable('CH_CONTAINER')
    if (-not [string]::IsNullOrEmpty($explicit)) { return $explicit }

    $stack = Get-EnvOrDefault -Name 'STORES_STACK' -Default 'radar-stores'
    $filter = "label=com.docker.swarm.service.name=$($stack)_clickhouse-s1r1"
    $swarmTask = (& docker ps -q --filter $filter 2>$null | Select-Object -First 1)
    if (-not [string]::IsNullOrEmpty($swarmTask)) { return $swarmTask }

    return 'pipeline_clickhouse_s1r1'
}

$CH_CONTAINER        = _Resolve-ChContainer
$SEED_DIR             = Get-EnvOrDefault -Name 'SEED_DIR' -Default 'data/silver_seed'
$TABLES               = @('gdelt_events', 'gdelt_mentions')
# The last 15-minute slice covered by the committed seed. `trim seed` uses
# it, so the window need not be remembered. Update it if the seed is ever
# rebuilt over a different period.
$SEED_LAST_SLICE      = Get-EnvOrDefault -Name 'SEED_LAST_SLICE' -Default '20260727171500'

# How long `restore` keeps retrying an INSERT that fails, and how often. 20
# minutes because the thing being waited out is a COLD START of the whole
# stores tier — ClickHouse, Keeper forming its quorum, and the validation
# layer creating the schema ON CLUSTER — and on a first run, with images
# still being pulled, that can genuinely take many minutes. Failing at 5
# would turn a slow start into a spurious error and send someone debugging a
# system that was merely booting.
#
# The budget is per TABLE, so a two-table restore can spend up to 2x this in
# the worst case. That is intentional: each table is a separate operation
# and a failure on the second says nothing about the first, which has
# already landed.
$RESTORE_MAX_WAIT    = [int](Get-EnvOrDefault -Name 'RESTORE_MAX_WAIT' -Default '1200')
$RESTORE_RETRY_EVERY = [int](Get-EnvOrDefault -Name 'RESTORE_RETRY_EVERY' -Default '10')

function Invoke-ChText {
    # For short text queries only (row counts, EXISTS TABLE, DDL) — never
    # for anything carrying Parquet bytes. See the file header comment.
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$QueryArgs)
    & docker exec -i $CH_CONTAINER clickhouse-client @QueryArgs
}

function Format-FileSize {
    param([long]$Bytes)
    $units = 'B', 'K', 'M', 'G', 'T'
    $size = [double]$Bytes
    $i = 0
    while ($size -ge 1024 -and $i -lt $units.Length - 1) {
        $size = $size / 1024
        $i++
    }
    if ($i -eq 0) { return "$Bytes$($units[$i])" }
    return "{0:N1}{1}" -f $size, $units[$i]
}

# Runs a cmd.exe-delegated command line and returns @{ ExitCode; ErrText }.
# $CommandLine must already be a complete, correctly quoted cmd.exe command
# (built by the callers below) — this just executes it and captures stderr.
function Invoke-CmdBinary {
    param([string]$CommandLine)
    $errFile = Join-Path $env:TEMP 'silver_snapshot_restore_err.txt'
    & cmd.exe /c "$CommandLine 2> `"$errFile`""
    $exitCode = $LASTEXITCODE
    $errText = ''
    if (Test-Path $errFile) {
        $errText = (Get-Content -Path $errFile -TotalCount 2 -ErrorAction SilentlyContinue) -join ' '
        if ($errText.Length -gt 120) { $errText = $errText.Substring(0, 120) }
        Remove-Item -Path $errFile -ErrorAction SilentlyContinue
    }
    return @{ ExitCode = $exitCode; ErrText = $errText }
}

switch ($Action) {

    'export' {
        New-Item -ItemType Directory -Force -Path $SEED_DIR | Out-Null
        foreach ($t in $TABLES) {
            Write-Host "Exporting $t ..."
            $outFile = Join-Path $SEED_DIR "$t.parquet"
            # FINAL collapses the ReplacingMergeTree duplicates, so the
            # snapshot holds one row per key and needs no deduplication when
            # restored.
            $cmdLine = "docker exec $CH_CONTAINER clickhouse-client --query `"SELECT * FROM $t FINAL FORMAT Parquet`" > `"$outFile`""
            & cmd.exe /c $cmdLine
            if ($LASTEXITCODE -ne 0) {
                Write-Error "Export of $t failed (exit $LASTEXITCODE)."
                exit 1
            }
            $rows = (Invoke-ChText --query "SELECT count() FROM $t FINAL").Trim()
            $size = Format-FileSize -Bytes (Get-Item $outFile).Length
            Write-Host ("  {0,-16} {1,8} rows  {2}" -f $t, $rows, $size)
        }
        Write-Host "Snapshot written to $SEED_DIR"
    }

    'restore' {
        # The silver schema is owned by the validation layer, which creates
        # it ON CLUSTER the first time it reaches ClickHouse. On a fresh
        # clone that happens seconds after the pipeline starts, so wait
        # rather than failing with "Unknown table expression" if this is
        # run the moment the containers are up.
        Write-Host -NoNewline 'Waiting for the silver schema (created by the validation layer) '
        $ready = $false
        for ($attempt = 1; $attempt -le 60; $attempt++) {
            $exists = (Invoke-ChText --query "EXISTS TABLE $($TABLES[0])" 2>$null)
            if ($exists -and $exists.Trim() -eq '1') {
                Write-Host " Ready after $((($attempt - 1) * 5))s"
                $ready = $true
                break
            }
            # A dot per attempt: this wait can last minutes on a cold
            # start, and silence for that long is indistinguishable from a
            # hang.
            Write-Host -NoNewline '.'
            Start-Sleep -Seconds 5
        }
        Write-Host ''
        if (-not $ready) {
            Write-Host "ERROR: $($TABLES[0]) Still does not exist after 5 minutes." -ForegroundColor Red
            Write-Host "       Is the pipeline running? The VALIDATION layer owns this schema" -ForegroundColor Red
            Write-Host "       and creates it at startup; the stores alone will not." -ForegroundColor Red
            Write-Host '         docker compose --env-file .env.single_machine up -d --build' -ForegroundColor Red
            exit 1
        }
        # NOTE: passing this check does NOT mean the tables accept writes.
        # It proves the name is registered, nothing more — see the retry
        # around the INSERT below, which is what actually waits for the
        # storage to be initialised.

        foreach ($t in $TABLES) {
            $f = Join-Path $SEED_DIR "$t.parquet"
            if (-not (Test-Path $f) -or (Get-Item $f).Length -eq 0) {
                Write-Host "Missing or empty: $f — skipped"
                continue
            }
            Write-Host "Restoring $t ..."
            # The Distributed table routes each row to its shard, exactly
            # as a live write would, so the sharding stays consistent with
            # the cluster layout.
            # insert_deduplicate=0 is REQUIRED, not an optimisation.
            # ReplicatedMergeTree remembers the checksums of recently
            # inserted blocks and silently skips a block it has seen
            # before. Restoring the same seed file after rows were deleted
            # — by `trim`, or by the retention job — inserts byte-identical
            # blocks, which ClickHouse would drop as duplicates: the
            # command reports success and restores NOTHING. Correctness
            # does not depend on this de-duplication anyway, because both
            # tables are ReplacingMergeTree and collapse genuine duplicate
            # rows by key at merge/FINAL time.
            # ── Retried, because EXISTS TABLE is not the same as "accepts writes" ────
            # The wait above proves the table NAME is registered. It does
            # not prove the storage behind it is initialised, and the two
            # are genuinely separable:
            #
            #   Code: 667. DB::Exception: Table is not initialized yet. (NOT_INITIALIZED)
            #
            # The retry IS the real INSERT rather than a lighter probe,
            # deliberately: any probe tests something slightly different
            # from the operation it is standing in for, and that gap is
            # exactly where this bug lived. Re-running a failed or partial
            # INSERT is safe for the same reason restoring twice is safe —
            # insert_deduplicate=0 forces the blocks through, and both
            # tables are ReplacingMergeTree, so repeats collapse by key
            # instead of duplicating.
            $deadline = (Get-Date).AddSeconds($RESTORE_MAX_WAIT)
            $attempt = 0
            $insertCmd = "docker exec -i $CH_CONTAINER clickhouse-client --query `"INSERT INTO $t SETTINGS insert_deduplicate = 0 FORMAT Parquet`" < `"$f`""
            while ($true) {
                $result = Invoke-CmdBinary -CommandLine $insertCmd
                if ($result.ExitCode -eq 0) { break }
                $attempt++
                if ((Get-Date) -ge $deadline) {
                    Write-Host ''
                    Write-Host "ERROR: $t could not be restored within ${RESTORE_MAX_WAIT}s ($attempt attempts)." -ForegroundColor Red
                    Write-Host "       Last error: $($result.ErrText)" -ForegroundColor Red
                    Write-Host '       The stores may still be starting, or the schema may not match' -ForegroundColor Red
                    Write-Host '       the seed. Check:  docker logs pipeline_clickhouse_s1r1' -ForegroundColor Red
                    exit 1
                }
                # Every failure is printed, not just the last. A silent
                # retry loop is indistinguishable from a hang, and the
                # error text is what says whether this is a startup race
                # (retry will fix it) or a schema mismatch (it will not,
                # and waiting out the full budget is pointless).
                Write-Host "  Attempt $attempt failed, retrying in ${RESTORE_RETRY_EVERY}s: $($result.ErrText)"
                Start-Sleep -Seconds $RESTORE_RETRY_EVERY
            }
            if ($attempt -gt 0) {
                Write-Host ("  {0,-16} succeeded on attempt {1}" -f $t, ($attempt + 1))
            }
            $rows = (Invoke-ChText --query "SELECT count() FROM $t FINAL").Trim()
            # On a MULTI-SHARD cluster this count can read LOW — it is
            # taken the moment the insert returns, while the Distributed
            # table is still handing rows to the second shard and
            # ReplicatedMergeTree is still copying them between replicas.
            # Single-machine mode has one shard and one replica, so it is
            # exact there. Re-run `SELECT count() FROM gdelt_events FINAL`
            # to confirm.
            Write-Host ("  {0,-16} now {1} rows" -f $t, $rows)
        }
        Write-Host 'Silver restored — the processing watermark trigger will build the gold'
    }

    'recreate' {
        # Apply a CHANGED table definition — a new ORDER BY, a new column,
        # a new index — to a volume that already holds the old one.
        #
        # Needed because the schema is created with CREATE TABLE IF NOT
        # EXISTS, so on an existing volume a changed definition is simply
        # ignored: the tables keep whatever shape they were first created
        # with. ClickHouse also cannot ALTER a sorting key into a different
        # order (only append columns to it), so the tables have to be
        # dropped and rebuilt.
        #
        # Safe, because silver is reproducible: the committed seed restores
        # in seconds and the live pipeline re-polls anything newer from
        # GDELT.
        Write-Host 'Dropping the silver tables (both local and Distributed, ON CLUSTER) ...'
        foreach ($t in @('gdelt_events', 'gdelt_mentions')) {
            Invoke-ChText --query "DROP TABLE IF EXISTS $t ON CLUSTER gnews_cluster SYNC" | Out-Null
            Invoke-ChText --query "DROP TABLE IF EXISTS ${t}_local ON CLUSTER gnews_cluster SYNC" | Out-Null
            Write-Host ("  {0,-16} dropped" -f $t)
        }
        Write-Host ''
        Write-Host 'Now restart the VALIDATION layer — it owns the schema and calls'
        Write-Host 'ensure_tables() once, at startup, so nothing recreates the tables until'
        Write-Host 'it restarts:'
        Write-Host ''
        Write-Host '    docker compose --env-file .env.single_machine restart validation     # testing'
        Write-Host '    docker service update --force radar_validation                # intended'
        Write-Host ''
        Write-Host 'then re-fill silver:'
        Write-Host ''
        Write-Host '    .\bootstrap\silver_snapshot.ps1 restore'
    }

    'wipe' {
        foreach ($t in $TABLES) {
            Invoke-ChText --query "TRUNCATE TABLE ${t}_local ON CLUSTER gnews_cluster" | Out-Null
            Write-Host ("  {0,-16} emptied" -f $t)
        }
    }

    'trim' {
        # Drop everything published AFTER a given slice, so the store holds
        # exactly one known period. Used when rebuilding the seed: the live
        # pipeline keeps polling while a backfill runs, so silver ends up
        # holding the backfill window PLUS whatever arrived meanwhile, and
        # a seed built from that would ship an arbitrary slice of "today"
        # to everyone who clones the repository.
        #
        # Events and mentions carry the slice timestamp in different
        # columns, and both are strings of fixed width, so a lexicographic
        # comparison is also chronological.
        $cutoffValue = $Cutoff
        # `trim seed` is the common case: keep exactly the window the
        # committed seed covers, discarding whatever the live pipeline has
        # polled since.
        if ($cutoffValue -eq 'seed') { $cutoffValue = $SEED_LAST_SLICE }
        if ([string]::IsNullOrEmpty($cutoffValue) -or $cutoffValue -notmatch '^\d{14}$') {
            Write-Host 'Usage: .\bootstrap\silver_snapshot.ps1 trim {seed|<YYYYMMDDHHMMSS>}   (e.g. 20260727171500)' -ForegroundColor Red
            exit 1
        }
        Write-Host "Removing rows published after $cutoffValue ..."
        # mutations_sync=2 waits for every replica, so the counts printed
        # below are final.
        Invoke-ChText --query "ALTER TABLE gdelt_events_local ON CLUSTER gnews_cluster DELETE WHERE DATEADDED > '$cutoffValue' SETTINGS mutations_sync = 2" | Out-Null
        Invoke-ChText --query "ALTER TABLE gdelt_mentions_local ON CLUSTER gnews_cluster DELETE WHERE MentionTimeDate > '$cutoffValue' SETTINGS mutations_sync = 2" | Out-Null
        foreach ($t in $TABLES) {
            $rows = (Invoke-ChText --query "SELECT count() FROM $t FINAL").Trim()
            Write-Host ("  {0,-16} {1,8} rows remain" -f $t, $rows)
        }
        $eventsSpan = (Invoke-ChText --query "SELECT concat(min(DATEADDED),' .. ',max(DATEADDED)) FROM gdelt_events FINAL").Trim()
        $mentionsSpan = (Invoke-ChText --query "SELECT concat(min(MentionTimeDate),' .. ',max(MentionTimeDate)) FROM gdelt_mentions FINAL").Trim()
        Write-Host "Events  now span: $eventsSpan"
        Write-Host "Mentions now span: $mentionsSpan"
    }

    default {
        Write-Host 'Usage: .\bootstrap\silver_snapshot.ps1 {export|restore|recreate|wipe|trim {seed|<YYYYMMDDHHMMSS>}}' -ForegroundColor Red
        exit 1
    }
}
