"""
backfill.py
Scarica tutti i file GDELT degli ultimi N giorni (default 30).
Salva i CSV raw compressi nella cartella data/raw/.
Da eseguire UNA SOLA VOLTA all'avvio del sistema.

Struttura output:
    data/raw/
        events/
            20240315143000.export.CSV.zip
            ...
        gkg/
            20240315143000.gkg.csv.zip
            ...

Uso:
    python -m ingestion.backfill              # ultimi 30 giorni
    python -m ingestion.backfill --days 7     # ultimi 7 giorni
    python -m ingestion.backfill --days 30 --workers 4
"""

import argparse
import logging
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from tqdm import tqdm

from src.ingestion.gdelt_urls import generate_urls_last_n_days, count_urls
from src.ingestion.paths import RAW_ZIP_DIR, RAW_CSV_DIR, ensure_ingestion_dirs


# ---------------------------------------------------------------------------
# Configurazione logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Costanti
# ---------------------------------------------------------------------------
MAX_RETRIES = 3
RETRY_DELAY_SEC = 5
REQUEST_TIMEOUT_SEC = 60

# GDELT ha rate limiting implicito — non superare 5-6 richieste parallele
DEFAULT_WORKERS = 4


# ---------------------------------------------------------------------------
# Download singolo file
# ---------------------------------------------------------------------------
def _extract_csv(zip_path: Path) -> Path:
    with zipfile.ZipFile(zip_path, "r") as archive:
        first_member = archive.namelist()[0]
        csv_path = RAW_CSV_DIR / Path(first_member).name
        if not csv_path.exists():
            tmp_path = csv_path.with_name(f".{csv_path.name}.tmp")
            tmp_path.write_bytes(archive.read(first_member))
            tmp_path.rename(csv_path)
        return csv_path


def download_file(url: str, dest_path: Path, retries: int = MAX_RETRIES) -> bool:
    """
    Scarica un singolo file GDELT e lo salva su disco.
    Se il file esiste già lo salta (idempotente — si può rieseguire).

    Args:
        url:       URL del file .zip GDELT
        dest_path: path locale dove salvare il file
        retries:   numero di tentativi in caso di errore

    Returns:
        True se scaricato con successo, False se fallito
    """
    # Salta se già scaricato (resume-friendly)
    if dest_path.exists():
        _extract_csv(dest_path)
        return True

    dest_path.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT_SEC, stream=True)

            # GDELT restituisce 404 per slot non ancora pubblicati
            # (es. slot futuri o buchi nell'archivio)
            if response.status_code == 404:
                log.debug(f"File non trovato (404): {url}")
                return False

            response.raise_for_status()

            # Scrivi in modo atomico: prima in .tmp, poi rinomina
            tmp_path = dest_path.with_suffix(".tmp")
            with open(tmp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            tmp_path.rename(dest_path)
            _extract_csv(dest_path)

            return True

        except requests.exceptions.RequestException as e:
            log.warning(f"Attempt {attempt}/{retries} failed for {url}: {e}")
            if attempt < retries:
                time.sleep(RETRY_DELAY_SEC * attempt)  # backoff lineare

    log.error(f"Download failed after {retries} attempts: {url}")
    return False


# ---------------------------------------------------------------------------
# Backfill principale
# ---------------------------------------------------------------------------
def run_backfill(n_days: int = 30, workers: int = DEFAULT_WORKERS):
    """
    Scarica tutti i file GDELT degli ultimi n_days giorni.

    Strategia:
    - Thread pool con 'workers' download paralleli
    - Skip automatico dei file già presenti (idempotente)
    - Progress bar con conteggio successi/fallimenti
    - I 404 sono normali per slot vuoti — non sono errori

    Args:
        n_days:  giorni di storico da scaricare
        workers: thread paralleli (default 4, max consigliato 6)
    """
    total = count_urls(n_days, file_types=("events", "mentions"))
    log.info(f"Backfill started: last {n_days} days")
    log.info(f"Estimated files to download: ~{total} ({workers} parallel threads)")
    ensure_ingestion_dirs()
    log.info(f"ZIP directory: {RAW_ZIP_DIR.resolve()}")
    log.info(f"CSV hand-off directory: {RAW_CSV_DIR.resolve()}")

    # Genera tutti i task
    tasks = []
    for item in generate_urls_last_n_days(n_days, file_types=("events", "mentions")):
        ts_str = item["timestamp"].strftime("%Y%m%d%H%M%S")
        file_type = item["file_type"]

        ext = "export.CSV.zip" if file_type == "events" else "mentions.CSV.zip"
        dest = RAW_ZIP_DIR / f"{ts_str}.{ext}"

        tasks.append((item["url"], dest))

    # Scarica in parallelo con progress bar
    success_count = 0
    fail_count = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(download_file, url, dest): (url, dest)
            for url, dest in tasks
        }

        with tqdm(total=len(tasks), desc="Backfill GDELT", unit="file") as pbar:
            for future in as_completed(futures):
                url, dest = futures[future]
                try:
                    result = future.result()
                    if result:
                        if dest.exists():
                            # Controlla se era già presente prima del download
                            # (approssimazione: se il file è molto vecchio era già lì)
                            success_count += 1
                        else:
                            success_count += 1
                    else:
                        fail_count += 1
                except Exception as e:
                    log.error(f"Unexpected error for {url}: {e}")
                    fail_count += 1

                pbar.update(1)
                pbar.set_postfix(
                    ok=success_count,
                    fail=fail_count,
                )

    log.info("=" * 50)
    log.info("Backfill complete.")
    log.info(f"  Successful:  {success_count}")
    log.info(f"  Failed:      {fail_count} (mostly normal 404 responses)")
    log.info(f"  ZIP dir:     {RAW_ZIP_DIR.resolve()}")
    log.info(f"  CSV dir:     {RAW_CSV_DIR.resolve()}")
    log.info("=" * 50)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download GDELT files from the last N days."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to download (default: 30)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Thread paralleli (default: {DEFAULT_WORKERS}, max consigliato: 6)",
    )
    args = parser.parse_args()

    run_backfill(n_days=args.days, workers=args.workers)
