#!/usr/bin/env python
"""
dev_tools_for_filter_diagnostics/test_tokenisation.py
------------------------------------------------------
Regression test: the keyword matcher must tokenise text identically in Python,
in ClickHouse and in Spark.

Why this exists
---------------
A real bug: a headline reading "Novo Nordisk's owner backs an Italian fund"
used a TYPOGRAPHIC apostrophe (U+2019). ClickHouse's splitByNonAlpha does not
treat non-ASCII punctuation as a separator, so it produced the token "nordisk's",
which the stemmer turned into "nordisk'" — and the keyword "Nordisk" never
matched. Spark, splitting on a regex, produced "nordisk" and matched. The two
engines silently disagreed, and the article was dropped from one path only.

Publishers use these characters constantly: curly apostrophes and quotes from
word processors, en and em dashes in headlines, ellipsis characters, non-breaking
spaces from CMS templates, and accented letters in names. Any of them could
reintroduce the same class of bug, so the rule is checked rather than assumed.

The rule: EVERY character that is not a-z or 0-9 is a separator, in all three
engines. Accented letters are separators too — "Nestlé" tokenises to "nestl"
everywhere — which is harmless precisely because it is symmetric: the user's
keyword is put through the same rule as the article text.

Usage (stores running; Spark checks are skipped if pyspark is absent):

    python3 dev_tools_for_filter_diagnostics/test_tokenisation.py
"""

import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "4-processing"))

import processor  # noqa: E402

CH_CONTAINER = "pipeline_clickhouse_s1r1"

# Each case is (label, text). The text is what an article title might contain.
CASES = [
    ("typographic apostrophe", "Novo Nordisk’s owner backs a fund"),
    ("straight apostrophe",    "Novo Nordisk's owner backs a fund"),
    ("en dash",                "TSMC – Taiwan chip output rises"),
    ("em dash",                "TSMC — Taiwan chip output rises"),
    ("hyphen",                 "TSMC - Taiwan chip output rises"),
    ("curly double quotes",    "“Rare earth” export curbs widen"),
    ("straight double quotes", '"Rare earth" export curbs widen'),
    ("ellipsis character",     "Chip shortage deepens… again"),
    ("non-breaking space",     "Rare earth export curbs"),
    ("accented letters",       "Nestlé and Danone face cocoa costs"),
    ("mixed punctuation",      "Samsung’s DDR5 — “best-in-class”, says SK Hynix"),
    ("digits and units",       "Brent crude at 82.5 USD/bbl, up 3%"),
    ("trailing plural",        "Silicon wafers and wafer plants"),
]


def python_tokens(text: str) -> list:
    """The reference implementation, as used to tokenise the user's keywords."""
    raw = [t for t in processor._TOKEN_SPLIT.split(text.lower()) if t]
    return [processor.stem_token(t) for t in raw]


def clickhouse_tokens(text: str) -> list:
    """The same text put through the SQL expression the matcher actually runs."""
    expr = processor._ROW_TOKENS_SQL.format(
        min_len=processor._MIN_TOKEN_LEN, title="{t}", keywords="''"
    ).replace("{t}", "%s" % _sql_literal(text))
    out = subprocess.run(
        ["docker", "exec", "-i", CH_CONTAINER, "clickhouse-client",
         "--query", f"SELECT arrayFilter(x -> x != '', {expr}) FORMAT TSVRaw"],
        capture_output=True, text=True,
    )
    if out.returncode != 0:
        raise RuntimeError(out.stderr.strip()[:200])
    body = out.stdout.strip()
    if body in ("", "[]"):
        return []
    return [p.strip().strip("'") for p in body[1:-1].split(",")]


def _sql_literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def spark_tokens(texts: list) -> dict:
    """The Spark expression from spark_gold.user_predicate, over all cases at once."""
    try:
        from pyspark.sql import SparkSession, functions as F
    except ImportError:
        return {}
    spark = (SparkSession.builder.master("local[2]").appName("tok")
             .config("spark.sql.shuffle.partitions", "1").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.createDataFrame([(t,) for t in texts], ["article_title"]) \
              .withColumn("article_keywords", F.lit(""))
    expr = (
        "array_distinct(transform("
        "  split(lower(concat_ws(' ', article_title, article_keywords)), '[^a-z0-9]+'),"
        "  t -> CASE WHEN length(t) > 3 AND right(t, 1) = 's'"
        "            THEN substring(t, 1, length(t) - 1) ELSE t END))"
    )
    rows = df.withColumn("toks", F.expr(f"array_remove({expr}, '')")).collect()
    spark.stop()
    return {r["article_title"]: list(r["toks"]) for r in rows}


def main() -> int:
    spark_map = spark_tokens([text for _, text in CASES])
    if not spark_map:
        print("note: pyspark not installed — Python/ClickHouse checked only\n")

    failures = 0
    for label, text in CASES:
        py = python_tokens(text)
        ch = clickhouse_tokens(text)
        sp = spark_map.get(text)

        # Spark's array_distinct drops repeats, so compare as sets.
        agree_ch = set(py) == set(ch)
        agree_sp = sp is None or set(py) == set(sp)
        ok = agree_ch and agree_sp

        print(f"{'PASS' if ok else 'FAIL'}  {label}")
        if not ok:
            failures += 1
            print(f"        python     : {sorted(set(py))}")
            print(f"        clickhouse : {sorted(set(ch))}")
            if sp is not None:
                print(f"        spark      : {sorted(set(sp))}")

    print()
    if failures:
        print(f"{failures} of {len(CASES)} cases DISAGREE — the engines tokenise differently")
        return 1
    print(f"all {len(CASES)} cases agree across every engine")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
