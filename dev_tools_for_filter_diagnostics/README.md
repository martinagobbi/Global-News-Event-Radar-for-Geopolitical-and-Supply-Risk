# Filter diagnostics

Explains, for every article in the gold layer, **why it got there** — which
bronze-to-silver rule admitted it, and which of the user's territory and keyword
criteria selected it.

The pipeline is a chain of narrow filters: roughly 97% of each GDELT slice is
discarded on the way to silver, and a large further fraction on the way to gold.
When a user's pool looks too small or too large, the useful question is which
rule is actually deciding. This tool answers it for the articles that survived.

## Running it

```bash
docker compose -f docker-compose.diagnostics.yml run --rm diagnostics
```

The stores must be running. The pipeline tier need not be. The run takes a couple
of seconds and produces `gold_provenance.csv` in this directory.

It is **diagnostic only**: Oracle, ClickHouse and MongoDB are opened read-only and
nothing is written to any of them, so it is safe to run at any time, including
while the pipeline is live.

## Why it cannot drift from the real filters

The script imports the pipeline's own filter code rather than restating it:

| Import | Supplies |
|----|----|
| `2-parsing/parser.py` | the bronze-to-silver criteria, and the code and keyword sets behind them |
| `4-processing/countries.py` | territory name → CAMEO and FIPS codes |
| `4-processing/processor.py` | keyword tokenisation, stemming and URL normalisation |

If a rule changes, the explanation changes with it. The one thing the script
restates is the *shape* of each decision — which is the point, since that is what
it is reporting.

## The columns

`gold_provenance.csv` holds one row per `(user_id, doc_id)` pair — that is, per
article per user, since the same article may reach several users.

| Column | Meaning |
|----|----|
| `user_id`, `doc_id`, `url`, `article_title` | which article, for whom |
| `global_event_id`, `event_dateadded` | the silver event it came from |
| `parse_f1_event_code` | the code satisfying F1, as `EventCode=…` or `EventRootCode=…` |
| `parse_f2_type_or_group` | the actor type or known group satisfying F2, if any |
| `parse_f3_alt_keyword` | the supply-chain words found in the actor names or URL, if any |
| `parse_branch` | `F2`, `F3` or `F2+F3` — which arm of the disjunction carried it |
| `geo_match_system` | `CAMEO` (matched an actor), `FIPS` (matched a location), or both |
| `geo_match_code`, `geo_match_field` | the code, and the column it was found in |
| `keyword_matched` | the user keyword that fired |
| `keyword_tokens` | the tokens or URL variant it reduced to |
| `keyword_match_field` | `url`, `article_title` or `article_keywords` |

The parsing filter is `F1 AND (F2 OR F3) AND has_source_url`, so `parse_f1_event_code`
is always populated, and at least one of F2/F3 always is. The territory and
keyword conditions are combined with **and**.

Two values in `keyword_match_field` indicate something other than a normal match:

- `(silver row gone)` — the gold row outlived the silver row it was derived from.
  Gold is only rebuilt for events still present in silver, so this marks a row
  that would no longer be produced today.
- `(stale gold row)` — the row is in gold, but no current keyword matches it. This
  is what a filter change looks like after the fact: the row was admitted under
  the previous rules and has not yet been rebuilt.

## What it showed

Run against the 30-day seed **before** the keyword matcher was fixed, it reported
nine gold articles, all matching on the **URL** and none on a title or on
extracted keywords. That was the visible symptom of two defects: the bootstrap
image was missing the NLTK tokeniser, so `article_keywords` was empty on all
99,175 enriched rows, and the matcher routed each row to a single field, so
enriched rows were never checked against their URL at all.

Run after both fixes and a full re-run of the backfill:

| | Before | After |
|----|----|----|
| Gold articles | 9 | **164** |
| `radar_agrifood` / `radar_electronics` / `radar_pharma` | 9 / 0 / 0 | **127 / 21 / 16** |
| Matched on `url` | 9 | 145 |
| Matched on `article_title` | 0 | 9 |
| Matched on `article_keywords` | 0 | **10** |

The last row is the one that could not have existed before: `article_keywords` was
empty on every row in the store, so no article could ever be selected by it. It is
now a working match field.

The parsing breakdown also becomes visible: 125 articles were admitted on the
actor-type criterion alone, 20 on the supply-chain-keyword criterion alone, and 19
on both. Territory matching splits 84 on location only, 17 on actor only, and 63
on both — which is the concrete case for keeping both code systems rather than
picking one.
