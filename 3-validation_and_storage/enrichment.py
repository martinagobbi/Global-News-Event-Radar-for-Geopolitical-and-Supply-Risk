"""
3-validation_and_storage/enrichment.py
--------------------------------------
Article metadata extraction with Newspaper3k — moved here from the parsing
layer. It runs AFTER the GLOBALEVENTID filter, so only the surviving mentions
are ever scraped (no wasted network work).

Each unique article URL is fetched once, in parallel (scraping is network-I/O
bound, so threads give a near-linear speed-up). For each URL we extract:
    * article_title    — the headline                       ("" on failure)
    * article_keywords — comma-joined keywords from .nlp()  ("" if disabled/failed)
    * enriched         — True iff a title was obtained

Hard time budget
----------------
enrich_dataframe() stops after `time_budget_s` seconds (default 600 = 10 min).
Any mention whose URL has not been scraped by then is left with
("", "", False) — the run never blocks the pipeline longer than the budget.

If Newspaper3k (or its NLTK 'punkt' data) is unavailable, enrichment no-ops
cleanly: every mention gets ("", "", False).
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger("validation.enrichment")

# Imported lazily-ish so the module still loads if the dependency is missing.
try:
    from newspaper import Article, Config
    _NEWSPAPER_AVAILABLE = True
except Exception as _exc:  # ImportError or native/lxml issues
    _NEWSPAPER_AVAILABLE = False
    logger.warning("newspaper3k unavailable (%s); enrichment disabled", _exc)


def _build_config(request_timeout: int = 8) -> "Config":
    cfg = Config()
    cfg.request_timeout = request_timeout
    cfg.fetch_images = False
    cfg.memoize_articles = False
    cfg.browser_user_agent = (
        "Mozilla/5.0 (compatible; SupplyRiskRadar/1.0; +research project)"
    )
    return cfg


def extract_article_metadata(url: str, config=None, do_nlp: bool = True) -> dict:
    """Download + parse one article URL. Never raises."""
    if not _NEWSPAPER_AVAILABLE or not url:
        return {"title": "", "keywords": [], "ok": False}
    try:
        article = Article(url, config=config or _build_config())
        article.download()
        article.parse()
        title = (article.title or "").strip()
        keywords: list[str] = []
        if do_nlp:
            try:
                article.nlp()  # needs NLTK 'punkt'
                keywords = list(article.keywords or [])
            except Exception as exc:  # noqa: BLE001
                logger.debug("NLP step failed for %s: %s", url, exc)
        return {"title": title, "keywords": keywords, "ok": bool(title)}
    except Exception as exc:  # noqa: BLE001
        logger.debug("Could not fetch %s: %s", url, exc)
        return {"title": "", "keywords": [], "ok": False}


def enrich_dataframe(
    df,
    url_column: str = "MentionIdentifier",
    max_workers: int = 8,
    do_nlp: bool = True,
    time_budget_s: int = 600,
):
    """
    Add article_title / article_keywords / enriched columns to a mentions
    DataFrame, in place, scraping each unique URL once. Stops after
    `time_budget_s`; mentions not scraped by then keep ("", "", False).
    """
    # Defaults first, so the columns always exist even on an early return.
    df["article_title"] = ""
    df["article_keywords"] = ""
    df["enriched"] = False

    if df.empty or not _NEWSPAPER_AVAILABLE:
        if not _NEWSPAPER_AVAILABLE and not df.empty:
            logger.warning("Skipping enrichment of %d mentions: newspaper3k unavailable",
                           len(df))
        return df

    unique_urls = [u for u in df[url_column].unique() if u]
    if not unique_urls:
        return df

    config = _build_config()
    results: dict[str, dict] = {}

    pool = ThreadPoolExecutor(max_workers=max_workers)
    futures = {pool.submit(extract_article_metadata, u, config, do_nlp): u
               for u in unique_urls}
    started = time.monotonic()
    try:
        for fut in as_completed(futures, timeout=time_budget_s):
            url = futures[fut]
            try:
                results[url] = fut.result()
            except Exception as exc:  # noqa: BLE001
                logger.debug("Worker failed for %s: %s", url, exc)
                results[url] = {"title": "", "keywords": [], "ok": False}
    except TimeoutError:
        logger.warning(
            "Enrichment hit %ds budget: %d/%d URLs scraped; the rest left un-enriched",
            time_budget_s, len(results), len(unique_urls),
        )
    finally:
        # Don't wait on the stragglers; cancel anything not yet started.
        pool.shutdown(wait=False, cancel_futures=True)

    # Map results back onto every mention (URLs with no result stay un-enriched).
    df["article_title"] = df[url_column].map(
        lambda u: results.get(u, {}).get("title", ""))
    df["article_keywords"] = df[url_column].map(
        lambda u: ", ".join(results.get(u, {}).get("keywords", [])))
    df["enriched"] = df[url_column].map(
        lambda u: bool(results.get(u, {}).get("ok", False)))

    logger.info("Enriched %d/%d unique URLs (%.0fs, %d workers)",
                sum(1 for r in results.values() if r.get("ok")),
                len(unique_urls), time.monotonic() - started, max_workers)
    return df
