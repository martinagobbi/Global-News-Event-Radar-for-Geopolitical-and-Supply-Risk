"""
2-parsing/enrichment.py
------------------------
Article metadata extraction from source URLs using Newspaper3k.

Why threads (worker nodes) here
-------------------------------
Scraping an article URL is dominated by network I/O (DNS, TLS, download),
not CPU. During I/O the Python GIL is released, so a ThreadPoolExecutor with
N workers fetches N URLs concurrently and gives a near-linear speed-up — far
simpler and lighter than multiprocessing, which only helps for CPU-bound work.
The light NLP step (keyword extraction) is CPU work but small relative to the
download, so threads remain the right choice. Worker count is configurable via
the MENTION_ENRICH_WORKERS environment variable (see main.py).

Reliability notes
-----------------
- Many URLs fail (paywalls, 404, blocks, timeouts). Every failure degrades
  gracefully: the mention is kept with empty title/keywords and enriched=False.
- URLs are de-duplicated before scraping: many mentions point to the same
  article, so each unique URL is fetched at most once per batch.
- A short request timeout prevents a single slow host from stalling the batch.
- newspaper3k requires the NLTK 'punkt' tokenizer for keyword extraction;
  the Dockerfile downloads it at build time. If it is missing, keyword
  extraction is skipped but titles are still extracted.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

# Newspaper3k is imported lazily so this module loads even if the dependency
# (or its native build deps) is unavailable — enrichment then no-ops cleanly.
try:
    from newspaper import Article, Config
    _NEWSPAPER_AVAILABLE = True
except Exception as _exc:  # ImportError or native/lxml issues
    _NEWSPAPER_AVAILABLE = False
    logger.warning("newspaper3k unavailable (%s); mention enrichment disabled", _exc)


def _build_config(request_timeout: int = 8) -> "Config":
    """Shared Newspaper3k config: short timeout, no images, browser-like UA."""
    cfg = Config()
    cfg.request_timeout = request_timeout
    cfg.fetch_images = False          # we only need text metadata
    cfg.memoize_articles = False
    cfg.browser_user_agent = (
        "Mozilla/5.0 (compatible; SupplyRiskRadar/1.0; +research project)"
    )
    return cfg


def extract_article_metadata(url: str, config=None, do_nlp: bool = True) -> dict:
    """
    Download and parse a single article URL.

    Returns
    -------
    dict with keys:
        title    : str        — article headline ("" on failure)
        keywords : list[str]   — extracted keywords ([] if NLP unavailable)
        ok       : bool        — True if a title was successfully extracted

    Never raises: any error returns ok=False with empty fields.
    """
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
            except Exception as exc:
                logger.debug("NLP step failed for %s: %s", url, exc)

        return {"title": title, "keywords": keywords, "ok": bool(title)}

    except Exception as exc:
        logger.debug("Could not fetch %s: %s", url, exc)
        return {"title": "", "keywords": [], "ok": False}


def enrich_mentions_parallel(
    mentions: list[dict],
    max_workers: int = 8,
    do_nlp: bool = True,
) -> list[dict]:
    """
    Enrich a list of silver-mention dicts in parallel.

    Each mention must carry a 'mention_url' key (from to_silver_mention()).
    Adds/overwrites three fields on every mention:
        article_title    : str
        article_keywords : str  (comma-separated)
        enriched         : bool

    Unique URLs are scraped once and the result fanned out to all mentions
    sharing that URL.

    Parameters
    ----------
    mentions    : list[dict] — silver mentions to enrich
    max_workers : int        — number of concurrent scraping threads
    do_nlp      : bool       — whether to run keyword extraction (slower)
    """
    if not mentions:
        return mentions

    # If Newspaper3k is unavailable, mark everything un-enriched and return.
    if not _NEWSPAPER_AVAILABLE:
        for m in mentions:
            m["article_title"] = ""
            m["article_keywords"] = ""
            m["enriched"] = False
        logger.warning("Skipping enrichment of %d mentions: newspaper3k unavailable", len(mentions))
        return mentions

    # De-duplicate URLs: many mentions reference the same article
    unique_urls = {m.get("mention_url", "") for m in mentions if m.get("mention_url")}

    if not unique_urls:
        for m in mentions:
            m["article_title"] = ""
            m["article_keywords"] = ""
            m["enriched"] = False
        return mentions

    config = _build_config()
    results: dict[str, dict] = {}

    # ── Worker pool: each thread scrapes one URL at a time ────────────────────
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_url = {
            pool.submit(extract_article_metadata, url, config, do_nlp): url
            for url in unique_urls
        }
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                results[url] = future.result()
            except Exception as exc:
                logger.debug("Worker failed for %s: %s", url, exc)
                results[url] = {"title": "", "keywords": [], "ok": False}

    # ── Map results back onto every mention ───────────────────────────────────
    enriched_count = 0
    for m in mentions:
        url = m.get("mention_url", "")
        meta = results.get(url, {"title": "", "keywords": [], "ok": False})
        m["article_title"] = meta["title"]
        m["article_keywords"] = ", ".join(meta["keywords"])
        m["enriched"] = meta["ok"]
        if meta["ok"]:
            enriched_count += 1

    logger.info(
        "Enriched %d/%d mentions (%d unique URLs, %d workers)",
        enriched_count, len(mentions), len(unique_urls), max_workers,
    )
    return mentions
