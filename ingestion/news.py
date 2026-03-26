import httpx
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

import trafilatura
from config.settings import POLYGON_API_KEY, NEWSAPI_KEY, NEWS_ARTICLE_LIMIT, load_managers
from db.database import init_db, insert_document, get_connection

logger = logging.getLogger(__name__)

POLYGON_BASE = "https://api.polygon.io/v2/reference/news"
NEWSAPI_BASE = "https://newsapi.org/v2/everything"


def fetch_article_text(url: str) -> Optional[str]:
    """Fetch and extract full article text using trafilatura."""
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(url, follow_redirects=True)
            resp.raise_for_status()
            downloaded = resp.text
        if not downloaded:
            return None
        text = trafilatura.extract(downloaded)
        return text
    except Exception as e:
        logger.warning("Failed to extract text from %s: %s", url, e)
        return None


def fetch_news_polygon(manager_name: str, since: str, until: str) -> list[dict]:
    """Fetch news from Polygon.io news API."""
    if not POLYGON_API_KEY:
        return []

    articles = []
    params = {
        "q": f'"{manager_name}"',
        "published_utc.gte": since,
        "published_utc.lte": until,
        "limit": NEWS_ARTICLE_LIMIT,
        "apiKey": POLYGON_API_KEY,
        "sort": "published_utc",
        "order": "desc",
    }

    try:
        with httpx.Client(timeout=30) as client:
            resp = client.get(POLYGON_BASE, params=params)
            resp.raise_for_status()
            data = resp.json()

        for article in data.get("results", []):
            articles.append({
                "title": article.get("title", ""),
                "url": article.get("article_url", ""),
                "published_at": article.get("published_utc", "")[:10],  # YYYY-MM-DD
                "description": article.get("description", ""),
                "source": article.get("publisher", {}).get("name", ""),
            })

        logger.info("Polygon returned %d articles for '%s'", len(articles), manager_name)
    except Exception as e:
        logger.error("Polygon API error for '%s': %s", manager_name, e)

    return articles


def fetch_news_newsapi(manager_name: str, since: str, until: str) -> list[dict]:
    """Fetch news from NewsAPI.org."""
    if not NEWSAPI_KEY:
        logger.warning("No NEWSAPI_KEY set, skipping NewsAPI")
        return []

    articles = []
    params = {
        "q": f'"{manager_name}"',
        "from": since,
        "to": until,
        "language": "en",
        "sortBy": "publishedAt",
        "apiKey": NEWSAPI_KEY,
        "pageSize": 100,
    }

    try:
        with httpx.Client(timeout=30) as client:
            resp = client.get(NEWSAPI_BASE, params=params)
            resp.raise_for_status()
            data = resp.json()

        if data.get("status") != "ok":
            logger.warning("NewsAPI returned non-ok status: %s", data.get("message"))
            return []

        for article in data.get("articles", []):
            published = article.get("publishedAt", "")[:10]
            articles.append({
                "title": article.get("title", ""),
                "url": article.get("url", ""),
                "published_at": published,
                "description": article.get("description", ""),
                "source": article.get("source", {}).get("name", ""),
            })

        logger.info("NewsAPI returned %d articles for '%s'", len(articles), manager_name)
    except Exception as e:
        logger.error("NewsAPI error for '%s': %s", manager_name, e)

    return articles


def is_already_stored(url: str) -> bool:
    """Check if a URL is already in the documents table with non-empty text.

    Returns False for documents that were stored without text content so that
    a subsequent ingestion run can re-fetch and populate the text.
    """
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id FROM documents WHERE url = ? AND raw_text IS NOT NULL AND TRIM(raw_text) != ''",
            (url,),
        ).fetchone()
    return row is not None


def fetch_and_store_news(manager: dict, since: str = None, until: str = None) -> int:
    """
    Fetch news for a manager and store to documents table.
    Returns count of new documents stored.
    """
    manager_id = manager["id"]
    manager_name = manager["name"]

    if until is None:
        until = date.today().isoformat()
    if since is None:
        since = (date.today() - timedelta(days=7)).isoformat()

    logger.info("Fetching news for %s from %s to %s", manager_name, since, until)

    # Try NewsAPI first (correctly filters by query string); fall back to Polygon
    # Note: Polygon free tier ignores the q parameter and returns generic articles
    articles = fetch_news_newsapi(manager_name, since, until)
    if not articles:
        articles = fetch_news_polygon(manager_name, since, until)

    if not articles:
        logger.warning("No articles found for %s", manager_name)
        return 0

    stored = 0
    for article in articles:
        url = article.get("url", "")
        if not url:
            continue

        # Skip if already stored
        if is_already_stored(url):
            logger.debug("Skipping already-stored URL: %s", url)
            continue

        # Fetch full article text
        full_text = fetch_article_text(url)

        # Build combined text: title + description + full body
        title = article.get("title", "")
        description = article.get("description", "")

        if full_text:
            combined = f"{title}\n\n{description}\n\n{full_text}"
        else:
            combined = f"{title}\n\n{description}"

        # Filter: skip articles with insufficient content
        if len(combined) < 200:
            logger.debug("Skipping article (content too short, %d chars): %s", len(combined), title)
            continue

        # Filter: only store if the manager's name appears in the content
        if manager_name.lower() not in combined.lower():
            logger.debug("Skipping article (manager name not found): %s", title)
            continue

        availability_date = article.get("published_at", until)

        try:
            doc_id = insert_document(
                manager=manager_id,
                source_type="news",
                availability_date=availability_date,
                url=url,
                raw_text=combined,
            )
            stored += 1
            logger.info("Stored news article: %s (doc_id=%d)", title[:80], doc_id)
        except Exception as e:
            logger.error("Error storing article '%s': %s", title[:60], e)

    logger.info("Stored %d new articles for %s", stored, manager_name)
    return stored


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    init_db()
    managers = load_managers()
    ackman = next(m for m in managers if m["id"] == "ackman")

    since = (date.today() - timedelta(days=7)).isoformat()
    until = date.today().isoformat()

    count = fetch_and_store_news(ackman, since=since, until=until)
    print(f"\nStored {count} new articles for Bill Ackman")

    # Print headlines
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT url, availability_date, substr(raw_text, 1, 120) as headline
            FROM documents
            WHERE manager = 'ackman' AND source_type = 'news'
            ORDER BY availability_date DESC
            LIMIT 10
        """).fetchall()

    print("\nRecent news articles:")
    for row in rows:
        print(f"  [{row['availability_date']}] {row['headline'][:100]}")
