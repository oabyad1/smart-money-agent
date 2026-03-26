"""PDF and HTML scraper for fund investor letters."""
import hashlib
import logging
import sys
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

sys.path.insert(0, str(Path(__file__).parent.parent))

import httpx
import trafilatura
from bs4 import BeautifulSoup

from config.settings import CACHE_DIR, load_managers
from db.database import get_connection, init_db, insert_document

logger = logging.getLogger(__name__)

CACHE_PATH = Path(CACHE_DIR) / "fund_letters"
HEADERS = {
    "User-Agent": "smart-money-agent contact@smartmoneyagent.dev",
    "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
}
REQUEST_TIMEOUT = 30.0


def _cache_key(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()


def _get_cached(url: str) -> Optional[bytes]:
    CACHE_PATH.mkdir(parents=True, exist_ok=True)
    p = CACHE_PATH / _cache_key(url)
    if p.exists():
        return p.read_bytes()
    return None


def _set_cached(url: str, content: bytes) -> None:
    CACHE_PATH.mkdir(parents=True, exist_ok=True)
    (CACHE_PATH / _cache_key(url)).write_bytes(content)


def fetch_url_bytes(url: str) -> Optional[bytes]:
    """Fetch raw bytes from URL, using cache if available."""
    cached = _get_cached(url)
    if cached is not None:
        logger.debug("Cache hit for %s", url)
        return cached

    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=True,
                          headers=HEADERS) as client:
            resp = client.get(url)
            resp.raise_for_status()
            content = resp.content
            _set_cached(url, content)
            return content
    except httpx.HTTPError as e:
        logger.error("HTTP error fetching %s: %s", url, e)
        return None
    except Exception as e:
        logger.error("Unexpected error fetching %s: %s", url, e)
        return None


def extract_text_from_pdf(pdf_bytes: bytes) -> Optional[str]:
    """Extract plain text from a PDF using pypdf."""
    try:
        from pypdf import PdfReader
        reader = PdfReader(BytesIO(pdf_bytes))
        parts = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                parts.append(text)
        return "\n\n".join(parts) if parts else None
    except Exception as e:
        logger.error("PDF extraction error: %s", e)
        return None


def extract_text_from_html(html_bytes: bytes, url: str) -> Optional[str]:
    """Extract article text from HTML using trafilatura."""
    try:
        html_str = html_bytes.decode("utf-8", errors="replace")
        text = trafilatura.extract(html_str)
        return text
    except Exception as e:
        logger.error("HTML extraction error for %s: %s", url, e)
        return None


def _is_pdf_url(url: str) -> bool:
    return url.lower().endswith(".pdf") or "pdf" in url.lower()


def _find_letter_links(index_url: str) -> list[dict]:
    """
    Fetch an index/letters page and find links to fund letters.
    Returns list of {url, title, guessed_date}.
    """
    content = fetch_url_bytes(index_url)
    if not content:
        return []

    try:
        soup = BeautifulSoup(content, "html.parser")
    except Exception as e:
        logger.error("BeautifulSoup parse error for %s: %s", index_url, e)
        return []

    base = "{uri.scheme}://{uri.netloc}".format(uri=urlparse(index_url))
    links = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True)

        # Only follow links that look like letters/memos/reports
        lower_text = text.lower()
        lower_href = href.lower()
        if not any(kw in lower_text or kw in lower_href
                   for kw in ["letter", "memo", "annual", "quarterly", "report",
                               "update", "outlook", "q1", "q2", "q3", "q4",
                               "2020", "2021", "2022", "2023", "2024", "2025", ".pdf"]):
            continue

        full_url = href if href.startswith("http") else urljoin(base, href)

        # Guess a year from the link text or URL
        guessed_date = None
        for year in range(2015, date.today().year + 1):
            if str(year) in href or str(year) in text:
                guessed_date = f"{year}-12-31"
                break

        links.append({
            "url": full_url,
            "title": text,
            "guessed_date": guessed_date,
        })

    # Deduplicate by URL
    seen = set()
    unique = []
    for link in links:
        if link["url"] not in seen:
            seen.add(link["url"])
            unique.append(link)

    return unique


def _already_stored(url: str) -> bool:
    """Check if a document with this URL is already in the database."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id FROM documents WHERE url = ? LIMIT 1", (url,)
        ).fetchone()
    return row is not None


def fetch_fund_letter(url: str, manager_id: str, content_date: Optional[str] = None) -> Optional[int]:
    """
    Fetch and store a single fund letter (PDF or HTML).
    Returns document id on success, None on failure.
    """
    if _already_stored(url):
        logger.debug("Already stored: %s", url)
        return None

    content = fetch_url_bytes(url)
    if not content:
        logger.warning("Could not fetch fund letter: %s", url)
        return None

    if _is_pdf_url(url) or content[:4] == b"%PDF":
        text = extract_text_from_pdf(content)
        logger.debug("Extracted PDF text from %s (%d chars)", url,
                     len(text) if text else 0)
    else:
        text = extract_text_from_html(content, url)
        logger.debug("Extracted HTML text from %s (%d chars)", url,
                     len(text) if text else 0)

    if not text or len(text.strip()) < 100:
        logger.warning("Too little text extracted from %s — skipping", url)
        return None

    availability_date = date.today().isoformat()

    doc_id = insert_document(
        manager=manager_id,
        source_type="fund_letter",
        availability_date=availability_date,
        content_date=content_date,
        url=url,
        raw_text=text,
    )
    logger.info("Stored fund letter: manager=%s url=%s doc_id=%d", manager_id, url, doc_id)
    return doc_id


def fetch_fund_letters_for_manager(manager: dict) -> int:
    """
    Fetch all discoverable fund letters for a manager.
    Returns count of new documents stored.
    """
    letter_url = manager.get("fund_letter_url")
    if not letter_url:
        logger.debug("No fund_letter_url for %s — skipping", manager["id"])
        return 0

    logger.info("Fetching fund letters for %s from %s", manager["name"], letter_url)
    stored = 0

    # First, try to find letter links from the index page
    links = _find_letter_links(letter_url)

    if links:
        logger.info("Found %d candidate letter links for %s", len(links), manager["name"])
        for link in links:
            doc_id = fetch_fund_letter(
                url=link["url"],
                manager_id=manager["id"],
                content_date=link.get("guessed_date"),
            )
            if doc_id:
                stored += 1
    else:
        # Fallback: treat the index URL itself as a letter
        logger.info("No letter links found; trying index URL directly for %s", manager["name"])
        doc_id = fetch_fund_letter(url=letter_url, manager_id=manager["id"])
        if doc_id:
            stored += 1

    logger.info("Stored %d new fund letters for %s", stored, manager["name"])
    return stored


def fetch_all_fund_letters() -> int:
    """Fetch fund letters for all configured managers. Returns total stored count."""
    init_db()
    managers = load_managers()
    total = 0
    for manager in managers:
        try:
            count = fetch_fund_letters_for_manager(manager)
            total += count
        except Exception as e:
            logger.error("Error fetching fund letters for %s: %s", manager["id"], e)
    logger.info("Fund letter fetch complete. Total new documents: %d", total)
    return total


if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(name)s %(message)s")
    total = fetch_all_fund_letters()
    print(f"Stored {total} new fund letters.")
