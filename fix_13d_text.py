#!/usr/bin/env python3
"""
fix_13d_text.py — Backfill raw_text for EDGAR documents where it is NULL or empty.

For each document whose source_type contains 'edgar' and whose raw_text is
missing, this script:
  1. Parses the CIK and accession number from the stored url.
  2. Fetches the primary filing document from SEC EDGAR.
  3. Extracts readable plain text from HTML (via trafilatura, with regex fallback).
  4. Writes the result back to raw_text in the database.

Run from the smart-money-agent directory:
    python fix_13d_text.py
"""

import logging
import re
import sys
import time
from pathlib import Path

# Ensure project root is on the path so we can import project modules.
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

import httpx

from db.database import get_connection

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("fix_13d_text")

# ---------------------------------------------------------------------------
# SEC EDGAR constants (mirror edgar.py)
# ---------------------------------------------------------------------------
USER_AGENT = "smart-money-agent contact@smartmoneyagent.dev"
RATE_LIMIT_DELAY = 0.12  # SEC recommends no more than 10 requests/second


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_url(url: str):
    """Return (cik_str, accession_dashed) from a stored EDGAR filing-index URL.

    Expected URL pattern:
        https://www.sec.gov/Archives/edgar/data/{cik}/{accession_nodash}/
    """
    if not url:
        return None, None
    # Match: .../edgar/data/<cik>/<18-digit-accession>/
    m = re.search(r"/edgar/data/(\d+)/(\d{18})/?", url)
    if not m:
        return None, None
    cik = m.group(1)
    acc_clean = m.group(2)
    # Convert 18-digit no-dash form to standard 20-char dashed form
    # Format: XXXXXXXXXX-YY-ZZZZZZ  (10 + 2 + 6 chars)
    accession = f"{acc_clean[0:10]}-{acc_clean[10:12]}-{acc_clean[12:18]}"
    return cik, accession


def _fetch_filing_index(cik: str, accession: str) -> dict:
    """Fetch the SEC filing index HTML and return {documents: [{type, document, href}]}."""
    padded = cik.zfill(10)
    acc_clean = accession.replace("-", "")
    url = (
        f"https://www.sec.gov/Archives/edgar/data/{int(padded)}"
        f"/{acc_clean}/{accession}-index.html"
    )
    try:
        with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=30) as client:
            resp = client.get(url)
            time.sleep(RATE_LIMIT_DELAY)
            if resp.status_code == 404:
                logger.warning("Filing index 404: %s", url)
                return {}
            resp.raise_for_status()
    except Exception as exc:
        logger.warning("Could not fetch filing index %s: %s", url, exc)
        return {}

    html = resp.text
    documents = []
    # Parse rows of the filing-index table (same regex pattern as edgar.py)
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.IGNORECASE | re.DOTALL):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.IGNORECASE | re.DOTALL)
        if len(cells) < 3:
            continue
        dtype = re.sub(r"<[^>]+>", "", cells[0]).strip()
        description = re.sub(r"<[^>]+>", "", cells[1]).strip() if len(cells) > 1 else ""
        doc_cell = cells[2] if len(cells) > 2 else ""
        href_m = re.search(r'href="([^"]+)"', doc_cell, re.IGNORECASE)
        fname_m = re.search(r">([^<]+)<", doc_cell)
        if href_m:
            documents.append({
                "type": dtype,
                "description": description,
                "document": fname_m.group(1).strip() if fname_m else "",
                "href": href_m.group(1),
            })
    return {"documents": documents}


def _extract_text_from_url(doc_url: str) -> str | None:
    """Download a filing document and return plain text."""
    try:
        with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=60) as client:
            resp = client.get(doc_url)
            time.sleep(RATE_LIMIT_DELAY)
            if resp.status_code == 404:
                logger.warning("Document 404: %s", doc_url)
                return None
            resp.raise_for_status()
    except Exception as exc:
        logger.warning("Failed to download %s: %s", doc_url, exc)
        return None

    content = resp.text
    lower_url = doc_url.lower()

    if lower_url.endswith((".htm", ".html")):
        try:
            import trafilatura
            text = trafilatura.extract(content)
            if text:
                return text
        except Exception:
            pass
        # Fallback: strip HTML tags
        text = re.sub(r"<[^>]+>", " ", content)
    else:
        # Plain-text / SGML full-submission files
        text = re.sub(r"<[^>]+>", " ", content)

    text = re.sub(r"\s{3,}", "\n\n", text).strip()
    return text or None


def _fetch_filing_text(cik: str, accession: str, form_type: str = "") -> str | None:
    """Fetch readable text for a 13D/13G filing (no local cache)."""
    index = _fetch_filing_index(cik, accession)
    documents = index.get("documents", [])

    primary_href = None
    form_upper = form_type.upper().replace(" ", "")

    # Pass 1: document whose type matches the form
    for doc in documents:
        dtype = doc.get("type", "").upper().replace(" ", "")
        fname = doc.get("document", "")
        if dtype == form_upper and fname.lower().endswith((".htm", ".html", ".txt")):
            primary_href = doc.get("href", "")
            break

    # Pass 2: first .htm / .txt in the index
    if not primary_href:
        for doc in documents:
            fname = doc.get("document", "")
            if fname.lower().endswith((".htm", ".html", ".txt")):
                primary_href = doc.get("href", "")
                break

    if not primary_href:
        logger.warning("No primary document found for CIK=%s accession=%s", cik, accession)
        return None

    doc_url = f"https://www.sec.gov{primary_href}"
    logger.debug("Fetching document: %s", doc_url)
    return _extract_text_from_url(doc_url)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    conn = get_connection()

    # Fetch all EDGAR documents with missing raw_text
    rows = conn.execute(
        """
        SELECT id, manager, source_type, url
        FROM documents
        WHERE (raw_text IS NULL OR TRIM(raw_text) = '')
          AND source_type LIKE '%edgar%'
        ORDER BY availability_date
        """
    ).fetchall()
    rows = [dict(r) for r in rows]

    total = len(rows)
    if total == 0:
        logger.info("No EDGAR documents with missing text found. Nothing to do.")
        return

    logger.info("Found %d EDGAR document(s) with missing raw_text.", total)

    updated = 0
    failed = 0

    for i, doc in enumerate(rows, start=1):
        doc_id = doc["id"]
        manager = doc["manager"]
        source_type = doc["source_type"]
        url = doc.get("url") or ""

        logger.info("[%d/%d] id=%d manager=%s type=%s url=%s",
                    i, total, doc_id, manager, source_type, url or "(none)")

        cik, accession = _parse_url(url)
        if not cik or not accession:
            logger.warning("  -> Cannot parse CIK/accession from url — skipping.")
            failed += 1
            continue

        logger.info("  -> CIK=%s accession=%s", cik, accession)

        # Infer form type from source_type (edgar_13d → "SC 13D", etc.)
        if "13d" in source_type.lower():
            form_type = "SC 13D"
        elif "13g" in source_type.lower():
            form_type = "SC 13G"
        else:
            form_type = ""

        text = _fetch_filing_text(cik, accession, form_type)

        if text:
            conn.execute(
                "UPDATE documents SET raw_text = ? WHERE id = ?",
                (text, doc_id),
            )
            conn.commit()
            preview = text[:120].replace("\n", " ")
            logger.info("  -> Updated %d chars. Preview: %s…", len(text), preview)
            updated += 1
        else:
            logger.warning("  -> Failed to retrieve text for id=%d.", doc_id)
            failed += 1

    conn.close()
    logger.info(
        "Done. %d/%d documents updated, %d failed/skipped.",
        updated, total, failed,
    )


if __name__ == "__main__":
    main()
