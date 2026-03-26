# ingestion/edgar.py
import asyncio
import httpx
import json
import logging
import os
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import CACHE_DIR, load_managers
from db.database import (get_connection, init_db, insert_document, insert_position,
                          get_latest_position, get_latest_13f_filing_date,
                          get_latest_positions_snapshot, get_latest_13d_filing_date)

logger = logging.getLogger(__name__)

EDGAR_BASE = "https://data.sec.gov"
EDGAR_BROWSE = "https://www.sec.gov/cgi-bin/browse-edgar"
USER_AGENT = "smart-money-agent contact@smartmoneyagent.dev"
RATE_LIMIT_DELAY = 0.12  # slightly above 0.1 for safety

CACHE_PATH = Path(CACHE_DIR) / "edgar"


def _cache_path(filename: str) -> Path:
    CACHE_PATH.mkdir(parents=True, exist_ok=True)
    return CACHE_PATH / filename


def _get_cached(filename: str) -> Optional[str]:
    p = _cache_path(filename)
    if p.exists():
        return p.read_text(encoding="utf-8")
    return None


def _set_cached(filename: str, content: str) -> None:
    _cache_path(filename).write_text(content, encoding="utf-8")


def _headers() -> dict:
    return {"User-Agent": USER_AGENT, "Accept": "application/json"}


def _pad_cik(cik: str) -> str:
    digits = cik.lstrip("0") or "0"
    return digits.zfill(10)


def fetch_company_tickers() -> dict:
    """Fetch CUSIP->ticker mapping from SEC. Returns dict keyed by CIK str."""
    cache_key = "company_tickers.json"
    cached = _get_cached(cache_key)
    if cached:
        return json.loads(cached)

    with httpx.Client(headers=_headers(), timeout=30) as client:
        resp = client.get("https://www.sec.gov/files/company_tickers.json")
        resp.raise_for_status()
        time.sleep(RATE_LIMIT_DELAY)

    data = resp.json()
    _set_cached(cache_key, json.dumps(data))
    logger.info("Fetched company tickers (%d entries)", len(data))
    return data


def build_cusip_to_ticker(tickers_data: dict) -> dict:
    """Build a ticker lookup by CIK (since SEC tickers.json doesn't have CUSIP)."""
    # Maps cik_str -> ticker
    result = {}
    for entry in tickers_data.values():
        cik_str = str(entry.get("cik_str", "")).zfill(10)
        ticker = entry.get("ticker", "")
        result[cik_str] = ticker
    return result


def fetch_submissions(cik: str) -> dict:
    """Fetch submission history for a CIK."""
    padded = _pad_cik(cik)
    cache_key = f"submissions_{padded}.json"
    cached = _get_cached(cache_key)
    if cached:
        return json.loads(cached)

    url = f"{EDGAR_BASE}/submissions/CIK{padded}.json"
    with httpx.Client(headers=_headers(), timeout=30) as client:
        resp = client.get(url)
        resp.raise_for_status()
        time.sleep(RATE_LIMIT_DELAY)

    data = resp.json()
    _set_cached(cache_key, json.dumps(data))
    logger.info("Fetched submissions for CIK %s", padded)
    return data


def get_filings_of_type(submissions: dict, form_type: str) -> list[dict]:
    """Extract filings of a given form type from submissions data."""
    recent = submissions.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    accessions = recent.get("accessionNumber", [])

    results = []
    for i, form in enumerate(forms):
        if form == form_type:
            results.append({
                "accession": accessions[i],
                "filing_date": dates[i],
                "form_type": form,
            })
    return results


def fetch_filing_index(cik: str, accession: str) -> dict:
    """Fetch the filing index HTML for a given accession number and return parsed documents list."""
    import re as _re
    padded = _pad_cik(cik)
    acc_clean = accession.replace("-", "")
    cache_key = f"index_{acc_clean}.json"
    cached = _get_cached(cache_key)
    if cached:
        return json.loads(cached)

    # SEC EDGAR HTML index — the JSON variant does not exist for older/newer filings
    url = f"https://www.sec.gov/Archives/edgar/data/{int(padded)}/{acc_clean}/{accession}-index.html"
    with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=30) as client:
        resp = client.get(url)
        if resp.status_code == 404:
            logger.warning("Filing index not found: %s", url)
            return {}
        resp.raise_for_status()
        time.sleep(RATE_LIMIT_DELAY)

    html = resp.text
    # Parse table rows: each has (seq, description, document href, type, size)
    # Pattern matches the raw file links (not the xsl-rendered ones)
    row_pattern = _re.compile(
        r'<td[^>]*>\s*(\d+)\s*</td>\s*'           # seq
        r'<td[^>]*>(.*?)</td>\s*'                  # description
        r'<td[^>]*><a href="(/Archives/[^"]+)">[^<]+</a></td>\s*'  # document href
        r'<td[^>]*>(.*?)</td>',                    # type
        _re.DOTALL | _re.IGNORECASE,
    )
    documents = []
    for m in row_pattern.finditer(html):
        href = m.group(3)
        doc_type = _re.sub(r'<[^>]+>', '', m.group(4)).strip()
        fname = href.split("/")[-1]
        documents.append({"document": fname, "type": doc_type, "href": href})

    data = {"documents": documents}
    _set_cached(cache_key, json.dumps(data))
    return data


def fetch_xml_holdings(cik: str, accession: str, filename: str) -> Optional[str]:
    """Fetch the XML holdings file for a 13F."""
    padded = _pad_cik(cik)
    acc_clean = accession.replace("-", "")
    # Include filename in cache key so different files for same accession don't collide
    safe_fname = filename.replace("/", "_").replace("\\", "_")
    cache_key = f"holdings_{acc_clean}_{safe_fname}"
    cached = _get_cached(cache_key)
    if cached:
        return cached

    url = f"https://www.sec.gov/Archives/edgar/data/{int(padded)}/{acc_clean}/{filename}"
    with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=60) as client:
        resp = client.get(url)
        if resp.status_code == 404:
            logger.warning("Holdings XML not found: %s", url)
            return None
        resp.raise_for_status()
        time.sleep(RATE_LIMIT_DELAY)

    content = resp.text
    _set_cached(cache_key, content)
    return content


def parse_13f_xml(xml_text: str) -> list[dict]:
    """Parse 13F XML holdings into a list of position dicts."""
    holdings = []

    # Try to strip namespace for easier parsing
    xml_text_clean = xml_text

    try:
        root = ET.fromstring(xml_text_clean)
    except ET.ParseError as e:
        logger.error("XML parse error: %s", e)
        return holdings

    # Handle namespaces — 13F uses various namespace prefixes
    ns_map = {}
    for prefix, uri in ET.iterparse.__doc__ and [] or []:
        ns_map[prefix] = uri

    # Try multiple namespace patterns used in 13F filings
    namespaces_to_try = [
        "",
        "{http://www.sec.gov/edgar/document/thirteenf/informationtable}",
        "{http://www.sec.gov/edgar/thirteenf/informationtable}",
    ]

    info_table = None
    for ns in namespaces_to_try:
        info_table = root.find(f".//{ns}informationTable")
        if info_table is not None:
            break

    if info_table is None:
        # Try finding infoTable directly
        info_table = root

    ns = ""
    # Detect namespace from first infoHolder element
    for elem in root.iter():
        tag = elem.tag
        if "}" in tag:
            ns = tag[:tag.index("}") + 1]
            break

    for entry in root.iter(f"{ns}infoTable"):
        try:
            def get_text(tag):
                el = entry.find(f"{ns}{tag}")
                return el.text.strip() if el is not None and el.text else ""

            name_of_issuer = get_text("nameOfIssuer")
            cusip = get_text("cusip")
            value_str = get_text("value")

            shares_el = entry.find(f"{ns}shrsOrPrnAmt")
            shares = 0
            if shares_el is not None:
                sh_el = shares_el.find(f"{ns}sshPrnamt")
                if sh_el is not None and sh_el.text:
                    try:
                        shares = int(sh_el.text.strip().replace(",", ""))
                    except ValueError:
                        shares = 0

            try:
                value_usd = int(value_str.replace(",", "")) * 1000  # SEC reports in thousands
            except ValueError:
                value_usd = 0

            holdings.append({
                "name_of_issuer": name_of_issuer,
                "cusip": cusip,
                "value_usd": value_usd,
                "shares": shares,
            })
        except Exception as e:
            logger.warning("Error parsing holding entry: %s", e)
            continue

    return holdings


def cusip_to_ticker_lookup(cusip: str, name: str, cusip_map: dict) -> str:
    """Best-effort CUSIP to ticker. Falls back to cleaning the issuer name."""
    # The SEC tickers.json doesn't have CUSIP; use name heuristics
    # Common mappings for major stocks
    KNOWN = {
        "037833100": "AAPL",
        "594918104": "MSFT",
        "02079K305": "GOOGL",
        "023135106": "AMZN",
        "88160R101": "TSLA",
        "67066G104": "NVDA",
        "46625H100": "JPM",
        "BAC": "BAC",
    }
    if cusip in KNOWN:
        return KNOWN[cusip]

    # Clean up name to approximate ticker
    name_upper = name.upper().replace(" ", "").replace(".", "").replace(",", "")
    # Return a placeholder — real system would use a CUSIP database
    return name[:10].upper().replace(" ", "").replace(".", "") or "UNKNOWN"


def process_13f_filing(manager_id: str, cik: str, filing: dict,
                        cusip_map: dict, prior_positions: dict) -> int:
    """Process a single 13F filing: fetch XML, parse, store positions. Returns count stored."""
    accession = filing["accession"]
    filing_date = filing["filing_date"]

    logger.info("Processing 13F for %s accession=%s date=%s", manager_id, accession, filing_date)

    # Fetch index to find the XML file
    index = fetch_filing_index(cik, accession)
    if not index:
        return 0

    # Find the information table XML — strictly prefer type "INFORMATION TABLE",
    # then fall back to name heuristics, then brute-force common filenames.
    xml_filename = None
    documents = index.get("documents", [])

    # Pass 1: exact type match "INFORMATION TABLE"
    seen_fnames = set()
    for doc in documents:
        fname = doc.get("document", "")
        dtype = doc.get("type", "").upper()
        if fname in seen_fnames:
            continue
        seen_fnames.add(fname)
        if "INFORMATION TABLE" in dtype and fname.lower().endswith(".xml"):
            xml_filename = fname
            break

    # Pass 2: filename heuristic (infotable, informationtable)
    if not xml_filename:
        seen_fnames2 = set()
        for doc in documents:
            fname = doc.get("document", "")
            if fname in seen_fnames2:
                continue
            seen_fnames2.add(fname)
            fl = fname.lower()
            if fl.endswith(".xml") and ("infotable" in fl or "informationtable" in fl):
                xml_filename = fname
                break

    # Pass 3: brute-force common naming patterns via direct fetch
    if not xml_filename:
        for suffix in ["infotable.xml", "form13fInfoTable.xml", "informationtable.xml"]:
            xml_text = fetch_xml_holdings(cik, accession, suffix)
            if xml_text and "<informationtable" in xml_text.lower():
                xml_filename = suffix
                break

    if not xml_filename:
        logger.warning("No XML holdings file found for %s %s", manager_id, accession)
        return 0

    xml_text = fetch_xml_holdings(cik, accession, xml_filename)
    if not xml_text:
        return 0

    holdings = parse_13f_xml(xml_text)
    if not holdings:
        logger.warning("No holdings parsed for %s %s", manager_id, accession)
        return 0

    # Compute period_of_report from the filing (approximate: ~45 days before filing)
    try:
        fd = datetime.strptime(filing_date, "%Y-%m-%d")
        # 13F covers quarter ending ~45 days before filing
        period_dt = fd - timedelta(days=45)
        # Round to nearest quarter end
        month = period_dt.month
        if month <= 3:
            period = date(period_dt.year - 1, 12, 31)
        elif month <= 6:
            period = date(period_dt.year, 3, 31)
        elif month <= 9:
            period = date(period_dt.year, 6, 30)
        else:
            period = date(period_dt.year, 9, 30)
        period_of_report = period.isoformat()
    except Exception:
        period_of_report = filing_date

    # Compute total portfolio value for pct calculation
    total_value = sum(h["value_usd"] for h in holdings) or 1

    stored = 0
    for h in holdings:
        ticker = cusip_to_ticker_lookup(h["cusip"], h["name_of_issuer"], cusip_map)
        shares = h["shares"]
        value_usd = h["value_usd"]
        pct = value_usd / total_value if total_value else 0

        # Compute delta vs prior quarter
        prior_key = f"{manager_id}_{ticker}"
        delta_shares = None
        delta_pct = None
        if prior_key in prior_positions:
            prior = prior_positions[prior_key]
            delta_shares = shares - prior["shares"]
            delta_pct = pct - prior.get("pct_of_portfolio", 0)

        try:
            insert_position(
                manager=manager_id,
                ticker=ticker,
                filing_date=filing_date,
                period_of_report=period_of_report,
                shares=shares,
                value_usd=value_usd,
                pct_of_portfolio=pct,
                delta_shares=delta_shares,
                delta_pct=delta_pct,
                filing_type="13F",
            )
            prior_positions[prior_key] = {"shares": shares, "pct_of_portfolio": pct}
            stored += 1
        except Exception as e:
            logger.error("Error inserting position %s %s: %s", manager_id, ticker, e)

    logger.info("Stored %d positions for %s filing %s", stored, manager_id, filing_date)
    return stored


def fetch_all_13f_for_manager(manager: dict) -> int:
    """Fetch all 13F filings for a manager. Returns total positions stored."""
    manager_id = manager["id"]
    cik = manager["cik"]
    logger.info("Starting 13F backfill for %s (CIK %s)", manager_id, cik)

    try:
        submissions = fetch_submissions(cik)
    except Exception as e:
        logger.error("Failed to fetch submissions for %s: %s", manager_id, e)
        return 0

    filings_13f = get_filings_of_type(submissions, "13F-HR")
    if not filings_13f:
        logger.warning("No 13F-HR filings found for %s", manager_id)
        return 0

    logger.info("Found %d 13F-HR filings for %s", len(filings_13f), manager_id)

    # Fetch company tickers for name resolution
    try:
        tickers_data = fetch_company_tickers()
        cusip_map = build_cusip_to_ticker(tickers_data)
    except Exception as e:
        logger.warning("Could not fetch company tickers: %s", e)
        cusip_map = {}

    # Process filings from oldest to newest to compute deltas correctly
    filings_sorted = sorted(filings_13f, key=lambda x: x["filing_date"])

    prior_positions = {}
    total_stored = 0

    for filing in filings_sorted:
        try:
            count = process_13f_filing(manager_id, cik, filing, cusip_map, prior_positions)
            total_stored += count
        except Exception as e:
            logger.error("Error processing filing %s for %s: %s",
                        filing["accession"], manager_id, e)
            continue

    logger.info("Total positions stored for %s: %d across %d filings",
                manager_id, total_stored, len(filings_sorted))
    return total_stored


def fetch_13d_filing_text(cik: str, accession: str, form_type: str) -> Optional[str]:
    """Fetch and return the text of the primary document in a 13D/13G filing."""
    import re as _re
    index = fetch_filing_index(cik, accession)
    documents = index.get("documents", [])

    primary_href = None
    # Pass 1: find document whose type matches the form (e.g. "SC 13D", "SC 13G")
    form_upper = form_type.upper().replace(" ", "")
    for doc in documents:
        dtype = doc.get("type", "").upper().replace(" ", "")
        fname = doc.get("document", "")
        if dtype == form_upper and fname.lower().endswith((".htm", ".html", ".txt")):
            primary_href = doc.get("href", "")
            break

    # Pass 2: fall back to first .htm or .txt document in the index
    if not primary_href:
        for doc in documents:
            fname = doc.get("document", "")
            if fname.lower().endswith((".htm", ".html", ".txt")):
                primary_href = doc.get("href", "")
                break

    if not primary_href:
        logger.warning("No primary document found in filing index for %s %s", cik, accession)
        return None

    url = f"https://www.sec.gov{primary_href}"
    cache_key = f"13d_text_{accession.replace('-', '')}_{primary_href.split('/')[-1]}"
    cached = _get_cached(cache_key)
    if cached:
        return cached

    try:
        with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=60) as client:
            resp = client.get(url)
            if resp.status_code == 404:
                logger.warning("Filing document not found: %s", url)
                return None
            resp.raise_for_status()
            time.sleep(RATE_LIMIT_DELAY)
        content = resp.text
    except Exception as e:
        logger.warning("Failed to fetch filing document %s: %s", url, e)
        return None

    # Extract plain text from HTML; keep raw text for plain-text filings
    if primary_href.lower().endswith((".htm", ".html")):
        try:
            import trafilatura
            text = trafilatura.extract(content) or _re.sub(r'<[^>]+>', ' ', content)
        except Exception:
            text = _re.sub(r'<[^>]+>', ' ', content)
    else:
        # Strip SGML/HTML tags present in many EDGAR .txt full-submission files
        text = _re.sub(r'<[^>]+>', ' ', content)
        text = _re.sub(r'\s{3,}', '\n\n', text).strip()

    _set_cached(cache_key, text)
    return text


def fetch_13d_filings_for_manager(manager: dict) -> int:
    """Fetch 13D/13G filings and store as documents. Returns count stored."""
    manager_id = manager["id"]
    cik = manager["cik"]

    try:
        submissions = fetch_submissions(cik)
    except Exception as e:
        logger.error("Failed to fetch submissions for %s: %s", manager_id, e)
        return 0

    filings_13d = get_filings_of_type(submissions, "SC 13D")
    filings_13g = get_filings_of_type(submissions, "SC 13G")
    all_filings = filings_13d + filings_13g

    stored = 0
    for filing in all_filings:
        try:
            filing_text = fetch_13d_filing_text(cik, filing["accession"], filing["form_type"])
            doc_id = insert_document(
                manager=manager_id,
                source_type="edgar_13d",
                availability_date=filing["filing_date"],
                url=f"https://www.sec.gov/Archives/edgar/data/{int(_pad_cik(cik))}/{filing['accession'].replace('-','')}/",
                raw_text=filing_text,
            )
            stored += 1
        except Exception as e:
            logger.error("Error inserting 13D doc: %s", e)

    if stored:
        logger.info("Stored %d 13D/13G filings for %s", stored, manager_id)
    return stored


def fetch_new_13f_for_manager(manager: dict) -> int:
    """Fetch only 13F filings newer than the most recent one stored for this manager.

    Used by the daily run. Falls back to processing all filings when no data exists yet
    (i.e. first run before a backfill). The backfill path uses fetch_all_13f_for_manager.
    """
    manager_id = manager["id"]
    cik = manager["cik"]

    latest_date = get_latest_13f_filing_date(manager_id)

    try:
        submissions = fetch_submissions(cik)
    except Exception as e:
        logger.error("Failed to fetch submissions for %s: %s", manager_id, e)
        return 0

    filings_13f = get_filings_of_type(submissions, "13F-HR")
    if not filings_13f:
        logger.warning("No 13F-HR filings found for %s", manager_id)
        return 0

    # Always sort oldest-first so deltas are computed in order
    filings_sorted = sorted(filings_13f, key=lambda x: x["filing_date"])

    if latest_date:
        new_filings = [f for f in filings_sorted if f["filing_date"] > latest_date]
        if not new_filings:
            logger.info("No new 13F filings for %s (latest stored: %s)", manager_id, latest_date)
            return 0
        logger.info("Found %d new 13F filing(s) for %s after %s",
                    len(new_filings), manager_id, latest_date)

        # Seed prior_positions from the snapshot of the last stored filing
        prior_positions = {}
        for pos in get_latest_positions_snapshot(manager_id):
            prior_key = f"{manager_id}_{pos['ticker']}"
            prior_positions[prior_key] = {
                "shares": pos["shares"],
                "pct_of_portfolio": pos.get("pct_of_portfolio", 0),
            }
    else:
        # No data yet — process everything so daily run works without a prior backfill
        logger.info("No existing 13F data for %s; processing all %d filing(s)",
                    manager_id, len(filings_sorted))
        new_filings = filings_sorted
        prior_positions = {}

    try:
        tickers_data = fetch_company_tickers()
        cusip_map = build_cusip_to_ticker(tickers_data)
    except Exception as e:
        logger.warning("Could not fetch company tickers: %s", e)
        cusip_map = {}

    total_stored = 0
    for filing in new_filings:
        try:
            count = process_13f_filing(manager_id, cik, filing, cusip_map, prior_positions)
            total_stored += count
        except Exception as e:
            logger.error("Error processing filing %s for %s: %s",
                         filing["accession"], manager_id, e)

    logger.info("Stored %d new positions for %s across %d filing(s)",
                total_stored, manager_id, len(new_filings))
    return total_stored


def fetch_new_13d_filings_for_manager(manager: dict) -> int:
    """Fetch only 13D/13G filings newer than the most recent one stored for this manager."""
    manager_id = manager["id"]
    cik = manager["cik"]

    latest_date = get_latest_13d_filing_date(manager_id)

    try:
        submissions = fetch_submissions(cik)
    except Exception as e:
        logger.error("Failed to fetch submissions for %s: %s", manager_id, e)
        return 0

    filings_13d = get_filings_of_type(submissions, "SC 13D")
    filings_13g = get_filings_of_type(submissions, "SC 13G")
    all_filings = filings_13d + filings_13g

    if latest_date:
        all_filings = [f for f in all_filings if f["filing_date"] > latest_date]

    stored = 0
    for filing in all_filings:
        try:
            filing_text = fetch_13d_filing_text(cik, filing["accession"], filing["form_type"])
            insert_document(
                manager=manager_id,
                source_type="edgar_13d",
                availability_date=filing["filing_date"],
                url=f"https://www.sec.gov/Archives/edgar/data/{int(_pad_cik(cik))}/{filing['accession'].replace('-','')}/",
                raw_text=filing_text,
            )
            stored += 1
        except Exception as e:
            logger.error("Error inserting 13D doc: %s", e)

    if stored:
        logger.info("Stored %d new 13D/13G filings for %s", stored, manager_id)
    return stored


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    init_db()
    managers = load_managers()

    # Test with Ackman
    ackman = next(m for m in managers if m["id"] == "ackman")
    count = fetch_all_13f_for_manager(ackman)
    print(f"\nTotal positions stored for Ackman: {count}")

    # Print top 10 holdings from most recent filing
    from db.database import get_connection
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT ticker, shares, value_usd, pct_of_portfolio, filing_date
            FROM positions
            WHERE manager = 'ackman'
            ORDER BY filing_date DESC, value_usd DESC
            LIMIT 10
        """).fetchall()

    print("\nTop 10 holdings (most recent filing):")
    print(f"{'Ticker':<15} {'Shares':>12} {'Value $M':>10} {'% Port':>8} {'Filed':>12}")
    print("-" * 60)
    for row in rows:
        print(f"{row['ticker']:<15} {row['shares']:>12,} {row['value_usd']/1e6:>10.1f} {row['pct_of_portfolio']:>7.1%} {row['filing_date']:>12}")
