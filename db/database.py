import sqlite3
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def get_db_path() -> str:
    return os.getenv("DB_PATH", str(Path(__file__).parent.parent / "data" / "smart_money.db"))


def get_connection() -> sqlite3.Connection:
    db_path = get_db_path()
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    schema = SCHEMA_PATH.read_text()
    with get_connection() as conn:
        conn.executescript(schema)
    logger.info("Database initialised at %s", get_db_path())


def insert_document(manager: str, source_type: str, availability_date: str,
                    content_date: str = None, url: str = None, raw_text: str = None) -> int:
    with get_connection() as conn:
        cur = conn.execute(
            """INSERT INTO documents (manager, source_type, availability_date, content_date, url, raw_text)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (manager, source_type, availability_date, content_date, url, raw_text)
        )
        doc_id = cur.lastrowid
    logger.debug("Inserted document id=%d manager=%s type=%s", doc_id, manager, source_type)
    return doc_id


def get_unprocessed_documents() -> list:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM documents WHERE processed = 0 ORDER BY availability_date"
        ).fetchall()
    return [dict(r) for r in rows]


def reset_empty_documents() -> int:
    """Reset processed flag to 0 for any document with NULL or empty raw_text.

    Documents ingested without text (e.g. failed article fetch, failed transcription)
    may have been marked processed=2 by the pipeline's empty-text guard.  Resetting
    them to 0 allows the ingestion step to re-fetch their content on the next run.
    Returns the number of documents reset.
    """
    with get_connection() as conn:
        cur = conn.execute(
            "UPDATE documents SET processed = 0 WHERE raw_text IS NULL OR TRIM(raw_text) = ''"
        )
        count = cur.rowcount
    if count:
        logger.info("Reset %d documents with empty/null text back to unprocessed", count)
    return count


def mark_processed(doc_id: int, status: int) -> None:
    with get_connection() as conn:
        conn.execute("UPDATE documents SET processed = ? WHERE id = ?", (status, doc_id))
    logger.debug("Marked document id=%d processed=%d", doc_id, status)


def get_latest_position(manager: str, ticker: str) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            """SELECT * FROM positions WHERE manager = ? AND ticker = ?
               ORDER BY filing_date DESC LIMIT 1""",
            (manager, ticker)
        ).fetchone()
    return dict(row) if row else None


def get_latest_13f_filing_date(manager: str) -> str | None:
    """Return the most recent 13F filing_date stored for a manager, or None."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT MAX(filing_date) AS latest FROM positions WHERE manager = ? AND filing_type = '13F'",
            (manager,)
        ).fetchone()
    return row["latest"] if row else None


def get_latest_positions_snapshot(manager: str) -> list:
    """Return all positions from the most recent 13F filing for a manager."""
    with get_connection() as conn:
        rows = conn.execute(
            """SELECT * FROM positions WHERE manager = ? AND filing_date = (
                   SELECT MAX(filing_date) FROM positions
                   WHERE manager = ? AND filing_type = '13F'
               )""",
            (manager, manager)
        ).fetchall()
    return [dict(r) for r in rows]


def get_latest_13d_filing_date(manager: str) -> str | None:
    """Return the most recent 13D/13G availability_date stored for a manager, or None."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT MAX(availability_date) AS latest FROM documents WHERE manager = ? AND source_type = 'edgar_13d'",
            (manager,)
        ).fetchone()
    return row["latest"] if row else None


def get_positions_as_of(manager: str, date: str) -> list:
    with get_connection() as conn:
        rows = conn.execute(
            """SELECT * FROM positions WHERE manager = ? AND filing_date <= ?
               ORDER BY filing_date DESC""",
            (manager, date)
        ).fetchall()
    return [dict(r) for r in rows]


def insert_position(manager: str, ticker: str, filing_date: str, period_of_report: str,
                    shares: int = None, value_usd: int = None, pct_of_portfolio: float = None,
                    delta_shares: int = None, delta_pct: float = None,
                    filing_type: str = "13F") -> int:
    with get_connection() as conn:
        cur = conn.execute(
            """INSERT INTO positions (manager, ticker, filing_date, period_of_report,
               shares, value_usd, pct_of_portfolio, delta_shares, delta_pct, filing_type)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (manager, ticker, filing_date, period_of_report, shares, value_usd,
             pct_of_portfolio, delta_shares, delta_pct, filing_type)
        )
        return cur.lastrowid


def insert_statement(doc_id: int, manager: str, quote_verbatim: str,
                     ticker: str = None, sentiment: str = None, hedge_flags: str = None,
                     conviction_level: str = None, pass_number: int = None) -> int:
    with get_connection() as conn:
        cur = conn.execute(
            """INSERT INTO statements (doc_id, manager, ticker, quote_verbatim, sentiment,
               hedge_flags, conviction_level, pass_number)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (doc_id, manager, ticker, quote_verbatim, sentiment, hedge_flags,
             conviction_level, pass_number)
        )
        return cur.lastrowid


def insert_signal(manager: str, ticker: str, signal_type: str, direction: str,
                  fired_date: str, raw_score: float = None, manager_weight: float = None,
                  final_confidence: float = None, statement_id: int = None,
                  position_id: int = None, notes: str = None) -> int:
    with get_connection() as conn:
        cur = conn.execute(
            """INSERT INTO signals (statement_id, position_id, manager, ticker, signal_type,
               raw_score, manager_weight, final_confidence, direction, fired_date, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (statement_id, position_id, manager, ticker, signal_type, raw_score,
             manager_weight, final_confidence, direction, fired_date, notes)
        )
        return cur.lastrowid
