import pytest
import os
import tempfile
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

@pytest.fixture(autouse=True)
def temp_db(tmp_path, monkeypatch):
    db_file = str(tmp_path / "test.db")
    monkeypatch.setenv("DB_PATH", db_file)
    # Re-import after env is set
    import importlib
    import db.database as database
    importlib.reload(database)
    database.init_db()
    yield database

def test_insert_and_retrieve_document(temp_db):
    db = temp_db
    doc_id = db.insert_document(
        manager="ackman",
        source_type="news",
        availability_date="2024-11-14",
        content_date="2024-11-13",
        url="https://example.com/article",
        raw_text="Bill Ackman is bullish on AAPL."
    )
    assert doc_id is not None
    docs = db.get_unprocessed_documents()
    assert any(d["id"] == doc_id for d in docs)
    doc = next(d for d in docs if d["id"] == doc_id)
    assert doc["availability_date"] == "2024-11-14"
    assert doc["manager"] == "ackman"

def test_availability_date_stored_correctly(temp_db):
    db = temp_db
    doc_id = db.insert_document(
        manager="burry",
        source_type="edgar_13f",
        availability_date="2024-11-14",
        content_date="2024-09-30",
    )
    docs = db.get_unprocessed_documents()
    doc = next(d for d in docs if d["id"] == doc_id)
    # availability_date and content_date must differ
    assert doc["availability_date"] != doc["content_date"]
    assert doc["availability_date"] == "2024-11-14"
    assert doc["content_date"] == "2024-09-30"

def test_insert_position(temp_db):
    db = temp_db
    pos_id = db.insert_position(
        manager="ackman",
        ticker="AAPL",
        filing_date="2024-11-14",
        period_of_report="2024-09-30",
        shares=1000000,
        value_usd=180000000,
        pct_of_portfolio=0.10,
    )
    assert pos_id is not None
    pos = db.get_latest_position("ackman", "AAPL")
    assert pos is not None
    assert pos["ticker"] == "AAPL"
    assert pos["filing_date"] == "2024-11-14"

def test_insert_statement(temp_db):
    db = temp_db
    doc_id = db.insert_document("ackman", "news", "2024-11-14")
    stmt_id = db.insert_statement(
        doc_id=doc_id,
        manager="ackman",
        quote_verbatim="We believe AAPL is substantially undervalued at current prices.",
        ticker="AAPL",
        sentiment="bullish",
        conviction_level="high",
        pass_number=3,
    )
    assert stmt_id is not None

def test_mark_processed(temp_db):
    db = temp_db
    doc_id = db.insert_document("ackman", "news", "2024-11-14")
    db.mark_processed(doc_id, 1)
    docs = db.get_unprocessed_documents()
    assert not any(d["id"] == doc_id for d in docs)

def test_get_positions_as_of(temp_db):
    db = temp_db
    db.insert_position("ackman", "AAPL", "2024-08-14", "2024-06-30", shares=500000)
    db.insert_position("ackman", "AAPL", "2024-11-14", "2024-09-30", shares=600000)
    db.insert_position("ackman", "MSFT", "2024-11-14", "2024-09-30", shares=200000)

    # As of Aug 14 — should only see the Aug filing
    positions = db.get_positions_as_of("ackman", "2024-08-14")
    tickers = [p["ticker"] for p in positions]
    assert "AAPL" in tickers
    # MSFT filed Nov 14, so not available as of Aug 14
    assert "MSFT" not in tickers
