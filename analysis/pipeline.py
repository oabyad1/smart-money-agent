"""Orchestrates the 6-pass analysis pipeline for a single document."""
import json
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import anthropic
from config.settings import ANTHROPIC_API_KEY, load_managers
from db.database import (
    get_connection, get_unprocessed_documents, mark_processed,
    get_positions_as_of, insert_statement, insert_signal,
)
from analysis.passes import (
    run_pass_1, run_pass_2, run_pass_3, run_pass_4, run_pass_5, run_pass_6,
)
from analysis.cross_reference import get_position_changes

logger = logging.getLogger(__name__)


def get_manager_config(manager_id: str) -> dict | None:
    managers = load_managers()
    return next((m for m in managers if m["id"] == manager_id), None)


def run_pipeline(doc_id: int) -> list[dict]:
    """
    Run the full 6-pass analysis pipeline for a document.
    Returns list of signal candidates (before scoring).
    Marks document as processed=1 on success, processed=2 on failure.
    """
    # Load document
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM documents WHERE id = ?", (doc_id,)).fetchone()

    if not row:
        logger.error("Document id=%d not found", doc_id)
        return []

    doc = dict(row)
    manager_id = doc["manager"]
    availability_date = doc["availability_date"]
    raw_text = doc.get("raw_text") or ""

    if not raw_text.strip():
        logger.warning("Document id=%d has no text, skipping", doc_id)
        return []

    manager_cfg = get_manager_config(manager_id)
    if not manager_cfg:
        logger.error("No config for manager %s", manager_id)
        mark_processed(doc_id, 2)
        return []

    manager_name = manager_cfg["name"]

    if not ANTHROPIC_API_KEY:
        logger.error("ANTHROPIC_API_KEY not set — cannot run analysis pipeline")
        mark_processed(doc_id, 2)
        return []

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    logger.info("Running pipeline for doc_id=%d manager=%s type=%s date=%s",
                doc_id, manager_id, doc["source_type"], availability_date)

    try:
        # Load known positions as of availability_date (never future data)
        known_positions = get_positions_as_of(manager_id, availability_date)
        position_changes = get_position_changes(manager_id, availability_date)

        # Pass 1: Entity extraction
        logger.info("Pass 1: entity extraction for doc_id=%d", doc_id)
        entities = run_pass_1(client, raw_text, manager_name)
        tickers = entities.get("tickers", [])
        logger.info("Pass 1 found %d tickers: %s", len(tickers), tickers[:10])

        if not tickers and not entities.get("companies"):
            logger.info("No entities found in doc_id=%d — no signals possible", doc_id)
            mark_processed(doc_id, 1)
            return []

        time.sleep(0.5)

        # Pass 2: Verbatim quote extraction
        logger.info("Pass 2: quote extraction for doc_id=%d", doc_id)
        quotes = run_pass_2(client, raw_text, manager_name, entities)
        if not quotes:
            logger.info("No quotes extracted from doc_id=%d", doc_id)
            mark_processed(doc_id, 1)
            return []

        time.sleep(0.5)

        # Pass 3: Sentiment + hedge classification
        logger.info("Pass 3: sentiment classification for doc_id=%d", doc_id)
        classified_quotes = run_pass_3(client, quotes, manager_name)

        time.sleep(0.5)

        # Pass 4: Absence/silence report
        logger.info("Pass 4: absence report for doc_id=%d", doc_id)
        absence_report = run_pass_4(client, raw_text, manager_name, known_positions, tickers)

        # Store statements to DB
        statement_ids = []
        for item in classified_quotes:
            ticker = item.get("ticker")
            for quote_data in item.get("quotes", []):
                if isinstance(quote_data, str):
                    quote_text = quote_data
                    sentiment = item.get("sentiment", "neutral")
                    conviction = item.get("conviction_level", "unclear")
                    hedge_flags = json.dumps(item.get("hedge_words", []))
                elif isinstance(quote_data, dict):
                    quote_text = quote_data.get("text", quote_data.get("quote", ""))
                    sentiment = quote_data.get("sentiment", "neutral")
                    conviction = quote_data.get("conviction_level", "unclear")
                    hedge_flags = json.dumps(quote_data.get("hedge_words", []))
                else:
                    continue

                if not quote_text:
                    continue

                stmt_id = insert_statement(
                    doc_id=doc_id,
                    manager=manager_id,
                    quote_verbatim=quote_text,
                    ticker=ticker,
                    sentiment=sentiment,
                    conviction_level=conviction,
                    hedge_flags=hedge_flags,
                    pass_number=3,
                )
                statement_ids.append(stmt_id)

        logger.info("Stored %d statements for doc_id=%d", len(statement_ids), doc_id)

        # Build statement list for Pass 5
        statements_for_p5 = []
        for item in classified_quotes:
            ticker = item.get("ticker", "")
            for quote_data in item.get("quotes", []):
                if isinstance(quote_data, str):
                    statements_for_p5.append({
                        "ticker": ticker,
                        "quote": quote_data,
                        "sentiment": item.get("sentiment", "neutral"),
                        "conviction_level": item.get("conviction_level", "unclear"),
                    })
                elif isinstance(quote_data, dict):
                    statements_for_p5.append({
                        "ticker": ticker,
                        "quote": quote_data.get("text", ""),
                        "sentiment": quote_data.get("sentiment", "neutral"),
                        "conviction_level": quote_data.get("conviction_level", "unclear"),
                    })

        silence_flags = absence_report.get("not_mentioned", [])

        time.sleep(0.5)

        # Pass 5: 13F cross-reference
        logger.info("Pass 5: 13F cross-reference for doc_id=%d", doc_id)
        raw_signals = run_pass_5(
            client, manager_name, statements_for_p5, position_changes, silence_flags
        )

        if not raw_signals:
            logger.info("Pass 5 produced no signals for doc_id=%d", doc_id)
            mark_processed(doc_id, 1)
            return []

        time.sleep(0.5)

        # Pass 6: Red team review
        logger.info("Pass 6: red team review for doc_id=%d", doc_id)
        red_team = run_pass_6(client, manager_name, raw_signals, raw_text)

        # Apply confidence reductions from red team
        confidence_reductions = {
            r["ticker"]: r["reduce_by"]
            for r in red_team.get("confidence_reductions", [])
            if isinstance(r, dict)
        }

        final_signals = []
        for sig in raw_signals:
            ticker = sig.get("ticker", "")
            raw_score = float(sig.get("raw_score", 0.5))
            reduction = confidence_reductions.get(ticker, 0)
            adjusted_score = max(0.0, raw_score - reduction)

            signal_type = sig.get("signal_type", "")
            direction = "fade" if signal_type == "distribution" else "follow"

            final_signals.append({
                "manager": manager_id,
                "ticker": ticker,
                "signal_type": signal_type,
                "raw_score": adjusted_score,
                "direction": direction,
                "reasoning": sig.get("reasoning", ""),
                "statement_sentiment": sig.get("statement_sentiment", ""),
                "position_direction": sig.get("position_direction", ""),
                "doc_id": doc_id,
            })

        mark_processed(doc_id, 1)
        logger.info("Pipeline complete for doc_id=%d: %d signals", doc_id, len(final_signals))
        return final_signals

    except Exception as e:
        logger.error("Pipeline failed for doc_id=%d: %s", doc_id, e, exc_info=True)
        mark_processed(doc_id, 2)
        return []
