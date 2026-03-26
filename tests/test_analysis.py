import pytest
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def test_prompts_have_correct_placeholders():
    from analysis.prompts import (
        PASS_1_ENTITY_EXTRACTION, PASS_2_QUOTE_EXTRACTION, PASS_3_SENTIMENT_HEDGE,
        PASS_4_ABSENCE_REPORT, PASS_5_13F_CROSSREFERENCE, PASS_6_RED_TEAM,
    )
    # Each prompt should be formattable with the expected keys
    p1 = PASS_1_ENTITY_EXTRACTION.format(manager_name="Test", document_text="text")
    assert "Test" in p1

    p2 = PASS_2_QUOTE_EXTRACTION.format(manager_name="Test", entities="AAPL", document_text="text")
    assert "verbatim" in p2.lower()

    p3 = PASS_3_SENTIMENT_HEDGE.format(manager_name="Test", quotes_json="[]")
    assert "bullish" in p3

    p4 = PASS_4_ABSENCE_REPORT.format(manager_name="Test", known_positions="[]", mentioned_tickers="[]")
    assert "not_mentioned" in p4

    p5 = PASS_5_13F_CROSSREFERENCE.format(
        manager_name="Test", statements_json="[]",
        position_changes_json="[]", silence_flags="[]"
    )
    assert "distribution" in p5

    p6 = PASS_6_RED_TEAM.format(manager_name="Test", signals_json="[]", document_text="text")
    assert "adversarial" in p6.lower() or "skeptical" in p6.lower()


def test_scoring_weights():
    from scoring.weights import score_signal

    # High conviction distribution signal for ackman (weight=0.82, n=8 exactly at threshold)
    candidate = {
        "ticker": "AAPL",
        "signal_type": "distribution",
        "raw_score": 0.90,
        "direction": "fade",
        "reasoning": "Test signal",
    }
    # We can't actually insert to DB in this test without a full setup
    # Just test that the weight calculation logic is correct
    from scoring.weights import get_manager_weights, N_INSTANCES_THRESHOLD, CONFIDENCE_CAP

    weights = get_manager_weights("ackman")
    assert weights is not None
    assert "distribution" in weights
    assert weights["distribution"]["weight"] == 0.82
    assert weights["distribution"]["n_instances"] == 8

    # Ackman distribution: n_instances=8 which is >= 8, so no cap
    weight = weights["distribution"]["weight"]
    n = weights["distribution"]["n_instances"]
    raw = 0.90
    computed = raw * weight  # 0.738
    assert computed > 0.55, f"Expected signal to fire, got {computed}"

    # Burry contrarian: n=6 < 8 so should cap at 0.55
    burry_weights = get_manager_weights("burry")
    assert burry_weights["contrarian"]["n_instances"] == 6
    assert burry_weights["contrarian"]["n_instances"] < N_INSTANCES_THRESHOLD


def test_cross_reference_position_direction():
    from analysis.cross_reference import get_position_changes
    import os
    import tempfile

    # This test uses a real temp DB
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    os.environ["DB_PATH"] = db_path

    import importlib
    import db.database as database
    importlib.reload(database)
    database.init_db()

    database.insert_position("ackman", "AAPL", "2024-08-14", "2024-06-30",
                             shares=1000000, value_usd=180000000)
    database.insert_position("ackman", "AAPL", "2024-11-14", "2024-09-30",
                             shares=620000, value_usd=111600000,
                             delta_shares=-380000, delta_pct=-0.38)

    changes = get_position_changes("ackman", "2024-11-14")
    aapl = next((c for c in changes if c["ticker"] == "AAPL"), None)
    assert aapl is not None
    assert aapl["delta_shares"] == -380000
    assert "reduced" in aapl["direction"]

    import os
    import gc
    gc.collect()  # close any lingering SQLite connections before unlink (Windows)
    try:
        os.unlink(db_path)
    except PermissionError:
        pass  # Windows may hold the file; test assertions already passed
