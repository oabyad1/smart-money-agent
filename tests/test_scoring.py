import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def test_calibration_report_empty_db(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    import importlib
    import db.database as db
    importlib.reload(db)
    db.init_db()

    from scoring.calibration import calibration_report
    report = calibration_report()

    # All buckets should exist but have count=0
    assert "0.55-0.65" in report
    assert "0.85-1.00" in report
    for bucket in report.values():
        assert bucket["count"] == 0
        assert bucket["actual_win_rate"] is None


def test_weight_lookup_all_managers():
    from scoring.weights import get_manager_weights
    manager_ids = ["ackman", "burry", "einhorn", "druckenmiller", "loeb", "icahn", "tepper", "marks"]
    for mid in manager_ids:
        weights = get_manager_weights(mid)
        assert weights is not None, f"No weights for {mid}"
        for sig_type in ["distribution", "accumulation", "contrarian"]:
            assert sig_type in weights, f"{mid} missing {sig_type}"
            assert 0 <= weights[sig_type]["weight"] <= 1


def test_signal_below_threshold_not_fired(tmp_path, monkeypatch):
    """Signals below 0.55 confidence should return None without DB insert."""
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    import importlib
    import db.database as db
    importlib.reload(db)
    db.init_db()

    from scoring import weights as w_module
    importlib.reload(w_module)

    # tepper distribution weight=0.45; raw_score=0.9 -> 0.45*0.9=0.405 < 0.55
    candidate = {
        "ticker": "AAPL",
        "signal_type": "distribution",
        "raw_score": 0.60,
        "direction": "fade",
        "reasoning": "Low weight test",
    }
    result = w_module.score_signal(candidate, "tepper")
    assert result is None  # 0.60 * 0.45 = 0.27 < 0.55
