"""Confidence calibration helpers — measures stated vs actual win rate."""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_connection

logger = logging.getLogger(__name__)

BUCKETS = [
    (0.55, 0.65, "0.55-0.65"),
    (0.65, 0.75, "0.65-0.75"),
    (0.75, 0.85, "0.75-0.85"),
    (0.85, 1.01, "0.85-1.00"),
]


def calibration_report() -> dict:
    """
    Compute calibration across confidence buckets.
    Returns dict of bucket -> {stated_confidence, actual_win_rate, count, is_calibrated}.
    Logs a warning if any bucket is off by more than 15 percentage points.
    """
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT s.final_confidence, pt.pnl_direction
            FROM signals s
            JOIN paper_trades pt ON pt.signal_id = s.id
            WHERE pt.pnl_direction IN ('win', 'loss')
        """).fetchall()

    report = {}

    for lo, hi, label in BUCKETS:
        bucket_rows = [r for r in rows if lo <= r["final_confidence"] < hi]
        count = len(bucket_rows)

        if count == 0:
            report[label] = {
                "stated_confidence_midpoint": (lo + min(hi, 1.0)) / 2,
                "actual_win_rate": None,
                "count": 0,
                "is_calibrated": None,
            }
            continue

        wins = sum(1 for r in bucket_rows if r["pnl_direction"] == "win")
        actual_win_rate = wins / count
        stated_midpoint = (lo + min(hi, 1.0)) / 2

        deviation = abs(actual_win_rate - stated_midpoint)
        is_calibrated = deviation <= 0.15

        if not is_calibrated:
            logger.warning(
                "Calibration warning: bucket %s stated=%.0f%% actual=%.0f%% (off by %.0f%%)",
                label, stated_midpoint * 100, actual_win_rate * 100, deviation * 100
            )

        report[label] = {
            "stated_confidence_midpoint": stated_midpoint,
            "actual_win_rate": actual_win_rate,
            "count": count,
            "is_calibrated": is_calibrated,
        }

    return report
