"""Apply manager weights to signal candidates and store fired signals."""
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import load_managers
from db.database import insert_signal

logger = logging.getLogger(__name__)

N_INSTANCES_THRESHOLD = 8
CONFIDENCE_CAP = 0.55
MIN_CONFIDENCE = 0.55


def get_manager_weights(manager_id: str) -> dict | None:
    managers = load_managers()
    mgr = next((m for m in managers if m["id"] == manager_id), None)
    return mgr.get("weights") if mgr else None


def score_signal(signal_candidate: dict, manager_id: str) -> dict | None:
    """
    Score a signal candidate using manager weights.
    Returns the stored signal dict if it fires (>= 0.55), else None.
    """
    weights = get_manager_weights(manager_id)
    if not weights:
        logger.warning("No weights found for manager %s", manager_id)
        return None

    signal_type = signal_candidate.get("signal_type", "")
    if signal_type not in weights:
        logger.warning("Unknown signal_type %s for manager %s", signal_type, manager_id)
        return None

    weight_config = weights[signal_type]
    weight = weight_config.get("weight", 0.5)
    n_instances = weight_config.get("n_instances", 0)

    raw_score = float(signal_candidate.get("raw_score", 0.5))

    # Compute final confidence
    final_confidence = raw_score * weight

    # n_instances gate: cap at 0.55 if insufficient historical data
    if n_instances < N_INSTANCES_THRESHOLD:
        final_confidence = min(final_confidence, CONFIDENCE_CAP)
        logger.debug("Capping confidence for %s/%s (n_instances=%d < %d)",
                    manager_id, signal_type, n_instances, N_INSTANCES_THRESHOLD)

    # Discard if below threshold
    if final_confidence < MIN_CONFIDENCE:
        logger.debug("Signal discarded: %s %s final_confidence=%.3f",
                    manager_id, signal_candidate.get("ticker"), final_confidence)
        return None

    ticker = signal_candidate.get("ticker", "")
    direction = signal_candidate.get("direction", "follow")
    fired_date = date.today().isoformat()

    signal_id = insert_signal(
        manager=manager_id,
        ticker=ticker,
        signal_type=signal_type,
        direction=direction,
        fired_date=fired_date,
        raw_score=raw_score,
        manager_weight=weight,
        final_confidence=final_confidence,
        notes=signal_candidate.get("reasoning", ""),
    )

    result = {
        "id": signal_id,
        "manager": manager_id,
        "ticker": ticker,
        "signal_type": signal_type,
        "direction": direction,
        "raw_score": raw_score,
        "manager_weight": weight,
        "final_confidence": final_confidence,
        "fired_date": fired_date,
    }

    logger.info("Signal fired: %s %s %s final_confidence=%.3f id=%d",
                manager_id, ticker, signal_type, final_confidence, signal_id)
    return result
