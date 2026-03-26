"""Match public statements to 13F position changes."""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.database import get_connection, get_positions_as_of

logger = logging.getLogger(__name__)


def get_position_changes(manager_id: str, as_of_date: str) -> list[dict]:
    """
    Get position changes for a manager as of a given date.
    Returns list of dicts with ticker, delta_shares, delta_pct, direction.
    """
    positions = get_positions_as_of(manager_id, as_of_date)

    # Get unique tickers and their most recent two filings to compute changes
    seen_tickers = {}
    for pos in positions:
        ticker = pos["ticker"]
        if ticker not in seen_tickers:
            seen_tickers[ticker] = pos

    changes = []
    for ticker, pos in seen_tickers.items():
        delta_shares = pos.get("delta_shares")
        delta_pct = pos.get("delta_pct")

        if delta_shares is None:
            direction = "new_position"
        elif delta_shares > 0:
            direction = f"increased_{abs(delta_pct or 0):.0%}"
        elif delta_shares < 0:
            pct_change = abs(delta_shares / (pos.get("shares", 1) - delta_shares + 1))
            direction = f"reduced_{pct_change:.0%}"
        else:
            direction = "unchanged"

        changes.append({
            "ticker": ticker,
            "shares": pos.get("shares", 0),
            "value_usd": pos.get("value_usd", 0),
            "delta_shares": delta_shares,
            "delta_pct": delta_pct,
            "direction": direction,
            "filing_date": pos.get("filing_date"),
        })

    return changes


def match_statements_to_positions(statements: list, position_changes: list) -> list:
    """
    Match extracted statements to position changes by ticker.
    Returns enriched statement list with position info attached.
    """
    position_map = {p["ticker"].upper(): p for p in position_changes}

    enriched = []
    for stmt in statements:
        ticker = (stmt.get("ticker") or "").upper()
        pos = position_map.get(ticker)
        enriched.append({
            **stmt,
            "position": pos,
        })

    return enriched
