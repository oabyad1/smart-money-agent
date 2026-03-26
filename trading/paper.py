"""Paper trade logging and P&L computation."""
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import yfinance as yf
from db.database import get_connection

logger = logging.getLogger(__name__)


def _get_next_open_price(ticker: str, after_date: str) -> tuple[float | None, str | None]:
    """
    Fetch the next trading day open price after a given date.
    Returns (price, date_str) or (None, None) if unavailable.
    """
    try:
        start = datetime.strptime(after_date, "%Y-%m-%d")
        end = start + timedelta(days=10)
        hist = yf.download(ticker, start=start.strftime("%Y-%m-%d"),
                          end=end.strftime("%Y-%m-%d"), progress=False, auto_adjust=True)
        if hist.empty:
            logger.warning("No price data for %s after %s", ticker, after_date)
            return None, None

        first_row = hist.iloc[0]
        open_price = float(first_row["Open"])
        price_date = hist.index[0].strftime("%Y-%m-%d")
        return open_price, price_date
    except Exception as e:
        logger.error("Price fetch error for %s: %s", ticker, e)
        return None, None


def _get_current_price(ticker: str) -> float | None:
    """Get the most recent close price for a ticker."""
    try:
        hist = yf.download(ticker, period="5d", progress=False, auto_adjust=True)
        if hist.empty:
            return None
        return float(hist["Close"].iloc[-1])
    except Exception as e:
        logger.error("Current price error for %s: %s", ticker, e)
        return None


def open_paper_trade(signal_id: int, ticker: str, direction: str,
                     fired_date: str = None) -> dict | None:
    """
    Open a paper trade for a signal.
    direction: 'long' (follow) or 'short' (fade)
    Returns the trade dict if successful, else None.
    """
    if fired_date is None:
        fired_date = date.today().isoformat()

    entry_price, entry_date = _get_next_open_price(ticker, fired_date)

    if entry_price is None:
        logger.warning("Could not get entry price for %s — trade not opened", ticker)
        return None

    with get_connection() as conn:
        cur = conn.execute(
            """INSERT INTO paper_trades
               (signal_id, ticker, direction, entry_price, entry_date, pnl_direction)
               VALUES (?, ?, ?, ?, ?, 'open')""",
            (signal_id, ticker, direction, entry_price, entry_date)
        )
        trade_id = cur.lastrowid

    result = {
        "id": trade_id,
        "signal_id": signal_id,
        "ticker": ticker,
        "direction": direction,
        "entry_price": entry_price,
        "entry_date": entry_date,
    }
    logger.info("Opened paper trade: id=%d %s %s @ $%.2f on %s",
                trade_id, direction.upper(), ticker, entry_price, entry_date)
    return result


def close_expired_trades() -> int:
    """
    Close all open paper trades that are 30+ days old.
    Returns count of trades closed.
    """
    cutoff = (date.today() - timedelta(days=30)).isoformat()

    with get_connection() as conn:
        open_trades = conn.execute(
            """SELECT * FROM paper_trades
               WHERE pnl_direction = 'open' AND entry_date <= ?""",
            (cutoff,)
        ).fetchall()

    closed = 0
    for trade in open_trades:
        trade = dict(trade)
        ticker = trade["ticker"]
        entry_price = trade["entry_price"]
        direction = trade["direction"]

        exit_price = _get_current_price(ticker)
        if exit_price is None:
            logger.warning("Could not get exit price for trade id=%d %s", trade["id"], ticker)
            continue

        exit_date = date.today().isoformat()
        hold_days = (datetime.strptime(exit_date, "%Y-%m-%d") -
                     datetime.strptime(trade["entry_date"], "%Y-%m-%d")).days

        if direction == "long":
            pnl_pct = (exit_price - entry_price) / entry_price
        else:  # short
            pnl_pct = (entry_price - exit_price) / entry_price

        pnl_direction = "win" if pnl_pct > 0 else "loss"

        with get_connection() as conn:
            conn.execute(
                """UPDATE paper_trades
                   SET exit_price = ?, exit_date = ?, hold_days = ?,
                       pnl_pct = ?, pnl_direction = ?
                   WHERE id = ?""",
                (exit_price, exit_date, hold_days, pnl_pct, pnl_direction, trade["id"])
            )

        logger.info("Closed trade id=%d %s %s: entry=%.2f exit=%.2f pnl=%.1f%% (%s)",
                    trade["id"], direction, ticker, entry_price, exit_price,
                    pnl_pct * 100, pnl_direction)
        closed += 1

    logger.info("Closed %d expired trades", closed)
    return closed


def get_portfolio_summary() -> dict:
    """
    Returns portfolio-level stats from all paper trades.
    """
    with get_connection() as conn:
        all_trades = conn.execute("SELECT * FROM paper_trades").fetchall()
        all_trades = [dict(t) for t in all_trades]

    total = len(all_trades)
    open_trades = [t for t in all_trades if t["pnl_direction"] == "open"]
    closed_trades = [t for t in all_trades if t["pnl_direction"] in ("win", "loss")]

    wins = [t for t in closed_trades if t["pnl_direction"] == "win"]
    win_rate = len(wins) / len(closed_trades) if closed_trades else 0.0

    pnl_values = [t["pnl_pct"] for t in closed_trades if t["pnl_pct"] is not None]
    avg_pnl = sum(pnl_values) / len(pnl_values) if pnl_values else 0.0
    total_return = sum(pnl_values)

    best = max(pnl_values) if pnl_values else 0.0
    worst = min(pnl_values) if pnl_values else 0.0

    # Per-manager breakdown
    with get_connection() as conn:
        mgr_rows = conn.execute("""
            SELECT s.manager,
                   COUNT(*) as trades,
                   SUM(CASE WHEN pt.pnl_direction='win' THEN 1 ELSE 0 END) as wins,
                   AVG(pt.pnl_pct) as avg_pnl
            FROM paper_trades pt
            JOIN signals s ON s.id = pt.signal_id
            WHERE pt.pnl_direction IN ('win','loss')
            GROUP BY s.manager
        """).fetchall()

    # Per-signal-type breakdown
    with get_connection() as conn:
        type_rows = conn.execute("""
            SELECT s.signal_type,
                   COUNT(*) as trades,
                   SUM(CASE WHEN pt.pnl_direction='win' THEN 1 ELSE 0 END) as wins,
                   AVG(pt.pnl_pct) as avg_pnl
            FROM paper_trades pt
            JOIN signals s ON s.id = pt.signal_id
            WHERE pt.pnl_direction IN ('win','loss')
            GROUP BY s.signal_type
        """).fetchall()

    return {
        "total_trades": total,
        "open_trades": len(open_trades),
        "closed_trades": len(closed_trades),
        "win_rate": win_rate,
        "avg_pnl_pct": avg_pnl,
        "total_simulated_return": total_return,
        "best_trade_pnl": best,
        "worst_trade_pnl": worst,
        "by_manager": [dict(r) for r in mgr_rows],
        "by_signal_type": [dict(r) for r in type_rows],
        "open_trade_list": open_trades,
        "closed_trade_list": closed_trades,
    }
