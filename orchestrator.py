"""Main daily run orchestrator for the Smart Money Signal Agent."""
import argparse
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from config.settings import setup_logging, load_managers, BRIEF_RECIPIENT_EMAIL
from db.database import init_db, get_unprocessed_documents
from ingestion.edgar import (fetch_all_13f_for_manager, fetch_13d_filings_for_manager,
                              fetch_new_13f_for_manager, fetch_new_13d_filings_for_manager)
from ingestion.news import fetch_and_store_news
from ingestion.youtube import fetch_youtube_for_manager
from ingestion.podcasts import fetch_podcasts_for_manager
from ingestion.fund_letters import fetch_fund_letters_for_manager
from analysis.pipeline import run_pipeline
from scoring.weights import score_signal
from trading.paper import close_expired_trades, open_paper_trade
from output.brief import send_brief

logger = logging.getLogger(__name__)


def yesterday() -> str:
    return (date.today() - timedelta(days=1)).isoformat()


def daily_run() -> None:
    logger.info("Daily run starting — %s", date.today().isoformat())

    # 1. Close expired paper trades from 30 days ago
    logger.info("Step 1: Closing expired paper trades")
    closed = close_expired_trades()
    logger.info("Closed %d expired trades", closed)

    managers = load_managers()

    # 2. Fetch new EDGAR filings (incremental — skip already-stored filings)
    logger.info("Step 2: Fetching new EDGAR filings for %d managers", len(managers))
    for manager in managers:
        try:
            n13f = fetch_new_13f_for_manager(manager)
            n13d = fetch_new_13d_filings_for_manager(manager)
            logger.info("EDGAR: %s — %d new 13F positions, %d new 13D filings",
                        manager["id"], n13f, n13d)
        except Exception as e:
            logger.error("EDGAR fetch failed for %s: %s", manager["id"], e)

    # 3. Fetch new news articles
    logger.info("Step 3: Fetching news articles")
    for manager in managers:
        try:
            n = fetch_and_store_news(manager, since=yesterday())
            logger.info("News: %s — %d articles stored", manager["id"], n)
        except Exception as e:
            logger.error("News fetch failed for %s: %s", manager["id"], e)

    # 4. Fetch and transcribe YouTube / podcast content
    logger.info("Step 4: Fetching YouTube and podcast content")
    for manager in managers:
        try:
            n_yt = fetch_youtube_for_manager(manager, days_back=2)
            logger.info("YouTube: %s — %d videos stored", manager["id"], n_yt)
        except Exception as e:
            logger.error("YouTube fetch failed for %s: %s", manager["id"], e)

        try:
            n_pod = fetch_podcasts_for_manager(manager, days_back=2)
            logger.info("Podcasts: %s — %d episodes stored", manager["id"], n_pod)
        except Exception as e:
            logger.error("Podcast fetch failed for %s: %s", manager["id"], e)

    # 5. Fetch fund letters
    logger.info("Step 5: Fetching fund letters")
    for manager in managers:
        try:
            n = fetch_fund_letters_for_manager(manager)
            logger.info("Fund letters: %s — %d documents stored", manager["id"], n)
        except Exception as e:
            logger.error("Fund letter fetch failed for %s: %s", manager["id"], e)

    # 6. Run analysis pipeline on all unprocessed documents
    logger.info("Step 6: Running analysis pipeline")
    docs = get_unprocessed_documents()
    logger.info("Found %d unprocessed documents", len(docs))

    signals_fired = 0
    trades_opened = 0

    for doc in docs:
        try:
            signal_candidates = run_pipeline(doc["id"])
            for candidate in signal_candidates:
                scored = score_signal(candidate, doc["manager"])
                if scored:
                    signals_fired += 1
                    trade = open_paper_trade(
                        signal_id=scored["id"],
                        ticker=scored["ticker"],
                        direction=scored["direction"],
                        fired_date=scored["fired_date"],
                    )
                    if trade:
                        trades_opened += 1
        except Exception as e:
            logger.error("Pipeline error for doc id=%d manager=%s: %s",
                         doc["id"], doc["manager"], e)

    logger.info("Pipeline complete: %d signals fired, %d trades opened",
                signals_fired, trades_opened)

    # 7. Send daily brief
    logger.info("Step 7: Sending daily brief")
    recipient = os.getenv("BRIEF_RECIPIENT_EMAIL", BRIEF_RECIPIENT_EMAIL)
    send_brief(recipient)

    logger.info("Daily run complete — %s", date.today().isoformat())


def backfill() -> None:
    """Run full historical EDGAR backfill for all managers."""
    logger.info("Starting historical EDGAR backfill")
    managers = load_managers()
    for manager in managers:
        try:
            n = fetch_all_13f_for_manager(manager)
            logger.info("Backfill: %s — %d positions loaded", manager["id"], n)
        except Exception as e:
            logger.error("Backfill failed for %s: %s", manager["id"], e)
    logger.info("Backfill complete")


def main() -> None:
    setup_logging()
    init_db()

    parser = argparse.ArgumentParser(description="Smart Money Signal Agent orchestrator")
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="Run the daily pipeline immediately instead of waiting for schedule",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Run full historical EDGAR backfill for all managers",
    )
    args = parser.parse_args()

    if args.backfill:
        backfill()
        return

    if args.run_now:
        daily_run()
        return

    # Scheduled mode — run daily at 06:00
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
    except ImportError:
        logger.error("apscheduler not installed. Run: pip install apscheduler")
        sys.exit(1)

    scheduler = BlockingScheduler()
    scheduler.add_job(daily_run, "cron", hour=6, minute=0)
    logger.info("Scheduler started — daily run at 06:00. Press Ctrl+C to stop.")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")


if __name__ == "__main__":
    main()
