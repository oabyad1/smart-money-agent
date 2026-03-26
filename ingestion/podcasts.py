import feedparser
import httpx
import logging
import os
import sys
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import CACHE_DIR
from db.database import init_db, insert_document, get_connection
from ingestion.youtube import transcribe_audio, ensure_dirs, CACHE_AUDIO

logger = logging.getLogger(__name__)


def is_episode_stored(url: str) -> bool:
    with get_connection() as conn:
        row = conn.execute("SELECT id FROM documents WHERE url = ?", (url,)).fetchone()
    return row is not None


def parse_rss_date(entry) -> str:
    """Parse RSS entry date to YYYY-MM-DD string."""
    for attr in ("published", "updated"):
        val = getattr(entry, attr, None)
        if val:
            try:
                dt = parsedate_to_datetime(val)
                return dt.strftime("%Y-%m-%d")
            except Exception:
                pass
    return datetime.now().strftime("%Y-%m-%d")


def download_podcast_audio(url: str, episode_id: str) -> Optional[Path]:
    """Download podcast audio file."""
    ensure_dirs()
    audio_path = CACHE_AUDIO / f"{episode_id}.mp3"
    if audio_path.exists():
        return audio_path

    try:
        with httpx.Client(timeout=300, follow_redirects=True) as client:
            with client.stream("GET", url) as resp:
                resp.raise_for_status()
                with open(audio_path, "wb") as f:
                    for chunk in resp.iter_bytes(chunk_size=8192):
                        f.write(chunk)
        return audio_path
    except Exception as e:
        logger.error("Error downloading podcast %s: %s", url, e)
        return None


def fetch_podcasts_for_manager(manager: dict, days_back: int = 30) -> int:
    """Fetch and transcribe podcast episodes for a manager. Returns count stored."""
    rss_feeds = manager.get("podcast_rss", [])
    if not rss_feeds:
        return 0

    manager_id = manager["id"]
    cutoff = datetime.now() - timedelta(days=days_back)
    stored = 0

    for rss_url in rss_feeds:
        logger.info("Parsing RSS feed for %s: %s", manager_id, rss_url)

        try:
            feed = feedparser.parse(rss_url)
        except Exception as e:
            logger.error("RSS parse error for %s: %s", rss_url, e)
            continue

        for entry in feed.entries:
            pub_date_str = parse_rss_date(entry)
            try:
                pub_dt = datetime.strptime(pub_date_str, "%Y-%m-%d")
            except Exception:
                pub_dt = datetime.now()

            if pub_dt < cutoff:
                continue

            # Find audio URL
            audio_url = None
            for enc in getattr(entry, "enclosures", []):
                if "audio" in enc.get("type", ""):
                    audio_url = enc.get("href") or enc.get("url")
                    break
            if not audio_url:
                audio_url = entry.get("link", "")

            if not audio_url:
                continue

            if is_episode_stored(audio_url):
                continue

            # Use a stable episode ID
            ep_id = entry.get("id", audio_url)[-50:].replace("/", "_").replace(":", "_")

            audio_path = download_podcast_audio(audio_url, ep_id)
            if not audio_path:
                continue

            transcript = transcribe_audio(audio_path, ep_id)
            if not transcript:
                continue

            try:
                doc_id = insert_document(
                    manager=manager_id,
                    source_type="podcast",
                    availability_date=pub_date_str,
                    url=audio_url,
                    raw_text=transcript,
                )
                stored += 1
                logger.info("Stored podcast transcript doc_id=%d", doc_id)
            except Exception as e:
                logger.error("Error storing podcast transcript: %s", e)

    return stored
