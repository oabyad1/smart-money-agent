import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import CACHE_DIR
from db.database import init_db, insert_document, get_connection

logger = logging.getLogger(__name__)

CACHE_AUDIO = Path(CACHE_DIR) / "audio"
CACHE_TRANSCRIPTS = Path(CACHE_DIR) / "transcripts"


def ensure_dirs():
    CACHE_AUDIO.mkdir(parents=True, exist_ok=True)
    CACHE_TRANSCRIPTS.mkdir(parents=True, exist_ok=True)


def is_video_stored(video_id: str) -> bool:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id FROM documents WHERE url LIKE ?", (f"%{video_id}%",)
        ).fetchone()
    return row is not None


def get_channel_videos(channel_url: str, days_back: int = 30) -> list[dict]:
    """List recent videos from a YouTube channel using yt-dlp."""
    try:
        result = subprocess.run(
            ["yt-dlp", "--flat-playlist", "--print", "%(id)s|%(upload_date)s|%(title)s",
             "--dateafter", (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d"),
             f"https://www.youtube.com/{channel_url}/videos"],
            capture_output=True, text=True, timeout=60
        )
        videos = []
        for line in result.stdout.strip().splitlines():
            parts = line.split("|", 2)
            if len(parts) == 3:
                vid_id, upload_date, title = parts
                if upload_date and len(upload_date) == 8:
                    date_fmt = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:]}"
                else:
                    date_fmt = datetime.now().strftime("%Y-%m-%d")
                videos.append({"id": vid_id, "upload_date": date_fmt, "title": title})
        return videos
    except FileNotFoundError:
        logger.warning("yt-dlp not installed — skipping YouTube ingestion")
        return []
    except Exception as e:
        logger.error("Error listing channel videos: %s", e)
        return []


def download_audio(video_id: str) -> Optional[Path]:
    """Download audio for a YouTube video."""
    ensure_dirs()
    audio_path = CACHE_AUDIO / f"{video_id}.mp3"
    if audio_path.exists():
        return audio_path

    url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        result = subprocess.run(
            ["yt-dlp", "--extract-audio", "--audio-format", "mp3",
             "--output", str(CACHE_AUDIO / f"{video_id}.%(ext)s"), url],
            capture_output=True, text=True, timeout=300
        )
        if result.returncode != 0:
            logger.error("yt-dlp failed for %s: %s", video_id, result.stderr[:200])
            return None
        return audio_path if audio_path.exists() else None
    except FileNotFoundError:
        logger.warning("yt-dlp not installed")
        return None
    except Exception as e:
        logger.error("Download error for %s: %s", video_id, e)
        return None


def transcribe_audio(audio_path: Path, video_id: str) -> Optional[str]:
    """Transcribe audio using Whisper. Returns transcript text."""
    transcript_path = CACHE_TRANSCRIPTS / f"{video_id}.txt"
    if transcript_path.exists():
        return transcript_path.read_text(encoding="utf-8")

    try:
        import whisper
        model = whisper.load_model("base")
        result = model.transcribe(str(audio_path))
        text = result["text"]
        transcript_path.write_text(text, encoding="utf-8")
        return text
    except ImportError:
        logger.warning("openai-whisper not installed — skipping transcription")
        return None
    except Exception as e:
        logger.error("Transcription error for %s: %s", video_id, e)
        return None


def fetch_youtube_for_manager(manager: dict, days_back: int = 30) -> int:
    """Fetch and transcribe YouTube videos for a manager. Returns count stored."""
    channels = manager.get("youtube_channels", [])
    if not channels:
        return 0

    manager_id = manager["id"]
    stored = 0

    for channel in channels:
        videos = get_channel_videos(channel, days_back=days_back)
        logger.info("Found %d recent videos for %s channel %s", len(videos), manager_id, channel)

        for video in videos:
            vid_id = video["id"]
            if is_video_stored(vid_id):
                logger.debug("Skipping already-stored video %s", vid_id)
                continue

            logger.info("Processing video %s: %s", vid_id, video["title"])

            audio_path = download_audio(vid_id)
            if not audio_path:
                continue

            transcript = transcribe_audio(audio_path, vid_id)
            if not transcript:
                continue

            word_count = len(transcript.split())
            logger.info("Transcript: %d words for video %s", word_count, vid_id)

            try:
                doc_id = insert_document(
                    manager=manager_id,
                    source_type="youtube",
                    availability_date=video["upload_date"],
                    url=f"https://www.youtube.com/watch?v={vid_id}",
                    raw_text=transcript,
                )
                stored += 1
                logger.info("Stored YouTube transcript doc_id=%d", doc_id)
            except Exception as e:
                logger.error("Error storing transcript: %s", e)

    return stored
