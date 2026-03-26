import os
import json
import logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent.parent

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY", "")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
BRIEF_RECIPIENT_EMAIL = os.getenv("BRIEF_RECIPIENT_EMAIL", "")
DB_PATH = os.getenv("DB_PATH", str(BASE_DIR / "data" / "smart_money.db"))
CACHE_DIR = os.getenv("CACHE_DIR", str(BASE_DIR / "cache"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
NEWS_ARTICLE_LIMIT = int(os.getenv("NEWS_ARTICLE_LIMIT", "20"))

MANAGERS_PATH = Path(__file__).parent / "managers.json"


def load_managers() -> list:
    with open(MANAGERS_PATH) as f:
        data = json.load(f)
    return data["managers"]


def setup_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
