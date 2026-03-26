# Smart Money Signal Agent

An automated intelligence system that detects divergence between what major fund managers say publicly and what they actually do with their positions, as disclosed in SEC 13F filings. When a manager talks bullishly about a stock while their 13F shows they are selling, that is a **distribution** signal. When they are silent about a large growing position, that is an **accumulation** signal. When they speak bearishly while secretly buying, that is a **contrarian accumulation** signal.

The system ingests data from SEC EDGAR, news APIs, YouTube, podcasts, and fund investor letters. It runs a 6-pass LLM analysis pipeline using the Claude API (claude-sonnet-4-6), scores signals using per-manager historical weights, logs paper trades, and sends a daily brief by 7am.

---

## Setup

### 1. Create and activate a virtual environment

```bash
python -m venv .venv
# On macOS/Linux:
source .venv/bin/activate
# On Windows:
.venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

Copy `.env.example` to `.env` and fill in your API keys:

```bash
cp .env.example .env
```

Required keys:
- `ANTHROPIC_API_KEY` — for the 6-pass analysis pipeline
- `POLYGON_API_KEY` or `NEWSAPI_KEY` — for news ingestion (one is sufficient)

Optional but recommended:
- `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD` — for email delivery of daily briefs
- `BRIEF_RECIPIENT_EMAIL` — where the daily brief is sent

---

## Running the historical EDGAR backfill

On first run, fetch all available 13F filing history (typically 10–20 years) for all configured managers:

```bash
python orchestrator.py --backfill
```

Raw XML responses are cached in `cache/edgar/` — they will never be re-fetched. For 8 managers, this takes 5–15 minutes depending on network speed.

---

## Running the daily orchestrator

### Run immediately (one-shot)

```bash
python orchestrator.py --run-now
```

This runs the full pipeline in sequence:
1. Closes any paper trades that have held for 30+ days
2. Fetches new EDGAR 13F/13D filings
3. Fetches news articles mentioning each manager
4. Downloads and transcribes new YouTube videos and podcast episodes
5. Scrapes new fund investor letters
6. Runs the 6-pass Claude analysis pipeline on all unprocessed documents
7. Scores signals and opens paper trades for fired signals
8. Sends the daily brief email

### Run on a schedule (every day at 06:00)

```bash
python orchestrator.py
```

The scheduler uses APScheduler and blocks until interrupted with Ctrl+C. For production, run inside a process manager (systemd, supervisor, or screen).

---

## Viewing the dashboard

```bash
streamlit run output/dashboard.py
```

Opens a browser dashboard with four tabs:

- **Today's signals** — all signals fired in the past 7 days with confidence, direction, and verbatim quotes
- **Paper portfolio** — open and closed paper trades with live P&L via yfinance
- **Manager scorecard** — per-manager weights, instance counts, and realised win rates
- **Signal log** — full searchable history filterable by manager, ticker, signal type, and date range

---

## Interpreting the daily brief

The brief is sent by email and also printed to stdout when no SMTP is configured. Key sections:

```
[FADE] AAPL — distribution — 82% confidence
Manager: ackman
Quote: "Apple remains one of the great businesses of our time..."
Position: Reduced 38% in Q3 2024 13F
Action: Paper Short opened at $182.45
```

- **[FADE]** means the signal suggests trading against the manager's public statement (they're talking up a stock they're selling).
- **[FOLLOW]** means the signal aligns with the manager's public statement (they're buying what they're praising, or quietly accumulating without mentioning it).
- **confidence** is `raw_score × manager_weight`, capped at 0.55 for manager+signal_type combinations with fewer than 8 historical instances.
- Only signals with confidence ≥ 0.55 are stored and traded.

---

## Project structure

```
smart-money-agent/
├── config/
│   ├── managers.json      — manager universe, CIKs, and historical weights
│   └── settings.py        — loads .env, exposes typed config
├── db/
│   ├── schema.sql         — canonical SQLite schema
│   └── database.py        — connection helpers and query functions
├── ingestion/
│   ├── edgar.py           — SEC EDGAR 13F/13D fetcher
│   ├── news.py            — Polygon.io / NewsAPI fetcher
│   ├── youtube.py         — yt-dlp downloader + Whisper transcriber
│   ├── podcasts.py        — RSS feed fetcher + Whisper transcriber
│   └── fund_letters.py    — PDF and HTML fund letter scraper
├── analysis/
│   ├── pipeline.py        — 6-pass Claude API pipeline orchestrator
│   ├── passes.py          — individual pass implementations
│   ├── prompts.py         — all prompt templates
│   └── cross_reference.py — matches statements to 13F position changes
├── scoring/
│   ├── weights.py         — applies manager weights, stores fired signals
│   └── calibration.py     — confidence calibration reports
├── trading/
│   └── paper.py           — paper trade logging and P&L computation
├── output/
│   ├── brief.py           — formats and sends the daily email brief
│   └── dashboard.py       — Streamlit dashboard
├── orchestrator.py        — main entry point and APScheduler setup
└── tests/                 — pytest test suite
```

---

## Running tests

```bash
cd smart-money-agent
pytest tests/ -v
```

All tests use an in-memory SQLite database and do not require API keys.
