CREATE TABLE IF NOT EXISTS documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    manager TEXT NOT NULL,
    source_type TEXT NOT NULL,
    availability_date TEXT NOT NULL,
    content_date TEXT,
    url TEXT,
    raw_text TEXT,
    processed INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    manager TEXT NOT NULL,
    ticker TEXT NOT NULL,
    filing_date TEXT NOT NULL,
    period_of_report TEXT NOT NULL,
    shares INTEGER,
    value_usd INTEGER,
    pct_of_portfolio REAL,
    delta_shares INTEGER,
    delta_pct REAL,
    filing_type TEXT DEFAULT '13F',
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS statements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id INTEGER REFERENCES documents(id),
    manager TEXT NOT NULL,
    ticker TEXT,
    quote_verbatim TEXT NOT NULL,
    sentiment TEXT,
    hedge_flags TEXT,
    conviction_level TEXT,
    pass_number INTEGER,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    statement_id INTEGER REFERENCES statements(id),
    position_id INTEGER REFERENCES positions(id),
    manager TEXT NOT NULL,
    ticker TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    raw_score REAL,
    manager_weight REAL,
    final_confidence REAL,
    direction TEXT NOT NULL,
    fired_date TEXT NOT NULL,
    status TEXT DEFAULT 'active',
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS paper_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id INTEGER REFERENCES signals(id),
    ticker TEXT NOT NULL,
    direction TEXT NOT NULL,
    entry_price REAL,
    entry_date TEXT,
    exit_price REAL,
    exit_date TEXT,
    hold_days INTEGER DEFAULT 30,
    pnl_pct REAL,
    pnl_direction TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_documents_manager ON documents(manager);
CREATE INDEX IF NOT EXISTS idx_documents_availability ON documents(availability_date);
CREATE INDEX IF NOT EXISTS idx_positions_manager_ticker ON positions(manager, ticker);
CREATE INDEX IF NOT EXISTS idx_signals_ticker ON signals(ticker);
CREATE INDEX IF NOT EXISTS idx_signals_fired_date ON signals(fired_date);
