-- Ghost Trader schema (Season 6 advisory parallel)
CREATE TABLE IF NOT EXISTS ghost_portfolio (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL UNIQUE,
  qty REAL NOT NULL,
  avg_cost REAL NOT NULL,
  opened_at TEXT NOT NULL,
  source_advisor TEXT,
  source_signal_id TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS ghost_cash (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  cash REAL NOT NULL,
  equity REAL NOT NULL,
  last_updated TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ghost_trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
  qty REAL NOT NULL,
  price REAL NOT NULL,
  fill_price REAL,
  venue TEXT NOT NULL CHECK (venue IN ('virtual','alpaca_shadow')),
  advisor TEXT NOT NULL,
  signal_id TEXT,
  status TEXT NOT NULL DEFAULT 'filled',
  rationale TEXT
);

CREATE TABLE IF NOT EXISTS ghost_seed (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  seeded_at TEXT NOT NULL,
  symbol TEXT,
  qty REAL,
  cost_basis REAL,
  cash_at_seed REAL,
  source TEXT DEFAULT 'schwab_snapshot'
);

CREATE TABLE IF NOT EXISTS ghost_equity_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date TEXT NOT NULL UNIQUE,
  ghost_equity REAL NOT NULL,
  schwab_equity REAL,
  delta REAL
);

CREATE INDEX IF NOT EXISTS idx_ghost_trades_ts ON ghost_trades(ts);
CREATE INDEX IF NOT EXISTS idx_ghost_trades_symbol ON ghost_trades(symbol);
CREATE INDEX IF NOT EXISTS idx_ghost_trades_advisor ON ghost_trades(advisor);
CREATE INDEX IF NOT EXISTS idx_ghost_equity_date ON ghost_equity_history(date);
