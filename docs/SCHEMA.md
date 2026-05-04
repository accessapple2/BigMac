# OllieTrades Schema Map (Partial — 10 Most-Used Tables)
*Generated 2026-05-04 (afternoon Scotty session). Reflects state of `data/trader.db` and `signal-center/signals.db` at commit b94afe6. Full 196-table version pending Tuesday's dedicated session.*

## How to use this doc
- **Two databases exist**: `data/trader.db` (autonomous trader, 196 tables, 226 MB) and `signal-center/signals.db` (Signal Center service, separate DB, 638 MB).
- Each table entry shows: schema verbatim, row count snapshot, primary writers, primary readers, one-paragraph purpose.
- For tables not in this doc, fall back to `sqlite3 <db> ".schema <table>"` until Tuesday's full inventory ships.

---

## Domain: Signals & Decisions

### `signals` (data/trader.db)
- **Row count:** 61,058 (as of 2026-05-04 21:49 UTC, season 6)
- **Schema:**
  ```sql
  CREATE TABLE signals (
      id INTEGER PRIMARY KEY,
      player_id TEXT NOT NULL REFERENCES ai_players(id),
      symbol TEXT NOT NULL,
      signal TEXT NOT NULL,
      confidence REAL,
      reasoning TEXT,
      asset_type TEXT DEFAULT 'stock',
      option_type TEXT,
      acted_on INTEGER DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      season INTEGER DEFAULT 1,
      sources TEXT DEFAULT '',
      timeframe TEXT DEFAULT 'SWING',
      execution_status TEXT DEFAULT 'PENDING',
      rejection_reason TEXT DEFAULT NULL,
      halted_emit INTEGER DEFAULT 0
  );
  CREATE INDEX idx_signals_player_ts ON signals(player_id, created_at);
  CREATE INDEX idx_signals_status ON signals(execution_status);
  ```
- **Writers:** `engine/paper_trader.py:1878`
- **Readers:** `healthcheck.py:150`, `strategies/bull_call_spread_v1.py:255`, `strategies/bear_put_spread_v1.py:274`, `agents/uhura_agent.py:302`, `reset_season2.py:70`
- **Purpose:** Per-player BUY/SELL/HOLD signal emissions from the AI fleet. Gated by `halt_mode` via `engine/halt_gate.py`. Backfilled column `halted_emit` flags pre-fix-#1 ghost rows; HM-C readers filter on `halted_emit = 0` for scoring. `execution_status` tracks downstream lifecycle (PENDING / EXECUTED / SKIPPED / REJECTED).
- **Notes:** Primary signal table. Symbol column is `symbol` (NOT `ticker`). Different table from `signal-center/signals.db::trade_signals`.

### `watchlist_signals` (data/trader.db)
- **Row count:** 1,230 (as of 2026-05-04)
- **Schema:**
  ```sql
  CREATE TABLE watchlist_signals (
      id INTEGER PRIMARY KEY,
      player_id TEXT NOT NULL REFERENCES ai_players(id),
      display_name TEXT NOT NULL,
      symbol TEXT NOT NULL,
      entry_price REAL NOT NULL,
      confidence REAL NOT NULL,
      reasoning TEXT,
      status TEXT DEFAULT 'active',
      confirmed INTEGER DEFAULT 0,
      current_price REAL,
      pnl_pct REAL DEFAULT 0,
      signal_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      resolved_at TIMESTAMP,
      exit_price REAL,
      halted_emit INTEGER DEFAULT 0
  );
  ```
- **Writers:** `engine/signal_tracker.py:64`
- **Readers:** `engine/signal_tracker.py:47,57,104,179,191`
- **Purpose:** High-conviction BUY tracker — durable record of player BUY signals with confidence ≥ threshold, used to monitor whether the AI fleet's calls panned out without committing internal-book capital. Status transitions: `active` → `closed` (TP/SL hit) or `expired`. `pnl_pct` updated by signal_tracker on each price refresh.
- **Notes:** All read/write traffic concentrated in `engine/signal_tracker.py`. Symbol column is `symbol`.

### `trade_signals` (signal-center/signals.db) ⚠️ Separate DB
- **Row count:** 1,147 (as of 2026-05-04 14:00 ET)
- **Schema:**
  ```sql
  CREATE TABLE trade_signals (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      type          TEXT NOT NULL DEFAULT 'SWING',
      symbol        TEXT NOT NULL,
      action        TEXT NOT NULL,
      entry_price   REAL,
      stop_loss     REAL,
      take_profit   REAL,
      confidence    INTEGER,
      agent_name    TEXT,
      model_used    TEXT,
      reasoning     TEXT,
      context_json  TEXT,
      sources_json  TEXT,
      timeframe     TEXT DEFAULT 'SWING',
      status        TEXT NOT NULL DEFAULT 'NEW',
      created_at    TEXT DEFAULT CURRENT_TIMESTAMP,
      executed_at   TEXT,
      dismissed_at  TEXT
  );
  CREATE INDEX idx_ts_created ON trade_signals(created_at);
  CREATE INDEX idx_ts_symbol  ON trade_signals(symbol);
  CREATE INDEX idx_ts_status  ON trade_signals(status);
  ```
- **Writers:** `signal-center/server.py:1874`
- **Readers:** `strategies/bull_call_spread_v1.py:180`, `strategies/bear_put_spread_v1.py:197`, `dashboard/app.py:10999,11010,11061`
- **Purpose:** Signal Center signal store. Different population than `data/trader.db::signals` — Signal Center emits curated/scanner signals (e.g. `options_flow_scanner`, squeeze setups) while `signals` holds per-player AI emissions. Spread strategies (bull_call_spread_v1, bear_put_spread_v1) read confidence from this table to gate execution.
- **Notes:** ⚠️ Lives in `signal-center/signals.db`, NOT `data/trader.db`. Schema uses `symbol` (NOT `ticker`). `confidence` is INTEGER here (0-100 scale), unlike `signals.confidence` which is REAL (0.0-1.0).

### `signal_outcomes` (signal-center/signals.db) ⚠️ Separate DB
- **Row count:** 1,147 (1:1 with `trade_signals`)
- **Schema:**
  ```sql
  CREATE TABLE signal_outcomes (
      id              INTEGER PRIMARY KEY AUTOINCREMENT,
      signal_id       INTEGER NOT NULL,
      tracked_entry   REAL,
      tracked_high    REAL,
      tracked_low     REAL,
      tracked_current REAL,
      would_hit_tp    INTEGER DEFAULT 0,
      would_hit_sl    INTEGER DEFAULT 0,
      theoretical_pnl REAL,
      actual_pnl      REAL,
      tracking_start  TEXT DEFAULT CURRENT_TIMESTAMP,
      last_updated    TEXT DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (signal_id) REFERENCES trade_signals(id)
  );
  CREATE INDEX idx_so_signal ON signal_outcomes(signal_id);
  ```
- **Writers:** `signal-center/server.py:517`
- **Readers:** `signal-center/server.py:513,2043,2135,2146,2158`
- **Purpose:** TP/SL forecast accuracy tracker for `trade_signals`. Updated as price action unfolds — records whether each signal's published TP or SL would have been hit, plus theoretical PnL if the call was followed verbatim. Drives the Signal Center scorecard ("how often is the scanner right?").
- **Notes:** ⚠️ Lives in `signal-center/signals.db`. Always LEFT JOIN'd against `trade_signals` via `signal_id`. 1:1 ratio observed today (every signal has an outcome row).

---

## Domain: Trades & Positions

### `trades` (data/trader.db)
- **Row count:** 1,762 (latest id 2077, season 6)
- **Schema:**
  ```sql
  CREATE TABLE trades (
      id INTEGER PRIMARY KEY,
      player_id TEXT NOT NULL REFERENCES ai_players(id),
      symbol TEXT NOT NULL,
      action TEXT NOT NULL,
      qty REAL,
      price REAL,
      asset_type TEXT DEFAULT 'stock',
      option_type TEXT,
      strike_price REAL,
      expiry_date TEXT,
      reasoning TEXT,
      confidence REAL,
      executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      exit_price REAL,
      realized_pnl REAL,
      entry_price REAL,
      season INTEGER DEFAULT 1,
      corrected_pnl REAL,
      sources TEXT DEFAULT '',
      timeframe TEXT DEFAULT 'SWING',
      alpaca_order_id TEXT,
      alpaca_status TEXT,
      execution_type TEXT DEFAULT 'simulated',
      spread_data TEXT,
      strategy_id TEXT
  );
  CREATE INDEX idx_trades_player_ts ON trades(player_id, executed_at);
  CREATE INDEX idx_trades_alpaca ON trades(alpaca_order_id);
  ```
- **Writers:** `engine/paper_trader.py:1003,1156`, `shared/matrix_bridge.py:196`, `scripts/import_webull_csv.py:62`, `scripts/close_player_positions.py:98`
- **Readers:** `healthcheck.py:149`, `main.py:1976`, `crew/routes.py:336`, `run_comprehensive_backtest.py:87`, `reset_season2.py:69`
- **Purpose:** Internal-book trade log — every BUY/SELL the AI fleet executes (paper or live-routed) lands here. PnL realized on SELL via `realized_pnl`/`corrected_pnl`. `execution_type` distinguishes `simulated` from broker-routed orders. `alpaca_order_id` populated when routed to Alpaca.
- **Notes:** ⚠️ Common confusion: this is the trade log, NOT `paper_trades` (which does not exist). Symbol column is `symbol` (NOT `ticker`).

### `positions` (data/trader.db)
- **Row count:** 40 (open positions only — closed positions live in `trades` history)
- **Schema:**
  ```sql
  CREATE TABLE positions (
      id INTEGER PRIMARY KEY,
      player_id TEXT NOT NULL REFERENCES ai_players(id),
      symbol TEXT NOT NULL,
      qty REAL,
      avg_price REAL,
      asset_type TEXT DEFAULT 'stock',
      option_type TEXT,
      strike_price REAL,
      expiry_date TEXT,
      opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      high_watermark REAL,
      UNIQUE(player_id, symbol, asset_type, option_type, strike_price, expiry_date)
  );
  ```
- **Writers:** `engine/paper_trader.py:992,997`, `shared/alpaca_portfolio_sync.py:149`, `shared/matrix_bridge.py:172`, `engine/webull_client.py:311`
- **Readers:** `main.py:1118`, `source_status_review.py:72`, `reset_season2.py:43,45,68`
- **Purpose:** Open positions for the internal AI fleet book. UNIQUE constraint on (player_id, symbol, asset_type, option_type, strike_price, expiry_date) prevents duplicate position rows for the same instrument. `high_watermark` tracks max favorable price for trailing-stop logic. Rows are deleted (not flagged) when fully closed; partial closes update `qty`.
- **Notes:** ⚠️ Common confusion: NOT `open_positions` (which does not exist). Symbol column is `symbol` (NOT `ticker`).

### `options_trades` (data/trader.db)
- **Row count:** 6 (mostly pre-gate-flip test rows; awaiting first live spread fire)
- **Schema:**
  ```sql
  CREATE TABLE options_trades (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      book_tag TEXT NOT NULL DEFAULT 'fleet',
      agent_id TEXT NOT NULL,
      structure TEXT NOT NULL,
      symbol TEXT NOT NULL,
      entry_date TEXT NOT NULL,
      exit_date TEXT,
      expiration TEXT NOT NULL,
      dte_at_entry INTEGER,
      legs_json TEXT NOT NULL,
      entry_credit_debit REAL NOT NULL,
      exit_credit_debit REAL,
      max_profit REAL,
      max_loss REAL,
      pnl REAL,
      pnl_pct REAL,
      status TEXT DEFAULT 'open',
      exit_reason TEXT,
      regime_at_entry TEXT,
      vix_at_entry REAL,
      notes TEXT,
      strategy_id TEXT,
      exit_tag TEXT DEFAULT 'single',
      broker_order_id TEXT,
      signal_id INTEGER,
      exec_status TEXT DEFAULT 'pending',
      contracts INTEGER DEFAULT 1,
      contracts_closed_so_far INTEGER DEFAULT 0
  );
  CREATE INDEX idx_options_trades_book ON options_trades(book_tag);
  CREATE INDEX idx_options_trades_agent ON options_trades(agent_id);
  CREATE INDEX idx_options_trades_status ON options_trades(status);
  CREATE INDEX idx_options_trades_date ON options_trades(entry_date);
  ```
- **Writers:** `strategies/executor.py:363`, `engine/options_exec.py:93`
- **Readers:** `strategies/exit_manager.py:106,119`, `strategies/bull_call_spread_v1.py:281`, `strategies/executor.py:193,309`
- **Purpose:** Options trade log for multi-leg structures (vertical spreads, etc.). `legs_json` holds the full leg list (action/option_type/strike/expiration/premium per leg). `signal_id` foreign-keys back to the source `trade_signals.id` that triggered the spread. Scaleout tracking via `contracts` / `contracts_closed_so_far` and `exit_tag`.
- **Notes:** Will accept entries when `bull_call_spread_v1` / `bear_put_spread_v1` fire post-gate-flip. Existing rows have `exec_status = 'test_cleanup'` (pre-gate-flip seeding).

---

## Domain: Real Broker (Monitor-Only)

### `schwab_holdings` (data/trader.db)
- **Row count:** 62 (multi-snapshot; latest snapshot 2026-05-04T12:15:00)
- **Schema:**
  ```sql
  CREATE TABLE schwab_holdings (
      id                  INTEGER PRIMARY KEY AUTOINCREMENT,
      snapshot_id         TEXT    NOT NULL,
      snapshot_ts         TEXT    NOT NULL,
      account_label       TEXT    NOT NULL,
      account_last4       TEXT    NOT NULL,
      symbol              TEXT    NOT NULL,
      description         TEXT,
      qty                 REAL,
      price               REAL,
      market_value        REAL,
      cost_basis          REAL,
      gain_dollar         REAL,
      gain_pct            REAL,
      day_change_dollar   REAL,
      day_change_pct      REAL,
      price_change_dollar REAL,
      price_change_pct    REAL,
      asset_type          TEXT    NOT NULL,
      reinvest            TEXT,
      reinvest_cap_gains  TEXT,
      is_summary_row      INTEGER NOT NULL DEFAULT 0,
      imported_at         TEXT    NOT NULL DEFAULT (datetime('now')),
      csv_source_path     TEXT    NOT NULL,
      UNIQUE(snapshot_id, symbol)
  );
  CREATE INDEX ix_schwab_holdings_snapshot ON schwab_holdings(snapshot_id);
  CREATE INDEX ix_schwab_holdings_symbol   ON schwab_holdings(symbol, snapshot_ts DESC);
  CREATE INDEX idx_schwab_holdings_summary ON schwab_holdings(is_summary_row);
  ```
- **Writers:** `scripts/import_schwab_csv.py:60` (uses `INSERT OR IGNORE INTO`)
- **Readers:** `scripts/import_schwab_csv.py:248,258`, `scripts/schwab_advisor.py:42,53`, `scripts/sync_schwab_to_real_holdings.py:29`
- **Purpose:** Schwab account snapshot history. Each Schwab CSV import (positions export) creates a new `snapshot_id`; rows persist forever for time-series gain/loss reconstruction. `is_summary_row=1` flags Schwab's account-total/cash rollup rows. Most-recent snapshot = dashboard truth.
- **Notes:** Refreshed via `scripts/schwab_csv_watcher.sh` watching `~/Downloads/`. Multi-snapshot table — never UPDATE'd in place. `sync_schwab_to_real_holdings.py` propagates the latest snapshot into `data/real_holdings.json`.

---

## Domain: Players & Halt State

### `ai_players` (data/trader.db)
- **Row count:** 49 (active fleet roster, season 6)
- **Schema:**
  ```sql
  CREATE TABLE ai_players (
      id TEXT PRIMARY KEY,
      display_name TEXT NOT NULL,
      provider TEXT NOT NULL,
      model_id TEXT NOT NULL,
      cash REAL DEFAULT 10000.00,
      is_active INTEGER DEFAULT 1,
      is_halted INTEGER DEFAULT 0,
      halt_reason TEXT,
      can_trade_live INTEGER DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      is_paused INTEGER DEFAULT 0,
      season INTEGER DEFAULT 1,
      is_human INTEGER DEFAULT 0,
      options_enabled INTEGER DEFAULT 0,
      short_enabled INTEGER DEFAULT 0,
      fallback_model TEXT,
      is_fallback INTEGER DEFAULT 0,
      crew_role TEXT DEFAULT 'active',
      role TEXT DEFAULT 'production',
      halt_mode TEXT DEFAULT 'active'
        CHECK(halt_mode IN ('full','exit_only','active')),
      halted_at TIMESTAMP
  );
  ```
- **Writers:** `setup_db.py:267`, `engine/strategies.py:977`, `engine/crew_scanner.py:3589`, `engine/agent_builder.py:91`, `shared/matrix_bridge.py:113`
- **Readers:** `main.py:1968`, `main_crew.py:179`, `source_status_review.py:68`, `crew/routes.py:278`, `reset_season2.py:65`
- **Purpose:** AI fleet roster — one row per agent (model + persona + cash + role + halt state). Halt state lives in `halt_mode`: `active` (normal), `exit_only` (no new entries, exits allowed), `full` (no signals at all). Legacy `is_halted` column preserved until HM-B retirement. `crew_role` distinguishes `active` from `xo` / archived roles. `role` distinguishes `production` from `lab` / `dev`.
- **Notes:** `halted_at` set manually per CLAUDE.md halt runbook. `fallback_model` is the cold-failover model used when the primary model is unreachable; `is_fallback=1` means the row itself is a fallback shadow agent.

### `kirk_advisory_log` (data/trader.db)
- **Row count:** 272 (daily writes, latest 2026-05-01)
- **Schema:**
  ```sql
  CREATE TABLE kirk_advisory_log (
      id              INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker          TEXT NOT NULL,
      action          TEXT,
      message         TEXT,
      alert_type      TEXT DEFAULT 'advisory',
      fear_greed_score REAL,
      vix_level       REAL,
      dismissed_at    TIMESTAMP,
      acted_on        INTEGER DEFAULT 0,
      acted_at        TIMESTAMP,
      created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  ```
- **Writers:** `engine/kirk_advisory.py:333`, `engine/trade_cards_api.py:1068`
- **Readers:** `dashboard/app.py:13503`, `engine/kirk_advisory.py:327`, `engine/trade_cards_api.py:1043,1059`, `scripts/ghost_advisor.py:410`
- **Purpose:** Daily Kirk advisor log (Webull-style portfolio guidance). One row per advisory event (BUY/SELL/HOLD/CRITICAL alert) with regime context (`fear_greed_score`, `vix_level`). `acted_on` and `dismissed_at` track whether Captain followed or dismissed each call.
- **Notes:** ⚠️ Schema uses `ticker` (NOT `symbol`) — this is the only documented table here that does. Different table from the retired Swing Desk variant (archived at `archive/retired/2026-05-04-kirk-swing-desk/`). Daily writes via `engine/kirk_advisory.py` and `engine/kirk_grok_advisor.py`.

---

## Common Naming Mismatches (Today's Lessons)

| Wrong guess | Actual | DB |
|---|---|---|
| `paper_trades` | `trades` | `data/trader.db` |
| `open_positions` | `positions` | `data/trader.db` |
| `trade_signals.ticker` | `trade_signals.symbol` | `signal-center/signals.db` |
| `signals` ≠ `trade_signals` | different tables, different DBs | both |

**Symbol column convention:** all 10 documented tables use `symbol` EXCEPT `kirk_advisory_log`, which uses `ticker`. Don't assume one or the other — check the schema.

**Confidence convention:** `signals.confidence` is REAL (0.0-1.0). `trade_signals.confidence` is INTEGER (0-100). They are not interchangeable.

---

## What's NOT in this doc

~186 other tables in `data/trader.db` and any other tables in `signal-center/signals.db`. Tuesday's session covers the full inventory grouped by domain.
