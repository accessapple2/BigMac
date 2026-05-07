# OllieTrades Schema Map — Full Inventory (196 + 11 tables)
*Originally generated 2026-05-04 as a 10-table partial (commit 1a8dcfe). Extended to full inventory on 2026-05-05 to remove the per-audit `sqlite3 .schema` ritual that has burned roughly 5 minutes on every investigation since the gate-flip.*

## How to use this doc
- **Two databases exist.** `data/trader.db` (autonomous trader, 195 user tables, ~226 MB) and `signal-center/signals.db` (Signal Center service, 11 user tables, ~638 MB). Lookups should always state which DB.
- **Entries are tiered** to keep the doc readable:
  - **Tier 1 (deep)** — schema verbatim, row count, primary writers, primary readers, purpose paragraph, notes. Reserved for tables with active production code paths or high traffic.
  - **Tier 2 (medium)** — schema line + row count + 1-line purpose. Tables with code paths but lower traffic, or recently archived candidates.
  - **Tier 3 (light)** — name + row count + 1-line note. Empty / vestigial / experimental tables. Look up with `sqlite3 data/trader.db ".schema <table>"` if you ever need the full DDL.
- **Tier 1 distribution:** the 10 from yesterday plus ~25 new entries identified by activity heatmap (most-recent timestamp on 2026-05-04 / 2026-05-05) and by `INSERT INTO` writers found in production code paths.

## Common Naming Mismatches

| Wrong guess | Actual | DB |
|---|---|---|
| `paper_trades` | `trades` | `data/trader.db` |
| `open_positions` | `positions` | `data/trader.db` |
| `trade_signals.ticker` | `trade_signals.symbol` | `signal-center/signals.db` |
| `signals` ≠ `trade_signals` | different tables, different DBs | both |
| `agent_state` | does not exist (vestigial reference in PED) | n/a |
| `signal_scorecard` (with rows) | exists but always empty (writer never wired) | `data/trader.db` |

**Symbol column convention:** most tables use `symbol`. Exceptions that use `ticker`: `kirk_advisory_log`, `fast_scan_results`, `institutional_holdings`, `institutional_signals`, `correlation_pairs`, `signal_multiplier_log`, `uoa_alerts`, `uoa_flow`, `uoa_daily_summary`, `impulse_alerts`, `aladdin_holdings` (uses `etf_ticker`/`holding_ticker`), `scotty_watchlist`, `reference_trades` (uses `symbol`). Always check the schema before joining.

**Confidence convention:** `signals.confidence` is REAL (0.0-1.0). `trade_signals.confidence` and `bridge_votes.confidence` are INTEGER (0-100). Not interchangeable.

**Naming inconsistency:** writes that use `created_at` are most common, but you'll also see `recorded_at` (portfolio_history, flow_lean_history, real_portfolio_history), `timestamp` (api_costs, agent_ratings, aladdin_signals, sarek_*, surak_*, janeway_*, crew_decisions, impulse_alerts), `imported_at` (reference_trades, schwab_holdings, rallies_trades), `detected_at` (volume_alerts, volatility_breakouts, impulse_alerts), `entry_date` / `executed_at` (options_trades, trades), `snapshot_ts` (schwab_holdings).

---

## Domain: Signals & Decisions

### `signals` (data/trader.db) — Tier 1
- **Row count:** 62,366 (as of 2026-05-05 13:13:49)
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

### `watchlist_signals` (data/trader.db) — Tier 1
- **Row count:** 1,241 (as of 2026-05-05 12:59:02)
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

### `signal_multiplier_log` (data/trader.db) — Tier 1
- **Row count:** 654 (latest 2026-05-04 19:46:44)
- **Schema:**
  ```sql
  CREATE TABLE signal_multiplier_log (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker      TEXT    NOT NULL,
      date        TEXT    NOT NULL,
      base_score  REAL    NOT NULL,
      gex_mult    REAL    NOT NULL,
      flow_mult   REAL    NOT NULL,
      final_score REAL    NOT NULL,
      trade_taken INTEGER NOT NULL DEFAULT 0,
      created_at  TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
  );
  ```
- **Writers:** Active scorer pipeline (look for `INSERT INTO signal_multiplier_log` if needed).
- **Readers:** Strategy multiplier ranking; dashboard scoring panels.
- **Purpose:** Audit trail for the multi-factor scoring pipeline. `base_score` is the agent's raw conviction; `gex_mult` and `flow_mult` weight by GEX regime and options-flow signal respectively; `final_score = base_score × gex_mult × flow_mult` is what gating decisions read. `trade_taken` is the post-hoc indicator of whether the signal actually fired.
- **Notes:** Symbol column is `ticker`. Use this table when investigating "why did/didn't agent X fire on ticker Y" — shows the pipeline scoring breakdown.

### `discoveries` (data/trader.db) — Tier 1
- **Row count:** 8,049
- **Schema:**
  ```sql
  CREATE TABLE discoveries (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      symbol TEXT NOT NULL,
      trigger_type TEXT NOT NULL,
      price REAL,
      change_pct REAL,
      volume REAL,
      rel_volume REAL,
      short_float REAL,
      details TEXT,
      detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      acted_on INTEGER DEFAULT 0
  );
  ```
- **Purpose:** Generic discovery hits from scanners (gap, volume spike, short squeeze candidates). `trigger_type` is the scanner name. `acted_on` flips when a downstream agent picks the row up.

### `dynamic_alerts` (data/trader.db) — Tier 1
- **Row count:** 12,861
- **Schema:**
  ```sql
  CREATE TABLE dynamic_alerts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      symbol TEXT NOT NULL,
      alert_type TEXT NOT NULL,
      message TEXT NOT NULL,
      severity TEXT DEFAULT 'info',
      price REAL,
      triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  ```
- **Purpose:** Generic alert sink for ad-hoc scanner messages. `severity` informs ntfy / dashboard surfaces.

### `trade_advisories` (data/trader.db) — Tier 1
- **Row count:** 4,277 (latest 2026-05-05 13:06:21)
- **Schema:**
  ```sql
  CREATE TABLE trade_advisories (
      id              INTEGER PRIMARY KEY AUTOINCREMENT,
      trade_date      TEXT NOT NULL,
      player_id       TEXT,
      symbol          TEXT,
      proposed_action TEXT,
      signal          TEXT,
      multiplier      REAL,
      reason          TEXT,
      condition       TEXT,
      session_type    TEXT,
      vix_regime      TEXT,
      overridden      INTEGER NOT NULL DEFAULT 0,
      created_at      TEXT NOT NULL DEFAULT (datetime('now'))
  );
  ```
- **Writers:** `engine/ready_room_advisor.py`
- **Readers:** `engine/holodeck_readyroom.py`, `scripts/ghost_advisor.py`
- **Purpose:** Pre-trade advisory log — ready-room advisor's recommendation per ticker per session. `overridden=1` means a higher layer overrode the advisory. Drives ghost-trading and the ready-room dashboard panel.

### Tier 2/3 (Signals & Decisions)
- `strategy_signals` (5,777, latest 2026-04-15) — per-strategy signal emissions from `engine/strategies.py` (legacy convergence scanner; silently dead path per HM 2026-05-03).
- `registry_signals` (84) — signal registry/dedup index.
- `sma_signals` (30) — SMA-cross signal log.
- `smart_money_signals` (14) — institutional flow synthesis hits.
- `external_picks` (4) — operator-supplied tickers from outside agents.
- `weekly_picks` (40, latest 2026-05-03) — Picard's weekly thesis picks.
- `signal_scorecard` (0, schema present) — empty, writer never wired (HM-S blocking item).
- `flash_alerts` (0) — empty alert sink, no active writers.

---

## Domain: Trades & Positions

### `trades` (data/trader.db) — Tier 1
- **Row count:** 1,769 (latest 2026-05-05 12:53:33)
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

### `positions` (data/trader.db) — Tier 1
- **Row count:** 39 (open positions only — closed positions live in `trades` history)
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

### `options_trades` (data/trader.db) — Tier 1
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

### `trade_outcomes` (data/trader.db) — Tier 1
- **Row count:** 775 (latest 2026-05-05 12:57:40)
- **Schema:**
  ```sql
  CREATE TABLE trade_outcomes (
      id                    INTEGER PRIMARY KEY AUTOINCREMENT,
      trade_id              INTEGER,
      player_id             TEXT NOT NULL,
      symbol                TEXT NOT NULL,
      entry_price           REAL,
      exit_price            REAL,
      entry_time            TEXT,
      exit_time             TEXT,
      pnl_dollars           REAL,
      pnl_percent           REAL,
      hold_duration_hours   REAL,
      regime_at_entry       TEXT,
      gex_regime_at_entry   TEXT,
      vix_at_entry          REAL,
      fear_greed_at_entry   REAL,
      strategy_name         TEXT,
      conviction_at_entry   REAL,
      outcome               TEXT,
      created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  ```
- **Writers:** `engine/trade_outcomes.py`
- **Readers:** `engine/trade_memory.py`, `engine/trade_outcomes.py`, `scripts/learning/extract_corpus.py`
- **Purpose:** Post-trade learning corpus — `trades` enriched with regime context (`regime_at_entry`, `gex_regime_at_entry`, `vix_at_entry`, `fear_greed_at_entry`) for offline learning and self-improvement. `outcome` is categorical: WIN/LOSS/EXPIRED/STOP/TARGET.
- **Notes:** `trade_id` foreign-keys back to `trades.id`. Used by `extract_corpus.py` to build the LLM training corpus.

### Tier 2/3 (Trades & Positions)
- `pair_trades` (30) — pair-trade ledger (long/short pair structures); legacy.
- `manual_trades` (0) — operator manual entries; empty.
- `kirk_swing_trades` (0) — retired Swing Desk trade log; archived 2026-05-04.
- `holly_deepdives` (10, latest 2026-04-21) — Holly intra-day deep-dive scratch trades.
- `rallies_trades` (8) — imported rallies.ai reference trades.
- `crew_trade_results` (8) — crew-system trade results (legacy CrewAI experiments).
- `bakeoff_trades` (0) / `bakeoff_runs` (1) — agent bake-off scaffolding; never wired.
- `battle_station_trades` (1, latest 2026-04-27) — battle-station trade log; dormant per fleet reality 2026-05-03.
- `sarek_paper_trades` (3) — Elder Council Sarek (5yr) paper-trade ledger.
- `janeway_paper_trades` (0) — Elder Council Janeway (10yr); empty.
- `surak_paper_trades` (0) — Elder Council Surak (20yr); empty.

---

## Domain: Real Broker (Monitor-Only)

### `schwab_holdings` (data/trader.db) — Tier 1
- **Row count:** 62 (multi-snapshot; latest snapshot 2026-04-24 12:48 ET — file-based ingestion)
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
- **Notes:** Refreshed via `scripts/schwab_csv_watcher.sh` watching `~/autonomous-trader/inbox/` (migrated from `~/Downloads/` 2026-05-07 per HM-AT-β — TCC blocked launchd from Downloads). Multi-snapshot table — never UPDATE'd in place. `sync_schwab_to_real_holdings.py` propagates the latest snapshot into `data/real_holdings.json`. Monitor-only per CLAUDE.md broker rule.

### Tier 2/3 (Real Broker)
- `webull_watchlist` (2) — pre-wind-down Webull watchlist; vestigial after Webull liquidation.
- `real_portfolio_history` (7, latest 2026-05-04 13:45) — daily rollup of real-broker totals (Schwab dominant).

---

## Domain: Players & Halt State

### `ai_players` (data/trader.db) — Tier 1
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
- **Purpose:** AI fleet roster — one row per agent (model + persona + cash + role + halt state). Halt state lives in `halt_mode`: `active` (normal), `exit_only` (no new entries, exits allowed), `full` (no signals at all). `crew_role` distinguishes `active` from `xo` / archived roles. `role` distinguishes `production` from `lab` / `dev`.
- **Notes:** ⚠️ HM-B (2026-05-04) **dropped the `is_halted` column** — single source of truth is now `halt_mode != 'active'`. `halted_at` set manually per CLAUDE.md halt runbook. `fallback_model` is the cold-failover model used when the primary model is unreachable; `is_fallback=1` means the row itself is a fallback shadow agent.

### `kirk_advisory_log` (data/trader.db) — Tier 1
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
- **Notes:** ⚠️ Schema uses `ticker` (NOT `symbol`). Different table from the retired Swing Desk variant (archived at `archive/retired/2026-05-04-kirk-swing-desk/`). Daily writes via `engine/kirk_advisory.py` and `engine/kirk_grok_advisor.py`.

### Tier 2/3 (Players & Halt State)
- `agent_id_aliases` (8) — display-name → player_id alias map.
- `agent_memory` (15, latest 2026-04-30) — per-player persistent memory blob (notes/state).
- `agent_allocation` (7) — capital allocation per player.
- `user_agents` (0) — operator-defined agents; empty.
- `kill_switch_log` (0) / `kill_switch_events_legacy` (1, vestigial 2026-04 era) — pre-halt-mode kill switch; superseded by `halt_mode`.

---

## Domain: Scoring, Ratings & Calibration

### `agent_ratings` (data/trader.db) — Tier 1
- **Row count:** 16,316 (latest 2026-05-05 01:54:10 — every agent rated each cycle)
- **Schema:**
  ```sql
  CREATE TABLE agent_ratings (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      player_id TEXT,
      timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
      period TEXT,
      total_trades INTEGER,
      wins INTEGER,
      losses INTEGER,
      win_rate REAL,
      total_pnl REAL,
      avg_win REAL,
      avg_loss REAL,
      profit_factor REAL,
      best_trade REAL,
      worst_trade REAL,
      consecutive_losses INTEGER,
      consecutive_wins INTEGER,
      avg_confidence REAL,
      volume_accuracy REAL,
      pass_rate REAL,
      rating TEXT,
      rating_score REAL
  );
  CREATE INDEX idx_agent_ratings_player_period ON agent_ratings(player_id, period, timestamp);
  ```
- **Writers/Readers:** `engine/agent_ratings.py` (both)
- **Purpose:** Periodic per-player performance ratings. `period` is the window label (e.g. `7d`, `30d`, `season`); `rating` is the categorical grade derived from `rating_score`. Drives leaderboard and halt-recommendation surfaces.

### `api_costs` (data/trader.db) — Tier 1
- **Row count:** 61,344 (latest 2026-05-05 06:13:49 — every API call logged)
- **Schema:**
  ```sql
  CREATE TABLE api_costs (
      id INTEGER PRIMARY KEY,
      player_id TEXT NOT NULL REFERENCES ai_players(id),
      call_type TEXT NOT NULL DEFAULT 'scan',
      input_tokens INTEGER DEFAULT 0,
      output_tokens INTEGER DEFAULT 0,
      cost_usd REAL DEFAULT 0.0,
      timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  ```
- **Writers:** `engine/cost_tracker.py`, `engine/kirk_grok_advisor.py`, `dashboard/app.py`
- **Readers:** `engine/cost_tracker.py`, `dashboard/app.py`
- **Purpose:** Token / dollar accounting per API call. Powers Cost Tracker dashboard panel and the Free Models First doctrine enforcement. Rows split by `call_type` (`scan`, `chat`, `briefing`, etc.). `cost_usd` is computed from token counts × per-model rate at write time.
- **Notes:** Note the `[object Object]` rendering bug in dashboard (fixed 2026-04-23 commit) — `fvp.free.total_pnl` / `fvp.paid.total_pnl` are the canonical readers.

### `model_stats` (data/trader.db) — Tier 1
- **Row count:** 601
- **Schema:**
  ```sql
  CREATE TABLE model_stats (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      model_name TEXT NOT NULL,
      response_time_ms INTEGER,
      tokens_used INTEGER,
      success INTEGER DEFAULT 1,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  ```
- **Purpose:** Per-call latency / success log per model name. Different lens than `api_costs`: this is performance/reliability, that is dollar accounting.

### Tier 2/3 (Scoring & Calibration)
- `model_scores` (3, latest 2026-05-01) — coarse model-vs-model score aggregates.
- `model_adjustments` (12, latest 2026-05-04) — manual model-tier adjustments (lab/prod).
- `model_watchlist` (0) — empty.
- `forecast_scorecards` (15, latest 2026-05-04 20:17) — forecast accuracy aggregates by horizon.
- `trust_scores` (0) — empty.
- `strategy_scores` (0) — empty.
- `strategy_optimization` (0) — empty.
- `bootstrap_metrics` (0) — empty.
- `indicator_benchmarks` (0) — empty.
- `signal_scorecard` (0) — empty (writer never wired; HM-S blocking item).
- `session_grades` (0) — empty.
- `prediction_accuracy` (sigDB, 4) — see signals.db section.

---

## Domain: Debates, Bridge Voting & Crew

### `debate_history_v2` (data/trader.db) — Tier 1
- **Row count:** 209 (latest 2026-05-05 00:28:05)
- **Schema:**
  ```sql
  CREATE TABLE debate_history_v2 (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker TEXT NOT NULL,
      picard_conviction INTEGER,
      picard_decision TEXT,
      picard_synthesis TEXT,
      picard_strongest_bull TEXT,
      picard_strongest_bear TEXT,
      risk_rating TEXT,
      risk_override TEXT,
      adjusted_conviction INTEGER,
      spock_assessment TEXT,
      crusher_assessment TEXT,
      scotty_assessment TEXT,
      bull_avg_conviction REAL,
      bear_avg_conviction REAL,
      agent_count INTEGER,
      stock_data_json TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      plutus_analysis TEXT
  );
  ```
- **Writers:** `engine/debate_engine.py`
- **Readers:** `engine/scenario_modeler.py`, `engine/fast_scanner.py`, `engine/trade_cards_api.py`
- **Purpose:** Per-ticker bull/bear debate verdicts. Picard fields are the synthesizer's output; Spock / Crusher / Scotty cells hold the risk-triad's assessment text. `adjusted_conviction` is the post-risk-override final score. `plutus_analysis` is a late-added column for the Plutus-3B veto.

### `debate_agent_verdicts` (data/trader.db) — Tier 1
- **Row count:** 2,508 (latest 2026-05-05 00:28:05)
- **Schema:**
  ```sql
  CREATE TABLE debate_agent_verdicts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      debate_id INTEGER NOT NULL,
      agent_name TEXT NOT NULL,
      side TEXT NOT NULL,
      lens TEXT NOT NULL,
      model TEXT,
      conviction INTEGER,
      thesis TEXT,
      key_data_point TEXT,
      raw_response TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (debate_id) REFERENCES debate_history_v2(id)
  );
  ```
- **Writers:** `engine/debate_engine.py`
- **Readers:** `engine/trade_cards_api.py`
- **Purpose:** Individual agent contributions to each debate. `side` is `bull`/`bear`; `lens` is the analytical perspective (e.g. `technicals`, `fundamentals`, `flow`, `macro`). `~12` rows per debate (multi-agent).

### `bridge_votes` (data/trader.db) — Tier 1
- **Row count:** 232 (latest 2026-05-05 13:04:17)
- **Schema:**
  ```sql
  CREATE TABLE bridge_votes (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      session_date TEXT    NOT NULL,
      session_time TEXT    NOT NULL,
      player_id    TEXT    NOT NULL,
      player_name  TEXT    NOT NULL,
      vote         TEXT    NOT NULL,   -- BUY | SELL | HOLD
      confidence   INTEGER NOT NULL,   -- 0-100
      reason       TEXT,
      model_used   TEXT,
      created_at   TEXT    NOT NULL DEFAULT (datetime('now'))
  );
  CREATE INDEX idx_bv_date ON bridge_votes (session_date, player_id);
  ```
- **Writers:** `engine/bridge_vote.py`
- **Readers:** `engine/bridge_vote.py`, `engine/morning_briefing.py`, `dashboard/app.py`
- **Purpose:** One row per agent per voting session. Bridge consensus is the daily 09:30 ET vote where all live players cast a single BUY/SELL/HOLD on each ticker; this table is the per-vote ledger. Note: 2026-05-03 fleet reality found bridge collection stalled 13:01 with 216 votes, partially recovered to 232 by 2026-05-05 — indicates intermittent collection.
- **Notes:** Stalled-collection alert is open carry-forward (HM-T-fleet may surface it).

### `bridge_consensus` (data/trader.db) — Tier 1
- **Row count:** 29 (latest 2026-05-05 13:04:17)
- **Schema:**
  ```sql
  CREATE TABLE bridge_consensus (
      id              INTEGER PRIMARY KEY AUTOINCREMENT,
      session_date    TEXT    NOT NULL,
      session_time    TEXT    NOT NULL,
      buy_votes       INTEGER NOT NULL DEFAULT 0,
      sell_votes      INTEGER NOT NULL DEFAULT 0,
      hold_votes      INTEGER NOT NULL DEFAULT 0,
      total_voters    INTEGER NOT NULL DEFAULT 0,
      conviction      TEXT    NOT NULL,  -- HIGH | MODERATE | HOLD
      consensus_vote  TEXT    NOT NULL,  -- BUY | SELL | HOLD
      avg_confidence  INTEGER NOT NULL DEFAULT 0,
      briefing_summary TEXT,
      created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
  );
  ```
- **Writers/Readers:** `engine/bridge_vote.py`, `engine/agent_builder.py`, `dashboard/app.py`
- **Purpose:** Aggregated voting result per session — one row per voting session summarizing the per-vote rows in `bridge_votes`. `consensus_vote` is the majority winner; `conviction` is derived from vote spread.

### `crew_decisions` (data/trader.db) — Tier 1
- **Row count:** 6,894 (latest 2026-05-04 19:46:49)
- **Schema:**
  ```sql
  CREATE TABLE crew_decisions (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      timestamp   TEXT    NOT NULL,
      agent_name  TEXT    NOT NULL,
      player_id   TEXT    NOT NULL,
      action      TEXT    NOT NULL,
      symbol      TEXT,
      confidence  INTEGER DEFAULT 0,
      reason      TEXT,
      market_data TEXT,
      gate_result TEXT,
      executed    INTEGER NOT NULL DEFAULT 0
  );
  ```
- **Writers/Readers:** `engine/crew_scanner.py`, `main.py`
- **Purpose:** Crew-system decision audit log — every crew agent's action with `gate_result` (which gate accepted/rejected) and `executed` flag (whether downstream paper_trader actually fired it). Use to debug "agent decided X but no trade fired" — `gate_result` will name the rejecting gate.

### `rikers_log` (data/trader.db) — Tier 1
- **Row count:** 2,631 (latest 2026-05-05 13:07:51 — XO synthesis fires every 10 min)
- **Schema:**
  ```sql
  CREATE TABLE rikers_log (
      id INTEGER PRIMARY KEY,
      entry_type TEXT NOT NULL DEFAULT 'manual',
      source TEXT NOT NULL DEFAULT 'captain',
      title TEXT,
      content TEXT,
      ticker TEXT,
      action TEXT,
      conviction REAL,
      outcome TEXT,
      outcome_pnl REAL,
      tags TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  ```
- **Writers:** `engine/riker_synthesis.py`, `engine/rikers_log.py`
- **Readers:** `engine/rikers_log.py`
- **Purpose:** XO Riker's running log of synthesis observations and operator/captain notes. `entry_type` distinguishes auto-synthesis from manual entries; `source='captain'` for operator inputs. The 10-min cadence comes from `main.py` scheduler.

### `war_room` (data/trader.db) — Tier 1
- **Row count:** 18,210 (latest 2026-05-05 13:12:45 — high-cadence write path)
- **Schema:**
  ```sql
  CREATE TABLE war_room (
      id INTEGER PRIMARY KEY,
      player_id TEXT NOT NULL REFERENCES ai_players(id),
      symbol TEXT NOT NULL,
      take TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      strategy_mode TEXT
  );
  ```
- **Writers:** `engine/rallies_intel.py`, `engine/strategies.py`, `engine/metals_commentary.py`
- **Readers:** `engine/picard_strategy.py`, `engine/q_entity.py`, `engine/riker_xo.py`, `engine/consensus.py`, `engine/metals_commentary.py`
- **Purpose:** Ticker-level "takes" stream — short blurbs each agent emits per symbol per scan. Picard / Riker-XO synthesize across these for daily briefings.

### `war_room_debates` (data/trader.db) — Tier 1
- **Row count:** 975
- **Schema:**
  ```sql
  CREATE TABLE war_room_debates (
      debate_id        TEXT PRIMARY KEY,
      symbol           TEXT NOT NULL,
      trigger          TEXT NOT NULL,
      started_at       TEXT NOT NULL,
      expected_agents  TEXT NOT NULL DEFAULT '[]',
      completed_agents TEXT NOT NULL DEFAULT '[]',
      status           TEXT NOT NULL DEFAULT 'running',
      finished_at      TEXT
  );
  ```
- **Purpose:** Debate session state machine — tracks which agents were expected to weigh in vs which actually did. `status` transitions running → complete / timeout. Used by orchestrator to detect stalled debates.

### Tier 2/3 (Debates, Bridge Voting & Crew)
- `debate_history` (1, 2026-03-31) — pre-v2 debate format; superseded.
- `crew_runs` (41, latest 2026-04-01) — legacy CrewAI run log.
- `crew_strategies` (31) — CrewAI strategy registry.
- `crew_trade_results` (8) — CrewAI trade results; legacy.
- `pike_votes` (0) — Pike Webull-advisor votes; empty (Pike retired).
- `quorum_votes` (0) — generic quorum scaffolding; empty.
- `risk_assessments` (1, 2026-03-31) — superseded by debate v2 risk fields.
- `captain_decisions` (1, 2026-03-22) — captain veto log; mostly idle.

---

## Domain: Scanners & Discovery

### `fast_scan_results` (data/trader.db) — Tier 1
- **Row count:** 3,376 (latest 2026-05-04 20:04:27)
- **Schema:**
  ```sql
  CREATE TABLE fast_scan_results (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker        TEXT    NOT NULL,
      signal        TEXT    NOT NULL,   -- BUY | SELL | WATCH | HOLD
      confidence    INTEGER,            -- 1-10
      price         REAL,
      thesis        TEXT,
      key_risk      TEXT,
      uoa_summary   TEXT,
      model         TEXT    DEFAULT 'gemma3:4b',
      scan_duration_s REAL,
      created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  ```
- **Writers:** `engine/fast_scanner.py`
- **Readers:** `engine/cash_manager.py`, `scripts/uhura_watch.py`, `dashboard/app.py`
- **Purpose:** Fast-scanner output — local LLM (gemma3:4b by default) scans the full universe for BUY/SELL/WATCH/HOLD per ticker. Confidence is INTEGER 1-10. `scan_duration_s` tracks per-ticker latency.

### `universe_scan` (data/trader.db) — Tier 1
- **Row count:** 2,741 (latest 2026-05-05 04:33:56)
- **Schema:**
  ```sql
  CREATE TABLE universe_scan (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      scan_date DATE NOT NULL,
      ticker TEXT NOT NULL,
      close REAL,
      volume INTEGER,
      volume_ratio REAL,
      rsi REAL,
      score INTEGER,
      signals TEXT,
      gap_pct REAL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(scan_date, ticker)
  );
  ```
- **Purpose:** Daily universe-wide screener output — one row per ticker per scan_date. UNIQUE constraint enforces idempotency. `signals` is a comma-joined string of fired sub-signals (e.g. `RSI_OVERSOLD,VOL_SPIKE`).

### `universe_stocks` (data/trader.db) — Tier 1
- **Row count:** 10,820
- **Schema:** `(symbol PRIMARY KEY, name, exchange, updated_at)`
- **Purpose:** Master universe — all symbols tracked. Refreshed periodically from broker / list import. Joined into scanners that need name/exchange context.

### Tier 2/3 (Scanners & Discovery)
- `scan_universe` (2,741) — alternate / older universe-scan table; same shape as `universe_scan`. Verify which is canonical before writing.
- `gap_scanner` (263) — overnight gap watchlist.
- `premarket_scan` (224) — pre-market screener output.
- `deep_scan_results` (490, latest 2026-04-29) — deep-dive scan results.
- `whale_detections` (77) — whale-trade detector hits.
- `imbalance_zones` (962) — order-imbalance zones (smart-money detection).
- `volatility_breakouts` (4,266) — opening-range / volatility breakout log.
- `volume_alerts` (336,738) — every volume-spike row; high cardinality.
- `volume_baselines` (10,547) — 20-day avg volume per symbol.
- `volume_daily_log` (32,716) — daily volume history.
- `impulse_alerts` (143, latest 2026-05-04 19:30) — momentum impulse detector hits.
- `patterns_tracked` (351) — chart-pattern detector tracking.
- `market_patterns` (20, latest 2026-04-01) — market-wide pattern hits; idle.
- `gex_strikes` (0) — empty per-strike GEX.

---

## Domain: Market Context (Regimes, Breadth, Sectors)

### `regime_history` (data/trader.db) — Tier 1
- **Row count:** 151 (latest 2026-05-05 13:12:45)
- **Schema:**
  ```sql
  CREATE TABLE regime_history (
      id INTEGER PRIMARY KEY,
      date TEXT NOT NULL UNIQUE,
      spy_close REAL,
      ma_8 REAL,
      ma_21 REAL,
      qqq_close REAL,
      qqq_ma_8 REAL,
      qqq_ma_21 REAL,
      regime TEXT NOT NULL,
      cross_date TEXT,
      cross_days_ago INTEGER,
      size_modifier REAL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  CREATE INDEX idx_regime_history_date ON regime_history(date);
  ```
- **Writers:** `engine/regime_ma.py`
- **Readers:** `engine/self_improvement.py`, `engine/trade_outcomes.py`, `engine/morning_briefing.py`
- **Purpose:** Daily regime classification (BULL / NEUTRAL / BEAR / CAUTIOUS) based on SPY/QQQ vs 8 & 21 day MAs. `cross_date` is the most-recent MA-cross date; `cross_days_ago` is the staleness counter; `size_modifier` is the regime-aware position-size scalar (1.0 normal, 0.5 cautious, etc.).

### `market_snapshots` (data/trader.db) — Tier 1
- **Row count:** 4,110 (latest 2026-05-03 15:01:56)
- **Schema:**
  ```sql
  CREATE TABLE market_snapshots (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      symbol      TEXT NOT NULL,
      date        TEXT NOT NULL,
      open REAL, high REAL, low REAL, close REAL, volume INTEGER,
      vwap REAL, change_pct REAL,
      spy_close REAL, qqq_close REAL, vix_close REAL,
      source      TEXT DEFAULT 'alpaca',
      created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(symbol, date)
  );
  ```
- **Purpose:** Daily OHLCV snapshot per symbol with SPY/QQQ/VIX context columns embedded for fast regime joins.

### `breadth_snapshots` (data/trader.db) — Tier 1
- **Row count:** 11,351 (latest 2026-05-05 13:06:17)
- **Schema:**
  ```sql
  CREATE TABLE breadth_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      trade_date TEXT NOT NULL,
      snap_time TEXT NOT NULL,
      breadth_score REAL,
      adv_count INTEGER,
      dec_count INTEGER,
      adv_decline_ratio REAL,
      spy_rsp_divergence REAL,
      iwm_confirmation TEXT,
      sector_leader TEXT,
      sector_laggard TEXT,
      rotation_type TEXT,
      sector_data_json TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );
  ```
- **Purpose:** Intra-day breadth panel — one row per scan tick. `spy_rsp_divergence` flags equal-weight vs cap-weight divergence (mega-cap concentration signal). `iwm_confirmation` reports small-cap follow-through. Drives the breadth widget on the dashboard.

### `sector_snapshots` (data/trader.db) — Tier 1
- **Row count:** 6,613 (latest 2026-05-05 13:06:21)
- **Schema:**
  ```sql
  CREATE TABLE sector_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      trade_date TEXT NOT NULL,
      snap_time TEXT NOT NULL,
      rotation_type TEXT,
      sector_leader TEXT,
      sector_laggard TEXT,
      spy_pct_change REAL,
      sector_data_json TEXT,
      momentum_5d_json TEXT,
      rotation_signal TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );
  ```
- **Purpose:** Sector-rotation snapshot per scan tick. `sector_data_json` holds the full XL* matrix; `rotation_signal` is the categorical recommendation (RISK_ON / RISK_OFF / CYCLICAL_LEADERSHIP / etc.).

### `correlation_snapshots` (data/trader.db) — Tier 1
- **Row count:** 6,441 (latest 2026-05-05 13:06:21)
- **Schema:**
  ```sql
  CREATE TABLE correlation_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      trade_date TEXT NOT NULL,
      snap_time TEXT NOT NULL,
      spy_pct REAL,
      tlt_pct REAL,
      gld_pct REAL,
      uup_pct REAL,
      hyg_pct REAL,
      alignment_score REAL,
      risk_mode TEXT,
      divergence_flags_json TEXT,
      signal TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );
  ```
- **Purpose:** Cross-asset alignment tracker — SPY vs TLT/GLD/UUP/HYG correlation regime. `risk_mode` is RISK_ON / RISK_OFF / DIVERGENT. `alignment_score` is the bundled correlation strength. Drives macro-divergence signals.

### Tier 2/3 (Market Context)
- `sector_rotation` (170, latest 2026-05-05 02:00:45) — daily-cadence sector rotation rankings (different from sector_snapshots which is intraday).
- `correlation_pairs` (38) — manually curated correlation-substitute map (e.g. SOXL ↔ SOXX).
- `market_events` (857, latest 2026-05-05 13:06:20) — economic calendar / earnings / FOMC events with impact ratings.
- `iv_history` (90) — IV history for tracked symbols (Day-N verification target per ops carry-forward).
- `morning_levels` (27, latest 2026-05-05 13:00:01) — morning support/resistance levels per symbol.
- `var_snapshots` (295, latest 2026-05-05 12:53:28) — daily VaR-95/99 portfolio risk snapshots (parametric + historical methods).

---

## Domain: Options Flow & Greeks (UOA, GEX)

### `uoa_alerts` (data/trader.db) — Tier 1
- **Row count:** 7,879 (latest 2026-05-04 13:17:12)
- **Schema:**
  ```sql
  CREATE TABLE uoa_alerts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      alert_date TEXT NOT NULL,
      alert_time TEXT NOT NULL,
      ticker TEXT NOT NULL,
      alert_type TEXT NOT NULL,    -- 'VOL_SPIKE', 'BIG_PREMIUM', 'PUT_WALL', 'CALL_SWEEP', 'SMART_MONEY'
      severity TEXT NOT NULL,      -- 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
      contract_type TEXT,
      strike REAL,
      expiration TEXT,
      vol_oi_ratio REAL,
      premium_total REAL,
      underlying_price REAL,
      description TEXT,
      chekov_match INTEGER DEFAULT 0,
      convergence_score REAL DEFAULT 0,
      acted_on INTEGER DEFAULT 0,
      outcome TEXT,
      outcome_pnl REAL,
      created_at TEXT DEFAULT (datetime('now'))
  );
  ```
- **Readers:** `engine/fast_scanner.py`, `engine/trade_cards_api.py`
- **Purpose:** Unusual-options-activity alert log. Categorical `alert_type` taxonomy. `chekov_match` flags whether the ticker is on Chekov's watchlist. `convergence_score` is the multi-signal alignment score (0-100); higher means more cross-signal confirmation.
- **Notes:** Symbol column is `ticker`. Writer not visible via simple grep — likely emitted by an external scanner script or scheduled task; investigate before relying on freshness.

### `uoa_flow` (data/trader.db) — Tier 1
- **Row count:** 7,832 (latest 2026-05-04 13:17:12)
- **Schema:**
  ```sql
  CREATE TABLE uoa_flow (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      scan_date TEXT NOT NULL,
      scan_time TEXT NOT NULL,
      ticker TEXT NOT NULL,
      contract_type TEXT NOT NULL,    -- 'CALL' or 'PUT'
      strike REAL NOT NULL,
      expiration TEXT NOT NULL,
      dte INTEGER,
      volume INTEGER, open_interest INTEGER, vol_oi_ratio REAL,
      last_price REAL, bid REAL, ask REAL,
      implied_volatility REAL, premium_total REAL,
      moneyness TEXT, underlying_price REAL, pct_otm REAL,
      sentiment TEXT,    -- 'BULLISH', 'BEARISH', 'NEUTRAL'
      source TEXT,       -- 'yfinance', 'barchart', 'cboe'
      created_at TEXT DEFAULT (datetime('now')),
      UNIQUE(scan_date, ticker, contract_type, strike, expiration)
  );
  ```
- **Readers:** `engine/trade_cards_api.py`
- **Purpose:** Per-contract UOA scan output — full strike-level options snapshot. `vol_oi_ratio > 1.0` is the unusual-activity threshold most readers use. `sentiment` is derived from delta and OI direction.
- **Notes:** Symbol column is `ticker`.

### `gex_levels` (data/trader.db) — Tier 1
- **Row count:** 841
- **Schema:**
  ```sql
  CREATE TABLE gex_levels (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      symbol TEXT NOT NULL,
      calc_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      spot_price REAL,
      king_node REAL,
      gamma_flip REAL,
      put_wall REAL,
      call_wall REAL,
      gamma_walls_above TEXT,
      gamma_walls_below TEXT,
      total_gex REAL,
      regime TEXT,
      composite_score REAL,
      composite_signal TEXT,
      composite_strength TEXT,
      ticker TEXT
  );
  ```
- **Purpose:** Per-symbol GEX (gamma exposure) calculation snapshots. `king_node` is the largest gamma cluster strike; `gamma_flip` is the zero-gamma level. `regime` categorizes positive-gamma (mean-reverting) vs negative-gamma (trending) market state.

### `gex_snapshots` (data/trader.db) — Tier 1
- **Row count:** 946 (latest 2026-05-05 12:57:34)
- **Schema:**
  ```sql
  CREATE TABLE gex_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      symbol TEXT NOT NULL,
      timestamp TEXT NOT NULL,
      spot_price REAL,
      max_gamma_strike REAL,
      zero_gamma_level REAL,
      put_wall REAL,
      call_wall REAL,
      gamma_flip REAL,
      total_gex REAL,
      levels_json TEXT,
      source TEXT DEFAULT 'alpaca',
      created_at TEXT DEFAULT (datetime('now'))
  );
  ```
- **Purpose:** Newer alpaca-sourced GEX snapshot table; running parallel to `gex_levels`. Verify which is canonical for any given consumer — the dual-table state is unresolved technical debt.

### `flow_lean_history` (data/trader.db) — Tier 1
- **Row count:** 2,294 (latest 2026-05-05 12:58:00)
- **Schema:**
  ```sql
  CREATE TABLE flow_lean_history (
      id INTEGER PRIMARY KEY,
      lean TEXT NOT NULL,
      conviction REAL NOT NULL,
      net_flow REAL NOT NULL,
      total_call_premium REAL NOT NULL,
      total_put_premium REAL NOT NULL,
      fresh_cb_call REAL NOT NULL DEFAULT 0,
      fresh_cb_put REAL NOT NULL DEFAULT 0,
      symbols_scanned INTEGER NOT NULL,
      details TEXT,
      recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  ```
- **Writers/Readers:** `engine/market_flow.py`
- **Readers (extra):** `crew/agents.py`
- **Purpose:** Market-wide options flow lean per scan tick. `lean` is BULLISH/BEARISH/NEUTRAL; `conviction` is 0-1 scaled. `fresh_cb_call`/`fresh_cb_put` track newly-detected call/put cluster bets. Used by macro/regime crews and the morning briefing.

### Tier 2/3 (Options Flow & Greeks)
- `uoa_daily_summary` (252, latest 2026-05-04 13:17:12) — daily aggregated UOA per ticker.
- `uoa_scan_log` (7) — UOA scanner audit log.
- `options_books` (2, latest 2026-04-21) — options book registry; legacy.
- `orcl_gex_alerts` (0) — empty (legacy ORCL-specific GEX).
- `theta_opportunities` (0) — theta-decay candidate scanner; never wired.
- `options_flow_history` (0) — empty.
- `bookoff_options` not present — **note**: 8 orphaned options strategies in `engine/options_agents.py` have **no main.py refs** per fleet reality 2026-05-03 (Phase 4 backlog).

---

## Domain: News, Sentiment & External Intel

### `news_pulse` (data/trader.db) — Tier 1
- **Row count:** 660 (latest 2026-05-05 12:53:36)
- **Schema:**
  ```sql
  CREATE TABLE news_pulse (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      trade_date TEXT NOT NULL,
      mood_score REAL,
      headline_count INTEGER,
      bullish_count INTEGER,
      bearish_count INTEGER,
      neutral_count INTEGER,
      top_themes_json TEXT,
      convergence_signal TEXT,
      news_summary TEXT,
      fetched_at TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );
  ```
- **Purpose:** Aggregated daily news mood — sentiment counts + thematic clusters via FinGPT pipeline. `convergence_signal` flags when bullish/bearish sentiment converges with technical signals.

### `insider_trades` (data/trader.db) — Tier 1
- **Row count:** 87 (latest 2026-05-05 12:30:30)
- **Schema:**
  ```sql
  CREATE TABLE insider_trades (
      id              INTEGER PRIMARY KEY AUTOINCREMENT,
      symbol          TEXT NOT NULL,
      insider_name    TEXT,
      title           TEXT,
      transaction_type TEXT,
      shares          INTEGER,
      price_per_share REAL,
      total_value     REAL,
      filing_date     TEXT,
      transaction_date TEXT,
      form_type       TEXT DEFAULT '4',
      source_url      TEXT,
      created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(symbol, insider_name, transaction_date, transaction_type)
  );
  ```
- **Writers:** `engine/data_ingestion.py`, `agents/uhura_agent.py`
- **Readers:** `engine/morning_briefing.py`, `dashboard/app.py`, `agents/uhura_agent.py`
- **Purpose:** SEC Form 4 insider buy/sell filings. Uhura (Lt. Uhura SEC EDGAR agent) is the primary writer; UNIQUE constraint prevents duplicate filing rows.

### `institutional_holdings` (data/trader.db) — Tier 1
- **Row count:** 379,142 (latest 2026-05-05 12:30:14 — largest table)
- **Schema:**
  ```sql
  CREATE TABLE institutional_holdings (
      id              INTEGER PRIMARY KEY AUTOINCREMENT,
      fund_name       TEXT NOT NULL,
      fund_cik        TEXT,
      ticker          TEXT,
      cusip           TEXT,
      shares          INTEGER,
      value_usd       INTEGER,
      period_of_report TEXT,
      filed_at        TEXT,
      created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  ```
- **Writers/Readers:** `agents/uhura_agent.py`, `dashboard/app.py`
- **Purpose:** SEC 13F institutional holdings — every quarterly filing row by every fund. Uhura ingests Form 13F to drive the institutional veto signal.
- **Notes:** Symbol column is `ticker`. Largest table by row count (379k). Joins to `ai_players` only via Uhura's downstream signal generation.

### `institutional_signals` (data/trader.db) — Tier 1
- **Row count:** 242 (latest 2026-05-05 12:31:55)
- **Schema:**
  ```sql
  CREATE TABLE institutional_signals (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker      TEXT NOT NULL,
      signal      TEXT NOT NULL,
      reasoning   TEXT,
      scan_date   TEXT NOT NULL,
      created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  ```
- **Writers/Readers:** `agents/uhura_agent.py`, `engine/archer_morning_synthesis.py`, `engine/uhura_bridge_integration.py`, `dashboard/app.py`
- **Purpose:** Aggregated institutional-level BUY/SELL/AVOID signals derived from 13F deltas. Uhura's veto layer reads this — when institutional `signal='AVOID'`, Active 4 entry signals get blocked.

### `aladdin_signals` (data/trader.db) — Tier 1
- **Row count:** 2,490 (latest 2026-05-05 03:09:40)
- **Schema:**
  ```sql
  CREATE TABLE aladdin_signals (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      timestamp   TEXT NOT NULL,
      source      TEXT NOT NULL,
      signal      TEXT NOT NULL,
      confidence  INTEGER NOT NULL DEFAULT 50,
      raw_data    TEXT,
      notes       TEXT
  );
  ```
- **Writers:** `agents/aladdin.py`
- **Readers:** `dashboard/app.py`
- **Purpose:** BlackRock iShares ETF flow signals. `source` discriminates inflow/outflow/sector lean. Currently rule-based agent (no LLM).

### `aladdin_holdings` (data/trader.db) — Tier 1
- **Row count:** 1,505
- **Schema:**
  ```sql
  CREATE TABLE aladdin_holdings (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      etf_ticker    TEXT NOT NULL,
      holding_ticker TEXT NOT NULL,
      weight        REAL,
      shares        REAL,
      market_value  REAL,
      date          TEXT NOT NULL
  );
  CREATE INDEX ix_aladdin_holdings_etf_date ON aladdin_holdings(etf_ticker, date);
  ```
- **Purpose:** Per-ETF holdings snapshot history. Aladdin uses these to compute concentration / rotation signals.

### `market_news` (data/trader.db) — Tier 1
- **Row count:** 11,121
- **Schema:** `(id, symbol, headline, summary, source, url, sentiment, fetched_at)`
- **Purpose:** Raw news feed per symbol. `sentiment` is FinGPT-tagged. Source for `news_pulse` aggregation.

### `ship_computer_alerts` (data/trader.db) — Tier 1
- **Row count:** 6,811 (latest 2026-05-05 13:09:44)
- **Schema:**
  ```sql
  CREATE TABLE ship_computer_alerts (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      alert_type  TEXT NOT NULL,
      symbol      TEXT NOT NULL,
      source      TEXT NOT NULL DEFAULT 'ship_computer',
      message     TEXT,
      detail      TEXT,
      created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      seen        INTEGER DEFAULT 0,
      seen_at     TIMESTAMP
  );
  ```
- **Writers/Readers:** `engine/portfolio_monitor.py`
- **Purpose:** Portfolio-monitor alert sink — ship-computer (the dashboard nervous system) writes alerts here, flips `seen=1` when an operator surface acks.

### Tier 2/3 (News, Sentiment & External Intel)
- `news_impact` (0) — empty.
- `notifications` (33, latest 2026-04-30) — generic ntfy notification log.
- `archer_briefings` (21, latest 2026-05-04 13:25) — Archer agent's daily briefings.
- `cto_briefings` (20, latest 2026-04-24) — CTO-style briefing log.
- `picard_briefings` (3) — Picard weekly thesis briefing log.
- `ready_room_briefings` (65, latest 2026-05-05 12:57:35) — pre-market ready-room sessions with GEX/PC ratio/VIX context.

### `ready_room_briefings` (data/trader.db) — Tier 1
- **Row count:** 65 (latest 2026-05-05 12:57:35)
- **Schema:**
  ```sql
  CREATE TABLE ready_room_briefings (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      symbol        TEXT    NOT NULL DEFAULT 'SPY',
      session_date  TEXT    NOT NULL,
      session_time  TEXT    NOT NULL,
      spot_price    REAL,
      call_wall     REAL,
      put_wall      REAL,
      max_pain      REAL,
      gamma_flip    REAL,
      max_gamma_strike REAL,
      total_gex     REAL,
      pc_ratio      REAL,
      vix           REAL,
      session_type  TEXT,
      signals_json  TEXT,
      gameplan      TEXT,
      created_at    TEXT DEFAULT (datetime('now'))
  );
  ```
- **Writers:** `engine/ready_room.py`
- **Readers:** `engine/ready_room_health.py`, `engine/ready_room.py`, `engine/holodeck_readyroom.py`, `engine/news_pulse.py`, `engine/eod_scorecard.py`
- **Purpose:** Pre-market and intraday ready-room session digests. `gameplan` is a free-text trading plan; `signals_json` is the snapshot of active signals at session time. Drives the Ready Room dashboard panel and EOD scorecard.

---

## Domain: Portfolio & Risk

### `portfolio_history` (data/trader.db) — Tier 1
- **Row count:** 3,598 (latest 2026-05-05 13:02:41 — drawdown gate reads this)
- **Schema:**
  ```sql
  CREATE TABLE portfolio_history (
      id INTEGER PRIMARY KEY,
      player_id TEXT NOT NULL REFERENCES ai_players(id),
      total_value REAL,
      cash REAL,
      positions_value REAL,
      recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      season INTEGER DEFAULT 1
  );
  CREATE INDEX idx_portfolio_history_season ON portfolio_history(season, recorded_at);
  CREATE INDEX idx_portfolio_history_player ON portfolio_history(player_id, season, recorded_at);
  ```
- **Writers:** `engine/paper_trader.py`, `scripts/recapitalize_player.py`, `shared/matrix_bridge.py`
- **Readers:** `engine/risk_manager.py` (drawdown gate), `engine/rallies_intel.py`, `engine/cost_tracker.py`, `engine/morning_briefing.py`, `engine/paper_trader.py`
- **Purpose:** Per-player equity curve. **Source of truth for the 20% drawdown auto-halt** (`engine/risk_manager.py::check_drawdown()` reads peak vs current here every cycle, called from `engine/ai_brain.py:817`).
- **Notes:** ⚠️ Note column is `recorded_at` not `created_at`. Indexed on `(season, recorded_at)` for fast window queries. Manual recapitalization (`scripts/recapitalize_player.py`) writes a peak-reset row.

### `portfolio_positions` (data/trader.db) — Tier 1
- **Row count:** 12 (latest 2026-04-20)
- **Schema:** richer position table with sub-portfolio support; check live writers before relying on freshness.
- **Notes:** Distinct from `positions` (which is the live AI fleet open positions). `portfolio_positions` is the human/sub-portfolio variant.

### `portfolios` (data/trader.db) — Tier 1
- **Row count:** 8 (latest 2026-04-20)
- **Purpose:** Sub-portfolio registry — distinct named portfolios (e.g. McCoy book, Capitol book) for partitioned accounting.

### `var_snapshots` (data/trader.db) — Tier 1
- **Row count:** 295 (latest 2026-05-05 12:53:28)
- **Schema:**
  ```sql
  CREATE TABLE var_snapshots (
      id              INTEGER PRIMARY KEY AUTOINCREMENT,
      portfolio_value REAL    NOT NULL,
      var_95_param    REAL,
      var_99_param    REAL,
      var_95_hist     REAL,
      var_99_hist     REAL,
      daily_vol_pct   REAL,
      position_count  INTEGER,
      top_risk_ticker TEXT,
      snapshot_date   TEXT    DEFAULT (date('now')),
      created_at      TEXT    DEFAULT (datetime('now'))
  );
  ```
- **Writers/Readers:** `engine/risk_var.py`, `engine/morning_briefing.py`
- **Purpose:** Daily Value-at-Risk snapshots — both parametric (normal-dist assumption) and historical (empirical) at 95/99%. Dashboard risk page reads `top_risk_ticker` for concentration warnings.

### Tier 2/3 (Portfolio & Risk)
- `cash_sweeps` (44, latest 2026-05-05 08:35:43) — daily cash-sweep ledger.
- `cash_manager_settings` (0) — empty.
- `sub_portfolios` (4, latest 2026-04-06) — sub-portfolio registry.
- `risk_alerts` (13, latest 2026-04-30) — discrete risk alerts.
- `portfolio_advice` (121, latest 2026-05-01) — Kirk-style portfolio advisor output.
- `portfolio_optimizations` (16, latest 2026-05-05 00:28:37) — portfolio optimization runs.
- `rebalance_recommendations` (80, latest 2026-05-05 00:28:42) — rebalance candidates.
- `rebalance_log` (0) — empty.
- `rebalance_targets` (0) — empty.
- `tax_harvester_settings` (0) — empty.
- `tax_harvests` (0) — empty.
- `wash_sale_log` (0) — empty.
- `trade_block_log` (1) — trade-block audit log.
- `scenario_models` (149, latest 2026-05-05 00:28:28) — scenario / what-if portfolio models.

---

## Domain: Backtest & Reference Data

### `reference_trades` (data/trader.db) — Tier 1
- **Row count:** 2,022 (latest imported 2026-05-05 03:30:02)
- **Schema:**
  ```sql
  CREATE TABLE reference_trades (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source TEXT DEFAULT 'rallies.ai',
      model_name TEXT,
      symbol TEXT,
      action TEXT,
      price REAL,
      qty REAL,
      reasoning TEXT,
      confidence REAL,
      outcome TEXT,
      pnl REAL,
      pnl_pct REAL,
      regime TEXT,
      traded_at TEXT,
      imported_at TEXT DEFAULT (datetime('now'))
  );
  ```
- **Writers:** `engine/rallies_parser.py`, `engine/reference_data.py`, `engine/importers/ai4trade_importer.py`
- **Readers:** `engine/rallies_parser.py`, `engine/importers/ai4trade_importer.py`, `scripts/learning/extract_corpus.py`, `engine/reference_data.py`, `crew/agents.py`
- **Purpose:** External reference trade log — imports rallies.ai and ai4trade reference outcomes used for benchmarking and corpus building. `source` flags origin; `model_name` identifies the upstream agent that called the trade.

### Tier 2/3 (Backtest & Reference Data)
- `backtest_history` (278) — historical backtest runs.
- `backtest_runs` (36, latest 2026-04-20) — backtest run metadata.
- `backtest_market_data` (1,888) — backtest price data cache.
- `backtest_results` (885) — backtest outcome results.
- `community_backtests` (2) — community-supplied backtests.
- `holodeck_backtest_results` (0) — empty (Holodeck simulator scaffolding).
- `strategy_backtests` (349, latest 2026-05-04 05:36:48) — per-strategy backtest results.
- `reference_discussions` (1) — reference discussion log.
- `bakeoff_runs` (1) / `bakeoff_trades` (0) — agent bake-off scaffolding.

---

## Domain: Crew & Specialty Agents

### `ai_chat` (data/trader.db) — Tier 1
- **Row count:** 950 (latest 2026-05-05 12:51:50)
- **Schema:** `(id, player_id, message, context, reply_to, created_at)`
- **Writers:** `engine/ai_chat.py`, `shared/matrix_bridge.py`
- **Readers:** `engine/ai_chat.py`, `dashboard/app.py`, `crew/routes.py`
- **Purpose:** Inter-agent chat log — agents leave messages for each other (or for the operator). `reply_to` foreign-keys back into the same table for threading. Surface: dashboard chat panel.

### `ai_journal` (data/trader.db) — Tier 1
- **Row count:** 598 (latest 2026-05-05 12:53:38)
- **Schema:** `(id, player_id, entry, created_at)`
- **Writers:** `engine/leader_signal.py`, `engine/ai_journal.py`
- **Readers:** `engine/ai_journal.py`
- **Purpose:** Per-agent free-form journal — daily reflection / thesis notes. Read by leader_signal to bias signal weighting toward agents whose recent journals show consistent thesis.

### `computer_chat_history` (data/trader.db) — Tier 1
- **Row count:** 1,135 (latest 2026-05-05 13:00:27)
- **Purpose:** Operator ↔ ship-computer chat log (the dashboard chat surface).

### Tier 2/3 (Crew & Specialty Agents)
- `janeway_signals` (32, latest 2026-05-02 05:45) — Elder Council Janeway 10-yr signals.
- `sarek_signals` (33, latest 2026-05-05 05:45) — Elder Council Sarek 5-yr signals.
- `surak_signals` (60, latest 2026-05-05 05:45) — Elder Council Surak 20-yr signals.
- `kirk_signals` (0) — empty (Kirk swing-desk retired 2026-05-04).
- `ollie_decisions` (54) — Ollie quality-gate decisions.
- `ollie_performance` (30, latest 2026-05-04 13:46) — Ollie performance log.
- `ollie_super_trades` (26, latest 2026-04-30) — high-conviction "super trade" log.
- `metals_ledger` (6) — metals/commodities position ledger.
- `daily_lessons` (18, latest 2026-05-01) — distilled daily lessons (write-back from EOD reviews).
- `season_history` (10) — season metadata (S1-S6.3+).
- `season_config` (1, latest 2026-04-10) — current season configuration row.
- `agent_memory` (15, latest 2026-04-30) — per-agent persistent memory.
- `pipeline_runs` (16, latest 2026-05-05 00:28:42) — pipeline execution log.
- `strategy_rotation` (315, latest 2026-05-04 21:38) — strategy rotation tracking.
- `strategies` (3, latest 2026-05-01) — registered strategy list.
- `external_picks` (4) — external operator picks.
- `weekly_picks` (40) — weekly pick log.

---

## Domain: Ghost Portfolio System

These tables form the ghost-trading subsystem (paper-trade overlay distinct from the AI fleet's `trades`/`positions`).

### Tier 2/3 (Ghost Portfolio System)
- `ghost_portfolio` (14) — ghost open positions.
- `ghost_trades` (9, latest 2026-05-01 11:50) — ghost trade ledger.
- `ghost_seed` (14) — ghost seed positions / starting state.
- `ghost_cash` (1) — ghost cash balance.
- `ghost_advisor_state` (4) — ghost advisor state machine.
- `ghost_cooldowns` (4) — ghost cooldown counters.
- `ghost_options_watch` (1, 2026-05-01) — ghost options watchlist.
- `ghost_equity_history` (2) — ghost equity curve.
- `ghost_ticker_cache` (18) — ghost ticker price cache.

---

## Domain: Battle Station & Day Blade

### Tier 2/3
- `battle_station_log` (28, latest 2026-04-02 08:24:54) — battle-station activity log; **dormant** per fleet reality 2026-05-03 (launchd feeders missing).
- `battle_station_trades` (1, 2026-04-27) — battle-station trade log; idle.
- (DayBlade has no dedicated table — emits to `signals` / `trades` like other agents. T'Pol/plutus is `is_halted=0` but emitted no signals since 2026-04-07; `dayblade-sulu` is halted since 2026-03-31.)

---

## Domain: Rallies / Reference / Misc

### Tier 2/3
- `rallies_models` (8) — rallies.ai model registry.
- `rallies_alerts` (6, latest 2026-03-21) — legacy rallies alert log.
- `rallies_trades` (8, imported 2026-03-21) — imported rallies trades.
- `scotty_first_seen` (8) — first-seen timestamp tracker (Scotty agent).
- `scotty_watchlist` (13, latest 2026-05-04 22:30:42) — Scotty's tracked symbols.
- `watchlist` (19) — generic dashboard watchlist.
- `webull_watchlist` (2) — pre-wind-down Webull watchlist.
- `short_watchlist` (0) — empty.
- `stock_fundamentals` (122) — fundamentals snapshot cache.
- `danelfin_scores` (42, latest 2026-05-04 03:00) — Danelfin quant score cache.
- `sma_signals` (30) — SMA-cross signal log.
- `smart_money_signals` (14) — institutional flow synthesis hits.

---

## Domain: Settings & Operational

### Tier 2/3
- `settings` (17) — generic key-value config store.
- `system_settings` (1) — global single-row settings.
- `session_fingerprints` (26, latest 2026-05-05 12:15:32) — session fingerprint dedup.
- `player_benchmark_cycles` (1) — benchmark cycle tracking.
- `player_funding_events` (1) — funding/recapitalization event log.
- `gemini_failover` (0) — empty (Gemini failover scaffolding).
- `generated_indexes` (0) — empty.
- `adaptive_rules` (15, latest 2026-05-03 23:00:01) — adaptive trading rules.
- `adaptive_weights` (0) — empty.
- `trade_explanations` (0) — empty.
- `trust_scores` (0) — empty.
- `holly_deepdives` (10, latest 2026-04-21) — Holly intraday deep-dives.
- `earnings_impact` (0) — empty (earnings event impact scaffolding).
- `earnings_universe` (0) — DEPRECATED orphan per HM-AR audit 2026-05-07. Writer (`engine/earnings_injector.py:78`) and reader (`get_active_earnings_universe()`) exist but are uncalled by any other module; no launchd/cron entry runs the script. Options-blackout enforcement uses an independent path (`engine/options_selector.py::_next_earnings_date` → `data/earnings_cache.json` + yfinance). The misnamed `main.py:679 run_earnings_universe_inject()` writes to `scan_universe`, not this table. Full audit: `docs/EARNINGS.md`. Retirement queued as HM-AR-β.

---

## signal-center/signals.db (Separate DB, 11 tables)

### `trade_signals` (signal-center/signals.db) — Tier 1
- **Row count:** 1,151 (1:1 with `signal_outcomes`)
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

### `signal_outcomes` (signal-center/signals.db) — Tier 1
- **Row count:** 1,151 (1:1 with `trade_signals`)
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

### `base_rate_features` (signal-center/signals.db) — Tier 1
- **Row count:** 205,348 (largest table in signals.db)
- **Schema:**
  ```sql
  CREATE TABLE base_rate_features (
      symbol         TEXT NOT NULL,
      date           TEXT NOT NULL,
      close          REAL,
      pct_change     REAL,
      rsi14          REAL,
      rsi_slope      REAL,
      vix_close      REAL,
      vix_pct_change REAL,
      spy_above_200  INTEGER,
      fwd_5d_return  REAL,
      fwd_5d_maxdd   REAL,
      b_move INTEGER, b_rsi INTEGER, b_rsi_slope INTEGER,
      b_vix INTEGER, b_vix_move INTEGER, b_trend INTEGER,
      PRIMARY KEY (symbol, date)
  );
  CREATE INDEX idx_br_date ON base_rate_features(date);
  CREATE INDEX idx_br_match ON base_rate_features(b_move, b_rsi, b_rsi_slope, b_vix, b_vix_move, b_trend);
  ```
- **Purpose:** Feature corpus for base-rate matching — every (symbol, date) pair gets pre-bucketed feature vector (`b_*` columns) for O(log n) similarity lookup. `fwd_5d_return` and `fwd_5d_maxdd` are the labels used to compute conditional probabilities. Big indexed table powering the base-rate scoring system.

### `signal_history` (signal-center/signals.db) — Tier 1
- **Row count:** 79,805
- **Schema:**
  ```sql
  CREATE TABLE signal_history (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      timestamp   TEXT NOT NULL,
      signal_name TEXT NOT NULL,
      value       TEXT,
      score       INTEGER,
      grade       TEXT,
      raw_data    TEXT,
      source      TEXT DEFAULT 'bridge'
  );
  CREATE INDEX idx_signal_time ON signal_history(timestamp);
  CREATE INDEX idx_signal_name ON signal_history(signal_name);
  ```
- **Purpose:** All bridge-emitted signal observations across time. `signal_name` is the source signal (e.g. `vix_regime`, `breadth`, `options_flow`). Used for retrospective scoring and `prediction_accuracy` updates.

### `intelligence_feed` (signal-center/signals.db) — Tier 1
- **Row count:** 16,547
- **Schema:**
  ```sql
  CREATE TABLE intelligence_feed (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      feed_type  TEXT NOT NULL,
      data       TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  CREATE INDEX idx_feed_type_ts ON intelligence_feed(feed_type, created_at);
  ```
- **Purpose:** Generic time-series feed of intelligence events (news pulses, scanner hits, regime updates). `feed_type` discriminates; `data` is JSON.

### `predictions` (signal-center/signals.db) — Tier 1
- **Row count:** 750
- **Schema:**
  ```sql
  CREATE TABLE predictions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      snap_date TEXT NOT NULL,
      symbol TEXT NOT NULL,
      price_at REAL,
      master_score INTEGER,
      regime TEXT,
      recommendation TEXT,
      tp1 REAL, tp2 REAL, stop_loss REAL, rr REAL,
      signal_json TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
  );
  ```
- **Purpose:** Signal-Center-issued predictions per (snap_date, symbol). `master_score` is the composite grade; `recommendation` is the categorical action. `signal_json` blob holds the full feature snapshot.

### Tier 2/3 (signal-center/signals.db)
- `prediction_results` (73) — prediction outcome tracking (TP1/TP2/SL hit flags).
- `prediction_accuracy` (4) — period-level accuracy aggregates.
- `base_rate_ingest_log` (49) — base-rate ingestion bookkeeping.
- `daily_snapshot` (0) — empty (master daily snapshot scaffolding).
- `execution_log` (0) — empty (Alpaca execution log; not wired).

---

## What's NOT in this doc (deliberately)

- Per-table indexes beyond what's shown above — run `sqlite3 data/trader.db ".indexes <tablename>"` for a full list.
- Migration history — see `setup_db.py` and the various `_archive` migration scripts.
- Field-level value distributions — beyond row counts, no cardinality / null-rate stats here. Run `SELECT COUNT(DISTINCT col) ...` if needed.
- The `_archive/` table variants (none of the snapshotted tables in this doc have archive twins; the archive lives on disk under `engine/_archive/2026-04-26/` etc., not in DB tables).

## When to update this doc

- After any schema migration (new table, dropped column, renamed table) — update the affected entry and bump the date in the header.
- After retiring an agent — flag its output tables as "archive" in their tier classification.
- After wiring a previously-empty table — promote from Tier 3 → Tier 2 / 1.
- Before any audit older than ~30 days — verify row counts for tables of interest.

---

*Generated by combined Tuesday session 2026-05-05 (Scotty). Tier classifications based on most-recent timestamps captured at session start. Live tables may have moved tiers since.*
