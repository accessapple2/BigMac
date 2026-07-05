import sqlite3
import os

DB_PATH = "data/trader.db"
# NOTE: All arena data (players, trades, signals, portfolio_history) lives in trader.db.
# There is no separate arena.db — any empty arena.db files in the project root or data/
# are dead artifacts and can be safely deleted.
OPENAI_CODEX_MODEL = os.environ.get("OPENAI_CODEX_MODEL", "gpt-5.2-codex")
OPENAI_CODEX_MINI_MODEL = os.environ.get("OPENAI_CODEX_MINI_MODEL", OPENAI_CODEX_MODEL)

def setup():
    """Idempotent DB bootstrap + canonical agent enforcement. Runs every main.py startup.

    Side effects (in order):
    1. Creates/migrates schema: ai_players, agent_ratings, fallback_model + is_fallback
       columns, and related tables. CREATE TABLE IF NOT EXISTS / ALTER TABLE ADD COLUMN
       guarded with try/except so re-runs are safe.
    2. INSERT OR IGNORE seeds 18 ai_players rows from the hardcoded list at lines 245-264.
       Existing rows are untouched.
    3. **UNCONDITIONALLY** UPDATEs model_id/provider/display_name for 18 agents at
       lines 274-291. This is the canonical-model enforcement step — any runtime
       edit to ai_players.model_id for these IDs will be silently reverted on next
       startup. (HM-BN 2026-05-15 regression root cause; fixed by commit 7eed0ca.)
    4. Seeds fallback_model column for agents missing one (lines 350+) — guarded by
       `WHERE fallback_model IS NULL OR fallback_model=''`, so safe to re-run.
    5. Resets is_active/is_paused per the shelved-crew list at the end of the function.

    Invoked from main.py:2151 (boot) and main.py:2735 (daily ratings refresh).
    """
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    c = conn.cursor()

    # HM-B-pre: dropped is_halted column (halt_mode is single source of truth; added by later migration)
    c.execute('''CREATE TABLE IF NOT EXISTS ai_players (
        id TEXT PRIMARY KEY,
        display_name TEXT NOT NULL,
        provider TEXT NOT NULL,
        model_id TEXT NOT NULL,
        cash REAL DEFAULT 10000.00,
        is_active INTEGER DEFAULT 1,
        halt_reason TEXT,
        can_trade_live INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS positions (
        id INTEGER PRIMARY KEY,
        player_id TEXT NOT NULL REFERENCES ai_players(id),
        symbol TEXT NOT NULL,
        qty REAL,
        avg_price REAL,
        asset_type TEXT DEFAULT 'stock',
        option_type TEXT,
        strike_price REAL,
        expiry_date TEXT,
        high_watermark REAL,
        opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(player_id, symbol, asset_type, option_type, strike_price, expiry_date)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS trades (
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
        signal_id INTEGER REFERENCES signals(id),  -- HM-SIGNAL-TRADE-FK 2026-05-20; NULL for non-signal-driven trades
        executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY,
        player_id TEXT NOT NULL REFERENCES ai_players(id),
        symbol TEXT NOT NULL,
        signal TEXT NOT NULL,
        confidence REAL,
        reasoning TEXT,
        asset_type TEXT DEFAULT 'stock',
        option_type TEXT,
        acted_on INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS portfolio_history (
        id INTEGER PRIMARY KEY,
        player_id TEXT NOT NULL REFERENCES ai_players(id),
        total_value REAL,
        cash REAL,
        positions_value REAL,
        recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS ai_chat (
        id INTEGER PRIMARY KEY,
        player_id TEXT NOT NULL REFERENCES ai_players(id),
        message TEXT NOT NULL,
        context TEXT,
        reply_to INTEGER REFERENCES ai_chat(id),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS ai_journal (
        id INTEGER PRIMARY KEY,
        player_id TEXT NOT NULL REFERENCES ai_players(id),
        entry TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS market_news (
        id INTEGER PRIMARY KEY,
        symbol TEXT,
        headline TEXT NOT NULL,
        summary TEXT,
        source TEXT,
        url TEXT,
        sentiment TEXT,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS war_room (
        id INTEGER PRIMARY KEY,
        player_id TEXT NOT NULL REFERENCES ai_players(id),
        symbol TEXT NOT NULL,
        take TEXT NOT NULL,
        strategy_mode TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS smart_money_signals (
        id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL,
        buyers TEXT NOT NULL,
        detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS season_config (
        season       INTEGER PRIMARY KEY,
        name         TEXT    NOT NULL,
        start_date   TEXT    NOT NULL,
        end_date     TEXT,
        active_agents TEXT,
        strategies   TEXT,
        alpha_gate   REAL    DEFAULT 0.3,
        triple_filter TEXT,
        proving_ground INTEGER DEFAULT 0,
        created_at   TEXT    DEFAULT (datetime('now'))
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS ollie_decisions (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        decided_at   TEXT    NOT NULL DEFAULT (datetime('now')),
        player_id    TEXT    NOT NULL,
        symbol       TEXT    NOT NULL,
        decision     TEXT    NOT NULL,
        ollie_score  REAL    NOT NULL,
        grade_pts    REAL,
        alpha_pts    REAL,
        agent_wr_pts REAL,
        regime_pts   REAL,
        reason       TEXT,
        market_regime TEXT,
        agent_conf   REAL
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS model_stats (
        id INTEGER PRIMARY KEY,
        player_id TEXT NOT NULL REFERENCES ai_players(id),
        api_calls INTEGER DEFAULT 0,
        total_cost REAL DEFAULT 0.0,
        date TEXT NOT NULL,
        UNIQUE(player_id, date)
    )''')

    # HM-GATE-REJECT-TELEMETRY-V1 2026-05-26
    c.execute('''CREATE TABLE IF NOT EXISTS gate_reject_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL DEFAULT (datetime('now')),
        player_id TEXT NOT NULL,
        symbol TEXT,
        gate_name TEXT NOT NULL,
        reason TEXT,
        signal_id INTEGER,
        price REAL,
        confidence REAL
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_gate_reject_player ON gate_reject_log(player_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_gate_reject_ts ON gate_reject_log(ts)')

    # === HM-EXEC-PIPELINE observe-first measurement layer (Part 1) ============
    c.execute('''CREATE TABLE IF NOT EXISTS signal_observations (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ts              TEXT NOT NULL,
        source          TEXT NOT NULL,
        ticker          TEXT NOT NULL,
        direction       TEXT NOT NULL,
        conviction      TEXT,
        grade           TEXT,
        confluence_meta TEXT,
        expiry          TEXT,
        is_context      INTEGER NOT NULL DEFAULT 0,
        acted_by_fleet  INTEGER,
        fleet_trade_id  INTEGER,
        fwd_return_1h   REAL,
        fwd_return_1d   REAL,
        fwd_return_exp  REAL,
        evaluated_at    TEXT
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS ix_sigobs_ts ON signal_observations(ts)')
    c.execute('CREATE INDEX IF NOT EXISTS ix_sigobs_source ON signal_observations(source)')
    c.execute('CREATE INDEX IF NOT EXISTS ix_sigobs_eval ON signal_observations(evaluated_at)')
    # === /HM-EXEC-PIPELINE ====================================================

    c.execute('''CREATE TABLE IF NOT EXISTS api_costs (
        id INTEGER PRIMARY KEY,
        player_id TEXT NOT NULL REFERENCES ai_players(id),
        call_type TEXT NOT NULL DEFAULT 'scan',
        input_tokens INTEGER DEFAULT 0,
        output_tokens INTEGER DEFAULT 0,
        cost_usd REAL DEFAULT 0.0,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS ghost_trades (
        id INTEGER PRIMARY KEY,
        player_id TEXT NOT NULL REFERENCES ai_players(id),
        symbol TEXT NOT NULL,
        confidence REAL,
        reasoning TEXT,
        entry_price REAL,
        outcome_price REAL,
        outcome_pnl_pct REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS pair_trades (
        id INTEGER PRIMARY KEY,
        long_symbol TEXT NOT NULL,
        short_symbol TEXT NOT NULL,
        sector TEXT,
        player_id TEXT NOT NULL REFERENCES ai_players(id),
        confidence REAL,
        details TEXT,
        detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS kill_switch_log (
        id INTEGER PRIMARY KEY,
        positions_closed INTEGER,
        total_pnl REAL,
        details TEXT,
        activated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS cto_briefings (
        id INTEGER PRIMARY KEY,
        briefing TEXT NOT NULL,
        signals_reviewed INTEGER DEFAULT 0,
        models_active INTEGER DEFAULT 0,
        steves_positions TEXT,
        flow_lean TEXT,
        regime TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS flow_lean_history (
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
    )''')

    # Seed AI players
    players = [
        ("ollama-local", "Lt. Cmdr. Geordi", "ollama", "qwen3:14b"),
        ("ollama-gemma27b", "Lt. Cmdr. Worf", "ollama", "qwen3:8b"),
        ("ollama-deepseek", "DeepSeek R1 7B", "ollama", "deepseek-r1:14b"),
        ("ollama-qwen3", "Lt. Cmdr. Scotty", "ollama", "qwen3:8b"),
        ("ollama-kimi", "Kimi (phi3:mini)", "ollama", "phi3:mini"),
        ("ollama-coder", "Lt. Cmdr. Data", "ollama", "qwen2.5-coder:7b"),
        ("ollama-llama", "Lt. Cmdr. Uhura", "ollama", "llama3.1:latest"),
        ("claude-sonnet", "Codex Prime", "ollama", "qwen3:8b"),
        ("claude-haiku", "Codex Scout", "ollama", "qwen2.5-coder:7b"),
        ("gpt-4o", "GPT-4o", "ollama", "qwen3:8b"),
        ("gpt-o3", "GPT-o3", "ollama", "deepseek-r1:7b"),
        ("gemini-2.5-pro", "Qwen3 14B Pro", "ollama", "qwen3:14b"),
        ("gemini-2.5-flash", "Lt. Cmdr. Worf", "ollama", "qwen3:8b"),
        ("grok-3", "Grok 3", "ollama", "qwen3:14b"),
        ("grok-4", "Lt. Cmdr. Spock", "ollama", "deepseek-r1:7b"),
        ("dayblade-0dte", "DayBlade Options", "dayblade", "options-s2"),
        ("webull", "Captain Kirk", "webull", "human"),
        ("cto-grok42", "CTO Grok 4.2", "ollama", "qwen2.5-coder:7b"),
        ("ollama-glm4", "Lt. Cmdr. GLM4", "ollama", "qwen3:8b"),
        ("ollama-plutus", "Dr. McCoy", "ollama", "0xroyce/plutus"),
        ("options-sosnoff", "Counselor Troi", "ollama", "qwen3:8b"),
        ("energy-arnold", "Cmdr. Trip Tucker", "ollama", "qwen3:8b"),
        ("dayblade-sulu", "Lt. Sulu", "ollama", "qwen3:14b"),
        ("dalio-metals", "Cmdr. Dalio", "physical", "metals-tracker"),
        ("mlx-qwen3", "Ensign Chekov", "ollama", "phi3:mini"),
    ]
    _newly_inserted = []
    for pid, name, provider, model in players:
        cash = 3500.00 if pid == "dayblade-0dte" else (0.0 if pid == "webull" else (0.0 if pid == "cto-grok42" else 7000.00))
        c.execute(
            "INSERT OR IGNORE INTO ai_players (id, display_name, provider, model_id, cash) VALUES (?,?,?,?,?)",
            (pid, name, provider, model, cash)
        )
        if c.rowcount == 1:
            _newly_inserted.append(pid)

    # Migrate ALL paid/paused players to free local Ollama — every agent active
    # 2026-04-20: patched — no more qwen3:8b (8GB swap storm on bigmac M4 16GB)
    # Routing: neo-matrix/capitol-trades/ollama-kimi → bigmac phi3:mini; all others → Ollie GPU
    #
    # HM-CN postmortem (eval ~2026-06-15): consider adding
    # `AND (model_id IS NULL OR model_id='')` to each UPDATE below (per the
    # fallback.py:128 pattern) so runtime model_id changes survive restarts.
    # Trade-off: canonical config changes would then require a row reset.
    # Source: HM-BN 2026-05-15 silent-revert incident (commit 7eed0ca).
    c.execute("UPDATE ai_players SET provider='ollama', model_id='ministral-3:3b' WHERE id='claude-sonnet'")           # was qwen3:8b
    c.execute("UPDATE ai_players SET provider='ollama', model_id='qwen2.5-coder:7b' WHERE id='claude-haiku'")
    c.execute("UPDATE ai_players SET provider='ollama', model_id='ministral-3:3b' WHERE id='gpt-4o'")                  # was qwen3:8b
    c.execute("UPDATE ai_players SET provider='ollama', model_id='ministral-3:3b' WHERE id='gpt-o3'")                  # was deepseek-r1:7b
    c.execute("UPDATE ai_players SET provider='ollama', model_id='qwen3:14b' WHERE id='grok-3'")                 # was qwen3:8b
    c.execute("UPDATE ai_players SET provider='ollama', model_id='deepseek-r1:7b' WHERE id='grok-4'")
    c.execute("UPDATE ai_players SET provider='ollama', model_id='qwen3:8b' WHERE id='cto-grok42'")  # 2026-06-01: devstral-small-2 removed for good → repoint to installed qwen3:8b (the unconditional UPDATE here was forcing the dead model back on every setup_db)
    c.execute("UPDATE ai_players SET provider='ollama', model_id='deepseek-r1:14b' WHERE id='ollama-deepseek'")  # was deepseek-r1:7b
    c.execute("UPDATE ai_players SET provider='ollama', model_id='ministral-3:3b', display_name='Lt. Cmdr. Worf' WHERE id='ollama-gemma27b'")    # was qwen3:8b
    c.execute("UPDATE ai_players SET provider='ollama', model_id='ministral-3:3b', display_name='Lt. Cmdr. GLM4' WHERE id='ollama-glm4'")        # was qwen3:8b
    c.execute("UPDATE ai_players SET provider='ollama', model_id='ministral-3:3b', display_name='Kimi (ministral-3:3b)' WHERE id='ollama-kimi'")      # was qwen3:8b
    c.execute("UPDATE ai_players SET provider='ollama', model_id='ministral-3:3b' WHERE id='gemini-2.5-flash'")        # was qwen3:8b
    c.execute("UPDATE ai_players SET display_name='Qwen3 14B Pro', provider='ollama', model_id='qwen3:14b' WHERE id='gemini-2.5-pro'")
    # HM-CN Phase 2 2026-05-17: truth-up to qwen3:8b. HM-BN had set gemma4:31b
    # as a variance from the Phase 4 proposal (gemma4:26b) but never live-tested
    # in production (main.py:127 was always qwen3:8b — silent bypass). HM-BN.2
    # specialty bakeoff (priority #1) will revalidate the right options model.
    c.execute("UPDATE ai_players SET provider='ollama', model_id='qwen3:8b' WHERE id='options-sosnoff'")            # 2026-05-17 truth-up
    c.execute("UPDATE ai_players SET model_id='ministral-3:3b' WHERE id='ollama-qwen3'")                               # was qwen3:8b
    c.execute("UPDATE ai_players SET model_id='ministral-3:3b' WHERE id='mlx-qwen3'")                                 # 2026-04-20: qwen3:8b → phi3:mini
    c.execute("UPDATE ai_players SET model_id='ministral-3:3b' WHERE id='energy-arnold'")                              # was qwen3:8b
    c.execute("UPDATE ai_players SET provider='ollama', model_id='ministral-3:3b' WHERE id='dalio-metals'")            # 2026-04-20: metals-tracker → qwen3:8b
    # Activate ALL agents (except permanently shelved Sniper Mode advisory crew)
    # ollie-auto is NOT shelved — he is Fleet Commander (is_paused=0, crew_role='commander')
    _shelved = "('dayblade-0dte','dayblade-sulu','super-agent')"
    c.execute(f"UPDATE ai_players SET is_active=1, is_paused=0 WHERE id != 'webull' AND id NOT IN {_shelved}")
    # Shelved advisory agents: keep paused permanently
    c.execute(f"UPDATE ai_players SET is_active=1, is_paused=1, crew_role='advisory' WHERE id IN {_shelved}")
    # Ollie: Fleet Commander — active, not paused, special commander role
    c.execute("UPDATE ai_players SET is_active=1, is_paused=0, crew_role='commander' WHERE id='ollie-auto'")

    # === HM-ROSTER-CAP-2026-07-04 =======================================
    # Hard ceiling on concurrently EXECUTING agents (see config.MAX_ACTIVE_AGENTS
    # and docs/DOCTRINE.md). Never auto-halts an already-active agent —
    # pre-existing overage is only ever logged loudly, every startup, for
    # manual Admiral resolution. The only thing this blocks outright is a
    # brand-new row inserted THIS run (via the seed loop above) defaulting
    # to halt_mode='active' once the roster is already at/over cap — it
    # must pass an audition (AUDITION_CRITERIA) before it can go active.
    #
    # "Executing" excludes three classes even when halt_mode='active':
    #   1. Tracking-route players (engine.trades_filter.TRACKING_PLAYERS —
    #      dalio-metals, enterprise-computer, schwab): their portfolio is
    #      execution_mode='tracking', they never place fleet orders, so
    #      they don't compete for an execution seat.
    #   2. Human-operated seats (is_human=1 — trade-desk, webull,
    #      enterprise-computer): a human placing a deliberate order isn't
    #      an autonomous agent competing for a seat (docs/XO_BACKLOG.md
    #      Item 21, TRADE_DESK_BYPASS_GATES doctrine).
    #   3. Sim/tracking-mode agents (crew_role='sim' — ollie-machine):
    #      "tracking-mode, can_trade_live=0; NOT in any scan/exec roster"
    #      per its own halt_reason.
    #   4. halt_mode='exit_only' agents are already excluded by the
    #      halt_mode='active' filter below — a draining/wind-down agent
    #      (e.g. gemini-2.5-flash until its last open position closes)
    #      places no NEW orders and so never consumed a slot to begin with.
    # HM-ROSTER-RECONCILE-8-2026-07-05: without excluding classes 2/3, the
    # 2 seats the Admiral deliberately left EMPTY for future audition
    # graduates get silently filled by ollie-machine/trade-desk in the raw
    # count (verified: 9 raw active rows, but only 6 are real executing
    # candidates -- without this fix the query reads exactly 8, at cap,
    # leaving zero real room for a graduate).
    #
    # Scope note: this guard only sees insertions made through THIS
    # function's seed loop. An out-of-band script that INSERTs or
    # UPDATEs halt_mode='active' directly against the DB is not
    # intercepted at the moment it runs — but since setup() runs on every
    # main.py startup, the resulting overage is still caught and logged
    # loudly on the very next restart via the active_count check below.
    from config import MAX_ACTIVE_AGENTS
    from engine.trades_filter import TRACKING_PLAYERS
    _track_sql = ", ".join("?" for _ in TRACKING_PLAYERS)
    _non_executing_sql = (
        f"id NOT IN ({_track_sql}) "
        f"AND COALESCE(is_human, 0) = 0 "
        f"AND COALESCE(crew_role, 'active') != 'sim'"
    )
    active_count = c.execute(
        f"SELECT COUNT(*) FROM ai_players WHERE COALESCE(halt_mode,'active')='active' "
        f"AND {_non_executing_sql}",
        TRACKING_PLAYERS,
    ).fetchone()[0]
    if active_count > MAX_ACTIVE_AGENTS:
        _blocked = []
        for _pid in _newly_inserted:
            if _pid in TRACKING_PLAYERS:
                continue  # tracking-route seats never consume an execution slot
            _row = c.execute(
                "SELECT COALESCE(halt_mode,'active') FROM ai_players WHERE id=?", (_pid,)
            ).fetchone()
            if _row and _row[0] == 'active':
                c.execute(
                    "UPDATE ai_players SET halt_mode='full', halt_reason=? WHERE id=?",
                    (f"[ROSTER-CAP] blocked at insert — roster at/over "
                     f"MAX_ACTIVE_AGENTS={MAX_ACTIVE_AGENTS}; requires a passing "
                     f"audition before activation (see AUDITION_CRITERIA in config.py)",
                     _pid)
                )
                _blocked.append(_pid)
        print(
            f"[ROSTER-CAP] WARNING: {active_count} executing agents exceeds "
            f"MAX_ACTIVE_AGENTS={MAX_ACTIVE_AGENTS} by {active_count - MAX_ACTIVE_AGENTS} "
            f"(tracking-route/human/sim seats excluded from this count per "
            f"the non-executing filter above). "
            f"New seats blocked from activating this run: {_blocked or 'none'}. "
            f"Pre-existing over-cap seats are NOT auto-halted — flagged for manual "
            f"Admiral resolution (one-in-one-out per docs/DOCTRINE.md)."
        )
    # === /HM-ROSTER-CAP-2026-07-04 =======================================

    c.execute('''CREATE TABLE IF NOT EXISTS watchlist_signals (
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
        resolved_at TIMESTAMP
    )''')

    # Gemini failover audit log
    c.execute('''CREATE TABLE IF NOT EXISTS gemini_failover (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        model_name  TEXT NOT NULL,
        reason      TEXT,
        activated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        recovered_at TIMESTAMP,
        is_active   INTEGER DEFAULT 1
    )''')

    # Add high_watermark column if missing (migration for existing DBs)
    try:
        c.execute("ALTER TABLE positions ADD COLUMN high_watermark REAL")
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add exit_price to watchlist_signals (for watching status)
    try:
        c.execute("ALTER TABLE watchlist_signals ADD COLUMN exit_price REAL")
    except sqlite3.OperationalError:
        pass

    # Add is_paused column to ai_players (for model control panel)
    try:
        c.execute("ALTER TABLE ai_players ADD COLUMN is_paused INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass

    # Add fallback columns to ai_players (for automatic free fallback routing)
    try:
        c.execute("ALTER TABLE ai_players ADD COLUMN fallback_model TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("ALTER TABLE ai_players ADD COLUMN is_fallback INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    # Seed fallback_model values for known paid players
    _fallback_seed = [
        ("grok-3", "qwen3:14b"), ("grok-4", "deepseek-r1:7b"),
        ("cto-grok42", "qwen2.5-coder:7b"), ("gpt-4o", "qwen3:8b"),
        ("gpt-o3", "deepseek-r1:7b"), ("claude-sonnet", "qwen3:8b"),
        ("claude-haiku", "qwen2.5-coder:7b"), ("gemini-2.5-flash", "qwen3:8b"),
        ("gemini-2.5-pro", "qwen3:14b"), ("options-sosnoff", "qwen3:8b"),
        ("dalio-metals", "qwen3:8b"), ("super-agent", "deepseek-r1:7b"),
        ("ollama-llama", "deepseek-r1:7b"),
    ]
    for _pid, _model in _fallback_seed:
        c.execute(
            "UPDATE ai_players SET fallback_model=? WHERE id=? "
            "AND (fallback_model IS NULL OR fallback_model='')",
            (_model, _pid)
        )
    # Seed fallbacks_enabled default setting
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('fallbacks_enabled', '1')")

    # Add is_human flag (human portfolios survive season resets)
    try:
        c.execute("ALTER TABLE ai_players ADD COLUMN is_human INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    c.execute("UPDATE ai_players SET is_human=1 WHERE id='webull'")

    # Backtest tables
    c.execute('''CREATE TABLE IF NOT EXISTS backtest_runs (
        id INTEGER PRIMARY KEY,
        run_type TEXT NOT NULL DEFAULT 'single',
        start_date TEXT NOT NULL,
        end_date TEXT NOT NULL,
        model_ids TEXT NOT NULL,
        status TEXT DEFAULT 'running',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMP
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS backtest_results (
        id INTEGER PRIMARY KEY,
        run_id INTEGER NOT NULL REFERENCES backtest_runs(id),
        player_id TEXT NOT NULL,
        display_name TEXT,
        test_date TEXT NOT NULL,
        final_value REAL,
        total_return_pct REAL,
        win_rate REAL,
        sharpe_ratio REAL,
        max_drawdown REAL,
        num_trades INTEGER,
        best_trade_pct REAL,
        worst_trade_pct REAL,
        trades_json TEXT,
        equity_json TEXT
    )''')

    # Season columns
    for table in ["ai_players", "trades", "signals", "portfolio_history"]:
        try:
            c.execute(f"ALTER TABLE {table} ADD COLUMN season INTEGER DEFAULT 1")
        except sqlite3.OperationalError:
            pass

    # Add exit_price and realized_pnl columns to trades (migration for existing DBs)
    # known_contaminated: HM-TRADES-PRICE-WRITEBACK Option B — flags pre-2026-05-21
    # routed rows with internal (non-broker) prices so Plutus/PnL exclude them.
    for col, typ in [("exit_price", "REAL"), ("realized_pnl", "REAL"), ("entry_price", "REAL"),
                     ("known_contaminated", "INTEGER DEFAULT 0")]:
        try:
            c.execute(f"ALTER TABLE trades ADD COLUMN {col} {typ}")
        except sqlite3.OperationalError:
            pass  # Column already exists

    # HM-SIGNAL-TRADE-FK 2026-05-20: link trade row to originating signal row.
    # SQLite doesn't enforce FK by default but the linkage is captured here:
    # trades.signal_id → signals.id (rowid). NULL for mechanical exits (Autopilot
    # trims, scale-outs, stop-loss) and any path where the originating signal
    # context is not in scope. See engine/paper_trader.buy() for the populate path.
    try:
        c.execute("ALTER TABLE trades ADD COLUMN signal_id INTEGER DEFAULT NULL")
    except sqlite3.OperationalError:
        pass  # Column already exists

    # HM-DECISION-AUDIT-V1 2026-05-20: unified decision-event log.
    # One row per: signal_emit / gate_reject / trade_fire. Captures market
    # snapshot at decision time (regime + spy_change + vix) plus FK references
    # to signals.id and trades.id. Resolves the multi-table-join pain
    # documented in project_hm_decision_support_observability_audit Audit B.
    # See engine.paper_trader._write_decision_audit for the writer.
    c.execute('''CREATE TABLE IF NOT EXISTS decision_audit (
        id INTEGER PRIMARY KEY,
        event_type TEXT NOT NULL,
        player_id TEXT,
        symbol TEXT,
        signal_id INTEGER,
        trade_id INTEGER,
        regime TEXT,
        spy_change REAL,
        vix REAL,
        confidence REAL,
        gate_verdict TEXT,
        reasoning_snippet TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )''')
    c.execute("CREATE INDEX IF NOT EXISTS idx_decision_audit_player_ts ON decision_audit(player_id, created_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_decision_audit_symbol_ts ON decision_audit(symbol, created_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_decision_audit_event ON decision_audit(event_type, created_at)")

    # HM-POST-EXIT-TRACKER 2026-05-20: flag exits that proved premature.
    # Seeded on every SELL via engine.paper_trader.sell hook; daily scan in
    # engine.post_exit_tracker.run_daily_scan checks current price vs exit.
    # If current > exit × (1 + threshold/100): flagged=1, [POST-EXIT-FLAG] log.
    c.execute('''CREATE TABLE IF NOT EXISTS post_exit_watch (
        id INTEGER PRIMARY KEY,
        player_id TEXT,
        symbol TEXT,
        exit_price REAL,
        exit_date TEXT,
        exit_pnl REAL,
        peak_price_after REAL,
        peak_date_after TEXT,
        missed_gain REAL,
        threshold_pct REAL DEFAULT 5.0,
        flagged INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
    )''')
    c.execute("CREATE INDEX IF NOT EXISTS idx_post_exit_watch_unflagged ON post_exit_watch(flagged, symbol)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_post_exit_watch_player ON post_exit_watch(player_id, exit_date)")

    # Add sources column to signals and trades (data source traceability)
    for table in ["signals", "trades"]:
        try:
            c.execute(f"ALTER TABLE {table} ADD COLUMN sources TEXT DEFAULT ''")
        except sqlite3.OperationalError:
            pass  # Column already exists

    # Add timeframe column to signals and trades (SCALP / SWING / POSITION classification)
    for table in ["signals", "trades"]:
        try:
            c.execute(f"ALTER TABLE {table} ADD COLUMN timeframe TEXT DEFAULT 'SWING'")
        except sqlite3.OperationalError:
            pass  # Column already exists

    # Add execution status tracking columns to signals
    try:
        c.execute("ALTER TABLE signals ADD COLUMN execution_status TEXT DEFAULT 'PENDING'")
    except sqlite3.OperationalError:
        pass  # Column already exists
    try:
        c.execute("ALTER TABLE signals ADD COLUMN rejection_reason TEXT DEFAULT NULL")
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Theta opportunities table (premium-selling scanner)
    c.execute("""
        CREATE TABLE IF NOT EXISTS theta_opportunities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            scan_date TEXT NOT NULL,
            iv_rank REAL,
            iv_percentile REAL,
            current_iv REAL,
            strategy_type TEXT,
            short_strike_call REAL,
            short_strike_put REAL,
            long_strike_call REAL,
            long_strike_put REAL,
            expiration TEXT,
            dte INTEGER,
            estimated_daily_theta REAL,
            max_risk REAL,
            theta_score INTEGER DEFAULT 0,
            is_range_bound INTEGER DEFAULT 0,
            earnings_warning INTEGER DEFAULT 0,
            earnings_date TEXT,
            spot_price REAL,
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Gap Scanner table — morning gap detection and intraday fill tracking
    c.execute("""
        CREATE TABLE IF NOT EXISTS gap_scanner (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            gap_direction TEXT NOT NULL,
            gap_pct REAL NOT NULL,
            gap_type TEXT NOT NULL,
            volume_ratio REAL,
            fill_probability REAL,
            filled INTEGER DEFAULT 0,
            fill_time_minutes INTEGER,
            fill_status TEXT DEFAULT 'OPEN',
            prev_close REAL,
            open_price REAL,
            high_of_day REAL,
            low_of_day REAL,
            sma20 REAL,
            scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            filled_at TIMESTAMP
        )
    """)

    # Physical metals purchase ledger — real cost basis tracking (NEVER DROP)
    c.execute('''CREATE TABLE IF NOT EXISTS metals_ledger (
        id INTEGER PRIMARY KEY,
        purchase_date TEXT NOT NULL,
        metal TEXT NOT NULL,
        qty_oz REAL NOT NULL,
        total_cost REAL NOT NULL,
        cost_per_oz REAL NOT NULL,
        source TEXT,
        notes TEXT
    )''')

    # Add strategy_mode column to war_room (for Strategy Mode feature)
    try:
        c.execute("ALTER TABLE war_room ADD COLUMN strategy_mode TEXT")
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Backtest history — save results for trend tracking (Fix 6)
    c.execute('''CREATE TABLE IF NOT EXISTS backtest_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id TEXT NOT NULL,
        player_name TEXT,
        run_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        period_days INTEGER DEFAULT 30,
        start_date TEXT,
        end_date TEXT,
        starting_value REAL DEFAULT 7000,
        final_value REAL,
        return_pct REAL,
        total_pnl REAL,
        win_count INTEGER DEFAULT 0,
        loss_count INTEGER DEFAULT 0,
        win_rate REAL DEFAULT 0,
        total_trades INTEGER DEFAULT 0,
        best_trade_pnl REAL,
        worst_trade_pnl REAL,
        best_trade_symbol TEXT,
        worst_trade_symbol TEXT,
        spy_return_pct REAL,
        rallies_top_return_pct REAL,
        rallies_top_name TEXT,
        notes TEXT,
        config_snapshot TEXT,
        guardrails_applied INTEGER DEFAULT 0,
        signals_tested INTEGER DEFAULT 0,
        signals_skipped INTEGER DEFAULT 0,
        skip_summary TEXT
    )''')

    # Migration: add guardrails_applied to existing backtest_history
    for col, typ, dflt in [
        ("guardrails_applied", "INTEGER", "0"),
        ("signals_tested", "INTEGER", "0"),
        ("signals_skipped", "INTEGER", "0"),
        ("skip_summary", "TEXT", "NULL"),
    ]:
        try:
            c.execute(f"ALTER TABLE backtest_history ADD COLUMN {col} {typ} DEFAULT {dflt}")
        except sqlite3.OperationalError:
            pass

    # Riker's Log — Captain's decision journal with officer recommendations
    c.execute('''CREATE TABLE IF NOT EXISTS rikers_log (
        id INTEGER PRIMARY KEY,
        entry_type TEXT NOT NULL DEFAULT 'manual',
        source TEXT NOT NULL DEFAULT 'captain',
        title TEXT,
        content TEXT NOT NULL,
        ticker TEXT,
        action TEXT,
        conviction REAL,
        outcome TEXT,
        outcome_pnl REAL,
        tags TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    # 8/21 MA Cross Regime History — one row per day, REPLACE on re-run
    c.execute('''CREATE TABLE IF NOT EXISTS regime_history (
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
    )''')
    c.execute("CREATE INDEX IF NOT EXISTS idx_regime_history_date ON regime_history(date)")

    # Agent Performance Rating System
    c.execute("""CREATE TABLE IF NOT EXISTS agent_ratings (
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
    )""")
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_agent_ratings_player_period "
        "ON agent_ratings(player_id, period, timestamp)"
    )

    # FinMem Agent Memory — 3-layer lesson storage (self_improvement loop)
    # INSERT ONLY — never drop or truncate (sacred data rule)
    c.execute("""CREATE TABLE IF NOT EXISTS agent_memory (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id    TEXT    NOT NULL,
        memory_layer TEXT    NOT NULL,  -- LESSON | WORKING | SHORT_TERM | LONG_TERM
        summary      TEXT    NOT NULL,
        score        REAL    DEFAULT 0, -- recency × importance (0.0–1.0)
        created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    c.execute(
        "CREATE INDEX IF NOT EXISTS idx_agent_memory_player_layer "
        "ON agent_memory(player_id, memory_layer, created_at)"
    )

    # === HM-EVENTS-BUS-FOUNDATION 2026-05-22 ============================
    # Canonical events bus + signals_v2 + engine_allocation + timeframe tags.
    # Spec: ~/.claude/projects/-Users-bigmac/memory/project_hm_events_bus_foundation.md
    #
    # events: single bus every data source writes to.
    c.execute("""CREATE TABLE IF NOT EXISTS events (
        id           INTEGER PRIMARY KEY,
        ts           TEXT    NOT NULL DEFAULT (datetime('now')),
        source       TEXT    NOT NULL,
        event_type   TEXT    NOT NULL,
        symbol       TEXT,
        payload      TEXT,
        processed    INTEGER DEFAULT 0,
        session_date TEXT
    )""")

    # signals_v2: normalized signal layer reading from events.
    c.execute("""CREATE TABLE IF NOT EXISTS signals_v2 (
        id               INTEGER PRIMARY KEY,
        ts               TEXT    NOT NULL DEFAULT (datetime('now')),
        source           TEXT    NOT NULL,
        signal_type      TEXT    NOT NULL,
        symbol           TEXT    NOT NULL,
        direction        TEXT,
        confidence       REAL,
        regime_fit       REAL,
        timeframe        TEXT,
        strategy_tag     TEXT,
        event_id         INTEGER,
        agent_debate_id  INTEGER,
        prompt_version   TEXT,
        metadata         TEXT,
        status           TEXT    DEFAULT 'pending',
        stale_after      TEXT,
        trade_id         INTEGER,
        created_at       TEXT    DEFAULT (datetime('now'))
    )""")

    # engine_allocation: capital + position caps per 0dte / swing / position engine.
    c.execute("""CREATE TABLE IF NOT EXISTS engine_allocation (
        engine             TEXT PRIMARY KEY,
        capital_pct        REAL,
        max_positions      INTEGER,
        max_per_trade_pct  REAL,
        updated_at         TEXT
    )""")
    # Seed defaults — only if rows missing (idempotent on re-runs).
    c.execute(
        "INSERT OR IGNORE INTO engine_allocation "
        "(engine, capital_pct, max_positions, max_per_trade_pct, updated_at) "
        "VALUES "
        "('0dte',     0.05,  3, 0.02, datetime('now')),"
        "('swing',    0.60, 15, 0.04, datetime('now')),"
        "('position', 0.35, 12, 0.06, datetime('now'))"
    )

    # ALTER TABLE ai_players ADD COLUMN timeframe — guarded for re-run.
    _existing_cols = {row[1] for row in c.execute("PRAGMA table_info(ai_players)")}
    if "timeframe" not in _existing_cols:
        c.execute("ALTER TABLE ai_players ADD COLUMN timeframe TEXT DEFAULT 'swing'")

    # === HM-PROMPT-VERSIONING (POC Day 2b) 2026-05-22 =====================
    # signals + trades each carry prompt_version so the learning loop
    # (Day 3a post-mortem + Ghost Scorecard) can compare WR/expectancy
    # across prompt revisions. Bump manually per-agent at the call site:
    # 'player_id_v1' → 'player_id_v2' when the prompt template changes.
    _sig_cols = {row[1] for row in c.execute("PRAGMA table_info(signals)")}
    if "prompt_version" not in _sig_cols:
        c.execute("ALTER TABLE signals ADD COLUMN prompt_version TEXT")
    _trd_cols = {row[1] for row in c.execute("PRAGMA table_info(trades)")}
    if "prompt_version" not in _trd_cols:
        c.execute("ALTER TABLE trades ADD COLUMN prompt_version TEXT")
    # === /HM-PROMPT-VERSIONING ==========================================

    # === HM-EXEC-PIPELINE Phase 0c: entry provenance columns ============
    for _col, _typ in [("grade", "TEXT"), ("voting_agents", "TEXT")]:
        try:
            c.execute(f"ALTER TABLE trades ADD COLUMN {_col} {_typ}")
        except sqlite3.OperationalError:
            pass
    # === /HM-EXEC-PIPELINE =============================================

    # Tag known agents into their actual engine. Defaults to 'swing' for new rows.
    # 0dte agents — sub-minute / minute scalpers.
    c.execute(
        "UPDATE ai_players SET timeframe='0dte' "
        " WHERE id IN ('dayblade-sulu','dayblade-0dte') "
        "   AND (timeframe IS NULL OR timeframe='swing')"
    )
    # Position-trade agents — Elder Council + physical metals tracker.
    c.execute(
        "UPDATE ai_players SET timeframe='position' "
        " WHERE id IN ('sarek','janeway','surak','dalio-metals','enterprise-computer') "
        "   AND (timeframe IS NULL OR timeframe='swing')"
    )
    # === /HM-EVENTS-BUS-FOUNDATION =====================================

    # Performance indexes — safe to re-run (IF NOT EXISTS)
    for _idx in [
        # Equity curve + comparison chart: scan by season, ordered by time
        "CREATE INDEX IF NOT EXISTS idx_portfolio_history_season ON portfolio_history(season, recorded_at)",
        # Equity curve filtered by player
        "CREATE INDEX IF NOT EXISTS idx_portfolio_history_player ON portfolio_history(player_id, season, recorded_at)",
        # Signals feed: player + time (most frequent query pattern)
        "CREATE INDEX IF NOT EXISTS idx_signals_player_ts ON signals(player_id, created_at)",
        # Signals status badge filtering
        "CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(execution_status)",
        # Trades history per player
        "CREATE INDEX IF NOT EXISTS idx_trades_player_ts ON trades(player_id, executed_at)",
        # HM-EVENTS-BUS-FOUNDATION indexes
        "CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts)",
        "CREATE INDEX IF NOT EXISTS idx_events_symbol ON events(symbol)",
        "CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type)",
        "CREATE INDEX IF NOT EXISTS idx_events_processed ON events(processed)",
        "CREATE INDEX IF NOT EXISTS idx_signals_v2_ts ON signals_v2(ts)",
        "CREATE INDEX IF NOT EXISTS idx_signals_v2_symbol ON signals_v2(symbol)",
        "CREATE INDEX IF NOT EXISTS idx_signals_v2_status ON signals_v2(status)",
        "CREATE INDEX IF NOT EXISTS idx_signals_v2_timeframe ON signals_v2(timeframe)",
        "CREATE INDEX IF NOT EXISTS idx_signals_v2_event ON signals_v2(event_id)",
        "CREATE INDEX IF NOT EXISTS idx_signals_v2_stale ON signals_v2(stale_after)",
    ]:
        c.execute(_idx)

    conn.commit()
    conn.close()
    print("Database ready with 14 AI players")

if __name__ == "__main__":
    setup()
