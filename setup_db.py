import sqlite3
import os

DB_PATH = "data/trader.db"
# NOTE: All arena data (players, trades, signals, portfolio_history) lives in trader.db.
# There is no separate arena.db — any empty arena.db files in the project root or data/
# are dead artifacts and can be safely deleted.
OPENAI_CODEX_MODEL = os.environ.get("OPENAI_CODEX_MODEL", "gpt-5.2-codex")
OPENAI_CODEX_MINI_MODEL = os.environ.get("OPENAI_CODEX_MINI_MODEL", OPENAI_CODEX_MODEL)

def setup():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS ai_players (
        id TEXT PRIMARY KEY,
        display_name TEXT NOT NULL,
        provider TEXT NOT NULL,
        model_id TEXT NOT NULL,
        cash REAL DEFAULT 10000.00,
        is_active INTEGER DEFAULT 1,
        is_halted INTEGER DEFAULT 0,
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
        ("ollama-gemma27b", "Qwen3.5 9B", "ollama", "qwen3.5:9b"),
        ("ollama-deepseek", "DeepSeek R1 7B", "ollama", "deepseek-r1:7b"),
        ("ollama-qwen3", "Lt. Cmdr. Scotty", "ollama", "qwen3.5:9b"),
        ("ollama-kimi", "Kimi → Qwen3.5", "ollama", "qwen3.5:9b"),
        ("ollama-coder", "Lt. Cmdr. Data", "ollama", "qwen2.5-coder:7b"),
        ("ollama-llama", "Lt. Cmdr. Uhura", "ollama", "llama3.1:latest"),
        ("claude-sonnet", "Codex Prime", "ollama", "qwen3.5:9b"),
        ("claude-haiku", "Codex Scout", "ollama", "qwen2.5-coder:7b"),
        ("gpt-4o", "GPT-4o", "ollama", "qwen3.5:9b"),
        ("gpt-o3", "GPT-o3", "ollama", "deepseek-r1:7b"),
        ("gemini-2.5-pro", "Qwen3 14B Pro", "ollama", "qwen3:14b"),
        ("gemini-2.5-flash", "Lt. Cmdr. Worf", "ollama", "qwen3.5:9b"),
        ("grok-3", "Grok 3", "ollama", "qwen3.5:9b"),
        ("grok-4", "Lt. Cmdr. Spock", "ollama", "deepseek-r1:7b"),
        ("dayblade-0dte", "DayBlade Options", "dayblade", "options-s2"),
        ("steve-webull", "Captain Kirk", "webull", "human"),
        ("cto-grok42", "CTO Grok 4.2", "ollama", "qwen2.5-coder:7b"),
        ("ollama-glm4", "GLM4 → Qwen3.5", "ollama", "qwen3.5:9b"),
        ("ollama-plutus", "Dr. McCoy", "ollama", "0xroyce/plutus"),
        ("options-sosnoff", "Counselor Troi", "ollama", "qwen3.5:9b"),
        ("energy-arnold", "Cmdr. Trip Tucker", "ollama", "qwen3.5:9b"),
        ("dayblade-sulu", "Lt. Sulu", "ollama", "qwen3:14b"),
        ("dalio-metals", "Cmdr. Dalio", "physical", "metals-tracker"),
        ("mlx-qwen3", "Ensign Chekov", "ollama", "qwen3.5:9b"),
    ]
    for pid, name, provider, model in players:
        cash = 3500.00 if pid == "dayblade-0dte" else (0.0 if pid == "steve-webull" else (0.0 if pid == "cto-grok42" else 7000.00))
        c.execute(
            "INSERT OR IGNORE INTO ai_players (id, display_name, provider, model_id, cash) VALUES (?,?,?,?,?)",
            (pid, name, provider, model, cash)
        )

    # Migrate ALL paid/paused players to free local Ollama — every agent active
    c.execute("UPDATE ai_players SET provider='ollama', model_id='qwen3.5:9b' WHERE id='claude-sonnet'")
    c.execute("UPDATE ai_players SET provider='ollama', model_id='qwen2.5-coder:7b' WHERE id='claude-haiku'")
    c.execute("UPDATE ai_players SET provider='ollama', model_id='qwen3.5:9b' WHERE id='gpt-4o'")
    c.execute("UPDATE ai_players SET provider='ollama', model_id='deepseek-r1:7b' WHERE id='gpt-o3'")
    c.execute("UPDATE ai_players SET provider='ollama', model_id='qwen3.5:9b' WHERE id='grok-3'")
    c.execute("UPDATE ai_players SET provider='ollama', model_id='deepseek-r1:7b' WHERE id='grok-4'")
    c.execute("UPDATE ai_players SET provider='ollama', model_id='qwen2.5-coder:7b' WHERE id='cto-grok42'")
    c.execute("UPDATE ai_players SET provider='ollama', model_id='deepseek-r1:7b' WHERE id='ollama-deepseek'")
    c.execute("UPDATE ai_players SET provider='ollama', model_id='qwen3.5:9b', display_name='Qwen3.5 9B' WHERE id='ollama-gemma27b'")
    c.execute("UPDATE ai_players SET provider='ollama', model_id='qwen3.5:9b', display_name='GLM4 → Qwen3.5' WHERE id='ollama-glm4'")
    c.execute("UPDATE ai_players SET provider='ollama', model_id='qwen3.5:9b', display_name='Kimi → Qwen3.5' WHERE id='ollama-kimi'")
    c.execute("UPDATE ai_players SET provider='ollama', model_id='qwen3.5:9b' WHERE id='gemini-2.5-flash'")
    c.execute("UPDATE ai_players SET display_name='Qwen3 14B Pro', provider='ollama', model_id='qwen3:14b' WHERE id='gemini-2.5-pro'")
    c.execute("UPDATE ai_players SET provider='ollama', model_id='qwen3.5:9b' WHERE id='options-sosnoff'")
    c.execute("UPDATE ai_players SET model_id='qwen3.5:9b' WHERE id='ollama-qwen3'")
    c.execute("UPDATE ai_players SET model_id='qwen3.5:9b' WHERE id='mlx-qwen3'")
    c.execute("UPDATE ai_players SET model_id='qwen3.5:9b' WHERE id='energy-arnold'")
    # Activate ALL agents (except permanently shelved Sniper Mode advisory crew)
    # ollie-auto is NOT shelved — he is Fleet Commander (is_paused=0, crew_role='commander')
    _shelved = "('capitol-trades','dalio-metals','dayblade-0dte','dayblade-sulu','super-agent')"
    c.execute(f"UPDATE ai_players SET is_active=1, is_paused=0 WHERE id != 'steve-webull' AND id NOT IN {_shelved}")
    # Shelved advisory agents: keep paused permanently
    c.execute(f"UPDATE ai_players SET is_active=1, is_paused=1, crew_role='advisory' WHERE id IN {_shelved}")
    # Ollie: Fleet Commander — active, not paused, special commander role
    c.execute("UPDATE ai_players SET is_active=1, is_paused=0, crew_role='commander' WHERE id='ollie-auto'")

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
        ("grok-3", "qwen3.5:9b"), ("grok-4", "deepseek-r1:7b"),
        ("cto-grok42", "qwen2.5-coder:7b"), ("gpt-4o", "qwen3.5:9b"),
        ("gpt-o3", "deepseek-r1:7b"), ("claude-sonnet", "qwen3.5:9b"),
        ("claude-haiku", "qwen2.5-coder:7b"), ("gemini-2.5-flash", "qwen3.5:9b"),
        ("gemini-2.5-pro", "qwen3:14b"), ("options-sosnoff", "qwen3.5:9b"),
        ("dalio-metals", "qwen3.5:9b"), ("super-agent", "deepseek-r1:7b"),
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
    c.execute("UPDATE ai_players SET is_human=1 WHERE id='steve-webull'")

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
    for col, typ in [("exit_price", "REAL"), ("realized_pnl", "REAL"), ("entry_price", "REAL")]:
        try:
            c.execute(f"ALTER TABLE trades ADD COLUMN {col} {typ}")
        except sqlite3.OperationalError:
            pass  # Column already exists

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
    ]:
        c.execute(_idx)

    conn.commit()
    conn.close()
    print("Database ready with 14 AI players")

if __name__ == "__main__":
    setup()
