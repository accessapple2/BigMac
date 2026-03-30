import sqlite3
import os

DB_PATH = "data/trader.db"
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
        ("ollama-local", "Lt. Cmdr. Geordi", "ollama", "gemma3:4b"),
        ("ollama-gemma27b", "Gemma3 27B", "ollama", "gemma3:27b"),
        ("ollama-deepseek", "DeepSeek R1 14B", "ollama", "deepseek-r1:14b"),
        ("ollama-qwen3", "Lt. Cmdr. Scotty", "ollama", "qwen3:8b"),
        ("ollama-llama", "Lt. Cmdr. Uhura", "ollama", "llama3.1:latest"),
        ("claude-sonnet", "Codex Prime", "openai", OPENAI_CODEX_MODEL),
        ("claude-haiku", "Codex Scout", "openai", OPENAI_CODEX_MINI_MODEL),
        ("gpt-4o", "GPT-4o", "openai", "gpt-4o"),
        ("gpt-o3", "GPT-o3", "openai", "o3"),
        ("gemini-2.5-pro", "Gemini 2.5 Pro", "google", "gemini-2.5-pro"),
        ("gemini-2.5-flash", "Lt. Cmdr. Worf", "google", "gemini-2.5-flash"),
        ("grok-3", "Grok 3", "xai", "grok-4-1-fast-reasoning"),
        ("grok-4", "Lt. Cmdr. Spock", "xai", "grok-4.20-0309-reasoning"),
        ("dayblade-0dte", "DayBlade Options", "dayblade", "options-s2"),
        ("steve-webull", "Captain Kirk", "webull", "human"),
        ("cto-grok42", "CTO Grok 4.2", "xai", "grok-4.20-0309-reasoning"),
        ("ollama-glm4", "GLM4 9B", "ollama", "glm4:9b"),
        ("ollama-plutus", "Dr. McCoy", "ollama", "0xroyce/plutus"),
        ("options-sosnoff", "Counselor Troi", "google", "gemini-2.5-flash"),
        ("energy-arnold", "Cmdr. Trip Tucker", "ollama", "qwen3:8b"),
        ("dayblade-sulu", "Lt. Sulu", "ollama", "qwen3:8b"),
        ("dalio-metals", "Cmdr. Dalio", "physical", "metals-tracker"),
    ]
    for pid, name, provider, model in players:
        cash = 3500.00 if pid == "dayblade-0dte" else (0.0 if pid == "steve-webull" else (0.0 if pid == "cto-grok42" else 7000.00))
        c.execute(
            "INSERT OR IGNORE INTO ai_players (id, display_name, provider, model_id, cash) VALUES (?,?,?,?,?)",
            (pid, name, provider, model, cash)
        )

    # Keep legacy player IDs for continuity, but migrate their provider/model to Codex.
    c.execute(
        "UPDATE ai_players SET display_name=?, provider='openai', model_id=? WHERE id='claude-sonnet'",
        ("Codex Prime", OPENAI_CODEX_MODEL),
    )
    c.execute(
        "UPDATE ai_players SET display_name=?, provider='openai', model_id=? WHERE id='claude-haiku'",
        ("Codex Scout", OPENAI_CODEX_MINI_MODEL),
    )

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
