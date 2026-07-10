import os
from dotenv import load_dotenv
load_dotenv(override=True)

# Trading Mode
PAPER_TRADING = True
TRADING_MODE = os.environ.get("TRADING_MODE", "paper")  # "paper" or "live"

# HM-AF-α 2026-05-06: spread cannibalization guard.
# Halts P1 (Battle Station 2-min monitor), P2 (12:45 MST EOD sweep), and
# P3 (dayblade.py post-trade close_all_options) until HM-AF-β (Layer 1
# spread-leg awareness) ships. See docs/XO_BACKLOG.md HM-AF.
# Reversal: set True and restart.
# HM-AF α-lift 2026-05-07: 24h soak passed (289+ α fires overnight, 0 errors,
# 0 β shadow fires); β/γ now the active cannibalization defense layer.
# HM-AF-δ player_id resolver (commit 2fea086) activates with real data.
SPREAD_CANNIBALIZATION_GUARD_ENABLED = False

# door1 2026-06-19: darked — 0/2 lifetime, -$1,670, fattest tail. Reversal: set True + restart.
ZERO_DTE_EXECUTION_ENABLED = False

# HM-EXEC-PIPELINE 2026-06-21: execution router (engine/execution_router.py).
# False = dry-run only (logs what it WOULD do, zero orders placed).
# True  = live execution via Alpaca paper bridge ONLY (RULE #1: never Schwab).
# Activation path: Phase 2 burn-in → Admiral GO → set True + canonical restart.
# N2 kill-switch: touch ~/autonomous-trader/KILL_SWITCH overrides this flag instantly.
EXEC_ROUTER_ENABLED = False

# HM-TRADE-DESK 2026-05-22: manual Captain trade desk on Alpaca paper.
# When True, orders submitted with agent_id='trade-desk' bypass check_trade
# gates (daily trade limit, MAX_POSITION_VALUE, kill switch, Uhura veto).
# Set False to subject the desk to the same rails as routed players.
# Reversal: flip + restart.
TRADE_DESK_BYPASS_GATES = True

# HM-SOURCE-DEDUP 2026-06-02: source-side signal dedup at the save_signal
# chokepoint (engine/paper_trader.py::save_signal). Suppresses re-emitting the
# same (player_id, symbol, signal/direction) while a prior emission is still
# within its staleness budget (events_bus._STALE_BUDGET_S for that timeframe),
# collapsing the per-cycle flood (e.g. Spock re-pushing INTC BUY every 120s scan
# cycle). Because it sits at the single chokepoint, it covers ALL emitters
# (Spock/Neo/Navigator/crew_scanner). The window AUTO-TUNES per timeframe and
# interlocks with Fix 1: intraday(900s)=15min suppression → ~1 emit per 15min per
# (player,symbol,direction); the 60-min re-entry guard handles actual entry.
# A genuine direction flip (BUY→SELL is a different `signal` value) is a distinct
# key → never suppressed. DB-backed (queries the persistent `signals` table) so it
# survives restarts. Pass force=True at the call site to bypass for manual/forced
# signals. SOURCE_DEDUP_WINDOW_MIN is the FALLBACK window (minutes) used only when
# a timeframe has NO staleness budget (e.g. unknown timeframe); 60 is symmetric
# with events_bus_consumer.REENTRY_COOLDOWN_MIN. Reversal: set False + restart.
SOURCE_DEDUP_ENABLED = True
SOURCE_DEDUP_WINDOW_MIN = 60

# FRED-BANKRATE 2026-06-12: promote the FRED Bankrate deposit-rate macro lean
# (engine/fred_bankrate_signal.py) from CONTEXT-ONLY (shown, never gates) to a
# CONFIRMATORY vote in Uhura's confluence (engine/uhura.py::_calculate_confluence).
# RAIL: confirmatory-only — it may lift an existing convergence over the gate but
# may NEVER originate a trade. It is counted only when the fleet already has
# >= fred_bankrate_signal.MIN_FLEET_VOTES (2) directional votes in the dominant
# direction (sole-voter -> never counts; enforced + asserted in
# fred_bankrate_signal.confirmatory_vote). Default OFF (shadow-first): when False,
# FRED stays context-only and the 86% gate is unchanged. Reversal: flip + restart.
FRED_CONFIRMATORY_VOTE_ENABLED = True

# === HM-BK Bull-Knife confirmatory scanners (shadow-first, default-OFF) ========
# Three independent technical confirmatory scanners on the 8a83f17 rail. Each
# writes its OWN table, has its OWN flag, emits a BULL/BEAR confirmatory vote that
# NEVER originates a trade (requires >= MIN_FLEET_VOTES=2 fleet votes to count;
# enforced+asserted in each module's confirmatory_vote). Flag gates BOTH the
# scheduler run AND the live vote. Default OFF = shadow-only (no ntfy, no count).
# Reversal/activation: flip the relevant flag + canonical restart.
AVWAP_CONFIRMATORY_VOTE_ENABLED = True    # HM-BK-B engine/bk_avwap_scanner.py (nightly)
BOX_CONFIRMATORY_VOTE_ENABLED = True      # HM-BK-C engine/bk_box_scanner.py (nightly, after B)
ORB_CONFIRMATORY_VOTE_ENABLED = True      # HM-BK-A engine/bk_orb_scanner.py (intraday)
BOX_SHORT_ENABLED = False                 # HM-BK-C: default long-only; enable for box-breakdown BEAR
ORB_SHORT_ENABLED = False                 # HM-BK-A: default long-only; enable for OR-low breakdown BEAR

# === HM-DEJAVU setup-similarity recall signal (shadow-first, default-OFF) =======
# engine/setup_similarity_signal.py: bge-m3 KNN over the closed-trade setup substrate
# (recall_corpus, refreshed by scripts/recall_refresh.py). Confirmatory-ONLY context read
# ("looks like N past setups, X% won, avg $Y"), weight 0.5, is_trigger False, abstains on no
# analog. NEVER originates a trade (sole-voter -> never counts; asserted in confirmatory_vote).
# When False: market_vote() returns None (witness untouched); recall()/shadow_log() still compute
# for shadow validation. Wiring into McCoy/Archer is a LATER ticket. Reversal: flip + restart.
SETUP_SIMILARITY_ENABLED = False

# === HM-GRADE-B-RELAX (reversal allowance, shadow-first default-OFF) ===========
# When ON, the grade-B fleet gate (engine/paper_trader.py) allows a 0.60-0.75-conv
# stock BUY in regime==CAUTIOUS_BEAR IF SPY has decisively RECLAIMED its 8-day MA
# (spy_close >= MIN_MA8_MARGIN_PCT above ma_8) — a reversal candidate. BEAR_CROSS
# still always blocks; CAUTIOUS_BEAR without the reclaim still blocks. Grade-A,
# conf<0.60, and options are untouched. OFF = original behavior. Live-flip via settings.
GRADE_B_REVERSAL_RELAX_ENABLED = False
GRADE_B_REVERSAL_MIN_MA8_MARGIN_PCT = 0.3   # SPY % above its 8MA to count as a decisive reclaim

# === HM-SHADOW-WITNESS-V7D (logged-only; NEVER influences the live decision) ===
# When ON, debate_engine logs a plutus-v7d critique alongside v1's on the SAME
# witness input (post-decision, non-blocking) to plutus_shadow_critiques for later
# v1-vs-v7d grading. v1 STAYS the active witness (ai_players.ollama-plutus). OFF =
# no shadow call. Live-flip via settings.
SHADOW_WITNESS_ENABLED = False


def live_flag(key: str, default: bool) -> bool:
    """Boolean flag with a config default that can be flipped LIVE (no restart) via
    the settings table: INSERT OR REPLACE INTO settings(key,value) VALUES(key,'true').
    Fail-safe -> the config default. Connects only when called (no import-time DB hit)."""
    import sqlite3
    try:
        c = sqlite3.connect("data/trader.db", timeout=5)
        row = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        c.close()
        if row and row[0] is not None:
            return str(row[0]).strip().lower() in ("1", "true", "on", "yes")
    except Exception:
        pass
    return default

# HM-BK leveraged/inverse universe exclusion (2026-06-14). scan_universe is
# ordered/weighted by avg_volume, which is dominated by leveraged/inverse ETFs —
# the highest-volume tickers on the tape. All three HM-BK scanners are LONG-ONLY
# directional confirmers (ORB breakout, aVWAP reclaim, box breakout), so these
# products are wrong twice over: (a) an inverse-ETF "BULL" signal (SOXS/TZA/NVD/
# SCO up) means the underlying is CRASHING — opposite of a long confirm; (b)
# 2x/3x products are noisy/mean-reverting and their volume spikes (34x seen on
# SOXS/TZA in ORB) trip the vol gate and skew the sample. Filter them out of the
# scan universe BEFORE signal capture, in every BK scanner.
# ONE SOURCE OF TRUTH: BK_EXCLUDE_LEVERAGED_INVERSE is canonical — HM-BK-A/B/C
# all reference it (ORB via the ORB_* alias below, unchanged; B/C import the
# canonical name). SCOPE = these scanners only: e.g. TQQQ stays in
# FIXED_WATCHLIST for other strategies, just skipped for BK breakouts.
# Seeded with common Direxion/ProShares 2x/3x bull+bear products, plus the
# leveraged/inverse names the live universe surfaced (TSLL 1.5x TSLA, NVD inverse
# NVDA, UVIX 2x VIX, SCO inverse crude). Extend freely (config constant).
BK_EXCLUDE_LEVERAGED_INVERSE = frozenset({
    "SOXL", "SOXS", "TNA", "TZA", "TQQQ", "SQQQ", "UPRO", "SPXU", "UDOW",
    "SDOW", "SPXL", "SPXS", "FAS", "FAZ", "LABU", "LABD", "YINN", "YANG",
    "TECL", "TECS", "NUGT", "DUST", "JNUG", "JDST", "SSO", "SDS", "QLD", "QID",
    "TSLL", "NVD", "UVIX", "SCO",
})
# Back-compat alias — HM-BK-A bk_orb_scanner imports this name; same set object.
ORB_EXCLUDE_LEVERAGED_INVERSE = BK_EXCLUDE_LEVERAGED_INVERSE

# 13F institutional-flow (SEC EDGAR) confirmatory vote — slow/structural macro-context
# voter on the 8a83f17 rail (never originates, MIN_FLEET_VOTES=2). Default OFF / shadow.
INSTITUTIONAL_13F_CONFIRMATORY_VOTE_ENABLED = False  # engine/institutional_13f_signal.py

# === SWINGDESK-W3 — agent auto-spreads (BUILT GATED-OFF, 2026-06-10) ===========
# Lets qualifying fleet agents propose/submit multi-leg spreads through the W2
# executor. NOTHING submits until burn-in passes AND the Admiral flips the master
# gate. The gate is enforced at ONE chokepoint (engine/auto_spread.py::
# submit_if_allowed). Flipping requires editing THIS line + a canonical restart —
# there is NO API flip path by design. Alpaca PAPER only; RULE #1 unchanged.
AUTO_SPREADS_ENABLED = False               # MASTER GATE. Config-edit + restart only.
# Eligible agent_ids (ships EMPTY — whitelisting an agent is a separate Admiral act).
AUTO_SPREAD_WHITELIST: list = []
# Strategies the Admiral has ATTESTED passed SUPER_MAX validation (DSR>=0.95 AND
# PBO<=0.30 AND Sharpe) — DSR/PBO is report-only with no automated registry, so
# eligibility is attested here. Ships EMPTY. (relative_strength can NEVER be added;
# it is hardcoded-excluded in engine/auto_spread.py regardless of this list.)
AUTO_SPREAD_VALIDATED_STRATEGIES: list = []
AUTO_SPREAD_MAX_DEBIT_PER_TRADE = 500.0    # $ net debit ceiling per spread
AUTO_SPREAD_MAX_OPEN = 5                    # max concurrent open auto-spreads
AUTO_SPREAD_MAX_NEW_PER_DAY = 3            # max new auto-spreads opened per day
AUTO_SPREAD_MAX_TOTAL_DEBIT = 2500.0       # max total open auto-spread debit ($)

# === HM-ALERT-COLLAB-LINKS Phase 1 — user alert definitions (2026-07-06) =====
# Gates engine/dynamic_alerts.py's alert_definitions reader (unions user-defined
# alerts with the existing hardcoded checks; hardcoded stays default-on regardless
# of this flag). False = reader is a no-op, table can exist with zero behavior
# change. Flip after smoke-restart verification. drafts/HM-ALERT-COLLAB-LINKS.md
# has the full plan; Phase 2 (encode/decode share links) is separate and unbuilt --
# this flag only covers the Phase 1 alert-definition model + CRUD.
# HMAC key for Phase 2 share-link signing lives at
# ~/.config/ollietrades/alert_link_secret (chmod 600, not in this repo) -- not
# read by anything yet, prepped ahead per Q3 of the Admiral's ruling.
# FLIPPED 2026-07-07 (Admiral GO, after-close bundled restart).
ALERT_DEFS_ENABLED = True

# HM-BUG-BATCH-2026-07-09 item 8: market-hours gate for user-facing
# TRADING-SIGNAL alert emission (engine.market_calendar.is_within_alert_hours(),
# used by engine/dynamic_alerts.py). Ops/health sentinel alerts are exempt --
# always fire 24/7 regardless of this window. Values are ET hour-of-day floats
# (9.5 == 9:30 AM). Default is regular session only; widen to (4.0, 20.0) to
# also allow pre-market/extended-hours signals through.
TRADING_ALERT_HOURS_ET = (9.5, 16.0)

AUTO_SPREAD_MIN_CONVICTION = 8.0           # proposing-agent conviction floor (>= 8)

# Tickers confirmed delisted/halted — excluded from all scan universes
DELISTED_BLACKLIST: set[str] = {
    "XCEM", "EAOA", "YFYA", "BULZ", "TDWDR", "TWLVR", "UCFIW", "VSTA",
    "WTGUR", "WSTNR", "WHLRL", "YHNAR", "CHARR", "CHPGR", "CCXIW", "EMISR",
    "EURKR", "FVNNR", "ASPCR", "ESHAR", "NOEMR",
    "MCW",  # HM-MCW-PHANTOM 2026-06-15: suspended ~05-19, no Alpaca bars. Belt;
            # the tradability gate (full_universe.tradable_symbols) auto-prunes it + HOLX/CTRA/etc too.
}

# HM-AQ-β 2026-05-07: WATCH_STOCKS constant removed. Dynamic universe is
# the source of truth, populated weekly by engine/universe_refresh.py and
# read via engine/universe.py::get_active_universe(). See docs/UNIVERSE.md.
# Direct callers should `from engine.universe import get_active_universe`.
# Legacy callers using config.get_effective_watchlist() continue to work
# (it now delegates to engine.universe with extras-overlay preserved).


def get_effective_watchlist() -> list:
    """Dynamic universe (engine.universe.get_active_universe) + extras overlay.

    HM-AQ-β 2026-05-07: was `list(WATCH_STOCKS) + extras`; now
    `get_active_universe() + extras`. The extras overlay from
    data/watchlist_extras.json is preserved for manual ad-hoc additions
    that aren't yet in the screener-derived universe.

    Defensive fallback: if engine.universe import fails, return a hardcoded
    20-name mega-cap list so callers stay operational. Refresher itself
    NTFYs on its own failures (HM-U posture).
    """
    try:
        from engine.universe import get_active_universe
        base = get_active_universe()
    except Exception:
        # Defensive fallback — same 20-name list that engine.universe
        # uses internally as _FALLBACK_UNIVERSE. Kept here in case the
        # engine.universe import itself fails (e.g., during a partial
        # deploy where the module isn't yet on PYTHONPATH).
        base = ["SPY", "QQQ", "TQQQ", "NVDA", "TSLA", "AAPL", "AMD", "META",
                "MSFT", "GOOGL", "AMZN", "MU", "ORCL", "NOW", "AVGO", "PLTR",
                "DELL", "XLE", "INTC", "NUKZ"]
    try:
        import json as _j
        with open("data/watchlist_extras.json") as _f:
            extras = _j.load(_f)
        for sym in extras.get("symbols", []):
            if sym and sym not in base:
                base.append(sym)
    except (FileNotFoundError, ValueError):
        pass
    return base

# Mr. Dalio's All Weather universe — bonds, gold ETF, commodities
# All four are GATE_EXEMPT in quality_gate.py (no earnings/revenue metrics for macro assets)
DALIO_SYMBOLS = ["TLT", "IEF", "GLD", "GSG", "DJP"]
DALIO_BOND_SYMBOLS = {"TLT", "IEF"}  # stored as asset_type='bond' in paper_trader

# AI Provider Keys
OLLAMA_MODEL = "phi3:mini"
OLLAMA_URL = "http://192.168.1.168:11434"          # Ollie Box (all heavy inference — 2026-04-24 routing fix)
OLLAMA_LOCAL_URL = "http://localhost:11434"        # bigmac residents only (phi3/gemma3/mistral)
OLLIE_URL  = "http://192.168.1.168:11434"          # Ollie Max — RTX 5080 16GB VRAM + 32GB sys RAM (Admiral-confirmed 2026-05-30; was mislabeled "RTX 5060")
# HM-PERF-FLEET-THROUGHPUT 2026-07-07: Ollie Max co-resides TWO 7-8B-class
# models (~10-12GB together, live /api/ps-confirmed) with server-side
# NUM_PARALLEL=2 -- the client-side queue (engine/ollama_queue.py) was still
# serializing to exactly 1 concurrent inference, premised on the old (wrong)
# "one model fits" assumption. Feature-flagged: set to 1 to roll back to the
# prior fully-serial behavior with zero other code changes.
OLLAMA_QUEUE_WORKERS = int(os.environ.get("OLLAMA_QUEUE_WORKERS", "2"))
# HM-PERF-FLEET-THROUGHPUT 2026-07-07 (post-restart follow-up): the first
# verification pass (after-hours, 6+ distinct models rotating against only
# 2 resident slots) showed swap rate UP 19.8%, not down -- working theory:
# worker 1+ picking up a DIFFERENT model than worker 0 is currently running
# increases model diversity in flight rather than pure same-model
# concurrency. Strict-affinity mode is the surgical fix if that theory
# holds under market-hours judgment (Admiral decision rule: judge on the
# scan-window same-model burst, not the evening mixed-model traffic this
# first pass happened to catch). Default OFF -- current shipped 2-worker
# behavior is unchanged unless this is explicitly flipped on.
OLLAMA_QUEUE_STRICT_AFFINITY = os.environ.get("OLLAMA_QUEUE_STRICT_AFFINITY", "0") == "1"
MLX_URL = "http://localhost:8899"
MLX_MODEL = "mlx-community/Qwen3-8B-4bit"
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_CODEX_MODEL = os.environ.get("OPENAI_CODEX_MODEL", "gpt-5.2-codex")
OPENAI_CODEX_MINI_MODEL = os.environ.get("OPENAI_CODEX_MINI_MODEL", OPENAI_CODEX_MODEL)
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GROK_API_KEY = os.environ.get("GROK_API_KEY", "")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "")
ALPHA_VANTAGE_KEY = os.environ.get("ALPHA_VANTAGE_KEY", "")
FRED_API_KEY = os.environ.get("FRED_API_KEY")
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "")  # Polygon.io — activates when key is added

# Alpaca Broker — canonical APCA_* names (paper trading only)
APCA_API_KEY_ID     = os.environ.get("APCA_API_KEY_ID", "")
APCA_API_SECRET_KEY = os.environ.get("APCA_API_SECRET_KEY", "")
ALPACA_API_KEY      = APCA_API_KEY_ID      # legacy alias — use APCA_* in new code
ALPACA_SECRET_KEY   = APCA_API_SECRET_KEY  # legacy alias — use APCA_* in new code

# Webull Broker
WEBULL_APP_KEY = os.environ.get("WEBULL_APP_KEY", "")
WEBULL_APP_SECRET = os.environ.get("WEBULL_APP_SECRET", "")
WEBULL_ACCOUNT_ID = os.environ.get("WEBULL_ACCOUNT_ID", "")

# Trading Rules
STARTING_CASH = 7000.00
POSITION_SIZE_PCT = 0.10
MAX_POSITIONS = 5  # HM-AGENT-RULES-CONSOLIDATION 2026-07-04: was 8, canonical everywhere now (bear=3, see risk_manager.NORMAL_MAX_POSITIONS/BEAR_MAX_POSITIONS)
STOP_LOSS_PCT = 0.05
TAKE_PROFIT_TIERS = [  # sell % of remaining position at each tier
    (0.05, 0.50),  # +5% profit → sell 50% — lock in gains early
    (0.10, 0.25),  # +10% profit → sell 25% of remaining
    (0.15, 0.50),  # +15% profit → sell 50% of remaining
    (0.25, 0.50),  # +25% profit → sell 50% of remaining
    (0.50, 1.00),  # +50% profit → sell everything left
]
MAX_POSITION_PCT = 0.30  # Canonical absolute cap (HM-AGENT-RULES-CONSOLIDATION 2026-07-04) — 30% for high-conviction (0.85+) positions
MAX_DRAWDOWN_PCT = 0.20
MIN_CASH_RESERVE_PCT = 0.20  # HM-AGENT-RULES-CONSOLIDATION 2026-07-04: was 0.15, canonical everywhere now (bear=0.35, see risk_manager.BEAR_MIN_CASH_PCT)
MAX_DAILY_TRADES = 30
OPTIONS_MAX_PCT = 0.05  # max 5% of portfolio per call/put position
OPTIONS_TOTAL_MAX_PCT = 0.10  # max 10% total options exposure (reduced from 20% to limit losses)
OPTIONS_DEFAULT_DTE = 30  # target days to expiry for arena options (not 0DTE)
OPTIONS_MIN_DTE = 7  # minimum acceptable DTE
OPTIONS_AUTO_CLOSE_DTE = 1  # auto-close options this many days before expiry
OPTIONS_STOP_LOSS_PCT = 0.50  # exit option if premium drops 50% from entry
OPTIONS_PREFER_ITM = True  # prefer ATM/slightly ITM strikes over OTM

# HM-TROI-GUARDRAILS-TRIM 2026-07-04: blocks NEW Troi CSP opens while the
# shared options book's CSP notional exceeds OPTIONS_TOTAL_MAX_PCT of the
# book's current_cash (engine.risk_manager.csp_options_cap_breached()).
# Existing open positions are never force-closed by this flag. Default ON —
# HM-TROI-MAXPOS-CAP-DEAD found this cap was silently non-functional (wrong
# source table) with 48 open CSPs / ~$1.3M notional already on the book.
TROI_CSP_CAP_GATE = True

# Telegram Alerts
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Dashboard
DASHBOARD_HOST = "0.0.0.0"
DASHBOARD_PORT = 8080

# Scan Intervals (seconds) — Dilithium Crystal Protocol v3
# v3 (2026-03-23): Widened to cut API costs ~60%. $77/mo → $30-35/mo target.
SCAN_INTERVAL_MARKET = 300      # Regular hours (9:30 AM - 3:00 PM ET) = 5 min
SCAN_INTERVAL_POWER_HOUR = 300  # Power hour (3:00 - 4:30 PM ET) = 5 min (matches market — sequential Ollama needs time)
SCAN_INTERVAL_EXTENDED = 900    # Pre-market & after-hours = 15 min (was 10 min)
SCAN_INTERVAL_OVERNIGHT = 1800  # Overnight 5PM-2AM MST = 30 min
SCAN_INTERVAL_WEEKEND = 3600    # Weekends = 1 hour
SCAN_INTERVAL_SECONDS = 300     # Default (legacy fallback)

# API Cost Budgets
DAILY_API_BUDGET = 5.00    # Hard limit — pause cloud scanning above this
DAILY_COST_WARNING = 4.00  # Warn threshold
MONTHLY_API_BUDGET = 35.00 # Soft limit — switch to economy mode
FREE_CALLS_DAILY_LIMIT = int(os.environ.get("FREE_CALLS_DAILY_LIMIT", "1000"))

# === HM-ROSTER-CAP-2026-07-04 — hard roster ceiling + audition gate =========
# Roster quality is enforced at the door, not by periodic culls (see
# docs/DOCTRINE.md, "Doctrine Lessons"). Enforced in setup_db.py's startup
# roster check (same mechanism that reverts runtime model_id edits).
MAX_ACTIVE_AGENTS = 8  # hard ceiling on concurrently halt_mode='active' agents

# No agent may activate or reactivate without clearing all four bars below,
# measured on clean-window data only (>= clean_window_start). Auditions run
# in tracking/shadow mode (signals logged, gated from execution) so a failed
# audition costs nothing. weekly_tuning_crew proposes pass/fail; Admiral
# approves. clean_window_start matches the established data-integrity cutoff
# used elsewhere (engine.trades_filter.GARBAGE_FLOOR,
# engine.crew.weekly_tuning_crew.CLEAN_CUTOFF) — same date, not a new one.
AUDITION_CRITERIA = {
    "clean_window_start": "2026-05-14",
    "min_guarded_trades": 20,
    "max_spam_rate_pct": 30.0,
    "min_honest_guarded_return_pct": 0.0,   # must be strictly positive
    "max_friction_to_pnl": 0.15,
}
# === /HM-ROSTER-CAP-2026-07-04 ===============================================

# AI Arena Players
#
# ⚠️ HM-MODEL-CONFIG-STALENESS (2026-05-30): the `model` field below is the INITIAL /
# informational value ONLY. The CANONICAL runtime model is `ai_players.model_id` (DB),
# enforced on startup per HM-BN doctrine (main.py:121). For ~10 agents the live runtime
# model differs from the `model` here — read the DB, NOT this file, for the live model.
# Confirmed mismatches (config → DB): ollama-plutus plutus-v1→0xroyce/plutus,
# ollama-qwen3 qwen3:8b→ministral-3:3b, ollama-llama llama3.1→qwen3:8b, ollama-local
# qwen3:8b→gemma3:4b, ollama-gemma27b qwen3:8b→ministral-3:3b, ollama-kimi
# kimi-k2.5→ministral-3:3b, qwen3-14b-grok3 qwen3:14b→qwen3:8b, qwen3-8b-4o→qwen3:8b,
# qwen3-8b-o3 o3→qwen3:8b. (NOT synced to DB here because some DB model_ids are garbage
# placeholders for no-LLM rule agents — e.g. neo-matrix model_id='8000 / Independent' —
# and syncing would propagate them. The DB is the runtime source; this comment is the fix.)
AI_PLAYERS = [
    {"id": "ollama-local",    "name": "Pike/Kirk 14B",      "provider": "ollama", "model": "qwen3:8b",         "url": OLLIE_URL},  # Ollie GPU
    {"id": "ollama-gemma27b", "name": "Qwen3 8B",           "provider": "ollama", "model": "qwen3:8b",          "url": OLLIE_URL},  # Ollie GPU — was qwen3:14b
    {"id": "ollama-deepseek", "name": "Spock R1 14B",       "provider": "ollama", "model": "deepseek-r1:14b",   "url": OLLIE_URL},  # Ollie GPU
    {"id": "ollama-qwen3",    "name": "Lt. Jadzia Dax",     "provider": "ollama", "model": "qwen3:8b",          "url": OLLIE_URL},  # Ollie GPU (model qwen3:8b here is deliberately-unsynced legacy; DB=ministral-3:3b is runtime truth per config.py:285)
    {"id": "ollama-coder",    "name": "Lt. Cmdr. Data",     "provider": "ollama", "model": "qwen2.5-coder:7b",  "url": OLLIE_URL},  # Ollie GPU — was qwen3-coder:30b
    {"id": "ollama-plutus",   "name": "Uhura Plutus",       "provider": "ollama", "model": "plutus-v1",    "url": OLLIE_URL},  # Ollie GPU — McCoy's finance brain
    {"id": "navigator",       "name": "Ensign Chekov",       "provider": "ollama", "model": "qwen3:8b",          "url": OLLIE_URL},  # Ollie GPU — backtest routing; live uses chekov_rules()
    {"id": "neo-matrix",      "name": "Neo Matrix",          "provider": "ollama", "model": "qwen3:14b",         "url": OLLIE_URL},  # 2026-04-23: rerouted to Ollie Box, freed bigmac RAM (Ollie Box has 32GB RAM, handles qwen3:14b)
    {"id": "ollama-llama",    "name": "Llama 3.1 8B",       "provider": "ollama", "model": "llama3.1:latest"},                      # bigmac localhost fallback
    {"id": "ollama-kimi",     "name": "Kimi K2.5",          "provider": "ollama", "model": "kimi-k2.5:cloud"},                      # cloud — unchanged
    {"id": "mlx-qwen3",       "name": "Qwen3 8B MLX",       "provider": "ollama", "model": "ministral-3:3b",            "url": OLLIE_URL},  # HM-BN.1 2026-05-17: was provider=mlx/Qwen3-8B-4bit; aligned to ai_players canonical ministral-3:3b on Ollie
    # HM-CN Phase 2 2026-05-17: local_redirect=True honors Free-Models-First
    # doctrine — these provider="openai" declarations route to local Ollama
    # using ai_players.model_id (which has been set to a free local model).
    # Without this flag, agent_routing.py would skip them.
    {"id": "qwen3-8b-sonnet",   "name": "Codex Prime",        "provider": "openai", "model": OPENAI_CODEX_MODEL,      "local_redirect": True},
    {"id": "qwen-coder-haiku",    "name": "Codex Scout",        "provider": "openai", "model": OPENAI_CODEX_MINI_MODEL, "local_redirect": True},
    {"id": "qwen3-8b-4o",          "name": "GPT-4o",             "provider": "openai", "model": "qwen3-8b-4o",         "local_redirect": True},
    {"id": "qwen3-8b-o3",          "name": "GPT-o3",             "provider": "openai", "model": "o3",                   "local_redirect": True},
    {"id": "qwen3-14b-pro",  "name": "Dalio Macro 8B",     "provider": "ollama", "model": "qwen3:8b",         "url": OLLIE_URL},  # HM-BE: name aligned with model (was "14B"; downgraded to 8B on 2026-04-20 swap-storm cleanup)
    {"id": "qwen3-8b-flash","name": "Worf 8B",            "provider": "ollama", "model": "qwen3:8b",          "url": OLLIE_URL},  # Ollie GPU — was gemini
    {"id": "qwen3-14b-grok3",          "name": "ex-Grok3 14B",       "provider": "ollama", "model": "qwen3:14b",         "url": OLLIE_URL},  # Ollie GPU — retired 2026-04-16
    {"id": "deepseek-7b-grok4",          "name": "ex-Grok4 8B",        "provider": "ollama", "model": "qwen3:8b",          "url": OLLIE_URL},  # Ollie GPU — retired 2026-04-16
]
