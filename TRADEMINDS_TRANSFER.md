# TRADEMINDS AUTONOMOUS TRADER — PROJECT TRANSFER DOCUMENT
**Generated:** 2026-03-27
**Purpose:** Complete handoff to another AI coding tool
**For:** Anyone taking over this codebase cold

---

## TABLE OF CONTENTS
1. [Architecture Overview](#1-architecture-overview)
2. [Ports & Servers](#2-ports--servers)
3. [Launch Commands](#3-launch-commands)
4. [Directory Structure](#4-directory-structure)
5. [All AI Players](#5-all-ai-players)
6. [Database Tables](#6-database-tables)
7. [API Keys Required](#7-api-keys-required)
8. [API Routes](#8-api-routes)
9. [Design Rules](#9-design-rules)
10. [Known Bugs & Fixes Applied](#10-known-bugs--fixes-applied)
11. [Configuration & Risk Params](#11-configuration--risk-params)
12. [Scheduler & Cron Jobs](#12-scheduler--cron-jobs)
13. [Key Engine Files](#13-key-engine-files)
14. [Sacred Rules — Never Break These](#14-sacred-rules--never-break-these)

---

## 1. ARCHITECTURE OVERVIEW

TradeMinds is an autonomous AI trading arena where 14+ AI models ("crew members") paper-trade against each other and a human benchmark. It is NOT a real money trading system (except the Alpaca paper account and the human's real Webull portfolio which is display-only).

```
┌─────────────────────────────────────────────────────────┐
│                    TRADEMINDS STACK                     │
├────────────────────┬────────────────────────────────────┤
│  PORT 8080         │  PORT 8000                         │
│  Arena Server      │  CrewAI Strategy System            │
│  main.py           │  main_crew.py                      │
│  dashboard/app.py  │  crew/ module                      │
│                    │                                    │
│  Serves:           │  Serves:                           │
│  - FastAPI REST    │  - FastAPI REST                    │
│  - Vanilla HTML UI │  - CrewAI pipeline                 │
│    (NOT React)     │  - Strategy generation             │
│    index.html      │  - Alpaca auto-trading             │
├────────────────────┴────────────────────────────────────┤
│                  DATA LAYER                             │
│  SQLite WAL — data/trader.db (54 tables)               │
│  SQLite WAL — data/arena.db  (empty, unused)           │
│  SQLite WAL — data/scanner.db (universe scans)         │
│  JSON cache — data/leaderboard_cache.json              │
├────────────────────────────────────────────────────────┤
│                  AI PROVIDERS                           │
│  Local:  Ollama @ localhost:11434                      │
│          Models: gemma3:4b, gemma3:27b, deepseek,      │
│                  qwen3, llama3.1, kimi, glm4, plutus   │
│  Cloud:  Claude (Anthropic), GPT-4o/o3 (OpenAI),      │
│          Gemini 2.5 Flash/Pro (Google), Grok 3/4 (xAI) │
│  Custom: DayBlade (0DTE options scalper)               │
│          CrewAI collective (super-agent)               │
├────────────────────────────────────────────────────────┤
│                  MARKET DATA                           │
│  yfinance — prices, technicals, options chains         │
│  OpenBB 4.4 — SEC insider, OECD macro, FOMC docs       │
│  Finnhub — insider sentiment, news, earnings           │
│  Alpha Vantage — company overviews, technicals         │
│  CBOE — GEX (gamma exposure) via Alpaca               │
│  Alpaca API — paper trading + GEX snapshots           │
└────────────────────────────────────────────────────────┘
```

### CRITICAL: The UI at port 8080 is vanilla HTML — NOT React

`~/autonomous-trader/dashboard/static/index.html` is a **single 14,387-line vanilla HTML/JS file**. It is served by FastAPI's StaticFiles mount. DO NOT edit `.jsx` files to change port 8080. React files exist at `dashboard/frontend/src/` but are NOT used by the main dashboard.

---

## 2. PORTS & SERVERS

| Port | File | Purpose |
|------|------|---------|
| 8080 | `main.py` → `dashboard/app.py` | Arena dashboard + all trading APIs |
| 8000 | `main_crew.py` | CrewAI strategy pipeline + Alpaca auto-trading |
| 11434 | Ollama (external) | Local LLM inference |

**URL:** `http://localhost:8080` — vanilla HTML dashboard
**Docs:** `http://localhost:8000/docs` — CrewAI API docs

---

## 3. LAUNCH COMMANDS

### Full System (recommended):
```bash
cd ~/autonomous-trader
./launch-trademinds.sh          # Full dev mode (servers + Claude Code)
./launch-trademinds.sh --servers # Servers only, no Claude
./launch-trademinds.sh --crew    # CrewAI pipeline mode
```

### Manual (what the script does internally):
```bash
cd ~/autonomous-trader

# Arena server (port 8080)
./venv/bin/python main.py > logs/arena.log 2>&1 &

# CrewAI server (port 8000)
./.venv-crew/bin/python main_crew.py > logs/crew.log 2>&1 &
```

### Python Environments:
- **Arena:** `./venv/` — Python 3.9, yfinance, FastAPI, OpenBB
- **Crew:** `./.venv-crew/` — Python 3.12, crewai, FastAPI

> **WARNING:** Do NOT use system Python or the wrong venv. Running `dashboard/app.py` with system Python will fail with `ModuleNotFoundError: No module named 'itsdangerous'`.

### Log Files:
- `logs/arena.log` — Arena server stdout/stderr
- `logs/crew.log` — CrewAI server stdout/stderr
- `logs/dashboard.log` — Dashboard-only restarts
- `logs/trader.log` — Trading engine
- `logs/premarket.log` — Pre-market scan

---

## 4. DIRECTORY STRUCTURE

```
~/autonomous-trader/
├── main.py                    # Arena server entrypoint (port 8080)
├── main_crew.py               # CrewAI server entrypoint (port 8000)
├── config.py                  # Watchlist, API keys, risk params, cost budgets
├── setup_db.py                # DB schema + player seed data
├── launch-trademinds.sh       # Startup script
│
├── data/
│   ├── trader.db              # PRIMARY database (54 tables)
│   ├── arena.db               # Empty/unused
│   ├── scanner.db             # Universe scan results
│   └── leaderboard_cache.json # Disk cache for leaderboard API
│
├── dashboard/
│   ├── app.py                 # 7,800+ line FastAPI app (180+ routes)
│   ├── static/
│   │   └── index.html         # THE UI — 14,387-line vanilla HTML/JS ← EDIT THIS
│   └── frontend/              # React+Vite (unused at port 8080)
│       └── src/App.jsx        # DO NOT edit for port 8080 changes
│
├── engine/
│   ├── paper_trader.py        # Buy/sell execution, cash updates, positions
│   ├── market_data.py         # yfinance price/indicator fetching
│   ├── dayblade.py            # 0DTE options scalper
│   ├── autopilot.py           # RSI-based profit taking, rebalancing
│   ├── ai_brain.py            # AI scan orchestration
│   ├── openbb_data.py         # OpenBB data layer
│   ├── metals_tracker.py      # Physical gold/silver tracker
│   ├── season_manager.py      # Season rotation
│   ├── risk_manager.py        # Position sizing, drawdown limits
│   ├── squeeze_scanner.py     # Short squeeze scanner
│   └── providers/
│       ├── base.py            # Base AI class + MODEL_PERSONALITIES dict
│       ├── ollama_provider.py # Ollama inference
│       ├── claude_provider.py # Anthropic Claude
│       ├── openai_provider.py # OpenAI GPT
│       ├── gemini_provider.py # Google Gemini
│       ├── grok_provider.py   # xAI Grok
│       └── dalio_provider.py  # All-Weather portfolio AI
│
├── crew/                      # CrewAI multi-agent system
│   ├── pipeline.py            # Main pipeline orchestration
│   ├── agents.py              # Agent definitions
│   ├── tasks.py               # Task definitions
│   └── learning.py            # Learning from closed positions
│
├── migrations/
│   └── 001_crew_and_portfolios.py   # Required migration (run once)
│
├── logs/                      # Runtime logs
├── venv/                      # Python 3.9 venv (Arena)
└── .venv-crew/                # Python 3.12 venv (CrewAI)
```

---

## 5. ALL AI PLAYERS

**Database:** `SELECT id, display_name, cash, provider FROM ai_players ORDER BY id;`

| player_id | Display Name | Cash (live) | Provider | Notes |
|-----------|-------------|-------------|----------|-------|
| `claude-haiku` | Claude Haiku 4.5 | $7,000.00 | anthropic | Season 4, full reset |
| `claude-sonnet` | Claude Sonnet 4 | $7,000.00 | anthropic | Season 4, full reset |
| `cto-grok42` | CTO Grok 4.2 | $7,000.00 | xai | Hidden from main leaderboard |
| `dalio-metals` | Mr. Dalio | $3,400.40 | google | All-weather portfolio; see Dalio Desync bug |
| `dayblade-0dte` | DayBlade Options | $3,500.00 | dayblade | 0DTE scalper, starting capital $3,500 |
| `dayblade-sulu` | Lt. Sulu | $5,241.44 | ollama | Intraday day trader, no overnight |
| `energy-arnold` | Cmdr. Trip Tucker | $5,044.89 | ollama | Energy sector specialist |
| `enterprise-computer` | Computer | $0.00 | system | Physical metals tracker (read-only, display) |
| `gemini-2.5-flash` | Lt. Cmdr. Worf | $5,182.74 | google | CAN SLIM enforcer, risk-off bias |
| `gemini-2.5-pro` | Gemini 2.5 Pro | $7,000.00 | google | Season 4, full reset |
| `gpt-4o` | GPT-4o | $7,000.00 | openai | Season 4, full reset |
| `gpt-o3` | GPT-o3 | $7,000.00 | openai | Season 4, full reset |
| `grok-3` | Grok 3 | $7,000.00 | xai | Season 4, full reset |
| `grok-4` | Lt. Cmdr. Spock | $6,299.99 | xai | Science Officer, pure logic |
| `navigator` | Ensign Chekov | $6,320.68 | system | Deep analysis navigator |
| `ollama-deepseek` | DeepSeek R1 14B | $7,000.00 | ollama | Season 4, full reset |
| `ollama-gemma27b` | Gemma3 27B | $7,000.00 | ollama | Season 4, full reset |
| `ollama-glm4` | GLM4 9B | $7,000.00 | ollama | Season 4, full reset |
| `ollama-kimi` | Kimi K2.5 | $7,000.00 | ollama | Season 4, full reset |
| `ollama-llama` | Lt. Cmdr. Uhura | $7,000.00 | ollama | Season 4, full reset |
| `ollama-local` | Lt. Cmdr. Geordi | $7,000.00 | ollama | Mean reversion specialist |
| `ollama-plutus` | Dr. McCoy | $7,000.00 | ollama | 6-vital-sign quant doctor |
| `ollama-qwen3` | Lt. Cmdr. Scotty | $7,000.00 | ollama | Event-driven, catalyst only |
| `options-sosnoff` | Counselor Troi | $5,182.74 | google | Options premium seller, empathic |
| `steve-webull` | Captain Kirk | $265.36 | webull | **HUMAN BENCHMARK — SACRED, never auto-trade** |
| `super-agent` | Mr. Anderson | $25,000.00 | crewai | CrewAI collective; starting capital $25,000 |

**Total:** 26 players (14 AI + 1 human + 11 specialized/system)

### Starting Capital by Player Type:
- All standard AI players: **$7,000** (Season 4+)
- `dayblade-0dte`: **$3,500** (lower risk profile)
- `super-agent` (Mr. Anderson): **$25,000** (CrewAI separate budget)
- `steve-webull` (Captain Kirk): **$0** (tracks real Webull positions, not paper cash)
- Historical (Season 1-3): $10,000 per player

---

## 6. DATABASE TABLES

**Database:** `data/trader.db` (SQLite WAL mode, 30s busy timeout, 54 tables)

```sql
-- Run: sqlite3 data/trader.db ".tables"
ai_chat                imbalance_zones        rikers_log
ai_journal             impulse_alerts         season_history
ai_players             kill_switch_log        settings
api_costs              market_news            signals
backtest_history       model_adjustments      sma_signals
backtest_results       model_scores           smart_money_signals
backtest_runs          model_stats            stock_fundamentals
captain_decisions      pair_trades            strategy_backtests
crew_runs              picard_briefings       strategy_optimization
crew_strategies        portfolio_history      strategy_scores
crew_trade_results     portfolio_positions    strategy_signals
cto_briefings          portfolios             theta_opportunities
daily_lessons          positions              trades
discoveries            quorum_votes           universe_scan
dynamic_alerts         rallies_alerts         volatility_breakouts
flow_lean_history      rallies_models         war_room
gap_scanner            rallies_trades         watchlist_signals
gemini_failover        reference_discussions  weekly_picks
gex_snapshots          reference_trades
ghost_trades           regime_history
```

### Critical Table Schemas:

**`ai_players`** — The crew roster:
```sql
id TEXT PRIMARY KEY,        -- e.g. 'claude-sonnet'
display_name TEXT,          -- e.g. 'Claude Sonnet 4'
provider TEXT,              -- 'anthropic','openai','google','xai','ollama','webull','crewai','system'
model_id TEXT,              -- actual model string used in API calls
cash REAL DEFAULT 7000,     -- current available cash
is_active INTEGER DEFAULT 1,
is_halted INTEGER DEFAULT 0, halt_reason TEXT,
is_paused INTEGER DEFAULT 0,
is_human INTEGER DEFAULT 0, -- GUARD: prevents auto-trading
options_enabled INTEGER DEFAULT 0,
season INTEGER DEFAULT 1
```

**`trades`** — All executed trades:
```sql
id INTEGER PRIMARY KEY,
player_id TEXT NOT NULL,    -- FK to ai_players.id
symbol TEXT NOT NULL,
action TEXT NOT NULL,       -- 'BUY','SELL','SHORT','COVER'
qty REAL,
price REAL,
asset_type TEXT DEFAULT 'stock',  -- 'stock','option'
option_type TEXT,           -- 'call','put' (options only)
strike_price REAL,
expiry_date TEXT,
reasoning TEXT,
confidence REAL,
executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
exit_price REAL,
realized_pnl REAL,
entry_price REAL,
season INTEGER DEFAULT 1,
sources TEXT DEFAULT '',
timeframe TEXT DEFAULT 'SWING'  -- 'SCALP','SWING','POSITION'
```

**`positions`** — Open positions:
```sql
id INTEGER PRIMARY KEY,
player_id TEXT NOT NULL,
symbol TEXT NOT NULL,
qty REAL,                   -- NEGATIVE for short positions
avg_price REAL,
asset_type TEXT DEFAULT 'stock',
option_type TEXT, strike_price REAL, expiry_date TEXT,
opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
high_watermark REAL
```

**`signals`** — AI recommendations (may or may not have been acted on):
```sql
id INTEGER PRIMARY KEY,
player_id TEXT NOT NULL,
symbol TEXT NOT NULL,
signal TEXT NOT NULL,       -- 'BUY','SELL','HOLD'
confidence REAL,
reasoning TEXT,
asset_type TEXT,
acted_on INTEGER DEFAULT 0,
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
sources TEXT,
timeframe TEXT,
execution_status TEXT DEFAULT 'PENDING',  -- 'PENDING','EXECUTED','REJECTED'
rejection_reason TEXT,
season INTEGER DEFAULT 1
```

**`portfolio_history`** — Equity curve snapshots (every scan):
```sql
id INTEGER PRIMARY KEY,
player_id TEXT NOT NULL,
total_value REAL,
cash REAL,
positions_value REAL,
recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
season INTEGER DEFAULT 1
```

**`settings`** — Key-value config store:
```sql
key TEXT PRIMARY KEY,
value TEXT
-- Includes: 'current_season', 'autopilot_enabled', 'kill_switch_active'
```

**`portfolio_positions`** — Super-Agent (Alpaca) positions:
```sql
id INTEGER PRIMARY KEY,
portfolio_id INTEGER,       -- portfolio_id=1 is Alpaca Paper
symbol TEXT,
quantity REAL,
entry_price REAL,
current_price REAL,
unrealized_pnl REAL,
closed_pnl REAL,
status TEXT,                -- 'open','closed'
```

---

## 7. API KEYS REQUIRED

From `config.py` — all are environment variables (set in `.env`):

| Variable | Service | Required? |
|----------|---------|-----------|
| `CLAUDE_API_KEY` | Anthropic Claude | Yes (for Claude Sonnet/Haiku) |
| `OPENAI_API_KEY` | OpenAI GPT | Yes (for GPT-4o, o3) |
| `GEMINI_API_KEY` | Google Gemini | Yes (for Gemini 2.5 Flash/Pro, Dalio) |
| `GROK_API_KEY` | xAI Grok | Yes (for Grok 3, Grok 4/Spock) |
| `GROQ_API_KEY` | Groq (fast Llama inference) | Optional |
| `FINNHUB_API_KEY` | Finnhub (news, insider, earnings) | Optional but recommended |
| `ALPHA_VANTAGE_KEY` | Alpha Vantage (fundamentals, technicals) | Optional |
| `FRED_API_KEY` | FRED (macro data) | Has hardcoded fallback |
| `POLYGON_API_KEY` | Polygon.io (options chains, bars) | Optional |
| `ALPACA_API_KEY` | Alpaca paper trading | Required for Alpaca section |
| `ALPACA_SECRET_KEY` | Alpaca secret | Required for Alpaca section |
| `WEBULL_APP_KEY` | Webull portfolio sync | Optional (display-only) |
| `WEBULL_APP_SECRET` | Webull auth | Optional |
| `WEBULL_ACCOUNT_ID` | Webull account | Optional |
| `TELEGRAM_BOT_TOKEN` | Telegram alerts | Optional |
| `TELEGRAM_CHAT_ID` | Telegram chat | Optional |

**No-key fallbacks:**
- OpenBB works without keys for: SEC insider_trading, OECD CPI/unemployment/GDP, FOMC docs
- yfinance has no key requirement (prices, options chains)
- All Ollama models are local (no API key)

---

## 8. API ROUTES

**All routes in `dashboard/app.py`** (180+ endpoints). Full list organized by domain:

### Core / Auth
```
GET  /                           → Serves index.html
GET  /login                      → Login page
POST /login                      → Auth handler
GET  /logout                     → Logout
GET  /api/status                 → System status
GET  /api/operations             → Operations summary
```

### Arena & Leaderboard
```
GET  /api/arena/leaderboard      → All players ranked by PnL (HEAVY: bulk Yahoo fetch)
GET  /api/arena/player/{id}      → Single player profile
GET  /api/arena/player/{id}/trades
GET  /api/arena/player/{id}/open-positions
GET  /api/arena/player/{id}/signals
GET  /api/arena/player/{id}/history
GET  /api/arena/player/{id}/pnl
GET  /api/arena/equity-curve     → Multi-player equity curves
GET  /api/arena/confidence       → Confidence distribution
GET  /api/arena/analytics        → Player analytics + correlations
```

### Market Data
```
GET  /api/market/prices          → Current prices (watchlist)
GET  /api/market/candles/{sym}   → OHLCV candles
GET  /api/market/correlation     → 30-day correlation matrix
GET  /api/market/options-flow    → Options flow summary
GET  /api/market/flow-lean       → Smart money flow lean
GET  /api/market/gex             → GEX map (all)
GET  /api/market/gex/{ticker}    → GEX for single ticker
GET  /api/market/vix             → VIX + history
GET  /api/market/vol-surface/{sym} → Volatility surface
GET  /api/market/earnings        → Earnings calendar
GET  /api/market-movers          → Top gainers/losers/active (5 each)
GET  /api/macro                  → Macro overview
GET  /api/macro/context          → Macro context
GET  /api/regime                 → Current market regime (BULL/CAUTIOUS/BEAR/CRISIS)
GET  /api/fear-greed             → Fear & Greed index
GET  /api/breadth                → Market breadth
GET  /api/volume-profile/{sym}   → Volume profile / VWAP zones
GET  /api/sector-heatmap         → Sector heatmap
```

### Options
```
GET  /api/options/greeks         → Greeks for all open options positions
GET  /api/options/theta-burn     → Theta decay schedule
GET  /api/high-iv                → High IV opportunity scanner
GET  /api/put-call-skew/{sym}    → Put/call skew
GET  /api/gex/{sym}              → GEX snapshot for symbol
GET  /api/gex/{sym}/history      → GEX history
GET  /api/theta/opportunities    → Premium-selling setups
POST /api/theta/scan             → Run theta scanner
```

### Trades & Signals
```
GET  /api/trades/recent          → Recent trades (all players)
GET  /api/trades/export          → CSV/JSON export
GET  /api/signals/recent         → Recent signals
GET  /api/signals/with-risk      → Signals with risk levels
GET  /api/signals/with-odds      → Signals with oddsmaker scores
```

### Pre-Market & Gaps
```
GET  /api/premarket-gaps         → Pre-market gap scanner
POST /api/premarket-analyze      → Analyze pre-market setups
GET  /api/gaps/today             → Today's gaps
GET  /api/gaps/history           → Gap history
GET  /api/gaps/stats             → Gap statistics
```

### Fundamental Research
```
GET  /api/fundamentals           → Watchlist fundamentals overview
GET  /api/fundamentals/{sym}     → Single stock fundamentals (P/E, EPS, ROE, etc.)
GET  /api/fundamentals/score/{sym}   → Quality score (0-100)
GET  /api/insider-trades/{sym}   → Insider activity for symbol
GET  /api/insider-alerts         → Insider activity alerts
GET  /api/sec/filings/{sym}      → SEC filings
GET  /api/intelligence/{sym}     → Comprehensive intelligence (all sources)
GET  /api/intelligence/full/{sym} → Full intelligence report
```

### Webull (Human Portfolio)
```
GET  /api/webull-portfolio       → Steve's portfolio snapshot
GET  /api/webull/live            → Live Webull data
GET  /api/webull/synced          → Last sync timestamp
POST /api/webull/sync            → Sync Webull positions to DB
```

### Alpaca Paper Trading
```
GET  /api/alpaca/status          → Account status (equity, cash, buying power)
GET  /api/alpaca/positions       → Open positions
GET  /api/alpaca/orders          → Order history
POST /api/alpaca/buy             → Place buy order
POST /api/alpaca/sell            → Place sell order
POST /api/alpaca/close/{sym}     → Close single position
POST /api/alpaca/close-all       → Nuke all positions
POST /api/alpaca/sync-positions  → Sync to DB
```

### DayBlade (0DTE Scalper)
```
GET  /api/dayblade/status        → Capital, positions, daily P&L
GET  /api/dayblade/trades        → Recent DayBlade trades
GET  /api/dayblade/scanner       → Pre-market gap candidates
GET  /api/dayblade/gap-candidates
```

### Mr. Dalio (Metals)
```
GET  /api/metals/portfolio       → Gold + silver portfolio
GET  /api/metals/signals         → Metals signals
GET  /api/metals/commentary      → Market commentary
GET  /api/metals/prices          → Spot prices (gold, silver)
POST /api/metals/add             → Add to position
POST /api/metals/sell            → Sell metals
POST /api/metals/set-cost        → Set cost basis
```

### War Room & Crew Consensus
```
GET  /api/war-room               → War room posts
POST /api/war-room/post          → Post message
POST /api/war-room/trigger       → Trigger event
POST /api/war-room/hail-q        → Summon Q (debate)
POST /api/war-room/command       → Captain's order
POST /api/war-room/top-picks     → Crew top picks vote
POST /api/war-room/poll          → Poll crew
```

### CTO / Science Officer (Spock)
```
GET  /api/cto/briefing           → Spock's latest briefing
POST /api/cto/generate           → Generate new briefing
GET  /api/first-officer/briefing → First Officer briefing
GET  /api/first-officer/status
POST /api/first-officer/ask      → Ask a question
```

### Riker's Log (Decision Journal)
```
GET  /api/rikers-log             → Decision log entries
POST /api/rikers-log             → Add decision
POST /api/rikers-log/{id}/outcome → Resolve outcome
GET  /api/rikers-log/stats
```

### Backtester / Holodeck
```
POST /api/backtest/run           → Run backtest
GET  /api/backtest/history-for/{id}
GET  /api/backtest/history-leaderboard
POST /api/backtest/save-result
```

### Strategy Lab
```
GET  /api/strategy-lab/strategies → Registered strategies
GET  /api/strategy-lab/latest    → Latest strategy run
GET  /api/strategy-lab/history   → Strategy history
GET  /api/strategy-lab/status/{run_id}
POST /api/strategy-lab/run       → Execute strategy
```

### Congress Intel
```
GET  /api/congress/trades        → Congressional stock trades
GET  /api/congress/overlap       → Crew vs Congress holdings overlap
GET  /api/congress/top-buys      → Top congressional buys
```

### Short Squeeze & Scanners
```
GET  /api/squeeze                → Bollinger squeeze scanner
GET  /api/screener               → Stock quality screener
GET  /api/discoveries            → Discovery alerts
POST /api/discoveries/scan       → Run discovery scan
```

### Seasons & System
```
GET  /api/seasons/history        → All seasons summary
POST /api/seasons/rotate         → Start new season
GET  /api/capital                → Capital allocation (all players)
GET  /api/system/ram             → RAM usage
GET  /api/costs/                 → API cost tracking
POST /api/model-control/pause/{id} → Pause single model
POST /api/model-control/pause-all
POST /api/model-control/force-scan
```

### New Compatibility Aliases (added 2026-03-27):
```
GET  /api/correlation            → 307 → /api/market/correlation
GET  /api/economy                → 307 → /api/macro
GET  /api/greeks                 → 307 → /api/options/greeks
GET  /api/options-flow           → 307 → /api/market/options-flow
GET  /api/premarket              → 307 → /api/premarket-gaps
GET  /api/short-squeeze          → 307 → /api/squeeze
GET  /api/webull/portfolio       → 307 → /api/webull-portfolio
GET  /api/pairs                  → 307 → /api/pair-trades
GET  /api/strategy-lab           → 307 → /api/strategy-lab/strategies
GET  /api/webull/positions       → 200 {"status":"coming_soon"}
GET  /api/vol-surface            → 200 {"status":"coming_soon","message":"Use /api/market/vol-surface/{symbol}"}
GET  /api/gex                    → 200 {"status":"coming_soon","message":"Use /api/market/gex/{ticker}"}
GET  /api/insider-trades         → 200 {"status":"coming_soon","message":"Use /api/insider-trades/{symbol}"}
```

---

## 9. DESIGN RULES

### Star Trek Theme (NON-NEGOTIABLE)
- The dashboard is the **Bridge** of USS TradeMinds
- All AI players are crew members with Star Trek names
- Never break character in chat/war room responses
- Section names: "The Bridge", "War Room", "XO's Ready Room", "Science Officer", etc.
- Terminology: "Dilithium Crystal Protocol" (scan timing), "Warp 9 scan" (full scan), "Beam to Alpaca" (paper trade)

### Colorblind-Safe Color System (CRITICAL)
DO NOT use red/green as the only differentiator. Use:
- **Gains / Positive / Bull:** `#2563eb` (Blue) + ▲ upward arrow
- **Losses / Negative / Bear:** `#ea580c` (Orange) + ▼ downward arrow
- **Neutral / Loading:** `#64748b` (Slate gray)
- **Accent / Brand:** `#6366f1` (Indigo) — var(--accent) in CSS
- **Warning:** `#eab308` (Yellow)
- **Success states that must still work:** use text labels ("+3.2%" not just green color)

This is a strict accessibility requirement. The P&L tracker, leaderboard, and all numeric displays must use blue/orange not green/red.

### UI Framework
- **NO external CSS frameworks** (no Tailwind, no Bootstrap in the HTML file)
- Custom CSS variables in `<style>` at top of index.html
- Key variables: `--bg`, `--surface`, `--border`, `--text`, `--muted`, `--accent`, `--green`, `--red`, `--font-mono`
- Dark theme only (no light mode)
- All charts: Chart.js (loaded via CDN)
- No React, no Vue, no Svelte in index.html

### Dashboard Architecture (index.html)
- Single 14,387-line file — all HTML, CSS, and JS
- Sections are `<div id="section-NAME">` with `display:none` by default
- Section switching: `showSection('NAME')` JS function
- Section init: `registerSectionInit('name', fetchFn, [intervals])` — lazy-loads on first visit
- Fetch dedup cache: 5-second TTL prevents duplicate API calls
- Page-load guard: `_pageLoading=true` blocks non-dashboard API calls during init; set to false at line 13932
- **Dashboard URLs** must be listed in `_dashboardUrls` array or they'll be blocked during page load
- Periodic refresh pattern: `setInterval(fn, ms)` with section-aware pause/resume

### Fetch Cache Gotcha
If you add a new API call to a dashboard function that runs at page load, you MUST add its URL prefix to `_dashboardUrls` array (around line 840 in index.html). Otherwise `_pageLoading=true` will intercept it and return `{}` silently. This caused the Market Movers bug.

---

## 10. KNOWN BUGS & FIXES APPLIED (2026-03-27)

### BUG 1: Dalio Portfolio Desync (FIXED)
**Symptom:** Leaderboard showed `dalio-metals` with cash=$7,000, 0 positions, 0 trades — even though DB had cash=$3,400, 8 positions, 8 trades.

**Root Cause:** Leaderboard SWR (stale-while-revalidate) background refresh was permanently stuck. The `"leaderboard"` key was only added to `_swr_locks` dict AFTER a full computation (line 783), but with a stale disk cache, full computation was never reached. So `_swr_locks.get("leaderboard")` always returned `None`, the background thread never started, and the 14-hour-old cache was served forever.

**Fix applied:**
1. `app.py` line 471: Changed `_swr_locks.get("leaderboard")` → `_swr_locks.setdefault("leaderboard", threading.Lock())` — creates lock before first request, not after
2. `leaderboard()` function: Added `_force: bool = False` parameter; background thread now calls `leaderboard(season=N, _force=True)` which bypasses both in-memory and disk caches to force fresh computation
3. Deleted `data/leaderboard_cache.json` to bust the stale 14-hour cache immediately

**Verification:** After server restart, `/api/arena/leaderboard` computes fresh data. Dalio should show cash=$3,400, 8 positions.

### BUG 2: Market Movers Not Rendering (FIXED)
**Symptom:** `/api/market-movers` returns valid data (5 gainers, 5 losers, 5 active) but the Market Movers card on The Bridge dashboard never shows rows — only "Loading market data..."

**Root Cause:** `fetchMarketMovers()` calls `fetch('/api/market-movers')`. The dashboard has a page-load guard that blocks non-dashboard API calls while `_pageLoading=true`. The `_dashboardUrls` array contained `'/api/market/movers'` (with slash, different path from the actual endpoint `'/api/market-movers'` with hyphen). The `_isDashboardUrl` check uses substring matching, so `'/api/market-movers'.indexOf('/api/market/movers')` returns -1 — NOT a match. First call was blocked, returned `{}`, card showed "Loading...". The 2-minute interval would eventually fix it, but most users never waited.

**Fix applied:** Added `'/api/market-movers'` to `_dashboardUrls` array in index.html (line 843).

### BUG 3: 13 Dead Endpoints Returning 404 (FIXED)
**Symptom:** 13 API paths returned 404.

**Root Cause:** Backend routes existed under canonical paths (e.g. `/api/market/correlation`) but clients expected shorter paths (e.g. `/api/correlation`).

**Fix applied:** Added 13 new routes to `app.py` before `if __name__` block:
- 7 redirect aliases (307) to canonical endpoints
- 6 stub routes returning `{"status":"coming_soon",...}`

See "New Compatibility Aliases" section above for full list.

### BUG 4: Alpaca Section Shows "Connecting to Alpaca..." (FIXED)
**Symptom:** Navigating to "🦙 Alpaca Paper" section never updates from default "Connecting to Alpaca..." text even though `/api/alpaca/status` returns `{connected: true, ...}`.

**Root Cause:** `fetchAlpacaAll` was registered via `registerSectionInit('alpaca', fetchAlpacaAll, ...)` which uses a queue-flush mechanism. If the queue flush fails silently, `_sectionInits['alpaca']` is undefined and `showSection('alpaca')` never calls `fetchAlpacaAll`. The `_sectionFetchMap` (a more reliable backup mechanism) did not have 'alpaca' registered.

**Fix applied:** Added `'alpaca': 'fetchAlpacaAll'` to `_sectionFetchMap` in index.html (line ~6525). The sectionFetchMap fires as a fallback when `_sectionInits[name]` is falsy.

### BUG 5: P&L Tracker Shows +$0 for Mr. Anderson (FIXED)
**Symptom:** The P&L tracker card on The Bridge always shows `▲ +$0 (0.0%)` for Mr. Anderson (super-agent), even though the leaderboard shows total_value=$24,922 (unrealized P&L = -$78).

**Root Cause:** `fetchPnlTracker()` runs at page load as part of `_dashboardFns`. It reads `window.arenaData` to find the player's `total_value`, but `fetchArenaLeaderboard()` (which populates `arenaData`) is async and may not have completed yet. When `arenaData` is null, the code falls back to `current = starting = 25000`, so `pnl = 0`.

**Fix applied:** Added guard at top of `fetchPnlTracker()` in index.html:
```javascript
if (!window.arenaData && typeof fetchArenaLeaderboard === 'function') {
    await fetchArenaLeaderboard();
}
```
This ensures leaderboard data is loaded before computing P&L.

### REMAINING KNOWN ISSUES (not fixed yet)
- **`super-agent` positions in DB:** `data/trader.db` shows no positions for `super-agent`, but the leaderboard computes positions from `portfolio_positions` table (Alpaca paper trades). These are separate — `portfolio_positions` is the source of truth for super-agent.
- **Webull API**: `/api/webull` (bare path) is in `_dashboardUrls` but returns 404. This wastes a request but doesn't break anything (non-ok responses aren't cached).
- **Alpaca position sync error:** `logs/arena.log` shows "Alpaca position sync error: name 'os' is not defined" — a missing `import os` somewhere in the sync code path.

---

## 11. CONFIGURATION & RISK PARAMS

**File:** `config.py`

### Watch Stocks (Primary Trading Universe):
```python
WATCH_STOCKS = [
    "SPY", "QQQ", "NVDA", "TSLA", "AAPL", "AMD", "META", "MSFT",
    "GOOGL", "AMZN", "MU", "ORCL", "NOW", "AVGO", "PLTR", "DELL"
]
```

### Risk Parameters:
| Parameter | Value | Notes |
|-----------|-------|-------|
| `STARTING_CASH` | $7,000 | Per AI player, Season 4+ |
| `POSITION_SIZE_PCT` | 10% | Default position size |
| `MAX_POSITIONS` | 5 | Reduced from 8 in Season 3 to force conviction |
| `STOP_LOSS_PCT` | 5% | Default hard stop |
| `MAX_POSITION_PCT` | 30% | For 0.85+ confidence trades |
| `MAX_DRAWDOWN_PCT` | 20% | Kill-switch threshold |
| `MIN_CASH_RESERVE_PCT` | 15% | Minimum cash floor |
| `MAX_DAILY_TRADES` | 30 | League-wide daily cap |
| `OPTIONS_MAX_PCT` | 5% | Per options position |
| `OPTIONS_TOTAL_MAX_PCT` | 10% | Total options exposure |
| `OPTIONS_DEFAULT_DTE` | 30 | Target days to expiry |
| `OPTIONS_AUTO_CLOSE_DTE` | 1 | Auto-close 1 day before expiry |
| `OPTIONS_STOP_LOSS_PCT` | 50% | Exit if premium drops 50% |

### Trading Improvements (Season 3, 2026-03-17):
- Model Personalities: Each AI has unique identity in `MODEL_PERSONALITIES` dict in `engine/providers/base.py`
- Thesis-Based Trading: Prompt requires catalyst/thesis before any buy
- Max 5 Positions: `config.py` + `engine/risk_manager.py`
- 24h Minimum Hold: `engine/paper_trader.py` blocks sells < 24h unless stop-loss

### Cost Budget (Dilithium Crystal Protocol v3):
| Budget | Value |
|--------|-------|
| Daily hard limit | $5.00 (pause cloud scanning) |
| Daily warning | $4.00 |
| Monthly soft limit | $35.00 |

---

## 12. SCHEDULER & CRON JOBS

**File:** `main.py` (Arena, port 8080)

| Job | Interval | Purpose |
|-----|----------|---------|
| `run_scanner()` | Every 5 min (adaptive) | Main AI arena scan |
| `run_dayblade()` | Every 15 sec | 0DTE options scalping |
| `run_ma_regime_update()` | Every 15 min | 8/21 MA cross regime |
| `run_vix_check()` | Every 5 min | VIX alert at >25 |
| `run_earnings_check()` | Every 1 hr | Earnings calendar sync |
| `run_daily_summary()` | Every 5 min (once at close) | Daily portfolio summary |
| `run_journal()` | Every 5 min (once at close) | AI journal entries |
| `run_gex_refresh()` | Every 15 min | GEX (CBOE) during market hours |
| `run_war_room()` | Every 3 min | War room crew consensus |
| `run_autopilot()` | Every 30 min | RSI-based profit taking, rebalance |
| `run_whisper()` | Every 10 min | Hidden signal network |
| `run_season_rotation()` | Every 5 min (fires Sunday 11:59 PM MST) | Season reset |

**Adaptive Scan Intervals (market hours vs overnight):**
- Pre-market / market hours: 3-5 min
- Power hour (2-3 PM ET): 90 sec
- After-hours / evening: 10-30 min
- Weekends: 1 hour

**File:** `main_crew.py` (CrewAI, port 8000)

Uses apscheduler CronTrigger:
- Pre-market scout scan
- Market open full pipeline (9:30 AM ET)
- Post-market analysis
- Sunday strategy review

---

## 13. KEY ENGINE FILES

### `engine/paper_trader.py` — Trade Execution
- `buy(player_id, symbol, qty, price, ...)` → Updates `ai_players.cash`, inserts `positions`, inserts `trades`
- `sell(player_id, symbol, qty, price, ...)` → Removes from `positions`, calculates `realized_pnl`, updates cash
- `_is_human_player(player_id)` → Guard against auto-trading Steve/Kirk
- `get_portfolio_with_pnl(player_id, prices)` → Computes total_value, unrealized P&L vs current market
- `_check_min_hold(player_id, symbol)` → Blocks sells < 24h (stop-loss exception)
- `_forward_to_alpaca(...)` → Mirrors stock trades to Alpaca paper account

### `engine/ai_brain.py` — AI Scan Orchestration
- Runs all AI providers in batches
- Batch 1 (early): Claude, GPT, Gemini, Grok (API models — fast)
- Batch 2 (later): Ollama models (local, 10+ min)
- `ThreadPoolExecutor.shutdown(wait=False)` prevents batch blocking
- Confidence thresholds: 0.55 for stocks, 0.80 for options

### `engine/providers/base.py` — Base AI Class
- `MODEL_PERSONALITIES` dict — unique trading identity per player
- `build_prompt()` — assembles context: prices, indicators, fundamentals, macro, personality
- Each provider subclass calls this then hits its API

### `engine/dayblade.py` — 0DTE Options Scalper
- Universe: 25 stocks (SPY, QQQ, TSLA, NVDA, IWM, ORCL, HIMS, AMZN, MSFT, AAPL, MU, AMD, META, SLV, NIO, INTC, PLTR, IBIT, RIVN, SOFI, GOOGL, NFLX, MSTR, AVGO, BABA)
- DTE bands: 0DTE (-30%/+100%), 1DTE (-40%/+100%), 2-3DTE (-40%/+75%), 4-7DTE (-50%/+150%)
- Scalp ladder: 40% first signal, 30% confirmation, 30% momentum chaser
- Double-down rule: +50% in 30 min → add more
- Closes all positions by 3:45 PM ET (no overnight holds)

### `engine/autopilot.py` — Automated Portfolio Management
- RSI > 70 → trim winning positions
- Max position: 25% of portfolio, trim target: 20%
- Min cash: 15% floor
- Enabled/disabled via `settings` table (`autopilot_enabled` key)

---

## 14. SACRED RULES — NEVER BREAK THESE

### 1. Steve-Webull / Captain Kirk is SACRED
```
player_id = 'steve-webull'
display_name = 'Captain Kirk'
```
This is the human benchmark tracking a real Webull account. It must NEVER be auto-traded. Three-layer guard:
- `engine/paper_trader.py`: `_is_human_player()` check
- `engine/autopilot.py`: explicit exclusion
- `engine/ai_brain.py`: explicit exclusion

If you add any new automated trading logic, add a check: `if player_id == 'steve-webull': return`

### 2. NEVER DELETE/DROP/TRUNCATE Data in trader.db or arena.db
Historical trade data, signals, portfolio history — all sacred. This is the entire basis for learning and season comparisons. You can add rows, update specific fields, archive. You can NEVER mass-delete.

If you need to "reset" a player: UPDATE `ai_players` SET `cash=7000` WHERE id=... and DELETE from `positions` WHERE player_id=... but NEVER delete from `trades`.

### 3. Port 8080 = Vanilla HTML Only
`dashboard/static/index.html` is the UI. It's one file. Edit it directly.
Do NOT touch `dashboard/frontend/src/*.jsx` files for port 8080 changes.
The React app exists but is NOT served by the main dashboard.

### 4. Colorblind Colors: Blue/Orange Only (No Red/Green)
- Gains/positive: `#2563eb` blue + ▲
- Losses/negative: `#ea580c` orange + ▼
- Never use pure red/green as the sole visual indicator

### 5. Season Data Must Be Tagged
Every trade, signal, and portfolio_history row must have a `season` integer. This enables historical comparisons. The current season number lives in `settings` table, key `current_season`.

### 6. AI Batch Execution Order
API models (Claude, GPT, Gemini, Grok) ALWAYS run in early batches. Ollama models run later. This is because Ollama takes 10+ minutes per full scan (30s per stock × 20 stocks). Mixing them blocks the fast API models unnecessarily.

---

## QUICK REFERENCE CHEAT SHEET

```bash
# Check current players & cash
sqlite3 data/trader.db "SELECT id, display_name, cash FROM ai_players ORDER BY cash DESC;"

# Check Dalio's positions
sqlite3 data/trader.db "SELECT * FROM positions WHERE player_id='dalio-metals';"

# Check recent trades
sqlite3 data/trader.db "SELECT player_id, symbol, action, qty, price, executed_at FROM trades ORDER BY executed_at DESC LIMIT 20;"

# Current season
sqlite3 data/trader.db "SELECT value FROM settings WHERE key='current_season';"

# Kill switch status
sqlite3 data/trader.db "SELECT * FROM settings WHERE key='kill_switch_active';"

# Autopilot status
sqlite3 data/trader.db "SELECT * FROM settings WHERE key='autopilot_enabled';"

# Test leaderboard (heavy call, triggers fresh compute if cache is stale)
curl http://localhost:8080/api/arena/leaderboard | python3 -m json.tool | head -50

# Test Alpaca connection
curl http://localhost:8080/api/alpaca/status

# Check API cost today
curl http://localhost:8080/api/costs/ | python3 -m json.tool

# Force a scan immediately
curl -X POST http://localhost:8080/api/model-control/force-scan

# Bust leaderboard cache (if showing stale data)
rm data/leaderboard_cache.json
# Then restart server OR wait for next request to recompute

# Restart just the dashboard server (use venv Python!)
kill $(lsof -t -i:8080)
./venv/bin/python dashboard/app.py >> logs/dashboard.log 2>&1 &
```

---

*End of transfer document — USS TradeMinds, Stardate 2026-03-27*
