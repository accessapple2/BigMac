import os
from dotenv import load_dotenv
load_dotenv(override=True)

# Trading Mode
PAPER_TRADING = True
TRADING_MODE = os.environ.get("TRADING_MODE", "paper")  # "paper" or "live"

# Watchlist (stocks only)
WATCH_STOCKS = ["SPY", "QQQ", "NVDA", "TSLA", "AAPL", "AMD", "META", "MSFT", "GOOGL", "AMZN", "MU", "ORCL", "NOW", "AVGO", "PLTR", "DELL"]
# Extended tickers (re-enable when RAM permits): XLE, XOM, COIN, MSTR, SOFI, RIVN, NIO, HIMS, IWM

# Mr. Dalio's All Weather universe — bonds, gold ETF, commodities
# All four are GATE_EXEMPT in quality_gate.py (no earnings/revenue metrics for macro assets)
DALIO_SYMBOLS = ["TLT", "IEF", "GLD", "GSG", "DJP"]
DALIO_BOND_SYMBOLS = {"TLT", "IEF"}  # stored as asset_type='bond' in paper_trader

# AI Provider Keys
OLLAMA_MODEL = "gemma3:4b"
OLLAMA_URL = "http://localhost:11434"
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
FRED_API_KEY = os.environ.get("FRED_API_KEY", "af1be7d4d6c0b45dabd721372952c3ab")
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "")  # Polygon.io — activates when key is added

# Webull Broker
WEBULL_APP_KEY = os.environ.get("WEBULL_APP_KEY", "")
WEBULL_APP_SECRET = os.environ.get("WEBULL_APP_SECRET", "")
WEBULL_ACCOUNT_ID = os.environ.get("WEBULL_ACCOUNT_ID", "")

# Trading Rules
STARTING_CASH = 7000.00
POSITION_SIZE_PCT = 0.10
MAX_POSITIONS = 8
STOP_LOSS_PCT = 0.05
TAKE_PROFIT_TIERS = [  # sell % of remaining position at each tier
    (0.05, 0.50),  # +5% profit → sell 50% — lock in gains early
    (0.10, 0.25),  # +10% profit → sell 25% of remaining
    (0.15, 0.50),  # +15% profit → sell 50% of remaining
    (0.25, 0.50),  # +25% profit → sell 50% of remaining
    (0.50, 1.00),  # +50% profit → sell everything left
]
MAX_POSITION_PCT = 0.30  # Allow up to 30% for high-conviction (0.85+) positions
MAX_DRAWDOWN_PCT = 0.20
MIN_CASH_RESERVE_PCT = 0.15
MAX_DAILY_TRADES = 30
OPTIONS_MAX_PCT = 0.05  # max 5% of portfolio per call/put position
OPTIONS_TOTAL_MAX_PCT = 0.10  # max 10% total options exposure (reduced from 20% to limit losses)
OPTIONS_DEFAULT_DTE = 30  # target days to expiry for arena options (not 0DTE)
OPTIONS_MIN_DTE = 7  # minimum acceptable DTE
OPTIONS_AUTO_CLOSE_DTE = 1  # auto-close options this many days before expiry
OPTIONS_STOP_LOSS_PCT = 0.50  # exit option if premium drops 50% from entry
OPTIONS_PREFER_ITM = True  # prefer ATM/slightly ITM strikes over OTM

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

# AI Arena Players
AI_PLAYERS = [
    {"id": "ollama-local", "name": "Gemma3 4B", "provider": "ollama", "model": "gemma3:4b"},
    {"id": "ollama-gemma27b", "name": "Gemma3 27B", "provider": "ollama", "model": "gemma3:27b"},
    {"id": "ollama-deepseek", "name": "DeepSeek R1 7B", "provider": "ollama", "model": "deepseek-r1:7b"},
    {"id": "ollama-qwen3", "name": "Qwen3 4B", "provider": "ollama", "model": "qwen3:4b"},
    {"id": "ollama-llama", "name": "Llama 3.1 8B", "provider": "ollama", "model": "llama3.1:latest"},
    {"id": "ollama-kimi", "name": "Kimi K2.5", "provider": "ollama", "model": "kimi-k2.5:cloud"},
    {"id": "mlx-qwen3", "name": "Qwen3 8B MLX", "provider": "mlx", "model": "mlx-community/Qwen3-8B-4bit"},
    {"id": "claude-sonnet", "name": "Codex Prime", "provider": "openai", "model": OPENAI_CODEX_MODEL},
    {"id": "claude-haiku", "name": "Codex Scout", "provider": "openai", "model": OPENAI_CODEX_MINI_MODEL},
    {"id": "gpt-4o", "name": "GPT-4o", "provider": "openai", "model": "gpt-4o"},
    {"id": "gpt-o3", "name": "GPT-o3", "provider": "openai", "model": "o3"},
    {"id": "gemini-2.5-pro", "name": "Gemini 2.5 Pro", "provider": "ollama", "model": "gemma3:4b"},
    {"id": "gemini-2.5-flash", "name": "Gemini 2.5 Flash", "provider": "ollama", "model": "gemma3:4b"},
    {"id": "grok-3", "name": "Grok 3", "provider": "xai", "model": "grok-3"},
    {"id": "grok-4", "name": "Grok 4", "provider": "xai", "model": "grok-4.20-0309-reasoning"},
]
