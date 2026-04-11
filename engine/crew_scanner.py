"""
Crew Scanner — Master Signal Pipeline for USS TradeMinds
=========================================================
Feeds signals to every agent every 5–60 minutes during market hours.

Pipeline per agent:
  1. gather_market_context()         — pull live market state
  2. should_agent_trade()            — mandate gate
  3. _query_ollama()                 — ask agent for trade idea
  4. gate checks (Troi, Event Shield, fleet exposure, daily limit)
  5. paper_trader.buy/sell()         — submit if all gates pass
  6. _log_decision()                 → crew_decisions table

Table: crew_decisions  (SACRED — never dropped/truncated)
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any

import requests

from engine.crew_specialization import CREW_MANIFEST, should_agent_trade

logger = logging.getLogger("crew_scanner")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [crew_scanner] %(levelname)s: %(message)s",
)

# Mutable flag — importers hold a reference to this dict and see live updates.
# Set True while a scan cycle is actively running; False otherwise.
# dashboard/app.py reads scan_state["active"] to throttle API responses.
scan_state: dict = {"active": False}

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_PATH = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))


def _init_risk_alerts_table() -> None:
    """Create risk_alerts table if it doesn't exist (idempotent)."""
    try:
        c = sqlite3.connect(DB_PATH, timeout=10)
        c.execute("""CREATE TABLE IF NOT EXISTS risk_alerts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            severity    TEXT NOT NULL,
            agent_id    TEXT,
            message     TEXT NOT NULL,
            detail      TEXT,
            acknowledged INTEGER DEFAULT 0,
            created_at  TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        c.commit()
        c.close()
    except Exception as e:
        logger.warning(f"risk_alerts table init error: {e}")


def _save_notification(title: str, body: str, severity: str = "info",
                        notif_type: str = "info", icon: str = "🔔",
                        agent_id: str = None) -> None:
    """Save notification to DB for the dashboard polling system."""
    try:
        c = sqlite3.connect(DB_PATH, timeout=10)
        c.execute("""CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
            type TEXT, severity TEXT, title TEXT, body TEXT,
            icon TEXT, agent_id TEXT, acknowledged INTEGER DEFAULT 0
        )""")
        exists = c.execute(
            "SELECT id FROM notifications WHERE title=? AND body=? "
            "AND timestamp >= datetime('now','-5 minutes')",
            (title, body)
        ).fetchone()
        if not exists:
            c.execute(
                "INSERT INTO notifications (type, severity, title, body, icon, agent_id) "
                "VALUES (?,?,?,?,?,?)",
                (notif_type, severity, title, body, icon, agent_id)
            )
            c.commit()
        c.close()
    except Exception as e:
        logger.warning(f"_save_notification error: {e}")


def _save_spock_alert(severity: str, message: str, agent_id: str = None, detail: str = None) -> None:
    """Persist a Spock risk alert. severity: CRITICAL | HIGH | MEDIUM."""
    try:
        c = sqlite3.connect(DB_PATH, timeout=10)
        # Avoid duplicate alerts in the last hour for the same agent+message
        exists = c.execute(
            """SELECT id FROM risk_alerts
               WHERE agent_id IS ? AND message=? AND acknowledged=0
               AND created_at >= datetime('now','-1 hour')""",
            (agent_id, message),
        ).fetchone()
        if not exists:
            c.execute(
                "INSERT INTO risk_alerts (severity, agent_id, message, detail) VALUES (?,?,?,?)",
                (severity, agent_id, message, detail),
            )
            c.commit()
            # Also push to notifications table
            _save_notification(
                title="🖖 Spock Risk Alert",
                body=message,
                severity="critical" if severity == "CRITICAL" else "alert",
                notif_type="alert",
                icon="🖖",
                agent_id=agent_id
            )
        c.close()
    except Exception as e:
        logger.warning(f"_save_spock_alert error: {e}")


def _is_agent_paused(player_id: str) -> bool:
    """Return True if agent has is_paused=1 in ai_players."""
    try:
        c = sqlite3.connect(DB_PATH, timeout=5)
        row = c.execute(
            "SELECT is_paused FROM ai_players WHERE id=?", (player_id,)
        ).fetchone()
        c.close()
        return bool(row and row[0])
    except Exception:
        return False


_MAX_DAILY_TRADES_PER_AGENT = 2   # guardrail: max trades per agent per day
_FLEET_EXPOSURE_MAX_PCT     = 60  # max % of total fleet invested at once

# ── Sniper Mode live gate (matches triple_threat.py SNIPER_ALPHA_THRESHOLD) ──
SNIPER_ALPHA_THRESHOLD = 0.3   # composite_alpha >= 0.3 required
SNIPER_MIN_CONFIDENCE  = 65    # LLM confidence >= 65 required (Signal Center B-grade proxy)
CSP_MIN_IVR            = 30    # CSP entries require IV Rank >= 30 (low-IV assignment risk)

# Strategies that bypass the Ollie gate (equity signals score low on options-heavy rubric)
BYPASS_OLLIE = {"rsi_bounce"}

# ── Ollie Commander (Fleet Commander — master approval gate) ──────────────────
OLLIE_ID = "ollie-auto"        # Ollie does not judge himself


def _get_live_alpha(symbol: str) -> float:
    """Return most recent composite_score for symbol from alpha_signals.db (0.0 if missing)."""
    try:
        import os as _os
        _alpha_db = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "..", "data", "alpha_signals.db")
        _ac = sqlite3.connect(_alpha_db, timeout=5)
        row = _ac.execute(
            "SELECT composite_score FROM composite_alpha WHERE symbol=? ORDER BY created_at DESC LIMIT 1",
            (symbol,)
        ).fetchone()
        _ac.close()
        return float(row[0]) if row and row[0] is not None else 0.0
    except Exception:
        return 0.0
_MAX_POSITION_PCT           = 5   # max % of agent equity per position

# Fast model used for all scan decisions — loads quickly, good enough for BUY/SELL/PASS.
# Each agent's assigned model is reserved for Ship's Computer chat and detailed analysis.
SCAN_MODEL = "0xroyce/plutus"

# ---------------------------------------------------------------------------
# Lean Fleet Protocol — Active scanner roster
# ---------------------------------------------------------------------------
# Only 2 Ollama models loaded at once to cap RAM.
# Alpha Squad rotates in 3 pairs per 15-min scan window.
#   Pair 1 (qwen3:14b x2):          Worf + Seven
#   Pair 2 (deepseek-r1:14b+phi4):  Spock + Sulu
#   Pair 3 (llama3.3:8b + plutus):  Uhura + McCoy
from engine.crew_specialization import ALPHA_SQUAD, SCAN_PAIRS, ADVISORY_CREW

ACTIVE_SCANNERS: list[str] = ["neo-matrix"]  # always-on (non-Ollama)

# Rules-based agents (no Ollama cost — API or rules engine)
RULES_SCANNERS: list[str] = ["dayblade-0dte", "capitol-trades", "dalio-metals"]

# Alpha Squad pair rotation — index cycles 0→1→2→0 each scan window
_ALPHA_PAIR_IDX: int = 0

def get_alpha_pair() -> list[str]:
    """Return current active Ollama pair; advance counter for next call.
    Deduplicates solo agents (e.g. McCoy solo = pair of two identical IDs)."""
    global _ALPHA_PAIR_IDX
    pair = SCAN_PAIRS[_ALPHA_PAIR_IDX % len(SCAN_PAIRS)]
    _ALPHA_PAIR_IDX += 1
    return list(dict.fromkeys(pair))  # deduplicate while preserving order

# Neo's preferred trading universe — tech/growth leaders only.
# These are prioritized in his scan picks and second-chance query.
NEO_PREFERRED: list[str] = [
    "NVDA", "AMD", "TSLA", "META", "AAPL", "AMZN",
    "NFLX", "GOOGL", "MSFT", "AVGO", "MU", "COIN",
    "PLTR", "SOFI", "INTC", "CRM", "TQQQ",
]


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def _ensure_table() -> None:
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS crew_decisions (
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
            )
        """)
        c.commit()
    finally:
        c.close()


_table_initialized  = False
_last_ollama_query: float = 0.0


def _ensure_warm() -> None:
    """If Ollama hasn't been queried in >10 min, send a 5-token ping to keep the model hot."""
    global _last_ollama_query
    if time.time() - _last_ollama_query > 600:
        try:
            requests.post(
                f"{_get_ollama_base_url()}/api/generate",
                json={
                    "model":   SCAN_MODEL,
                    "prompt":  "ready",
                    "stream":  False,
                    "think":   False,
                    "options": {"num_predict": 5},
                },
                timeout=180,
            )
        except Exception:
            pass
    _last_ollama_query = time.time()


def _init_once() -> None:
    global _table_initialized
    if not _table_initialized:
        _ensure_table()
        _init_risk_alerts_table()
        _table_initialized = True


# ---------------------------------------------------------------------------
# Market context
# ---------------------------------------------------------------------------

def gather_market_context() -> dict[str, Any]:
    """Pull current market state from all available sources."""
    ctx: dict[str, Any] = {
        "session_type":      "UNKNOWN",
        "vix":               0.0,
        "pc_ratio":          1.0,
        "fg_score":          None,
        "breadth_score":     None,
        "momentum_score":    0.0,
        "spy_day_return":    0.0,
        "spy_price":         0.0,
        "deep_scan_top":     [],
        "sector_leaders":    [],
        "sector_laggards":   [],
        "sector_leader":     None,
        "troi_signal":       "GO",
        "troi_multiplier":   1.0,
        "event_shield":      "NONE",
        "event_shield_blocked": False,
        "spy_wall_signal":   "NONE",
        "spy_wall_reason":   "",
        "volume_spikes":     [],
        "spy_volume_ratio":  1.0,
    }

    # ── Ready Room briefing ──────────────────────────────────────────────────
    try:
        from engine.ready_room import get_latest_briefing
        b = get_latest_briefing() or {}
        ctx["session_type"] = b.get("session_type") or "UNKNOWN"
        ctx["vix"]          = float(b.get("vix") or 0)
        ctx["pc_ratio"]     = float(b.get("pc_ratio") or 1.0)
        ctx["spy_price"]    = float(b.get("spot_price") or 0)
    except Exception as e:
        logger.warning(f"Ready room error: {e}")

    # ── Fear & Greed ────────────────────────────────────────────────────────
    try:
        from engine.fear_greed import get_fear_greed_index
        fg = get_fear_greed_index()
        if fg and fg.get("score") is not None:
            ctx["fg_score"] = int(fg["score"])
    except Exception as e:
        logger.warning(f"F&G error: {e}")

    # ── Market breadth ───────────────────────────────────────────────────────
    try:
        from engine.breadth_scanner import get_breadth_snapshot
        bs = get_breadth_snapshot()
        if bs:
            ctx["breadth_score"] = float(bs.get("breadth_score") or 0)
    except Exception as e:
        logger.warning(f"Breadth error: {e}")

    # ── Momentum ─────────────────────────────────────────────────────────────
    try:
        from engine.momentum_tracker import get_intraday_momentum
        mom = get_intraday_momentum()
        if mom:
            ctx["momentum_score"] = float(mom.get("trend_score") or mom.get("momentum_score") or 0)
    except Exception as e:
        logger.warning(f"Momentum error: {e}")

    # ── SPY day return ────────────────────────────────────────────────────────
    try:
        from engine.market_data import get_stock_price
        spy = get_stock_price("SPY")
        ctx["spy_day_return"] = float(spy.get("change_pct") or spy.get("change_percent") or 0)
        if not ctx["spy_price"]:
            ctx["spy_price"] = float(spy.get("price") or 0)
    except Exception as e:
        logger.warning(f"SPY price error: {e}")

    # ── Troi advisory ─────────────────────────────────────────────────────────
    try:
        from engine.ready_room_advisor import should_i_trade
        adv = should_i_trade("SPY", "BUY", "crew_scanner")
        ctx["troi_signal"]     = adv.get("signal", "GO")
        ctx["troi_multiplier"] = float(adv.get("position_size_multiplier", 1.0))
    except Exception as e:
        logger.warning(f"Troi advisory error: {e}")

    # ── Deep Scan (with diversity filter) ────────────────────────────────────
    try:
        from engine.deep_scan import get_deep_scan_results
        ds = get_deep_scan_results(limit=15)  # fetch more to allow diversity selection
        raw = [dict(r) for r in (ds.get("results") or [])]
        vix_now = float(ctx.get("vix", 20))
        ctx["deep_scan_top"] = _diversify_scan_picks(raw, vix=vix_now)
    except Exception as e:
        logger.warning(f"Deep scan error: {e}")

    # ── Sector heatmap ────────────────────────────────────────────────────────
    try:
        from engine.sector_heatmap import get_sector_heatmap
        sh = get_sector_heatmap()
        ctx["sector_leader"]   = sh.get("sector_leader") or None
        ctx["sector_laggard"]  = sh.get("sector_laggard") or None
        sectors_list = sh.get("sectors", [])
        ctx["sector_leaders"]  = [s.get("ticker", "?") for s in sectors_list[:3]]
        ctx["sector_laggards"] = [s.get("ticker", "?") for s in reversed(sectors_list[-3:])]
    except Exception as e:
        logger.warning(f"Sector heatmap error: {e}")

    # ── SPY Wall Strategy ────────────────────────────────────────────────────
    try:
        from engine.spy_wall_strategy import check_spy_wall_setup
        wall = check_spy_wall_setup()
        ctx["spy_wall_signal"] = wall.get("signal", "NONE")
        ctx["spy_wall_reason"] = wall.get("reason", "")
    except Exception as e:
        logger.warning(f"SPY wall strategy error: {e}")
        ctx["spy_wall_signal"] = "NONE"
        ctx["spy_wall_reason"] = ""

    # ── Event Shield ──────────────────────────────────────────────────────────
    try:
        from engine.event_shield import get_event_shield_status
        es = get_event_shield_status()
        ctx["event_shield"]         = es.get("highest_impact", "NONE")
        ctx["event_shield_blocked"] = bool(es.get("shield_active") and
                                           es.get("highest_impact") == "CRITICAL")
    except Exception as e:
        logger.warning(f"Event shield error: {e}")

    # ── Volume spikes ──────────────────────────────────────────────────────────
    try:
        spikes = _get_volume_spikes()
        ctx["volume_spikes"] = spikes
        spy_spike = next((s for s in spikes if s["symbol"] == "SPY"), None)
        ctx["spy_volume_ratio"] = spy_spike["volume_ratio"] if spy_spike else 1.0
    except Exception as e:
        logger.warning(f"Volume spikes error: {e}")

    # ── Finviz daily watchlist ─────────────────────────────────────────────────
    try:
        from engine.premarket_scanner import get_todays_watchlist
        wl = get_todays_watchlist()
        ctx["daily_watchlist"]       = wl.get("symbols", [])
        ctx["daily_watchlist_picks"] = wl.get("picks", [])
    except Exception as e:
        logger.warning(f"Daily watchlist error: {e}")
        ctx["daily_watchlist"]       = []
        ctx["daily_watchlist_picks"] = []

    return ctx


# ---------------------------------------------------------------------------
# Guardrails
# ---------------------------------------------------------------------------

def _count_today_trades(player_id: str) -> int:
    """How many executed crew_decisions trades has this agent placed today?"""
    today = datetime.now().strftime("%Y-%m-%d")
    c = _conn()
    try:
        row = c.execute(
            "SELECT COUNT(*) FROM crew_decisions WHERE player_id=? AND date(timestamp)=? AND executed=1",
            (player_id, today),
        ).fetchone()
        return row[0] if row else 0
    except Exception:
        return 0
    finally:
        c.close()


def _total_fleet_exposure_pct() -> float:
    """Rough fleet-wide invested % = (total_value - total_cash) / total_value."""
    try:
        c = _conn()
        rows = c.execute(
            "SELECT cash, total_value FROM portfolios WHERE type='paper'"
        ).fetchall()
        c.close()
        total_cash  = sum(float(r[0] or 0) for r in rows)
        total_value = sum(float(r[1] or 0) for r in rows)
        if total_value <= 0:
            return 0.0
        return (total_value - total_cash) / total_value * 100
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Volume spike scanner
# ---------------------------------------------------------------------------

_SPIKE_TICKERS = [
    "SPY", "QQQ", "NVDA", "TSLA", "AMD", "AAPL", "MSFT",
    "META", "GOOGL", "AMZN", "MU", "AVGO", "PLTR", "DELL",
    "NOW", "MRVL", "NFLX", "COIN", "INTC", "SOFI",
    "GLD", "SLV", "XLE", "XLF", "XLK",
]


def _get_volume_spikes() -> list[dict]:
    """
    Find tickers with unusual volume today (>1.5x 5-day avg).
    Uses 6 days of OHLCV via yfinance — runs in ~1-2s.
    Returns list sorted by volume_ratio descending.
    """
    try:
        import yfinance as yf
        import pandas as pd
        raw = yf.download(
            _SPIKE_TICKERS, period="6d", progress=False,
            auto_adjust=True, threads=True,
        )
        spikes: list[dict] = []
        for t in _SPIKE_TICKERS:
            try:
                # Handle both MultiIndex layouts yfinance uses
                if isinstance(raw.columns, pd.MultiIndex):
                    top = raw.columns.get_level_values(0)[0]
                    if top in ("Close", "Open", "High", "Low", "Volume"):
                        # Default layout: (field, ticker)
                        vol   = raw["Volume"][t].dropna()
                        close = raw["Close"][t].dropna()
                    else:
                        # group_by="ticker" layout: (ticker, field)
                        vol   = raw[t]["Volume"].dropna()
                        close = raw[t]["Close"].dropna()
                else:
                    vol   = raw["Volume"].dropna()
                    close = raw["Close"].dropna()

                if len(vol) < 2:
                    continue
                today_vol = float(vol.iloc[-1])
                avg_vol   = float(vol.iloc[:-1].mean())
                if avg_vol <= 0:
                    continue
                ratio = today_vol / avg_vol
                if ratio < 1.5:
                    continue
                today_px = float(close.iloc[-1])
                prev_px  = float(close.iloc[-2]) if len(close) >= 2 else today_px
                change   = (today_px / prev_px - 1) * 100 if prev_px > 0 else 0.0
                spikes.append({
                    "symbol":       t,
                    "volume_ratio": round(ratio, 1),
                    "price":        round(today_px, 2),
                    "change_pct":   round(change, 1),
                })
            except Exception:
                continue
        return sorted(spikes, key=lambda x: x["volume_ratio"], reverse=True)
    except Exception as e:
        logger.warning(f"_get_volume_spikes error: {e}")
        return []


# ---------------------------------------------------------------------------
# Ollama
# ---------------------------------------------------------------------------

_OLLAMA_URL_CACHE: str | None = None


def _get_ollama_base_url() -> str:
    global _OLLAMA_URL_CACHE
    if _OLLAMA_URL_CACHE:
        return _OLLAMA_URL_CACHE
    try:
        from config import OLLAMA_URL
        _OLLAMA_URL_CACHE = OLLAMA_URL.rstrip("/")
    except Exception:
        _OLLAMA_URL_CACHE = os.environ.get("OLLAMA_URL", "http://localhost:11434").rstrip("/")
    return _OLLAMA_URL_CACHE


def _query_ollama(player_id: str, model: str, system_prompt: str,
                  user_prompt: str, timeout: int = 90) -> str:
    """Call Ollama /api/generate and return the raw response text."""
    global _last_ollama_query
    base_url = _get_ollama_base_url()
    full_prompt = f"<system>{system_prompt}</system>\n\n{user_prompt}"
    try:
        resp = requests.post(
            f"{base_url}/api/generate",
            json={
                "model":   model,
                "prompt":  full_prompt,
                "stream":  False,
                "think":   False,
                "options": {"num_predict": 120 if player_id == "neo-matrix" else 80, "temperature": 0.3},
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        response = data.get("response", "").strip()
        _last_ollama_query = time.time()
        logger.info(f"Ollama RAW response for {player_id}: {response[:200]!r}")
        return response
    except requests.Timeout:
        logger.warning(f"Ollama timeout for {player_id} ({model})")
        return ""
    except Exception as e:
        logger.warning(f"Ollama call failed for {player_id}: {e}")
        return ""


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------

# Per-agent scan focus hints
_AGENT_SCAN_HINTS: dict[str, str] = {
    "grok-4":          "RSI oversold/overbought stocks from Deep Scan. Session must NOT be trending. Give me mean reversion setups.",
    "dayblade-sulu":   "Trending stocks with momentum > 30 from Deep Scan. Session must be trending. Give me momentum entries.",
    "energy-arnold":   "P/C ratio extremes and F&G extremes. Give me contrarian plays against the crowd.",
    "gemini-2.5-flash":"Bearish setups: rising VIX, weak breadth, inverse ETFs or shorts. Only if session is bearish.",
    "options-sosnoff": "Sentiment divergences: news vs options vs F&G. Where is the crowd wrong?",
    "ollama-coder":    "Pure quant: highest signal_strength from Deep Scan. No sentiment, just numbers.",
    "mlx-qwen3":       "Breakout stocks: 20-day high on 2x volume from Deep Scan.",
    "ollama-local":    "Sector rotation: buy leading sector ETF, short lagging.",
    "gemini-2.5-pro":  "Pure quant: pick the symbol with highest signal_strength from Deep Scan. No sentiment.",
    "ollama-plutus":   "Crisis doctor: only if VIX > 22. Look for oversold bounces and panic sells to fade.",
    "ollama-qwen3":    "Defensive/value: XLU, XLP, XLV when risk-off rotation.",
    "ollama-llama":    "Options flow confluence: 4+ signals aligning.",
    "ollama-deepseek": "Contrarian divergences: where do signals conflict?",
    "ollama-gemma27b": "Swing setups: pullback to 20MA in uptrend, 2-5 day hold.",
    "dayblade-0dte":   "SPY GEX wall strategy: {spy_wall_signal}. {spy_wall_reason}",
}

# Words that should NOT be treated as stock tickers in response parsing
_NON_TICKER_WORDS = {
    "TRADE", "BUY", "SELL", "SHORT", "HOLD", "PASS", "HIGH", "LOW",
    "RSI", "VIX", "ETF", "ATM", "OTM", "ITM", "DTE", "PCR",
    "THE", "AND", "FOR", "WITH", "FROM", "INTO", "THAT",
    "THIS", "WILL", "HAVE", "MORE", "THAN", "ALSO", "WHEN", "THEN",
    "YOUR", "THEY", "BEEN", "WOULD", "COULD", "SHOULD", "BOTH",
    "MARKET", "STOCK", "PRICE", "ABOVE", "BELOW", "BASED", "GIVEN",
    "SESSION", "SIGNAL", "TREND", "SECTOR", "SETUP", "ENTRY",
    "CONFIDENCE", "REASON", "DECISION", "TRADE",
}


def _build_market_summary(ctx: dict[str, Any], player_id: str) -> str:
    """Build the per-agent market data block for the prompt."""
    ds_top  = ctx.get("deep_scan_top", [])
    ds_str  = ", ".join(
        f"{r.get('symbol','?')}({float(r.get('signal_strength',0)):.2f})"
        for r in ds_top[:5]
    ) if ds_top else "none"

    try:
        from engine.paper_trader import get_portfolio
        port      = get_portfolio(player_id)
        positions = port.get("positions", [])
        pos_str   = ", ".join(p["symbol"] for p in positions) if positions else "none"
    except Exception:
        pos_str = "unknown"

    mandate = CREW_MANIFEST.get(player_id, {})
    sectors_lead = ", ".join(ctx.get("sector_leaders", [])[:3]) or "?"
    sectors_lag  = ", ".join(ctx.get("sector_laggards", [])[-3:]) or "?"

    return (
        f"Session: {ctx.get('session_type','?')} | "
        f"VIX: {float(ctx.get('vix',0)):.1f} | "
        f"Breadth: {ctx.get('breadth_score','?')} | "
        f"Momentum: {float(ctx.get('momentum_score',0)):.0f}\n"
        f"F&G: {ctx.get('fg_score','?')} | "
        f"P/C: {float(ctx.get('pc_ratio',1.0)):.2f} | "
        f"Troi: {ctx.get('troi_signal','GO')}\n"
        f"Deep Scan top picks: {ds_str}\n"
        f"Sectors leading: {sectors_lead} | lagging: {sectors_lag}\n"
        f"Your current positions: {pos_str}\n"
        f"Your mandate: {mandate.get('strategy','unrestricted')}"
    )


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def _parse_ollama_decision(response: str) -> dict[str, Any]:
    """
    Parse agent response into a structured decision.
    Returns: {action, symbol, confidence, reason}
    """
    logger.info(f"Parsing response: {response[:200]!r}")
    if not response:
        return {"action": "PASS", "symbol": None, "confidence": 0, "reason": "No response"}

    resp_upper = response.upper()

    # Explicit PASS/HOLD
    if any(w in resp_upper for w in ("PASS", "HOLD", "NO TRADE", "STAND DOWN", "STAND_DOWN")):
        return {"action": "PASS", "symbol": None, "confidence": 0, "reason": response[:300]}

    # Detect TRADE intent
    has_trade = "TRADE" in resp_upper
    has_direction = any(w in resp_upper for w in ("BUY", "SELL", "SHORT"))

    if not (has_trade or has_direction):
        return {"action": "PASS", "symbol": None, "confidence": 0, "reason": response[:300]}

    # Action
    action = "BUY"
    if "SHORT" in resp_upper or "SELL SHORT" in resp_upper:
        action = "SHORT"
    elif "SELL" in resp_upper and "BUY" not in resp_upper:
        action = "SELL"

    # Symbol — look for 2-5 uppercase letter runs that aren't noise words
    raw_symbols = re.findall(r'\b([A-Z]{2,5})\b', response)
    tickers = [s for s in raw_symbols if s not in _NON_TICKER_WORDS]
    symbol = tickers[0] if tickers else "SPY"

    # Confidence (0-100 integer)
    confidence = 50
    m = re.search(r'(\d{1,3})\s*%?\s*(?:confidence|conf)', response, re.IGNORECASE)
    if not m:
        m = re.search(r'confidence[:\s]+(\d{1,3})', response, re.IGNORECASE)
    if not m:
        # bare number 50-99 near end of response
        nums = re.findall(r'\b([5-9]\d)\b', response)
        m_val = nums[-1] if nums else None
        if m_val:
            v = int(m_val)
            if 50 <= v <= 99:
                confidence = v
    else:
        v = int(m.group(1))
        if 0 < v <= 100:
            confidence = v

    logger.info(f"Parse result: action={action} symbol={symbol} confidence={confidence}")
    return {
        "action":     action,
        "symbol":     symbol,
        "confidence": confidence,
        "reason":     response[:400],
    }


# ---------------------------------------------------------------------------
# Rules-based agent decisions (no Ollama)
# ---------------------------------------------------------------------------

def spock_rules(market_ctx: dict[str, Any], scan_picks: list[dict]) -> dict[str, Any]:
    """Spock — Mean Reversion: RSI extremes, support bounce, volume reversal flush."""
    # Primary: RSI extremes from scan picks
    for pick in scan_picks[:5]:
        symbol = pick["symbol"]
        rsi    = pick.get("rsi_14", 50)
        price  = pick.get("close", 0)
        sma20  = pick.get("sma_20", 0)

        if rsi < 30 and sma20 > 0 and abs(price - sma20) / sma20 < 0.01:
            return {
                "action": "BUY", "symbol": symbol, "confidence": 80,
                "reason": f"RSI {rsi:.0f} oversold, near SMA20",
            }
        if rsi > 75 and sma20 > 0 and (price - sma20) / sma20 > 0.03:
            return {
                "action": "SELL", "symbol": symbol, "confidence": 75,
                "reason": f"RSI {rsi:.0f} overbought, extended above SMA20",
            }

    # Secondary: high-volume flush reversal (institutional volume on big down day)
    for spike in market_ctx.get("volume_spikes", []):
        if spike["volume_ratio"] > 2.0 and spike["change_pct"] < -3.0:
            return {
                "action": "BUY", "symbol": spike["symbol"], "confidence": 77,
                "reason": (
                    f"Volume reversal: {spike['symbol']} down {spike['change_pct']:.1f}% "
                    f"on {spike['volume_ratio']:.1f}x vol — institutional flush, mean reversion play"
                ),
            }

    return {"action": "PASS", "reason": "No RSI extremes or volume reversal setups found"}


_DAX_NON_MOMENTUM = {"SH", "SQQQ", "TLT", "GLD", "SLV", "XLU", "XLP"}


def dax_rules(market_ctx: dict[str, Any], scan_picks: list[dict]) -> dict[str, Any]:
    """Dax — Swing Breakout: 20MA cross on volume, or pure volume momentum spike."""
    session = market_ctx.get("session_type", "")
    if "BEAR" in session or "DOWN" in session:
        return {"action": "PASS", "reason": "Dax sits out bear/down sessions"}

    # Primary: classic breakout above 20MA with volume confirmation
    for pick in scan_picks[:5]:
        symbol       = pick["symbol"]
        price        = pick.get("close", 0)
        sma20        = pick.get("sma_20", 0)
        volume_ratio = pick.get("volume_ratio", 1)
        roc_5d       = pick.get("roc_5d", 0)

        if price > sma20 and sma20 > 0 and volume_ratio > 1.5 and roc_5d > 1:
            return {
                "action": "BUY", "symbol": symbol, "confidence": 78,
                "reason": f"Breakout above 20MA, vol {volume_ratio:.1f}x, 5d ROC +{roc_5d:.1f}%",
            }

    # Secondary: pure volume momentum — 2x+ volume AND up > 1% today
    for spike in market_ctx.get("volume_spikes", []):
        if (spike["volume_ratio"] > 2.0
                and spike["change_pct"] > 1.0
                and spike["symbol"] not in _DAX_NON_MOMENTUM):
            return {
                "action": "BUY", "symbol": spike["symbol"], "confidence": 80,
                "reason": (
                    f"Volume breakout: {spike['symbol']} +{spike['change_pct']:.1f}% "
                    f"on {spike['volume_ratio']:.1f}x vol — institutional momentum"
                ),
            }

    return {"action": "PASS", "reason": "No swing breakouts or volume momentum setups found"}


_MCCOY_DEFENSIVES = {"XLU", "GLD", "SLV", "SH", "SQQQ", "TLT", "XLP"}


def mccoy_rules(market_ctx: dict[str, Any], scan_picks: list[dict]) -> dict[str, Any]:
    """McCoy — Crisis Doctor: VIX > 18 only; VIX-tiered priority + rotation if last pick lost."""
    vix = float(market_ctx.get("vix", 20))

    if vix < 18:
        return {"action": "PASS", "reason": f"VIX {vix:.1f} too low, McCoy waits for elevated vol"}

    # VIX-tiered priority order
    if vix > 30:
        priority = ["GLD", "SH", "TLT", "SLV", "XLU", "XLP", "SQQQ"]  # crisis: gold first
    elif vix >= 25:
        priority = ["TLT", "GLD", "SH", "XLU", "SLV", "XLP", "SQQQ"]  # stress: bonds first
    elif vix >= 22:
        priority = ["XLU", "TLT", "GLD", "SH", "SLV", "XLP", "SQQQ"]  # elevated: yield first
    else:  # 18-22
        priority = ["XLU", "XLP", "TLT", "GLD", "SPY", "QQQ", "IWM"]  # caution: defensives + broad

    # Check if last McCoy trade was a loss — if so, skip that symbol (rotate)
    skip_symbol: str | None = None
    try:
        _c = _conn()
        last = _c.execute(
            "SELECT symbol, realized_pnl FROM trades WHERE player_id='ollama-plutus' "
            "AND action='SELL' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        _c.close()
        if last and last["realized_pnl"] and float(last["realized_pnl"]) < 0:
            skip_symbol = last["symbol"]
            logger.info(f"[MCCOY] Last trade {skip_symbol} lost ${float(last['realized_pnl']):.2f} — rotating")
    except Exception:
        pass

    # Build candidate momentum map from scan picks and vol spikes
    momentum: dict[str, float] = {}
    for pick in scan_picks:
        if pick["symbol"] in _MCCOY_DEFENSIVES:
            momentum[pick["symbol"]] = float(pick.get("roc_5d", 0))
    for spike in market_ctx.get("volume_spikes", []):
        sym = spike["symbol"]
        if sym in _MCCOY_DEFENSIVES and spike["change_pct"] > 0:
            momentum[sym] = momentum.get(sym, 0) + float(spike["change_pct"])

    conf = 87 if vix > 30 else 82

    # Walk priority list, skip loser, pick first with acceptable momentum
    for sym in priority:
        if sym == skip_symbol:
            continue
        roc = momentum.get(sym, None)
        if roc is not None and roc > -10:
            note = f" [rotated from {skip_symbol}]" if skip_symbol else ""
            return {
                "action": "BUY", "symbol": sym, "confidence": conf,
                "reason": f"VIX {vix:.1f} crisis, priority {sym} ({roc:+.1f}% momentum){note}",
            }

    # Fallback: best momentum from any non-skipped defensive
    candidates = [(sym, roc) for sym, roc in momentum.items()
                  if sym != skip_symbol and roc > -10]
    if candidates:
        best_sym, best_roc = max(candidates, key=lambda x: x[1])
        return {
            "action": "BUY", "symbol": best_sym, "confidence": conf,
            "reason": f"VIX {vix:.1f}, {best_sym} best available defensive ({best_roc:+.1f}%)",
        }

    if vix > 30:
        fallback = "GLD" if skip_symbol != "GLD" else "SH"
        return {"action": "BUY", "symbol": fallback, "confidence": 85,
                "reason": f"Extreme VIX {vix:.1f}, hard fallback to {fallback}"}
    return {"action": "PASS", "reason": f"VIX {vix:.1f} elevated but no defensive setup"}


_DATA_SECTOR_MAP: dict[str, str] = {
    "AAPL": "XLK", "MSFT": "XLK", "NVDA": "XLK", "AMD": "XLK", "INTC": "XLK",
    "AVGO": "XLK", "QCOM": "XLK", "TXN": "XLK", "CRM": "XLK", "GOOGL": "XLK",
    "META": "XLK", "NFLX": "XLK", "AMZN": "XLC", "TSLA": "XLY", "HD": "XLY",
    "JPM": "XLF",  "BAC": "XLF",  "V": "XLF",    "MA": "XLF",
    "UNH": "XLV",  "JNJ": "XLV",  "LLY": "XLV",  "MRK": "XLV", "ABBV": "XLV",
    "XOM": "XLE",  "CVX": "XLE",
    "GE": "XLI",   "CAT": "XLI",  "HON": "XLI",  "UPS": "XLI",
    "WMT": "XLP",  "KO": "XLP",   "PEP": "XLP",
    "NEE": "XLU",  "DUK": "XLU",
}


def data_rules(market_ctx: dict[str, Any], scan_picks: list[dict]) -> dict[str, Any]:
    """
    Data — The Android: pure quantitative scoring, no bias or intuition.

    Positive signals:
      RSI < 35          +3  (oversold)
      RSI 35-45         +1  (moderately oversold)
      Volume > 2x avg   +2  (institutional interest)
      Above SMA20       +1  (trend confirmation)
      MACD crossing up  +2  (momentum turning)
      Sector leading    +1  (sector tailwind)
      Green candle today+1  (positive price action)
      Gap up on volume  +2  (change>2% AND vol>2x — institutional gap)
      Below SMA50       -1  (trend headwind)
      VIX > 30          -1  (market stress)
      Down >5% in 5d    -2  (falling knife penalty)

    Score >= 8 → BUY conf=90 (full size)
    Score 6-7  → BUY conf=75 (standard)
    Score 4-5  → BUY conf=55 (half size)
    Score < 4  → PASS
    """
    vix = float(market_ctx.get("vix", 20))

    # Determine leading sectors from volume spikes
    leading_sectors: set[str] = set()
    for spike in market_ctx.get("volume_spikes", []):
        sym = spike.get("symbol", "")
        sec = _DATA_SECTOR_MAP.get(sym)
        if sec and spike.get("change_pct", 0) > 0:
            leading_sectors.add(sec)

    best: dict | None = None
    best_score = 0

    for pick in scan_picks[:8]:
        symbol       = pick.get("symbol", "")
        rsi          = float(pick.get("rsi_14", 50))
        vol_ratio    = float(pick.get("volume_ratio", 1))
        close        = float(pick.get("close", 0))
        sma20        = float(pick.get("sma_20", 0))
        sma50        = float(pick.get("sma_50", 0))
        change_today = float(pick.get("change_today", 0))
        roc_5d       = float(pick.get("roc_5d", 0))

        score = 0
        reasons: list[str] = []

        # RSI scoring
        if rsi < 35:
            score += 3
            reasons.append(f"RSI {rsi:.0f}")
        elif rsi < 45:
            score += 1
            reasons.append(f"RSI {rsi:.0f} moderate oversold")

        if vol_ratio >= 2.0:
            score += 2
            reasons.append(f"vol {vol_ratio:.1f}x")
        if sma20 > 0 and close > sma20:
            score += 1
            reasons.append("above SMA20")
        if change_today > 0:
            score += 1
            reasons.append("green candle")

        # Gap up on volume: meaningful move + institutional participation
        if change_today > 2.0 and vol_ratio > 2.0:
            score += 2
            reasons.append(f"gap+vol ({change_today:.1f}%+{vol_ratio:.1f}x)")

        # Sector leading
        sector = _DATA_SECTOR_MAP.get(symbol)
        if sector and sector in leading_sectors:
            score += 1
            reasons.append(f"{sector} leading")

        # MACD cross up — use pre-computed flag if available (backtest),
        # otherwise fetch from market_data (live)
        if "macd_cross_up" in pick:
            if pick["macd_cross_up"]:
                score += 2
                reasons.append("MACD cross↑")
        elif score >= 2:  # only fetch live MACD if already has some signal
            try:
                data = get_stock_data(symbol)
                if data:
                    macd_val    = float(data.get("macd", 0) or 0)
                    macd_signal = float(data.get("macd_signal", 0) or 0)
                    macd_hist   = float(data.get("macd_histogram", 0) or 0)
                    if macd_hist > 0 and macd_val > macd_signal:
                        score += 2
                        reasons.append("MACD cross↑")
            except Exception:
                pass

        # Negative signals
        if sma50 > 0 and close < sma50:
            score -= 1
            reasons.append("below SMA50")
        if vix > 30:
            score -= 1
            reasons.append(f"VIX {vix:.0f}")
        if roc_5d < -5.0:
            score -= 2
            reasons.append(f"knife {roc_5d:.1f}%")

        if score > best_score:
            best_score = score
            best = {"symbol": symbol, "score": score, "reasons": reasons}

    if best is None or best_score < 4:
        return {"action": "PASS", "reason": f"No symbol scored >= 4 (best={best_score})"}

    reason_str = f"Data score {best['score']}: {', '.join(best['reasons'])}"
    if best_score >= 8:
        return {
            "action": "BUY", "symbol": best["symbol"],
            "confidence": 90, "reason": reason_str,
        }
    elif best_score >= 6:
        return {
            "action": "BUY", "symbol": best["symbol"],
            "confidence": 75, "reason": reason_str,
        }
    else:
        return {
            "action": "BUY", "symbol": best["symbol"],
            "confidence": 55, "reason": f"[half size] {reason_str}",
        }


def worf_rules(market_ctx: dict[str, Any], scan_picks: list[dict]) -> dict[str, Any]:
    """Worf — Bear Specialist: inverse ETFs. Priority: SH→SQQQ→TLT→GLD. Never duplicates McCoy."""
    vix      = float(market_ctx.get("vix", 20))
    session  = market_ctx.get("session_type", "")
    momentum = float(market_ctx.get("momentum_score", 0))
    logger.info(f"[WORF CHECK] VIX={vix:.1f}, session={session}, momentum={momentum:.1f}")

    if vix < 20:
        return {"action": "PASS", "reason": f"VIX {vix:.1f} too low, Worf holds fire"}
    if "BULL" in session or session == "TRENDING_UP":
        return {"action": "PASS", "reason": f"Session {session} — Worf stands down in confirmed bulls"}

    # Check what McCoy is currently holding — Worf never duplicates
    mccoy_symbols: set[str] = set()
    try:
        _c = _conn()
        rows = _c.execute(
            "SELECT symbol FROM positions WHERE player_id='ollama-plutus' AND status='open'"
        ).fetchall()
        _c.close()
        mccoy_symbols = {r["symbol"] for r in rows}
    except Exception:
        pass

    # Worf priority: SH (safe) → SQQQ (aggressive) → TLT (rates) → GLD (dollar)
    # Strong bear momentum → prefer SQQQ
    if momentum < -30:
        worf_priority = ["SQQQ", "SH", "TLT", "GLD"]
    else:
        worf_priority = ["SH", "SQQQ", "TLT", "GLD"]

    scan_map = {p["symbol"]: p for p in scan_picks}
    conf = 85 if vix > 25 else 75

    for sym in worf_priority:
        if sym in mccoy_symbols:
            continue  # never duplicate McCoy
        if sym in scan_map:
            overlap = f" (McCoy has {', '.join(mccoy_symbols)})" if mccoy_symbols else ""
            return {
                "action": "BUY", "symbol": sym, "confidence": conf,
                "reason": f"VIX {vix:.1f}, {session} — {sym} bear play{overlap}",
            }

    # No scan match — default by priority, skip McCoy overlap
    conf = 80 if vix > 25 else 70
    for sym in worf_priority:
        if sym not in mccoy_symbols:
            overlap = f" (avoided McCoy overlap: {', '.join(mccoy_symbols)})" if mccoy_symbols else ""
            return {
                "action": "BUY", "symbol": sym, "confidence": conf,
                "reason": f"VIX {vix:.1f}, {session} — bearish hedge{overlap}",
            }

    return {"action": "PASS", "reason": f"VIX {vix:.1f} but all bear instruments overlap with McCoy"}


_UHURA_EARNINGS_WATCH = [
    "NVDA", "AMD", "TSLA", "META", "AAPL", "MSFT", "GOOGL",
    "AMZN", "NFLX", "AVGO", "CRM", "ORCL", "MU", "INTC",
    "COIN", "PLTR", "NOW", "DELL", "JPM", "BAC", "GS",
    "LLY", "PFE", "ABBV", "HD", "COST", "PEP", "KO",
]
_uhura_earnings_cache: dict = {}   # {date_str: [(symbol, days_until), ...]}

# ── Scan pick diversity tracking ─────────────────────────────────────────────
# Ring buffer of the last 3 cycle pick lists — used to rotate stale symbols
_recent_picks_history: list[list[str]] = []  # [[sym,...], [sym,...], [sym,...]]

_DIVERSITY_DEFENSIVES = {"GLD", "TLT", "IEF", "SLV", "XLU", "XLP", "SH", "SQQQ"}
_DIVERSITY_SECTOR_MAP: dict[str, str] = {
    "AAPL": "tech",  "MSFT": "tech",  "NVDA": "tech",  "AMD":  "tech",
    "INTC": "tech",  "AVGO": "tech",  "QCOM": "tech",  "CRM":  "tech",
    "GOOGL":"tech",  "META": "tech",  "NFLX": "tech",  "ORCL": "tech",
    "AMZN": "tech",  "TSLA": "tech",  "TQQQ": "tech",  "QQQ":  "tech",
    "MU":   "tech",  "PLTR": "tech",  "DELL": "tech",  "NOW":  "tech",
    "JPM":  "fin",   "BAC":  "fin",   "GS":   "fin",   "V":    "fin",
    "UNH":  "health","LLY":  "health","PFE":  "health","ABBV": "health",
    "XOM":  "energy","CVX":  "energy","XLE":  "energy",
    "GE":   "indus", "CAT":  "indus", "HON":  "indus",
    "WMT":  "cons",  "KO":   "cons",  "PEP":  "cons",  "HD":   "cons",
    "COIN": "crypto",
    "GLD":  "def",   "TLT":  "def",   "IEF":  "def",   "XLU":  "def",
    "XLP":  "def",   "SLV":  "def",   "SH":   "def",   "SQQQ": "def",
}


def _diversify_scan_picks(raw_picks: list[dict], vix: float = 20.0) -> list[dict]:
    """
    Enforce diversity on scan picks before distributing to rules agents.
    Rules:
      1. Max 2 picks per sector
      2. Include at least 1 defensive (GLD/TLT/XLU/SH) if VIX > 22
      3. Deprioritise symbols seen in ALL 3 of the last cycles (stale)
      4. Return up to 6 diversified picks (agents get more variety to choose from)
    """
    global _recent_picks_history

    # Track staleness — penalise symbols seen in all 3 recent cycles
    all_recent: set[str] = set()
    if len(_recent_picks_history) >= 3:
        sets = [set(s) for s in _recent_picks_history[-3:]]
        all_recent = sets[0] & sets[1] & sets[2]  # in every cycle = stale

    sector_count: dict[str, int] = {}
    selected: list[dict] = []
    deferred: list[dict] = []  # stale picks go here as fallback

    # Sort: non-stale first, then by signal_strength descending
    sorted_picks = sorted(
        raw_picks,
        key=lambda p: (
            1 if p.get("symbol") in all_recent else 0,
            -float(p.get("signal_strength", 0)),
        ),
    )

    # Must-have: one defensive if VIX elevated and none already in raw_picks
    needs_defensive = vix > 22 and not any(
        p.get("symbol") in _DIVERSITY_DEFENSIVES for p in sorted_picks
    )

    for pick in sorted_picks:
        sym = pick.get("symbol", "")
        sector = _DIVERSITY_SECTOR_MAP.get(sym, "other")
        count = sector_count.get(sector, 0)

        if sym in all_recent:
            deferred.append(pick)
            continue

        if count >= 2:
            continue  # sector already has 2 reps

        sector_count[sector] = count + 1
        selected.append(pick)

        if len(selected) >= 6:
            break

    # If no defensive present and VIX elevated, add highest-signal defensive from deferred/raw
    if needs_defensive:
        for pick in raw_picks:
            if pick.get("symbol") in _DIVERSITY_DEFENSIVES:
                if not any(p["symbol"] == pick["symbol"] for p in selected):
                    if len(selected) >= 6:
                        selected[-1] = pick  # replace last with defensive
                    else:
                        selected.append(pick)
                    break

    # Pad with stale picks if we don't have 5
    for pick in deferred:
        if len(selected) >= 5:
            break
        sym = pick.get("symbol", "")
        sector = _DIVERSITY_SECTOR_MAP.get(sym, "other")
        if sector_count.get(sector, 0) < 2:
            sector_count[sector] = sector_count.get(sector, 0) + 1
            selected.append(pick)

    # Update rotation history (keep last 3)
    _recent_picks_history.append([p.get("symbol", "") for p in selected])
    if len(_recent_picks_history) > 3:
        _recent_picks_history.pop(0)

    return selected[:6]


def _check_upcoming_earnings() -> list[dict]:
    """Return list of {symbol, days_until} for earnings in next 3 trading days."""
    from datetime import datetime as _dt
    today_str = _dt.now().strftime("%Y-%m-%d")
    if today_str in _uhura_earnings_cache:
        return _uhura_earnings_cache[today_str]

    upcoming: list[dict] = []
    try:
        import yfinance as yf
        today = _dt.now()
        for sym in _UHURA_EARNINGS_WATCH:
            try:
                t = yf.Ticker(sym)
                dates = t.earnings_dates
                if dates is not None and len(dates) > 0:
                    next_date = dates.index[0]
                    nd = next_date.date() if hasattr(next_date, "date") else next_date
                    days_until = (nd - today.date()).days
                    if 0 <= days_until <= 3:
                        upcoming.append({"symbol": sym, "days_until": days_until})
            except Exception:
                pass
    except Exception:
        pass

    _uhura_earnings_cache[today_str] = upcoming
    return upcoming


def uhura_rules(market_ctx: dict[str, Any], scan_picks: list[dict]) -> dict[str, Any]:
    """Uhura — Earnings Catalyst: real earnings calendar first, proxy fallback."""
    scan_syms = {p["symbol"] for p in scan_picks}

    # Primary: real earnings calendar (next 3 trading days)
    upcoming = _check_upcoming_earnings()
    if upcoming:
        # Prefer symbols also in scan picks (have fresh price data)
        for ev in sorted(upcoming, key=lambda x: x["days_until"]):
            sym = ev["symbol"]
            days = ev["days_until"]
            conf = 85 if days == 0 else (78 if days == 1 else 70)
            label = "today" if days == 0 else f"in {days}d"
            return {
                "action": "BUY", "symbol": sym, "confidence": conf,
                "reason": f"Earnings {label}: {sym} pre-earnings momentum play",
            }

    # Fallback: reactive proxy via >4% move + >2x volume
    for pick in scan_picks:
        change = float(pick.get("change_today", 0))
        vol_r  = float(pick.get("volume_ratio", 1))
        if abs(change) > 4.0 and vol_r > 2.0 and change > 0:
            return {
                "action": "BUY", "symbol": pick["symbol"], "confidence": 80,
                "reason": (
                    f"Earnings-proxy catalyst: {pick['symbol']} "
                    f"+{change:.1f}% on {vol_r:.1f}x volume"
                ),
            }
    return {"action": "PASS", "reason": "No earnings catalysts (calendar clear, no >4% proxy moves)"}


def tpol_rules(market_ctx: dict[str, Any], scan_picks: list[dict]) -> dict[str, Any]:
    """T'Pol — Vulcan Precision: highest signal_strength candidate, logic-driven, no bias."""
    if not scan_picks:
        return {"action": "PASS", "reason": "T'Pol: no scan picks available"}
    best = max(scan_picks, key=lambda p: float(p.get("signal_strength", 0)))
    strength = float(best.get("signal_strength", 0))
    if strength < 0.7:
        return {"action": "PASS", "reason": f"T'Pol: best signal_strength {strength:.2f} below 0.70 threshold"}
    conf = min(90, int(strength * 100))
    return {
        "action": "BUY", "symbol": best["symbol"], "confidence": conf,
        "reason": f"T'Pol: {best['symbol']} highest signal_strength {strength:.2f} — pure quant selection",
    }


# ---------------------------------------------------------------------------
# Pattern filters
# ---------------------------------------------------------------------------

def _is_falling_knife(symbol: str, scan_picks: list[dict]) -> bool:
    """
    Return True if symbol is in an active downtrend with no oversold floor yet.
    Falling knife: down >5% in 5 days AND RSI still > 35 (not yet washed out).
    Momentum/breakout agents should avoid these.
    """
    pick = next((p for p in scan_picks if p["symbol"] == symbol), None)
    if not pick:
        return False
    return pick.get("roc_5d", 0) < -5.0 and pick.get("rsi_14", 50) > 35


def _find_bounces(scan_picks: list[dict]) -> list[dict]:
    """
    Oversold bounce candidates: RSI < 35 with price within 3% of SMA20 (floor forming).
    """
    return [
        p for p in scan_picks
        if p.get("rsi_14", 50) < 35
        and float(p.get("close", 0)) > 0
        and float(p.get("sma_20", 0)) > 0
        and float(p.get("close", 0)) >= float(p.get("sma_20", 0)) * 0.97
    ]


# ---------------------------------------------------------------------------
# Position management — runs BEFORE new scans each cycle
# ---------------------------------------------------------------------------

# Per-agent scaled-exit tiers: [(profit_threshold, fraction_to_sell, tier_label)]
# Listed in descending order of threshold; highest tier fires first.
_SCALED_EXIT_TIERS: dict[str, list[tuple[float, float, str]]] = {
    "neo-matrix":    [(0.08, 0.15, "T3"), (0.05, 0.25, "T2"), (0.03, 0.50, "T1")],
    "grok-4":        [(0.05, 0.25, "T2"), (0.03, 0.50, "T1")],
    "ollama-qwen3":  [(0.06, 0.25, "T2"), (0.04, 0.50, "T1")],
    "ollama-plutus": [(0.04, 0.50, "T1")],
}

# Cache: {f"{player_id}|{symbol}|{tier}": date_str} — prevents re-firing same tier same day
_tiers_triggered: dict[str, str] = {}

# Neo trailing stop: highest price seen after T1 scale exit fires
# {symbol: high_watermark} — resets when the position is fully closed
_neo_trail_highs: dict[str, float] = {}


def _update_neo_trailing_stops() -> int:
    """
    After Neo's T1 scale exit fires, the remaining runner uses a 5% trailing stop
    that floors at avg cost (never sells at a loss once T1 is locked in).
    Trail = max(avg_cost, high_watermark * 0.95) — rises with price, never falls.
    Returns count of trailing stop exits executed.
    """
    exited = 0
    today  = datetime.now().strftime("%Y-%m-%d")
    try:
        from engine.paper_trader import get_portfolio, sell
        from engine.market_data import get_stock_price
        port = get_portfolio("neo-matrix")
        for pos in port.get("positions", []):
            if pos.get("asset_type", "stock") != "stock":
                continue
            symbol   = pos["symbol"]
            avg_cost = float(pos.get("avg_price") or 0)
            if avg_cost <= 0:
                continue
            # Only trail if T1 fired today (partial sell already locked in gains)
            if _tiers_triggered.get(f"neo-matrix|{symbol}|T1") != today:
                continue
            px      = get_stock_price(symbol)
            current = float(px.get("price") or 0)
            if current <= 0:
                continue
            # Update high watermark
            new_high = max(_neo_trail_highs.get(symbol, current), current)
            _neo_trail_highs[symbol] = new_high
            # Trail floor: 5% below high, never below cost basis
            trail_floor = max(avg_cost, new_high * 0.95)
            if current <= trail_floor:
                result = sell(
                    player_id  = "neo-matrix",
                    symbol     = symbol,
                    price      = current,
                    reasoning  = (
                        f"TRAILING STOP: {symbol} ${current:.2f} hit trail "
                        f"${trail_floor:.2f} (5% below high ${new_high:.2f})"
                    ),
                    confidence = 1.0,
                )
                if result:
                    exited += 1
                    _neo_trail_highs.pop(symbol, None)
                    logger.info(
                        f"🏃 NEO TRAIL: sold {symbol} @ ${current:.2f} "
                        f"(high ${new_high:.2f} → trail ${trail_floor:.2f})"
                    )
    except Exception as e:
        logger.error(f"_update_neo_trailing_stops error: {e}")
    return exited


def _check_hard_stops() -> int:
    """
    Immediately sell any stock position down -8% or more.
    Called first each scan cycle — protect capital before new trades.
    Returns count of positions cut.
    """
    cut = 0
    for player_id in ACTIVE_SCANNERS + RULES_SCANNERS + ALPHA_SQUAD:
        try:
            from engine.paper_trader import get_portfolio, sell
            from engine.market_data import get_stock_price
            port = get_portfolio(player_id)
            for pos in port.get("positions", []):
                if pos.get("asset_type", "stock") != "stock":
                    continue
                symbol   = pos["symbol"]
                avg_cost = float(pos.get("avg_price") or 0)
                if avg_cost <= 0:
                    continue
                px = get_stock_price(symbol)
                current = float(px.get("price") or 0)
                if current <= 0:
                    continue
                pnl_pct = (current - avg_cost) / avg_cost
                if pnl_pct <= -0.08:
                    result = sell(
                        player_id  = player_id,
                        symbol     = symbol,
                        price      = current,
                        reasoning  = f"HARD STOP: {pnl_pct*100:.1f}% loss exceeds -8% limit",
                        confidence = 1.0,
                    )
                    if result:
                        cut += 1
                        logger.info(f"✂️  HARD STOP: {player_id} cut {symbol} at {pnl_pct*100:.1f}%")
        except Exception as e:
            logger.error(f"_check_hard_stops error for {player_id}: {e}")
    return cut


def _check_scaled_exits(volatile_day: bool = False) -> int:
    """
    Partial-sell into strength at tiered profit targets.
    Each (player, symbol, tier) fires at most once per day.

    volatile_day: if True (SPY vol > 1.5x avg), thresholds shift down 1%
                  so gains are locked in faster before they evaporate.
    Returns count of partial sells executed.
    """
    sold = 0
    today     = datetime.now().strftime("%Y-%m-%d")
    vol_shift = -0.01 if volatile_day else 0.0   # lower thresholds by 1% on hot days

    for player_id, tiers in _SCALED_EXIT_TIERS.items():
        try:
            from engine.paper_trader import get_portfolio, sell_partial
            from engine.market_data import get_stock_price
            port = get_portfolio(player_id)
            for pos in port.get("positions", []):
                if pos.get("asset_type", "stock") != "stock":
                    continue
                symbol   = pos["symbol"]
                avg_cost = float(pos.get("avg_price") or 0)
                qty      = float(pos.get("qty") or 0)
                if avg_cost <= 0 or qty < 0.01:
                    continue
                px = get_stock_price(symbol)
                current = float(px.get("price") or 0)
                if current <= 0:
                    continue
                pnl_pct = (current - avg_cost) / avg_cost
                # Check tiers highest-first; fire at most one tier per cycle
                for threshold, fraction, label in sorted(tiers, key=lambda x: -x[0]):
                    effective_threshold = threshold + vol_shift
                    if pnl_pct < effective_threshold:
                        continue
                    cache_key = f"{player_id}|{symbol}|{label}"
                    if _tiers_triggered.get(cache_key) == today:
                        continue  # already fired today
                    sell_qty = qty * fraction
                    if sell_qty < 0.01:
                        break
                    vol_note = " [volatile day -1%]" if volatile_day else ""
                    result = sell_partial(
                        player_id = player_id,
                        symbol    = symbol,
                        price     = current,
                        qty       = sell_qty,
                        reasoning = (
                            f"Scaled exit {label}{vol_note}: +{pnl_pct*100:.1f}% — "
                            f"selling {fraction*100:.0f}% ({sell_qty:.2f} sh)"
                        ),
                        confidence = 0.9,
                    )
                    if result:
                        _tiers_triggered[cache_key] = today
                        sold += 1
                        logger.info(
                            f"📈 SCALED EXIT {label}{vol_note}: {player_id} sold "
                            f"{fraction*100:.0f}% of {symbol} at +{pnl_pct*100:.1f}%"
                        )
                    break
        except Exception as e:
            logger.error(f"_check_scaled_exits error for {player_id}: {e}")
    return sold


def _check_dip_buys() -> int:
    """
    Average down on existing positions that pull back 3-5% from avg cost.
    Strict rules: non-crisis only (VIX < 25), SPY flat/up, agent hasn't traded today.
    Returns count of dip buys executed.
    """
    bought = 0
    spy_ret = 0.0
    vix_now = 0.0
    try:
        from engine.market_data import get_stock_price
        spy_px  = get_stock_price("SPY")
        spy_ret = float(spy_px.get("change_pct") or spy_px.get("change_percent") or 0)
        if spy_ret < -0.5:
            return 0  # broad market falling — no averaging down
    except Exception:
        return 0
    try:
        from engine.ready_room import get_latest_briefing
        vix_now = float((get_latest_briefing() or {}).get("vix") or 0)
        if vix_now >= 25:
            return 0  # crisis mode
    except Exception:
        pass

    for player_id in ACTIVE_SCANNERS + RULES_SCANNERS + ALPHA_SQUAD:
        try:
            from engine.paper_trader import get_portfolio, buy
            from engine.market_data import get_stock_price
            port      = get_portfolio(player_id)
            cash      = float(port.get("cash") or 0)
            positions = [p for p in port.get("positions", []) if p.get("asset_type", "stock") == "stock"]
            if cash < 300 or not positions or len(positions) >= 3:
                continue
            if _count_today_trades(player_id) >= 1:
                continue  # already traded today — no dip buys
            for pos in positions:
                symbol   = pos["symbol"]
                avg_cost = float(pos.get("avg_price") or 0)
                if avg_cost <= 0:
                    continue
                px_data = get_stock_price(symbol)
                current = float(px_data.get("price") or 0)
                if current <= 0:
                    continue
                dip_pct = (current - avg_cost) / avg_cost
                if not (-0.05 <= dip_pct <= -0.03):
                    continue
                _dip_stop   = round(current * 0.97, 2)   # 3% below entry
                _dip_target = round(avg_cost * 1.02, 2)  # 2% above original avg cost
                result = buy(
                    player_id = player_id,
                    symbol    = symbol,
                    price     = current,
                    reasoning = (
                        f"Dip buy: {symbol} down {dip_pct*100:.1f}% from avg ${avg_cost:.2f} "
                        f"— averaging down. SPY {spy_ret:+.1f}%, VIX {vix_now:.1f} "
                        f"[STOP: ${_dip_stop:.2f}] [TARGET: ${_dip_target:.2f}]"
                    ),
                    confidence = 0.65,
                    timeframe  = "SWING",
                )
                if result:
                    bought += 1
                    logger.info(f"📉 DIP BUY: {player_id} averaged down {symbol} at {dip_pct*100:.1f}%")
        except Exception as e:
            logger.error(f"_check_dip_buys error for {player_id}: {e}")
    return bought


# ---------------------------------------------------------------------------
# Time-of-day session label (item 17)
# ---------------------------------------------------------------------------

def _get_market_session_label() -> str:
    """
    Return a trading session label based on current Eastern Time.
    Injected into Ollama prompts so agents adjust strategy by time of day.
    """
    try:
        from datetime import timezone as _tz
        import zoneinfo
        et   = zoneinfo.ZoneInfo("America/New_York")
        now  = datetime.now(_tz.utc).astimezone(et)
        hm   = now.hour * 60 + now.minute
        if hm < 9 * 60 + 30:
            return "PRE-MARKET"
        if hm < 10 * 60:
            return "OPENING RANGE — high volatility, wait for direction confirmation before entering"
        if hm < 14 * 60:
            return "MIDDAY — lower volatility, trend-following setups work best"
        if hm < 15 * 60 + 30:
            return "POWER HOUR — volume increasing, momentum trades and reversals"
        return "CLOSE APPROACHING — no new entries, manage existing positions only"
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Consecutive loss cooldown (item 18)
# ---------------------------------------------------------------------------

def _check_cooldown(player_id: str) -> bool:
    """
    Return True if agent is on a 1-day cooldown due to 3 consecutive losses.
    Checks the last 3 SELL trades for the agent; if all have realized_pnl < 0,
    the agent is in cooldown for the rest of the trading day.
    """
    try:
        c = _conn()
        rows = c.execute(
            """SELECT realized_pnl FROM trades
               WHERE player_id = ? AND action = 'SELL' AND realized_pnl IS NOT NULL
               ORDER BY executed_at DESC LIMIT 3""",
            (player_id,),
        ).fetchall()
        c.close()
        if len(rows) == 3 and all((r[0] or 0) < 0 for r in rows):
            return True
    except Exception as e:
        logger.debug(f"_check_cooldown error for {player_id}: {e}")
    return False


# ---------------------------------------------------------------------------
# Rules-based single-agent scan (same gate chain as _scan_single_agent)
# ---------------------------------------------------------------------------

def _scan_rules_agent(player_id: str, market_ctx: dict[str, Any]) -> dict[str, Any]:
    """Run gates + rules decision for Spock/Dax/McCoy. No Ollama call."""
    _init_once()
    mandate = CREW_MANIFEST.get(player_id)
    if not mandate:
        return {"player_id": player_id, "skipped": True, "reason": "No mandate"}

    display_name = mandate.get("display_name", player_id)

    if mandate.get("max_positions", 0) == 0:
        return {"player_id": player_id, "skipped": True, "reason": "No trading mandate (max_positions=0)"}

    # is_paused check — Spock can pause an agent via dashboard
    if _is_agent_paused(player_id):
        return {"player_id": player_id, "skipped": True, "reason": "Agent paused"}

    # Gate 1: Mandate
    allowed, gate_reason = should_agent_trade(player_id, market_ctx)
    if not allowed:
        _log_decision(player_id, display_name, "PASS", None, 0,
                      gate_reason, market_ctx, "MANDATE_BLOCKED", False)
        return {"player_id": player_id, "action": "PASS", "reason": gate_reason, "gate": "mandate"}

    is_unrestricted = bool(mandate.get("unrestricted"))

    # Gate 2: Consecutive loss cooldown — ALERT ONLY, Spock warns but Captain decides
    if _check_cooldown(player_id):
        alert_msg = f"{display_name} has 3 consecutive losses. RECOMMEND: pause or reduce size."
        logger.info(f"⚠️  Spock alert: {alert_msg}")
        _save_spock_alert("HIGH", alert_msg, player_id,
                          f"Last 3 trades all losses for {display_name}")

    # Gate 3: Daily trade limit
    if not is_unrestricted:
        trades_today = _count_today_trades(player_id)
        if trades_today >= _MAX_DAILY_TRADES_PER_AGENT:
            reason = f"Daily limit: {trades_today}/{_MAX_DAILY_TRADES_PER_AGENT} trades today"
            _log_decision(player_id, display_name, "PASS", None, 0,
                          reason, market_ctx, "DAILY_LIMIT", False)
            return {"player_id": player_id, "action": "PASS", "reason": reason, "gate": "daily_limit"}

    # Gate 4: Fleet exposure cap
    if not is_unrestricted:
        fleet_pct = _total_fleet_exposure_pct()
        if fleet_pct > _FLEET_EXPOSURE_MAX_PCT:
            reason = f"Fleet exposure {fleet_pct:.0f}% exceeds {_FLEET_EXPOSURE_MAX_PCT}% limit"
            _log_decision(player_id, display_name, "PASS", None, 0,
                          reason, market_ctx, "FLEET_EXPOSURE", False)
            return {"player_id": player_id, "action": "PASS", "reason": reason, "gate": "fleet_exposure"}

    # Gate 5: Troi signal
    # T'Pol and McCoy use their own VIX/mandate gates — exempt from Troi STAND_DOWN.
    _TROI_STAND_DOWN_EXEMPT = {"dayblade-0dte", "ollama-plutus"}
    troi_signal = market_ctx.get("troi_signal", "GO")
    if troi_signal == "STAND_DOWN" and player_id not in _TROI_STAND_DOWN_EXEMPT:
        reason = "Troi: STAND_DOWN — market structure unfavorable"
        _log_decision(player_id, display_name, "PASS", None, 0,
                      reason, market_ctx, "TROI_STAND_DOWN", False)
        return {"player_id": player_id, "action": "PASS", "reason": reason, "gate": "troi"}
    troi_caution_multiplier = 0.5 if troi_signal == "CAUTION" else 1.0

    # Gate 6: Event Shield CRITICAL
    if market_ctx.get("event_shield_blocked"):
        reason = "Event Shield: CRITICAL event in progress — no trading"
        _log_decision(player_id, display_name, "PASS", None, 0,
                      reason, market_ctx, "EVENT_SHIELD", False)
        return {"player_id": player_id, "action": "PASS", "reason": reason, "gate": "event_shield"}

    # ── Rules decision ────────────────────────────────────────────────────────
    scan_picks = market_ctx.get("deep_scan_top", [])
    if player_id == "grok-4":
        decision = spock_rules(market_ctx, scan_picks)
    elif player_id == "ollama-qwen3":
        decision = dax_rules(market_ctx, scan_picks)
    elif player_id == "ollama-plutus":
        decision = mccoy_rules(market_ctx, scan_picks)
    elif player_id in ("data-tng", "ollama-coder"):
        decision = data_rules(market_ctx, scan_picks)
    elif player_id == "ollama-llama":
        decision = uhura_rules(market_ctx, scan_picks)
    elif player_id == "gemini-2.5-flash":
        decision = worf_rules(market_ctx, scan_picks)
    elif player_id == "dayblade-0dte":
        decision = tpol_rules(market_ctx, scan_picks)
    else:
        decision = {"action": "PASS", "reason": "Unknown rules agent"}

    action     = decision.get("action", "PASS")
    symbol     = decision.get("symbol")
    confidence = decision.get("confidence", 0)
    reason_str = decision.get("reason", "")

    if troi_caution_multiplier < 1.0:
        reason_str = f"[CAUTION half-size] {reason_str}"

    if action == "PASS":
        _log_decision(player_id, display_name, "PASS", None, 0,
                      reason_str, market_ctx, "RULES_PASS", False)
        return {"player_id": player_id, "action": "PASS", "reason": reason_str}

    if not symbol:
        _log_decision(player_id, display_name, "PASS", None, 0,
                      "No symbol from rules", market_ctx, "NO_SYMBOL", False)
        return {"player_id": player_id, "action": "PASS", "reason": "No symbol from rules"}

    # Falling knife filter: skip BUY if symbol is in active downtrend
    if action == "BUY" and _is_falling_knife(symbol, scan_picks):
        knife_reason = f"FALLING KNIFE blocked: {symbol} down >5% in 5d, RSI still > 35"
        _log_decision(player_id, display_name, "PASS", symbol, 0,
                      knife_reason, market_ctx, "FALLING_KNIFE", False)
        return {"player_id": player_id, "action": "PASS", "reason": knife_reason}

    # Universe restriction
    universe = mandate.get("universe")
    if universe and symbol not in universe:
        blocked_reason = f"{symbol} not in {display_name}'s universe"
        _log_decision(player_id, display_name, "PASS", symbol, 0,
                      blocked_reason, market_ctx, "UNIVERSE_BLOCK", False)
        return {"player_id": player_id, "action": "PASS", "reason": blocked_reason}

    # Get current price
    try:
        from engine.market_data import get_stock_price
        price_data = get_stock_price(symbol)
        price      = float(price_data.get("price") or 0)
        if price <= 0:
            raise ValueError(f"invalid price {price}")
    except Exception as e:
        reason_np = f"Price fetch failed for {symbol}: {e}"
        _log_decision(player_id, display_name, "PASS", symbol, 0,
                      reason_np, market_ctx, "NO_PRICE", False)
        return {"player_id": player_id, "action": "PASS", "reason": reason_np}

    # Submit trade
    conf_normalized = confidence / 100.0
    executed        = False
    gate_result     = "TRADE_SUBMITTED"

    try:
        from engine.paper_trader import buy, sell
        if action == "BUY":
            result      = buy(player_id=player_id, symbol=symbol, price=price,
                              reasoning=reason_str, confidence=conf_normalized,
                              timeframe="INTRADAY",
                              sizing_multiplier=troi_caution_multiplier)
            executed    = bool(result)
            gate_result = "EXECUTED" if executed else "TRADE_REJECTED"
        elif action == "SELL":
            result      = sell(player_id=player_id, symbol=symbol, price=price,
                               reasoning=reason_str, confidence=conf_normalized)
            executed    = bool(result)
            gate_result = "EXECUTED" if executed else "TRADE_REJECTED"
    except Exception as e:
        gate_result = "TRADE_ERROR"
        logger.error(f"{display_name} rules trade error: {e}")

    _log_decision(player_id, display_name, action, symbol, confidence,
                  reason_str, market_ctx, gate_result, executed)

    if executed:
        logger.info(f"✅ {display_name} [RULES]: {action} {symbol} @ ${price:.2f} conf={confidence}%")

    return {
        "player_id":   player_id,
        "action":      action,
        "symbol":      symbol,
        "confidence":  confidence,
        "executed":    executed,
        "gate_result": gate_result,
    }


# ---------------------------------------------------------------------------
# Decision logging
# ---------------------------------------------------------------------------

def _log_decision(
    player_id:    str,
    display_name: str,
    action:       str,
    symbol:       str | None,
    confidence:   int,
    reason:       str,
    market_ctx:   dict[str, Any],
    gate_result:  str,
    executed:     bool,
) -> None:
    c = _conn()
    try:
        c.execute(
            """
            INSERT INTO crew_decisions
                (timestamp, agent_name, player_id, action, symbol, confidence, reason, market_data, gate_result, executed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.now(timezone.utc).isoformat(),
                display_name,
                player_id,
                action,
                symbol,
                confidence,
                (reason or "")[:500],
                json.dumps({
                    "session": market_ctx.get("session_type"),
                    "vix":     market_ctx.get("vix"),
                    "fg":      market_ctx.get("fg_score"),
                    "pc":      market_ctx.get("pc_ratio"),
                }),
                (gate_result or "")[:300],
                1 if executed else 0,
            ),
        )
        c.commit()
    except Exception as e:
        logger.error(f"_log_decision error: {e}")
    finally:
        c.close()


# ---------------------------------------------------------------------------
# Single-agent scan
# ---------------------------------------------------------------------------

def _scan_single_agent(player_id: str, market_ctx: dict[str, Any]) -> dict[str, Any]:
    """
    Run the full scan pipeline for one agent.
    Returns a result dict describing what happened.
    """
    _init_once()
    mandate = CREW_MANIFEST.get(player_id)
    if not mandate:
        return {"player_id": player_id, "skipped": True, "reason": "No mandate"}

    display_name = mandate.get("display_name", player_id)

    # Skip bridge voters and zero-position agents
    if mandate.get("max_positions", 0) == 0:
        return {"player_id": player_id, "skipped": True, "reason": "No trading mandate (max_positions=0)"}

    # is_paused check — Spock can pause an agent via dashboard
    if _is_agent_paused(player_id):
        return {"player_id": player_id, "skipped": True, "reason": "Agent paused"}

    # ── Gate 1: Mandate check ─────────────────────────────────────────────────
    allowed, gate_reason = should_agent_trade(player_id, market_ctx)
    if not allowed:
        _log_decision(player_id, display_name, "PASS", None, 0,
                      gate_reason, market_ctx, "MANDATE_BLOCKED", False)
        return {"player_id": player_id, "action": "PASS", "reason": gate_reason, "gate": "mandate"}

    # Unrestricted agents (Janeway, Q) bypass daily-limit and fleet-exposure gates.
    # Only Event Shield CRITICAL and Troi STAND_DOWN can stop them.
    is_unrestricted = bool(mandate.get("unrestricted"))

    # ── Gate 2: Consecutive loss cooldown — ALERT ONLY, Spock warns but Captain decides
    if _check_cooldown(player_id):
        alert_msg = f"{display_name} has 3 consecutive losses. RECOMMEND: pause or reduce size."
        logger.info(f"⚠️  Spock alert: {alert_msg}")
        _save_spock_alert("HIGH", alert_msg, player_id,
                          f"Last 3 trades all losses for {display_name}")

    # ── Gate 3: Daily trade limit ─────────────────────────────────────────────
    if not is_unrestricted:
        trades_today = _count_today_trades(player_id)
        if trades_today >= _MAX_DAILY_TRADES_PER_AGENT:
            reason = f"Daily limit: {trades_today}/{_MAX_DAILY_TRADES_PER_AGENT} trades today"
            _log_decision(player_id, display_name, "PASS", None, 0,
                          reason, market_ctx, "DAILY_LIMIT", False)
            return {"player_id": player_id, "action": "PASS", "reason": reason, "gate": "daily_limit"}

    # ── Gate 4: Fleet exposure cap ────────────────────────────────────────────
    if not is_unrestricted:
        fleet_pct = _total_fleet_exposure_pct()
        if fleet_pct > _FLEET_EXPOSURE_MAX_PCT:
            reason = f"Fleet exposure {fleet_pct:.0f}% exceeds {_FLEET_EXPOSURE_MAX_PCT}% limit"
            _log_decision(player_id, display_name, "PASS", None, 0,
                          reason, market_ctx, "FLEET_EXPOSURE", False)
            return {"player_id": player_id, "action": "PASS", "reason": reason, "gate": "fleet_exposure"}

    # ── Gate 5: Troi signal ───────────────────────────────────────────────────
    # T'Pol and McCoy use their own VIX/mandate gates — exempt from Troi STAND_DOWN.
    _TROI_STAND_DOWN_EXEMPT = {"dayblade-0dte", "ollama-plutus"}
    troi_signal = market_ctx.get("troi_signal", "GO")
    if troi_signal == "STAND_DOWN" and player_id not in _TROI_STAND_DOWN_EXEMPT:
        reason = "Troi: STAND_DOWN — market structure unfavorable"
        _log_decision(player_id, display_name, "PASS", None, 0,
                      reason, market_ctx, "TROI_STAND_DOWN", False)
        return {"player_id": player_id, "action": "PASS", "reason": reason, "gate": "troi"}
    # CAUTION → allow trade but halve position size via confidence reduction
    troi_caution_multiplier = 0.5 if troi_signal == "CAUTION" else 1.0

    # ── Gate 6: Event Shield CRITICAL ─────────────────────────────────────────
    if market_ctx.get("event_shield_blocked"):
        reason = f"Event Shield: CRITICAL event in progress — no trading"
        _log_decision(player_id, display_name, "PASS", None, 0,
                      reason, market_ctx, "EVENT_SHIELD", False)
        return {"player_id": player_id, "action": "PASS", "reason": reason, "gate": "event_shield"}

    # ── Build prompts ──────────────────────────────────────────────────────────
    ds_top        = market_ctx.get("deep_scan_top", [])
    spy_vol_ratio = float(market_ctx.get("spy_volume_ratio", 1.0))
    vol_spikes    = market_ctx.get("volume_spikes", [])
    breadth       = market_ctx.get("breadth_score", "?")
    time_label    = _get_market_session_label()
    time_line     = f"Time of day: {time_label}\n" if time_label else ""
    vol_flag      = " ⚠ HIGH VOLUME DAY" if spy_vol_ratio >= 1.5 else ""

    if player_id == "neo-matrix":
        # ── NEO: aggressive rebuilt system prompt ──────────────────────────────
        system_prompt = (
            "You are Neo — The One. You see the Matrix. You find trades others miss. "
            "You are AGGRESSIVE but SMART.\n\n"
            "YOUR RULES:\n"
            "1. On HIGH VOLUME days (>1.5x avg): you MUST find a trade. "
            "PASS is not acceptable when volume is screaming.\n"
            "2. You prefer MOMENTUM — stocks moving WITH volume. Up on high volume = BUY. "
            "Down after extended drop on high volume = BUY the bounce.\n"
            "3. You trade the LEADERS — NVDA, AMD, TSLA, META, AAPL, AMZN, NFLX, GOOGL. "
            "Not XOM, not GE. The stocks that move the market.\n"
            "4. Confidence must be 70+ to trade. On high volume days, 60+ is acceptable.\n"
            "5. NEVER buy falling knives — down >5% in 5 days AND below 20-day avg: SKIP.\n"
            "6. Look for SETUPS: Breakout (crossing resistance on vol), "
            "Bounce (oversold + green candle + vol), Momentum (up >1% on 2x vol).\n"
            "7. Your thesis must be ONE sentence.\n\n"
            "CONFIDENCE SCORING GUIDE:\n"
            "  90+: Perfect setup — trend + volume + RSI + sector + catalyst all align. Rare. Go big.\n"
            "  80-89: Strong setup — 4/5 signals align. Standard size.\n"
            "  70-79: Good setup — 3/5 signals align. Smaller size.\n"
            "  60-69: Speculative — 2/5 signals align. Minimum size (high-vol days only).\n"
            "  50: DEFAULT IS NOT ACCEPTABLE. If you can't score above 60, say PASS.\n"
            "      A confidence of 50 means you're guessing — don't guess.\n\n"
            "JUSTIFY your confidence with specific signals:\n"
            "  'TRADE BUY NVDA 82 — RSI 38 bounce + 2.3x volume + above SMA20 + tech leading (4/5)'\n\n"
            "Format: TRADE BUY [SYMBOL] [CONFIDENCE 0-100] [ONE SENTENCE THESIS]\n"
            "Or: PASS [ONE SENTENCE WHY]"
        )
        # Neo sees ONLY preferred symbols — sort volume spikes first, then ROC
        neo_ds = [p for p in ds_top if p.get("symbol") in NEO_PREFERRED]
        neo_ds.sort(key=lambda p: (-(p.get("volume_ratio", 1)), -(p.get("roc_5d", 0))))
        ds_syms = ", ".join(
            f"{r.get('symbol','?')}({float(r.get('signal_strength',0)):.2f})"
            for r in neo_ds[:5]
        ) or "none (no preferred symbols in scan today)"
        # Volume spikes for Neo's preferred names
        neo_spikes = [s for s in vol_spikes if s["symbol"] in NEO_PREFERRED][:3]
        vol_spike_lines = "\n".join(
            f"  {s['symbol']}: ${s['price']:.2f} "
            f"({'+'if s['change_pct']>=0 else ''}{s['change_pct']:.1f}%) "
            f"vol {s['volume_ratio']:.1f}x"
            for s in neo_spikes
        ) or "  none in preferred list"
        # Bounce candidates
        bounce_lines = "\n".join(
            f"  {b['symbol']}: RSI {b.get('rsi_14',0):.0f}, "
            f"5d {b.get('roc_5d',0):+.1f}%, {b.get('volume_ratio',1):.1f}x vol"
            for b in _find_bounces(ds_top)[:2]
        ) or "  none"
        # Sector ETF flow from volume spikes
        _sector_etfs = {"XLK", "XLE", "XLF", "XLU", "XLP", "GLD"}
        sector_flow = "  " + " | ".join(
            f"{s['symbol']} {s['change_pct']:+.1f}%"
            for s in vol_spikes if s["symbol"] in _sector_etfs
        ) or "  unavailable"
        # Falling knives to avoid
        knives = [
            p["symbol"] for p in ds_top
            if p.get("roc_5d", 0) < -5.0
            and float(p.get("close", 0)) < float(p.get("sma_20", 99999))
        ][:4]
        user_prompt = (
            f"Session: {market_ctx.get('session_type','?')}{vol_flag}\n"
            f"{time_line}"
            f"VIX: {float(market_ctx.get('vix',0)):.1f} | "
            f"F&G: {market_ctx.get('fg_score','?')} | "
            f"SPY: ${float(market_ctx.get('spy_price',0)):.2f} "
            f"({float(market_ctx.get('spy_day_return',0)):+.1f}%) "
            f"Vol: {spy_vol_ratio:.1f}x\n"
            f"Momentum: {float(market_ctx.get('momentum_score',0)):.0f} | "
            f"Breadth: {breadth}/11\n\n"
            f"TOP VOLUME SPIKES (leaders):\n{vol_spike_lines}\n\n"
            f"BOUNCE CANDIDATES:\n{bounce_lines}\n\n"
            f"SECTOR FLOW:\n{sector_flow}\n\n"
            f"AVOID (falling knives): {', '.join(knives) if knives else 'none'}\n\n"
            f"Top scan picks: {ds_syms}\n"
            f"Your mandate: Trade the leaders. Find momentum. Catch bounces.\n"
            f"Decision?"
        )

    else:
        # ── All other Ollama agents — standard prompt ──────────────────────────
        _raw_hint = _AGENT_SCAN_HINTS.get(
            player_id,
            f"Find the best setup for your mandate: {mandate.get('strategy', '')}",
        )
        scan_hint = _raw_hint.format(
            spy_wall_signal=market_ctx.get("spy_wall_signal", "NONE"),
            spy_wall_reason=market_ctx.get("spy_wall_reason", ""),
        ) if "{spy_wall_signal}" in _raw_hint else _raw_hint
        ds_syms   = ", ".join(r.get("symbol", "?") for r in ds_top[:5]) if ds_top else "none"
        spike_str = (
            ", ".join(
                f"{s['symbol']} {s['volume_ratio']:.1f}x"
                f"({'+'if s['change_pct']>=0 else ''}{s['change_pct']:.1f}%)"
                for s in vol_spikes[:5]
            ) if vol_spikes else "none"
        )
        system_prompt = (
            f"You are {display_name}. Decide: TRADE or PASS.\n"
            f"Format: TRADE BUY/SELL [SYMBOL] [CONFIDENCE 0-100] [REASON]\n"
            f"Or: PASS [REASON]\n"
            f"One line only. No explanation."
        )
        user_prompt = (
            f"Session: {market_ctx.get('session_type','?')}{vol_flag}\n"
            f"{time_line}"
            f"VIX: {float(market_ctx.get('vix', 0)):.1f} | "
            f"SPY vol: {spy_vol_ratio:.1f}x avg\n"
            f"Volume spikes: {spike_str}\n"
            f"Momentum: {float(market_ctx.get('momentum_score', 0)):.0f} | "
            f"F&G: {market_ctx.get('fg_score','?')} | "
            f"Breadth: {breadth}/11\n"
            f"Top scan picks: {ds_syms}\n"
            f"Your mandate: {mandate.get('strategy', 'unrestricted')}. {scan_hint}\n"
            f"Decision?"
        )

    logger.info(f"Querying Ollama: {display_name} ({SCAN_MODEL})…")
    response = _query_ollama(player_id, SCAN_MODEL, system_prompt, user_prompt)

    if not response:
        reason = "Ollama timeout — no response"
        _log_decision(player_id, display_name, "PASS", None, 0,
                      reason, market_ctx, "OLLAMA_TIMEOUT", False)
        return {"player_id": player_id, "action": "PASS", "reason": reason, "gate": "timeout"}

    decision   = _parse_ollama_decision(response)
    action     = decision["action"]
    symbol     = decision.get("symbol")
    confidence = decision.get("confidence", 50)
    reason_str = decision.get("reason", "")
    # Troi CAUTION: annotate reason; sizing reduction is handled in paper_trader.buy()
    if troi_caution_multiplier < 1.0:
        reason_str = f"[CAUTION half-size] {reason_str}"

    if action == "PASS":
        # Neo gets a second shot on high-volume days — focused query on top spikes only
        if player_id == "neo-matrix" and spy_vol_ratio >= 1.5:
            neo_vol_picks = sorted(
                [s for s in vol_spikes if s["symbol"] in NEO_PREFERRED],
                key=lambda x: -x["volume_ratio"]
            )[:4]
            if neo_vol_picks:
                pick_lines = "\n".join(
                    f"{s['symbol']} ${s['price']:.2f} ({s['change_pct']:+.1f}%) vol {s['volume_ratio']:.1f}x"
                    for s in neo_vol_picks
                )
                retry_prompt = (
                    f"HIGH VOLUME DAY. Pick the best setup:\n"
                    f"{pick_lines}\n"
                    f"TRADE BUY [SYMBOL] [CONFIDENCE] [REASON] or PASS [WHY]"
                )
                logger.info(f"Neo PASS on vol day — second look: {[s['symbol'] for s in neo_vol_picks]}")
                r2 = _query_ollama(player_id, SCAN_MODEL, system_prompt, retry_prompt)
                if r2:
                    d2 = _parse_ollama_decision(r2)
                    if d2["action"] != "PASS":
                        action     = d2["action"]
                        symbol     = d2.get("symbol")
                        confidence = d2.get("confidence", 60)
                        reason_str = f"[2nd look vol day] {d2.get('reason','')}"

            # Mr. Anderson — aggressive momentum + gap-and-go channel signals
            if player_id == "super-agent":
                from engine.channel_scanner import scan_channel as _ch_scan
                try:
                    _anderson_gap  = _ch_scan("gap-and-go")[:3]
                    _anderson_mom  = _ch_scan("momentum-breakout")[:3]
                    _anderson_sigs = _anderson_gap + _anderson_mom
                    if _anderson_sigs:
                        anderson_picks = [
                            f"{s['symbol']} gap={s.get('change_pct', 0):+.1f}% rvol={s.get('rel_volume', 0):.1f}x"
                            for s in _anderson_sigs[:4]
                        ]
                        context_parts.append(
                            "MR. ANDERSON CHANNEL SIGNALS (gap-and-go + momentum): "
                            + " | ".join(anderson_picks)
                        )
                except Exception:
                    pass

        if action == "PASS":
            _log_decision(player_id, display_name, "PASS", None, 0,
                          reason_str, market_ctx, "AGENT_PASS", False)
            return {"player_id": player_id, "action": "PASS", "reason": reason_str}

    # Agent wants to trade
    if not symbol:
        _log_decision(player_id, display_name, "PASS", None, 0,
                      "No symbol in response", market_ctx, "NO_SYMBOL", False)
        return {"player_id": player_id, "action": "PASS", "reason": "No symbol in response"}

    # Falling knife filter
    if action == "BUY":
        scan_picks_for_knife = market_ctx.get("deep_scan_top", [])
        if _is_falling_knife(symbol, scan_picks_for_knife):
            knife_reason = f"FALLING KNIFE blocked: {symbol} down >5% in 5d, RSI still > 35"
            _log_decision(player_id, display_name, "PASS", symbol, 0,
                          knife_reason, market_ctx, "FALLING_KNIFE", False)
            return {"player_id": player_id, "action": "PASS", "reason": knife_reason}

    # ── Universe restriction check ────────────────────────────────────────────
    universe = mandate.get("universe")
    if universe and symbol not in universe:
        blocked_reason = f"{symbol} not in {display_name}'s universe"
        _log_decision(player_id, display_name, "PASS", symbol, 0,
                      blocked_reason, market_ctx, "UNIVERSE_BLOCK", False)
        return {"player_id": player_id, "action": "PASS", "reason": blocked_reason}

    # ── Get current price ─────────────────────────────────────────────────────
    try:
        from engine.market_data import get_stock_price
        price_data = get_stock_price(symbol)
        price      = float(price_data.get("price") or 0)
        if price <= 0:
            raise ValueError(f"invalid price {price}")
    except Exception as e:
        reason_np = f"Price fetch failed for {symbol}: {e}"
        _log_decision(player_id, display_name, "PASS", symbol, 0,
                      reason_np, market_ctx, "NO_PRICE", False)
        return {"player_id": player_id, "action": "PASS", "reason": reason_np}

    # ── Gate 7: Sniper Mode alpha gate ────────────────────────────────────────
    # Dual filter: composite_alpha >= 0.3 AND LLM confidence >= 65
    # Unrestricted agents (Neo) bypass alpha gate — they trust their own signals
    if not is_unrestricted and action == "BUY":
        live_alpha = _get_live_alpha(symbol)
        if live_alpha < SNIPER_ALPHA_THRESHOLD:
            sniper_reason = (
                f"Sniper gate: {symbol} alpha={live_alpha:.3f} < {SNIPER_ALPHA_THRESHOLD} threshold"
            )
            _log_decision(player_id, display_name, "PASS", symbol, confidence,
                          sniper_reason, market_ctx, "SNIPER_ALPHA_GATE", False)
            return {"player_id": player_id, "action": "PASS", "reason": sniper_reason, "gate": "sniper_alpha"}
        if confidence < SNIPER_MIN_CONFIDENCE:
            sniper_reason = (
                f"Sniper gate: confidence={confidence} < {SNIPER_MIN_CONFIDENCE} minimum"
            )
            _log_decision(player_id, display_name, "PASS", symbol, confidence,
                          sniper_reason, market_ctx, "SNIPER_CONF_GATE", False)
            return {"player_id": player_id, "action": "PASS", "reason": sniper_reason, "gate": "sniper_conf"}

    # ── Gate 8a: CSP IV Rank filter ───────────────────────────────────────────
    # CSPs need adequate premium cushion — block low-IV entries (assignment risk)
    if action == "BUY":
        _strategy_hint = mandate.get("strategy") or mandate.get("preferred_strategies", [None])[0]
        if _strategy_hint == "csp":
            try:
                from engine.high_iv_scanner import _get_iv_rank as _live_ivr
                _iv_data = _live_ivr(symbol)
                _ivr_val = _iv_data.get("iv_rank", 50.0) if _iv_data else 50.0
            except Exception:
                _ivr_val = 50.0  # assume acceptable if data unavailable
            if _ivr_val < CSP_MIN_IVR:
                _ivr_reason = (
                    f"CSP IV filter: {symbol} IVR={_ivr_val:.1f} < {CSP_MIN_IVR} minimum "
                    f"(low premium = assignment risk)"
                )
                _log_decision(player_id, display_name, "PASS", symbol, confidence,
                              _ivr_reason, market_ctx, "CSP_LOW_IVR", False)
                logger.info(f"📉 CSP blocked {symbol}: IVR={_ivr_val:.1f} < {CSP_MIN_IVR}")
                return {"player_id": player_id, "action": "PASS", "reason": _ivr_reason, "gate": "csp_ivr"}

    # ── Gate 8: Ollie Commander approval ──────────────────────────────────────
    # Ollie scores every Sniper BUY — unrestricted agents (Neo) still pass through
    # rsi_bounce bypasses Ollie (equity signals score poorly on options-heavy rubric)
    if action == "BUY" and player_id != OLLIE_ID:
        try:
            from engine.ollie_commander import approve_or_reject as _ollie_approve
            _strategy = mandate.get("strategy") or mandate.get("preferred_strategies", [None])[0]
            # rsi_bounce bypasses Ollie — equity signals score low on options rubric
            if _strategy in BYPASS_OLLIE:
                logger.debug(f"🟢 Ollie bypassed for {player_id} {symbol} ({_strategy})")
                reason_str = f"[Ollie⏭ bypass:{_strategy}] {reason_str}"
                _approved, _ollie_score, _ollie_reason = True, 0.0, "bypass"
            else:
                _approved, _ollie_score, _ollie_reason = _ollie_approve(
                    player_id  = player_id,
                    symbol     = symbol,
                    confidence = float(confidence),
                    strategy   = _strategy,
                    market_ctx = market_ctx,
                )
            if not _approved:
                _log_decision(player_id, display_name, "PASS", symbol, confidence,
                              f"Ollie NO-GO: {_ollie_reason}", market_ctx,
                              "OLLIE_REJECTED", False)
                logger.info(f"🐾 Ollie REJECTED {player_id} {symbol} — {_ollie_reason}")
                return {
                    "player_id":   player_id,
                    "action":      "PASS",
                    "reason":      f"Ollie NO-GO: score={_ollie_score:.2f} < 2.0",
                    "gate":        "ollie_commander",
                    "ollie_score": _ollie_score,
                }
            # Approved — annotate the reason string for the trade log
            reason_str = f"[Ollie✓ {_ollie_score:.2f}] {reason_str}"
        except Exception as _ollie_err:
            logger.warning(f"Ollie Commander error (bypassing gate): {_ollie_err}")

    # ── Submit trade ──────────────────────────────────────────────────────────
    conf_normalized = confidence / 100.0
    executed        = False
    gate_result     = "TRADE_SUBMITTED"

    # Neo: confidence-based position sizing
    neo_qty: float | None = None
    if player_id == "neo-matrix" and action == "BUY" and price > 0:
        try:
            from engine.paper_trader import get_portfolio as _gp
            _cash = float(_gp("neo-matrix").get("cash", 0))
            if confidence >= 90:
                _size_pct = 0.10
            elif confidence >= 80:
                _size_pct = 0.07
            elif confidence >= 70:
                _size_pct = 0.05
            else:  # 60-69: cautious entry on high-vol day only
                _size_pct = 0.03
            neo_qty = (_cash * _size_pct) / price
        except Exception:
            neo_qty = None

    try:
        from engine.paper_trader import buy, sell
        if action == "BUY":
            result = buy(
                player_id         = player_id,
                symbol            = symbol,
                price             = price,
                qty               = neo_qty,   # None = default sizing for non-Neo agents
                reasoning         = reason_str,
                confidence        = conf_normalized,
                timeframe         = "INTRADAY",
                sizing_multiplier = troi_caution_multiplier,
            )
            executed    = bool(result)
            gate_result = "EXECUTED" if executed else "TRADE_REJECTED"
        elif action == "SELL":
            result = sell(
                player_id  = player_id,
                symbol     = symbol,
                price      = price,
                reasoning  = reason_str,
                confidence = conf_normalized,
            )
            executed    = bool(result)
            gate_result = "EXECUTED" if executed else "TRADE_REJECTED"
        else:
            # SHORT — treat as BUY on an inverse ETF or log only
            gate_result = "SHORT_LOGGED"
            logger.info(f"{display_name}: SHORT {symbol} — logged, not executed (no short selling in paper mode)")
    except Exception as e:
        gate_result = f"TRADE_ERROR"
        logger.error(f"{display_name} trade error: {e}")

    _log_decision(player_id, display_name, action, symbol, confidence,
                  reason_str, market_ctx, gate_result, executed)

    if executed:
        logger.info(f"✅ {display_name}: {action} {symbol} @ ${price:.2f} conf={confidence}%")
        _save_notification(
            title=f"📈 {display_name} opened position",
            body=f"{display_name} {action} {symbol} (conf: {confidence}%)",
            severity="trade",
            notif_type="trade",
            icon="📈",
            agent_id=player_id
        )

    return {
        "player_id":  player_id,
        "action":     action,
        "symbol":     symbol,
        "confidence": confidence,
        "executed":   executed,
        "gate_result":gate_result,
    }


# ---------------------------------------------------------------------------
# Master scan cycle
# ---------------------------------------------------------------------------

def run_scan_cycle(
    tier_filter: str | None = None,
    verbose:     bool       = True,
) -> dict[str, Any]:
    """
    Run the crew scanner for one cycle.
    Wrapped in top-level try/except so a crash never kills the scheduler.
    """
    try:
        return _run_scan_cycle_inner(tier_filter=tier_filter, verbose=verbose)
    except Exception as e:
        logger.error(f"Scan cycle crashed (will retry next cycle): {e}", exc_info=True)
        return {"error": str(e), "agents_scanned": 0, "made_trades": 0}


def _run_scan_cycle_inner(
    tier_filter: str | None = None,
    verbose:     bool       = True,
) -> dict[str, Any]:
    scan_state["active"] = True
    try:
        return _run_scan_cycle_body(tier_filter=tier_filter, verbose=verbose)
    finally:
        scan_state["active"] = False


def _run_scan_cycle_body(
    tier_filter: str | None = None,
    verbose:     bool       = True,
) -> dict[str, Any]:
    _init_once()
    _ensure_warm()

    # ── Market context first — position management needs vol/VIX data ────────
    ctx = gather_market_context()

    # Volatile day: SPY volume > 1.5x avg → shift scaled-exit thresholds down 1%
    volatile_day = float(ctx.get("spy_volume_ratio", 1.0)) >= 1.5

    # ── Position management (no LLM, instant) ─────────────────────────────────
    neo_trail_exits = _update_neo_trailing_stops()
    hard_stops_cut  = _check_hard_stops()
    scaled_exits    = _check_scaled_exits(volatile_day=volatile_day)
    dip_buys        = _check_dip_buys()
    if neo_trail_exits:
        logger.info(f"🏃 Neo trailing stops fired: {neo_trail_exits} runner(s) closed")
    if hard_stops_cut:
        logger.info(f"✂️  Hard stops fired: {hard_stops_cut} position(s) cut")
    if scaled_exits:
        logger.info(f"📈 Scaled exits fired: {scaled_exits} partial sell(s)"
                    + (" [volatile day]" if volatile_day else ""))
    if dip_buys:
        logger.info(f"📉 Dip buys fired: {dip_buys} position(s) averaged")

    total          = 0
    passed_mandate = 0
    made_trades    = 0
    blocked        = 0

    def _tally(result: dict) -> None:
        nonlocal total, passed_mandate, made_trades, blocked
        if result.get("skipped"):
            return
        total += 1
        act  = result.get("action", "PASS")
        gate = result.get("gate", "")
        if act == "PASS":
            if gate == "mandate":
                pass
            elif gate:
                blocked += 1
            else:
                passed_mandate += 1
        else:
            passed_mandate += 1
            if result.get("executed"):
                made_trades += 1

    # ── Rules agents first (instant — no Ollama) ─────────────────────────────
    for player_id in RULES_SCANNERS:
        try:
            _tally(_scan_rules_agent(player_id, ctx))
        except Exception as e:
            logger.error(f"run_scan_cycle rules error for {player_id}: {e}")

    # ── Always-on non-Ollama agents ───────────────────────────────────────────
    for player_id in ACTIVE_SCANNERS:
        try:
            _tally(_scan_single_agent(player_id, ctx))
        except Exception as e:
            logger.error(f"run_scan_cycle error for {player_id}: {e}")
        time.sleep(0.5)

    # ── Alpha Squad pair rotation (≤2 Ollama models loaded at once) ───────────
    alpha_pair = get_alpha_pair()
    logger.info(f"Alpha Squad scan: pair={alpha_pair}")
    for player_id in alpha_pair:
        if _is_agent_paused(player_id):
            continue
        try:
            _tally(_scan_single_agent(player_id, ctx))
        except Exception as e:
            logger.error(f"run_scan_cycle alpha error for {player_id}: {e}")
        time.sleep(1.0)  # gap between pair agents to avoid Ollama swap

    summary = {
        "scan_time":        datetime.now(timezone.utc).isoformat(),
        "tier_filter":      tier_filter or "all",
        "agents_scanned":   total,
        "passed_mandate":   passed_mandate,
        "made_trades":      made_trades,
        "blocked_by_gates": blocked,
        "neo_trail_exits":  neo_trail_exits,
        "hard_stops_cut":   hard_stops_cut,
        "scaled_exits":     scaled_exits,
        "dip_buys":         dip_buys,
        "session_type":     ctx.get("session_type"),
        "vix":              ctx.get("vix"),
        "troi_signal":      ctx.get("troi_signal"),
    }

    if verbose:
        vix_val = ctx.get("vix") or 0
        logger.info(
            "[CrewScanner] %s: %d agents, %d cleared mandate, %d trade(s), "
            "%d gate-blocked | session=%s VIX=%.1f Troi=%s",
            tier_filter or "ALL", total, passed_mandate, made_trades, blocked,
            ctx.get("session_type", "?"), float(vix_val),
            ctx.get("troi_signal", "?"),
        )

    # ── User-created agents (natural language rules) ───────────────────────
    try:
        from engine.agent_builder import check_user_agents
        ua_triggered = check_user_agents({
            "vix":         ctx.get("vix"),
            "session_type": ctx.get("session_type"),
            "gex_regime":  ctx.get("gex_regime") or ctx.get("troi_signal"),
        })
        if ua_triggered:
            summary["user_agents_triggered"] = ua_triggered
    except Exception as _ua_err:
        logger.warning(f"[CrewScanner] user_agents check error: {_ua_err}")

    # ── Spock risk alerts — evaluate after each scan cycle ─────────────────
    _run_spock_risk_alerts()

    # ── Ollie Auto — Signal Center + fleet consensus entry pipeline ─────────
    # Shelved in Sniper Mode Go Live — skip if is_paused=1 in DB
    if not _is_agent_paused("ollie-auto"):
        try:
            ollie_trades = ollie_auto_check(ctx)
            if ollie_trades:
                summary["ollie_auto_trades"] = len(ollie_trades)
                logger.info("[OllieAuto] executed %d trade(s) this cycle", len(ollie_trades))
        except Exception as _ha_err:
            logger.warning(f"[OllieAuto] scan hook error: {_ha_err}")

    # ── Ollie Tiered TP — monitor open super trades each cycle ───────────────
    try:
        tp_actions = _ollie_check_tiered_tp()
        if tp_actions:
            summary["ollie_tp_actions"] = len(tp_actions)
            logger.info("[OllieAuto] tiered TP: %d action(s) this cycle", len(tp_actions))
    except Exception as _tp_err:
        logger.warning(f"[OllieAuto] tiered TP check error: {_tp_err}")

    return summary


def _run_spock_risk_alerts() -> None:
    """Evaluate per-agent and fleet-wide daily P&L and emit Spock alerts."""
    _init_risk_alerts_table()
    try:
        c = sqlite3.connect(DB_PATH, timeout=10)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Per-agent: today's closed P&L
        agent_rows = c.execute(
            """SELECT player_id, SUM(realized_pnl) as day_pnl
               FROM trades
               WHERE action='SELL' AND realized_pnl IS NOT NULL
               AND date(executed_at) = ?
               GROUP BY player_id""",
            (today,),
        ).fetchall()

        fleet_today = 0.0
        for row in agent_rows:
            pid = row[0]
            day_pnl = row[1] or 0.0
            fleet_today += day_pnl
            if day_pnl < -500:
                name_row = c.execute(
                    "SELECT display_name FROM ai_players WHERE id=?", (pid,)
                ).fetchone()
                name = name_row[0] if name_row else pid
                msg = f"{name} lost ${abs(day_pnl):.0f} today. RECOMMEND: halt for remainder of day."
                _save_spock_alert("HIGH", msg, pid, f"Agent daily P&L: ${day_pnl:.2f}")

        # Fleet-wide
        if fleet_today < -1500:
            msg = f"Fleet down ${abs(fleet_today):.0f} today. RECOMMEND: halt ALL new entries."
            _save_spock_alert("CRITICAL", msg, None, f"Fleet daily P&L: ${fleet_today:.2f}")
        elif fleet_today < -800:
            msg = f"Fleet down ${abs(fleet_today):.0f} today. RECOMMEND: reduce to half size."
            _save_spock_alert("HIGH", msg, None, f"Fleet daily P&L: ${fleet_today:.2f}")

        c.close()
    except Exception as e:
        logger.warning(f"_run_spock_risk_alerts error: {e}")


# ---------------------------------------------------------------------------
# Dashboard query
# ---------------------------------------------------------------------------

def get_crew_decisions(limit: int = 50) -> list[dict[str, Any]]:
    """Return recent crew decisions for the dashboard."""
    _init_once()
    c = _conn()
    try:
        rows = c.execute(
            """
            SELECT timestamp, agent_name, player_id, action, symbol,
                   confidence, reason, gate_result, executed
            FROM crew_decisions
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"get_crew_decisions error: {e}")
        return []
    finally:
        c.close()


# ---------------------------------------------------------------------------
# Ollie Auto-Trader
# ---------------------------------------------------------------------------

_OLLIE_AUTO_ID = "ollie-auto"
_OLLIE_AUTO_SEASON = 5


def _init_ollie_auto_player() -> None:
    """Ensure ollie-auto exists in ai_players with season-5 cash."""
    try:
        c = sqlite3.connect(DB_PATH, timeout=10)
        c.execute(
            """INSERT OR IGNORE INTO ai_players
               (id, display_name, provider, model_id, cash, season, is_active)
               VALUES (?, 'Ollie', 'ollie', 'ollie-auto', 10000.0, ?, 1)""",
            (_OLLIE_AUTO_ID, _OLLIE_AUTO_SEASON),
        )
        c.commit()
        c.close()
    except Exception as e:
        logger.warning(f"[OllieAuto] init player error: {e}")


# ---------------------------------------------------------------------------
# Ollie Super Trader — Signal Center integration helpers
# ---------------------------------------------------------------------------

_SIGNAL_CENTER_DB = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "signal-center", "signals.db"
)


def _init_ollie_super_trades_table() -> None:
    """Create ollie_super_trades table (tiered TP tracker + backtesting log)."""
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS ollie_super_trades (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                buy_trade_id   INTEGER,
                symbol         TEXT NOT NULL,
                player_id      TEXT NOT NULL,
                entry_price    REAL,
                stop           REAL,
                tp1            REAL,
                tp2            REAL,
                tp3            REAL,
                trail_stop     REAL,
                initial_qty    REAL,
                regime         TEXT,
                signal_source  TEXT,
                signal_grade   TEXT,
                signal_score   REAL,
                success_prob   REAL,
                tp1_hit        INTEGER DEFAULT 0,
                tp2_hit        INTEGER DEFAULT 0,
                tp3_hit        INTEGER DEFAULT 0,
                closed         INTEGER DEFAULT 0,
                created_at     TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.commit()
    finally:
        c.close()


def _init_ollie_performance_table() -> None:
    """Create ollie_performance table — INSERT ONLY, never delete."""
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS ollie_performance (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol              TEXT NOT NULL,
                player_id           TEXT NOT NULL,
                grade_at_entry      TEXT,
                score_at_entry      REAL,
                probability_at_entry REAL,
                regime_at_entry     TEXT,
                signal_source       TEXT,
                entry_price         REAL,
                exit_price          REAL,
                pnl                 REAL,
                pnl_pct             REAL,
                exit_reason         TEXT,
                hold_hours          REAL,
                tp1_hit             INTEGER DEFAULT 0,
                tp2_hit             INTEGER DEFAULT 0,
                tp3_hit             INTEGER DEFAULT 0,
                created_at          TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.commit()
    finally:
        c.close()


def _fetch_sc_top_picks(limit: int = 10) -> list[dict]:
    """Query signal-center/signals.db for today's top SCREENER picks.

    Score mapping (SCREENER congress_insider, range 1-4):
        4 → scaled 80 → Grade A (≥ 75),  success_prob 0.80
        3 → scaled 60 → Grade B (≥ 60),  success_prob 0.65
        < 3 → below threshold, skipped

    Falls back to yesterday's data if today has no SCREENER rows.
    """
    import sqlite3 as _sq
    try:
        c = _sq.connect(_SIGNAL_CENTER_DB, check_same_thread=False, timeout=5)
        c.row_factory = _sq.Row
        for days_back in (0, 1):
            rows = c.execute("""
                SELECT
                  json_extract(data, '$.symbol')                      AS symbol,
                  MAX(CAST(json_extract(data, '$.score') AS REAL))     AS raw_score,
                  json_extract(data, '$.preset')                       AS preset,
                  MAX(created_at)                                      AS latest
                FROM intelligence_feed
                WHERE feed_type IN ('SCREENER', 'PREMARKET_SCAN')
                  AND date(created_at) = date('now', ? || ' days')
                  AND json_extract(data, '$.symbol') IS NOT NULL
                  AND CAST(json_extract(data, '$.score') AS REAL) >= 3
                GROUP BY json_extract(data, '$.symbol')
                ORDER BY raw_score DESC
                LIMIT ?
            """, (f"-{days_back}", limit)).fetchall()
            if rows:
                break
        c.close()
    except Exception as e:
        logger.warning(f"[OllieAuto] signal center DB error: {e}")
        return []

    result = []
    for r in rows:
        raw = float(r["raw_score"] or 0)
        scaled = raw * 20.0            # 4→80(A), 3→60(B)
        if scaled >= 75:
            grade, prob = "A", 0.80
        elif scaled >= 60:
            grade, prob = "B", 0.65
        else:
            continue
        result.append({
            "symbol":       r["symbol"],
            "raw_score":    raw,
            "scaled_score": scaled,
            "grade":        grade,
            "success_prob": prob,
            "preset":       r["preset"] or "",
            "source":       "signal_center",
        })
    return result


def _fetch_trade_levels_9000(symbol: str) -> dict | None:
    """Fetch ATR-based long trade levels from Signal Center port 9000 (cached)."""
    import urllib.request
    try:
        url = f"http://127.0.0.1:9000/api/trade-levels/{symbol.upper()}"
        with urllib.request.urlopen(url, timeout=8) as r:
            data = json.loads(r.read())
        return None if data.get("error") else data
    except Exception:
        return None


def _fetch_trade_levels_bulk(symbols: list) -> dict:
    """Fetch trade levels for multiple symbols in one request via /bulk endpoint.
    Returns {symbol: levels_dict} for all that succeeded.
    """
    import urllib.request, urllib.parse
    if not symbols:
        return {}
    qs = urllib.parse.urlencode({"symbols": ",".join(s.upper() for s in symbols)})
    try:
        url = f"http://127.0.0.1:9000/api/trade-levels/bulk?{qs}"
        with urllib.request.urlopen(url, timeout=15) as r:
            data = json.loads(r.read())
        return data.get("levels", {})
    except Exception:
        # Fall back to individual fetches
        result = {}
        for sym in symbols:
            lv = _fetch_trade_levels_9000(sym)
            if lv:
                result[sym] = lv
        return result


def _get_regime_from_8080() -> str:
    """Return current market regime string from 8080/api/regime."""
    import urllib.request
    try:
        with urllib.request.urlopen("http://127.0.0.1:8080/api/regime", timeout=3) as r:
            data = json.loads(r.read())
        return (data.get("regime") or "UNKNOWN").upper()
    except Exception:
        return "UNKNOWN"


def _ollie_update_tp(st_id: int, tp1_hit: bool = False, tp2_hit: bool = False,
                     tp3_hit: bool = False, trail_stop: float | None = None,
                     closed: bool = False) -> None:
    """Update TP hit flags, trail_stop, and closed in ollie_super_trades."""
    c = _conn()
    try:
        fields, vals = [], []
        if tp1_hit:               fields.append("tp1_hit=1")
        if tp2_hit:               fields.append("tp2_hit=1")
        if tp3_hit:               fields.append("tp3_hit=1")
        if closed:                fields.append("closed=1")
        if trail_stop is not None: fields.append("trail_stop=?"); vals.append(trail_stop)
        if not fields:            return
        vals.append(st_id)
        c.execute(f"UPDATE ollie_super_trades SET {', '.join(fields)} WHERE id=?", vals)
        c.commit()
    finally:
        c.close()


def _ollie_mark_closed(st_id: int) -> None:
    c = _conn()
    try:
        c.execute("UPDATE ollie_super_trades SET closed=1 WHERE id=?", (st_id,))
        c.commit()
    finally:
        c.close()


def _ollie_record_performance(st_row: dict, exit_price: float, exit_reason: str,
                               entry_ts: str | None = None) -> None:
    """INSERT a row into ollie_performance for backtesting. Never deletes."""
    _init_ollie_performance_table()
    entry = float(st_row.get("entry_price") or 0)
    if not entry:
        return
    pnl_pct = round((exit_price - entry) / entry * 100, 3) if entry else 0.0
    pnl     = round((exit_price - entry) * float(st_row.get("initial_qty") or 0), 2)
    # Calculate hold time
    hold_hours = None
    try:
        from datetime import datetime as _dt
        created = _dt.fromisoformat(st_row.get("created_at", ""))
        hold_hours = round(((_dt.utcnow() - created).total_seconds()) / 3600, 2)
    except Exception:
        pass
    c = _conn()
    try:
        c.execute("""
            INSERT INTO ollie_performance
              (symbol, player_id, grade_at_entry, score_at_entry, probability_at_entry,
               regime_at_entry, signal_source, entry_price, exit_price, pnl, pnl_pct,
               exit_reason, hold_hours, tp1_hit, tp2_hit, tp3_hit)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            st_row["symbol"], st_row["player_id"],
            st_row.get("signal_grade"), st_row.get("signal_score"),
            st_row.get("success_prob"), st_row.get("regime"), st_row.get("signal_source"),
            entry, exit_price, pnl, pnl_pct, exit_reason, hold_hours,
            int(st_row.get("tp1_hit", 0)), int(st_row.get("tp2_hit", 0)),
            int(st_row.get("tp3_hit", 0)),
        ))
        c.commit()
    finally:
        c.close()


# ---------------------------------------------------------------------------
# Options strategy selector (used by T'Pol / dayblade-0dte)
# ---------------------------------------------------------------------------

def select_options_strategy(symbol: str, regime: str, vix: float,
                             gex: dict | None = None) -> dict:
    """Return the recommended options strategy for current conditions.

    Returns dict with: strategy, rationale, dte_min, dte_max, delta_min, delta_max
    """
    regime = (regime or "UNKNOWN").upper()
    vix    = float(vix or 0)

    if vix < 18 and ("BULL" in regime or "TRENDING" in regime):
        return {
            "strategy":   "BUY_CALL",
            "rationale":  f"Low VIX ({vix:.0f}) + {regime}: buy calls for defined-risk upside",
            "dte_min":    30, "dte_max": 45,
            "delta_min":  0.40, "delta_max": 0.60,
            "direction":  "long",
        }
    if 18 <= vix <= 25 and "CAUTIOUS" in regime:
        return {
            "strategy":   "IRON_CONDOR" if vix >= 22 else "CREDIT_SPREAD",
            "rationale":  f"Elevated VIX ({vix:.0f}) + CAUTIOUS: sell premium, not direction",
            "dte_min":    21, "dte_max": 35,
            "delta_min":  0.20, "delta_max": 0.35,
            "direction":  "neutral",
        }
    if 25 <= vix <= 35 and ("BEAR" in regime or "TRENDING_BEAR" in regime):
        return {
            "strategy":   "BUY_PUT",
            "rationale":  f"High VIX ({vix:.0f}) + {regime}: buy puts for downside exposure",
            "dte_min":    30, "dte_max": 45,
            "delta_min":  0.40, "delta_max": 0.60,
            "direction":  "short",
        }
    if vix > 35 or "CRISIS" in regime:
        return {
            "strategy":   "PROTECTIVE_PUT",
            "rationale":  f"Crisis VIX ({vix:.0f}) + {regime}: protect existing positions only",
            "dte_min":    14, "dte_max": 30,
            "delta_min":  0.30, "delta_max": 0.50,
            "direction":  "hedge",
        }
    # Default: same as low-VIX bull
    return {
        "strategy":   "BUY_CALL",
        "rationale":  f"VIX {vix:.0f} + {regime}: default to calls",
        "dte_min":    30, "dte_max": 45,
        "delta_min":  0.40, "delta_max": 0.60,
        "direction":  "long",
    }


# ---------------------------------------------------------------------------
# Ollie Auto — main entry + tiered TP monitor
# ---------------------------------------------------------------------------

def ollie_auto_check(ctx: dict | None = None) -> list[dict]:
    """Ollie Super Trader pipeline.

    Entry triggers (EITHER qualifies):
      A. Signal Center Grade A (score≥75) or B (score≥60) AND price in entry zone
      B. Fleet consensus 3+ agents AND price within 1% of avg entry

    Regime gates:
      TRENDING_BULL / BULL : stocks + calls, full size
      CAUTIOUS             : stocks only, 25% size (size_factor 0.25)
      BEAR / TRENDING_BEAR : no stock longs (puts/inverse ETFs — future phase)
      CRISIS               : no new trades

    Levels source: Signal Center /api/trade-levels/<symbol> (ATR-based)
    Fallback:      2% ATR proxy

    Exit stored in reasoning: [STOP: $X] [TP1: $X] [TP2: $X] [TP3: $X]
    Tiered: TP1=50% @ 1:1R, TP2=25% @ 2:1R, TP3=25% @ wall/3:1R
    Trail stop: 3% below highest price after TP1 hit.
    """
    _init_ollie_auto_player()
    _init_ollie_super_trades_table()
    _init_ollie_performance_table()
    executed: list[dict] = []

    # ── Regime gate ─────────────────────────────────────────────────────────
    regime = _get_regime_from_8080()
    if "CRISIS" in regime:
        logger.info("[OllieAuto] CRISIS regime — standing down")
        return []
    if "BEAR" in regime:
        logger.info("[OllieAuto] BEAR regime — no stock longs (puts/inverse future phase)")
        return []

    cautious     = "CAUTIOUS" in regime
    size_factor  = 0.25 if cautious else 1.0

    try:
        import yfinance as yf
        from engine.paper_trader import buy as pt_buy, get_portfolio as pt_portfolio

        # ── Signal Center grade A/B picks ─────────────────────────────────
        sc_picks = _fetch_sc_top_picks(limit=10)
        sc_map   = {p["symbol"]: p for p in sc_picks}

        # ── Fleet consensus picks (3+ agents) ─────────────────────────────
        c = _conn()
        fleet_rows = c.execute("""
            SELECT symbol,
                   COUNT(DISTINCT player_id) AS fleet_count,
                   AVG(confidence)           AS avg_conf,
                   AVG(entry_price)          AS avg_entry,
                   GROUP_CONCAT(DISTINCT player_id) AS agents
            FROM watchlist_signals
            WHERE status = 'active'
            GROUP BY symbol
            HAVING fleet_count >= 3
            ORDER BY fleet_count DESC, avg_conf DESC
            LIMIT 10
        """).fetchall()
        fleet_map = {r[0]: dict(zip(
            ["symbol","fleet_count","avg_conf","avg_entry","agents"], r
        )) for r in fleet_rows}

        today_str   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        open_syms   = set(r[0] for r in c.execute(
            "SELECT DISTINCT symbol FROM trades WHERE player_id=? "
            "AND action='BUY' AND realized_pnl IS NULL", (_OLLIE_AUTO_ID,)
        ).fetchall())
        traded_today = set(r[0] for r in c.execute(
            "SELECT DISTINCT symbol FROM trades WHERE player_id=? AND date(executed_at)=?",
            (_OLLIE_AUTO_ID, today_str)
        ).fetchall())
        c.close()

        skip_syms = open_syms | traded_today
        candidates = set(sc_map.keys()) | set(fleet_map.keys())

        # ── Bulk pre-fetch trade levels for all candidates ──────────────────
        tradeable = [s for s in candidates if s not in skip_syms]
        bulk_levels = _fetch_trade_levels_bulk(tradeable)
        logger.info(
            "[OllieAuto] regime=%s cautious=%s | SC picks=%d fleet picks=%d | skip=%d | levels=%d/%d",
            regime, cautious, len(sc_map), len(fleet_map), len(skip_syms),
            len(bulk_levels), len(tradeable)
        )

        for symbol in candidates:
            if symbol in skip_syms:
                continue

            sc  = sc_map.get(symbol)
            flt = fleet_map.get(symbol)

            # Determine best signal quality
            if sc and sc["success_prob"] >= 0.60:
                src        = "signal_center"
                grade      = sc["grade"]
                sc_score   = sc["scaled_score"]
                s_prob     = sc["success_prob"]
            elif flt and int(flt.get("fleet_count") or 0) >= 3:
                src        = "fleet_consensus"
                grade      = "B"
                sc_score   = 60.0
                s_prob     = 0.60
            else:
                continue

            # ── Trade levels from bulk pre-fetch (fallback to individual) ───
            levels   = bulk_levels.get(symbol) or _fetch_trade_levels_9000(symbol)
            long_lvl = (levels or {}).get("long", {})

            # ── Live price ─────────────────────────────────────────────────
            try:
                fi    = yf.Ticker(symbol).fast_info
                price = float(fi.get("lastPrice") or fi.get("last_price") or 0)
            except Exception:
                continue
            if price <= 0:
                continue

            # ── Entry zone check ───────────────────────────────────────────
            in_zone = False
            if long_lvl.get("entry_lo") and long_lvl.get("entry_hi"):
                e_lo    = float(long_lvl["entry_lo"])
                e_hi    = float(long_lvl["entry_hi"]) * 1.005
                in_zone = e_lo <= price <= e_hi
            if not in_zone and flt:
                avg_e   = float(flt.get("avg_entry") or 0)
                in_zone = bool(avg_e > 0 and abs(price - avg_e) / avg_e <= 0.01)
            if not in_zone and sc and levels:
                # Price near Signal Center's snapshot price (within 0.5%)
                lv_price = float(levels.get("price") or 0)
                in_zone  = bool(lv_price > 0 and abs(price - lv_price) / lv_price <= 0.005)
            if not in_zone:
                logger.info("[OllieAuto] %s skipped — price $%.2f outside entry zone", symbol, price)
                continue

            # ── Stop / TP levels ───────────────────────────────────────────
            if long_lvl.get("stop_loss") and long_lvl.get("tp1"):
                stop = round(float(long_lvl["stop_loss"]), 2)
                tp1  = round(float(long_lvl["tp1"]), 2)
                tp2  = round(float(long_lvl.get("tp2") or price + (price - stop) * 2), 2)
                tp3  = round(float(long_lvl.get("tp3") or price + (price - stop) * 3), 2)
            else:
                risk = price * 0.025
                stop = round(price - risk * 2.5, 2)
                tp1  = round(price + risk * 0.75, 2)
                tp2  = round(price + risk * 5.0, 2)
                tp3  = round(price + risk * 7.5, 2)

            # ── Explicit qty for regime sizing ─────────────────────────────
            port = pt_portfolio(_OLLIE_AUTO_ID)
            cash = float((port or {}).get("cash") or 0)
            base_alloc = 0.05            # 5% of cash base
            qty = round(cash * base_alloc * size_factor / price, 4)
            if qty <= 0:
                continue

            reasoning = (
                f"Ollie Super Trader | {src.upper()} grade={grade} "
                f"score={sc_score:.0f} prob={s_prob:.0%} | "
                f"regime={regime} size_factor={size_factor} | "
                f"[STOP: ${stop:.2f}] [TARGET: ${tp1:.2f}] [TP1: ${tp1:.2f}] [TP2: ${tp2:.2f}] [TP3: ${tp3:.2f}]"
            )
            if flt:
                reasoning += f" | fleet={flt.get('fleet_count')} agents"

            try:
                result = pt_buy(
                    player_id  = _OLLIE_AUTO_ID,
                    symbol     = symbol,
                    qty        = qty,
                    price      = price,
                    asset_type = "stock",
                    reasoning  = reasoning,
                    confidence = s_prob,
                    timeframe  = "SWING",
                )
                if not result:
                    continue

                trade_id = result.get("id")
                actual_qty = float(result.get("qty") or qty)

                # Log to ollie_super_trades
                c2 = _conn()
                try:
                    c2.execute("""
                        INSERT INTO ollie_super_trades
                          (buy_trade_id, symbol, player_id, entry_price,
                           stop, tp1, tp2, tp3, trail_stop, initial_qty,
                           regime, signal_source, signal_grade, signal_score, success_prob)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (trade_id, symbol, _OLLIE_AUTO_ID, price,
                          stop, tp1, tp2, tp3, stop, actual_qty,
                          regime, src, grade, sc_score, s_prob))
                    c2.commit()
                finally:
                    c2.close()

                executed.append({
                    "symbol": symbol, "price": price, "qty": actual_qty,
                    "grade": grade, "prob": s_prob, "source": src,
                    "stop": stop, "tp1": tp1, "tp2": tp2, "tp3": tp3,
                    "regime": regime, "trade_id": trade_id,
                })
                logger.info(
                    "[OllieAuto] BUY %s %.4f @ $%.2f | %s grade=%s prob=%.0f%% "
                    "stop=%.2f TP1=%.2f TP2=%.2f TP3=%.2f regime=%s",
                    symbol, actual_qty, price, src, grade, s_prob * 100,
                    stop, tp1, tp2, tp3, regime,
                )
                try:
                    from engine.ntfy import notify_ollie_buy
                    notify_ollie_buy(symbol, price, actual_qty, grade, s_prob,
                                     stop, tp1, tp2, tp3, regime, src)
                except Exception:
                    pass
            except Exception as e:
                logger.warning(f"[OllieAuto] buy error for {symbol}: {e}")

    except Exception as e:
        logger.error(f"[OllieAuto] ollie_auto_check error: {e}", exc_info=True)

    # ── Small-cap momentum scanner (CRITICAL volume + gap 20%+, $1-$10) ────
    try:
        sc_trades = _ollie_small_cap_scan()
        executed.extend(sc_trades)
    except Exception as e:
        logger.warning("[OllieAuto] small_cap_scan error: %s", e)

    # ── Channel Scanner signals (gap_and_go, momentum_breakout, reversal_bounce, volatility_breakout) ──
    try:
        ch_trades = _ollie_channel_scan()
        executed.extend(ch_trades)
    except Exception as e:
        logger.warning("[OllieAuto] channel_scan error: %s", e)

    return executed


def _ollie_small_cap_scan() -> list[dict]:
    """Ollie Small-Cap Momentum Scanner — trades CRITICAL volume alerts.

    Entry criteria (ALL must pass):
      - alert_type = 'red_alert' (CRITICAL 100x+ volume) from volume_scanner
      - price $1.00 – $10.00
      - gap_pct >= +20%
      - not already open or traded today

    Sizing:
      - max $200 notional per position (hard cap)
      - qty = min(floor($200 / price), max_shares)

    Exits (managed by _ollie_check_tiered_tp):
      - Stop: -5% from entry
      - TP1:  +10% (sell 50%)
      - TP2:  +15% (sell 25%)
      - TP3:  +20% (close remaining 25%)
    """
    import yfinance as yf
    from engine.paper_trader import buy as pt_buy, get_portfolio as pt_portfolio

    executed: list[dict] = []

    # ── Regime gate (re-use same rules as main ollie pipeline) ──────────────
    regime = _get_regime_from_8080()
    if "CRISIS" in regime or "BEAR" in regime:
        logger.info("[OllieSmallCap] standing down — regime=%s", regime)
        return []
    size_factor = 0.50 if "CAUTIOUS" in regime else 1.0  # half-size in cautious

    MAX_POSITION_USD = 200.0
    SCALP_STOP_PCT   = 0.05   # 5%
    TP1_PCT          = 0.10   # 10%
    TP2_PCT          = 0.15   # 15%
    TP3_PCT          = 0.20   # 20%
    PRICE_MIN        = 1.00
    PRICE_MAX        = 10.00
    GAP_MIN_PCT      = 20.0
    VOL_MIN_X        = 100.0  # 100x = CRITICAL threshold

    try:
        from engine.volume_scanner import get_todays_volume_alerts

        alerts = get_todays_volume_alerts(limit=50)
        critical_alerts = [
            a for a in alerts
            if float(a.get("relative_volume") or 0) >= VOL_MIN_X
            and PRICE_MIN <= float(a.get("price") or 0) <= PRICE_MAX
            and float(a.get("gap_pct") or 0) >= GAP_MIN_PCT
        ]

        if not critical_alerts:
            return []

        today_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        c = _conn()
        open_syms = set(r[0] for r in c.execute(
            "SELECT DISTINCT symbol FROM trades WHERE player_id=? "
            "AND action='BUY' AND realized_pnl IS NULL", (_OLLIE_AUTO_ID,)
        ).fetchall())
        traded_today = set(r[0] for r in c.execute(
            "SELECT DISTINCT symbol FROM trades WHERE player_id=? AND date(executed_at)=?",
            (_OLLIE_AUTO_ID, today_str)
        ).fetchall())
        c.close()
        skip_syms = open_syms | traded_today

        logger.info(
            "[OllieSmallCap] regime=%s | CRITICAL alerts=%d skip=%d",
            regime, len(critical_alerts), len(skip_syms),
        )

        for alert in critical_alerts:
            symbol  = alert["symbol"]
            if symbol in skip_syms:
                continue

            # Live price confirmation
            try:
                fi    = yf.Ticker(symbol).fast_info
                price = float(fi.get("lastPrice") or fi.get("last_price") or 0)
            except Exception:
                continue
            if not (PRICE_MIN <= price <= PRICE_MAX):
                continue

            # Levels
            stop = round(price * (1 - SCALP_STOP_PCT), 4)
            tp1  = round(price * (1 + TP1_PCT), 4)
            tp2  = round(price * (1 + TP2_PCT), 4)
            tp3  = round(price * (1 + TP3_PCT), 4)

            # Sizing: max $200, respect cautious half-size
            max_usd = MAX_POSITION_USD * size_factor
            qty     = round(max_usd / price, 4)
            if qty <= 0:
                continue

            rel_vol = float(alert.get("relative_volume") or 0)
            gap_pct = float(alert.get("gap_pct") or 0)
            reasoning = (
                f"Ollie SmallCap Scalp | CRITICAL_VOLUME {rel_vol:.0f}x | "
                f"gap=+{gap_pct:.1f}% price=${price:.2f} | "
                f"regime={regime} max_pos=${max_usd:.0f} | "
                f"[STOP: ${stop:.4f}] [TARGET: ${tp1:.4f}] "
                f"[TP1: ${tp1:.4f}] [TP2: ${tp2:.4f}] [TP3: ${tp3:.4f}]"
            )

            try:
                result = pt_buy(
                    player_id  = _OLLIE_AUTO_ID,
                    symbol     = symbol,
                    qty        = qty,
                    price      = price,
                    asset_type = "stock",
                    reasoning  = reasoning,
                    confidence = 0.55,
                    timeframe  = "SCALP",
                )
                if not result:
                    continue

                trade_id   = result.get("id")
                actual_qty = float(result.get("qty") or qty)

                c2 = _conn()
                try:
                    c2.execute("""
                        INSERT INTO ollie_super_trades
                          (buy_trade_id, symbol, player_id, entry_price,
                           stop, tp1, tp2, tp3, trail_stop, initial_qty,
                           regime, signal_source, signal_grade, signal_score, success_prob)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (trade_id, symbol, _OLLIE_AUTO_ID, price,
                          stop, tp1, tp2, tp3, stop, actual_qty,
                          regime, "smallcap_momentum", "C", 50.0, 0.55))
                    c2.commit()
                finally:
                    c2.close()

                skip_syms.add(symbol)
                executed.append({
                    "symbol": symbol, "price": price, "qty": actual_qty,
                    "rel_vol": rel_vol, "gap_pct": gap_pct,
                    "stop": stop, "tp1": tp1, "tp2": tp2, "tp3": tp3,
                    "source": "smallcap_momentum", "trade_id": trade_id,
                })
                logger.info(
                    "[OllieSmallCap] BUY %s %.4f @ $%.2f | %.0fx volume gap=+%.1f%% "
                    "stop=%.4f TP1=%.4f regime=%s",
                    symbol, actual_qty, price, rel_vol, gap_pct, stop, tp1, regime,
                )
                try:
                    from engine.ntfy import notify_ollie_buy
                    notify_ollie_buy(symbol, price, actual_qty, "C", 0.55,
                                     stop, tp1, tp2, tp3, regime, "smallcap_momentum")
                except Exception:
                    pass
            except Exception as e:
                logger.warning("[OllieSmallCap] buy error for %s: %e", symbol, e)

    except Exception as e:
        logger.error("[OllieSmallCap] scan error: %s", e, exc_info=True)

    return executed


def _ollie_channel_scan() -> list[dict]:
    """Ollie Channel Scanner — trades gap_and_go, momentum_breakout, reversal_bounce,
    volatility_breakout signals from channel_scanner.py during market hours.

    Same conviction and sizing logic as the main ollie pipeline:
      - 5% base cash allocation, scaled by regime size_factor
      - Stop: 2.5% below entry, TP1/TP2/TP3 tiers
      - Max 3 new channel trades per run
    """
    import yfinance as yf
    from engine.paper_trader import buy as pt_buy, get_portfolio as pt_portfolio

    executed: list[dict] = []

    regime = _get_regime_from_8080()
    if "CRISIS" in regime or "BEAR" in regime:
        logger.info("[OllieChannel] standing down — regime=%s", regime)
        return []
    size_factor = 0.50 if "CAUTIOUS" in regime else 1.0

    CHANNEL_CONF   = 0.62   # same as Grade-B threshold in main pipeline
    MAX_TRADES     = 3
    BASE_ALLOC     = 0.05   # 5% of cash

    # Channel strategies to run: (name, scanner_fn, conf_boost)
    CHANNEL_MAP = {
        "gap_and_go":        ("gap-and-go",        0.68),
        "momentum_breakout": ("momentum-breakout",  0.70),
        "reversal_bounce":   ("reversal-bounce",    0.62),
        "volatility_breakout": ("volatility-breakout", 0.65),
    }

    try:
        from engine.channel_scanner import scan_channel

        today_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        c = _conn()
        open_syms = set(r[0] for r in c.execute(
            "SELECT DISTINCT symbol FROM trades WHERE player_id=? "
            "AND action='BUY' AND realized_pnl IS NULL", (_OLLIE_AUTO_ID,)
        ).fetchall())
        traded_today = set(r[0] for r in c.execute(
            "SELECT DISTINCT symbol FROM trades WHERE player_id=? AND date(executed_at)=?",
            (_OLLIE_AUTO_ID, today_str)
        ).fetchall())
        c.close()
        skip_syms = open_syms | traded_today

        seen_symbols: set[str] = set()
        trades_placed = 0

        for strat_name, (channel_key, conf) in CHANNEL_MAP.items():
            if trades_placed >= MAX_TRADES:
                break
            try:
                signals = scan_channel(channel_key)
            except Exception as e:
                logger.debug("[OllieChannel] %s scan error: %s", channel_key, e)
                continue

            for sig in signals[:5]:
                if trades_placed >= MAX_TRADES:
                    break
                symbol = sig.get("symbol", "")
                if not symbol or symbol in skip_syms or symbol in seen_symbols:
                    continue

                # Get live price
                try:
                    fi    = yf.Ticker(symbol).fast_info
                    price = float(fi.get("lastPrice") or fi.get("last_price") or 0)
                except Exception:
                    price = float(sig.get("price") or 0)
                if price <= 0:
                    continue

                # Build levels: ATR-based using rel_volume as proxy signal quality
                rel_vol = float(sig.get("rel_volume") or 1.0)
                risk_pct = max(0.015, min(0.04, 0.025 / max(rel_vol, 0.5)))
                risk    = price * risk_pct
                stop    = round(price - risk * 2.5, 2)
                tp1     = round(price + risk * 0.75, 2)
                tp2     = round(price + risk * 2.0, 2)
                tp3     = round(price + risk * 3.0, 2)

                port     = pt_portfolio(_OLLIE_AUTO_ID)
                cash_bal = float((port or {}).get("cash") or 0)
                qty      = round(cash_bal * BASE_ALLOC * size_factor / price, 4)
                if qty <= 0:
                    continue

                reasoning = (
                    f"Ollie Channel | {strat_name.upper()} | "
                    f"regime={regime} size_factor={size_factor} conf={conf:.0%} | "
                    f"rel_vol={rel_vol:.1f}x | "
                    f"[STOP: ${stop:.2f}] [TP1: ${tp1:.2f}] [TP2: ${tp2:.2f}] [TP3: ${tp3:.2f}]"
                )

                try:
                    result = pt_buy(
                        player_id  = _OLLIE_AUTO_ID,
                        symbol     = symbol,
                        qty        = qty,
                        price      = price,
                        asset_type = "stock",
                        reasoning  = reasoning,
                        confidence = conf,
                        timeframe  = "SCALP",
                    )
                    if not result:
                        continue

                    trade_id   = result.get("id")
                    actual_qty = float(result.get("qty") or qty)
                    seen_symbols.add(symbol)
                    trades_placed += 1

                    # Log to ollie_super_trades
                    c2 = _conn()
                    try:
                        c2.execute("""
                            INSERT INTO ollie_super_trades
                              (buy_trade_id, symbol, player_id, entry_price,
                               stop, tp1, tp2, tp3, trail_stop, initial_qty,
                               regime, signal_source, signal_grade, signal_score, success_prob)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (trade_id, symbol, _OLLIE_AUTO_ID, price,
                              stop, tp1, tp2, tp3, stop, actual_qty,
                              regime, f"channel_{strat_name}", "B", 62.0, conf))
                        c2.commit()
                    finally:
                        c2.close()

                    executed.append({
                        "symbol": symbol, "price": price, "qty": actual_qty,
                        "grade": "B", "prob": conf, "source": f"channel_{strat_name}",
                        "stop": stop, "tp1": tp1, "tp2": tp2, "tp3": tp3,
                        "regime": regime, "trade_id": trade_id,
                    })
                    logger.info(
                        "[OllieChannel] BUY %s %.4f @ $%.2f | %s conf=%.0f%% stop=%.2f TP1=%.2f",
                        symbol, actual_qty, price, strat_name, conf * 100, stop, tp1,
                    )
                    try:
                        from engine.ntfy import notify_ollie_buy
                        notify_ollie_buy(symbol, price, actual_qty, "B", conf,
                                         stop, tp1, tp2, tp3, regime, f"channel_{strat_name}")
                    except Exception:
                        pass
                except Exception as e:
                    logger.warning("[OllieChannel] buy error for %s: %s", symbol, e)

    except Exception as e:
        logger.error("[OllieChannel] error: %s", e)

    return executed


def _ollie_check_tiered_tp() -> list[dict]:
    """Monitor open Ollie Super Trades and execute tiered partial sells.

    TP1 (1:1R): sell 50% of initial_qty, move trail_stop to entry
    TP2 (2:1R): sell 25% of initial_qty
    TP3 (wall/3:1R): close remaining 25%
    Trail stop: 3% below highest price after TP1 hit (updated each cycle)
    Hard stop: close all if price ≤ stop and TP1 not yet hit
    """
    import yfinance as yf
    from engine.paper_trader import sell_partial, get_position

    _init_ollie_super_trades_table()
    actions: list[dict] = []

    c = _conn()
    try:
        open_trades = c.execute("""
            SELECT id, symbol, player_id, entry_price, stop, tp1, tp2, tp3,
                   trail_stop, initial_qty, tp1_hit, tp2_hit, tp3_hit,
                   signal_grade, signal_source, success_prob,
                   regime, signal_score, created_at
            FROM ollie_super_trades
            WHERE player_id = ? AND closed = 0
        """, (_OLLIE_AUTO_ID,)).fetchall()
    finally:
        c.close()

    for trade in open_trades:
        st       = dict(trade)
        symbol   = st["symbol"]
        entry    = float(st["entry_price"] or 0)
        stop     = float(st["stop"] or 0)
        tp1      = float(st["tp1"] or 0)
        tp2      = float(st["tp2"] or 0)
        tp3      = float(st["tp3"] or 0)
        ts       = float(st["trail_stop"] or stop)
        init_qty = float(st["initial_qty"] or 0)
        tp1_hit  = bool(st["tp1_hit"])
        tp2_hit  = bool(st["tp2_hit"])
        tp3_hit  = bool(st["tp3_hit"])
        st_id    = st["id"]
        grade    = st["signal_grade"] or "?"

        pos = get_position(_OLLIE_AUTO_ID, symbol)
        if not pos or float(pos.get("qty") or 0) <= 0:
            _ollie_mark_closed(st_id)
            _ollie_record_performance(st, entry, "external_close")
            continue

        try:
            fi    = yf.Ticker(symbol).fast_info
            price = float(fi.get("lastPrice") or fi.get("last_price") or 0)
        except Exception:
            continue
        if price <= 0:
            continue

        cur_qty = float(pos["qty"])

        # ── Update trailing stop after TP1: 3% below current high ─────────
        if tp1_hit:
            new_ts = round(price * 0.97, 2)
            if new_ts > ts:
                ts = new_ts
                _ollie_update_tp(st_id, trail_stop=new_ts)

        # ── Trail stop hit ─────────────────────────────────────────────────
        if tp1_hit and ts > 0 and price <= ts:
            reason = f"Ollie Super: TRAIL-STOP ${price:.2f}≤${ts:.2f} grade={grade} tp"
            res = sell_partial(_OLLIE_AUTO_ID, symbol, price, cur_qty, reasoning=reason)
            if res:
                _ollie_mark_closed(st_id)
                _ollie_record_performance(st, price, f"trail_stop@{ts:.2f}")
                actions.append({"action": "trail_stop", "symbol": symbol, "price": price})
                logger.info("[OllieAuto] TRAIL-STOP %s @ $%.2f", symbol, price)
                try:
                    from engine.ntfy import notify_ollie_tp
                    notify_ollie_tp("trail_stop", symbol, price)
                except Exception:
                    pass
            continue

        # ── Hard stop (before TP1) ─────────────────────────────────────────
        if not tp1_hit and stop > 0 and price <= stop:
            reason = f"Ollie Super: STOP-LOSS ${price:.2f}≤${stop:.2f} grade={grade} tp"
            res = sell_partial(_OLLIE_AUTO_ID, symbol, price, cur_qty, reasoning=reason)
            if res:
                _ollie_mark_closed(st_id)
                _ollie_record_performance(st, price, f"stop_loss@{stop:.2f}")
                actions.append({"action": "stop_loss", "symbol": symbol, "price": price})
                logger.info("[OllieAuto] STOP-LOSS %s @ $%.2f", symbol, price)
                try:
                    from engine.ntfy import notify_ollie_tp
                    notify_ollie_tp("stop_loss", symbol, price)
                except Exception:
                    pass
            continue

        # ── Time stop: exit full position if TP1 not hit by 11 AM ET ─────
        # Guard: trade must be at least 24 hours old — swing trades need room to run.
        if not tp1_hit:
            try:
                from zoneinfo import ZoneInfo
                import datetime as _dt
                _et_now = _dt.datetime.now(ZoneInfo("America/New_York"))
                _cutoff = _et_now.replace(hour=11, minute=0, second=0, microsecond=0)
                _created_raw = st.get("created_at") or ""
                _created_utc = _dt.datetime.fromisoformat(
                    _created_raw.replace("Z", "+00:00")
                ).replace(tzinfo=_dt.timezone.utc) if _created_raw else None
                _age_hours = (
                    (_dt.datetime.now(_dt.timezone.utc) - _created_utc).total_seconds() / 3600
                    if _created_utc else 999
                )
                if _age_hours < 24:
                    pass  # too new — let the trade run its full swing
                elif _et_now >= _cutoff:
                    reason = (
                        f"Ollie Super: TIME-STOP {_et_now.strftime('%H:%M ET')} "
                        f"TP1 not hit grade={grade} entry=${entry:.2f}"
                    )
                    res = sell_partial(_OLLIE_AUTO_ID, symbol, price, cur_qty, reasoning=reason)
                    if res:
                        _ollie_mark_closed(st_id)
                        _ollie_record_performance(st, price, "TIME_STOP")
                        actions.append({"action": "time_stop", "symbol": symbol, "price": price})
                        logger.info(
                            "[OllieAuto] TIME-STOP %s @ $%.2f (11AM ET cutoff, TP1 not hit)",
                            symbol, price,
                        )
                        try:
                            from engine.ntfy import notify_ollie_tp
                            notify_ollie_tp("time_stop", symbol, price, qty=cur_qty)
                        except Exception:
                            pass
                    continue
            except Exception:
                pass  # zoneinfo unavailable — skip time stop silently

        # ── TP3: close remaining 25% ───────────────────────────────────────
        if tp2_hit and not tp3_hit and tp3 > 0 and price >= tp3:
            reason = f"Ollie Super: TP3 ${price:.2f}≥${tp3:.2f} grade={grade} tp3 take-profit"
            res = sell_partial(_OLLIE_AUTO_ID, symbol, price, cur_qty, reasoning=reason)
            if res:
                st["tp3_hit"] = 1
                _ollie_update_tp(st_id, tp3_hit=True, closed=True)
                _ollie_record_performance(st, price, f"tp3@{tp3:.2f}")
                actions.append({"action": "tp3", "symbol": symbol, "price": price})
                logger.info("[OllieAuto] TP3 %s @ $%.2f qty=%.4f", symbol, price, cur_qty)
                try:
                    from engine.ntfy import notify_ollie_tp
                    notify_ollie_tp("tp3", symbol, price, qty=cur_qty)
                except Exception:
                    pass
            continue

        # ── TP2: sell 25% of initial qty ──────────────────────────────────
        if tp1_hit and not tp2_hit and tp2 > 0 and price >= tp2:
            q25 = min(round(init_qty * 0.25, 4), cur_qty)
            if q25 <= 0:
                continue
            reason = f"Ollie Super: TP2 ${price:.2f}≥${tp2:.2f} grade={grade} tp2 take-profit"
            res = sell_partial(_OLLIE_AUTO_ID, symbol, price, q25, reasoning=reason)
            if res:
                _ollie_update_tp(st_id, tp2_hit=True)
                actions.append({"action": "tp2", "symbol": symbol, "price": price, "qty": q25})
                logger.info("[OllieAuto] TP2 %s @ $%.2f qty=%.4f", symbol, price, q25)
                try:
                    from engine.ntfy import notify_ollie_tp
                    notify_ollie_tp("tp2", symbol, price, qty=q25)
                except Exception:
                    pass
            continue

        # ── TP1: sell 50% of initial qty, trail stop → entry ──────────────
        if not tp1_hit and tp1 > 0 and price >= tp1:
            q50 = min(round(init_qty * 0.50, 4), cur_qty)
            if q50 <= 0:
                continue
            reason = f"Ollie Super: TP1 ${price:.2f}≥${tp1:.2f} grade={grade} tp1 take-profit"
            res = sell_partial(_OLLIE_AUTO_ID, symbol, price, q50, reasoning=reason)
            if res:
                _ollie_update_tp(st_id, tp1_hit=True, trail_stop=entry)
                actions.append({"action": "tp1", "symbol": symbol, "price": price, "qty": q50})
                logger.info(
                    "[OllieAuto] TP1 %s @ $%.2f qty=%.4f | trail→entry $%.2f",
                    symbol, price, q50, entry,
                )
                try:
                    from engine.ntfy import notify_ollie_tp
                    notify_ollie_tp("tp1", symbol, price, qty=q50)
                except Exception:
                    pass

    return actions


# ---------------------------------------------------------------------------
# CLI test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== Crew Scanner — Manual Test Cycle ===")
    summary = run_scan_cycle(tier_filter=None, verbose=True)
    print(f"\nSummary: {summary}")
    print(f"\nRecent decisions:")
    for d in get_crew_decisions(limit=20):
        ts   = d["timestamp"][:19]
        name = d["agent_name"]
        act  = d["action"]
        sym  = d.get("symbol") or "—"
        conf = d.get("confidence") or 0
        ok   = "✅ EXEC" if d.get("executed") else "   pass"
        print(f"  {ts}  {ok}  {name}: {act} {sym} ({conf}%)")
