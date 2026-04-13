"""Kirk Grok Advisor — uses xAI Grok to analyze Kirk's Webull positions
for long swing trade recommendations.

Runs twice per market day (9:30 AM and 1:30 PM ET).
Stores results in portfolio_advice table (8-hour TTL per scan).
Daily cost cap: $0.50 (configurable via GROK_ADVISOR_DAILY_CAP env var).
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta

import pytz
import requests

logger = logging.getLogger("kirk_grok_advisor")

DB = "data/trader.db"
PLAYER_ID = "kirk-grok-advisor"

# Model: override with GROK_MODEL env var if xAI releases a newer version
MODEL = os.getenv("GROK_MODEL", "grok-4-0709")

# Cost cap (USD/day). Override with GROK_ADVISOR_DAILY_CAP env var.
DAILY_COST_CAP = float(os.getenv("GROK_ADVISOR_DAILY_CAP", "0.50"))

# xAI API pricing (per 1M tokens) — grok-3 / grok-4 rates as of 2025
INPUT_RATE_PER_M = 3.00
OUTPUT_RATE_PER_M = 15.00

XAI_BASE_URL = "https://api.x.ai/v1"

# Ollama fallback
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("CREWAI_MODEL", "qwen3:14b")

SYSTEM_PROMPT = (
    "You are a swing trade advisor for a small retail portfolio (~$6,500). "
    "Analyze each position for:\n"
    "- Trend direction (bullish/bearish/neutral)\n"
    "- Key support/resistance levels\n"
    "- Optimal hold period (days/weeks)\n"
    "- Risk level (stop loss recommendation)\n"
    "- Action: HOLD / TRIM / ADD / SELL\n\n"
    "Keep reasoning concise (1-2 sentences). "
    "This is a long-term swing portfolio, not day trading.\n\n"
    "Respond with a JSON array only. Each element must have exactly these keys:\n"
    '{"symbol":"AAPL","action":"HOLD","confidence":0.75,'
    '"reasoning":"...","support_level":150.00,"resistance_level":175.00,'
    '"stop_loss":145.00,"target_price":185.00,"hold_period":"2-3 weeks"}\n\n'
    "Return ONLY valid JSON — no markdown, no preamble, no trailing text."
)


# ── DB helpers ─────────────────────────────────────────────────────────────


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def _init_db():
    """Create portfolio_advice table; migrate existing table to add new columns."""
    conn = _conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS portfolio_advice (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol           TEXT NOT NULL,
            advisor          TEXT NOT NULL DEFAULT 'grok',
            action           TEXT,
            confidence       REAL,
            reasoning        TEXT,
            support_level    REAL,
            resistance_level REAL,
            stop_loss        REAL,
            target_price     REAL,
            hold_period      TEXT,
            model_used       TEXT,
            response_time_ms INTEGER,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at       TIMESTAMP,
            acknowledged     INTEGER DEFAULT 0,
            acknowledged_at  TIMESTAMP,
            raw_response     TEXT
        )
    """)
    # Safe migrations for tables that already existed without these columns
    for col, typedef in [("model_used", "TEXT"), ("response_time_ms", "INTEGER")]:
        try:
            conn.execute(f"ALTER TABLE portfolio_advice ADD COLUMN {col} {typedef}")
        except Exception:
            pass  # Column already exists
    conn.commit()
    conn.close()


# ── Cost helpers ────────────────────────────────────────────────────────────


def get_daily_cost() -> float:
    """Return total Grok advisor spend today (UTC date)."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    try:
        conn = _conn()
        row = conn.execute(
            "SELECT SUM(cost_usd) AS total FROM api_costs "
            "WHERE player_id=? AND date(timestamp)=?",
            (PLAYER_ID, today),
        ).fetchone()
        conn.close()
        return float(row["total"] or 0) if row else 0.0
    except Exception:
        return 0.0


def _log_cost(input_tok: int, output_tok: int) -> float:
    """Write api_costs + model_stats rows. Returns cost_usd."""
    cost_usd = (
        (input_tok / 1_000_000) * INPUT_RATE_PER_M
        + (output_tok / 1_000_000) * OUTPUT_RATE_PER_M
    )
    today = datetime.utcnow().strftime("%Y-%m-%d")
    now_iso = datetime.utcnow().isoformat()
    try:
        conn = _conn()
        conn.execute(
            "INSERT INTO api_costs "
            "(player_id, call_type, input_tokens, output_tokens, cost_usd, timestamp) "
            "VALUES (?,?,?,?,?,?)",
            (PLAYER_ID, "swing_advisory", input_tok, output_tok, cost_usd, now_iso),
        )
        conn.execute(
            "INSERT INTO model_stats (player_id, api_calls, total_cost, date) "
            "VALUES (?,1,?,?) "
            "ON CONFLICT(player_id, date) DO UPDATE SET "
            "api_calls=api_calls+1, total_cost=total_cost+?",
            (PLAYER_ID, cost_usd, today, cost_usd),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Cost log failed: %s", e)
    return cost_usd


# ── Position fetch ──────────────────────────────────────────────────────────


def _get_positions() -> list[dict]:
    """Return Kirk's current Webull positions from DB."""
    try:
        conn = _conn()
        rows = conn.execute(
            "SELECT p.symbol, p.qty, p.avg_price, "
            "  p.avg_price AS current_price "
            "FROM positions p "
            "WHERE p.player_id='steve-webull' AND p.qty > 0 "
            "ORDER BY p.symbol",
        ).fetchall()
        conn.close()
        result = []
        for r in rows:
            row = dict(r)
            avg = row.get("avg_price") or 0
            cur = row.get("current_price") or avg
            row["pnl_pct"] = round((cur - avg) / avg * 100, 2) if avg else 0
            result.append(row)
        return result
    except Exception as e:
        logger.warning("Failed to fetch positions: %s", e)
        return []


# ── Grok API ────────────────────────────────────────────────────────────────


def _call_grok(prompt: str) -> tuple[str, int, int]:
    """POST to xAI /v1/chat/completions. Returns (text, input_tokens, output_tokens)."""
    api_key = os.getenv("XAI_API_KEY") or os.getenv("GROK_API_KEY", "")
    if not api_key:
        raise ValueError(
            "xAI API key not found. Set XAI_API_KEY or GROK_API_KEY in .env"
        )

    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.3,
        "max_tokens": 2048,
    }
    resp = requests.post(
        f"{XAI_BASE_URL}/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()

    data = resp.json()
    content = data["choices"][0]["message"]["content"]
    usage = data.get("usage", {})
    input_tok = usage.get("prompt_tokens") or (len(prompt) // 4)
    output_tok = usage.get("completion_tokens") or (len(content) // 4)
    return content, input_tok, output_tok


def _call_ollama(prompt: str) -> tuple[str, int, int]:
    """Call local Ollama as fallback. Returns (text, input_tokens, output_tokens)."""
    full_prompt = SYSTEM_PROMPT + "\n\n" + prompt
    resp = requests.post(
        f"{OLLAMA_BASE_URL}/api/generate",
        json={"model": OLLAMA_MODEL, "prompt": full_prompt, "stream": False},
        timeout=120,
    )
    resp.raise_for_status()
    content = resp.json().get("response", "")
    input_tok = len(full_prompt) // 4
    output_tok = len(content) // 4
    return content, input_tok, output_tok


# ── Parse / persist ─────────────────────────────────────────────────────────


def _parse_advice(raw: str) -> list[dict]:
    """Strip markdown fences and parse JSON array from Grok response."""
    text = raw.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        # Drop first line (``` or ```json) and last line (```)
        text = "\n".join(lines[1:-1]).strip()
    return json.loads(text)


def _save_advice(
    items: list[dict],
    raw_response: str,
    model_used: str = "",
    response_time_ms: int = 0,
):
    """Upsert advice rows; expire previous un-acknowledged advice per symbol."""
    now = datetime.utcnow()
    now_iso = now.isoformat()
    expires_iso = (now + timedelta(hours=8)).isoformat()
    conn = _conn()
    for item in items:
        symbol = (item.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        # Mark old un-acknowledged advice as expired
        conn.execute(
            "UPDATE portfolio_advice SET expires_at=? "
            "WHERE symbol=? AND advisor='grok' AND acknowledged=0 "
            "  AND (expires_at IS NULL OR expires_at > ?)",
            (now_iso, symbol, now_iso),
        )
        conn.execute(
            "INSERT INTO portfolio_advice "
            "(symbol, advisor, action, confidence, reasoning, "
            " support_level, resistance_level, stop_loss, target_price, "
            " hold_period, model_used, response_time_ms, created_at, expires_at, raw_response) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                symbol, "grok",
                item.get("action", "HOLD"),
                item.get("confidence"),
                item.get("reasoning", ""),
                item.get("support_level"),
                item.get("resistance_level"),
                item.get("stop_loss"),
                item.get("target_price"),
                item.get("hold_period", ""),
                model_used,
                response_time_ms,
                now_iso,
                expires_iso,
                raw_response,
            ),
        )
    conn.commit()
    conn.close()


# ── Public API ──────────────────────────────────────────────────────────────


def run_grok_advisory() -> dict:
    """Scan Kirk's positions and store Grok swing recommendations.

    Returns a summary dict (or {"skipped": True, "reason": ...} if gated).
    """
    _init_db()

    # Cost check (cap only blocks Grok; Ollama runs free as fallback)
    daily_cost = get_daily_cost()

    positions = _get_positions()
    if not positions:
        logger.info("Grok advisor: no Kirk positions — skipping")
        return {"skipped": True, "reason": "no_positions"}

    # Build user prompt
    lines = []
    for p in positions:
        lines.append(
            f"  {p['symbol']}: {p['qty']} shares, "
            f"entry ${p.get('avg_price', 0):.2f}, "
            f"current ${p.get('current_price', 0):.2f}, "
            f"P&L {p.get('pnl_pct', 0):+.1f}%"
        )
    prompt = (
        "Kirk's Webull swing portfolio (~$6,500 account):\n"
        + "\n".join(lines)
        + "\n\nProvide swing trade advice for each position."
    )

    # Try Grok first; fall back to Ollama if cap hit or API error
    use_ollama = daily_cost >= DAILY_COST_CAP * 0.9  # near-cap → pre-emptively switch
    cost = 0.0
    model_used = ""
    t0 = time.time()

    if not use_ollama:
        try:
            raw, input_tok, output_tok = _call_grok(prompt)
            cost = _log_cost(input_tok, output_tok)
            model_used = MODEL
            logger.info(
                "Grok advisory: %d positions, %d/%d tok, $%.6f (day total $%.4f)",
                len(positions), input_tok, output_tok, cost, daily_cost + cost,
            )
        except Exception as e:
            logger.warning("Grok API failed (%s) — falling back to Ollama", e)
            use_ollama = True

    if use_ollama:
        try:
            raw, input_tok, output_tok = _call_ollama(prompt)
            model_used = OLLAMA_MODEL
            logger.info(
                "Ollama fallback advisory: %d positions, %d/%d tok (free)",
                len(positions), input_tok, output_tok,
            )
        except Exception as e:
            logger.error("Ollama fallback also failed: %s", e)
            return {"error": str(e)}

    response_time_ms = int((time.time() - t0) * 1000)

    try:
        items = _parse_advice(raw)
    except Exception as e:
        logger.error("Advisory parse error: %s | raw: %.200s", e, raw)
        return {"error": f"parse_error: {e}", "raw_preview": raw[:400]}

    _save_advice(items, raw, model_used=model_used, response_time_ms=response_time_ms)
    logger.info("Advisory saved: %d recommendations via %s", len(items), model_used)

    return {
        "symbols_analyzed": len(items),
        "input_tokens": input_tok,
        "output_tokens": output_tok,
        "cost_usd": round(cost, 6),
        "daily_total": round(daily_cost + cost, 6),
        "daily_cap": DAILY_COST_CAP,
        "model": model_used,
        "response_time_ms": response_time_ms,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


def get_latest_advice() -> list[dict]:
    """Return most recent non-expired Grok advice, one row per symbol."""
    _init_db()
    try:
        conn = _conn()
        rows = conn.execute(
            "SELECT id, symbol, action, confidence, reasoning, "
            "  support_level, resistance_level, stop_loss, target_price, "
            "  hold_period, model_used, response_time_ms, created_at, expires_at, acknowledged "
            "FROM portfolio_advice "
            "WHERE advisor='grok' "
            "  AND (expires_at IS NULL OR expires_at > datetime('now')) "
            "ORDER BY created_at DESC",
        ).fetchall()
        conn.close()
        seen: set[str] = set()
        result = []
        for r in rows:
            sym = r["symbol"]
            if sym not in seen:
                seen.add(sym)
                result.append(dict(r))
        return result
    except Exception as e:
        logger.error("get_latest_advice: %s", e)
        return []


def get_scan_meta() -> dict:
    """Return metadata about the last scan (timestamp, cost, model)."""
    _init_db()
    try:
        conn = _conn()
        row = conn.execute(
            "SELECT created_at, model_used, COUNT(*) AS cnt "
            "FROM portfolio_advice WHERE advisor='grok' "
            "ORDER BY created_at DESC LIMIT 1",
        ).fetchone()
        conn.close()
        last_scan = row["created_at"] if row and row["cnt"] else None
        last_model = row["model_used"] if row and row["cnt"] else None
    except Exception:
        last_scan = None
        last_model = None
    daily_cost = get_daily_cost()
    return {
        "last_scan": last_scan,
        "last_model": last_model or MODEL,
        "daily_cost": daily_cost,
        "daily_cap": DAILY_COST_CAP,
        "model": MODEL,
        "fallback_model": OLLAMA_MODEL,
        "using_fallback": daily_cost >= DAILY_COST_CAP * 0.9,
    }
