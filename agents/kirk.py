"""Kirk — Swing Desk Primary (Holly Swing Advisor).

Strategy: 3–10 day swing setups on Starfleet portfolio universe.
Model: qwen3:8b (local Ollama, free).

Workflow:
  - Ghost-trades every signal for 30 days before promotion to Active 4.
  - On ambiguous setups (confidence 50–65), defers to Pike (mistral:7b) for veto.
  - On clear setups (confidence > 65), records BUY signal directly.

Sacred rules:
  - NEVER drops `kirk_signals` or `kirk_swing_trades` tables.
  - All trades are ghost (paper) until 30-day Sharpe beats current Active 4 #4.

Public entry points:
  - propose_swing(ticker, context) -> dict signal payload
  - get_kirk_brief()               -> dict (today's open swing setups)

S6.3 STUB (2026-04-16). Replaces Grok-4 / Troi-as-Webull-advisor under
Free Models First doctrine.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from datetime import datetime
from typing import Any

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
_DB = "data/trader.db"
_OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
_MODEL = "qwen3:8b"
_AGENT = "kirk"
_AMBIGUOUS_LO = 50
_AMBIGUOUS_HI = 65   # 50–65 → ask Pike; >65 → fire; <50 → skip
_GHOST_HOLD_MIN_DAYS = 3
_GHOST_HOLD_MAX_DAYS = 10

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
os.makedirs("logs", exist_ok=True)
logger = logging.getLogger(_AGENT)
if not logger.handlers:
    h = logging.FileHandler(f"logs/{_AGENT}.log")
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(h)
    logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------

def _ensure_tables() -> None:
    try:
        con = sqlite3.connect(_DB, check_same_thread=False)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {_AGENT}_signals (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   TEXT NOT NULL,
                ticker      TEXT NOT NULL,
                signal      TEXT NOT NULL,
                confidence  INTEGER NOT NULL DEFAULT 50,
                hold_days   INTEGER,
                rationale   TEXT,
                pike_vote   TEXT,
                raw_data    TEXT
            )
        """)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {_AGENT}_swing_trades (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                opened_at   TEXT NOT NULL,
                closed_at   TEXT,
                ticker      TEXT NOT NULL,
                action      TEXT NOT NULL,
                entry_px    REAL,
                exit_px     REAL,
                hold_days   INTEGER,
                pnl_pct     REAL,
                ghost       INTEGER NOT NULL DEFAULT 1,
                notes       TEXT
            )
        """)
        con.commit()
        con.close()
    except Exception as exc:
        logger.error("ensure_tables: %s", exc)


_ensure_tables()


def _db() -> sqlite3.Connection:
    return sqlite3.connect(_DB, check_same_thread=False, timeout=30)


# ---------------------------------------------------------------------------
# Ollama
# ---------------------------------------------------------------------------

def _call_ollama(prompt: str, timeout: int = 60) -> str:
    try:
        r = requests.post(
            f"{_OLLAMA_URL}/api/generate",
            json={
                "model": _MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.25, "num_ctx": 4096},
            },
            timeout=timeout,
        )
        r.raise_for_status()
        return (r.json() or {}).get("response", "").strip()
    except Exception as exc:
        logger.warning("ollama call failed: %s", exc)
        return ""


# ---------------------------------------------------------------------------
# Public — propose a swing setup
# ---------------------------------------------------------------------------

def propose_swing(ticker: str, context: dict | None = None) -> dict:
    """Evaluate a 3–10 day swing setup. Records ghost-trade signal.

    On ambiguous confidence (50–65), pings Pike for veto vote and records it.
    """
    ticker = (ticker or "").upper().strip()
    if not ticker:
        return {"ok": False, "error": "no ticker"}

    ctx_str = json.dumps(context or {}, sort_keys=True)[:1500]
    prompt = (
        "You are Kirk, the swing-desk primary on Starfleet portfolio. "
        f"Evaluate {ticker} for a 3–10 day swing trade. "
        "Return JSON only:\n"
        '{"action":"BUY|SKIP","confidence":0-100,"hold_days":3-10,'
        '"rationale":"one sentence","stop_pct":-3 to -8,"target_pct":+3 to +12}\n\n'
        f"Context: {ctx_str}\n"
        "Today: " + datetime.now().strftime("%Y-%m-%d")
    )
    raw = _call_ollama(prompt)

    parsed: dict = {"action": "SKIP", "confidence": 0, "hold_days": 0,
                    "rationale": "", "stop_pct": 0.0, "target_pct": 0.0}
    if raw:
        try:
            start = raw.find("{")
            end = raw.rfind("}")
            if start >= 0 and end > start:
                parsed.update(json.loads(raw[start : end + 1]))
        except Exception as exc:
            logger.warning("propose_swing parse: %s", exc)

    confidence = int(parsed.get("confidence") or 0)
    action = (parsed.get("action") or "SKIP").upper()
    hold_days = max(_GHOST_HOLD_MIN_DAYS,
                    min(_GHOST_HOLD_MAX_DAYS, int(parsed.get("hold_days") or 5)))

    # Pike consult on ambiguous setups
    pike_vote = ""
    if action == "BUY" and _AMBIGUOUS_LO <= confidence <= _AMBIGUOUS_HI:
        try:
            from agents.pike import second_opinion  # lazy import
            pv = second_opinion(ticker, parsed)
            pike_vote = pv.get("vote", "")
            if pike_vote == "VETO":
                action = "SKIP"
                logger.info("Kirk %s: Pike VETO at conf=%d", ticker, confidence)
        except Exception as exc:
            logger.warning("Pike consult failed (%s) — proceeding without veto", exc)

    ts = datetime.now().isoformat()
    try:
        con = _db()
        con.execute(
            f"INSERT INTO {_AGENT}_signals "
            "(timestamp, ticker, signal, confidence, hold_days, rationale, pike_vote, raw_data) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (ts, ticker, action, confidence, hold_days,
             parsed.get("rationale", "")[:300], pike_vote, json.dumps(parsed)),
        )
        # Open ghost trade only on BUY
        if action == "BUY":
            con.execute(
                f"INSERT INTO {_AGENT}_swing_trades "
                "(opened_at, ticker, action, hold_days, ghost, notes) "
                "VALUES (?,?,?,?,?,?)",
                (ts, ticker, "BUY", hold_days, 1,
                 (parsed.get("rationale") or "")[:200]),
            )
        con.commit()
        con.close()
    except Exception as exc:
        logger.error("propose_swing persist: %s", exc)

    return {
        "ok": True,
        "agent": _AGENT,
        "ticker": ticker,
        "action": action,
        "confidence": confidence,
        "hold_days": hold_days,
        "pike_vote": pike_vote,
        "rationale": parsed.get("rationale", ""),
        "stop_pct": parsed.get("stop_pct"),
        "target_pct": parsed.get("target_pct"),
        "timestamp": ts,
    }


# ---------------------------------------------------------------------------
# Public — open swings brief
# ---------------------------------------------------------------------------

def get_kirk_brief() -> dict:
    """Return today's open swing trades + recent signal stats."""
    out: dict[str, Any] = {"agent": _AGENT, "open": [], "recent": []}
    try:
        con = _db()
        opens = con.execute(
            f"SELECT opened_at, ticker, hold_days, notes "
            f"FROM {_AGENT}_swing_trades "
            "WHERE closed_at IS NULL ORDER BY opened_at DESC LIMIT 30"
        ).fetchall()
        out["open"] = [
            {"opened_at": r[0], "ticker": r[1], "hold_days": r[2], "notes": r[3]}
            for r in opens
        ]
        recent = con.execute(
            f"SELECT timestamp, ticker, signal, confidence, pike_vote "
            f"FROM {_AGENT}_signals ORDER BY id DESC LIMIT 20"
        ).fetchall()
        out["recent"] = [
            {"ts": r[0], "ticker": r[1], "signal": r[2],
             "confidence": r[3], "pike_vote": r[4]}
            for r in recent
        ]
        con.close()
    except Exception as exc:
        logger.error("get_kirk_brief: %s", exc)
    return out


if __name__ == "__main__":
    print(json.dumps(propose_swing("AAPL", {"rsi": 32, "vix": 14.2}), indent=2))
