"""Sarek — Elder Council 5-Year Horizon Agent.

Strategy: Quality compounders + dividend aristocrats. Monthly rebalance cadence.
Model: qwen3:8b (local Ollama, free).
Universe seed: high-ROIC, 25+ year dividend growers, low-debt mega/large caps.

Sacred rules:
  - NEVER drops `sarek_signals` or `sarek_paper_trades` tables.
  - Logs every monthly DCA tranche to `sarek_paper_trades` for ghost scoring.
  - Scored on 6-month rolling Sharpe — not daily.

Public entry points:
  - get_sarek_brief()           -> dict (latest thesis + watchlist)
  - run_monthly_dca(amount=500) -> writes paper-trade rows for current rebalance

This is a STUB (S6.3, 2026-04-16) — wire to live data feed once Polygon.io
Options Starter is activated, or Yahoo Finance for fundamentals in the meantime.
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
_HORIZON_YEARS = 5
_AGENT = "sarek"
_CACHE: dict[str, Any] = {"brief": None, "ts": 0}
_CACHE_TTL = 60 * 60 * 24  # 24h — patient investor, no need to refresh hot

# Seed watchlist (hand-picked aristocrats + quality compounders).
# Replace with screener output once fundamentals feed lands.
_SEED_UNIVERSE = [
    "JNJ", "PG", "KO", "PEP", "WMT", "MSFT", "AAPL",  # quality compounders
    "MMM", "ITW", "CL", "CLX", "EMR", "GPC",          # dividend aristocrats
    "BRK.B", "V", "MA", "COST", "LIN",                # moat compounders
]

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
# Tables (sacred — IF NOT EXISTS only)
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
                thesis      TEXT,
                raw_data    TEXT
            )
        """)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {_AGENT}_paper_trades (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   TEXT NOT NULL,
                ticker      TEXT NOT NULL,
                action      TEXT NOT NULL,        -- BUY / SELL / HOLD
                shares      REAL,
                price       REAL,
                tranche_usd REAL,
                horizon     TEXT NOT NULL DEFAULT '5y',
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
# Ollama call
# ---------------------------------------------------------------------------

def _call_ollama(prompt: str, timeout: int = 90) -> str:
    """Single-shot generation against local Ollama. Returns '' on failure."""
    try:
        r = requests.post(
            f"{_OLLAMA_URL}/api/generate",
            json={
                "model": _MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.2, "num_ctx": 4096},
            },
            timeout=timeout,
        )
        r.raise_for_status()
        return (r.json() or {}).get("response", "").strip()
    except Exception as exc:
        logger.warning("ollama call failed: %s", exc)
        return ""


# ---------------------------------------------------------------------------
# Public — thesis brief
# ---------------------------------------------------------------------------

def get_sarek_brief(force: bool = False) -> dict:
    """Return Sarek's current 5-year thesis brief.

    Cached 24h. force=True bypasses cache.
    """
    now = time.time()
    if not force and _CACHE["brief"] and (now - _CACHE["ts"]) < _CACHE_TTL:
        return _CACHE["brief"]

    prompt = (
        "You are Sarek, a 5-year-horizon equity strategist on the Elder Council. "
        "Pick 3 highest-conviction names from this universe based on quality + dividend "
        "durability, and explain in 2 sentences each. Return JSON only:\n"
        '{"picks":[{"ticker":"X","thesis":"..."}], "macro_view":"..."}\n\n'
        f"Universe: {', '.join(_SEED_UNIVERSE)}\n"
        "Today: " + datetime.now().strftime("%Y-%m-%d")
    )

    raw = _call_ollama(prompt)
    parsed: dict = {"picks": [], "macro_view": "", "raw": raw}
    if raw:
        try:
            # Extract JSON block (model may wrap in prose)
            start = raw.find("{")
            end = raw.rfind("}")
            if start >= 0 and end > start:
                parsed.update(json.loads(raw[start : end + 1]))
        except Exception as exc:
            logger.warning("brief parse: %s", exc)

    brief = {
        "agent": _AGENT,
        "horizon_years": _HORIZON_YEARS,
        "model": _MODEL,
        "timestamp": datetime.now().isoformat(),
        "picks": parsed.get("picks", []),
        "macro_view": parsed.get("macro_view", ""),
        "universe": _SEED_UNIVERSE,
    }

    # Persist top picks as signals
    try:
        con = _db()
        for p in brief["picks"][:5]:
            con.execute(
                f"INSERT INTO {_AGENT}_signals "
                "(timestamp, ticker, signal, confidence, thesis, raw_data) "
                "VALUES (?,?,?,?,?,?)",
                (
                    brief["timestamp"],
                    (p.get("ticker") or "").upper(),
                    "BUY",
                    70,
                    p.get("thesis", ""),
                    json.dumps(p),
                ),
            )
        con.commit()
        con.close()
    except Exception as exc:
        logger.error("persist signals: %s", exc)

    _CACHE["brief"] = brief
    _CACHE["ts"] = now
    return brief


# ---------------------------------------------------------------------------
# Public — monthly DCA (ghost paper trades)
# ---------------------------------------------------------------------------

def run_monthly_dca(amount_usd: float = 500.0) -> list[dict]:
    """Allocate monthly DCA tranche evenly across current top picks.

    Writes BUY rows to sarek_paper_trades. No live broker calls — ghost only.
    Price is left NULL here; reconciler should patch with VWAP at fill time.
    """
    brief = get_sarek_brief()
    picks = brief.get("picks") or []
    if not picks:
        # Fallback: spread across first 3 of seed universe
        picks = [{"ticker": t, "thesis": "fallback DCA"} for t in _SEED_UNIVERSE[:3]]

    per_tranche = round(amount_usd / max(len(picks), 1), 2)
    rows: list[dict] = []
    ts = datetime.now().isoformat()

    try:
        con = _db()
        for p in picks:
            ticker = (p.get("ticker") or "").upper()
            if not ticker:
                continue
            con.execute(
                f"INSERT INTO {_AGENT}_paper_trades "
                "(timestamp, ticker, action, tranche_usd, horizon, notes) "
                "VALUES (?,?,?,?,?,?)",
                (ts, ticker, "BUY", per_tranche, f"{_HORIZON_YEARS}y",
                 (p.get("thesis") or "")[:200]),
            )
            rows.append({"ticker": ticker, "tranche_usd": per_tranche})
        con.commit()
        con.close()
        logger.info("Sarek DCA: %d tranches @ $%.2f each", len(rows), per_tranche)
    except Exception as exc:
        logger.error("DCA write: %s", exc)

    return rows


if __name__ == "__main__":
    print(json.dumps(get_sarek_brief(force=True), indent=2)[:1000])
