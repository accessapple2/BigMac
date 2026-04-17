"""Janeway — Elder Council 10-Year Horizon Agent.

Strategy: Innovation S-curves + moat leaders. Quarterly review cadence.
Model: phi3:mini (local Ollama, free — Phi lineage is orthogonal to all other
fleet models, per Free Models First doctrine real-orthogonality rule).

Sacred rules:
  - NEVER drops `janeway_signals` or `janeway_paper_trades` tables.
  - Logs every quarterly DCA tranche to `janeway_paper_trades` for ghost scoring.
  - Scored on 6-month rolling Sharpe — not daily.

Public entry points:
  - get_janeway_brief()             -> dict (latest thesis + watchlist)
  - run_quarterly_dca(amount=1000)  -> writes paper-trade rows for current rebalance

This is a STUB (S6.3, 2026-04-16).
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
_MODEL = "phi3:mini"
_HORIZON_YEARS = 10
_AGENT = "janeway"
_CACHE: dict[str, Any] = {"brief": None, "ts": 0}
_CACHE_TTL = 60 * 60 * 24 * 7  # 7d — quarterly cadence

# Innovation S-curve + moat leader seed (AI infra, biotech, energy transition)
_SEED_UNIVERSE = [
    "NVDA", "TSM", "AVGO", "ASML", "AMD",            # AI infra
    "GOOGL", "META", "AMZN", "MSFT",                  # platform moats
    "NVO", "LLY", "REGN", "VRTX",                     # biotech leaders
    "ENPH", "FSLR", "TSLA", "RIVN",                   # energy transition
    "SHOP", "MELI", "SE",                             # commerce S-curves
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
                action      TEXT NOT NULL,
                shares      REAL,
                price       REAL,
                tranche_usd REAL,
                horizon     TEXT NOT NULL DEFAULT '10y',
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

def _call_ollama(prompt: str, timeout: int = 90) -> str:
    try:
        r = requests.post(
            f"{_OLLAMA_URL}/api/generate",
            json={
                "model": _MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.3, "num_ctx": 4096},
            },
            timeout=timeout,
        )
        r.raise_for_status()
        return (r.json() or {}).get("response", "").strip()
    except Exception as exc:
        logger.warning("ollama call failed: %s", exc)
        return ""


# ---------------------------------------------------------------------------
# Public — brief
# ---------------------------------------------------------------------------

def get_janeway_brief(force: bool = False) -> dict:
    """Janeway's 10-year innovation thesis. Cached 7d (quarterly cadence)."""
    now = time.time()
    if not force and _CACHE["brief"] and (now - _CACHE["ts"]) < _CACHE_TTL:
        return _CACHE["brief"]

    prompt = (
        "You are Janeway, a 10-year innovation strategist on the Elder Council. "
        "Pick 4 highest-conviction names from this universe based on durable S-curves "
        "and unbreachable moats. Return JSON only:\n"
        '{"picks":[{"ticker":"X","thesis":"...","s_curve":"early|mid|late"}], '
        '"macro_view":"..."}\n\n'
        f"Universe: {', '.join(_SEED_UNIVERSE)}\n"
        "Today: " + datetime.now().strftime("%Y-%m-%d")
    )

    raw = _call_ollama(prompt)
    parsed: dict = {"picks": [], "macro_view": "", "raw": raw}
    if raw:
        try:
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

    try:
        con = _db()
        for p in brief["picks"][:6]:
            con.execute(
                f"INSERT INTO {_AGENT}_signals "
                "(timestamp, ticker, signal, confidence, thesis, raw_data) "
                "VALUES (?,?,?,?,?,?)",
                (
                    brief["timestamp"],
                    (p.get("ticker") or "").upper(),
                    "BUY",
                    72,
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
# Public — quarterly DCA
# ---------------------------------------------------------------------------

def run_quarterly_dca(amount_usd: float = 1000.0) -> list[dict]:
    """Allocate quarterly DCA evenly across top picks. Writes ghost paper trades."""
    brief = get_janeway_brief()
    picks = brief.get("picks") or []
    if not picks:
        picks = [{"ticker": t, "thesis": "fallback DCA"} for t in _SEED_UNIVERSE[:4]]

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
        logger.info("Janeway quarterly DCA: %d tranches @ $%.2f", len(rows), per_tranche)
    except Exception as exc:
        logger.error("DCA write: %s", exc)

    return rows


if __name__ == "__main__":
    print(json.dumps(get_janeway_brief(force=True), indent=2)[:1000])
