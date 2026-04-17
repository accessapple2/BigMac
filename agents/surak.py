"""Surak — Elder Council 20-Year Horizon Agent.

Strategy: Secular themes (energy, AI, demographics). Annual rebalance.
Model: gemma3:4b (local Ollama, free — Gemma lineage is orthogonal to Qwen/Phi/
DeepSeek/Mistral/Plutus/Llama on the fleet, per Free Models First doctrine).

Sacred rules:
  - NEVER drops `surak_signals` or `surak_paper_trades` tables.
  - Annual DCA only; logs to `surak_paper_trades` for ghost scoring.
  - Scored on 6-month rolling Sharpe (annualised).

Public entry points:
  - get_surak_brief()             -> dict (latest secular thesis)
  - run_annual_dca(amount=2000)   -> writes paper-trade rows for annual rebalance

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
_MODEL = "gemma3:4b"
_HORIZON_YEARS = 20
_AGENT = "surak"
_CACHE: dict[str, Any] = {"brief": None, "ts": 0}
_CACHE_TTL = 60 * 60 * 24 * 30  # 30d — annual cadence, monthly cache OK

# Secular-theme universe (20-year tailwinds)
_SEED_UNIVERSE = [
    # Energy transition
    "ENPH", "FSLR", "NEE", "BEP", "ICLN",
    # AI / compute long arc
    "NVDA", "TSM", "ASML", "GOOGL",
    # Demographics (aging, healthcare)
    "UNH", "ISRG", "MDT", "ABBV",
    # Critical materials / scarcity
    "LIT", "REMX", "URA", "GLD",
    # Water / infrastructure
    "AWK", "XYL", "PWR",
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
                thesis      TEXT,
                theme       TEXT,
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
                horizon     TEXT NOT NULL DEFAULT '20y',
                theme       TEXT,
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
# Public — brief
# ---------------------------------------------------------------------------

def get_surak_brief(force: bool = False) -> dict:
    """Surak's 20-year secular thesis. Cached 30d (annual cadence)."""
    now = time.time()
    if not force and _CACHE["brief"] and (now - _CACHE["ts"]) < _CACHE_TTL:
        return _CACHE["brief"]

    prompt = (
        "You are Surak, a 20-year secular-theme strategist on the Elder Council. "
        "Pick 5 highest-conviction names from this universe across themes: "
        "energy transition, AI/compute, aging demographics, critical materials, "
        "and water/infrastructure. Return JSON only:\n"
        '{"picks":[{"ticker":"X","thesis":"...","theme":"..."}], "macro_view":"..."}\n\n'
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
        for p in brief["picks"][:8]:
            con.execute(
                f"INSERT INTO {_AGENT}_signals "
                "(timestamp, ticker, signal, confidence, thesis, theme, raw_data) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    brief["timestamp"],
                    (p.get("ticker") or "").upper(),
                    "BUY",
                    65,
                    p.get("thesis", ""),
                    p.get("theme", ""),
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
# Public — annual DCA
# ---------------------------------------------------------------------------

def run_annual_dca(amount_usd: float = 2000.0) -> list[dict]:
    """Allocate annual DCA across secular-theme top picks. Writes ghost trades."""
    brief = get_surak_brief()
    picks = brief.get("picks") or []
    if not picks:
        picks = [{"ticker": t, "thesis": "fallback DCA", "theme": "seed"}
                 for t in _SEED_UNIVERSE[:5]]

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
                "(timestamp, ticker, action, tranche_usd, horizon, theme, notes) "
                "VALUES (?,?,?,?,?,?,?)",
                (ts, ticker, "BUY", per_tranche, f"{_HORIZON_YEARS}y",
                 p.get("theme", ""), (p.get("thesis") or "")[:200]),
            )
            rows.append({"ticker": ticker, "tranche_usd": per_tranche,
                         "theme": p.get("theme", "")})
        con.commit()
        con.close()
        logger.info("Surak annual DCA: %d tranches @ $%.2f", len(rows), per_tranche)
    except Exception as exc:
        logger.error("DCA write: %s", exc)

    return rows


if __name__ == "__main__":
    print(json.dumps(get_surak_brief(force=True), indent=2)[:1000])
