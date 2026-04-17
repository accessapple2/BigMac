"""Pike — Swing Desk Backup / Second-Opinion Veto.

Role: Vote on Kirk's ambiguous swing setups (Kirk confidence 50–65).
Model: mistral:7b (local Ollama, free — Mistral lineage is orthogonal to Kirk's
qwen3:8b for real second-opinion diversity, per Free Models First doctrine).

Sacred rules:
  - NEVER drops `pike_votes` table.
  - Pike never opens trades on his own — strictly veto/confirm only.

Public entry point:
  - second_opinion(ticker, kirk_proposal) -> dict {vote: CONFIRM|VETO|ABSTAIN}

S6.3 STUB (2026-04-16).
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
_DB = "data/trader.db"
_OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
_MODEL = "mistral:7b"
_AGENT = "pike"

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
            CREATE TABLE IF NOT EXISTS {_AGENT}_votes (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp     TEXT NOT NULL,
                ticker        TEXT NOT NULL,
                kirk_action   TEXT,
                kirk_conf     INTEGER,
                vote          TEXT NOT NULL,    -- CONFIRM / VETO / ABSTAIN
                rationale     TEXT,
                raw_data      TEXT
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
# Public — second opinion
# ---------------------------------------------------------------------------

def second_opinion(ticker: str, kirk_proposal: dict) -> dict:
    """Vote on Kirk's ambiguous swing proposal.

    Returns: {"vote": "CONFIRM"|"VETO"|"ABSTAIN", "rationale": "..."}
    A VETO suppresses the trade; CONFIRM lets it proceed.
    """
    ticker = (ticker or "").upper().strip()
    if not ticker:
        return {"vote": "ABSTAIN", "rationale": "empty ticker"}

    kp_str = json.dumps(kirk_proposal or {}, sort_keys=True)[:800]
    prompt = (
        "You are Pike, swing-desk backup. Review Kirk's proposal and cast ONE vote: "
        "CONFIRM (agree), VETO (disagree, kill the trade), or ABSTAIN (insufficient signal). "
        "Lean VETO when risk/reward is unclear. Return JSON only:\n"
        '{"vote":"CONFIRM|VETO|ABSTAIN","rationale":"one sentence"}\n\n'
        f"Kirk's proposal for {ticker}: {kp_str}\n"
        "Today: " + datetime.now().strftime("%Y-%m-%d")
    )

    raw = _call_ollama(prompt)
    parsed = {"vote": "ABSTAIN", "rationale": ""}
    if raw:
        try:
            start = raw.find("{")
            end = raw.rfind("}")
            if start >= 0 and end > start:
                parsed.update(json.loads(raw[start : end + 1]))
        except Exception as exc:
            logger.warning("vote parse: %s", exc)

    vote = (parsed.get("vote") or "ABSTAIN").upper()
    if vote not in ("CONFIRM", "VETO", "ABSTAIN"):
        vote = "ABSTAIN"

    ts = datetime.now().isoformat()
    try:
        con = _db()
        con.execute(
            f"INSERT INTO {_AGENT}_votes "
            "(timestamp, ticker, kirk_action, kirk_conf, vote, rationale, raw_data) "
            "VALUES (?,?,?,?,?,?,?)",
            (ts, ticker,
             (kirk_proposal or {}).get("action", ""),
             int((kirk_proposal or {}).get("confidence", 0) or 0),
             vote, parsed.get("rationale", "")[:300], json.dumps(parsed)),
        )
        con.commit()
        con.close()
    except Exception as exc:
        logger.error("vote persist: %s", exc)

    logger.info("Pike %s on %s (Kirk conf=%s)", vote, ticker,
                (kirk_proposal or {}).get("confidence"))
    return {"vote": vote, "rationale": parsed.get("rationale", ""),
            "agent": _AGENT, "timestamp": ts}


if __name__ == "__main__":
    sample = {"action": "BUY", "confidence": 58, "hold_days": 5,
              "rationale": "RSI bounce", "stop_pct": -4, "target_pct": +6}
    print(json.dumps(second_opinion("AAPL", sample), indent=2))
