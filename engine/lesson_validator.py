"""engine/lesson_validator.py — HM-LESSON-VALIDATION: shadow-first CULLING loop for the
FinMem Reflexion lessons.

CORE FRAMING (the safety): this is a CULLING loop — kill demonstrably-HARMFUL lessons, do
NOT anoint winners at N=5. Asymmetric: quick to demote clear harm, slow to confirm good.
Claiming more than the data supports is the GB-gate mistake one level up.

The FinMem loop (self_improvement.py) generates good lessons ("Do not short INTC in
BULL_CROSS") + decays them by recency×magnitude — but NEVER checks whether following a
lesson helped. This adds that check, SHADOW-ONLY: it logs would-be salience adjustments to
lesson_validation_shadow and NEVER touches agent_memory.score until eyes-on'd.

Design (per the approved scope):
  Q1 ATTRIBUTION  parse parseable lessons → {player,ticker,regime,action}; scan decision_audit
                  AFTER created_at → followed (prohibited action not taken) vs ignored (taken,
                  real outcome). Only parseable lessons validated; coverage reported.
  Q3 COUNTERFACTUAL  followed prohibitions have no trade → price the would-be trade over a
                  horizon (reuse the shadow/price machinery). would-be loss=lesson right;
                  would-be win=false prohibition.
  Q5 GUARDS       (1) min-N (k=5) before any verdict; (2) significance margin, not raw counts;
                  (3) FORWARD-ONLY (created_at strictly after the lesson); (4) ADVISORY-ONLY
                  (adjusts salience-shadow, never a live gate).
  Q4 DECAY        factor × existing salience: helpful 1.5 / harmful 0.2 / provisional 1.0
                  (NEUTRAL — a good-but-rarely-tested lesson is never killed for being untested).
  Q2 CLUSTERING   CONSERVATIVE — group by (action, regime, sector); FLAGGED weak link. Primary
                  path is PER-CONDITION validation; cluster aggregation is reported as a
                  secondary signal only (fall back to per-condition if clustering looks shaky).
"""
from __future__ import annotations

import re
import sqlite3
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
DB = "data/trader.db"

MIN_TESTS = 5            # k — minimum forward tests before any non-provisional verdict
HORIZON_DAYS = 5         # counterfactual horizon for followed prohibitions
SIG_MARGIN = 0.70        # of tested instances, the lesson must be "right" ≥70% to cull/confirm
FACTOR = {"harmful": 0.2, "helpful": 1.5, "provisional": 1.0}

_ACTION = {"shorting": "short", "short": "short", "buy": "buy", "buying": "buy",
           "sell": "sell", "selling": "sell", "bought": "buy", "sold": "sell"}
# "Do not|Avoid|Never [action] [TICKER] ... in [REGIME]"
_LESSON_RE = re.compile(
    r"(?:do not|don't|avoid|never)\s+(\w+)\s+(?:short(?:ing)?\s+|buy(?:ing)?\s+|selling\s+)?"
    r"([A-Z]{2,5})\b.*?\bin\s+([A-Z_]+)", re.IGNORECASE)


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB, timeout=30.0); c.row_factory = sqlite3.Row
    return c


def _ensure_shadow(conn) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS lesson_validation_shadow (
               id INTEGER PRIMARY KEY AUTOINCREMENT, run_date TEXT, agent_memory_id INTEGER,
               player_id TEXT, rule TEXT, ticker TEXT, regime TEXT, action TEXT,
               n_tests INTEGER, n_followed INTEGER, n_ignored INTEGER,
               right_rate REAL, verdict TEXT, cur_score REAL, would_be_score REAL,
               cluster TEXT, note TEXT)""")
    conn.commit()


# ── Q1: parse lessons ─────────────────────────────────────────────────────────
def _parse_lessons(conn) -> tuple[list[dict], int]:
    rows = conn.execute(
        "SELECT id, player_id, summary, score, created_at FROM agent_memory "
        "WHERE memory_layer LIKE '%LESSON%'").fetchall()
    parsed, total_rules = [], 0
    for r in rows:
        for raw in re.split(r"\n|\d\.\s*", str(r["summary"] or "")):
            raw = raw.strip(" *—-").strip()
            if len(raw) < 10:
                continue
            total_rules += 1
            m = _LESSON_RE.search(raw)
            if not m:
                continue
            action = _ACTION.get(m.group(1).lower())
            if not action or m.group(2).lower() == "any":
                continue
            parsed.append({
                "amid": r["id"], "player_id": r["player_id"], "rule": raw[:160],
                "action": action, "ticker": m.group(2).upper(),
                "regime": m.group(3).upper(), "score": float(r["score"] or 0),
                "created_at": str(r["created_at"]),
            })
    return parsed, total_rules


def _sector(conn, ticker: str) -> str:
    try:
        row = conn.execute("SELECT sector FROM scan_universe WHERE symbol=?", (ticker,)).fetchone()
        return (row["sector"] if row and row["sector"] else "unknown")
    except Exception:
        return "unknown"


# ── Q1+Q3: attribution + counterfactual ───────────────────────────────────────
def _attribute(conn, L: dict) -> dict:
    """FORWARD-ONLY: only decisions strictly after the lesson's created_at. A prohibited
    action that fired = IGNORED (real outcome); a non-fire on the matched condition =
    FOLLOWED (counterfactual). Returns counts + 'right' tally."""
    want_trade_action = "BUY" if L["action"] == "buy" else "SELL"   # short/sell → SELL
    rows = conn.execute(
        """SELECT event_type, trade_id, created_at FROM decision_audit
           WHERE player_id=? AND symbol=? AND regime=? AND created_at > ?
           ORDER BY created_at""",
        (L["player_id"], L["ticker"], L["regime"], L["created_at"])).fetchall()
    n_followed = n_ignored = n_right = 0
    for d in rows:
        if d["event_type"] == "trade_fire" and d["trade_id"]:
            tr = conn.execute("SELECT action, realized_pnl FROM trades WHERE id=?",
                              (d["trade_id"],)).fetchone()
            if tr and (tr["action"] or "").upper() == want_trade_action:
                n_ignored += 1                      # prohibited action was TAKEN
                if tr["realized_pnl"] is not None and tr["realized_pnl"] < 0:
                    n_right += 1                    # it lost → the prohibition was RIGHT
            # opposite-direction or no-action rows fall through to followed below
        else:
            n_followed += 1                          # gate_reject / signal-no-trade = obeyed
            cf = _counterfactual(conn, L, d["created_at"])
            if cf is not None and cf < 0:
                n_right += 1                          # blocked trade would have lost → RIGHT
    n_tests = n_followed + n_ignored
    return {"n_followed": n_followed, "n_ignored": n_ignored, "n_tests": n_tests,
            "right_rate": round(n_right / n_tests, 3) if n_tests else None}


def _counterfactual(conn, L: dict, at_iso: str):
    """Would-be return of the prohibited trade over HORIZON_DAYS, from daily closes. Returns
    signed % (long view; for a short prohibition, invert) or None if no price data."""
    try:
        d0 = at_iso[:10]
        b = sqlite3.connect("data/backtest.db", timeout=15.0)
        rows = b.execute(
            "SELECT trade_date, close FROM backtest_market_data WHERE symbol=? AND trade_date>=? "
            "ORDER BY trade_date LIMIT ?", (L["ticker"], d0, HORIZON_DAYS + 1)).fetchall()
        b.close()
        if len(rows) < 2:
            return None
        p0, p1 = rows[0][1], rows[-1][1]
        if not p0:
            return None
        ret = (p1 - p0) / p0 * 100
        return ret if L["action"] == "buy" else -ret   # short: gain when price falls
    except Exception:
        return None


# ── Q4+Q5: verdict (asymmetric, guarded) ──────────────────────────────────────
def _verdict(att: dict) -> str:
    """CULLING-biased: only 'harmful' (cull) or 'helpful' (confirm) when k tests + a clear
    margin; everything else PROVISIONAL. Slow to confirm, focus on culling clear harm."""
    n, rr = att["n_tests"], att["right_rate"]
    if n < MIN_TESTS or rr is None:
        return "provisional"           # guard 1: min-N
    if rr <= (1 - SIG_MARGIN):
        return "harmful"               # lesson is wrong ≥70% of the time → cull
    if rr >= SIG_MARGIN:
        return "helpful"               # slow-confirm (still advisory-only)
    return "provisional"               # guard 2: no clear margin


def run_validation(shadow: bool = True) -> dict:
    """Validate lessons, SHADOW-ONLY (logs would-be salience; never touches agent_memory).
    shadow=False is reserved for a future approved live mode — NOT used yet."""
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    conn = _conn()
    try:
        _ensure_shadow(conn)
        conn.execute("DELETE FROM lesson_validation_shadow WHERE run_date=?", (run_date,))
        lessons, total_rules = _parse_lessons(conn)
        results = []
        for L in lessons:
            att = _attribute(conn, L)
            v = _verdict(att)
            would = round(L["score"] * FACTOR[v], 3)
            cluster = f"{L['action']}|{L['regime']}|{_sector(conn, L['ticker'])}"
            conn.execute(
                """INSERT INTO lesson_validation_shadow
                   (run_date,agent_memory_id,player_id,rule,ticker,regime,action,n_tests,
                    n_followed,n_ignored,right_rate,verdict,cur_score,would_be_score,cluster,note)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (run_date, L["amid"], L["player_id"], L["rule"], L["ticker"], L["regime"],
                 L["action"], att["n_tests"], att["n_followed"], att["n_ignored"],
                 att["right_rate"], v, L["score"], would, cluster,
                 "SHADOW — agent_memory.score NOT modified"))
            results.append({**L, **att, "verdict": v, "would_be_score": would, "cluster": cluster})
        conn.commit()
        from collections import Counter
        vc = Counter(r["verdict"] for r in results)
        return {
            "run_date": run_date, "mode": "SHADOW — no live salience change",
            "parse_coverage": f"{len(lessons)}/{total_rules} rules parseable",
            "verdicts": dict(vc), "n_lessons": len(results),
            "min_tests_k": MIN_TESTS, "results": results,
            "note": ("CULLING loop: only clearly-harmful lessons (right_rate ≤ %.0f%% over ≥%d "
                     "forward tests) are flagged to cull; the rest stay PROVISIONAL — honest "
                     "given thin/one-regime data. Advisory-only; agent_memory untouched."
                     % ((1 - SIG_MARGIN) * 100, MIN_TESTS)),
        }
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import json
    r = run_validation(shadow=True)
    r2 = {k: v for k, v in r.items() if k != "results"}
    print(json.dumps(r2, indent=2))
    print("\nSample classifications:")
    for x in r["results"][:6]:
        print("  [%s] %s %s/%s n=%d right=%s score %.2f→%.2f cluster=%s"
              % (x["verdict"], x["ticker"], x["action"], x["regime"], x["n_tests"],
                 x["right_rate"], x["score"], x["would_be_score"], x["cluster"]))
