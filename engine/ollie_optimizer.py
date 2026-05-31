"""engine/ollie_optimizer.py — HM-OLLIE-LEARN Phase 1: regime-conditional rule-optimizer.

PURPOSE
    Ollie (ollie_commander.approve_or_reject) is a STATIC quality gate: a fixed
    OllieScore >= 2.0 threshold, hand-patched per regime (HM-GRADE-B-REGIME-GATE).
    This module LEARNS those gate thresholds from the preserved corpus instead of
    hand-coding them — it sweeps the OllieScore threshold (global AND per-regime)
    against real graded decisions+outcomes, validates the winner OUT-OF-SAMPLE,
    and PROPOSES (never auto-applies) the result.

WHY THIS IS NOW VIABLE (preserved-data audit 2026-05-30)
    The thin-data ceiling (160 ollie-only trades) was a scoping artifact. The true
    corpus is ~5 months / 1,321 cross-agent closed trades + 2,944 replayed Ollie
    decisions + dedicated OOS holdout sets. That supports threshold optimization
    with a real in-sample/out-of-sample split.

DATA (all in data/backtest.db unless noted)
    TRAIN (in-sample):  backtest_v5_ollie_decisions   (score, grade, regime, pnl, outcome, threshold)
    OOS (held-out):     backtest_decisions_oos + backtest_decisions_oos_c
    LIVE cross-check:   data/trader.db trades          (grade, regime, scaled_score, realized_pnl)

DESIGN GUARANTEES
    - REGIME-CONDITIONAL: a threshold is optimized per regime (bull/choppy/bear),
      cross-referenced to regime_history's taxonomy. This is the lesson the
      hand-coded HM-GRADE-B-REGIME-GATE encoded — now found automatically.
    - OOS-VALIDATED: the threshold chosen on TRAIN is scored on the HELD-OUT OOS
      sets. We report in-sample vs OOS and whether it beats the static 2.0 baseline
      out-of-sample. Fit-and-test on the same data is explicitly avoided.
    - PROPOSE-NOT-APPLY: writes suggestions to the `ollie_optimization` table only.
      It NEVER mutates ollie_commander.THRESHOLD. A human/gate applies changes.
      (Live trading agent — never auto-degrade Ollie on an optimization artifact.)
    - FAIL-LOUD: missing tables / empty corpus -> status=error + NTFY, never a
      silent no-op.
    - CPU-only: pure SQL + arithmetic over existing decision rows. No vectorbt,
      no GPU. (Can run on the idle 5080 box overnight but does not need it.)
"""
from __future__ import annotations

import os
import sqlite3
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

BACKTEST_DB = "data/backtest.db"
TRADER_DB = "data/trader.db"

# Current production gate (engine/ollie_commander.py THRESHOLD) — the baseline we
# must beat out-of-sample before proposing a change.
BASELINE_THRESHOLD = 2.0

# Threshold grid to sweep. OllieScore runs 0–5, so sweep the full upper range;
# MIN_SAMPLES naturally caps over-tightening (cells with too few approved trades
# are dropped), and a boundary-pin at the grid max is flagged in the proposal.
THRESHOLD_GRID = [round(1.0 + 0.25 * i, 2) for i in range(17)]  # 1.0 … 5.0
_GRID_MAX = THRESHOLD_GRID[-1]

# Minimum approved-sample count for a (regime, threshold) cell to be trustworthy.
MIN_SAMPLES = 15

# Regime → coarse class for conditioning. The decision tables already store the
# coarse class directly (bull/bear/neutral/volatile); the regime_history taxonomy
# (BULL_CROSS/CAUTIOUS_BEAR/…) is mapped onto the same buckets so both sources align.
REGIME_CLASS = {
    "BULL": "bull", "BULL_CROSS": "bull", "CAUTIOUS_BULL": "bull",
    "BEAR": "bear", "BEAR_CROSS": "bear", "CAUTIOUS_BEAR": "bear",
    "CAUTIOUS": "cautious",   # the decision tables' 2nd-largest bucket — its own class
    "NEUTRAL": "neutral", "VOLATILE": "volatile", "CRISIS": "bear",
}


# ── DB helpers ──────────────────────────────────────────────────────────────────
def _conn(db: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db, timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _regime_class(regime: str | None) -> str:
    if not regime:
        return "unknown"
    return REGIME_CLASS.get(regime.strip().upper(), "unknown")


# ── Corpus loaders ──────────────────────────────────────────────────────────────
def _load_decisions(conn: sqlite3.Connection, table: str) -> list[dict]:
    """Load (score, regime_class, pnl, win) rows from a decisions table.

    The decision tables (backtest_v5_ollie_decisions, backtest_decisions_oos*)
    carry: ollie_score (the gate score, 0-5), regime (coarse class), and
    shadow_pnl_pct (realized % return of the shadowed trade). A row is scoreable
    only with a numeric score AND pnl; rows missing either are dropped.
    """
    if not _table_exists(conn, table):
        return []
    rows = conn.execute(
        f"SELECT ollie_score, regime, shadow_pnl_pct FROM {table}"
    ).fetchall()
    out: list[dict] = []
    for r in rows:
        score = r["ollie_score"]
        pnl = r["shadow_pnl_pct"]
        if score is None or pnl is None:
            continue
        try:
            score = float(score)
            pnl = float(pnl)
        except (TypeError, ValueError):
            continue
        out.append({
            "score": score,
            "regime": _regime_class(r["regime"]),
            "pnl": pnl,            # % return points (objective = total return pts)
            "win": pnl > 0,
        })
    return out


def _live_baseline() -> dict:
    """Aggregate sanity baseline from real cross-agent closed trades.

    The live `trades` table does NOT carry an OllieScore or a regime column, so
    a threshold sweep cannot be applied to it directly. Instead we report what
    the live fleet actually realized (net PnL, win rate, n) as context — the
    ground-truth the proposed gate is ultimately trying to improve. Honest about
    the limit: this is a baseline, not a threshold evaluation.
    """
    conn = _conn(TRADER_DB)
    try:
        if not _table_exists(conn, "trades"):
            return {"n": 0, "net_pnl": None, "win_rate": None}
        rows = conn.execute(
            "SELECT realized_pnl FROM trades WHERE realized_pnl IS NOT NULL"
        ).fetchall()
    finally:
        conn.close()
    pnls = [float(r["realized_pnl"]) for r in rows]
    if not pnls:
        return {"n": 0, "net_pnl": None, "win_rate": None}
    wins = sum(1 for p in pnls if p > 0)
    return {
        "n": len(pnls),
        "net_pnl": round(sum(pnls), 2),
        "win_rate": round(wins / len(pnls), 4),
    }


# ── Scoring ─────────────────────────────────────────────────────────────────────
def _evaluate(rows: list[dict], threshold: float) -> dict:
    """Score a gate threshold over a decision set.

    A row is APPROVED if score >= threshold. We tally the realized outcome of
    approved rows (what the gate would let through) AND the opportunity cost of
    rejected rows (missed winners / correctly-avoided losers).
    """
    approved = [r for r in rows if r["score"] >= threshold]
    rejected = [r for r in rows if r["score"] < threshold]
    n = len(approved)
    net = sum(r["pnl"] for r in approved)
    wins = sum(1 for r in approved if r["win"])
    missed_winners = sum(1 for r in rejected if r["win"])
    avoided_losers = sum(1 for r in rejected if not r["win"])
    return {
        "threshold": threshold,
        "n_approved": n,
        "net_pnl": round(net, 2),
        "avg_pnl": round(net / n, 4) if n else 0.0,
        "win_rate": round(wins / n, 4) if n else 0.0,
        "missed_winners": missed_winners,
        "avoided_losers": avoided_losers,
    }


def _best_threshold(rows: list[dict]) -> dict | None:
    """Sweep the grid; pick the threshold maximizing net_pnl among cells that
    clear MIN_SAMPLES. Returns the full evaluation of the winner, or None if no
    cell has enough samples."""
    candidates = []
    for t in THRESHOLD_GRID:
        ev = _evaluate(rows, t)
        if ev["n_approved"] >= MIN_SAMPLES:
            candidates.append(ev)
    if not candidates:
        return None
    return max(candidates, key=lambda e: e["net_pnl"])


# ── Persistence ─────────────────────────────────────────────────────────────────
def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS ollie_optimization (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               run_date TEXT,
               scope TEXT,              -- 'global' | regime class ('bull'/'bear'/...)
               current_threshold REAL,  -- baseline (production gate)
               suggested_threshold REAL,
               in_sample_pnl REAL,
               in_sample_n INTEGER,
               oos_pnl REAL,            -- suggested threshold on the held-out OOS set
               oos_baseline_pnl REAL,   -- baseline threshold on the same OOS set
               oos_improvement REAL,    -- oos_pnl - oos_baseline_pnl
               oos_n INTEGER,
               live_pnl REAL,           -- suggested threshold on live closed trades
               beats_baseline_oos INTEGER,  -- 1 if oos_improvement > 0
               status TEXT,             -- always 'propose' (never auto-applied)
               detail TEXT
           )"""
    )
    conn.commit()


def _record(conn: sqlite3.Connection, run_date: str, scope: str, suggested: dict,
            oos: dict, oos_base: dict, live: dict, detail: str) -> None:
    improvement = round(oos["net_pnl"] - oos_base["net_pnl"], 2)
    conn.execute(
        """INSERT INTO ollie_optimization
           (run_date, scope, current_threshold, suggested_threshold,
            in_sample_pnl, in_sample_n, oos_pnl, oos_baseline_pnl,
            oos_improvement, oos_n, live_pnl, beats_baseline_oos, status, detail)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (run_date, scope, BASELINE_THRESHOLD, suggested["threshold"],
         suggested["net_pnl"], suggested["n_approved"], oos["net_pnl"],
         oos_base["net_pnl"], improvement, oos["n_approved"], live.get("net_pnl"),
         1 if improvement > 0 else 0, "propose", detail),
    )
    conn.commit()


# ── Orchestrator ────────────────────────────────────────────────────────────────
def run_ollie_optimizer() -> dict:
    """Optimize Ollie's gate threshold (global + per-regime), OOS-validate, propose.

    Returns a status dict. status='error' (with NTFY) on any data shortfall —
    never a silent no-op.
    """
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _fail(cause: str) -> dict:
        logger.error("[ollie_optimizer] %s", cause)
        try:
            from engine.notifier import notify
            notify(f"🔴 OLLIE-OPTIMIZER FAIL: {cause}", priority="high")
        except Exception:
            pass
        return {"status": "error", "cause": cause}

    bt = _conn(BACKTEST_DB)
    try:
        train = _load_decisions(bt, "backtest_v5_ollie_decisions")
        oos = (_load_decisions(bt, "backtest_decisions_oos")
               + _load_decisions(bt, "backtest_decisions_oos_c"))
    finally:
        bt.close()

    if len(train) < MIN_SAMPLES * 2:
        return _fail(f"train corpus too small ({len(train)} rows)")
    if len(oos) < MIN_SAMPLES:
        return _fail(f"OOS corpus too small ({len(oos)} rows)")

    live = _live_baseline()

    proposals: list[dict] = []
    out = _conn(BACKTEST_DB)
    try:
        _ensure_table(out)
        # Idempotent re-run: clear any prior rows for today before recording.
        out.execute("DELETE FROM ollie_optimization WHERE run_date=?", (run_date,))
        out.commit()

        # ── GLOBAL ──────────────────────────────────────────────────────────
        best = _best_threshold(train)
        if best is None:
            return _fail("no global threshold cell cleared MIN_SAMPLES")
        oos_sugg = _evaluate(oos, best["threshold"])
        oos_base = _evaluate(oos, BASELINE_THRESHOLD)
        _pin = " [BOUNDARY-PINNED@grid-max — true optimum may be higher]" if best["threshold"] >= _GRID_MAX else ""
        detail = (f"grid sweep {THRESHOLD_GRID[0]}–{THRESHOLD_GRID[-1]}{_pin}; "
                  f"is_wr={best['win_rate']:.0%} oos_wr={oos_sugg['win_rate']:.0%} "
                  f"missed_winners_oos={oos_sugg['missed_winners']} | "
                  f"live_baseline net={live['net_pnl']} wr={live['win_rate']} n={live['n']}")
        _record(out, run_date, "global", best, oos_sugg, oos_base, live, detail)
        proposals.append({
            "scope": "global", "suggested": best["threshold"],
            "baseline": BASELINE_THRESHOLD,
            "oos_improvement": round(oos_sugg["net_pnl"] - oos_base["net_pnl"], 2),
            "beats_baseline_oos": oos_sugg["net_pnl"] > oos_base["net_pnl"],
        })

        # ── PER-REGIME ──────────────────────────────────────────────────────
        for rc in ("bull", "bear", "cautious", "neutral", "volatile"):
            tr = [r for r in train if r["regime"] == rc]
            ev_oos = [r for r in oos if r["regime"] == rc]
            if len(tr) < MIN_SAMPLES or len(ev_oos) < MIN_SAMPLES:
                logger.info("[ollie_optimizer] regime=%s skipped (train=%d oos=%d)",
                            rc, len(tr), len(ev_oos))
                continue
            rbest = _best_threshold(tr)
            if rbest is None:
                continue
            r_oos = _evaluate(ev_oos, rbest["threshold"])
            r_base = _evaluate(ev_oos, BASELINE_THRESHOLD)
            # live baseline is not regime-split (trades table has no regime column).
            r_live = {"net_pnl": None}
            _rpin = " [PINNED@grid-max]" if rbest["threshold"] >= _GRID_MAX else ""
            rdetail = (f"regime={rc} is_n={rbest['n_approved']} "
                       f"is_wr={rbest['win_rate']:.0%} oos_wr={r_oos['win_rate']:.0%}{_rpin}")
            _record(out, run_date, rc, rbest, r_oos, r_base, r_live, rdetail)
            proposals.append({
                "scope": rc, "suggested": rbest["threshold"],
                "baseline": BASELINE_THRESHOLD,
                "oos_improvement": round(r_oos["net_pnl"] - r_base["net_pnl"], 2),
                "beats_baseline_oos": r_oos["net_pnl"] > r_base["net_pnl"],
            })
    finally:
        out.close()

    return {
        "status": "ok",
        "run_date": run_date,
        "train_n": len(train),
        "oos_n": len(oos),
        "live_baseline": live,
        "proposals": proposals,
        "note": "PROPOSE-mode — ollie_commander.THRESHOLD NOT modified. Human/gate applies.",
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import json
    print(json.dumps(run_ollie_optimizer(), indent=2))
