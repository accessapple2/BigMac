"""engine/calibration_map.py — HM-XO-PLAN-2026-09 Phase 2, item B.2: confidence calibration map.

Fits stated confidence -> realized hit rate, bucketed per regime, from
decision_audit (trade_fire events) joined to trades' realized outcomes.
Exposes get_calibrated_confidence() as the function a future sizing gate
can call.

DOES NOT WIRE INTO SIZING. Per the directive: this is the input Phase 1.3
consumes after it lands; Phase 1.3 itself (actually sizing off the
calibrated number) is a separate, later, deliberate change. Nothing in
this file is imported by any live decision or execution path today.

IMPORTANT for whoever wires Phase 1.3's sizing (Admiral decision,
2026-09-10, docs/XO_PLAN_2026-09.md's Phase 1.3 section): the fail-OPEN
behavior below (returning stated_confidence unchanged when a bucket has
too little data) is correct for a pass/fail GATE, where "no evidence
either way" shouldn't block a trade. It is WRONG for SIZING -- a sizing
tier built on top of this function must apply its own fail-CLOSED rule
(base allocation only, never the top tier, when the bucket has no
evidence), not treat this function's fail-open passthrough as if it were
verified confidence. Do not change this function's own fail-open default
to satisfy that -- the gate still needs it. Apply the fail-closed rule in
the sizing layer instead.

METHOD: simple binned, not isotonic -- deliberately, not by default.
Per-regime sample sizes (live 2026-09-10, `trade_fire` events with a
clean trade_id join): BULL_CROSS=131, CAUTIOUS_BEAR=32, CAUTIOUS_BULL=27,
BEAR_CROSS=3. Isotonic regression (sklearn.isotonic.IsotonicRegression,
available in this venv) fits a monotonic step function and needs enough
points per step to distinguish signal from noise -- even the largest
bucket here (131) would only support ~5-6 reliable steps under a
reasonable per-step minimum, and BEAR_CROSS's 3 points can't support any
statistical fit at all, isotonic or otherwise. Simple fixed-width binning
(0.1-wide) is more transparent and auditable for a capital-sizing input
than a monotonic fit that would be substantially noise in the thin
regimes -- same preference this codebase already shows elsewhere (e.g.
setup_similarity_signal.py's win-rate thresholds, not a fitted curve).
Revisit if/when per-regime sample sizes grow an order of magnitude (see
docs/XO_BACKLOG.md's dataset-exporter item, which will grow this pool
going forward).

Data source: decision_audit WHERE event_type='trade_fire' joined to
trades on trade_id, restricted to engine.trades_filter.CLEAN_TRADES_WHERE
(this repo's canonical "which trades count" boundary -- excludes pre-S5
mispricing garbage and tracking-route pollution). hit = 1 if the trade's
realized P&L (corrected_pnl where pnl_basis_invalid=1, else realized_pnl)
was > 0, else 0.
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path

from engine.trades_filter import CLEAN_TRADES_WHERE

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "trader.db"

BIN_WIDTH = 0.1
BIN_EDGES = [round(0.5 + i * BIN_WIDTH, 2) for i in range(6)]  # 0.5, 0.6, ..., 1.0

# Below this many observations in a bucket, its calibrated rate is not
# trustworthy -- get_calibrated_confidence() falls back to the stated
# confidence unchanged for that (regime, bucket) rather than report a
# rate computed from a handful of trades.
MIN_BUCKET_N = 8

CACHE_TTL_S = 900  # 15 min -- decision_audit/trades change slowly relative to a scan cycle
_map_cache: dict = {"built_at": 0.0, "map": None}


def _conn(db_path: str | None = None) -> sqlite3.Connection:
    c = sqlite3.connect(str(db_path or DB_PATH), timeout=15)
    c.execute("PRAGMA busy_timeout=15000")
    c.row_factory = sqlite3.Row
    return c


def load_calibration_data(db_path: str | None = None) -> list[dict]:
    """One row per clean trade_fire event: regime, stated confidence, hit (0/1)."""
    conn = _conn(db_path)
    try:
        rows = conn.execute(f"""
            SELECT da.regime AS regime, da.confidence AS confidence,
                   CASE WHEN t.pnl_basis_invalid=1 THEN t.corrected_pnl ELSE t.realized_pnl END AS pnl
            FROM decision_audit da
            JOIN trades t ON t.id = da.trade_id
            WHERE da.event_type = 'trade_fire'
              AND da.trade_id IS NOT NULL
              AND da.confidence IS NOT NULL
              AND da.regime IS NOT NULL
              AND {CLEAN_TRADES_WHERE.replace("executed_at", "t.executed_at").replace("player_id", "t.player_id")}
        """).fetchall()
    finally:
        conn.close()
    out = []
    for r in rows:
        if r["pnl"] is None:
            continue
        out.append({"regime": r["regime"], "confidence": float(r["confidence"]), "hit": 1 if r["pnl"] > 0 else 0})
    return out


def _bucket_index(confidence: float) -> int:
    for i in range(len(BIN_EDGES) - 1):
        if BIN_EDGES[i] <= confidence < BIN_EDGES[i + 1]:
            return i
    return len(BIN_EDGES) - 2 if confidence >= BIN_EDGES[-1] else 0


def fit_binned_calibration(data: list[dict]) -> dict:
    """Returns {regime: [{lo, hi, n, hit_rate|None, avg_stated}, ...]} -- one
    entry per BIN_EDGES bucket per regime seen in the data. hit_rate is
    None (not 0.0) when n < MIN_BUCKET_N -- "not enough data" must never
    be silently indistinguishable from "0% hit rate"."""
    by_regime: dict[str, list[dict]] = {}
    regimes = sorted({d["regime"] for d in data})
    for regime in regimes:
        rows = [d for d in data if d["regime"] == regime]
        buckets = []
        for i in range(len(BIN_EDGES) - 1):
            lo, hi = BIN_EDGES[i], BIN_EDGES[i + 1]
            in_bucket = [r for r in rows if _bucket_index(r["confidence"]) == i]
            n = len(in_bucket)
            hit_rate = (sum(r["hit"] for r in in_bucket) / n) if n >= MIN_BUCKET_N else None
            avg_stated = (sum(r["confidence"] for r in in_bucket) / n) if n else None
            buckets.append({"lo": lo, "hi": hi, "n": n, "hit_rate": hit_rate, "avg_stated": avg_stated})
        by_regime[regime] = buckets
    return by_regime


def _cached_map(db_path: str | None = None) -> dict:
    now = time.time()
    if now - _map_cache["built_at"] > CACHE_TTL_S or _map_cache["map"] is None:
        data = load_calibration_data(db_path)
        _map_cache["map"] = fit_binned_calibration(data)
        _map_cache["built_at"] = now
    return _map_cache["map"]


def get_calibrated_confidence(regime: str, stated_confidence: float, db_path: str | None = None) -> float:
    """The function a future sizing gate calls (NOT wired in yet -- see
    module docstring). Returns the bucket's realized hit rate for this
    regime if the bucket has >= MIN_BUCKET_N observations; otherwise
    returns stated_confidence UNCHANGED (fails open to the raw number,
    never fabricates a rate from too little data, never silently returns
    0.0 for "unknown")."""
    cal = _cached_map(db_path)
    buckets = cal.get(regime)
    if not buckets:
        return stated_confidence
    idx = _bucket_index(stated_confidence)
    if idx >= len(buckets):
        return stated_confidence
    bucket = buckets[idx]
    if bucket["hit_rate"] is None:
        return stated_confidence
    return bucket["hit_rate"]
