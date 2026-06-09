"""engine/filter_contribution.py — HM-DRYDOCK #4 leave-one-out (ablation) filter contribution.

Bounded, OFFLINE-scheduled per-filter ablation: for a capped universe over a fixed 90d window, for
every (symbol, day) candidate compute which entry filters pass, then measure forward FWD_DAYS return.
Contribution of filter X = leave-one-out diff = mean_fwd(ALL filters pass) − mean_fwd(all-but-X pass).
A positive value = X screens out weaker entries (improves the cohort's forward return).

HONEST SCOPE: this is a leave-one-out ABLATION on signal forward-return — NOT a full position-sim
backtest and NOT a Shapley decomposition (filters interact; toggle-off diffs don't sum to the whole).
Directional, not exact. Labeled as such in the UI.

RESOURCE SAFETY (per Admiral): capped universe + fixed window + per-sweep wall-clock cap; runs OFFLINE
off-peak (cron, NOT during the 05:45–06:05 morning cadence or market hours) → writes a cache JSON. The
endpoint only ever READS the cache; it never triggers a live sweep.
"""
from __future__ import annotations

import os
import json
import time
import logging

logger = logging.getLogger(__name__)

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_PATH = os.path.join(_ROOT, "data", "filter_contribution.json")

CAP_UNIVERSE = 40        # bound: at most N symbols
WINDOW_DAYS  = 90        # fixed lookback window
FWD_DAYS     = 5         # forward-return horizon
SWEEP_TIMEOUT_S = 90     # hard wall-clock cap; partial result cached if exceeded


def _universe() -> list:
    """Capped universe: watchlist + a slice of scan_universe, deduped, bounded."""
    syms: list = []
    try:
        import sqlite3
        db = os.path.join(_ROOT, "data", "trader.db")
        c = sqlite3.connect(db, timeout=10)
        syms += [r[0] for r in c.execute("SELECT symbol FROM watchlist WHERE is_active=1").fetchall()]
        syms += [r[0] for r in c.execute(
            "SELECT symbol FROM scan_universe ORDER BY avg_volume DESC LIMIT 60").fetchall()]
        c.close()
    except Exception as e:
        logger.warning("filter_contribution universe load failed: %s", e)
    seen, out = set(), []
    for s in syms:
        s = (s or "").upper()
        if s and s not in seen:
            seen.add(s); out.append(s)
        if len(out) >= CAP_UNIVERSE:
            break
    return out


# ── filters (computable from daily bars + the rs_rank table) ─────────────────
def _sma(vals, n):
    return sum(vals[-n:]) / n if len(vals) >= n else None


def _filters_for(highs, lows, closes, vols, i, rs_rank):
    """Return {filter_name: bool} for the candidate at bar index i. i must leave FWD_DAYS ahead."""
    c = closes[: i + 1]
    v = vols[: i + 1]
    f = {}
    sma50 = _sma(c, 50)
    f["trend_sma50"] = bool(sma50 and c[-1] > sma50)                       # above 50-day
    f["momentum_20d"] = bool(len(c) >= 21 and (c[-1] / c[-21] - 1) > 0)     # positive 20d
    f["volume_surge"] = bool(len(v) >= 21 and v[-1] > (sum(v[-21:-1]) / 20) * 1.2)  # >1.2x 20d avg
    f["not_extended"] = bool(sma50 and c[-1] < sma50 * 1.20)               # not >20% over 50MA (avoid chasing)
    f["rs_leader"] = bool(rs_rank is not None and rs_rank >= 70)            # RS rank >=70 (observation-grade)
    return f


def run_sweep() -> dict:
    """Run the bounded ablation. Returns the result dict and writes it to CACHE_PATH."""
    t0 = time.time()
    from engine import market_adapter
    from engine.market_calendar import az_now
    universe = _universe()
    bars, src = market_adapter.bulk_daily_ohlcv(universe)
    # rs_rank lookup (observation-grade; read-only)
    rs_map = {}
    try:
        import sqlite3
        c = sqlite3.connect(os.path.join(_ROOT, "data", "trader.db"), timeout=10)
        qs = ",".join("?" * len(universe))
        rs_map = {s.upper(): v for s, v in c.execute(
            f"SELECT symbol, rs_rank_blended FROM rs_rank WHERE symbol IN ({qs})", tuple(universe)
        ).fetchall() if v is not None}
        c.close()
    except Exception:
        pass

    filt_names = ["trend_sma50", "momentum_20d", "volume_surge", "not_extended", "rs_leader"]
    all_pass_fwd, leave_out_fwd = [], {f: [] for f in filt_names}
    samples = 0
    timed_out = False

    for sym, df in bars.items():
        if time.time() - t0 > SWEEP_TIMEOUT_S:
            timed_out = True
            break
        try:
            if df is None or getattr(df, "empty", True) or len(df) < 60:
                continue
            highs = list(df["High"].values)[-WINDOW_DAYS - FWD_DAYS:]
            lows = list(df["Low"].values)[-WINDOW_DAYS - FWD_DAYS:]
            closes = list(df["Close"].values)[-WINDOW_DAYS - FWD_DAYS:]
            vols = list(df["Volume"].values)[-WINDOW_DAYS - FWD_DAYS:]
            rs = rs_map.get(sym.upper())
            for i in range(50, len(closes) - FWD_DAYS):     # need 50 bars history + FWD ahead
                f = _filters_for(highs, lows, closes, vols, i, rs)
                fwd = (closes[i + FWD_DAYS] / closes[i] - 1) * 100.0
                if all(f.values()):
                    all_pass_fwd.append(fwd)
                # leave-one-out cohorts: all pass EXCEPT exactly the relaxed one
                for fx in filt_names:
                    if all(v for k, v in f.items() if k != fx):
                        leave_out_fwd[fx].append(fwd)
                samples += 1
        except Exception:
            continue

    def _mean(xs):
        return round(sum(xs) / len(xs), 3) if xs else None

    base = _mean(all_pass_fwd)
    contrib = []
    for fx in filt_names:
        lo = _mean(leave_out_fwd[fx])
        c_val = round(base - lo, 3) if (base is not None and lo is not None) else None
        contrib.append({"filter": fx, "leave_out_fwd_pct": lo, "contribution_pct": c_val,
                        "leave_out_n": len(leave_out_fwd[fx])})
    contrib.sort(key=lambda r: (r["contribution_pct"] is not None, r["contribution_pct"] or 0), reverse=True)

    result = {
        "computed_at": az_now().strftime("%Y-%m-%d %H:%M:%S AZ"),
        "method": "leave-one-out ablation (forward-return); NOT a position-sim backtest or Shapley",
        "window_days": WINDOW_DAYS, "fwd_days": FWD_DAYS,
        "universe_n": len(universe), "samples": samples,
        "data_source": src, "timed_out": timed_out,
        "baseline_all_pass_fwd_pct": base, "baseline_n": len(all_pass_fwd),
        "contributions": contrib,
    }
    try:
        with open(CACHE_PATH, "w") as fh:
            json.dump(result, fh, indent=2, default=str)
    except Exception as e:
        logger.warning("filter_contribution cache write failed: %s", e)
    return result


def get_cached() -> dict:
    """READ-ONLY: serve the last offline sweep. Never triggers a live sweep."""
    try:
        if os.path.exists(CACHE_PATH):
            with open(CACHE_PATH) as fh:
                return json.load(fh)
    except Exception as e:
        logger.warning("filter_contribution cache read failed: %s", e)
    return {"unavailable": True, "message": "No sweep cached yet — runs off-peak (02:30 AZ)."}


if __name__ == "__main__":
    import sys
    if "--sweep" in sys.argv:
        r = run_sweep()
        print(json.dumps(r, indent=2, default=str))
    else:
        print(json.dumps(get_cached(), indent=2, default=str))
