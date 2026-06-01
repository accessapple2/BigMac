"""Decorrelated PBO config grid for `relative_strength` (HM-VALIDATION-RIGOR).

The first grid (strategies/pbo_relative_strength.py) varied only lookback × percentile ×
hold — 36 near-duplicate variants of ONE cross-sectional RS-momentum signal, so the
config return streams were ~collinear and PBO sat near 0.5 by construction. That makes
0.48 an unreliable fragility read.

This grid varies the REAL decision axes so the configs make genuinely different bets:
  - lookback window      L  ∈ {40, 60, 90}
  - RS-rank threshold    P  ∈ {70, 85}
  - ENTRY TRIGGER        E  ∈ {level, breakout, trend, accel}   <- primary decorrelator
  - holding horizon      H  ∈ {3, 10}
  - UNIVERSE SLICE       U  ∈ {all, liquid, illiquid}           <- primary decorrelator
= 144 configs. Entry-trigger changes the SIGNAL MATH (level vs onset vs trend-confirmed
vs acceleration → different names/timing); universe-slice changes WHICH names (RS ranked
within each slice → independent portfolios). Both genuinely decorrelate the streams.

We measure mean pairwise |correlation| of the streams and compare to the old grid — if it
doesn't drop, the new PBO is no more trustworthy than 0.48, so we report that honestly.

OBSERVATION-ONLY. No order path, no graduation, no sacred-data writes. Two stages:
  Stage 1 (.venv):           python -c "from strategies.pbo_rs_decorrelated import fetch; fetch('drafts/_rs_ohlcv.pkl')"
  Stage 2 (.venv-backtest):  .venv-backtest/bin/python3 strategies/pbo_rs_decorrelated.py drafts/_rs_ohlcv.pkl
"""
from __future__ import annotations

import sys
import pickle

LOOKBACKS = [40, 60, 90]
PCT       = [70, 85]
TRIGGERS  = ["level", "breakout", "trend", "accel"]
HOLDS     = [3, 10]
SLICES    = ["all", "liquid", "illiquid"]
BENCHMARK = "SPY"
MIN_NONZERO_FRAC = 0.05   # drop configs whose return stream is >95% cash (too sparse to rank)


def _universe():
    import sqlite3, os
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    conn = sqlite3.connect(os.path.join(root, "data", "trader.db"))
    try:
        rows = conn.execute("SELECT symbol FROM rs_rank").fetchall()
    finally:
        conn.close()
    return sorted({r[0].upper() for r in rows if r[0]})


def fetch(out_path: str) -> None:
    """Stage 1 (.venv): persist {symbol: [(date, close, volume), ...]} (Close+Volume
    so Stage 2 can build liquidity slices). 1y daily bars."""
    sys.path.insert(0, ".")
    from engine.market_data import get_bulk_daily_ohlcv
    syms = _universe()
    if BENCHMARK not in syms:
        syms.append(BENCHMARK)
    print(f"[fetch] {len(syms)} symbols, range=1y (OHLCV)")
    bars = get_bulk_daily_ohlcv(syms, range_str="1y")
    out = {}
    for sym, df in bars.items():
        try:
            if df is None or len(df) == 0 or "Close" not in df.columns:
                continue
            sub = df[["Close", "Volume"]].dropna()
            out[sym] = [(str(idx.date()) if hasattr(idx, "date") else str(idx),
                         float(c), float(v)) for idx, c, v in
                        zip(sub.index, sub["Close"], sub["Volume"])]
        except Exception:
            continue
    with open(out_path, "wb") as f:
        pickle.dump(out, f)
    print(f"[fetch] persisted {len(out)} symbols -> {out_path}")


def _frames(prices):
    """Return aligned (close_df, vol_df, spy_series, universe_list)."""
    import pandas as pd
    closes, vols = {}, {}
    for sym, rows in prices.items():
        if not rows:
            continue
        # rows may be (date,close) [old] or (date,close,volume) [new]
        closes[sym] = pd.Series({r[0]: r[1] for r in rows})
        if len(rows[0]) >= 3:
            vols[sym] = pd.Series({r[0]: r[2] for r in rows})
    px = pd.DataFrame(closes).sort_index()
    px.index = pd.to_datetime(px.index)
    px = px.sort_index()
    min_obs = int(0.9 * len(px))
    keep = [c for c in px.columns if px[c].notna().sum() >= min_obs]
    px = px[keep].dropna(how="any")
    vol = pd.DataFrame({k: v for k, v in vols.items() if k in keep})
    if not vol.empty:
        vol.index = pd.to_datetime(vol.index)
        vol = vol.reindex(px.index).ffill()
    return px, vol


def _slice_members(universe, px, vol, which):
    """Partition universe by trailing dollar-volume (liquid = top half)."""
    if which == "all" or vol is None or vol.empty:
        return list(universe)
    import numpy as np
    dollar = (px[universe] * vol[universe]).mean(axis=0)
    med = float(np.nanmedian(dollar.values))
    if which == "liquid":
        return [s for s in universe if dollar.get(s, 0) >= med]
    return [s for s in universe if dollar.get(s, 0) < med]


def build_matrix(prices):
    """Return (M TxN, names, idx, members_by_slice). The decorrelated grid."""
    import numpy as np
    import pandas as pd
    px, vol = _frames(prices)
    if BENCHMARK not in px.columns:
        raise SystemExit("benchmark missing")
    spy = px[BENCHMARK]
    universe = [c for c in px.columns if c != BENCHMARK]
    T = len(px)
    max_L = max(LOOKBACKS)
    start, end = max_L + 1, T - 1
    idx = px.index[start:end]
    fwd_all = px[universe].pct_change().shift(-1)

    cols = {}
    members = {u: _slice_members(universe, px, vol, u) for u in SLICES}
    for U in SLICES:
        members_u = members[U]
        if len(members_u) < 10:
            continue
        sub = px[members_u]
        fwd = fwd_all[members_u]
        for L in LOOKBACKS:
            sym_ret = sub / sub.shift(L) - 1.0
            spy_ret = spy / spy.shift(L) - 1.0
            excess = sym_ret.sub(spy_ret, axis=0)
            pct = excess.rank(axis=1, pct=True) * 100.0
            sma = sub.rolling(L).mean()
            for P in PCT:
                base = pct >= P
                trig = {
                    "level":    base,
                    "breakout": base & (pct.shift(1) < P),
                    "trend":    base & (sub > sma),
                    "accel":    base & (excess > excess.shift(max(1, L // 2))),
                }
                for E in TRIGGERS:
                    sel = trig[E].fillna(False)
                    for H in HOLDS:
                        held = sel.copy().values
                        last = None
                        for i in range(T):
                            if (i % H) == 0 or last is None:
                                last = held[i].copy()
                            else:
                                held[i] = last
                        helddf = pd.DataFrame(held, index=sel.index, columns=sel.columns)
                        r = fwd.where(helddf).mean(axis=1, skipna=True).fillna(0.0)
                        series = r.iloc[start:end].values
                        if np.mean(series != 0) >= MIN_NONZERO_FRAC and np.std(series) > 0:
                            cols[f"U{U}_L{L}_P{P}_{E}_H{H}"] = series
    names = list(cols.keys())
    M = np.column_stack([cols[n] for n in names])
    return M, names, idx, {u: len(members[u]) for u in SLICES}


def _mean_abs_corr(M):
    import numpy as np
    if M.shape[1] < 2:
        return None
    C = np.corrcoef(M, rowvar=False)
    iu = np.triu_indices_from(C, k=1)
    vals = np.abs(C[iu])
    vals = vals[np.isfinite(vals)]
    return float(np.mean(vals)) if len(vals) else None


def run(prices_path):
    import numpy as np
    sys.path.insert(0, ".")
    from strategies.validation import cscv_pbo
    with open(prices_path, "rb") as f:
        prices = pickle.load(f)

    M, names, idx, slice_sizes = build_matrix(prices)
    T, N = M.shape
    new_corr = _mean_abs_corr(M)

    # Contrast: old collinear grid's mean correlation, same data. The old builder
    # expects (date, close) pairs — strip volume from the new (date, close, vol) rows.
    old_corr = None
    try:
        from strategies.pbo_relative_strength import _build_matrix as _old_build
        prices_close = {s: [(r[0], r[1]) for r in rows] for s, rows in prices.items() if rows}
        oldM, _, _ = _old_build(prices_close)
        old_corr = _mean_abs_corr(oldM)
    except Exception as e:
        old_corr = f"err:{e}"

    res = cscv_pbo(M, n_blocks=16, purge=max(HOLDS), embargo=max(HOLDS))
    mu = M.mean(axis=0); sd = M.std(axis=0, ddof=1)
    sharpe = np.where(sd > 0, mu / sd * np.sqrt(252), 0.0)
    order = np.argsort(-sharpe)
    pbo = res.get("pbo")

    decorrelated = (isinstance(old_corr, float) and new_corr is not None
                    and new_corr < old_corr - 0.10)
    if pbo is None:
        verdict = "PBO uncomputable"
    elif not decorrelated:
        verdict = ("INCONCLUSIVE — grid did not decorrelate enough (mean|corr| not "
                   "materially below the old grid); PBO no more trustworthy than 0.48")
    elif pbo >= 0.30:
        verdict = ("GENUINELY FRAGILE — PBO>=0.30 on a decorrelated grid → DO NOT graduate")
    else:
        verdict = ("0.48 WAS A COLLINEARITY ARTIFACT — PBO<0.30 on a decorrelated grid → "
                   "forward OOS shadow is the final arbiter")

    return {
        "setup": "relative_strength (decorrelated grid)",
        "T_days": int(T), "N_configs": int(N),
        "date_range": [str(idx[0].date()), str(idx[-1].date())],
        "axes": {"lookbacks": LOOKBACKS, "pct": PCT, "triggers": TRIGGERS,
                 "holds": HOLDS, "slices": SLICES},
        "slice_sizes": slice_sizes,
        "mean_abs_corr_new": round(new_corr, 4) if new_corr is not None else None,
        "mean_abs_corr_old_grid": round(old_corr, 4) if isinstance(old_corr, float) else old_corr,
        "decorrelated": bool(decorrelated),
        "pbo": pbo, "fragile": res.get("fragile"),
        "n_splits": res.get("n_splits"), "median_logit": res.get("median_logit"),
        "purge": res.get("purge"), "embargo": res.get("embargo"),
        "sharpe_median": round(float(np.median(sharpe)), 3),
        "sharpe_top5": [(names[i], round(float(sharpe[i]), 3)) for i in order[:5]],
        "verdict": verdict,
    }


if __name__ == "__main__":
    import json
    path = sys.argv[1] if len(sys.argv) > 1 else "drafts/_rs_ohlcv.pkl"
    print(json.dumps(run(path), indent=2))
