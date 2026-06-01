"""PBO config matrix for the `relative_strength` setup (HM-VALIDATION-RIGOR, W0 leg 2).

WHY: the graduation gate is DSR>=0.95 AND PBO<=0.30. relative_strength clears DSR
at every horizon (n=444), but PBO over just the 2 live setups is a DEGENERATE
N=2 artifact (~0.55 coin-flip). PBO is only meaningful over a LARGE config
universe. This builds that universe for relative_strength: a parameter grid
(RS lookback x RS-percentile threshold x rebalance/hold) backtested cross-
sectionally over the live rs_rank universe, producing a T(days) x N(configs)
daily-return matrix fed to strategies.validation.cscv_pbo.

OBSERVATION-ONLY. No order path, no DB writes to sacred data. Two stages so the
heavy isolated compute runs under .venv-backtest with ONLY numpy/pandas/validation:

  Stage 1 (main .venv, has engine deps):
      python -c "from strategies.pbo_relative_strength import fetch_prices; \
                 fetch_prices('drafts/_rs_prices.pkl')"
  Stage 2 (.venv-backtest, isolated):
      .venv-backtest/bin/python3 strategies/pbo_relative_strength.py drafts/_rs_prices.pkl

The RS metric mirrors the live rs_rank definition (trailing-L return, excess vs
SPY, universe-wide percentile) so the configs are faithful variants of the real
setup, not an unrelated factor.
"""
from __future__ import annotations

import sys
import pickle

# Parameter grid — 36 configs (matches the 345-sweep's non-degenerate N).
LOOKBACKS = [20, 40, 60, 90]      # trading-day RS measurement window (rs_rank uses ~60 / 12wk)
PCT_THRESH = [70, 80, 90]         # rs_rank-style percentile floor (live filter uses 70-80)
HOLDS = [3, 5, 10]                # rebalance / hold horizon (trading days)
BENCHMARK = "SPY"


def _universe() -> list:
    """Live rs_rank universe (the symbols the real setup ranks over)."""
    import sqlite3
    import os
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    conn = sqlite3.connect(os.path.join(root, "data", "trader.db"))
    try:
        rows = conn.execute("SELECT symbol FROM rs_rank").fetchall()
    finally:
        conn.close()
    syms = sorted({r[0].upper() for r in rows if r[0]})
    return syms


def fetch_prices(out_path: str) -> None:
    """Stage 1 (main .venv): fetch 1y daily Close for universe+SPY, persist a tidy
    {symbol: [(date_iso, close), ...]} dict so Stage 2 needs no engine deps."""
    sys.path.insert(0, ".")
    from engine.market_data import get_bulk_daily_ohlcv
    syms = _universe()
    if BENCHMARK not in syms:
        syms.append(BENCHMARK)
    print(f"[fetch] {len(syms)} symbols, range=1y")
    bars = get_bulk_daily_ohlcv(syms, range_str="1y")
    out = {}
    for sym, df in bars.items():
        try:
            if df is None or len(df) == 0 or "Close" not in df.columns:
                continue
            ser = df["Close"].dropna()
            out[sym] = [(str(idx.date()) if hasattr(idx, "date") else str(idx), float(v))
                        for idx, v in ser.items()]
        except Exception:
            continue
    with open(out_path, "wb") as f:
        pickle.dump(out, f)
    print(f"[fetch] persisted {len(out)} symbols with Close history -> {out_path}")


def _build_matrix(prices: dict):
    """Stage 2 compute (numpy/pandas only). Returns (returns_matrix TxN, config_names)."""
    import numpy as np
    import pandas as pd

    # Align all symbols on a common date index.
    cols = {}
    for sym, pairs in prices.items():
        if not pairs:
            continue
        s = pd.Series({d: c for d, c in pairs})
        cols[sym] = s
    px = pd.DataFrame(cols).sort_index()
    px.index = pd.to_datetime(px.index)
    px = px.sort_index()
    if BENCHMARK not in px.columns:
        raise SystemExit(f"benchmark {BENCHMARK} missing from fetched prices")

    # Keep symbols with near-full history (drop thin listings — they distort percentile).
    min_obs = int(0.9 * len(px))
    keep = [c for c in px.columns if px[c].notna().sum() >= min_obs]
    px = px[keep].dropna(how="any")
    spy = px[BENCHMARK]
    universe = [c for c in px.columns if c != BENCHMARK]
    T = len(px)
    print(f"[build] aligned T={T} days x {len(universe)} symbols (full-history); "
          f"benchmark={BENCHMARK}")

    fwd = px[universe].pct_change().shift(-1)  # next-day return per symbol (return earned holding day t)

    matrix = {}
    max_L = max(LOOKBACKS)
    start = max_L + 1
    end = T - 1  # need t+1 for fwd
    idx = px.index[start:end]

    for L in LOOKBACKS:
        sym_ret_L = px[universe] / px[universe].shift(L) - 1.0
        spy_ret_L = spy / spy.shift(L) - 1.0
        excess = sym_ret_L.sub(spy_ret_L, axis=0)            # RS excess vs SPY (rs_rank definition)
        pct = excess.rank(axis=1, pct=True) * 100.0          # universe-wide percentile per day
        for P in PCT_THRESH:
            sel_all = pct >= P                               # boolean selection per day
            for H in HOLDS:
                held = sel_all.copy()
                # rebalance every H days: carry the last rebalance selection forward
                reb_rows = list(range(0, T))
                last = None
                heldvals = held.values
                for i in reb_rows:
                    if (i % H) == 0 or last is None:
                        last = heldvals[i].copy()
                    else:
                        heldvals[i] = last
                held = pd.DataFrame(heldvals, index=held.index, columns=held.columns)
                # daily portfolio return = equal-weight mean fwd return of held names (cash=0 if none)
                masked = fwd.where(held)
                r = masked.mean(axis=1, skipna=True).fillna(0.0)
                col = r.iloc[start:end].values
                matrix[f"L{L}_P{P}_H{H}"] = col

    names = list(matrix.keys())
    M = np.column_stack([matrix[n] for n in names])
    return M, names, idx


def run_pbo(prices_path: str) -> dict:
    """Stage 2 (.venv-backtest): build matrix + run CSCV PBO + per-config Sharpe."""
    import numpy as np
    sys.path.insert(0, ".")
    from strategies.validation import cscv_pbo

    with open(prices_path, "rb") as f:
        prices = pickle.load(f)
    M, names, idx = _build_matrix(prices)
    T, N = M.shape

    # Annualized Sharpe per config (daily -> *sqrt(252)); for ranking/context.
    mu = M.mean(axis=0)
    sd = M.std(axis=0, ddof=1)
    sharpe = np.where(sd > 0, mu / sd * np.sqrt(252), 0.0)

    # purge/embargo ~ max hold so OOS doesn't overlap IS labels.
    res = cscv_pbo(M, n_blocks=16, purge=max(HOLDS), embargo=max(HOLDS))

    order = np.argsort(-sharpe)
    top = [(names[i], round(float(sharpe[i]), 3)) for i in order[:5]]
    bot = [(names[i], round(float(sharpe[i]), 3)) for i in order[-5:]]

    out = {
        "setup": "relative_strength",
        "T_days": int(T), "N_configs": int(N),
        "date_range": [str(idx[0].date()), str(idx[-1].date())],
        "grid": {"lookbacks": LOOKBACKS, "pct_thresh": PCT_THRESH, "holds": HOLDS},
        "pbo": res.get("pbo"), "fragile": res.get("fragile"),
        "n_splits": res.get("n_splits"), "n_blocks": res.get("n_blocks"),
        "median_logit": res.get("median_logit"),
        "purge": res.get("purge"), "embargo": res.get("embargo"),
        "sharpe_top5": top, "sharpe_bottom5": bot,
        "sharpe_median": round(float(np.median(sharpe)), 3),
        "gate_pbo_leg": ("PASS (<=0.30)" if (res.get("pbo") is not None and res["pbo"] <= 0.30)
                         else "FAIL (>0.30)" if res.get("pbo") is not None else "n/a"),
    }
    return out


if __name__ == "__main__":
    import json
    path = sys.argv[1] if len(sys.argv) > 1 else "drafts/_rs_prices.pkl"
    report = run_pbo(path)
    print(json.dumps(report, indent=2))
