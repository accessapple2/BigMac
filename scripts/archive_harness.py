#!/usr/bin/env python3
"""archive_harness.py — PSR/DSR per strategy across archived seasons, stdlib-only.

HM-VALIDATION-RIGOR (rebuild, 2026-09-09). The original `archive_harness.py`
this ticket references does not exist anywhere -- not in this repo's git
history (any branch, including deleted files), not on bigmac's filesystem,
not on the X9 backup drive. No 2026-08-24 spec was found in the relay doc
history either (checked). This is a from-scratch build using
`strategies/validation.py`'s Deflated/Probabilistic Sharpe Ratio math
(Bailey & Lopez de Prado 2014) as the documented starting point, reimplemented
stdlib-only (no numpy/scipy) so it runs without the research venv.

Read-only: opens trader.db with `mode=ro`. Never writes anything.

Design decisions (recorded here since no external spec existed to follow):

1. PROVENANCE CEILING. Every ranked strategy carries a `ceiling` tag capping
   how much its DSR/PSR should be trusted, independent of whether a number
   was *computable*:
     - "INSUFFICIENT_N"   n < min_n (default 5) -- DSR/PSR withheld (None),
                          never silently reported on a near-meaningless sample
     - "OPTIONS_EXCLUDED" some of this strategy's option closes this season
                          were excluded as price-implausible (see #3) --
                          numbers shown, but scoped to what survived
     - "OK"               no known data-quality flag
   The ceiling is always present in the output, never silently omitted.

2. HARD REFUSAL WITHOUT trials_tested. `rank_strategies()` requires
   `trials_tested` as a mandatory keyword-only argument -- omitting it is a
   TypeError, not a silent `len(rows)` fallback (the exact undercounting
   failure mode `strategies/validation.py`'s own docstring warns about:
   "Undercount N => DSR is itself inflated"). Passing a `trials_tested`
   smaller than the number of strategies actually being ranked raises
   ValueError -- you cannot rank N strategies while claiming fewer than N
   trials were tested.

3. OPTIONS HANDLING. Verified 2026-09-09 (see
   `data/reports/relay/relay_2026-09-09_plutus-v1-archaeology-and-season-dsr.md`):
   `trades` rows with `asset_type='option'` can have `entry_price`=premium
   but `price`=the UNDERLYING's price at exit (a confirmed RECORDING bug --
   the same corrupted number is also baked into that row's `reasoning` text,
   e.g. "+2047.1%", proving it happened at write time, not a read-side
   misparse). Empirically isolated to season 1 (27 of 31 option closes, 2
   players) via an exhaustive per-season sweep -- seasons 2-7 do not show
   the pattern. This harness computes an entry/exit ratio per option close
   and EXCLUDES any where `price > option_ratio_ceiling * entry_price`
   (default 15x) -- chosen because `engine/paper_trader.py`'s own
   sanity-check comment states "options have legitimate 5-10x swings", so
   15x has headroom above the documented legitimate range while still
   catching the observed 20-30x corruption. Excluded rows are counted, never
   silently dropped. Multi-leg spread strategies (`options_trades` table,
   different schema: entry_credit_debit/exit_credit_debit) are out of scope
   for this pass.

4. NO NAIVE COMPOUNDING. Per-trade returns are never compounded (no
   cumulative product) -- a multi-position book with partial exits is not
   one serially-compounding account, and naive compounding produced absurd
   (10^8 %) "total return" figures during the 2026-09-09 season sweep that
   predates this file. Reports arithmetic mean/median return and Sharpe.

5. stdlib only. Normal CDF/inverse-CDF via `statistics.NormalDist` (stdlib
   since Python 3.8), not `scipy.stats.norm`. Skewness/kurtosis via raw
   population moments (matches scipy.stats' default `bias=True` convention
   -- cross-validated against `strategies/validation.py`'s own frozen golden
   test in tests/test_archive_harness.py).

PBO/CSCV (Probability of Backtest Overfitting) is explicitly OUT OF SCOPE
for this rebuild -- `strategies/validation.py`'s numpy-based `cscv_pbo()`
remains the place for that; this harness is PSR/DSR only, per the ask.

Usage:
  python3 scripts/archive_harness.py --season 6 --trials-tested 15
  python3 scripts/archive_harness.py --season 3 --trials-tested 10 --min-n 3
"""
from __future__ import annotations

import argparse
import sqlite3
import statistics
import sys
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DB_PATH = REPO / "data" / "trader.db"

EULER = 0.5772156649015329
DSR_GRADUATE = 0.95
MIN_N_DEFAULT = 5
OPTION_RATIO_CEILING_DEFAULT = 15.0

_STANDARD_NORMAL = statistics.NormalDist(0.0, 1.0)


def norm_cdf(x: float) -> float:
    return _STANDARD_NORMAL.cdf(x)


def norm_ppf(p: float) -> float:
    return _STANDARD_NORMAL.inv_cdf(p)


# ============================================================================
# Moments — population convention (bias=True), matches scipy.stats' default
# ============================================================================
def _moments(returns: list[float]) -> tuple[float, float, float, float]:
    n = len(returns)
    mean = sum(returns) / n
    m2 = sum((v - mean) ** 2 for v in returns) / n
    m3 = sum((v - mean) ** 3 for v in returns) / n
    m4 = sum((v - mean) ** 4 for v in returns) / n
    return mean, m2, m3, m4


def skewness(returns: list[float]) -> float:
    if len(returns) < 3:
        return 0.0
    _, m2, m3, _ = _moments(returns)
    if m2 <= 0:
        return 0.0
    return m3 / (m2 ** 1.5)


def excess_kurtosis_plus3(returns: list[float]) -> float:
    """Kurtosis with fisher=False convention: a normal distribution == 3.0."""
    if len(returns) < 4:
        return 3.0
    _, m2, _, m4 = _moments(returns)
    if m2 <= 0:
        return 3.0
    return m4 / (m2 ** 2)


def sharpe_per_trade(returns: list[float]) -> float:
    if len(returns) < 2:
        return 0.0
    mean = sum(returns) / len(returns)
    sd = statistics.stdev(returns)  # sample stdev (ddof=1), matches np.std(ddof=1)
    return mean / sd if sd > 0 else 0.0


# ============================================================================
# Deflated / Probabilistic Sharpe Ratio (Bailey & Lopez de Prado 2014)
# ============================================================================
def expected_max_sharpe(sharpes: list[float], trials_tested: int) -> tuple[float, float, int]:
    """SR0 = expected max Sharpe under the null across `trials_tested` trials.

    `trials_tested` MUST be >= the number of Sharpes actually supplied — you
    cannot claim fewer trials were tested than you have results for; that is
    exactly the undercounting that inflates DSR. Raises ValueError otherwise.
    """
    finite = [s for s in sharpes if s == s and s not in (float("inf"), float("-inf"))]
    if trials_tested < len(finite):
        raise ValueError(
            f"trials_tested={trials_tested} is less than the {len(finite)} "
            "strategies actually being ranked — can't test fewer trials than "
            "you have results for (undercounting N inflates DSR)."
        )
    n = trials_tested
    if len(finite) > 1:
        mean = sum(finite) / len(finite)
        var = sum((s - mean) ** 2 for s in finite) / (len(finite) - 1)
    else:
        var = 0.0
    if var <= 0 or n < 2:
        return 0.0, var, n
    z1 = norm_ppf(1.0 - 1.0 / n)
    z2 = norm_ppf(1.0 - 1.0 / (n * 2.718281828459045))
    sr0 = (var ** 0.5) * ((1.0 - EULER) * z1 + EULER * z2)
    return sr0, var, n


def deflated_sharpe(sr_hat: float, T: int | None, skew: float = 0.0,
                    kurt: float = 3.0, sr0: float = 0.0) -> float | None:
    """DSR = P(true Sharpe > sr0 | sr_hat observed over T obs). None if
    undefined (T too small or a degenerate variance adjustment) — never NaN,
    so a caller can't accidentally propagate a silent not-a-number."""
    if T is None or T < 2:
        return None
    var_adj = 1.0 - skew * sr_hat + ((kurt - 1.0) / 4.0) * (sr_hat ** 2)
    if var_adj <= 0:
        return None
    z = (sr_hat - sr0) * ((T - 1) ** 0.5) / (var_adj ** 0.5)
    return norm_cdf(z)


def probabilistic_sharpe(sr_hat: float, T: int | None, skew: float = 0.0,
                         kurt: float = 3.0, sr_bench: float = 0.0) -> float | None:
    """PSR = P(true Sharpe > sr_bench). DSR with an explicit benchmark."""
    return deflated_sharpe(sr_hat, T, skew, kurt, sr_bench)


# ============================================================================
# Data loading — read-only, options handled per design note #3
# ============================================================================
@dataclass
class LoadResult:
    player_id: str
    season: int
    returns: list[float] = field(default_factory=list)
    n_stock: int = 0
    n_options_kept: int = 0
    n_options_excluded: int = 0


def _stock_returns(conn: sqlite3.Connection, season: int, player_id: str) -> list[float]:
    rows = conn.execute(
        "SELECT entry_price, price FROM trades "
        "WHERE season=? AND player_id=? AND action='SELL' AND asset_type='stock' "
        "  AND realized_pnl IS NOT NULL AND entry_price IS NOT NULL AND entry_price > 0",
        (season, player_id),
    ).fetchall()
    return [(exit_p - entry_p) / entry_p for entry_p, exit_p in rows]


def _option_returns(conn: sqlite3.Connection, season: int, player_id: str,
                    ratio_ceiling: float) -> tuple[list[float], int, int]:
    """Returns (returns, n_kept, n_excluded). Excludes any close where the
    exit/entry ratio exceeds `ratio_ceiling` — see design note #3."""
    rows = conn.execute(
        "SELECT entry_price, price FROM trades "
        "WHERE season=? AND player_id=? AND action='SELL' AND asset_type='option' "
        "  AND realized_pnl IS NOT NULL AND entry_price IS NOT NULL AND entry_price > 0",
        (season, player_id),
    ).fetchall()
    kept, excluded = [], 0
    for entry_p, exit_p in rows:
        if exit_p > ratio_ceiling * entry_p:
            excluded += 1
            continue
        kept.append((exit_p - entry_p) / entry_p)
    return kept, len(kept), excluded


def load_strategy_returns(conn: sqlite3.Connection, season: int, player_id: str,
                          option_ratio_ceiling: float = OPTION_RATIO_CEILING_DEFAULT) -> LoadResult:
    stock = _stock_returns(conn, season, player_id)
    opt_returns, n_opt_kept, n_opt_excluded = _option_returns(
        conn, season, player_id, option_ratio_ceiling
    )
    return LoadResult(
        player_id=player_id, season=season,
        returns=stock + opt_returns,
        n_stock=len(stock), n_options_kept=n_opt_kept, n_options_excluded=n_opt_excluded,
    )


def list_players(conn: sqlite3.Connection, season: int) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT player_id FROM trades WHERE season=? AND action='SELL' "
        "  AND realized_pnl IS NOT NULL",
        (season,),
    ).fetchall()
    return sorted(r[0] for r in rows)


# ============================================================================
# Ranking — the provenance ceiling + hard-refusal contract live here
# ============================================================================
@dataclass
class StrategyResult:
    player_id: str
    season: int
    n: int
    raw_sharpe: float
    mean_return_pct: float | None
    median_return_pct: float | None
    win_rate_pct: float | None
    psr: float | None
    dsr: float | None
    ceiling: str
    n_options_excluded: int


def rank_strategies(conn: sqlite3.Connection, season: int, *, trials_tested: int,
                    min_n: int = MIN_N_DEFAULT,
                    option_ratio_ceiling: float = OPTION_RATIO_CEILING_DEFAULT) -> dict:
    """Rank every strategy (player_id) active in `season` by DSR.

    `trials_tested` is mandatory (see design note #2) — a caller that omits
    it gets a TypeError before this function body even runs; a caller that
    passes a value smaller than the number of strategies found gets a
    ValueError from `expected_max_sharpe` below.
    """
    players = list_players(conn, season)
    loaded = [load_strategy_returns(conn, season, p, option_ratio_ceiling) for p in players]

    sharpes = [sharpe_per_trade(lr.returns) for lr in loaded if len(lr.returns) >= min_n]
    sr0, var_null, n_trials = expected_max_sharpe(sharpes, trials_tested)

    results: list[StrategyResult] = []
    for lr in loaded:
        n = len(lr.returns)
        if n < min_n:
            results.append(StrategyResult(
                player_id=lr.player_id, season=season, n=n, raw_sharpe=0.0,
                mean_return_pct=None, median_return_pct=None, win_rate_pct=None,
                psr=None, dsr=None, ceiling="INSUFFICIENT_N",
                n_options_excluded=lr.n_options_excluded,
            ))
            continue
        raw_sr = sharpe_per_trade(lr.returns)
        skew = skewness(lr.returns)
        kurt = excess_kurtosis_plus3(lr.returns)
        psr = probabilistic_sharpe(raw_sr, n, skew=skew, kurt=kurt, sr_bench=0.0)
        dsr = deflated_sharpe(raw_sr, n, skew=skew, kurt=kurt, sr0=sr0)
        wins = sum(1 for r in lr.returns if r > 0)
        ceiling = "OPTIONS_EXCLUDED" if lr.n_options_excluded > 0 else "OK"
        results.append(StrategyResult(
            player_id=lr.player_id, season=season, n=n,
            raw_sharpe=round(raw_sr, 4),
            mean_return_pct=round(statistics.mean(lr.returns) * 100, 3),
            median_return_pct=round(statistics.median(lr.returns) * 100, 3),
            win_rate_pct=round(100 * wins / n, 1),
            psr=(round(psr, 4) if psr is not None else None),
            dsr=(round(dsr, 4) if dsr is not None else None),
            ceiling=ceiling,
            n_options_excluded=lr.n_options_excluded,
        ))

    results.sort(key=lambda r: (r.dsr if r.dsr is not None else -1), reverse=True)
    return {
        "season": season,
        "trials_tested": n_trials,
        "sr0_null": round(sr0, 4),
        "min_n": min_n,
        "option_ratio_ceiling": option_ratio_ceiling,
        "results": results,
    }


def format_report(report: dict) -> str:
    lines = [
        f"season {report['season']}  trials_tested={report['trials_tested']}  "
        f"SR0(null)={report['sr0_null']}  min_n={report['min_n']}"
    ]
    for r in report["results"]:
        dsr = f"{r.dsr:.4f}" if r.dsr is not None else "  n/a "
        psr = f"{r.psr:.4f}" if r.psr is not None else "  n/a "
        lines.append(
            f"  {r.player_id[:22]:22s} n={r.n:4d}  rawSR={r.raw_sharpe:+7.3f}  "
            f"PSR={psr}  DSR={dsr}  [{r.ceiling}]"
            + (f"  ({r.n_options_excluded} options excluded)" if r.n_options_excluded else "")
        )
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--trials-tested", type=int, required=True,
                    help="how many strategies/configs were compared this season — "
                         "mandatory, no default (see design note #2)")
    ap.add_argument("--min-n", type=int, default=MIN_N_DEFAULT)
    ap.add_argument("--option-ratio-ceiling", type=float, default=OPTION_RATIO_CEILING_DEFAULT)
    ap.add_argument("--db", default=str(DB_PATH))
    args = ap.parse_args()

    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    try:
        report = rank_strategies(
            conn, args.season, trials_tested=args.trials_tested,
            min_n=args.min_n, option_ratio_ceiling=args.option_ratio_ceiling,
        )
    finally:
        conn.close()
    print(format_report(report))


if __name__ == "__main__":
    main()
