"""strategies/validation.py — HM-VALIDATION-RIGOR: honest metrics as a
first-class harness capability.

Promotes the HM-BACKTEST-123 post-hoc `dsr.py` to a first-class module and adds:
  - TRIAL-COUNT tracking (the piece the literature flags hardest) — feeds N into
    the Deflated Sharpe Ratio.
  - A ranking-report layer so raw Sharpe is NEVER shown alone: every ranking
    point calls `deflate_ranking()` and prints DSR beside raw SR.
  - The graduation GATE (report-only; Admiral enforces): DSR >= 0.95 AND PBO <= 0.3.
  - Guardrail constants (t-stat 3.0, slippage stress, P95-DD sizing).
  - Phase B: Combinatorial Purged CV -> Probability of Backtest Overfitting (PBO).

DSR math is the Bailey & López de Prado (2014) implementation, promoted verbatim
from `logs/hm_backtest_123/dsr.py` (proven against HM-BACKTEST-123). UNIT-TESTED
to reproduce the_continuation OOS DSR = 0.8695 (see tests/test_validation.py).

UNITS CONTRACT (unchanged): all Sharpes passed to deflated_sharpe()/expected_max_sharpe()
MUST be in the SAME per-observation units as T (per-trade SR with T=#trades, OR
per-bar/per-day SR with T=#bars/#days). Never mix annualized SR with a trade-count T.

Research-only. No live-trader calls, no DB writes.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

import numpy as np
from scipy.stats import norm

EULER = 0.5772156649015329

# ── Guardrails (HM-VALIDATION-RIGOR) ───────────────────────────────────────
T_STAT_MIN = 3.0                 # not 2.0 — minimum |t| on mean return to consider
SLIPPAGE_STRESS_RT = (0.001, 0.003)   # 0.1%–0.3% round-trip slippage stress band
DSR_GRADUATE = 0.95              # graduate only at DSR >= 0.95 ...
PBO_GRADUATE = 0.30              # ... AND PBO <= 0.30
PBO_FRAGILE = 0.30               # PBO above this => flag FRAGILE


# ════════════════════════════════════════════════════════════════════════════
# Deflated Sharpe Ratio (Bailey & López de Prado 2014) — promoted verbatim
# ════════════════════════════════════════════════════════════════════════════
def expected_max_sharpe(sharpe_trials, n_trials: int | None = None):
    """Expected maximum Sharpe under the null (no skill) across N independent trials.

    Returns (SR0, variance_of_trial_sharpes, N). Sharpes in whatever units you pass.
    """
    arr = np.asarray([s for s in sharpe_trials if np.isfinite(s)], dtype=float)
    N = int(n_trials if n_trials is not None else len(arr))
    if len(arr) > 1:
        V = float(np.var(arr, ddof=1))
    else:
        V = 0.0
    if V <= 0 or N < 2:
        return 0.0, V, N
    z1 = norm.ppf(1.0 - 1.0 / N)
    z2 = norm.ppf(1.0 - 1.0 / (N * np.e))
    sr0 = np.sqrt(V) * ((1.0 - EULER) * z1 + EULER * z2)
    return float(sr0), V, N


def deflated_sharpe(sr_hat: float, T: int, skew: float = 0.0,
                    kurt: float = 3.0, sr0: float = 0.0) -> float:
    """DSR = probability the true Sharpe > sr0, given sr_hat over T obs.

    sr_hat, sr0 in PER-OBSERVATION units. T = number of return observations.
    skew/kurt of the return stream (kurt=3 => normal). Returns prob in [0,1].
    """
    if T is None or T < 2:
        return float("nan")
    var_adj = 1.0 - skew * sr_hat + ((kurt - 1.0) / 4.0) * (sr_hat ** 2)
    if var_adj <= 0:
        return float("nan")
    z = (sr_hat - sr0) * np.sqrt(T - 1) / np.sqrt(var_adj)
    return float(norm.cdf(z))


def probabilistic_sharpe(sr_hat: float, T: int, skew: float = 0.0,
                         kurt: float = 3.0, sr_bench: float = 0.0) -> float:
    """PSR = P(true SR > sr_bench). DSR with explicit benchmark."""
    return deflated_sharpe(sr_hat, T, skew, kurt, sr_bench)


def trade_metrics(returns) -> dict:
    """Per-trade-return metrics. returns = iterable of per-trade fractional returns."""
    r = np.asarray([x for x in returns if np.isfinite(x)], dtype=float)
    n = len(r)
    if n == 0:
        return dict(n=0)
    wins = r[r > 0]
    losses = r[r <= 0]
    eq = np.cumprod(1.0 + r)
    peak = np.maximum.accumulate(eq)
    dd = (eq - peak) / peak
    sr_pt = float(np.mean(r) / np.std(r, ddof=1)) if n > 1 and np.std(r, ddof=1) > 0 else 0.0
    from scipy.stats import skew as _sk, kurtosis as _ku
    return dict(
        n=n,
        total_return_pct=float((eq[-1] - 1.0) * 100.0),
        win_rate_pct=float(len(wins) / n * 100.0),
        profit_factor=float(wins.sum() / -losses.sum()) if losses.sum() < 0 else float("inf"),
        max_drawdown_pct=float(dd.min() * 100.0),
        sharpe_per_trade=sr_pt,
        avg_trade_pct=float(np.mean(r) * 100.0),
        skew=float(_sk(r)) if n > 2 else 0.0,
        kurtosis=float(_ku(r, fisher=False)) if n > 3 else 3.0,
    )


# ════════════════════════════════════════════════════════════════════════════
# Trial-count tracking — the piece the literature flags hardest
# ════════════════════════════════════════════════════════════════════════════
@dataclass
class TrialLog:
    """Records every variant tested in a selection so N feeds DSR honestly.

    A 'trial' is one configuration/strategy whose in-sample Sharpe you looked at
    while choosing the winner. Undercount N => DSR is itself inflated. Record ALL
    of them — parameter sweeps, symbol scans, exit variants.
    """
    label: str = "selection"
    sharpes: list = field(default_factory=list)   # per-observation-unit SR of each trial
    names: list = field(default_factory=list)

    def add(self, sharpe: float, name: str | None = None) -> None:
        self.sharpes.append(float(sharpe))
        self.names.append(name if name is not None else f"trial_{len(self.sharpes)}")

    def add_many(self, sharpes) -> None:
        for s in sharpes:
            self.add(s)

    @property
    def n_trials(self) -> int:
        return len(self.sharpes)

    def sr0(self, n_trials: int | None = None):
        """SR0 = expected max Sharpe under the null across the recorded trials."""
        return expected_max_sharpe(self.sharpes, n_trials=n_trials or self.n_trials)


# ════════════════════════════════════════════════════════════════════════════
# Ranking report — raw Sharpe NEVER shown alone
# ════════════════════════════════════════════════════════════════════════════
def deflate_ranking(trials: list, n_trials: int | None = None, pbo: float | None = None) -> dict:
    """Deflate a ranking of strategies. `trials` = list of dicts each with:
        name, sharpe (per-obs SR), T (obs count), skew, kurt
    Computes SR0 over ALL trial Sharpes, then DSR for each. Returns the ranked
    table (DSR-sorted) plus the selection verdict on the top entry.

    n_trials lets you pass a LARGER N than len(trials) when more variants were
    tested than survive to the table (honest deflation). pbo (optional, Phase B)
    is attached to the winner for the graduation gate.
    """
    rows = [t for t in trials if t.get("sharpe") is not None and np.isfinite(t["sharpe"])]
    sr0, V, N = expected_max_sharpe([t["sharpe"] for t in rows],
                                    n_trials=n_trials or len(rows))
    out = []
    for t in rows:
        dsr = deflated_sharpe(t["sharpe"], int(t.get("T", 0) or 0),
                              skew=float(t.get("skew", 0.0)),
                              kurt=float(t.get("kurt", 3.0)), sr0=sr0)
        out.append({
            "name": t.get("name", "?"),
            "raw_sharpe": round(float(t["sharpe"]), 4),
            "T": int(t.get("T", 0) or 0),
            "dsr": (round(dsr, 4) if np.isfinite(dsr) else None),
            "dsr_passes": bool(np.isfinite(dsr) and dsr >= DSR_GRADUATE),
        })
    out.sort(key=lambda x: (x["dsr"] if x["dsr"] is not None else -1), reverse=True)
    winner = out[0] if out else None
    return {
        "n_trials": N, "sr0": round(sr0, 4), "var_trials": V,
        "ranking": out,
        "winner": (graduation_verdict(winner["dsr"], pbo, name=winner["name"])
                   if winner else None),
    }


def graduation_verdict(dsr: float | None, pbo: float | None = None,
                       name: str = "strategy") -> dict:
    """Report-only graduation verdict. Graduate ONLY at DSR>=0.95 AND PBO<=0.3.
    Admiral enforces; this never auto-graduates anything."""
    reasons = []
    dsr_ok = dsr is not None and np.isfinite(dsr) and dsr >= DSR_GRADUATE
    if not dsr_ok:
        reasons.append(f"DSR {dsr if dsr is not None else 'nan'} < {DSR_GRADUATE}")
    pbo_ok = True
    if pbo is not None:
        pbo_ok = pbo <= PBO_GRADUATE
        if not pbo_ok:
            reasons.append(f"PBO {round(pbo, 3)} > {PBO_GRADUATE} (FRAGILE)")
    else:
        reasons.append("PBO not computed (Phase B)")
    graduate = dsr_ok and pbo_ok and pbo is not None
    return {
        "name": name, "dsr": dsr, "pbo": pbo,
        "dsr_passes": dsr_ok, "pbo_passes": (pbo_ok if pbo is not None else None),
        "verdict": "GRADUATE" if graduate else "HOLD",
        "reasons": reasons,
        "note": "report-only — graduation is the Admiral's call",
    }


# ════════════════════════════════════════════════════════════════════════════
# Phase B — Combinatorial Purged CV -> Probability of Backtest Overfitting (PBO)
# CSCV method (Bailey, Borwein, López de Prado, Zhu 2014). PBO > 0.30 => FRAGILE.
# ════════════════════════════════════════════════════════════════════════════
def _sharpe_cols(M: np.ndarray) -> np.ndarray:
    """Per-column (per-strategy) Sharpe over the given rows. Safe on zero-variance."""
    mu = np.nanmean(M, axis=0)
    sd = np.nanstd(M, axis=0, ddof=1)
    out = np.zeros_like(mu)
    nz = sd > 0
    out[nz] = mu[nz] / sd[nz]
    return out


def _purge_embargo_rows(rows: np.ndarray, is_block_ids: set, block_of_row: np.ndarray,
                        purge: int, embargo: int) -> np.ndarray:
    """Drop OOS rows whose labels overlap an adjacent IS block (purge) plus an
    embargo gap. `purge`/`embargo` are observation counts. Handles overlapping
    holds (the_continuation holds up to max_hold bars across block boundaries)."""
    if purge <= 0 and embargo <= 0:
        return rows
    keep = []
    for r in rows:
        # if any IS row lies within [r-purge, r+embargo], this OOS obs leaks -> drop
        lo, hi = r - purge, r + embargo
        leak = False
        for j in range(max(0, lo), min(len(block_of_row), hi + 1)):
            if block_of_row[j] in is_block_ids:
                leak = True
                break
        if not leak:
            keep.append(r)
    return np.asarray(keep, dtype=int)


def cscv_pbo(returns_matrix, n_blocks: int = 16, purge: int = 0, embargo: int = 0,
             max_splits: int = 20000) -> dict:
    """Probability of Backtest Overfitting via Combinatorial (Purged) CV.

    returns_matrix: (T observations) x (N strategies/configs) per-period returns.
    Splits T into n_blocks contiguous blocks; for every combination of n_blocks/2
    blocks as IS (complement = OOS), picks the IS-best strategy and finds its OOS
    rank. PBO = P(IS-best ranks below the OOS median) = P(logit(rank) <= 0).

    purge/embargo (obs counts) remove OOS rows whose labels overlap IS blocks —
    use ~max_hold for overlapping-hold strategies. PBO > 0.30 => FRAGILE.
    """
    from itertools import combinations
    M = np.asarray(returns_matrix, dtype=float)
    if M.ndim != 2 or M.shape[1] < 2:
        return {"pbo": None, "error": "need >=2 strategies (columns) for PBO"}
    T, N = M.shape
    if n_blocks % 2 != 0:
        n_blocks -= 1
    n_blocks = max(2, min(n_blocks, T))
    blocks = np.array_split(np.arange(T), n_blocks)
    block_of_row = np.empty(T, dtype=int)
    for bid, b in enumerate(blocks):
        block_of_row[b] = bid
    half = n_blocks // 2

    all_combos = list(combinations(range(n_blocks), half))
    sampled = False
    if len(all_combos) > max_splits:
        # deterministic stride sample (no RNG — reproducible)
        step = len(all_combos) // max_splits
        all_combos = all_combos[::step][:max_splits]
        sampled = True

    lambdas, n_oos_below = [], 0
    for is_combo in all_combos:
        is_set = set(is_combo)
        is_rows = np.concatenate([blocks[i] for i in is_combo])
        oos_rows = np.concatenate([blocks[i] for i in range(n_blocks) if i not in is_set])
        if purge or embargo:
            oos_rows = _purge_embargo_rows(oos_rows, is_set, block_of_row, purge, embargo)
        if len(oos_rows) < 2 or len(is_rows) < 2:
            continue
        is_perf = _sharpe_cols(M[is_rows])
        oos_perf = _sharpe_cols(M[oos_rows])
        n_star = int(np.argmax(is_perf))
        order = np.argsort(oos_perf)            # ascending: worst..best
        rank = int(np.where(order == n_star)[0][0]) + 1   # 1..N
        omega = rank / (N + 1.0)
        omega = min(max(omega, 1e-9), 1 - 1e-9)
        lam = math.log(omega / (1.0 - omega))
        lambdas.append(lam)
        if lam <= 0:
            n_oos_below += 1
    if not lambdas:
        return {"pbo": None, "error": "no valid splits after purge/embargo"}
    lambdas = np.asarray(lambdas)
    pbo = float(n_oos_below / len(lambdas))
    return {
        "pbo": round(pbo, 4),
        "fragile": bool(pbo > PBO_FRAGILE),
        "n_splits": len(lambdas),
        "n_strategies": N, "n_blocks": n_blocks,
        "median_logit": round(float(np.median(lambdas)), 4),
        "purge": purge, "embargo": embargo, "sampled_combos": sampled,
    }


def format_ranking(report: dict) -> str:
    """One-line-per-strategy text block; raw SR and DSR side by side."""
    lines = [f"  trials N={report['n_trials']}  SR0(null)={report['sr0']}  "
             f"[gate: DSR>={DSR_GRADUATE} AND PBO<={PBO_GRADUATE}]"]
    for r in report["ranking"]:
        dsr = f"{r['dsr']:.4f}" if r["dsr"] is not None else "  nan "
        flag = "PASS" if r["dsr_passes"] else "fail"
        lines.append(f"    {r['name'][:30]:30s}  rawSR={r['raw_sharpe']:+.3f}  "
                     f"DSR={dsr} [{flag}]  T={r['T']}")
    w = report.get("winner")
    if w:
        lines.append(f"  WINNER {w['name'][:30]}: {w['verdict']}  "
                     f"(DSR={w['dsr']}, PBO={w['pbo']}) — {'; '.join(w['reasons']) or 'clears gate'}")
    return "\n".join(lines)
