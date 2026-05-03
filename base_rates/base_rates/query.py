"""Match today's bucket vector against history, return aggregate stats."""
from __future__ import annotations

import sqlite3
import statistics
from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

from .buckets import describe_buckets


@dataclass
class BaseRateResult:
    symbol: str
    as_of: str
    forward_days: int
    n_matches: int
    win_rate: float | None        # pct of matches with fwd_return > 0
    median_5d: float | None
    mean_5d: float | None
    p25_5d: float | None
    p75_5d: float | None
    median_maxdd: float | None
    today_buckets: dict[str, str] = field(default_factory=dict)
    match_dates: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    ignored_dims: list[str] = field(default_factory=list)  # which dims were NOT used

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _percentile(sorted_vals: list[float], pct: float) -> float | None:
    """Linear-interpolation percentile. Empty list → None."""
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    k = (len(sorted_vals) - 1) * pct
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def base_rate(
    symbol: str,
    db_path: str | Path,
    as_of: str | None = None,
    forward_days: int = 5,
    min_n: int = 30,
    ignore_slope: bool = True,       # v0.2: dropped - empirically zero-info
    ignore_vix_move: bool = True,    # v0.2: dropped - redundant with move_intensity
) -> BaseRateResult:
    """Compute the base rate for `symbol` on `as_of` date.

    `as_of` is YYYY-MM-DD. If None, uses the latest date in the DB for that symbol.
    Matches against rows with the SAME 6-bucket vector. Excludes the as_of row itself.
    """
    symbol = symbol.upper()
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row

        # 1. Get today's row
        if as_of:
            row = conn.execute(
                "SELECT * FROM base_rate_features WHERE symbol=? AND date=?",
                (symbol, as_of),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM base_rate_features WHERE symbol=? ORDER BY date DESC LIMIT 1",
                (symbol,),
            ).fetchone()

        if not row:
            return BaseRateResult(
                symbol=symbol,
                as_of=as_of or "",
                forward_days=forward_days,
                n_matches=0,
                win_rate=None, median_5d=None, mean_5d=None,
                p25_5d=None, p75_5d=None, median_maxdd=None,
                warnings=[f"no row found for {symbol} on {as_of or 'latest'} — run ingest"],
            )

        as_of_str = row["date"]
        today_buckets = {
            "move_intensity": row["b_move"],
            "rsi_zone": row["b_rsi"],
            "rsi_slope": row["b_rsi_slope"],
            "vix_level": row["b_vix"],
            "vix_move": row["b_vix_move"],
            "market_trend": row["b_trend"],
        }
        ignored = []
        if ignore_slope:
            ignored.append("rsi_slope")
        if ignore_vix_move:
            ignored.append("vix_move")

        # 2. Find all historical rows with same bucket vector for this symbol
        #    (exclude today, exclude rows where forward outcome is NaN)
        # Build WHERE clause and params together, dimension by dimension.
        # Each dimension contributes (clause, value) only if it's active.
        dims = [
            ("b_move = ?",       today_buckets["move_intensity"]),
            ("b_rsi = ?",        today_buckets["rsi_zone"]),
            ("b_vix = ?",        today_buckets["vix_level"]),
            ("b_trend = ?",      today_buckets["market_trend"]),
        ]
        if not ignore_slope:
            dims.append(("b_rsi_slope = ?", today_buckets["rsi_slope"]))
        if not ignore_vix_move:
            dims.append(("b_vix_move = ?",  today_buckets["vix_move"]))

        clauses = ["symbol = ?", "date != ?"] + [d[0] for d in dims] + ["fwd_5d_return IS NOT NULL"]
        params  = [symbol, as_of_str]        + [d[1] for d in dims]

        sql = ("SELECT date, fwd_5d_return, fwd_5d_maxdd "
               "FROM base_rate_features WHERE " + " AND ".join(clauses))
        cur = conn.execute(sql, params)
        matches = cur.fetchall()

    # 3. Aggregate
    returns = sorted(m["fwd_5d_return"] for m in matches)
    maxdds = sorted(m["fwd_5d_maxdd"] for m in matches if m["fwd_5d_maxdd"] is not None)
    dates = [m["date"] for m in matches]

    n = len(returns)
    if n == 0:
        result = BaseRateResult(
            symbol=symbol, as_of=as_of_str, forward_days=forward_days,
            n_matches=0, win_rate=None, median_5d=None, mean_5d=None,
            p25_5d=None, p75_5d=None, median_maxdd=None,
            today_buckets=describe_buckets(today_buckets),
            match_dates=[],
            warnings=["zero historical matches — bucket vector is unprecedented for this symbol"],
            ignored_dims=ignored,
        )
        return result

    win_rate = sum(1 for r in returns if r > 0) / n
    result = BaseRateResult(
        symbol=symbol,
        as_of=as_of_str,
        forward_days=forward_days,
        n_matches=n,
        win_rate=win_rate,
        median_5d=statistics.median(returns),
        mean_5d=statistics.mean(returns),
        p25_5d=_percentile(returns, 0.25),
        p75_5d=_percentile(returns, 0.75),
        median_maxdd=statistics.median(maxdds) if maxdds else None,
        today_buckets=describe_buckets(today_buckets),
        match_dates=dates,
        ignored_dims=ignored,
    )

    if n < min_n:
        result.warnings.append(
            f"N={n} below min_n={min_n} — wide confidence interval, treat with skepticism"
        )

    # Spread warning: if the IQR is wider than 10%, flag it
    if result.p25_5d is not None and result.p75_5d is not None:
        iqr = result.p75_5d - result.p25_5d
        if iqr > 0.10:
            result.warnings.append(
                f"wide IQR ({iqr*100:.1f}pp) — outcomes are highly variable, median may mislead"
            )

    return result
