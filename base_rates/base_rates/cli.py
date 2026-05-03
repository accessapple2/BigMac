"""Command-line interface: python -m base_rates AAPL [--date YYYY-MM-DD] [--json]"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .query import base_rate


def _fmt_pct(v: float | None, digits: int = 1) -> str:
    if v is None:
        return "n/a"
    return f"{v*100:+.{digits}f}%"


def _fmt_pct_unsigned(v: float | None, digits: int = 1) -> str:
    if v is None:
        return "n/a"
    return f"{v*100:.{digits}f}%"


def render_text(r) -> str:
    lines = []
    lines.append(f"\n=== Base Rate: {r.symbol} as of {r.as_of} ===\n")

    if r.n_matches == 0:
        lines.append("  N=0 — no historical analogs found.")
        for w in r.warnings:
            lines.append(f"  ⚠  {w}")
        return "\n".join(lines)

    lines.append("Today's bucket vector (matched dims):")
    for k, v in r.today_buckets.items():
        if k in r.ignored_dims:
            lines.append(f"  {k:<16} {v}    [ignored]")
        else:
            lines.append(f"  {k:<16} {v}")

    lines.append("")
    lines.append(f"Historical matches: N = {r.n_matches}")
    lines.append(f"  Win rate ({r.forward_days}d):     {_fmt_pct_unsigned(r.win_rate)}")
    lines.append(f"  Median fwd return:    {_fmt_pct(r.median_5d, 2)}")
    lines.append(f"  Mean fwd return:      {_fmt_pct(r.mean_5d, 2)}")
    lines.append(f"  P25 / P75:            {_fmt_pct(r.p25_5d, 2)} / {_fmt_pct(r.p75_5d, 2)}")
    lines.append(f"  Median path maxDD:    {_fmt_pct(r.median_maxdd, 2)}")

    if r.warnings:
        lines.append("")
        for w in r.warnings:
            lines.append(f"  ⚠  {w}")

    lines.append("")
    return "\n".join(lines)


def main() -> None:
    p = argparse.ArgumentParser(
        prog="python -m base_rates",
        description="Compute conditional base rate for a symbol from local history.",
    )
    p.add_argument("symbol", help="Ticker (e.g. AAPL)")
    p.add_argument("--date", default=None, help="As-of date YYYY-MM-DD (default: latest)")
    p.add_argument("--db", default="signals.db", help="Path to signals.db")
    p.add_argument("--forward-days", type=int, default=5)
    p.add_argument("--min-n", type=int, default=30, help="Warn if N < this (default 30)")
    # v0.2: rsi_slope and vix_move are now ignored by default. These flags
    # opt back into the v0.1 strict matching for research/comparison purposes.
    p.add_argument("--with-slope", action="store_true",
                   help="Match on rsi_slope (v0.1 behavior; usually zero-info)")
    p.add_argument("--with-vix-move", action="store_true",
                   help="Match on vix_move (v0.1 behavior; usually redundant with move_intensity)")
    p.add_argument("--json", action="store_true", help="Emit JSON instead of text")
    args = p.parse_args()

    if not Path(args.db).exists():
        sys.exit(f"ERROR: db not found at {args.db}. Run: python -m base_rates.migrate --db {args.db}")

    result = base_rate(
        symbol=args.symbol,
        db_path=args.db,
        as_of=args.date,
        forward_days=args.forward_days,
        min_n=args.min_n,
        ignore_slope=not args.with_slope,
        ignore_vix_move=not args.with_vix_move,
    )

    if args.json:
        print(json.dumps(result.to_dict(), default=str, indent=2))
    else:
        print(render_text(result))


if __name__ == "__main__":
    main()
