#!/usr/bin/env python3
"""HM-XO-PLAN-2026-09 Phase 1.1 acceptance check.

Read-only: never writes to trader.db. Recomputes
engine.providers.base.check_invalidation_missing() offline against stored
signals.invalidation/reference_price for McCoy (ollama-plutus) on a given
trading day, and reports the % of directional (BUY/BUY_CALL/BUY_PUT/SHORT)
signals with a PLAUSIBLE invalidation -- not just non-empty. Bar: >=90%,
with no drop in format-parse success vs. the pre-1.1 baseline.

One-shot gate check, not a recurring cron (unlike Phase 0's baseline
tracker) -- 1.1 either clears this bar or the prompt wording gets
iterated before 1.2 starts. See docs/XO_PLAN_2026-09.md.

Usage:
  python3 scripts/mccoy_invalidation_acceptance.py                # today
  python3 scripts/mccoy_invalidation_acceptance.py --date 2026-09-10
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from engine.providers.base import check_invalidation_missing, _INVALIDATION_REQUIRED_ACTIONS  # noqa: E402

DB_PATH = REPO / "data" / "trader.db"
MCCOY_ID = "ollama-plutus"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="YYYY-MM-DD (America/Phoenix trading day); default today")
    args = ap.parse_args()
    d = (datetime.strptime(args.date, "%Y-%m-%d").date() if args.date
         else datetime.now(ZoneInfo("America/Phoenix")).date())

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    rows = conn.execute(
        "SELECT symbol, signal, invalidation, reference_price, created_at FROM signals "
        "WHERE player_id=? AND date(created_at)=date(?)",
        (MCCOY_ID, str(d)),
    ).fetchall()
    conn.close()

    directional = [r for r in rows if r[1] in _INVALIDATION_REQUIRED_ACTIONS]
    total = len(rows)
    n_dir = len(directional)
    if n_dir == 0:
        print(f"[mccoy-1.1-acceptance] {d}: no directional McCoy signals found "
              f"({total} total signals) -- nothing to score. Too early, or Phase 1.1 "
              f"code hasn't shipped/restarted for this date yet.")
        return

    n_plausible = 0
    failures: Counter = Counter()
    examples: list[str] = []
    for symbol, action, invalidation, reference_price, created_at in directional:
        issue = check_invalidation_missing(action, invalidation, reference_price)
        if issue is None:
            n_plausible += 1
        else:
            reason = issue.split(" (")[0].split(":")[0]
            failures[reason] += 1
            if len(examples) < 8:
                examples.append(f"  {created_at} {symbol} {action}: {issue}")

    pct = 100 * n_plausible / n_dir
    verdict = "PASS" if pct >= 90.0 else "FAIL"
    print(f"[mccoy-1.1-acceptance] {d}: {n_plausible}/{n_dir} directional signals "
          f"plausible ({pct:.1f}%) -- bar is 90% -- {verdict}")
    if failures:
        print("  failure breakdown:")
        for reason, count in failures.most_common():
            print(f"    {reason}: {count}")
        print("  examples:")
        for ex in examples:
            print(ex)


if __name__ == "__main__":
    main()
