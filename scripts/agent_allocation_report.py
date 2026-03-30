#!/usr/bin/env python3
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.paper_trader import get_capital_allocation_policy


DB_PATH = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))


def main() -> int:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT id, display_name, COALESCE(is_active, 1) AS is_active, COALESCE(is_human, 0) AS is_human
        FROM ai_players
        WHERE COALESCE(is_active, 1)=1
        ORDER BY id
        """
    ).fetchall()
    conn.close()

    print("PHASE_6_ALLOCATION_REPORT")
    for row in rows:
        policy = get_capital_allocation_policy(row["id"])
        print(
            f"{row['id']}: tier={policy['tier']} multiplier={policy['multiplier']:.2f} "
            f"cycle_return={policy['return_pct']:.2f}% win_rate={policy['win_rate']:.1f}% "
            f"trade_count={policy['trade_count']} reason={policy['reason']} "
            f"cycle={policy['benchmark_label'] or '-'}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
