#!/usr/bin/env python3
"""Signal-history depth per agent — decides 90d vs 180d backtest windows.
Run from ~/autonomous-trader: python3 signal_depth.py [limit]"""
import sqlite3
import sys

limit = int(sys.argv[1]) if len(sys.argv) > 1 else 20
c = sqlite3.connect("data/trader.db")
rows = c.execute(
    """
    SELECT player_id, COUNT(*) AS n, MIN(created_at) AS oldest, MAX(created_at) AS newest
      FROM signals
     WHERE signal IN ('BUY', 'BUY_CALL', 'BUY_PUT')
     GROUP BY player_id
     ORDER BY n DESC
     LIMIT ?
    """,
    (limit,),
).fetchall()
print(f"{'player_id':<32} {'signals':>8}  {'oldest':<10}  {'newest':<10}")
for pid, n, old, new in rows:
    print(f"{pid:<32} {n:>8}  {str(old)[:10]:<10}  {str(new)[:10]:<10}")
