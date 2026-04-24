"""
IV History Backfill — run once daily until iv_history has 5+ rows per ticker.

Current limitation: Polygon's snapshot endpoint gives CURRENT IV only.
We can't fetch historical IV without option contract OHLCV data (requires
knowing specific contract tickers for each past date).

Strategy: record today's reading. Run this daily for 5 trading days,
then the bootstrapping phase ends and true IV rank kicks in.

After 20 trading days the threshold can be raised back to 20.
"""
from __future__ import annotations
import sys
from datetime import date
from pathlib import Path

# Ensure project root is on sys.path when running as a script
sys.path.insert(0, str(Path(__file__).parent))

from strategies.polygon_client import fetch_atm_iv
from strategies.iv_rank import _record_iv, _fetch_history

TICKERS = ["SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "META", "GOOGL", "AMZN", "TSLA"]


def run():
    today = date.today().isoformat()
    print(f"IV backfill — {today}")
    print("=" * 50)

    recorded = 0
    skipped = 0

    for ticker in TICKERS:
        history = _fetch_history(ticker)
        iv = fetch_atm_iv(ticker, target_dte=30)

        if iv is None:
            print(f"  {ticker:6s}  SKIP  (fetch_atm_iv returned None)")
            skipped += 1
            continue

        _record_iv(ticker, iv, "polygon-backfill")
        history_after = _fetch_history(ticker)
        print(f"  {ticker:6s}  IV={iv:.4f}  history={len(history_after)} rows")
        recorded += 1

    print()
    print(f"Done. {recorded} recorded, {skipped} skipped.")
    print(f"Run again on each of the next {max(0, 4 - recorded)} trading days.")
    print("After 5 days: set MIN_HISTORY_DAYS = 20 in strategies/iv_rank.py")

if __name__ == "__main__":
    run()
