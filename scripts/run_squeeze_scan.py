"""
Standalone squeeze scanner.

Calls engine.squeeze_scanner.run_scan() directly, no main.py dependency.
Replaces the in-main.py schedule that used to expose /api/squeeze.

Defensive guards:
  - Market hours check (scanner uses finvizfinance + yfinance which
    rate-limit heavily; no reason to run 24/7)
  - Graceful error reporting (uhura-watch will resume polling once
    this runs successfully)

Scheduled via com.ollietrades.squeeze-scan.plist every 15 minutes
(squeeze state is slow-moving; 10-min was overkill).
"""
from __future__ import annotations
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def is_trading_window_now() -> bool:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    if now_et.weekday() >= 5:
        return False
    now_min = now_et.hour * 60 + now_et.minute
    return (7 * 60) <= now_min < (18 * 60)


def main() -> int:
    start = datetime.now(timezone.utc)
    print(f"[squeeze-scan] starting at {start.isoformat()}")

    if not is_trading_window_now():
        now_et = datetime.now(ZoneInfo("America/New_York"))
        print(f"[squeeze-scan] outside trading window "
              f"({now_et.strftime('%Y-%m-%d %H:%M %Z %A')}) — exiting")
        return 0

    try:
        from engine.squeeze_scanner import run_scan
    except Exception as e:
        print(f"[squeeze-scan] FATAL import failure: {type(e).__name__}: {e}")
        traceback.print_exc()
        return 1

    try:
        result = run_scan()
        end = datetime.now(timezone.utc)
        duration = (end - start).total_seconds()
        print(f"[squeeze-scan] completed in {duration:.1f}s")
        if isinstance(result, dict):
            candidates = result.get("results", result.get("candidates", []))
            print(f"[squeeze-scan] {len(candidates)} candidates")
            for c in candidates[:5]:
                print(f"  {c}")
        elif isinstance(result, list):
            print(f"[squeeze-scan] {len(result)} candidates")
            for c in result[:5]:
                print(f"  {c}")
        else:
            print(f"[squeeze-scan] result: {type(result).__name__}")
        return 0
    except Exception as e:
        print(f"[squeeze-scan] RUN ERROR: {type(e).__name__}: {e}")
        traceback.print_exc()
        return 2


if __name__ == "__main__":
    sys.exit(main())
