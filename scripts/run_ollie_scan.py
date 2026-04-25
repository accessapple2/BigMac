"""
Standalone ollie-auto scanner.

Calls engine.crew_scanner directly — no main.py dependency.
Replaces the in-main.py schedule.every(10).minutes job so ollie-auto
can trade without reanimating the full 40-job daemon.

Defensive guards (added by XO):
  - Real market-hours check (is_extended_trading_hours() unconditionally
    returns True, which is unsafe for a standalone 24/7 scheduler)
  - Early-exit when regime is UNKNOWN (port 8080 dashboard is down; no
    reason to scan against a dead regime signal)

Accepts ATR-proxy fallback from port 9000 Signal Center — that's designed
to degrade gracefully.

Scheduled via com.ollietrades.ollie-scan.plist every 10 minutes.
"""
from __future__ import annotations
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Extended trading window, Eastern Time:
# Pre-market 07:00-09:30, Regular 09:30-16:00, After-hours 16:00-18:00
EXTENDED_TRADING_START_ET = (7, 0)
EXTENDED_TRADING_END_ET = (18, 0)


_SIGNALS_DB = PROJECT_ROOT / "signal-center" / "signals.db"


def _get_top_sc_candidates(limit: int = 20) -> list[str]:
    """Return today's top SC symbols from intelligence_feed (score ≥ 3).

    Mirrors the query used by crew_scanner._fetch_sc_top_picks so the
    pre-warm covers the same candidate set that ollie_auto_check will
    evaluate. Falls back to yesterday if today has no qualifying rows.
    Non-fatal: returns [] on any DB error.
    """
    import sqlite3 as _sq
    try:
        if not _SIGNALS_DB.exists():
            return []
        c = _sq.connect(str(_SIGNALS_DB), check_same_thread=False, timeout=5)
        for days_back in (0, 1):
            rows = c.execute("""
                SELECT json_extract(data, '$.symbol') AS symbol
                FROM intelligence_feed
                WHERE feed_type IN ('SCREENER', 'PREMARKET_SCAN')
                  AND date(created_at) = date('now', ? || ' days')
                  AND json_extract(data, '$.symbol') IS NOT NULL
                  AND CAST(json_extract(data, '$.score') AS REAL) >= 3
                GROUP BY json_extract(data, '$.symbol')
                ORDER BY MAX(CAST(json_extract(data, '$.score') AS REAL)) DESC
                LIMIT ?
            """, (f"-{days_back}", limit)).fetchall()
            if rows:
                c.close()
                return [r[0] for r in rows if r[0]]
        c.close()
    except Exception:
        pass
    return []


def is_trading_window_now() -> bool:
    """Returns True only if it's a weekday 07:00-18:00 ET."""
    now_et = datetime.now(ZoneInfo("America/New_York"))
    if now_et.weekday() >= 5:
        return False
    start_min = EXTENDED_TRADING_START_ET[0] * 60 + EXTENDED_TRADING_START_ET[1]
    end_min = EXTENDED_TRADING_END_ET[0] * 60 + EXTENDED_TRADING_END_ET[1]
    now_min = now_et.hour * 60 + now_et.minute
    return start_min <= now_min < end_min


def main() -> int:
    start = datetime.now(timezone.utc)
    print(f"[ollie-scan] starting at {start.isoformat()}")

    # Guard 1: real market-hours check
    if not is_trading_window_now():
        now_et = datetime.now(ZoneInfo("America/New_York"))
        print(f"[ollie-scan] outside trading window "
              f"({now_et.strftime('%Y-%m-%d %H:%M %Z %A')}) — exiting")
        return 0

    # Import guard
    try:
        from engine.crew_scanner import ollie_auto_check, gather_market_context, _get_regime_from_8080
    except Exception as e:
        print(f"[ollie-scan] FATAL import failure: {type(e).__name__}: {e}")
        traceback.print_exc()
        return 1

    # Guard 2: regime sanity
    try:
        regime = _get_regime_from_8080()
        print(f"[ollie-scan] regime: {regime}")
        if regime == "UNKNOWN":
            print(f"[ollie-scan] regime UNKNOWN (dashboard port 8080 likely down) — exiting without scan")
            return 0
        if regime in ("BEAR", "CRISIS"):
            print(f"[ollie-scan] regime {regime} — ollie_auto_check will decide whether to trade")
    except Exception as e:
        print(f"[ollie-scan] regime check failed: {type(e).__name__}: {e}")
        print(f"[ollie-scan] proceeding since regime function failure != dead data")
        # Don't early-exit on this — regime function may throw for reasons
        # unrelated to data quality. Let ollie_auto_check handle it.

    # Run the actual scan
    try:
        ctx = gather_market_context()

        # Pre-warm fundamentals cache in parallel before quality_gate loop.
        # Each uncached symbol costs ~8 s sequentially inside passes_quality_gate();
        # parallelising collapses N×8 s → ~8 s (bounded by slowest single call).
        # Symbols already in the 24 h DB cache return in milliseconds — only true
        # cache misses hit Yahoo Finance. Non-fatal: throttle errors fall through
        # to the normal quality_gate path.
        try:
            import concurrent.futures as _cf
            from config import get_effective_watchlist as _get_wl
            from engine.stock_fundamentals import fetch_fundamentals as _ff
            _wl = _get_wl()
            _sc = _get_top_sc_candidates(limit=20)
            _wl_set = set(_wl)
            _sc_extra = [s for s in _sc if s not in _wl_set]
            # dict.fromkeys preserves insertion order and deduplicates
            _all = list(dict.fromkeys(_wl + _sc_extra))
            _warm_start = datetime.now(timezone.utc)
            with _cf.ThreadPoolExecutor(max_workers=4) as _pool:
                list(_pool.map(_ff, _all))
            _warm_s = (datetime.now(timezone.utc) - _warm_start).total_seconds()
            print(
                f"[ollie-scan] fundamentals pre-warmed "
                f"({len(_wl)} watchlist + {len(_sc_extra)} SC = {len(_all)} unique, {_warm_s:.1f}s)"
            )
        except Exception as _e:
            print(f"[ollie-scan] fundamentals pre-warm skipped: {_e}")

        trades = ollie_auto_check(ctx)
        end = datetime.now(timezone.utc)
        duration = (end - start).total_seconds()
        print(f"[ollie-scan] completed in {duration:.1f}s")
        if trades:
            print(f"[ollie-scan] result: {trades}")
        else:
            print(f"[ollie-scan] no new trades this cycle")
        return 0
    except Exception as e:
        print(f"[ollie-scan] RUN ERROR: {type(e).__name__}: {e}")
        traceback.print_exc()
        return 2


if __name__ == "__main__":
    sys.exit(main())
