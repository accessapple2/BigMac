"""tests/test_trades_tz_gate_regression.py — HM-TRADES-TZ-GATE-2026-09-12.

Regression test for the trades-table UTC-vs-local-date gate bug: a trade
5pm-midnight Arizona has a UTC calendar date one day ahead of its real
local day, so any gate comparing `date(<UTC column>)=?` against a
Python-local `date.today()`/`datetime.now()` string either misses that
trade the evening it happens or double-counts it into the next calendar
day. See engine.market_calendar.local_day_utc_bounds() for the full
mechanism and the fix (range-bound queries via that helper instead of
`date()` string matching).

Scoped to the 9 real trades/battle_station_trades gates fixed in this
pass (fleet + per-player daily caps, once-per-day/symbol dedups). NOT the
~40 other `date(col)=?` sites elsewhere in the repo (briefings, journal
entries, signal dedup, cost tracking, etc.) -- those don't gate order
flow, so they're a separate, lower-priority sweep, not this test's job.

Same shape as scripts/ci/grep_gate_scan.py (comment/docstring-aware regex
scan, reusing its own prose-stripping) so the pattern is caught even if
reintroduced with different wording -- this test does not check for the
literal old lines, it checks for the *shape* of the bug.
"""
from __future__ import annotations

import re
import sqlite3
import sys
from datetime import date
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.ci.grep_gate_scan import _blank_prose  # noqa: E402

# The UTC-stored timestamp columns these gates key off (trades.executed_at,
# battle_station_trades.timestamp) -- a bare `date(<col>)=` string match
# against either is the same bug regardless of which table.
_FORBIDDEN = re.compile(r"date\(\s*(executed_at|timestamp)\s*\)\s*=")

# Files holding trades/battle_station_trades daily-count or dedup gates,
# fixed 2026-09-12 to use local_day_utc_bounds() range queries. Any of
# these regressing to a bare date()=? match reintroduces the exact bug.
_GATE_FILES = [
    "engine/battle_station_0dte.py",
    "engine/risk_manager.py",
    "engine/paper_trader.py",
    "engine/trade_gateway.py",
    "engine/m5_allocator.py",
    "engine/capitol_fund.py",
    "engine/crew_scanner.py",
    "engine/dayblade.py",
    "main.py",
]


def test_no_gate_uses_bare_date_string_match_on_utc_timestamp():
    hits: list[str] = []
    for relpath in _GATE_FILES:
        full = _ROOT / relpath
        original = full.read_text(encoding="utf-8", errors="replace")
        scan_text = _blank_prose(original) if relpath.endswith(".py") else original
        for i, line in enumerate(scan_text.splitlines(), start=1):
            if _FORBIDDEN.search(line):
                hits.append(f"{relpath}:{i}")
    assert not hits, (
        "Found a bare `date(executed_at)=` / `date(timestamp)=` string-match "
        "gate against a UTC-stored trades column -- this is the exact "
        "HM-TRADES-TZ-GATE-2026-09-12 bug (misses/duplicates a 5pm-midnight "
        "AZ trade across the local-vs-UTC calendar-day boundary). Use "
        "engine.market_calendar.local_day_utc_bounds() with a range query "
        "(`datetime(col) >= ? AND datetime(col) < ?`) instead. Hits:\n"
        + "\n".join(hits)
    )


def test_local_day_utc_bounds_matches_sqlite_current_timestamp_semantics():
    """Guards the helper's own correctness, not just its adoption -- uses
    the real historical case (qwen3-8b-flash trade id 1780) this bug
    actually mishandled: a trade at 8:55pm Arizona on 2026-04-24, stored
    (via SQLite CURRENT_TIMESTAMP) as UTC 2026-04-25 03:55:42.
    """
    from engine.market_calendar import local_day_utc_bounds

    start_utc, end_utc = local_day_utc_bounds(date(2026, 4, 24))
    assert start_utc == "2026-04-24 07:00:00"
    assert end_utc == "2026-04-25 07:00:00"

    conn = sqlite3.connect(":memory:")
    try:
        in_own_day = conn.execute(
            "SELECT datetime('2026-04-25 03:55:42') >= datetime(?) "
            "AND datetime('2026-04-25 03:55:42') < datetime(?)",
            (start_utc, end_utc),
        ).fetchone()[0]
        assert in_own_day == 1, "an 8:55pm AZ trade must fall inside its own local day's bounds"

        # Must NOT fall inside the PREVIOUS local day's bounds -- the old
        # `date(executed_at)='2026-04-24'` string match would have missed
        # it there too (UTC date is 2026-04-25, not 2026-04-24), which is
        # only half the bug; the other half (this row landing in the NEXT
        # day's bucket instead) is covered by test_local_day_utc_bounds_
        # excludes_the_next_local_day below.
        prev_start, prev_end = local_day_utc_bounds(date(2026, 4, 23))
        in_prev_day = conn.execute(
            "SELECT datetime('2026-04-25 03:55:42') >= datetime(?) "
            "AND datetime('2026-04-25 03:55:42') < datetime(?)",
            (prev_start, prev_end),
        ).fetchone()[0]
        assert in_prev_day == 0
    finally:
        conn.close()


def test_local_day_utc_bounds_excludes_the_next_local_day():
    """The other half of the mechanism: the old bug's `date(executed_at)=?`
    bound to the NEXT local day's date string DID match this row (UTC date
    2026-04-25 == that day's string), silently double-counting an evening
    trade into the following day's cap. The fixed range-bound query must
    exclude it.
    """
    from engine.market_calendar import local_day_utc_bounds

    conn = sqlite3.connect(":memory:")
    try:
        next_start, next_end = local_day_utc_bounds(date(2026, 4, 25))
        in_next_day = conn.execute(
            "SELECT datetime('2026-04-25 03:55:42') >= datetime(?) "
            "AND datetime('2026-04-25 03:55:42') < datetime(?)",
            (next_start, next_end),
        ).fetchone()[0]
        assert in_next_day == 0, (
            "an 8:55pm AZ trade from the PREVIOUS local day must not leak "
            "into the next local day's count -- this is the exact "
            "double-counting half of the original bug"
        )
    finally:
        conn.close()
