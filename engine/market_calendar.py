"""engine/market_calendar.py — US market holiday + hours calendar.

HM-MARKET-HOLIDAY-CALENDAR Phase A 2026-05-25.

Authoritative source for market-open/closed state. Imported by every
signal-emission and order-submission path:
  - engine/paper_trader.py::buy() / ::sell() / ::short_sell()
  - engine/alpaca_bridge.py::submit*() / ::buy() / ::sell()
  - dashboard/app.py (market-status banner)
  - direct-call agents (neo-matrix, etc.) at their call sites

Reference incident: Memorial Day 2026-05-25. Trader fired 6 Alpaca orders
+ 2 simulated positions on a market-closed day because production had no
calendar awareness (only backtest scripts did). All orders were cancelled
in the emergency arc (commits 6cdf9d5 + c35aa51 + 02d3558); this module
is the structural fix.

Holiday data sourced from NYSE published schedules (2025-2027). Annual
review required — bank a calendar-year-end ticket each December for the
upcoming year. Observed-day rules: holiday on Saturday → previous Friday;
holiday on Sunday → following Monday. Early-close (1pm ET) on Black
Friday, Christmas Eve when a weekday, and day-before July 4 when that
day is Tuesday-Friday.
"""
from __future__ import annotations

import enum
from datetime import datetime, date, time, timedelta
from typing import Optional

import pytz
from zoneinfo import ZoneInfo

# ── Timezones ───────────────────────────────────────────────────────────
ET = pytz.timezone("America/New_York")
UTC = pytz.UTC

# Arizona (Phoenix) via stdlib zoneinfo — NOT pytz. The long-running trader process
# was observed serving datetime.now(pytz.timezone("US/Arizona")) skewed 7h (the cached
# pytz singleton's offset got corrupted in-process; naive now() stayed correct). zoneinfo
# is a separate, immutable-per-key cache immune to that corruption. Phoenix has no DST,
# so az_now() is exact and always == MST. Use this for ALL time-of-day gating.
_AZ = ZoneInfo("America/Phoenix")


def az_now() -> datetime:
    """Current Arizona time (stdlib zoneinfo; corruption-proof vs the pytz path)."""
    return datetime.now(_AZ)


def utc_now_str() -> str:
    """Canonical DB timestamp: space-separated UTC, whole-second precision —
    byte-identical to SQLite ``CURRENT_TIMESTAMP``. HM-TZ Stage 3 write convention.

    Use for any datetime written to a DB column. (JSON/API fields use T-separated
    UTC with a ``+00:00`` offset instead — do NOT use this there.)
    """
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def parse_dt(value, *, assume_utc: bool = True) -> Optional[datetime]:
    """Tolerant ISO-8601 parse → timezone-aware UTC datetime (or None).

    HM-TZ Stage 2b canonical read helper. Accepts space- or T-separated strings,
    a trailing ``Z``, an explicit ``+HH:MM`` offset, naive strings, or an existing
    ``datetime``. Naive inputs are assumed UTC (the project's storage convention),
    so a value read from a space-separated ``CURRENT_TIMESTAMP`` column comes back
    correctly tagged. Over-long fractional seconds (e.g. Alpaca nanosecond ``Z``
    stamps) are clamped to microseconds. Returns None on empty/unparseable input.

    Replaces scattered ``datetime.fromisoformat(s.replace("Z",""))`` hacks. For
    naive arithmetic call sites this is drop-in: aware-UTC preserves both elapsed
    math and ``.hour``/``.weekday()`` (the old naive values were already UTC).
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        s = str(value).strip().replace("Z", "+00:00")
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            import re
            s2 = re.sub(r"(\.\d{6})\d+", r"\1", s)  # clamp ns → µs
            try:
                dt = datetime.fromisoformat(s2)
            except ValueError:
                return None
    if dt.tzinfo is None:
        return UTC.localize(dt) if assume_utc else dt
    return dt.astimezone(UTC)

# ── Hours (ET) ──────────────────────────────────────────────────────────
MARKET_OPEN_TIME = time(9, 30)   # regular open
MARKET_CLOSE_TIME = time(16, 0)  # regular close
EARLY_CLOSE_TIME = time(13, 0)   # early-close days


class MarketStatus(enum.Enum):
    """Mutually-exclusive market-state buckets returned by
    :func:`get_market_status`."""

    OPEN = "open"
    CLOSED_WEEKEND = "closed_weekend"
    CLOSED_HOLIDAY = "closed_holiday"
    CLOSED_EARLY = "closed_early"            # past 1pm on early-close day
    CLOSED_BEFORE_HOURS = "closed_before_hours"
    CLOSED_AFTER_HOURS = "closed_after_hours"


# ── Holiday tables ──────────────────────────────────────────────────────
# Year → {date: human-readable name}. NYSE-observed (Monday-Friday only;
# Saturday holidays observed on prior Friday, Sunday on following Monday).
US_HOLIDAYS: dict[int, dict[date, str]] = {
    2025: {
        date(2025, 1, 1):   "New Year's Day",
        date(2025, 1, 20):  "Martin Luther King Jr. Day",
        date(2025, 2, 17):  "Presidents Day",
        date(2025, 4, 18):  "Good Friday",
        date(2025, 5, 26):  "Memorial Day",
        date(2025, 6, 19):  "Juneteenth",
        date(2025, 7, 4):   "Independence Day",
        date(2025, 9, 1):   "Labor Day",
        date(2025, 11, 27): "Thanksgiving",
        date(2025, 12, 25): "Christmas",
    },
    2026: {
        date(2026, 1, 1):   "New Year's Day",
        date(2026, 1, 19):  "Martin Luther King Jr. Day",
        date(2026, 2, 16):  "Presidents Day",
        date(2026, 4, 3):   "Good Friday",
        date(2026, 5, 25):  "Memorial Day",
        date(2026, 6, 19):  "Juneteenth",
        date(2026, 7, 3):   "Independence Day (observed)",  # Jul 4 is Saturday
        date(2026, 9, 7):   "Labor Day",
        date(2026, 11, 26): "Thanksgiving",
        date(2026, 12, 25): "Christmas",
    },
    2027: {
        date(2027, 1, 1):   "New Year's Day",
        date(2027, 1, 18):  "Martin Luther King Jr. Day",
        date(2027, 2, 15):  "Presidents Day",
        date(2027, 3, 26):  "Good Friday",
        date(2027, 5, 31):  "Memorial Day",
        date(2027, 6, 18):  "Juneteenth (observed)",         # Jun 19 is Saturday
        date(2027, 7, 5):   "Independence Day (observed)",   # Jul 4 is Sunday
        date(2027, 9, 6):   "Labor Day",
        date(2027, 11, 25): "Thanksgiving",
        date(2027, 12, 24): "Christmas (observed)",          # Dec 25 is Saturday
    },
}

# Early-close (1pm ET) days. Per NYSE: day before Independence Day when
# that falls on Tue-Fri; day after Thanksgiving (Black Friday); Christmas
# Eve when a weekday and Dec 25 itself isn't observed there.
EARLY_CLOSE_DAYS: dict[int, set[date]] = {
    2025: {
        date(2025, 7, 3),    # July 3 Thursday — day before July 4 Friday
        date(2025, 11, 28),  # Black Friday
        date(2025, 12, 24),  # Christmas Eve Wednesday
    },
    2026: {
        # No July early close — July 4 is Saturday, observed full-holiday Friday July 3
        date(2026, 11, 27),  # Black Friday
        date(2026, 12, 24),  # Christmas Eve Thursday
    },
    2027: {
        # No July early close — July 4 is Sunday, observed Monday July 5
        date(2027, 11, 26),  # Black Friday
        # No Christmas Eve early close — Dec 24 is observed Christmas (full holiday)
    },
}


# ── Public API ──────────────────────────────────────────────────────────


def is_us_market_holiday(d: date) -> bool:
    """True iff ``d`` is a full NYSE market-closed holiday."""
    year_map = US_HOLIDAYS.get(d.year)
    return year_map is not None and d in year_map


def get_holiday_name(d: date) -> Optional[str]:
    """Return the holiday name for ``d``, or None if not a holiday.

    Useful for the dashboard banner — e.g. "MARKET CLOSED · Memorial Day"."""
    year_map = US_HOLIDAYS.get(d.year)
    if year_map is None:
        return None
    return year_map.get(d)


def is_early_close_day(d: date) -> bool:
    """True iff ``d`` is an NYSE early-close (1pm ET) day."""
    return d in EARLY_CLOSE_DAYS.get(d.year, set())


def _to_et(now: Optional[datetime]) -> datetime:
    """Convert any datetime (or None=now) to ET, timezone-aware."""
    if now is None:
        now = datetime.now(UTC)
    elif now.tzinfo is None:
        # Naive datetime — assume UTC by convention
        now = UTC.localize(now)
    return now.astimezone(ET)


def get_market_status(now: Optional[datetime] = None) -> MarketStatus:
    """Return the mutually-exclusive market state at ``now``.

    Default ``now`` is the current UTC time. Naive datetimes are assumed
    UTC. The comparison happens in ET (NYSE local time). DST handled by
    pytz.

    Decision order (first-match wins):
      1. Saturday/Sunday  -> CLOSED_WEEKEND
      2. NYSE holiday     -> CLOSED_HOLIDAY
      3. Before 9:30 ET   -> CLOSED_BEFORE_HOURS
      4. Early-close day AND past 1:00 ET    -> CLOSED_EARLY
      5. Regular day AND past 4:00 ET        -> CLOSED_AFTER_HOURS
      6. otherwise        -> OPEN
    """
    now_et = _to_et(now)
    d_et = now_et.date()
    t_et = now_et.time()

    if d_et.weekday() >= 5:
        return MarketStatus.CLOSED_WEEKEND
    if is_us_market_holiday(d_et):
        return MarketStatus.CLOSED_HOLIDAY
    if t_et < MARKET_OPEN_TIME:
        return MarketStatus.CLOSED_BEFORE_HOURS
    if is_early_close_day(d_et):
        if t_et >= EARLY_CLOSE_TIME:
            return MarketStatus.CLOSED_EARLY
    else:
        if t_et >= MARKET_CLOSE_TIME:
            return MarketStatus.CLOSED_AFTER_HOURS
    return MarketStatus.OPEN


def is_us_market_open(now: Optional[datetime] = None) -> bool:
    """True iff the US market is open for regular trading at ``now``."""
    return get_market_status(now) == MarketStatus.OPEN


def market_closed_reason(now: Optional[datetime] = None) -> Optional[str]:
    """Structured rejection reason if market is closed at ``now``, else None.

    Used by signal-emission and order-submission gates per HM-MARKET-
    HOLIDAY-CALENDAR Phase B. Format::

        market_closed_<status_value>[ (holiday_name)]

    Examples::

        market_closed_weekend
        market_closed_holiday (Memorial Day)
        market_closed_before_hours
        market_closed_early

    Callers should append the prefix ``[HM-MARKET-CLOSED]`` when logging.
    """
    status = get_market_status(now)
    if status == MarketStatus.OPEN:
        return None
    base = f"market_{status.value}"
    if status == MarketStatus.CLOSED_HOLIDAY:
        now_et = _to_et(now)
        nm = get_holiday_name(now_et.date())
        if nm:
            return f"{base} ({nm})"
    return base


def next_market_open(now: Optional[datetime] = None) -> datetime:
    """Return the next datetime when the market opens (ET-localized).

    If ``now`` is before today's 9:30 ET and today is a trading day,
    returns today's 9:30 ET. Otherwise returns the 9:30 ET of the next
    trading day (skipping weekends + holidays).
    """
    now_et = _to_et(now)

    candidate_d = now_et.date()
    candidate_dt = ET.localize(datetime.combine(candidate_d, MARKET_OPEN_TIME))
    if candidate_dt <= now_et:
        candidate_d = candidate_d + timedelta(days=1)

    # Walk forward until we find a trading day.
    for _ in range(14):  # bound the search; 14 days covers any holiday cluster
        if candidate_d.weekday() < 5 and not is_us_market_holiday(candidate_d):
            return ET.localize(datetime.combine(candidate_d, MARKET_OPEN_TIME))
        candidate_d = candidate_d + timedelta(days=1)
    raise RuntimeError(
        f"next_market_open: no trading day found within 14 days of {now_et}; "
        "US_HOLIDAYS table likely needs extension"
    )
