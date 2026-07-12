"""HM-MARKET-HOLIDAY-CALENDAR Phase A tests.

Cases enumerated in the mission brief plus additional coverage for
``next_market_open`` and holiday-name lookup.
"""
from __future__ import annotations

import unittest
from datetime import datetime, date

import pytz

from engine.market_calendar import (
    ET,
    UTC,
    MarketStatus,
    get_holiday_name,
    get_market_status,
    is_early_close_day,
    is_us_market_holiday,
    is_us_market_open,
    market_hours_elapsed,
    next_market_open,
)


def _et(year, month, day, hour=0, minute=0) -> datetime:
    """Helper: build an ET-localized datetime."""
    return ET.localize(datetime(year, month, day, hour, minute))


class MarketCalendarTests(unittest.TestCase):
    # ── Case 1: Memorial Day 2026 (today!) — any time → CLOSED_HOLIDAY ──
    def test_memorial_day_2026_closed_holiday(self) -> None:
        for hour in (8, 10, 13, 16, 22):
            with self.subTest(hour=hour):
                self.assertEqual(
                    get_market_status(_et(2026, 5, 25, hour, 0)),
                    MarketStatus.CLOSED_HOLIDAY,
                )
        self.assertTrue(is_us_market_holiday(date(2026, 5, 25)))
        self.assertEqual(get_holiday_name(date(2026, 5, 25)), "Memorial Day")
        self.assertFalse(is_us_market_open(_et(2026, 5, 25, 10, 0)))

    # ── Case 2: Saturday Jan 17 2026 → CLOSED_WEEKEND ───────────────────
    def test_saturday_jan_17_2026_weekend(self) -> None:
        # Jan 17, 2026 is a Saturday
        self.assertEqual(date(2026, 1, 17).weekday(), 5)
        self.assertEqual(
            get_market_status(_et(2026, 1, 17, 10, 0)),
            MarketStatus.CLOSED_WEEKEND,
        )

    # ── Case 3: Christmas Day 2026 (Friday Dec 25) → CLOSED_HOLIDAY ─────
    def test_christmas_day_2026_holiday(self) -> None:
        self.assertEqual(date(2026, 12, 25).weekday(), 4)  # Friday
        self.assertEqual(
            get_market_status(_et(2026, 12, 25, 10, 0)),
            MarketStatus.CLOSED_HOLIDAY,
        )
        self.assertEqual(get_holiday_name(date(2026, 12, 25)), "Christmas")

    # ── Case 4: New Year's 2026 (Thursday Jan 1) → CLOSED_HOLIDAY ───────
    def test_new_years_2026_holiday(self) -> None:
        self.assertEqual(date(2026, 1, 1).weekday(), 3)  # Thursday
        self.assertEqual(
            get_market_status(_et(2026, 1, 1, 10, 0)),
            MarketStatus.CLOSED_HOLIDAY,
        )

    # ── Case 5: Black Friday 2026 (Friday Nov 27) at 1:30 ET → CLOSED_EARLY ──
    def test_black_friday_2026_early_close(self) -> None:
        # Day-of weekday check (Friday) + early-close flag
        self.assertEqual(date(2026, 11, 27).weekday(), 4)  # Friday
        self.assertTrue(is_early_close_day(date(2026, 11, 27)))
        # 12:30 ET — still open (early close is 1:00 ET)
        self.assertEqual(
            get_market_status(_et(2026, 11, 27, 12, 30)),
            MarketStatus.OPEN,
        )
        # 1:30 ET — past early close
        self.assertEqual(
            get_market_status(_et(2026, 11, 27, 13, 30)),
            MarketStatus.CLOSED_EARLY,
        )

    # ── Case 6: July 3 2026 — observed Independence Day (Saturday July 4) ──
    def test_july_3_2026_observed_holiday(self) -> None:
        # July 4, 2026 is Saturday → NYSE observes Friday July 3 as full holiday
        self.assertEqual(date(2026, 7, 4).weekday(), 5)  # Saturday confirmed
        self.assertTrue(is_us_market_holiday(date(2026, 7, 3)))
        self.assertEqual(
            get_holiday_name(date(2026, 7, 3)),
            "Independence Day (observed)",
        )
        self.assertEqual(
            get_market_status(_et(2026, 7, 3, 10, 0)),
            MarketStatus.CLOSED_HOLIDAY,
        )
        # July 4 itself: a Saturday — weekend (NOT marked as holiday in the table)
        self.assertEqual(
            get_market_status(_et(2026, 7, 4, 10, 0)),
            MarketStatus.CLOSED_WEEKEND,
        )

    # ── Case 7: Regular Tuesday 10:00 ET → OPEN ─────────────────────────
    def test_regular_tuesday_open(self) -> None:
        # Tue 2026-05-26 — day after Memorial Day, a regular trading day
        self.assertEqual(date(2026, 5, 26).weekday(), 1)  # Tuesday
        self.assertFalse(is_us_market_holiday(date(2026, 5, 26)))
        self.assertEqual(
            get_market_status(_et(2026, 5, 26, 10, 0)),
            MarketStatus.OPEN,
        )
        self.assertTrue(is_us_market_open(_et(2026, 5, 26, 10, 0)))

    # ── Case 8: Regular Tuesday 16:30 ET → CLOSED_AFTER_HOURS ───────────
    def test_regular_tuesday_after_hours(self) -> None:
        self.assertEqual(
            get_market_status(_et(2026, 5, 26, 16, 30)),
            MarketStatus.CLOSED_AFTER_HOURS,
        )

    # ── Case 9: Regular Tuesday 09:00 ET → CLOSED_BEFORE_HOURS ──────────
    def test_regular_tuesday_before_hours(self) -> None:
        self.assertEqual(
            get_market_status(_et(2026, 5, 26, 9, 0)),
            MarketStatus.CLOSED_BEFORE_HOURS,
        )

    # ── Case 10: Good Friday 2026 (April 3) → CLOSED_HOLIDAY ────────────
    def test_good_friday_2026_holiday(self) -> None:
        self.assertEqual(date(2026, 4, 3).weekday(), 4)  # Friday
        self.assertTrue(is_us_market_holiday(date(2026, 4, 3)))
        self.assertEqual(get_holiday_name(date(2026, 4, 3)), "Good Friday")
        self.assertEqual(
            get_market_status(_et(2026, 4, 3, 10, 0)),
            MarketStatus.CLOSED_HOLIDAY,
        )

    # ── next_market_open — from Memorial Day Monday 2026-05-25 ──────────
    def test_next_market_open_from_memorial_day(self) -> None:
        nxt = next_market_open(_et(2026, 5, 25, 10, 0))
        self.assertEqual(nxt.date(), date(2026, 5, 26))
        self.assertEqual(nxt.hour, 9)
        self.assertEqual(nxt.minute, 30)

    # ── next_market_open — from Friday 16:30 ET → Monday 09:30 ET ───────
    def test_next_market_open_from_friday_afterhours(self) -> None:
        # Friday 2026-05-22, after hours; next open should be Monday 2026-05-25
        # ... but May 25 is Memorial Day, so the actual next open is Tuesday May 26.
        nxt = next_market_open(_et(2026, 5, 22, 16, 30))
        self.assertEqual(nxt.date(), date(2026, 5, 26))

    # ── next_market_open — from Saturday spans weekend + MLK ────────────
    def test_next_market_open_from_saturday(self) -> None:
        # Saturday Jan 17, 2026. Mon Jan 19 is MLK Day → next open is
        # Tuesday Jan 20 09:30 ET (skips weekend AND skips the MLK holiday).
        nxt = next_market_open(_et(2026, 1, 17, 10, 0))
        self.assertEqual(nxt.date(), date(2026, 1, 20))

    def test_next_market_open_from_friday_simple_weekend(self) -> None:
        # Friday May 22, 2026 16:30 ET. Mon May 25 is Memorial Day (covered
        # by test_next_market_open_from_friday_afterhours). Here use Friday
        # May 29 16:30 — no holiday Monday following → Mon Jun 1 09:30 ET.
        nxt = next_market_open(_et(2026, 5, 29, 16, 30))
        self.assertEqual(nxt.date(), date(2026, 6, 1))

    # ── next_market_open — from Good Friday → Monday April 6 ───────────
    def test_next_market_open_from_good_friday(self) -> None:
        nxt = next_market_open(_et(2026, 4, 3, 10, 0))
        # Apr 4 Sat, Apr 5 Sun, Apr 6 Mon trading
        self.assertEqual(nxt.date(), date(2026, 4, 6))

    # ── 2025 + 2027 spot checks ─────────────────────────────────────────
    def test_2025_thanksgiving(self) -> None:
        self.assertTrue(is_us_market_holiday(date(2025, 11, 27)))
        self.assertEqual(get_holiday_name(date(2025, 11, 27)), "Thanksgiving")

    def test_2027_juneteenth_observed(self) -> None:
        # Jun 19, 2027 is Saturday → observed Friday Jun 18
        self.assertEqual(date(2027, 6, 19).weekday(), 5)
        self.assertTrue(is_us_market_holiday(date(2027, 6, 18)))
        self.assertEqual(
            get_holiday_name(date(2027, 6, 18)),
            "Juneteenth (observed)",
        )

    # ── Naive datetime input handling ──────────────────────────────────
    def test_naive_datetime_assumed_utc(self) -> None:
        # Memorial Day 2026-05-25 at 14:30 UTC == 10:30 ET — should be CLOSED_HOLIDAY
        naive = datetime(2026, 5, 25, 14, 30)  # naive, treated as UTC
        self.assertEqual(get_market_status(naive), MarketStatus.CLOSED_HOLIDAY)


# ── market_hours_elapsed — HM-SENTINEL-ACK (2026-07-12) ─────────────────
class MarketHoursElapsedTests(unittest.TestCase):
    def test_same_day_partial_session(self) -> None:
        # Monday 2026-07-13: 09:30 -> 11:30 ET, no other trading days involved.
        start = _et(2026, 7, 13, 9, 30)
        end = _et(2026, 7, 13, 11, 30)
        self.assertAlmostEqual(market_hours_elapsed(start, end), 2.0, places=6)

    def test_full_weekend_contributes_zero(self) -> None:
        # Friday close (16:00 ET) -> Monday open (09:30 ET): no session time
        # in between at all -- weekend contributes zero, not ~65.5 wall-clock hours.
        start = _et(2026, 7, 10, 16, 0)   # Friday close
        end = _et(2026, 7, 13, 9, 30)     # Monday open
        self.assertEqual(market_hours_elapsed(start, end), 0.0)

    def test_real_backlog_scenario_created_just_after_friday_close(self) -> None:
        # The actual HM-SIGNALS-V2-FIFO-STARVATION weekend case: oldest
        # pending row created 2026-07-10 20:01:52 UTC == 16:01:52 ET, ~2
        # minutes after Friday's 16:00 close. Checked Sunday mid-day -- must
        # still read ~0 market-hours elapsed (Friday's 2 remaining minutes
        # are already past close; Sat/Sun contribute nothing) even though
        # wall-clock elapsed is ~45-46 hours.
        start = datetime(2026, 7, 10, 20, 1, 52, tzinfo=UTC)
        end = datetime(2026, 7, 12, 18, 0, 0, tzinfo=UTC)  # Sunday ~11am MST
        self.assertEqual(market_hours_elapsed(start, end), 0.0)

    def test_spans_one_weekday_boundary(self) -> None:
        # Monday 15:00 ET -> Tuesday 10:30 ET: 1h (Mon 15:00-16:00) + 1h
        # (Tue 09:30-10:30) = 2.0h, overnight gap contributes nothing.
        start = _et(2026, 7, 13, 15, 0)
        end = _et(2026, 7, 14, 10, 30)
        self.assertAlmostEqual(market_hours_elapsed(start, end), 2.0, places=6)

    def test_early_close_day_caps_at_1pm_et(self) -> None:
        # Black Friday 2026-11-27 is an early-close (1pm ET) day. 09:30 ->
        # 15:00 same day must cap at 13:00, i.e. 3.5h, not 5.5h.
        self.assertTrue(is_early_close_day(date(2026, 11, 27)))
        start = _et(2026, 11, 27, 9, 30)
        end = _et(2026, 11, 27, 15, 0)
        self.assertAlmostEqual(market_hours_elapsed(start, end), 3.5, places=6)

    def test_holiday_contributes_zero(self) -> None:
        # Memorial Day 2026-05-25 sits between Friday 5/22 and Tuesday 5/26.
        # Only the two real trading days' overlaps should count.
        start = _et(2026, 5, 22, 15, 0)   # Friday, 1h of session left
        end = _et(2026, 5, 26, 10, 30)    # Tuesday, 1h of session elapsed
        self.assertAlmostEqual(market_hours_elapsed(start, end), 2.0, places=6)

    def test_end_before_start_is_zero(self) -> None:
        start = _et(2026, 7, 13, 11, 0)
        end = _et(2026, 7, 13, 9, 30)
        self.assertEqual(market_hours_elapsed(start, end), 0.0)

    def test_defaults_end_to_now(self) -> None:
        # Just confirm it runs and returns a non-negative float when `end`
        # is omitted -- not asserting an exact value since "now" moves.
        result = market_hours_elapsed(_et(2020, 1, 2, 9, 30))
        self.assertIsInstance(result, float)
        self.assertGreaterEqual(result, 0.0)


if __name__ == "__main__":
    unittest.main()
