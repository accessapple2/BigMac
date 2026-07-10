"""HM-TELEGRAM-NTFY-UNIFY (2026-07-07) — dynamic_alerts._notify() tests.

Confirms dynamic_alerts.py routes through engine.alert_channels.send_alert
(ntfy-first, severity-routed) instead of the retired _send_telegram silent-
catch path. Same temp-DB isolation pattern as test_alert_defs.py
(monkeypatch the module-level DB constant) -- check_rsi_extremes writes a
real row via _save_alert, must never touch production trader.db.
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine import dynamic_alerts as da  # noqa: E402


def test_check_rsi_extremes_routes_through_alert_channels():
    """rsi=25 (oversold) must call engine.alert_channels.send_alert exactly
    once, with level="info" (severity "medium" maps to "info" per _notify's
    level_map) and alert_type="dyn_rsi_oversold_<SYM>" -- not the retired
    telegram path."""
    symbol = "ZZZTESTNOTIFY"
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "test_trader.db")
        with patch.object(da, "DB", db_path):
            da.ensure_alerts_table()
            da._alert_cooldown.clear()  # module-level global -- avoid cross-test cooldown bleed
            # Market-hours gate (item 8) is orthogonal to what this test
            # checks -- force it open so the test is time-independent.
            with patch("engine.market_calendar.is_within_alert_hours", return_value=True):
                with patch("engine.alert_channels.send_alert") as mock_send:
                    mock_send.return_value = {"ntfy": True}
                    alerts = da.check_rsi_extremes(symbol, price=42.0, indicators={"rsi": 25})

    assert len(alerts) == 1
    assert alerts[0]["type"] == "rsi_oversold"
    mock_send.assert_called_once()
    _, kwargs = mock_send.call_args
    assert kwargs["level"] == "info", "severity 'medium' must map to level 'info'"
    assert kwargs["alert_type"] == f"dyn_rsi_oversold_{symbol}", (
        "alert_type must be per-symbol so alert_channels' rate limit can't "
        "cross-suppress different symbols"
    )
    assert kwargs["source"] == "dyn_rsi_oversold", (
        "HM-DYNALERTS-HYGIENE: source must be dyn_<alert_type> (no symbol "
        "suffix, unlike alert_type) so Rung 1's isActionable() can classify "
        "on the type prefix alone"
    )


def test_no_telegram_module_import_in_dynamic_alerts():
    """Static + dynamic proof the telegram path is fully retired: no
    functional telegram reference remains (import, _send_telegram, module
    path), and exercising every hardcoded check + the user-definition
    _fire() path never imports engine.telegram_alerts.

    Matches the directive's literal verification command
    (`grep -n "telegram" engine/dynamic_alerts.py`, case-sensitive,
    lowercase pattern -- zero hits), NOT a blanket case-insensitive ban:
    the ticket cross-reference "HM-TELEGRAM-NTFY-UNIFY" legitimately
    appears in _notify's docstring (uppercase, explaining what this
    function replaced) and must not trip a false positive here."""
    src = Path(da.__file__).read_text()
    assert "telegram" not in src, (
        'dynamic_alerts.py must have zero lowercase "telegram" references '
        "(the directive's own grep check) -- functional remnants would be "
        "lowercase (import, function/module names); the uppercase "
        "HM-TELEGRAM-NTFY-UNIFY ticket reference is fine and intentionally "
        "not what this asserts against"
    )

    sys.modules.pop("engine.telegram_alerts", None)  # ensure a clean slate for this check
    symbol = "ZZZTESTNOTELEGRAM"
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "test_trader.db")
        with patch.object(da, "DB", db_path):
            da.ensure_alerts_table()
            da._alert_cooldown.clear()
            # Market-hours gate (item 8) is orthogonal to what this test
            # checks -- force it open so the _notify() path this test
            # exercises actually reaches send_alert, not just the gate.
            with patch("engine.market_calendar.is_within_alert_hours", return_value=True):
                with patch("engine.alert_channels.send_alert") as mock_send:
                    mock_send.return_value = {"ntfy": True}
                    da.check_rsi_extremes(symbol, price=10.0, indicators={"rsi": 75})
                    da.check_volume_spikes(symbol, price=10.0, indicators={"volume_ratio": 3.0})
                    da.check_macd_crossovers(symbol, price=10.0, indicators={"macd_histogram": 0.05})

    assert "engine.telegram_alerts" not in sys.modules, (
        "no dynamic_alerts code path may import engine.telegram_alerts"
    )


def test_db_notification_writes_source_as_type():
    """HM-DYNALERTS-HYGIENE 2026-07-07: _db_notification's inserted row's
    `type` column must equal the passed `source`, and fall back to
    "alert_channel" when source is omitted (backward compat for the 12
    other send_alert callers that don't pass it). Also the regression
    guard for the column-name bug found while verifying this INSERT
    against the live schema (prior code wrote a nonexistent `created_at`
    column and silently failed every call) -- if that regressed, both
    inserts below would raise sqlite3.OperationalError instead of the
    assertions failing cleanly."""
    from engine import alert_channels as ac

    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "test_trader.db")
        conn = sqlite3.connect(db_path)
        conn.execute("""CREATE TABLE notifications (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT DEFAULT CURRENT_TIMESTAMP,
            type        TEXT,
            severity    TEXT,
            title       TEXT,
            body        TEXT,
            icon        TEXT,
            agent_id    TEXT,
            acknowledged INTEGER DEFAULT 0
        )""")
        conn.commit()
        conn.close()

        with patch.object(ac, "_DB_PATH", db_path):
            ac._db_notification("t1", "b1", "info", source="dyn_rsi_oversold")
            ac._db_notification("t2", "b2", "warning")  # source omitted

        conn = sqlite3.connect(db_path)
        rows = conn.execute("SELECT title, type FROM notifications ORDER BY id").fetchall()
        conn.close()

    assert rows == [
        ("t1", "dyn_rsi_oversold"),
        ("t2", "alert_channel"),
    ]


def test_notify_failure_is_logged_not_silently_swallowed():
    """No-silent-catch check: if send_alert itself raises, _notify must
    catch it (never let a notification failure break the caller) but must
    NOT be a bare `except: pass` -- verified by confirming the console
    logger actually gets invoked on failure."""
    with patch("engine.market_calendar.is_within_alert_hours", return_value=True):
        with patch("engine.alert_channels.send_alert", side_effect=RuntimeError("boom")):
            with patch.object(da, "console") as mock_console:
                da._notify("test message", "high", "test_type", "ZZZ")
    mock_console.log.assert_called_once()
    logged = mock_console.log.call_args[0][0]
    assert "notify failed" in logged and "boom" in logged


# ─── HM-BUG-BATCH-2026-07-09 item 8: after-hours alert gating ────────────────
#
# Root cause of the reported symptom (trading-signal alerts firing at 8:42 PM
# AZ, well after the 4:00 PM ET close): main.py's scan cadence never fully
# stops overnight (it widens instead -- 5min market hours -> 30min evening ->
# 30min overnight), and dynamic_alerts.py's _notify() had no gate of its own,
# so the same handful of symbols kept re-alerting on stale after-close prices
# for hours. Fixed by gating _notify() on engine.market_calendar.
# is_within_alert_hours(). These tests freeze the clock at 23:42 ET (the
# literal reported scenario, translated from AZ to ET) and assert trading
# signals are suppressed while an ops/health sentinel alert -- which never
# goes through this gate -- still fires.

import datetime as _dt
from zoneinfo import ZoneInfo as _ZoneInfo

_ET = _ZoneInfo("America/New_York")
# 2026-07-09 is a Thursday, not a holiday -- isolates the assertion to the
# hour-of-day check alone, not weekend/holiday logic (covered separately below).
_FROZEN_2342_ET = _dt.datetime(2026, 7, 9, 23, 42, tzinfo=_ET)
_FROZEN_1000_ET = _dt.datetime(2026, 7, 9, 10, 0, tzinfo=_ET)  # regular session


def test_is_within_alert_hours_false_at_2342_et():
    """The literal reported scenario: 23:42 ET (8:42 PM AZ) is hours after
    the 16:00 ET close -- must read as outside alert hours."""
    from engine.market_calendar import is_within_alert_hours
    assert is_within_alert_hours(_FROZEN_2342_ET) is False


def test_is_within_alert_hours_true_during_regular_session():
    """Sanity check the same function isn't just always-False -- 10:00 ET
    on a weekday is squarely inside the default 9:30-16:00 window."""
    from engine.market_calendar import is_within_alert_hours
    assert is_within_alert_hours(_FROZEN_1000_ET) is True


def test_is_within_alert_hours_false_on_weekend_even_in_widened_config():
    """Weekends are excluded regardless of the configured hour range --
    widening config.TRADING_ALERT_HOURS_ET must never resurrect weekend
    alerts."""
    from engine.market_calendar import is_within_alert_hours
    saturday_midday_et = _dt.datetime(2026, 7, 11, 12, 0, tzinfo=_ET)  # 2026-07-11 is a Saturday
    with patch("config.TRADING_ALERT_HOURS_ET", (0.0, 24.0)):
        assert is_within_alert_hours(saturday_midday_et) is False


def test_dynamic_alerts_notify_suppressed_at_2342_et():
    """The actual alert pipeline, frozen at the reported time: _notify()
    must never reach send_alert() when 'now' resolves to 23:42 ET, proving
    the gate is wired into the real emission path, not just unit-tested in
    isolation."""
    with patch("engine.market_calendar._to_et", return_value=_FROZEN_2342_ET):
        with patch("engine.alert_channels.send_alert") as mock_send:
            da._notify("test message", "high", "test_type", "ZZZ")
    mock_send.assert_not_called()


def test_ops_sentinel_alert_not_gated_by_market_hours():
    """The other half of the assertion: an ops/health sentinel alert (going
    straight through engine.alert_channels.send_alert, the same function
    hm_ops_sentinel.py calls -- dynamic_alerts.py's gate is NOT in that
    path at all) must still fire at the same frozen 23:42 ET moment.
    Mocks the channel internals (never touch real ntfy/DB) and just proves
    send_alert() dispatches unconditionally regardless of time of day."""
    from engine import alert_channels as ac
    with patch.object(ac, "_send_ntfy", return_value=True) as mock_ntfy, \
         patch.object(ac, "_db_notification") as mock_db, \
         patch.object(ac, "_rate_ok", return_value=True), \
         patch.object(ac, "_mark_rate_limit_sent"):
        result = ac.send_alert(
            "signals_v2 pending queue -- oldest-pending age=368h (> 48h)",
            level=ac.AlertLevel.WARNING,
            alert_type="sentinel_signals_v2_queue",
        )
    assert result.get("ntfy") is True
    mock_ntfy.assert_called()
    mock_db.assert_called_once()
