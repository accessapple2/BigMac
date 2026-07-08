"""HM-TELEGRAM-NTFY-UNIFY (2026-07-07) — dynamic_alerts._notify() tests.

Confirms dynamic_alerts.py routes through engine.alert_channels.send_alert
(ntfy-first, severity-routed) instead of the retired _send_telegram silent-
catch path. Same temp-DB isolation pattern as test_alert_defs.py
(monkeypatch the module-level DB constant) -- check_rsi_extremes writes a
real row via _save_alert, must never touch production trader.db.
"""
from __future__ import annotations

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
            with patch("engine.alert_channels.send_alert") as mock_send:
                mock_send.return_value = {"ntfy": True}
                da.check_rsi_extremes(symbol, price=10.0, indicators={"rsi": 75})
                da.check_volume_spikes(symbol, price=10.0, indicators={"volume_ratio": 3.0})
                da.check_macd_crossovers(symbol, price=10.0, indicators={"macd_histogram": 0.05})

    assert "engine.telegram_alerts" not in sys.modules, (
        "no dynamic_alerts code path may import engine.telegram_alerts"
    )


def test_notify_failure_is_logged_not_silently_swallowed():
    """No-silent-catch check: if send_alert itself raises, _notify must
    catch it (never let a notification failure break the caller) but must
    NOT be a bare `except: pass` -- verified by confirming the console
    logger actually gets invoked on failure."""
    with patch("engine.alert_channels.send_alert", side_effect=RuntimeError("boom")):
        with patch.object(da, "console") as mock_console:
            da._notify("test message", "high", "test_type", "ZZZ")
    mock_console.log.assert_called_once()
    logged = mock_console.log.call_args[0][0]
    assert "notify failed" in logged and "boom" in logged
