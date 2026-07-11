"""HM-NTFY-IPV6-NOROUTE-SWEEP 2026-07-10 — batch 1 of 2 (delegate/Pattern A).

Covers the 13 files fixed by delegating to the already-hardened
engine.alert_channels._send_ntfy() (forces IPv4) instead of a separate
requests.post()/urllib.request implementation of the same POST. Each test
mocks _send_ntfy and asserts it's called with the right title/message/
priority/topic -- the hardening itself is covered by
tests/test_engine_ntfy_ipv6_fix.py and the original alert_channels fix.
"""
from __future__ import annotations

import sys
import threading
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class _ImmediateThread:
    """Stand-in for threading.Thread that runs target() synchronously in
    start(), so fire-and-forget senders become testable without a real
    background thread / sleep-and-hope."""

    def __init__(self, target=None, args=(), kwargs=None, daemon=None):
        self._target = target
        self._args = args
        self._kwargs = kwargs or {}

    def start(self):
        self._target(*self._args, **self._kwargs)


# ── agents/scotty/scotty.py ─────────────────────────────────────────────────
def test_scotty_send_ntfy_routes_through_hardened_send_ntfy():
    import agents.scotty.scotty as scotty
    from agents.scotty.scoring import SqueezeScore, TickerSnapshot

    snap = TickerSnapshot(ticker="GME", short_pct=35.0, float_shares_m=50.0,
                           days_to_cover=3.0, vol_ratio=5.0)
    alert = SqueezeScore(ticker="GME", score=4, signals={}, snapshot=snap)

    with patch("engine.alert_channels._send_ntfy") as mock_send:
        scotty._send_ntfy([alert])

    mock_send.assert_called_once()
    kwargs = mock_send.call_args.kwargs
    assert kwargs["title"] == "Squeeze pressure critical"
    assert "GME" in kwargs["message"]
    assert kwargs["priority"] == "high"
    assert kwargs["topic"] == scotty.NTFY_TOPIC


# ── engine/squeeze_scanner.py ───────────────────────────────────────────────
def test_squeeze_scanner_ntfy_priority_candidates_routes_through_hardened_send(tmp_path, monkeypatch):
    import sqlite3
    import engine.squeeze_scanner as ss

    # Pin NTFY_ADMIN_TOPIC explicitly -- the real .env sets it to
    # Ollie-Alert-35, and dotenv loading elsewhere in the suite can leak
    # that into this process's environ ahead of this test.
    monkeypatch.setenv("NTFY_ADMIN_TOPIC", "ollietrades-admin")

    db_path = str(tmp_path / "squeeze_test.db")
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE squeeze_watch (
            id INTEGER PRIMARY KEY, symbol TEXT, composite_score REAL,
            short_pct REAL, float_m REAL, vol_ratio REAL, rsi REAL,
            price_at_scan REAL, threshold_tier TEXT, ntfy_sent INTEGER,
            ntfy_deferred INTEGER, dismissed INTEGER, scan_ts TEXT
        )
    """)
    conn.execute("""
        INSERT INTO squeeze_watch (symbol, composite_score, short_pct, float_m,
            vol_ratio, rsi, price_at_scan, threshold_tier, ntfy_sent,
            ntfy_deferred, dismissed, scan_ts)
        VALUES ('GME', 90, 35.0, 50.0, 5.0, 60.0, 20.0, 'PRIORITY', 0, 0, 0, '2026-07-10')
    """)
    conn.commit()
    conn.close()

    with patch("engine.alert_channels._send_ntfy") as mock_send:
        fired = ss._ntfy_priority_candidates(db_path=db_path)

    assert fired == 1
    mock_send.assert_called_once()
    kwargs = mock_send.call_args.kwargs
    assert "GME" in kwargs["title"]
    assert kwargs["topic"] == "ollietrades-admin"


# ── engine/alpha_signals.py ─────────────────────────────────────────────────
def test_alpha_signals_ntfy_send_routes_through_hardened_send_ntfy():
    import engine.alpha_signals as als

    with patch.object(als.threading, "Thread", _ImmediateThread):
        with patch("engine.alert_channels._send_ntfy") as mock_send:
            als._ntfy_send("Dilithium Crystal — ALPHA BUY", "body text",
                            priority=4, tags=["white_check_mark", "crystal_ball"])

    mock_send.assert_called_once()
    kwargs = mock_send.call_args.kwargs
    assert kwargs["priority"] == "high"
    assert kwargs["tags"] == "white_check_mark,crystal_ball"
    assert kwargs["topic"] == als._NTFY_TOPIC


# ── engine/morning_briefing.py ──────────────────────────────────────────────
def test_morning_briefing_push_admin_ntfy_routes_through_hardened_send_ntfy():
    import engine.morning_briefing as mb

    with patch.object(mb.threading, "Thread", _ImmediateThread):
        with patch("engine.alert_channels._send_ntfy") as mock_send:
            mb._push_admin_ntfy("Morning Intel", "body text", priority=4)

    mock_send.assert_called_once()
    kwargs = mock_send.call_args.kwargs
    assert kwargs["priority"] == "high"
    assert kwargs["tags"] == "newspaper"
    assert kwargs["topic"] == mb._ADMIN_NTFY_TOPIC


# ── engine/dayblade_scanner.py ──────────────────────────────────────────────
def test_dayblade_scanner_push_ntfy_routes_through_hardened_send_ntfy(monkeypatch):
    import engine.dayblade_scanner as db

    monkeypatch.setenv("NTFY_URL", "https://ntfy.sh/ollietrades-dayblade")

    with patch("engine.alert_channels._send_ntfy") as mock_send:
        db._push_ntfy("CALL", 550.0, ["vwap reclaim"], 80, 551.0, 1.25)

    mock_send.assert_called_once()
    kwargs = mock_send.call_args.kwargs
    assert kwargs["topic"] == "ollietrades-dayblade"
    assert kwargs["priority"] == "high"


def test_dayblade_scanner_push_ntfy_noop_without_url(monkeypatch):
    import engine.dayblade_scanner as db

    monkeypatch.delenv("NTFY_URL", raising=False)

    with patch("engine.alert_channels._send_ntfy") as mock_send:
        db._push_ntfy("CALL", 550.0, ["vwap reclaim"], 80, 551.0, 1.25)

    mock_send.assert_not_called()


# ── engine/archer/alerts.py ─────────────────────────────────────────────────
def test_archer_alerts_run_cycle_routes_through_hardened_send_ntfy(tmp_path, monkeypatch):
    import engine.archer.alerts as aa

    monkeypatch.setattr(aa, "TRADER_DB", tmp_path / "trader_test.db")
    monkeypatch.setattr(aa, "_candidates", lambda: [
        {"tier": "RED", "symbol": "NVDA", "systems": ["convergence"], "count": 5},
    ])
    monkeypatch.setattr(aa, "_save_notification", lambda *a, **k: None)

    with patch("engine.alert_channels._send_ntfy") as mock_send:
        result = aa.run_alert_cycle()

    assert result["red"] == 1
    mock_send.assert_called_once()
    kwargs = mock_send.call_args.kwargs
    assert "NVDA" in kwargs["title"]
    assert kwargs["priority"] == "high"
    assert kwargs["topic"] == "ollietrades-admin"


# ── engine/archer_morning_synthesis.py ──────────────────────────────────────
def test_archer_morning_synthesis_send_briefing_routes_through_hardened_send_ntfy(monkeypatch):
    import engine.archer_morning_synthesis as ams

    monkeypatch.setattr(ams, "init_table", lambda: None)
    monkeypatch.setattr(ams, "build_briefing", lambda: "Archer out. Good hunting, Admiral.")

    with patch("engine.alert_channels._send_ntfy") as mock_send:
        ams.send_briefing()

    assert mock_send.call_count == 2
    topics = {c.kwargs["topic"] for c in mock_send.call_args_list}
    assert topics == {"ollietrades-admin", "ollietrades-crew"}


# ── engine/universe_refresh.py ──────────────────────────────────────────────
def test_universe_refresh_ntfy_routes_through_hardened_send_ntfy():
    import engine.universe_refresh as ur

    with patch("engine.alert_channels._send_ntfy") as mock_send:
        ur._ntfy("refresh aborted", priority="high")

    mock_send.assert_called_once()
    kwargs = mock_send.call_args.kwargs
    assert kwargs["title"] == "HM-AQ-beta refresh"
    assert kwargs["topic"] == "ollietrades-admin"
    assert kwargs["priority"] == "high"


# ── engine/orcl_gex_alerts.py ───────────────────────────────────────────────
def test_orcl_gex_alerts_send_ntfy_routes_through_hardened_send_ntfy():
    import engine.orcl_gex_alerts as oga

    with patch("engine.alert_channels._send_ntfy") as mock_send:
        oga.send_ntfy("gamma flip crossed", priority="urgent")

    mock_send.assert_called_once()
    kwargs = mock_send.call_args.kwargs
    assert kwargs["title"] == "ORCL GEX Alert"
    assert kwargs["topic"] == oga.NTFY_TOPIC
    assert kwargs["priority"] == "urgent"


# ── engine/fred_data.py ─────────────────────────────────────────────────────
def test_fred_data_ntfy_carts_release_routes_through_hardened_send_ntfy():
    import engine.fred_data as fd

    with patch("engine.alert_channels._send_ntfy") as mock_send:
        fd._ntfy_carts_release("CARTS Nowcast", 1.5, 1.2)

    mock_send.assert_called_once()
    kwargs = mock_send.call_args.kwargs
    assert kwargs["title"] == "CARTS nowcast updated"
    assert kwargs["topic"] == fd._CARTS_NTFY_TOPIC
    assert "1.50" in kwargs["message"] or "+1.50" in kwargs["message"]


# ── engine/fleet_auditor.py ─────────────────────────────────────────────────
def test_fleet_auditor_push_ntfy_routes_through_hardened_send_ntfy():
    import engine.fleet_auditor as fa

    with patch("engine.alert_channels._send_ntfy") as mock_send:
        fa._push_ntfy("Fleet DOWN", "trader process not responding", priority="high")

    mock_send.assert_called_once()
    kwargs = mock_send.call_args.kwargs
    assert kwargs["title"] == "Fleet DOWN"
    assert kwargs["topic"] == fa.NTFY_TOPIC
    assert kwargs["tags"] == "warning,fleet,auditor"


# ── engine/universe_scanner.py ──────────────────────────────────────────────
def test_universe_scanner_sp500_alert_routes_through_hardened_send_ntfy():
    import engine.universe_scanner as us

    us._sp500_alerted = False  # reset the once-per-process dedup guard

    with patch("engine.alert_channels._send_ntfy") as mock_send:
        us._sp500_alert("stale source detected")

    mock_send.assert_called_once()
    kwargs = mock_send.call_args.kwargs
    assert kwargs["title"] == "S&P500 source degraded"
    assert kwargs["priority"] == "high"


def test_universe_scanner_sp500_alert_dedupes_within_process():
    import engine.universe_scanner as us

    us._sp500_alerted = False
    with patch("engine.alert_channels._send_ntfy") as mock_send:
        us._sp500_alert("first")
        us._sp500_alert("second")

    mock_send.assert_called_once()  # only the first fires


# ── signal-center/server.py ─────────────────────────────────────────────────
# Runs under its own Python 3.9 venv (venv/bin/python3) with flask + pyotp,
# neither of which is installed in this repo's main .venv test environment
# (HM-NTFY-IPV6-NOROUTE-SWEEP 2026-07-10) -- skip gracefully rather than
# failing collection under the main suite.
import pytest  # noqa: E402


def test_signal_center_morpheus_log_action_routes_through_hardened_send_ntfy():
    pytest.importorskip("flask")
    pytest.importorskip("pyotp")

    sc_dir = str(ROOT / "signal-center")
    if sc_dir not in sys.path:
        sys.path.insert(0, sc_dir)
    import server as sc

    class _FakeCursor:
        lastrowid = 1

    class _FakeDB:
        def execute(self, *a, **k):
            return _FakeCursor()

        def commit(self):
            pass

        def close(self):
            pass

    with patch.object(sc, "get_db", lambda: _FakeDB()):
        with patch("engine.alert_channels._send_ntfy") as mock_send:
            sc._morpheus_log_action("kill_switch", "admiral", "FAILED", {"reason": "test"})

    mock_send.assert_called_once()
    kwargs = mock_send.call_args.kwargs
    assert "kill_switch" in kwargs["title"]
    assert kwargs["priority"] == "high"
