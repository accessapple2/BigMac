"""HM-NTFY-IPV6-NOROUTE-SWEEP 2026-07-10 — batch 2 of 2 (standalone/Pattern B).

Covers the 14 files fixed with a self-contained IPv4-force lock+monkeypatch
(zero engine/ dependency, mirroring watchdog.py's own fix). Each test
verifies socket.getaddrinfo is forced to AF_INET-only for the duration of
the send and restored afterward -- the actual bug (this box has no working
IPv6 route to ntfy.sh, HM-NTFY-IPV6-NOROUTE 2026-07-07) is
address-family-ordering-dependent, so the only reliable regression check is
"was the patch installed and removed correctly", not "did a real socket
connect".
"""
from __future__ import annotations

import socket
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fake_urlopen_response():
    resp = MagicMock()
    resp.status = 200
    resp.__enter__ = lambda self: resp
    resp.__exit__ = lambda self, *a: False
    return resp


# ── scripts/ghost_advisor.py ────────────────────────────────────────────────
def test_ghost_advisor_send_ntfy_forces_and_restores_ipv4(monkeypatch):
    import scripts.ghost_advisor as ga

    original = socket.getaddrinfo
    seen = {}

    def fake_urlopen(req, timeout=3):
        seen["getaddrinfo"] = socket.getaddrinfo
        return _fake_urlopen_response()

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        ga.send_ntfy("Title", "body")

    assert seen["getaddrinfo"] is ga._ipv4_only_getaddrinfo
    assert socket.getaddrinfo is original


# ── scripts/uhura_watch.py ──────────────────────────────────────────────────
def test_uhura_watch_send_ntfy_forces_and_restores_ipv4(monkeypatch):
    import scripts.uhura_watch as uw

    monkeypatch.setattr(uw, "_load_dedup_state", lambda: {})
    monkeypatch.setattr(uw, "_save_dedup_state", lambda state: None)

    original = socket.getaddrinfo
    seen = {}

    def fake_urlopen(req, timeout=5):
        seen["getaddrinfo"] = socket.getaddrinfo
        return _fake_urlopen_response()

    with patch("scripts.uhura_watch.urlopen", side_effect=fake_urlopen):
        uw.send_ntfy(["anomaly one"])

    assert seen["getaddrinfo"] is uw._ipv4_only_getaddrinfo
    assert socket.getaddrinfo is original


# ── scripts/q_market_open_ping.py ───────────────────────────────────────────
def test_q_market_open_ping_ntfy_forces_and_restores_ipv4():
    import scripts.q_market_open_ping as qp

    original = socket.getaddrinfo
    seen = {}

    def fake_urlopen(req, timeout=8):
        seen["getaddrinfo"] = socket.getaddrinfo
        return _fake_urlopen_response()

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        qp._ntfy("Title", "body")

    assert seen["getaddrinfo"] is qp._ipv4_only_getaddrinfo
    assert socket.getaddrinfo is original


# ── scripts/q_dissent_watch.py ──────────────────────────────────────────────
def test_q_dissent_watch_ntfy_forces_and_restores_ipv4():
    import scripts.q_dissent_watch as qd

    original = socket.getaddrinfo
    seen = {}

    def fake_urlopen(req, timeout=8):
        seen["getaddrinfo"] = socket.getaddrinfo
        return _fake_urlopen_response()

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        qd._ntfy("Title", "body")

    assert seen["getaddrinfo"] is qd._ipv4_only_getaddrinfo
    assert socket.getaddrinfo is original


# ── scripts/import_schwab_csv.py ────────────────────────────────────────────
def test_import_schwab_csv_ntfy_forces_and_restores_ipv4():
    import scripts.import_schwab_csv as isc

    original = socket.getaddrinfo
    seen = {}

    def fake_urlopen(req, timeout=5):
        seen["getaddrinfo"] = socket.getaddrinfo
        return _fake_urlopen_response()

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        isc._ntfy("some message")

    assert seen["getaddrinfo"] is isc._ipv4_only_getaddrinfo
    assert socket.getaddrinfo is original


# ── scripts/model_watcher.py ────────────────────────────────────────────────
def test_model_watcher_ntfy_send_forces_and_restores_ipv4():
    import scripts.model_watcher as mw

    original = socket.getaddrinfo
    seen = {}

    def fake_urlopen(req, timeout=mw.HTTP_TIMEOUT):
        seen["getaddrinfo"] = socket.getaddrinfo
        return _fake_urlopen_response()

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        mw.ntfy_send("ollietrades-admin", "Title", "body")

    assert seen["getaddrinfo"] is mw._ipv4_only_getaddrinfo
    assert socket.getaddrinfo is original


def test_model_watcher_ntfy_send_noop_without_topic():
    import scripts.model_watcher as mw

    with patch("urllib.request.urlopen") as mock_urlopen:
        mw.ntfy_send("", "Title", "body")

    mock_urlopen.assert_not_called()


# ── scripts/model_sweep_v2.py ───────────────────────────────────────────────
# HM-VENV-BACKTEST-ISOLATION: model_sweep_v2.py imports vectorbt, which only
# lives in .venv-backtest (never the serving/test venv) — skip under the main
# suite rather than importing it where it doesn't belong.
def test_model_sweep_v2_push_ntfy_forces_and_restores_ipv4():
    pytest.importorskip("vectorbt")
    import scripts.model_sweep_v2 as ms

    original = socket.getaddrinfo
    seen = {}

    def fake_urlopen(req, timeout=6):
        seen["getaddrinfo"] = socket.getaddrinfo
        return _fake_urlopen_response()

    with patch("scripts.model_sweep_v2.urlopen", side_effect=fake_urlopen):
        ms.push_ntfy("Title", "body")

    assert seen["getaddrinfo"] is ms._ipv4_only_getaddrinfo
    assert socket.getaddrinfo is original


# ── scripts/schwab_drawdown_alert.py ────────────────────────────────────────
def test_schwab_drawdown_alert_ntfy_forces_and_restores_ipv4():
    import scripts.schwab_drawdown_alert as sda

    original = socket.getaddrinfo
    seen = {}

    class FakeResp:
        pass

    def fake_post(url, data=None, headers=None, timeout=None):
        seen["getaddrinfo"] = socket.getaddrinfo
        return FakeResp()

    with patch("scripts.schwab_drawdown_alert.requests.post", side_effect=fake_post):
        sda._ntfy("title", "body")

    assert seen["getaddrinfo"] is sda._ipv4_only_getaddrinfo
    assert socket.getaddrinfo is original


# ── scripts/fleet_heartbeat.py ──────────────────────────────────────────────
def test_fleet_heartbeat_ntfy_forces_and_restores_ipv4():
    import scripts.fleet_heartbeat as fh

    original = socket.getaddrinfo
    seen = {}

    def fake_post(url, data=None, headers=None, timeout=None):
        seen["getaddrinfo"] = socket.getaddrinfo
        return MagicMock()

    with patch("scripts.fleet_heartbeat.requests.post", side_effect=fake_post):
        fh._ntfy("title", "body")

    assert seen["getaddrinfo"] is fh._ipv4_only_getaddrinfo
    assert socket.getaddrinfo is original


# ── scripts/kimi_cut_watch.py ───────────────────────────────────────────────
def test_kimi_cut_watch_ntfy_forces_and_restores_ipv4():
    import scripts.kimi_cut_watch as kc

    original = socket.getaddrinfo
    seen = {}

    def fake_urlopen(req, timeout=8):
        seen["getaddrinfo"] = socket.getaddrinfo
        return _fake_urlopen_response()

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        kc._ntfy("Title", "body")

    assert seen["getaddrinfo"] is kc._ipv4_only_getaddrinfo
    assert socket.getaddrinfo is original


# ── scripts/iren_flip_watch.py ──────────────────────────────────────────────
def test_iren_flip_watch_ntfy_forces_and_restores_ipv4():
    import scripts.iren_flip_watch as ifw

    original = socket.getaddrinfo
    seen = {}

    def fake_urlopen(req, timeout=10):
        seen["getaddrinfo"] = socket.getaddrinfo
        return _fake_urlopen_response()

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        ifw._ntfy("Title", "body")

    assert seen["getaddrinfo"] is ifw._ipv4_only_getaddrinfo
    assert socket.getaddrinfo is original


# ── scripts/learning/check_pipeline.py ──────────────────────────────────────
def test_check_pipeline_ping_forces_and_restores_ipv4():
    import scripts.learning.check_pipeline as cp

    original = socket.getaddrinfo
    seen = {}

    def fake_post(url, data=None, headers=None, timeout=None):
        seen["getaddrinfo"] = socket.getaddrinfo
        return MagicMock()

    with patch("scripts.learning.check_pipeline.requests.post", side_effect=fake_post):
        ok = cp.ping("Title", "body")

    assert ok is True
    assert seen["getaddrinfo"] is cp._ipv4_only_getaddrinfo
    assert socket.getaddrinfo is original


# ── healthcheck.py ───────────────────────────────────────────────────────────
def test_healthcheck_push_ntfy_forces_and_restores_ipv4():
    import healthcheck as hc

    original = socket.getaddrinfo
    seen = {}

    def fake_urlopen(req, timeout=6):
        seen["getaddrinfo"] = socket.getaddrinfo
        return _fake_urlopen_response()

    with patch("healthcheck.urlopen", side_effect=fake_urlopen):
        hc.push_ntfy("Title", "body")

    assert seen["getaddrinfo"] is hc._ipv4_only_getaddrinfo
    assert socket.getaddrinfo is original


# ── swingdesk/shadow_autopilot.py ───────────────────────────────────────────
def test_shadow_autopilot_ntfy_admin_forces_and_restores_ipv4():
    # shadow_autopilot.py does `from options_engine import ...` (bare name,
    # not `swingdesk.options_engine`) -- it expects swingdesk/ itself on
    # sys.path, same as its own production invocation.
    swingdesk_dir = str(ROOT / "swingdesk")
    if swingdesk_dir not in sys.path:
        sys.path.insert(0, swingdesk_dir)
    import swingdesk.shadow_autopilot as sa

    original = socket.getaddrinfo
    seen = {}

    def fake_urlopen(req, timeout=8):
        seen["getaddrinfo"] = socket.getaddrinfo
        return _fake_urlopen_response()

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        ok = sa._ntfy_admin("Title", "body")

    assert ok is True
    assert seen["getaddrinfo"] is sa._ipv4_only_getaddrinfo
    assert socket.getaddrinfo is original


# ── restoration-on-failure spot-check (shared code path, one representative) ─
def test_ghost_advisor_restores_getaddrinfo_even_on_send_failure():
    import scripts.ghost_advisor as ga

    original = socket.getaddrinfo
    with patch("urllib.request.urlopen", side_effect=OSError("No route to host")):
        ga.send_ntfy("Title", "body")  # must not raise

    assert socket.getaddrinfo is original


def test_healthcheck_restores_getaddrinfo_even_on_send_failure():
    import healthcheck as hc

    original = socket.getaddrinfo
    with patch("healthcheck.urlopen", side_effect=OSError("No route to host")):
        hc.push_ntfy("Title", "body")  # must not raise

    assert socket.getaddrinfo is original
