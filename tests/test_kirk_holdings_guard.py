"""HM-FIX-REAL-HOLDINGS (FIX-1) — reader-guard regression tests.

Locks the contract that engine.kirk_advisory._load_real_holdings refuses to hand
back holdings it cannot prove are live-Schwab-sourced AND fresh, and — critically —
does NOT false-trip on weekends/holidays (the bug the old wall-clock rule had).

Adapted to the REAL on-disk shape (nested accounts.schwab block), not the flat
shape in the original directive.
"""
import json
from datetime import timedelta
from types import SimpleNamespace

import pytest

import engine.kirk_advisory as k
from engine.market_calendar import ET


# ── helpers ──────────────────────────────────────────────────────────────────
def _et(y, m, d, hh, mm):
    """Timezone-aware ET datetime."""
    from datetime import datetime
    return ET.localize(datetime(y, m, d, hh, mm))


def _write_holdings(path, *, source="live_api", positions=None, with_source=True):
    block = {
        "label": "Schwab",
        "role": "primary",
        "is_active": True,
        "cash_balance": 28053.9,
        "positions": positions if positions is not None else [],
        "last_updated": "2026-06-05 21:50:55 EDT",
    }
    if with_source:
        block["source"] = source
    data = {"last_updated": "2026-06-05", "accounts": {"schwab": block}}
    path.write_text(json.dumps(data))
    return path


@pytest.fixture
def holdings(tmp_path, monkeypatch):
    """Point the loader at a tmp file and capture (never send) any alert.

    Returns a namespace with `.path` (the tmp json file) and `.fired`
    (list of (reason, detail) tuples the alert hook would have sent).
    """
    p = tmp_path / "real_holdings.json"
    monkeypatch.setattr(k, "REAL_HOLDINGS_PATH", p)
    fired = []
    monkeypatch.setattr(k, "_alert_untrusted_holdings",
                        lambda reason, detail: fired.append((reason, detail)))
    return SimpleNamespace(path=p, fired=fired)


# ── source-provenance guard (deterministic; precedes freshness) ──────────────
def test_refuses_manual_source(holdings):
    _write_holdings(holdings.path, source="manual")
    res = k._load_real_holdings()
    assert res["stale"] is True
    assert res["stale_reason"] == "untrusted_source"
    assert res["positions"] == []
    assert holdings.fired and holdings.fired[0][0] == "untrusted_source"


def test_refuses_missing_source(holdings):
    _write_holdings(holdings.path, with_source=False)
    res = k._load_real_holdings()
    assert res["stale"] is True
    assert res["stale_reason"] == "untrusted_source"


def test_refuses_missing_file(holdings):
    # never written → does not exist
    res = k._load_real_holdings()
    assert res["stale"] is True
    assert res["stale_reason"] == "file_missing"
    assert holdings.fired and holdings.fired[0][0] == "file_missing"


def test_refuses_parse_error(holdings):
    holdings.path.write_text("{not valid json")
    res = k._load_real_holdings()
    assert res["stale"] is True
    assert res["stale_reason"] == "parse_error"


# ── accepts fresh, real-sourced data (mtime=now is trivially fresh) ──────────
def test_accepts_fresh_live_api(holdings):
    _write_holdings(holdings.path, source="live_api",
                    positions=[{"symbol": "AAPL", "qty": 10, "avg_cost": 150.0}])
    res = k._load_real_holdings()
    assert res["stale"] is False
    assert res["holdings_source"] == "live_api"
    assert [p["symbol"] for p in res["positions"]] == ["AAPL"]
    assert not holdings.fired


def test_accepts_fresh_csv_snapshot(holdings):
    _write_holdings(holdings.path, source="csv_snapshot")
    res = k._load_real_holdings()
    assert res["stale"] is False
    assert res["holdings_source"] == "csv_snapshot"


# ── market-aware freshness (the weekend non-false-trip is the headline case) ──
def test_weekend_friday_data_is_not_stale():
    """Sun 2026-06-07: Friday-evening data is the freshest possible — NOT stale."""
    now = _et(2026, 6, 7, 12, 0)                 # Sunday
    written = _et(2026, 6, 5, 21, 50).timestamp()  # Friday 21:50 ET
    assert k._holdings_stale_reason(written, now_et=now) is None


def test_monday_preopen_friday_data_is_not_stale():
    now = _et(2026, 6, 8, 8, 30)                  # Monday pre-open (closed)
    written = _et(2026, 6, 5, 21, 50).timestamp()  # Friday
    assert k._holdings_stale_reason(written, now_et=now) is None


def test_stale_when_a_full_session_elapsed():
    """Tue evening with data still from Friday → a session closed un-synced → stale."""
    now = _et(2026, 6, 9, 20, 0)                  # Tuesday after-hours (closed)
    written = _et(2026, 6, 5, 21, 50).timestamp()  # Friday
    assert k._holdings_stale_reason(written, now_et=now) == "holdings_stale"


def test_rth_recent_write_is_fresh():
    now = _et(2026, 6, 10, 11, 0)                 # Wednesday, market open
    written = (now - timedelta(minutes=30)).timestamp()
    assert k._holdings_stale_reason(written, now_et=now) is None


def test_rth_old_write_is_stale():
    now = _et(2026, 6, 10, 11, 0)                 # Wednesday, market open
    written = (now - timedelta(minutes=180)).timestamp()  # 6+ missed cron ticks
    assert k._holdings_stale_reason(written, now_et=now) == "holdings_stale_rth"


def test_last_market_close_skips_weekend():
    sunday = _et(2026, 6, 7, 12, 0)
    lc = k._last_market_close(sunday)
    assert lc is not None
    assert lc.date().isoformat() == "2026-06-05"  # Friday
    assert (lc.hour, lc.minute) == (16, 0)
