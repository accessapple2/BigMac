"""tests/test_gex_freshness_gate.py -- HM-GEX-FRESHNESS-GATE-2026-09-01.

Captain's finding: engine/ready_room.py and engine/dynamic_advisor.py each
independently overlaid engine.canonical_gex.canonical_gex()'s result onto
their own base values whenever it returned no "error" -- but a dead daily
collector (HM-GEX-RETIRED, 07-21) still returns a structurally valid,
error-free row forever after, so both silently kept overwriting live
Alpaca-derived spot/walls/flip with a frozen 6-week-old snapshot.

Fix: engine.canonical_gex.canonical_gex_if_fresh() is the ONE shared
freshness gate both call sites now use (consolidated, not duplicated).
This file tests, in order:
  1. snapshot_age_days() -- the age-parsing primitive.
  2. canonical_gex_if_fresh() -- the gate itself.
  3. ready_room.py's overlay: fresh -> applies, stale -> falls through.
  4. dynamic_advisor.py's overlay: same two cases.
  5. dashboard/app.py's _gex_age_days/_gex_is_stale delegate correctly.

Never touches the real trader.db -- ready_room's DB path is patched to a
tmp_path file. No live network calls -- VIX/P-C-ratio/gex_calculator are
all mocked.
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import engine.canonical_gex as canonical_gex_mod  # noqa: E402
import engine.ready_room as ready_room  # noqa: E402
import engine.dynamic_advisor as dynamic_advisor  # noqa: E402


# ── 1. snapshot_age_days ───────────────────────────────────────────────────

def test_snapshot_age_days_none_and_unparseable():
    assert canonical_gex_mod.snapshot_age_days(None) is None
    assert canonical_gex_mod.snapshot_age_days("") is None
    assert canonical_gex_mod.snapshot_age_days("garbage") is None


def test_snapshot_age_days_fresh_vs_frozen():
    now = datetime.now(timezone.utc)
    assert canonical_gex_mod.snapshot_age_days(now.isoformat()) < 0.01
    # the literal live symptom -- HM-GEX-RETIRED's frozen row
    assert canonical_gex_mod.snapshot_age_days("2026-07-21T13:05:00") > 40


# ── 2. canonical_gex_if_fresh ───────────────────────────────────────────────

def _mk(as_of, **kw):
    d = {"underlying": "SPY", "_asof": as_of, "_src": "test"}
    d.update(kw)
    return d


def test_if_fresh_returns_dict_when_within_threshold():
    fresh_asof = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    with patch.object(canonical_gex_mod, "canonical_gex", return_value=_mk(fresh_asof, spot=740)):
        result = canonical_gex_mod.canonical_gex_if_fresh("SPY")
    assert result is not None
    assert result["spot"] == 740


def test_if_fresh_returns_none_when_stale():
    stale_asof = "2026-07-21T13:05:00"
    with patch.object(canonical_gex_mod, "canonical_gex", return_value=_mk(stale_asof, spot=740)):
        result = canonical_gex_mod.canonical_gex_if_fresh("SPY")
    assert result is None


def test_if_fresh_returns_none_on_error():
    with patch.object(canonical_gex_mod, "canonical_gex", return_value={"underlying": "SPY", "error": "boom"}):
        assert canonical_gex_mod.canonical_gex_if_fresh("SPY") is None


def test_if_fresh_returns_none_on_missing_asof():
    with patch.object(canonical_gex_mod, "canonical_gex", return_value={"underlying": "SPY", "spot": 740}):
        assert canonical_gex_mod.canonical_gex_if_fresh("SPY") is None


def test_threshold_is_one_day_and_visible():
    """Not a buried magic number -- importable, named, documented."""
    assert canonical_gex_mod.CANONICAL_GEX_MAX_AGE_DAYS == 1.0


# ── 3. ready_room.py overlay: fresh applies, stale falls through ──────────

class _FakeProfile:
    """Mimics gex_calculator.GEXProfile's fields ready_room.py reads."""
    def __init__(self):
        self.spot_price = 100.0
        self.call_wall = 110.0
        self.put_wall = 90.0
        self.zero_gamma_level = 95.0
        self.max_gamma_strike = 105.0
        self.total_gex = 1.0e9
        self.levels = []


def _make_ready_room_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "trader.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""CREATE TABLE ready_room_briefings (
        id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, session_date TEXT,
        session_time TEXT, spot_price REAL, call_wall REAL, put_wall REAL,
        max_pain REAL, gamma_flip REAL, max_gamma_strike REAL, total_gex REAL,
        pc_ratio REAL, vix REAL, session_type TEXT, signals_json TEXT,
        gameplan TEXT, created_at TEXT DEFAULT (datetime('now')))""")
    conn.commit()
    conn.close()
    return db_path


def _run_ready_room_briefing(tmp_path, canonical_fresh_return):
    ready_room._CACHE.clear()
    db_path = _make_ready_room_db(tmp_path)
    fake_gex_calculator = MagicMock()
    fake_gex_calculator.compute_gex_sync.return_value = _FakeProfile()
    with patch.object(ready_room, "DB", str(db_path)), \
         patch.dict(sys.modules, {"gex_calculator": fake_gex_calculator}), \
         patch("engine.canonical_gex.canonical_gex_if_fresh", return_value=canonical_fresh_return), \
         patch.object(ready_room, "_get_vix", return_value=18.0), \
         patch.object(ready_room, "_get_pc_ratio_cboe", return_value=1.0):
        return ready_room.generate_ready_room_briefing(force=True)


def test_ready_room_overlay_applies_when_fresh(tmp_path):
    fresh = _mk(
        (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(),
        spot=200.0, call_wall=210.0, put_wall=190.0, gamma_flip=195.0,
        king_node=205.0, total_gex=2.0e9,
    )
    result = _run_ready_room_briefing(tmp_path, fresh)
    assert result["spot_price"] == 200.0
    assert result["call_wall"] == 210.0
    assert result["put_wall"] == 190.0
    assert result["gamma_flip"] == 195.0
    assert result["max_gamma_strike"] == 205.0


def test_ready_room_overlay_falls_through_when_stale(tmp_path):
    """The exact live symptom: canonical_gex_if_fresh() returns None (as it
    does for real today), so the legacy Alpaca profile values must stand
    untouched -- not the frozen canonical snapshot."""
    result = _run_ready_room_briefing(tmp_path, None)
    assert result["spot_price"] == 100.0
    assert result["call_wall"] == 110.0
    assert result["put_wall"] == 90.0
    assert result["gamma_flip"] == 95.0
    assert result["max_gamma_strike"] == 105.0


# ── 4. dynamic_advisor.py overlay: fresh applies, stale falls through ─────

def _run_generate_advisory(canonical_fresh_return, cond_overrides=None):
    cond = {
        "condition": "NORMAL", "condition_score": 50, "session_type": "CHOP",
        "trend_score": 0, "spy_price": 500.0, "put_wall": 480.0,
        "call_wall": 520.0, "gamma_flip": 490.0, "max_pain": 500.0,
        "pc_ratio": 1.0, "skew_value": 0, "buy_pct": 50,
    }
    if cond_overrides:
        cond.update(cond_overrides)
    gathered = {
        "condition": cond, "vix": {}, "fear_greed": {}, "breadth": {},
        "correlations": {}, "events": {}, "news": {},
    }
    with patch.object(dynamic_advisor, "_gather", return_value=gathered), \
         patch("engine.canonical_gex.canonical_gex_if_fresh", return_value=canonical_fresh_return):
        dynamic_advisor._cache.clear()
        dynamic_advisor._cache_ts = 0.0
        return dynamic_advisor.generate_advisory(force=True)


def test_dynamic_advisor_overlay_applies_when_fresh():
    """generate_advisory() has no structured levels field -- the put/call
    wall values it derives surface in market_read.summary's "Key levels"
    sentence (dynamic_advisor.py:889-898). Assert on that."""
    fresh = _mk(
        (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(),
        put_wall=481.0, call_wall=521.0, gamma_flip=491.0,
    )
    adv = _run_generate_advisory(fresh)
    summary = adv["market_read"]["summary"]
    assert "call wall $521" in summary
    assert "put wall $481" in summary
    assert "gamma flip $491" in summary


def test_dynamic_advisor_overlay_falls_through_when_stale():
    """/api/ready-room/advisory must render red_alert's own (live) walls,
    NOT the frozen 2026-07-21 canonical snapshot, when canonical_gex_
    if_fresh() returns None -- the real, current state of this pipeline."""
    adv = _run_generate_advisory(None)
    summary = adv["market_read"]["summary"]
    assert "call wall $520" in summary
    assert "put wall $480" in summary
    assert "gamma flip $490" in summary


# ── 5. dashboard/app.py delegates to the same canonical helper ────────────

def test_dashboard_gex_age_days_delegates_to_canonical():
    """Same underlying `now`-relative computation in two separate calls a
    few microseconds apart -- compare to millisecond precision, not exact
    float equality."""
    import dashboard.app as app_module
    ts = "2026-07-21T13:05:00"
    assert app_module._gex_age_days(ts) == pytest.approx(
        canonical_gex_mod.snapshot_age_days(ts), abs=1e-6
    )


def test_dashboard_gex_is_stale_uses_shared_threshold():
    import dashboard.app as app_module
    fresh_ts = datetime.now(timezone.utc).isoformat()
    assert app_module._gex_is_stale(fresh_ts) is False
    assert app_module._gex_is_stale("2026-07-21T13:05:00") is True
    assert app_module._gex_is_stale(None) is True


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
