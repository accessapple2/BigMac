#!/usr/bin/env python3
"""Tests for HM-BK-A opening-range-breakout confirmatory scanner — rail + detection."""

from datetime import datetime

import pytest

from engine import bk_orb_scanner as m

try:
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    ET = None


# ── Confirmatory-only rail ────────────────────────────────────────────────

def test_sole_voter_never_counts():
    v = m.confirmatory_vote(1, "BULL")
    assert v["counts_toward_convergence"] is False
    assert v["is_trigger"] is False
    assert v["trade_permitted_on_orb_alone"] is False


def test_counts_only_with_min_fleet_votes():
    v = m.confirmatory_vote(2, "BULL")
    assert v["counts_toward_convergence"] is True
    assert v["direction"] == "BULLISH"


def test_neutral_never_counts_and_trigger_false():
    assert m.MIN_FLEET_VOTES == 2
    for fv in (0, 2, 9):
        assert m.confirmatory_vote(fv, None)["counts_toward_convergence"] is False
    for sig in ("BULL", "BEAR", None):
        assert m.confirmatory_vote(2, sig)["is_trigger"] is False


# ── Intraday detection ────────────────────────────────────────────────────

def _bar(h, mn, o, hi, lo, c, v):
    return {"ts_ms": 0, "et": datetime(2026, 6, 12, h, mn, tzinfo=ET),
            "o": o, "h": hi, "l": lo, "c": c, "v": v}


def _session(or_high=101.0, or_low=99.0, break_to=None, break_vol=500_000,
             or_vol_each=100_000):
    """15-min OR in [99,101], then post-window bars. break_to>OR_high triggers."""
    bars = []
    # OR window 09:30–09:44 (15 one-min bars)
    for mn in range(30, 45):
        bars.append(_bar(9, mn, 100, or_high, or_low, 100, or_vol_each))
    # post-window bars 09:45 onward
    for mn in range(45, 55):
        c = 100.0
        hi, lo = 100.5, 99.5
        v = 50_000
        if break_to is not None and mn == 50:
            c, hi, v = break_to, break_to + 0.2, break_vol
        bars.append(_bar(9, mn, 100, hi, lo, c, v))
    return bars


@pytest.mark.skipif(ET is None, reason="zoneinfo unavailable")
def test_valid_upside_break_emits_bull():
    bars = _session(break_to=102.0, break_vol=5_000_000)  # close>OR_high, big vol
    # trailing median first-window vol = 1.5M; OR window vol = 15*100k = 1.5M, cum at
    # break ~1.5M + small + 5M >> 1.5*1.5M -> passes
    sig = m.detect_session("TEST", "2026-06-12", bars, median_first_vol=1_500_000)
    assert sig is not None and sig["signal"] == "BULL", sig
    assert sig["or_high"] == 101.0
    assert sig["break_price"] == 102.0
    assert sig["vol_mult"] >= m.VOL_MULT


@pytest.mark.skipif(ET is None, reason="zoneinfo unavailable")
def test_no_break_no_signal():
    bars = _session(break_to=None)  # never exceeds OR_high
    assert m.detect_session("TEST", "2026-06-12", bars, median_first_vol=1_500_000) is None


@pytest.mark.skipif(ET is None, reason="zoneinfo unavailable")
def test_break_but_volume_too_low_rejected():
    bars = _session(break_to=102.0, break_vol=1)  # break, but trivial volume
    # huge trailing median so cum/median < 1.5
    sig = m.detect_session("TEST", "2026-06-12", bars, median_first_vol=10**12)
    assert sig is None


@pytest.mark.skipif(ET is None, reason="zoneinfo unavailable")
def test_short_disabled_by_default():
    bars = []
    for mn in range(30, 45):
        bars.append(_bar(9, mn, 100, 101, 99, 100, 100_000))
    for mn in range(45, 55):
        c = 97.0 if mn == 50 else 100.0
        v = 5_000_000 if mn == 50 else 50_000
        bars.append(_bar(9, mn, 100, 100.5, 96.5, c, v))
    assert m.detect_session("TEST", "2026-06-12", bars, 1_500_000, short_enabled=False) is None
    s = m.detect_session("TEST", "2026-06-12", bars, 1_500_000, short_enabled=True)
    assert s is not None and s["signal"] == "BEAR"


@pytest.mark.skipif(ET is None, reason="zoneinfo unavailable")
def test_median_first_vol_trailing():
    sessions = {}
    for day in range(1, 6):
        bars = []
        for mn in range(30, 45):
            bars.append({"ts_ms": 0, "et": datetime(2026, 6, day, 9, mn, tzinfo=ET),
                         "o": 100, "h": 101, "l": 99, "c": 100, "v": day * 10_000})
        sessions[f"2026-06-0{day}"] = bars
    med, n = m._median_first_vol(sessions, "2026-06-05", m.OR_MINUTES)
    # prior sessions 1..4 -> first-window vols = 15*[10k,20k,30k,40k] = [150k,300k,450k,600k]
    assert n == 4
    assert med == 375_000  # median of [150k,300k,450k,600k]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
