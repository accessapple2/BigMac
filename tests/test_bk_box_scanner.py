#!/usr/bin/env python3
"""Tests for HM-BK-C tight-box breakout confirmatory scanner — rail + detection."""

import numpy as np
import pandas as pd
import pytest

from engine import bk_box_scanner as m


# ── Confirmatory-only rail ────────────────────────────────────────────────

def test_sole_voter_never_counts():
    v = m.confirmatory_vote(1, "BULL")
    assert v["counts_toward_convergence"] is False
    assert v["is_trigger"] is False
    assert v["trade_permitted_on_box_alone"] is False


def test_counts_only_with_min_fleet_votes():
    v = m.confirmatory_vote(2, "BULL")
    assert v["counts_toward_convergence"] is True
    assert v["direction"] == "BULLISH"


def test_neutral_never_counts():
    for fv in (0, 2, 9):
        assert m.confirmatory_vote(fv, None)["counts_toward_convergence"] is False


def test_is_trigger_always_false_and_min_votes_two():
    assert m.MIN_FLEET_VOTES == 2
    for sig in ("BULL", "BEAR", None):
        assert m.confirmatory_vote(2, sig)["is_trigger"] is False


# ── Detection ─────────────────────────────────────────────────────────────

def _ohlc(o, h, l, c, v):
    idx = pd.date_range("2026-01-01", periods=len(c), freq="B")
    return pd.DataFrame({"Open": o, "High": h, "Low": l, "Close": c, "Volume": v}, index=idx)


def test_tight_box_upside_breakout_emits_bull():
    # 24 sessions oscillating tightly in [99,101], then a breakout to 104 on 2x vol.
    n = 25
    base = [100, 101, 99, 100.5, 99.5] * 5  # 25 vals, tight ~2% range
    c = base[:24] + [104.0]
    h = [x + 0.4 for x in c[:24]] + [104.5]
    l = [x - 0.4 for x in c[:24]] + [100.8]
    o = [100] * 24 + [101.2]
    v = [1_000_000] * 24 + [2_500_000]  # 2.5x breakout volume
    sigs = m.detect(_ohlc(o, h, l, c, v), "TEST", short_enabled=False)
    assert any(s["signal"] == "BULL" for s in sigs), sigs
    s = sigs[0]
    assert s["width_pct"] <= m.WIDTH_MAX_PCT
    assert s["duration_days"] >= m.MIN_DURATION
    assert s["vol_mult"] >= m.VOL_MULT
    assert set(s) == {"symbol", "asof", "box_top", "box_bottom",
                      "width_pct", "duration_days", "vol_mult", "signal"}


def test_breakout_on_low_volume_rejected():
    n = 25
    base = [100, 101, 99, 100.5, 99.5] * 5
    c = base[:24] + [104.0]
    h = [x + 0.4 for x in c[:24]] + [104.5]
    l = [x - 0.4 for x in c[:24]] + [100.8]
    o = [100] * 24 + [101.2]
    v = [1_000_000] * 25  # NO volume expansion on the break
    assert m.detect(_ohlc(o, h, l, c, v), "TEST", short_enabled=False) == []


def test_wide_box_rejected():
    # range ~30% -> not a tight box even with a breakout + volume
    c = [100, 130, 100, 130, 100, 130, 100, 130, 100, 130,
         100, 130, 100, 130, 100, 130, 100, 130, 100, 130,
         100, 130, 100, 130, 140]
    h = [x + 1 for x in c]
    l = [x - 1 for x in c]
    o = [100] * 25
    v = [1_000_000] * 24 + [3_000_000]
    assert m.detect(_ohlc(o, h, l, c, v), "TEST", short_enabled=False) == []


def test_short_disabled_by_default():
    # tight box, breaks DOWN on volume — long-only default => no signal
    n = 25
    base = [100, 101, 99, 100.5, 99.5] * 5
    c = base[:24] + [96.0]
    h = [x + 0.4 for x in c[:24]] + [99.2]
    l = [x - 0.4 for x in c[:24]] + [95.5]
    o = [100] * 24 + [99.0]
    v = [1_000_000] * 24 + [2_500_000]
    assert m.detect(_ohlc(o, h, l, c, v), "TEST", short_enabled=False) == []
    # but fires BEAR when short explicitly enabled
    bear = m.detect(_ohlc(o, h, l, c, v), "TEST", short_enabled=True)
    assert any(s["signal"] == "BEAR" for s in bear), bear


def test_too_few_bars_returns_empty():
    o = h = l = c = [100] * 10
    v = [1_000_000] * 10
    assert m.detect(_ohlc(o, h, l, c, v), "TEST") == []


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
