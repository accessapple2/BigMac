"""Tests for buckets module. No I/O, pure logic."""
from __future__ import annotations

import math

from base_rates.buckets import (
    DEFAULT_BUCKETS,
    _bucket_index,
    assign_buckets,
    bucket_vector_key,
    describe_buckets,
)


def test_bucket_index_below_first_edge():
    assert _bucket_index(-0.10, [-0.05, 0.0, 0.05]) == 0


def test_bucket_index_at_edge_goes_right():
    # value < edge → bucket below; value == edge → bucket at-or-above
    edges = [-0.05, 0.0, 0.05]
    assert _bucket_index(-0.05, edges) == 1   # not <-0.05, so next bucket
    assert _bucket_index(0.0, edges) == 2
    assert _bucket_index(0.05, edges) == 3


def test_bucket_index_above_last_edge():
    assert _bucket_index(0.20, [-0.05, 0.0, 0.05]) == 3


def test_bucket_index_handles_nan_and_none():
    assert _bucket_index(float("nan"), [0, 1]) == -1
    assert _bucket_index(None, [0, 1]) == -1


def test_assign_buckets_typical():
    # +5% move, RSI 75, slope rising, VIX 18, VIX up 3%, SPY uptrend
    b = assign_buckets(
        pct_change=0.05,
        rsi14=75,
        rsi_slope=1.5,
        vix_close=18,
        vix_pct_change=0.03,
        spy_above_200=1,
    )
    # +0.05 falls in bucket 6 (>0.05)
    assert b["move_intensity"] == 6
    # RSI 75 → bucket 3 (>70)
    assert b["rsi_zone"] == 3
    # rising
    assert b["rsi_slope"] == 1
    # VIX 18 → bucket 1 (15-20)
    assert b["vix_level"] == 1
    # VIX +3% → bucket 4 (>=3%, was bucket 3 with old +/-5% edges)
    assert b["vix_move"] == 4
    # uptrend
    assert b["market_trend"] == 1


def test_assign_buckets_negative_move():
    b = assign_buckets(
        pct_change=-0.094,  # the ONDS example, -9.4%
        rsi14=42,
        rsi_slope=-2,
        vix_close=22,
        vix_pct_change=0.06,
        spy_above_200=1,
    )
    # -9.4% < -0.05 → bucket 0
    assert b["move_intensity"] == 0
    # RSI 42 → bucket 1 (30-50)
    assert b["rsi_zone"] == 1
    # falling
    assert b["rsi_slope"] == 0
    # VIX 22 → bucket 2 (20-25)
    assert b["vix_level"] == 2
    # VIX +6% → bucket 4 (>5%)
    assert b["vix_move"] == 4


def test_bucket_vector_key_is_stable_tuple():
    b = {"move_intensity": 1, "rsi_zone": 2, "rsi_slope": 1,
         "vix_level": 3, "vix_move": 0, "market_trend": 1}
    k = bucket_vector_key(b)
    assert k == (1, 2, 1, 3, 0, 1)
    assert isinstance(k, tuple)


def test_describe_buckets_human_readable():
    b = assign_buckets(
        pct_change=0.05,
        rsi14=75,
        rsi_slope=1.5,
        vix_close=18,
        vix_pct_change=0.03,
        spy_above_200=1,
    )
    d = describe_buckets(b)
    assert d["rsi_slope"] == "rising"
    assert d["market_trend"] == "SPY>200d"
    assert "70" in d["rsi_zone"]


def test_overrides_change_buckets():
    # Override: only one cutoff at 0
    b = assign_buckets(
        pct_change=0.02, rsi14=50, rsi_slope=1,
        vix_close=20, vix_pct_change=0.0, spy_above_200=1,
        overrides={"move_intensity": [0.0]},
    )
    # 0.02 > 0 → bucket 1
    assert b["move_intensity"] == 1
