#!/usr/bin/env python3
"""Tests for HM-BK-B anchored-VWAP confirmatory scanner — rail + detection.

Mirrors tests/test_fred_bankrate_signal.py: the load-bearing assertions are the
confirmatory-only rail (never originates, sole-voter never counts) plus the
anchored-VWAP cross detection on synthetic frames (no network)."""

import numpy as np
import pandas as pd
import pytest

from engine import bk_avwap_scanner as m


# ── Confirmatory-only rail ────────────────────────────────────────────────

def test_sole_voter_never_counts():
    v = m.confirmatory_vote(fleet_directional_votes=1, signal="BULL")
    assert v["counts_toward_convergence"] is False
    assert v["is_sole_voter"] is True
    assert v["is_trigger"] is False
    assert v["trade_permitted_on_avwap_alone"] is False


def test_counts_only_with_min_fleet_votes():
    v = m.confirmatory_vote(fleet_directional_votes=2, signal="BEAR")
    assert v["counts_toward_convergence"] is True
    assert v["direction"] == "BEARISH"
    assert v["is_sole_voter"] is False


def test_neutral_or_none_never_counts():
    for fv in (0, 2, 5):
        v = m.confirmatory_vote(fleet_directional_votes=fv, signal=None)
        assert v["counts_toward_convergence"] is False
        assert v["direction"] == "NEUTRAL"


def test_is_trigger_always_false():
    for sig in ("BULL", "BEAR", None):
        for fv in (0, 1, 2, 9):
            assert m.confirmatory_vote(fv, sig)["is_trigger"] is False


def test_min_fleet_votes_is_two():
    assert m.MIN_FLEET_VOTES == 2


# ── Detection on synthetic frames ─────────────────────────────────────────

def _frame(closes, highs=None, lows=None, opens=None, vols=None):
    n = len(closes)
    highs = highs or [c * 1.01 for c in closes]
    lows = lows or [c * 0.99 for c in closes]
    opens = opens or list(closes)
    vols = vols or [1_000_000] * n
    idx = pd.date_range("2026-01-01", periods=n, freq="B")
    return pd.DataFrame(
        {"Open": opens, "High": highs, "Low": lows, "Close": closes, "Volume": vols},
        index=idx,
    )


def _ohlc(o, h, l, c):
    n = len(c)
    idx = pd.date_range("2026-01-01", periods=n, freq="B")
    return pd.DataFrame(
        {"Open": o, "High": h, "Low": l, "Close": c, "Volume": [1_000_000] * n},
        index=idx,
    )


def test_reclaim_emits_bull():
    # flat 100 -> gap UP to 108 (resistance anchor at idx 25) -> 8 bars BELOW it at
    # 104 -> last bar reclaims to 112 without itself gapping. Hand-computed:
    # aVWAP@33≈104.44 (prev close 104, below); aVWAP@34≈104.93 (close 112, above) -> BULL.
    o = [100] * 25 + [108] + [104] * 8 + [104]
    c = [100] * 25 + [108] + [104] * 8 + [112]
    h = [100.5] * 25 + [108.5] + [104.5] * 8 + [112.5]
    l = [99.5] * 25 + [107.5] + [103.5] * 8 + [103.5]
    sigs = m.detect(_ohlc(o, h, l, c), "TEST")
    bull = [s for s in sigs if s["signal"] == "BULL"]
    assert bull, sigs
    assert any(s["anchor_type"] == "gap" for s in bull), bull
    assert all(s["close"] > s["avwap_price"] for s in bull)


def test_loss_emits_bear():
    # flat 100 -> gap DOWN to 92 (support anchor at idx 25) -> 8 bars ABOVE it at
    # 95 -> last bar loses it to 88. aVWAP@33≈94.67 (prev 95, above);
    # aVWAP@34≈94.23 (close 88, below) -> BEAR.
    o = [100] * 25 + [92] + [95] * 8 + [95]
    c = [100] * 25 + [92] + [95] * 8 + [88]
    h = [100.5] * 25 + [92.5] + [95.5] * 8 + [95.5]
    l = [99.5] * 25 + [91.5] + [94.5] * 8 + [87.5]
    sigs = m.detect(_ohlc(o, h, l, c), "TEST")
    bear = [s for s in sigs if s["signal"] == "BEAR"]
    assert bear, sigs
    assert all(s["close"] < s["avwap_price"] for s in bear)


def test_too_few_bars_returns_empty():
    df = _frame([100, 101, 102])
    assert m.detect(df, "TEST") == []


def test_missing_columns_returns_empty():
    df = pd.DataFrame({"foo": range(60), "bar": range(60)})
    assert m.detect(df, "TEST") == []


def test_anchored_vwap_monotonic_window():
    df = _frame([100 + i for i in range(50)])
    series = m._anchored_vwap(m._norm(df), anchor_idx=10)
    assert np.isnan(series[9])           # before anchor = NaN
    assert not np.isnan(series[10])      # at anchor = defined
    assert not np.isnan(series[-1])


def test_confluence_n_clusters():
    # three aVWAPs: two within band of price, one far away -> cluster of 2
    assert m._confluence_n(100.0, [100.2, 100.5, 130.0]) == 2
    assert m._confluence_n(100.0, [130.0, 140.0]) == 0   # price not interacting
    assert m._confluence_n(100.0, [100.1]) == 0          # need >= 2


def test_gap_anchor_detects_major_gap():
    closes = [100] * 20 + [120] + [121] * 10   # +20% gap up at idx 20
    opens = [100] * 20 + [120] + [121] * 10
    df = _frame(closes, opens=opens)
    assert m._gap_anchor(m._norm(df)) == 20


# ── UHURA wiring (market_vote + confluence fold-in) ───────────────────────

def _sig(sym, signal, asof="2026-06-13"):
    return {"symbol": sym, "asof": asof, "anchor_type": "gap", "signal": signal,
            "avwap_price": 10.0, "close": 10.5, "confluence_n": 0}


def test_market_vote_abstains_when_flag_off():
    assert m.market_vote(enabled=False) is None


def test_market_vote_abstains_when_no_fresh_signal(tmp_path):
    db = str(tmp_path / "empty.db")
    conn = m._conn(db); m._ensure_schema(conn); conn.close()
    assert m.market_vote(enabled=True, db_path=db) is None


def test_market_vote_bull_mapping(tmp_path):
    db = str(tmp_path / "bull.db")
    m._persist([_sig("AAA", "BULL"), _sig("BBB", "BULL"), _sig("CCC", "BEAR")], db)
    mv = m.market_vote(enabled=True, db_path=db)
    assert mv is not None
    assert mv["direction"] == "BULLISH"          # 2 reclaim vs 1 loss
    assert mv["weight"] == m.CONFIRMATORY_WEIGHT == 1.0
    assert mv["bull"] == 2 and mv["bear"] == 1


def test_market_vote_bear_mapping(tmp_path):
    db = str(tmp_path / "bear.db")
    m._persist([_sig("X", "BEAR"), _sig("Y", "BEAR"), _sig("Z", "BULL")], db)
    assert m.market_vote(enabled=True, db_path=db)["direction"] == "BEARISH"


def test_market_vote_watchlist_filter(tmp_path):
    db = str(tmp_path / "wl.db")
    m._persist([_sig("AAA", "BULL")], db)
    assert m.market_vote(enabled=True, db_path=db, watchlist=["ZZZ"]) is None
    assert m.market_vote(enabled=True, db_path=db, watchlist=["AAA"]) is not None


def test_market_vote_only_latest_session_counts(tmp_path):
    db = str(tmp_path / "fresh.db")
    # old BEAR session + newer BULL session -> only the newest asof counts
    m._persist([_sig("OLD", "BEAR", asof="2026-06-01")], db)
    m._persist([_sig("NEW", "BULL", asof="2026-06-13")], db)
    mv = m.market_vote(enabled=True, db_path=db)
    assert mv["direction"] == "BULLISH" and mv["bull"] == 1 and mv["bear"] == 0


def test_confluence_foldin_requires_two_fleet_votes():
    import engine.uhura as U
    u = U.LtUhura()
    SV = U.SignalVote
    conf = SV("bk_avwap", "BULLISH", 1.0, "[confirm]", is_confirmatory=True)
    # 2 originating fleet votes -> confirmatory counts
    c2 = u._calculate_confluence([SV("a", "BULLISH", 1.0, "x"),
                                  SV("b", "BULLISH", 1.0, "y"), conf])
    assert c2["confirmatory_applied"] == 1 and c2["aligned_count"] == 3
    # 1 originating fleet vote -> confirmatory must NOT count (never originates)
    c1 = u._calculate_confluence([SV("a", "BULLISH", 1.0, "x"), conf])
    assert c1["confirmatory_applied"] == 0 and c1["aligned_count"] == 1


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
