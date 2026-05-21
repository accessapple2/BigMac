"""HM-SLOW-FUNDAMENTALS Phase 2 (2026-05-21) — regression suite for the
`bars=` short-circuit threaded through trendlines / chart_patterns /
pattern_alerts / channel_scanner.

Phase 1 (PR #60) shipped `engine.market_data.get_bulk_daily_ohlcv` —
chunked + parallel + cached Alpaca bulk-bars fetcher. Phase 2 wires it
into the four `/api/...` dashboard endpoints that previously fanned out
per-symbol Yahoo HTTP calls over the ~3000-symbol universe. The Yahoo
rate-limit floor (~300s) made the prior pattern timeout-bound at 25s
with stale-fallback responses; bulk OHLCV is ~10s cold and ~0s warm.

The contract these tests pin down:
  - Every consumer accepts `bars: dict | None = None` and the legacy
    Yahoo path is preserved when `bars` is omitted.
  - When `bars` is provided, NO Yahoo HTTP call is issued (the
    `_fetch_daily_ohlcv` / `_yahoo_chart` helper is never reached).
  - Results are equivalent in shape to the Yahoo path.

See: project_hm_bulk_daily_ohlcv_phase1_shipped,
     project_hm_slow_fundamentals_refactor_scope.
"""
from __future__ import annotations

import inspect
from unittest.mock import patch

import pandas as pd
import pytest


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _synthetic_bars(symbols: list, rows: int = 70) -> dict:
    """Build a deterministic OHLCV DataFrame per symbol matching the
    Phase 1 schema (Open/High/Low/Close/Volume, DatetimeIndex name='Date').

    Prices oscillate so that channel_scanner's RSI math (which requires both
    gains AND losses) has live values to chew on.
    """
    index = pd.date_range("2026-02-01", periods=rows, freq="D", name="Date")
    out: dict = {}
    for i, sym in enumerate(symbols):
        base = 100.0 + i * 10
        # Mild oscillation: up 1.0 on even days, down 0.5 on odd days
        closes: list = [base]
        for j in range(1, rows):
            closes.append(closes[-1] + (1.0 if j % 2 == 0 else -0.5))
        out[sym] = pd.DataFrame(
            {
                "Open":   [c - 0.20 for c in closes],
                "High":   [c + 0.50 for c in closes],
                "Low":    [c - 0.50 for c in closes],
                "Close":  closes,
                "Volume": [1_000_000 + j * 1_000 for j in range(rows)],
            },
            index=index,
        )
    return out


# ----------------------------------------------------------------------
# Signature invariants — bars must be accepted at every Phase 2 consumer
# ----------------------------------------------------------------------

@pytest.mark.parametrize("module_path, fn_name", [
    ("engine.trendlines", "detect_support_resistance"),
    ("engine.trendlines", "get_all_levels"),
    ("engine.chart_patterns", "detect_patterns"),
    ("engine.chart_patterns", "detect_all_patterns"),
    ("engine.pattern_alerts", "get_pattern_alert_tiles"),
    ("engine.channel_scanner", "_get_stock_data"),
    ("engine.channel_scanner", "_scan_all"),
    ("engine.channel_scanner", "scan_channel"),
    ("engine.channel_scanner", "scan_gap_and_go"),
    ("engine.channel_scanner", "scan_momentum_breakout"),
    ("engine.channel_scanner", "scan_reversal_bounce"),
    ("engine.channel_scanner", "scan_short_squeeze"),
    ("engine.channel_scanner", "scan_earnings_runner"),
    ("engine.channel_scanner", "scan_volatility_breakout"),
    ("engine.channel_scanner", "scan_discovery"),
    ("engine.channel_scanner", "get_all_channels"),
])
def test_consumer_accepts_bars_kwarg(module_path: str, fn_name: str) -> None:
    mod = __import__(module_path, fromlist=[fn_name])
    fn = getattr(mod, fn_name)
    sig = inspect.signature(fn)
    assert "bars" in sig.parameters, f"{module_path}.{fn_name} missing bars="
    assert sig.parameters["bars"].default is None, \
        f"{module_path}.{fn_name} bars default should be None"


# ----------------------------------------------------------------------
# Trendlines — bars short-circuit
# ----------------------------------------------------------------------

def test_trendlines_bars_path_skips_yahoo(monkeypatch) -> None:
    from engine import trendlines
    trendlines._cache.clear()  # type: ignore[attr-defined]

    yahoo_called = []
    monkeypatch.setattr(
        trendlines,
        "_fetch_daily_ohlcv",
        lambda *a, **k: yahoo_called.append((a, k)) or None,  # type: ignore[func-returns-value]
    )

    bars = _synthetic_bars(["TEST"], rows=70)
    result = trendlines.detect_support_resistance("TEST", bars=bars)

    assert not yahoo_called, "yahoo path must not be invoked when bars provided"
    # Result shape: symbol + current_price + support/resistance arrays
    assert result is not None
    assert result["symbol"] == "TEST"
    assert "support" in result and "resistance" in result
    assert "high_30d" in result and "low_30d" in result


def test_trendlines_bars_missing_symbol_returns_none(monkeypatch) -> None:
    from engine import trendlines
    trendlines._cache.clear()  # type: ignore[attr-defined]

    monkeypatch.setattr(trendlines, "_fetch_daily_ohlcv",
                        lambda *a, **k: pytest.fail("yahoo must not be hit"))

    bars: dict = {}  # symbol not in dict
    assert trendlines.detect_support_resistance("MISSING", bars=bars) is None


def test_trendlines_get_all_levels_threads_bars(monkeypatch) -> None:
    from engine import trendlines
    trendlines._cache.clear()  # type: ignore[attr-defined]
    monkeypatch.setattr(trendlines, "_fetch_daily_ohlcv",
                        lambda *a, **k: pytest.fail("yahoo must not be hit"))
    bars = _synthetic_bars(["A", "B"], rows=70)
    result = trendlines.get_all_levels(["A", "B"], bars=bars)
    assert "A" in result and "B" in result


# ----------------------------------------------------------------------
# Chart patterns — bars short-circuit
# ----------------------------------------------------------------------

def test_chart_patterns_bars_path_skips_yahoo(monkeypatch) -> None:
    from engine import chart_patterns
    chart_patterns._cache.clear()  # type: ignore[attr-defined]
    monkeypatch.setattr(chart_patterns, "_fetch_daily_ohlcv",
                        lambda *a, **k: pytest.fail("yahoo must not be hit"))
    bars = _synthetic_bars(["TEST"], rows=70)
    result = chart_patterns.detect_patterns("TEST", bars=bars)
    assert isinstance(result, list)  # may be empty if no pattern; shape OK


def test_chart_patterns_bars_under_min_rows_returns_empty(monkeypatch) -> None:
    from engine import chart_patterns
    chart_patterns._cache.clear()  # type: ignore[attr-defined]
    monkeypatch.setattr(chart_patterns, "_fetch_daily_ohlcv",
                        lambda *a, **k: pytest.fail("yahoo must not be hit"))
    bars = _synthetic_bars(["SHORT"], rows=10)  # below the 20-row min
    assert chart_patterns.detect_patterns("SHORT", bars=bars) == []


def test_chart_patterns_detect_all_threads_bars(monkeypatch) -> None:
    from engine import chart_patterns
    chart_patterns._cache.clear()  # type: ignore[attr-defined]
    monkeypatch.setattr(chart_patterns, "_fetch_daily_ohlcv",
                        lambda *a, **k: pytest.fail("yahoo must not be hit"))
    bars = _synthetic_bars(["A", "B"], rows=70)
    result = chart_patterns.detect_all_patterns(["A", "B"], bars=bars)
    assert isinstance(result, list)


# ----------------------------------------------------------------------
# Pattern alerts — bars flow through to detect_all_patterns
# ----------------------------------------------------------------------

def test_pattern_alerts_threads_bars_into_detect_all_patterns(monkeypatch) -> None:
    from engine import pattern_alerts
    pattern_alerts._cache.clear()  # type: ignore[attr-defined]

    received = {}

    def _spy(symbols: list, bars=None) -> list:
        received["symbols"] = symbols
        received["bars"] = bars
        return []

    monkeypatch.setattr("engine.chart_patterns.detect_all_patterns", _spy)
    bars = _synthetic_bars(["A", "B"], rows=70)
    pattern_alerts.get_pattern_alert_tiles(["A", "B"], bars=bars)
    assert received["bars"] is bars
    assert received["symbols"] == ["A", "B"]


# ----------------------------------------------------------------------
# Channel scanner — bars short-circuit at _get_stock_data
# ----------------------------------------------------------------------

def test_channel_scanner_get_stock_data_bars_skips_yahoo(monkeypatch) -> None:
    from engine import channel_scanner
    from engine import market_data

    monkeypatch.setattr(
        market_data,
        "_yahoo_chart",
        lambda *a, **k: pytest.fail("yahoo must not be hit in bars path"),
    )
    monkeypatch.setattr(
        market_data,
        "get_stock_price",
        lambda sym: {"symbol": sym, "price": 100.0, "volume": 200_000, "change_pct": 0.5},
    )

    bars = _synthetic_bars(["TEST"], rows=70)
    result = channel_scanner._get_stock_data("TEST", bars=bars)
    assert result is not None
    # Volume from bars carried into rel_volume math.
    assert result["high_52w"] is not None
    assert result["low_52w"] is not None
    assert result["rel_volume"] is not None
    assert result["rsi"] is not None


def test_channel_scanner_scan_channel_threads_bars(monkeypatch) -> None:
    from engine import channel_scanner

    received = {}

    def _spy(bars=None) -> list:
        received["bars"] = bars
        return [{"symbol": "STUB"}]

    monkeypatch.setattr(channel_scanner, "scan_gap_and_go", _spy)
    bars = _synthetic_bars(["X"], rows=70)
    result = channel_scanner.scan_channel("gap-and-go", bars=bars)
    assert received["bars"] is bars
    assert result == [{"symbol": "STUB"}]


def test_channel_scanner_get_all_channels_threads_bars(monkeypatch) -> None:
    from engine import channel_scanner

    received: dict = {}

    def _make_spy(name: str):
        def _fn(bars=None) -> list:
            received[name] = bars
            return []
        return _fn

    for fn_name in (
        "scan_gap_and_go", "scan_momentum_breakout", "scan_reversal_bounce",
        "scan_short_squeeze", "scan_earnings_runner",
        "scan_volatility_breakout", "scan_discovery",
    ):
        monkeypatch.setattr(channel_scanner, fn_name, _make_spy(fn_name))

    bars = _synthetic_bars(["X"], rows=70)
    channel_scanner.get_all_channels(bars=bars)
    for fn_name in received:
        assert received[fn_name] is bars, f"{fn_name} did not receive bars"


# ----------------------------------------------------------------------
# Legacy path — omitting bars must NOT change call surface
# ----------------------------------------------------------------------

def test_trendlines_omitted_bars_uses_yahoo_fetch(monkeypatch) -> None:
    """Sanity: when bars is omitted, the Yahoo path is still reachable."""
    from engine import trendlines
    trendlines._cache.clear()  # type: ignore[attr-defined]

    fetch_called = []

    def _fake_fetch(symbol: str, range_: str = "3mo"):
        fetch_called.append((symbol, range_))
        return None  # treat as no-data — return None propagates cleanly

    monkeypatch.setattr(trendlines, "_fetch_daily_ohlcv", _fake_fetch)

    result = trendlines.detect_support_resistance("LEGACY")  # bars omitted
    assert fetch_called == [("LEGACY", "3mo")]
    assert result is None  # no data path
