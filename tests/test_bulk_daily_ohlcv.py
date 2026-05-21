"""Tests for engine.market_data.get_bulk_daily_ohlcv (HM-SLOW-FUNDAMENTALS Phase 1).

Covers:
  - Empty input short-circuits to empty dict (no network)
  - Unknown range_str logs warning + falls back to 3mo (no crash)
  - Silent-failure trap: HTTP 200 with {"message": ...} but no "bars" key
    returns {} from the chunk helper (defended in _alpaca_bulk_bars_chunk)
  - Chunking: 250 symbols → 3 chunks (100+100+50)
  - Cache: second call for the same (symbol, range_str) hits the cache and
    skips the network entirely
  - Returned DataFrames have the trendlines-compatible schema:
      columns ⊇ {Open, High, Low, Close, Volume}, DatetimeIndex named "Date"
  - Live smoke (marked, opt-in): 3-symbol fetch against Alpaca IEX returns
    real bars matching the verified Task-2 shape.

Run:
    python3 -m pytest tests/test_bulk_daily_ohlcv.py -v
    python3 -m pytest tests/test_bulk_daily_ohlcv.py -v -m live  # network
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


REQUIRED_COLS = {"Open", "High", "Low", "Close", "Volume"}


@pytest.fixture(autouse=True)
def _clear_bulk_cache():
    """Each test starts with a clean module-level cache."""
    from engine import market_data as md
    with md._bulk_bars_cache_lock:
        md._bulk_bars_cache.clear()
    yield
    with md._bulk_bars_cache_lock:
        md._bulk_bars_cache.clear()


def _fake_bars_df(n_rows: int = 63) -> pd.DataFrame:
    """Build a DataFrame in the shape _alpaca_bulk_bars_chunk produces."""
    idx = pd.date_range(end="2026-05-21", periods=n_rows, freq="B", name="Date")
    return pd.DataFrame(
        {
            "Open":   [100.0 + i * 0.1 for i in range(n_rows)],
            "High":   [101.0 + i * 0.1 for i in range(n_rows)],
            "Low":    [ 99.0 + i * 0.1 for i in range(n_rows)],
            "Close":  [100.5 + i * 0.1 for i in range(n_rows)],
            "Volume":     [1_000_000  for _ in range(n_rows)],
        },
        index=idx,
    )


# ---------------------------------------------------------------------------
# Unit: empty input short-circuits
# ---------------------------------------------------------------------------

def test_empty_symbols_returns_empty_dict():
    """get_bulk_daily_ohlcv([]) must return {} without any network call."""
    from engine.market_data import get_bulk_daily_ohlcv

    with patch("engine.market_data._alpaca_bulk_bars_chunk") as chunk_mock:
        result = get_bulk_daily_ohlcv([])
        assert result == {}
        chunk_mock.assert_not_called()


# ---------------------------------------------------------------------------
# Unit: unknown range_str falls back without crash
# ---------------------------------------------------------------------------

def test_unknown_range_str_falls_back_to_3mo():
    """Unknown range string logs + defaults to 3mo. No crash."""
    from engine.market_data import get_bulk_daily_ohlcv

    captured_dates: list = []

    def fake_chunk(chunk, start_iso, end_iso):
        captured_dates.append((start_iso, end_iso))
        return {sym: _fake_bars_df() for sym in chunk}

    with patch("engine.market_data._alpaca_bulk_bars_chunk", side_effect=fake_chunk):
        result = get_bulk_daily_ohlcv(["AAPL"], range_str="banana")
        assert "AAPL" in result
        assert not result["AAPL"].empty
        # One chunk fired with a real date range.
        assert len(captured_dates) == 1


# ---------------------------------------------------------------------------
# Unit: chunking math
# ---------------------------------------------------------------------------

def test_chunking_splits_at_chunk_size():
    """250 symbols at chunk_size=100 must produce exactly 3 chunks (100+100+50)."""
    from engine.market_data import get_bulk_daily_ohlcv, _BULK_BARS_CHUNK_SIZE

    assert _BULK_BARS_CHUNK_SIZE == 100, "chunk size constant changed; update test"
    symbols = [f"SYM{i:03d}" for i in range(250)]
    chunk_sizes: list = []

    def fake_chunk(chunk, start_iso, end_iso):
        chunk_sizes.append(len(chunk))
        return {sym: _fake_bars_df() for sym in chunk}

    with patch("engine.market_data._alpaca_bulk_bars_chunk", side_effect=fake_chunk):
        result = get_bulk_daily_ohlcv(symbols)

    assert sorted(chunk_sizes) == [50, 100, 100]
    assert len(result) == 250
    assert all(not result[s].empty for s in symbols)


# ---------------------------------------------------------------------------
# Unit: cache hit on second call
# ---------------------------------------------------------------------------

def test_cache_hit_skips_network_on_second_call():
    """Second identical call must NOT invoke _alpaca_bulk_bars_chunk."""
    from engine.market_data import get_bulk_daily_ohlcv

    call_count = {"n": 0}

    def fake_chunk(chunk, start_iso, end_iso):
        call_count["n"] += 1
        return {sym: _fake_bars_df() for sym in chunk}

    with patch("engine.market_data._alpaca_bulk_bars_chunk", side_effect=fake_chunk):
        r1 = get_bulk_daily_ohlcv(["AAPL", "MSFT"])
        first_n = call_count["n"]
        r2 = get_bulk_daily_ohlcv(["AAPL", "MSFT"])
        second_n = call_count["n"]

    assert first_n == 1, "first call should fire exactly one chunk"
    assert second_n == 1, "second call must hit cache — no additional chunks"
    assert set(r2) == {"AAPL", "MSFT"}
    assert not r2["AAPL"].empty


def test_cache_partial_hit_only_fetches_misses():
    """Cache one symbol, then ask for two — only the missing symbol fans out."""
    from engine.market_data import get_bulk_daily_ohlcv

    received_chunks: list = []

    def fake_chunk(chunk, start_iso, end_iso):
        received_chunks.append(list(chunk))
        return {sym: _fake_bars_df() for sym in chunk}

    with patch("engine.market_data._alpaca_bulk_bars_chunk", side_effect=fake_chunk):
        get_bulk_daily_ohlcv(["AAPL"])         # warms cache
        get_bulk_daily_ohlcv(["AAPL", "MSFT"]) # AAPL hit, MSFT miss

    assert received_chunks == [["AAPL"], ["MSFT"]]


def test_cache_ttl_expires():
    """Past _BULK_BARS_CACHE_TTL a previously-cached symbol is fetched again."""
    from engine import market_data as md

    def fake_chunk(chunk, start_iso, end_iso):
        return {sym: _fake_bars_df() for sym in chunk}

    with patch("engine.market_data._alpaca_bulk_bars_chunk", side_effect=fake_chunk):
        md.get_bulk_daily_ohlcv(["AAPL"])

    # Backdate the cache entry past TTL.
    with md._bulk_bars_cache_lock:
        md._bulk_bars_cache[("AAPL", "3mo")]["ts"] = (
            time.time() - md._BULK_BARS_CACHE_TTL - 1
        )

    misses: list = []

    def fake_chunk_2(chunk, start_iso, end_iso):
        misses.extend(chunk)
        return {sym: _fake_bars_df() for sym in chunk}

    with patch("engine.market_data._alpaca_bulk_bars_chunk", side_effect=fake_chunk_2):
        md.get_bulk_daily_ohlcv(["AAPL"])

    assert misses == ["AAPL"], "expired entry should force a re-fetch"


# ---------------------------------------------------------------------------
# Unit: silent-failure trap (HTTP 200 + message-only body)
# ---------------------------------------------------------------------------

def test_silent_failure_trap_returns_empty_chunk():
    """Chunk helper must defend against HTTP 200 with no 'bars' key."""
    from engine.market_data import _alpaca_bulk_bars_chunk

    class FakeResp:
        ok = True
        status_code = 200

        @staticmethod
        def json():
            return {"message": "subscription does not permit querying recent SIP data"}

    with patch("engine.market_data._get_alpaca_headers", return_value={"x": "y"}), \
         patch("engine.market_data.requests.get", return_value=FakeResp()):
        out = _alpaca_bulk_bars_chunk(["AAPL", "MSFT"], "2026-02-21", "2026-05-21")

    assert out == {}, (
        "message-only response must surface as empty dict so caller backfills "
        "empty DataFrames per the contract"
    )


def test_silent_failure_backfills_empty_dataframes():
    """When all chunks return {}, get_bulk_daily_ohlcv must still return a
    full result dict with empty DataFrames per requested symbol (consumer
    contract: every requested symbol gets a key)."""
    from engine.market_data import get_bulk_daily_ohlcv

    with patch("engine.market_data._alpaca_bulk_bars_chunk", return_value={}):
        result = get_bulk_daily_ohlcv(["AAPL", "MSFT", "GOOGL"])

    assert set(result) == {"AAPL", "MSFT", "GOOGL"}
    assert all(result[s].empty for s in result), \
        "silent-failure should yield empty DataFrames, not missing keys"


# ---------------------------------------------------------------------------
# Unit: DataFrame schema matches trendlines consumer
# ---------------------------------------------------------------------------

def test_dataframe_schema_matches_trendlines_consumer():
    """Returned DataFrames have High/Low/Close/Open/Volume (+ optional extras)
    and a DatetimeIndex named 'Date' — drop-in for engine.trendlines."""
    from engine.market_data import get_bulk_daily_ohlcv

    with patch("engine.market_data._alpaca_bulk_bars_chunk",
               side_effect=lambda chunk, s, e: {sym: _fake_bars_df() for sym in chunk}):
        result = get_bulk_daily_ohlcv(["AAPL"])

    df = result["AAPL"]
    assert REQUIRED_COLS.issubset(set(df.columns)), \
        f"missing required columns; got {list(df.columns)}"
    assert isinstance(df.index, pd.DatetimeIndex), \
        f"index must be DatetimeIndex; got {type(df.index).__name__}"
    assert df.index.name == "Date", f"index name must be 'Date'; got {df.index.name!r}"
    assert len(df) >= 15, "trendlines._fetch_daily_ohlcv rejects < 15 rows"


# ---------------------------------------------------------------------------
# Live smoke — opt-in, requires network + Alpaca creds in .env
# ---------------------------------------------------------------------------

@pytest.mark.live
def test_live_three_symbol_fetch_against_alpaca():
    """Real network call: AAPL/MSFT/GOOGL over default 3mo range.

    Skipped unless -m live is passed. Verifies the actual endpoint still
    behaves per the 2026-05-21 verification (≥ 60 bars per symbol, all
    five required columns present).
    """
    from engine.market_data import get_bulk_daily_ohlcv, _get_alpaca_headers

    if not _get_alpaca_headers():
        pytest.skip("APCA_API_KEY_ID / APCA_API_SECRET_KEY not configured")

    result = get_bulk_daily_ohlcv(["AAPL", "MSFT", "GOOGL"], range_str="3mo")

    assert set(result) == {"AAPL", "MSFT", "GOOGL"}
    for sym, df in result.items():
        assert not df.empty, f"{sym} returned empty DataFrame"
        assert REQUIRED_COLS.issubset(set(df.columns)), \
            f"{sym} missing required columns; got {list(df.columns)}"
        assert len(df) >= 60, f"{sym} only {len(df)} bars (expected ~63 for 3mo)"
