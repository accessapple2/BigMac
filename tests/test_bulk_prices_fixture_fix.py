"""HM-BULK-PRICES-FIXTURE-FIX 2026-05-21 — regression suite for the bulk-prices
snapshots migration.

Pre-fix bug surface: `_get_alpaca_bulk_prices` hit `/v2/stocks/quotes/latest`
and parsed via `_alpaca_quote_to_price`. Quotes only carry bid/ask, so the
parser hardcoded `change_pct=0.0` and stuffed `ask_size` (typically 40) into
the `volume` field. Every consumer (heatmap, market-movers) got fixture-shaped
data. Fix routes through `_get_alpaca_bulk_snapshots`, which gives last +
dailyBar.{high,low,volume} + prevDailyBar.close → real change_pct + real
cumulative daily volume.

See: project_hm_bulk_prices_fixture_bug.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from engine import market_data


@pytest.fixture(autouse=True)
def _clear_price_cache(monkeypatch):
    """Empty the in-process price cache between tests so cached pre-fix
    values don't bleed into post-fix probes."""
    if hasattr(market_data, "_price_cache"):
        market_data._price_cache.clear()
    yield
    if hasattr(market_data, "_price_cache"):
        market_data._price_cache.clear()


def _fake_snapshots(symbols, **kwargs) -> dict:
    """Synthetic snapshots payload — covers the realistic Alpaca shape."""
    return {
        "SPY": {
            "symbol": "SPY",
            "last_price": 742.91,
            "open_price": 740.55,
            "high": 744.10,
            "low": 740.20,
            "volume": 1_344_374,
            "prev_close": 741.30,
            "ts": "2026-05-21T20:00:00Z",
        },
        "NVDA": {
            "symbol": "NVDA",
            "last_price": 219.17,
            "open_price": 223.50,
            "high": 224.10,
            "low": 218.85,
            "volume": 6_222_899,
            "prev_close": 223.49,
            "ts": "2026-05-21T20:00:00Z",
        },
        "EMPTY": {
            # Missing last_price — should be skipped per existing convention.
            "symbol": "EMPTY",
            "last_price": None,
            "prev_close": 10.0,
            "volume": 0,
        },
        "NOPREV": {
            # Missing prev_close — should produce change_pct=0.0, not crash.
            "symbol": "NOPREV",
            "last_price": 50.00,
            "open_price": 50.00,
            "high": 50.10,
            "low": 49.90,
            "volume": 1000,
            "prev_close": None,
        },
    }


def test_bulk_prices_uses_snapshots_endpoint() -> None:
    """The bulk path must call _get_alpaca_bulk_snapshots, not _alpaca_quote_to_price.

    This is the architectural invariant: snapshots have prev_close + cumulative
    volume, quotes do not. Switching back to quotes would silently re-introduce
    the fixture bug.
    """
    with patch.object(market_data, "_get_alpaca_bulk_snapshots", side_effect=_fake_snapshots) as mock_snap:
        result = market_data._get_alpaca_bulk_prices(["SPY", "NVDA"])
    mock_snap.assert_called_once()
    assert "SPY" in result and "NVDA" in result


def test_change_pct_computed_from_prev_close() -> None:
    """change_pct = (last - prev) / prev * 100 — not the hardcoded 0.0."""
    with patch.object(market_data, "_get_alpaca_bulk_snapshots", side_effect=_fake_snapshots):
        result = market_data._get_alpaca_bulk_prices(["SPY", "NVDA"])
    # SPY: (742.91 - 741.30) / 741.30 * 100 = 0.2172...% → rounds to 0.22
    assert result["SPY"]["change_pct"] == pytest.approx(0.22, abs=0.01)
    # NVDA: (219.17 - 223.49) / 223.49 * 100 = -1.9329...% → rounds to -1.93
    assert result["NVDA"]["change_pct"] == pytest.approx(-1.93, abs=0.01)


def test_volume_is_cumulative_daily_not_ask_size() -> None:
    """volume reads from dailyBar.v (cumulative trade volume), not ask_size.

    The "vol=40" pre-fix shape was Alpaca's ask_size on quiet symbols. Real
    daily volume for liquid names should be in the hundreds-of-thousands to
    millions.
    """
    with patch.object(market_data, "_get_alpaca_bulk_snapshots", side_effect=_fake_snapshots):
        result = market_data._get_alpaca_bulk_prices(["SPY", "NVDA"])
    assert result["SPY"]["volume"] == 1_344_374
    assert result["NVDA"]["volume"] == 6_222_899


def test_source_marker_is_alpaca_snapshot() -> None:
    """Source distinguishes new path (alpaca_snapshot) from legacy quote-based shape (alpaca)."""
    with patch.object(market_data, "_get_alpaca_bulk_snapshots", side_effect=_fake_snapshots):
        result = market_data._get_alpaca_bulk_prices(["SPY"])
    assert result["SPY"]["source"] == "alpaca_snapshot"


def test_missing_last_price_is_skipped() -> None:
    """Snapshots without last_price should not appear in the result."""
    with patch.object(market_data, "_get_alpaca_bulk_snapshots", side_effect=_fake_snapshots):
        result = market_data._get_alpaca_bulk_prices(["SPY", "EMPTY"])
    assert "EMPTY" not in result
    assert "SPY" in result


def test_missing_prev_close_does_not_crash() -> None:
    """Symbols with last_price but no prev_close yield change_pct=0.0 (graceful)."""
    with patch.object(market_data, "_get_alpaca_bulk_snapshots", side_effect=_fake_snapshots):
        result = market_data._get_alpaca_bulk_prices(["NOPREV"])
    assert "NOPREV" in result
    assert result["NOPREV"]["change_pct"] == 0.0
    assert result["NOPREV"]["volume"] == 1000


def test_empty_snapshots_returns_empty_dict() -> None:
    """If the snapshots fetcher returns {}, bulk_prices returns {} (caller falls through)."""
    with patch.object(market_data, "_get_alpaca_bulk_snapshots", return_value={}):
        result = market_data._get_alpaca_bulk_prices(["SPY"])
    assert result == {}


def test_result_shape_matches_legacy_consumers() -> None:
    """Required fields preserved so heatmap / movers consumers don't need updating."""
    with patch.object(market_data, "_get_alpaca_bulk_snapshots", side_effect=_fake_snapshots):
        result = market_data._get_alpaca_bulk_prices(["SPY"])
    row = result["SPY"]
    for key in ("symbol", "price", "change_pct", "high", "low", "volume", "timestamp", "source"):
        assert key in row, f"missing field {key!r} from bulk_prices output"


def test_no_quote_to_price_invocation() -> None:
    """The legacy quote-based parser should not be called from the bulk path."""
    with patch.object(market_data, "_get_alpaca_bulk_snapshots", side_effect=_fake_snapshots), \
         patch.object(market_data, "_alpaca_quote_to_price") as mock_q2p:
        market_data._get_alpaca_bulk_prices(["SPY", "NVDA"])
    mock_q2p.assert_not_called()
