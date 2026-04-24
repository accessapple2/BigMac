"""
Option chain lookup + strike selection.

Provider priority (CHAIN_PROVIDER env var, default 'alpaca'):
  'mock'    — synthetic quotes only (offline / test)
  'polygon' — Polygon.io, fall back to mock on None
  'alpaca'  — Alpaca Markets, fall back to mock on None (NOT polygon)
  'auto'    — alpaca -> polygon -> mock
"""
from __future__ import annotations
import os
from typing import Optional
from .mock_data import is_mock_mode, mock_spread_quote, SpreadQuote

CHAIN_PROVIDER: str = os.environ.get("CHAIN_PROVIDER", "alpaca")

# Set after every get_spread_quote() call — readable by callers for logging.
last_provider_used: str = "none"


def get_spread_quote(
    ticker: str, structure: str, dte_target: int, width: float = 1.0,
) -> Optional[SpreadQuote]:
    """Returns None if quote unavailable (illiquid, rate-limited, etc.)."""
    global last_provider_used

    # Explicit CHAIN_PROVIDER env var wins over module default.
    # This lets FORCE_MOCK_DATA=1 (for IV rank) coexist with CHAIN_PROVIDER=alpaca (for chain).
    explicit = os.environ.get("CHAIN_PROVIDER")
    provider = explicit if explicit else CHAIN_PROVIDER

    # mock path: explicit 'mock' OR FORCE_MOCK_DATA with no live-provider override
    if provider == "mock" or (is_mock_mode() and not explicit):
        return _use_mock(ticker, structure, dte_target, width)

    # Alpaca path ('alpaca' default, or 'auto')
    if provider in ("alpaca", "auto"):
        try:
            from .alpaca_chain_client import build_spread_quote as _alpaca
            result = _alpaca(ticker, structure, dte_target, width)
            if result is not None:
                last_provider_used = "alpaca"
                print(f"[chain_lookup] provider=alpaca ticker={ticker} "
                      f"structure={structure} result=ok")
                return result
            _fallback_log("alpaca", ticker, structure, dte_target,
                          "trying polygon" if provider == "auto" else "falling back to mock")
        except ImportError as e:
            print(f"[chain_lookup] alpaca import error: {e}")

    # Polygon path: only when explicitly requested ('polygon' or 'auto')
    # NOT reached when provider='alpaca' — Refinement 1
    if provider in ("polygon", "auto"):
        try:
            from .polygon_client import build_spread_quote as _poly
            result = _poly(ticker, structure, dte_target, width)
            if result is not None:
                last_provider_used = "polygon"
                print(f"[chain_lookup] provider=polygon ticker={ticker} "
                      f"structure={structure} result=ok")
                return result
            _fallback_log("polygon", ticker, structure, dte_target, "falling back to mock")
        except ImportError as e:
            print(f"[chain_lookup] polygon import error: {e}")

    return _use_mock(ticker, structure, dte_target, width)


def _use_mock(ticker: str, structure: str, dte_target: int, width: float) -> Optional[SpreadQuote]:
    global last_provider_used
    result = mock_spread_quote(ticker, structure, dte_target, width)
    last_provider_used = "mock"
    status = "ok" if result is not None else "none"
    print(f"[chain_lookup] provider=mock ticker={ticker} structure={structure} result={status}")
    return result


def _fallback_log(provider: str, ticker: str, structure: str, dte: int, next_step: str) -> None:
    print(f"[chain_lookup] {provider} returned None for {ticker}/{structure}/{dte}d — {next_step}")


def select_width(spot_price: float) -> float:
    """
    Width must exceed ~2x ATM premium for spread math to work.
    ATM premium ~= 1.5% of spot; need width >= ~3% of spot.
    """
    if spot_price < 30:
        return 1.0
    elif spot_price < 100:
        return 2.5
    elif spot_price < 250:
        return 5.0
    elif spot_price < 500:
        return 10.0
    else:
        return 20.0  # SPY ~$700, QQQ ~$600
