"""
Mock data source for Task 5 development.

PURPOSE: Unblock bull_spread_v1 signal generation while Polygon.io activation
is in flight. Designed to swap cleanly to real Polygon endpoints when live.

USAGE:
  - USE_MOCK_DATA flag defaults to True when POLYGON_API_KEY env var is unset
  - Every mock call logs a warning so we can't accidentally trade on fake data
  - Mock values span the full IV-rank decision space to exercise all branches

DO NOT USE FOR LIVE TRADING. The logs are intentionally loud.
"""
from __future__ import annotations
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Optional

# Import polygon_config so .env is loaded before we check the key
from .polygon_config import is_polygon_configured


def is_mock_mode() -> bool:
    """True when we should use mocks (Polygon key not set or mock forced)."""
    if os.environ.get("FORCE_MOCK_DATA", "").lower() in ("1", "true", "yes"):
        return True
    return not is_polygon_configured()


def _warn_mock(context: str) -> None:
    print(f"⚠️  MOCK DATA in use ({context}) — NOT FOR LIVE TRADING", file=sys.stderr)


# Edge-case IV ranks to exercise every branch of the strategy decision tree.
# Under-threshold (debit call): SPY=18, AAPL=35, NVDA=39
# Boundary: QQQ=50
# Over-threshold (credit put): MSFT=55, META=72, GOOGL=85, TSLA=92
# Extreme: IWM=95 (very high), AMZN=8 (very low)
MOCK_IV_RANK: dict[str, float] = {
    "SPY":   18.0,
    "AAPL":  35.0,
    "NVDA":  39.0,
    "QQQ":   50.0,
    "MSFT":  55.0,
    "META":  72.0,
    "GOOGL": 85.0,
    "TSLA":  92.0,
    "IWM":   95.0,
    "AMZN":   8.0,
}

# Current spot prices (roughly realistic as of Apr 2026)
MOCK_SPOT: dict[str, float] = {
    "SPY":   708.64,
    "QQQ":   598.20,
    "IWM":   220.50,
    "AAPL":  210.30,
    "MSFT":  358.96,
    "NVDA":  197.42,
    "META":  612.80,
    "GOOGL": 182.15,
    "AMZN":  225.40,
    "TSLA":  285.10,
}

# Synthetic 30-day historical price trajectory shape for setup classification.
# Shape indicates the setup:
#   "momentum" = rising hard, recent breakout (short DTE signal)
#   "pullback" = prior uptrend then pullback, now reclaiming (longer DTE signal)
#   "neutral"  = flat/choppy (default 10 DTE)
MOCK_SETUP: dict[str, str] = {
    "SPY":   "neutral",
    "QQQ":   "momentum",
    "IWM":   "pullback",
    "AAPL":  "pullback",
    "MSFT":  "momentum",
    "NVDA":  "neutral",
    "META":  "momentum",
    "GOOGL": "pullback",
    "AMZN":  "neutral",
    "TSLA":  "momentum",
}


@dataclass
class MockIVResult:
    ticker: str
    iv_rank: float
    iv_percentile: float
    is_elevated: bool
    source: str = "mock"


def mock_iv_rank(ticker: str) -> Optional[MockIVResult]:
    _warn_mock(f"iv_rank({ticker})")
    if ticker not in MOCK_IV_RANK:
        return None
    rank = MOCK_IV_RANK[ticker]
    return MockIVResult(
        ticker=ticker,
        iv_rank=rank,
        iv_percentile=rank,  # Close enough for mock
        is_elevated=(rank >= 80.0),
    )


def mock_spot_price(ticker: str) -> Optional[float]:
    _warn_mock(f"spot_price({ticker})")
    return MOCK_SPOT.get(ticker)


def mock_setup_classification(ticker: str) -> str:
    """Returns 'momentum' | 'pullback' | 'neutral'. Used to choose DTE."""
    _warn_mock(f"setup({ticker})")
    return MOCK_SETUP.get(ticker, "neutral")


@dataclass
class OptionLeg:
    """One leg of a multi-leg option order."""
    action: str      # "buy" | "sell"
    option_type: str # "call" | "put"
    strike: float
    expiration: str  # YYYY-MM-DD
    premium: float   # Mid-market estimate


@dataclass
class SpreadQuote:
    """Synthetic spread quote. Matches the shape we'll need from Polygon."""
    ticker: str
    structure: str    # "bull_call_spread" | "bull_put_spread"
    long_leg: OptionLeg
    short_leg: OptionLeg
    net_debit: float  # + = we pay, - = we collect (credit)
    net_credit: float # Always positive; 0 for debit spreads
    max_profit: float
    max_loss: float
    width: float
    dte: int


def _next_friday_at_least(days_out: int) -> str:
    """Return the next Friday >= days_out from today."""
    target = date.today() + timedelta(days=days_out)
    # Weekday: Mon=0, Fri=4
    days_to_friday = (4 - target.weekday()) % 7
    friday = target + timedelta(days=days_to_friday)
    return friday.isoformat()


def mock_spread_quote(
    ticker: str, structure: str, dte_target: int, width: float = 1.0
) -> Optional[SpreadQuote]:
    """
    Return a synthetic but realistic spread quote.

    For bull_call_spread: long ATM call, short ATM+width call
    For bull_put_spread:  short ATM put,  long ATM-width put (credit)

    Premium is a rough percent-of-spot heuristic:
    - ATM call/put: ~1.5% of spot for 10 DTE
    - Scales roughly with sqrt(DTE) and IV
    This is good enough for dev; real Polygon data swaps it out.
    """
    _warn_mock(f"spread_quote({ticker}, {structure}, {dte_target}d)")

    spot = mock_spot_price(ticker)
    if spot is None:
        return None

    import math
    iv = MOCK_IV_RANK.get(ticker, 50.0) / 100.0  # Rough IV proxy, 0-1 scale
    time_scale = math.sqrt(max(dte_target, 1) / 10.0)  # Normalize to 10 DTE
    atm_premium = spot * 0.015 * (1.0 + iv) * time_scale

    expiration = _next_friday_at_least(dte_target)

    if structure == "bull_call_spread":
        long_strike = float(math.floor(spot))
        short_strike = long_strike + width
        long_premium = atm_premium
        short_premium = atm_premium * 0.55  # OTM is cheaper
        net_debit = long_premium - short_premium
        if net_debit >= width:
            print(f"[mock] reject {ticker} {structure}: "
                  f"debit ${net_debit:.2f} >= width ${width}", file=sys.stderr)
            return None
        if short_strike <= long_strike:
            print(f"[mock] reject {ticker} {structure}: strikes collapsed",
                  file=sys.stderr)
            return None
        return SpreadQuote(
            ticker=ticker, structure=structure,
            long_leg=OptionLeg("buy", "call", long_strike, expiration, long_premium),
            short_leg=OptionLeg("sell", "call", short_strike, expiration, short_premium),
            net_debit=net_debit, net_credit=0.0,
            max_profit=(width - net_debit) * 100.0,
            max_loss=net_debit * 100.0,
            width=width, dte=dte_target,
        )
    elif structure == "bull_put_spread":
        short_strike = float(math.floor(spot))
        long_strike = short_strike - width
        short_premium = atm_premium
        long_premium = atm_premium * 0.55
        net_credit = short_premium - long_premium
        if net_credit >= width:
            print(f"[mock] reject {ticker} {structure}: "
                  f"credit ${net_credit:.2f} >= width ${width}", file=sys.stderr)
            return None
        if long_strike >= short_strike:
            print(f"[mock] reject {ticker} {structure}: strikes collapsed",
                  file=sys.stderr)
            return None
        return SpreadQuote(
            ticker=ticker, structure=structure,
            short_leg=OptionLeg("sell", "put", short_strike, expiration, short_premium),
            long_leg=OptionLeg("buy", "put", long_strike, expiration, long_premium),
            net_debit=0.0, net_credit=net_credit,
            max_profit=net_credit * 100.0,
            max_loss=(width - net_credit) * 100.0,
            width=width, dte=dte_target,
        )
    else:
        print(f"[mock_data] unknown structure: {structure}", file=sys.stderr)
        return None
