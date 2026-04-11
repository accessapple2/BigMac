"""
FRED (Federal Reserve Economic Data) integration.
Fetches macro indicators and computes regime signals.
Requires FRED_API_KEY environment variable.
"""

import os
import requests
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"


def _fetch_series(api_key: str, series_id: str, limit: int = 5) -> list[dict]:
    """Fetch the most recent N observations for a FRED series."""
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": limit,
    }
    try:
        resp = requests.get(FRED_BASE_URL, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json().get("observations", [])
    except Exception as e:
        logger.warning(f"FRED fetch failed for {series_id}: {e}")
        return []


def _parse_value(obs: dict) -> float | None:
    """Parse a FRED observation value, returning None for missing data."""
    val = obs.get("value", ".")
    if val == "." or val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _compute_trend(current: float | None, previous: float | None) -> str:
    """Return RISING, FALLING, or FLAT based on direction of change."""
    if current is None or previous is None:
        return "FLAT"
    delta = current - previous
    if delta > 0.01:
        return "RISING"
    elif delta < -0.01:
        return "FALLING"
    return "FLAT"


def get_fred_indicators() -> dict:
    """
    Fetch key macro indicators from the FRED API.

    Returns a dict keyed by series ID, each containing:
        name, signal description, value, previous, trend, last_updated
    """
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        logger.error("FRED_API_KEY not set in environment")
        return {}

    indicators = {
        'T10Y2Y': {
            'name': 'Yield Curve (10Y-2Y)',
            'signal': 'BEARISH if inverted (<0), watch flattening',
        },
        'UNRATE': {
            'name': 'Unemployment Rate',
            'signal': 'RISK_OFF if rising, RISK_ON if falling',
        },
        'CPIAUCSL': {
            'name': 'CPI Inflation (All Items)',
            'signal': 'RISK_OFF if rising (hot inflation), RISK_ON if falling',
        },
        'ICSA': {
            'name': 'Initial Jobless Claims',
            'signal': 'RISK_OFF if >250k, RISK_ON if below',
        },
        'DFF': {
            'name': 'Federal Funds Effective Rate',
            'signal': 'Context for monetary policy tightness',
        },
        'BAMLH0A0HYM2': {
            'name': 'High Yield Credit Spread (OAS)',
            'signal': 'RISK_OFF if widening, RISK_ON if tightening',
        },
    }

    for series_id, meta in indicators.items():
        observations = _fetch_series(api_key, series_id, limit=5)

        # FRED returns desc order; index 0 = most recent
        current = _parse_value(observations[0]) if len(observations) > 0 else None
        previous = _parse_value(observations[1]) if len(observations) > 1 else None

        last_updated = observations[0].get("date") if observations else None

        meta["value"] = current
        meta["previous"] = previous
        meta["trend"] = _compute_trend(current, previous)
        meta["last_updated"] = last_updated

    return indicators


def get_macro_regime_signal(indicators: dict | None = None) -> dict:
    """
    Score macro conditions and return a regime signal.

    Args:
        indicators: output of get_fred_indicators(). Fetched fresh if not provided.

    Returns:
        {
            "regime": "BULLISH" | "BEARISH" | "NEUTRAL",
            "bullish_signals": int,
            "bearish_signals": int,
            "details": list[str],
        }
    """
    if indicators is None:
        indicators = get_fred_indicators()

    bullish_signals = 0
    bearish_signals = 0
    details = []

    # Yield curve
    if 'T10Y2Y' in indicators:
        val = indicators['T10Y2Y']['value']
        if val is not None:
            if val < 0:
                bearish_signals += 2
                details.append(f"T10Y2Y={val:.2f} (inverted) → bearish +2")
            elif val < 0.5:
                bearish_signals += 1
                details.append(f"T10Y2Y={val:.2f} (flat) → bearish +1")
            else:
                bullish_signals += 1
                details.append(f"T10Y2Y={val:.2f} (normal) → bullish +1")

    # Unemployment rate
    if 'UNRATE' in indicators:
        trend = indicators['UNRATE']['trend']
        val = indicators['UNRATE']['value']
        if trend == 'RISING':
            bearish_signals += 1
            details.append(f"UNRATE={val} (rising) → bearish +1")
        elif trend == 'FALLING':
            bullish_signals += 1
            details.append(f"UNRATE={val} (falling) → bullish +1")

    # Initial jobless claims
    if 'ICSA' in indicators:
        val = indicators['ICSA']['value']
        if val is not None:
            if val > 250000:
                bearish_signals += 1
                details.append(f"ICSA={val:,.0f} (>250k) → bearish +1")
            else:
                bullish_signals += 1
                details.append(f"ICSA={val:,.0f} (healthy) → bullish +1")

    # CPI inflation
    if 'CPIAUCSL' in indicators:
        trend = indicators['CPIAUCSL']['trend']
        val = indicators['CPIAUCSL']['value']
        if trend == 'RISING':
            bearish_signals += 1
            details.append(f"CPIAUCSL={val} (rising) → bearish +1")
        elif trend == 'FALLING':
            bullish_signals += 1
            details.append(f"CPIAUCSL={val} (falling) → bullish +1")

    # Credit spreads - widening = risk off, tightening = risk on
    if 'BAMLH0A0HYM2' in indicators:
        val = indicators['BAMLH0A0HYM2']['value']
        trend = indicators['BAMLH0A0HYM2']['trend']
        if val is not None:
            if val > 5.0:
                bearish_signals += 3
                details.append(f"BAMLH0A0HYM2={val:.2f} (extreme stress) → bearish +3")
            elif val > 4.0:
                bearish_signals += 2
                details.append(f"BAMLH0A0HYM2={val:.2f} (elevated risk) → bearish +2")
            elif val > 3.0:
                bearish_signals += 1
                details.append(f"BAMLH0A0HYM2={val:.2f} (caution zone) → bearish +1")
            else:
                bullish_signals += 1
                details.append(f"BAMLH0A0HYM2={val:.2f} (compressed spreads) → bullish +1")
        if trend == 'RISING':
            bearish_signals += 1
            details.append("BAMLH0A0HYM2 widening → bearish +1")
        elif trend == 'FALLING':
            bullish_signals += 1
            details.append("BAMLH0A0HYM2 tightening → bullish +1")

    # Final regime
    if bearish_signals >= 3:
        regime = "BEARISH"
    elif bullish_signals >= 3:
        regime = "BULLISH"
    else:
        regime = "NEUTRAL"

    return {
        "regime": regime,
        "bullish_signals": bullish_signals,
        "bearish_signals": bearish_signals,
        "details": details,
        "timestamp": datetime.utcnow().isoformat(),
    }
