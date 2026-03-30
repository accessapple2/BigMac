"""Market Regime Detector — classify market as Bull/Bear/Choppy/Crash/Melt-Up."""
from __future__ import annotations
import threading
from datetime import datetime
from rich.console import Console

console = Console()

# Cached regime
_regime_cache: dict = {}
_regime_lock = threading.Lock()
_CACHE_TTL = 300  # 5 minutes


def detect_regime() -> dict:
    """Classify the current market regime.

    Uses SPY trend vs 50/200 MA, VIX level, and momentum to determine regime.

    Returns {regime, description, aggression_modifier, vix, spy_vs_50ma, spy_vs_200ma, updated}.
    """
    import time
    now = time.time()
    with _regime_lock:
        if _regime_cache and (now - _regime_cache.get("_ts", 0)) < _CACHE_TTL:
            return {k: v for k, v in _regime_cache.items() if k != "_ts"}

    try:
        from engine.market_data import get_stock_price, get_technical_indicators

        # SPY data
        spy_data = get_stock_price("SPY")
        spy_indicators = get_technical_indicators("SPY")

        if not spy_indicators or "error" in spy_data:
            return _default_regime()

        spy_price = spy_data.get("price", 0)
        spy_change = spy_data.get("change_pct", 0)
        sma50 = spy_indicators.get("sma_50", spy_price)
        sma200 = spy_indicators.get("sma_200", spy_price)
        rsi = spy_indicators.get("rsi", 50)
        vol_ratio = spy_indicators.get("volume_ratio", 1.0)

        # VIX
        vix_price = 20.0
        try:
            from engine.vix_monitor import get_vix_status
            vix = get_vix_status()
            if vix and vix.get("price"):
                vix_price = vix["price"]
        except Exception:
            pass

        # Compute regime
        above_50 = spy_price > sma50 if sma50 else True
        above_200 = spy_price > sma200 if sma200 else True
        golden_cross = sma50 > sma200 if (sma50 and sma200) else True

        spy_vs_50ma = round((spy_price / sma50 - 1) * 100, 2) if sma50 else 0
        spy_vs_200ma = round((spy_price / sma200 - 1) * 100, 2) if sma200 else 0

        # Classification logic
        if vix_price >= 30 and spy_change <= -2:
            regime = "CRASH_MODE"
            description = "Extreme fear. VIX elevated, sharp selloff in progress."
            aggression = 0.3  # Very conservative
        elif vix_price >= 25 and not above_200:
            regime = "BEAR_TREND"
            description = "Below 200 MA with elevated VIX. Defensive positioning."
            aggression = 0.5
        elif not above_50 and not above_200:
            regime = "BEAR_TREND"
            description = "Below both major MAs. Trend is down."
            aggression = 0.5
        elif rsi > 75 and above_50 and above_200 and vix_price < 15:
            regime = "MELT_UP"
            description = "Extreme bullish momentum, low VIX. Euphoria zone — trim into strength."
            aggression = 0.7
        elif above_50 and above_200 and golden_cross and vix_price < 20:
            regime = "BULL_TREND"
            description = "Above both MAs, golden cross, low VIX. Buy the dip."
            aggression = 1.0
        elif above_200 and not above_50:
            regime = "CHOPPY"
            description = "Above 200 MA but below 50 MA. Consolidation. Reduce size."
            aggression = 0.7
        else:
            regime = "CHOPPY"
            description = "Mixed signals. Trade smaller, wait for clarity."
            aggression = 0.7

        result = {
            "regime": regime,
            "description": description,
            "aggression_modifier": aggression,
            "vix": round(vix_price, 2),
            "spy_price": round(spy_price, 2),
            "spy_change": round(spy_change, 2),
            "spy_vs_50ma": spy_vs_50ma,
            "spy_vs_200ma": spy_vs_200ma,
            "rsi": round(rsi, 1) if rsi else None,
            "updated": datetime.now().isoformat(),
        }

        with _regime_lock:
            _regime_cache.update(result)
            _regime_cache["_ts"] = now

        return result

    except Exception as e:
        console.log(f"[red]Regime detection error: {e}")
        return _default_regime()


def _default_regime() -> dict:
    return {
        "regime": "UNKNOWN",
        "description": "Unable to determine market regime.",
        "aggression_modifier": 0.7,
        "vix": None,
        "spy_price": None,
        "spy_change": None,
        "spy_vs_50ma": None,
        "spy_vs_200ma": None,
        "rsi": None,
        "updated": datetime.now().isoformat(),
    }


def build_regime_prompt_section() -> str:
    """Build a text block for injection into AI prompts.
    Combines legacy VIX/MA regime with the 8/21 MA cross regime (primary signal).
    """
    regime = detect_regime()
    if regime["regime"] == "UNKNOWN":
        return ""

    base = (
        f"\n=== MARKET REGIME: {regime['regime']} ===\n"
        f"{regime['description']}\n"
        f"VIX: {regime['vix']}, SPY: ${regime['spy_price']} ({regime['spy_change']:+.2f}%)\n"
        f"SPY vs 50MA: {regime['spy_vs_50ma']:+.2f}% | vs 200MA: {regime['spy_vs_200ma']:+.2f}%\n"
        f"Aggression modifier: {regime['aggression_modifier']:.0%} "
        f"(reduce position sizes and conviction thresholds accordingly)\n"
    )

    # Append 8/21 MA cross section — primary trend signal, always shown
    try:
        from engine.regime_ma import build_ma_cross_prompt_section
        ma_section = build_ma_cross_prompt_section()
        if ma_section:
            base += ma_section
    except Exception:
        pass

    return base
