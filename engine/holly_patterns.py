#!/usr/bin/env python3
"""
Holly Pattern Scanner — Feature 6E
Six additional pattern detectors to enhance signal diversity.

Patterns:
  1. volume_spike      — unusual volume surge on up-day
  2. gap_up            — opening gap above prior close with continuation
  3. rsi_oversold      — RSI deeply oversold with reversal signal
  4. breakout          — price clearing resistance on volume
  5. pullback_support  — orderly pullback to 50MA support in uptrend
  6. sector_momentum   — sector ETF trend backing individual pick

Interface mirrors other rules functions:
    holly_rules(market_ctx, scan_picks) -> {"action": "BUY"|"PASS", ...}
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────
VOLUME_SPIKE_RATIO   = 2.0   # volume ≥ 2× average
GAP_UP_MIN_PCT       = 0.015 # gap ≥ 1.5%
RSI_OVERSOLD_MAX     = 32    # RSI ≤ 32 = deeply oversold
RSI_REVERSAL_MIN     = 35    # RSI must be ticking above 35 to confirm reversal
BREAKOUT_VOL_RATIO   = 1.5   # breakout needs 1.5× avg volume
PULLBACK_DIST_MAX    = 0.04  # price within 4% of SMA50 = support zone
SECTOR_ETF_MAP: dict[str, str] = {
    # Tech
    "AAPL": "XLK", "MSFT": "XLK", "NVDA": "XLK", "AMD": "XLK", "AVGO": "XLK",
    "META": "XLK", "GOOG": "XLK", "GOOGL": "XLK",
    # Financials
    "JPM": "XLF", "BAC": "XLF", "GS": "XLF", "MS": "XLF", "HOOD": "XLF",
    # Energy
    "XOM": "XLE", "CVX": "XLE", "OXY": "XLE",
    # Healthcare
    "UNH": "XLV", "JNJ": "XLV", "PFE": "XLV",
    # Consumer discretionary
    "AMZN": "XLY", "TSLA": "XLY", "HD": "XLY",
    # Industrials
    "CAT": "XLI", "BA": "XLI", "HON": "XLI",
    # Materials / Metals
    "AG": "XME", "HL": "XME", "NEM": "XME",
}
MIN_SCORE = 5  # minimum composite score to fire a BUY


# ── Individual pattern detectors ─────────────────────────────────────────────

def _detect_volume_spike(pick: dict) -> tuple[int, str]:
    """Score a volume spike: high volume on a positive-return day."""
    vol_ratio = float(pick.get("volume_ratio", 1.0))
    change    = float(pick.get("change_pct", pick.get("roc_1d", 0)))
    score = 0
    notes = []

    if vol_ratio >= VOLUME_SPIKE_RATIO * 1.5:  # extreme: 3× avg
        score += 4
        notes.append(f"vol={vol_ratio:.1f}x [extreme]")
    elif vol_ratio >= VOLUME_SPIKE_RATIO:
        score += 2
        notes.append(f"vol={vol_ratio:.1f}x")

    if change > 0.01:
        score += 1
        notes.append(f"up {change:.1%}")

    return score, " ".join(notes)


def _detect_gap_up(pick: dict) -> tuple[int, str]:
    """Score a gap-up open with continuation (open well above prior close)."""
    close  = float(pick.get("close", 0))
    open_  = float(pick.get("open", 0))
    prev   = float(pick.get("prev_close", 0))

    if close <= 0 or open_ <= 0 or prev <= 0:
        return 0, ""

    gap_pct  = (open_ - prev) / prev if prev > 0 else 0
    cont_pct = (close - open_) / open_ if open_ > 0 else 0
    score = 0
    notes = []

    if gap_pct >= GAP_UP_MIN_PCT * 2:  # strong gap ≥ 3%
        score += 3
        notes.append(f"gap={gap_pct:.1%} [strong]")
    elif gap_pct >= GAP_UP_MIN_PCT:
        score += 2
        notes.append(f"gap={gap_pct:.1%}")

    if cont_pct > 0:  # held gains after gap
        score += 1
        notes.append(f"held +{cont_pct:.1%}")

    return score, " ".join(notes)


def _detect_rsi_oversold(pick: dict) -> tuple[int, str]:
    """Score an RSI oversold reversal: deeply oversold + momentum turning."""
    rsi     = float(pick.get("rsi_14", 50))
    roc_5d  = float(pick.get("roc_5d", 0))
    vol_ratio = float(pick.get("volume_ratio", 1.0))
    score = 0
    notes = []

    if rsi <= RSI_OVERSOLD_MAX:
        score += 3
        notes.append(f"RSI={rsi:.0f} [oversold]")
    elif rsi <= 38:
        score += 1
        notes.append(f"RSI={rsi:.0f} [low]")
    else:
        return 0, ""  # not oversold enough

    if roc_5d > 0:  # 5-day momentum turning positive
        score += 2
        notes.append(f"roc5d={roc_5d:.1%} [reversal]")

    if vol_ratio >= 1.3:  # volume confirming bounce
        score += 1
        notes.append(f"vol={vol_ratio:.1f}x")

    return score, " ".join(notes)


def _detect_breakout(pick: dict) -> tuple[int, str]:
    """Score a breakout above SMA20 resistance with volume confirmation."""
    close     = float(pick.get("close", 0))
    sma20     = float(pick.get("sma_20", 0))
    vol_ratio = float(pick.get("volume_ratio", 1.0))
    roc_5d    = float(pick.get("roc_5d", 0))

    if close <= 0 or sma20 <= 0:
        return 0, ""

    dist = (close - sma20) / sma20
    score = 0
    notes = []

    # Cleanly above SMA20 (not extended >8%)
    if 0.005 <= dist <= 0.08:
        score += 3
        notes.append(f"above SMA20 +{dist:.1%}")
    elif 0 < dist < 0.005:
        score += 1
        notes.append(f"just above SMA20 +{dist:.1%}")
    else:
        return 0, ""  # below or too extended

    if vol_ratio >= BREAKOUT_VOL_RATIO:
        score += 2
        notes.append(f"vol={vol_ratio:.1f}x")

    if roc_5d > 0.02:
        score += 1
        notes.append(f"momentum roc5d={roc_5d:.1%}")

    return score, " ".join(notes)


def _detect_pullback_support(pick: dict) -> tuple[int, str]:
    """Score a pullback to 50MA support in an established uptrend."""
    close  = float(pick.get("close", 0))
    sma20  = float(pick.get("sma_20", 0))
    sma50  = float(pick.get("sma_50", 0))
    rsi    = float(pick.get("rsi_14", 50))

    if close <= 0 or sma50 <= 0:
        return 0, ""

    dist_50 = (close - sma50) / sma50
    score = 0
    notes = []

    # Near SMA50 support (within 4%)
    if -PULLBACK_DIST_MAX <= dist_50 <= PULLBACK_DIST_MAX:
        score += 3
        notes.append(f"near SMA50 {dist_50:+.1%}")
    else:
        return 0, ""

    # Uptrend: SMA20 > SMA50
    if sma20 > 0 and sma20 > sma50:
        score += 2
        notes.append("SMA20>SMA50 uptrend")

    # RSI not overbought
    if 35 <= rsi <= 60:
        score += 1
        notes.append(f"RSI={rsi:.0f}")

    return score, " ".join(notes)


def _detect_sector_momentum(pick: dict, market_ctx: dict) -> tuple[int, str]:
    """Score sector-ETF backing: pick in a sector whose ETF is in uptrend."""
    sym = pick.get("symbol", "")
    etf = SECTOR_ETF_MAP.get(sym)
    if not etf:
        return 0, ""

    # Look for ETF in scan_picks or volume_spikes as a proxy for ETF health
    all_picks = market_ctx.get("deep_scan_top", []) + market_ctx.get("volume_spikes", [])
    etf_data  = next((p for p in all_picks if p.get("symbol") == etf), None)

    score = 0
    notes = []

    if etf_data:
        etf_roc = float(etf_data.get("roc_5d", 0))
        if etf_roc > 0.01:
            score += 3
            notes.append(f"{etf} sector +{etf_roc:.1%}")
        elif etf_roc > 0:
            score += 1
            notes.append(f"{etf} sector positive")

    # Fallback: check regime
    regime = str(market_ctx.get("regime", "")).upper()
    if "BULL" in regime:
        score += 1
        notes.append(f"regime={regime}")

    return score, " ".join(notes)


# ── Composite scorer ──────────────────────────────────────────────────────────

def score_pick(pick: dict, market_ctx: dict) -> tuple[int, str, str]:
    """
    Return (total_score, best_pattern_name, reason_string) for a single pick.
    Applies all 6 detectors; picks the highest-scoring pattern as primary label.
    """
    sym = pick.get("symbol", "")
    detectors = [
        ("volume_spike",    _detect_volume_spike(pick)),
        ("gap_up",          _detect_gap_up(pick)),
        ("rsi_oversold",    _detect_rsi_oversold(pick)),
        ("breakout",        _detect_breakout(pick)),
        ("pullback_support", _detect_pullback_support(pick)),
        ("sector_momentum", _detect_sector_momentum(pick, market_ctx)),
    ]

    # Sum all scores
    total = sum(s for _, (s, _) in detectors)

    # Primary pattern = highest individual score
    best_name, (best_score, best_notes) = max(
        detectors, key=lambda x: x[1][0]
    )

    # Build reason string from all firing detectors
    parts = [f"{name}({notes})" for name, (s, notes) in detectors if s > 0]
    reason = f"Holly: {sym} [{best_name}] score={total} — " + " | ".join(parts)

    return total, best_name, reason


# ── Public rules function ─────────────────────────────────────────────────────

def holly_rules(market_ctx: dict[str, Any], scan_picks: list[dict]) -> dict[str, Any]:
    """
    Holly pattern scanner — evaluates scan picks across 6 pattern detectors.
    Returns the highest-scoring pick if score >= MIN_SCORE.
    """
    vix    = float(market_ctx.get("vix", 20))
    regime = str(market_ctx.get("regime", market_ctx.get("market_regime", "NEUTRAL"))).upper()

    # Stand down in high-stress market conditions
    if vix > 38 or regime in ("CRISIS",):
        return {"action": "PASS", "reason": f"Holly: stand-down (VIX={vix:.1f} regime={regime})"}

    best_sym   = None
    best_score = 0
    best_reason = ""
    best_pattern = ""
    best_conf  = 55

    for pick in scan_picks[:10]:
        sym = pick.get("symbol", "")
        if not sym:
            continue

        try:
            total, pattern, reason = score_pick(pick, market_ctx)
        except Exception as e:
            logger.debug(f"Holly score_pick error for {sym}: {e}")
            continue

        if total > best_score:
            best_score   = total
            best_sym     = sym
            best_reason  = reason
            best_pattern = pattern

    if best_sym and best_score >= MIN_SCORE:
        # Scale confidence: MIN_SCORE (5) → 60, 10+ → 85
        best_conf = min(85, 55 + best_score * 3)
        return {
            "action":     "BUY",
            "symbol":     best_sym,
            "confidence": best_conf,
            "reason":     best_reason,
            "strategy":   f"holly_{best_pattern}",
        }

    return {
        "action": "PASS",
        "reason": f"Holly: no high-confidence setups (best={best_score}/{MIN_SCORE} required)",
    }


# ── Standalone test ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    _sample_picks = [
        {
            "symbol": "NVDA", "close": 125.0, "open": 122.0, "prev_close": 120.0,
            "sma_20": 118.0, "sma_50": 110.0, "rsi_14": 52, "volume_ratio": 2.5,
            "roc_5d": 0.04, "change_pct": 0.025,
        },
        {
            "symbol": "HOOD", "close": 28.5, "open": 28.0, "prev_close": 27.0,
            "sma_20": 27.5, "sma_50": 26.0, "rsi_14": 29, "volume_ratio": 3.1,
            "roc_5d": -0.01, "change_pct": 0.018,
        },
        {
            "symbol": "AMD", "close": 95.0, "open": 94.8, "prev_close": 94.5,
            "sma_20": 92.0, "sma_50": 95.5, "rsi_14": 44, "volume_ratio": 1.1,
            "roc_5d": 0.005, "change_pct": 0.005,
        },
    ]
    _ctx = {"vix": 18.0, "regime": "BULL_CROSS", "deep_scan_top": _sample_picks, "volume_spikes": []}
    result = holly_rules(_ctx, _sample_picks)
    print(result)
