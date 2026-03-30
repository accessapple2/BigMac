"""Lightweight investor-style score built from cached fundamentals and macro state."""
from __future__ import annotations
import threading
import time
from engine.market_data import _is_yf_limited, _set_yf_limited
from datetime import datetime
from rich.console import Console

console = Console()

_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 3600  # 1 hour — fundamentals don't change fast

# Approximate sector average P/E ratios
SECTOR_PE_AVERAGES = {
    "Technology": 30,
    "Communication Services": 22,
    "Consumer Cyclical": 25,
    "Consumer Defensive": 22,
    "Financial Services": 15,
    "Healthcare": 20,
    "Industrials": 22,
    "Energy": 12,
    "Basic Materials": 18,
    "Real Estate": 35,
    "Utilities": 18,
}
DEFAULT_SECTOR_PE = 22


def _score_grade(total_score: int) -> str:
    if total_score >= 80:
        return "A"
    if total_score >= 65:
        return "B"
    if total_score >= 50:
        return "C"
    if total_score >= 35:
        return "D"
    return "F"


def _degraded_result(symbol: str) -> dict:
    macro = _macro_score(symbol, "")
    total_score = 50 + max(-10, macro["score"] - 12)
    return {
        "symbol": symbol,
        "total_score": total_score,
        "grade": _score_grade(total_score),
        "components": {
            "valuation": {"score": 12, "pe_ratio": None, "sector": "", "sector_avg_pe": DEFAULT_SECTOR_PE, "peg_ratio": None, "price_to_book": None},
            "growth": {"score": 12, "revenue_growth_pct": None, "earnings_growth_pct": None},
            "quality": {"score": 12, "profit_margin": None, "roe": None, "debt_to_equity": None, "insider_pct": None, "institutional_pct": None},
            "macro": macro,
        },
        "company_name": symbol,
        "sector": "",
        "market_cap": None,
        "valuation_score": 12,
        "growth_score": 12,
        "quality_score": 12,
        "macro_score": macro["score"],
        "updated": datetime.now().isoformat(),
        "degraded": True,
    }


def _macro_score(symbol: str, sector: str) -> dict:
    score = 12
    notes = []
    try:
        from engine.regime_detector import detect_regime
        regime = detect_regime()
        reg = regime.get("regime", "UNKNOWN")
        if reg in ("BULL_TREND", "MELT_UP"):
            score += 4
            notes.append(reg)
        elif reg in ("BEAR_TREND", "CRASH_MODE"):
            score -= 4
            notes.append(reg)
        elif reg == "CHOPPY":
            notes.append(reg)
    except Exception:
        regime = {}

    try:
        from engine.cross_asset import get_cross_asset_monitor
        monitor = get_cross_asset_monitor()
        bias = (monitor.get("macro_bias") or {}).get("bias", "NEUTRAL")
        if bias in ("BULLISH", "RISK-ON"):
            score += 4
            notes.append(bias)
        elif bias in ("BEARISH", "RISK-OFF"):
            score -= 4
            notes.append(bias)
    except Exception:
        bias = "NEUTRAL"

    defensive = {"Energy", "Utilities", "Consumer Defensive", "Healthcare"}
    cyclical = {"Technology", "Communication Services", "Consumer Cyclical", "Industrials"}
    if bias in ("BEARISH", "RISK-OFF") and sector in defensive:
        score += 5
        notes.append("defensive tailwind")
    elif bias in ("BEARISH", "RISK-OFF") and sector in cyclical:
        score -= 4
        notes.append("cyclical headwind")
    elif bias in ("BULLISH", "RISK-ON") and sector in cyclical:
        score += 5
        notes.append("risk-on tailwind")
    elif bias in ("BULLISH", "RISK-ON") and sector in defensive:
        score -= 2
        notes.append("defensive lag")

    if symbol in {"XLE", "CVX", "XOM"} and bias in ("BULLISH", "RISK-ON"):
        score -= 1
    return {
        "score": max(0, min(25, score)),
        "macro_bias": bias,
        "notes": notes,
    }


def compute_fundamental_score(symbol: str) -> dict | None:
    """Compute fundamental score (0-100) for a symbol.

    Components:
    - Valuation (25)
    - Growth (25)
    - Quality/ownership (25)
    - Macro fit (25)
    """
    symbol = symbol.upper()
    cache_key = f"fund_{symbol}"
    with _cache_lock:
        if cache_key in _cache and time.time() - _cache[cache_key]["ts"] < _CACHE_TTL:
            return _cache[cache_key]["data"]

    if _is_yf_limited():
        result = _degraded_result(symbol)
        with _cache_lock:
            _cache[cache_key] = {"data": result, "ts": time.time()}
        return result
    try:
        from engine.stock_fundamentals import fetch_fundamentals
        info = fetch_fundamentals(symbol)
        if not info:
            result = _degraded_result(symbol)
            with _cache_lock:
                _cache[cache_key] = {"data": result, "ts": time.time()}
            return result

        components = {}
        total_score = 0

        # --- Valuation (25 pts) ---
        pe_score = 12
        pe_ratio = info.get("pe_trailing") or info.get("pe_forward")
        sector = info.get("sector", "")
        sector_avg = SECTOR_PE_AVERAGES.get(sector, DEFAULT_SECTOR_PE)
        peg_ratio = info.get("peg_ratio")
        price_to_book = info.get("price_to_book")

        if pe_ratio and pe_ratio > 0:
            pe_relative = pe_ratio / sector_avg
            if pe_relative < 0.5:
                pe_score = 24
            elif pe_relative < 0.8:
                pe_score = 21
            elif pe_relative < 1.0:
                pe_score = 18
            elif pe_relative < 1.3:
                pe_score = 13
            elif pe_relative < 2.0:
                pe_score = 7
            else:
                pe_score = 3
        elif pe_ratio and pe_ratio < 0:
            pe_score = 1
        if peg_ratio and 0 < peg_ratio < 1.5:
            pe_score = min(25, pe_score + 2)
        elif peg_ratio and peg_ratio > 3:
            pe_score = max(0, pe_score - 2)
        if price_to_book and price_to_book > 8:
            pe_score = max(0, pe_score - 1)

        components["valuation"] = {
            "score": pe_score,
            "pe_ratio": round(pe_ratio, 2) if pe_ratio else None,
            "sector": sector,
            "sector_avg_pe": sector_avg,
            "peg_ratio": peg_ratio,
            "price_to_book": price_to_book,
        }
        total_score += pe_score

        # --- Growth (25 pts) ---
        rev_score = 12
        rev_growth = info.get("revenue_growth")
        earn_growth = info.get("earnings_growth")
        if rev_growth is not None:
            if rev_growth > 30:
                rev_score = 22
            elif rev_growth > 15:
                rev_score = 19
            elif rev_growth > 5:
                rev_score = 16
            elif rev_growth > 0:
                rev_score = 12
            elif rev_growth > -10:
                rev_score = 7
            else:
                rev_score = 3
        if earn_growth is not None:
            if earn_growth > 20:
                rev_score = min(25, rev_score + 3)
            elif earn_growth < -10:
                rev_score = max(0, rev_score - 3)
        components["growth"] = {
            "score": rev_score,
            "revenue_growth_pct": rev_growth,
            "earnings_growth_pct": earn_growth,
        }
        total_score += rev_score

        # --- Quality / Ownership (25 pts) ---
        quality_score = 12
        profit_margin = info.get("profit_margin")
        roe = info.get("roe")
        debt_to_equity = info.get("debt_to_equity")
        insider_pct = info.get("insider_pct")
        inst_pct = info.get("institutional_pct")

        if profit_margin is not None:
            if profit_margin > 25:
                quality_score += 5
            elif profit_margin > 10:
                quality_score += 3
            elif profit_margin < 0:
                quality_score -= 4
        if roe is not None:
            if roe > 20:
                quality_score += 4
            elif roe < 0:
                quality_score -= 4
        if debt_to_equity is not None:
            if debt_to_equity < 50:
                quality_score += 2
            elif debt_to_equity > 150:
                quality_score -= 3
        if insider_pct is not None and insider_pct > 5:
            quality_score += 1
        if inst_pct is not None and inst_pct > 70:
            quality_score += 1
        quality_score = max(0, min(25, quality_score))

        components["quality"] = {
            "score": quality_score,
            "profit_margin": profit_margin,
            "roe": roe,
            "debt_to_equity": debt_to_equity,
            "insider_pct": insider_pct,
            "institutional_pct": inst_pct,
        }
        total_score += quality_score

        # --- Macro Fit (25 pts) ---
        macro = _macro_score(symbol, sector)
        components["macro"] = macro
        total_score += macro["score"]

        grade = _score_grade(total_score)

        result = {
            "symbol": symbol,
            "total_score": total_score,
            "grade": grade,
            "components": components,
            "company_name": info.get("company_name", symbol),
            "sector": sector,
            "market_cap": info.get("market_cap"),
            "valuation_score": pe_score,
            "growth_score": rev_score,
            "quality_score": quality_score,
            "macro_score": macro["score"],
            "updated": datetime.now().isoformat(),
        }

        with _cache_lock:
            _cache[cache_key] = {"data": result, "ts": time.time()}

        return result

    except Exception as e:
        err = str(e)
        if "Too Many Requests" in err or "Rate" in err:
            _set_yf_limited()
        console.log(f"[red]Fundamental score error for {symbol}: {e}")
        return None


def scan_fundamentals(symbols: list = None) -> list:
    """Compute fundamental scores for all watchlist symbols."""
    if symbols is None:
        from config import WATCH_STOCKS
        symbols = WATCH_STOCKS

    results = []
    for sym in symbols:
        score = compute_fundamental_score(sym)
        if score:
            results.append(score)

    results.sort(key=lambda x: x["total_score"], reverse=True)
    return results


def build_fundamental_prompt_section(symbol: str) -> str:
    """Build prompt section for AI injection."""
    score = compute_fundamental_score(symbol)
    if not score:
        return ""

    c = score["components"]
    lines = [f"\n--- Fundamental Score: {symbol} ({score['grade']}, {score['total_score']}/100) ---"]

    pe = c["valuation"]
    if pe["pe_ratio"]:
        lines.append(f"  P/E: {pe['pe_ratio']} vs sector avg {pe['sector_avg_pe']} ({pe['score']}/25)")
    rev = c["growth"]
    if rev["revenue_growth_pct"] is not None:
        lines.append(f"  Revenue Growth: {rev['revenue_growth_pct']:+.1f}% ({rev['score']}/25)")
    if rev["earnings_growth_pct"] is not None:
        lines.append(f"  Earnings Growth: {rev['earnings_growth_pct']:+.1f}%")
    quality = c["quality"]
    if quality["profit_margin"] is not None:
        lines.append(f"  Profit Margin: {quality['profit_margin']:+.1f}% ({quality['score']}/25)")
    macro = c["macro"]
    lines.append(f"  Macro Fit: {macro['score']}/25 [{macro['macro_bias']}]")

    return "\n".join(lines)
