"""Quality Gate V3 — Every stock must pass 3/5 checks before a BUY is allowed.

Prevents buying losers by requiring fundamental quality + technical health.
"""
from __future__ import annotations
from rich.console import Console

console = Console()

# Symbols that skip quality gate (ETFs/indices have no earnings/revenue data)
# Dalio All Weather core holdings explicitly whitelisted — bonds, gold, and commodities
# are macro assets with no traditional earnings/revenue metrics.
GATE_EXEMPT = {
    "SPY", "QQQ", "XLE", "XOP", "IWM", "DIA", "GLD", "SLV", "USO",
    # Dalio All Weather — bonds & commodities (no earnings data by design)
    "TLT", "TLH", "IEF", "SHY", "BND",   # Treasuries / bond ETFs
    "GSG", "DJP", "PDBC", "DBC",           # Broad commodity ETFs
    "IAU", "SGOL",                          # Gold alternatives
}


def passes_quality_gate(symbol: str, indicators: dict = None) -> tuple:
    """Check if a stock passes the quality gate for buying.

    Returns (passes: bool, score: int out of 5, details: list[str]).
    Must pass 3 of 5 checks.

    HM-QG-CALIBRATION Patch 1 (2026-05-14): ETF-shape fast-path. If
    fundamentals come back with earnings_growth, revenue_growth, AND
    recommendation all None, the symbol is treated as an ETF — pass on
    technical health alone, unless RSI >= 70 (still block overbought).
    Fixes calibration bug where ETFs like TQQQ, IBIT, NUKZ scored 1.5/5
    and were rejected despite being legitimate buy candidates.
    """
    if symbol in GATE_EXEMPT:
        return True, 5, ["ETF/Index exempt"]

    score = 0
    details = []
    indicators = indicators or {}

    fund = None
    try:
        from engine.stock_fundamentals import fetch_fundamentals
        fund = fetch_fundamentals(symbol)
    except Exception as e:
        score += 1.5
        details.append(f"fundamentals error: {type(e).__name__}: {e!r}")

    # Patch 1: ETF-shape fast-path — all three core fundamentals are None.
    if fund and \
       fund.get("earnings_growth") is None and \
       fund.get("revenue_growth") is None and \
       fund.get("recommendation") is None:
        rsi = indicators.get("rsi")
        if rsi is not None and rsi >= 70:
            return False, 0, [
                f"ETF-shape detected (no fundamentals); RSI={rsi:.0f} overbought — BLOCKED"
            ]
        rsi_note = f"RSI={rsi:.0f} OK" if rsi is not None else "RSI=N/A"
        return True, 4, [f"ETF-shape detected (no fundamentals); {rsi_note}"]

    if fund:
        # 1. Earnings beat (positive earnings growth = recent beat)
        eg = fund.get("earnings_growth")
        if eg is not None and eg > 0:
            score += 1
            details.append(f"earnings_growth={eg:+.1f}%")
        elif eg is not None:
            details.append(f"FAIL earnings_growth={eg:.1f}%")
        else:
            details.append("SKIP earnings_growth=N/A")
            score += 0.5  # Partial credit for missing data

        # 2. Revenue growing (positive YoY)
        rg = fund.get("revenue_growth")
        if rg is not None and rg > 0:
            score += 1
            details.append(f"revenue_growth={rg:+.1f}%")
        elif rg is not None:
            details.append(f"FAIL revenue_growth={rg:.1f}%")
        else:
            details.append("SKIP revenue_growth=N/A")
            score += 0.5

        # 4. Analyst consensus Buy or Strong Buy
        # Patch 2 (HM-QG-CALIBRATION): "hold"/"neutral" — the median
        # Wall Street rating — now earn 0.5 partial credit instead of
        # being treated as outright FAIL. Outright sells still FAIL.
        rec = fund.get("recommendation", "")
        _rec_low = (rec or "").lower()
        if _rec_low in ("buy", "strongbuy", "strong_buy", "overweight"):
            score += 1
            details.append(f"analyst={rec}")
        elif _rec_low in ("hold", "neutral"):
            score += 0.5
            details.append(f"analyst={rec} (partial)")
        elif rec:
            details.append(f"FAIL analyst={rec}")
        else:
            score += 0.5
            details.append("SKIP analyst=N/A")
    elif not details:
        # No fundamentals available, no exception — partial credit.
        score += 1.5
        details.append("fundamentals unavailable — partial pass")

    # 3. Not overbought (RSI < 70)
    rsi = indicators.get("rsi")
    if rsi is not None:
        if rsi < 70:
            score += 1
            details.append(f"RSI={rsi:.0f} (OK)")
        else:
            details.append(f"FAIL RSI={rsi:.0f} (overbought)")
    else:
        score += 0.5
        details.append("SKIP RSI=N/A")

    # 5. Smart money signal (3+ models recently bought this stock)
    try:
        from engine.smart_money import get_recent_smart_money
        sm = get_recent_smart_money(limit=20)
        if sm:
            for s in sm:
                if s.get("symbol") == symbol:
                    score += 1
                    details.append(f"smart_money: {s.get('buyers', 'yes')}")
                    break
            else:
                details.append("FAIL smart_money=none")
        else:
            details.append("SKIP smart_money=N/A")
    except Exception:
        details.append("SKIP smart_money=error")

    passes = int(score) >= 3
    return passes, int(score), details
