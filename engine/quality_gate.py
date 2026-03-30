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
    """
    if symbol in GATE_EXEMPT:
        return True, 5, ["ETF/Index exempt"]

    score = 0
    details = []
    indicators = indicators or {}

    # 1. Earnings beat (positive earnings growth = recent beat)
    try:
        from engine.stock_fundamentals import fetch_fundamentals
        fund = fetch_fundamentals(symbol)
        if fund:
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
            rec = fund.get("recommendation", "")
            if rec and rec.lower() in ("buy", "strongbuy", "strong_buy", "overweight"):
                score += 1
                details.append(f"analyst={rec}")
            elif rec:
                details.append(f"FAIL analyst={rec}")
            else:
                details.append("SKIP analyst=N/A")
        else:
            # No fundamentals available — give partial credit
            score += 1.5
            details.append("fundamentals unavailable — partial pass")
    except Exception as e:
        score += 1.5
        details.append(f"fundamentals error: {e}")

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
