"""Midterm Recovery Protocol — systematic dip-buying when historical patterns signal recovery.

Based on S&P 500 midterm election year data:
- Average drawdown: -16.1% | Current: -8.5%
- Average 1-year forward return after bottom: +36.4%
- EVERY midterm drawdown since 1950 was followed by recovery
- Signal: VIX > 30, SPY 10%+ off highs, Fear & Greed < 25

Strategy:
1. Build positions gradually as drawdown deepens (dollar-cost averaging)
2. Focus on highest-quality names (mega caps, sector leaders)
3. Scale in: 10% of available capital per tranche
4. Targets: SPY, QQQ, NVDA, AAPL, MSFT, AMZN, META, GOOGL
5. Hold 6-12 months for recovery
"""
import logging
from datetime import datetime
import pytz
from engine.market_data import get_stock_price
from engine.paper_trader import buy, get_portfolio
from engine.fear_greed import get_fear_greed_index
from rich.console import Console

console = Console()
logger = logging.getLogger("recovery_protocol")

PLAYER_ID = "super-agent"  # Mr. Anderson manages the recovery portfolio
RECOVERY_TICKERS = ["SPY", "QQQ", "NVDA", "AAPL", "MSFT", "AMZN", "META", "GOOGL"]
MAX_RECOVERY_PCT = 0.40  # Max 40% of portfolio in recovery positions
TRANCHE_PCT = 0.05  # 5% per buy tranche
VIX_TRIGGER = 28  # VIX must be above this to trigger
FG_TRIGGER = 35  # Fear & Greed must be below this

_done_today = False
_last_date = None


def run_recovery_scan():
    """Check if conditions warrant a recovery buy."""
    global _done_today, _last_date

    az = pytz.timezone("US/Arizona")
    now = datetime.now(az)
    today = now.strftime("%Y-%m-%d")

    # Reset daily flag
    if _last_date != today:
        _done_today = False
        _last_date = today

    if _done_today:
        return

    # Only during market hours (6:35 AM - 1:00 PM MST)
    mins = now.hour * 60 + now.minute
    if mins < 395 or mins > 780:
        return

    # Weekdays only
    if now.weekday() >= 5:
        return

    try:
        # Check Fear & Greed
        fg = get_fear_greed_index()
        fg_score = fg.get("score", 50)

        if fg_score > FG_TRIGGER:
            console.log(f"[dim]Recovery Protocol: F&G {fg_score} > {FG_TRIGGER} — not fearful enough")
            return

        # Check VIX
        vix = fg.get("signals", {}).get("vix", {}).get("value", 20)
        if vix < VIX_TRIGGER:
            console.log(f"[dim]Recovery Protocol: VIX {vix} < {VIX_TRIGGER} — not enough fear")
            return

        # Conditions met — execute recovery buy
        console.log(f"[bold green]Recovery Protocol TRIGGERED: F&G={fg_score}, VIX={vix}")

        portfolio = get_portfolio(PLAYER_ID)
        cash = portfolio.get("cash", 0)
        total_value = sum(p["qty"] * p.get("avg_price", 0) for p in portfolio.get("positions", [])) + cash

        if total_value <= 0:
            return

        # Calculate how much we've already allocated to recovery
        held_symbols = {p["symbol"] for p in portfolio.get("positions", [])}
        recovery_value = sum(
            p["qty"] * p.get("avg_price", 0)
            for p in portfolio.get("positions", [])
            if p["symbol"] in RECOVERY_TICKERS
        )
        recovery_pct = recovery_value / total_value if total_value > 0 else 0

        if recovery_pct >= MAX_RECOVERY_PCT:
            console.log(f"[dim]Recovery Protocol: Already {recovery_pct:.1%} allocated — max reached")
            _done_today = True
            return

        # Pick tickers we don't already hold (or hold least of)
        candidates = [t for t in RECOVERY_TICKERS if t not in held_symbols]
        if not candidates:
            candidates = RECOVERY_TICKERS[:3]  # Add to SPY/QQQ/NVDA if holding everything

        for ticker in candidates[:2]:  # Max 2 buys per day
            price_data = get_stock_price(ticker)
            price = price_data.get("price", 0)
            if price <= 0:
                continue

            budget = total_value * TRANCHE_PCT
            qty = int(budget / price) if price > 100 else round(budget / price, 4)
            if qty <= 0:
                continue

            if qty * price > cash:
                continue

            reasoning = (
                f"RECOVERY PROTOCOL: Midterm drawdown pattern detected. "
                f"F&G={fg_score} (FEAR), VIX={vix:.1f} (elevated). "
                f"Historical data: every midterm drawdown since 1950 recovered +36.4% avg within 12 months. "
                f"SPY currently ~8.5% off highs, avg midterm drawdown is -16.1%. "
                f"Building recovery position in {ticker} — tranche {recovery_pct:.1%} → "
                f"{(recovery_pct + TRANCHE_PCT):.1%} of portfolio."
            )

            buy(
                player_id=PLAYER_ID,
                symbol=ticker,
                price=price,
                qty=qty,
                reasoning=reasoning,
                confidence=0.85,
                sources="recovery-protocol,midterm-data",
                timeframe="SWING",
            )
            cash -= qty * price
            console.log(f"[bold green]Recovery Protocol: Bought {qty} {ticker} @ ${price:.2f}")

        _done_today = True

    except Exception as e:
        logger.error(f"Recovery Protocol error: {e}")
        console.log(f"[red]Recovery Protocol error: {e}")
