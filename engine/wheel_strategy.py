"""Counselor Troi's Wheel Strategy — sell puts on high-IV ETFs for premium income.

3/5/30 Rule (adapted from Matt Giannino):
- 3: Focus on 3-5 liquid leveraged ETFs (TQQQ, SOXL, UPRO, TNA, UVXY)
- 5: Target 5% return on capital per trade
- 30: Sell 30-day options (theta sweet spot)

The Wheel:
1. Sell cash-secured put → collect premium
2. If assigned → own shares at discount → sell covered call
3. If called away → keep premium + capital gain → restart wheel
4. High VIX = fat premiums = BEST time to sell options
"""
import logging
from datetime import datetime, timedelta
import pytz
from engine.paper_trader import buy, sell, get_portfolio
from engine.market_data import get_stock_price
from engine.fear_greed import get_fear_greed_index
from rich.console import Console

console = Console()
logger = logging.getLogger("wheel_strategy")

PLAYER_ID = "options-sosnoff"  # Counselor Troi
WHEEL_TICKERS = ["TQQQ", "SOXL", "UPRO", "TNA", "QQQ", "SPY"]
TARGET_RETURN = 0.05   # 5% per trade
DTE_TARGET = 30        # 30-day options (theta sweet spot)
MAX_POSITIONS = 3      # Max 3 concurrent wheel positions
POSITION_SIZE_PCT = 0.25  # 25% of portfolio per wheel position
MIN_VIX = 18           # Don't sell when VIX too low (thin premiums)
MIN_PREMIUM_RETURN = 3.0  # Skip if estimated return < 3%

_done_today = False
_last_date = None


def _is_market_hours() -> bool:
    az = pytz.timezone("US/Arizona")
    now = datetime.now(az)
    if now.weekday() >= 5:
        return False
    mins = now.hour * 60 + now.minute
    return 400 <= mins <= 780  # 6:40 AM–1:00 PM AZ (9:30–4 PM ET)


def run_wheel_scan():
    """Scan for wheel opportunities — sell puts on high-IV leveraged ETFs."""
    global _done_today, _last_date

    az = pytz.timezone("US/Arizona")
    today = datetime.now(az).strftime("%Y-%m-%d")
    if _last_date != today:
        _done_today = False
        _last_date = today

    if _done_today:
        return
    if not _is_market_hours():
        return

    try:
        portfolio = get_portfolio(PLAYER_ID)
        cash = portfolio.get("cash", 0)
        positions = portfolio.get("positions", [])

        # Count active wheel option positions
        wheel_puts = [
            p for p in positions
            if p["symbol"] in WHEEL_TICKERS and p.get("asset_type") == "option"
        ]
        if len(wheel_puts) >= MAX_POSITIONS:
            console.log("[dim]Wheel: Max put positions reached")
            _done_today = True
            return

        # Check VIX — high VIX = fat premiums (best selling environment)
        vix = 20.0  # default
        try:
            fg = get_fear_greed_index()
            vix_val = fg.get("signals", {}).get("vix", {}).get("value")
            if vix_val:
                vix = float(vix_val)
        except Exception:
            pass

        if vix < MIN_VIX:
            console.log(f"[dim]Wheel: VIX {vix:.1f} too low — premiums thin, skipping")
            _done_today = True
            return

        total_value = cash + sum(
            p["qty"] * p.get("avg_price", 0) for p in positions
        )
        budget_per_position = total_value * POSITION_SIZE_PCT

        held_symbols = {p["symbol"] for p in positions}

        for ticker in WHEEL_TICKERS:
            if len(wheel_puts) >= MAX_POSITIONS:
                break
            if ticker in held_symbols:
                continue  # already have a position on this name

            price_data = get_stock_price(ticker)
            price = price_data.get("price", 0)
            if price <= 0:
                continue

            # Strike: 10-15% OTM — want to collect premium, NOT get assigned
            otm_pct = 0.12
            put_strike = round(price * (1 - otm_pct), 2)

            # Premium estimate: VIX-scaled, capped at 8%
            # At VIX=30: ~6% of stock price; at VIX=20: ~4%
            premium_pct = min(0.08, vix / 500.0)
            estimated_premium = round(price * premium_pct, 2)

            # Shares secured by the cash
            shares = int(budget_per_position / put_strike)
            if shares <= 0:
                continue

            total_premium = estimated_premium * shares
            premium_return = (total_premium / (put_strike * shares)) * 100

            if premium_return < MIN_PREMIUM_RETURN:
                console.log(f"[dim]Wheel: {ticker} return {premium_return:.1f}% < {MIN_PREMIUM_RETURN}% minimum, skipping")
                continue

            expiry = (datetime.now() + timedelta(days=DTE_TARGET)).strftime("%Y-%m-%d")

            reasoning = (
                f"WHEEL STRATEGY: Selling {DTE_TARGET}-day cash-secured put on {ticker}. "
                f"Strike ${put_strike} ({otm_pct*100:.0f}% OTM from ${price:.2f}). "
                f"VIX {vix:.1f} = elevated premiums — prime selling conditions. "
                f"Estimated premium: ${estimated_premium:.2f}/share "
                f"(${total_premium:.0f} total, {premium_return:.1f}% return on capital). "
                f"If assigned, will own {ticker} at ${put_strike} discount and sell covered calls. "
                f"3/5/30 Rule: targeting 5% return on 30-day cycle. "
                f"Troi senses extreme anxiety in the market — the premium is rich with fear."
            )

            result = buy(
                player_id=PLAYER_ID,
                symbol=ticker,
                price=estimated_premium,
                qty=float(shares),
                reasoning=reasoning,
                confidence=0.80,
                asset_type="option",
                option_type="put",
                strike_price=put_strike,
                expiry_date=expiry,
                sources="wheel-strategy,troi",
                timeframe="SWING",
            )
            if result:
                wheel_puts.append({"symbol": ticker})
                console.log(
                    f"[bold green]🎡 Wheel: Sold {shares}x {ticker} ${put_strike}P "
                    f"@ ${estimated_premium:.2f} | {premium_return:.1f}% return | exp {expiry}"
                )

        _done_today = True

    except Exception as e:
        logger.error(f"Wheel strategy error: {e}")
        console.log(f"[red]Wheel error: {e}")


def check_wheel_assignments():
    """Check if sold puts are ITM/expired and handle assignment → covered call phase."""
    try:
        portfolio = get_portfolio(PLAYER_ID)
        positions = portfolio.get("positions", [])

        for pos in positions:
            if pos.get("asset_type") != "option" or pos.get("option_type") != "put":
                continue

            symbol = pos["symbol"]
            strike = pos.get("strike_price") or 0
            expiry = pos.get("expiry_date", "")
            if not strike or not expiry:
                continue

            price_data = get_stock_price(symbol)
            current_price = price_data.get("price", 0)
            if current_price <= 0:
                continue

            try:
                exp_date = datetime.strptime(expiry[:10], "%Y-%m-%d")
            except ValueError:
                continue

            if datetime.now() >= exp_date and current_price < strike:
                # ASSIGNED — stock assigned at strike price, transition to covered call phase
                console.log(
                    f"[yellow]🎡 Wheel: {symbol} put ASSIGNED at ${strike} "
                    f"(stock at ${current_price:.2f}, diff ${strike - current_price:.2f})"
                )
                # Remove the put position
                sell(
                    player_id=PLAYER_ID,
                    symbol=symbol,
                    price=pos["avg_price"],
                    asset_type="option",
                    reasoning=(
                        f"Wheel: Put expired ITM at ${strike}, assigned. "
                        f"Stock at ${current_price:.2f}. Transitioning to covered call phase."
                    ),
                    sources="wheel-assignment",
                )
                # Buy the stock at strike (assignment price = effective discount)
                buy(
                    player_id=PLAYER_ID,
                    symbol=symbol,
                    price=strike,
                    qty=pos["qty"],
                    reasoning=(
                        f"Wheel: Assigned on ${strike} put (stock ${current_price:.2f}, "
                        f"${strike - current_price:.2f} above market = built-in cushion from premium). "
                        f"Phase 2: Now selling covered calls to generate further income."
                    ),
                    confidence=0.85,
                    asset_type="stock",
                    sources="wheel-assignment",
                    timeframe="SWING",
                )

    except Exception as e:
        logger.error(f"Wheel assignment check error: {e}")
        console.log(f"[red]Wheel assignment error: {e}")


def get_wheel_status() -> dict:
    """Return wheel status summary for dashboard display."""
    try:
        portfolio = get_portfolio(PLAYER_ID)
        positions = portfolio.get("positions", [])
        puts = [p for p in positions if p.get("asset_type") == "option" and p.get("option_type") == "put"]
        stocks = [p for p in positions if p.get("asset_type") == "stock" and p["symbol"] in WHEEL_TICKERS]
        total_premium = sum(p["qty"] * p.get("avg_price", 0) for p in puts)
        return {
            "puts_open": len(puts),
            "stocks_held": len(stocks),
            "total_premium_collected": round(total_premium, 2),
            "positions": puts + stocks,
        }
    except Exception:
        return {"puts_open": 0, "stocks_held": 0, "total_premium_collected": 0, "positions": []}
