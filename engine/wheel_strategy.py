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
from engine.paper_trader import get_portfolio
# HM-W1F4 2026-05-17: canonical options-trade helper for sell-put accounting.
# Replaces paper_trader.buy(asset_type="option") which was unconditional BUY
# accounting (debit cash + long qty) — wrong for sell-to-open semantics.
# HM-W1F5 2026-05-17: close_options_trade now actively used in
# check_wheel_assignments (was zero-callers per audit G4 before this commit).
from engine.options_exec import open_options_trade, close_options_trade
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

            # HM-W1F4 2026-05-17: route through options_exec.open_options_trade(structure="csp")
            # instead of paper_trader.buy(). buy() was unconditional BUY accounting (debit cash +
            # long qty) which silently miscoded every wheel sell-put. options_exec is the canonical
            # paper-options helper used by bull_put_spread_v1 — handles credit/debit cleanly,
            # writes options_trades with entry_credit_debit > 0 for short premium, credits
            # options_books.fleet.current_cash. Decouples from ai_players.cash per Admiral
            # approval (matches bull_put_spread_v1 convention).
            # Bridge Voter gate at paper_trader.py:621 is now bypassed because we no longer
            # call buy() for the sell-put entry path.
            contracts = max(1, int(shares / 100))  # 100 underlying shares per options contract
            trade_id = open_options_trade(
                book_tag="fleet",
                agent_id=PLAYER_ID,
                structure="csp",
                symbol=ticker,
                expiration=expiry,
                legs=[
                    {"side": "short", "type": "put", "strike": put_strike,
                     "qty": contracts, "entry_price": estimated_premium},
                ],
                regime=None,  # TODO: pull from get_latest_briefing() if needed
                vix=vix,
                notes=reasoning[:500],
            )
            if trade_id:
                wheel_puts.append({"symbol": ticker})
                console.log(
                    f"[bold green]🎡 Wheel: Sold {contracts}x {ticker} ${put_strike}P "
                    f"@ ${estimated_premium:.2f} (contracts × 100 shares = {contracts*100}) | "
                    f"{premium_return:.1f}% return | exp {expiry} | trade_id={trade_id}"
                )

        _done_today = True

    except Exception as e:
        logger.error(f"Wheel strategy error: {e}")
        console.log(f"[red]Wheel error: {e}")


def check_wheel_assignments():
    """Check open wheel CSPs at expiry — close OTM as expired_worthless.

    HM-W1F5 2026-05-17: rewritten to iterate options_trades (canonical
    source-of-truth post-HM-W1F4) instead of the positions table.
    Wheel CSPs opened via options_exec.open_options_trade never write
    to positions; the old check_wheel_assignments was a structural
    no-op for all post-W1F4 wheel positions.

    Scope: OTM-at-expiry close only. ITM/assigned branch deferred to
    HM-WHEEL-ASSIGNMENT-LEDGER epic per Admiral disposition Option C
    (G5). ITM-at-expiry positions trigger NTFY + skip; Admiral closes
    manually until cash-source decision lands.
    """
    import json
    import sqlite3

    try:
        conn = sqlite3.connect("data/trader.db")
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, symbol, expiration, legs_json, entry_credit_debit "
            "FROM options_trades "
            "WHERE agent_id = ? AND status = 'open' AND structure = 'csp'",
            (PLAYER_ID,),
        ).fetchall()
        conn.close()

        if not rows:
            return

        today = datetime.now().date()
        for row in rows:
            trade_id = row["id"]
            symbol = row["symbol"]
            try:
                exp_date = datetime.strptime(row["expiration"][:10], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                console.log(f"[red]Wheel: bad expiration on trade {trade_id} ({row['expiration']}) — skipping")
                continue

            if today < exp_date:
                continue  # not yet at expiry

            # At-or-past-expiry: compute intrinsic value of the short put
            try:
                legs = json.loads(row["legs_json"])
            except (json.JSONDecodeError, TypeError):
                console.log(f"[red]Wheel: bad legs_json on trade {trade_id} — skipping")
                continue
            short_puts = [l for l in legs if l.get("side") == "short" and l.get("type") == "put"]
            if not short_puts:
                console.log(f"[red]Wheel: trade {trade_id} has no short put leg — skipping")
                continue
            put_leg = short_puts[0]
            strike = float(put_leg.get("strike", 0))
            qty = int(put_leg.get("qty", 0))
            if strike <= 0 or qty <= 0:
                continue

            price_data = get_stock_price(symbol)
            current_price = price_data.get("price", 0)
            if current_price <= 0:
                console.log(f"[red]Wheel: no price for {symbol} on trade {trade_id} — skipping")
                continue

            intrinsic = max(0.0, round(strike - current_price, 2))

            if intrinsic > 0:
                # ITM at expiry — Fix #5 scope is OTM-only per G5 Option C disposition.
                # NTFY Admiral and skip; manual close required until
                # HM-WHEEL-ASSIGNMENT-LEDGER ships.
                console.log(
                    f"[red]🎡 Wheel: {symbol} put ITM at expiry — strike ${strike}, "
                    f"spot ${current_price:.2f}, intrinsic ${intrinsic:.2f}. "
                    f"trade_id={trade_id} — MANUAL ADMIRAL CLOSE REQUIRED "
                    f"(Fix #5 OTM-only scope)."
                )
                try:
                    from engine.alert_channels import send_alert, AlertLevel
                    send_alert(
                        message=(f"Wheel ITM at expiry: {symbol} strike ${strike}, "
                                 f"spot ${current_price:.2f}, trade_id={trade_id} — "
                                 f"manual close needed"),
                        level=AlertLevel.WARNING,
                        alert_type=f"hm-w1f5-wheel-itm-expiry-{symbol}",
                        rate_limit_secs=86400,
                    )
                except Exception:
                    pass
                continue

            # OTM at expiry — close at $0 intrinsic via canonical helper
            pnl = close_options_trade(
                trade_id=trade_id,
                exit_legs=[{
                    "side": "short", "type": "put",
                    "strike": strike, "qty": qty,
                    "exit_price": 0.0,
                }],
                exit_reason="expired_otm",
            )
            if pnl is not None:
                console.log(
                    f"[bold green]🎡 Wheel: {symbol} put expired OTM — closed trade_id={trade_id} "
                    f"strike ${strike}, spot ${current_price:.2f}, pnl=${pnl:.2f}"
                )
            else:
                console.log(
                    f"[red]Wheel: close_options_trade returned None for trade_id={trade_id} "
                    f"({symbol}) — may already be closed or row missing"
                )

    except Exception as e:
        logger.error(f"Wheel assignment check error: {type(e).__name__}: {e!r}")
        console.log(f"[red]Wheel assignment error: {type(e).__name__}: {e!r}")


def get_wheel_status() -> dict:
    """Return wheel status summary for dashboard display.

    HM-W1F5 2026-05-17: read options_trades (canonical post-HM-W1F4) instead
    of the positions table. Previous implementation reported 0 wheel positions
    despite live opens in options_trades. Stock-side post-assignment count
    stays at 0 until HM-WHEEL-ASSIGNMENT-LEDGER ships per Admiral disposition.
    """
    import json
    import sqlite3
    try:
        conn = sqlite3.connect("data/trader.db")
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, symbol, legs_json, entry_credit_debit, expiration "
            "FROM options_trades WHERE agent_id = ? AND status = 'open' AND structure = 'csp'",
            (PLAYER_ID,),
        ).fetchall()
        conn.close()

        positions = []
        total_premium = 0.0
        for r in rows:
            total_premium += float(r["entry_credit_debit"] or 0.0)
            try:
                legs = json.loads(r["legs_json"])
                put_leg = next((l for l in legs if l.get("side") == "short" and l.get("type") == "put"), None)
            except (json.JSONDecodeError, TypeError):
                put_leg = None
            positions.append({
                "trade_id": r["id"],
                "symbol": r["symbol"],
                "asset_type": "option",
                "option_type": "put",
                "strike_price": put_leg.get("strike") if put_leg else None,
                "qty": put_leg.get("qty") if put_leg else None,
                "avg_price": put_leg.get("entry_price") if put_leg else None,
                "entry_credit": float(r["entry_credit_debit"]),
                "expiry_date": r["expiration"],
            })

        return {
            "puts_open": len(positions),
            "stocks_held": 0,
            "total_premium_collected": round(total_premium, 2),
            "positions": positions,
        }
    except Exception:
        return {"puts_open": 0, "stocks_held": 0, "total_premium_collected": 0, "positions": []}
