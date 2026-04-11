"""Portfolio Autopilot — RSI-based profit-taking, auto-rebalance overweight positions, maintain cash floor."""
from __future__ import annotations
import sqlite3
from datetime import datetime
from rich.console import Console

console = Console()
DB = "data/trader.db"

MAX_POSITION_PCT = 0.25
TRIM_TARGET_PCT = 0.20
MIN_CASH_PCT = 0.15


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def is_autopilot_enabled() -> bool:
    """Check if autopilot is enabled (stored in DB settings)."""
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT value FROM settings WHERE key='autopilot_enabled'"
        ).fetchone()
        conn.close()
        return bool(row and row["value"] == "1")
    except Exception:
        conn.close()
        return False


def set_autopilot(enabled: bool):
    """Toggle autopilot on/off."""
    conn = _conn()
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('autopilot_enabled', ?)",
        ("1" if enabled else "0",)
    )
    conn.commit()
    conn.close()


def run_autopilot(prices: dict):
    """Run autopilot rebalancing for all active players."""
    if not is_autopilot_enabled():
        return

    from engine.risk_manager import RiskManager
    if not RiskManager.is_market_hours():
        return

    from engine.paper_trader import get_portfolio, sell, sell_partial
    from engine.telegram_alerts import send_alert

    conn = _conn()
    players = conn.execute(
        "SELECT id, display_name FROM ai_players WHERE is_active=1 AND id != 'dayblade-0dte'"
    ).fetchall()
    conn.close()

    for player in players:
        pid = player["id"]

        # GUARD: Never trade human portfolios (Steve's Webull benchmark)
        if "steve" in pid.lower() or "webull" in pid.lower():
            continue
        # Also check is_human flag in DB
        conn2 = _conn()
        is_human = conn2.execute("SELECT is_human FROM ai_players WHERE id=?", (pid,)).fetchone()
        conn2.close()
        if is_human and is_human["is_human"]:
            continue

        portfolio = get_portfolio(pid)
        positions = portfolio["positions"]
        cash = portfolio["cash"]

        if not positions:
            continue

        # Calculate total portfolio value
        total_value = cash + sum(
            p["qty"] * prices.get(p["symbol"], {}).get("price", p["avg_price"])
            for p in positions
        )
        if total_value <= 0:
            continue

        # 0a. Dust cleanup: close any position worth less than $10
        for pos in list(positions):
            if pos.get("asset_type") == "option":
                continue
            sym = pos["symbol"]
            current_price = prices.get(sym, {}).get("price", pos["avg_price"])
            pos_value = pos["qty"] * current_price
            if 0 < pos_value < 10:
                try:
                    result = sell(
                        pid, sym, current_price,
                        asset_type="stock",
                        reasoning=f"Autopilot dust cleanup: position value ${pos_value:.2f} < $10 threshold",
                    )
                    if result:
                        console.log(
                            f"[yellow]DUST CLEANUP: {pid} {sym} — closed {pos['qty']} shares (${pos_value:.2f}) to free cash"
                        )
                except Exception:
                    pass

        # Refresh after dust cleanup
        portfolio = get_portfolio(pid)
        positions = portfolio["positions"]
        cash = portfolio["cash"]

        # 0b. RSI-based profit-taking: trim overbought positions
        try:
            from engine.market_data import get_technical_indicators
            for pos in positions:
                if pos.get("asset_type") == "option":
                    continue
                sym = pos["symbol"]
                current_price = prices.get(sym, {}).get("price", pos["avg_price"])

                # Skip RSI trim if position value < $50 (don't create dust)
                pos_value = pos["qty"] * current_price
                if pos_value < 50:
                    continue

                try:
                    ind = get_technical_indicators(sym)
                    rsi = ind.get("rsi") if ind else None
                except Exception:
                    rsi = None
                if rsi is None:
                    continue

                if rsi > 80:
                    trim_frac = 0.75
                    trim_qty = round(pos["qty"] * trim_frac, 4)
                    # After trim, remaining value must be >= $50 or close entire position
                    remaining_value = (pos["qty"] - trim_qty) * current_price
                    if remaining_value < 50 and pos_value >= 50:
                        # Close entire position instead of leaving dust
                        result = sell(
                            pid, sym, current_price,
                            asset_type="stock",
                            reasoning=f"Autopilot RSI trim: RSI {rsi:.0f} > 80, closing full position (remaining would be dust)",
                        )
                    elif trim_qty > 0.001 and pos["qty"] > trim_qty:
                        result = sell_partial(
                            pid, sym, current_price, qty=trim_qty,
                            asset_type="stock",
                            reasoning=f"Autopilot RSI trim: RSI {rsi:.0f} > 80 (EXTREME OVERBOUGHT), trimming 75%",
                        )
                    else:
                        result = None
                    if result:
                        console.log(
                            f"[red]RSI TRIM: {pid} {sym} RSI={rsi:.0f} — sold @ ${current_price:.2f}"
                        )
                        send_alert(
                            f"<b>RSI TRIM (>80)</b>\n"
                            f"{player['display_name']}: {sym} RSI={rsi:.0f}, trimmed @ ${current_price:.2f}"
                        )
                elif rsi > 70:
                    trim_frac = 0.50
                    trim_qty = round(pos["qty"] * trim_frac, 4)
                    remaining_value = (pos["qty"] - trim_qty) * current_price
                    if remaining_value < 50 and pos_value >= 50:
                        result = sell(
                            pid, sym, current_price,
                            asset_type="stock",
                            reasoning=f"Autopilot RSI trim: RSI {rsi:.0f} > 70, closing full position (remaining would be dust)",
                        )
                    elif trim_qty > 0.001 and pos["qty"] > trim_qty:
                        result = sell_partial(
                            pid, sym, current_price, qty=trim_qty,
                            asset_type="stock",
                            reasoning=f"Autopilot RSI trim: RSI {rsi:.0f} > 70 (OVERBOUGHT), trimming 50%",
                        )
                    else:
                        result = None
                    if result:
                        console.log(
                            f"[yellow]RSI TRIM: {pid} {sym} RSI={rsi:.0f} — sold @ ${current_price:.2f}"
                        )
                        send_alert(
                            f"<b>RSI TRIM (>70)</b>\n"
                            f"{player['display_name']}: {sym} RSI={rsi:.0f}, trimmed @ ${current_price:.2f}"
                        )
        except Exception as e:
            console.log(f"[red]RSI trim error for {pid}: {e}")

        # Refresh portfolio after RSI trims
        portfolio = get_portfolio(pid)
        positions = portfolio["positions"]
        cash = portfolio["cash"]

        # 1. Profit-taking: sell into strength at tiered levels
        from config import TAKE_PROFIT_TIERS
        for pos in positions:
            if pos.get("asset_type") == "option":
                continue  # Options have their own SL/TP in risk_manager
            sym = pos["symbol"]
            current_price = prices.get(sym, {}).get("price", pos["avg_price"])
            avg_price = pos["avg_price"]
            if avg_price <= 0:
                continue
            gain_pct = (current_price - avg_price) / avg_price

            for tier_pct, sell_frac in TAKE_PROFIT_TIERS:
                if gain_pct >= tier_pct:
                    # Cooldown: skip if this tier already fired for this symbol in the last 24h
                    _conn2 = _conn()
                    _recent = _conn2.execute(
                        "SELECT COUNT(*) FROM trades WHERE player_id=? AND symbol=? "
                        "AND action='SELL' AND reasoning LIKE ? "
                        "AND executed_at >= datetime('now', '-24 hours')",
                        (pid, sym, f"%hit +{tier_pct:.0%} tier%"),
                    ).fetchone()[0]
                    _conn2.close()
                    if _recent > 0:
                        break  # Already took profits at this tier today
                    sell_qty = round(pos["qty"] * sell_frac, 4)
                    if sell_qty > 0.001 and pos["qty"] > sell_qty:
                        result = sell_partial(
                            pid, sym, current_price, qty=sell_qty,
                            asset_type="stock",
                            reasoning=f"Autopilot profit-take: +{gain_pct:.0%} hit +{tier_pct:.0%} tier, selling {sell_frac:.0%}",
                        )
                        if result:
                            console.log(
                                f"[green]PROFIT-TAKE: {pid} {sym} +{gain_pct:.0%} — sold {sell_qty} @ ${current_price:.2f}"
                            )
                            send_alert(
                                f"<b>PROFIT-TAKE</b>\n"
                                f"{player['display_name']}: {sym} +{gain_pct:.0%}, sold {sell_qty} @ ${current_price:.2f}"
                            )
                        break  # Only trigger highest applicable tier per cycle

        # 2. Trim positions exceeding 25%
        portfolio = get_portfolio(pid)  # Refresh after profit-takes
        positions = portfolio["positions"]
        cash = portfolio["cash"]
        total_value = cash + sum(
            p["qty"] * prices.get(p["symbol"], {}).get("price", p["avg_price"])
            for p in positions
        )
        if total_value <= 0:
            continue

        for pos in positions:
            sym = pos["symbol"]
            current_price = prices.get(sym, {}).get("price", pos["avg_price"])
            position_value = pos["qty"] * current_price
            position_pct = position_value / total_value

            if position_pct > MAX_POSITION_PCT:
                target_value = total_value * TRIM_TARGET_PCT
                excess_value = position_value - target_value
                trim_qty = round(excess_value / current_price, 4)

                if trim_qty > 0:
                    result = sell_partial(
                        pid, sym, current_price, qty=trim_qty,
                        asset_type=pos.get("asset_type", "stock"),
                        reasoning=f"Autopilot trim: {position_pct:.0%} → {TRIM_TARGET_PCT:.0%}",
                        option_type=pos.get("option_type"),
                    )
                    if result:
                        console.log(
                            f"[yellow]AUTOPILOT: Trimmed {pid} {sym} "
                            f"from {position_pct:.0%} to ~{TRIM_TARGET_PCT:.0%}"
                        )
                        send_alert(
                            f"<b>AUTOPILOT TRIM</b>\n"
                            f"{player['display_name']}: Trimmed {sym} from {position_pct:.0%} to ~{TRIM_TARGET_PCT:.0%}\n"
                            f"Sold {trim_qty} shares @ ${current_price:.2f}"
                        )

        # 2. If cash < 15%, sell lowest-conviction position
        portfolio = get_portfolio(pid)  # Refresh after trims
        cash = portfolio["cash"]
        total_value = cash + sum(
            p["qty"] * prices.get(p["symbol"], {}).get("price", p["avg_price"])
            for p in portfolio["positions"]
        )
        if total_value > 0 and cash / total_value < MIN_CASH_PCT and portfolio["positions"]:
            # Find lowest-conviction position (most recent signal with lowest confidence)
            conn2 = _conn()
            lowest = None
            lowest_conf = 2.0
            for pos in portfolio["positions"]:
                row = conn2.execute(
                    "SELECT confidence FROM signals WHERE player_id=? AND symbol=? "
                    "ORDER BY created_at DESC LIMIT 1",
                    (pid, pos["symbol"])
                ).fetchone()
                conf = row["confidence"] if row and row["confidence"] else 0.5
                if conf < lowest_conf:
                    lowest_conf = conf
                    lowest = pos
            conn2.close()

            if lowest:
                from engine.paper_trader import sell
                sym = lowest["symbol"]
                current_price = prices.get(sym, {}).get("price", lowest["avg_price"])
                result = sell(
                    pid, sym, current_price,
                    asset_type=lowest.get("asset_type", "stock"),
                    reasoning=f"Autopilot: cash below {MIN_CASH_PCT:.0%}, selling lowest conviction ({lowest_conf:.0%})",
                    option_type=lowest.get("option_type"),
                )
                if result:
                    console.log(
                        f"[yellow]AUTOPILOT: Sold {pid} {sym} (lowest conviction) to restore cash reserve"
                    )
