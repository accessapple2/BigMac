"""Portfolio Autopilot — RSI-based profit-taking, auto-rebalance overweight positions, maintain cash floor."""
from __future__ import annotations
import sqlite3
from datetime import datetime
from rich.console import Console

from engine.halt_gate import HALTED_EMIT_FILTER

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


def _collect_all_positions(pid: str) -> list:
    """HM-AUTOPILOT-OPTION-CLOSE 2026-05-18: dual-table position view.

    Returns positions[] from BOTH the legacy positions table AND the
    canonical options_trades table for this player_id. Each entry
    carries a 'source_table' discriminator for downstream dispatch
    (sell() vs close_options_trade()).

    Today, only the cash-restore site (L329 caller) uses this helper —
    audit verdict was "PROTECTIVE TODAY" because cash/total ≈ 1.0 for
    options-sosnoff and the trigger never fires. Migration is doctrinal
    cleanup per Fix #5 G8 inventory + bull_spread_v1 canonical ship
    (commit 161253d) which armed re-eval trigger #3.
    """
    import json as _json
    from engine.paper_trader import get_portfolio

    portfolio = get_portfolio(pid)
    legacy = list(portfolio.get("positions") or [])
    for p in legacy:
        p["source_table"] = "positions"

    canonical: list = []
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT id, structure, symbol, expiration, legs_json, "
            "       entry_credit_debit, contracts "
            "FROM options_trades WHERE agent_id=? AND status='open'",
            (pid,),
        ).fetchall()
    finally:
        conn.close()

    for r in rows:
        try:
            legs = _json.loads(r["legs_json"])
        except (TypeError, _json.JSONDecodeError):
            continue
        # First leg drives the conviction-lookup symbol + representative
        # asset_type. Multi-leg structures (bull_put_spread etc.) still
        # surface as one row keyed off the underlying.
        canonical.append({
            "source_table": "options_trades",
            "trade_id":     r["id"],
            "structure":    r["structure"],
            "symbol":       r["symbol"],
            "asset_type":   "option",
            "expiration":   r["expiration"],
            "contracts":    r["contracts"],
            "legs":         legs,
            "entry_credit_debit": r["entry_credit_debit"],
            # autopilot's cash-restore uses qty × price to estimate value;
            # for canonical rows use the entry credit as a proxy.
            "qty":          1,
            "avg_price":    abs(float(r["entry_credit_debit"] or 0)) / 100.0,
        })
    return legacy + canonical


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
        # HM-AK-β 2026-05-07: halt_mode filter — skip halted_full and exit_only rows
        # HM-AK-γ 2026-05-07: drop redundant dayblade-0dte exclusion (halt_mode='full' covers it)
        "SELECT id, display_name FROM ai_players WHERE is_active=1 AND halt_mode='active'"
    ).fetchall()
    conn.close()

    for player in players:
        pid = player["id"]

        # GUARD: Never trade human portfolios (Steve's Webull benchmark)
        if "steve" in pid.lower() or "webull" in pid.lower():
            continue
        # HM-Y (2026-05-05): use is_auto_tradeable helper — composes humans
        # AND passive broker mirrors (alpaca-mirror, etc.). Was a bare
        # is_human DB read; replaced to gate alpaca-mirror at the source
        # rather than relying on the 24h min-hold downstream guard
        # (autopilot profit-take attempted SELL on alpaca-mirror's WMB at
        # 08:07:04 post-Item3-restart; min-hold caught it, but HM-Y catches
        # it earlier).
        from engine.halt_gate import is_auto_tradeable
        conn2 = _conn()
        try:
            if not is_auto_tradeable(pid, conn2):
                continue
        finally:
            conn2.close()

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
                current_price = prices.get(sym, {}).get("price", 0)
                if current_price <= 0:
                    continue  # no price data — skip RSI trim (was: phantom $0 P&L exits)

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
        # HM-AUTOPILOT-OPTION-CLOSE 2026-05-18: dual-table scan via
        # _collect_all_positions — was positions-table-only (blind to the
        # 4 canonical options_trades open rows). source_table key on each
        # entry routes the close path: 'positions' → paper_trader.sell(),
        # 'options_trades' → engine.options_exec.close_options_trade().
        portfolio = get_portfolio(pid)  # Refresh after trims (cash side)
        cash = portfolio["cash"]
        all_positions = _collect_all_positions(pid)
        total_value = cash + sum(
            (p.get("qty") or 0) * prices.get(p.get("symbol"), {}).get("price", p.get("avg_price") or 0)
            for p in all_positions
        )
        if total_value > 0 and cash / total_value < MIN_CASH_PCT and all_positions:
            # Find lowest-conviction position (most recent signal with lowest confidence)
            conn2 = _conn()
            lowest = None
            lowest_conf = 2.0
            for pos in all_positions:
                # HM-C: filter halted-player emissions from scorecard/calibration math
                row = conn2.execute(
                    f"SELECT confidence FROM signals WHERE player_id=? AND symbol=? "
                    f"AND {HALTED_EMIT_FILTER} "
                    f"ORDER BY created_at DESC LIMIT 1",
                    (pid, pos["symbol"])
                ).fetchone()
                conf = row["confidence"] if row and row["confidence"] else 0.5
                if conf < lowest_conf:
                    lowest_conf = conf
                    lowest = pos
            conn2.close()

            if lowest:
                sym = lowest["symbol"]
                current_price = prices.get(sym, {}).get("price", lowest.get("avg_price") or 0)
                if lowest.get("source_table") == "options_trades":
                    # Canonical close path — exit at current intrinsic for
                    # the put side (CSP) or zero-cost for OTM at the moment.
                    # Conservative: close at current_price as exit_price per
                    # leg (this is the spot-price proxy; for true premium
                    # exit, future commits should use the polygon helper
                    # HM-POLYGON-OPTIONS-CHAIN-QUOTE-HELPER mid quote).
                    from engine.options_exec import close_options_trade
                    legs = lowest.get("legs") or []
                    exit_legs = [
                        dict(leg, exit_price=float(current_price or 0))
                        for leg in legs
                    ]
                    pnl = close_options_trade(
                        lowest["trade_id"], exit_legs,
                        exit_reason=f"autopilot_cash_restore_conv_{lowest_conf:.2f}",
                    )
                    if pnl is not None:
                        console.log(
                            f"[yellow]AUTOPILOT: Closed {pid} {sym} canonical "
                            f"trade_id={lowest['trade_id']} structure={lowest.get('structure')} "
                            f"pnl=${pnl:.2f} (cash-restore conv={lowest_conf:.0%})"
                        )
                else:
                    from engine.paper_trader import sell
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
