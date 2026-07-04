"""HM-GUARDIAN-ADOPTION (2026-06-12) — exit-only stop sweep.

The fleet's normal stop-check runs inside `_run_player`, which the scan loop only
invokes for `halt_mode='active'` players (ai_brain.py:288). Any `exit_only`
player holding a position is therefore never stop-checked by that loop.

HM-AGENT-RULES-CONSOLIDATION 2026-07-04 (AGENT-RULES-REVIEW-2026-07-03.md
Inconsistency #20): originally hardcoded to guardian-of-forever only. An audit
found 4 more exit_only seats silently holding 15 uncovered positions between
them (ollama-qwen3 x1, navigator x5 options, ollie-auto x8, gemini-2.5-flash
x1 short) — guardian-of-forever's own 8 were the only ones actually covered.
Generalized to sweep every exit_only agent with an open position, queried
fresh each run, so newly-benched seats get coverage automatically instead of
requiring a code change each time.

Per agent: load positions → flat-12% stop + standard tiered TP (RiskManager
defaults; conviction-scaled stops only apply to AI_SIGNAL_PLAYERS) → execute
exits via sell()/sell_partial() (routes to Alpaca Paper — the exit CLOSES the
real broker position) → feed the stuck_stop_guard watchdog → NTFY
ollietrades-admin a batched summary of every sell in the sweep.

Scheduled every 10 min from main.py (same cadence as run_chekov_stoploss).
Never raises into the scheduler.
"""
from __future__ import annotations
import sqlite3
from rich.console import Console

console = Console()

GUARDIAN_ID = "guardian-of-forever"  # kept for reference; no longer used to filter
DB_PATH = "data/trader.db"


def _exit_only_ids_with_positions() -> list[str]:
    """Every halt_mode='exit_only' player currently holding a non-zero position."""
    db = sqlite3.connect(DB_PATH, timeout=5)
    try:
        rows = db.execute("""
            SELECT DISTINCT p.player_id
            FROM positions p
            JOIN ai_players a ON a.id = p.player_id
            WHERE a.halt_mode = 'exit_only' AND p.qty != 0
        """).fetchall()
        return [r[0] for r in rows]
    finally:
        db.close()


def run_guardian_sweep() -> dict:
    """Run one stop/TP sweep across every exit_only agent holding open positions.
    Returns {checked, sold:[...], agents_swept:[...]} summary."""
    summary = {"checked": 0, "sold": [], "agents_swept": []}
    try:
        player_ids = _exit_only_ids_with_positions()
    except Exception as e:
        console.log(f"[red][guardian] roster query failed: {type(e).__name__}: {e!r}")
        return summary

    for player_id in player_ids:
        try:
            one = _sweep_agent(player_id)
        except Exception as e:
            console.log(f"[red][guardian] sweep error for {player_id}: {type(e).__name__}: {e!r}")
            continue
        summary["checked"] += one["checked"]
        summary["sold"].extend(one["sold"])
        if one["checked"]:
            summary["agents_swept"].append(player_id)

    if summary["sold"]:
        _ntfy_summary(summary["sold"])
    return summary


def _sweep_agent(player_id: str) -> dict:
    """Run one stop/TP sweep for a single exit_only agent."""
    summary = {"checked": 0, "sold": []}
    from engine.paper_trader import get_portfolio, sell, sell_partial
    from engine.market_data import get_bulk_prices
    from engine.risk_manager import RiskManager

    port = get_portfolio(player_id)
    positions = (port or {}).get("positions", []) if port else []
    if not positions:
        return summary
    summary["checked"] = len(positions)

    symbols = [p["symbol"] for p in positions]
    prices = get_bulk_prices(symbols) or {}
    entry_by_sym = {p["symbol"]: p.get("avg_price") for p in positions}

    risk = RiskManager()  # defaults: flat 12% stop, standard tiered TP
    actions = risk.check_stop_loss_take_profit(player_id, positions, prices)

    for action in actions:
        sym = action["symbol"]
        pdata = prices.get(sym) or {}
        px = pdata.get("price")
        if px is None:
            console.log(f"[yellow][guardian] no price for {sym} — skipping {action.get('action')} ({player_id})")
            continue
        if action["action"] == "SELL_PARTIAL":
            res = sell_partial(
                player_id, sym, px, qty=action["qty"],
                asset_type=action.get("asset_type", "stock"),
                reasoning=action["reason"], option_type=action.get("option_type"),
            )
        else:
            res = sell(
                player_id, sym, px,
                asset_type=action.get("asset_type", "stock"),
                reasoning=action["reason"], option_type=action.get("option_type"),
            )
        console.log(f"[cyan][guardian] {player_id}: {action['reason']} on {sym} -> "
                    f"{'FILLED' if res else 'NO-FILL'}")

        # stuck-stop watchdog coverage (only for stop-loss exits)
        if str(action.get("reason", "")).lower().startswith(("stop-loss", "options stop-loss")):
            try:
                from engine.stuck_stop_guard import record_stop_outcome
                record_stop_outcome(player_id, sym, executed=bool(res), detail=action["reason"])
            except Exception as e:
                console.log(f"[yellow][guardian] stuck-stop hook error: {type(e).__name__}: {e!r}")

        if res:
            entry = entry_by_sym.get(sym)
            # full sell() returns a dict without "qty"; fall back to the
            # action qty (pos qty for a stop) so the NTFY always has a qty.
            qty = (res.get("qty") if isinstance(res, dict) else None) or action.get("qty")
            pnl_pct = ((px - entry) / entry * 100) if (entry and px) else None
            summary["sold"].append({
                "player_id": player_id, "symbol": sym, "qty": qty, "entry": entry,
                "exit": px, "pnl_pct": pnl_pct, "reason": action["reason"],
            })

    return summary


def _ntfy_summary(sold: list) -> None:
    """One batched NTFY to ollietrades-admin covering every sell in this sweep."""
    try:
        from engine.alert_channels import send_alert, AlertLevel
        lines = []
        for s in sold:
            pnl = f"{s['pnl_pct']:+.1f}%" if s.get("pnl_pct") is not None else "n/a"
            entry = f"${s['entry']:.2f}" if s.get("entry") is not None else "n/a"
            lines.append(f"[{s['player_id']}] {s['symbol']} {s['qty']}@{entry}->${s['exit']:.2f} ({pnl})")
        msg = (f"Guardian sweep closed {len(sold)} exit-only orphan position(s) on stop/TP "
               f"(routed to Alpaca Paper):\n" + "\n".join(lines))
        send_alert(
            message=msg,
            level=AlertLevel.WARNING,
            alert_type="guardian_sweep_sells",
            title=f"🛡️ Guardian closed {len(sold)} orphan position(s)",
            audience="admin",
            rate_limit_secs=60,   # batch is already per-sweep; short window guards re-fire
        )
        console.log(f"[green][guardian] NTFY summary sent ({len(sold)} sells)")
    except Exception as e:
        console.log(f"[yellow][guardian] NTFY failed: {type(e).__name__}: {e!r}")
