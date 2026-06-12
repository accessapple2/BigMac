"""HM-GUARDIAN-ADOPTION (2026-06-12) — exit-only stop sweep for guardian-of-forever.

The fleet's normal stop-check runs inside `_run_player`, which the scan loop only
invokes for `halt_mode='active'` players (ai_brain.py:288). guardian-of-forever is
`exit_only` (so it NEVER buys / is never scanned) — therefore it would never be
stop-checked by that loop. This dedicated sweep gives it coverage:

  load guardian positions → flat-12% stop + standard tiered TP (RiskManager
  defaults; guardian is NOT in AI_SIGNAL_PLAYERS so there is no conviction scaling)
  → execute exits via sell()/sell_partial() (guardian routes to Alpaca Paper, so
  the exit CLOSES the real broker position) → feed the stuck_stop_guard watchdog
  → NTFY ollietrades-admin a BATCHED summary of every sell in the sweep.

Scheduled every 10 min from main.py (same cadence as run_chekov_stoploss).
Never raises into the scheduler.
"""
from __future__ import annotations
from rich.console import Console

console = Console()

GUARDIAN_ID = "guardian-of-forever"


def run_guardian_sweep() -> dict:
    """Run one guardian stop/TP sweep. Returns {checked, sold:[...]} summary."""
    summary = {"checked": 0, "sold": []}
    try:
        from engine.paper_trader import get_portfolio, sell, sell_partial
        from engine.market_data import get_bulk_prices
        from engine.risk_manager import RiskManager

        port = get_portfolio(GUARDIAN_ID)
        positions = (port or {}).get("positions", []) if port else []
        if not positions:
            return summary
        summary["checked"] = len(positions)

        symbols = [p["symbol"] for p in positions]
        prices = get_bulk_prices(symbols) or {}
        entry_by_sym = {p["symbol"]: p.get("avg_price") for p in positions}

        risk = RiskManager()  # defaults: flat 12% stop, standard tiered TP
        actions = risk.check_stop_loss_take_profit(GUARDIAN_ID, positions, prices)

        for action in actions:
            sym = action["symbol"]
            pdata = prices.get(sym) or {}
            px = pdata.get("price")
            if px is None:
                console.log(f"[yellow][guardian] no price for {sym} — skipping {action.get('action')}")
                continue
            if action["action"] == "SELL_PARTIAL":
                res = sell_partial(
                    GUARDIAN_ID, sym, px, qty=action["qty"],
                    asset_type=action.get("asset_type", "stock"),
                    reasoning=action["reason"], option_type=action.get("option_type"),
                )
            else:
                res = sell(
                    GUARDIAN_ID, sym, px,
                    asset_type=action.get("asset_type", "stock"),
                    reasoning=action["reason"], option_type=action.get("option_type"),
                )
            console.log(f"[cyan][guardian] {action['reason']} on {sym} -> "
                        f"{'FILLED' if res else 'NO-FILL'}")

            # stuck-stop watchdog coverage (only for stop-loss exits)
            if str(action.get("reason", "")).lower().startswith(("stop-loss", "options stop-loss")):
                try:
                    from engine.stuck_stop_guard import record_stop_outcome
                    record_stop_outcome(GUARDIAN_ID, sym, executed=bool(res), detail=action["reason"])
                except Exception as e:
                    console.log(f"[yellow][guardian] stuck-stop hook error: {type(e).__name__}: {e!r}")

            if res:
                entry = entry_by_sym.get(sym)
                qty = res.get("qty") if isinstance(res, dict) else action.get("qty")
                pnl_pct = ((px - entry) / entry * 100) if (entry and px) else None
                summary["sold"].append({
                    "symbol": sym, "qty": qty, "entry": entry, "exit": px,
                    "pnl_pct": pnl_pct, "reason": action["reason"],
                })

        if summary["sold"]:
            _ntfy_summary(summary["sold"])
        return summary
    except Exception as e:
        console.log(f"[red][guardian] sweep error: {type(e).__name__}: {e!r}")
        return summary


def _ntfy_summary(sold: list) -> None:
    """One batched NTFY to ollietrades-admin covering every sell in this sweep."""
    try:
        from engine.alert_channels import send_alert, AlertLevel
        lines = []
        for s in sold:
            pnl = f"{s['pnl_pct']:+.1f}%" if s.get("pnl_pct") is not None else "n/a"
            entry = f"${s['entry']:.2f}" if s.get("entry") is not None else "n/a"
            lines.append(f"{s['symbol']} {s['qty']}@{entry}->${s['exit']:.2f} ({pnl})")
        msg = (f"Guardian-of-forever closed {len(sold)} orphan position(s) on stop/TP "
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
