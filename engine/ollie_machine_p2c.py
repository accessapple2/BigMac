"""Ollie Machine — Step 7 P2c: brackets + re-enter ledger + exit-monitor (SIM-only).

Run BY HAND. Builds on P2b (the tradeable-universe filter re-wrote `ollie_machine_picks`
with a clean top-3 = GSAT / BB / BRUN). Here we:
  1. Generate brackets for the new top-3 via the EXISTING /api/trade-levels endpoint
     (entry/stop/tp; the ~1.6x ATR stops stand now that survivors have real ATR).
     Write them onto the picks rows.
  2. Clear + re-enter the ledger — flat the stale P2a rows (CNTA/SILA/GSAT), enter the
     new top-3 at their brackets (2% notional, 5-concurrent cap, -2% daily breaker).
  3. Build + run the exit-monitor — checks open ledger positions against current price,
     closes on stop or tp hit (writes realized_pnl, closed_at, exit_price, exit_reason),
     which arms the -2% daily breaker for subsequent re-enters.

SIM-only throughout: can_trade_live=0 + portfolio.execution_mode='tracking' + not in any
broker-routing / scan roster list. No executor call, no scheduling, no restart. The
`ollie_machine_ledger` table is SIM-private — the running trader has no reader.

Exit-monitor fill convention (point-in-time snapshot check):
  • stop hit (price <= stop)  → exit at CURRENT price  (market-on-breach, pessimistic)
  • tp hit   (price >= tp1)   → exit at tp1            (limit fill)
  realized_pnl = (exit_price - entry) * qty   (long-only)
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from engine import market_data
from engine import ollie_machine_p2a as p2a

TOP3 = ("GSAT", "BB", "BRUN")   # confirmed-clean momentum names (P2b top-3)


def ensure_exit_columns(conn: sqlite3.Connection) -> None:
    """Add exit_price / exit_reason if absent (realized_pnl + closed_at already exist)."""
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(ollie_machine_ledger)").fetchall()}
    if "exit_price" not in cols:
        conn.execute("ALTER TABLE ollie_machine_ledger ADD COLUMN exit_price REAL")
    if "exit_reason" not in cols:
        conn.execute("ALTER TABLE ollie_machine_ledger ADD COLUMN exit_reason TEXT")
    conn.commit()


def generate_top3_brackets(conn: sqlite3.Connection) -> list[dict]:
    """Fetch trade-levels for the top-3 picks, write brackets onto their picks rows,
    return the bracketed list in the shape p2a.sim_enter expects."""
    rows = conn.execute(
        "SELECT id, symbol, convergence_count, conviction_rank, signals_fired, rs_rank, convergence_type "
        "FROM ollie_machine_picks WHERE conviction_rank <= 3 ORDER BY conviction_rank"
    ).fetchall()
    out = []
    for p in rows:
        lv = p2a.fetch_levels(p["symbol"])
        if lv:
            conn.execute(
                "UPDATE ollie_machine_picks SET entry_price=?, stop=?, tp1=?, tp2=?, tp3=? WHERE id=?",
                (lv["entry"], lv["stop"], lv["tp1"], lv["tp2"], lv["tp3"], p["id"]),
            )
        out.append({**dict(p), "levels": lv})
    conn.commit()
    return out


# ─────────────────────────── exit-monitor ────────────────────────────────────
def _current_price(symbol: str) -> float | None:
    d = market_data.get_stock_price(symbol)
    if isinstance(d, dict):
        px = d.get("price")
        return float(px) if px else None
    return float(d) if d else None


def exit_monitor(conn: sqlite3.Connection) -> dict:
    """Check open ledger positions vs current price; close on stop/tp hit. Long-only."""
    ts = datetime.now(timezone.utc).isoformat()
    rows = conn.execute(
        "SELECT * FROM ollie_machine_ledger WHERE player_id=? AND status='open'", (p2a.PLAYER_ID,)
    ).fetchall()
    closed, still_open, no_price = [], [], []
    for r in rows:
        sym, entry, qty, stop, tp1 = r["symbol"], r["entry_price"], r["qty"], r["stop"], r["tp1"]
        px = _current_price(sym)
        if px is None:
            no_price.append(sym)
            still_open.append({"symbol": sym, "price": None})
            continue
        exit_price = exit_reason = None
        if stop is not None and px <= stop:
            exit_price, exit_reason = px, "stop"          # market-on-breach (pessimistic)
        elif tp1 is not None and px >= tp1:
            exit_price, exit_reason = tp1, "tp1"          # limit fill
        if exit_reason:
            realized = round((exit_price - entry) * qty, 2)
            conn.execute(
                "UPDATE ollie_machine_ledger SET status='closed', exit_price=?, exit_reason=?, "
                "realized_pnl=?, closed_at=? WHERE id=?",
                (exit_price, exit_reason, realized, ts, r["id"]),
            )
            closed.append({"symbol": sym, "exit_price": exit_price, "reason": exit_reason,
                           "realized_pnl": realized, "price": px})
        else:
            still_open.append({"symbol": sym, "price": px, "entry": entry, "stop": stop, "tp1": tp1})
    conn.commit()
    return {"ts": ts, "closed": closed, "still_open": still_open, "no_price": no_price}


def run() -> dict:
    conn = p2a._conn()
    try:
        p2a.ensure_ledger_table(conn)
        ensure_exit_columns(conn)
        reg = p2a.register_player(conn)                       # idempotent
        bracketed = generate_top3_brackets(conn)
        entry = p2a.sim_enter(conn, bracketed, reg["portfolio_id"], source="p2c-sim")
        monitor = exit_monitor(conn)
        ledger = [dict(r) for r in conn.execute(
            "SELECT * FROM ollie_machine_ledger WHERE player_id=? ORDER BY status DESC, pick_rank",
            (p2a.PLAYER_ID,)).fetchall()]
        return {"reg": reg, "bracketed": bracketed, "entry": entry, "monitor": monitor, "ledger": ledger}
    finally:
        conn.close()


if __name__ == "__main__":
    r = run()
    bracketed, e, mon, ledger = r["bracketed"], r["entry"], r["monitor"], r["ledger"]

    print("\n=== Ollie Machine P2c — brackets + re-enter + exit-monitor (SIM/tracking) ===")

    print("\n--- brackets for new top-3 (via /api/trade-levels, written to picks) ---")
    print(f"  {'#':>2} {'SYM':6} {'entry':>9} {'stop':>9} {'tp1':>9} {'tp2':>9} {'tp3':>9} {'rr':>5} {'sl%':>6}")
    for p in bracketed:
        lv = p.get("levels")
        if not lv:
            print(f"  {p['conviction_rank']:>2} {p['symbol']:6}  (no levels — endpoint unavailable)"); continue
        rr = f"{lv['rr']:.2f}" if lv.get("rr") is not None else "  —"
        slp = f"{lv['sl_pct']:.2f}" if lv.get("sl_pct") is not None else "   —"
        print(f"  {p['conviction_rank']:>2} {p['symbol']:6} {lv['entry']:>9.2f} {lv['stop']:>9.2f} "
              f"{(lv['tp1'] or 0):>9.2f} {(lv['tp2'] or 0):>9.2f} {(lv['tp3'] or 0):>9.2f} {rr:>5} {slp:>6}")

    print(f"\n--- re-enter ledger (2% notional=${e['notional_per']:.0f}/trade, 5-cap, "
          f"-2% breaker={'TRIPPED' if e['breaker_tripped'] else 'ok'} "
          f"[today realized ${e['today_realized']:.2f}]) ---")
    print("  cleared stale P2a rows (CNTA/SILA/GSAT) → entered new top-3:")
    for o in e["opened"]:
        print(f"    {o['symbol']:6} qty={o['qty']:>4} @ {o['entry']:.2f}  notional=${o['notional']:.2f}  "
              f"stop={o['stop']:.2f} tp1={o['tp1']}  risk=${o['risk_amount']:.2f} (rr={o['rr']})")
    for s in e["skipped"]:
        print(f"    SKIP {s['symbol']:6} — {s['reason']}")

    print(f"\n--- exit-monitor run @ {mon['ts']} ---")
    print(f"  closed: {len(mon['closed'])}   still-open: {len(mon['still_open'])}   no-price: {len(mon['no_price'])}")
    for c in mon["closed"]:
        print(f"    CLOSE {c['symbol']:6} {c['reason']:5} @ {c['exit_price']:.2f} "
              f"(mark {c['price']:.2f})  realized=${c['realized_pnl']:.2f}")
    if not mon["closed"]:
        print("    (0 closes — positions just entered; stop/tp logic + P&L math exercised below)")

    print("\n--- ledger ---")
    print(f"  {'SYM':6} {'st':6} {'qty':>4} {'entry':>9} {'stop':>9} {'tp1':>9} {'mark':>9} {'distÆ’':>8}")
    for L in ledger:
        mk = next((x["price"] for x in mon["still_open"] + mon["closed"] if x["symbol"] == L["symbol"]), None)
        markp = f"{mk:.2f}" if mk else "   —"
        # distance to nearer trigger as a sanity readout
        dist = ""
        if mk and L["stop"]:
            to_stop = (mk - L["stop"]) / mk * 100
            to_tp = ((L["tp1"] - mk) / mk * 100) if L["tp1"] else None
            dist = f"S-{to_stop:.1f}%" + (f"/T+{to_tp:.1f}%" if to_tp is not None else "")
        rp = f"  realized=${L['realized_pnl']:.2f}" if L["status"] == "closed" else ""
        print(f"  {L['symbol']:6} {L['status']:6} {L['qty']:>4.0f} {L['entry_price']:>9.2f} "
              f"{(L['stop'] or 0):>9.2f} {(L['tp1'] or 0):>9.2f} {markp:>9} {dist:>8}{rp}")

    print("\n--- SIM-safety: can_trade_live=0 + execution_mode=tracking + not in routing/scan lists. "
          "ledger is SIM-private. No executor/scheduling/restart. ---")
