"""HM-MCCOY-TARGET-RELATIVE 2026-07-02 -- backtest: old flat-tier system
(actual realized $ P&L, already in the DB) vs new target-relative system
(simulated against real daily OHLC price paths for the same positions).

Known conservative bias against the NEW system: if both T1 and T2 thresholds
are crossed within the same daily bar, this simulation only fires T1 that
day and defers T2 to the next bar (no intraday ordering available from daily
OHLC) -- likely understates the new system's capture on fast movers.

Run: venv/bin/python scripts/backtest_mccoy_target_relative.py
"""
import sqlite3
import re
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.market_data import get_bulk_daily_ohlcv
from engine.crew_scanner import _target_relative_tiers, MCCOY_GLOBAL_HARD_STOP_PCT

PATTERN = re.compile(r"AUTO-STOP:\s*-([\d.]+)%.*?AUTO-TARGET:\s*\+([\d.]+)%")

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "trader.db")


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        "SELECT id, symbol, price, qty, executed_at FROM trades "
        "WHERE player_id='ollama-plutus' AND action='BUY' AND asset_type='stock' "
        "ORDER BY executed_at"
    )
    buys = cur.fetchall()

    candidates = []
    for b in buys:
        cur.execute("SELECT reasoning FROM trades WHERE id=?", (b["id"],))
        reasoning = cur.fetchone()["reasoning"] or ""
        m = PATTERN.search(reasoning)
        if not m:
            continue
        target_pct = float(m.group(2)) / 100.0
        cur.execute(
            "SELECT qty, price, realized_pnl, executed_at FROM trades "
            "WHERE player_id='ollama-plutus' AND symbol=? AND action='SELL' AND asset_type='stock' "
            "AND executed_at > ? ORDER BY executed_at",
            (b["symbol"], b["executed_at"]),
        )
        sells = cur.fetchall()
        if not sells:
            continue  # still open, no resolution to backtest against
        total_sold_qty = sum(s["qty"] for s in sells)
        actual_pnl = sum((s["realized_pnl"] or 0) for s in sells)
        if total_sold_qty < b["qty"] * 0.95:
            continue  # not fully closed yet (allow small rounding slack)
        candidates.append({
            "symbol": b["symbol"],
            "entry_price": b["price"],
            "entry_qty": b["qty"],
            "buy_date": b["executed_at"][:10],
            "target_pct": target_pct,
            "actual_pnl": actual_pnl,
        })

    print(f"{len(candidates)} fully-closed McCoy stock positions with signal metadata\n")

    symbols = sorted(set(c["symbol"] for c in candidates))
    bars = get_bulk_daily_ohlcv(symbols, range_str="3mo")

    rows_out = []
    for c in candidates:
        df = bars.get(c["symbol"])
        if df is None or df.empty:
            print(f"  SKIP {c['symbol']}: no OHLC data")
            continue
        df = df[df.index >= c["buy_date"]]
        if df.empty:
            print(f"  SKIP {c['symbol']}: no bars on/after buy_date {c['buy_date']}")
            continue

        entry = c["entry_price"]
        qty = c["entry_qty"]
        stop_price = entry * (1 - MCCOY_GLOBAL_HARD_STOP_PCT)
        tiers = _target_relative_tiers(c["target_pct"], MCCOY_GLOBAL_HARD_STOP_PCT)
        t1_price = entry * (1 + tiers["t1_pct"])
        t2_price = entry * (1 + tiers["t2_pct"])

        remaining_qty = qty
        realized = 0.0
        t1_fired = t2_fired = trailing = closed = False
        trail_high = None
        exit_note = "still open at end of available data"

        for date, row in df.iterrows():
            lo, hi = float(row["Low"]), float(row["High"])
            if lo <= stop_price:
                realized += remaining_qty * (stop_price - entry)
                exit_note = f"hard stop @ ${stop_price:.2f} on {date.date()}"
                closed = True
                break
            if trailing:
                trail_high = max(trail_high, hi)
                trail_floor = max(entry, trail_high * (1 - 0.03))
                if lo <= trail_floor:
                    realized += remaining_qty * (trail_floor - entry)
                    exit_note = f"trail stop @ ${trail_floor:.2f} on {date.date()}"
                    closed = True
                    break
                continue
            if not t1_fired and hi >= t1_price:
                sell_qty = qty * (1 / 3)
                realized += sell_qty * (t1_price - entry)
                remaining_qty -= sell_qty
                t1_fired = True
                continue
            if t1_fired and not t2_fired and hi >= t2_price:
                sell_qty = remaining_qty * 0.5
                realized += sell_qty * (t2_price - entry)
                remaining_qty -= sell_qty
                t2_fired = True
                trailing = True
                trail_high = hi
                continue

        if not closed:
            last_close = float(df.iloc[-1]["Close"])
            realized += remaining_qty * (last_close - entry)

        rows_out.append({
            "symbol": c["symbol"], "buy_date": c["buy_date"],
            "old_pnl": c["actual_pnl"], "new_pnl": realized,
            "exit_note": exit_note, "resolved": closed,
        })

    print(f"{'symbol':<8}{'buy_date':<12}{'old_$':>10}{'new_$':>10}{'delta':>10}  exit")
    for r in rows_out:
        print(f"{r['symbol']:<8}{r['buy_date']:<12}{r['old_pnl']:>10.2f}{r['new_pnl']:>10.2f}"
              f"{r['new_pnl']-r['old_pnl']:>10.2f}  {r['exit_note']}")

    resolved = [r for r in rows_out if r["resolved"]]
    still_open = [r for r in rows_out if not r["resolved"]]
    for label, group in [("RESOLVED", resolved), ("STILL-OPEN (mark-to-market)", still_open)]:
        o = sum(r["old_pnl"] for r in group)
        n = sum(r["new_pnl"] for r in group)
        print(f"\n{label} ({len(group)} positions): old=${o:.2f} new=${n:.2f} delta=${n-o:.2f}")
    o_all = sum(r["old_pnl"] for r in rows_out)
    n_all = sum(r["new_pnl"] for r in rows_out)
    print(f"\nTOTAL ({len(rows_out)} positions): old=${o_all:.2f} new=${n_all:.2f} delta=${n_all-o_all:.2f}")


if __name__ == "__main__":
    main()
