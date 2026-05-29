#!/usr/bin/env python3
"""HM-TRADES-PRICE-WRITEBACK backfill (WAVE 1.1).

Corrects pre-2026-05-21 routed-player trade rows still marked
execution_type='simulated' by matching them to filled Alpaca paper orders
(symbol + side + qty + time proximity) and writing back the broker fill:
  - BUY  : entry_price = filled_avg_price
  - SELL : exit_price  = filled_avg_price, realized_pnl recomputed from the
           row's entry_price (residual: SELL entry stays internal-derived;
           a future FIFO pass could refine it)
  - both : alpaca_order_id, alpaca_status='filled', execution_type='alpaca_paper'

Dry-run by default (prints match rate + PnL-delta preview, NO writes).
Pass --apply to UPDATE (requires an existing data/trader.db.backup-* file).
"""
import sys, glob, sqlite3, datetime
from collections import defaultdict, Counter
from pathlib import Path

ROOT = Path.home() / "autonomous-trader"
DB = ROOT / "data" / "trader.db"
ROUTED = ("super-agent", "ollie-auto", "neo-matrix")
FIX_CUTOFF = "2026-05-21"
QTY_TOL = 0.02      # relative qty tolerance (fractional-share rounding)
TIME_WIN_S = 3600   # ± window between trade.executed_at and order fill/submit


def fetch_alpaca_filled():
    sys.path.insert(0, str(ROOT))
    from engine.alpaca_bridge import AlpacaBridge
    from alpaca.trading.requests import GetOrdersRequest
    from alpaca.trading.enums import QueryOrderStatus
    b = AlpacaBridge()
    if not b.client:
        print("NO ALPACA CLIENT — backfill infeasible"); sys.exit(2)
    seen, out, until = set(), [], None
    for _ in range(80):
        batch = list(b.client.get_orders(
            GetOrdersRequest(status=QueryOrderStatus.ALL, limit=500,
                             until=until, direction="desc")))
        new = [o for o in batch if str(o.id) not in seen]
        if not new:
            break
        for o in new:
            seen.add(str(o.id))
            if o.filled_avg_price is None or o.side is None or o.symbol is None:
                continue  # skip unfilled + multi-leg/options parent orders (no top-level side)
            side = o.side.value if hasattr(o.side, "value") else str(o.side)
            out.append({
                "id": str(o.id), "symbol": o.symbol, "side": side,
                "qty": float(o.qty) if o.qty else (float(o.filled_qty) if o.filled_qty else 0.0),
                "fill": float(o.filled_avg_price),
                "t": o.filled_at or o.submitted_at,
            })
        until = min(o.submitted_at for o in batch)
        if len(batch) < 500:
            break
    return out


def parse_dt(s):
    s = s.replace("T", " ").split(".")[0]
    return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=datetime.timezone.utc)


def main(apply_writes=False):
    orders = fetch_alpaca_filled()
    by_key = defaultdict(list)
    for o in orders:
        by_key[(o["symbol"], o["side"])].append(o)

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, player_id, symbol, action, qty, entry_price, exit_price, "
        "realized_pnl, executed_at FROM trades "
        "WHERE executed_at < ? AND execution_type='simulated' "
        "AND player_id IN (?,?,?) ORDER BY executed_at",
        (FIX_CUTOFF, *ROUTED)).fetchall()

    used = set()
    updates = []   # (id, player, action, fill, order_id, old_pnl, new_pnl)
    unmatched = []
    for r in rows:
        side = "buy" if r["action"] == "BUY" else "sell"
        tdt = parse_dt(r["executed_at"])
        q = r["qty"] or 0.0
        best, bestdt = None, None
        for o in by_key.get((r["symbol"], side), []):
            if o["id"] in used:
                continue
            if q > 0 and o["qty"] > 0 and abs(o["qty"] - q) / max(q, o["qty"]) > QTY_TOL:
                continue
            dts = abs((o["t"] - tdt).total_seconds())
            if dts > TIME_WIN_S:
                continue
            if best is None or dts < bestdt:
                best, bestdt = o, dts
        if best is None:
            unmatched.append(r); continue
        used.add(best["id"])
        new_pnl = None
        if r["action"] == "SELL" and r["entry_price"] is not None:
            new_pnl = (best["fill"] - float(r["entry_price"])) * q
        updates.append((r["id"], r["player_id"], r["action"], best["fill"],
                        best["id"], r["realized_pnl"], new_pnl))

    total, matched = len(rows), len(updates)
    print("=== DRY-RUN MATCH REPORT ===")
    print(f"pre-fix routed simulated trades : {total}")
    print(f"matched to Alpaca filled order  : {matched} ({100*matched//max(total,1)}%)")
    print(f"unmatched                       : {len(unmatched)}")
    mp, up = Counter(), Counter()
    for u in updates: mp[u[1]] += 1
    for r in unmatched: up[r["player_id"]] += 1
    print("per-player  matched / unmatched:")
    for p in ROUTED:
        print(f"  {p:<12} {mp[p]:>4} / {up[p]}")
    # PnL-delta preview (SELLs only)
    print("PnL correction preview (SELL realized_pnl, old -> new):")
    pnl_old, pnl_new = Counter(), Counter()
    for (_id, pl, act, fill, oid, old, new) in updates:
        if act == "SELL" and new is not None:
            pnl_old[pl] += float(old or 0.0)
            pnl_new[pl] += new
    for p in ROUTED:
        if pnl_old[p] or pnl_new[p]:
            print(f"  {p:<12} {pnl_old[p]:+.2f} -> {pnl_new[p]:+.2f}  (Δ {pnl_new[p]-pnl_old[p]:+.2f})")

    if apply_writes:
        if not glob.glob(str(ROOT / "data" / "trader.db.backup-*")):
            print("NO BACKUP FOUND — ABORTING WRITE"); sys.exit(3)
        n = 0
        for (tid, pl, act, fill, oid, old, new) in updates:
            if act == "BUY":
                conn.execute(
                    "UPDATE trades SET entry_price=?, alpaca_order_id=?, "
                    "alpaca_status='filled', execution_type='alpaca_paper' WHERE id=?",
                    (fill, oid, tid))
            elif new is not None:
                conn.execute(
                    "UPDATE trades SET exit_price=?, realized_pnl=?, alpaca_order_id=?, "
                    "alpaca_status='filled', execution_type='alpaca_paper' WHERE id=?",
                    (fill, new, oid, tid))
            else:
                conn.execute(
                    "UPDATE trades SET exit_price=?, alpaca_order_id=?, "
                    "alpaca_status='filled', execution_type='alpaca_paper' WHERE id=?",
                    (fill, oid, tid))
            n += 1
        conn.commit()
        print(f"APPLIED {n} updates.")
    conn.close()


if __name__ == "__main__":
    main(apply_writes="--apply" in sys.argv)
