"""
Emergency close-all for strategy:bull_spread_v1 paper positions.

Usage:
  cd /Users/bigmac/autonomous-trader
  venv/bin/python3 scripts/kill_bull_spread.py [--dry-run] [--confirm]

--dry-run: list what WOULD be closed, don't actually close
--confirm: required flag to actually close positions (safety belt)

Closes every open position whose strategy_id='bull_spread_v1' by calling
close_options_position (long legs) or submit_single_option side=buy (short
legs) — the same BTC/STC routing _close_live uses.

After closing each leg successfully, marks options_trades row as
exec_status='killed' and sets exit_reason='kill_script'.

Admiral's first-line abort is the Alpaca paper UI. This is the backup when
UI isn't available or when something wedges up the strategy scheduler.
"""
from __future__ import annotations
import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "trader.db"


def list_open_positions():
    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.execute("""
            SELECT id, symbol, structure, contracts, legs_json,
                   contracts_closed_so_far, entry_date, exit_tag
            FROM options_trades
            WHERE strategy_id = 'bull_spread_v1'
              AND exec_status = 'open'
        """)
        return cur.fetchall()
    finally:
        conn.close()


def occ_symbol(underlying: str, leg: dict) -> str:
    """Match the format used in strategies/executor.py::_occ_symbol"""
    from datetime import date
    exp = date.fromisoformat(leg["expiration"])
    yy = exp.strftime("%y")
    mm = exp.strftime("%m")
    dd = exp.strftime("%d")
    cp = "C" if leg["option_type"] == "call" else "P"
    strike_int = int(round(leg["strike"] * 1000))
    return f"{underlying}{yy}{mm}{dd}{cp}{strike_int:08d}"


def close_one_leg(underlying: str, leg: dict, qty: int, player_id: str):
    """Close a single leg with correct BTC/STC direction."""
    try:
        from engine.alpaca_options import close_options_position, submit_single_option
    except ImportError as e:
        return {"error": f"import failed: {e}"}

    occ = occ_symbol(underlying, leg)
    try:
        if leg["action"] == "buy":
            # We own this leg — SELL to close
            return close_options_position(
                player_id=player_id, contract_symbol=occ, qty=qty,
            )
        else:
            # We sold this leg — BUY to close
            return submit_single_option(
                player_id=player_id, contract_symbol=occ, qty=qty, side="buy",
            )
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


def mark_killed(position_id: int):
    conn = sqlite3.connect(str(DB_PATH))
    try:
        conn.execute("""
            UPDATE options_trades
            SET exec_status = 'killed',
                exit_date = CURRENT_TIMESTAMP,
                exit_reason = COALESCE(exit_reason || ' | ', '') || 'kill_script'
            WHERE id = ?
        """, (position_id,))
        conn.commit()
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Emergency close-all for bull_spread_v1")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be closed without closing")
    parser.add_argument("--confirm", action="store_true",
                        help="REQUIRED to actually close positions (safety)")
    args = parser.parse_args()

    positions = list_open_positions()

    if not positions:
        print("No open bull_spread_v1 positions. Nothing to kill.")
        return 0

    print(f"Found {len(positions)} open bull_spread_v1 positions:")
    print("-" * 70)
    for p in positions:
        pid, sym, struct, ct, legs_json, closed_so_far, entry, tag = p
        remaining = ct - (closed_so_far or 0)
        print(f"  id={pid} {sym} {struct} {remaining}/{ct} open "
              f"entry={entry} tag={tag}")
    print("-" * 70)

    if args.dry_run:
        print("\n--dry-run set. No close orders submitted.")
        return 0

    if not args.confirm:
        print("\nUse --confirm to actually close these positions.")
        print("For safety this flag is required — running without it is no-op.")
        return 1

    print("\nCLOSING POSITIONS...")
    player_id = "strategy:bull_spread_v1"
    all_ok = True
    for p in positions:
        pid, sym, struct, ct, legs_json, closed_so_far, entry, tag = p
        remaining = ct - (closed_so_far or 0)
        try:
            legs = json.loads(legs_json)
        except Exception as e:
            print(f"  id={pid}: FAIL to parse legs_json: {e}")
            all_ok = False
            continue

        leg_results = []
        for leg in legs:
            r = close_one_leg(sym, leg, remaining, player_id)
            leg_results.append({"leg": leg, "result": r})

        errors = [lr for lr in leg_results if isinstance(lr["result"], dict) and lr["result"].get("error")]
        if errors:
            print(f"  id={pid}: PARTIAL FAIL — {len(errors)} of {len(leg_results)} legs errored")
            for e in errors:
                print(f"    leg {e['leg']['option_type']}@{e['leg']['strike']}: {e['result']['error']}")
            all_ok = False
        else:
            mark_killed(pid)
            print(f"  id={pid}: KILLED ({remaining} contracts, {len(leg_results)} legs)")

    print("-" * 70)
    if all_ok:
        print("All positions killed successfully.")
        return 0
    else:
        print("SOME CLOSES FAILED. Check Alpaca UI manually.")
        return 2


if __name__ == "__main__":
    sys.exit(main())
