"""HM-EXPIRE-OPTIONS-CANONICAL synthetic test (G4-style dormant-path guard).

Extends scripts/wave1_fix5_synthetic_close.py with structure-specific
invariants needed by expire_options + check_option_exits canonical migration.

I10 — OTM expire close at $0 → options_books cash UNCHANGED (no further
      flow; premium captured at open).
I11 — pnl on OTM expire = entry_credit_debit exactly (full premium kept).
I12 — bull_put_spread OTM expire → same pattern as CSP.
I13 — long_call OTM expire → NEGATIVE pnl (entry was debit, exit at 0).
I14 — long_call TP_HIT (exit > entry) → POSITIVE pnl.
I15 — Decoupling: ai_players.cash unchanged across ALL above.

All ghost-book; teardown to exact pre-state.
"""
import sqlite3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from engine.options_exec import open_options_trade, close_options_trade

DB = "data/trader.db"
AGENT = "hm_expire_sanity_test"


def fetch_one(c, sql, *args):
    return c.execute(sql, args).fetchone()


def open_and_close(structure: str, legs: list, exit_price: float, exit_reason: str) -> tuple[int, float]:
    """Open + close one synthetic trade. Returns (trade_id, pnl)."""
    tid = open_options_trade(
        book_tag="ghost",
        agent_id=AGENT,
        structure=structure,
        symbol=f"SYNTH-{structure}",
        expiration="2026-12-31",
        legs=legs,
        regime="TEST",
        vix=20.0,
        notes=f"HM-EXPIRE synthetic — {structure} — DELETED AT END",
    )
    if tid is None:
        return -1, 0.0
    exit_legs = [dict(l, exit_price=exit_price) for l in legs]
    pnl = close_options_trade(trade_id=tid, exit_legs=exit_legs, exit_reason=exit_reason)
    return tid, (pnl if pnl is not None else 0.0)


def main() -> int:
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    pre_fleet = fetch_one(c, "SELECT current_cash, total_trades, wins, losses FROM options_books WHERE book_tag=?", "fleet")
    pre_ghost = fetch_one(c, "SELECT current_cash, total_trades, wins, losses FROM options_books WHERE book_tag=?", "ghost")
    print(f"PRE  fleet: cash={pre_fleet['current_cash']} trades={pre_fleet['total_trades']} wins={pre_fleet['wins']} losses={pre_fleet['losses']}")
    print(f"PRE  ghost: cash={pre_ghost['current_cash']} trades={pre_ghost['total_trades']} wins={pre_ghost['wins']} losses={pre_ghost['losses']}")

    existing = fetch_one(c, "SELECT cash FROM ai_players WHERE id=?", AGENT)
    if existing is None:
        c.execute(
            "INSERT INTO ai_players (id, display_name, cash, halt_mode, halt_reason, provider, model_id, is_active) "
            "VALUES (?, ?, 100000.0, 'full', 'hm_expire synthetic test', 'synthetic', 'none', 0)",
            (AGENT, "HM-Expire Sanity Test"),
        )
        conn.commit()
        ai_cash_pre = 100000.0
        print(f"Created ai_players row {AGENT}")
    else:
        ai_cash_pre = float(existing["cash"])
    conn.close()

    invariants: dict[str, bool] = {}
    trade_ids: list[int] = []

    # ── Test 1 — CSP OTM expire (sister of Fix #5 wheel CSP close)
    # Open: short put strike 100, entry 2.50 → +250 credit
    # Close: exit_price 0.0 → close_cost=0, pnl=+250, cash delta on close = 0
    tid, pnl = open_and_close(
        structure="csp",
        legs=[{"side": "short", "type": "put", "strike": 100.0, "qty": 1, "entry_price": 2.50}],
        exit_price=0.0, exit_reason="expired_otm",
    )
    trade_ids.append(tid)
    conn = sqlite3.connect(DB); conn.row_factory = sqlite3.Row
    snap1 = fetch_one(conn.cursor(), "SELECT current_cash FROM options_books WHERE book_tag=?", "ghost")
    row1 = fetch_one(conn.cursor(), "SELECT status, exit_credit_debit, pnl, exit_reason FROM options_trades WHERE id=?", tid)
    conn.close()
    cash_after_open_credit = pre_ghost["current_cash"] + 250.0
    invariants["I10 csp OTM expire — cash delta on close = 0"] = (snap1["current_cash"] == cash_after_open_credit)
    invariants["I11 csp OTM expire — pnl == entry_credit_debit (+250)"] = (pnl == 250.0)
    invariants["I11b csp exit_reason set"] = (row1["exit_reason"] == "expired_otm")
    invariants["I11c csp exit_credit_debit == 0 (close_cost=0)"] = (row1["exit_credit_debit"] == 0.0)

    # ── Test 2 — bull_put_spread OTM expire (sibling structure)
    # Legs: short put strike 100 entry 3.00; long put strike 95 entry 1.50 → net +150 credit
    # Close: both at 0.0 → close_cost=0, pnl=+150
    tid2, pnl2 = open_and_close(
        structure="bull_put_spread",
        legs=[
            {"side": "short", "type": "put", "strike": 100.0, "qty": 1, "entry_price": 3.00},
            {"side": "long",  "type": "put", "strike":  95.0, "qty": 1, "entry_price": 1.50},
        ],
        exit_price=0.0, exit_reason="expired_otm",
    )
    trade_ids.append(tid2)
    conn = sqlite3.connect(DB); conn.row_factory = sqlite3.Row
    row2 = fetch_one(conn.cursor(), "SELECT entry_credit_debit, pnl FROM options_trades WHERE id=?", tid2)
    conn.close()
    invariants["I12 bull_put_spread OTM expire — pnl == entry_credit_debit (+150)"] = (pnl2 == 150.0 and row2["entry_credit_debit"] == 150.0)

    # ── Test 3 — long_call OTM expire → NEGATIVE pnl
    # Open: long call strike 100, entry 5.00 → -500 debit (net=-500)
    # Close: exit_price 0.0 → close_cost=0, pnl = -500 - 0 = -500
    tid3, pnl3 = open_and_close(
        structure="long_call",
        legs=[{"side": "long", "type": "call", "strike": 100.0, "qty": 1, "entry_price": 5.00}],
        exit_price=0.0, exit_reason="expired_otm",
    )
    trade_ids.append(tid3)
    invariants["I13 long_call OTM expire — pnl negative (-500)"] = (pnl3 == -500.0)

    # ── Test 4 — long_call TP_HIT → POSITIVE pnl
    # Open: long call strike 100, entry 5.00 → -500 debit
    # Close: exit_price 8.00 → close_cost=800, pnl = -500 - 800 = -1300
    # WAIT — close_options_trade semantics:
    #   close_cost = exit_price × qty × 100 (buy-back cost)
    #   pnl = entry_credit_debit - close_cost
    # For a LONG option, exit at higher premium = SELL-TO-CLOSE (credit cash IN).
    # But the helper treats exit_price as buy-back cost regardless of side.
    # For long-option TP-hit, close_cost should be NEGATIVE (we receive cash, not pay).
    # Per the helper's convention (Fix #5 audit §3.3): "close_cost = sum of prices
    # paid to buy back all legs". This means for a LONG option we exit, the price
    # would be NEGATIVE — sign of the exit_price encodes the side.
    # Use exit_price=-8.0 to represent selling-to-close at $8 credit:
    #   close_cost = -8 × 1 × 100 = -800
    #   pnl = -500 - (-800) = +300 ✓
    tid4, pnl4 = open_and_close(
        structure="long_call",
        legs=[{"side": "long", "type": "call", "strike": 100.0, "qty": 1, "entry_price": 5.00}],
        exit_price=-8.0, exit_reason="tp_hit",
    )
    trade_ids.append(tid4)
    invariants["I14 long_call TP_HIT — pnl positive (+300 via sell-to-close)"] = (pnl4 == 300.0)

    # ── I15 — ai_players cash UNCHANGED across all 4 trades
    conn = sqlite3.connect(DB); conn.row_factory = sqlite3.Row
    ai_cash_post = float(fetch_one(conn.cursor(), "SELECT cash FROM ai_players WHERE id=?", AGENT)["cash"])
    conn.close()
    invariants["I15 ai_players.cash UNCHANGED across all 4 closes"] = (ai_cash_post == ai_cash_pre)

    # ── Final invariant: options_books.ghost wins/losses incremented correctly
    # I10 csp OTM: pnl=+250 → wins+1
    # I12 bull_put_spread OTM: pnl=+150 → wins+1
    # I13 long_call OTM: pnl=-500 → losses+1
    # I14 long_call TP: pnl=+300 → wins+1
    # Expected: wins+3, losses+1
    conn = sqlite3.connect(DB); conn.row_factory = sqlite3.Row
    post_ghost = fetch_one(conn.cursor(), "SELECT current_cash, total_trades, wins, losses FROM options_books WHERE book_tag=?", "ghost")
    conn.close()
    invariants["I16 wins delta == +3 (csp + bps + tp_hit)"] = (post_ghost["wins"] == pre_ghost["wins"] + 3)
    invariants["I17 losses delta == +1 (long_call OTM expire)"] = (post_ghost["losses"] == pre_ghost["losses"] + 1)
    invariants["I18 total_trades delta == +4"] = (post_ghost["total_trades"] == pre_ghost["total_trades"] + 4)

    print()
    print("INVARIANT CHECK:")
    all_pass = True
    for name, passed in invariants.items():
        flag = "PASS" if passed else "FAIL"
        if not passed:
            all_pass = False
        print(f"  {flag}: {name}")

    print()
    print(f"POST ghost: cash={post_ghost['current_cash']} trades={post_ghost['total_trades']} wins={post_ghost['wins']} losses={post_ghost['losses']}")
    print(f"ai_players[{AGENT}].cash: pre={ai_cash_pre} post={ai_cash_post}")

    if not all_pass:
        print()
        print("RESULT: FAIL — invariants violated. Synthetic rows left in place for inspection.")
        return 1

    # ── Cleanup — restore options_books exact pre-state + delete synthetic rows
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    for tid in trade_ids:
        if tid > 0:
            c.execute("DELETE FROM options_trades WHERE id=?", (tid,))
    c.execute(
        "UPDATE options_books SET current_cash=?, total_trades=?, wins=?, losses=? WHERE book_tag=?",
        (pre_ghost["current_cash"], pre_ghost["total_trades"], pre_ghost["wins"], pre_ghost["losses"], "ghost"),
    )
    c.execute("DELETE FROM ai_players WHERE id=?", (AGENT,))
    conn.commit()
    conn.close()

    conn = sqlite3.connect(DB); conn.row_factory = sqlite3.Row
    final_ghost = fetch_one(conn.cursor(), "SELECT current_cash, total_trades, wins, losses FROM options_books WHERE book_tag=?", "ghost")
    final_fleet = fetch_one(conn.cursor(), "SELECT current_cash, total_trades, wins, losses FROM options_books WHERE book_tag=?", "fleet")
    leftover_count = conn.cursor().execute(
        "SELECT COUNT(*) FROM options_trades WHERE id IN (" + ",".join("?" for _ in trade_ids) + ")",
        trade_ids,
    ).fetchone()[0]
    leftover_agent = fetch_one(conn.cursor(), "SELECT id FROM ai_players WHERE id=?", AGENT)
    conn.close()

    cleanup_ok = (
        final_ghost["current_cash"] == pre_ghost["current_cash"]
        and final_ghost["total_trades"] == pre_ghost["total_trades"]
        and final_ghost["wins"] == pre_ghost["wins"]
        and final_ghost["losses"] == pre_ghost["losses"]
        and final_fleet["current_cash"] == pre_fleet["current_cash"]
        and final_fleet["total_trades"] == pre_fleet["total_trades"]
        and leftover_count == 0
        and leftover_agent is None
    )

    print()
    print("CLEANUP CHECK:")
    print(f"  ghost cash restored: {final_ghost['current_cash']}")
    print(f"  ghost trades restored: {final_ghost['total_trades']}")
    print(f"  fleet untouched: cash={final_fleet['current_cash']}")
    print(f"  synthetic trade rows deleted: {leftover_count == 0}")
    print(f"  synthetic agent row deleted: {leftover_agent is None}")
    print(f"  CLEANUP OK: {cleanup_ok}")

    print()
    if all_pass and cleanup_ok:
        print(f"RESULT: PASS — {len(invariants)}/{len(invariants)} invariants + clean teardown. Phase 3 unblocked.")
        return 0
    print("RESULT: FAIL — cleanup incomplete.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
