"""Wave 1 Fix #5 — synthetic close test (mandatory G4 dormant-code guard).

close_options_trade has ZERO production callers per audit
reports/wave1_fix5_assignment_path_analysis.md §3.3. Verify all 8
invariants before wiring it as production-load-bearing from
wheel_strategy.py::check_wheel_assignments.

Pattern mirrors Fix #4 §4 synthetic test. Distinct agent_id
'wave1_fix5_sanity_test' so cleanup is unambiguous. ai_players row
created + removed for the agent to verify decoupling invariant
(Fix #4 G10.5 doctrine).

Pre/post options_books delta must be exactly zero after cleanup.
"""
import json
import sqlite3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from engine.options_exec import open_options_trade, close_options_trade

DB = "data/trader.db"
AGENT = "wave1_fix5_sanity_test"


def fetch(c, sql, *args):
    return c.execute(sql, args).fetchone()


def main() -> int:
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Pre-state snapshot — fleet + ghost books
    pre_fleet = fetch(c, "SELECT current_cash, total_trades, wins, losses FROM options_books WHERE book_tag=?", "fleet")
    pre_ghost = fetch(c, "SELECT current_cash, total_trades, wins, losses FROM options_books WHERE book_tag=?", "ghost")
    print(f"PRE  fleet: cash={pre_fleet['current_cash']} trades={pre_fleet['total_trades']} wins={pre_fleet['wins']} losses={pre_fleet['losses']}")
    print(f"PRE  ghost: cash={pre_ghost['current_cash']} trades={pre_ghost['total_trades']} wins={pre_ghost['wins']} losses={pre_ghost['losses']}")

    # Stash synthetic agent in ai_players to test the decoupling invariant.
    # If row exists from a prior run, reuse; otherwise create.
    existing = fetch(c, "SELECT cash, halt_mode FROM ai_players WHERE id=?", AGENT)
    if existing is None:
        c.execute(
            "INSERT INTO ai_players (id, display_name, cash, halt_mode, halt_reason, provider, model_id, is_active) "
            "VALUES (?, ?, 100000.0, 'full', 'wave1_fix5 synthetic test agent', 'synthetic', 'none', 0)",
            (AGENT, "Wave1 Fix5 Sanity Test"),
        )
        conn.commit()
        ai_cash_pre = 100000.0
        print(f"Created synthetic ai_players row {AGENT} cash=100000.0 halt_mode=full")
    else:
        ai_cash_pre = float(existing["cash"])
        print(f"Reusing existing ai_players row {AGENT} cash={ai_cash_pre}")
    conn.close()

    # Open synthetic short put on the GHOST book (insulates fleet ledger).
    tid = open_options_trade(
        book_tag="ghost",
        agent_id=AGENT,
        structure="csp",
        symbol="SYNTH",
        expiration="2026-12-31",
        legs=[{"side": "short", "type": "put", "strike": 100.0, "qty": 1, "entry_price": 2.50}],
        regime="TEST",
        vix=20.0,
        notes="Wave 1 Fix #5 synthetic close test — DELETED AT END OF SCRIPT",
    )
    if tid is None:
        print("FAIL: open_options_trade returned None — cannot proceed")
        return 1
    print(f"Opened synthetic trade id={tid}")

    # Mid-state — ghost cash should have moved +250 (= +2.50 * 1 * 100)
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    mid_ghost = fetch(c, "SELECT current_cash, total_trades FROM options_books WHERE book_tag=?", "ghost")
    expected_mid_cash = pre_ghost["current_cash"] + 250.0
    print(f"MID  ghost: cash={mid_ghost['current_cash']} expected={expected_mid_cash} "
          f"delta={mid_ghost['current_cash'] - pre_ghost['current_cash']}")
    conn.close()

    # Close synthetic short put — buy-to-close at 0.50 (OTM-style)
    pnl = close_options_trade(
        trade_id=tid,
        exit_legs=[{"side": "short", "type": "put", "strike": 100.0, "qty": 1, "exit_price": 0.50}],
        exit_reason="synthetic_test",
    )
    if pnl is None:
        print("FAIL: close_options_trade returned None")
        return 1
    print(f"Closed synthetic trade pnl={pnl} expected=200.00")

    # Post-state — verify all 8 invariants
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    row = fetch(c, "SELECT status, exit_credit_debit, exit_date, pnl, pnl_pct, exit_reason FROM options_trades WHERE id=?", tid)
    post_ghost = fetch(c, "SELECT current_cash, total_trades, wins, losses FROM options_books WHERE book_tag=?", "ghost")
    ai_cash_post_raw = fetch(c, "SELECT cash FROM ai_players WHERE id=?", AGENT)
    ai_cash_post = float(ai_cash_post_raw["cash"]) if ai_cash_post_raw else None
    conn.close()

    checks = {
        "I1 status flipped to closed":
            row["status"] == "closed",
        "I2 exit_credit_debit < 0 (debit on close)":
            row["exit_credit_debit"] is not None and row["exit_credit_debit"] < 0 and row["exit_credit_debit"] == -50.0,
        "I3 exit_date populated":
            row["exit_date"] is not None and len(row["exit_date"]) > 0,
        "I4 pnl computed correctly (entry 250 - close 50 = +200)":
            row["pnl"] == 200.0,
        "I5 pnl_pct (CSP max_loss=None → NULL is by design)":
            row["pnl_pct"] is None,
        "I6 exit_reason set":
            row["exit_reason"] == "synthetic_test",
        "I7 options_books.ghost.current_cash decremented by close_cost (-50)":
            abs(post_ghost["current_cash"] - (pre_ghost["current_cash"] + 250.0 - 50.0)) < 0.01,
        "I8 options_books.ghost.wins incremented (+1, pnl > 0)":
            post_ghost["wins"] == pre_ghost["wins"] + 1,
        "I9 ai_players.cash UNCHANGED (decoupling per G10.5 doctrine)":
            ai_cash_post == ai_cash_pre,
    }

    print()
    print("INVARIANT CHECK:")
    all_pass = True
    for name, passed in checks.items():
        flag = "PASS" if passed else "FAIL"
        if not passed:
            all_pass = False
        print(f"  {flag}: {name}")

    print()
    print(f"options_trades row: status={row['status']} exit_credit_debit={row['exit_credit_debit']} "
          f"exit_date={row['exit_date']} pnl={row['pnl']} pnl_pct={row['pnl_pct']} exit_reason={row['exit_reason']}")
    print(f"POST ghost: cash={post_ghost['current_cash']} trades={post_ghost['total_trades']} "
          f"wins={post_ghost['wins']} losses={post_ghost['losses']}")
    print(f"ai_players[{AGENT}].cash: pre={ai_cash_pre} post={ai_cash_post}")

    if not all_pass:
        print()
        print("RESULT: FAIL — invariants violated. Synthetic trade left in place for inspection.")
        return 1

    # Cleanup — restore options_books ghost cash + delete synthetic rows.
    # Per Fix #4 pattern, pre/post delta must be exactly zero.
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("DELETE FROM options_trades WHERE id=?", (tid,))
    c.execute(
        "UPDATE options_books SET current_cash=?, total_trades=?, wins=?, losses=? WHERE book_tag=?",
        (pre_ghost["current_cash"], pre_ghost["total_trades"], pre_ghost["wins"], pre_ghost["losses"], "ghost"),
    )
    c.execute("DELETE FROM ai_players WHERE id=?", (AGENT,))
    conn.commit()
    conn.close()

    # Verify cleanup restored exact pre-state
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    final_ghost = fetch(c, "SELECT current_cash, total_trades, wins, losses FROM options_books WHERE book_tag=?", "ghost")
    final_fleet = fetch(c, "SELECT current_cash, total_trades, wins, losses FROM options_books WHERE book_tag=?", "fleet")
    leftover_trade = fetch(c, "SELECT id FROM options_trades WHERE id=?", tid)
    leftover_agent = fetch(c, "SELECT id FROM ai_players WHERE id=?", AGENT)
    conn.close()

    print()
    print("CLEANUP CHECK:")
    cleanup_ok = (
        final_ghost["current_cash"] == pre_ghost["current_cash"]
        and final_ghost["total_trades"] == pre_ghost["total_trades"]
        and final_ghost["wins"] == pre_ghost["wins"]
        and final_ghost["losses"] == pre_ghost["losses"]
        and final_fleet["current_cash"] == pre_fleet["current_cash"]
        and final_fleet["total_trades"] == pre_fleet["total_trades"]
        and leftover_trade is None
        and leftover_agent is None
    )
    print(f"  ghost cash restored: {final_ghost['current_cash']} (pre={pre_ghost['current_cash']})")
    print(f"  ghost total_trades restored: {final_ghost['total_trades']} (pre={pre_ghost['total_trades']})")
    print(f"  fleet untouched: cash={final_fleet['current_cash']} (pre={pre_fleet['current_cash']})")
    print(f"  synthetic trade row deleted: {leftover_trade is None}")
    print(f"  synthetic agent row deleted: {leftover_agent is None}")
    print(f"  CLEANUP OK: {cleanup_ok}")

    print()
    if all_pass and cleanup_ok:
        print("RESULT: PASS — 9/9 invariants + clean teardown. Phase 3 unblocked.")
        return 0
    print("RESULT: FAIL — cleanup incomplete.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
