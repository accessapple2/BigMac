"""HM-WHEEL-ASSIGNMENT-LEDGER synthetic ITM-forcing test (2026-05-18).

Creates 3 disposable synthetic wheel CSPs at controlled strikes, fires
engine.wheel_assignment_ledger.assign_csp on each with forced ITM spot,
and verifies the 12 invariants. Self-cleans the synthetic rows on exit.

Run:
    cd ~/autonomous-trader && venv/bin/python3 \
        scripts/hm_wheel_assignment_ledger_synthetic.py

Per audit §6 — synthetic symbols (TEST_TQQQ etc.) prevent any real
Polygon fetch and isolate the canonical writer.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.options_exec import open_options_trade
from engine.wheel_assignment_ledger import assign_csp

DB_PATH = "data/trader.db"
AGENT_ID = "synthetic_test"


def _conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def _cleanup(trade_ids: list[int], pre_book: dict | None) -> None:
    """Delete synthetic rows AND restore options_books counters to pre-state.

    Mirrors wave1_fix5_synthetic_close.py reversibility doctrine: row
    deletion alone leaves the cash + counter side effects of
    open_options_trade / close_options_trade behind. Must explicitly
    UPDATE options_books back to pre-state for full reversibility.
    """
    if not trade_ids:
        return
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executemany(
            "DELETE FROM paper_assignment_liability WHERE source_trade_id=?",
            [(tid,) for tid in trade_ids],
        )
        conn.executemany(
            "DELETE FROM options_trades WHERE id=?",
            [(tid,) for tid in trade_ids],
        )
        if pre_book is not None:
            conn.execute(
                "UPDATE options_books "
                "SET current_cash=?, total_trades=?, wins=?, losses=? "
                "WHERE book_tag='fleet'",
                (
                    pre_book["current_cash"], pre_book["total_trades"],
                    pre_book["wins"], pre_book["losses"],
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _check(label: str, actual, expected, results: list) -> bool:
    ok = actual == expected
    sym = "✓" if ok else "✗"
    print(f"  {sym} {label}: actual={actual!r} expected={expected!r}")
    results.append((label, ok))
    return ok


def _check_truthy(label: str, actual, results: list) -> bool:
    ok = bool(actual)
    sym = "✓" if ok else "✗"
    print(f"  {sym} {label}: {actual!r}")
    results.append((label, ok))
    return ok


def main() -> int:
    trade_ids: list[int] = []
    results: list[tuple[str, bool]] = []

    # Pre-state snapshot (for invariants 10 + 11 + cleanup restoration)
    conn = _conn()
    try:
        pre_book_row = conn.execute(
            "SELECT current_cash, total_trades, wins, losses "
            "FROM options_books WHERE book_tag='fleet'"
        ).fetchone()
        pre_cash = float(pre_book_row["current_cash"]) if pre_book_row else 0.0
        pre_book = dict(pre_book_row) if pre_book_row else None
        pre_player_row = conn.execute(
            "SELECT cash FROM ai_players WHERE id='options-sosnoff'"
        ).fetchone()
        pre_player_cash = (
            float(pre_player_row["cash"]) if pre_player_row else None
        )
    finally:
        conn.close()

    print(f"[PRE]  options_books.current_cash = {pre_cash}")
    print(f"[PRE]  ai_players.cash[options-sosnoff] = {pre_player_cash}")

    # Spot < Strike → guaranteed ITM. Strike = spot × 1.5 so intrinsic is
    # exactly half the strike, far enough to make rounding errors visible
    # if they exist.
    fake_spots = {
        "SYN_HMWAL_A": 50.0,
        "SYN_HMWAL_B": 30.0,
        "SYN_HMWAL_C": 80.0,
    }
    strikes = {sym: spot * 1.5 for sym, spot in fake_spots.items()}
    entry_premium = 2.0  # $2.00/share = $200/contract premium
    today_str = datetime.now().strftime("%Y-%m-%d")
    exp_future = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")

    try:
        # Open 3 synthetic CSPs at strikes guaranteed-ITM vs fake spots.
        for sym, strike in strikes.items():
            tid = open_options_trade(
                book_tag="fleet",
                agent_id=AGENT_ID,
                structure="csp",
                symbol=sym,
                expiration=exp_future,
                legs=[
                    {
                        "side": "short", "type": "put",
                        "strike": strike, "qty": 1,
                        "entry_price": entry_premium,
                    }
                ],
                notes="HM-WHEEL-ASSIGNMENT-LEDGER synthetic test",
            )
            if not tid:
                print(f"  ✗ open_options_trade returned None for {sym}")
                return 1
            trade_ids.append(tid)
            print(f"  opened synthetic CSP: {sym} strike={strike} tid={tid}")

        # Backdate expirations to today so the canonical readers would
        # see them past-expiry. Direct assign_csp() doesn't actually need
        # this (it works on the strike + spot params), but we do it for
        # cleanliness so the test row's data shape mirrors a real
        # expiry-day assignment.
        conn = sqlite3.connect(DB_PATH)
        try:
            conn.executemany(
                "UPDATE options_trades SET expiration=? WHERE id=?",
                [(today_str, tid) for tid in trade_ids],
            )
            conn.commit()
        finally:
            conn.close()

        # FIRE 1: assign each CSP with forced ITM spot.
        first_fire: list[dict] = []
        for tid, (sym, spot) in zip(trade_ids, fake_spots.items()):
            r = assign_csp(tid, spot, assignment_date=today_str)
            print(f"  assign_csp(tid={tid}, sym={sym}, spot={spot}) -> {r}")
            first_fire.append(r)

        # ────────── Invariants 1-9, 12 ──────────
        for r, tid, (sym, spot) in zip(first_fire, trade_ids, fake_spots.items()):
            strike = strikes[sym]
            intrinsic_expected = round(strike - spot, 2)
            # 1. assign_csp returns status='assigned'
            _check(f"[I1] tid={tid} status", r.get("status"), "assigned", results)

            # Read row + ledger
            conn = _conn()
            try:
                trade_row = conn.execute(
                    "SELECT status, exit_reason, pnl, entry_credit_debit "
                    "FROM options_trades WHERE id=?", (tid,),
                ).fetchone()
                ledger_rows = conn.execute(
                    "SELECT * FROM paper_assignment_liability "
                    "WHERE source_trade_id=?", (tid,),
                ).fetchall()
            finally:
                conn.close()

            # 2. options_trades.status='closed'
            _check(f"[I2] tid={tid} options_trades.status",
                   trade_row["status"], "closed", results)
            # 3. options_trades.exit_reason='expired_itm_assigned'
            _check(f"[I3] tid={tid} exit_reason",
                   trade_row["exit_reason"], "expired_itm_assigned", results)
            # 4. options_trades.pnl numeric (entry_credit - intrinsic*100)
            expected_pnl = (
                float(trade_row["entry_credit_debit"]) - intrinsic_expected * 100.0
            )
            _check(f"[I4] tid={tid} pnl",
                   round(float(trade_row["pnl"]), 2),
                   round(expected_pnl, 2), results)
            # 5. exactly 1 ledger row per source_trade_id with status='open'
            _check(f"[I5] tid={tid} ledger row count",
                   len(ledger_rows), 1, results)
            if ledger_rows:
                lrow = ledger_rows[0]
                # 6. source_trade_id matches
                _check(f"[I6] tid={tid} ledger.source_trade_id",
                       lrow["source_trade_id"], tid, results)
                # 7. qty_shares = 100 × qty
                _check(f"[I7] tid={tid} qty_shares",
                       lrow["qty_shares"], 100, results)
                # 8. cash_secured_capital = strike × qty_shares
                _check(f"[I8] tid={tid} cash_secured_capital",
                       round(float(lrow["cash_secured_capital"]), 2),
                       round(strike * 100, 2), results)
                # 9. side = 'long_shares'
                _check(f"[I9] tid={tid} side",
                       lrow["side"], "long_shares", results)

        # ────────── Invariant 10 — options_books.current_cash delta ──────────
        conn = _conn()
        try:
            post_cash_row = conn.execute(
                "SELECT current_cash FROM options_books WHERE book_tag='fleet'"
            ).fetchone()
            post_cash = float(post_cash_row["current_cash"]) if post_cash_row else 0.0
            post_player_row = conn.execute(
                "SELECT cash FROM ai_players WHERE id='options-sosnoff'"
            ).fetchone()
            post_player_cash = (
                float(post_player_row["cash"]) if post_player_row else None
            )
        finally:
            conn.close()

        # net options_books delta = sum of [+entry_credit - close_cost] across 3 trades
        # entry_credit per CSP = entry_premium × 1 × 100 = $200; close_cost = intrinsic × 100.
        # so per-trade delta = 200 - intrinsic*100 = 200 - (strike*1.5-spot_then? no spot)
        # Actually open_options_trade credits +200 each (total +600 across 3), then
        # close_options_trade subtracts close_cost per row. Total delta:
        entry_total = entry_premium * 100 * len(trade_ids)
        close_total = sum(
            round(strikes[s] - fake_spots[s], 2) * 100 for s in fake_spots
        )
        expected_cash_delta = round(entry_total - close_total, 2)
        actual_cash_delta = round(post_cash - pre_cash, 2)
        _check("[I10] options_books.current_cash delta",
               actual_cash_delta, expected_cash_delta, results)

        # ────────── Invariant 11 — ai_players.cash unchanged (G20=C) ──────────
        _check("[I11] ai_players.cash[options-sosnoff] unchanged (G20=C)",
               post_player_cash, pre_player_cash, results)

        # ────────── Invariant 12 — re-fire is idempotent ──────────
        second_fire = []
        for tid, (sym, spot) in zip(trade_ids, fake_spots.items()):
            r = assign_csp(tid, spot, assignment_date=today_str)
            second_fire.append(r)
            _check(f"[I12] tid={tid} re-fire status",
                   r.get("status"), "noop", results)
            _check(f"[I12] tid={tid} re-fire reason",
                   r.get("reason"), "trade_not_open", results)

        # Confirm only 3 ledger rows total (no duplicate INSERT on re-fire)
        conn = _conn()
        try:
            total_rows = conn.execute(
                "SELECT COUNT(*) AS n FROM paper_assignment_liability "
                "WHERE source_trade_id IN (?, ?, ?)",
                tuple(trade_ids),
            ).fetchone()["n"]
        finally:
            conn.close()
        _check("[I12] total ledger rows after re-fire",
               total_rows, 3, results)

    except Exception:
        traceback.print_exc()
        results.append(("ERROR_DURING_TEST", False))
    finally:
        _cleanup(trade_ids, pre_book)
        print(f"  cleaned up {len(trade_ids)} synthetic options_trades + ledger rows; "
              f"options_books restored to pre-state")
        # Final verify: cleanup actually restored cash + counters
        conn = _conn()
        try:
            final_book = conn.execute(
                "SELECT current_cash, total_trades, wins, losses "
                "FROM options_books WHERE book_tag='fleet'"
            ).fetchone()
        finally:
            conn.close()
        if pre_book and final_book:
            cleanup_ok = (
                float(final_book["current_cash"]) == pre_book["current_cash"]
                and final_book["total_trades"] == pre_book["total_trades"]
                and final_book["wins"] == pre_book["wins"]
                and final_book["losses"] == pre_book["losses"]
            )
            print(f"  cleanup verify: "
                  f"cash={final_book['current_cash']} trades={final_book['total_trades']} "
                  f"wins={final_book['wins']} losses={final_book['losses']} "
                  f"{'OK' if cleanup_ok else 'FAIL'}")
            if not cleanup_ok:
                results.append(("CLEANUP_RESTORE", False))

    # ────────── Summary ──────────
    passed = sum(1 for _, ok in results if ok)
    total = len(results)
    print(f"\n[SUMMARY] {passed}/{total} invariants PASSED")
    if passed != total:
        print("[FAIL] failed invariants:")
        for label, ok in results:
            if not ok:
                print(f"  - {label}")
        return 1
    print("[OK] all invariants PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
