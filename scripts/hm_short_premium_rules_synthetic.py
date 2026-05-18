"""HM-CHECK-OPTION-EXITS-SHORT-PREMIUM-RULES synthetic test (2026-05-18).

Creates 3 disposable synthetic CSPs and drives each through one branch of
the short-premium TP/SL/TIME-STOP decision tree:

  - CSP-A: deep OTM, 5 DTE  → TP fires (premium decayed to ≤50%)
  - CSP-B: deep ITM, 60 DTE → SL fires (premium expanded ≥2× via intrinsic)
  - CSP-C: ATM, 21 DTE      → TIME-STOP fires (no TP/SL trigger)

Verifies exit_reason for each, idempotent re-fire (noop), and full
options_books restore on cleanup (same reversibility doctrine as Item 1).

Run:
    cd ~/autonomous-trader && venv/bin/python3 \
        scripts/hm_short_premium_rules_synthetic.py
"""
from __future__ import annotations

import sqlite3
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.options_exec import open_options_trade
from engine.paper_trader import _check_option_exits_canonical_short_premium

DB_PATH = "data/trader.db"
AGENT_ID = "synthetic_test_csp_rules"


def _conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def _cleanup(trade_ids: list, pre_book) -> None:
    if not trade_ids:
        return
    conn = sqlite3.connect(DB_PATH)
    try:
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


def _check(label: str, actual, expected, results: list) -> None:
    ok = actual == expected
    sym = "✓" if ok else "✗"
    print(f"  {sym} {label}: actual={actual!r} expected={expected!r}")
    results.append((label, ok))


def main() -> int:
    trade_ids: list = []
    results: list = []
    entry_premium = 2.0  # $2.00/share → $200/contract

    # Pre-state snapshot
    conn = _conn()
    try:
        pre_book = dict(conn.execute(
            "SELECT current_cash, total_trades, wins, losses "
            "FROM options_books WHERE book_tag='fleet'"
        ).fetchone())
    finally:
        conn.close()
    print(f"[PRE] options_books: {pre_book}")

    today = datetime.now()
    exp_tp = (today + timedelta(days=5)).strftime("%Y-%m-%d")
    exp_sl = (today + timedelta(days=60)).strftime("%Y-%m-%d")
    exp_ts = (today + timedelta(days=20)).strftime("%Y-%m-%d")

    cases = [
        # symbol, strike, stock_price, expiration, expected_exit_tag
        ("SYN_CSP_TP", 50.0, 60.0, exp_tp, "tp_premium_decay_50pct"),
        ("SYN_CSP_SL", 50.0, 40.0, exp_sl, "sl_premium_expansion_2x"),
        ("SYN_CSP_TS", 50.0, 50.0, exp_ts, "time_stop_21dte"),
    ]

    try:
        for sym, strike, _, exp, _ in cases:
            tid = open_options_trade(
                book_tag="fleet", agent_id=AGENT_ID, structure="csp",
                symbol=sym, expiration=exp,
                legs=[{
                    "side": "short", "type": "put", "strike": strike,
                    "qty": 1, "entry_price": entry_premium,
                }],
                notes="HM-CHECK-OPTION-EXITS-SHORT-PREMIUM-RULES synthetic",
            )
            if not tid:
                print(f"  ✗ open_options_trade returned None for {sym}")
                return 1
            trade_ids.append(tid)
            print(f"  opened {sym} strike={strike} exp={exp} tid={tid}")

        # Build prices dict — drives estimate_option_price (Polygon returns
        # None for synthetic underlyings, so falls back to BSM-style estimate).
        prices = {
            sym: {"price": stock}
            for sym, _, stock, _, _ in cases
        }

        closed = _check_option_exits_canonical_short_premium(prices)
        print(f"\nclosed entries: {len(closed)}")
        for c in closed:
            print(f"  closed: tid={c['trade_id']} sym={c['symbol']} "
                  f"exit={c['exit_reason']} pnl={c['pnl']}")

        # Verify each case fired with correct exit_tag
        for tid, (sym, strike, stock, exp, expected_tag) in zip(trade_ids, cases):
            matched = next((c for c in closed if c["trade_id"] == tid), None)
            if matched is None:
                print(f"  ✗ {sym} (tid={tid}) DID NOT FIRE — expected {expected_tag}")
                results.append((f"{sym}-fires", False))
                continue
            _check(f"{sym} exit_reason", matched["exit_reason"], expected_tag, results)
            results.append((f"{sym} pnl present", matched["pnl"] is not None))
            print(f"    {sym} pnl: {matched['pnl']}")

            # Verify status='closed' in options_trades
            conn = _conn()
            try:
                row = conn.execute(
                    "SELECT status, exit_reason FROM options_trades WHERE id=?",
                    (tid,),
                ).fetchone()
            finally:
                conn.close()
            _check(f"{sym} status", row["status"], "closed", results)
            _check(f"{sym} exit_reason on row", row["exit_reason"], expected_tag, results)

        # Idempotent re-fire: closed rows skipped (status='open' guard)
        rerun = _check_option_exits_canonical_short_premium(prices)
        _check("re-fire returns 0 closes", len(rerun), 0, results)

    except Exception:
        traceback.print_exc()
        results.append(("ERROR_DURING_TEST", False))
    finally:
        _cleanup(trade_ids, pre_book)
        # Verify cleanup restored pre-state
        conn = _conn()
        try:
            post_book = dict(conn.execute(
                "SELECT current_cash, total_trades, wins, losses "
                "FROM options_books WHERE book_tag='fleet'"
            ).fetchone())
        finally:
            conn.close()
        cleanup_ok = post_book == pre_book
        print(f"\n[CLEANUP] post={post_book} {'OK' if cleanup_ok else 'FAIL'}")
        if not cleanup_ok:
            results.append(("cleanup_restore", False))

    passed = sum(1 for _, ok in results if ok)
    total = len(results)
    print(f"\n[SUMMARY] {passed}/{total} checks PASSED")
    if passed != total:
        print("[FAIL]")
        for label, ok in results:
            if not ok:
                print(f"  - {label}")
        return 1
    print("[OK]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
