#!/usr/bin/env python3
"""HM-GRADE-B-BACKTEST 2026-05-20 — replay May 2026 Grade B entries through the
new HM-GRADE-B-REGIME-GATE (PR #43) + HM-GRADE-B-SPY-INTRADAY-GATE (PR #47).

Diagnostic only — no DB writes. Shows projected May Grade B PnL with both
gates active vs the actual May PnL.

Layer 1 (regime): block if regime_history.regime ∈ {BEAR_CROSS, CAUTIOUS_BEAR}
Layer 2 (SPY): block if SPY intraday change_pct < -0.1% on the entry day

Joins:
- ollie_super_trades (signal_grade='B', regime, entry_price, created_at)
- trades (matched by buy_trade_id) for realized_pnl after exit
- regime_history (date) for the day's regime
- portfolio_history or SPY price snapshots for intraday SPY change

For SPY change_pct, we approximate using the daily close-to-close of SPY from
the regime_history table itself (spy_close field). The "intraday" check is
imperfect at backtest time — we use prior_close → entry_day_close as a proxy.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from collections import defaultdict

REPO_ROOT = Path(__file__).resolve().parent.parent
DB = REPO_ROOT / "data" / "trader.db"

BEARISH_REGIMES = {"BEAR_CROSS", "CAUTIOUS_BEAR"}
SPY_INTRADAY_THRESHOLD = -0.1  # percent — gate blocks if SPY chg < -0.1%

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(str(DB))
    c.row_factory = sqlite3.Row
    return c


def fetch_may_grade_b_trades() -> list[dict]:
    """All Grade B entries from ollie-auto in May 2026, joined to trades for realized_pnl."""
    c = _conn()
    rows = c.execute(
        """
        SELECT
            ost.id AS ost_id,
            ost.buy_trade_id,
            ost.symbol,
            ost.entry_price,
            ost.regime AS regime_at_entry,
            ost.created_at AS entry_ts,
            date(ost.created_at) AS entry_date,
            t.realized_pnl AS buy_pnl,
            (SELECT SUM(realized_pnl) FROM trades
              WHERE player_id='ollie-auto' AND symbol=ost.symbol
              AND action='SELL'
              AND executed_at > ost.created_at
              AND executed_at < date(ost.created_at, '+30 days')) AS exit_pnl_sum
        FROM ollie_super_trades ost
        LEFT JOIN trades t ON t.id = ost.buy_trade_id
        WHERE ost.signal_grade='B'
        AND substr(ost.created_at,1,7) = '2026-05'
        ORDER BY ost.created_at;
        """
    ).fetchall()
    c.close()
    return [dict(r) for r in rows]


def fetch_regime_for_date(date_str: str) -> str | None:
    c = _conn()
    row = c.execute(
        "SELECT regime FROM regime_history WHERE date=? ORDER BY id DESC LIMIT 1",
        (date_str,),
    ).fetchone()
    c.close()
    return row["regime"] if row else None


def fetch_spy_chg_for_date(date_str: str) -> float | None:
    """SPY close-to-close pct change for the given date (used as proxy for
    intraday change — backtest can't reconstruct true intraday timing)."""
    c = _conn()
    # Get target date close + prior trading day close
    today_row = c.execute(
        "SELECT spy_close FROM regime_history WHERE date=? LIMIT 1", (date_str,)
    ).fetchone()
    prior_row = c.execute(
        "SELECT spy_close FROM regime_history WHERE date < ? "
        "ORDER BY date DESC LIMIT 1",
        (date_str,),
    ).fetchone()
    c.close()
    if not today_row or not prior_row:
        return None
    today_close = today_row["spy_close"]
    prior_close = prior_row["spy_close"]
    if today_close is None or prior_close is None or prior_close == 0:
        return None
    return (today_close - prior_close) / prior_close * 100.0


def main() -> int:
    print("══════ HM-GRADE-B-BACKTEST 2026-05-20 ══════")
    print(f"DB: {DB}")
    print(f"Period: 2026-05 (May)")
    print(f"Gate L1 (regime): BLOCK if regime ∈ {sorted(BEARISH_REGIMES)}")
    print(f"Gate L2 (SPY):    BLOCK if SPY chg < {SPY_INTRADAY_THRESHOLD}% (close-to-close proxy)")
    print()

    trades = fetch_may_grade_b_trades()
    if not trades:
        print("  No Grade B trades found in May 2026. Done.")
        return 0
    print(f"  → {len(trades)} Grade B entries found in May 2026")
    print()

    taken_pnl = 0.0
    avoided_loss = 0.0
    avoided_gain = 0.0
    n_taken = 0
    n_blocked_regime = 0
    n_blocked_spy = 0
    n_unresolved = 0  # no exit PnL on file

    rows_log = []

    for t in trades:
        date_str = t["entry_date"]
        sym = t["symbol"]
        regime_day = fetch_regime_for_date(date_str)
        spy_chg = fetch_spy_chg_for_date(date_str)
        # Use exit_pnl_sum as the realized outcome for this entry
        pnl = t["exit_pnl_sum"]
        if pnl is None:
            n_unresolved += 1

        # Apply gates in stack order
        block_reason = None
        if regime_day in BEARISH_REGIMES:
            block_reason = f"L1 regime={regime_day}"
            n_blocked_regime += 1
        elif spy_chg is not None and spy_chg < SPY_INTRADAY_THRESHOLD:
            block_reason = f"L2 SPY={spy_chg:+.3f}%"
            n_blocked_spy += 1

        if block_reason:
            # Trade would have been BLOCKED — count its actual PnL as avoided
            if pnl is not None:
                if pnl < 0:
                    avoided_loss += abs(pnl)
                else:
                    avoided_gain += pnl
        else:
            n_taken += 1
            if pnl is not None:
                taken_pnl += pnl

        rows_log.append({
            "date": date_str,
            "sym": sym,
            "regime": regime_day,
            "spy_chg": spy_chg,
            "blocked": bool(block_reason),
            "reason": block_reason or "TAKEN",
            "pnl": pnl,
        })

    # Per-trade ledger
    print("Per-trade ledger:")
    print(f"  {'date':12} {'sym':6} {'regime':14} {'spy%':>7} {'verdict':20} {'pnl':>9}")
    print(f"  {'-'*12} {'-'*6} {'-'*14} {'-'*7} {'-'*20} {'-'*9}")
    for r in rows_log:
        verdict = r["reason"][:20]
        spy_disp = f"{r['spy_chg']:+.3f}" if r["spy_chg"] is not None else "n/a"
        pnl_disp = f"{r['pnl']:+.2f}" if r["pnl"] is not None else "open"
        print(f"  {r['date']:12} {r['sym']:6} {str(r['regime']):14} {spy_disp:>7} {verdict:20} {pnl_disp:>9}")
    print()

    # Summary
    n_blocked = n_blocked_regime + n_blocked_spy
    actual_pnl = sum(r["pnl"] for r in rows_log if r["pnl"] is not None)

    print("══════ SUMMARY ══════")
    print(f"  Trades total:           {len(trades)}")
    print(f"  Taken (gates allowed):  {n_taken}")
    print(f"  Blocked by L1 (regime): {n_blocked_regime}")
    print(f"  Blocked by L2 (SPY):    {n_blocked_spy}")
    print(f"  Total blocked:          {n_blocked}")
    print(f"  Unresolved (no exit PnL on file): {n_unresolved}")
    print()
    print(f"  Actual May Grade B PnL (no gates):     {actual_pnl:+.2f}")
    print(f"  Projected May Grade B PnL with gates:  {taken_pnl:+.2f}")
    print(f"  Avoided loss (blocked, would've lost): {avoided_loss:+.2f}")
    print(f"  Foregone gain (blocked, would've won): {avoided_gain:+.2f}")
    print(f"  Net effect of gates: {taken_pnl - actual_pnl:+.2f}")
    print()
    print("Notes:")
    print("  - 'SPY chg' uses close-to-close as proxy for intraday — real gate")
    print("    fires on live intraday change_pct, which can be tighter/looser.")
    print("  - 'Unresolved' trades have no matched SELL within 30d window; their")
    print("    PnL doesn't contribute to either taken_pnl or avoided figures.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
