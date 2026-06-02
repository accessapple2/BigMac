"""Ollie Machine — P3 scheduled loop. Assembles the existing P1/P2 modules into a
daily evaluate→enter + an intraday exit-monitor. NO new doctrine logic — pure
orchestration over ollie_machine (P1/P2b), ollie_machine_p2a, ollie_machine_p2c.

SIM-ONLY / TRACKING by construction: every write lands in ollie_machine_ledger /
ollie_machine_picks. This loop NEVER calls paper_trader.buy() or any executor — the
SIM entry path (`p2a.sim_enter`) is a direct INSERT into ollie_machine_ledger. The
player stays can_trade_live=0 + portfolio.execution_mode='tracking' + absent from
every scan/exec roster, so the live trader's own loops still don't act on it.
"""
from __future__ import annotations

from engine import ollie_machine as om
from engine import ollie_machine_p2a as p2a
from engine import ollie_machine_p2c as p2c

ENTRY_SOURCE = "p3-sim"


def run_daily_cycle() -> dict:
    """Daily (post-close, fresh nightly signals): evaluate → universe filter →
    >=2-of-4 → rank → top-N → brackets (/api/trade-levels) → SIM-enter (ledger-direct,
    2% notional / 5-concurrent cap / -2% daily breaker). Returns a summary for logging."""
    res = om.run(write=True, apply_filter=True)          # refresh ollie_machine_picks
    conn = p2a._conn()
    try:
        p2a.ensure_ledger_table(conn)
        p2c.ensure_exit_columns(conn)
        reg = p2a.register_player(conn)                  # idempotent
        bracketed = p2c.generate_top3_brackets(conn)     # writes brackets onto top-3 picks
        entry = p2a.sim_enter(conn, bracketed, reg["portfolio_id"], source=ENTRY_SOURCE)
    finally:
        conn.close()
    quals = res.get("clean_qualifiers") or res.get("qualifiers") or []
    return {
        "ts": res.get("ts"),
        "universe_pre": res.get("universe_pre"),
        "universe_post": res.get("universe_post"),
        "qualifiers": len(quals),
        "top": [s["symbol"] for s in res.get("top", [])],
        "opened": entry.get("opened", []),
        "skipped": entry.get("skipped", []),
        "breaker_tripped": entry.get("breaker_tripped"),
        "today_realized": entry.get("today_realized"),
    }


def run_exit_monitor() -> dict:
    """Intraday: check open ledger positions vs current price; close on stop/tp
    (realized_pnl, closed_at, exit_price, exit_reason), feeding the -2% daily breaker."""
    conn = p2a._conn()
    try:
        return p2c.exit_monitor(conn)
    finally:
        conn.close()
