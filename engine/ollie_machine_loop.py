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


# ── HM-OLLIE-MACHINE-BRACKET-WINDOW 2026-06-05 — split daily cycle into two ────
# phases so each runs when its data dependency is healthy:
#   • run_pick_generation()  — 21:00 post-close, on fresh nightly RS/Minervini
#                              signals (pick SELECTION only; levels stay NULL).
#   • run_bracket_and_enter()— pre-open market window (06:30), when /api/trade-
#                              levels is warm + computable. The 21:00 window had
#                              the endpoint cold (warmer off after 20:00) which
#                              left every pick bracket-less and the ledger empty.
# run_daily_cycle() above stays intact for manual / backtest use.
def run_pick_generation() -> dict:
    """21:00 post-close: refresh ollie_machine_picks from fresh nightly signals.
    SELECTION ONLY — bracketing + entry happen in run_bracket_and_enter()."""
    res = om.run(write=True, apply_filter=True)
    return {
        "ts": res.get("ts"),
        "universe_pre": res.get("universe_pre"),
        "universe_post": res.get("universe_post"),
        "top": [s["symbol"] for s in res.get("top", [])],
    }


def run_bracket_and_enter() -> dict:
    """Pre-open market window: bracket the latest picks via /api/trade-levels +
    SIM-enter (ledger-direct, flat-then-enter). Same guards as run_daily_cycle —
    NEVER calls paper_trader.buy()."""
    conn = p2a._conn()
    try:
        p2a.ensure_ledger_table(conn)
        p2c.ensure_exit_columns(conn)
        reg = p2a.register_player(conn)
        bracketed = p2c.generate_top3_brackets(conn)
        entry = p2a.sim_enter(conn, bracketed, reg["portfolio_id"], source=ENTRY_SOURCE)
    finally:
        conn.close()
    return {
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
