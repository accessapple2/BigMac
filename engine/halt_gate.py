"""Halt-gate: single source of truth for AI-player halt state.

Per XO_AUDIT_2026-05-03.md #1 + Admiral resolution of Open Q#1
(2026-05-03): halts must never trap capital. Three modes:

  'active'    — normal operation
  'exit_only' — no signals, no new entries; exits permitted (default for halts)
  'full'      — no signals, no trades, no exits (reserved for runaway agents)

Existing `is_halted=1` rows migrated to halt_mode='exit_only'. HM-B
(2026-05-04, commit 9256890) already dropped the `is_halted` column from
`ai_players` entirely, after all production read paths migrated to
halt_mode -- this docstring was stale (HM-AGENT-RULES-CONSOLIDATION
2026-07-04, AGENT-RULES-REVIEW-2026-07-03.md Inconsistency #10; see
CLAUDE.md's halt_mode doctrine section for the migration record).
`halt_mode` is the single source of truth; there is no is_halted fallback.

This module is a thin DB-read helper. Halt state changes rarely; the
trader service restarts on config change anyway, so a per-process cache
would buy little. Callers pass their own connection.
"""
from __future__ import annotations

import sqlite3
from typing import Literal

HaltMode = Literal["active", "exit_only", "full"]


def halt_mode(conn: sqlite3.Connection, player_id: str) -> HaltMode:
    """Return the player's halt_mode. Defaults to 'active' for unknown players."""
    row = conn.execute(
        "SELECT halt_mode FROM ai_players WHERE id = ?", (player_id,)
    ).fetchone()
    if row is None:
        return "active"
    mode = row[0] if not hasattr(row, "keys") else row["halt_mode"]
    return mode or "active"


def can_emit_signal(conn: sqlite3.Connection, player_id: str) -> bool:
    """Halted players in any non-active mode cannot emit signals."""
    return halt_mode(conn, player_id) == "active"


def can_open_position(conn: sqlite3.Connection, player_id: str) -> bool:
    """Only active players can open new positions."""
    return halt_mode(conn, player_id) == "active"


def can_close_position(conn: sqlite3.Connection, player_id: str) -> bool:
    """exit_only and active can close. full cannot."""
    return halt_mode(conn, player_id) in ("active", "exit_only")


# ─── Auto-trade eligibility (HM-Y) ──────────────────────────────────────────
# HM-Y (2026-05-05): players excluded from automated trading. Includes humans
# (is_human=1) AND passive broker mirrors (declared below). Mirrors are
# semantically distinct from humans — they're read-only reflections of
# external broker state — but operationally identical: any locally-initiated
# trade would diverge from broker truth. Introduced alongside webull dual-role
# split (commit 5186408) where alpaca-mirror became a passive sync target
# whose positions an autopilot scaleout immediately tried to mutate.
_PASSIVE_MIRROR_PLAYER_IDS = frozenset({
    "alpaca-mirror",
    # Future: schwab-mirror, ibkr-mirror, etc. when broker mirrors land.
})


def is_auto_tradeable(player_id: str, conn: sqlite3.Connection | None = None) -> bool:
    """Return True if this player_id is eligible for automated trading.

    Excludes:
    - Passive broker mirrors (declared in _PASSIVE_MIRROR_PLAYER_IDS)
    - Human players (is_human=1) — Steve's actual broker accounts
    - Unknown player_ids (defensive — never auto-trade something we don't know)
    - can_trade_live=0 players, but ONLY once check_can_trade_live_backfill()
      has confirmed the fleet-wide backfill ran (see that function's
      docstring — HM-AUDITION-GATE-2026-07-05). Before that check has run
      (or if it failed), this falls back to the pre-2026-07-05 is_human-only
      behavior rather than risk a false-negative fleet-wide halt from stale
      can_trade_live=0 data.

    Pass conn for hot paths (avoids reconnect cost). Omit conn and the helper
    will manage its own connection against data/trader.db.
    """
    if player_id in _PASSIVE_MIRROR_PLAYER_IDS:
        return False
    owns_conn = conn is None
    if owns_conn:
        conn = sqlite3.connect("data/trader.db", timeout=10)
    try:
        row = conn.execute(
            "SELECT is_human, can_trade_live FROM ai_players WHERE id = ?", (player_id,)
        ).fetchone()
    finally:
        if owns_conn:
            conn.close()
    if row is None:
        return False
    is_human_value = row[0] if not hasattr(row, "keys") else row["is_human"]
    result = not bool(is_human_value)
    if result and _CAN_TRADE_LIVE_ENFORCEMENT_READY:
        can_trade_live_value = row[1] if not hasattr(row, "keys") else row["can_trade_live"]
        result = bool(can_trade_live_value)
    return result


# HM-AUDITION-GATE-2026-07-05 — can_trade_live enforcement readiness.
# Module-level cache, set exactly once by check_can_trade_live_backfill()
# (called from setup_db.py at every startup). None/False = not enforced
# (legacy is_human-only behavior in is_auto_tradeable above); True = the
# fleet-wide backfill was verified present this boot, so can_trade_live=0
# genuinely blocks auto-trading. Starts False so a process that never calls
# the checker (e.g. a standalone script importing this module) never
# silently enforces against stale/unbackfilled data.
_CAN_TRADE_LIVE_ENFORCEMENT_READY = False


def check_can_trade_live_backfill(conn: sqlite3.Connection) -> bool:
    """Fleet-wide sanity check for the can_trade_live backfill migration.

    can_trade_live was purely decorative before HM-AUDITION-GATE-2026-07-05
    (checked nowhere in engine.paper_trader/engine.halt_gate; every one of
    79 ai_players rows had it =0, including every genuinely-executing
    agent). Turning enforcement on in is_auto_tradeable() without first
    backfilling can_trade_live=1 for every currently-legitimately-executing
    agent would instantly halt the live fleet.

    This compares every player is_auto_tradeable() will actually gate once
    enforcement is on against how many of those ALSO have can_trade_live=1.
    Two groups, found by tracing every _is_human_player()/is_auto_tradeable()
    call site in engine.paper_trader (buy, sell, sell_partial, short_sell,
    allocation-policy, position-value paths — HM-AUDITION-GATE-2026-07-05
    review, 2026-07-05):
      1. halt_mode='active', non-human, non-tracking-route, non-sim,
         non-auditioning — the obvious "currently executing" group.
      2. halt_mode='exit_only' agents that currently HOLD AT LEAST ONE OPEN
         POSITION. exit_only agents are excluded from the main scan roster
         (ai_brain.py only iterates halt_mode='active'), so group 1's query
         never sees them — but their real closing sell() calls (guardian-of-
         forever's dedicated stop sweep, and the other exit_only agents that
         sweep extended to per HM-AGENT-RULES-CONSOLIDATION) go through the
         SAME _is_human_player() gate inside paper_trader.sell(). Missing
         this group would silently strand real open positions with no close
         path the moment enforcement turns on — exactly the failure mode
         halt_mode='exit_only' exists to prevent in the first place.

    Only if every agent in BOTH groups has can_trade_live=1 is enforcement
    judged safe to enable. Call once at startup (setup_db.py); the
    module-level _CAN_TRADE_LIVE_ENFORCEMENT_READY flag caches the result
    for is_auto_tradeable()'s hot path. Fails closed (enforcement OFF) on
    any query error — see the module-level flag's docstring for why that's
    the safe direction.
    """
    global _CAN_TRADE_LIVE_ENFORCEMENT_READY
    try:
        from engine.trades_filter import TRACKING_PLAYERS
        track_sql = ", ".join("?" for _ in TRACKING_PLAYERS)
        # All four queries alias ai_players as p so this fragment is safe to
        # reuse verbatim whether or not the query also joins `positions`
        # (which has its own `id` column — bare `id NOT IN (...)` would be
        # ambiguous in the join queries below without the p. prefix).
        _exclude_sql = (
            f"COALESCE(p.is_human,0)=0 AND COALESCE(p.crew_role,'active') "
            f"NOT IN ('sim','auditioning') AND p.id NOT IN ({track_sql})"
        )
        total_active = conn.execute(
            f"SELECT COUNT(*) FROM ai_players p WHERE COALESCE(p.halt_mode,'active')='active' "
            f"AND {_exclude_sql}",
            TRACKING_PLAYERS,
        ).fetchone()[0]
        live_active = conn.execute(
            f"SELECT COUNT(*) FROM ai_players p WHERE COALESCE(p.halt_mode,'active')='active' "
            f"AND p.can_trade_live=1 AND {_exclude_sql}",
            TRACKING_PLAYERS,
        ).fetchone()[0]
        total_exit_with_positions = conn.execute(
            f"SELECT COUNT(DISTINCT p.id) FROM ai_players p "
            f"JOIN positions pos ON pos.player_id = p.id "
            f"WHERE p.halt_mode='exit_only' AND {_exclude_sql}",
            TRACKING_PLAYERS,
        ).fetchone()[0]
        live_exit_with_positions = conn.execute(
            f"SELECT COUNT(DISTINCT p.id) FROM ai_players p "
            f"JOIN positions pos ON pos.player_id = p.id "
            f"WHERE p.halt_mode='exit_only' AND p.can_trade_live=1 AND {_exclude_sql}",
            TRACKING_PLAYERS,
        ).fetchone()[0]
        _CAN_TRADE_LIVE_ENFORCEMENT_READY = (
            total_active > 0
            and total_active == live_active
            and total_exit_with_positions == live_exit_with_positions
        )
    except Exception:
        _CAN_TRADE_LIVE_ENFORCEMENT_READY = False
    return _CAN_TRADE_LIVE_ENFORCEMENT_READY


# ─── Read-path filter for scoring/calibration consumers (HM-C) ───────────────
# Per XO_AUDIT_2026-05-03 #1 follow-up HM-C: 1,156 rows in `signals` /
# `watchlist_signals` were backfilled with halted_emit=1 by fix #1. This
# constant is the single source of truth for the read-side filter so that a
# future migration (e.g. when `is_halted`/`halted_emit` is replaced by a
# `halt_mode` join) only has to change one line.
#
# Use ONLY in scoring/calibration/leaderboard read paths — not in raw
# signal-feed display panels, diagnostic counts, or per-player forensic views.
HALTED_EMIT_FILTER = "halted_emit = 0"


def with_halted_filter(where_clause: str = "") -> str:
    """Compose a WHERE clause that includes the halted_emit filter.

    Example:
        sql = f"SELECT ... FROM signals WHERE {with_halted_filter('player_id = ?')}"
    """
    if where_clause.strip():
        return f"({where_clause}) AND {HALTED_EMIT_FILTER}"
    return HALTED_EMIT_FILTER
