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
            "SELECT is_human FROM ai_players WHERE id = ?", (player_id,)
        ).fetchone()
    finally:
        if owns_conn:
            conn.close()
    if row is None:
        return False
    is_human_value = row[0] if not hasattr(row, "keys") else row["is_human"]
    return not bool(is_human_value)


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
