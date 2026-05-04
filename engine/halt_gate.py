"""Halt-gate: single source of truth for AI-player halt state.

Per XO_AUDIT_2026-05-03.md #1 + Admiral resolution of Open Q#1
(2026-05-03): halts must never trap capital. Three modes:

  'active'    — normal operation
  'exit_only' — no signals, no new entries; exits permitted (default for halts)
  'full'      — no signals, no trades, no exits (reserved for runaway agents)

Existing `is_halted=1` rows migrated to halt_mode='exit_only'. The
`is_halted` column remains for backwards-compat with ~22 read sites
(dashboard, war_room, morning_briefing, etc.) and will be retired in a
follow-up sprint after those sites migrate to halt_mode.

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
