"""HM-EVENTS-BUS-FOUNDATION — canonical events + signals_v2 bus helpers.

Every data source writes events via emit_event(). Every signal layer
emits normalized rows via emit_signal_v2(). Trades and debates back-fill
signals_v2 via the mark_* helpers.

ALL functions are fail-safe by design: any DB / connection / payload error
is caught, logged as `[EVENTS-BUS-WARN]` with type+repr (HM-Z/HM-AA error
posture), and the function returns None / False. **A bus write MUST NEVER
block a trade or signal emit.**

Spec: ~/.claude/projects/-Users-bigmac/memory/project_hm_events_bus_foundation.md
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from rich.console import Console

console = Console()

_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "trader.db"

# Stale-after budgets per timeframe (Captain spec). Unit: SECONDS
# (consumed via timedelta(seconds=...) in _compute_stale_after below).
# HM-INTRADAY-STALE-BUDGET 2026-06-02: 'intraday' was MISSING from this map, so
# crew_scanner's INTRADAY emissions (paper_trader.save_signal timeframe="INTRADAY")
# resolved budget=None → stale_after NULL → born never-stale, which let the INTC
# pile rebuild in signals_v2 (consumer treats NULL as never-stale, events_bus_
# consumer.py:91). 900s (15min) chosen to clear the 60s consumer poll AND the 120s
# crew_scanner emit cycle while still expiring same-session (« 6.5h RTH).
_STALE_BUDGET_S: dict[str, int] = {
    "0dte": 2,
    "intraday": 900,
    # HM-BACKTEST-REALISM 2026-07-03 (XO audit): swing was 30s — shorter than
    # intraday (900s) and shorter than the 60s consumer poll, so a swing signal
    # born just after a poll tick expired before it could EVER be dispatched.
    # Swing setups (5-30 day holds) stay valid for hours; 3600s clears the 60s
    # poll + 120s emit cycle with margin while still expiring same-session.
    "swing": 3600,
    "position": 300,
}


def _conn() -> sqlite3.Connection:
    """Per-call connection — keeps each writer thread-safe and short-lived."""
    c = sqlite3.connect(str(_DB_PATH), timeout=10, check_same_thread=False)
    return c


def _compute_stale_after(timeframe: str | None) -> str | None:
    """Return ISO timestamp at which a signal of this timeframe expires."""
    if not timeframe:
        return None
    budget = _STALE_BUDGET_S.get(timeframe.lower())
    if budget is None:
        return None
    # HM-TZ Stage 3: stale_after canonical space-UTC (was T+Z); readers (_parse_iso_utc,
    # events_bus.py:283) handle the space form. Future-dated expiry stamp.
    return (datetime.utcnow() + timedelta(seconds=budget)).strftime('%Y-%m-%d %H:%M:%S')


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def emit_event(
    *,
    source: str,
    event_type: str,
    symbol: str | None = None,
    payload: dict | None = None,
    session_date: str | None = None,
) -> int | None:
    """Write a row to the canonical events table. Returns event_id or None.

    payload is JSON-serialized via repr fallback for non-JSON-friendly types.
    session_date defaults to current local date (regime-history convention).
    """
    try:
        payload_str = (
            json.dumps(payload, default=str) if payload is not None else None
        )
        if session_date is None:
            session_date = datetime.now().date().isoformat()
        conn = _conn()
        try:
            cur = conn.execute(
                "INSERT INTO events (source, event_type, symbol, payload, "
                "                    session_date) "
                "VALUES (?,?,?,?,?)",
                (source, event_type, symbol, payload_str, session_date),
            )
            conn.commit()
            return int(cur.lastrowid)
        finally:
            conn.close()
    except Exception as e:
        console.log(
            f"[yellow][EVENTS-BUS-WARN] emit_event source={source} "
            f"type={event_type} sym={symbol}: {type(e).__name__}: {e!r}"
        )
        return None


def emit_signal_v2(
    *,
    source: str,
    signal_type: str,
    symbol: str,
    direction: str | None = None,
    confidence: float | None = None,
    regime_fit: float | None = None,
    timeframe: str | None = None,
    strategy_tag: str | None = None,
    event_id: int | None = None,
    agent_debate_id: int | None = None,
    prompt_version: str | None = None,
    metadata: dict | None = None,
    stale_after: str | None = None,
) -> int | None:
    """Write a normalized signal row to signals_v2. Returns signal_v2_id.

    stale_after: ISO timestamp. If None, computed from timeframe via
    _STALE_BUDGET_S. Pass an explicit value to override (e.g. for delayed
    swing signals that should expire faster).
    """
    try:
        if stale_after is None:
            stale_after = _compute_stale_after(timeframe)
        metadata_str = (
            json.dumps(metadata, default=str) if metadata is not None else None
        )
        conn = _conn()
        try:
            cur = conn.execute(
                "INSERT INTO signals_v2 "
                "(source, signal_type, symbol, direction, confidence, "
                " regime_fit, timeframe, strategy_tag, event_id, "
                " agent_debate_id, prompt_version, metadata, "
                " stale_after) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    source, signal_type, symbol, direction, confidence,
                    regime_fit, timeframe, strategy_tag, event_id,
                    agent_debate_id, prompt_version, metadata_str,
                    stale_after,
                ),
            )
            conn.commit()
            return int(cur.lastrowid)
        finally:
            conn.close()
    except Exception as e:
        console.log(
            f"[yellow][EVENTS-BUS-WARN] emit_signal_v2 source={source} "
            f"sym={symbol} type={signal_type}: {type(e).__name__}: {e!r}"
        )
        return None


def mark_signal_executed(
    *, signal_id: int | None, trade_id: int | None
) -> bool:
    """UPDATE signals_v2 status='executed' + trade_id when a fill lands.

    Called from paper_trader.buy() / sell() after a successful INSERT INTO
    trades. signal_id may be None (legacy / unattributed trades) — no-op
    in that case.
    """
    if signal_id is None:
        return False
    try:
        conn = _conn()
        try:
            # HM-DASHBOARD-SELFCLOSE-METRIC: read created_at first so we can
            # classify dispatch latency after the UPDATE lands.
            _row = conn.execute(
                "SELECT created_at FROM signals_v2 WHERE id=?",
                (int(signal_id),),
            ).fetchone()
            conn.execute(
                "UPDATE signals_v2 SET status='executed', trade_id=? "
                " WHERE id=?",
                (trade_id, int(signal_id)),
            )
            conn.commit()
            # HM-DASHBOARD-SELFCLOSE-METRIC: <50ms = same-buy self-close
            # (paper_trader.buy() audit path); >=50ms = events-bus consumer
            # round-trip. Best-effort, never blocks the UPDATE.
            if _row and _row[0]:
                try:
                    _created = datetime.fromisoformat(
                        str(_row[0]).replace("Z", "+00:00")
                    ).replace(tzinfo=None)
                    _elapsed_ms = (datetime.utcnow() - _created).total_seconds() * 1000
                    _tag = "[SELF-CLOSE]" if _elapsed_ms < 50 else "[CONSUMER-DISPATCH]"
                    console.log(
                        f"{_tag} sig={signal_id} trade={trade_id} "
                        f"elapsed_ms={_elapsed_ms:.1f}"
                    )
                except Exception:
                    pass  # metric is best-effort, never block fill
            return True
        finally:
            conn.close()
    except Exception as e:
        console.log(
            f"[yellow][EVENTS-BUS-WARN] mark_signal_executed "
            f"sig={signal_id} trade={trade_id}: {type(e).__name__}: {e!r}"
        )
        return False


def mark_signal_expired(*, signal_id: int) -> bool:
    """UPDATE signals_v2 status='expired' for a stale rejection."""
    try:
        conn = _conn()
        try:
            conn.execute(
                "UPDATE signals_v2 SET status='expired' WHERE id=?",
                (int(signal_id),),
            )
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception as e:
        console.log(
            f"[yellow][EVENTS-BUS-WARN] mark_signal_expired "
            f"sig={signal_id}: {type(e).__name__}: {e!r}"
        )
        return False


def mark_signal_linked_to_debate(
    *, signal_id: int, debate_id: int
) -> bool:
    """Backfill signals_v2.agent_debate_id when a debate completes."""
    try:
        conn = _conn()
        try:
            conn.execute(
                "UPDATE signals_v2 SET agent_debate_id=? WHERE id=?",
                (int(debate_id), int(signal_id)),
            )
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception as e:
        console.log(
            f"[yellow][EVENTS-BUS-WARN] mark_signal_linked_to_debate "
            f"sig={signal_id} debate={debate_id}: "
            f"{type(e).__name__}: {e!r}"
        )
        return False


def get_signal_v2(signal_id: int) -> dict | None:
    """Read a signals_v2 row by id. Returns dict or None on miss/error."""
    try:
        conn = _conn()
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM signals_v2 WHERE id=?", (int(signal_id),),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
    except Exception:
        return None


def is_signal_stale(signal_id: int) -> tuple[bool, str | None]:
    """Check signals_v2.stale_after against current UTC time.

    Returns (stale: bool, age_summary: str|None). True means the signal
    has aged past its stale_after timestamp and should be rejected.
    age_summary is a human-readable "Ns past" / "Ns left" string for
    logging.
    """
    row = get_signal_v2(signal_id)
    if not row:
        return (False, None)
    sa = row.get("stale_after")
    if not sa:
        return (False, "no_budget")
    try:
        # Tolerate trailing Z (UTC ISO 8601)
        sa_clean = sa.rstrip("Z")
        sa_dt = datetime.fromisoformat(sa_clean)
        now = datetime.utcnow()
        delta_s = (now - sa_dt).total_seconds()
        if delta_s > 0:
            return (True, f"{delta_s:.1f}s past")
        return (False, f"{-delta_s:.1f}s left")
    except Exception:
        return (False, "parse_error")
