"""
USS TradeMinds — HM-FINMEM Writer Module (engine/finmem_writers.py)

Event-trigger writers for the agent_memory table. Extends the existing
engine/finmem_memory.py reader (which produces the layered memory text
block injected into brain_context prompts) with the WRITE side.

Three event triggers (all defensive — failures never raise to caller):
- on_sell_close()      — SHORT_TERM memory on every closed trade
- on_regime_change()   — MID_TERM memory on regime shift
- on_earnings_outcome()— MID_TERM memory on earnings beat/miss for held names

Two scheduled jobs:
- decay_daily()        — multiply score by decay_rate, prune score < 0.05
- promote_weekly()     — SHORT→MID, MID→LONG based on cumulative score

Pilot scope per Captain spec (HM-FINMEM 2026-05-23):
- McCoy (ollama-plutus)
- Worf  (qwen3-8b-flash)
- Grok  (cto-grok42)
- Ollie (ollie-auto)

The pilot list is enforced by `_PILOT_AGENTS` — non-pilot agents trigger
no-op writes so the broader fleet keeps the prior behavior. Captain can
widen scope by adding to the set; no other code change needed.

Schema (agent_memory after HM-FINMEM 2026-05-23 ALTER TABLE):
    id, player_id, memory_layer, summary, score, created_at,
    decay_rate, tags

memory_layer enum extended:
    LESSON | WORKING | SHORT_TERM | MID_TERM | LONG_TERM
    (MID_TERM is new in this commit per Captain's spec; existing
    LESSON / SHORT_TERM / LONG_TERM rows untouched.)

decay_rate defaults per layer (Captain spec):
    SHORT_TERM: 0.85/day   (relevant 24-48h, half-life ~4 days)
    MID_TERM  : 0.95/day   (relevant 30 days, half-life ~14 days)
    LONG_TERM : 0.99/day   (relevant 90+ days, half-life ~70 days)

tags column: comma-separated lowercase strings for filterable queries
    e.g. "regime:bull_cross,strategy:csp,symbol:aapl"
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Iterable, Optional

logger = logging.getLogger(__name__)

_DB_PATH = "data/trader.db"

# Pilot agents per HM-FINMEM 2026-05-23 Captain spec. Non-pilot agents
# bypass the writers (no-op) so the broader fleet stays on the prior
# read-only memory behavior. Widen by adding player_ids here.
_PILOT_AGENTS: set[str] = {
    "ollama-plutus",     # McCoy
    "qwen3-8b-flash",    # Worf (canonical, post-rename of Gemini/Gemma shadows)
    "cto-grok42",        # Grok
    "ollie-auto",        # Ollie
}

# Initial relevance score on insert (0-1 scale)
_INITIAL_SCORE = 1.0

# Per-layer decay rate (multiply score by this each day)
_DECAY_RATES = {
    "SHORT_TERM": 0.85,
    "MID_TERM":   0.95,
    "LONG_TERM":  0.99,
}

# Prune threshold — rows with score below this are deleted in decay_daily()
_PRUNE_FLOOR = 0.05

# Promotion thresholds (cumulative score / sample size during the
# weekly look-back window) for promote_weekly()
_PROMOTE_SHORT_TO_MID  = 3.0   # SHORT_TERM rows for a player+symbol whose
                                # summed score in the week exceeds this
                                # → consolidate into a MID_TERM summary
_PROMOTE_MID_TO_LONG   = 8.0   # same logic, MID_TERM → LONG_TERM


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB_PATH, check_same_thread=False, timeout=10)
    c.row_factory = sqlite3.Row
    return c


def _bust_finmem_cache(player_id: str) -> None:
    """Invalidate finmem_memory.py's 5-min cache for this player so the
    next read picks up the new memory immediately. Fail-safe: cache module
    not importable → silently ignore.
    """
    try:
        from engine.finmem_memory import _cache, _cache_lock
        with _cache_lock:
            _cache.pop(player_id, None)
    except Exception:
        pass


def _is_pilot(player_id: str) -> bool:
    return player_id in _PILOT_AGENTS


def _write(
    *,
    player_id: str,
    memory_layer: str,
    summary: str,
    tags: Optional[Iterable[str]] = None,
    score: float = _INITIAL_SCORE,
) -> Optional[int]:
    """Defensive insert. Returns rowid on success, None on error or
    non-pilot. Caller (event-trigger function) should always-allow
    failure: a memory write error never affects production trading.
    """
    if not _is_pilot(player_id):
        return None
    if not summary:
        return None
    layer = memory_layer.upper()
    decay = _DECAY_RATES.get(layer)
    tags_str = ",".join(sorted(set(t.lower().strip() for t in (tags or []) if t))) or None
    try:
        c = _conn()
        try:
            cur = c.execute(
                "INSERT INTO agent_memory "
                "(player_id, memory_layer, summary, score, decay_rate, tags) "
                "VALUES (?,?,?,?,?,?)",
                (player_id, layer, summary[:500], float(score), decay, tags_str),
            )
            c.commit()
            rowid = cur.lastrowid
        finally:
            c.close()
        _bust_finmem_cache(player_id)
        return rowid
    except Exception as e:
        logger.debug(
            "[HM-FINMEM-WRITER] write failed player=%s layer=%s: %s: %r",
            player_id, layer, type(e).__name__, e,
        )
        return None


# ── Event triggers ──────────────────────────────────────────────────────────


def on_sell_close(
    *,
    player_id: str,
    symbol: str,
    entry_price: Optional[float],
    exit_price: Optional[float],
    realized_pnl: Optional[float],
    reasoning: Optional[str] = None,
) -> Optional[int]:
    """SHORT_TERM memory on every closed trade. Caller is paper_trader.sell()
    after the trade is persisted. Defensive — any error returns None.
    """
    try:
        ep = float(entry_price) if entry_price is not None else None
        xp = float(exit_price) if exit_price is not None else None
        pnl = float(realized_pnl) if realized_pnl is not None else None
        parts = [f"{symbol}"]
        if ep is not None and xp is not None:
            parts.append(f"entry ${ep:.2f} → exit ${xp:.2f}")
        if pnl is not None:
            sign = "+" if pnl >= 0 else "−"
            parts.append(f"P&L {sign}${abs(pnl):.2f}")
        # Reasoning excerpt (trim to keep summary readable)
        if reasoning:
            short_reason = reasoning.replace("\n", " ").strip()[:120]
            if short_reason:
                parts.append(f"reason: {short_reason}")
        summary = " · ".join(parts)
        tags = [f"symbol:{symbol.lower()}"]
        if pnl is not None:
            tags.append("win" if pnl >= 0 else "loss")
        return _write(
            player_id=player_id, memory_layer="SHORT_TERM",
            summary=summary, tags=tags,
        )
    except Exception as e:
        logger.debug("[HM-FINMEM-WRITER] on_sell_close: %s: %r", type(e).__name__, e)
        return None


def on_regime_change(
    *,
    prev_regime: Optional[str],
    new_regime: str,
    vix: Optional[float] = None,
    spy_price: Optional[float] = None,
) -> int:
    """MID_TERM memory for ALL pilot agents on regime shift. Returns
    count of pilots successfully written (0..len(_PILOT_AGENTS)).
    Caller is the regime emitter (regime_router / scheduler).
    """
    if not new_regime or new_regime == prev_regime:
        return 0
    parts = [f"Regime shift {prev_regime or 'UNKNOWN'} → {new_regime}"]
    if vix is not None:
        parts.append(f"VIX {vix:.1f}")
    if spy_price is not None:
        parts.append(f"SPY ${spy_price:.2f}")
    summary = " · ".join(parts)
    tags = [f"regime:{new_regime.lower()}", "event:regime_change"]
    written = 0
    for pid in _PILOT_AGENTS:
        if _write(player_id=pid, memory_layer="MID_TERM",
                  summary=summary, tags=tags) is not None:
            written += 1
    return written


def on_earnings_outcome(
    *,
    symbol: str,
    outcome: str,                # 'beat' | 'miss' | 'inline'
    surprise_pct: Optional[float] = None,
    held_by: Optional[Iterable[str]] = None,
) -> int:
    """MID_TERM memory for any pilot holding the symbol when earnings
    print drops. `held_by` should be the list of player_ids actually
    holding the symbol; falls back to ALL pilots if None (caller's
    choice — passing a filtered list is preferred so non-holding
    pilots don't accumulate irrelevant noise).

    Returns count of pilots successfully written.
    """
    if not symbol or not outcome:
        return 0
    parts = [f"{symbol} earnings {outcome.upper()}"]
    if surprise_pct is not None:
        sign = "+" if surprise_pct >= 0 else "−"
        parts.append(f"surprise {sign}{abs(surprise_pct):.1f}%")
    summary = " · ".join(parts)
    tags = [f"symbol:{symbol.lower()}", "event:earnings",
            f"outcome:{outcome.lower()}"]
    target_pids = list(held_by) if held_by else list(_PILOT_AGENTS)
    written = 0
    for pid in target_pids:
        if pid in _PILOT_AGENTS:
            if _write(player_id=pid, memory_layer="MID_TERM",
                      summary=summary, tags=tags) is not None:
                written += 1
    return written


# ── Scheduled jobs ──────────────────────────────────────────────────────────


def decay_daily() -> dict:
    """Daily decay scorer — multiply score by decay_rate (per-layer), then
    prune rows with score < _PRUNE_FLOOR. Returns
    {decayed: N, pruned: N, remaining: N} for telemetry.

    Idempotent within a day: calling twice double-decays. Cron should
    fire once per day at 00:00 AZ.
    """
    out = {"decayed": 0, "pruned": 0, "remaining": 0}
    try:
        c = _conn()
        try:
            # Decay rows that have a non-null decay_rate (only HM-FINMEM
            # rows). Legacy rows without decay_rate stay frozen.
            cur = c.execute(
                "UPDATE agent_memory "
                "   SET score = COALESCE(score, 0) * COALESCE(decay_rate, 1.0) "
                " WHERE decay_rate IS NOT NULL"
            )
            out["decayed"] = cur.rowcount
            # Prune rows below the floor (same scope — only HM-FINMEM rows).
            cur = c.execute(
                "DELETE FROM agent_memory "
                " WHERE decay_rate IS NOT NULL "
                "   AND score < ?",
                (_PRUNE_FLOOR,),
            )
            out["pruned"] = cur.rowcount
            c.commit()
            out["remaining"] = c.execute(
                "SELECT COUNT(*) FROM agent_memory WHERE decay_rate IS NOT NULL"
            ).fetchone()[0]
        finally:
            c.close()
        # Bust cache for all pilots since their memory shape may have
        # changed (entries pruned or scores re-ranked).
        for pid in _PILOT_AGENTS:
            _bust_finmem_cache(pid)
    except Exception as e:
        logger.warning(
            "[HM-FINMEM-DECAY] failed: %s: %r", type(e).__name__, e,
        )
    return out


def promote_weekly() -> dict:
    """Sunday promotion job — consolidate frequently-referenced
    SHORT_TERM rows into MID_TERM summaries, and frequently-referenced
    MID_TERM rows into LONG_TERM summaries. Groups by (player_id,
    primary symbol tag) over the last 7 days; if cumulative score
    exceeds the threshold, writes a consolidated summary in the next
    layer up with `event:promoted` tag.

    Does NOT delete the source rows — the decay scorer will prune
    them naturally as their scores fall.

    Returns {short_to_mid: N, mid_to_long: N} for telemetry. Cron
    should fire once per week on Sunday 00:30 AZ (offset 30min after
    daily decay to avoid race).
    """
    out = {"short_to_mid": 0, "mid_to_long": 0}
    try:
        c = _conn()
        try:
            # SHORT→MID: group SHORT_TERM rows from last 7d by
            # (player_id, first symbol tag) and consolidate where
            # cum_score > threshold.
            short_groups = c.execute(
                "SELECT player_id, tags, COUNT(*) AS n, "
                "       ROUND(SUM(score), 2) AS cum_score "
                "  FROM agent_memory "
                " WHERE memory_layer = 'SHORT_TERM' "
                "   AND decay_rate IS NOT NULL "
                "   AND created_at >= datetime('now', '-7 days') "
                "   AND tags LIKE 'symbol:%' "
                " GROUP BY player_id, tags "
                "HAVING cum_score > ?",
                (_PROMOTE_SHORT_TO_MID,),
            ).fetchall()
            for row in short_groups:
                pid = row["player_id"]
                if not _is_pilot(pid):
                    continue
                summary = (
                    f"Promoted from SHORT: {row['n']} entries in 7d "
                    f"(cum score {row['cum_score']:.2f}) tagged {row['tags']}"
                )
                _write(player_id=pid, memory_layer="MID_TERM",
                       summary=summary,
                       tags=[row["tags"], "event:promoted"])
                out["short_to_mid"] += 1

            # MID→LONG: same pattern, looser threshold.
            mid_groups = c.execute(
                "SELECT player_id, tags, COUNT(*) AS n, "
                "       ROUND(SUM(score), 2) AS cum_score "
                "  FROM agent_memory "
                " WHERE memory_layer = 'MID_TERM' "
                "   AND decay_rate IS NOT NULL "
                "   AND created_at >= datetime('now', '-30 days') "
                "   AND tags IS NOT NULL "
                " GROUP BY player_id, tags "
                "HAVING cum_score > ?",
                (_PROMOTE_MID_TO_LONG,),
            ).fetchall()
            for row in mid_groups:
                pid = row["player_id"]
                if not _is_pilot(pid):
                    continue
                summary = (
                    f"Promoted from MID: {row['n']} entries in 30d "
                    f"(cum score {row['cum_score']:.2f}) tagged {row['tags']}"
                )
                _write(player_id=pid, memory_layer="LONG_TERM",
                       summary=summary,
                       tags=[row["tags"], "event:promoted"])
                out["mid_to_long"] += 1
        finally:
            c.close()
    except Exception as e:
        logger.warning(
            "[HM-FINMEM-PROMOTE] failed: %s: %r", type(e).__name__, e,
        )
    return out
