"""Stage 2 — Confluence engine.

Reads pending signals from signals_v2, groups by (symbol, direction), applies
the Stage-0 winning-signal filter, and returns a ranked actionable queue.

Signals below the winning bar are classified NO_TRADE and excluded.
This module is read-only w.r.t. the DB — it never mutates signal status.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field

from engine.winning_signal import VALID_GRADES, SELF_CERTIFYING_SOURCES, MIN_BUS_SOURCES, is_winning

_DB = "data/trader.db"


@dataclass
class ConfluenceEntry:
    symbol: str
    direction: str              # BULLISH | BEARISH
    source_count: int           # distinct sources agreeing on this pair
    avg_confidence: float
    signal_ids: list[int]       # signals_v2 IDs contributing to this entry
    sources: list[str]          # deduplicated source names
    grade: str | None           # best grade across contributing signals
    is_winning: bool
    primary_source: str         # highest-trust source in the group
    timeframe: str | None


def get_actionable_queue(
    min_confidence: float = 0.65,
    limit: int = 20,
    symbol: str | None = None,
) -> list[ConfluenceEntry]:
    """Return ranked list of winning-signal entries from signals_v2.

    Only considers signals where status='pending' and not yet stale.
    Results are sorted by source_count DESC, avg_confidence DESC.
    """
    conn = sqlite3.connect(_DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        where_parts = [
            "status = 'pending'",
            "datetime('now') < stale_after",
            "confidence >= ?",
            "direction IS NOT NULL",
            "direction != 'NEUTRAL'",
        ]
        params: list = [min_confidence]
        if symbol:
            where_parts.append("symbol = ?")
            params.append(symbol)

        rows = conn.execute(
            f"""
            SELECT symbol, direction,
                   COUNT(DISTINCT source)               AS source_count,
                   COUNT(*)                             AS signal_count,
                   AVG(confidence)                      AS avg_conf,
                   GROUP_CONCAT(id)                     AS ids,
                   GROUP_CONCAT(DISTINCT source)        AS sources,
                   MAX(confidence)                      AS max_conf,
                   MAX(timeframe)                       AS timeframe,
                   MAX(metadata)                        AS sample_meta
              FROM signals_v2
             WHERE {' AND '.join(where_parts)}
             GROUP BY symbol, direction
             ORDER BY source_count DESC, avg_conf DESC
             LIMIT ?
            """,
            params + [limit],
        ).fetchall()

        conn.execute(
            f"""
            SELECT id, source, symbol, direction, confidence, metadata
              FROM signals_v2
             WHERE status = 'pending'
               AND datetime('now') < stale_after
               AND confidence >= ?
               AND direction IS NOT NULL
               AND direction != 'NEUTRAL'
            """,
            [min_confidence],
        )  # keep conn alive for the per-row meta read below
    except Exception:
        conn.close()
        return []

    try:
        # Build detailed per-(symbol, direction) index for grade extraction
        detail_rows = conn.execute(
            f"""
            SELECT id, source, symbol, direction, confidence, metadata
              FROM signals_v2
             WHERE status = 'pending'
               AND datetime('now') < stale_after
               AND confidence >= ?
               AND direction IS NOT NULL
               AND direction != 'NEUTRAL'
            """,
            [min_confidence],
        ).fetchall()
    except Exception:
        detail_rows = []
    finally:
        conn.close()

    # Build per-(symbol, direction) grade map from metadata
    grade_map: dict[tuple[str, str], str | None] = {}
    id_map: dict[tuple[str, str], list[int]] = {}
    src_map: dict[tuple[str, str], list[str]] = {}
    for dr in detail_rows:
        key = (dr["symbol"], dr["direction"])
        grade = _extract_grade(dr["metadata"])
        if grade in VALID_GRADES:
            grade_map[key] = grade
        elif key not in grade_map:
            grade_map[key] = None
        id_map.setdefault(key, []).append(dr["id"])
        src_map.setdefault(key, []).append(dr["source"])

    results: list[ConfluenceEntry] = []
    for r in rows:
        key = (r["symbol"], r["direction"])
        sources_raw = [s.strip() for s in (r["sources"] or "").split(",") if s.strip()]
        sources_dedup = list(dict.fromkeys(sources_raw))
        grade = grade_map.get(key)
        primary = _primary_source(sources_dedup)
        winning = is_winning(primary, r["source_count"], grade)
        entry = ConfluenceEntry(
            symbol=r["symbol"],
            direction=r["direction"],
            source_count=r["source_count"],
            avg_confidence=round(float(r["avg_conf"] or 0), 4),
            signal_ids=id_map.get(key, []),
            sources=sources_dedup,
            grade=grade,
            is_winning=winning,
            primary_source=primary,
            timeframe=r["timeframe"],
        )
        results.append(entry)

    return [e for e in results if e.is_winning]


def _extract_grade(metadata_json: str | None) -> str | None:
    """Parse grade from signals_v2.metadata JSON field. Returns None on error."""
    if not metadata_json:
        return None
    try:
        meta = json.loads(metadata_json)
        return meta.get("grade")
    except Exception:
        return None


def _primary_source(sources: list[str]) -> str:
    """Return the highest-trust source from a list (UHURA > others)."""
    for s in sources:
        if s in SELF_CERTIFYING_SOURCES:
            return s
    return sources[0] if sources else "unknown"
