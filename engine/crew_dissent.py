"""HM-CREW-DISSENT — crew-dissent eval pipeline.

When the bridge officers (Spock / Data / Uhura) disagree on a ticker, log the
dissenter(s), then later resolve whether the dissenter was RIGHT against the
realized 5-day outcome (signals.db scored_predictions). Builds a per-officer
contrarian track record.

ADVISORY ONLY — this is a measurement/reporting pipeline. No order path, never
executes a trade. Tables live in data/trader.db; outcomes read from
signal-center/signals.db (read-only ATTACH).

Source: build_consensus() (engine/consensus.py) per-ticker spock/data/uhura
stances. Wired in engine/riker_xo.py right after build_consensus() is computed.
No bare except:pass — every failure is logged with type+repr.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Optional

logger = logging.getLogger("crew_dissent")

_ROOT = Path(__file__).resolve().parent.parent
TRADER_DB = _ROOT / "data" / "trader.db"
SIGNALS_DB = _ROOT / "signal-center" / "signals.db"

HORIZON_DAYS = 5  # resolution horizon (matches the W0 forward-scoring window)

# Officer raw action (from build_consensus stances) -> directional call.
_ACTION_TO_CALL = {
    "BUY": "BULL", "ADD": "BULL",
    "SELL": "BEAR", "CLOSE": "BEAR", "TRIM": "BEAR",
    "HOLD": "HOLD",
}

# Officer display names, in the order build_consensus exposes them per ticker.
_OFFICERS = ("Spock", "Data", "Uhura")


def _today_str() -> str:
    """Current AZ date 'YYYY-MM-DD' (matches DATE(entry_date) in scored_predictions)."""
    try:
        from engine.market_calendar import az_now
        return az_now().strftime("%Y-%m-%d")
    except Exception as e:
        logger.warning("[crew_dissent] az_now unavailable (%s: %r) — falling back to UTC date",
                       type(e).__name__, e)
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _ensure_tables() -> None:
    conn = sqlite3.connect(str(TRADER_DB), timeout=15)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS crew_dissent_log (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol            TEXT    NOT NULL,
                dissent_date      TEXT    NOT NULL,
                dissenter         TEXT    NOT NULL,
                dissenter_call    TEXT    NOT NULL,          -- BULL/BEAR/HOLD
                consensus_call    TEXT,
                consensus_size    INTEGER,
                total_voters      INTEGER,
                dissent_magnitude TEXT,                      -- MINOR/SPLIT/OUTLIER
                outcome_r         REAL,
                dissenter_correct INTEGER,                   -- 1 / 0 / NULL (unresolved)
                resolved_at       TEXT,
                created_at        TEXT DEFAULT (datetime('now')),
                UNIQUE(symbol, dissent_date, dissenter)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS crew_dissent_stats (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                dissenter          TEXT    NOT NULL,
                window_days        INTEGER NOT NULL,         -- 30 / 90
                dissent_count      INTEGER,
                correct_count      INTEGER,
                accuracy           REAL,
                avg_r_when_correct REAL,
                avg_r_when_wrong   REAL,
                last_computed      TEXT,
                UNIQUE(dissenter, window_days)
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def _magnitude(consensus_size: int, total_voters: int) -> str:
    """MINOR = exactly 1 dissenter; SPLIT = even split (no true majority);
    OUTLIER = consensus is a lone plurality (everyone scattered, size==1)."""
    n_dissent = total_voters - consensus_size
    if total_voters >= 2 and consensus_size * 2 == total_voters:
        return "SPLIT"
    if n_dissent == 1:
        return "MINOR"
    if consensus_size == 1:
        return "OUTLIER"
    return "SPLIT"


def log_dissents(consensus_result: dict) -> dict:
    """For each ticker, map the 3 officers' stances to BULL/BEAR/HOLD, find the
    majority, and INSERT OR IGNORE a dissent row for each officer that disagrees.

    Returns {tickers_examined, dissents_logged, skipped_unanimous, skipped_thin}.
    """
    stats = {"tickers_examined": 0, "dissents_logged": 0,
             "skipped_unanimous": 0, "skipped_thin": 0}
    if not isinstance(consensus_result, dict):
        logger.warning("[crew_dissent] log_dissents got non-dict (%s) — skip", type(consensus_result).__name__)
        return stats

    tickers = consensus_result.get("tickers") or {}
    if not tickers:
        return stats

    try:
        _ensure_tables()
    except Exception as e:
        logger.warning("[crew_dissent] ensure_tables failed: %s: %r", type(e).__name__, e)
        return stats

    dissent_date = _today_str()
    conn = sqlite3.connect(str(TRADER_DB), timeout=15)
    try:
        for symbol, data in tickers.items():
            if not isinstance(data, dict):
                continue
            stats["tickers_examined"] += 1

            # Map each present officer's action to a directional call.
            calls: dict[str, str] = {}
            for officer, key in zip(_OFFICERS, ("spock", "data", "uhura")):
                stance = data.get(key)
                if not stance:
                    continue
                act = (stance.get("action") or "").upper()
                call = _ACTION_TO_CALL.get(act)
                if call:
                    calls[officer] = call

            total_voters = len(calls)
            if total_voters < 2:
                stats["skipped_thin"] += 1
                continue

            # Majority call.
            counts: dict[str, int] = {}
            for c in calls.values():
                counts[c] = counts.get(c, 0) + 1
            consensus_call = max(counts, key=counts.get)
            consensus_size = counts[consensus_call]

            if consensus_size == total_voters:
                stats["skipped_unanimous"] += 1
                continue

            magnitude = _magnitude(consensus_size, total_voters)
            sym = (symbol or "").strip().upper()
            if not sym:
                continue

            for officer, call in calls.items():
                if call == consensus_call:
                    continue
                try:
                    cur = conn.execute(
                        """INSERT OR IGNORE INTO crew_dissent_log
                           (symbol, dissent_date, dissenter, dissenter_call,
                            consensus_call, consensus_size, total_voters, dissent_magnitude)
                           VALUES (?,?,?,?,?,?,?,?)""",
                        (sym, dissent_date, officer, call,
                         consensus_call, consensus_size, total_voters, magnitude),
                    )
                    if cur.rowcount:
                        stats["dissents_logged"] += 1
                except Exception as e:
                    logger.warning("[crew_dissent] insert failed %s/%s/%s: %s: %r",
                                   sym, dissent_date, officer, type(e).__name__, e)
        conn.commit()
    finally:
        conn.close()
    return stats


def resolve_dissent_outcomes() -> dict:
    """Resolve unresolved dissents against realized 5-day outcomes in signals.db.

    Reads signal-center/signals.db (ATTACH), writes data/trader.db. A dissent is
    resolved only when a matching scored_predictions row (symbol + same date +
    horizon=5) is CLOSED with a non-null r_multiple. dissenter_correct = 1 when
    (BULL & r>0) or (BEAR & r<0), else 0.

    Returns {checked, resolved, still_pending}.
    """
    out = {"checked": 0, "resolved": 0, "still_pending": 0}
    try:
        _ensure_tables()
    except Exception as e:
        logger.warning("[crew_dissent] ensure_tables failed: %s: %r", type(e).__name__, e)
        return out

    if not SIGNALS_DB.exists():
        logger.warning("[crew_dissent] signals.db not found at %s — cannot resolve", SIGNALS_DB)
        return out

    conn = sqlite3.connect(str(TRADER_DB), timeout=20)
    try:
        try:
            conn.execute("ATTACH DATABASE ? AS sig", (str(SIGNALS_DB),))
        except Exception as e:
            logger.warning("[crew_dissent] ATTACH signals.db failed: %s: %r", type(e).__name__, e)
            return out

        unresolved = conn.execute(
            "SELECT id, symbol, dissent_date, dissenter_call FROM crew_dissent_log "
            "WHERE dissenter_correct IS NULL"
        ).fetchall()
        out["checked"] = len(unresolved)

        for row_id, symbol, dissent_date, call in unresolved:
            try:
                match = conn.execute(
                    """SELECT r_multiple FROM sig.scored_predictions
                       WHERE symbol = ? AND DATE(entry_date) = ?
                         AND horizon_days = ? AND closed = 1 AND r_multiple IS NOT NULL
                       ORDER BY scored_at DESC LIMIT 1""",
                    (symbol, dissent_date, HORIZON_DAYS),
                ).fetchone()
            except Exception as e:
                logger.warning("[crew_dissent] resolve query failed id=%s: %s: %r",
                               row_id, type(e).__name__, e)
                continue

            if not match or match[0] is None:
                out["still_pending"] += 1
                continue

            r = float(match[0])
            if call == "BULL":
                correct = 1 if r > 0 else 0
            elif call == "BEAR":
                correct = 1 if r < 0 else 0
            else:  # HOLD — undefined under the directional rule; mark not-correct
                correct = 0
            try:
                conn.execute(
                    "UPDATE crew_dissent_log SET outcome_r=?, dissenter_correct=?, "
                    "resolved_at=datetime('now') WHERE id=?",
                    (r, correct, row_id),
                )
                out["resolved"] += 1
            except Exception as e:
                logger.warning("[crew_dissent] resolve update failed id=%s: %s: %r",
                               row_id, type(e).__name__, e)
        conn.commit()
    finally:
        try:
            conn.execute("DETACH DATABASE sig")
        except Exception:
            pass
        conn.close()
    return out


def recompute_dissent_stats() -> dict:
    """Recompute per-dissenter accuracy for the 30d and 90d windows; UPSERT into
    crew_dissent_stats. Returns {rows_upserted}."""
    res = {"rows_upserted": 0}
    try:
        _ensure_tables()
    except Exception as e:
        logger.warning("[crew_dissent] ensure_tables failed: %s: %r", type(e).__name__, e)
        return res

    conn = sqlite3.connect(str(TRADER_DB), timeout=15)
    try:
        dissenters = [r[0] for r in conn.execute(
            "SELECT DISTINCT dissenter FROM crew_dissent_log"
        ).fetchall()]

        for dissenter in dissenters:
            for window in (30, 90):
                rows = conn.execute(
                    """SELECT dissenter_correct, outcome_r FROM crew_dissent_log
                       WHERE dissenter = ? AND dissenter_correct IS NOT NULL
                         AND dissent_date >= date('now', ?)""",
                    (dissenter, f"-{window} days"),
                ).fetchall()
                dissent_count = len(rows)
                correct_count = sum(1 for c, _ in rows if c == 1)
                accuracy = round(correct_count / dissent_count, 4) if dissent_count else None
                correct_rs = [r for c, r in rows if c == 1 and r is not None]
                wrong_rs = [r for c, r in rows if c == 0 and r is not None]
                avg_correct = round(sum(correct_rs) / len(correct_rs), 4) if correct_rs else None
                avg_wrong = round(sum(wrong_rs) / len(wrong_rs), 4) if wrong_rs else None
                try:
                    conn.execute(
                        """INSERT INTO crew_dissent_stats
                           (dissenter, window_days, dissent_count, correct_count, accuracy,
                            avg_r_when_correct, avg_r_when_wrong, last_computed)
                           VALUES (?,?,?,?,?,?,?,datetime('now'))
                           ON CONFLICT(dissenter, window_days) DO UPDATE SET
                             dissent_count=excluded.dissent_count,
                             correct_count=excluded.correct_count,
                             accuracy=excluded.accuracy,
                             avg_r_when_correct=excluded.avg_r_when_correct,
                             avg_r_when_wrong=excluded.avg_r_when_wrong,
                             last_computed=excluded.last_computed""",
                        (dissenter, window, dissent_count, correct_count, accuracy,
                         avg_correct, avg_wrong),
                    )
                    res["rows_upserted"] += 1
                except Exception as e:
                    logger.warning("[crew_dissent] stats upsert failed %s/%dd: %s: %r",
                                   dissenter, window, type(e).__name__, e)
        conn.commit()
    finally:
        conn.close()
    return res
