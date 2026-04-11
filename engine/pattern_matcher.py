"""
Mr. Spock's Historical Analysis — Session Pattern Matcher
----------------------------------------------------------
Captures a daily session "fingerprint" from the morning briefing,
compares to historical fingerprints, and returns the top-3 most similar
past sessions with outcome summaries.

Gracefully returns "Not enough data yet" until 10+ trading days are
stored.

Table: session_fingerprints  (SACRED — never dropped/truncated)
Endpoint: GET /api/ready-room/similar-days
"""
from __future__ import annotations

import os
import json
import sqlite3
from datetime import date, datetime, timezone
from typing import Any

_TRADEMINDS_DB = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)
_ALERT_DB = os.environ.get("TRADER_DB", "autonomous_trader.db")

MIN_DAYS_FOR_MATCHING = 10


def _init_db() -> None:
    conn = sqlite3.connect(_TRADEMINDS_DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS session_fingerprints (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date      TEXT NOT NULL UNIQUE,
            session_type    TEXT,
            vix_regime      TEXT,
            pc_ratio_bucket TEXT,
            gex_polarity    TEXT,
            skew_direction  TEXT,
            momentum_open   TEXT,
            vix_state       TEXT,
            spot_price      REAL,
            call_wall       REAL,
            put_wall        REAL,
            gamma_flip      REAL,
            vix             REAL,
            pc_ratio        REAL,
            total_gex_b     REAL,
            outcome_spy_range_pct  REAL,
            outcome_trended        INTEGER,
            outcome_session_actual TEXT,
            outcome_grade          TEXT,
            raw_json        TEXT,
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()


_init_db()


# ── Bucket helpers ────────────────────────────────────────────────────────────
def _pc_bucket(pc: float | None) -> str:
    if pc is None:
        return "UNKNOWN"
    if pc > 1.3:
        return "HIGH_FEAR"
    if pc > 1.0:
        return "MILD_BEAR"
    if pc > 0.8:
        return "NEUTRAL"
    return "BULLISH"


def _gex_polarity(gex_b: float | None) -> str:
    if gex_b is None:
        return "UNKNOWN"
    if gex_b > 1.0:
        return "STRONG_POS"
    if gex_b > 0:
        return "POS"
    if gex_b > -1.0:
        return "NEG"
    return "STRONG_NEG"


def _skew_direction(skew: float | None) -> str:
    if skew is None:
        return "UNKNOWN"
    if skew > 3:
        return "FEAR"
    if skew > 1:
        return "MILD_FEAR"
    if skew < -1:
        return "GREED"
    return "NEUTRAL"


def _momentum_bucket(ts: float | None) -> str:
    if ts is None:
        return "UNKNOWN"
    if ts > 40:
        return "BULLISH"
    if ts > 10:
        return "MILD_BULL"
    if ts < -40:
        return "BEARISH"
    if ts < -10:
        return "MILD_BEAR"
    return "NEUTRAL"


# ── Capture fingerprint ───────────────────────────────────────────────────────
def capture_fingerprint(trade_date: str | None = None) -> dict[str, Any]:
    """
    Capture today's morning fingerprint from the latest briefing.
    Idempotent — won't overwrite an existing fingerprint for the same date.
    """
    today = trade_date or date.today().isoformat()

    # Skip if already captured
    try:
        conn = sqlite3.connect(_TRADEMINDS_DB, timeout=30)
        existing = conn.execute(
            "SELECT id FROM session_fingerprints WHERE trade_date=?", (today,)
        ).fetchone()
        conn.close()
        if existing:
            return {"skipped": True, "date": today}
    except Exception:
        pass

    # Gather data
    briefing: dict = {}
    skew_score = None
    momentum_ts = None
    try:
        from engine.ready_room import get_latest_briefing
        briefing = get_latest_briefing() or {}
    except Exception:
        pass
    try:
        from engine.iv_skew import get_iv_skew
        skew_score = get_iv_skew().get("skew_score")
    except Exception:
        pass
    try:
        from engine.momentum_tracker import get_intraday_momentum
        momentum_ts = get_intraday_momentum().get("trend_score")
    except Exception:
        pass

    if not briefing or not briefing.get("session_type"):
        return {"skipped": True, "reason": "No briefing data yet"}

    fp = {
        "trade_date":     today,
        "session_type":   briefing.get("session_type"),
        "vix_regime":     briefing.get("vix_regime") or "UNKNOWN",
        "pc_ratio_bucket": _pc_bucket(briefing.get("pc_ratio")),
        "gex_polarity":   _gex_polarity(briefing.get("total_gex_b")),
        "skew_direction": _skew_direction(skew_score),
        "momentum_open":  _momentum_bucket(momentum_ts),
        "vix_state":      briefing.get("vix_state") or "UNKNOWN",
        "spot_price":     briefing.get("spot_price"),
        "call_wall":      briefing.get("call_wall"),
        "put_wall":       briefing.get("put_wall"),
        "gamma_flip":     briefing.get("gamma_flip"),
        "vix":            briefing.get("vix"),
        "pc_ratio":       briefing.get("pc_ratio"),
        "total_gex_b":    briefing.get("total_gex_b"),
    }

    try:
        conn = sqlite3.connect(_TRADEMINDS_DB, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            INSERT OR IGNORE INTO session_fingerprints
                (trade_date, session_type, vix_regime, pc_ratio_bucket, gex_polarity,
                 skew_direction, momentum_open, vix_state, spot_price, call_wall,
                 put_wall, gamma_flip, vix, pc_ratio, total_gex_b, raw_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            fp["trade_date"], fp["session_type"], fp["vix_regime"],
            fp["pc_ratio_bucket"], fp["gex_polarity"], fp["skew_direction"],
            fp["momentum_open"], fp["vix_state"], fp["spot_price"],
            fp["call_wall"], fp["put_wall"], fp["gamma_flip"],
            fp["vix"], fp["pc_ratio"], fp["total_gex_b"],
            json.dumps(fp),
        ))
        conn.commit()
        conn.close()
        return {"saved": True, **fp}
    except Exception as exc:
        return {"error": str(exc)}


def backfill_outcomes() -> int:
    """
    For any fingerprints without outcomes, try to fill from forecast_scorecards
    and intraday_snapshots.
    """
    updated = 0
    try:
        conn = sqlite3.connect(_TRADEMINDS_DB, timeout=30)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT f.id, f.trade_date
            FROM session_fingerprints f
            LEFT JOIN forecast_scorecards sc ON f.trade_date = sc.trade_date
            WHERE f.outcome_grade IS NULL
        """).fetchall()

        for row in rows:
            td = row["trade_date"]
            sc_row = conn.execute(
                "SELECT overall_grade, actual_session_type, spy_range_pct FROM forecast_scorecards WHERE trade_date=?",
                (td,)
            ).fetchone()
            if sc_row:
                trended = 1 if (sc_row["spy_range_pct"] or 0) > 0.5 else 0
                conn.execute("""
                    UPDATE session_fingerprints
                    SET outcome_grade=?, outcome_session_actual=?, outcome_spy_range_pct=?, outcome_trended=?
                    WHERE id=?
                """, (sc_row["overall_grade"], sc_row["actual_session_type"],
                      sc_row["spy_range_pct"], trended, row["id"]))
                updated += 1

        conn.commit()
        conn.close()
    except Exception:
        pass
    return updated


# ── Distance scoring ──────────────────────────────────────────────────────────
def _distance(fp_a: dict, fp_b: dict) -> float:
    """Lower = more similar. Max possible = 10."""
    score = 0.0
    if fp_a.get("session_type")    == fp_b.get("session_type"):    score += 3
    if fp_a.get("vix_regime")      == fp_b.get("vix_regime"):      score += 2
    if fp_a.get("gex_polarity")    == fp_b.get("gex_polarity"):    score += 2
    if fp_a.get("skew_direction")  == fp_b.get("skew_direction"):  score += 1
    if fp_a.get("momentum_open")   == fp_b.get("momentum_open"):   score += 1
    # PC ratio within 0.1
    pc_a = fp_a.get("pc_ratio") or 0
    pc_b = fp_b.get("pc_ratio") or 0
    if pc_a and pc_b and abs(pc_a - pc_b) <= 0.1:
        score += 1
    return 10.0 - score   # invert: lower = more similar


# ── Main API ──────────────────────────────────────────────────────────────────
def get_similar_days(limit: int = 3) -> dict[str, Any]:
    """
    Compare today's fingerprint to history.
    Returns top-3 most similar past sessions + outcome summaries.
    """
    today = date.today().isoformat()

    # Get today's fingerprint (capture if missing)
    try:
        conn = sqlite3.connect(_TRADEMINDS_DB, timeout=30)
        conn.row_factory = sqlite3.Row
        today_fp = conn.execute(
            "SELECT * FROM session_fingerprints WHERE trade_date=?", (today,)
        ).fetchone()
        conn.close()
        if today_fp:
            today_fp = dict(today_fp)
        else:
            capture_fingerprint(today)
            conn = sqlite3.connect(_TRADEMINDS_DB, timeout=30)
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM session_fingerprints WHERE trade_date=?", (today,)
            ).fetchone()
            conn.close()
            today_fp = dict(row) if row else None
    except Exception as exc:
        return {"error": str(exc), "similar_days": []}

    if not today_fp or not today_fp.get("session_type"):
        return {
            "similar_days": [],
            "message": "No fingerprint for today yet — will capture at morning briefing.",
            "total_days": 0,
        }

    # Get all historical fingerprints (excluding today)
    try:
        conn = sqlite3.connect(_TRADEMINDS_DB, timeout=30)
        conn.row_factory = sqlite3.Row
        all_fps = conn.execute(
            "SELECT * FROM session_fingerprints WHERE trade_date != ? ORDER BY trade_date DESC",
            (today,)
        ).fetchall()
        conn.close()
        all_fps = [dict(r) for r in all_fps]
    except Exception as exc:
        return {"error": str(exc), "similar_days": []}

    if len(all_fps) < MIN_DAYS_FOR_MATCHING:
        return {
            "similar_days": [],
            "message": f"Mr. Spock needs {MIN_DAYS_FOR_MATCHING} trading days of historical data for pattern matching. Current: {len(all_fps)} day(s). Engage patience.",
            "total_days": len(all_fps),
            "needed": MIN_DAYS_FOR_MATCHING - len(all_fps),
        }

    # Backfill outcomes if available
    backfill_outcomes()

    # Score all historical days
    scored = sorted(all_fps, key=lambda fp: _distance(today_fp, fp))
    top_n  = scored[:limit]

    similar: list[dict] = []
    for fp in top_n:
        similarity = round((10 - _distance(today_fp, fp)) / 10 * 100, 0)
        outcome_parts = []
        if fp.get("outcome_spy_range_pct") is not None:
            rng = fp["outcome_spy_range_pct"]
            direction = "up" if fp.get("outcome_trended") else "down/flat"
            outcome_parts.append(f"Range {rng:.2f}% ({direction})")
        if fp.get("outcome_session_actual"):
            outcome_parts.append(f"Session: {fp['outcome_session_actual']}")
        if fp.get("outcome_grade"):
            outcome_parts.append(f"Forecast grade: {fp['outcome_grade']}")
        outcome = " | ".join(outcome_parts) if outcome_parts else "Outcome not yet scored"

        similar.append({
            "date":           fp["trade_date"],
            "session_type":   fp["session_type"],
            "vix_regime":     fp["vix_regime"],
            "similarity_pct": similarity,
            "matching_on":    _matching_fields(today_fp, fp),
            "outcome":        outcome,
            "spy_range_pct":  fp.get("outcome_spy_range_pct"),
            "grade":          fp.get("outcome_grade"),
        })

    return {
        "today": {
            "date":          today,
            "session_type":  today_fp.get("session_type"),
            "vix_regime":    today_fp.get("vix_regime"),
            "gex_polarity":  today_fp.get("gex_polarity"),
            "skew_direction": today_fp.get("skew_direction"),
        },
        "similar_days":   similar,
        "total_days":     len(all_fps),
        "spock_analysis": _spock_summary(today_fp, similar),
    }


def _matching_fields(fp_a: dict, fp_b: dict) -> list[str]:
    fields = []
    if fp_a.get("session_type")   == fp_b.get("session_type"):   fields.append("session_type")
    if fp_a.get("vix_regime")     == fp_b.get("vix_regime"):     fields.append("vix_regime")
    if fp_a.get("gex_polarity")   == fp_b.get("gex_polarity"):   fields.append("gex_polarity")
    if fp_a.get("skew_direction") == fp_b.get("skew_direction"): fields.append("skew_direction")
    if fp_a.get("momentum_open")  == fp_b.get("momentum_open"):  fields.append("momentum_open")
    pc_a = fp_a.get("pc_ratio") or 0
    pc_b = fp_b.get("pc_ratio") or 0
    if pc_a and pc_b and abs(pc_a - pc_b) <= 0.1:
        fields.append("pc_ratio")
    return fields


def _spock_summary(today_fp: dict, similar: list[dict]) -> str:
    if not similar:
        return "Insufficient historical data for pattern analysis — check back after 10 trading days."
    outcomes = [s["outcome"] for s in similar if "Range" in s.get("outcome", "")]
    if not outcomes:
        return f"Top match: {similar[0]['date']} ({similar[0]['similarity_pct']:.0f}% similar) — outcome pending."
    return (
        f"Today's {today_fp.get('session_type')} setup most closely resembles "
        + ", ".join(
            f"{s['date']} ({s['similarity_pct']:.0f}% — {s['outcome']})"
            for s in similar[:3]
        )
        + ". — Mr. Spock"
    )
