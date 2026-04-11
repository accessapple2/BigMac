"""
Captain's Log — End-of-Day Forecast Scorecard
----------------------------------------------
Runs at 4:15 PM ET on market days. Grades the morning Ready Room
forecast against what actually happened intraday.

Table: forecast_scorecards  (SACRED — never dropped/truncated)
Schedule: 16:15 ET = 13:15 AZ (wired into main.py scheduler)
"""
from __future__ import annotations

import os
import sqlite3
from datetime import date, datetime, timezone
from typing import Any

_TRADEMINDS_DB = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)
_ALERT_DB = os.environ.get("TRADER_DB", "autonomous_trader.db")

_eod_done_today = False


def _init_db() -> None:
    conn = sqlite3.connect(_TRADEMINDS_DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS forecast_scorecards (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date              TEXT NOT NULL UNIQUE,
            morning_session_call    TEXT,
            actual_session_type     TEXT,
            session_correct         INTEGER,
            put_wall_respected      INTEGER,
            call_wall_respected     INTEGER,
            max_pain_magnet         INTEGER,
            levels_respected_score  REAL,
            go_days_trended         INTEGER,
            stand_down_days_chopped INTEGER,
            condition_accuracy      REAL,
            overall_grade           TEXT,
            spy_open                REAL,
            spy_close               REAL,
            spy_range_pct           REAL,
            snapshot_count          INTEGER,
            notes                   TEXT,
            created_at              TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()


_init_db()


# ── Fetch data ────────────────────────────────────────────────────────────────
def _get_morning_briefing(trade_date: str) -> dict[str, Any]:
    """Get the first Ready Room briefing of the day."""
    try:
        conn = sqlite3.connect(_TRADEMINDS_DB, timeout=30)
        conn.row_factory = sqlite3.Row
        row = conn.execute("""
            SELECT * FROM ready_room_briefings
            WHERE date(created_at) = ?
            ORDER BY id ASC LIMIT 1
        """, (trade_date,)).fetchone()
        conn.close()
        return dict(row) if row else {}
    except Exception:
        return {}


def _get_intraday_snapshots(trade_date: str) -> list[dict]:
    """Get all intraday snapshots from Red Alert for the day."""
    try:
        conn = sqlite3.connect(_ALERT_DB, timeout=30)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT * FROM intraday_snapshots
            WHERE snap_date = ?
            ORDER BY id ASC
        """, (trade_date,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def _get_morning_condition(trade_date: str) -> dict[str, Any]:
    """Get the first intraday snapshot (morning condition)."""
    try:
        conn = sqlite3.connect(_ALERT_DB, timeout=30)
        conn.row_factory = sqlite3.Row
        row = conn.execute("""
            SELECT * FROM intraday_snapshots
            WHERE snap_date = ?
            ORDER BY id ASC LIMIT 1
        """, (trade_date,)).fetchone()
        conn.close()
        return dict(row) if row else {}
    except Exception:
        return {}


# ── Grade computation ─────────────────────────────────────────────────────────
_LETTER_GRADES = [
    (90, "A"),
    (80, "B"),
    (70, "C"),
    (60, "D"),
    (0,  "F"),
]


def _letter_grade(score_pct: float) -> str:
    for threshold, grade in _LETTER_GRADES:
        if score_pct >= threshold:
            return grade
    return "F"


def _determine_actual_session(snapshots: list[dict]) -> str:
    """Determine what the session actually did from intraday data."""
    if not snapshots:
        return "UNKNOWN"
    session_counts: dict[str, int] = {}
    for s in snapshots:
        st = s.get("session_type")
        if st:
            session_counts[st] = session_counts.get(st, 0) + 1
    if not session_counts:
        return "UNKNOWN"
    # Majority session type
    return max(session_counts, key=lambda k: session_counts[k])


def _check_levels(snapshots: list[dict], briefing: dict) -> dict[str, Any]:
    """Grade whether key levels were respected."""
    call_wall  = briefing.get("call_wall")  or 0
    put_wall   = briefing.get("put_wall")   or 0
    max_pain   = briefing.get("max_pain")   or 0

    if not snapshots or not any([call_wall, put_wall, max_pain]):
        return {"put_wall_respected": None, "call_wall_respected": None, "max_pain_magnet": None, "score": 0.5}

    prices = [s["spot_price"] for s in snapshots if s.get("spot_price")]
    if not prices:
        return {"put_wall_respected": None, "call_wall_respected": None, "max_pain_magnet": None, "score": 0.5}

    spy_high = max(prices)
    spy_low  = min(prices)
    spy_open = prices[0]
    spy_close = prices[-1]

    checks_passed = 0
    checks_total  = 0

    # Call wall respected = price didn't close significantly above it
    call_respected = None
    if call_wall > 0:
        checks_total += 1
        call_respected = spy_close <= call_wall * 1.002  # within 0.2%
        if call_respected:
            checks_passed += 1

    # Put wall respected = price didn't close significantly below it
    put_respected = None
    if put_wall > 0:
        checks_total += 1
        put_respected = spy_close >= put_wall * 0.998
        if put_respected:
            checks_passed += 1

    # Max pain magnet = close within 0.5% of max pain
    max_pain_hit = None
    if max_pain > 0:
        checks_total += 1
        max_pain_hit = abs(spy_close - max_pain) / max_pain < 0.005
        if max_pain_hit:
            checks_passed += 1

    score = (checks_passed / checks_total) if checks_total > 0 else 0.5

    return {
        "put_wall_respected":  1 if put_respected  else 0 if put_respected  is not None else None,
        "call_wall_respected": 1 if call_respected else 0 if call_respected is not None else None,
        "max_pain_magnet":     1 if max_pain_hit   else 0 if max_pain_hit   is not None else None,
        "score":               score,
        "spy_open":  spy_open,
        "spy_close": spy_close,
        "spy_range_pct": round((spy_high - spy_low) / spy_open * 100, 3) if spy_open else 0,
    }


def _check_condition_accuracy(morning_condition: str, snapshots: list[dict]) -> float:
    """
    GO days: did the market trend (range > 0.5%)?
    STAND_DOWN / RED days: was it choppy (range < 0.3%)?
    """
    if not snapshots or not morning_condition:
        return 0.5
    prices = [s["spot_price"] for s in snapshots if s.get("spot_price")]
    if len(prices) < 2:
        return 0.5
    spy_range_pct = (max(prices) - min(prices)) / prices[0] * 100

    if morning_condition == "GREEN":
        # Good day if it trended (range > 0.5%)
        return 1.0 if spy_range_pct > 0.5 else 0.0
    elif morning_condition == "RED":
        # Good stand-down if it was choppy (range < 0.4%)
        return 1.0 if spy_range_pct < 0.4 else 0.0
    else:  # YELLOW
        # Partial credit if moderate range (0.3-0.6%)
        return 0.7 if 0.3 <= spy_range_pct <= 0.6 else 0.4


# ── Main scoring function ─────────────────────────────────────────────────────
def run_eod_scorecard(trade_date: str | None = None, force: bool = False) -> dict[str, Any]:
    """
    Grade today's morning forecast. Idempotent — won't double-score a day
    unless force=True.
    """
    global _eod_done_today
    if _eod_done_today and not force:
        return {"skipped": True, "reason": "Already scored today"}

    today = trade_date or date.today().isoformat()

    # Check if already scored
    try:
        conn = sqlite3.connect(_TRADEMINDS_DB, timeout=30)
        existing = conn.execute(
            "SELECT id FROM forecast_scorecards WHERE trade_date=?", (today,)
        ).fetchone()
        conn.close()
        if existing and not force:
            _eod_done_today = True
            return {"skipped": True, "reason": f"Already scored {today}"}
    except Exception:
        pass

    briefing   = _get_morning_briefing(today)
    snapshots  = _get_intraday_snapshots(today)
    morning_cd = _get_morning_condition(today)

    from rich.console import Console
    console = Console()

    if not briefing:
        console.log(f"[yellow]EOD Scorecard: no morning briefing found for {today}")
        return {"error": "No morning briefing"}
    if len(snapshots) < 3:
        console.log(f"[yellow]EOD Scorecard: only {len(snapshots)} intraday snapshots — skipping grade")
        return {"error": f"Insufficient snapshots ({len(snapshots)})"}

    morning_call    = briefing.get("session_type", "UNKNOWN")
    actual_session  = _determine_actual_session(snapshots)
    session_correct = 1 if morning_call == actual_session else 0

    levels = _check_levels(snapshots, briefing)
    mc_cond = morning_cd.get("condition", "UNKNOWN")
    cond_acc = _check_condition_accuracy(mc_cond, snapshots)

    # ── Overall grade (weighted) ──────────────────────────────────────────────
    # session correct 40%, levels 35%, condition accuracy 25%
    overall_pct = (
        session_correct * 40 +
        levels["score"]  * 35 +
        cond_acc         * 25
    )
    grade = _letter_grade(overall_pct)

    notes_parts = []
    if session_correct:
        notes_parts.append(f"Session call correct ({morning_call})")
    else:
        notes_parts.append(f"Session call wrong ({morning_call} → actual {actual_session})")
    notes_parts.append(f"Levels score {levels['score']*100:.0f}%")
    notes_parts.append(f"Condition accuracy {cond_acc*100:.0f}%")

    scorecard = {
        "trade_date":              today,
        "morning_session_call":    morning_call,
        "actual_session_type":     actual_session,
        "session_correct":         session_correct,
        "put_wall_respected":      levels.get("put_wall_respected"),
        "call_wall_respected":     levels.get("call_wall_respected"),
        "max_pain_magnet":         levels.get("max_pain_magnet"),
        "levels_respected_score":  round(levels["score"], 3),
        "condition_accuracy":      round(cond_acc, 3),
        "overall_grade":           grade,
        "spy_open":                levels.get("spy_open"),
        "spy_close":               levels.get("spy_close"),
        "spy_range_pct":           levels.get("spy_range_pct"),
        "snapshot_count":          len(snapshots),
        "notes":                   " | ".join(notes_parts),
    }

    # Persist
    try:
        conn = sqlite3.connect(_TRADEMINDS_DB, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            INSERT OR REPLACE INTO forecast_scorecards
                (trade_date, morning_session_call, actual_session_type, session_correct,
                 put_wall_respected, call_wall_respected, max_pain_magnet,
                 levels_respected_score, condition_accuracy, overall_grade,
                 spy_open, spy_close, spy_range_pct, snapshot_count, notes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, tuple(scorecard.get(k) for k in [
            "trade_date", "morning_session_call", "actual_session_type", "session_correct",
            "put_wall_respected", "call_wall_respected", "max_pain_magnet",
            "levels_respected_score", "condition_accuracy", "overall_grade",
            "spy_open", "spy_close", "spy_range_pct", "snapshot_count", "notes",
        ]))
        conn.commit()
        conn.close()
    except Exception as exc:
        console.log(f"[red]EOD Scorecard: save error: {exc}")

    console.log(
        f"[cyan]Captain's Log — EOD Scorecard {today}: "
        f"Grade {grade} | {morning_call}→{actual_session} "
        f"({'✓' if session_correct else '✗'}) | "
        f"Levels {levels['score']*100:.0f}% | Cond {cond_acc*100:.0f}%"
    )
    _eod_done_today = True
    return scorecard


def get_rolling_accuracy(days: int = 14) -> dict[str, Any]:
    """Rolling forecast accuracy over last N trading days."""
    try:
        conn = sqlite3.connect(_TRADEMINDS_DB, timeout=30)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT overall_grade, session_correct, levels_respected_score,
                   condition_accuracy, trade_date
            FROM forecast_scorecards
            ORDER BY trade_date DESC
            LIMIT ?
        """, (days,)).fetchall()
        conn.close()
        if not rows:
            return {"days": 0, "accuracy_pct": None, "message": "No scorecard data yet"}

        grade_points = {"A": 95, "B": 85, "C": 75, "D": 65, "F": 50}
        total_score = sum(grade_points.get(r["overall_grade"], 50) for r in rows)
        avg = total_score / len(rows)

        session_acc = sum(r["session_correct"] or 0 for r in rows) / len(rows) * 100
        levels_acc  = sum(r["levels_respected_score"] or 0 for r in rows) / len(rows) * 100

        return {
            "days":          len(rows),
            "accuracy_pct":  round(avg, 1),
            "session_accuracy_pct": round(session_acc, 1),
            "levels_accuracy_pct":  round(levels_acc, 1),
            "grade_label":   _letter_grade(avg),
            "recent_grades": [r["overall_grade"] for r in rows],
        }
    except Exception as exc:
        return {"error": str(exc)}


# ── Scheduler wrapper ─────────────────────────────────────────────────────────
def run_eod_scorecard_job():
    """Called by main.py scheduler at 4:15 PM ET (13:15 AZ) on weekdays."""
    global _eod_done_today
    try:
        import pytz
        from datetime import datetime as _dt
        az = pytz.timezone("US/Arizona")
        now = _dt.now(az)
        if now.weekday() >= 5:
            return
        # Fire between 13:15–13:25 AZ (4:15–4:25 PM ET)
        now_mins = now.hour * 60 + now.minute
        if 795 <= now_mins <= 805:  # 13:15–13:25 AZ
            if not _eod_done_today:
                run_eod_scorecard()
        # Reset flag after midnight
        if now.hour < 6:
            _eod_done_today = False
    except Exception as exc:
        from rich.console import Console
        Console().log(f"[red]EOD Scorecard job error: {exc}")
