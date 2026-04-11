"""
Computer, Recalibrate Sensors — Adaptive Signal Weight Tuner
Self-tuning system that adjusts condition signal weights based on historical accuracy.
"""

import logging
import os
import sqlite3
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

_DB = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))
_ALERT_DB = os.path.expanduser("~/autonomous-trader/autonomous_trader.db")

DEFAULT_WEIGHTS: dict[str, float] = {
    "session_type": 0.30,
    "momentum":     0.25,
    "vix":          0.20,
    "volume":       0.15,
    "skew":         0.10,
}

MIN_WEIGHT = 0.05
MAX_WEIGHT = 0.40
MIN_DAYS   = 15


def _init_db() -> None:
    try:
        conn = sqlite3.connect(_DB, timeout=30)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS adaptive_weights (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_date TEXT NOT NULL,
                signal_name TEXT NOT NULL,
                accuracy_pct REAL,
                old_weight REAL,
                new_weight REAL,
                sample_days INTEGER,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(run_date, signal_name)
            )
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error("adaptive_tuner _init_db failed: %s", e)


def get_current_weights() -> dict[str, float]:
    """
    Returns current active weights from the adaptive_weights table.
    Falls back to DEFAULT_WEIGHTS if no tuning results yet or < MIN_DAYS data.
    """
    try:
        conn = sqlite3.connect(_DB, timeout=30)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT run_date FROM adaptive_weights ORDER BY created_at DESC LIMIT 1"
        ).fetchone()

        if not row:
            conn.close()
            return dict(DEFAULT_WEIGHTS)

        latest_date = row["run_date"]
        rows = conn.execute(
            "SELECT signal_name, new_weight, sample_days FROM adaptive_weights WHERE run_date = ?",
            (latest_date,),
        ).fetchall()
        conn.close()

        if not rows:
            return dict(DEFAULT_WEIGHTS)

        # Check if we had enough data
        sample_days = rows[0]["sample_days"] if rows else 0
        if sample_days < MIN_DAYS:
            return dict(DEFAULT_WEIGHTS)

        weights = {r["signal_name"]: r["new_weight"] for r in rows}
        # Fill any missing keys with defaults
        for k, v in DEFAULT_WEIGHTS.items():
            if k not in weights:
                weights[k] = v
        return weights

    except Exception as e:
        logger.error("get_current_weights failed: %s", e)
        return dict(DEFAULT_WEIGHTS)


def _fetch_intraday_snapshots(days: int = 30) -> list[dict]:
    """Read recent intraday_snapshots from autonomous_trader.db."""
    try:
        conn = sqlite3.connect(_ALERT_DB, timeout=30)
        conn.row_factory = sqlite3.Row
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = conn.execute(
            """
            SELECT trade_date, condition, condition_score, session_type,
                   trend_score, vix_regime, skew_score, buy_volume, sell_volume,
                   spot_price
            FROM intraday_snapshots
            WHERE trade_date >= ?
            ORDER BY trade_date ASC, created_at DESC
            """,
            (cutoff,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error("_fetch_intraday_snapshots failed: %s", e)
        return []


def _fetch_scorecards(days: int = 30) -> list[dict]:
    """Read forecast_scorecards from trader.db."""
    try:
        conn = sqlite3.connect(_DB, timeout=30)
        conn.row_factory = sqlite3.Row
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = conn.execute(
            """
            SELECT trade_date, session_correct, direction_correct, overall_grade
            FROM forecast_scorecards
            WHERE trade_date >= ?
            ORDER BY trade_date ASC
            """,
            (cutoff,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error("_fetch_scorecards failed: %s", e)
        return []


def _get_last_snapshot_per_day(snapshots: list[dict]) -> dict[str, dict]:
    """Return the last intraday snapshot per trade_date."""
    by_date: dict[str, dict] = {}
    for snap in snapshots:
        by_date[snap["trade_date"]] = snap  # list is ordered ASC, last wins per date
    return by_date


def _compute_accuracies(snapshots_by_day: dict[str, dict], scorecards: list[dict]) -> dict[str, float]:
    """Compute per-signal accuracy percentages."""
    scorecard_by_date = {s["trade_date"]: s for s in scorecards}
    trade_dates = sorted(set(snapshots_by_day.keys()) & set(scorecard_by_date.keys()))

    counts: dict[str, list[int]] = {k: [] for k in DEFAULT_WEIGHTS}

    sorted_dates = sorted(snapshots_by_day.keys())

    for i, date in enumerate(trade_dates):
        snap = snapshots_by_day[date]
        sc = scorecard_by_date[date]

        # session_type accuracy
        session_correct = sc.get("session_correct")
        if session_correct is not None:
            counts["session_type"].append(1 if session_correct else 0)

        # momentum accuracy: trend_score direction vs next day open/close
        trend_score = snap.get("trend_score") or 0.0
        spot = snap.get("spot_price") or 0.0
        idx_in_sorted = sorted_dates.index(date) if date in sorted_dates else -1
        if idx_in_sorted >= 0 and idx_in_sorted + 1 < len(sorted_dates):
            next_date = sorted_dates[idx_in_sorted + 1]
            next_snap = snapshots_by_day.get(next_date)
            if next_snap and spot and spot != 0:
                next_spot = next_snap.get("spot_price") or spot
                spy_moved_up = next_spot > spot
                momentum_correct = (trend_score > 30 and spy_moved_up) or (trend_score < -30 and not spy_moved_up)
                if abs(trend_score) > 30:
                    counts["momentum"].append(1 if momentum_correct else 0)

        # vix accuracy: stressed vix should produce low condition_score
        vix_regime = (snap.get("vix_regime") or "").upper()
        condition_score = snap.get("condition_score") or 50
        if vix_regime in ("STRESSED", "CRISIS"):
            counts["vix"].append(1 if condition_score < 45 else 0)
        elif vix_regime in ("CALM", "NORMAL"):
            counts["vix"].append(1 if condition_score >= 45 else 0)

        # volume accuracy: buy > sell should align with GREEN/YELLOW condition
        buy_vol = snap.get("buy_volume") or 0
        sell_vol = snap.get("sell_volume") or 0
        condition = (snap.get("condition") or "").upper()
        if buy_vol > 0 or sell_vol > 0:
            if buy_vol > sell_vol:
                counts["volume"].append(1 if condition in ("GREEN", "YELLOW") else 0)
            else:
                counts["volume"].append(1 if condition in ("RED", "YELLOW") else 0)

        # skew accuracy: high skew should warn (REVERSAL_RISK or RED)
        skew_score = snap.get("skew_score") or 0.0
        if skew_score > 5:
            warned_correctly = condition == "RED" or "REVERSAL" in condition
            counts["skew"].append(1 if warned_correctly else 0)

    accuracies: dict[str, float] = {}
    for signal, results in counts.items():
        if results:
            accuracies[signal] = round(sum(results) / len(results) * 100, 2)
        else:
            accuracies[signal] = 50.0  # neutral if no data

    return accuracies


def _adjust_weights(accuracies: dict[str, float], old_weights: dict[str, float]) -> dict[str, float]:
    """Apply accuracy-based adjustments and normalize."""
    new_weights: dict[str, float] = {}
    for signal, old_w in old_weights.items():
        acc = accuracies.get(signal, 50.0)
        accuracy_delta = acc - 50.0
        adjustment = accuracy_delta * 0.002
        raw = old_w + adjustment
        new_weights[signal] = max(MIN_WEIGHT, min(MAX_WEIGHT, raw))

    # Normalize to sum to 1.0
    total = sum(new_weights.values())
    if total > 0:
        new_weights = {k: round(v / total, 6) for k, v in new_weights.items()}
    return new_weights


def _save_weights(run_date: str, weights: dict[str, float], old_weights: dict[str, float],
                  accuracies: dict[str, float], sample_days: int) -> None:
    try:
        conn = sqlite3.connect(_DB, timeout=30)
        for signal_name, new_weight in weights.items():
            conn.execute(
                """
                INSERT INTO adaptive_weights
                  (run_date, signal_name, accuracy_pct, old_weight, new_weight, sample_days)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_date, signal_name) DO UPDATE SET
                  accuracy_pct=excluded.accuracy_pct,
                  old_weight=excluded.old_weight,
                  new_weight=excluded.new_weight,
                  sample_days=excluded.sample_days
                """,
                (run_date, signal_name, accuracies.get(signal_name, 50.0),
                 old_weights.get(signal_name, DEFAULT_WEIGHTS.get(signal_name, 0.1)),
                 new_weight, sample_days),
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error("_save_weights failed: %s", e)


def run_adaptive_tuning(force: bool = False) -> dict:
    """
    Analyze signal accuracy over last 20 trading days and update weights.
    Returns tuning results with new weights and per-signal accuracies.
    """
    run_date = datetime.now().strftime("%Y-%m-%d")

    try:
        # Check if already ran today
        if not force:
            try:
                conn = sqlite3.connect(_DB, timeout=30)
                existing = conn.execute(
                    "SELECT COUNT(*) FROM adaptive_weights WHERE run_date = ?", (run_date,)
                ).fetchone()[0]
                conn.close()
                if existing > 0:
                    return get_weights_status()
            except Exception:
                pass

        snapshots = _fetch_intraday_snapshots(days=30)
        scorecards = _fetch_scorecards(days=30)

        if len(scorecards) < MIN_DAYS:
            return {
                "status": "insufficient_data",
                "days": len(scorecards),
                "weights": dict(DEFAULT_WEIGHTS),
                "message": f"Need {MIN_DAYS}+ trading days of scorecard data, have {len(scorecards)}",
            }

        snapshots_by_day = _get_last_snapshot_per_day(snapshots)
        accuracies = _compute_accuracies(snapshots_by_day, scorecards)
        old_weights = get_current_weights()
        new_weights = _adjust_weights(accuracies, old_weights)

        _save_weights(run_date, new_weights, old_weights, accuracies, len(scorecards))

        return {
            "status": "tuned",
            "days_analyzed": len(scorecards),
            "weights": new_weights,
            "accuracies": accuracies,
            "run_date": run_date,
        }

    except Exception as e:
        logger.error("run_adaptive_tuning failed: %s", e)
        return {
            "status": "error",
            "error": str(e),
            "weights": dict(DEFAULT_WEIGHTS),
            "run_date": run_date,
        }


def get_weights_status() -> dict:
    """API endpoint response with current weights, accuracies, and metadata."""
    try:
        conn = sqlite3.connect(_DB, timeout=30)
        conn.row_factory = sqlite3.Row

        latest_row = conn.execute(
            "SELECT run_date FROM adaptive_weights ORDER BY created_at DESC LIMIT 1"
        ).fetchone()

        if not latest_row:
            conn.close()
            return {
                "weights": dict(DEFAULT_WEIGHTS),
                "accuracies": {},
                "status": "default",
                "last_tuned": None,
                "days_of_data": 0,
                "is_default": True,
            }

        latest_date = latest_row["run_date"]
        rows = conn.execute(
            "SELECT signal_name, new_weight, accuracy_pct, sample_days FROM adaptive_weights WHERE run_date = ?",
            (latest_date,),
        ).fetchall()
        conn.close()

        weights = {r["signal_name"]: r["new_weight"] for r in rows}
        accuracies = {r["signal_name"]: r["accuracy_pct"] for r in rows}
        sample_days = rows[0]["sample_days"] if rows else 0
        is_default = sample_days < MIN_DAYS

        if is_default:
            weights = dict(DEFAULT_WEIGHTS)

        return {
            "weights": weights,
            "accuracies": accuracies,
            "status": "default" if is_default else "tuned",
            "last_tuned": latest_date,
            "days_of_data": sample_days,
            "is_default": is_default,
        }

    except Exception as e:
        logger.error("get_weights_status failed: %s", e)
        return {
            "weights": dict(DEFAULT_WEIGHTS),
            "accuracies": {},
            "status": "error",
            "last_tuned": None,
            "days_of_data": 0,
            "is_default": True,
        }


def run_adaptive_tuner_weekly() -> None:
    """Called every Sunday at 11:00 AM."""
    logger.info("adaptive_tuner: running weekly recalibration")
    result = run_adaptive_tuning(force=True)
    logger.info("adaptive_tuner weekly result: status=%s days=%s",
                result.get("status"), result.get("days_analyzed", result.get("days")))


# Initialize DB on module load
_init_db()
