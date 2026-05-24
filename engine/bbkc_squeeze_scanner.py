"""BB/KC Volatility-Compression Squeeze Scanner — HM-SQUEEZE-BBKC-COMPRESSION.

Detects the TTM-style "squeeze" setup: Bollinger Bands(20, 2σ) fully inside
Keltner Channels(20, 1.5×ATR) on the daily timeframe. The longer the bands
remain compressed, the higher the conviction.

This is ORTHOGONAL to engine/squeeze_scanner.py (short-interest squeeze).
Both write to the shared `squeeze_watch` table; the `kind` column
discriminates ('short_interest' | 'bbkc').

Tiers — by consecutive in-squeeze days:
    WATCH    : 5–9d   (composite 25–45)
    ALERT    : 10–19d (composite 50–95)
    PRIORITY : 20d+   (composite 100)

Ship state: gated by ``BBKC_SQUEEZE_WATCHER_ENABLED`` env flag (default OFF).
Soaks in shadow before the Captain flips the flag.

Universe: ``scan_universe`` table (~3,026 symbols) — daily OHLCV pulled via
``engine.market_data.get_bulk_daily_ohlcv`` (Alpaca bulk-bars, 30-min cache).

Surfacing:
    - GET /api/squeeze/recent?kind=bbkc (dashboard tab in section-squeeze)
    - PRIORITY tier hits NTFY ``ollietrades-admin`` (first per process lifetime).

HM-AO-β Ghost Watcher pattern — surfaces candidates only; does NOT write to
the signals table and does NOT route to paper_trader.
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from rich.console import Console

console = Console()

_DB_PATH = str(Path(__file__).resolve().parent.parent / "data" / "trader.db")

# ─── Scanner constants ────────────────────────────────────────────────────
_BB_PERIOD: int = 20
_BB_K: float = 2.0
_KC_PERIOD: int = 20
_KC_K: float = 1.5
_MIN_BARS_REQUIRED: int = 25   # need at least period+5 for stable ATR/stdev
_MIN_PERSIST_DAYS: int = 5     # below this duration, skip persistence
_DEDUPE_HOURS: int = 24
_CACHE_TTL: int = 300          # 5 min in-memory cache on run_scan() result
_NTFY_PER_RUN_CAP: int = 5     # belt-and-suspenders cap to prevent notification
                               # storm on first scan against a virgin DB

# Tier mapping by consecutive in-squeeze days
_TIER_WATCH_MIN: int = 5
_TIER_ALERT_MIN: int = 10
_TIER_PRIORITY_MIN: int = 20

# Shared mutable scan state (module-level singleton)
_scan_lock = threading.Lock()
_last_result: dict | None = None
_last_scan_ts: float = 0.0
_ntfy_fired_classes: set[str] = set()   # per-process-lifetime dedupe for NTFY


# ─── Indicator helpers ────────────────────────────────────────────────────


def _sma(arr: np.ndarray, n: int) -> np.ndarray:
    """Simple moving average. Returns array same length as ``arr`` with the
    first ``n-1`` entries set to NaN."""
    if len(arr) < n:
        return np.full(len(arr), np.nan)
    out = np.full(len(arr), np.nan)
    cumsum = np.cumsum(arr, dtype=float)
    out[n - 1] = cumsum[n - 1] / n
    out[n:] = (cumsum[n:] - cumsum[:-n]) / n
    return out


def _rolling_std(arr: np.ndarray, n: int) -> np.ndarray:
    """Population stddev rolling window. NaN-padded for first n-1 entries."""
    if len(arr) < n:
        return np.full(len(arr), np.nan)
    out = np.full(len(arr), np.nan)
    for i in range(n - 1, len(arr)):
        window = arr[i - n + 1 : i + 1]
        out[i] = float(np.std(window))
    return out


def _atr_series(
    highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, n: int
) -> np.ndarray:
    """Wilder ATR series. NaN-padded for first n entries."""
    if len(closes) < n + 1:
        return np.full(len(closes), np.nan)
    prev_close = np.roll(closes, 1)
    prev_close[0] = closes[0]
    tr = np.maximum.reduce(
        [highs - lows, np.abs(highs - prev_close), np.abs(lows - prev_close)]
    )
    out = np.full(len(closes), np.nan)
    out[n] = float(np.mean(tr[1 : n + 1]))
    for i in range(n + 1, len(closes)):
        out[i] = (out[i - 1] * (n - 1) + tr[i]) / n
    return out


def _compute_bands(df: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame with bb_upper/bb_lower/kc_upper/kc_lower columns
    aligned to ``df``'s index. NaN where insufficient history.

    KC center uses SMA(20) for symmetry with BB (Captain spec 2026-05-24).
    """
    closes = df["Close"].to_numpy(dtype=float)
    highs = df["High"].to_numpy(dtype=float)
    lows = df["Low"].to_numpy(dtype=float)

    bb_mid = _sma(closes, _BB_PERIOD)
    bb_std = _rolling_std(closes, _BB_PERIOD)
    kc_mid = _sma(closes, _KC_PERIOD)
    atr = _atr_series(highs, lows, closes, _KC_PERIOD)

    bands = pd.DataFrame(
        {
            "bb_upper": bb_mid + _BB_K * bb_std,
            "bb_lower": bb_mid - _BB_K * bb_std,
            "kc_upper": kc_mid + _KC_K * atr,
            "kc_lower": kc_mid - _KC_K * atr,
            "bb_mid": bb_mid,
        },
        index=df.index,
    )
    return bands


def _detect_squeeze_run(df: pd.DataFrame) -> tuple[bool, int, float, float, float]:
    """Inspect the most-recent bar for an active BB-inside-KC squeeze and
    count consecutive prior bars satisfying the same condition.

    Returns: ``(in_squeeze_now, consecutive_days, bb_width_pct, kc_width_pct,
    last_close)``.

    - ``bb_width_pct`` = (bb_upper - bb_lower) / bb_mid * 100 at most recent bar
    - ``kc_width_pct`` = (kc_upper - kc_lower) / bb_mid * 100 at most recent bar
    - ``consecutive_days`` is 0 when not in squeeze on the latest bar.
    """
    if len(df) < _MIN_BARS_REQUIRED:
        return (False, 0, 0.0, 0.0, 0.0)

    bands = _compute_bands(df)
    in_sq = (bands["bb_upper"] < bands["kc_upper"]) & (
        bands["bb_lower"] > bands["kc_lower"]
    )
    in_sq = in_sq.fillna(False).to_numpy()

    last_idx = len(in_sq) - 1
    if not bool(in_sq[last_idx]):
        return (False, 0, 0.0, 0.0, float(df["Close"].iloc[-1]))

    # Count back from last_idx while in_sq stays True
    run = 0
    for i in range(last_idx, -1, -1):
        if in_sq[i]:
            run += 1
        else:
            break

    last_row = bands.iloc[-1]
    last_close = float(df["Close"].iloc[-1])
    bb_mid = float(last_row["bb_mid"]) if last_row["bb_mid"] else last_close
    if bb_mid == 0:
        bb_mid = last_close if last_close else 1.0
    bb_width_pct = (
        (float(last_row["bb_upper"]) - float(last_row["bb_lower"])) / bb_mid * 100.0
    )
    kc_width_pct = (
        (float(last_row["kc_upper"]) - float(last_row["kc_lower"])) / bb_mid * 100.0
    )
    return (True, run, bb_width_pct, kc_width_pct, last_close)


# ─── Tier mapping ─────────────────────────────────────────────────────────


def _tier_for_duration(days: int) -> str:
    if days >= _TIER_PRIORITY_MIN:
        return "PRIORITY"
    if days >= _TIER_ALERT_MIN:
        return "ALERT"
    return "WATCH"


def _composite_for_duration(days: int) -> float:
    """Linear 5 pts/day, capped at 100. Maps:
    5d→25, 10d→50, 15d→75, 20d→100, 30d→100 (saturated)."""
    return float(min(100.0, max(0.0, days * 5.0)))


def _tier_rank(tier: str) -> int:
    return {"WATCH": 1, "ALERT": 2, "PRIORITY": 3}.get(tier, 0)


# ─── Universe + bar fetch ────────────────────────────────────────────────


def _load_universe(db_path: str | None = None) -> list[str]:
    """Pull symbol list from scan_universe. Defensive on missing table."""
    try:
        conn = _conn(db_path or _DB_PATH)
        try:
            rows = conn.execute("SELECT symbol FROM scan_universe").fetchall()
            return [r["symbol"] for r in rows if r["symbol"]]
        finally:
            conn.close()
    except Exception as e:
        console.log(
            f"[yellow]bbkc_squeeze: scan_universe load failed: "
            f"{type(e).__name__}: {e!r}"
        )
        return []


# ─── Persistence ─────────────────────────────────────────────────────────


def _conn(db_path: str | None = None) -> sqlite3.Connection:
    db_path = db_path or _DB_PATH
    c = sqlite3.connect(db_path, timeout=10)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def _is_quiet_hours_et() -> bool:
    """22:00-06:00 ET = 02:00-10:00 UTC (during DST)."""
    hour_utc = datetime.now(timezone.utc).hour
    return 2 <= hour_utc < 10


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """Idempotent: add kind + bbkc_duration_days if missing. Mirrors the
    HM-DASH.4 pattern in engine/squeeze_scanner.py — fresh DBs / migration
    drift are handled without crashing the scan."""
    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(squeeze_watch)")}
        if "kind" not in cols:
            conn.execute(
                "ALTER TABLE squeeze_watch ADD COLUMN kind TEXT NOT NULL "
                "DEFAULT 'short_interest'"
            )
            conn.execute(
                "UPDATE squeeze_watch SET kind='short_interest' "
                "WHERE kind IS NULL OR kind=''"
            )
        if "bbkc_duration_days" not in cols:
            conn.execute(
                "ALTER TABLE squeeze_watch ADD COLUMN bbkc_duration_days INTEGER"
            )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_squeeze_watch_kind_ts "
            "ON squeeze_watch(kind, scan_ts DESC) WHERE dismissed = 0"
        )
        conn.commit()
    except Exception as e:
        console.log(
            f"[yellow]bbkc_squeeze: _ensure_schema: "
            f"{type(e).__name__}: {e!r}"
        )


def _persist_results(
    results: list[dict], db_path: str | None = None
) -> dict:
    db_path = db_path or _DB_PATH
    """Write each result with duration >= _MIN_PERSIST_DAYS into squeeze_watch
    with kind='bbkc'. Returns summary dict.

    Dedupe: if same symbol has a non-dismissed bbkc row in last _DEDUPE_HOURS,
    only insert if the new tier is strictly higher.
    """
    summary = {
        "inserted": 0,
        "deferred": 0,
        "skipped_dedup": 0,
        "ntfy_fired": 0,
    }
    if not results:
        return summary

    quiet = _is_quiet_hours_et()
    cutoff_ts = (
        datetime.now(timezone.utc) - timedelta(hours=_DEDUPE_HOURS)
    ).isoformat()
    scan_ts = datetime.now(timezone.utc).isoformat()

    try:
        conn = _conn(db_path)
    except Exception as e:
        console.log(
            f"[yellow]bbkc_squeeze: persist conn failed: "
            f"{type(e).__name__}: {e!r}"
        )
        return summary

    try:
        _ensure_schema(conn)
        ntfy_fired_this_run = 0

        for r in results:
            duration = int(r.get("duration_days", 0) or 0)
            if duration < _MIN_PERSIST_DAYS:
                continue

            symbol = (r.get("symbol") or "").strip().upper()
            if not symbol:
                continue

            composite = _composite_for_duration(duration)
            tier = _tier_for_duration(duration)

            # Dedupe vs prior bbkc row for same symbol
            row = conn.execute(
                """SELECT id, threshold_tier FROM squeeze_watch
                   WHERE symbol = ? AND kind = 'bbkc'
                     AND scan_ts >= ? AND dismissed = 0
                   ORDER BY scan_ts DESC LIMIT 1""",
                (symbol, cutoff_ts),
            ).fetchone()
            if row is not None and _tier_rank(row["threshold_tier"]) >= _tier_rank(
                tier
            ):
                summary["skipped_dedup"] += 1
                continue

            notes = (
                f"duration_days={duration}; "
                f"bb_width_pct={r.get('bb_width_pct'):.2f}; "
                f"kc_width_pct={r.get('kc_width_pct'):.2f}; "
                f"tightness={r.get('tightness'):.2f}"
            )
            # breakout_score reused as "tightness" (1 - bb/kc ratio); 0→1 range
            breakout = float(r.get("tightness", 0.0))
            ntfy_deferred = 1 if (tier == "PRIORITY" and quiet) else 0
            price_at_scan = float(r.get("last_close", 0.0))

            try:
                conn.execute(
                    """INSERT INTO squeeze_watch
                       (symbol, scan_ts, short_pct, float_m, vol_ratio, rsi,
                        breakout_score, composite_score, threshold_tier,
                        price_at_scan, notes, ntfy_sent, ntfy_deferred,
                        kind, bbkc_duration_days)
                       VALUES (?, ?, NULL, NULL, NULL, NULL,
                               ?, ?, ?,
                               ?, ?, 0, ?,
                               'bbkc', ?)""",
                    (
                        symbol,
                        scan_ts,
                        breakout,
                        composite,
                        tier,
                        price_at_scan,
                        notes,
                        ntfy_deferred,
                        duration,
                    ),
                )
                summary["inserted"] += 1
                if ntfy_deferred:
                    summary["deferred"] += 1

                if (
                    tier == "PRIORITY"
                    and not ntfy_deferred
                    and ntfy_fired_this_run < _NTFY_PER_RUN_CAP
                ):
                    if _fire_ntfy(symbol, duration):
                        summary["ntfy_fired"] += 1
                        ntfy_fired_this_run += 1
                        conn.execute(
                            "UPDATE squeeze_watch SET ntfy_sent=1 "
                            "WHERE symbol=? AND scan_ts=? AND kind='bbkc'",
                            (symbol, scan_ts),
                        )
            except Exception as e:
                console.log(
                    f"[yellow]bbkc_squeeze: insert {symbol} failed: "
                    f"{type(e).__name__}: {e!r}"
                )
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return summary


def _fire_ntfy(symbol: str, duration: int) -> bool:
    """First-occurrence-per-process NTFY for PRIORITY tier. Returns True if
    the call ran (regardless of HTTP outcome), False on dedupe-skip."""
    key = f"bbkc_priority::{symbol}"
    if key in _ntfy_fired_classes:
        return False
    _ntfy_fired_classes.add(key)
    try:
        from engine.alert_channels import send_alert

        send_alert(
            level="warning",
            alert_type=f"bbkc_squeeze_priority_{symbol}",
            message=(
                f"🔥 BB/KC Squeeze PRIORITY — {symbol} ({duration}d coil). "
                f"Volatility compression on the daily; breakout pending."
            ),
            title=f"🔥 BB/KC Squeeze PRIORITY — {symbol}",
            audience="admin",
            rate_limit_secs=86400,
        )
        return True
    except Exception as e:
        console.log(
            f"[yellow]bbkc_squeeze: NTFY {symbol} failed: "
            f"{type(e).__name__}: {e!r}"
        )
        return False


# ─── Main scan loop ──────────────────────────────────────────────────────


def run_scan(force: bool = False) -> dict:
    """Run the BB/KC compression scan. Cached 5 min unless force=True.

    Returns ``{results, scanned_at, candidate_count, watch_persist}``.
    """
    global _last_result, _last_scan_ts

    if not force and _last_result is not None:
        if time.time() - _last_scan_ts < _CACHE_TTL:
            return _last_result

    with _scan_lock:
        if not force and _last_result is not None:
            if time.time() - _last_scan_ts < _CACHE_TTL:
                return _last_result

        universe = _load_universe()
        console.log(
            f"[cyan]BBKC Squeeze Scanner: universe={len(universe)} symbols"
        )
        if not universe:
            empty = {
                "results": [],
                "scanned_at": datetime.now().isoformat(),
                "candidate_count": 0,
                "watch_persist": {},
            }
            _last_result = empty
            _last_scan_ts = time.time()
            return empty

        # Pull 3-mo daily OHLCV (~63 trading days; gives plenty for 20-period
        # BB/KC plus history). 30-min cache in market_data so subsequent
        # cycles inside that window are free.
        try:
            from engine.market_data import get_bulk_daily_ohlcv

            bars_by_sym = get_bulk_daily_ohlcv(universe, range_str="3mo")
        except Exception as e:
            console.log(
                f"[yellow]bbkc_squeeze: get_bulk_daily_ohlcv failed: "
                f"{type(e).__name__}: {e!r}"
            )
            bars_by_sym = {}

        results: list[dict] = []
        scanned = 0
        for symbol, df in bars_by_sym.items():
            if df is None or df.empty or len(df) < _MIN_BARS_REQUIRED:
                continue
            scanned += 1
            try:
                in_sq, duration, bb_w, kc_w, last_close = _detect_squeeze_run(df)
            except Exception as e:
                console.log(
                    f"[yellow]bbkc_squeeze: detect {symbol} failed: "
                    f"{type(e).__name__}: {e!r}"
                )
                continue
            if not in_sq or duration < _MIN_PERSIST_DAYS:
                continue
            tightness = 1.0 - (bb_w / kc_w) if kc_w > 0 else 0.0
            results.append(
                {
                    "symbol": symbol,
                    "duration_days": duration,
                    "bb_width_pct": bb_w,
                    "kc_width_pct": kc_w,
                    "tightness": max(0.0, min(1.0, tightness)),
                    "last_close": last_close,
                    "tier": _tier_for_duration(duration),
                }
            )

        # Sort by duration desc, then tightness desc
        results.sort(
            key=lambda r: (r["duration_days"], r["tightness"]),
            reverse=True,
        )

        persist_summary: dict = {}
        try:
            persist_summary = _persist_results(results)
        except Exception as e:
            console.log(
                f"[yellow]bbkc_squeeze: persist top-level failed: "
                f"{type(e).__name__}: {e!r}"
            )

        _last_result = {
            "results": results,
            "scanned_at": datetime.now().isoformat(),
            "candidate_count": scanned,
            "watch_persist": persist_summary,
        }
        _last_scan_ts = time.time()

        console.log(
            f"[green]BBKC Squeeze Scanner: scanned={scanned} "
            f"in_squeeze={len(results)} "
            f"inserted={persist_summary.get('inserted', 0)} "
            f"ntfy={persist_summary.get('ntfy_fired', 0)}"
        )
        return _last_result


__all__ = ["run_scan"]
