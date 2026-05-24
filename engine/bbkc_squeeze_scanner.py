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
                               # (HM-SQUEEZE-RELEASE-DETECT: shared between
                               # entry-NTFYs and release-NTFYs in one run)

# HM-SQUEEZE-RELEASE-DETECT
_RELEASE_VOL_GATE: float = 2.0          # min vol_ratio for release-NTFY
_RELEASE_LOOKBACK_DAYS: int = 30        # how far back to scan unreleased rows
_RELEASE_VOL_MEAN_WINDOW: int = 20      # bars used for vol-baseline mean

# HM-SQUEEZE-PRE-BREAKOUT-COMPOSITE
_COMPOSITE_MIN_DURATION: int = 10       # ALERT tier or higher
_COMPOSITE_RANGE_WINDOW: int = 20       # bars for high/low range
_COMPOSITE_RANGE_POS_FLOOR: float = 75.0    # price in top 25% of 20d range
_COMPOSITE_ATR_WINDOW: int = 20         # ATR window for vol baseline
_COMPOSITE_VOL_CONTRACT_CEIL: float = 0.85  # today_atr <= 85% of prior mean
_COMPOSITE_RS_FLOOR: int = 80           # rs_rank threshold for composite_rs_pass

# HM-SQUEEZE-PRE-BREAKOUT-WATCH 2026-05-24 — precursor alert: tight to
# the 10d ceiling, coil still active, volume normal (not yet breakout).
_PREWATCH_MIN_DURATION: int = 10        # same ALERT-tier floor as composite
_PREWATCH_HIGH_WINDOW: int = 10         # 10-day reference high
_PREWATCH_DIST_TO_HIGH_PCT: float = 2.0 # within ±2% of 10d high
_PREWATCH_VOL_LO: float = 0.7           # neutral vol band (avoid contractions
_PREWATCH_VOL_HI: float = 1.3           # AND pre-spikes — both signal release)
_PREWATCH_VOL_MEAN_WINDOW: int = 20

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


def _compute_composite_factors(df: pd.DataFrame) -> tuple[float, float]:
    """Returns (range_position_pct, vol_contracting_pct).

    - ``range_position_pct``: where close[-1] sits inside the 20-day range
      as 0–100. 100 = at 20d high, 0 = at 20d low. NaN if range collapsed
      (high == low) or history too short.
    - ``vol_contracting_pct``: today's ATR(20) divided by the mean of the
      prior 20 ATR(20) values. <1.0 = volatility shrinking. NaN if
      insufficient history.
    """
    if df is None or df.empty:
        return (float("nan"), float("nan"))
    highs = df["High"].to_numpy(dtype=float)
    lows = df["Low"].to_numpy(dtype=float)
    closes = df["Close"].to_numpy(dtype=float)
    n = len(closes)
    if n < _COMPOSITE_RANGE_WINDOW + _COMPOSITE_ATR_WINDOW + 1:
        return (float("nan"), float("nan"))

    # Range position in last 20 bars
    win_high = float(np.nanmax(highs[-_COMPOSITE_RANGE_WINDOW:]))
    win_low = float(np.nanmin(lows[-_COMPOSITE_RANGE_WINDOW:]))
    close_now = float(closes[-1])
    if win_high <= win_low:
        range_position_pct = float("nan")
    else:
        range_position_pct = (
            (close_now - win_low) / (win_high - win_low) * 100.0
        )

    # Vol contraction: today's ATR vs prior 20-day ATR mean
    atr_series = _atr_series(highs, lows, closes, _COMPOSITE_ATR_WINDOW)
    today_atr = float(atr_series[-1])
    prior = atr_series[-(_COMPOSITE_ATR_WINDOW + 1) : -1]
    prior_valid = prior[~np.isnan(prior)]
    if today_atr <= 0 or len(prior_valid) == 0:
        vol_contracting_pct = float("nan")
    else:
        prior_mean = float(np.mean(prior_valid))
        vol_contracting_pct = (
            (today_atr / prior_mean) if prior_mean > 0 else float("nan")
        )

    return (range_position_pct, vol_contracting_pct)


def _compute_prebreakout_factors(df: pd.DataFrame) -> tuple[float, float]:
    """Returns (dist_to_10d_high_pct, neutral_vol_ratio).

    - ``dist_to_10d_high_pct`` = abs(close[-1] - high_10d) / high_10d * 100.
      Lower = tighter to the ceiling. NaN if history < 10 bars.
    - ``neutral_vol_ratio`` = volume[-1] / mean(volume[-21:-1]). The
      pre-breakout-watch flag fires only when this is in [0.7, 1.3] — both
      contraction-died (<0.7) and pre-spike (>1.3) signal release, not watch.
      NaN if insufficient history or no volume.
    """
    if df is None or df.empty:
        return (float("nan"), float("nan"))
    highs = df["High"].to_numpy(dtype=float)
    closes = df["Close"].to_numpy(dtype=float)
    volumes = df["Volume"].to_numpy(dtype=float)
    n = len(closes)
    if n < max(_PREWATCH_HIGH_WINDOW + 1, _PREWATCH_VOL_MEAN_WINDOW + 1):
        return (float("nan"), float("nan"))

    win_high = float(np.nanmax(highs[-_PREWATCH_HIGH_WINDOW:]))
    close_now = float(closes[-1])
    if win_high <= 0:
        dist_pct = float("nan")
    else:
        dist_pct = abs(close_now - win_high) / win_high * 100.0

    today_vol = float(volumes[-1])
    prior = volumes[-(_PREWATCH_VOL_MEAN_WINDOW + 1) : -1]
    prior_valid = prior[~np.isnan(prior)]
    if today_vol <= 0 or len(prior_valid) == 0:
        vol_ratio = float("nan")
    else:
        prior_mean = float(np.mean(prior_valid))
        vol_ratio = (
            (today_vol / prior_mean) if prior_mean > 0 else float("nan")
        )
    return (dist_pct, vol_ratio)


def _load_rs_pass_set(
    threshold: int = _COMPOSITE_RS_FLOOR, db_path: str | None = None
) -> set[str]:
    """One-shot read from rs_rank — returns set of symbols with
    rs_rank >= threshold. Empty if rs_rank table missing / unpopulated."""
    db_path = db_path or _DB_PATH
    try:
        conn = _conn(db_path)
        try:
            rows = conn.execute(
                "SELECT symbol FROM rs_rank WHERE rs_rank >= ?",
                (int(threshold),),
            ).fetchall()
            return {r["symbol"] for r in rows}
        finally:
            conn.close()
    except Exception as e:
        console.log(
            f"[dim]bbkc_squeeze: rs_rank table empty/missing "
            f"({type(e).__name__}: {e!r}) — composite_rs_pass=0 for all"
        )
        return set()


def _detect_release(
    df: pd.DataFrame,
) -> tuple[bool, str | None, float, float, float]:
    """Inspect the most-recent bar for a fresh BB-out-of-KC release.

    A release fires when bar[-2] was BB-inside-KC AND bar[-1] is NOT.
    Direction:
      - bb_upper > kc_upper → 'up'
      - bb_lower < kc_lower → 'down'
      - both true (rare)    → larger excess wins

    Returns ``(released, direction, volume_ratio, last_close, excess)``.
    ``volume_ratio`` is volume[-1] / mean(volume[-21:-1]). The caller
    decides whether to NTFY based on ``_RELEASE_VOL_GATE``.
    """
    if len(df) < _MIN_BARS_REQUIRED + 1:
        return (False, None, 0.0, 0.0, 0.0)

    bands = _compute_bands(df)
    in_sq = (bands["bb_upper"] < bands["kc_upper"]) & (
        bands["bb_lower"] > bands["kc_lower"]
    )
    in_sq = in_sq.fillna(False).to_numpy()
    if len(in_sq) < 2:
        return (False, None, 0.0, 0.0, 0.0)
    # bar[-2] must have been in squeeze, bar[-1] must NOT be
    if not (bool(in_sq[-2]) and not bool(in_sq[-1])):
        return (False, None, 0.0, 0.0, 0.0)

    # BB and KC are both symmetric around SMA(20), so bb_upper-kc_upper and
    # kc_lower-bb_lower are equal in magnitude — the band geometry alone
    # cannot distinguish direction. The actual directional signal is the
    # close: a release fires UP when close pierces the prior bar's BB upper
    # (or simply moves up), DOWN when it pierces the lower. Use close
    # position vs prior bar's BB edges, falling back to close direction.
    last = bands.iloc[-1]
    prev = bands.iloc[-2]
    close_now = float(df["Close"].iloc[-1])
    close_prev = float(df["Close"].iloc[-2])
    prev_bb_upper = float(prev["bb_upper"]) if pd.notna(prev["bb_upper"]) else close_prev
    prev_bb_lower = float(prev["bb_lower"]) if pd.notna(prev["bb_lower"]) else close_prev

    excess = abs(float(last["bb_upper"]) - float(last["kc_upper"]))
    if close_now > prev_bb_upper:
        direction = "up"
    elif close_now < prev_bb_lower:
        direction = "down"
    elif close_now > close_prev:
        direction = "up"
    elif close_now < close_prev:
        direction = "down"
    else:
        return (False, None, 0.0, 0.0, 0.0)

    # Volume confirmation: today vs prior 20-day mean (today excluded)
    volumes = df["Volume"].to_numpy(dtype=float)
    today_vol = float(volumes[-1])
    prior = volumes[-(_RELEASE_VOL_MEAN_WINDOW + 1) : -1]
    prior_mean = float(np.mean(prior)) if len(prior) and np.mean(prior) > 0 else 0.0
    vol_ratio = (today_vol / prior_mean) if prior_mean > 0 else 0.0

    last_close = float(df["Close"].iloc[-1])
    return (True, direction, vol_ratio, last_close, excess)


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
    """Idempotent: add kind/bbkc_duration_days/release_* columns if missing.
    Mirrors the HM-DASH.4 pattern in engine/squeeze_scanner.py — fresh DBs /
    migration drift are handled without crashing the scan."""
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
        # HM-SQUEEZE-RELEASE-DETECT 2026-05-24
        if "released_at" not in cols:
            conn.execute("ALTER TABLE squeeze_watch ADD COLUMN released_at TEXT")
        if "release_direction" not in cols:
            conn.execute("ALTER TABLE squeeze_watch ADD COLUMN release_direction TEXT")
        if "release_volume_ratio" not in cols:
            conn.execute("ALTER TABLE squeeze_watch ADD COLUMN release_volume_ratio REAL")
        if "release_close" not in cols:
            conn.execute("ALTER TABLE squeeze_watch ADD COLUMN release_close REAL")
        # HM-SQUEEZE-PRE-BREAKOUT-COMPOSITE 2026-05-24
        if "composite_pass" not in cols:
            conn.execute(
                "ALTER TABLE squeeze_watch ADD COLUMN composite_pass INTEGER DEFAULT 0"
            )
        if "range_position_pct" not in cols:
            conn.execute(
                "ALTER TABLE squeeze_watch ADD COLUMN range_position_pct REAL"
            )
        if "vol_contracting_pct" not in cols:
            conn.execute(
                "ALTER TABLE squeeze_watch ADD COLUMN vol_contracting_pct REAL"
            )
        if "composite_rs_pass" not in cols:
            conn.execute(
                "ALTER TABLE squeeze_watch ADD COLUMN composite_rs_pass INTEGER DEFAULT 0"
            )
        # HM-SQUEEZE-PRE-BREAKOUT-WATCH 2026-05-24
        if "pre_breakout_watch" not in cols:
            conn.execute(
                "ALTER TABLE squeeze_watch ADD COLUMN pre_breakout_watch INTEGER DEFAULT 0"
            )
        if "dist_to_10d_high_pct" not in cols:
            conn.execute(
                "ALTER TABLE squeeze_watch ADD COLUMN dist_to_10d_high_pct REAL"
            )
        if "neutral_vol_ratio" not in cols:
            conn.execute(
                "ALTER TABLE squeeze_watch ADD COLUMN neutral_vol_ratio REAL"
            )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_squeeze_watch_kind_ts "
            "ON squeeze_watch(kind, scan_ts DESC) WHERE dismissed = 0"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_squeeze_watch_release "
            "ON squeeze_watch(kind, released_at DESC) "
            "WHERE released_at IS NOT NULL"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_squeeze_watch_composite "
            "ON squeeze_watch(kind, composite_pass DESC, scan_ts DESC) "
            "WHERE composite_pass = 1 AND dismissed = 0"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_squeeze_watch_prebreakout "
            "ON squeeze_watch(kind, pre_breakout_watch DESC, scan_ts DESC) "
            "WHERE pre_breakout_watch = 1 AND dismissed = 0"
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

            # HM-SQUEEZE-PRE-BREAKOUT-COMPOSITE — composite flags + factors
            composite_pass_flag = 1 if r.get("composite_pass") else 0
            composite_rs_pass_flag = 1 if r.get("composite_rs_pass") else 0
            range_pos_val = r.get("range_position_pct")
            vol_contract_val = r.get("vol_contracting_pct")
            # HM-SQUEEZE-PRE-BREAKOUT-WATCH — precursor flag + factors
            prewatch_flag = 1 if r.get("pre_breakout_watch") else 0
            dist_10d_val = r.get("dist_to_10d_high_pct")
            neutral_vol_val = r.get("neutral_vol_ratio")

            notes_extra = ""
            if composite_pass_flag:
                rp_disp = f"{range_pos_val:.1f}" if range_pos_val is not None else "—"
                vc_disp = f"{vol_contract_val:.2f}" if vol_contract_val is not None else "—"
                notes_extra += (
                    f"; composite=YES range_pos={rp_disp}% "
                    f"vol_contract={vc_disp}× "
                    f"rs80={'YES' if composite_rs_pass_flag else 'no'}"
                )
            if prewatch_flag:
                d_disp = f"{dist_10d_val:.2f}" if dist_10d_val is not None else "—"
                nv_disp = f"{neutral_vol_val:.2f}" if neutral_vol_val is not None else "—"
                notes_extra += (
                    f"; prewatch=YES dist_10d={d_disp}% neutral_vol={nv_disp}×"
                )
            notes = (
                f"duration_days={duration}; "
                f"bb_width_pct={r.get('bb_width_pct'):.2f}; "
                f"kc_width_pct={r.get('kc_width_pct'):.2f}; "
                f"tightness={r.get('tightness'):.2f}"
                f"{notes_extra}"
            )
            # breakout_score reused as "tightness" (1 - bb/kc ratio); 0→1 range
            breakout = float(r.get("tightness", 0.0))
            # NTFY eligibility: PRIORITY entry OR composite OR pre-breakout-watch
            ntfy_eligible_tier = (
                (tier == "PRIORITY")
                or bool(composite_pass_flag)
                or bool(prewatch_flag)
            )
            ntfy_deferred = 1 if (ntfy_eligible_tier and quiet) else 0
            price_at_scan = float(r.get("last_close", 0.0))

            try:
                conn.execute(
                    """INSERT INTO squeeze_watch
                       (symbol, scan_ts, short_pct, float_m, vol_ratio, rsi,
                        breakout_score, composite_score, threshold_tier,
                        price_at_scan, notes, ntfy_sent, ntfy_deferred,
                        kind, bbkc_duration_days,
                        composite_pass, range_position_pct,
                        vol_contracting_pct, composite_rs_pass,
                        pre_breakout_watch, dist_to_10d_high_pct,
                        neutral_vol_ratio)
                       VALUES (?, ?, NULL, NULL, NULL, NULL,
                               ?, ?, ?,
                               ?, ?, 0, ?,
                               'bbkc', ?,
                               ?, ?, ?, ?,
                               ?, ?, ?)""",
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
                        composite_pass_flag,
                        range_pos_val,
                        vol_contract_val,
                        composite_rs_pass_flag,
                        prewatch_flag,
                        dist_10d_val,
                        neutral_vol_val,
                    ),
                )
                summary["inserted"] += 1
                if ntfy_deferred:
                    summary["deferred"] += 1

                # NTFY routing — priority order (single fire per insertion):
                # 1. composite_pass=1 → 🎯 composite NTFY
                # 2. pre_breakout_watch=1 (and not composite) → 👀 prewatch NTFY
                # 3. plain PRIORITY tier entry → 🔥 entry NTFY
                # ALERT-tier without composite/prewatch → no NTFY (existing rule).
                if (
                    ntfy_eligible_tier
                    and not ntfy_deferred
                    and ntfy_fired_this_run < _NTFY_PER_RUN_CAP
                ):
                    fire_fn_result = False
                    if composite_pass_flag:
                        fire_fn_result = _fire_composite_ntfy(
                            symbol,
                            duration,
                            range_pos_val,
                            vol_contract_val,
                            bool(composite_rs_pass_flag),
                        )
                    elif prewatch_flag:
                        fire_fn_result = _fire_pre_breakout_watch_ntfy(
                            symbol,
                            duration,
                            dist_10d_val,
                            neutral_vol_val,
                        )
                    elif tier == "PRIORITY":
                        fire_fn_result = _fire_ntfy(symbol, duration)
                    if fire_fn_result:
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


def _fire_pre_breakout_watch_ntfy(
    symbol: str,
    duration: int,
    dist_pct: float | None,
    vol_ratio: float | None,
) -> bool:
    """HM-SQUEEZE-PRE-BREAKOUT-WATCH NTFY. Precursor-level alert — fires
    when the symbol is tight to the 10d ceiling with neutral volume but
    hasn't released yet. Per-symbol 24h rate-limit + in-process dedupe."""
    key = f"bbkc_prewatch::{symbol}"
    if key in _ntfy_fired_classes:
        return False
    _ntfy_fired_classes.add(key)
    try:
        from engine.alert_channels import send_alert

        dist_str = f"{dist_pct:.2f}%" if dist_pct is not None else "—"
        vol_str = f"{vol_ratio:.2f}×" if vol_ratio is not None else "—"
        send_alert(
            level="warning",
            alert_type=f"bbkc_squeeze_prewatch_{symbol}",
            message=(
                f"👀 BB/KC Pre-Breakout WATCH — {symbol} ({duration}d coil, "
                f"{dist_str} from 10d high, vol {vol_str}). Tight to the "
                f"ceiling — precursor to release."
            ),
            title=f"👀 BB/KC Pre-Breakout WATCH — {symbol}",
            audience="admin",
            rate_limit_secs=86400,
        )
        return True
    except Exception as e:
        console.log(
            f"[yellow]bbkc_squeeze: prewatch NTFY {symbol} failed: "
            f"{type(e).__name__}: {e!r}"
        )
        return False


def _fire_composite_ntfy(
    symbol: str,
    duration: int,
    range_pos: float | None,
    vol_contract: float | None,
    rs_pass: bool,
) -> bool:
    """HM-SQUEEZE-PRE-BREAKOUT-COMPOSITE NTFY. Replaces the plain entry
    NTFY for composite hits — single notification per insertion. Per-
    symbol 24h rate-limit + in-process dedupe."""
    key = f"bbkc_composite::{symbol}"
    if key in _ntfy_fired_classes:
        return False
    _ntfy_fired_classes.add(key)
    try:
        from engine.alert_channels import send_alert

        rp_str = f"{range_pos:.0f}%" if range_pos is not None else "—"
        vc_str = f"{vol_contract:.2f}×" if vol_contract is not None else "—"
        rs_tag = " RS✓" if rs_pass else ""
        send_alert(
            level="warning",
            alert_type=f"bbkc_squeeze_composite_{symbol}",
            message=(
                f"🎯 BB/KC Pre-Breakout — {symbol} ({duration}d coil, "
                f"top {rp_str} of 20d range, vol {vc_str}){rs_tag}. "
                f"Directional-bias setup — coiling under the lid."
            ),
            title=f"🎯 BB/KC Pre-Breakout — {symbol}{rs_tag}",
            audience="admin",
            rate_limit_secs=86400,
        )
        return True
    except Exception as e:
        console.log(
            f"[yellow]bbkc_squeeze: composite NTFY {symbol} failed: "
            f"{type(e).__name__}: {e!r}"
        )
        return False


def _fire_release_ntfy(
    symbol: str, direction: str, duration: int, vol_ratio: float
) -> bool:
    """NTFY a BB/KC release. Per-symbol 24h rate-limit via alert_type +
    in-process dedupe (a single squeeze row can only release once). Returns
    True if the send_alert call ran, False on dedupe-skip."""
    key = f"bbkc_release::{symbol}"
    if key in _ntfy_fired_classes:
        return False
    _ntfy_fired_classes.add(key)
    try:
        from engine.alert_channels import send_alert

        arrow = "↑" if direction == "up" else "↓"
        send_alert(
            level="warning",
            alert_type=f"bbkc_squeeze_release_{symbol}",
            message=(
                f"🚀 BB/KC Squeeze RELEASE — {symbol} {arrow} "
                f"({duration}d coil broken). Volume {vol_ratio:.1f}× the "
                f"20d mean."
            ),
            title=f"🚀 BB/KC Squeeze RELEASE — {symbol} {arrow}",
            audience="admin",
            rate_limit_secs=86400,
        )
        return True
    except Exception as e:
        console.log(
            f"[yellow]bbkc_squeeze: release NTFY {symbol} failed: "
            f"{type(e).__name__}: {e!r}"
        )
        return False


def _scan_for_releases(
    bars_by_sym: dict,
    db_path: str | None = None,
    remaining_ntfy_budget: int = 0,
) -> dict:
    """Second pass: for every unreleased ALERT/PRIORITY bbkc row scanned in
    the last _RELEASE_LOOKBACK_DAYS days, run _detect_release() against
    today's bar. UPDATE the row with release_* columns when a release fires.
    NTFY only when ``vol_ratio >= _RELEASE_VOL_GATE`` AND budget remaining.

    Returns ``{detected, ntfy_fired, deferred, skipped}``.
    """
    db_path = db_path or _DB_PATH
    summary = {"detected": 0, "ntfy_fired": 0, "deferred": 0, "skipped": 0}
    quiet = _is_quiet_hours_et()
    now_utc = datetime.now(timezone.utc)
    lookback_cutoff = (
        now_utc - timedelta(days=_RELEASE_LOOKBACK_DAYS)
    ).isoformat()
    released_at = now_utc.isoformat()

    try:
        conn = _conn(db_path)
    except Exception as e:
        console.log(
            f"[yellow]bbkc_squeeze: release-scan conn failed: "
            f"{type(e).__name__}: {e!r}"
        )
        return summary

    fired_this_run = 0
    try:
        _ensure_schema(conn)
        rows = conn.execute(
            """SELECT id, symbol, threshold_tier, bbkc_duration_days
                 FROM squeeze_watch
                WHERE kind = 'bbkc'
                  AND dismissed = 0
                  AND released_at IS NULL
                  AND threshold_tier IN ('ALERT', 'PRIORITY')
                  AND scan_ts >= ?
                ORDER BY scan_ts DESC""",
            (lookback_cutoff,),
        ).fetchall()

        for row in rows:
            symbol = row["symbol"]
            df = bars_by_sym.get(symbol)
            if df is None or df.empty:
                continue
            try:
                released, direction, vol_ratio, last_close, _excess = (
                    _detect_release(df)
                )
            except Exception as e:
                console.log(
                    f"[yellow]bbkc_squeeze: release-detect {symbol}: "
                    f"{type(e).__name__}: {e!r}"
                )
                continue
            if not released:
                continue

            summary["detected"] += 1
            try:
                conn.execute(
                    "UPDATE squeeze_watch "
                    "SET released_at = ?, release_direction = ?, "
                    "    release_volume_ratio = ?, release_close = ? "
                    "WHERE id = ?",
                    (
                        released_at,
                        direction,
                        float(vol_ratio),
                        float(last_close),
                        row["id"],
                    ),
                )
            except Exception as e:
                console.log(
                    f"[yellow]bbkc_squeeze: release UPDATE {symbol} failed: "
                    f"{type(e).__name__}: {e!r}"
                )
                continue

            # NTFY gate: volume + budget + not quiet-hours-deferred.
            duration = int(row["bbkc_duration_days"] or 0)
            if vol_ratio < _RELEASE_VOL_GATE:
                summary["skipped"] += 1
                continue
            if quiet:
                summary["deferred"] += 1
                continue
            if fired_this_run >= remaining_ntfy_budget:
                summary["skipped"] += 1
                continue
            if _fire_release_ntfy(symbol, direction, duration, vol_ratio):
                summary["ntfy_fired"] += 1
                fired_this_run += 1

        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return summary


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

        # HM-SQUEEZE-PRE-BREAKOUT-COMPOSITE — pre-load rs_rank ≥ 80 set once
        # for the bonus composite_rs_pass field (LEFT JOIN at scan time).
        rs_pass_set = _load_rs_pass_set()

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

            # HM-SQUEEZE-PRE-BREAKOUT-COMPOSITE — compute factors + flag pass
            try:
                range_pos, vol_contract = _compute_composite_factors(df)
            except Exception as e:
                console.log(
                    f"[yellow]bbkc_squeeze: composite factors {symbol}: "
                    f"{type(e).__name__}: {e!r}"
                )
                range_pos, vol_contract = float("nan"), float("nan")
            composite_pass = bool(
                duration >= _COMPOSITE_MIN_DURATION
                and not np.isnan(range_pos)
                and range_pos >= _COMPOSITE_RANGE_POS_FLOOR
                and not np.isnan(vol_contract)
                and vol_contract <= _COMPOSITE_VOL_CONTRACT_CEIL
            )
            composite_rs_pass = symbol in rs_pass_set

            # HM-SQUEEZE-PRE-BREAKOUT-WATCH — tighter precursor flag
            try:
                dist_to_10d, neutral_vol = _compute_prebreakout_factors(df)
            except Exception as e:
                console.log(
                    f"[yellow]bbkc_squeeze: prewatch factors {symbol}: "
                    f"{type(e).__name__}: {e!r}"
                )
                dist_to_10d, neutral_vol = float("nan"), float("nan")
            pre_breakout_watch = bool(
                duration >= _PREWATCH_MIN_DURATION
                and not np.isnan(dist_to_10d)
                and dist_to_10d <= _PREWATCH_DIST_TO_HIGH_PCT
                and not np.isnan(neutral_vol)
                and _PREWATCH_VOL_LO <= neutral_vol <= _PREWATCH_VOL_HI
            )

            results.append(
                {
                    "symbol": symbol,
                    "duration_days": duration,
                    "bb_width_pct": bb_w,
                    "kc_width_pct": kc_w,
                    "tightness": max(0.0, min(1.0, tightness)),
                    "last_close": last_close,
                    "tier": _tier_for_duration(duration),
                    "range_position_pct": (
                        None if np.isnan(range_pos) else float(range_pos)
                    ),
                    "vol_contracting_pct": (
                        None if np.isnan(vol_contract) else float(vol_contract)
                    ),
                    "composite_pass": composite_pass,
                    "composite_rs_pass": composite_rs_pass,
                    "pre_breakout_watch": pre_breakout_watch,
                    "dist_to_10d_high_pct": (
                        None if np.isnan(dist_to_10d) else float(dist_to_10d)
                    ),
                    "neutral_vol_ratio": (
                        None if np.isnan(neutral_vol) else float(neutral_vol)
                    ),
                }
            )

        # Sort: composite_pass DESC (true first), then duration DESC, then tightness DESC.
        # Composite hits surface at the top of the BB/KC tab regardless of filter state.
        results.sort(
            key=lambda r: (
                1 if r["composite_pass"] else 0,
                r["duration_days"],
                r["tightness"],
            ),
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

        # HM-SQUEEZE-RELEASE-DETECT — second pass: detect releases of
        # previously-watched coils using the SAME bars dict (no re-fetch).
        # Shares the per-run NTFY cap with the entry-NTFY pass.
        release_summary: dict = {}
        try:
            entry_ntfy = int(persist_summary.get("ntfy_fired", 0) or 0)
            budget = max(0, _NTFY_PER_RUN_CAP - entry_ntfy)
            release_summary = _scan_for_releases(
                bars_by_sym, remaining_ntfy_budget=budget
            )
        except Exception as e:
            console.log(
                f"[yellow]bbkc_squeeze: release-scan top-level failed: "
                f"{type(e).__name__}: {e!r}"
            )

        composite_count = sum(1 for r in results if r.get("composite_pass"))
        prewatch_count = sum(1 for r in results if r.get("pre_breakout_watch"))

        _last_result = {
            "results": results,
            "scanned_at": datetime.now().isoformat(),
            "candidate_count": scanned,
            "composite_count": composite_count,
            "prewatch_count": prewatch_count,
            "watch_persist": persist_summary,
            "release_scan": release_summary,
        }
        _last_scan_ts = time.time()

        console.log(
            f"[green]BBKC Squeeze Scanner: scanned={scanned} "
            f"in_squeeze={len(results)} composite={composite_count} "
            f"prewatch={prewatch_count} "
            f"inserted={persist_summary.get('inserted', 0)} "
            f"ntfy={persist_summary.get('ntfy_fired', 0)} · "
            f"released={release_summary.get('detected', 0)} "
            f"rel_ntfy={release_summary.get('ntfy_fired', 0)}"
        )
        return _last_result


__all__ = ["run_scan"]
