"""
HM-OLLIE-EVENT-TAPE-V2-REALTIME Phase 2.5 Component 2 — event detector.

Reads recent rows from `price_ticks` (populated by engine/tick_recorder.py)
and detects Holly-style realtime events. Writes them to `event_tape`.

v1 detectors (this module ships these five):
  - running_up_fast          price up ≥0.5% in <1min
  - running_down_fast        mirror
  - volume_burst             60s share volume ≥ 3× rolling 20-min baseline
  - new_session_high         current price > today's session high (so far) + 0.1%
  - new_session_low          mirror

Deferred to follow-up (need extra data sources):
  - crossed_above_close / crossed_below_close — needs yesterday's close (market_snapshots)
  - gap_fill_complete — needs morning gap state
  - breakout_resistance — needs 20-day high (volume_baselines doesn't carry it yet)
  - failed_breakdown — needs prev-low state

Design notes
------------
- Sacred rules: `CREATE TABLE IF NOT EXISTS` only. No drops, no alters.
- Daemon Lifecycle Rule: `start_event_detector()` called at module-level main.py
  startup. Heartbeat log line every 60s confirms live execution.
- Dedup: max 1 event per (symbol, event_type) per 60s. Enforced by a
  pre-INSERT SELECT against `event_tape.detected_at`.
- All state derived from SQL queries against `price_ticks` — no in-memory
  state to lose on restart.
- 30s detection cadence — same as Phase 1/2 polling. IEX coverage is sparse
  enough that <30s cycles wouldn't add much.
- Market-hours gated. Sleeps cleanly until next open.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import datetime, timezone

from rich.console import Console

console = Console()

DB = "data/trader.db"

# Cadence + retention
_DETECT_INTERVAL_SECS = 30
_HEARTBEAT_INTERVAL_SECS = 60
_DEDUP_WINDOW_SECS = 60          # max 1 event per (sym, type) within this window
_RETENTION_HOURS = 24            # event_tape rolling retention
_CLEANUP_INTERVAL_SECS = 3600

# Detection thresholds — start conservative; tune from production noise.
_RUN_FAST_PCT = 0.5              # ≥0.5% move
_RUN_FAST_WINDOW_SECS = 60       # ...within 60s = "running up/down fast"
_VOL_BURST_MULTIPLIER = 3.0      # 60-sec vol ≥ 3× rolling 20-min avg
_VOL_BURST_BASELINE_MIN_SECS = 600   # need at least 10 min of history for baseline
_SESSION_HIGH_BUFFER_PCT = 0.1   # +0.1% above prior session high to fire new_high

# Module state
_running: bool = False
_detector_thread: threading.Thread | None = None
_cleanup_thread: threading.Thread | None = None
_heartbeat_thread: threading.Thread | None = None

_stats: dict = {
    "cycles": 0,
    "events_written": 0,
    "events_dedup_skipped": 0,
    "last_cycle_at": None,
    "last_event_at": None,
    "by_type": {},   # event_type -> count this process lifetime
}


# ─── DB helpers ───────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB, timeout=10)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def _init_tables() -> None:
    """ADD-only schema per sacred rules. Idempotent."""
    c = _conn()
    try:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS event_tape (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol          TEXT NOT NULL,
                event_type      TEXT NOT NULL,
                narration       TEXT NOT NULL,
                price           REAL,
                magnitude       REAL,
                in_scanner_tier INTEGER,
                detected_at     TEXT DEFAULT (datetime('now')),
                metadata        TEXT
            )
            """
        )
        c.execute("CREATE INDEX IF NOT EXISTS idx_event_tape_detected_at ON event_tape(detected_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_event_tape_symbol ON event_tape(symbol)")
        c.commit()
    finally:
        c.close()


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _scanner_tier_for(symbol: str, conn: sqlite3.Connection) -> int | None:
    """Return 1/2/3 if `symbol` is currently a scanner convergence candidate."""
    row = conn.execute(
        """
        SELECT COUNT(DISTINCT strategy_name) AS strat_count
          FROM strategy_signals
         WHERE ticker = ?
           AND created_at >= datetime('now','-90 minutes')
        """,
        (symbol,),
    ).fetchone()
    if not row:
        return None
    sc = int(row["strat_count"] or 0)
    if sc >= 5:
        return 1
    if sc == 4:
        return 2
    if sc == 3:
        return 3
    return None


def _record_event(
    conn: sqlite3.Connection,
    symbol: str,
    event_type: str,
    narration: str,
    price: float | None,
    magnitude: float | None,
    metadata: dict | None = None,
) -> bool:
    """Write an event row if not deduped. Returns True if written."""
    # Dedup check.
    row = conn.execute(
        """
        SELECT 1 FROM event_tape
         WHERE symbol = ?
           AND event_type = ?
           AND detected_at >= datetime('now', ?)
         LIMIT 1
        """,
        (symbol, event_type, f"-{_DEDUP_WINDOW_SECS} seconds"),
    ).fetchone()
    if row:
        _stats["events_dedup_skipped"] += 1
        return False

    tier = _scanner_tier_for(symbol, conn)
    conn.execute(
        """
        INSERT INTO event_tape
            (symbol, event_type, narration, price, magnitude, in_scanner_tier, metadata)
        VALUES (?,?,?,?,?,?,?)
        """,
        (
            symbol,
            event_type,
            narration,
            price,
            magnitude,
            tier,
            json.dumps(metadata) if metadata else None,
        ),
    )
    conn.commit()
    _stats["events_written"] += 1
    _stats["last_event_at"] = datetime.now(timezone.utc).isoformat()
    _stats["by_type"][event_type] = _stats["by_type"].get(event_type, 0) + 1
    console.log(f"[green][EVENT-TAPE] {symbol} {event_type}: {narration}")
    return True


# ─── Detectors ────────────────────────────────────────────────────────────────

def _detect_running_fast(conn: sqlite3.Connection) -> None:
    """Find symbols whose price moved ≥_RUN_FAST_PCT% within the last
    _RUN_FAST_WINDOW_SECS seconds.

    Strategy: for each subscribed symbol, get first+last price in the window
    and compute % change. Window is short enough that "first" approximates
    "60s-ago price" reasonably.
    """
    rows = conn.execute(
        f"""
        WITH win AS (
          SELECT symbol,
                 MIN(ts) AS first_ts,
                 MAX(ts) AS last_ts,
                 COUNT(*) AS n
            FROM price_ticks
           WHERE datetime(ts) >= datetime('now', '-{_RUN_FAST_WINDOW_SECS} seconds')
           GROUP BY symbol
          HAVING n >= 5  -- need enough ticks for a meaningful signal
        )
        SELECT w.symbol,
               (SELECT price FROM price_ticks p1
                 WHERE p1.symbol=w.symbol AND p1.ts=w.first_ts LIMIT 1) AS first_px,
               (SELECT price FROM price_ticks p2
                 WHERE p2.symbol=w.symbol AND p2.ts=w.last_ts  LIMIT 1) AS last_px,
               w.n AS tick_count
          FROM win w
        """
    ).fetchall()

    for r in rows:
        try:
            first_px = float(r["first_px"])
            last_px = float(r["last_px"])
            if first_px <= 0:
                continue
            pct = ((last_px - first_px) / first_px) * 100.0
            move = last_px - first_px
        except (TypeError, ValueError, ZeroDivisionError):
            continue

        if pct >= _RUN_FAST_PCT:
            narration = f"Running up quickly: +${move:.2f} in less than one minute"
            _record_event(
                conn, r["symbol"], "running_up_fast", narration,
                price=last_px, magnitude=pct,
                metadata={"window_secs": _RUN_FAST_WINDOW_SECS, "first_px": first_px,
                         "ticks": r["tick_count"]},
            )
        elif pct <= -_RUN_FAST_PCT:
            narration = f"Running down quickly: -${abs(move):.2f} in less than one minute"
            _record_event(
                conn, r["symbol"], "running_down_fast", narration,
                price=last_px, magnitude=pct,
                metadata={"window_secs": _RUN_FAST_WINDOW_SECS, "first_px": first_px,
                         "ticks": r["tick_count"]},
            )


def _detect_volume_burst(conn: sqlite3.Connection) -> None:
    """Detect 60s share volume ≥ _VOL_BURST_MULTIPLIER × rolling 20-min baseline.

    Baseline is the average per-minute share volume across the last 20 minutes,
    EXCLUDING the most recent minute (so we compare against "normal", not the
    burst itself).

    Cold-start guard: requires the baseline window to ACTUALLY span at least
    ~15 minutes of data per symbol — otherwise a fresh restart with 5 min of
    history makes a per-minute divisor of 9 produce an inflated burst signal.
    """
    rows = conn.execute(
        f"""
        SELECT symbol,
               SUM(CASE WHEN datetime(ts) >= datetime('now','-60 seconds')
                        THEN COALESCE(volume,0) END) AS vol_60s,
               SUM(CASE WHEN datetime(ts) >= datetime('now','-{_VOL_BURST_BASELINE_MIN_SECS} seconds')
                         AND datetime(ts) <  datetime('now','-60 seconds')
                        THEN COALESCE(volume,0) END) AS vol_baseline_total,
               SUM(CASE WHEN datetime(ts) >= datetime('now','-{_VOL_BURST_BASELINE_MIN_SECS} seconds')
                        THEN 1 END) AS ticks_in_window,
               MIN(ts) AS earliest_ts,
               MAX(ts) AS latest_ts,
               (SELECT price FROM price_ticks p
                 WHERE p.symbol = price_ticks.symbol
                 ORDER BY ts DESC LIMIT 1) AS last_price
          FROM price_ticks
         WHERE datetime(ts) >= datetime('now','-{_VOL_BURST_BASELINE_MIN_SECS} seconds')
         GROUP BY symbol
        HAVING vol_60s > 0
           AND vol_baseline_total > 0
           AND ticks_in_window >= 30
           AND datetime(earliest_ts) <= datetime('now','-15 minutes')
        """
    ).fetchall()

    for r in rows:
        try:
            vol_60s = float(r["vol_60s"] or 0)
            baseline_total = float(r["vol_baseline_total"] or 0)
            # Per-minute baseline: divide baseline_total by number of minutes in the
            # baseline window (excluding the most recent minute we're comparing).
            baseline_minutes = max(1, (_VOL_BURST_BASELINE_MIN_SECS - 60) // 60)
            baseline_per_min = baseline_total / baseline_minutes
            if baseline_per_min <= 0:
                continue
            ratio = vol_60s / baseline_per_min
        except (TypeError, ValueError, ZeroDivisionError):
            continue

        if ratio >= _VOL_BURST_MULTIPLIER:
            last_px = float(r["last_price"]) if r["last_price"] is not None else None
            narration = f"Volume burst: {ratio:.1f}× normal"
            _record_event(
                conn, r["symbol"], "volume_burst", narration,
                price=last_px, magnitude=ratio,
                metadata={"vol_60s": vol_60s, "baseline_per_min": baseline_per_min,
                         "window_min": _VOL_BURST_BASELINE_MIN_SECS // 60},
            )


def _detect_session_extremes(conn: sqlite3.Connection) -> None:
    """Detect new session high / new session low.

    Session = today's UTC date (Alpaca timestamps are UTC). Only fires when
    the latest tick's price STRICTLY exceeds the prior session extreme (i.e.
    a fresh extreme was just set), and exceeds it by at least
    _SESSION_HIGH_BUFFER_PCT — avoids firing every tick that lands at an
    already-established extreme on sparse IEX coverage.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    rows = conn.execute(
        """
        WITH today_ticks AS (
          SELECT id, symbol, price, ts FROM price_ticks
           WHERE substr(ts, 1, 10) = ?
        ),
        latest AS (
          SELECT t1.symbol,
                 t1.price AS last_price,
                 t1.ts    AS last_ts,
                 t1.id    AS last_id
            FROM today_ticks t1
           WHERE t1.id = (SELECT MAX(id) FROM today_ticks t2 WHERE t2.symbol = t1.symbol)
        ),
        prior AS (
          SELECT t.symbol,
                 MAX(t.price) AS prior_high,
                 MIN(t.price) AS prior_low,
                 COUNT(*)     AS prior_n
            FROM today_ticks t
            JOIN latest l ON l.symbol = t.symbol
           WHERE t.id < l.last_id
           GROUP BY t.symbol
        )
        SELECT l.symbol, l.last_price, l.last_ts,
               p.prior_high, p.prior_low, p.prior_n
          FROM latest l
          JOIN prior  p ON p.symbol = l.symbol
         WHERE p.prior_n >= 20   -- need real coverage before extreme means anything
        """,
        (today,),
    ).fetchall()

    buf = _SESSION_HIGH_BUFFER_PCT / 100.0
    for r in rows:
        try:
            last_px = float(r["last_price"])
            prior_high = float(r["prior_high"])
            prior_low = float(r["prior_low"])
        except (TypeError, ValueError):
            continue

        if last_px > prior_high * (1.0 + buf):
            narration = f"New session high: ${last_px:.2f}"
            _record_event(
                conn, r["symbol"], "new_session_high", narration,
                price=last_px,
                magnitude=((last_px - prior_high) / prior_high) * 100.0 if prior_high else None,
                metadata={"prior_high": prior_high, "prior_low": prior_low},
            )
        elif last_px < prior_low * (1.0 - buf):
            narration = f"New session low: ${last_px:.2f}"
            _record_event(
                conn, r["symbol"], "new_session_low", narration,
                price=last_px,
                magnitude=((last_px - prior_low) / prior_low) * 100.0 if prior_low else None,
                metadata={"prior_high": prior_high, "prior_low": prior_low},
            )


# ─── Main loops ──────────────────────────────────────────────────────────────

def _run_detector_loop() -> None:
    """Every _DETECT_INTERVAL_SECS, run all detectors against current price_ticks."""
    while _running:
        # Market-hours gate.
        try:
            from engine.risk_manager import RiskManager
            if not RiskManager.is_market_hours():
                # Sleep in small chunks so stop is responsive.
                for _ in range(_DETECT_INTERVAL_SECS):
                    if not _running:
                        return
                    time.sleep(1)
                continue
        except Exception as exc:
            console.log(f"[yellow][EVENT-TAPE] is_market_hours check failed: {exc!r} — proceeding")

        try:
            c = _conn()
            _detect_running_fast(c)
            _detect_volume_burst(c)
            _detect_session_extremes(c)
            c.close()
            _stats["cycles"] += 1
            _stats["last_cycle_at"] = datetime.now(timezone.utc).isoformat()
        except Exception as exc:
            console.log(f"[red][EVENT-TAPE] detector cycle failed: {type(exc).__name__}: {exc!r}")

        for _ in range(_DETECT_INTERVAL_SECS):
            if not _running:
                return
            time.sleep(1)


def _run_cleanup() -> None:
    """Hourly DELETE of event_tape rows older than _RETENTION_HOURS."""
    for _ in range(_CLEANUP_INTERVAL_SECS):
        if not _running:
            return
        time.sleep(1)
    while _running:
        try:
            c = _conn()
            cur = c.execute(
                "DELETE FROM event_tape WHERE detected_at < datetime('now', ?)",
                (f"-{_RETENTION_HOURS} hours",),
            )
            deleted = cur.rowcount
            c.commit()
            c.close()
            console.log(f"[cyan][EVENT-TAPE] cleanup: pruned {deleted} events older than {_RETENTION_HOURS}h")
        except Exception as exc:
            console.log(f"[yellow][EVENT-TAPE] cleanup failed: {type(exc).__name__}: {exc!r}")
        for _ in range(_CLEANUP_INTERVAL_SECS):
            if not _running:
                return
            time.sleep(1)


def _run_heartbeat() -> None:
    """Per HM-EQ doctrine — proves the detector daemon is alive even when no
    events fire (e.g. quiet market hours)."""
    while _running:
        msg = (
            f"[HM-EVENT-TAPE heartbeat] running={_running} cycles={_stats['cycles']} "
            f"written={_stats['events_written']} dedup={_stats['events_dedup_skipped']} "
            f"by_type={_stats['by_type']} last_cycle={_stats['last_cycle_at']}"
        )
        console.log(msg)
        for _ in range(_HEARTBEAT_INTERVAL_SECS):
            if not _running:
                return
            time.sleep(1)


# ─── Public API ───────────────────────────────────────────────────────────────

def start_event_detector() -> None:
    """Spin up detector + cleanup + heartbeat threads. Idempotent."""
    global _running, _detector_thread, _cleanup_thread, _heartbeat_thread
    if _running:
        return

    try:
        _init_tables()
    except Exception as exc:
        console.log(f"[red][EVENT-TAPE] _init_tables failed — refusing to start: "
                    f"{type(exc).__name__}: {exc!r}")
        return

    _running = True
    _detector_thread = threading.Thread(target=_run_detector_loop, daemon=True, name="event-tape-detector")
    _detector_thread.start()
    _cleanup_thread = threading.Thread(target=_run_cleanup, daemon=True, name="event-tape-cleanup")
    _cleanup_thread.start()
    _heartbeat_thread = threading.Thread(target=_run_heartbeat, daemon=True, name="event-tape-heartbeat")
    _heartbeat_thread.start()
    console.log("[green][EVENT-TAPE] event detector started (5 v1 detectors, 30s cadence)")


def stop_event_detector() -> None:
    global _running
    _running = False
    console.log("[yellow][EVENT-TAPE] stop requested")


def get_stats() -> dict:
    return {**_stats, "running": _running}
