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

C5 Phase 2.5 detectors (HM-SCANNER-EVENT-DETECTORS-C5):
  - gap_fill_complete        price returns to prev_close after gapping; once per session
  - breakout_resistance      price > 20d high AND today_vol > 1.2× 20d avg; once per day
  - failed_breakdown         session_low < prev_low then reclaimed; once per session
  - vwap_reclaim             crosses back above session VWAP; 30-min dedup
  - power_hour_thrust        last-60min vol > 1.5× session avg in last hour ET; once per session

Deferred:
  - crossed_above_close / crossed_below_close — needs yesterday's close (market_snapshots)

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

from engine.risk_manager import RiskManager

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


# ─── Session-level dedup helpers (used by C5 detectors) ──────────────────────

def _fired_today(conn: sqlite3.Connection, symbol: str, event_type: str) -> bool:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    row = conn.execute(
        "SELECT 1 FROM event_tape WHERE symbol=? AND event_type=? "
        "AND substr(detected_at,1,10)=? LIMIT 1",
        (symbol, event_type, today),
    ).fetchone()
    return row is not None


def _fired_within(conn: sqlite3.Connection, symbol: str, event_type: str, secs: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM event_tape WHERE symbol=? AND event_type=? "
        "AND detected_at >= datetime('now', ?) LIMIT 1",
        (symbol, event_type, f"-{secs} seconds"),
    ).fetchone()
    return row is not None


def _is_power_hour_et() -> bool:
    """Return True if current ET wall-clock hour is >= 15 (i.e. last hour before close)."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York")).hour >= 15
    except Exception:
        # Fallback: approximate via UTC (US/Eastern is UTC-5 standard, UTC-4 DST).
        # 15:00 ET ≈ 19:00-20:00 UTC depending on DST. Use 19:00 UTC as floor.
        return datetime.now(timezone.utc).hour >= 19


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


# ─── C5 Phase 2.5 detectors ──────────────────────────────────────────────────

def _detect_gap_fill_complete(conn: sqlite3.Connection) -> None:
    """Fire when price returns to prior day's close after gapping open.

    Requires gap ≥ 0.5% from prev_close at today's open. Fires once per
    (symbol, session) when current price crosses back through prev_close
    from the gap side.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = conn.execute(
        """
        WITH prev AS (
          SELECT symbol, close AS prev_close
            FROM market_snapshots
           WHERE date = (SELECT MAX(date) FROM market_snapshots WHERE date < ?)
        ),
        today_ticks AS (
          SELECT symbol, price, id
            FROM price_ticks
           WHERE substr(ts,1,10) = ?
        ),
        agg AS (
          SELECT t.symbol,
                 (SELECT price FROM today_ticks WHERE symbol = t.symbol ORDER BY id ASC  LIMIT 1) AS today_open,
                 (SELECT price FROM today_ticks WHERE symbol = t.symbol ORDER BY id DESC LIMIT 1) AS current_price,
                 COUNT(*) AS n
            FROM today_ticks t
           GROUP BY t.symbol
          HAVING n >= 5
        )
        SELECT a.symbol, a.today_open, a.current_price, p.prev_close
          FROM agg a
          JOIN prev p ON p.symbol = a.symbol
        """,
        (today, today),
    ).fetchall()

    for r in rows:
        try:
            today_open = float(r["today_open"])
            current = float(r["current_price"])
            prev_close = float(r["prev_close"])
        except (TypeError, ValueError):
            continue
        if prev_close <= 0:
            continue
        gap_up = today_open > prev_close * 1.005
        gap_down = today_open < prev_close * 0.995
        if not (gap_up or gap_down):
            continue
        filled = (gap_up and current <= prev_close) or (gap_down and current >= prev_close)
        if not filled:
            continue
        if _fired_today(conn, r["symbol"], "gap_fill_complete"):
            continue
        gap_pct = abs(today_open - prev_close) / prev_close * 100.0
        direction = "down" if gap_up else "up"
        narration = f"Gap fill complete: {direction} to prev close ${prev_close:.2f}"
        _record_event(
            conn, r["symbol"], "gap_fill_complete", narration,
            price=current, magnitude=gap_pct,
            metadata={"today_open": today_open, "prev_close": prev_close,
                      "gap_pct": gap_pct, "direction": direction},
        )


def _detect_breakout_resistance(conn: sqlite3.Connection) -> None:
    """Fire when current price > 20-day high AND today's volume > 1.2× 20d avg.

    20d window pulls from market_snapshots (requires ≥15 days of history).
    Today's volume = SUM(price_ticks.volume) for today. Fires once per
    (symbol, day).
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = conn.execute(
        """
        WITH ms_20 AS (
          SELECT symbol,
                 MAX(high) AS hi_20d,
                 AVG(volume) AS avg_vol_20d,
                 COUNT(*) AS n_days
            FROM market_snapshots
           WHERE date >= date(?, '-30 days') AND date < ?
           GROUP BY symbol
          HAVING n_days >= 15
        ),
        today_agg AS (
          SELECT symbol,
                 SUM(COALESCE(volume,0)) AS today_vol,
                 (SELECT price FROM price_ticks p
                   WHERE p.symbol = price_ticks.symbol
                   ORDER BY id DESC LIMIT 1) AS last_price,
                 COUNT(*) AS n
            FROM price_ticks
           WHERE substr(ts,1,10) = ?
           GROUP BY symbol
          HAVING n >= 20
        )
        SELECT t.symbol, t.last_price, t.today_vol, m.hi_20d, m.avg_vol_20d
          FROM today_agg t
          JOIN ms_20 m ON m.symbol = t.symbol
         WHERE t.last_price > m.hi_20d
           AND t.today_vol > m.avg_vol_20d * 1.2
        """,
        (today, today, today),
    ).fetchall()

    for r in rows:
        if _fired_today(conn, r["symbol"], "breakout_resistance"):
            continue
        try:
            last = float(r["last_price"])
            hi20 = float(r["hi_20d"])
            if hi20 <= 0:
                continue
            pct_over = (last - hi20) / hi20 * 100.0
        except (TypeError, ValueError):
            continue
        narration = f"Breakout above 20d high: ${last:.2f} (+{pct_over:.2f}% over ${hi20:.2f})"
        _record_event(
            conn, r["symbol"], "breakout_resistance", narration,
            price=last, magnitude=pct_over,
            metadata={"hi_20d": hi20,
                      "today_vol": float(r["today_vol"] or 0),
                      "avg_vol_20d": float(r["avg_vol_20d"] or 0)},
        )


def _detect_failed_breakdown(conn: sqlite3.Connection) -> None:
    """Fire when session_low < prev_low AND current_price > prev_low.

    Captures a false-breakdown reclaim. Fires once per (symbol, session).
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = conn.execute(
        """
        WITH prev AS (
          SELECT symbol, low AS prev_low
            FROM market_snapshots
           WHERE date = (SELECT MAX(date) FROM market_snapshots WHERE date < ?)
        ),
        today_agg AS (
          SELECT symbol,
                 MIN(price) AS session_low,
                 (SELECT price FROM price_ticks p
                   WHERE p.symbol = price_ticks.symbol
                   ORDER BY id DESC LIMIT 1) AS current_price,
                 COUNT(*) AS n
            FROM price_ticks
           WHERE substr(ts,1,10) = ?
           GROUP BY symbol
          HAVING n >= 10
        )
        SELECT t.symbol, t.session_low, t.current_price, p.prev_low
          FROM today_agg t
          JOIN prev p ON p.symbol = t.symbol
         WHERE t.session_low < p.prev_low
           AND t.current_price > p.prev_low
        """,
        (today, today),
    ).fetchall()

    for r in rows:
        if _fired_today(conn, r["symbol"], "failed_breakdown"):
            continue
        try:
            session_low = float(r["session_low"])
            prev_low = float(r["prev_low"])
            current = float(r["current_price"])
        except (TypeError, ValueError):
            continue
        if prev_low <= 0:
            continue
        narration = (f"Failed breakdown: dipped to ${session_low:.2f} below prev "
                     f"low ${prev_low:.2f}, reclaimed at ${current:.2f}")
        _record_event(
            conn, r["symbol"], "failed_breakdown", narration,
            price=current,
            magnitude=((current - prev_low) / prev_low) * 100.0,
            metadata={"session_low": session_low, "prev_low": prev_low},
        )


def _detect_vwap_reclaim(conn: sqlite3.Connection) -> None:
    """Fire when prev_tick_price < session VWAP and current_price >= VWAP.

    VWAP = SUM(price * volume) / SUM(volume) for the session. 30-min dedup
    window so a stock that flips around VWAP doesn't fire every cycle.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = conn.execute(
        """
        SELECT symbol,
               SUM(price * COALESCE(volume,0)) / NULLIF(SUM(COALESCE(volume,0)), 0) AS vwap,
               (SELECT price FROM price_ticks p
                 WHERE p.symbol = price_ticks.symbol
                 ORDER BY id DESC LIMIT 1) AS current_price,
               (SELECT price FROM price_ticks p
                 WHERE p.symbol = price_ticks.symbol
                 ORDER BY id DESC LIMIT 1 OFFSET 1) AS prev_price,
               COUNT(*) AS n
          FROM price_ticks
         WHERE substr(ts,1,10) = ?
         GROUP BY symbol
        HAVING n >= 20 AND vwap IS NOT NULL
        """,
        (today,),
    ).fetchall()

    for r in rows:
        try:
            vwap = float(r["vwap"])
            current = float(r["current_price"])
            prev = float(r["prev_price"])
        except (TypeError, ValueError):
            continue
        if vwap <= 0:
            continue
        if not (prev < vwap and current >= vwap):
            continue
        if _fired_within(conn, r["symbol"], "vwap_reclaim", 30 * 60):
            continue
        narration = f"VWAP reclaim: crossed back above ${vwap:.2f}"
        _record_event(
            conn, r["symbol"], "vwap_reclaim", narration,
            price=current,
            magnitude=((current - vwap) / vwap) * 100.0,
            metadata={"vwap": vwap, "prev_price": prev},
        )


def _detect_power_hour_thrust(conn: sqlite3.Connection) -> None:
    """Fire in last 60 min of ET session when last-60min volume rate exceeds
    1.5× the session average rate. Once per (symbol, session).
    """
    if not _is_power_hour_et():
        return
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = conn.execute(
        """
        SELECT symbol,
               SUM(CASE WHEN datetime(ts) >= datetime('now', '-60 minutes')
                        THEN COALESCE(volume,0) END) AS vol_60min,
               SUM(COALESCE(volume,0)) AS vol_total,
               (julianday(MAX(ts)) - julianday(MIN(ts))) * 24.0 AS hours_elapsed,
               (SELECT price FROM price_ticks p
                 WHERE p.symbol = price_ticks.symbol
                 ORDER BY id DESC LIMIT 1) AS last_price,
               COUNT(*) AS n
          FROM price_ticks
         WHERE substr(ts,1,10) = ?
         GROUP BY symbol
        HAVING n >= 30 AND vol_60min > 0 AND vol_total > 0
        """,
        (today,),
    ).fetchall()

    for r in rows:
        if _fired_today(conn, r["symbol"], "power_hour_thrust"):
            continue
        try:
            vol_60min = float(r["vol_60min"] or 0)
            vol_total = float(r["vol_total"] or 0)
            hours_elapsed = float(r["hours_elapsed"] or 0)
        except (TypeError, ValueError):
            continue
        if hours_elapsed <= 0:
            continue
        avg_per_hour = vol_total / hours_elapsed
        if avg_per_hour <= 0:
            continue
        ratio = vol_60min / avg_per_hour
        if ratio < 1.5:
            continue
        last_px = float(r["last_price"]) if r["last_price"] is not None else None
        narration = f"Power-hour thrust: {ratio:.1f}× session avg in last 60min"
        _record_event(
            conn, r["symbol"], "power_hour_thrust", narration,
            price=last_px, magnitude=ratio,
            metadata={"vol_60min": vol_60min, "avg_per_hour": avg_per_hour,
                      "hours_elapsed": hours_elapsed},
        )


# ─── Main loops ──────────────────────────────────────────────────────────────

def _run_detector_loop() -> None:
    """Every _DETECT_INTERVAL_SECS, run all detectors against current price_ticks."""
    while _running:
        # Market-hours gate.
        try:
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
            # HM-SCANNER-EVENT-DETECTORS-C5
            _detect_gap_fill_complete(c)
            _detect_breakout_resistance(c)
            _detect_failed_breakdown(c)
            _detect_vwap_reclaim(c)
            _detect_power_hour_thrust(c)
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


# HM-EVENT-TAPE-STALENESS-WATCHDOG-2026-07-09: the detector thread went silently
# dead for 21.5h+ (cycles=0 the entire time, no exception ever logged) with no
# alarm anywhere to catch it -- discovered only by a manual dashboard read. This
# watchdog alarms on CYCLE staleness (the loop not advancing), not event-count
# staleness: _run_heartbeat's own docstring above already documents "no events
# fire" as an expected, normal state during quiet market hours, so a
# zero-events-based alarm would false-page on legitimately quiet stretches. Cycle
# staleness is the metric that actually failed here and catches the real bug
# class (thread wedged/deadlocked) without that false-positive risk.
_STALENESS_THRESHOLD_SECS = 15 * 60   # 15 min, per HM-STOP-COVERAGE-GAP session ask
_staleness_alert_sent = False


def _check_detector_staleness() -> None:
    """Alarm if the detector loop hasn't completed a cycle in >15min during
    market hours. Edge-triggered (fires once, auto-clears on recovery) —
    same pattern as main.py's HM-SCAN-LIVENESS-WATCHDOG."""
    global _staleness_alert_sent
    try:
        if not RiskManager.is_market_hours():
            return
    except Exception:
        return
    last = _stats["last_cycle_at"]
    if last is None:
        age = float("inf")
    else:
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(last)).total_seconds()
    if age > _STALENESS_THRESHOLD_SECS:
        if not _staleness_alert_sent:
            try:
                from engine.alert_channels import send_alert, AlertLevel
                send_alert(
                    f"Event-tape detector stalled — last completed cycle "
                    f"{('never' if last is None else f'{age/60:.1f}min ago')} "
                    f"(threshold {_STALENESS_THRESHOLD_SECS/60:.0f}min)",
                    level=AlertLevel.WARNING,
                    alert_type="event_tape_staleness",
                    title="Event-tape detector stall",
                    source="sys_event_tape_liveness",
                )
            except Exception as e:
                console.log(f"[red][HM-EVENT-TAPE-STALENESS] send_alert failed: {e}")
            console.log(f"[red][HM-EVENT-TAPE-STALENESS] ALERT — age={'inf' if last is None else f'{age:.0f}s'}")
            _staleness_alert_sent = True
    elif _staleness_alert_sent:
        console.log(f"[green][HM-EVENT-TAPE-STALENESS] recovered — age={age:.0f}s")
        _staleness_alert_sent = False


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
        _check_detector_staleness()
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

    # HM-EVENT-TAPE-STALENESS-WATCHDOG-2026-07-09: seed last_cycle_at at start
    # time (not None) so the watchdog doesn't false-alarm with age=inf in the
    # 30s window before the detector thread completes its first real cycle --
    # same fix shape as main.py's HM-SCAN-LIVENESS-WATCHDOG seeding.
    _stats["last_cycle_at"] = datetime.now(timezone.utc).isoformat()

    _running = True
    _detector_thread = threading.Thread(target=_run_detector_loop, daemon=True, name="event-tape-detector")
    _detector_thread.start()
    _cleanup_thread = threading.Thread(target=_run_cleanup, daemon=True, name="event-tape-cleanup")
    _cleanup_thread.start()
    _heartbeat_thread = threading.Thread(target=_run_heartbeat, daemon=True, name="event-tape-heartbeat")
    _heartbeat_thread.start()
    console.log("[green][EVENT-TAPE] event detector started (5 v1 + 5 C5 detectors, 30s cadence)")


def stop_event_detector() -> None:
    global _running
    _running = False
    console.log("[yellow][EVENT-TAPE] stop requested")


def get_stats() -> dict:
    return {**_stats, "running": _running}
