"""
HM-OLLIE-EVENT-TAPE-V2-REALTIME Phase 2.5 Component 1 — Alpaca IEX tick recorder.

Subscribes to Alpaca's IEX WebSocket trade stream (free with paper account)
and records ticks into the `price_ticks` table. Feeds the Phase 2.5 event
detector (Component 2).

Design notes
------------
- Sacred rules: ADD-only schema. `CREATE TABLE IF NOT EXISTS` is the only DDL
  this module issues. Never drops or alters anything.
- Daemon Lifecycle Rule (CLAUDE.md 2026-05-12): `start_tick_recorder()` is
  invoked at module-level main.py startup, not lazy from a scan path. Heartbeat
  log line every minute confirms live execution.
- Bounded memory: `_tick_queue` is a `queue.Queue(maxsize=1000)`. When full,
  oldest tick is dropped (`_stats['ticks_dropped']` increments) — prevents
  WebSocket backpressure if writer thread stalls.
- DB writes are batched: writer thread drains the queue every second OR when
  buffer hits 100 ticks, whichever first. `executemany()` for the INSERT.
- Reconnect: exponential backoff 10s → 20s → 40s → max 120s, mirroring
  engine/realtime_monitor.py.
- Market-hours gated: sleeps cleanly until next open when market closed.
- Cleanup: hourly DELETE for ticks older than 4h (rolling retention).
- Tier-aware sampling per the spec is deferred to a follow-up (C1.x). For now
  every received tick on a subscribed symbol is recorded.

Original spec targeted Polygon. Pivot rationale: Polygon Stocks Starter ($29/mo)
does NOT include WS trades — requires Advanced ($499/mo). Alpaca IEX feed is
free with the existing paper account; SIP upgrade ($99/mo) is the documented
upgrade path. See drafts/HM-LESSON-VERIFY-DATA-SOURCE-FIRST.md.

Alpaca WS protocol: https://alpaca.markets/docs/api-references/market-data-api/stock-pricing-data/realtime/
Frame shapes:
  auth     → {"action":"auth","key":KEY,"secret":SECRET}
  subscribe→ {"action":"subscribe","trades":["AAPL","MSFT"]}
  trade ev → {"T":"t","S":"MSFT","i":12053,"x":"V","p":412.33,"s":40,
              "c":["@"],"z":"C","t":"2026-05-27T18:17:17.514045708Z"}
  status   → {"T":"success","msg":"authenticated"}
           | {"T":"error","msg":"...","code":...}
           | {"T":"subscription","trades":[...], ...}
"""
from __future__ import annotations

import json
import queue
import sqlite3
import threading
import time
from datetime import datetime, timezone
from typing import Iterable

from rich.console import Console

console = Console()

DB = "data/trader.db"
# Alpaca IEX feed — free with paper account. SIP upgrade path:
# wss://stream.data.alpaca.markets/v2/sip ($99/mo, full venue coverage).
_ALPACA_WS_URL = "wss://stream.data.alpaca.markets/v2/iex"

# Bounded queue: WS thread enqueues, writer thread drains. Drops oldest on full.
_TICK_QUEUE_MAX = 1000
_WRITE_BATCH_MAX = 100   # flush at this many ticks OR every second
_WRITE_FLUSH_SECS = 1.0

# Alpaca IEX free tier caps at 30 trade subscriptions per connection.
# HM-TICK-RECORDER-CAP-FIX 2026-05-29: use the full documented cap (was 28
# conservative margin) — the real bottleneck was a 40-position book monopolizing
# every slot, not the cap itself; see _CONVERGENCE_RESERVE + _get_universe.
_MAX_SUBSCRIBED_SYMBOLS = 30
# Guaranteed slots for top scanner-convergence candidates (by strategy count),
# allocated BEFORE positions so a large position book can't starve imminent-entry
# candidates of all ticks. Tunable; 10 covers all ≥4-strategy convergence + margin.
_CONVERGENCE_RESERVE = 10
_UNIVERSE_REFRESH_SECS = 300   # recompute subscription set every 5 min

# Rolling retention: ticks older than this are pruned hourly.
_RETENTION_HOURS = 4
_CLEANUP_INTERVAL_SECS = 3600  # once an hour

# Heartbeat cadence — proves the daemon is alive even if the market is quiet.
_HEARTBEAT_INTERVAL_SECS = 60

# Module-level state
_running: bool = False
_connected: bool = False
_ws = None  # populated to allow controlled close
_ws_thread: threading.Thread | None = None
_writer_thread: threading.Thread | None = None
_cleanup_thread: threading.Thread | None = None
_heartbeat_thread: threading.Thread | None = None

_tick_queue: "queue.Queue[tuple[str, float, int | None, str]]" = queue.Queue(maxsize=_TICK_QUEUE_MAX)
_subscribed: set[str] = set()
_subscribe_lock = threading.Lock()

_stats: dict = {
    "ticks_received": 0,
    "ticks_dropped": 0,
    "ticks_written": 0,
    "last_tick_at": None,
    "last_subscribe_at": None,
    "reconnects": 0,
    "ws_failures": 0,
}


# ─── DB helpers ───────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB, timeout=10)
    c.execute("PRAGMA journal_mode=WAL")
    return c


def _init_tables() -> None:
    """Idempotent ADD-only schema. Sacred rule honored."""
    c = _conn()
    try:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS price_ticks (
                id     INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                price  REAL NOT NULL,
                volume INTEGER,
                ts     TEXT NOT NULL
            )
            """
        )
        c.execute("CREATE INDEX IF NOT EXISTS idx_price_ticks_symbol_ts ON price_ticks(symbol, ts)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_price_ticks_ts ON price_ticks(ts)")
        c.commit()
    finally:
        c.close()


# ─── Universe (symbols to subscribe) ──────────────────────────────────────────

def _get_universe() -> list[str]:
    """Combine in-fleet positions, current scanner convergence tickers, and the
    active watchlist into a bounded set of symbols to subscribe.

    Hard-capped at _MAX_SUBSCRIBED_SYMBOLS to keep the WS firehose manageable.
    """
    symbols: list[str] = []
    seen: set[str] = set()

    def _add(sym: str | None) -> None:
        if not sym:
            return
        sym = sym.strip().upper()
        if sym and sym not in seen and len(symbols) < _MAX_SUBSCRIBED_SYMBOLS:
            symbols.append(sym)
            seen.add(sym)

    # HM-TICK-RECORDER-CAP-FIX 2026-05-29: gather the buckets first, then allocate
    # with a RESERVED block for top convergence so a large position book (40 stock
    # positions vs 30 slots) can't starve the scanner's imminent-entry candidates
    # of all ticks (pre-fix: 0 convergence subscribed, incl. a 6-strategy ticker).
    positions: list[str] = []
    convergence: list[str] = []
    try:
        c = _conn()
        positions = [s for (s,) in c.execute(
            "SELECT DISTINCT symbol FROM positions WHERE qty IS NOT NULL AND qty != 0 AND asset_type='stock'"
        ).fetchall()]
        convergence = [s for (s,) in c.execute(
            """
            SELECT ticker
              FROM strategy_signals
             WHERE created_at >= datetime('now','-90 minutes')
             GROUP BY ticker
            HAVING COUNT(DISTINCT strategy_name) >= 3
             ORDER BY COUNT(DISTINCT strategy_name) DESC
            """
        ).fetchall()]
        c.close()
    except Exception as exc:
        console.log(f"[yellow][TICK-REC] universe DB query failed: {type(exc).__name__}: {exc!r}")

    # 1) RESERVE: top convergence candidates (by strategy count) — guaranteed slots
    for s in convergence[:_CONVERGENCE_RESERVE]:
        _add(s)
    # 2) Active fleet positions — fill remaining slots after the reserve
    for s in positions:
        _add(s)
    # 3) Remaining convergence (beyond the reserve), if room
    for s in convergence[_CONVERGENCE_RESERVE:]:
        _add(s)
    # 4) Active watchlist (engine/universe convention used by realtime_monitor), if room
    try:
        from engine.universe import get_active_universe
        for s in get_active_universe():
            _add(s)
    except Exception as exc:
        console.log(f"[yellow][TICK-REC] get_active_universe failed: {type(exc).__name__}: {exc!r}")

    return symbols


def _subscription_diff(new_set: Iterable[str]) -> tuple[list[str], list[str]]:
    """Compute (to_add, to_remove) against the currently subscribed set."""
    new = set(new_set)
    with _subscribe_lock:
        to_add = sorted(new - _subscribed)
        to_remove = sorted(_subscribed - new)
    return to_add, to_remove


# ─── WebSocket thread ─────────────────────────────────────────────────────────

def _on_ws_message(ws, raw: str) -> None:
    """Handle a frame from Alpaca. Frames are JSON arrays of events.

    Alpaca's auth flow: connect → server sends {"T":"success","msg":"connected"};
    we send auth → server sends {"T":"success","msg":"authenticated"}; only
    THEN may we subscribe. So unlike Polygon, the subscribe is sent from inside
    this handler upon seeing the authenticated status.
    """
    try:
        events = json.loads(raw)
    except Exception:
        return
    if not isinstance(events, list):
        return
    for ev in events:
        kind = ev.get("T")
        if kind == "t":   # trade
            sym = ev.get("S")
            price = ev.get("p")
            size = ev.get("s")  # share count
            ts_iso = ev.get("t")   # already ISO string, e.g. "2026-05-27T18:17:17.514045708Z"
            if not sym or price is None:
                continue
            # Normalize Alpaca's high-precision timestamp to plain ISO (drop the
            # nanosecond tail Python can't parse cleanly; we don't need ns
            # precision for tick storage).
            iso = ts_iso or datetime.now(timezone.utc).isoformat()
            try:
                _tick_queue.put_nowait((sym, float(price), int(size) if size is not None else None, iso))
                _stats["ticks_received"] += 1
                _stats["last_tick_at"] = iso
            except queue.Full:
                # Drop oldest to make room for newest — backpressure relief.
                try:
                    _tick_queue.get_nowait()
                    _stats["ticks_dropped"] += 1
                    _tick_queue.put_nowait((sym, float(price), int(size) if size is not None else None, iso))
                    _stats["ticks_received"] += 1
                except queue.Empty:
                    pass
        elif kind == "success":
            msg = ev.get("msg", "")
            console.log(f"[cyan][TICK-REC] {msg}")
            if msg == "authenticated":
                # Auth complete — subscribe to the universe now.
                syms = _get_universe()
                if not syms:
                    console.log("[yellow][TICK-REC] no symbols in universe — nothing to subscribe yet")
                    return
                try:
                    ws.send(json.dumps({"action": "subscribe", "trades": syms}))
                    with _subscribe_lock:
                        _subscribed.clear()
                        _subscribed.update(syms)
                    _stats["last_subscribe_at"] = datetime.now(timezone.utc).isoformat()
                    console.log(f"[green][TICK-REC] subscribed to {len(syms)} symbols (first 5: {syms[:5]})")
                except Exception as exc:
                    console.log(f"[red][TICK-REC] subscribe send failed: {type(exc).__name__}: {exc!r}")
        elif kind == "subscription":
            trades_subbed = ev.get("trades", [])
            console.log(f"[cyan][TICK-REC] subscription ack: {len(trades_subbed)} trade channels")
        elif kind == "error":
            msg = ev.get("msg", "")
            code = ev.get("code", "")
            console.log(f"[red][TICK-REC] Alpaca error code={code} msg={msg}")


def _on_ws_open(ws) -> None:
    global _connected
    from config import ALPACA_API_KEY, ALPACA_SECRET_KEY
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        console.log("[red][TICK-REC] ALPACA_API_KEY / ALPACA_SECRET_KEY missing — cannot auth")
        return
    console.log("[green][TICK-REC] WS connected — sending auth")
    try:
        ws.send(json.dumps({"action": "auth", "key": ALPACA_API_KEY, "secret": ALPACA_SECRET_KEY}))
    except Exception as exc:
        console.log(f"[red][TICK-REC] auth send failed: {type(exc).__name__}: {exc!r}")
        return
    # Subscribe happens in _on_ws_message when we see {"T":"success","msg":"authenticated"}.
    _connected = True


def _on_ws_error(_ws_unused, error) -> None:
    global _connected
    _connected = False
    console.log(f"[red][TICK-REC] WS error: {error!r}")


def _on_ws_close(_ws_unused, code, msg) -> None:
    global _connected
    _connected = False
    console.log(f"[yellow][TICK-REC] WS closed code={code} msg={msg}")


def _run_websocket() -> None:
    """Outer WS loop — exponential backoff, market-hours gated."""
    global _connected, _ws

    import websocket
    from config import ALPACA_API_KEY, ALPACA_SECRET_KEY

    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        console.log("[red][TICK-REC] ALPACA credentials missing — recorder cannot start")
        return

    backoff_seq = [10, 20, 40, 80, 120]
    failures = 0

    while _running:
        # Market-hours gate — sleep until next open.
        try:
            from engine.risk_manager import RiskManager
            if not RiskManager.is_market_hours():
                _connected = False
                console.log("[yellow][TICK-REC] market closed — sleeping 60s")
                for _ in range(60):
                    if not _running:
                        return
                    time.sleep(1)
                continue
        except Exception as exc:
            console.log(f"[yellow][TICK-REC] is_market_hours check failed: {exc!r} — proceeding anyway")

        try:
            _ws = websocket.WebSocketApp(
                _ALPACA_WS_URL,
                on_open=_on_ws_open,
                on_message=_on_ws_message,
                on_error=_on_ws_error,
                on_close=_on_ws_close,
            )
            # ping_interval keeps the connection healthy through silent periods.
            _ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as exc:
            console.log(f"[red][TICK-REC] WebSocketApp exception: {type(exc).__name__}: {exc!r}")

        if not _running:
            return

        failures += 1
        _stats["ws_failures"] = failures
        _stats["reconnects"] += 1
        backoff = backoff_seq[min(failures - 1, len(backoff_seq) - 1)]
        console.log(f"[yellow][TICK-REC] reconnecting in {backoff}s (attempt {failures})")
        for _ in range(backoff):
            if not _running:
                return
            time.sleep(1)


# ─── Writer thread ────────────────────────────────────────────────────────────

def _run_writer() -> None:
    """Drain _tick_queue and batch-INSERT into price_ticks."""
    buffer: list[tuple[str, float, int | None, str]] = []
    last_flush = time.time()
    while _running:
        try:
            timeout = max(0.05, _WRITE_FLUSH_SECS - (time.time() - last_flush))
            try:
                tick = _tick_queue.get(timeout=timeout)
                buffer.append(tick)
            except queue.Empty:
                pass

            now = time.time()
            should_flush = (
                buffer
                and (len(buffer) >= _WRITE_BATCH_MAX or (now - last_flush) >= _WRITE_FLUSH_SECS)
            )
            if should_flush:
                _flush_buffer(buffer)
                buffer = []
                last_flush = now
        except Exception as exc:
            console.log(f"[red][TICK-REC] writer loop error: {type(exc).__name__}: {exc!r}")
            time.sleep(1)

    # Final flush on shutdown.
    if buffer:
        _flush_buffer(buffer)


def _flush_buffer(buffer: list[tuple[str, float, int | None, str]]) -> None:
    """Batch INSERT a flush of ticks. Swallows DB errors with a log line."""
    if not buffer:
        return
    try:
        c = _conn()
        c.executemany(
            "INSERT INTO price_ticks (symbol, price, volume, ts) VALUES (?,?,?,?)",
            buffer,
        )
        c.commit()
        c.close()
        _stats["ticks_written"] += len(buffer)
    except Exception as exc:
        console.log(f"[red][TICK-REC] DB write failed (lost {len(buffer)} ticks): "
                    f"{type(exc).__name__}: {exc!r}")


# ─── Cleanup thread ───────────────────────────────────────────────────────────

def _run_cleanup() -> None:
    """Hourly DELETE of ticks older than _RETENTION_HOURS."""
    # Wait one cleanup interval before first prune — gives the recorder a chance
    # to populate without immediately deleting.
    for _ in range(_CLEANUP_INTERVAL_SECS):
        if not _running:
            return
        time.sleep(1)

    while _running:
        try:
            c = _conn()
            cur = c.execute(
                "DELETE FROM price_ticks WHERE ts < datetime('now', ?)",
                (f"-{_RETENTION_HOURS} hours",),
            )
            deleted = cur.rowcount
            c.commit()
            c.close()
            console.log(f"[cyan][TICK-REC] cleanup: pruned {deleted} ticks older than {_RETENTION_HOURS}h")
        except Exception as exc:
            console.log(f"[yellow][TICK-REC] cleanup failed: {type(exc).__name__}: {exc!r}")
        # Sleep until next cleanup
        for _ in range(_CLEANUP_INTERVAL_SECS):
            if not _running:
                return
            time.sleep(1)


# ─── Heartbeat thread ─────────────────────────────────────────────────────────

def _run_heartbeat() -> None:
    """Periodic log line proving the daemon is alive — HM-EQ doctrine.

    Without this, a silently-dead recorder could look identical to a market-closed
    one. The heartbeat surfaces stats every minute so a log scan catches stalls.
    """
    while _running:
        msg = (
            f"[HM-TICK-REC heartbeat] running={_running} connected={_connected} "
            f"subs={len(_subscribed)} recv={_stats['ticks_received']} "
            f"drop={_stats['ticks_dropped']} write={_stats['ticks_written']} "
            f"reconnects={_stats['reconnects']} last_tick={_stats['last_tick_at']}"
        )
        console.log(msg)
        for _ in range(_HEARTBEAT_INTERVAL_SECS):
            if not _running:
                return
            time.sleep(1)


# ─── Universe refresh thread ──────────────────────────────────────────────────

def _run_universe_refresh() -> None:
    """Periodically recompute the universe and update Polygon subscriptions."""
    # Wait one interval before the first refresh — initial subscription happened
    # in _on_ws_open.
    for _ in range(_UNIVERSE_REFRESH_SECS):
        if not _running:
            return
        time.sleep(1)

    while _running:
        try:
            if _connected and _ws is not None:
                new_syms = _get_universe()
                to_add, to_remove = _subscription_diff(new_syms)
                if to_add:
                    _ws.send(json.dumps({"action": "subscribe", "trades": to_add}))
                if to_remove:
                    _ws.send(json.dumps({"action": "unsubscribe", "trades": to_remove}))
                if to_add or to_remove:
                    with _subscribe_lock:
                        _subscribed.update(to_add)
                        _subscribed.difference_update(to_remove)
                    _stats["last_subscribe_at"] = datetime.now(timezone.utc).isoformat()
                    console.log(f"[cyan][TICK-REC] resub: +{len(to_add)} −{len(to_remove)} "
                                f"(total {len(_subscribed)})")
        except Exception as exc:
            console.log(f"[yellow][TICK-REC] universe refresh failed: {type(exc).__name__}: {exc!r}")
        for _ in range(_UNIVERSE_REFRESH_SECS):
            if not _running:
                return
            time.sleep(1)


# ─── Public API ───────────────────────────────────────────────────────────────

def start_tick_recorder() -> None:
    """Spin up the WS + writer + cleanup + heartbeat threads. Idempotent."""
    global _running, _ws_thread, _writer_thread, _cleanup_thread, _heartbeat_thread

    if _running:
        return

    try:
        _init_tables()
    except Exception as exc:
        console.log(f"[red][TICK-REC] _init_tables failed — refusing to start: {type(exc).__name__}: {exc!r}")
        return

    _running = True

    _ws_thread = threading.Thread(target=_run_websocket, daemon=True, name="tick-recorder-ws")
    _ws_thread.start()

    _writer_thread = threading.Thread(target=_run_writer, daemon=True, name="tick-recorder-writer")
    _writer_thread.start()

    _cleanup_thread = threading.Thread(target=_run_cleanup, daemon=True, name="tick-recorder-cleanup")
    _cleanup_thread.start()

    _heartbeat_thread = threading.Thread(target=_run_heartbeat, daemon=True, name="tick-recorder-heartbeat")
    _heartbeat_thread.start()

    threading.Thread(target=_run_universe_refresh, daemon=True, name="tick-recorder-univ").start()

    console.log("[green][TICK-REC] tick_recorder started (WS + writer + cleanup + heartbeat)")


def stop_tick_recorder() -> None:
    """Signal all threads to wind down. Safe to call multiple times."""
    global _running, _connected
    _running = False
    _connected = False
    try:
        if _ws is not None:
            _ws.close()
    except Exception:
        pass
    console.log("[yellow][TICK-REC] stop requested")


def get_stats() -> dict:
    """Snapshot stats for /api or debug. Returns a shallow copy."""
    return {**_stats, "running": _running, "connected": _connected, "subscribed": len(_subscribed)}
