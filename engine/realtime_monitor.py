"""Real-Time Price Monitor — fast-reaction layer using Finnhub WebSocket.

Streams live prices, detects spikes (>3% in 5 min or volume >2x), and triggers
instant single-stock AI scans on the cheapest model (Gemini Flash).
"""
from __future__ import annotations
import json
import sqlite3
import threading
import time
from collections import deque
from datetime import datetime
from rich.console import Console

console = Console()
DB = "data/trader.db"

# Rolling window: track price + volume over 5 minutes
_price_windows: dict[str, deque] = {}   # symbol -> deque of (timestamp, price, volume)
_alerts: deque = deque(maxlen=50)        # recent alerts for dashboard
_lock = threading.Lock()
_ws_thread: threading.Thread | None = None
_running = False
_connected = False

# Thresholds
PRICE_SPIKE_PCT = 3.0     # +/- 3% in rolling window
VOLUME_SPIKE_MULT = 2.0   # 2x average volume
WINDOW_SECONDS = 300       # 5-minute rolling window
COOLDOWN_SECONDS = 300     # Don't re-alert same stock within 5 min
_last_alert_time: dict[str, float] = {}  # symbol -> last alert timestamp


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=10)
    c.execute("PRAGMA journal_mode=WAL")
    return c


def _save_alert(symbol: str, alert_type: str, message: str, severity: str = "warning", price: float = 0):
    """Save alert to DB and in-memory buffer."""
    alert = {
        "symbol": symbol,
        "alert_type": alert_type,
        "message": message,
        "severity": severity,
        "price": price,
        "triggered_at": datetime.now().isoformat(),
    }
    with _lock:
        _alerts.appendleft(alert)

    try:
        conn = _conn()
        conn.execute(
            "INSERT INTO dynamic_alerts (symbol, alert_type, message, severity, price) VALUES (?,?,?,?,?)",
            (symbol, alert_type, message, severity, price),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def get_recent_alerts(limit: int = 20) -> list:
    """Get recent realtime alerts from memory (fast) or DB (fallback)."""
    with _lock:
        if _alerts:
            return list(_alerts)[:limit]

    # Fallback: read from DB
    try:
        conn = _conn()
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM dynamic_alerts ORDER BY triggered_at DESC LIMIT ?", (limit,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def get_monitor_status() -> dict:
    """Return current monitor state for dashboard."""
    with _lock:
        tracked = len(_price_windows)
        alert_count = len(_alerts)
    return {
        "running": _running,
        "connected": _connected,
        "tracked_symbols": tracked,
        "recent_alerts": alert_count,
    }


def _get_watchlist() -> list[str]:
    """Get watchlist + active discovery symbols."""
    from config import WATCH_STOCKS
    symbols = list(WATCH_STOCKS)
    try:
        from engine.discovery_scanner import get_cached_discoveries
        discoveries = get_cached_discoveries()
        for d in discoveries[:5]:
            sym = d.get("symbol", "")
            if sym and sym not in symbols:
                symbols.append(sym)
    except Exception:
        pass
    return symbols


def _check_spike(symbol: str, new_price: float, volume: float = 0):
    """Check if symbol has spiked beyond thresholds in the rolling window."""
    now = time.time()

    with _lock:
        if symbol not in _price_windows:
            _price_windows[symbol] = deque(maxlen=600)  # ~10 per sec max
        window = _price_windows[symbol]
        window.append((now, new_price, volume))

        # Purge old entries outside window
        cutoff = now - WINDOW_SECONDS
        while window and window[0][0] < cutoff:
            window.popleft()

        if len(window) < 2:
            return

        # Calculate price change over window
        oldest_price = window[0][1]
        if oldest_price <= 0:
            return
        pct_change = ((new_price / oldest_price) - 1) * 100

        # Calculate volume spike (compare recent volume to window average)
        volumes = [v for _, _, v in window if v > 0]
        vol_ratio = 0
        if len(volumes) > 10:
            recent_vol = sum(volumes[-5:]) / 5
            avg_vol = sum(volumes[:-5]) / len(volumes[:-5])
            if avg_vol > 0:
                vol_ratio = recent_vol / avg_vol

    # Check thresholds
    is_price_spike = abs(pct_change) >= PRICE_SPIKE_PCT
    is_volume_spike = vol_ratio >= VOLUME_SPIKE_MULT

    if not is_price_spike and not is_volume_spike:
        return

    # Cooldown check
    if symbol in _last_alert_time and (now - _last_alert_time[symbol]) < COOLDOWN_SECONDS:
        return
    _last_alert_time[symbol] = now

    # Build alert
    direction = "+" if pct_change > 0 else ""
    elapsed = int(now - (window[0][0] if window else now))
    elapsed_min = max(1, elapsed // 60)

    parts = []
    if is_price_spike:
        parts.append(f"{direction}{pct_change:.1f}% in {elapsed_min}min")
    if is_volume_spike:
        parts.append(f"volume {vol_ratio:.1f}x")
    detail = ", ".join(parts)

    severity = "critical" if abs(pct_change) >= 5 else "warning"
    msg = f"REALTIME ALERT: {symbol} {detail} @ ${new_price:.2f}"
    console.log(f"[bold {'red' if pct_change < 0 else 'green'}]{msg}")

    _save_alert(symbol, "realtime_spike", msg, severity, new_price)


# ─── Finnhub WebSocket connection ─────────────────────────────────────────────

def _wait_for_market_open():
    """Sleep until market opens (4 AM ET / 2 AM MT). Returns False if monitor stopped."""
    from engine.risk_manager import RiskManager
    while _running:
        if RiskManager.is_market_hours():
            return True
        # Sleep in small intervals so we can stop quickly
        for _ in range(60):
            if not _running:
                return False
            time.sleep(1)
    return False


def _run_websocket():
    """Connect to Finnhub WebSocket and stream live trades."""
    global _running, _connected
    import websocket

    from config import FINNHUB_API_KEY
    if not FINNHUB_API_KEY:
        console.log("[yellow]Realtime Monitor: No FINNHUB_API_KEY — using polling fallback")
        _run_polling_fallback()
        return

    url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"
    symbols = _get_watchlist()

    def on_open(ws):
        global _connected
        _connected = True
        console.log(f"[green]Realtime Monitor: WebSocket connected, subscribing to {len(symbols)} symbols")
        for sym in symbols:
            ws.send(json.dumps({"type": "subscribe", "symbol": sym}))

    def on_message(ws, message):
        try:
            data = json.loads(message)
            if data.get("type") != "trade":
                return
            for trade in data.get("data", []):
                symbol = trade.get("s", "")
                price = trade.get("p", 0)
                volume = trade.get("v", 0)
                if symbol and price > 0:
                    _check_spike(symbol, price, volume)
        except Exception:
            pass

    def on_error(ws, error):
        global _connected
        _connected = False
        console.log(f"[red]Realtime Monitor WebSocket error: {error}")

    def on_close(ws, close_status_code, close_msg):
        global _connected
        _connected = False
        console.log(f"[yellow]Realtime Monitor: WebSocket closed ({close_status_code})")

    ws_failures = 0
    while _running:
        # Don't connect when market is closed — sleep until next open
        from engine.risk_manager import RiskManager
        if not RiskManager.is_market_hours():
            _connected = False
            console.log("[yellow]Realtime Monitor: Market closed — sleeping until next open")
            if not _wait_for_market_open():
                return
            ws_failures = 0  # Reset failures on new market day
            console.log("[green]Realtime Monitor: Market open — connecting WebSocket")

        try:
            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            console.log(f"[red]Realtime Monitor WebSocket exception: {e}")

        if _running:
            _connected = False
            ws_failures += 1
            if ws_failures >= 3:
                console.log("[yellow]Realtime Monitor: WebSocket failed 3x — switching to polling fallback")
                _run_polling_fallback()
                return
            console.log(f"[yellow]Realtime Monitor: Reconnecting in 10s (attempt {ws_failures}/3)...")
            time.sleep(10)
            symbols = _get_watchlist()


def _run_polling_fallback():
    """Fallback: poll Yahoo prices every 30s if no Finnhub key."""
    global _connected
    _connected = True  # "connected" via polling
    console.log("[cyan]Realtime Monitor: Running in polling mode (30s interval)")

    while _running:
        # Skip polling when market is closed
        from engine.risk_manager import RiskManager
        if not RiskManager.is_market_hours():
            _connected = False
            console.log("[yellow]Realtime Monitor polling: Market closed — sleeping until next open")
            if not _wait_for_market_open():
                return
            _connected = True
            console.log("[green]Realtime Monitor polling: Market open — resuming")

        try:
            symbols = _get_watchlist()
            from engine.market_data import get_all_prices
            prices = get_all_prices(symbols)
            for sym, data in prices.items():
                if "error" not in data:
                    _check_spike(sym, data.get("price", 0), data.get("volume", 0))
        except Exception as e:
            console.log(f"[red]Realtime polling error: {e}")

        # Sleep in small intervals so we can stop quickly
        for _ in range(30):
            if not _running:
                break
            time.sleep(1)


# ─── Start / Stop ─────────────────────────────────────────────────────────────

def start_monitor():
    """Start the realtime price monitor in a background thread."""
    global _running, _ws_thread
    if _running:
        return

    _running = True
    _ws_thread = threading.Thread(target=_run_websocket, daemon=True, name="realtime-monitor")
    _ws_thread.start()
    console.log("[green]Realtime Monitor started")


def stop_monitor():
    """Stop the realtime monitor."""
    global _running, _connected
    _running = False
    _connected = False
    console.log("[yellow]Realtime Monitor stopped")
