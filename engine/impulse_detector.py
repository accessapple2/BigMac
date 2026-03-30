"""Hourly Impulse Alert Detector — find momentum impulse signals on the watchlist.

Checks each watchlist stock every hour for:
  - Volume spike:   current hourly volume > 2× 20-hour average
  - Price impulse:  hourly candle body > 1.5× 20-hour ATR
  - Breakout:       price breaks above/below the 5-day range on heavy volume

Strength score 1–10 blends volume ratio + ATR ratio + breakout bonus.
Alerts saved to impulse_alerts table; injected into AI prompts when active (<2h old).

Icons (colorblind-safe — shape + direction, not just color):
  ▲ Bullish impulse
  ▼ Bearish impulse
"""
from __future__ import annotations
import sqlite3
import threading
import time
from datetime import datetime, timezone
from rich.console import Console

console = Console()
DB = "data/trader.db"

# In-memory cache: symbol → {result, ts}
_cache: dict[str, dict] = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 1800  # 30 minutes — hourly bars don't change intra-bar


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def ensure_impulse_table():
    conn = _conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS impulse_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            direction TEXT NOT NULL,
            strength_score REAL NOT NULL,
            volume_ratio REAL,
            atr_ratio REAL,
            signal_types TEXT,
            entry_zone TEXT,
            stop_level REAL,
            candle_body REAL,
            avg_atr REAL,
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def _fetch_hourly_candles(symbol: str) -> list[dict]:
    """Fetch 5 days of hourly OHLCV candles via the existing market_data helper."""
    try:
        from engine.market_data import get_intraday_candles
        candles = get_intraday_candles(symbol, interval="1h", range_="5d")
        # Filter out candles with missing/zero data
        return [c for c in candles if c.get("close") and c.get("volume") is not None]
    except Exception as e:
        console.log(f"[yellow]Impulse: candle fetch error {symbol}: {e}")
        return []


def _calc_atr(candles: list[dict]) -> float:
    """Average True Range over the candle list."""
    trs = []
    for i in range(1, len(candles)):
        h = candles[i]["high"]
        l = candles[i]["low"]
        prev_c = candles[i - 1]["close"]
        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
        trs.append(tr)
    return sum(trs) / len(trs) if trs else 0.0


def analyze_impulse(symbol: str) -> dict | None:
    """Analyze a symbol for hourly impulse signals. Returns alert dict or None."""
    with _cache_lock:
        cached = _cache.get(symbol)
        if cached and (time.time() - cached["ts"]) < _CACHE_TTL:
            return cached["result"]

    candles = _fetch_hourly_candles(symbol)

    # Need at least 22 bars: 1 current + 1 completed + 20 lookback
    if len(candles) < 22:
        with _cache_lock:
            _cache[symbol] = {"result": None, "ts": time.time()}
        return None

    # Use second-to-last bar as the "just completed" hourly bar
    # (last bar may be incomplete mid-hour)
    bar = candles[-2]
    lookback = candles[-22:-2]   # 20 completed bars for averages

    # --- Volume ---
    avg_vol = sum(c["volume"] for c in lookback) / len(lookback)
    volume_ratio = bar["volume"] / avg_vol if avg_vol > 0 else 0.0

    # --- ATR ---
    avg_atr = _calc_atr(lookback)
    candle_body = abs(bar["close"] - bar["open"])
    atr_ratio = candle_body / avg_atr if avg_atr > 0 else 0.0

    # --- 5-day range breakout (all bars except current) ---
    all_prior = candles[:-2]
    range_high = max(c["high"] for c in all_prior)
    range_low  = min(c["low"]  for c in all_prior)
    breakout_up   = bar["high"] > range_high and volume_ratio >= 1.5
    breakout_down = bar["low"]  < range_low  and volume_ratio >= 1.5

    # --- Signal classification ---
    signal_types = []
    if volume_ratio >= 2.0:
        signal_types.append("volume_spike")
    if atr_ratio >= 1.5:
        signal_types.append("price_impulse")
    if breakout_up or breakout_down:
        signal_types.append("breakout")

    if not signal_types:
        with _cache_lock:
            _cache[symbol] = {"result": None, "ts": time.time()}
        return None

    # --- Direction ---
    bullish = bar["close"] >= bar["open"]
    if breakout_up:
        bullish = True
    elif breakout_down:
        bullish = False
    direction = "bullish" if bullish else "bearish"

    # --- Strength score 1–10 ---
    vol_score = min(5.0, (volume_ratio / 2.0) * 3.0)   # 2× vol = 3, 3.3× vol = 5
    atr_score = min(3.0, (atr_ratio / 1.5) * 2.0)      # 1.5× ATR = 2, 2.25× = 3
    breakout_bonus = 2.0 if (breakout_up or breakout_down) else 0.0
    strength_score = round(min(10.0, vol_score + atr_score + breakout_bonus), 1)

    # --- Entry zone and stop ---
    price = bar["close"]
    half_atr = avg_atr * 0.5
    if bullish:
        entry_low  = round(price, 2)
        entry_high = round(price + avg_atr * 0.2, 2)
        stop_level = round(bar["low"] - half_atr, 2)
        entry_zone = f"${entry_low:.2f}–${entry_high:.2f}"
    else:
        entry_low  = round(price - avg_atr * 0.2, 2)
        entry_high = round(price, 2)
        stop_level = round(bar["high"] + half_atr, 2)
        entry_zone = f"${entry_low:.2f}–${entry_high:.2f}"

    result = {
        "ticker": symbol,
        "timestamp": bar["time"],
        "direction": direction,
        "strength_score": strength_score,
        "volume_ratio": round(volume_ratio, 2),
        "atr_ratio": round(atr_ratio, 2),
        "signal_types": ",".join(signal_types),
        "entry_zone": entry_zone,
        "stop_level": stop_level,
        "candle_body": round(candle_body, 4),
        "avg_atr": round(avg_atr, 4),
        "price": price,
        "range_high": round(range_high, 2),
        "range_low": round(range_low, 2),
        "icon": "▲" if bullish else "▼",
    }

    with _cache_lock:
        _cache[symbol] = {"result": result, "ts": time.time()}

    return result


def save_impulse_alert(alert: dict) -> None:
    """Persist an impulse alert. Deduplicates by ticker + bar timestamp."""
    conn = _conn()
    try:
        existing = conn.execute(
            "SELECT id FROM impulse_alerts WHERE ticker=? AND timestamp=?",
            (alert["ticker"], alert["timestamp"])
        ).fetchone()
        if not existing:
            conn.execute(
                "INSERT INTO impulse_alerts "
                "(ticker, timestamp, direction, strength_score, volume_ratio, atr_ratio, "
                "signal_types, entry_zone, stop_level, candle_body, avg_atr) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    alert["ticker"], alert["timestamp"], alert["direction"],
                    alert["strength_score"], alert["volume_ratio"], alert["atr_ratio"],
                    alert["signal_types"], alert["entry_zone"], alert["stop_level"],
                    alert["candle_body"], alert["avg_atr"],
                )
            )
            conn.commit()
            icon = "▲" if alert["direction"] == "bullish" else "▼"
            console.log(
                f"[bold cyan]IMPULSE {icon} {alert['ticker']}: "
                f"{alert['direction'].upper()} strength={alert['strength_score']}/10 "
                f"vol={alert['volume_ratio']:.1f}× atr={alert['atr_ratio']:.1f}×"
            )
    except Exception as e:
        console.log(f"[red]Impulse save error: {e}")
    finally:
        conn.close()


def scan_all_impulses(symbols: list[str] | None = None) -> list[dict]:
    """Scan all symbols for impulse signals. Returns list of active alerts."""
    if symbols is None:
        from config import WATCH_STOCKS
        symbols = WATCH_STOCKS

    from concurrent.futures import ThreadPoolExecutor, as_completed
    alerts = []

    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(analyze_impulse, sym): sym for sym in symbols}
        for fut in as_completed(futs, timeout=120):
            sym = futs[fut]
            try:
                result = fut.result()
                if result:
                    save_impulse_alert(result)
                    alerts.append(result)
            except Exception as e:
                console.log(f"[red]Impulse scan error {sym}: {e}")

    alerts.sort(key=lambda x: x["strength_score"], reverse=True)
    return alerts


def get_active_impulse_alerts(max_age_hours: float = 2.0) -> list[dict]:
    """Return alerts from the DB that are still within max_age_hours."""
    conn = _conn()
    rows = conn.execute(
        "SELECT ticker, timestamp, direction, strength_score, volume_ratio, atr_ratio, "
        "signal_types, entry_zone, stop_level, detected_at "
        "FROM impulse_alerts "
        "WHERE detected_at >= datetime('now', ?) "
        "ORDER BY strength_score DESC, detected_at DESC",
        (f"-{int(max_age_hours * 60)} minutes",)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_recent_impulse_alerts(limit: int = 50) -> list[dict]:
    """Return most recent impulse alerts from DB regardless of age."""
    conn = _conn()
    rows = conn.execute(
        "SELECT ticker, timestamp, direction, strength_score, volume_ratio, atr_ratio, "
        "signal_types, entry_zone, stop_level, detected_at "
        "FROM impulse_alerts "
        "ORDER BY detected_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def build_impulse_prompt_section(symbol: str) -> str:
    """Return impulse context string for AI prompt injection.

    Only fires when an active alert (<2h) exists for this symbol.
    """
    try:
        alerts = get_active_impulse_alerts(max_age_hours=2.0)
        match = next((a for a in alerts if a["ticker"] == symbol), None)
        if not match:
            return ""

        icon = "▲" if match["direction"] == "bullish" else "▼"
        sigs = match.get("signal_types", "").replace(",", " + ")
        score = match["strength_score"]
        vol_r = match["volume_ratio"]
        atr_r = match["atr_ratio"]
        entry = match["entry_zone"]
        stop  = match["stop_level"]

        urgency = "HIGH URGENCY" if score >= 7 else "MODERATE URGENCY"
        lines = [
            f"\n=== HOURLY IMPULSE ALERT [{icon} {match['direction'].upper()}] ===",
            f"  {urgency}: Impulse strength {score}/10 detected on {symbol}",
            f"  Signals: {sigs}",
            f"  Volume: {vol_r:.1f}× 20-hour average | ATR ratio: {atr_r:.1f}×",
            f"  Suggested entry zone: {entry}",
            f"  Stop level: ${stop:.2f}",
        ]

        if score >= 7:
            if match["direction"] == "bullish":
                lines.append(
                    "  ▲ STRONG BULLISH IMPULSE — institutions may be accumulating aggressively.\n"
                    "  Consider BUY with tight stop at suggested level. Momentum favors longs."
                )
            else:
                lines.append(
                    "  ▼ STRONG BEARISH IMPULSE — heavy selling detected, possible distribution.\n"
                    "  Consider SELL/avoid long. Momentum favors shorts or cash."
                )
        else:
            lines.append(
                f"  {'▲' if match['direction'] == 'bullish' else '▼'} Moderate impulse — "
                "confirm with overall market regime before acting."
            )

        return "\n".join(lines) + "\n"
    except Exception:
        return ""
