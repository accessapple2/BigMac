"""Dynamic Alerts — monitor trendline breaks, RSI extremes, volume spikes, MACD crossovers."""
from __future__ import annotations
import sqlite3
import time
from datetime import datetime
from rich.console import Console

console = Console()
DB = "data/trader.db"

# Cooldown: don't re-alert the same condition within 30 min
_alert_cooldown: dict = {}
COOLDOWN_SECONDS = 1800


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def ensure_alerts_table():
    conn = _conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dynamic_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            alert_type TEXT NOT NULL,
            message TEXT NOT NULL,
            severity TEXT DEFAULT 'info',
            price REAL,
            triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def _should_alert(key: str) -> bool:
    """Check cooldown for this alert key."""
    now = time.time()
    if key in _alert_cooldown and now - _alert_cooldown[key] < COOLDOWN_SECONDS:
        return False
    _alert_cooldown[key] = now
    return True


def _save_alert(symbol: str, alert_type: str, message: str, severity: str, price: float):
    """Save alert to database."""
    conn = _conn()
    conn.execute(
        "INSERT INTO dynamic_alerts (symbol, alert_type, message, severity, price) "
        "VALUES (?, ?, ?, ?, ?)",
        (symbol, alert_type, message, severity, price)
    )
    conn.commit()
    conn.close()


def _send_telegram(message: str):
    """Send alert via Telegram."""
    try:
        from engine.telegram_alerts import send_alert
        send_alert(message)
    except Exception:
        pass


def check_trendline_breaks(symbol: str, price: float, indicators: dict):
    """Check if price broke through support or resistance."""
    try:
        from engine.trendlines import detect_support_resistance
        sr = detect_support_resistance(symbol)
        if not sr:
            return []
    except Exception:
        return []

    alerts = []

    # Check resistance breaks (bullish breakout)
    for r in sr.get("resistance", []):
        if price > r * 1.002:  # Confirmed break above (0.2% buffer)
            key = f"resist_break_{symbol}_{r}"
            if _should_alert(key):
                msg = f"BREAKOUT: {symbol} broke above resistance ${r:.2f} — now ${price:.2f}"
                _save_alert(symbol, "resistance_break", msg, "high", price)
                _send_telegram(f"<b>BREAKOUT</b> {symbol} broke resistance ${r:.2f} — ${price:.2f}")
                alerts.append({"type": "resistance_break", "symbol": symbol, "level": r, "price": price, "severity": "high"})
            break  # Only alert on first broken resistance

    # Check support breaks (bearish breakdown)
    for s in sr.get("support", []):
        if price < s * 0.998:  # Confirmed break below
            key = f"support_break_{symbol}_{s}"
            if _should_alert(key):
                msg = f"BREAKDOWN: {symbol} broke below support ${s:.2f} — now ${price:.2f}"
                _save_alert(symbol, "support_break", msg, "high", price)
                _send_telegram(f"<b>BREAKDOWN</b> {symbol} broke support ${s:.2f} — ${price:.2f}")
                alerts.append({"type": "support_break", "symbol": symbol, "level": s, "price": price, "severity": "high"})
            break

    return alerts


def check_rsi_extremes(symbol: str, price: float, indicators: dict):
    """Check for RSI oversold/overbought conditions."""
    rsi = indicators.get("rsi")
    if rsi is None:
        return []

    alerts = []
    if rsi < 30:
        key = f"rsi_oversold_{symbol}"
        if _should_alert(key):
            msg = f"RSI OVERSOLD: {symbol} RSI={rsi:.1f} — potential bounce zone"
            _save_alert(symbol, "rsi_oversold", msg, "medium", price)
            _send_telegram(f"<b>RSI OVERSOLD</b> {symbol} RSI={rsi:.1f} @ ${price:.2f}")
            alerts.append({"type": "rsi_oversold", "symbol": symbol, "rsi": rsi, "price": price, "severity": "medium"})

    elif rsi > 70:
        key = f"rsi_overbought_{symbol}"
        if _should_alert(key):
            msg = f"RSI OVERBOUGHT: {symbol} RSI={rsi:.1f} — potential reversal zone"
            _save_alert(symbol, "rsi_overbought", msg, "medium", price)
            _send_telegram(f"<b>RSI OVERBOUGHT</b> {symbol} RSI={rsi:.1f} @ ${price:.2f}")
            alerts.append({"type": "rsi_overbought", "symbol": symbol, "rsi": rsi, "price": price, "severity": "medium"})

    return alerts


def check_volume_spikes(symbol: str, price: float, indicators: dict):
    """Check for volume spikes > 2x average."""
    vol_ratio = indicators.get("volume_ratio")
    if vol_ratio is None:
        return []

    alerts = []
    if vol_ratio >= 2.0:
        key = f"vol_spike_{symbol}"
        if _should_alert(key):
            msg = f"VOLUME SPIKE: {symbol} trading at {vol_ratio:.1f}x average volume"
            _save_alert(symbol, "volume_spike", msg, "medium", price)
            _send_telegram(f"<b>VOL SPIKE</b> {symbol} {vol_ratio:.1f}x avg volume @ ${price:.2f}")
            alerts.append({"type": "volume_spike", "symbol": symbol, "vol_ratio": vol_ratio, "price": price, "severity": "medium"})

    return alerts


def check_macd_crossovers(symbol: str, price: float, indicators: dict):
    """Check for MACD crossovers (histogram sign change)."""
    macd_hist = indicators.get("macd_histogram")
    if macd_hist is None:
        return []

    alerts = []
    # We need previous histogram to detect crossover
    # Use a small threshold to detect fresh crossover
    if abs(macd_hist) < 0.1 and macd_hist != 0:
        direction = "BULLISH" if macd_hist > 0 else "BEARISH"
        key = f"macd_cross_{symbol}_{direction}"
        if _should_alert(key):
            msg = f"MACD {direction} CROSS: {symbol} — histogram={macd_hist:.4f}"
            severity = "medium"
            _save_alert(symbol, "macd_crossover", msg, severity, price)
            _send_telegram(f"<b>MACD {direction}</b> {symbol} crossover @ ${price:.2f}")
            alerts.append({"type": "macd_crossover", "symbol": symbol, "direction": direction, "histogram": macd_hist, "price": price, "severity": severity})

    return alerts


def run_dynamic_alerts(prices: dict, indicators: dict):
    """Run all dynamic alert checks for all symbols with data."""
    ensure_alerts_table()
    all_alerts = []

    for sym, data in prices.items():
        price = data.get("price", 0)
        if price <= 0:
            continue

        sym_indicators = indicators.get(sym, {})

        all_alerts.extend(check_trendline_breaks(sym, price, sym_indicators))
        all_alerts.extend(check_rsi_extremes(sym, price, sym_indicators))
        all_alerts.extend(check_volume_spikes(sym, price, sym_indicators))
        all_alerts.extend(check_macd_crossovers(sym, price, sym_indicators))

    if all_alerts:
        console.log(f"[yellow]Dynamic alerts: {len(all_alerts)} triggered")

    return all_alerts


def get_recent_alerts(limit: int = 50) -> list:
    """Get recent dynamic alerts."""
    ensure_alerts_table()
    conn = _conn()
    alerts = conn.execute(
        "SELECT * FROM dynamic_alerts ORDER BY triggered_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return [dict(a) for a in alerts]


def get_active_alerts(minutes: int = 30) -> list:
    """Get alerts from the last N minutes (for dashboard banner)."""
    ensure_alerts_table()
    conn = _conn()
    alerts = conn.execute(
        "SELECT * FROM dynamic_alerts WHERE triggered_at >= datetime('now', ?) ORDER BY triggered_at DESC",
        (f"-{minutes} minutes",)
    ).fetchall()
    conn.close()
    return [dict(a) for a in alerts]
