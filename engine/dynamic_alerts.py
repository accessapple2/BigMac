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
    """HM-DYNALERTS-HYGIENE 2026-07-07: routed through the shared
    engine.db_conn helper (busy_timeout=30000, synchronous=NORMAL) instead
    of a bespoke sqlite3.connect() site, per HM-WAL-BUSY-TIMEOUT-HYGIENE
    wave-1 doctrine. journal_mode is durable on the DB file already --
    re-issuing PRAGMA journal_mode=WAL per connection was a no-op on every
    hot path, dropped."""
    from engine.db_conn import get_conn
    c = get_conn(DB, check_same_thread=False)
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


def _notify(message: str, severity: str, alert_type: str, symbol: str, source: str = None):
    """Route through unified alert channels (HM-TELEGRAM-NTFY-UNIFY 2026-07-07).
    Severity mapping: dynamic_alerts' high->warning, medium/low->info;
    user-definition severities (info/warning/red_alert) pass through.
    alert_type is per-symbol so alert_channels' 300s/type rate limit can't
    cross-suppress different symbols; the stricter 1800s per-key cooldown
    upstream (_should_alert) remains the primary dedup.
    No silent catch: failures are logged (Error Handling Posture).

    HM-DYNALERTS-HYGIENE 2026-07-07: `source` (optional override, defaults
    to f"dyn_{alert_type}") is threaded to alert_channels.send_alert's
    notifications.type column so the Rung 1 contact card can classify
    fired dynamic/user alerts as ACTIONABLE. The hardcoded checks (six
    call sites) rely on the default; the user-definition _fire() path
    passes source=f"user_{kind}" explicitly since its own alert_type is
    already "user_{kind}" and would otherwise double-prefix to
    "dyn_user_{kind}"."""
    level_map = {"high": "warning", "medium": "info", "low": "info",
                 "info": "info", "warning": "warning", "red_alert": "red_alert"}
    try:
        from engine.alert_channels import send_alert
        send_alert(
            message,
            level=level_map.get(severity, "info"),
            alert_type=f"dyn_{alert_type}_{symbol}",
            title=f"Dynamic Alert: {symbol}",
            source=source if source is not None else f"dyn_{alert_type}",
        )
    except Exception as e:
        console.log(f"[red]dynamic_alerts: notify failed for {symbol}/{alert_type}: {e}")


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
    # FIX 2026-06-01: require a GENUINE upward cross (prev_close < r <= price), not just
    # price > r. The old `price > r` fired on ANY resistance below price — including levels
    # the stock cleared long ago — so e.g. MRVL @ $219 "broke above $93.43" every poll.
    # prev_close comes from detect_support_resistance (yesterday's close); paired with the
    # above-spot resistance filter, only a fresh cross above a nearby level alerts.
    prev = sr.get("prev_close")

    # Check resistance breaks (bullish breakout)
    for r in sr.get("resistance", []):
        if prev is not None and prev < r <= price:  # genuine upward cross of resistance
            key = f"resist_break_{symbol}_{r}"
            if _should_alert(key):
                msg = f"BREAKOUT: {symbol} broke above resistance ${r:.2f} — now ${price:.2f}"
                _save_alert(symbol, "resistance_break", msg, "high", price)
                _notify(msg, "high", "resistance_break", symbol)
                alerts.append({"type": "resistance_break", "symbol": symbol, "level": r, "price": price, "severity": "high"})
            break  # Only alert on first broken resistance

    # Check support breaks (bearish breakdown)
    for s in sr.get("support", []):
        if prev is not None and prev > s >= price:  # genuine downward cross of support
            key = f"support_break_{symbol}_{s}"
            if _should_alert(key):
                msg = f"BREAKDOWN: {symbol} broke below support ${s:.2f} — now ${price:.2f}"
                _save_alert(symbol, "support_break", msg, "high", price)
                _notify(msg, "high", "support_break", symbol)
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
            _notify(msg, "medium", "rsi_oversold", symbol)
            alerts.append({"type": "rsi_oversold", "symbol": symbol, "rsi": rsi, "price": price, "severity": "medium"})

    elif rsi > 70:
        key = f"rsi_overbought_{symbol}"
        if _should_alert(key):
            msg = f"RSI OVERBOUGHT: {symbol} RSI={rsi:.1f} — potential reversal zone"
            _save_alert(symbol, "rsi_overbought", msg, "medium", price)
            _notify(msg, "medium", "rsi_overbought", symbol)
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
            _notify(msg, "medium", "volume_spike", symbol)
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
            _notify(msg, "medium", "macd_crossover", symbol)
            alerts.append({"type": "macd_crossover", "symbol": symbol, "direction": direction, "histogram": macd_hist, "price": price, "severity": severity})

    return alerts


# === HM-ALERT-COLLAB-LINKS Phase 1 (2026-07-06, Admiral-approved) ===========
# User-defined alerts, additive to the hardcoded checks above (which stay
# default-on regardless of ALERT_DEFS_ENABLED). `kind` is an allowlist enum --
# a stored/imported definition can only exercise one of these five evaluators,
# never arbitrary code. See drafts/HM-ALERT-COLLAB-LINKS.md for the full plan.
import json as _json

ALERT_DEF_KINDS = frozenset({"price_level", "rsi", "volume_spike", "macd_cross", "trendline"})


def _fetch_enabled_definitions(symbol: str) -> list:
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT * FROM alert_definitions WHERE enabled=1 AND symbol=?", (symbol,)
        ).fetchall()
    except sqlite3.OperationalError:
        return []  # table not migrated yet on this DB -- fail closed, not loud
    finally:
        conn.close()
    return [dict(r) for r in rows]


def _mark_triggered(defn_id: int):
    conn = _conn()
    conn.execute(
        "UPDATE alert_definitions SET last_triggered_at=CURRENT_TIMESTAMP WHERE id=?",
        (defn_id,),
    )
    conn.commit()
    conn.close()


def check_user_definition(defn: dict, price: float, indicators: dict):
    """Evaluate ONE user-defined alert. `kind` not in ALERT_DEF_KINDS is
    silently skipped (defensive -- CRUD validation should already reject
    this at write time; belt-and-suspenders here since a link-import path
    lands in Phase 2)."""
    symbol = defn["symbol"]
    kind = defn.get("kind")
    if kind not in ALERT_DEF_KINDS:
        return []
    try:
        params = _json.loads(defn.get("params_json") or "{}")
    except ValueError:
        return []
    severity = defn.get("severity") or "info"
    key = f"userdef_{defn['id']}"
    alerts = []

    def _fire(message: str, extra: dict):
        if not _should_alert(key):
            return
        _save_alert(symbol, f"user_{kind}", message, severity, price)
        _notify(f"{symbol}: {message}", severity, f"user_{kind}", symbol, source=f"user_{kind}")
        _mark_triggered(defn["id"])
        alerts.append({"type": f"user_{kind}", "symbol": symbol, "price": price,
                       "severity": severity, "definition_id": defn["id"], **extra})

    if kind == "price_level":
        level = params.get("level")
        direction = params.get("direction", "above")
        if level is None:
            return []
        if direction == "above" and price >= level:
            _fire(f"price ${price:.2f} crossed above ${level:.2f}", {"level": level})
        elif direction == "below" and price <= level:
            _fire(f"price ${price:.2f} crossed below ${level:.2f}", {"level": level})

    elif kind == "rsi":
        rsi = indicators.get("rsi")
        if rsi is None:
            return []
        direction = params.get("direction", "oversold")
        threshold = params.get("threshold", 30 if direction == "oversold" else 70)
        if direction == "oversold" and rsi <= threshold:
            _fire(f"RSI={rsi:.1f} <= {threshold} (oversold)", {"rsi": rsi})
        elif direction == "overbought" and rsi >= threshold:
            _fire(f"RSI={rsi:.1f} >= {threshold} (overbought)", {"rsi": rsi})

    elif kind == "volume_spike":
        vol_ratio = indicators.get("volume_ratio")
        threshold = params.get("threshold", 2.0)
        if vol_ratio is not None and vol_ratio >= threshold:
            _fire(f"volume {vol_ratio:.1f}x average (threshold {threshold}x)", {"vol_ratio": vol_ratio})

    elif kind == "macd_cross":
        macd_hist = indicators.get("macd_histogram")
        if macd_hist is None or macd_hist == 0:
            return []
        direction = params.get("direction")  # "bullish", "bearish", or None = either
        actual = "bullish" if macd_hist > 0 else "bearish"
        if abs(macd_hist) < params.get("threshold", 0.1) and (direction in (None, actual)):
            _fire(f"MACD {actual} cross (histogram={macd_hist:.4f})", {"direction": actual})

    elif kind == "trendline":
        try:
            from engine.trendlines import detect_support_resistance
            sr = detect_support_resistance(symbol) or {}
        except Exception:
            return []
        prev = sr.get("prev_close")
        if prev is None:
            return []
        for r in sr.get("resistance", []):
            if prev < r <= price:
                _fire(f"broke above resistance ${r:.2f}", {"level": r, "direction": "resistance"})
                break
        for s in sr.get("support", []):
            if prev > s >= price:
                _fire(f"broke below support ${s:.2f}", {"level": s, "direction": "support"})
                break

    return alerts


def run_user_alert_definitions(prices: dict, indicators: dict) -> list:
    """Evaluate all enabled user-defined alerts. No-op (zero DB reads) when
    config.ALERT_DEFS_ENABLED is False -- Phase 1 ships gated off by default."""
    try:
        from config import ALERT_DEFS_ENABLED
    except Exception:
        ALERT_DEFS_ENABLED = False
    if not ALERT_DEFS_ENABLED:
        return []

    all_alerts = []
    for sym, data in prices.items():
        price = data.get("price", 0)
        if price <= 0:
            continue
        defs = _fetch_enabled_definitions(sym)
        if not defs:
            continue
        sym_indicators = indicators.get(sym, {})
        for defn in defs:
            all_alerts.extend(check_user_definition(defn, price, sym_indicators))
    return all_alerts
# === /HM-ALERT-COLLAB-LINKS Phase 1 =========================================


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

    all_alerts.extend(run_user_alert_definitions(prices, indicators))

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
