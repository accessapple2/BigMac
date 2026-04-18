#!/usr/bin/env python3
"""
ORCL GEX Alert System — Based on TanukiTrade analysis April 14, 2026
Monitors key GEX levels and fires alerts for pullback entries and breakouts.

GEX context:
  $140 put wall  = strong support (bounced multiple times)
  $170 C1 wall   = largest call GEX + highest absolute GEX (key resistance)
  $200           = extension target (highest call OI)
  Fleet entry was $149.68, target $164.65 already hit.

Usage: cd ~/autonomous-trader && venv/bin/python3 engine/orcl_gex_alerts.py
"""
from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ORCL_GEX] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_PATH    = Path(__file__).parent.parent / "data" / "trader.db"
NTFY_TOPIC = "ollietrades-crew"
YF_HEADERS = {"User-Agent": "Mozilla/5.0 OllieTrades/6.0"}

ORCL_LEVELS = {
    "put_wall":          140.0,
    "entry_zone_low":    145.0,
    "entry_zone_high":   152.0,
    "fleet_entry":       149.68,
    "c1_resistance":     170.0,
    "breakout_confirm":  172.0,
    "extension_target":  200.0,
}


def init_table():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS orcl_gex_alerts (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_type TEXT NOT NULL,
            price      REAL,
            rsi        REAL,
            message    TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def get_price(symbol: str = "ORCL") -> float | None:
    """Fetch current price from Yahoo Finance (no auth needed)."""
    try:
        r = requests.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
            params={"interval": "1m", "range": "1d"},
            headers=YF_HEADERS,
            timeout=10,
        )
        r.raise_for_status()
        meta = r.json()["chart"]["result"][0]["meta"]
        return float(meta.get("regularMarketPrice", 0) or 0)
    except Exception as e:
        log.warning(f"Price fetch failed for {symbol}: {e}")
        return None


def get_rsi(symbol: str = "ORCL", period: int = 14) -> float:
    """Calculate RSI from Yahoo Finance daily bars."""
    try:
        r = requests.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
            params={"interval": "1d", "range": "2mo"},
            headers=YF_HEADERS,
            timeout=10,
        )
        r.raise_for_status()
        closes = r.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        closes = [c for c in closes if c is not None]
        if len(closes) < period + 1:
            return 50.0

        deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        gains  = [d if d > 0 else 0.0 for d in deltas[-period:]]
        losses = [-d if d < 0 else 0.0 for d in deltas[-period:]]
        avg_g  = sum(gains) / period
        avg_l  = sum(losses) / period
        if avg_l == 0:
            return 100.0
        rs = avg_g / avg_l
        return round(100 - (100 / (1 + rs)), 1)
    except Exception as e:
        log.warning(f"RSI calc failed: {e}")
        return 50.0


def send_ntfy(message: str, priority: str = "default"):
    try:
        requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers={"Priority": priority, "Title": "ORCL GEX Alert"},
            timeout=5,
        )
        log.info(f"ntfy sent: {message[:60]}")
    except Exception as e:
        log.warning(f"ntfy failed: {e}")


def log_alert(alert_type: str, price: float, rsi: float, message: str):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO orcl_gex_alerts (alert_type, price, rsi, message) VALUES (?, ?, ?, ?)",
        (alert_type, price, rsi, message),
    )
    conn.commit()
    conn.close()


def check_levels() -> dict:
    """Check ORCL price against GEX levels. Call every 5 min during market hours."""
    price = get_price()
    if not price:
        return {}

    rsi    = get_rsi()
    alerts = []
    lv     = ORCL_LEVELS

    # Oversold at put wall
    if price <= lv["put_wall"] + 2 and rsi < 30:
        msg = (f"ORCL ${price:.2f} near $140 put wall + RSI={rsi:.0f} oversold — "
               f"strong bounce zone")
        send_ntfy(msg, priority="urgent")
        log_alert("oversold_bounce", price, rsi, msg)
        alerts.append(("oversold_bounce", msg))

    # Pullback entry zone with oversold RSI
    elif lv["entry_zone_low"] <= price <= lv["entry_zone_high"] and rsi < 40:
        msg = (f"ORCL pullback to entry zone ${price:.2f} (zone {lv['entry_zone_low']}"
               f"–{lv['entry_zone_high']}) RSI={rsi:.0f} — fleet entry was $149.68")
        send_ntfy(msg, priority="high")
        log_alert("pullback_entry", price, rsi, msg)
        alerts.append(("pullback_entry", msg))

    # Testing C1 resistance — no ntfy, just log
    elif lv["c1_resistance"] - 2 <= price <= lv["c1_resistance"] + 2:
        msg = (f"ORCL ${price:.2f} testing C1 resistance at $170 — "
               f"watch for rejection or breakout above $172")
        log_alert("c1_test", price, rsi, msg)
        alerts.append(("c1_test", msg))

    # Confirmed breakout above C1
    elif price > lv["breakout_confirm"]:
        msg = (f"ORCL breakout confirmed ${price:.2f} > $172 — "
               f"extension target $200")
        send_ntfy(msg, priority="high")
        log_alert("breakout", price, rsi, msg)
        alerts.append(("breakout", msg))

    result = {"price": price, "rsi": rsi, "alerts": alerts, "levels": lv}
    log.info(f"ORCL ${price:.2f}  RSI={rsi:.1f}  alerts={len(alerts)}")
    return result


def run():
    init_table()
    log.info("=== ORCL GEX Alert System ===")
    log.info(f"Levels: {ORCL_LEVELS}")
    result = check_levels()
    if result:
        for a_type, msg in result.get("alerts", []):
            log.info(f"  [{a_type}] {msg}")
    log.info("=== Done ===")


if __name__ == "__main__":
    run()
