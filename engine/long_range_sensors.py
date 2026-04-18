#!/usr/bin/env python3
"""
Long Range Sensors — Whale Volume Detection System
Scans for unusual volume spikes that precede big moves.
Posts to Signal Center /api/signal + ntfy push.

Usage: cd ~/autonomous-trader && venv/bin/python3 engine/long_range_sensors.py
"""
from __future__ import annotations

import logging
import os
import sqlite3
import time
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [LRS] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_PATH           = Path(__file__).parent.parent / "data" / "trader.db"
SIGNAL_CENTER_URL = "http://localhost:9000/api/signal"
NTFY_TOPIC        = "ollietrades-crew"
YF_HEADERS        = {"User-Agent": "Mozilla/5.0 OllieTrades/6.0"}

WATCHLIST = [
    # Captain's Portfolio
    "AMD", "INTC", "MSFT", "MU", "TQQQ", "NET", "NUKZ", "XLE",
    # High conviction
    "NVDA", "META", "TSLA", "AAPL", "GOOGL", "AMZN", "ORCL", "AVGO",
    # Recent scanner hits
    "DLO", "EMXC", "CPNG", "AXP", "AAL", "BBIO", "ARRY", "ABR",
]

VOLUME_MULTIPLIER_ALERT = 3.0
VOLUME_MULTIPLIER_WHALE = 10.0
VOLUME_MULTIPLIER_MEGA  = 50.0


def init_table():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS whale_detections (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT NOT NULL,
            rel_volume  REAL,
            price       REAL,
            change_pct  REAL,
            alert_type  TEXT,
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def _alpaca_headers() -> dict | None:
    """Return Alpaca auth headers from env, or None if keys missing."""
    from dotenv import load_dotenv
    load_dotenv()
    key    = os.getenv("ALPACA_API_KEY", "")
    secret = os.getenv("ALPACA_SECRET_KEY", "")
    if not key or not secret:
        return None
    return {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}


def get_volume_data(symbols: list[str]) -> dict[str, dict]:
    """Fetch 5-day daily bars from Alpaca (primary) for volume ratio calc.
    Falls back to Yahoo per-symbol if Alpaca unavailable."""
    hdrs = _alpaca_headers()
    if hdrs:
        return _get_volume_data_alpaca(symbols, hdrs)
    return _get_volume_data_yahoo(symbols)


def _get_volume_data_alpaca(symbols: list[str], hdrs: dict) -> dict[str, dict]:
    """Single bulk Alpaca bars call for all symbols — no per-symbol Yahoo rate limit."""
    from datetime import datetime as _dt, timedelta as _td
    results: dict[str, dict] = {}
    try:
        start = (_dt.utcnow() - _td(days=10)).strftime("%Y-%m-%d")
        r = requests.get(
            "https://data.alpaca.markets/v2/stocks/bars",
            headers=hdrs,
            params={
                "symbols":   ",".join(symbols),
                "timeframe": "1Day",
                "start":     start,
                "limit":     len(symbols) * 10,
                "feed":      "iex",
            },
            timeout=15,
        )
        if not r.ok:
            log.warning("Alpaca bars %s — falling back to Yahoo", r.status_code)
            return _get_volume_data_yahoo(symbols)

        bars_by_sym = r.json().get("bars", {})
        for symbol in symbols:
            try:
                bars = bars_by_sym.get(symbol, [])
                if len(bars) < 2:
                    continue
                vols   = [float(b.get("v") or 0) for b in bars]
                closes = [float(b.get("c") or 0) for b in bars]
                if not closes[-1]:
                    continue
                cur_vol    = vols[-1]
                prev_vol   = vols[-2] or 1
                cur_price  = closes[-1]
                prev_close = closes[-2]
                change_pct = ((cur_price - prev_close) / max(prev_close, 0.01)) * 100
                rel_vol    = cur_vol / max(prev_vol, 1)
                results[symbol] = {
                    "price":       round(cur_price, 2),
                    "volume":      int(cur_vol),
                    "prev_volume": int(prev_vol),
                    "rel_volume":  round(rel_vol, 2),
                    "change_pct":  round(change_pct, 2),
                }
            except Exception as e:
                log.debug(f"{symbol}: parse error — {e}")
    except Exception as e:
        log.warning(f"Alpaca volume fetch failed — {e}")
        return _get_volume_data_yahoo(symbols)
    return results


def _get_volume_data_yahoo(symbols: list[str]) -> dict[str, dict]:
    """Yahoo fallback: per-symbol with 1s throttle and 429 skip."""
    results: dict[str, dict] = {}
    for symbol in symbols:
        try:
            r = requests.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
                params={"interval": "1d", "range": "5d"},
                headers=YF_HEADERS,
                timeout=10,
            )
            if r.status_code == 429:
                log.warning("Yahoo 429 on %s — stopping Yahoo fallback", symbol)
                break
            r.raise_for_status()
            data    = r.json()["chart"]["result"][0]
            meta    = data["meta"]
            quotes  = data["indicators"]["quote"][0]
            volumes = [v for v in quotes.get("volume", []) if v is not None]
            closes  = [c for c in quotes.get("close",  []) if c is not None]
            if len(volumes) < 2 or len(closes) < 2:
                continue
            cur_vol    = volumes[-1]
            prev_vol   = volumes[-2] or 1
            cur_price  = float(meta.get("regularMarketPrice", closes[-1]))
            prev_close = closes[-2]
            change_pct = ((cur_price - prev_close) / max(prev_close, 0.01)) * 100
            rel_vol    = cur_vol / max(prev_vol, 1)
            results[symbol] = {
                "price":       round(cur_price, 2),
                "volume":      cur_vol,
                "prev_volume": prev_vol,
                "rel_volume":  round(rel_vol, 2),
                "change_pct":  round(change_pct, 2),
            }
            time.sleep(1.0)   # 1 req/sec max
        except Exception as e:
            log.debug(f"{symbol}: volume fetch failed — {e}")
    return results


def send_ntfy(message: str, priority: str = "default"):
    try:
        requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers={"Priority": priority, "Title": "Long Range Sensors"},
            timeout=5,
        )
    except Exception as e:
        log.warning(f"ntfy failed: {e}")


def post_signal(symbol: str, price: float, rel_vol: float, change_pct: float,
                alert_type: str):
    """Post to Signal Center using correct Tractor Beam payload format."""
    confidence = 85 if alert_type == "whale" else 95
    action     = "BUY" if change_pct >= 0 else "SELL"
    label      = "MEGA WHALE" if alert_type == "mega_whale" else "WHALE"
    payload = {
        "symbol":          symbol,
        "action":          action,
        "type":            "SWING",
        "confidence":      confidence,
        "agent":           "long_range_sensors",
        "model":           "volume_analysis",
        "reasoning":       (f"[LRS] {label}: {rel_vol:.0f}x volume at ${price:.2f} "
                            f"({change_pct:+.1f}%)"),
        "price":           price,
        "stop_loss":       round(price * 0.95, 2),
        "take_profit":     round(price * 1.10, 2),
        "timeframe":       "SWING",
        "context_summary": f"Unusual volume {rel_vol:.0f}x previous day | {alert_type}",
        "sources":         ["long_range_sensors", "yahoo_finance"],
    }
    try:
        r = requests.post(SIGNAL_CENTER_URL, json=payload, timeout=5)
        if r.status_code in (200, 201):
            log.info(f"  ✓ Signal posted: {action} {symbol}")
        else:
            log.warning(f"  ✗ {symbol} signal → HTTP {r.status_code}")
    except Exception as e:
        log.warning(f"  ✗ signal post failed: {e}")


def log_detection(symbol: str, rel_vol: float, price: float,
                  change_pct: float, alert_type: str):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO whale_detections (symbol, rel_volume, price, change_pct, alert_type) "
        "VALUES (?, ?, ?, ?, ?)",
        (symbol, rel_vol, price, change_pct, alert_type),
    )
    conn.commit()
    conn.close()


def scan_for_whales() -> list[tuple]:
    """Scan WATCHLIST for unusual volume. Call every 5 min during market hours."""
    log.info(f"Scanning {len(WATCHLIST)} tickers for unusual volume...")
    volume_data = get_volume_data(WATCHLIST)
    detections  = []

    for symbol, d in volume_data.items():
        rv      = d["rel_volume"]
        price   = d["price"]
        change  = d["change_pct"]

        if rv >= VOLUME_MULTIPLIER_MEGA:
            alert_type = "mega_whale"
            msg = (f"MEGA WHALE: {symbol} {rv:.0f}x volume @ ${price:.2f} "
                   f"({change:+.1f}%)")
            send_ntfy(msg, priority="urgent")
            post_signal(symbol, price, rv, change, alert_type)
            log.info(f"  {msg}")

        elif rv >= VOLUME_MULTIPLIER_WHALE:
            alert_type = "whale"
            msg = f"WHALE: {symbol} {rv:.0f}x volume @ ${price:.2f} ({change:+.1f}%)"
            send_ntfy(msg, priority="high")
            post_signal(symbol, price, rv, change, alert_type)
            log.info(f"  {msg}")

        elif rv >= VOLUME_MULTIPLIER_ALERT:
            alert_type = "spike"
            log.info(f"  Volume spike: {symbol} {rv:.1f}x @ ${price:.2f} "
                     f"({change:+.1f}%)")
        else:
            continue   # normal volume — skip

        log_detection(symbol, rv, price, change, alert_type)
        detections.append((alert_type, symbol, rv))

    log.info(f"Scan complete: {len(detections)} detections "
             f"({sum(1 for d in detections if d[0] in ('whale','mega_whale'))} whale-level)")
    return detections


def run():
    init_table()
    log.info("=== Long Range Sensors — Whale Volume Detector ===")
    scan_for_whales()
    log.info("=== Done ===")


if __name__ == "__main__":
    run()
