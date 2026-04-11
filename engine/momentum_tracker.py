"""
Intraday Momentum Tracker
--------------------------
Uses Alpaca 5-min SPY bars to compute cumulative volume delta and a
trend score from -100 (max bearish) to +100 (max bullish).

Table: intraday_momentum  (never dropped)
Endpoint: GET /api/ready-room/momentum
"""
from __future__ import annotations

import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any

DB = "autonomous_trader.db"

_cache: dict[str, Any] = {}
_cache_ts: float = 0.0
CACHE_TTL = 120  # 2 minutes


def _init_db() -> None:
    conn = sqlite3.connect(DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS intraday_momentum (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol          TEXT NOT NULL DEFAULT 'SPY',
            bars_count      INTEGER,
            buy_volume      REAL,
            sell_volume     REAL,
            volume_delta    REAL,
            trend_score     REAL,
            vwap            REAL,
            last_price      REAL,
            price_vs_vwap   REAL,
            signal          TEXT,
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()


_init_db()


def _fetch_bars(symbol: str = "SPY") -> list[dict]:
    """Fetch today's 5-min bars from Alpaca."""
    try:
        from alpaca.data import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame
        from datetime import date

        api_key = os.environ.get("ALPACA_API_KEY", "")
        secret  = os.environ.get("ALPACA_SECRET_KEY", "")
        client  = StockHistoricalDataClient(api_key, secret)

        today = date.today()
        req = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=TimeFrame.Minute,
            start=datetime(today.year, today.month, today.day, 9, 30),
            limit=80,  # up to ~6.5 hours of 5-min bars
            feed="iex",
        )
        bars_resp = client.get_stock_bars(req)
        raw = bars_resp.data.get(symbol, [])
        return [
            {
                "open":   b.open,
                "close":  b.close,
                "volume": b.volume,
                "vwap":   getattr(b, "vwap", None),
            }
            for b in raw
        ]
    except Exception as exc:
        return []


def _compute_delta(bars: list[dict]) -> dict[str, Any]:
    buy_vol = sell_vol = 0.0
    vwap_sum = vwap_count = 0
    last_close = None

    for b in bars:
        vol = b.get("volume") or 0
        if (b.get("close") or 0) >= (b.get("open") or 0):
            buy_vol += vol
        else:
            sell_vol += vol
        if b.get("vwap"):
            vwap_sum += b["vwap"]
            vwap_count += 1
        last_close = b.get("close")

    total = buy_vol + sell_vol
    delta = buy_vol - sell_vol

    # Trend score: delta as % of total, scaled to -100..+100
    trend_score = round((delta / total) * 100, 1) if total > 0 else 0.0

    vwap_avg = round(vwap_sum / vwap_count, 2) if vwap_count else None
    price_vs_vwap = None
    if last_close and vwap_avg:
        price_vs_vwap = round((last_close - vwap_avg) / vwap_avg * 100, 3)

    # Signal
    if trend_score >= 60:
        signal = f"🚀 Strong bullish momentum (score {trend_score}). Buy pressure dominant."
    elif trend_score >= 30:
        signal = f"📈 Mild bullish bias (score {trend_score}). Watch for continuation."
    elif trend_score <= -60:
        signal = f"🔻 Strong bearish momentum (score {trend_score}). Sell pressure dominant."
    elif trend_score <= -30:
        signal = f"📉 Mild bearish bias (score {trend_score}). Watch for breakdown."
    else:
        signal = f"⚖️ Neutral / choppy (score {trend_score}). No clear edge."

    return {
        "bars_count":   len(bars),
        "buy_volume":   round(buy_vol, 0),
        "sell_volume":  round(sell_vol, 0),
        "volume_delta": round(delta, 0),
        "trend_score":  trend_score,
        "vwap":         vwap_avg,
        "last_price":   last_close,
        "price_vs_vwap": price_vs_vwap,
        "signal":       signal,
    }


def get_intraday_momentum(symbol: str = "SPY", force: bool = False) -> dict[str, Any]:
    global _cache, _cache_ts
    now = time.time()
    if not force and _cache and (now - _cache_ts) < CACHE_TTL:
        return _cache

    bars = _fetch_bars(symbol)
    if not bars:
        result = {
            "symbol": symbol,
            "bars_count": 0,
            "trend_score": 0,
            "signal": "No intraday bar data available (market may be closed).",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        _cache = result
        _cache_ts = now
        return result

    delta = _compute_delta(bars)
    result = {"symbol": symbol, **delta, "fetched_at": datetime.now(timezone.utc).isoformat()}

    try:
        conn = sqlite3.connect(DB, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            INSERT INTO intraday_momentum
                (symbol, bars_count, buy_volume, sell_volume, volume_delta,
                 trend_score, vwap, last_price, price_vs_vwap, signal)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            symbol,
            delta["bars_count"], delta["buy_volume"], delta["sell_volume"],
            delta["volume_delta"], delta["trend_score"], delta["vwap"],
            delta["last_price"], delta["price_vs_vwap"], delta["signal"],
        ))
        conn.commit()
        conn.close()
    except Exception:
        pass

    _cache = result
    _cache_ts = now
    return result
