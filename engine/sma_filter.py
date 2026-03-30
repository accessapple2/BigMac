"""200 SMA Filter — detect key 200-day SMA signals for the watchlist.

Signal types:
  "200 SMA Bounce"    — price tested 200 SMA from above and bounced up
  "200 SMA Breakdown" — price crossed below 200 SMA (yesterday above, today below)
  "200 SMA Reclaim"   — price crossed back above 200 SMA (yesterday below, today above)

Dashboard icons (colorblind-safe, not just color):
  ▲  above 200 SMA (healthy)
  ◆  testing 200 SMA (within ±2%)
  ▼  below 200 SMA (caution)
  ▲! Reclaim signal
  ▼! Breakdown signal
  ◆▲ Bounce signal
"""
from __future__ import annotations
import sqlite3
import threading
import time
from datetime import date
from pathlib import Path
from rich.console import Console

console = Console()
DB = "data/trader.db"

_cache: dict = {"data": {}, "ts": 0}
_cache_lock = threading.Lock()
_CACHE_TTL = 900  # 15 minutes


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def ensure_sma_table():
    conn = _conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sma_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            sma_200_value REAL,
            current_price REAL,
            signal_type TEXT,
            distance_pct REAL,
            above_sma200 INTEGER DEFAULT 1,
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def _fetch_history(symbol: str) -> list[float] | None:
    """Fetch ~210 days of daily closes via yfinance. Returns list oldest→newest."""
    try:
        import yfinance as yf
        tk = yf.Ticker(symbol)
        hist = tk.history(period="1y", auto_adjust=True)
        if hist.empty or len(hist) < 200:
            return None
        return list(hist["Close"])
    except Exception as e:
        console.log(f"[yellow]SMA fetch error {symbol}: {e}")
        return None


def get_sma_200_status(symbol: str) -> dict:
    """Compute 200 SMA status and classify signal type for a single symbol."""
    closes = _fetch_history(symbol)
    if closes is None or len(closes) < 201:
        return {}

    # Last 3 days for signal detection
    c0 = closes[-1]   # today
    c1 = closes[-2]   # yesterday
    c2 = closes[-3]   # day before

    # Rolling 200 SMA at each of those days
    def sma(offset: int) -> float:
        window = closes[-(200 + offset): -offset if offset > 0 else None]
        return sum(window) / 200

    sma0 = sma(0)
    sma1 = sma(1)

    dist0 = (c0 - sma0) / sma0 * 100   # today's distance %
    dist1 = (c1 - sma1) / sma1 * 100   # yesterday's distance %

    above_today = c0 >= sma0
    above_yest  = c1 >= sma1
    is_testing  = abs(dist0) <= 2.0

    signal_type: str | None = None

    if not above_today and above_yest:
        signal_type = "200 SMA Breakdown"
    elif above_today and not above_yest:
        signal_type = "200 SMA Reclaim"
    elif above_today and abs(dist1) <= 2.5 and c0 > c1:
        # Was hugging SMA from above, now bouncing higher
        signal_type = "200 SMA Bounce"

    # Dashboard icon (colorblind-safe)
    if signal_type == "200 SMA Breakdown":
        icon = "▼!"
    elif signal_type == "200 SMA Reclaim":
        icon = "▲!"
    elif signal_type == "200 SMA Bounce":
        icon = "◆▲"
    elif is_testing and above_today:
        icon = "◆"
    elif is_testing and not above_today:
        icon = "◆"
    elif above_today:
        icon = "▲"
    else:
        icon = "▼"

    return {
        "symbol": symbol,
        "sma_200": round(sma0, 2),
        "current_price": round(c0, 2),
        "distance_pct": round(dist0, 2),
        "above_sma200": above_today,
        "is_testing": is_testing,
        "signal_type": signal_type,
        "icon": icon,
    }


def save_sma_signal(status: dict) -> None:
    """Persist a signal to sma_signals table. Deduplicates by ticker+date+signal_type."""
    if not status or not status.get("signal_type"):
        return
    today = date.today().isoformat()
    conn = _conn()
    try:
        existing = conn.execute(
            "SELECT id FROM sma_signals WHERE ticker=? AND date=? AND signal_type=?",
            (status["symbol"], today, status["signal_type"])
        ).fetchone()
        if not existing:
            conn.execute(
                "INSERT INTO sma_signals (ticker, date, sma_200_value, current_price, "
                "signal_type, distance_pct, above_sma200) VALUES (?,?,?,?,?,?,?)",
                (
                    status["symbol"], today,
                    status["sma_200"], status["current_price"],
                    status["signal_type"], status["distance_pct"],
                    1 if status["above_sma200"] else 0,
                )
            )
            conn.commit()
            console.log(f"[cyan]200 SMA: {status['symbol']} → {status['signal_type']} "
                        f"({status['distance_pct']:+.2f}% from ${status['sma_200']:.2f})")
    except Exception as e:
        console.log(f"[red]SMA save error: {e}")
    finally:
        conn.close()


def scan_all_sma_signals(symbols: list[str] | None = None) -> dict[str, dict]:
    """Scan all symbols, save signals, return status dict keyed by symbol."""
    if symbols is None:
        from config import WATCH_STOCKS
        symbols = WATCH_STOCKS

    from concurrent.futures import ThreadPoolExecutor, as_completed
    results: dict[str, dict] = {}

    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(get_sma_200_status, sym): sym for sym in symbols}
        for fut in as_completed(futs, timeout=120):
            sym = futs[fut]
            try:
                st = fut.result()
                if st:
                    results[sym] = st
                    save_sma_signal(st)
            except Exception as e:
                console.log(f"[red]SMA scan error {sym}: {e}")

    with _cache_lock:
        _cache["data"] = results
        _cache["ts"] = time.time()

    return results


def get_cached_sma_status(symbols: list[str] | None = None) -> dict[str, dict]:
    """Return cached SMA status, refreshing if stale."""
    with _cache_lock:
        age = time.time() - _cache["ts"]
        if _cache["data"] and age < _CACHE_TTL:
            return dict(_cache["data"])
    return scan_all_sma_signals(symbols)


def build_sma_prompt_section(symbol: str, indicators: dict | None = None) -> str:
    """Return a formatted 200 SMA context string for injection into AI prompts.

    Only returns content when the stock is testing the 200 SMA or has a fresh signal.
    Uses cached data to avoid extra yfinance calls per-symbol during scan.
    """
    with _cache_lock:
        cached = _cache["data"].get(symbol)

    if not cached and indicators:
        # Fall back to pre-computed indicator data if no cache yet
        sma200 = indicators.get("sma_200")
        price = indicators.get("current_price") or indicators.get("price")
        if not sma200 or not price:
            return ""
        dist = (price - sma200) / sma200 * 100
        if abs(dist) > 5.0:
            return ""
        above = price >= sma200
        return (
            f"\n=== 200 SMA CONTEXT ===\n"
            f"  200 SMA: ${sma200:.2f} | Price: ${price:.2f} | Distance: {dist:+.2f}%\n"
            f"  Position: {'ABOVE' if above else 'BELOW'} 200 SMA — institutional decision zone\n"
            f"  ⚠ Price is within 5% of the 200-day SMA. This is a critical technical level.\n"
            f"  Institutions and hedge funds watch the 200 SMA closely for entries/exits.\n"
        )

    if not cached:
        return ""

    dist = cached.get("distance_pct", 0)
    sma200 = cached.get("sma_200", 0)
    price = cached.get("current_price", 0)
    signal = cached.get("signal_type")
    is_testing = cached.get("is_testing", False)
    above = cached.get("above_sma200", True)
    icon = cached.get("icon", "▲")

    # Only inject if within 5% or there's an active signal
    if abs(dist) > 5.0 and not signal:
        return ""

    lines = [
        f"\n=== 200 SMA ANALYSIS [{icon}] ===",
        f"  200 SMA: ${sma200:.2f} | Current: ${price:.2f} | Distance: {dist:+.2f}%",
        f"  Position: {'ABOVE' if above else 'BELOW'} — {'Testing from above' if above and is_testing else 'Testing from below' if not above and is_testing else ('Healthy above' if above else 'Under pressure')}",
    ]

    if signal == "200 SMA Breakdown":
        lines += [
            "  ▼! BREAKDOWN SIGNAL: Price just crossed BELOW the 200 SMA.",
            "  This is a major bearish development — institutions may be selling.",
            "  Watch for re-test of 200 SMA as resistance. High-confidence SELL if holding.",
        ]
    elif signal == "200 SMA Reclaim":
        lines += [
            "  ▲! RECLAIM SIGNAL: Price crossed back ABOVE the 200 SMA.",
            "  This is a major bullish reversal — institutions may be re-accumulating.",
            "  Reclaims often lead to strong rallies if confirmed with volume.",
        ]
    elif signal == "200 SMA Bounce":
        lines += [
            "  ◆▲ BOUNCE SIGNAL: Price tested 200 SMA and bounced higher.",
            "  Classic institutional buy zone — big money defends the 200 SMA.",
            "  High-probability long setup if RSI confirms (not overbought).",
        ]
    elif is_testing:
        lines.append(
            "  ◆ TESTING 200 SMA: Price within 2% — this is a decision point.\n"
            "  Breakdowns below are very bearish; holds above are very bullish.\n"
            "  Wait for directional confirmation before committing capital."
        )

    return "\n".join(lines) + "\n"


def get_recent_sma_signals(limit: int = 30) -> list[dict]:
    """Fetch recent signals from the sma_signals table for the dashboard."""
    conn = _conn()
    rows = conn.execute(
        "SELECT ticker, date, sma_200_value, current_price, signal_type, "
        "distance_pct, above_sma200, detected_at "
        "FROM sma_signals ORDER BY detected_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
