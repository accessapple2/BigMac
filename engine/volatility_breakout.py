"""Volatility Breakout Scanner — Opening Range breakouts with ATR confirmation.

Detects intraday breakouts above/below the first 15-minute Opening Range,
confirmed by ATR(14) expansion and above-average volume. Feeds signals to
AI models and DayBlade for high-probability entries.
"""
from __future__ import annotations
import sqlite3
import time
import threading
from datetime import datetime, timedelta
from rich.console import Console

console = Console()
DB = "data/trader.db"

# ── Cache ──────────────────────────────────────────────────────────
_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 120  # 2 minutes — breakouts are time-sensitive

# ── Breakout Tracking (for success rate) ──────────────────────────
_TRACKING_TABLE_CREATED = False


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def ensure_breakout_table():
    global _TRACKING_TABLE_CREATED
    if _TRACKING_TABLE_CREATED:
        return
    conn = _conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS volatility_breakouts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            breakout_price REAL NOT NULL,
            or_high REAL,
            or_low REAL,
            atr REAL,
            volume_ratio REAL,
            score REAL,
            outcome_pct REAL,
            outcome_hit INTEGER DEFAULT 0,
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            resolved_at TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    _TRACKING_TABLE_CREATED = True


def _get_cached(key: str):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and time.time() - entry["ts"] < _CACHE_TTL:
            return entry["data"]
    return None


def _set_cached(key: str, data):
    with _cache_lock:
        _cache[key] = {"data": data, "ts": time.time()}


# ── Core Scanner ───────────────────────────────────────────────────

def compute_opening_range(candles_5m: list) -> dict | None:
    """Compute the Opening Range from the first 15 minutes (3 × 5-min candles).

    Returns {or_high, or_low, or_range} or None if insufficient data.
    """
    if len(candles_5m) < 3:
        return None

    # First 3 candles = first 15 minutes
    or_candles = candles_5m[:3]
    or_high = max(c["high"] for c in or_candles)
    or_low = min(c["low"] for c in or_candles)

    return {
        "or_high": or_high,
        "or_low": or_low,
        "or_range": or_high - or_low,
    }


def compute_atr(candles: list, period: int = 14) -> float | None:
    """Compute ATR(14) from OHLCV candles."""
    if len(candles) < period + 1:
        return None

    true_ranges = []
    for i in range(1, len(candles)):
        h = candles[i]["high"]
        l = candles[i]["low"]
        prev_c = candles[i - 1]["close"]
        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
        true_ranges.append(tr)

    if len(true_ranges) < period:
        return None

    return sum(true_ranges[-period:]) / period


def compute_volume_ratio(candles: list) -> float:
    """Peak volume since opening range vs 20-candle average.
    Uses the highest volume bar since the OR (excluding in-progress candle with 0 vol),
    because breakouts spike on one bar then normalize.
    """
    if len(candles) < 6:
        return 1.0

    # Post-OR candles (skip first 3 = opening range)
    post_or = [c for c in candles[3:] if c["volume"] > 0]
    if not post_or:
        return 1.0

    # Peak volume since opening range
    peak_vol = max(c["volume"] for c in post_or)

    # Average volume across all completed candles (excluding zero-vol in-progress)
    valid = [c for c in candles if c["volume"] > 0]
    if len(valid) < 3:
        return 1.0
    avg_vol = sum(c["volume"] for c in valid) / len(valid)
    if avg_vol <= 0:
        return 1.0
    return round(peak_vol / avg_vol, 2)


def detect_breakout(symbol: str) -> dict | None:
    """Detect an Opening Range breakout for a single symbol.

    Returns breakout signal dict or None if no breakout.
    """
    cached = _get_cached(f"breakout_{symbol}")
    if cached is not None:
        return cached if cached else None

    from engine.market_data import get_intraday_candles

    candles = get_intraday_candles(symbol, interval="5m", range_="1d")
    if not candles or len(candles) < 6:  # Need at least 30 min of data
        _set_cached(f"breakout_{symbol}", False)
        return None

    # Opening range from first 15 min
    opening = compute_opening_range(candles)
    if not opening or opening["or_range"] <= 0:
        _set_cached(f"breakout_{symbol}", False)
        return None

    # ATR(14) on 5-min candles
    atr = compute_atr(candles, period=14)
    if not atr or atr <= 0:
        _set_cached(f"breakout_{symbol}", False)
        return None

    # Current price = last candle close
    current = candles[-1]["close"]
    current_vol_ratio = compute_volume_ratio(candles)

    # Breakout thresholds
    breakout_buffer = 0.5 * atr
    bull_level = opening["or_high"] + breakout_buffer
    bear_level = opening["or_low"] - breakout_buffer

    direction = None
    breakout_price = None
    distance = 0

    if current > bull_level:
        direction = "BULL"
        breakout_price = bull_level
        distance = current - bull_level
    elif current < bear_level:
        direction = "BEAR"
        breakout_price = bear_level
        distance = bear_level - current

    if not direction:
        _set_cached(f"breakout_{symbol}", False)
        return None

    # Volume confirmation: need above-average volume
    if current_vol_ratio < 1.2:
        _set_cached(f"breakout_{symbol}", False)
        return None

    # Score the breakout (0-100)
    # Components: volume (0-40), distance (0-30), time of day (0-30)
    vol_score = min(40, current_vol_ratio * 15)

    distance_pct = (distance / current) * 100 if current > 0 else 0
    dist_score = min(30, distance_pct * 30)

    # Time of day: earlier breakouts are stronger (candle index / total candles)
    candle_index = len(candles) - 3  # exclude opening range candles
    total_possible = 78  # ~6.5 hours of trading in 5-min candles
    time_pct = candle_index / total_possible if total_possible > 0 else 0.5
    time_score = max(0, 30 * (1 - time_pct))  # Earlier = higher score

    total_score = round(vol_score + dist_score + time_score, 1)

    result = {
        "symbol": symbol,
        "direction": direction,
        "current_price": current,
        "breakout_price": round(breakout_price, 2),
        "or_high": opening["or_high"],
        "or_low": opening["or_low"],
        "or_range": round(opening["or_range"], 2),
        "atr": round(atr, 4),
        "volume_ratio": current_vol_ratio,
        "distance": round(distance, 2),
        "distance_pct": round(distance_pct, 2),
        "score": total_score,
        "candles_since_open": len(candles) - 3,
        "detected_at": datetime.now().isoformat(),
    }

    _set_cached(f"breakout_{symbol}", result)
    return result


def scan_all_breakouts(symbols: list = None) -> list:
    """Scan all watchlist symbols for volatility breakouts."""
    if symbols is None:
        from config import WATCH_STOCKS
        symbols = WATCH_STOCKS

    from concurrent.futures import ThreadPoolExecutor, as_completed

    breakouts = []
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(detect_breakout, sym): sym for sym in symbols}
        for future in as_completed(futures, timeout=60):
            try:
                result = future.result(timeout=10)
                if result:
                    breakouts.append(result)
            except Exception:
                pass

    # Sort by score descending
    breakouts.sort(key=lambda x: x["score"], reverse=True)
    return breakouts


# ── AI Prompt Injection ────────────────────────────────────────────

def build_breakout_prompt_section(symbol: str) -> str:
    """Build prompt section for AI models when a breakout is detected."""
    breakout = detect_breakout(symbol)
    if not breakout:
        return ""

    dir_label = "BULLISH" if breakout["direction"] == "BULL" else "BEARISH"
    level = "opening range high" if breakout["direction"] == "BULL" else "opening range low"
    or_price = breakout["or_high"] if breakout["direction"] == "BULL" else breakout["or_low"]

    strength = "STRONG" if breakout["score"] >= 60 else "MODERATE" if breakout["score"] >= 40 else "WEAK"

    return (
        f"\n=== VOLATILITY BREAKOUT: {symbol} {dir_label} ===\n"
        f"Price broke {'above' if breakout['direction'] == 'BULL' else 'below'} "
        f"${or_price:.2f} {level} on {breakout['volume_ratio']:.1f}x volume.\n"
        f"Opening Range: ${breakout['or_low']:.2f} - ${breakout['or_high']:.2f} "
        f"(range ${breakout['or_range']:.2f})\n"
        f"ATR(14) 5m: ${breakout['atr']:.2f} | Distance: +{breakout['distance_pct']:.1f}%\n"
        f"Breakout Score: {breakout['score']:.0f}/100 [{strength}]\n"
        f"{'HIGH PROBABILITY SETUP — consider immediate entry.' if breakout['score'] >= 60 else 'Monitor for confirmation.'}\n"
    )


def build_dayblade_breakout_section(symbol: str) -> str:
    """Build DayBlade-specific breakout section (more aggressive framing)."""
    breakout = detect_breakout(symbol)
    if not breakout:
        return ""

    dir_label = "BULL" if breakout["direction"] == "BULL" else "BEAR"
    action = "BUY_CALL" if dir_label == "BULL" else "BUY_PUT"

    return (
        f"\n=== OPENING RANGE BREAKOUT — {action} SETUP ===\n"
        f"{symbol} broke {'above OR high' if dir_label == 'BULL' else 'below OR low'} "
        f"${breakout['or_high'] if dir_label == 'BULL' else breakout['or_low']:.2f} "
        f"+ 0.5×ATR buffer on {breakout['volume_ratio']:.1f}x volume.\n"
        f"Score: {breakout['score']:.0f}/100 | This is the highest-probability 0DTE setup.\n"
        f"PRIORITY: Take this trade if score > 50 and volume > 1.5x.\n"
    )


# ── Breakout Tracking & Success Rate ──────────────────────────────

def record_breakout(breakout: dict):
    """Save a breakout detection for later success tracking."""
    ensure_breakout_table()
    conn = _conn()
    conn.execute(
        "INSERT INTO volatility_breakouts "
        "(symbol, direction, breakout_price, or_high, or_low, atr, volume_ratio, score) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (breakout["symbol"], breakout["direction"], breakout["breakout_price"],
         breakout["or_high"], breakout["or_low"], breakout["atr"],
         breakout["volume_ratio"], breakout["score"])
    )
    conn.commit()
    conn.close()


def check_breakout_outcomes():
    """Check unresolved breakouts — did price continue +1% in breakout direction?"""
    ensure_breakout_table()
    from engine.market_data import get_stock_price

    conn = _conn()
    pending = conn.execute(
        "SELECT id, symbol, direction, breakout_price FROM volatility_breakouts "
        "WHERE resolved_at IS NULL AND detected_at >= datetime('now', '-1 day')"
    ).fetchall()

    resolved = 0
    for row in pending:
        price_data = get_stock_price(row["symbol"])
        if "error" in price_data:
            continue

        current = price_data["price"]
        entry = row["breakout_price"]
        if entry <= 0:
            continue

        pct_move = ((current - entry) / entry) * 100
        if row["direction"] == "BEAR":
            pct_move = -pct_move  # For bear breakouts, down is positive

        # Resolve if +1% hit OR if older than 4 hours
        hit = 1 if pct_move >= 1.0 else 0
        age_hours = 4  # Auto-resolve after 4 hours
        detected = row["detected_at"] if hasattr(row, "detected_at") else None

        if hit or pct_move <= -2.0:  # Hit target or stopped out
            conn.execute(
                "UPDATE volatility_breakouts SET outcome_pct=?, outcome_hit=?, resolved_at=CURRENT_TIMESTAMP WHERE id=?",
                (round(pct_move, 2), hit, row["id"])
            )
            resolved += 1

    conn.commit()
    conn.close()
    return resolved


def get_breakout_stats() -> dict:
    """Get breakout success rate statistics."""
    ensure_breakout_table()
    conn = _conn()

    total = conn.execute("SELECT COUNT(*) FROM volatility_breakouts WHERE resolved_at IS NOT NULL").fetchone()[0]
    hits = conn.execute("SELECT COUNT(*) FROM volatility_breakouts WHERE outcome_hit=1").fetchone()[0]
    pending = conn.execute("SELECT COUNT(*) FROM volatility_breakouts WHERE resolved_at IS NULL").fetchone()[0]

    avg_outcome = 0
    if total > 0:
        row = conn.execute("SELECT AVG(outcome_pct) FROM volatility_breakouts WHERE resolved_at IS NOT NULL").fetchone()
        avg_outcome = round(row[0] or 0, 2)

    # By score bucket
    buckets = {}
    for label, lo, hi in [("strong", 60, 100), ("moderate", 40, 60), ("weak", 0, 40)]:
        row = conn.execute(
            "SELECT COUNT(*) as total, SUM(CASE WHEN outcome_hit=1 THEN 1 ELSE 0 END) as hits "
            "FROM volatility_breakouts WHERE resolved_at IS NOT NULL AND score >= ? AND score < ?",
            (lo, hi)
        ).fetchone()
        t = row["total"]
        h = row["hits"] or 0
        buckets[label] = {"total": t, "hits": h, "rate": round(h / t * 100, 1) if t > 0 else 0}

    conn.close()

    return {
        "total_resolved": total,
        "total_hits": hits,
        "success_rate": round(hits / total * 100, 1) if total > 0 else 0,
        "avg_outcome_pct": avg_outcome,
        "pending": pending,
        "by_score": buckets,
    }


def get_recent_breakouts(limit: int = 20) -> list:
    """Get recent breakout signals for dashboard display."""
    ensure_breakout_table()
    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM volatility_breakouts ORDER BY detected_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── Main Runner (called from scan cycle) ──────────────────────────

def run_breakout_scan(symbols: list = None) -> list:
    """Full scan: detect breakouts, record them, check outcomes, log results."""
    breakouts = scan_all_breakouts(symbols)

    # Record new breakouts
    for b in breakouts:
        try:
            record_breakout(b)
        except Exception:
            pass  # Duplicate or DB busy

    # Check outcomes of previous breakouts
    try:
        check_breakout_outcomes()
    except Exception:
        pass

    if breakouts:
        symbols_str = ", ".join(f"{b['symbol']}({b['direction']})" for b in breakouts[:5])
        console.log(f"[bold yellow]BREAKOUT: {symbols_str}")

    return breakouts
