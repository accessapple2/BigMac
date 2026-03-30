"""Morning Gap Scanner — classify and track intraday price gaps.

Runs at market open (9:30–10:00 AM ET = 7:30–8:00 AM AZ) using actual opening prices
vs previous close. Tracks gap fills throughout the trading day.

Gap Types:
  Common:    Low volume, range-bound.  Fill prob: 75%  — most likely fills same day.
  Breakaway: High volume (>2x), breaks key 20d level.  Fill prob: 15%  — trend starter.
  Runaway:   Mid-trend, moderate/high volume.           Fill prob: 20%  — continuation.
  Exhaustion: Extended move + climactic volume.         Fill prob: 65%  — reversal likely.

Fill Status: OPEN → PARTIAL (>50% filled) → FILLED (price returned to prev close).

Dashboard icons: ▲ gap up (green) · ▼ gap down (red) · ↔ PARTIAL · ✓ FILLED
"""
from __future__ import annotations
import sqlite3
import threading
import time
from datetime import datetime, date, timedelta
from rich.console import Console

console = Console()
DB = "data/trader.db"
MIN_GAP_PCT = 0.5  # ignore gaps smaller than this

_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 300  # 5-minute cache for live data


# ── Database ──────────────────────────────────────────────────────────────────

def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def ensure_gap_table():
    conn = _conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gap_scanner (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            gap_direction TEXT NOT NULL,
            gap_pct REAL NOT NULL,
            gap_type TEXT NOT NULL,
            volume_ratio REAL,
            fill_probability REAL,
            filled INTEGER DEFAULT 0,
            fill_time_minutes INTEGER,
            fill_status TEXT DEFAULT 'OPEN',
            prev_close REAL,
            open_price REAL,
            high_of_day REAL,
            low_of_day REAL,
            sma20 REAL,
            scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            filled_at TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


# ── Market data helpers ────────────────────────────────────────────────────────

def _get_daily_candles(symbol: str, days: int = 25) -> list[dict]:
    """Fetch N days of daily OHLCV candles."""
    from engine.market_data import get_intraday_candles
    candles = get_intraday_candles(symbol, interval="1d", range_="2mo")
    return candles[-days:] if candles else []


def _get_current_price(symbol: str) -> float | None:
    """Return latest price for fill tracking."""
    try:
        from engine.market_data import get_stock_price
        d = get_stock_price(symbol)
        return d.get("price")
    except Exception:
        return None


# ── Gap classification ─────────────────────────────────────────────────────────

def _classify_gap(gap_pct: float, volume_ratio: float,
                   context: dict) -> tuple[str, float]:
    """Return (gap_type, fill_probability).

    context keys: near_high, near_low, in_trend, overextended
    """
    # Exhaustion: overextended price + climactic volume = likely reversal
    if context.get("overextended") and volume_ratio >= 1.5:
        return "Exhaustion", 65.0

    # Breakaway: breaks 20-day range boundary on heavy volume = trend start
    is_breakout = context.get("near_high") and gap_pct > 0
    is_breakdown = context.get("near_low") and gap_pct < 0
    if volume_ratio >= 2.0 and (is_breakout or is_breakdown):
        return "Breakaway", 15.0

    # Runaway: established trend + moderate-high volume = continuation
    if context.get("in_trend") and volume_ratio >= 1.5 and not context.get("overextended"):
        return "Runaway", 20.0

    # Common: low volume, range-bound, or unclear context = likely fills
    if volume_ratio < 1.5:
        return "Common", 75.0

    # High volume but no special context → lean Breakaway
    if volume_ratio >= 2.0:
        return "Breakaway", 20.0

    return "Common", 55.0


# ── Core per-symbol scan ───────────────────────────────────────────────────────

def scan_gap(symbol: str) -> dict | None:
    """Detect and classify today's gap for one symbol.

    Uses actual market-open price vs previous close (requires market to be open
    or have at least one candle today). Returns None if no qualifying gap found.
    """
    daily = _get_daily_candles(symbol, days=25)
    if len(daily) < 3:
        return None

    today_str = date.today().isoformat()

    # Determine if today's candle is present
    today_candle = daily[-1]
    prev_candle  = daily[-2]

    today_date = today_candle["time"][:10]
    if today_date != today_str:
        # Today's candle not yet available (market not open / data lag)
        return None

    prev_close = prev_candle["close"]
    today_open = today_candle["open"]

    if not prev_close or not today_open or prev_close <= 0:
        return None

    gap_pct = round((today_open - prev_close) / prev_close * 100, 2)
    if abs(gap_pct) < MIN_GAP_PCT:
        return None

    # ── Volume context ────────────────────────────────────────────────────────
    history = daily[-21:-1]  # 20 completed trading days before today
    if len(history) < 5:
        return None

    avg_vol = sum(c["volume"] for c in history if c.get("volume")) / len(history)
    today_vol = today_candle.get("volume", 0) or 0

    # Adjust partial-day volume to full-day estimate using elapsed trading minutes
    now = datetime.now()
    market_open_today = now.replace(hour=9, minute=30, second=0, microsecond=0)
    elapsed_min = max(1, (now - market_open_today).total_seconds() / 60)
    if elapsed_min < 390:  # market not closed
        projected_vol = today_vol * (390 / min(elapsed_min, 390))
    else:
        projected_vol = today_vol
    volume_ratio = round(projected_vol / avg_vol, 2) if avg_vol > 0 else 1.0

    # ── Price context for classification ─────────────────────────────────────
    recent_highs = [c["high"] for c in history if c.get("high")]
    recent_lows  = [c["low"]  for c in history if c.get("low")]
    closes = [c["close"] for c in history if c.get("close")]

    sma20        = sum(closes) / len(closes) if closes else today_open
    range_high   = max(recent_highs) if recent_highs else today_open
    range_low    = min(recent_lows)  if recent_lows  else today_open

    near_high    = today_open >= range_high * 0.97   # within 3% of 20d high
    near_low     = today_open <= range_low  * 1.03   # within 3% of 20d low
    in_trend     = today_open > sma20
    overextended = today_open > sma20 * 1.15 or today_open < sma20 * 0.85

    context = {
        "near_high":    near_high,
        "near_low":     near_low,
        "in_trend":     in_trend,
        "overextended": overextended,
    }

    gap_type, fill_prob = _classify_gap(gap_pct, volume_ratio, context)

    return {
        "ticker":           symbol,
        "date":             today_str,
        "gap_direction":    "up" if gap_pct > 0 else "down",
        "gap_pct":          gap_pct,
        "gap_type":         gap_type,
        "volume_ratio":     volume_ratio,
        "fill_probability": fill_prob,
        "filled":           False,
        "fill_status":      "OPEN",
        "fill_time_minutes": None,
        "prev_close":       round(prev_close, 2),
        "open_price":       round(today_open,  2),
        "high_of_day":      round(today_candle.get("high", today_open), 2),
        "low_of_day":       round(today_candle.get("low",  today_open), 2),
        "sma20":            round(sma20, 2),
    }


def _save_gap(gap: dict):
    """Upsert gap record — one row per (ticker, date)."""
    conn = _conn()
    try:
        existing = conn.execute(
            "SELECT id, filled FROM gap_scanner WHERE ticker=? AND date=?",
            (gap["ticker"], gap["date"])
        ).fetchone()
        if existing:
            # Only update fill-related fields and volume_ratio; preserve scanned_at
            conn.execute(
                "UPDATE gap_scanner SET volume_ratio=?, high_of_day=?, low_of_day=? WHERE id=?",
                (gap["volume_ratio"], gap["high_of_day"], gap["low_of_day"], existing["id"])
            )
        else:
            conn.execute(
                "INSERT INTO gap_scanner "
                "(ticker, date, gap_direction, gap_pct, gap_type, volume_ratio, "
                "fill_probability, filled, fill_status, fill_time_minutes, "
                "prev_close, open_price, high_of_day, low_of_day, sma20) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    gap["ticker"], gap["date"], gap["gap_direction"], gap["gap_pct"],
                    gap["gap_type"], gap["volume_ratio"], gap["fill_probability"],
                    0, "OPEN", None,
                    gap["prev_close"], gap["open_price"],
                    gap["high_of_day"], gap["low_of_day"], gap["sma20"],
                )
            )
        conn.commit()
    except Exception as e:
        console.log(f"[red]Gap save error ({gap['ticker']}): {e}")
    finally:
        conn.close()


def scan_all_gaps(symbols: list[str] | None = None) -> list[dict]:
    """Scan all watchlist symbols for today's gaps. Returns sorted list."""
    if symbols is None:
        from config import WATCH_STOCKS
        symbols = WATCH_STOCKS

    results = []
    for sym in symbols:
        try:
            gap = scan_gap(sym)
            if gap:
                _save_gap(gap)
                direction_icon = "▲" if gap["gap_direction"] == "up" else "▼"
                console.log(
                    f"[cyan]Gap {direction_icon} {sym}: {gap['gap_pct']:+.2f}% "
                    f"({gap['gap_type']}) VR={gap['volume_ratio']:.1f}x "
                    f"FillP={gap['fill_probability']:.0f}%"
                )
                results.append(gap)
        except Exception as e:
            console.log(f"[yellow]Gap scan skip {sym}: {e}")

    results.sort(key=lambda x: abs(x["gap_pct"]), reverse=True)

    with _cache_lock:
        _cache["today"] = {"gaps": results, "ts": time.time()}

    return results


# ── Fill tracking ─────────────────────────────────────────────────────────────

def update_gap_fills(symbols: list[str] | None = None):
    """Check open gaps from today and mark as filled/partial when price returns."""
    if symbols is None:
        from config import WATCH_STOCKS
        symbols = WATCH_STOCKS

    today = date.today().isoformat()
    conn = _conn()
    open_gaps = conn.execute(
        "SELECT * FROM gap_scanner WHERE date=? AND filled=0",
        (today,)
    ).fetchall()
    conn.close()

    if not open_gaps:
        return

    # Market open time today (AZ = MST, UTC-7; ET opens at 9:30 = 7:30 AZ)
    now = datetime.now()
    market_open = now.replace(hour=7, minute=30, second=0, microsecond=0)
    if now < market_open:
        return  # Pre-market, skip fill check

    for row in open_gaps:
        sym = row["ticker"]
        if sym not in symbols:
            continue
        try:
            price = _get_current_price(sym)
            if not price:
                continue

            prev_close = row["prev_close"]
            gap_dir    = row["gap_direction"]
            open_price = row["open_price"]

            # Gap fill condition
            if gap_dir == "up":
                filled = price <= prev_close
                pct_filled = (open_price - price) / (open_price - prev_close) * 100 if open_price > prev_close else 0
            else:
                filled = price >= prev_close
                pct_filled = (price - open_price) / (prev_close - open_price) * 100 if prev_close > open_price else 0

            pct_filled = max(0, min(100, pct_filled))

            elapsed_min = int((now - market_open).total_seconds() / 60)

            conn2 = _conn()
            if filled:
                conn2.execute(
                    "UPDATE gap_scanner SET filled=1, fill_status='FILLED', "
                    "fill_time_minutes=?, filled_at=CURRENT_TIMESTAMP WHERE id=?",
                    (elapsed_min, row["id"])
                )
                console.log(f"[green]Gap filled: {sym} {gap_dir} in {elapsed_min}min")
            elif pct_filled >= 50:
                conn2.execute(
                    "UPDATE gap_scanner SET fill_status='PARTIAL' WHERE id=?",
                    (row["id"],)
                )
            conn2.commit()
            conn2.close()

        except Exception as e:
            console.log(f"[yellow]Gap fill check skip {sym}: {e}")


# ── DB queries ────────────────────────────────────────────────────────────────

def get_todays_gaps(min_gap_pct: float = 0.0) -> list[dict]:
    """Return today's gaps from DB, sorted by abs gap size."""
    today = date.today().isoformat()
    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM gap_scanner WHERE date=? AND gap_pct >= ? ORDER BY ABS(gap_pct) DESC",
        (today, min_gap_pct)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_recent_gaps(limit: int = 100) -> list[dict]:
    """Return recent gaps across all dates, sorted by date desc then abs gap."""
    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM gap_scanner ORDER BY date DESC, ABS(gap_pct) DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_cached_gaps() -> list[dict]:
    """Return in-memory cached today gaps, or fall back to DB."""
    with _cache_lock:
        cached = _cache.get("today")
        if cached and (time.time() - cached["ts"]) < _CACHE_TTL:
            return cached["gaps"]
    return get_todays_gaps()


def get_gap_fill_stats(days: int = 30) -> dict:
    """Aggregate fill statistics per gap type for the last N days."""
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    conn = _conn()
    rows = conn.execute(
        "SELECT gap_type, COUNT(*) as total, "
        "SUM(CASE WHEN filled=1 THEN 1 ELSE 0 END) as filled_count, "
        "AVG(CASE WHEN filled=1 THEN fill_time_minutes ELSE NULL END) as avg_fill_min "
        "FROM gap_scanner WHERE date >= ? GROUP BY gap_type",
        (cutoff,)
    ).fetchall()
    conn.close()
    return {
        row["gap_type"]: {
            "total": row["total"],
            "filled": row["filled_count"],
            "fill_rate_pct": round(row["filled_count"] / row["total"] * 100, 1) if row["total"] else 0,
            "avg_fill_minutes": round(row["avg_fill_min"], 0) if row["avg_fill_min"] else None,
        }
        for row in rows
    }


# ── AI prompt injection ───────────────────────────────────────────────────────

def build_gap_prompt_section(symbol: str) -> str:
    """Return gap context for AI prompt injection.

    Injects when today's gap is > 0.5% or a significant gap from yesterday.
    """
    try:
        today = date.today().isoformat()
        conn = _conn()
        row = conn.execute(
            "SELECT * FROM gap_scanner WHERE ticker=? AND date=? LIMIT 1",
            (symbol, today)
        ).fetchone()

        if not row:
            # Check yesterday (gap may still be relevant)
            yesterday = (date.today() - timedelta(days=1)).isoformat()
            row = conn.execute(
                "SELECT * FROM gap_scanner WHERE ticker=? AND date=? "
                "AND filled=0 AND ABS(gap_pct) >= 1.5 LIMIT 1",
                (symbol, yesterday)
            ).fetchone()
        conn.close()

        if not row:
            return ""

        r = dict(row)
        abs_gap = abs(r.get("gap_pct", 0))
        if abs_gap < 0.5:
            return ""

        dir_icon = "▲" if r["gap_direction"] == "up" else "▼"
        fill_status = r.get("fill_status", "OPEN")
        gap_type = r.get("gap_type", "Common")

        lines = [f"\n=== MORNING GAP ==="]
        lines.append(
            f"  {dir_icon} {r['gap_direction'].upper()} gap: {r['gap_pct']:+.2f}% "
            f"(open ${r['open_price']:.2f} vs prev close ${r['prev_close']:.2f})"
        )
        lines.append(
            f"  Type: {gap_type} | Fill prob: {r['fill_probability']:.0f}% | "
            f"Volume: {r['volume_ratio']:.1f}x avg | Status: {fill_status}"
        )

        if gap_type == "Common":
            lines.append(
                f"  Common gaps fill ~75% of the time. "
                f"{'Fade-the-gap (short above prev close) is the typical play.' if r['gap_direction'] == 'up' else 'Buy the pullback toward prev close.'}"
            )
        elif gap_type == "Breakaway":
            lines.append(
                f"  Breakaway gap on high volume — likely a NEW TREND start. "
                f"Gap-and-go is high probability; fading is dangerous."
            )
        elif gap_type == "Runaway":
            lines.append(
                f"  Runaway gap mid-trend — continuation signal. "
                f"{'Add to longs on pullback.' if r['gap_direction'] == 'up' else 'Short continuation expected.'}"
            )
        elif gap_type == "Exhaustion":
            lines.append(
                f"  Exhaustion gap — possible REVERSAL signal. "
                f"Watch for price to stall and fade. Consider {'bearish' if r['gap_direction'] == 'up' else 'bullish'} setup."
            )

        if fill_status == "FILLED":
            lines.append(f"  ✓ Gap already filled in {r.get('fill_time_minutes', '?')} minutes")
        elif fill_status == "PARTIAL":
            lines.append(f"  ↔ Gap partially filled — watch for full fill or rejection")

        return "\n".join(lines) + "\n"

    except Exception:
        return ""
