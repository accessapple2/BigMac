"""Supply/Demand Imbalance Zone Detector — Fair Value Gap (FVG) analysis.

For each watchlist stock, scans daily (3-month) and hourly (5-day) candles
to identify price imbalances where buy/sell orders were left unfilled.

Algorithm — three consecutive candles [A, B, C]:
  Demand zone (bullish FVG): A.high < C.low and B is bullish
    → Gap between A's high and C's low — unfilled buy orders
    → Zone: [A.high, C.low]
  Supply zone (bearish FVG): A.low > C.high and B is bearish
    → Gap between C's high and A's low — unfilled sell orders
    → Zone: [C.high, A.low]

Zones marked "tested" when price re-enters them.
Untested zones = highest-probability trade setups.

Dashboard icons (colorblind-safe):
  ◆ Demand zone (bullish)
  ◇ Supply zone (bearish)
  → approaching flag appended when within 1%
"""
from __future__ import annotations
import sqlite3
import threading
import time
from datetime import datetime, date
from rich.console import Console

console = Console()
DB = "data/trader.db"

# Cache: symbol → {zones: list, ts: float}
_cache: dict[str, dict] = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 3600  # 1 hour — zones are stable intraday


# ── Database ──────────────────────────────────────────────────────────────────

def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def ensure_imbalance_table():
    conn = _conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS imbalance_zones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            zone_type TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            price_high REAL NOT NULL,
            price_low REAL NOT NULL,
            created_date TEXT NOT NULL,
            impulse_date TEXT,
            zone_strength REAL DEFAULT 0,
            tested INTEGER DEFAULT 0,
            tested_date TEXT,
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


# ── Detection logic ───────────────────────────────────────────────────────────

def _fetch_candles(symbol: str, timeframe: str) -> list[dict]:
    """Fetch OHLCV candles for the given timeframe."""
    from engine.market_data import get_intraday_candles
    if timeframe == "daily":
        candles = get_intraday_candles(symbol, interval="1d", range_="3mo")
    else:  # hourly
        candles = get_intraday_candles(symbol, interval="1h", range_="5d")
    return [c for c in candles if c.get("close") and c.get("high") and c.get("low")]


def _avg_atr(candles: list[dict]) -> float:
    """Average True Range over the candle list."""
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / len(trs) if trs else 1.0


def _find_fvg_zones(candles: list[dict], timeframe: str) -> list[dict]:
    """Scan candle list for Fair Value Gaps and return zone dicts."""
    if len(candles) < 3:
        return []

    avg_atr = _avg_atr(candles)
    zones = []

    for i in range(1, len(candles) - 1):
        a, b, c = candles[i - 1], candles[i], candles[i + 1]

        # Minimum zone size: 15% of ATR (filter noise)
        min_gap = avg_atr * 0.15

        # --- Demand zone (bullish FVG) ---
        gap_lo, gap_hi = a["high"], c["low"]
        if gap_hi > gap_lo and (gap_hi - gap_lo) >= min_gap:
            bull_body = b["close"] - b["open"]
            if bull_body > 0:  # bullish impulse candle
                strength = round(min(10.0, (bull_body / avg_atr) * 5), 1)
                zones.append({
                    "zone_type": "demand",
                    "timeframe": timeframe,
                    "price_low": round(gap_lo, 4),
                    "price_high": round(gap_hi, 4),
                    "created_date": a["time"][:10],
                    "impulse_date": b["time"][:10],
                    "zone_strength": strength,
                    "icon": "◆",
                })

        # --- Supply zone (bearish FVG) ---
        gap_lo2, gap_hi2 = c["high"], a["low"]
        if gap_hi2 > gap_lo2 and (gap_hi2 - gap_lo2) >= min_gap:
            bear_body = b["open"] - b["close"]
            if bear_body > 0:  # bearish impulse candle
                strength = round(min(10.0, (bear_body / avg_atr) * 5), 1)
                zones.append({
                    "zone_type": "supply",
                    "timeframe": timeframe,
                    "price_low": round(gap_lo2, 4),
                    "price_high": round(gap_hi2, 4),
                    "created_date": a["time"][:10],
                    "impulse_date": b["time"][:10],
                    "zone_strength": strength,
                    "icon": "◇",
                })

    return zones


def _save_zone(ticker: str, zone: dict) -> bool:
    """Insert zone if not already present. Returns True if new."""
    conn = _conn()
    try:
        # Deduplicate by ticker + zone_type + timeframe + created_date
        existing = conn.execute(
            "SELECT id, price_high, price_low FROM imbalance_zones "
            "WHERE ticker=? AND zone_type=? AND timeframe=? AND created_date=?",
            (ticker, zone["zone_type"], zone["timeframe"], zone["created_date"])
        ).fetchone()
        if existing:
            # Update strength if our data is richer
            return False
        conn.execute(
            "INSERT INTO imbalance_zones "
            "(ticker, zone_type, timeframe, price_high, price_low, "
            "created_date, impulse_date, zone_strength) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (
                ticker, zone["zone_type"], zone["timeframe"],
                zone["price_high"], zone["price_low"],
                zone["created_date"], zone["impulse_date"],
                zone["zone_strength"],
            )
        )
        conn.commit()
        return True
    except Exception as e:
        console.log(f"[red]Imbalance save error ({ticker}): {e}")
        return False
    finally:
        conn.close()


def _update_tested_zones(ticker: str, current_price: float, current_low: float, current_high: float):
    """Mark untested zones as tested when price re-enters them."""
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT id, zone_type, price_high, price_low "
            "FROM imbalance_zones WHERE ticker=? AND tested=0",
            (ticker,)
        ).fetchall()
        today = date.today().isoformat()
        for row in rows:
            touched = False
            if row["zone_type"] == "demand":
                # Price came down into the demand zone
                touched = current_low <= row["price_high"]
            else:  # supply
                # Price went up into the supply zone
                touched = current_high >= row["price_low"]
            if touched:
                conn.execute(
                    "UPDATE imbalance_zones SET tested=1, tested_date=? WHERE id=?",
                    (today, row["id"])
                )
        conn.commit()
    except Exception as e:
        console.log(f"[red]Tested update error ({ticker}): {e}")
    finally:
        conn.close()


# ── Public scan interface ─────────────────────────────────────────────────────

def scan_imbalances(symbol: str) -> list[dict]:
    """Scan one symbol across daily+hourly timeframes. Returns all zones found."""
    with _cache_lock:
        cached = _cache.get(symbol)
        if cached and (time.time() - cached["ts"]) < _CACHE_TTL:
            return cached["zones"]

    all_zones = []
    for tf in ("daily", "hourly"):
        try:
            candles = _fetch_candles(symbol, tf)
            zones = _find_fvg_zones(candles, tf)
            new_count = 0
            for zone in zones:
                if _save_zone(symbol, zone):
                    new_count += 1
                    icon = zone["icon"]
                    console.log(
                        f"[dim]{icon} {symbol} {zone['zone_type'].upper()} ({tf}): "
                        f"${zone['price_low']:.2f}–${zone['price_high']:.2f} "
                        f"strength={zone['zone_strength']}"
                    )
            if new_count:
                console.log(f"[cyan]Imbalance: {symbol} {tf} → {new_count} new zone(s)")
            all_zones.extend(zones)
        except Exception as e:
            console.log(f"[yellow]Imbalance scan error {symbol}/{tf}: {e}")

    with _cache_lock:
        _cache[symbol] = {"zones": all_zones, "ts": time.time()}

    return all_zones


def scan_all_imbalances(symbols: list[str] | None = None) -> dict[str, list[dict]]:
    """Scan all watchlist symbols. Returns {symbol: [zones]}."""
    if symbols is None:
        from config import WATCH_STOCKS
        symbols = WATCH_STOCKS

    from concurrent.futures import ThreadPoolExecutor, as_completed
    results: dict[str, list[dict]] = {}

    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {ex.submit(scan_imbalances, sym): sym for sym in symbols}
        for fut in as_completed(futs, timeout=180):
            sym = futs[fut]
            try:
                results[sym] = fut.result()
            except Exception as e:
                console.log(f"[red]Imbalance error {sym}: {e}")

    return results


def update_tested_for_symbol(symbol: str, price: float | None = None):
    """Check and mark tested zones for a symbol using current price data."""
    try:
        if price is None:
            from engine.market_data import get_stock_price
            data = get_stock_price(symbol)
            price = data.get("price", 0)
            low = data.get("low", price)
            high = data.get("high", price)
        else:
            low = high = price
        if price:
            _update_tested_zones(symbol, price, low, high)
    except Exception as e:
        console.log(f"[yellow]Tested update error {symbol}: {e}")


# ── DB queries ────────────────────────────────────────────────────────────────

def get_untested_zones(ticker: str | None = None, limit: int = 100) -> list[dict]:
    """Return untested imbalance zones, optionally filtered by ticker."""
    conn = _conn()
    if ticker:
        rows = conn.execute(
            "SELECT * FROM imbalance_zones WHERE ticker=? AND tested=0 "
            "ORDER BY zone_strength DESC, detected_at DESC LIMIT ?",
            (ticker, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM imbalance_zones WHERE tested=0 "
            "ORDER BY zone_strength DESC, detected_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_zones(ticker: str | None = None, limit: int = 200) -> list[dict]:
    """Return all zones (tested + untested) for the dashboard."""
    conn = _conn()
    if ticker:
        rows = conn.execute(
            "SELECT * FROM imbalance_zones WHERE ticker=? "
            "ORDER BY tested ASC, zone_strength DESC, detected_at DESC LIMIT ?",
            (ticker, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM imbalance_zones "
            "ORDER BY tested ASC, zone_strength DESC, detected_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_approaching_zones(symbol: str, current_price: float, threshold_pct: float = 1.0) -> list[dict]:
    """Return untested zones within threshold_pct of current price."""
    zones = get_untested_zones(symbol)
    approaching = []
    for z in zones:
        if z["zone_type"] == "demand":
            # Approaching from above: price is within threshold% above zone top
            dist_pct = (current_price - z["price_high"]) / current_price * 100
            if 0 <= dist_pct <= threshold_pct:
                z["distance_pct"] = round(dist_pct, 2)
                z["approach"] = "above"
                approaching.append(z)
        else:  # supply
            # Approaching from below: price is within threshold% below zone bottom
            dist_pct = (z["price_low"] - current_price) / current_price * 100
            if 0 <= dist_pct <= threshold_pct:
                z["distance_pct"] = round(dist_pct, 2)
                z["approach"] = "below"
                approaching.append(z)
    return approaching


# ── AI prompt injection ───────────────────────────────────────────────────────

def build_imbalance_prompt_section(symbol: str, current_price: float | None = None) -> str:
    """Return imbalance zone context for AI prompt injection.

    Includes:
    - Zones price is currently approaching (within 1%)
    - Nearest untested zones above and below
    """
    try:
        if current_price is None:
            from engine.market_data import get_stock_price
            data = get_stock_price(symbol)
            current_price = data.get("price")
        if not current_price:
            return ""

        # Update tested zones with current price
        try:
            _update_tested_zones(symbol, current_price, current_price * 0.995, current_price * 1.005)
        except Exception:
            pass

        untested = get_untested_zones(symbol, limit=20)
        if not untested:
            return ""

        # Approaching zones (within 1%)
        approaching = [z for z in untested if _zone_distance_pct(z, current_price) <= 1.0]

        # Nearest demand zone below price
        demands_below = sorted(
            [z for z in untested if z["zone_type"] == "demand" and z["price_high"] < current_price],
            key=lambda z: current_price - z["price_high"]
        )
        # Nearest supply zone above price
        supplies_above = sorted(
            [z for z in untested if z["zone_type"] == "supply" and z["price_low"] > current_price],
            key=lambda z: z["price_low"] - current_price
        )

        # Only inject if there are zones within 5% OR approaching zones exist
        nearest_demand_pct = _zone_distance_pct(demands_below[0], current_price) if demands_below else 999
        nearest_supply_pct = _zone_distance_pct(supplies_above[0], current_price) if supplies_above else 999
        if nearest_demand_pct > 5.0 and nearest_supply_pct > 5.0 and not approaching:
            return ""

        lines = [f"\n=== SUPPLY/DEMAND IMBALANCE ZONES ==="]

        if approaching:
            for z in approaching[:2]:
                icon = "◆" if z["zone_type"] == "demand" else "◇"
                dist = _zone_distance_pct(z, current_price)
                lines.append(
                    f"  ⚠ APPROACHING {z['zone_type'].upper()} ZONE [{icon}] — {dist:.2f}% away!\n"
                    f"    Zone: ${z['price_low']:.2f}–${z['price_high']:.2f} "
                    f"({z['timeframe']}, strength {z['zone_strength']}/10)\n"
                    f"    This is a high-probability reversal/reaction level. "
                    f"{'Watch for bounce or breakdown.' if z['zone_type']=='demand' else 'Watch for rejection or breakout.'}"
                )

        if demands_below and nearest_demand_pct <= 5.0:
            z = demands_below[0]
            lines.append(
                f"  ◆ Nearest demand zone: ${z['price_low']:.2f}–${z['price_high']:.2f} "
                f"({nearest_demand_pct:.2f}% below, {z['timeframe']}, strength {z['zone_strength']}/10)"
            )

        if supplies_above and nearest_supply_pct <= 5.0:
            z = supplies_above[0]
            lines.append(
                f"  ◇ Nearest supply zone: ${z['price_low']:.2f}–${z['price_high']:.2f} "
                f"({nearest_supply_pct:.2f}% above, {z['timeframe']}, strength {z['zone_strength']}/10)"
            )

        if len(lines) == 1:
            return ""

        return "\n".join(lines) + "\n"

    except Exception:
        return ""


def _zone_distance_pct(zone: dict, price: float) -> float:
    """Distance from price to nearest edge of zone, in percent."""
    if zone["zone_type"] == "demand":
        return abs(price - zone["price_high"]) / price * 100
    else:
        return abs(zone["price_low"] - price) / price * 100
