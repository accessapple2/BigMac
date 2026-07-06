"""Volume Scanner — Starfleet-Grade full market volume explosion detector.

Scans the FULL market (10,000 stocks) every 15 minutes during market hours
using Alpaca snapshots. No yfinance. No Yahoo rate limits.

Architecture: FUNNEL
  10,000 stocks → Alpaca snapshots (~10 API calls) → volume filter → 50-150 hot stocks
  → relative volume calc → save to volume_alerts → post to War Room → feed to Chekov

Functions:
    scan_full_market()    — Every 15 min during market hours: full universe sweep
    red_alert_check()     — Every 2 min during market hours: re-scan today's hot stocks
    get_todays_volume_alerts(limit)  — Returns today's alerts for strategy integration
    build_volume_radar_prompt_section()  — Prompt block for AI models
"""
from __future__ import annotations

import logging
import os
import sqlite3
from engine.db_conn import get_conn
import contextlib
import time
from datetime import datetime, date
from typing import Optional
from zoneinfo import ZoneInfo
from engine.market_calendar import utc_now_str  # HM-TZ Stage 3: canonical space-UTC writes

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TRADER_DB = "data/trader.db"
ALPACA_DATA_BASE = "https://data.alpaca.markets"
SNAPSHOT_BATCH = 1000   # Alpaca limit per snapshot call
REQ_DELAY = 0.15        # seconds between snapshot batches

# Volume filter thresholds
MIN_PRICE = 0.50
MIN_DOLLAR_VOLUME = 500_000
REL_VOL_TRIGGER = 10.0    # 10x normal volume
GAP_TRIGGER = 5.0         # 5% gap up or down
RED_ALERT_THRESHOLD = 50.0
CRITICAL_THRESHOLD = 100.0

# ---------------------------------------------------------------------------
# Regular trading hours (RTH), in market-local time (US/Eastern).
# Used for the time-of-day relative-volume correction (see _session_fraction_elapsed).
# ---------------------------------------------------------------------------
_MARKET_TZ = ZoneInfo("America/New_York")
RTH_OPEN_MINUTE = 9 * 60 + 30     # 09:30 ET
RTH_CLOSE_MINUTE = 16 * 60        # 16:00 ET
RTH_TOTAL_MINUTES = RTH_CLOSE_MINUTE - RTH_OPEN_MINUTE   # 390
# Floor on the elapsed-fraction so the first minutes of the session don't divide
# partial volume by a near-zero fraction (which would amplify a single block trade
# into a fake spike). Treat anything before 09:45 ET as 15 minutes elapsed.
MIN_SESSION_FRACTION = 15.0 / RTH_TOTAL_MINUTES

# War Room player ID for Chekov's volume posts
CHEKOV_PLAYER_ID = "navigator"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [volume_scanner] %(levelname)s: %(message)s")
logger = logging.getLogger("volume_scanner")

# ---------------------------------------------------------------------------
# SSE broadcast hook
# app.py registers broadcast_scanner_alert() here at startup.
# No circular import — volume_scanner never imports from app.py.
# ---------------------------------------------------------------------------
_scan_callbacks: list = []


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _conn() -> sqlite3.Connection:
    c = get_conn(TRADER_DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def _init_tables():
    with contextlib.closing(_conn()) as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS volume_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                price REAL,
                relative_volume REAL,
                gap_pct REAL,
                dollar_volume REAL,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_volume_alerts_date ON volume_alerts(detected_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_volume_alerts_symbol ON volume_alerts(symbol, detected_at)")
        c.commit()


# ---------------------------------------------------------------------------
# Alpaca auth headers
# ---------------------------------------------------------------------------

def _alpaca_headers() -> dict:
    from dotenv import load_dotenv
    load_dotenv()
    key = os.getenv("APCA_API_KEY_ID", "")
    secret = os.getenv("APCA_API_SECRET_KEY", "")
    if not key or not secret:
        raise RuntimeError("APCA_API_KEY_ID / APCA_API_SECRET_KEY not set in .env")
    return {
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": secret,
        "Accept": "application/json",
    }


# ---------------------------------------------------------------------------
# Snapshot fetcher
# ---------------------------------------------------------------------------

def _fetch_snapshots(symbols: list[str], headers: dict) -> dict[str, dict]:
    """Fetch Alpaca snapshots for a batch of symbols.

    Returns {symbol: snapshot_dict} for all symbols that returned data.
    """
    symbols_csv = ",".join(symbols)
    try:
        resp = requests.get(
            f"{ALPACA_DATA_BASE}/v2/stocks/snapshots",
            headers=headers,
            params={"symbols": symbols_csv, "feed": "iex"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"Snapshot fetch failed ({len(symbols)} symbols): {e}")
        return {}


# ---------------------------------------------------------------------------
# Parse snapshot into enriched stock data
# ---------------------------------------------------------------------------

def _session_fraction_elapsed(now: Optional[datetime] = None) -> float:
    """Fraction of the RTH session (09:30–16:00 ET) elapsed, in (0, 1].

    Today's Alpaca daily-bar volume is a PARTIAL cumulative figure that grows
    through the session, whereas the 20d baseline is a COMPLETE-day average.
    Comparing them directly deflates relative volume mid-session. Scaling the
    baseline by this fraction makes the comparison apples-to-apples at any time
    of day (a stock running at its normal pace lands at ~1.0x all session long;
    a true 10x spike clears the trigger immediately instead of only after close).

    Clamped to MIN_SESSION_FRACTION early in the session (avoids amplifying
    opening-print noise / div-by-zero) and to 1.0 outside RTH (a completed bar
    compares against the full-day average; pre-open partials under-fire, which is
    the safe direction).
    """
    now = now or datetime.now(_MARKET_TZ)
    if now.tzinfo is None:
        now = now.replace(tzinfo=_MARKET_TZ)
    else:
        now = now.astimezone(_MARKET_TZ)

    minutes = now.hour * 60 + now.minute + now.second / 60.0
    elapsed = minutes - RTH_OPEN_MINUTE
    if elapsed <= 0:
        # Pre-open: avoid dividing premarket volume by ~0; compare to full day (under-fires).
        return 1.0
    frac = elapsed / RTH_TOTAL_MINUTES
    if frac >= 1.0:
        return 1.0
    return max(frac, MIN_SESSION_FRACTION)


def _parse_snapshot(
    sym: str,
    snap: dict,
    baselines: dict[str, float],
    session_frac: Optional[float] = None,
) -> Optional[dict]:
    """Extract key metrics from an Alpaca snapshot dict.

    Returns None if data is missing or price is below minimum.
    """
    try:
        latest_trade = snap.get("latestTrade") or snap.get("latest_trade") or {}
        daily_bar = snap.get("dailyBar") or snap.get("daily_bar") or {}
        prev_bar = snap.get("prevDailyBar") or snap.get("prev_daily_bar") or {}

        price = latest_trade.get("p") or daily_bar.get("c")
        if not price or price < MIN_PRICE:
            return None

        current_vol = daily_bar.get("v", 0)
        open_price = daily_bar.get("o", price)
        prev_close = prev_bar.get("c", open_price)

        if not current_vol or current_vol == 0:
            return None

        dollar_volume = price * current_vol
        gap_pct = ((open_price - prev_close) / prev_close * 100) if prev_close else 0.0

        # Relative volume — time-of-day corrected.
        # current_vol is today's PARTIAL cumulative volume; avg_vol is a 20d
        # COMPLETE-day average. Scale the baseline by the fraction of the RTH
        # session elapsed so the ratio isn't deflated mid-session (the old
        # current_vol/avg_vol only cleared 10x once the bar completed → real
        # spikes landed post-close). See _session_fraction_elapsed.
        if session_frac is None:
            session_frac = _session_fraction_elapsed()
        avg_vol = baselines.get(sym, 0)
        expected_vol = avg_vol * session_frac
        rel_vol = (current_vol / expected_vol) if expected_vol and expected_vol > 0 else 0.0

        return {
            "symbol": sym,
            "price": round(price, 4),
            "current_volume": current_vol,
            "avg_volume_20d": avg_vol,
            "relative_volume": round(rel_vol, 2),
            "gap_pct": round(gap_pct, 2),
            "dollar_volume": round(dollar_volume, 0),
        }
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Filter logic
# ---------------------------------------------------------------------------

def _passes_filter(stock: dict) -> Optional[str]:
    """Return alert_type if stock passes any filter, else None."""
    price = stock.get("price", 0)
    rel_vol = stock.get("relative_volume", 0)
    gap_pct = stock.get("gap_pct", 0)
    dollar_volume = stock.get("dollar_volume", 0)

    if price < MIN_PRICE:
        return None

    if rel_vol >= REL_VOL_TRIGGER and dollar_volume >= MIN_DOLLAR_VOLUME:
        return "volume_explosion"
    if gap_pct >= GAP_TRIGGER:
        return "gap_up"
    if gap_pct <= -GAP_TRIGGER:
        return "gap_down"
    return None


# ---------------------------------------------------------------------------
# War Room poster
# ---------------------------------------------------------------------------

def _post_to_war_room(symbol: str, message: str):
    """Post a volume alert to the War Room as Chekov."""
    try:
        from engine.war_room import save_hot_take
        save_hot_take(CHEKOV_PLAYER_ID, symbol, message)
    except Exception as e:
        logger.warning(f"War Room post failed: {e}")


# ---------------------------------------------------------------------------
# Core scan functions
# ---------------------------------------------------------------------------

def scan_full_market() -> list[dict]:
    """Scan the full market for volume explosions.

    Runs every 15 min during market hours (6:30 AM - 1:00 PM MST).
    Returns list of hot stocks that passed the volume/gap filter.
    """
    _init_tables()
    headers = _alpaca_headers()

    # Get universe
    from engine.full_universe import get_universe
    universe = get_universe()
    if not universe:
        logger.warning("Universe empty — skipping scan")
        return []

    logger.info(f"Volume Radar: scanning {len(universe)} stocks...")
    scan_start = time.monotonic()

    # Load baselines
    from engine.volume_baselines import get_baselines
    baselines = get_baselines(universe)
    logger.info(f"Baselines loaded: {len(baselines)} stocks have 20d avg volume")

    # Fetch snapshots in batches of 1000
    all_snapshots: dict[str, dict] = {}
    for i in range(0, len(universe), SNAPSHOT_BATCH):
        batch = universe[i : i + SNAPSHOT_BATCH]
        snaps = _fetch_snapshots(batch, headers)
        all_snapshots.update(snaps)
        if i > 0:
            time.sleep(REQ_DELAY)

    logger.info(f"Snapshots received: {len(all_snapshots)} symbols")

    # Parse and filter
    hot_stocks: list[dict] = []
    now = utc_now_str()   # HM-TZ Stage 3: detected_at canonical space-UTC
    session_frac = _session_fraction_elapsed()   # computed once per scan, applied to every symbol

    with contextlib.closing(_conn()) as c:
        for sym, snap in all_snapshots.items():
            stock = _parse_snapshot(sym, snap, baselines, session_frac)
            if not stock:
                continue
            alert_type = _passes_filter(stock)
            if not alert_type:
                continue

            stock["alert_type"] = alert_type
            stock["detected_at"] = now
            hot_stocks.append(stock)

            # Save to DB
            c.execute(
                """
                INSERT INTO volume_alerts
                    (symbol, alert_type, price, relative_volume, gap_pct, dollar_volume, detected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    stock["symbol"], alert_type,
                    stock["price"], stock["relative_volume"],
                    stock["gap_pct"], stock["dollar_volume"],
                    now,
                ),
            )
        c.commit()

        # Fire SSE broadcast callbacks (registered by app.py at startup)
        for _cb in _scan_callbacks:
            try:
                for _s in hot_stocks:
                    _cb({
                        "symbol":           _s["symbol"],
                        "alert_type":       _s["alert_type"],
                        "price":            _s.get("price"),
                        "relative_volume":  _s.get("relative_volume"),
                        "gap_pct":          _s.get("gap_pct"),
                        "dollar_volume":    _s.get("dollar_volume"),
                        "detected_at":      _s.get("detected_at", now),
                    })
            except Exception as _e:
                logger.warning(f"[SSE] broadcast callback error: {_e}")

    elapsed = time.monotonic() - scan_start
    hot_stocks.sort(key=lambda x: x.get("relative_volume", 0), reverse=True)

    count = len(hot_stocks)
    top5 = ", ".join(
        f"{s['symbol']}({s['relative_volume']:.0f}x)"
        for s in hot_stocks[:5]
    )
    msg = (
        f"🧭 VOLUME RADAR: {count} stocks detected with unusual activity. "
        f"Top 5: {top5}. Scan time: {elapsed:.1f}s."
    )
    logger.info(msg)

    if count > 0:
        _post_to_war_room("MARKET", msg)

    return hot_stocks


def red_alert_check() -> None:
    """Re-scan today's hot stocks for extreme volume spikes.

    Runs every 2 min during market hours. Only hits today's volume_alerts symbols
    (1 API call for 50-150 symbols instead of 10 calls for 10,000).
    """
    _init_tables()
    headers = _alpaca_headers()

    today = date.today().isoformat()
    try:
        with contextlib.closing(_conn()) as c:
            rows = c.execute(
                "SELECT DISTINCT symbol FROM volume_alerts WHERE date(detected_at)=?",
                (today,),
            ).fetchall()
        todays_symbols = [r["symbol"] for r in rows]
    except Exception:
        return

    if not todays_symbols:
        return

    # One snapshot call for all today's hot stocks
    snaps = _fetch_snapshots(todays_symbols, headers)
    if not snaps:
        return

    from engine.volume_baselines import get_baselines
    baselines = get_baselines(todays_symbols)

    # Check for already-flagged red alerts
    try:
        with contextlib.closing(_conn()) as c:
            existing_red = {
                r["symbol"] for r in c.execute(
                    "SELECT DISTINCT symbol FROM volume_alerts "
                    "WHERE date(detected_at)=? AND alert_type='red_alert'",
                    (today,),
                ).fetchall()
            }
    except Exception:
        existing_red = set()

    now = utc_now_str()   # HM-TZ Stage 3: detected_at canonical space-UTC
    session_frac = _session_fraction_elapsed()   # computed once per scan, applied to every symbol
    with contextlib.closing(_conn()) as c:
        for sym, snap in snaps.items():
            stock = _parse_snapshot(sym, snap, baselines, session_frac)
            if not stock:
                continue

            rel_vol = stock.get("relative_volume", 0)
            price = stock.get("price", 0)
            gap = stock.get("gap_pct", 0)
            direction = "up" if gap >= 0 else "down"
            abs_gap = abs(gap)

            if rel_vol >= CRITICAL_THRESHOLD:
                # CRITICAL alert
                msg = (
                    f"🔴🔴 CRITICAL: {sym} — {rel_vol:.0f}x volume, this is not a drill! "
                    f"Price ${price:.2f}, {direction} {abs_gap:.1f}%"
                )
                logger.warning(msg)
                _post_to_war_room(sym, msg)
                if sym not in existing_red:
                    c.execute(
                        "INSERT INTO volume_alerts "
                        "(symbol, alert_type, price, relative_volume, gap_pct, dollar_volume, detected_at) "
                        "VALUES (?,?,?,?,?,?,?)",
                        (sym, "red_alert", price, rel_vol, gap, stock["dollar_volume"], now),
                    )
                    existing_red.add(sym)

            elif rel_vol >= RED_ALERT_THRESHOLD and sym not in existing_red:
                msg = (
                    f"🔴 RED ALERT: {sym} volume explosion — {rel_vol:.0f}x normal! "
                    f"Price ${price:.2f}, {direction} {abs_gap:.1f}%. All hands to stations!"
                )
                logger.warning(msg)
                _post_to_war_room(sym, msg)
                c.execute(
                    "INSERT INTO volume_alerts "
                    "(symbol, alert_type, price, relative_volume, gap_pct, dollar_volume, detected_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (sym, "red_alert", price, rel_vol, gap, stock["dollar_volume"], now),
                )
                existing_red.add(sym)

        c.commit()


# ---------------------------------------------------------------------------
# Integration helpers
# ---------------------------------------------------------------------------

def get_todays_volume_alerts(limit: int = 200) -> list[dict]:
    """Return today's volume alerts deduplicated to ONE row per symbol (most recent).

    Adds two enrichment fields:
      hits_today       — how many times this symbol fired today
      first_seen_today — ISO timestamp of first hit today
      days_active      — how many of the last 10 calendar days it fired at all

    Used by strategies.py, /api/volume-radar, and Sniff Scan frontend.
    """
    _init_tables()
    today = date.today().isoformat()
    try:
        with contextlib.closing(_conn()) as c:
            # Step 1: one row per symbol — most recent row for price/rvol/alert_type,
            # plus hit count and first-seen for today.
            deduped = c.execute(
                """
                SELECT va.symbol, va.alert_type, va.price, va.relative_volume,
                       va.gap_pct, va.dollar_volume, va.detected_at,
                       grouped.hits_today, grouped.first_seen_today
                FROM volume_alerts va
                INNER JOIN (
                    SELECT symbol,
                           MAX(detected_at)  AS latest,
                           COUNT(*)          AS hits_today,
                           MIN(detected_at)  AS first_seen_today
                    FROM volume_alerts
                    WHERE date(detected_at, 'localtime') = date('now', 'localtime')
                    GROUP BY symbol
                ) grouped
                  ON va.symbol = grouped.symbol
                 AND va.detected_at = grouped.latest
                ORDER BY va.relative_volume DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

            symbols = [r["symbol"] for r in deduped]
            # Step 2: multi-day persistence (last 10 calendar days)
            days_active: dict[str, int] = {}
            if symbols:
                placeholders = ",".join("?" * len(symbols))
                persistence = c.execute(
                    f"""
                    SELECT symbol, COUNT(DISTINCT date(detected_at, 'localtime')) AS days_active
                    FROM volume_alerts
                    WHERE detected_at >= date('now', '-10 days')  -- HM-TZ-COMPLETION 2026-06-02: detected_at is UTC (utc_now_str); boundary must be UTC too (was 'localtime')
                      AND symbol IN ({placeholders})
                    GROUP BY symbol
                    """,
                    symbols,
                ).fetchall()
                days_active = {r["symbol"]: r["days_active"] for r in persistence}

        result = []
        for r in deduped:
            row = dict(r)
            row["days_active"] = days_active.get(r["symbol"], 1)
            result.append(row)
        # HM-MCW-PHANTOM 2026-06-15: drop blacklisted + non-tradable (delisted/suspended) names
        # so dead tickers (MCW/HOLX/…) stop appearing as "hot" on the volume radar / scan feed.
        # FAIL-OPEN: only the hand-blacklist applies if the Alpaca tradable set is unavailable.
        try:
            from config import DELISTED_BLACKLIST
            try:
                from engine.full_universe import tradable_symbols as _ts
                _tradable = _ts()
            except Exception:
                _tradable = None
            result = [row for row in result
                      if row["symbol"] not in DELISTED_BLACKLIST
                      and (_tradable is None or row["symbol"] in _tradable)]
        except Exception:
            pass
        return result
    except Exception as e:
        logger.warning(f"get_todays_volume_alerts failed: {e}")
        return []


def build_volume_radar_prompt_section() -> str:
    """Build AI model prompt block showing today's top volume exploders."""
    try:
        alerts = get_todays_volume_alerts(limit=15)
        if not alerts:
            return ""

        lines = ["=== 🔴 VOLUME RADAR — Full Market Scanner ==="]
        lines.append(f"Today's unusual volume activity ({len(alerts)} stocks flagged):")

        for a in alerts[:10]:
            sym = a["symbol"]
            rel = a.get("relative_volume", 0)
            price = a.get("price", 0)
            gap = a.get("gap_pct", 0)
            atype = a.get("alert_type", "")

            badge = "🔴🔴" if rel >= CRITICAL_THRESHOLD else "🔴" if rel >= RED_ALERT_THRESHOLD else "🟠" if rel >= 25 else "🟡"
            gap_str = f"{gap:+.1f}%" if gap else ""
            lines.append(f"  {badge} {sym}: {rel:.0f}x vol | ${price:.2f} {gap_str} | {atype}")

        return "\n".join(lines)
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "scan"
    if cmd == "red":
        red_alert_check()
    elif cmd == "alerts":
        alerts = get_todays_volume_alerts()
        for a in alerts:
            print(a)
    else:
        results = scan_full_market()
        print(f"\nHot stocks found: {len(results)}")
        for s in results[:20]:
            print(f"  {s['symbol']}: {s['relative_volume']:.1f}x | ${s['price']:.2f} | {s['gap_pct']:+.1f}% | {s['alert_type']}")
