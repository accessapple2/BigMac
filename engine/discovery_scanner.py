"""Discovery Scanner — find new opportunities beyond the watchlist.

Scans Yahoo Finance's most active stocks daily and flags tickers with:
- Short float > 15% (squeeze candidates)
- Relative volume > 2x (unusual activity)
- Gapping > 3% (momentum plays)
- Insider buying (smart money)

Discoveries are fed to AI models as prompt injections and stored for dashboard display.
"""
from __future__ import annotations
import sqlite3
import time
import threading
import json
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console

console = Console()
DB = "data/trader.db"
CACHE_FILE = Path("data/discovery_cache.json")

_cache = {"data": [], "ts": 0}
_cache_lock = threading.Lock()
_CACHE_TTL = 1800  # 30 minutes — discoveries don't change fast


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def ensure_discovery_table():
    conn = _conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS discoveries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            trigger_type TEXT NOT NULL,
            price REAL,
            change_pct REAL,
            volume REAL,
            rel_volume REAL,
            short_float REAL,
            details TEXT,
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            acted_on INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()


def get_most_active_tickers(count: int = 100) -> list:
    """Fetch the most active tickers from Yahoo Finance."""
    import requests

    tickers = []

    # Yahoo most active screener via direct HTTP
    try:
        url = "https://query2.finance.yahoo.com/v1/finance/screener/predefined/saved"
        params = {"scrIds": "most_actives", "count": count}
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, params=params, headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json()
            quotes = data.get("finance", {}).get("result", [{}])[0].get("quotes", [])
            for q in quotes:
                sym = q.get("symbol", "")
                if sym and "." not in sym and "-" not in sym and len(sym) <= 5:
                    tickers.append({
                        "symbol": sym,
                        "price": q.get("regularMarketPrice", 0),
                        "change_pct": q.get("regularMarketChangePercent", 0),
                        "volume": q.get("regularMarketVolume", 0),
                        "avg_volume": q.get("averageDailyVolume3Month", 0),
                        "market_cap": q.get("marketCap", 0),
                        "short_name": q.get("shortName", ""),
                    })
    except Exception as e:
        console.log(f"[red]Yahoo most-active fetch error: {e}")

    # Fallback: use trending tickers from whisper network
    if len(tickers) < 20:
        try:
            from engine.whisper_network import get_trending_tickers
            trending = get_trending_tickers()
            existing = {t["symbol"] for t in tickers}
            for t in trending:
                if t["symbol"] not in existing:
                    tickers.append({
                        "symbol": t["symbol"],
                        "price": t.get("price", 0),
                        "change_pct": t.get("change_pct", 0),
                        "volume": 0,
                        "avg_volume": 0,
                        "market_cap": 0,
                        "short_name": "",
                    })
        except Exception:
            pass

    # Also add a curated list of popular volatile tickers
    popular = [
        "PLUG", "POWI", "SOFI", "RIVN", "NIO", "COIN", "MSTR", "HIMS",
        "HOOD", "MARA", "RIOT", "SNAP", "UBER", "ROKU", "DKNG",
        "LCID", "IONQ", "RKLB", "AFRM", "UPST", "PATH", "CRWD",
        "NET", "SNOW", "ABNB", "SQ", "SHOP", "SE", "BABA",
        "XLE", "XOM", "CVX", "IWM", "GLD", "SLV",
        "CTMX", "BCRX", "DTI", "CING", "NBIS",
    ]
    existing = {t["symbol"] for t in tickers}
    for sym in popular:
        if sym not in existing:
            tickers.append({
                "symbol": sym, "price": 0, "change_pct": 0,
                "volume": 0, "avg_volume": 0, "market_cap": 0, "short_name": "",
            })

    return tickers[:150]


def enrich_ticker(ticker: dict) -> dict | None:
    """Fetch live data for a single ticker to check discovery criteria."""
    from engine.market_data import get_stock_price

    sym = ticker["symbol"]
    try:
        data = get_stock_price(sym)
        if "error" in data:
            return None

        price = data.get("price", 0)
        change_pct = data.get("change_pct", 0)
        volume = data.get("volume", 0)

        # Relative volume
        avg_vol = ticker.get("avg_volume", 0)
        if avg_vol <= 0:
            # Estimate from 20-day average via chart
            try:
                from engine.market_data import _yahoo_chart
                chart = _yahoo_chart(sym, interval="1d", range_="1mo")
                if chart:
                    quotes = chart.get("indicators", {}).get("quote", [{}])[0]
                    vols = [v for v in (quotes.get("volume") or []) if v and v > 0]
                    if len(vols) > 1:
                        avg_vol = sum(vols[:-1]) / len(vols[:-1])
            except Exception:
                pass

        rel_vol = round(volume / avg_vol, 1) if avg_vol > 0 else 0

        # Short float (from fundamentals cache if available)
        short_float = 0
        try:
            import yfinance as yf
            info = yf.Ticker(sym).info
            shares_short = info.get("sharesShort", 0) or 0
            shares_float = info.get("floatShares", 0) or 0
            if shares_float > 0:
                short_float = round(shares_short / shares_float * 100, 1)
        except Exception:
            pass

        return {
            "symbol": sym,
            "price": price,
            "change_pct": round(change_pct, 2),
            "volume": volume,
            "rel_volume": rel_vol,
            "short_float": short_float,
            "name": ticker.get("short_name", ""),
        }
    except Exception:
        return None


def scan_discoveries() -> list:
    """Run the discovery scanner — find opportunities outside the watchlist."""
    with _cache_lock:
        if time.time() - _cache["ts"] < _CACHE_TTL and _cache["data"]:
            return _cache["data"]

    from config import WATCH_STOCKS
    watchlist_set = set(WATCH_STOCKS)

    # Get candidate tickers
    candidates = get_most_active_tickers(100)
    # Filter out watchlist stocks (we already scan those)
    candidates = [t for t in candidates if t["symbol"] not in watchlist_set]

    # Enrich in parallel (limit to 30 to avoid rate limiting)
    enriched = []
    batch = candidates[:30]
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(enrich_ticker, t): t for t in batch}
        for f in as_completed(futures, timeout=60):
            try:
                result = f.result(timeout=10)
                if result:
                    enriched.append(result)
            except Exception:
                pass

    # Score and filter discoveries from existing sources
    discoveries = []
    seen_symbols = set()
    for stock in enriched:
        triggers = []

        if stock["short_float"] >= 15:
            triggers.append(f"short_squeeze ({stock['short_float']:.0f}% short float)")

        if stock["rel_volume"] >= 2.0:
            triggers.append(f"unusual_volume ({stock['rel_volume']:.1f}x avg)")

        if abs(stock["change_pct"]) >= 3.0:
            direction = "gapping_up" if stock["change_pct"] > 0 else "gapping_down"
            triggers.append(f"{direction} ({stock['change_pct']:+.1f}%)")

        if not triggers:
            continue

        seen_symbols.add(stock["symbol"])
        discoveries.append({
            **stock,
            "triggers": triggers,
            "trigger_type": triggers[0].split(" ")[0],
            "score": len(triggers) * 25 + abs(stock["change_pct"]) * 5 + stock["rel_volume"] * 10,
        })

    # --- Finviz Elite additional source ---
    try:
        from shared.finviz_scanner import get_finviz_discoveries
        finviz_hits = get_finviz_discoveries()
        for d in finviz_hits:
            sym = d.get("symbol", "")
            if not sym or sym in watchlist_set:
                continue
            if sym in seen_symbols:
                # Enrich existing entry with Finviz triggers instead of duplicating
                for existing in discoveries:
                    if existing["symbol"] == sym:
                        for t in d.get("triggers", []):
                            if t not in existing["triggers"]:
                                existing["triggers"].append(t)
                        existing["score"] += d.get("score", 0)
                        break
            else:
                seen_symbols.add(sym)
                discoveries.append(d)
    except Exception as e:
        console.log(f"[dim]Finviz discovery source error: {e}")

    discoveries.sort(key=lambda x: x["score"], reverse=True)
    discoveries = discoveries[:20]

    # Cache results
    with _cache_lock:
        _cache["data"] = discoveries
        _cache["ts"] = time.time()

    # Save to file for persistence
    try:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        CACHE_FILE.write_text(json.dumps({
            "discoveries": discoveries,
            "scanned_at": datetime.now().isoformat(),
            "candidates_checked": len(enriched),
        }, indent=2))
    except Exception:
        pass

    return discoveries


def record_discoveries(discoveries: list):
    """Save discoveries to database for tracking."""
    ensure_discovery_table()
    conn = _conn()
    for d in discoveries:
        try:
            conn.execute(
                "INSERT INTO discoveries (symbol, trigger_type, price, change_pct, volume, rel_volume, short_float, details) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (d["symbol"], d["trigger_type"], d["price"], d["change_pct"],
                 d["volume"], d["rel_volume"], d["short_float"],
                 ", ".join(d.get("triggers", [])))
            )
        except Exception:
            pass
    conn.commit()
    conn.close()


def build_discovery_prompt_section() -> str:
    """Build prompt section with top discoveries for AI models."""
    discoveries = get_cached_discoveries()
    if not discoveries:
        return ""

    lines = ["\n=== NEW OPPORTUNITIES (Discovery Scanner) ==="]
    for d in discoveries[:5]:
        triggers_str = ", ".join(d.get("triggers", []))
        lines.append(
            f"  NEW: {d['symbol']} — ${d['price']:.2f}, {triggers_str}"
        )
    lines.append("Consider these if they match your thesis. They are NOT on the main watchlist.")
    return "\n".join(lines) + "\n"


def get_cached_discoveries() -> list:
    """Get discoveries from cache (memory or file)."""
    with _cache_lock:
        if _cache["data"] and time.time() - _cache["ts"] < _CACHE_TTL:
            return _cache["data"]

    # Try file cache
    try:
        if CACHE_FILE.exists():
            data = json.loads(CACHE_FILE.read_text())
            discoveries = data.get("discoveries", [])
            if discoveries:
                with _cache_lock:
                    _cache["data"] = discoveries
                    _cache["ts"] = time.time()
                return discoveries
    except Exception:
        pass

    return []


def get_recent_discoveries(limit: int = 30) -> list:
    """Get recent discoveries from database."""
    ensure_discovery_table()
    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM discoveries ORDER BY detected_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def run_discovery_scan() -> list:
    """Full scan: discover, record, log."""
    discoveries = scan_discoveries()

    if discoveries:
        record_discoveries(discoveries)
        top = discoveries[:3]
        names = ", ".join(f"{d['symbol']}({d['trigger_type']})" for d in top)
        console.log(f"[bold magenta]DISCOVERY: {names} + {len(discoveries)-3} more")

    return discoveries
