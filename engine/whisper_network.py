"""Whisper Network — detect trending tickers via Yahoo direct HTTP."""
from __future__ import annotations
import threading
import time
from datetime import datetime
from engine.market_data import _yahoo_chart
from rich.console import Console

console = Console()

_trending_cache: list = []
_cache_lock = threading.Lock()
_CACHE_TTL = 600  # 10 minutes


def get_trending_tickers(prices: dict = None) -> list:
    """Get trending tickers by detecting big movers in watchlist + popular tickers.

    Returns list of {symbol, price, change_pct, reason, detected_at}.

    HM-RUN-SCAN-WATCHDOG Loop 5D: when `prices` (run_scan's already-fetched bulk price
    dict) is supplied, derive movers directly from it — no network calls. The legacy
    path below looped `_yahoo_chart` over the full ~3,000-symbol active universe
    serially (the §C ctx:catalyst:trending cold hang; same class as the Loop 1
    indicators bug). Falls back to the per-symbol path when called with no prices
    (backward-compat for other callers, e.g. check_watchlist_trending).
    """
    now = time.time()
    with _cache_lock:
        if _trending_cache and (now - _trending_cache[0].get("_ts", 0)) < _CACHE_TTL:
            return [{k: v for k, v in t.items() if k != "_ts"} for t in _trending_cache]

    # HM-RUN-SCAN-WATCHDOG Loop 5D: fast path — movers from in-hand bulk prices.
    if prices:
        try:
            trending = []
            for sym, data in prices.items():
                try:
                    change_pct = data.get("change_pct")
                    price = data.get("price")
                    if change_pct is None or not price:
                        continue
                    if abs(float(change_pct)) >= 3.0:
                        trending.append({
                            "symbol": sym,
                            "price": round(float(price), 2),
                            "change_pct": round(float(change_pct), 2),
                            "reason": "big_move",
                            "detected_at": datetime.now().isoformat(),
                        })
                except (TypeError, ValueError):
                    continue
            trending.sort(key=lambda x: abs(x.get("change_pct", 0)), reverse=True)
            trending = trending[:10]
            with _cache_lock:
                _trending_cache.clear()
                for t in trending:
                    t["_ts"] = now
                    _trending_cache.append(t)
            return trending
        except Exception as e:
            console.log(f"[red]Whisper network (prices fast-path) error: {e}")
            return []

    try:
        from engine.universe import get_active_universe
        extra_tickers = ["SOFI", "RIVN", "NIO", "COIN", "MARA", "HOOD", "SNAP", "UBER", "XYZ", "ROKU"]
        all_tickers = list(set(get_active_universe() + extra_tickers))

        # HM-RUN-SCAN-WATCHDOG Loop 5C/TIER-1: hard wall-clock budget on the legacy
        # per-symbol Yahoo loop. This no-args path (build_whisper_prompt_section /
        # check_watchlist_trending / discovery_scanner) was the DOMINANT §C
        # ctx:catalyst:trending / infer:{sym}:prompt:whisper hang — ~3,048 serial
        # _yahoo_chart calls when the 600s cache lapsed mid-scan. Normal path is the
        # prices= fast-path above (cache pre-warmed by the catalyst block each cycle);
        # this bounded fallback returns partial results instead of wedging the scan.
        _loop_deadline = time.monotonic() + 15.0
        trending = []
        for sym in all_tickers:
            if time.monotonic() > _loop_deadline:
                console.log(f"[yellow][WHISPER-BUDGET] 15s hit at {len(trending)} hits / partial scan — returning partial")
                break
            try:
                chart = _yahoo_chart(sym, interval="1m", range_="1d")
                if not chart:
                    continue
                meta = chart.get("meta", {})
                price = meta.get("regularMarketPrice")
                prev_close = meta.get("chartPreviousClose") or meta.get("previousClose")
                if not price or not prev_close or prev_close <= 0:
                    continue

                change_pct = ((price / prev_close) - 1) * 100
                # Flag as "trending" if move > 3%
                if abs(change_pct) >= 3.0:
                    trending.append({
                        "symbol": sym,
                        "price": round(float(price), 2),
                        "change_pct": round(change_pct, 2),
                        "reason": "big_move",
                        "detected_at": datetime.now().isoformat(),
                    })
            except Exception:
                continue

        # Sort by absolute change
        trending.sort(key=lambda x: abs(x.get("change_pct", 0)), reverse=True)
        trending = trending[:10]  # Top 10

        with _cache_lock:
            _trending_cache.clear()
            for t in trending:
                t["_ts"] = now
                _trending_cache.append(t)

        return trending

    except Exception as e:
        console.log(f"[red]Whisper network error: {e}")
        return []


def check_watchlist_trending() -> list:
    """Check if any watchlist stocks are trending. Returns matching tickers."""
    from engine.universe import get_active_universe
    trending = get_trending_tickers()
    watchlist_set = set(get_active_universe())
    return [t for t in trending if t["symbol"] in watchlist_set]


def build_whisper_prompt_section(symbol: str) -> str:
    """Build text block for AI prompt injection if the symbol is trending."""
    trending = get_trending_tickers()
    match = next((t for t in trending if t["symbol"] == symbol), None)
    if not match:
        return ""

    return (
        f"\n=== SOCIAL BUZZ: {symbol} IS TRENDING ===\n"
        f"Detected as trending: {match['reason']} ({match['change_pct']:+.2f}% move)\n"
        f"Social/market attention elevated — factor momentum and crowd behavior into your analysis.\n"
    )


def run_whisper_check():
    """Periodic check — alert on trending watchlist stocks."""
    from engine.risk_manager import RiskManager
    if not RiskManager.is_market_hours():
        return

    trending_watchlist = check_watchlist_trending()
    if trending_watchlist:
        symbols = [t["symbol"] for t in trending_watchlist]
        console.log(f"[bold cyan]WHISPER: Trending watchlist stocks: {', '.join(symbols)}")

        try:
            from engine.telegram_alerts import send_alert
            lines = ["👁 <b>WHISPER NETWORK</b>\nTrending watchlist stocks:\n"]
            for t in trending_watchlist:
                lines.append(f"  • <b>{t['symbol']}</b>: {t['change_pct']:+.2f}% ({t['reason']})")
            send_alert("\n".join(lines))
        except Exception as e:
            console.log(f"[red]Whisper alert error: {e}")
