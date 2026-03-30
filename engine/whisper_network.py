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


def get_trending_tickers() -> list:
    """Get trending tickers by detecting big movers in watchlist + popular tickers.

    Returns list of {symbol, price, change_pct, reason, detected_at}.
    """
    now = time.time()
    with _cache_lock:
        if _trending_cache and (now - _trending_cache[0].get("_ts", 0)) < _CACHE_TTL:
            return [{k: v for k, v in t.items() if k != "_ts"} for t in _trending_cache]

    try:
        from config import WATCH_STOCKS
        extra_tickers = ["SOFI", "RIVN", "NIO", "COIN", "MARA", "HOOD", "SNAP", "UBER", "XYZ", "ROKU"]
        all_tickers = list(set(WATCH_STOCKS + extra_tickers))

        trending = []
        for sym in all_tickers:
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
    from config import WATCH_STOCKS
    trending = get_trending_tickers()
    watchlist_set = set(WATCH_STOCKS)
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
