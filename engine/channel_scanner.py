"""Channel Bar -- pre-built scan templates like Trade Ideas channels."""
from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console

import config

console = Console()


def _get_stock_data(symbol: str) -> dict | None:
    """Get comprehensive stock data for scanning."""
    from engine.market_data import get_stock_price, _yahoo_chart

    price_data = get_stock_price(symbol)
    if "error" in price_data:
        return None

    # Get daily chart for technicals
    chart = _yahoo_chart(symbol, interval="1d", range_="3mo")
    if not chart:
        return {
            **price_data,
            "rsi": None,
            "high_52w": None,
            "low_52w": None,
            "avg_volume": None,
            "rel_volume": None,
        }

    indicators = chart.get("indicators", {})
    quotes = indicators.get("quote", [{}])[0]
    closes = [c for c in (quotes.get("close") or []) if c is not None]
    volumes = [v for v in (quotes.get("volume") or []) if v is not None]
    highs = [h for h in (quotes.get("high") or []) if h is not None]
    lows = [low for low in (quotes.get("low") or []) if low is not None]

    # RSI(14)
    rsi = None
    if len(closes) >= 15:
        gains, losses = [], []
        for i in range(1, len(closes)):
            d = closes[i] - closes[i - 1]
            gains.append(d if d > 0 else 0)
            losses.append(abs(d) if d < 0 else 0)
        if len(gains) >= 14:
            avg_gain = sum(gains[-14:]) / 14
            avg_loss = sum(losses[-14:]) / 14
            if avg_loss > 0:
                rs = avg_gain / avg_loss
                rsi = round(100 - (100 / (1 + rs)), 1)

    # 52-week high/low (use 3mo data as approximation)
    high_52w = max(highs) if highs else None
    low_52w = min(lows) if lows else None

    # Relative volume
    avg_volume = sum(volumes[:-1]) / len(volumes[:-1]) if len(volumes) > 1 else None
    today_vol = price_data.get("volume", 0)
    rel_volume = round(today_vol / avg_volume, 1) if avg_volume and avg_volume > 0 else None

    return {
        **price_data,
        "rsi": rsi,
        "high_52w": high_52w,
        "low_52w": low_52w,
        "avg_volume": avg_volume,
        "rel_volume": rel_volume,
        "closes": closes[-5:],  # last 5 closes for trend
    }


def _scan_all() -> list:
    """Fetch data for all watchlist stocks in parallel."""
    results = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_get_stock_data, sym): sym for sym in config.WATCH_STOCKS}
        for f in as_completed(futs):
            try:
                data = f.result()
                if data:
                    results.append(data)
            except Exception:
                pass
    return results


def scan_gap_and_go() -> list:
    """Stocks gapping >3% on high volume."""
    data = _scan_all()
    return sorted(
        [
            s
            for s in data
            if abs(s.get("change_pct", 0)) > 3 and (s.get("rel_volume") or 0) > 1.5
        ],
        key=lambda x: abs(x["change_pct"]),
        reverse=True,
    )


def scan_momentum_breakout() -> list:
    """New highs on 2x+ relative volume."""
    data = _scan_all()
    results = []
    for s in data:
        if (
            s.get("high_52w")
            and s["price"] >= s["high_52w"] * 0.98
            and (s.get("rel_volume") or 0) >= 2.0
        ):
            results.append(s)
    return sorted(results, key=lambda x: x.get("rel_volume", 0), reverse=True)


def scan_reversal_bounce() -> list:
    """RSI <30 bouncing off support."""
    data = _scan_all()
    results = []
    for s in data:
        if s.get("rsi") and s["rsi"] < 30 and s.get("change_pct", 0) > 0:
            results.append(s)
    return sorted(results, key=lambda x: x.get("rsi", 100))


def scan_short_squeeze() -> list:
    """Short float >15%, price rising. Uses cached fundamentals if available."""
    from pathlib import Path
    import json
    import time

    data = _scan_all()
    fund_file = Path("data/stock_fundamentals.json")
    fundamentals = {}
    if fund_file.exists():
        try:
            raw = json.loads(fund_file.read_text())
            if (
                isinstance(raw, dict)
                and "data" in raw
                and time.time() - raw.get("timestamp", 0) < 7200
            ):
                for entry in raw["data"]:
                    fundamentals[entry.get("symbol", "")] = entry
        except Exception:
            pass

    results = []
    for s in data:
        short_pct = fundamentals.get(s["symbol"], {}).get("short_pct_float")
        if short_pct and short_pct > 15 and s.get("change_pct", 0) > 0:
            s["short_float"] = short_pct
            results.append(s)
    return sorted(results, key=lambda x: x.get("short_float", 0), reverse=True)


def scan_earnings_runner() -> list:
    """Stocks with earnings within 3 days and price rising."""
    try:
        from engine.finnhub_data import get_earnings_calendar
        from datetime import datetime, timedelta

        earnings = get_earnings_calendar(
            from_date=datetime.now().strftime("%Y-%m-%d"),
            to_date=(datetime.now() + timedelta(days=3)).strftime("%Y-%m-%d"),
        )
        earning_syms = {e["symbol"] for e in earnings}
    except Exception:
        earning_syms = set()

    if not earning_syms:
        return []

    data = _scan_all()
    return [
        s
        for s in data
        if s["symbol"] in earning_syms and s.get("change_pct", 0) > 0
    ]


def scan_volatility_breakout() -> list:
    """Opening Range breakouts confirmed by ATR + volume."""
    try:
        from engine.volatility_breakout import scan_all_breakouts
        return scan_all_breakouts()
    except Exception:
        return []


def scan_discovery() -> list:
    """New opportunities outside the watchlist."""
    try:
        from engine.discovery_scanner import get_cached_discoveries
        return get_cached_discoveries()
    except Exception:
        return []


def scan_channel(channel: str) -> list:
    """Run a named channel scan."""
    channels = {
        "gap-and-go": scan_gap_and_go,
        "momentum-breakout": scan_momentum_breakout,
        "reversal-bounce": scan_reversal_bounce,
        "short-squeeze": scan_short_squeeze,
        "earnings-runner": scan_earnings_runner,
        "volatility-breakout": scan_volatility_breakout,
        "discovery": scan_discovery,
    }
    fn = channels.get(channel)
    if not fn:
        return []
    return fn()


def get_all_channels() -> dict:
    """Run all channels and return results."""
    return {
        "gap-and-go": scan_gap_and_go(),
        "momentum-breakout": scan_momentum_breakout(),
        "reversal-bounce": scan_reversal_bounce(),
        "short-squeeze": scan_short_squeeze(),
        "earnings-runner": scan_earnings_runner(),
        "volatility-breakout": scan_volatility_breakout(),
        "discovery": scan_discovery(),
    }
