"""Channel Bar -- pre-built Starfleet scan channel templates."""
from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, TimeoutError as _FutTimeoutError, as_completed
from rich.console import Console

import config
from engine.universe import get_active_universe

console = Console()


def _get_stock_data(symbol: str, bars: dict | None = None) -> dict | None:
    """Get comprehensive stock data for scanning.

    HM-SLOW-FUNDAMENTALS Phase 2 (2026-05-21): when `bars` (a pre-fetched
    `get_bulk_daily_ohlcv` dict) is provided, read `bars[symbol]` instead of
    making a per-symbol Yahoo `_yahoo_chart` call. The DataFrame columns are
    Open / High / Low / Close / Volume per the Phase 1 schema.
    """
    from engine.market_data import get_stock_price, _yahoo_chart

    price_data = get_stock_price(symbol)
    if "error" in price_data:
        return None

    closes: list = []
    volumes: list = []
    highs: list = []
    lows: list = []

    if bars is not None:
        df = bars.get(symbol)
        if df is None or df.empty:
            return {
                **price_data,
                "rsi": None,
                "high_52w": None,
                "low_52w": None,
                "avg_volume": None,
                "rel_volume": None,
            }
        closes = [float(c) for c in df["Close"].dropna().tolist()]
        volumes = [float(v) for v in df["Volume"].dropna().tolist()]
        highs = [float(h) for h in df["High"].dropna().tolist()]
        lows = [float(low) for low in df["Low"].dropna().tolist()]
    else:
        # Legacy Yahoo path — kept for callers that don't supply bars.
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


def _scan_all(bars: dict | None = None) -> list:
    """Fetch data for all watchlist stocks in parallel.

    HM-SLOW-FUNDAMENTALS Phase 2: when `bars` is provided, each per-symbol
    `_get_stock_data` call reads its OHLCV slice from `bars[symbol]` instead
    of fanning out to Yahoo.
    """
    # HM-CHANNEL-SCANNER-DEADLOCK P1-FINAL (2026-05-27): manual executor
    # lifecycle with shutdown(wait=False, cancel_futures=True). The earlier P1
    # added timeouts to as_completed + f.result but kept `with ThreadPoolExecutor`,
    # which forces shutdown(wait=True) on __exit__ — that re-blocks indefinitely
    # if any worker is still stuck inside _yahoo_lock. Manual try/finally with
    # cancel_futures=True is the only correct pattern when futures may hang
    # past the as_completed timeout.
    # _yahoo_lock serializes all 8 workers; with a 1s/req throttle + 10s HTTP
    # timeout + 3×backoff on 429, worst-case per-call wall is ~25s. timeout=30
    # on as_completed gives one batch a bounded ceiling; timeout=5 on f.result
    # guards against a future that completed-but-blocked elsewhere.
    results = []
    ex = ThreadPoolExecutor(max_workers=8)
    try:
        futs = {ex.submit(_get_stock_data, sym, bars): sym for sym in get_active_universe()}
        try:
            for f in as_completed(futs, timeout=30):
                sym = futs.get(f, "?")
                try:
                    data = f.result(timeout=5)
                    if data:
                        results.append(data)
                except _FutTimeoutError:
                    console.log(f"[yellow][CHANNEL-SCAN] {sym} result timeout — skipping")
                except Exception as _scan_e:
                    console.log(f"[yellow][CHANNEL-SCAN] {sym} error — skipping: "
                                f"{type(_scan_e).__name__}: {_scan_e!r}")
        except _FutTimeoutError:
            done = sum(1 for f in futs if f.done())
            console.log(f"[yellow][CHANNEL-SCAN] as_completed batch timeout after 30s "
                        f"({done}/{len(futs)} futures finished) — returning partial results")
    finally:
        # cancel_futures=True cancels not-yet-started futures; wait=False
        # leaves running futures to finish in background without blocking
        # the caller. Workers stuck in _yahoo_lock are abandoned — they'll
        # eventually unblock when the lock releases, but we don't wait.
        ex.shutdown(wait=False, cancel_futures=True)
    return results


def scan_gap_and_go(bars: dict | None = None) -> list:
    """Stocks gapping >3% on high volume."""
    data = _scan_all(bars=bars)
    return sorted(
        [
            s
            for s in data
            if abs(s.get("change_pct", 0)) > 3 and (s.get("rel_volume") or 0) > 1.5
        ],
        key=lambda x: abs(x["change_pct"]),
        reverse=True,
    )


def scan_momentum_breakout(bars: dict | None = None) -> list:
    """New highs on 2x+ relative volume."""
    data = _scan_all(bars=bars)
    results = []
    for s in data:
        if (
            s.get("high_52w")
            and s["price"] >= s["high_52w"] * 0.98
            and (s.get("rel_volume") or 0) >= 2.0
        ):
            results.append(s)
    return sorted(results, key=lambda x: x.get("rel_volume", 0), reverse=True)


def scan_reversal_bounce(bars: dict | None = None) -> list:
    """RSI <30 bouncing off support."""
    data = _scan_all(bars=bars)
    results = []
    for s in data:
        if s.get("rsi") and s["rsi"] < 30 and s.get("change_pct", 0) > 0:
            results.append(s)
    return sorted(results, key=lambda x: x.get("rsi", 100))


def scan_short_squeeze(bars: dict | None = None) -> list:
    """Short float >15%, price rising. Uses cached fundamentals if available."""
    from pathlib import Path
    import json
    import time

    data = _scan_all(bars=bars)
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


def scan_earnings_runner(bars: dict | None = None) -> list:
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

    data = _scan_all(bars=bars)
    return [
        s
        for s in data
        if s["symbol"] in earning_syms and s.get("change_pct", 0) > 0
    ]


def scan_volatility_breakout(bars: dict | None = None) -> list:
    """Opening Range breakouts confirmed by ATR + volume.

    `bars` is accepted for interface uniformity with the other scan_*
    helpers; the underlying volatility_breakout module reads its own
    intraday data and does not consume daily bars today.
    """
    try:
        from engine.volatility_breakout import scan_all_breakouts
        return scan_all_breakouts()
    except Exception:
        return []


def scan_discovery(bars: dict | None = None) -> list:
    """New opportunities outside the watchlist.

    `bars` accepted for interface uniformity (see scan_volatility_breakout).
    """
    try:
        from engine.discovery_scanner import get_cached_discoveries
        return get_cached_discoveries()
    except Exception:
        return []


def scan_channel(channel: str, bars: dict | None = None) -> list:
    """Run a named channel scan.

    HM-SLOW-FUNDAMENTALS Phase 2 (2026-05-21): `bars` threads through to the
    underlying scan_* helpers so a single pre-fetched bulk OHLCV dict is
    shared across all channels in one /api/channels invocation.
    """
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
    return fn(bars=bars)


def get_all_channels(bars: dict | None = None) -> dict:
    """Run all channels and return results."""
    return {
        "gap-and-go": scan_gap_and_go(bars=bars),
        "momentum-breakout": scan_momentum_breakout(bars=bars),
        "reversal-bounce": scan_reversal_bounce(bars=bars),
        "short-squeeze": scan_short_squeeze(bars=bars),
        "earnings-runner": scan_earnings_runner(bars=bars),
        "volatility-breakout": scan_volatility_breakout(bars=bars),
        "discovery": scan_discovery(bars=bars),
    }
