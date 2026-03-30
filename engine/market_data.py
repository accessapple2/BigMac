"""Market data — Yahoo direct HTTP only (no yfinance)."""
from __future__ import annotations
import ccxt
import pandas as pd
import numpy as np
import requests
import time
import threading
import sqlite3
from datetime import datetime
from rich.console import Console

console = Console()

# Rate-limit cooldown for Yahoo direct
_yahoo_limited_until = 0
_COOLDOWN_SECONDS = 120  # 2 minutes
_cooldown_logged = False

# Price cache (symbol -> {data, ts})
_price_cache = {}
_PRICE_CACHE_TTL = 300  # 5 minutes — matches scan interval, prevents redundant Yahoo calls

DB_PATH = "data/trader.db"

# Yahoo crumb/cookie session for authenticated endpoints (options chains, quoteSummary)
_yahoo_session = None
_yahoo_crumb = None
_yahoo_session_ts = 0
_YAHOO_SESSION_TTL = 1800  # 30 minutes


def _get_yahoo_session():
    """Get authenticated Yahoo session with crumb for v7/v10 endpoints."""
    global _yahoo_session, _yahoo_crumb, _yahoo_session_ts
    now = time.time()
    if _yahoo_session and _yahoo_crumb and (now - _yahoo_session_ts) < _YAHOO_SESSION_TTL:
        return _yahoo_session, _yahoo_crumb
    try:
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0"})
        s.get("https://fc.yahoo.com", timeout=10)
        crumb_resp = s.get("https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=10)
        if crumb_resp.status_code == 200 and crumb_resp.text:
            _yahoo_session = s
            _yahoo_crumb = crumb_resp.text
            _yahoo_session_ts = now
            return s, crumb_resp.text
    except Exception:
        pass
    return None, None


def yahoo_options_chain(symbol: str) -> dict | None:
    """Fetch options chain from Yahoo Finance v7 with crumb auth."""
    s, crumb = _get_yahoo_session()
    if not s or not crumb:
        return None
    try:
        url = f"https://query1.finance.yahoo.com/v7/finance/options/{symbol}?crumb={crumb}"
        r = s.get(url, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        result = data.get("optionChain", {}).get("result", [])
        return result[0] if result else None
    except Exception:
        return None


def yahoo_options_chain_for_date(symbol: str, expiry_ts: int) -> dict | None:
    """Fetch options chain for a specific expiry date (Unix timestamp)."""
    s, crumb = _get_yahoo_session()
    if not s or not crumb:
        return None
    try:
        url = f"https://query1.finance.yahoo.com/v7/finance/options/{symbol}?date={expiry_ts}&crumb={crumb}"
        r = s.get(url, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        result = data.get("optionChain", {}).get("result", [])
        if not result:
            return None
        options = result[0].get("options", [])
        return options[0] if options else None
    except Exception:
        return None


def yahoo_quote_summary(symbol: str, modules: str = "calendarEvents") -> dict | None:
    """Fetch quote summary from Yahoo Finance v10 with crumb auth."""
    s, crumb = _get_yahoo_session()
    if not s or not crumb:
        return None
    try:
        url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}?modules={modules}&crumb={crumb}"
        r = s.get(url, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        results = data.get("quoteSummary", {}).get("result", [])
        return results[0] if results else None
    except Exception:
        return None

_YAHOO_UA_POOL = [
    "Mozilla/5.0",
    "Mozilla/5.0 (compatible)",
    "TradeMinds/1.0",
]
_yahoo_ua_idx = 0


def _get_yahoo_headers():
    """Rotate User-Agent to avoid per-UA rate limiting."""
    global _yahoo_ua_idx
    ua = _YAHOO_UA_POOL[_yahoo_ua_idx % len(_YAHOO_UA_POOL)]
    _yahoo_ua_idx += 1
    return {"User-Agent": ua}

# Keep these stubs so the 24 engine modules that import them don't break
_yf_limited_until = 0


def _is_yf_limited():
    """Always True — yfinance is disabled."""
    return True


def _set_yf_limited():
    """No-op — yfinance is disabled."""
    pass


def _is_yahoo_limited():
    return time.time() < _yahoo_limited_until


def _set_yahoo_limited():
    global _yahoo_limited_until, _cooldown_logged
    if not _is_yahoo_limited():
        _cooldown_logged = False
    _yahoo_limited_until = time.time() + _COOLDOWN_SECONDS
    if not _cooldown_logged:
        console.log(f"[yellow]Yahoo direct rate limited — cooldown {_COOLDOWN_SECONDS}s, using DB cache")
        _cooldown_logged = True


def _get_cached_price(symbol):
    """Return cached price if fresh enough."""
    if symbol in _price_cache:
        entry = _price_cache[symbol]
        if time.time() - entry["ts"] < _PRICE_CACHE_TTL:
            return entry["data"]
    return None


def _cache_price(symbol, data):
    """Store price in cache."""
    _price_cache[symbol] = {"data": data, "ts": time.time()}


_last_yahoo_call = 0
_YAHOO_MIN_GAP = 0.3  # seconds between Yahoo calls to avoid rate limiting
_yahoo_lock = threading.Lock()


def _yahoo_chart(symbol, interval="1m", range_="1d"):
    """Fetch chart data from Yahoo Finance direct HTTP endpoint."""
    global _last_yahoo_call
    if _is_yahoo_limited():
        return None
    # Thread-safe throttle: only one Yahoo call at a time
    with _yahoo_lock:
        elapsed = time.time() - _last_yahoo_call
        if elapsed < _YAHOO_MIN_GAP:
            time.sleep(_YAHOO_MIN_GAP - elapsed)
        if _is_yahoo_limited():
            return None
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval={interval}&range={range_}"
        for attempt in range(3):
            try:
                _last_yahoo_call = time.time()
                r = requests.get(url, headers=_get_yahoo_headers(), timeout=10)
                if r.status_code == 429:
                    if attempt < 2:
                        time.sleep(2 + attempt * 2)  # 2s, 4s backoff
                        continue
                    _set_yahoo_limited()
                    return None
                if r.status_code != 200:
                    return None
                data = r.json()
                result = data.get("chart", {}).get("result", [])
                if not result:
                    return None
                return result[0]
            except Exception:
                if attempt < 2:
                    time.sleep(1)
                    continue
                return None
        return None


def get_bulk_prices(symbols: list, timeout: int = 5) -> dict:
    """Fetch ALL symbols in ONE Yahoo Finance batch request — much faster than individual calls.
    Falls back to get_all_prices() on any error. Returns {symbol: price_data}."""
    if not symbols:
        return {}
    try:
        sym_str = ",".join(symbols)
        url = (
            "https://query1.finance.yahoo.com/v7/finance/quote"
            f"?symbols={sym_str}"
            "&fields=regularMarketPrice,regularMarketChangePercent,"
            "regularMarketVolume,regularMarketDayHigh,regularMarketDayLow,"
            "regularMarketPreviousClose"
        )
        r = requests.get(url, headers=_get_yahoo_headers(), timeout=timeout)
        if r.status_code != 200:
            raise ValueError(f"HTTP {r.status_code}")
        body = r.json()
        quotes = body.get("quoteResponse", {}).get("result", [])
        if not quotes:
            raise ValueError("empty result")
        results = {}
        for q in quotes:
            sym = q.get("symbol")
            price = q.get("regularMarketPrice", 0)
            prev = q.get("regularMarketPreviousClose", price) or price
            chg = round((price - prev) / prev * 100, 2) if prev else 0
            data = {
                "symbol": sym,
                "price": round(float(price), 2),
                "change_pct": round(float(q.get("regularMarketChangePercent", chg)), 2),
                "high": round(float(q.get("regularMarketDayHigh", price)), 2),
                "low": round(float(q.get("regularMarketDayLow", price)), 2),
                "volume": int(q.get("regularMarketVolume", 0)),
                "timestamp": datetime.now().isoformat(),
                "source": "yahoo_bulk",
            }
            if sym:
                results[sym] = data
                _cache_price(sym, data)
        return results
    except Exception:
        # Fallback to individual calls
        return get_all_prices(symbols)


def get_all_prices(symbols: list) -> dict:
    """Fetch all prices in parallel. Returns {symbol: price_data}."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    results = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(get_stock_price, sym): sym for sym in symbols}
        for f in as_completed(futs):
            sym = futs[f]
            try:
                data = f.result()
                if "error" not in data:
                    results[sym] = data
            except Exception:
                pass
    return results


def get_stock_price(symbol):
    """Fetch stock price: Yahoo → Finnhub → Alpha Vantage → DB cache."""
    cached = _get_cached_price(symbol)
    if cached:
        return cached

    # Source 1: Yahoo direct HTTP
    chart = _yahoo_chart(symbol, interval="1m", range_="1d")
    if chart:
        meta = chart.get("meta", {})
        price = meta.get("regularMarketPrice", 0)
        prev_close = meta.get("chartPreviousClose") or meta.get("previousClose") or price
        if price:
            chg = round((price - prev_close) / prev_close * 100, 2) if prev_close else 0
            data = {
                "symbol": symbol,
                "price": round(float(price), 2),
                "change_pct": chg,
                "high": round(float(meta.get("regularMarketDayHigh", price)), 2),
                "low": round(float(meta.get("regularMarketDayLow", price)), 2),
                "volume": int(meta.get("regularMarketVolume", 0)),
                "timestamp": datetime.now().isoformat(),
                "source": "yahoo_direct",
            }
            _cache_price(symbol, data)
            return data

    # Source 2: Finnhub fallback
    try:
        from engine.finnhub_data import get_quote as fh_quote
        fh = fh_quote(symbol)
        if fh and "error" not in fh:
            _cache_price(symbol, fh)
            return fh
    except Exception:
        pass

    # Source 3: Alpha Vantage fallback
    try:
        from engine.alphavantage_data import get_quote as av_quote
        av = av_quote(symbol)
        if av and "error" not in av:
            _cache_price(symbol, av)
            return av
    except Exception:
        pass

    # Source 4: DB fallback (last known price)
    data = _try_db_fallback(symbol)
    if data:
        _cache_price(symbol, data)
        return data

    return {"symbol": symbol, "error": "All price sources failed"}


def _try_db_fallback(symbol):
    """Last known price from trades or positions database."""
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT price, executed_at FROM trades WHERE symbol=? ORDER BY executed_at DESC LIMIT 1",
            (symbol,)
        ).fetchone()
        if row:
            conn.close()
            return {
                "symbol": symbol,
                "price": round(float(row["price"]), 2),
                "change_pct": 0,
                "high": round(float(row["price"]), 2),
                "low": round(float(row["price"]), 2),
                "volume": 0,
                "timestamp": row["executed_at"],
                "source": "db_cache",
            }
        row = conn.execute(
            "SELECT avg_price FROM positions WHERE symbol=? LIMIT 1", (symbol,)
        ).fetchone()
        conn.close()
        if row:
            return {
                "symbol": symbol,
                "price": round(float(row["avg_price"]), 2),
                "change_pct": 0,
                "high": round(float(row["avg_price"]), 2),
                "low": round(float(row["avg_price"]), 2),
                "volume": 0,
                "timestamp": datetime.now().isoformat(),
                "source": "db_position",
            }
    except Exception:
        pass
    return None


def _calc_rsi(series: pd.Series, period: int = 14) -> float:
    """Calculate RSI from a price series."""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return round(float(rsi.iloc[-1]), 2) if not rsi.empty and pd.notna(rsi.iloc[-1]) else 50.0


def _calc_macd(series: pd.Series) -> dict:
    """Calculate MACD (12, 26, 9) from a price series."""
    ema12 = series.ewm(span=12, adjust=False).mean()
    ema26 = series.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    histogram = macd_line - signal_line
    return {
        "macd": round(float(macd_line.iloc[-1]), 4) if pd.notna(macd_line.iloc[-1]) else 0.0,
        "signal": round(float(signal_line.iloc[-1]), 4) if pd.notna(signal_line.iloc[-1]) else 0.0,
        "histogram": round(float(histogram.iloc[-1]), 4) if pd.notna(histogram.iloc[-1]) else 0.0,
    }


def get_technical_indicators(symbol: str) -> dict:
    """Fetch daily data via Yahoo direct and compute RSI, MACD, SMA 50/200, volume ratio."""
    chart = _yahoo_chart(symbol, interval="1d", range_="1y")
    if not chart:
        return {}
    try:
        timestamps = chart.get("timestamp", [])
        indicators = chart.get("indicators", {})
        quotes = indicators.get("quote", [{}])[0]

        closes = quotes.get("close", [])
        volumes = quotes.get("volume", [])
        highs = quotes.get("high", [])
        lows = quotes.get("low", [])

        if not closes or len(closes) < 30:
            return {}

        # Build pandas Series, filtering None values
        close = pd.Series([c for c in closes if c is not None])
        volume = pd.Series([v if v is not None else 0 for v in volumes])

        if len(close) < 30:
            return {}

        sma_50 = round(float(close.rolling(50).mean().iloc[-1]), 2) if len(close) >= 50 else None
        sma_200 = round(float(close.rolling(200).mean().iloc[-1]), 2) if len(close) >= 200 else None
        rsi = _calc_rsi(close)
        macd = _calc_macd(close)

        avg_vol_20 = volume.rolling(20).mean().iloc[-1]
        vol_ratio = round(float(volume.iloc[-1] / avg_vol_20), 2) if avg_vol_20 > 0 else 1.0

        current = float(close.iloc[-1])
        above_50 = current > sma_50 if sma_50 else None
        above_200 = current > sma_200 if sma_200 else None

        return {
            "rsi": rsi,
            "macd": macd["macd"],
            "macd_signal": macd["signal"],
            "macd_histogram": macd["histogram"],
            "sma_50": sma_50,
            "sma_200": sma_200,
            "above_sma50": above_50,
            "above_sma200": above_200,
            "volume_ratio": vol_ratio,
            "avg_volume_20d": int(avg_vol_20) if pd.notna(avg_vol_20) else 0,
        }
    except Exception as e:
        console.log(f"[red]Indicators error for {symbol}: {e}")
        return {}


def get_intraday_candles(symbol: str, interval: str = "5m", range_: str = "1d") -> list:
    """Fetch OHLCV candles via Yahoo direct with configurable range."""
    chart = _yahoo_chart(symbol, interval=interval, range_=range_)
    if not chart:
        return []
    try:
        timestamps = chart.get("timestamp", [])
        quotes = chart.get("indicators", {}).get("quote", [{}])[0]

        opens = quotes.get("open", [])
        highs = quotes.get("high", [])
        lows = quotes.get("low", [])
        closes = quotes.get("close", [])
        volumes = quotes.get("volume", [])

        candles = []
        for i, ts in enumerate(timestamps):
            if i >= len(closes) or closes[i] is None:
                continue
            candles.append({
                "time": datetime.fromtimestamp(ts).isoformat(),
                "open": round(float(opens[i] or closes[i]), 2),
                "high": round(float(highs[i] or closes[i]), 2),
                "low": round(float(lows[i] or closes[i]), 2),
                "close": round(float(closes[i]), 2),
                "volume": int(volumes[i] or 0),
            })
        return candles
    except Exception as e:
        console.log(f"[red]Intraday error for {symbol}: {e}")
        return []


def get_crypto_price(symbol, exchange_id="kraken"):
    try:
        exchange = getattr(ccxt, exchange_id)()
        t = exchange.fetch_ticker(symbol)
        return {"symbol": symbol, "price": round(float(t["last"]), 2), "change_pct": round(float(t["percentage"] or 0), 2), "high": round(float(t["high"]), 2), "low": round(float(t["low"]), 2), "timestamp": datetime.now().isoformat()}
    except Exception as e:
        return {"symbol": symbol, "error": str(e)}


def scan_all(stocks, crypto):
    results = {"stocks": {}, "crypto": {}, "scanned_at": datetime.now().isoformat()}
    for s in stocks:
        results["stocks"][s] = get_stock_price(s)
        console.log(f"[cyan]📈 {s}: ${results['stocks'][s].get('price', 'ERR')}")
    for s in crypto:
        results["crypto"][s] = get_crypto_price(s)
        console.log(f"[magenta]₿ {s}: ${results['crypto'][s].get('price', 'ERR')}")
    return results
