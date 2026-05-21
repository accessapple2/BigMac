"""Market data — Alpaca primary, Yahoo fallback (no yfinance)."""
from __future__ import annotations
import os
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
_COOLDOWN_SECONDS = 60   # back off 60s on 429
_cooldown_logged = False

# Price cache (symbol -> {data, ts}) — shared across all agents
_price_cache = {}
_PRICE_CACHE_TTL = 60    # 60s: multiple agents share one pull per symbol

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
_YAHOO_MIN_GAP = 1.0  # max 1 request/second to Yahoo Finance
_yahoo_lock = threading.Lock()

# ── Alpaca market data — direct HTTP (primary price source, no SDK dependency) ─
_alpaca_headers: dict | None = None


def _get_alpaca_headers() -> dict | None:
    """Return Alpaca API auth headers, loading keys from .env once."""
    global _alpaca_headers
    if _alpaca_headers is not None:
        return _alpaca_headers or None
    try:
        from dotenv import load_dotenv
        load_dotenv()
        key = os.getenv("APCA_API_KEY_ID", "")
        secret = os.getenv("APCA_API_SECRET_KEY", "")
        if key and secret:
            _alpaca_headers = {
                "APCA-API-KEY-ID": key,
                "APCA-API-SECRET-KEY": secret,
            }
        else:
            _alpaca_headers = {}   # empty dict = tried, no keys
    except Exception:
        _alpaca_headers = {}
    return _alpaca_headers or None


_ALPACA_BASE = "https://data.alpaca.markets/v2/stocks"


def _alpaca_quote_to_price(symbol: str, q: dict) -> dict:
    """Normalise an Alpaca quote dict into our standard price structure."""
    ask = float(q.get("ap") or q.get("ask_price") or 0)
    bid = float(q.get("bp") or q.get("bid_price") or 0)
    price = round((ask + bid) / 2, 2) if ask and bid else (ask or bid)
    return {
        "symbol": symbol,
        "price": price,
        "change_pct": 0.0,
        "high": price,
        "low": price,
        "volume": int(q.get("as") or q.get("ask_size") or 0),
        "timestamp": datetime.now().isoformat(),
        "source": "alpaca",
    }


def _get_alpaca_price(symbol: str) -> dict | None:
    """GET /v2/stocks/{symbol}/quotes/latest — single symbol, ~5 ms, no rate limit."""
    hdrs = _get_alpaca_headers()
    if not hdrs:
        return None
    try:
        r = requests.get(
            f"{_ALPACA_BASE}/{symbol}/quotes/latest",
            headers=hdrs,
            timeout=5,
        )
        if not r.ok:
            return None
        q = r.json().get("quote", {})
        if not q:
            return None
        data = _alpaca_quote_to_price(symbol, q)
        if not data["price"]:
            return None
        return data
    except Exception:
        return None


def _get_alpaca_bulk_prices(symbols: list) -> dict:
    """GET /v2/stocks/quotes/latest?symbols=... — all symbols in one call."""
    hdrs = _get_alpaca_headers()
    if not hdrs:
        return {}
    try:
        r = requests.get(
            f"{_ALPACA_BASE}/quotes/latest",
            headers=hdrs,
            params={"symbols": ",".join(symbols), "feed": "iex"},
            timeout=10,
        )
        if not r.ok:
            return {}
        quotes = r.json().get("quotes", {})
        results = {}
        for sym, q in quotes.items():
            if not q:
                continue
            data = _alpaca_quote_to_price(sym, q)
            if data["price"]:
                results[sym] = data
                _cache_price(sym, data)
        return results
    except Exception:
        return {}


# === Phase 2: Race tile snapshots ===
def _get_alpaca_bulk_snapshots(symbols: list) -> dict:
    """GET /v2/stocks/snapshots?symbols=... — open + last + volume per symbol in one call.

    Returns {symbol: {symbol, last_price, open_price, high, low, volume,
    prev_close, ts}}. Skips rows missing either latestTrade.p or dailyBar.o.
    Empty dict on auth/HTTP/JSON failure. No chunking — Alpaca supports
    thousands per request and the Race universe is <1000 names.
    """
    hdrs = _get_alpaca_headers()
    if not hdrs:
        return {}
    try:
        r = requests.get(
            f"{_ALPACA_BASE}/snapshots",
            headers=hdrs,
            params={"symbols": ",".join(symbols), "feed": "iex"},
            timeout=15,
        )
        if not r.ok:
            return {}
        body = r.json()
        # Multi-symbol snapshots are FLAT (verified 2026-05-10 live probe).
        # Defensive: tolerate a future wrapped shape too.
        snaps = body.get("snapshots", body)
        results = {}
        for sym, snap in snaps.items():
            if not isinstance(snap, dict):
                continue
            db = snap.get("dailyBar") or {}
            lt = snap.get("latestTrade") or {}
            pdb = snap.get("prevDailyBar") or {}
            last_price = lt.get("p")
            open_price = db.get("o")
            if last_price is None or open_price is None:
                continue
            results[sym] = {
                "symbol": sym,
                "last_price": float(last_price),
                "open_price": float(open_price),
                "high": float(db.get("h") or 0.0),
                "low": float(db.get("l") or 0.0),
                "volume": int(db.get("v") or 0),
                "prev_close": (float(pdb.get("c")) if pdb.get("c") is not None else None),
                "ts": lt.get("t") or db.get("t"),
            }
        return results
    except Exception:
        return {}


def get_bulk_snapshots(symbols: list) -> dict:
    """Public batched snapshot fetch for Race / Scanner tiles.

    Today delegates to Alpaca only. Yahoo has no equivalent single-call
    'open + last + volume' endpoint, so no fallback is wired. Future
    enhancement could derive `open_price` from a Yahoo chart endpoint
    if Alpaca is down.
    """
    if not symbols:
        return {}
    return _get_alpaca_bulk_snapshots(symbols)
# === end Phase 2: Race tile snapshots ===


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
    """Fetch ALL symbols in one request.
    Priority: cache → Alpaca bulk → Yahoo batch → individual fallback.
    Uses extended-hours prices (postMarketPrice / preMarketPrice) when available."""
    if not symbols:
        return {}

    # 1. Return any symbols already cached
    results = {}
    missing = []
    for sym in symbols:
        cached = _get_cached_price(sym)
        if cached:
            results[sym] = cached
        else:
            missing.append(sym)
    if not missing:
        return results

    # 2. Try Alpaca bulk (primary — no rate limit issues)
    alpaca_results = _get_alpaca_bulk_prices(missing)
    results.update(alpaca_results)
    missing = [s for s in missing if s not in alpaca_results]
    if not missing:
        return results

    # 3. Yahoo batch for any Alpaca misses (respect rate limit)
    if _is_yahoo_limited():
        # Yahoo limited — fall back to individual (tries Finnhub/AV/DB)
        results.update(get_all_prices(missing))
        return results
    try:
        sym_str = ",".join(missing)
        url = (
            "https://query1.finance.yahoo.com/v7/finance/quote"
            f"?symbols={sym_str}"
            "&fields=regularMarketPrice,regularMarketChangePercent,"
            "regularMarketVolume,regularMarketDayHigh,regularMarketDayLow,"
            "regularMarketPreviousClose,"
            "postMarketPrice,postMarketChangePercent,"
            "preMarketPrice,preMarketChangePercent"
        )
        with _yahoo_lock:
            elapsed = time.time() - _last_yahoo_call
            if elapsed < _YAHOO_MIN_GAP:
                time.sleep(_YAHOO_MIN_GAP - elapsed)
            session, crumb = _get_yahoo_session()
            if session and crumb:
                url += f"&crumb={crumb}"
                r = session.get(url, headers=_get_yahoo_headers(), timeout=timeout)
            else:
                r = requests.get(url, headers=_get_yahoo_headers(), timeout=timeout)
        if r.status_code == 429:
            _set_yahoo_limited()
            raise ValueError("429 rate limited")
        if r.status_code != 200:
            raise ValueError(f"HTTP {r.status_code}")
        body = r.json()
        quotes = body.get("quoteResponse", {}).get("result", [])
        if not quotes:
            raise ValueError("empty result")
        for q in quotes:
            sym = q.get("symbol")
            regular_price = float(q.get("regularMarketPrice") or 0)
            # Prefer extended-hours price when available (non-zero means session is active)
            post_price = float(q.get("postMarketPrice") or 0)
            pre_price = float(q.get("preMarketPrice") or 0)
            if post_price:
                price = post_price
                chg = round(float(q.get("postMarketChangePercent") or 0), 2)
                source = "yahoo_bulk_ah"
            elif pre_price:
                price = pre_price
                chg = round(float(q.get("preMarketChangePercent") or 0), 2)
                source = "yahoo_bulk_pm"
            else:
                price = regular_price
                prev = float(q.get("regularMarketPreviousClose") or price) or price
                chg = round((price - prev) / prev * 100, 2) if prev else 0
                chg = round(float(q.get("regularMarketChangePercent") or chg), 2)
                source = "yahoo_bulk"
            data = {
                "symbol": sym,
                "price": round(price, 2),
                "change_pct": chg,
                "high": round(float(q.get("regularMarketDayHigh") or price), 2),
                "low": round(float(q.get("regularMarketDayLow") or price), 2),
                "volume": int(q.get("regularMarketVolume") or 0),
                "timestamp": datetime.now().isoformat(),
                "source": source,
            }
            if sym:
                results[sym] = data
                _cache_price(sym, data)
        return results
    except Exception:
        results.update(get_all_prices(missing))
        return results


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


def _is_valid_quote(quote):
    """Reject empty/default Alpaca responses (volume=0 + change=0 + flat range).

    Some symbols (low-liquidity, non-IEX-covered) return a stub dict with
    price set but volume=0 and high==low==price. These should fall through
    to other sources rather than being treated as valid quotes.

    HM-AH 2026-05-06: surfaced when CRDO returned price=169.65 (real $196.09)
    with volume=0 + change_pct=0 + high==low, triggering false Kirk -8% stop alert.
    """
    if not quote or "price" not in quote:
        return False
    if quote.get("price", 0) <= 0:
        return False
    if (quote.get("volume", 0) == 0
        and quote.get("change_pct", 0) == 0
        and quote.get("high") == quote.get("low") == quote.get("price")):
        return False
    return True


def get_stock_price(symbol):
    """Fetch stock price: cache → Alpaca → Yahoo → Finnhub → Alpha Vantage → DB."""
    cached = _get_cached_price(symbol)
    if cached:
        return cached

    # Source 1: Alpaca market data (primary — generous rate limits, no 429 risk)
    alpaca_data = _get_alpaca_price(symbol)
    if alpaca_data and _is_valid_quote(alpaca_data):  # HM-AH 2026-05-06: stub-quote rejection
        _cache_price(symbol, alpaca_data)
        return alpaca_data

    # Source 2: Yahoo direct HTTP (fallback)
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

    # Source 3: Finnhub fallback
    try:
        from engine.finnhub_data import get_quote as fh_quote
        fh = fh_quote(symbol)
        if fh and "error" not in fh:
            _cache_price(symbol, fh)
            return fh
    except Exception:
        pass

    # Source 4: Alpha Vantage fallback
    try:
        from engine.alphavantage_data import get_quote as av_quote
        av = av_quote(symbol)
        if av and "error" not in av:
            _cache_price(symbol, av)
            return av
    except Exception:
        pass

    # Source 5: DB fallback (last known price)
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
            "SELECT price, executed_at FROM trades WHERE symbol=? AND asset_type='stock' ORDER BY executed_at DESC LIMIT 1",
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
            "SELECT avg_price FROM positions WHERE symbol=? AND asset_type='stock' LIMIT 1", (symbol,)
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
    """Fetch OHLCV candles. HM-CB: Polygon-first → Alpaca → Yahoo cascade.

    Polygon Stocks Starter (paid, $29/mo) gives consistent sub-500ms cold-
    symbol latency. Alpaca free-tier is inconsistent on cold bars (some
    symbols 6+ seconds, some 429 too-many-requests). Yahoo direct HTTP
    is the historic slow path. The fallback cascade lets us survive any
    single upstream's transient failure without changing the output
    schema seen by callers.
    """
    # === HM-CB ===
    # Polygon primary — paid tier, no rate-limit surprises, consistent
    # ~400ms per call. Returns 5-min bars in {c,h,l,o,t,v} shape. We
    # adapt to the existing {time,open,high,low,close,volume} schema.
    try:
        import os as _os_p, requests as _req_p
        from datetime import datetime as _dt_p, timedelta as _td_p
        _key_p = _os_p.environ.get("POLYGON_API_KEY", "")
        if not _key_p:
            raise RuntimeError("POLYGON_API_KEY unavailable")

        # Map interval to Polygon's {multiplier, timespan} pair.
        _itv_map = {
            "1m":  (1,  "minute"),
            "5m":  (5,  "minute"),
            "15m": (15, "minute"),
            "30m": (30, "minute"),
            "1h":  (1,  "hour"),
            "1d":  (1,  "day"),
        }
        _itv = _itv_map.get(interval)
        if _itv is None:
            raise ValueError(f"unsupported interval {interval!r}")
        _mult, _span = _itv

        # Map range to days window with 2x padding for weekends/holidays.
        _days_map_p = {"1d": 1, "5d": 5, "1mo": 30, "3mo": 90, "6mo": 180, "1y": 365}
        _days_p = _days_map_p.get(range_, 5)
        _end_p = _dt_p.utcnow().strftime("%Y-%m-%d")
        _start_p = (_dt_p.utcnow() - _td_p(days=_days_p * 2)).strftime("%Y-%m-%d")

        _url = (
            f"https://api.polygon.io/v2/aggs/ticker/{symbol.upper()}"
            f"/range/{_mult}/{_span}/{_start_p}/{_end_p}"
            f"?apiKey={_key_p}&limit=500"
        )
        _r = _req_p.get(_url, timeout=5)
        if _r.status_code != 200:
            raise RuntimeError(f"Polygon HTTP {_r.status_code}")
        _data_p = _r.json()
        _rows = _data_p.get("results", []) or []
        if not _rows:
            raise RuntimeError("Polygon returned 0 bars")

        candles = []
        for _row in _rows:
            _ts_ms = _row.get("t")
            if _ts_ms is None:
                continue
            _iso = datetime.utcfromtimestamp(_ts_ms / 1000).isoformat() + "Z"
            candles.append({
                "time":   _iso,
                "open":   round(float(_row.get("o", 0)), 2),
                "high":   round(float(_row.get("h", 0)), 2),
                "low":    round(float(_row.get("l", 0)), 2),
                "close":  round(float(_row.get("c", 0)), 2),
                "volume": int(_row.get("v", 0) or 0),
            })
        return candles
    except Exception as _e_p:
        console.log(f"[yellow]HM-CB Polygon candles fallback to Alpaca for {symbol}: {type(_e_p).__name__}: {_e_p!r}[/yellow]")
    # === /HM-CB ===

    # === HM-CA ===
    try:
        from alpaca.data import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
        from datetime import datetime as _dt, timedelta as _td
        import os as _os

        _key = _os.environ.get("APCA_API_KEY_ID", "")
        _secret = _os.environ.get("APCA_API_SECRET_KEY", "")
        if not (_key and _secret):
            raise RuntimeError("Alpaca credentials unavailable")

        # Parse interval string → Alpaca TimeFrame
        _tf_map = {
            "1m":  TimeFrame.Minute,
            "5m":  TimeFrame(5,  TimeFrameUnit.Minute),
            "15m": TimeFrame(15, TimeFrameUnit.Minute),
            "30m": TimeFrame(30, TimeFrameUnit.Minute),
            "1h":  TimeFrame.Hour,
            "1d":  TimeFrame.Day,
        }
        _tf = _tf_map.get(interval)
        if _tf is None:
            raise ValueError(f"unsupported interval {interval!r}")

        # Parse range string → days window (x2 for weekend/holiday padding)
        _days_map = {"1d": 1, "5d": 5, "1mo": 30, "3mo": 90, "6mo": 180, "1y": 365}
        _days = _days_map.get(range_, 5)
        _end = _dt.now()
        _start = _end - _td(days=_days * 2)

        _client = StockHistoricalDataClient(_key, _secret)
        _req = StockBarsRequest(
            symbol_or_symbols=[symbol.upper()],
            timeframe=_tf,
            start=_start,
            end=_end,
            feed="iex",
        )
        _resp = _client.get_stock_bars(_req)
        _raw = _resp.data.get(symbol.upper(), [])
        if not _raw:
            raise RuntimeError("no bars returned")

        candles = []
        for _bar in _raw:
            _ts = _bar.timestamp
            _iso = _ts.isoformat() if hasattr(_ts, "isoformat") else str(_ts)
            candles.append({
                "time":   _iso,
                "open":   round(float(_bar.open), 2),
                "high":   round(float(_bar.high), 2),
                "low":    round(float(_bar.low), 2),
                "close":  round(float(_bar.close), 2),
                "volume": int(_bar.volume or 0),
            })
        return candles
    except Exception as _e:
        # Fall through to Yahoo. Log once per failure class via [yellow]
        # so persistent Alpaca breakage surfaces but we still serve data.
        console.log(f"[yellow]HM-CA Alpaca candles fallback to Yahoo for {symbol}: {type(_e).__name__}: {_e!r}[/yellow]")
    # === /HM-CA ===

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
                "time": datetime.utcfromtimestamp(ts).isoformat() + "Z",
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


def get_polygon_bars(
    symbols,
    timeframe: str = "1Day",
    days: int = 300,
    max_workers: int = 10,
) -> "pd.DataFrame | dict":
    """Fetch OHLCV bars from Polygon — drop-in replacement for get_alpaca_bars().

    Polygon Starter ($29/mo) has unlimited API calls and 5+ years of history,
    so this avoids the Alpaca free-tier limits (50-symbol batch, 155-day cap).
    Default days=300 calendar yields ~210 trading days — enough for SMA200.

    Single symbol  → pandas DataFrame with DatetimeIndex and columns
                     Open, High, Low, Close, Volume  (matches Alpaca layout).
    List of symbols → dict {symbol: DataFrame}.
    Returns empty DataFrame / {} on failure so callers degrade gracefully.

    Note: timeframe parameter accepted for API compatibility but only "1Day"
    is currently wired through (uses fetch_daily_bars under the hood).
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from strategies.polygon_client import fetch_daily_bars
    from strategies.polygon_config import is_polygon_configured

    if not is_polygon_configured():
        return pd.DataFrame() if isinstance(symbols, str) else {}

    single = isinstance(symbols, str)
    sym_list = [symbols] if single else list(symbols)

    def _fetch_one(sym):
        try:
            bars = fetch_daily_bars(sym, days=days)
            if not bars:
                return sym, None
            df = pd.DataFrame(bars)
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")
            df = df.rename(columns={
                "open":   "Open",
                "high":   "High",
                "low":    "Low",
                "close":  "Close",
                "volume": "Volume",
            })
            return sym, df
        except Exception:
            return sym, None

    result: dict = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_fetch_one, s) for s in sym_list]
        for fut in as_completed(futures):
            sym, df = fut.result()
            if df is not None and not df.empty:
                result[sym] = df

    if single:
        return result.get(symbols, pd.DataFrame())
    return result


def get_alpaca_bars(
    symbols,
    timeframe: str = "1Day",
    days: int = 30,
) -> "pd.DataFrame | dict":
    """Fetch OHLCV bars from Alpaca — drop-in replacement for yf.download().

    Single symbol  → pandas DataFrame with DatetimeIndex and columns
                     Open, High, Low, Close, Volume  (matches yfinance layout).
    List of symbols → dict {symbol: DataFrame}.
    Returns empty DataFrame / {} on failure so callers degrade gracefully.
    """
    hdrs = _get_alpaca_headers()
    if not hdrs:
        return pd.DataFrame() if isinstance(symbols, str) else {}
    single = isinstance(symbols, str)
    sym_list = [symbols] if single else list(symbols)
    start = (datetime.utcnow() - pd.Timedelta(days=days + 5)).strftime("%Y-%m-%d")
    try:
        r = requests.get(
            f"{_ALPACA_BASE}/bars",
            headers=hdrs,
            params={
                "symbols":   ",".join(sym_list),
                "timeframe": timeframe,
                "start":     start,
                "limit":     len(sym_list) * (days + 5),
                "feed":      "iex",
                "sort":      "asc",
            },
            timeout=15,
        )
        if not r.ok:
            # === HM-AX: batch failed — fall back to per-symbol calls (2026-05-11) ===
            # Root cause (2026-05-11 AM): one halted ticker (e.g., CTRA) in the
            # batch returned null data and Alpaca 400'd the entire batch, silently
            # dropping good tickers from the scan window. Per-symbol fallback
            # keeps the good ones flowing.
            if single:
                console.log(f"[yellow]get_alpaca_bars HTTP {r.status_code} for {sym_list[0]}")
                return pd.DataFrame()
            console.log(
                f"[yellow]get_alpaca_bars batch HTTP {r.status_code} for "
                f"{len(sym_list)} syms — falling back per-symbol"
            )
            result: dict = {}
            for sym in sym_list:
                try:
                    rr = requests.get(
                        f"{_ALPACA_BASE}/{sym}/bars",
                        headers=hdrs,
                        params={
                            "timeframe": timeframe,
                            "start":     start,
                            "limit":     days + 5,
                            "feed":      "iex",
                            "sort":      "asc",
                        },
                        timeout=10,
                    )
                    if not rr.ok:
                        result[sym] = pd.DataFrame()
                        continue
                    rows = rr.json().get("bars") or []
                    if not rows:
                        result[sym] = pd.DataFrame()
                        continue
                    df = pd.DataFrame(rows)
                    df["t"] = pd.to_datetime(df["t"])
                    df = df.set_index("t").rename(columns={
                        "o": "Open", "h": "High", "l": "Low",
                        "c": "Close", "v": "Volume",
                    })
                    df.index.name = "Date"
                    keep = [c for c in ("Open", "High", "Low", "Close", "Volume") if c in df.columns]
                    result[sym] = df[keep].tail(days)
                except Exception as e:
                    console.log(f"[yellow]per-symbol fallback failed for {sym}: {e}")
                    result[sym] = pd.DataFrame()
            return result
            # === end HM-AX ===
        bars_by_sym = r.json().get("bars", {})
        result: dict = {}
        for sym in sym_list:
            rows = bars_by_sym.get(sym, [])
            if not rows:
                result[sym] = pd.DataFrame()
                continue
            df = pd.DataFrame(rows)
            df["t"] = pd.to_datetime(df["t"])
            df = df.set_index("t").rename(columns={
                "o": "Open", "h": "High", "l": "Low",
                "c": "Close", "v": "Volume",
            })
            df.index.name = "Date"
            keep = [c for c in ("Open", "High", "Low", "Close", "Volume") if c in df.columns]
            result[sym] = df[keep].tail(days)
        return result[sym_list[0]] if single else result
    except Exception as e:
        console.log(f"[yellow]get_alpaca_bars error: {e}")
        return pd.DataFrame() if single else {}


# === HM-SLOW-FUNDAMENTALS Phase 1 — bulk daily OHLCV fetcher ===
# Verified live 2026-05-21: 3,026-symbol scan_universe completes in 8.64s
# parallel (8 workers, chunk=100). Avoids the 10,000-row response cap at
# chunk=100 × ~63 bars = ~6,300 rows. feed=iex is mandatory on paper tier;
# without it Alpaca returns HTTP 200 with body {"message":"subscription
# does not permit querying recent SIP data"} — silent-failure trap.
_BULK_BARS_CHUNK_SIZE = 100
_BULK_BARS_PARALLELISM = 8
_BULK_BARS_TIMEOUT_S = 10
_BULK_BARS_FEED = "iex"
_BULK_BARS_CACHE_TTL = 1800  # 30 min — matches HM-SLOW-FUNDAMENTALS scope memo
_bulk_bars_cache: dict = {}  # {(symbol, range_str): {"data": pd.DataFrame, "ts": float}}
_bulk_bars_cache_lock = threading.Lock()

# Range-string → calendar days back (with +5d slop for weekends/holidays so we
# always get at least the requested trading-day count after Alpaca filters).
_BULK_BARS_RANGE_MAP = {
    "1mo": 35,
    "3mo": 95,
    "6mo": 185,
    "1y": 370,
}


def _alpaca_bulk_bars_chunk(
    symbols_chunk: list,
    start_date_iso: str,
    end_date_iso: str,
) -> dict:
    """One Alpaca multi-symbol bars call for up to ~_BULK_BARS_CHUNK_SIZE symbols.

    Returns {symbol: pd.DataFrame[Open, High, Low, Close, Volume]} indexed by
    DatetimeIndex named "Date". Empty DataFrame for symbols with no rows.
    Returns {} on auth/HTTP/JSON failure so the caller backfills empties.

    Defensive: handles the silent-failure pattern where Alpaca returns
    HTTP 200 with body {"message": "..."} when the feed/subscription
    doesn't match the request (no "bars" key present).
    """
    hdrs = _get_alpaca_headers()
    if not hdrs:
        return {}
    try:
        r = requests.get(
            f"{_ALPACA_BASE}/bars",
            headers=hdrs,
            params={
                "symbols":   ",".join(symbols_chunk),
                "timeframe": "1Day",
                "start":     start_date_iso,
                "end":       end_date_iso,
                "limit":     10000,
                "feed":      _BULK_BARS_FEED,
                "sort":      "asc",
            },
            timeout=_BULK_BARS_TIMEOUT_S,
        )
        if not r.ok:
            console.log(
                f"[yellow]_alpaca_bulk_bars_chunk HTTP {r.status_code} "
                f"for {len(symbols_chunk)} syms"
            )
            return {}
        j = r.json()
        if "bars" not in j:
            # Silent-failure trap: HTTP 200 + subscription/feed error body.
            msg = j.get("message", "no-bars-key")
            console.log(
                f"[yellow]_alpaca_bulk_bars_chunk message-only response: {msg}"
            )
            return {}
        bars_by_sym = j.get("bars") or {}
        result: dict = {}
        for sym in symbols_chunk:
            rows = bars_by_sym.get(sym) or []
            if not rows:
                result[sym] = pd.DataFrame()
                continue
            df = pd.DataFrame(rows)
            df["t"] = pd.to_datetime(df["t"])
            df = df.set_index("t").rename(columns={
                "o": "Open", "h": "High", "l": "Low",
                "c": "Close", "v": "Volume",
            })
            df.index.name = "Date"
            keep = [c for c in ("Open", "High", "Low", "Close", "Volume") if c in df.columns]
            result[sym] = df[keep]
        return result
    except Exception as e:
        console.log(
            f"[yellow]_alpaca_bulk_bars_chunk error "
            f"({type(e).__name__}: {e!r}) — {len(symbols_chunk)} syms"
        )
        return {}


def get_bulk_daily_ohlcv(
    symbols: list,
    range_str: str = "3mo",
) -> dict:
    """Fetch daily OHLCV bars for many symbols via Alpaca's bulk-bars endpoint.

    Chunks the request into ``_BULK_BARS_CHUNK_SIZE``-symbol groups and fans
    out via a ``_BULK_BARS_PARALLELISM``-worker ThreadPoolExecutor. Caches
    each symbol individually for ``_BULK_BARS_CACHE_TTL`` seconds (30 min).
    Cache key is (symbol, range_str) so different ranges don't collide.

    Returns ``{symbol: pd.DataFrame}`` where each DataFrame has columns
    ``Open, High, Low, Close, Volume`` indexed by a DatetimeIndex named
    "Date". Symbols with no data get an empty DataFrame (matches the
    contract used by engine.trendlines._fetch_daily_ohlcv and friends).

    Verified 2026-05-21: full 3,026-symbol scan_universe completes in
    ~8.6s parallel (HM-SLOW-FUNDAMENTALS Phase 1 baseline).
    """
    if not symbols:
        return {}
    if range_str not in _BULK_BARS_RANGE_MAP:
        console.log(
            f"[yellow]get_bulk_daily_ohlcv unknown range_str={range_str!r} — "
            f"falling back to 3mo. Known: {list(_BULK_BARS_RANGE_MAP)}"
        )
        range_str = "3mo"

    now = time.time()
    result: dict = {}
    misses: list = []
    with _bulk_bars_cache_lock:
        for sym in symbols:
            entry = _bulk_bars_cache.get((sym, range_str))
            if entry and (now - entry["ts"]) < _BULK_BARS_CACHE_TTL:
                result[sym] = entry["data"]
            else:
                misses.append(sym)
    if not misses:
        return result

    end_dt = datetime.utcnow()
    days_back = _BULK_BARS_RANGE_MAP[range_str]
    start_iso = (end_dt - pd.Timedelta(days=days_back)).strftime("%Y-%m-%d")
    end_iso = end_dt.strftime("%Y-%m-%d")

    chunks = [
        misses[i:i + _BULK_BARS_CHUNK_SIZE]
        for i in range(0, len(misses), _BULK_BARS_CHUNK_SIZE)
    ]

    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=_BULK_BARS_PARALLELISM
    ) as ex:
        futures = [
            ex.submit(_alpaca_bulk_bars_chunk, chunk, start_iso, end_iso)
            for chunk in chunks
        ]
        for fut in concurrent.futures.as_completed(futures):
            try:
                chunk_result = fut.result(timeout=_BULK_BARS_TIMEOUT_S + 5)
                result.update(chunk_result)
            except Exception as e:
                console.log(
                    f"[yellow]get_bulk_daily_ohlcv chunk failed "
                    f"({type(e).__name__}: {e!r})"
                )

    with _bulk_bars_cache_lock:
        for sym, df in result.items():
            if not df.empty:
                _bulk_bars_cache[(sym, range_str)] = {"data": df, "ts": now}

    for sym in symbols:
        if sym not in result:
            result[sym] = pd.DataFrame()

    return result
# === /HM-SLOW-FUNDAMENTALS Phase 1 ===


# VIX cache — avoid hammering Yahoo for index data
_vix_cache: dict = {}
_VIX_CACHE_TTL = 300  # 5 min — VIX doesn't need 60s freshness


def get_vix() -> float:
    """Return current VIX level using Yahoo direct HTTP (index, not stock — separate bucket).
    Cached 5 min. Returns 20.0 as a safe default on failure."""
    now = time.time()
    if _vix_cache.get("ts") and now - _vix_cache["ts"] < _VIX_CACHE_TTL:
        return _vix_cache["value"]
    try:
        r = requests.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX",
            params={"interval": "1d", "range": "2d"},
            headers=_get_yahoo_headers(),
            timeout=8,
        )
        if r.ok:
            meta = r.json()["chart"]["result"][0]["meta"]
            val = float(meta.get("regularMarketPrice") or meta.get("chartPreviousClose") or 20.0)
            _vix_cache["value"] = round(val, 2)
            _vix_cache["ts"] = now
            return _vix_cache["value"]
    except Exception:
        pass
    return _vix_cache.get("value", 20.0)


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
