"""screener_engine.py — Finviz-grade stock screener for TradeMinds.

run_screener(filters) → list of matching stock dicts
get_universe() → list of symbols (watchlist + extended)

Data sources:
  - yfinance for price/fundamentals (cached 5 min for prices, 1h for fundamentals)
  - trader.db for fleet consensus, signals, agent trades
  - autonomous_trader.db for GEX data
  - congress_scraper for congress activity

Filter keys accepted (all optional, skip if not provided):
  # Fundamental
  mktcap: mega/large/mid/small (>200B/>10B/>2B/<2B)
  pe_max: float
  pe_min: float
  div_yield_min: float (percent, e.g. 2.0 = 2%)
  eps_growth_pos: bool
  roe_min: float (percent)
  sector: str (exact match)

  # Technical
  rsi_max: float
  rsi_min: float
  above_sma20: bool
  above_sma50: bool
  above_sma200: bool
  rvol_min: float (relative volume)
  change_min: float (percent)
  change_max: float (percent)
  gap_up_min: float (percent gap from prev close)
  gap_down_max: float (percent gap, negative)
  beta_max: float
  beta_min: float

  # TradeMinds exclusive
  fleet_bull_min: int (% of fleet bullish, 0-100)
  fleet_bear_min: int
  has_congress: bool
  has_active_signal: bool
  backtest_winrate_min: float (0-100)
  red_alert_condition: str (GO/CAUTION/STAND DOWN)
  uptrend: bool (EMA8 > EMA21)
  downtrend: bool

  # Screener meta
  limit: int (default 50)
  sort_by: str (change/rvol/rsi/fleet/score, default score)
  sort_dir: str (asc/desc, default desc)
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import urllib.request
from typing import Any
import logging

logger = logging.getLogger(__name__)

# ── Cache ─────────────────────────────────────────────────────────────────────
_price_cache: dict[str, dict] = {}
_price_cache_ts: dict[str, float] = {}
_fund_cache: dict[str, dict] = {}
_fund_cache_ts: dict[str, float] = {}
_universe_cache: list[str] = []
_universe_cache_ts: float = 0.0
_screener_cache: dict[str, Any] = {}   # hash(filters) → {ts, results}
_PRICE_TTL    = 300    # 5 min
_FUND_TTL     = 3600   # 1 hour
_UNIVERSE_TTL = 86400  # 24 hours
_SCREENER_TTL = 300    # 5 min full-scan result cache
_cache_lock = threading.Lock()

DB_PATH   = "data/trader.db"
ATDB_PATH = "autonomous_trader.db"

WATCHLIST = [
    "SPY", "QQQ", "TQQQ", "NVDA", "TSLA", "AAPL", "AMD", "META",
    "MSFT", "GOOGL", "AMZN", "MU", "ORCL", "NOW", "AVGO", "PLTR", "DELL",
]

# Alpaca paper API — keys from env (same as rest of project)
_ALPACA_KEY    = os.getenv("ALPACA_API_KEY", "")
_ALPACA_SECRET = os.getenv("ALPACA_SECRET_KEY", "")
_ALPACA_BASE   = "https://paper-api.alpaca.markets"

# Only major liquid exchanges; OTC/pink sheets excluded
_LIQUID_EXCHANGES = {"NYSE", "NASDAQ", "ARCA", "BATS", "NYSEARCA"}


def _fetch_alpaca_assets() -> list[str]:
    """Fetch all active, tradeable US equity symbols from Alpaca.
    Returns symbols on liquid exchanges only, no special chars (warrants/preferred).
    """
    if not _ALPACA_KEY:
        return []
    symbols: list[str] = []
    page_token: str | None = None
    try:
        while True:
            url = f"{_ALPACA_BASE}/v2/assets?status=active&asset_class=us_equity&limit=1000"
            if page_token:
                url += f"&page_token={page_token}"
            req = urllib.request.Request(
                url,
                headers={
                    "APCA-API-KEY-ID": _ALPACA_KEY,
                    "APCA-API-SECRET-KEY": _ALPACA_SECRET,
                },
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                batch = json.loads(r.read())
            if not batch:
                break
            for asset in batch:
                sym = asset.get("symbol", "")
                # Skip OTC, non-tradable, and symbols with special chars
                if not asset.get("tradable"):
                    continue
                if asset.get("exchange", "") not in _LIQUID_EXCHANGES:
                    continue
                if not sym or not sym.isalpha() or len(sym) > 5:
                    continue
                symbols.append(sym)
            # Alpaca uses next_page_token in headers
            if len(batch) < 1000:
                break
            page_token = None  # Alpaca v2 assets doesn't paginate via token
            break  # single page covers all
    except Exception as e:
        logger.warning("Alpaca asset fetch failed: %s", e)
    return symbols


def get_full_universe() -> list[str]:
    """Full market universe: Alpaca active NYSE/NASDAQ symbols (24h cache).
    Falls back to watchlist if Alpaca is unavailable.
    """
    global _universe_cache, _universe_cache_ts
    with _cache_lock:
        if _universe_cache and time.time() - _universe_cache_ts < _UNIVERSE_TTL:
            return _universe_cache

    syms = _fetch_alpaca_assets()
    if not syms:
        logger.warning("Alpaca universe empty — falling back to watchlist + DB symbols")
        syms = get_watchlist_universe()

    with _cache_lock:
        _universe_cache = syms
        _universe_cache_ts = time.time()
    logger.info("Full universe loaded: %d symbols", len(syms))
    return syms


def get_watchlist_universe() -> list[str]:
    """Watchlist + DB-tracked symbols (original narrow universe)."""
    syms = set(WATCHLIST)
    try:
        db = sqlite3.connect(DB_PATH, timeout=5)
        rows = db.execute(
            "SELECT DISTINCT symbol FROM trades WHERE symbol IS NOT NULL"
        ).fetchall()
        db.close()
        for r in rows:
            syms.add(r[0])
    except Exception:
        pass
    return sorted(syms)


def get_universe() -> list[str]:
    """Backwards-compat alias → watchlist universe."""
    return get_watchlist_universe()


def _parse_yf_row(today, prev, df) -> dict:
    """Extract price-level fields from a yfinance DataFrame row."""
    def _f(val):
        try:
            v = val.iloc[0] if hasattr(val, "iloc") else val
            return float(v) if v == v else 0.0  # NaN guard
        except Exception:
            return 0.0

    close = _f(today["Close"])
    prev_close = _f(prev["Close"])
    vol = _f(today.get("Volume", 0))
    avg_vol = float(df["Volume"].mean()) if len(df) >= 2 else vol
    return {
        "price":      close,
        "prev_close": prev_close,
        "change":     ((close - prev_close) / prev_close * 100) if prev_close else 0.0,
        "volume":     vol,
        "avg_volume": avg_vol,
        "rvol":       vol / avg_vol if avg_vol > 0 else 1.0,
        "high":       _f(today.get("High", close)),
        "low":        _f(today.get("Low", close)),
        "open":       _f(today.get("Open", close)),
        "rsi": None, "sma20": None, "sma50": None, "sma200": None,
        "ema8": None, "ema21": None,
        "above_sma20": False, "above_sma50": False, "above_sma200": False,
        "uptrend": False,
    }


def _get_prices(symbols: list[str]) -> dict[str, dict]:
    """Batch-fetch prices via yfinance with 5-min cache. Handles any size list in chunks."""
    now = time.time()
    missing: list[str] = []
    result: dict[str, dict] = {}

    with _cache_lock:
        for s in symbols:
            if s in _price_cache and now - _price_cache_ts.get(s, 0) < _PRICE_TTL:
                result[s] = _price_cache[s]
            else:
                missing.append(s)

    if not missing:
        return result

    import yfinance as yf

    # Chunk into 200-symbol batches to avoid yfinance request limits
    CHUNK = 200
    for i in range(0, len(missing), CHUNK):
        chunk = missing[i: i + CHUNK]
        try:
            tickers = yf.download(
                chunk, period="2d", interval="1d",
                group_by="ticker", auto_adjust=True, progress=False, threads=True,
            )
            for s in chunk:
                try:
                    df = tickers if len(chunk) == 1 else tickers.get(s)
                    if df is None or df.empty or len(df) < 1:
                        continue
                    today = df.iloc[-1]
                    prev  = df.iloc[-2] if len(df) > 1 else today
                    data  = _parse_yf_row(today, prev, df)
                    with _cache_lock:
                        _price_cache[s] = data
                        _price_cache_ts[s] = now
                    result[s] = data
                except Exception as e:
                    logger.debug("price parse %s: %s", s, e)
        except Exception as e:
            logger.debug("yfinance chunk %d: %s", i, e)

    return result


def _get_fundamentals(symbol: str) -> dict:
    """Fetch fundamental data with 1-hour cache."""
    now = time.time()
    with _cache_lock:
        if symbol in _fund_cache and now - _fund_cache_ts.get(symbol, 0) < _FUND_TTL:
            return _fund_cache[symbol]
    data: dict[str, Any] = {
        "market_cap": None, "pe": None, "forward_pe": None,
        "eps": None, "div_yield": None, "roe": None,
        "beta": None, "sector": None, "industry": None,
        "short_float": None, "insider_own": None, "inst_own": None,
        "profit_margin": None, "debt_equity": None,
    }
    try:
        import yfinance as yf
        info = yf.Ticker(symbol).info or {}
        data["market_cap"]    = info.get("marketCap")
        data["pe"]            = info.get("trailingPE")
        data["forward_pe"]    = info.get("forwardPE")
        data["eps"]           = info.get("trailingEps")
        data["div_yield"]     = (info.get("dividendYield") or 0) * 100
        data["roe"]           = (info.get("returnOnEquity") or 0) * 100
        data["beta"]          = info.get("beta")
        data["sector"]        = info.get("sector", "")
        data["industry"]      = info.get("industry", "")
        data["short_float"]   = (info.get("shortPercentOfFloat") or 0) * 100
        data["profit_margin"] = (info.get("profitMargins") or 0) * 100
        data["debt_equity"]   = info.get("debtToEquity")
    except Exception as e:
        logger.debug("fundamentals %s: %s", symbol, e)
    with _cache_lock:
        _fund_cache[symbol] = data
        _fund_cache_ts[symbol] = now
    return data


def _get_fleet_data(symbol: str) -> dict:
    """Pull fleet consensus from trader.db (last 4h trades)."""
    out = {"fleet_bull": 0, "fleet_bear": 0, "fleet_hold": 0, "fleet_total": 0,
           "has_active_signal": False, "backtest_winrate": None}
    try:
        db = sqlite3.connect(DB_PATH, timeout=5)
        db.row_factory = sqlite3.Row
        rows = db.execute("""
            SELECT action, confidence FROM trades
            WHERE symbol=? AND action IN ('BUY','BUY_CALL','SELL','BUY_PUT','SHORT','HOLD')
              AND executed_at >= datetime('now','-4 hours')
            ORDER BY executed_at DESC LIMIT 30
        """, (symbol,)).fetchall()
        total = len(rows)
        if total:
            buys = sum(1 for r in rows if r["action"] in ("BUY","BUY_CALL"))
            bear = sum(1 for r in rows if r["action"] in ("BUY_PUT","SHORT"))
            hold = sum(1 for r in rows if r["action"] == "HOLD")
            out["fleet_bull"]  = round(buys / total * 100)
            out["fleet_bear"]  = round(bear / total * 100)
            out["fleet_hold"]  = round(hold / total * 100)
            out["fleet_total"] = total
        # Backtest win rate
        hist = db.execute("""
            SELECT realized_pnl FROM trades
            WHERE symbol=? AND action='SELL' AND realized_pnl IS NOT NULL
            ORDER BY executed_at DESC LIMIT 20
        """, (symbol,)).fetchall()
        if len(hist) >= 3:
            wins = sum(1 for r in hist if (r[0] or 0) > 0)
            out["backtest_winrate"] = round(wins / len(hist) * 100)
        # Active signal
        try:
            sig = db.execute("""
                SELECT id FROM signals WHERE symbol=?
                  AND created_at >= datetime('now','-2 hours')
                LIMIT 1
            """, (symbol,)).fetchone()
            out["has_active_signal"] = sig is not None
        except Exception:
            pass
        db.close()
    except Exception as e:
        logger.debug("fleet_data %s: %s", symbol, e)
    return out


def _get_congress_flag(symbol: str) -> bool:
    """Check if Congress has traded this symbol recently."""
    try:
        from engine.congress_scraper import get_congress_trades_for_ticker
        trades = get_congress_trades_for_ticker(symbol)
        return bool(trades)
    except Exception:
        return False


def _get_gex_data(symbol: str) -> dict:
    """Pull GEX regime from autonomous_trader.db."""
    out = {"gex_regime": None, "put_wall": None, "call_wall": None, "gamma_flip": None}
    try:
        db = sqlite3.connect(ATDB_PATH, timeout=5)
        db.row_factory = sqlite3.Row
        row = db.execute("""
            SELECT * FROM gex_levels WHERE symbol=?
            ORDER BY updated_at DESC LIMIT 1
        """, (symbol,)).fetchone()
        db.close()
        if row:
            out["gex_regime"]  = row["regime"] if "regime" in row.keys() else None
            out["put_wall"]    = row["put_wall"] if "put_wall" in row.keys() else None
            out["call_wall"]   = row["call_wall"] if "call_wall" in row.keys() else None
            out["gamma_flip"]  = row["gamma_flip"] if "gamma_flip" in row.keys() else None
    except Exception:
        pass
    return out


def _score_stock(row: dict) -> int:
    """TradeMinds composite score for ranking."""
    s = 0
    if (row.get("change") or 0) > 0: s += 1
    if (row.get("rvol") or 0) > 1.5: s += 1
    rsi = row.get("rsi")
    if rsi is not None:
        if rsi < 30: s += 3
        elif rsi < 40: s += 1
        elif rsi > 70: s -= 2
    if (row.get("fleet_bull") or 0) > 60: s += 2
    if (row.get("fleet_bear") or 0) > 60: s -= 2
    if row.get("has_congress"): s += 2
    if row.get("has_active_signal"): s += 1
    bwr = row.get("backtest_winrate")
    if bwr is not None:
        if bwr > 65: s += 2
        elif bwr < 35: s -= 1
    if row.get("above_sma200"): s += 1
    if row.get("uptrend"): s += 1
    return s


def _passes_filters(row: dict, f: dict) -> bool:
    """Return True if stock passes all active filters."""
    # Market cap
    mc = row.get("market_cap") or 0
    mktcap = f.get("mktcap", "")
    if mktcap:
        if mktcap == "mega"  and mc < 200e9: return False
        if mktcap == "large" and (mc < 10e9  or mc > 200e9): return False
        if mktcap == "mid"   and (mc < 2e9   or mc > 10e9):  return False
        if mktcap == "small" and mc > 2e9: return False
    # P/E
    pe = row.get("pe")
    if f.get("pe_max") is not None and (pe is None or pe > f["pe_max"]): return False
    if f.get("pe_min") is not None and (pe is None or pe < f["pe_min"]): return False
    # Dividend
    if f.get("div_yield_min") is not None:
        dy = row.get("div_yield") or 0
        if dy < f["div_yield_min"]: return False
    # ROE
    if f.get("roe_min") is not None:
        roe = row.get("roe") or 0
        if roe < f["roe_min"]: return False
    # Sector
    if f.get("sector") and row.get("sector", "") != f["sector"]: return False
    # Beta
    beta = row.get("beta")
    if f.get("beta_max") is not None and (beta is None or beta > f["beta_max"]): return False
    if f.get("beta_min") is not None and (beta is None or beta < f["beta_min"]): return False
    # RSI
    rsi = row.get("rsi")
    if f.get("rsi_max") is not None and (rsi is None or rsi > f["rsi_max"]): return False
    if f.get("rsi_min") is not None and (rsi is None or rsi < f["rsi_min"]): return False
    # MA filters
    if f.get("above_sma20")  and not row.get("above_sma20"):  return False
    if f.get("above_sma50")  and not row.get("above_sma50"):  return False
    if f.get("above_sma200") and not row.get("above_sma200"): return False
    if f.get("uptrend")      and not row.get("uptrend"):      return False
    if f.get("downtrend")    and row.get("uptrend"):          return False
    # Volume / change
    rvol = row.get("rvol") or 0
    if f.get("rvol_min") is not None and rvol < f["rvol_min"]: return False
    chg = row.get("change") or 0
    if f.get("change_min") is not None and chg < f["change_min"]: return False
    if f.get("change_max") is not None and chg > f["change_max"]: return False
    # Gap
    if f.get("gap_up_min") is not None and chg < f["gap_up_min"]: return False
    if f.get("gap_down_max") is not None and chg > f["gap_down_max"]: return False
    # TradeMinds
    if f.get("fleet_bull_min") is not None:
        if (row.get("fleet_bull") or 0) < f["fleet_bull_min"]: return False
    if f.get("fleet_bear_min") is not None:
        if (row.get("fleet_bear") or 0) < f["fleet_bear_min"]: return False
    if f.get("has_congress") and not row.get("has_congress"): return False
    if f.get("has_active_signal") and not row.get("has_active_signal"): return False
    if f.get("backtest_winrate_min") is not None:
        bwr = row.get("backtest_winrate")
        if bwr is None or bwr < f["backtest_winrate_min"]: return False
    return True


def run_screener(filters: dict | None = None) -> list[dict]:
    """Two-pass full-market screener. Results cached 5 minutes.

    Pass 1 — cheap bulk yfinance price download for entire universe:
      Filters: price >= $2, volume >= 100K, basic change/rvol/rsi_max/rsi_min.
      Selects top 200 survivors by dollar-volume (price * volume).

    Pass 2 — fundamentals + TradeMinds signals for those ≤200 survivors:
      Applies all remaining filters (mktcap, PE, sector, fleet, congress, GEX).

    Set filters['watchlist_only']=True to bypass full scan (fast, 17 stocks).
    """
    f = filters or {}
    limit    = int(f.get("limit", 50))
    sort_by  = f.get("sort_by", "score")
    sort_dir = f.get("sort_dir", "desc")

    # ── Cache check (5-min TTL keyed on filter fingerprint) ──────────────────
    cache_key = json.dumps(f, sort_keys=True, default=str)
    with _cache_lock:
        cached = _screener_cache.get(cache_key)
        if cached and time.time() - cached["ts"] < _SCREENER_TTL:
            logger.debug("screener cache hit (%d results)", len(cached["results"]))
            return cached["results"]

    # ── Choose universe ───────────────────────────────────────────────────────
    if f.get("watchlist_only"):
        symbols = get_watchlist_universe()
    else:
        symbols = get_full_universe()
    logger.info("screener: scanning %d symbols", len(symbols))

    # ── PASS 1: bulk price fetch + fast pre-filters ───────────────────────────
    prices = _get_prices(symbols)

    pass1: list[tuple[float, str, dict]] = []  # (dollar_volume, sym, price_data)
    for sym, pd in prices.items():
        price = pd.get("price") or 0.0
        vol   = pd.get("volume") or 0.0
        chg   = pd.get("change") or 0.0
        rvol  = pd.get("rvol") or 0.0

        # Hard liquidity floor — excludes penny stocks and dead tickers
        if price < 2.0 or vol < 100_000:
            continue

        # Fast filter checks that only need price data
        if f.get("change_min") is not None and chg < f["change_min"]:
            continue
        if f.get("change_max") is not None and chg > f["change_max"]:
            continue
        if f.get("rvol_min") is not None and rvol < f["rvol_min"]:
            continue
        if f.get("gap_up_min") is not None and chg < f["gap_up_min"]:
            continue
        if f.get("gap_down_max") is not None and chg > f["gap_down_max"]:
            continue

        dollar_vol = price * vol
        pass1.append((dollar_vol, sym, pd))

    # Cap at top 200 by dollar-volume to keep pass 2 fast
    pass1.sort(reverse=True)
    survivors = pass1[:200]
    logger.info("screener pass1: %d → %d survivors", len(pass1), len(survivors))

    # ── PASS 2: fundamentals + TradeMinds enrichment + full filters ───────────
    needs_fund = any(f.get(k) is not None for k in (
        "mktcap", "pe_max", "pe_min", "div_yield_min", "roe_min",
        "sector", "beta_max", "beta_min",
    ))

    results: list[dict] = []
    for _, sym, price_data in survivors:
        try:
            row: dict[str, Any] = {"symbol": sym, **price_data}

            # Fleet (fast DB query — only for watchlist symbols to avoid per-row overhead)
            if sym in set(WATCHLIST) or f.get("fleet_bull_min") or f.get("fleet_bear_min") \
                    or f.get("has_active_signal") or f.get("backtest_winrate_min"):
                row.update(_get_fleet_data(sym))
            else:
                row.update({"fleet_bull": 0, "fleet_bear": 0, "fleet_hold": 0,
                             "fleet_total": 0, "has_active_signal": False,
                             "backtest_winrate": None})

            # Fundamentals
            if needs_fund:
                row.update(_get_fundamentals(sym))

            # Congress
            if f.get("has_congress"):
                row["has_congress"] = _get_congress_flag(sym)
            else:
                row["has_congress"] = False

            # GEX
            if f.get("gex_regime"):
                gex = _get_gex_data(sym)
                row.update(gex)
                if row.get("gex_regime") != f["gex_regime"]:
                    continue

            row["score"] = _score_stock(row)

            if _passes_filters(row, f):
                results.append(row)
        except Exception as e:
            logger.debug("screener pass2 %s: %s", sym, e)

    # ── Sort + cache ──────────────────────────────────────────────────────────
    reverse = sort_dir != "asc"
    results.sort(key=lambda r: (r.get(sort_by) or 0), reverse=reverse)
    final = results[:limit]

    with _cache_lock:
        _screener_cache[cache_key] = {"ts": time.time(), "results": final}

    logger.info("screener complete: %d results from %d symbols", len(final), len(symbols))
    return final


PRESETS = {
    "oversold_bounce": {
        "rsi_max": 30,
        "rvol_min": 1.5,
        "sort_by": "rsi",
        "sort_dir": "asc",
        "description": "RSI Oversold + Volume Surge",
    },
    "momentum_break": {
        "above_sma20": True,
        "rvol_min": 2.0,
        "change_min": 1.0,
        "sort_by": "rvol",
        "description": "Price above SMA20 + Gap Up + High Volume",
    },
    "congress_insider": {
        "has_congress": True,
        "sort_by": "score",
        "description": "Congressional buying activity",
    },
    "dte0_setup": {
        "mktcap": "mega",
        "rvol_min": 1.5,
        "sort_by": "rvol",
        "description": "High liquidity + volume — ideal 0DTE",
    },
    "dividend_value": {
        "pe_max": 20,
        "div_yield_min": 2.0,
        "sort_by": "div_yield_min",
        "sort_dir": "desc",
        "description": "P/E<20 + Dividend Yield>2%",
    },
    "worf_arsenal": {
        "fleet_bear_min": 60,
        "change_max": -1.0,
        "sort_by": "fleet_bear",
        "sort_dir": "desc",
        "description": "Fleet bearish + downward momentum",
    },
    "convergence": {
        "fleet_bull_min": 60,
        "has_active_signal": True,
        "sort_by": "score",
        "description": "Fleet bullish + active signal alignment",
    },
}
