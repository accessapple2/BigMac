"""OpenBB Data Layer — centralized data fetching via OpenBB Platform.

Provides:
1. Real-time prices for watchlist stocks (via yfinance direct — OpenBB yfinance bridge is broken)
2. Fundamental data (P/E, revenue growth, EPS, margins) per stock
3. Insider trading activity (via SEC)
4. SEC filings and earnings dates
5. Economic calendar (CPI, unemployment, GDP, interest rates, FOMC)
6. Options chains with full Greeks for DayBlade
"""
from __future__ import annotations
import threading
import time
from datetime import datetime
from engine.market_data import _is_yf_limited, _set_yf_limited
from rich.console import Console

console = Console()

# ── Thread-safe caches ──────────────────────────────────────────────

_cache: dict = {}
_cache_lock = threading.Lock()

_FUNDAMENTALS_TTL = 3600      # 1 hour
_INSIDER_TTL = 3600            # 1 hour
_FILINGS_TTL = 3600            # 1 hour
_ECON_TTL = 7200               # 2 hours — macro data updates slowly
_OPTIONS_CHAIN_TTL = 300       # 5 minutes


def _get_cache(key: str, ttl: int):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and time.time() - entry["ts"] < ttl:
            return entry["data"]
    return None


def _set_cache(key: str, data):
    with _cache_lock:
        _cache[key] = {"data": data, "ts": time.time()}


def _obb():
    """Lazy import OpenBB to avoid import-time overhead."""
    from openbb import obb
    return obb


# ── 1. Real-Time Prices ────────────────────────────────────────────

def get_stock_price(symbol: str) -> dict:
    """Fetch current stock price. Uses yfinance directly (OpenBB yfinance bridge incompatible)."""
    from engine.market_data import get_stock_price as yf_price
    return yf_price(symbol)


def get_batch_prices(symbols: list) -> dict:
    """Fetch prices for multiple symbols."""
    from engine.market_data import get_stock_price as yf_price
    prices = {}
    for sym in symbols:
        data = yf_price(sym)
        if "error" not in data:
            prices[sym] = data
    return prices


# ── 2. Fundamental Data ────────────────────────────────────────────

def get_fundamentals(symbol: str) -> dict | None:
    """Get fundamental data: P/E, EPS, revenue growth, margins, market cap.
    Uses yfinance directly for reliability.
    """
    cache_key = f"obb_fund_{symbol}"
    cached = _get_cache(cache_key, _FUNDAMENTALS_TTL)
    if cached:
        return cached

    if _is_yf_limited():
        return None
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info or {}

        result = {
            "symbol": symbol,
            "company_name": info.get("shortName", symbol),
            "sector": info.get("sector", "Unknown"),
            "industry": info.get("industry", "Unknown"),
            "market_cap": info.get("marketCap"),
            "pe_trailing": _safe_round(info.get("trailingPE")),
            "pe_forward": _safe_round(info.get("forwardPE")),
            "peg_ratio": _safe_round(info.get("pegRatio")),
            "eps_trailing": _safe_round(info.get("trailingEps")),
            "eps_forward": _safe_round(info.get("forwardEps")),
            "revenue_growth": _safe_pct(info.get("revenueGrowth")),
            "earnings_growth": _safe_pct(info.get("earningsGrowth")),
            "gross_margin": _safe_pct(info.get("grossMargins")),
            "operating_margin": _safe_pct(info.get("operatingMargins")),
            "profit_margin": _safe_pct(info.get("profitMargins")),
            "roe": _safe_pct(info.get("returnOnEquity")),
            "roa": _safe_pct(info.get("returnOnAssets")),
            "debt_to_equity": _safe_round(info.get("debtToEquity")),
            "current_ratio": _safe_round(info.get("currentRatio")),
            "free_cash_flow": info.get("freeCashflow"),
            "dividend_yield": _safe_pct(info.get("dividendYield")),
            "beta": _safe_round(info.get("beta")),
            "52w_high": _safe_round(info.get("fiftyTwoWeekHigh")),
            "52w_low": _safe_round(info.get("fiftyTwoWeekLow")),
            "price_to_book": _safe_round(info.get("priceToBook")),
            "insider_pct": _safe_pct(info.get("heldPercentInsiders")),
            "institutional_pct": _safe_pct(info.get("heldPercentInstitutions")),
            "updated": datetime.now().isoformat(),
        }

        _set_cache(cache_key, result)
        return result

    except Exception as e:
        err = str(e)
        if "Too Many Requests" in err or "Rate" in err:
            _set_yf_limited()
        console.log(f"[red]OpenBB fundamentals error for {symbol}: {e}")
        return None


def get_all_fundamentals(symbols: list = None) -> list:
    """Get fundamentals for all watchlist symbols."""
    if symbols is None:
        from config import WATCH_STOCKS
        symbols = WATCH_STOCKS

    results = []
    for sym in symbols:
        data = get_fundamentals(sym)
        if data:
            results.append(data)
    return results


# ── 3. Insider Trading Activity (SEC) ──────────────────────────────

def get_insider_trading(symbol: str) -> list:
    """Fetch recent insider transactions from SEC via OpenBB."""
    cache_key = f"obb_insider_{symbol}"
    cached = _get_cache(cache_key, _INSIDER_TTL)
    if cached is not None:
        return cached

    for attempt in range(2):
        try:
            obb = _obb()
            r = obb.equity.ownership.insider_trading(symbol=symbol, provider="sec")
            df = r.to_df()

            if df.empty:
                _set_cache(cache_key, [])
                return []

            transactions = []
            for _, row in df.head(20).iterrows():
                txn_type = str(row.get("transaction_type", ""))
                is_buy = "purchase" in txn_type.lower() or "acquisition" in txn_type.lower()
                is_sale = "sale" in txn_type.lower() or "disposition" in txn_type.lower()

                transactions.append({
                    "owner": str(row.get("owner_name", "Unknown")),
                    "type": "BUY" if is_buy else "SELL" if is_sale else "OTHER",
                    "transaction_type": txn_type[:60],
                    "shares": _safe_round(row.get("securities_transacted")),
                    "price": _safe_round(row.get("transaction_price")),
                    "date": str(row.get("transaction_date", ""))[:10],
                    "filing_date": str(row.get("filing_date", ""))[:10],
                    "is_director": bool(row.get("director", False)),
                    "is_officer": bool(row.get("officer", False)),
                    "is_10pct_owner": bool(row.get("ten_percent_owner", False)),
                    "security_type": str(row.get("security_type", ""))[:40],
                })

            _set_cache(cache_key, transactions)
            return transactions

        except UnboundLocalError:
            # OpenBB SEC provider has an internal 'conn' bug — retry once
            if attempt == 0:
                continue
            _set_cache(cache_key, [])
            return []
        except Exception as e:
            console.log(f"[dim]Insider trading fetch skip {symbol}: {e}")
            _set_cache(cache_key, [])
            return []


def get_insider_summary(symbol: str) -> dict:
    """Summarize insider activity: net buys vs sells, notable transactions."""
    transactions = get_insider_trading(symbol)
    if not transactions:
        return {"symbol": symbol, "buys": 0, "sells": 0, "net": "neutral", "transactions": []}

    buys = sum(1 for t in transactions if t["type"] == "BUY")
    sells = sum(1 for t in transactions if t["type"] == "SELL")
    buy_value = sum((t["shares"] or 0) * (t["price"] or 0) for t in transactions if t["type"] == "BUY")
    sell_value = sum((t["shares"] or 0) * (t["price"] or 0) for t in transactions if t["type"] == "SELL")

    if buys > sells * 2:
        net = "strong_buying"
    elif buys > sells:
        net = "buying"
    elif sells > buys * 2:
        net = "strong_selling"
    elif sells > buys:
        net = "selling"
    else:
        net = "neutral"

    # Notable: officers/directors with large transactions
    notable = [t for t in transactions if (t["is_director"] or t["is_officer"]) and t["type"] in ("BUY", "SELL")]

    return {
        "symbol": symbol,
        "buys": buys,
        "sells": sells,
        "buy_value": round(buy_value, 2),
        "sell_value": round(sell_value, 2),
        "net": net,
        "notable": notable[:5],
        "transactions": transactions[:10],
    }


# ── 4. SEC Filings & Earnings Dates ────────────────────────────────

def get_sec_filings(symbol: str) -> list:
    """Fetch recent SEC filings via OpenBB SEC provider."""
    cache_key = f"obb_filings_{symbol}"
    cached = _get_cache(cache_key, _FILINGS_TTL)
    if cached is not None:
        return cached

    try:
        obb = _obb()
        r = obb.equity.fundamental.filings(symbol=symbol, provider="sec", type="10-K,10-Q,8-K")
        df = r.to_df()

        if df.empty:
            _set_cache(cache_key, [])
            return []

        filings = []
        for _, row in df.head(15).iterrows():
            filings.append({
                "type": str(row.get("type", "")),
                "date": str(row.get("filing_date", str(row.get("date", ""))))[:10],
                "url": str(row.get("link", row.get("url", ""))),
                "description": str(row.get("title", row.get("description", "")))[:100],
            })

        _set_cache(cache_key, filings)
        return filings

    except Exception as e:
        console.log(f"[dim]SEC filings skip {symbol}: {e}")
        _set_cache(cache_key, [])
        return []


def get_earnings_dates(symbols: list) -> list:
    """Get upcoming earnings dates using yfinance (most reliable free source)."""
    from engine.earnings_calendar import fetch_earnings
    return fetch_earnings(symbols)


# ── 5. Economic Calendar ───────────────────────────────────────────

def get_economic_calendar() -> dict:
    """Fetch macro economic data: CPI, unemployment, interest rates, GDP, FOMC.
    Uses OECD provider (no API key required).
    """
    cache_key = "obb_econ_calendar"
    cached = _get_cache(cache_key, _ECON_TTL)
    if cached:
        return cached

    result = {
        "cpi": None,
        "unemployment": None,
        "interest_rate": None,
        "gdp": None,
        "fomc": [],
        "updated": datetime.now().isoformat(),
    }

    obb = _obb()

    # CPI (inflation)
    try:
        r = obb.economy.cpi(provider="oecd", country="united_states")
        df = r.to_df()
        if not df.empty:
            latest = df.tail(1).iloc[0]
            result["cpi"] = {
                "value": round(float(latest["value"]) * 100, 2),
                "date": str(df.index[-1])[:10],
                "trend": _trend_from_series(df["value"].tail(6)),
            }
    except Exception as e:
        console.log(f"[dim]CPI fetch error: {e}")

    # Unemployment
    try:
        r = obb.economy.unemployment(provider="oecd", country="united_states")
        df = r.to_df()
        if not df.empty:
            latest = df.tail(1).iloc[0]
            result["unemployment"] = {
                "value": round(float(latest["value"]) * 100, 1),
                "date": str(df.index[-1])[:10],
                "trend": _trend_from_series(df["value"].tail(6)),
            }
    except Exception as e:
        console.log(f"[dim]Unemployment fetch error: {e}")

    # Interest Rates
    try:
        r = obb.economy.interest_rates(provider="oecd", country="united_states")
        df = r.to_df()
        if not df.empty:
            latest = df.tail(1).iloc[0]
            result["interest_rate"] = {
                "value": round(float(latest["value"]) * 100, 2),
                "date": str(df.index[-1])[:10],
                "trend": _trend_from_series(df["value"].tail(6)),
            }
    except Exception as e:
        console.log(f"[dim]Interest rate fetch error: {e}")

    # GDP
    try:
        r = obb.economy.gdp.nominal(provider="oecd", country="united_states")
        df = r.to_df()
        if not df.empty:
            vals = df["value"].tail(4)
            if len(vals) >= 2:
                growth = (float(vals.iloc[-1]) - float(vals.iloc[-2])) / float(vals.iloc[-2]) * 100
            else:
                growth = 0
            result["gdp"] = {
                "value_billions": round(float(vals.iloc[-1]) / 1e9, 1),
                "qoq_growth": round(growth, 2),
                "date": str(df.index[-1])[:10],
            }
    except Exception as e:
        console.log(f"[dim]GDP fetch error: {e}")

    # FOMC Documents (meeting dates/statements)
    try:
        r = obb.economy.fomc_documents()
        if isinstance(r, list):
            fomc_items = []
            for item in r[:10]:
                if hasattr(item, '__dict__'):
                    d = item.__dict__ if not hasattr(item, 'model_dump') else item.model_dump()
                elif isinstance(item, dict):
                    d = item
                else:
                    continue
                fomc_items.append({
                    "date": str(d.get("date", d.get("meeting_date", "")))[:10],
                    "title": str(d.get("title", d.get("type", "")))[:100],
                    "url": str(d.get("url", d.get("link", ""))),
                })
            result["fomc"] = fomc_items
    except Exception as e:
        console.log(f"[dim]FOMC fetch error: {e}")

    _set_cache(cache_key, result)
    return result


def build_economic_prompt_section() -> str:
    """Build economic calendar section for AI prompt injection."""
    econ = get_economic_calendar()
    if not econ:
        return ""

    lines = ["\n--- Economic Environment ---"]

    cpi = econ.get("cpi")
    if cpi:
        lines.append(f"  CPI (Inflation): {cpi['value']}% YoY [{cpi['trend']}] (as of {cpi['date']})")

    unemp = econ.get("unemployment")
    if unemp:
        lines.append(f"  Unemployment: {unemp['value']}% [{unemp['trend']}] (as of {unemp['date']})")

    rate = econ.get("interest_rate")
    if rate:
        lines.append(f"  Interest Rate: {rate['value']}% [{rate['trend']}] (as of {rate['date']})")

    gdp = econ.get("gdp")
    if gdp:
        lines.append(f"  GDP: ${gdp['value_billions']:.0f}B (QoQ: {gdp['qoq_growth']:+.2f}%)")

    fomc = econ.get("fomc", [])
    if fomc:
        lines.append(f"  Recent FOMC: {fomc[0].get('title', 'N/A')} ({fomc[0].get('date', 'N/A')})")

    return "\n".join(lines)


def build_fundamentals_prompt_section(symbol: str) -> str:
    """Build enriched fundamentals section for AI prompt injection."""
    data = get_fundamentals(symbol)
    if not data:
        return ""

    lines = [f"\n--- Fundamentals: {symbol} ({data.get('company_name', symbol)}) ---"]

    pe = data.get("pe_trailing")
    pe_fwd = data.get("pe_forward")
    if pe:
        lines.append(f"  P/E (TTM): {pe}" + (f" | Forward P/E: {pe_fwd}" if pe_fwd else ""))

    eps = data.get("eps_trailing")
    eps_fwd = data.get("eps_forward")
    if eps:
        lines.append(f"  EPS (TTM): ${eps}" + (f" | Forward EPS: ${eps_fwd}" if eps_fwd else ""))

    rev = data.get("revenue_growth")
    earn = data.get("earnings_growth")
    if rev is not None:
        lines.append(f"  Revenue Growth: {rev:+.1f}%" + (f" | Earnings Growth: {earn:+.1f}%" if earn is not None else ""))

    gm = data.get("gross_margin")
    om = data.get("operating_margin")
    pm = data.get("profit_margin")
    margin_parts = []
    if gm is not None:
        margin_parts.append(f"Gross: {gm:.1f}%")
    if om is not None:
        margin_parts.append(f"Operating: {om:.1f}%")
    if pm is not None:
        margin_parts.append(f"Net: {pm:.1f}%")
    if margin_parts:
        lines.append(f"  Margins: {' | '.join(margin_parts)}")

    roe = data.get("roe")
    if roe is not None:
        lines.append(f"  ROE: {roe:.1f}%")

    de = data.get("debt_to_equity")
    if de is not None:
        health = "LOW" if de < 50 else "MODERATE" if de < 100 else "HIGH"
        lines.append(f"  Debt/Equity: {de:.1f} [{health}]")

    # Insider summary
    insider = get_insider_summary(symbol)
    if insider and insider["net"] != "neutral":
        net_label = insider["net"].upper().replace("_", " ")
        lines.append(f"  Insider Activity: {net_label} ({insider['buys']} buys / {insider['sells']} sells)")

    return "\n".join(lines)


# ── 6. Options Chains with Greeks ──────────────────────────────────

def get_options_chain(symbol: str, expiry: str = None) -> dict | None:
    """Fetch options chain with full Greeks. Uses yfinance directly."""
    cache_key = f"obb_opts_{symbol}_{expiry or 'nearest'}"
    cached = _get_cache(cache_key, _OPTIONS_CHAIN_TTL)
    if cached:
        return cached

    if _is_yf_limited():
        return None
    try:
        ticker = yf.Ticker(symbol)
        expirations = ticker.options
        if not expirations:
            return None

        # Pick target expiry
        target_exp = expiry or expirations[0]
        if target_exp not in expirations:
            # Find closest
            today = datetime.now().strftime("%Y-%m-%d")
            target_exp = expirations[0]
            for exp in expirations:
                if exp >= today:
                    target_exp = exp
                    break

        chain = ticker.option_chain(target_exp)

        def _chain_to_list(df, opt_type):
            rows = []
            for _, row in df.iterrows():
                rows.append({
                    "strike": _safe_round(row.get("strike")),
                    "last_price": _safe_round(row.get("lastPrice")),
                    "bid": _safe_round(row.get("bid")),
                    "ask": _safe_round(row.get("ask")),
                    "volume": int(row.get("volume", 0)) if _notna(row.get("volume")) else 0,
                    "open_interest": int(row.get("openInterest", 0)) if _notna(row.get("openInterest")) else 0,
                    "iv": _safe_round(row.get("impliedVolatility")),
                    "delta": _safe_round(row.get("delta")),
                    "gamma": _safe_round(row.get("gamma")),
                    "theta": _safe_round(row.get("theta")),
                    "vega": _safe_round(row.get("vega")),
                    "type": opt_type,
                })
            return rows

        result = {
            "symbol": symbol,
            "expiry": target_exp,
            "expirations": list(expirations),
            "calls": _chain_to_list(chain.calls, "call"),
            "puts": _chain_to_list(chain.puts, "put"),
        }

        _set_cache(cache_key, result)
        return result

    except Exception as e:
        err = str(e)
        if "Too Many Requests" in err or "Rate" in err:
            _set_yf_limited()
        console.log(f"[red]Options chain error for {symbol}: {e}")
        return None


# ── Helpers ─────────────────────────────────────────────────────────

def _safe_round(val, decimals=2) -> float | None:
    if val is None:
        return None
    try:
        import pandas as pd
        if pd.isna(val):
            return None
    except (ImportError, TypeError):
        pass
    try:
        return round(float(val), decimals)
    except (ValueError, TypeError):
        return None


def _safe_pct(val) -> float | None:
    """Convert ratio to percentage (0.15 -> 15.0)."""
    if val is None:
        return None
    try:
        import pandas as pd
        if pd.isna(val):
            return None
    except (ImportError, TypeError):
        pass
    try:
        return round(float(val) * 100, 2)
    except (ValueError, TypeError):
        return None


def _notna(val) -> bool:
    if val is None:
        return False
    try:
        import pandas as pd
        return pd.notna(val)
    except (ImportError, TypeError):
        return val is not None


def _trend_from_series(series) -> str:
    """Determine trend from last few data points."""
    try:
        vals = [float(v) for v in series if _notna(v)]
        if len(vals) < 2:
            return "STABLE"
        recent = vals[-3:] if len(vals) >= 3 else vals
        if all(recent[i] <= recent[i+1] for i in range(len(recent)-1)):
            return "RISING"
        elif all(recent[i] >= recent[i+1] for i in range(len(recent)-1)):
            return "FALLING"
        return "MIXED"
    except Exception:
        return "STABLE"
