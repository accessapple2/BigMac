"""Alpha Vantage technicals/fundamentals + FRED economic data."""
from __future__ import annotations
import requests
import json
import time
from pathlib import Path
from datetime import datetime, timedelta
from rich.console import Console

import config

console = Console()

_AV_BASE = "https://www.alphavantage.co/query"
_FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

# Cache to avoid hammering free-tier APIs
_av_cache = {}
_fred_cache = {}
_AV_CACHE_TTL = 3600  # 1 hour for fundamentals
_FRED_CACHE_TTL = 86400  # 24 hours for macro data
_CREDIT_CACHE_TTL = 3600  # 1 hour for credit spread monitoring


def _av_get(function: str, params: dict = None) -> dict | None:
    """Make authenticated Alpha Vantage API call."""
    key = config.ALPHA_VANTAGE_KEY
    if not key:
        return None
    if params is None:
        params = {}
    params["function"] = function
    params["apikey"] = key

    cache_key = f"{function}:{json.dumps(params, sort_keys=True)}"
    if cache_key in _av_cache:
        entry = _av_cache[cache_key]
        if time.time() - entry["ts"] < _AV_CACHE_TTL:
            return entry["data"]

    try:
        r = requests.get(_AV_BASE, params=params, timeout=15)
        if r.status_code != 200:
            return None
        data = r.json()
        if "Error Message" in data or "Note" in data:
            if "Note" in data:
                console.log("[yellow]Alpha Vantage rate limited (5 calls/min free tier)")
            return None
        _av_cache[cache_key] = {"data": data, "ts": time.time()}
        return data
    except Exception as e:
        console.log(f"[red]Alpha Vantage error: {e}")
        return None


# --- Technical Indicators ---

def get_rsi(symbol: str, interval: str = "daily", time_period: int = 14) -> dict | None:
    """Get RSI from Alpha Vantage as cross-check against Yahoo calc."""
    data = _av_get("RSI", {"symbol": symbol.upper(), "interval": interval, "time_period": time_period, "series_type": "close"})
    if not data or "Technical Analysis: RSI" not in data:
        return None
    analysis = data["Technical Analysis: RSI"]
    latest_date = list(analysis.keys())[0]
    return {
        "symbol": symbol.upper(),
        "rsi": round(float(analysis[latest_date]["RSI"]), 2),
        "date": latest_date,
        "source": "alphavantage",
    }


def get_macd(symbol: str, interval: str = "daily") -> dict | None:
    """Get MACD from Alpha Vantage."""
    data = _av_get("MACD", {"symbol": symbol.upper(), "interval": interval, "series_type": "close"})
    if not data or "Technical Analysis: MACD" not in data:
        return None
    analysis = data["Technical Analysis: MACD"]
    latest_date = list(analysis.keys())[0]
    vals = analysis[latest_date]
    return {
        "symbol": symbol.upper(),
        "macd": round(float(vals["MACD"]), 4),
        "signal": round(float(vals["MACD_Signal"]), 4),
        "histogram": round(float(vals["MACD_Hist"]), 4),
        "date": latest_date,
        "source": "alphavantage",
    }


def get_sma(symbol: str, interval: str = "daily", time_period: int = 20) -> dict | None:
    """Get SMA from Alpha Vantage."""
    data = _av_get("SMA", {"symbol": symbol.upper(), "interval": interval, "time_period": time_period, "series_type": "close"})
    if not data or "Technical Analysis: SMA" not in data:
        return None
    analysis = data["Technical Analysis: SMA"]
    latest_date = list(analysis.keys())[0]
    return {
        "symbol": symbol.upper(),
        "sma": round(float(analysis[latest_date]["SMA"]), 2),
        "period": time_period,
        "date": latest_date,
        "source": "alphavantage",
    }


# --- Fundamentals ---

def get_company_overview(symbol: str) -> dict | None:
    """Get company fundamental data: P/E, EPS, market cap, 52-week range, dividend yield."""
    data = _av_get("OVERVIEW", {"symbol": symbol.upper()})
    if not data or "Symbol" not in data:
        return None

    def _safe_float(val):
        try:
            return round(float(val), 2) if val and val != "None" and val != "-" else None
        except (ValueError, TypeError):
            return None

    return {
        "symbol": data.get("Symbol", symbol.upper()),
        "name": data.get("Name", ""),
        "sector": data.get("Sector", ""),
        "industry": data.get("Industry", ""),
        "market_cap": _safe_float(data.get("MarketCapitalization")),
        "pe_ratio": _safe_float(data.get("PERatio")),
        "forward_pe": _safe_float(data.get("ForwardPE")),
        "eps": _safe_float(data.get("EPS")),
        "dividend_yield": _safe_float(data.get("DividendYield")),
        "fifty_two_week_high": _safe_float(data.get("52WeekHigh")),
        "fifty_two_week_low": _safe_float(data.get("52WeekLow")),
        "beta": _safe_float(data.get("Beta")),
        "profit_margin": _safe_float(data.get("ProfitMargin")),
        "revenue_growth_yoy": _safe_float(data.get("QuarterlyRevenueGrowthYOY")),
        "analyst_target": _safe_float(data.get("AnalystTargetPrice")),
        "source": "alphavantage",
    }


def get_earnings_surprises(symbol: str) -> list:
    """Get last 4 quarters of earnings: actual vs estimate."""
    data = _av_get("EARNINGS", {"symbol": symbol.upper()})
    if not data or "quarterlyEarnings" not in data:
        return []

    results = []
    for q in data["quarterlyEarnings"][:4]:
        estimate = q.get("estimatedEPS")
        actual = q.get("reportedEPS")
        try:
            est_f = float(estimate) if estimate and estimate != "None" else None
            act_f = float(actual) if actual and actual != "None" else None
        except (ValueError, TypeError):
            est_f = act_f = None

        surprise_pct = None
        if est_f and act_f and est_f != 0:
            surprise_pct = round((act_f - est_f) / abs(est_f) * 100, 1)

        results.append({
            "symbol": symbol.upper(),
            "fiscal_date": q.get("fiscalDateEnding", ""),
            "reported_date": q.get("reportedDate", ""),
            "estimate": est_f,
            "actual": act_f,
            "surprise_pct": surprise_pct,
            "beat": act_f > est_f if (act_f is not None and est_f is not None) else None,
        })
    return results


def get_quote(symbol: str) -> dict | None:
    """Get real-time quote from Alpha Vantage as fallback price source.
    Note: Free tier is 25 requests/day for GLOBAL_QUOTE.
    """
    data = _av_get("GLOBAL_QUOTE", {"symbol": symbol.upper()})
    if not data or "Global Quote" not in data:
        return None

    q = data["Global Quote"]
    price = q.get("05. price")
    prev_close = q.get("08. previous close")
    if not price:
        return None

    try:
        price_f = round(float(price), 2)
        prev_f = round(float(prev_close), 2) if prev_close else price_f
        change_pct = round((price_f - prev_f) / prev_f * 100, 2) if prev_f else 0
    except (ValueError, TypeError):
        return None

    return {
        "symbol": symbol.upper(),
        "price": price_f,
        "change_pct": change_pct,
        "high": round(float(q.get("03. high", price)), 2),
        "low": round(float(q.get("04. low", price)), 2),
        "volume": int(q.get("06. volume", 0)),
        "timestamp": datetime.now().isoformat(),
        "source": "alphavantage",
    }


# --- FRED Economic Data ---

def _fred_get(series_id: str, limit: int = 1) -> list:
    """Fetch latest observation(s) from FRED API."""
    cache_key = f"fred:{series_id}:{limit}"
    if cache_key in _fred_cache:
        entry = _fred_cache[cache_key]
        if time.time() - entry["ts"] < _FRED_CACHE_TTL:
            return entry["data"]

    fred_key = getattr(config, "FRED_API_KEY", "")
    if not fred_key or fred_key == "DEMO":
        return []
    try:
        r = requests.get(_FRED_BASE, params={
            "series_id": series_id,
            "api_key": fred_key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": limit,
        }, timeout=10)
        if r.status_code != 200:
            return []
        data = r.json().get("observations", [])
        _fred_cache[cache_key] = {"data": data, "ts": time.time()}
        return data
    except Exception as e:
        console.log(f"[red]FRED error: {e}")
        return []


def get_macro_data() -> dict:
    """Get key macro economic indicators from FRED.
    Returns CPI, Fed Funds Rate, Unemployment, 10Y Treasury, GDP growth.
    """
    indicators = {
        "cpi_yoy": {"series": "CPIAUCSL", "label": "CPI YoY"},
        "fed_rate": {"series": "FEDFUNDS", "label": "Fed Funds Rate"},
        "unemployment": {"series": "UNRATE", "label": "Unemployment Rate"},
        "treasury_10y": {"series": "DGS10", "label": "10Y Treasury"},
        "gdp_growth": {"series": "A191RL1Q225SBEA", "label": "GDP Growth (Q/Q)"},
        "baa_spread": {"series": "BAA10Y", "label": "Baa Corp Bond Spread"},
        "ted_spread": {"series": "TEDRATE", "label": "TED Spread"},
        "bank_tightening": {"series": "DRTSCILM", "label": "Bank Tightening Standards"},
        "consumer_sentiment": {"series": "UMCSENT", "label": "Consumer Sentiment"},
        "vix_fred": {"series": "VIXCLS", "label": "VIX (FRED)"},
    }

    result = {}
    for key, info in indicators.items():
        obs = _fred_get(info["series"], limit=2)
        if obs:
            latest = obs[0]
            val = latest.get("value", ".")
            if val != ".":
                try:
                    result[key] = {
                        "value": round(float(val), 2),
                        "date": latest.get("date", ""),
                        "label": info["label"],
                    }
                except (ValueError, TypeError):
                    pass

    return result


def build_macro_context() -> str:
    """Build macro context string for AI model prompts."""
    macro = get_macro_data()
    if not macro:
        return ""

    parts = []
    if "fed_rate" in macro:
        parts.append(f"Fed Funds Rate: {macro['fed_rate']['value']}%")
    if "cpi_yoy" in macro:
        parts.append(f"CPI: {macro['cpi_yoy']['value']}")
    if "unemployment" in macro:
        parts.append(f"Unemployment: {macro['unemployment']['value']}%")
    if "treasury_10y" in macro:
        parts.append(f"10Y Treasury: {macro['treasury_10y']['value']}%")
    if "gdp_growth" in macro:
        parts.append(f"GDP Growth: {macro['gdp_growth']['value']}%")

    if "baa_spread" in macro:
        parts.append(f"Baa Spread: {macro['baa_spread']['value']}%")
    if "ted_spread" in macro:
        parts.append(f"TED Spread: {macro['ted_spread']['value']}%")
    if "bank_tightening" in macro:
        parts.append(f"Bank Tightening: {macro['bank_tightening']['value']}%")
    if "consumer_sentiment" in macro:
        parts.append(f"Consumer Sentiment: {macro['consumer_sentiment']['value']}")
    if "vix_fred" in macro:
        parts.append(f"VIX (FRED): {macro['vix_fred']['value']}")

    return "Macro environment: " + ", ".join(parts) if parts else ""


def get_credit_stress_signal() -> dict | None:
    """Check FRED credit spreads for stress signals.

    Pulls ~2 weeks of BAA10Y (Baa corporate bond spread) and TEDRATE
    (TED spread) to detect rapid widening. Returns a dict with warning
    details if BAA10Y widens 50+ bps in the last 5 trading days, or None.
    """
    cache_key = "credit_stress_signal"
    if cache_key in _fred_cache:
        entry = _fred_cache[cache_key]
        if time.time() - entry["ts"] < _CREDIT_CACHE_TTL:
            return entry["data"]

    result = None
    spreads = {}

    # Pull recent observations for each credit series
    for series_id, label in [
        ("BAA10Y", "Baa Corp Bond Spread"),
        ("TEDRATE", "TED Spread"),
        ("DRTSCILM", "Bank Tightening Standards"),
    ]:
        obs = _fred_get(series_id, limit=10)
        valid = [o for o in obs if o.get("value", ".") != "."]
        if valid:
            try:
                latest_val = float(valid[0]["value"])
                spreads[series_id] = {
                    "latest": latest_val,
                    "date": valid[0].get("date", ""),
                    "label": label,
                }
                # Check week-over-week change (use obs ~5 trading days back)
                if len(valid) >= 5:
                    week_ago_val = float(valid[4]["value"])
                    spreads[series_id]["week_ago"] = week_ago_val
                    spreads[series_id]["change_bps"] = round(
                        (latest_val - week_ago_val) * 100, 1
                    )
            except (ValueError, TypeError):
                pass

    # Trigger on BAA10Y widening 50+ bps in a week
    baa = spreads.get("BAA10Y", {})
    baa_change = baa.get("change_bps", 0)
    ted = spreads.get("TEDRATE", {})
    ted_change = ted.get("change_bps", 0)

    if baa_change >= 50:
        parts = [
            f"Baa corporate bond spread widened {baa_change:+.0f} bps this week "
            f"to {baa.get('latest', 0):.2f}% (from {baa.get('week_ago', 0):.2f}%).",
        ]
        if ted_change > 0:
            parts.append(
                f"TED spread also rising (+{ted_change:.0f} bps to "
                f"{ted.get('latest', 0):.2f}%)."
            )
        bank = spreads.get("DRTSCILM", {})
        if bank.get("latest", 0) > 0:
            parts.append(
                f"Bank lending standards tightening ({bank['latest']:.1f}% net "
                f"tightening)."
            )
        result = {
            "triggered": True,
            "baa_change_bps": baa_change,
            "ted_change_bps": ted_change,
            "spreads": spreads,
            "message": " ".join(parts),
        }
    else:
        result = None

    _fred_cache[cache_key] = {"data": result, "ts": time.time()}
    return result


def build_credit_stress_prompt() -> str:
    """Build a CREDIT STRESS WARNING block for AI model prompts.

    Returns a warning string if credit spreads are widening dangerously,
    or empty string if conditions are normal.
    """
    signal = get_credit_stress_signal()
    if not signal or not signal.get("triggered"):
        return ""

    return (
        "\n=== CREDIT STRESS WARNING (FRED DATA) ===\n"
        f"{signal['message']} "
        "Credit spreads widening at this pace signals institutional stress and rising "
        "default risk. REDUCE EXPOSURE to financials, high-yield plays, and leveraged "
        "names. TIGHTEN stop-losses across all positions. Favor quality large-caps and "
        "cash preservation. Do NOT initiate new broad-market longs until spreads stabilize.\n"
    )


def build_ai_context(symbol: str) -> str:
    """Build Alpha Vantage context string for AI model prompts."""
    parts = []

    # Company overview
    overview = get_company_overview(symbol)
    if overview:
        o = overview
        bits = []
        if o.get("pe_ratio"): bits.append(f"P/E: {o['pe_ratio']}")
        if o.get("eps"): bits.append(f"EPS: ${o['eps']}")
        if o.get("revenue_growth_yoy"): bits.append(f"Rev Growth: {o['revenue_growth_yoy']}%")
        if o.get("analyst_target"): bits.append(f"Analyst Target: ${o['analyst_target']}")
        if o.get("dividend_yield"): bits.append(f"Div Yield: {o['dividend_yield']}%")
        if bits:
            parts.append(f"{symbol} fundamentals: " + ", ".join(bits))

    # Earnings surprises
    surprises = get_earnings_surprises(symbol)
    if surprises:
        beats = sum(1 for s in surprises if s.get("beat"))
        latest = surprises[0]
        if latest.get("surprise_pct") is not None:
            parts.append(f"{symbol} earnings: {'beat' if latest['beat'] else 'missed'} by {latest['surprise_pct']}% last quarter ({beats}/4 beats)")

    return " | ".join(parts) if parts else ""
