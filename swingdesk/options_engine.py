"""
SwingDesk Options Engine
IVR Scanner + Spread Selector (IC, Bull Put, Bear Call, CSP)
TastyTrade methodology: 16 delta, 45 DTE, 50% profit target, 21 DTE exit
"""

import math, json, urllib.request, urllib.parse, os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# ── ENV ───────────────────────────────────────────────────────────────────────
def _load_env():
    # HM-OTASTY-ENV-WIRE 2026-05-27: was ~/autonomous-trader/.env (MAIN fleet) —
    # repointed to swingdesk/.env to keep the O-Tasty account isolated.
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip(); v = v.strip().strip('"').strip("'")
        if k and v and k not in os.environ:
            os.environ[k] = v

_load_env()
POLYGON_KEY = os.environ.get("POLYGON_API_KEY", "")
POLYGON_BASE = "https://api.polygon.io"

# ── TASTY TRADE CONSTANTS ─────────────────────────────────────────────────────
TARGET_DTE          = 45
EXIT_DTE            = 21
TARGET_DELTA        = 0.20       # HM-O-TASTY-DOCTRINE: 20Δ short strike (was 0.16)
PROFIT_TARGET_PCT   = 0.50       # close at 50% of credit
LOSS_LIMIT_MULT     = 2.0        # close at 2x credit received
MIN_IVR             = 50         # only sell above IVR 50
MIN_CREDIT_RATIO    = 0.33       # IC (two-sided): credit >= 1/3 of width
# HM-O-TASTY-DOCTRINE 2026-05-28: single-sided credit spreads (BPS/BCS) collect
# ~half what a two-sided IC does, so the 0.33 bar filtered them out entirely and
# the directional recommendation could never pick them. A directional spread at
# 20Δ acceptably collects ~20% of width.
SINGLE_SIDED_MIN_CREDIT_RATIO = 0.20
MAX_BPE_PCT         = 0.05       # max 5% of portfolio per trade

UNIVERSE = [
    "SPY","QQQ","AAPL","MSFT","NVDA","AMD","META","GOOGL","AMZN","TSLA",
    "JPM","GS","BAC","XOM","LLY","UNH","SMCI","PLTR","SOFI","COIN",
    "MSTR","ARM","AMAT","LRCX","IWM","GLD","SLV","TLT"
]

# ── POLYGON HELPERS ───────────────────────────────────────────────────────────
def _pg(path: str, params: dict = {}) -> dict:
    params["apiKey"] = POLYGON_KEY
    qs  = urllib.parse.urlencode(params)
    url = f"{POLYGON_BASE}{path}?{qs}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "SwingDesk/1.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"error": str(e), "results": []}

def get_iv_history(symbol: str, days: int = 365) -> list:
    """
    Approximate IV history using Polygon options snapshot IV values.
    Falls back to estimated IV from HV if options data unavailable.
    """
    end   = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=days + 10)).strftime("%Y-%m-%d")
    data  = _pg(f"/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}",
                {"adjusted": "true", "sort": "asc", "limit": "365"})
    bars  = data.get("results", [])
    if len(bars) < 20:
        return []

    # Estimate HV-based IV proxy (HV * 1.15 is TastyTrade's empirical overstatement)
    closes = [b["c"] for b in bars]
    ivs    = []
    for i in range(20, len(closes)):
        window  = closes[i-20:i]
        returns = [math.log(window[j]/window[j-1]) for j in range(1, len(window))]
        hv_daily = math.sqrt(sum(r**2 for r in returns) / (len(returns) - 1))
        hv_ann   = hv_daily * math.sqrt(252) * 100
        iv_est   = round(hv_ann * 1.15, 2)   # IV premium over HV
        ivs.append(iv_est)
    return ivs

def get_atm_iv(symbol: str) -> float:
    """Get current ATM IV from Polygon options snapshot."""
    snap = _pg(f"/v3/snapshot/options/{symbol}",
               {"limit": "10", "strike_price_gte": "0", "contract_type": "call"})
    results = snap.get("results", [])
    if not results:
        # Fall back to HV-based estimate
        ivs = get_iv_history(symbol, days=30)
        return ivs[-1] if ivs else 25.0

    # Find nearest ATM option
    price_snap = _pg(f"/v2/last/trade/{symbol}")
    spot = price_snap.get("results", {}).get("p", 0)
    if not spot:
        return 25.0

    best = None
    best_dist = float("inf")
    for r in results:
        details = r.get("details", {})
        strike  = details.get("strike_price", 0)
        iv      = r.get("implied_volatility", 0)
        if iv and strike:
            dist = abs(strike - spot)
            if dist < best_dist:
                best_dist = dist
                best = iv * 100   # convert to percentage

    return round(best, 2) if best else 25.0

def calc_ivr(symbol: str) -> dict:
    """
    Calculate IV Rank and IV Percentile.
    IVR = (current IV - 52w low) / (52w high - 52w low) * 100
    """
    iv_hist = get_iv_history(symbol, days=365)
    if len(iv_hist) < 30:
        return {"ivr": None, "ivp": None, "iv_current": None,
                "iv_high": None, "iv_low": None, "error": "insufficient data"}

    iv_current = iv_hist[-1]
    iv_high    = max(iv_hist)
    iv_low     = min(iv_hist)

    ivr = ((iv_current - iv_low) / (iv_high - iv_low) * 100) if iv_high != iv_low else 50.0
    ivp = (sum(1 for v in iv_hist if v < iv_current) / len(iv_hist) * 100)

    return {
        "symbol":     symbol,
        "iv_current": round(iv_current, 1),
        "iv_high":    round(iv_high, 1),
        "iv_low":     round(iv_low, 1),
        "ivr":        round(ivr, 1),
        "ivp":        round(ivp, 1),
        "sell_signal": ivr >= MIN_IVR
    }

# ── EXPIRATION FINDER ─────────────────────────────────────────────────────────
def find_target_expiration(symbol: str) -> Optional[str]:
    """Find the expiration closest to 45 DTE."""
    target_date = datetime.now() + timedelta(days=TARGET_DTE)
    data = _pg(f"/v3/reference/options/{symbol}",
               {"expiration_date_gte": datetime.now().strftime("%Y-%m-%d"),
                "expiration_date_lte": (datetime.now() + timedelta(days=75)).strftime("%Y-%m-%d"),
                "limit": "50", "contract_type": "call"})
    expirations = sorted(set(
        r["details"]["expiration_date"]
        for r in data.get("results", [])
        if "details" in r and "expiration_date" in r["details"]
    ))
    if not expirations:
        # Fallback: generate monthly expiration (~45 DTE)
        return target_date.strftime("%Y-%m-%d")
    # Find closest to 45 DTE
    return min(expirations, key=lambda d: abs((datetime.strptime(d, "%Y-%m-%d") - datetime.now()).days - TARGET_DTE))

def get_dte(expiration: str) -> int:
    exp_dt = datetime.strptime(expiration, "%Y-%m-%d")
    return max((exp_dt - datetime.now()).days, 0)

# ── BLACK-SCHOLES (APPROX) ────────────────────────────────────────────────────
def norm_cdf(x: float) -> float:
    """Standard normal CDF approximation."""
    t = 1 / (1 + 0.2316419 * abs(x))
    poly = t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))))
    result = 1 - (1/math.sqrt(2*math.pi)) * math.exp(-0.5*x*x) * poly
    return result if x >= 0 else 1 - result

def bs_delta(S: float, K: float, T: float, r: float, sigma: float, opt_type: str = "call") -> float:
    """Black-Scholes delta."""
    if T <= 0 or sigma <= 0:
        return 0
    d1 = (math.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*math.sqrt(T))
    if opt_type == "call":
        return norm_cdf(d1)
    else:
        return norm_cdf(d1) - 1

def bs_price(S: float, K: float, T: float, r: float, sigma: float, opt_type: str = "call") -> float:
    """Black-Scholes option price."""
    if T <= 0:
        if opt_type == "call":
            return max(S - K, 0)
        return max(K - S, 0)
    d1 = (math.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*math.sqrt(T))
    d2 = d1 - sigma*math.sqrt(T)
    if opt_type == "call":
        return S*norm_cdf(d1) - K*math.exp(-r*T)*norm_cdf(d2)
    else:
        return K*math.exp(-r*T)*norm_cdf(-d2) - S*norm_cdf(-d1)

def find_strike_for_delta(S: float, T: float, sigma: float, target_delta: float,
                          opt_type: str = "put", r: float = 0.045) -> float:
    """Binary search for strike at target delta."""
    lo, hi = S * 0.50, S * 1.50
    for _ in range(50):
        mid   = (lo + hi) / 2
        delta = bs_delta(S, mid, T, r, sigma, opt_type)
        if opt_type == "put":
            # Put delta is DECREASING in K (K↑ → delta 0→−1). Target = −target_delta.
            # delta > −target_delta ⇒ too close to 0 ⇒ strike too low ⇒ raise it.
            # (HM-OTASTY-STRIKE-SOLVER 2026-05-28: branch was inverted — drove the
            #  search to the S*1.5 bound, producing far-above-spot put strikes and
            #  negative IC max_loss.)
            if delta > -target_delta:
                lo = mid
            else:
                hi = mid
        else:
            if delta < target_delta:
                hi = mid
            else:
                lo = mid
    return round((lo + hi) / 2, 0)

# ── SPREAD BUILDERS ───────────────────────────────────────────────────────────
def _round_strike(strike: float, tick: float = 1.0) -> float:
    return round(round(strike / tick) * tick, 2)

def build_iron_condor(symbol: str, spot: float, iv_pct: float,
                      expiration: str, portfolio_size: float = 52340) -> dict:
    """
    Iron Condor: sell 16δ put spread + sell 16δ call spread.
    Width: 5 points for stocks < $100, 10 for $100-500, wider for SPY/QQQ.
    """
    dte    = get_dte(expiration)
    T      = dte / 365
    sigma  = iv_pct / 100
    r      = 0.045
    width  = 5 if spot < 100 else 10 if spot < 300 else 25 if spot >= 500 else 10

    # Strikes
    short_put    = _round_strike(find_strike_for_delta(spot, T, sigma, TARGET_DELTA, "put"))
    long_put     = _round_strike(short_put - width)
    short_call   = _round_strike(find_strike_for_delta(spot, T, sigma, TARGET_DELTA, "call"))
    long_call    = _round_strike(short_call + width)

    # Prices
    sp_price = bs_price(spot, short_put,  T, r, sigma, "put")
    lp_price = bs_price(spot, long_put,   T, r, sigma, "put")
    sc_price = bs_price(spot, short_call, T, r, sigma, "call")
    lc_price = bs_price(spot, long_call,  T, r, sigma, "call")

    credit      = round((sp_price - lp_price + sc_price - lc_price), 2)
    max_loss    = round(width - credit, 2)
    profit_tgt  = round(credit * PROFIT_TARGET_PCT, 2)
    loss_limit  = round(credit * LOSS_LIMIT_MULT, 2)
    pop         = round((1 - TARGET_DELTA * 2) * 100, 1)
    bpe         = round(max_loss * 100, 0)          # per contract
    contracts   = max(1, int((portfolio_size * MAX_BPE_PCT) / bpe)) if bpe > 0 else 1
    min_credit  = round(width * MIN_CREDIT_RATIO, 2)
    viable      = credit >= min_credit and credit > 0

    return {
        "structure":    "Iron Condor",
        "symbol":       symbol,
        "expiration":   expiration,
        "dte":          dte,
        "spot":         round(spot, 2),
        "legs": {
            "long_put":   long_put,
            "short_put":  short_put,
            "short_call": short_call,
            "long_call":  long_call
        },
        "credit":       round(credit, 2),
        "credit_per_contract": round(credit * 100, 0),
        "min_credit":   min_credit,
        "max_loss":     round(max_loss, 2),
        "max_loss_per_contract": round(max_loss * 100, 0),
        "profit_target": profit_tgt,
        "loss_limit":   loss_limit,
        "pop":          pop,
        "breakeven_low":  round(short_put - credit, 2),
        "breakeven_high": round(short_call + credit, 2),
        "contracts":    contracts,
        "total_credit": round(credit * 100 * contracts, 0),
        "total_risk":   round(max_loss * 100 * contracts, 0),
        "viable":       viable,
        "exit_dte":     EXIT_DTE,
        "profit_exit":  f"Close at ${profit_tgt} debit",
        "loss_exit":    f"Close at ${loss_limit} debit"
    }

def build_bull_put_spread(symbol: str, spot: float, iv_pct: float,
                          expiration: str, portfolio_size: float = 52340) -> dict:
    """Bull Put Spread: sell 16δ put, buy lower put. Bullish/neutral."""
    dte   = get_dte(expiration)
    T     = dte / 365
    sigma = iv_pct / 100
    r     = 0.045
    width = 5 if spot < 100 else 10 if spot < 300 else 25 if spot >= 500 else 10

    short_put = _round_strike(find_strike_for_delta(spot, T, sigma, TARGET_DELTA, "put"))
    long_put  = _round_strike(short_put - width)

    sp_price = bs_price(spot, short_put, T, r, sigma, "put")
    lp_price = bs_price(spot, long_put,  T, r, sigma, "put")

    credit     = round(sp_price - lp_price, 2)
    max_loss   = round(width - credit, 2)
    profit_tgt = round(credit * PROFIT_TARGET_PCT, 2)
    loss_limit = round(credit * LOSS_LIMIT_MULT, 2)
    pop        = round((1 - TARGET_DELTA) * 100, 1)
    bpe        = round(max_loss * 100, 0)
    contracts  = max(1, int((portfolio_size * MAX_BPE_PCT) / bpe)) if bpe > 0 else 1
    min_credit = round(width * SINGLE_SIDED_MIN_CREDIT_RATIO, 2)
    viable     = credit >= min_credit and credit > 0

    return {
        "structure":    "Bull Put Spread",
        "symbol":       symbol,
        "expiration":   expiration,
        "dte":          dte,
        "spot":         round(spot, 2),
        "bias":         "Bullish / Neutral",
        "legs": {
            "short_put": short_put,
            "long_put":  long_put
        },
        "credit":       round(credit, 2),
        "credit_per_contract": round(credit * 100, 0),
        "min_credit":   min_credit,
        "max_loss":     round(max_loss, 2),
        "max_loss_per_contract": round(max_loss * 100, 0),
        "profit_target": profit_tgt,
        "loss_limit":   loss_limit,
        "pop":          pop,
        "breakeven":    round(short_put - credit, 2),
        "contracts":    contracts,
        "total_credit": round(credit * 100 * contracts, 0),
        "total_risk":   round(max_loss * 100 * contracts, 0),
        "viable":       viable,
        "exit_dte":     EXIT_DTE
    }

def build_bear_call_spread(symbol: str, spot: float, iv_pct: float,
                           expiration: str, portfolio_size: float = 52340) -> dict:
    """Bear Call Spread: sell 16δ call, buy higher call. Bearish/neutral."""
    dte   = get_dte(expiration)
    T     = dte / 365
    sigma = iv_pct / 100
    r     = 0.045

    width = 5 if spot < 100 else 10 if spot < 300 else 25 if spot >= 500 else 10

    short_call = _round_strike(find_strike_for_delta(spot, T, sigma, TARGET_DELTA, "call"))
    long_call  = _round_strike(short_call + width)

    sc_price = bs_price(spot, short_call, T, r, sigma, "call")
    lc_price = bs_price(spot, long_call,  T, r, sigma, "call")

    credit     = round(sc_price - lc_price, 2)
    max_loss   = round(width - credit, 2)
    profit_tgt = round(credit * PROFIT_TARGET_PCT, 2)
    loss_limit = round(credit * LOSS_LIMIT_MULT, 2)
    pop        = round((1 - TARGET_DELTA) * 100, 1)
    bpe        = round(max_loss * 100, 0)
    contracts  = max(1, int((portfolio_size * MAX_BPE_PCT) / bpe)) if bpe > 0 else 1
    min_credit = round(width * SINGLE_SIDED_MIN_CREDIT_RATIO, 2)
    viable     = credit >= min_credit and credit > 0

    return {
        "structure":    "Bear Call Spread",
        "symbol":       symbol,
        "expiration":   expiration,
        "dte":          dte,
        "spot":         round(spot, 2),
        "bias":         "Bearish / Neutral",
        "legs": {
            "short_call": short_call,
            "long_call":  long_call
        },
        "credit":       round(credit, 2),
        "credit_per_contract": round(credit * 100, 0),
        "min_credit":   min_credit,
        "max_loss":     round(max_loss, 2),
        "max_loss_per_contract": round(max_loss * 100, 0),
        "profit_target": profit_tgt,
        "loss_limit":   loss_limit,
        "pop":          pop,
        "breakeven":    round(short_call + credit, 2),
        "contracts":    contracts,
        "total_credit": round(credit * 100 * contracts, 0),
        "total_risk":   round(max_loss * 100 * contracts, 0),
        "viable":       viable,
        "exit_dte":     EXIT_DTE
    }

def build_csp(symbol: str, spot: float, iv_pct: float,
              expiration: str, portfolio_size: float = 52340) -> dict:
    """
    Cash-Secured Put: sell 16δ put, secured by cash.
    Only use on stocks you'd actually want to own.
    """
    dte   = get_dte(expiration)
    T     = dte / 365
    sigma = iv_pct / 100
    r     = 0.045

    short_put  = _round_strike(find_strike_for_delta(spot, T, sigma, TARGET_DELTA, "put"))
    sp_price   = bs_price(spot, short_put, T, r, sigma, "put")

    credit     = round(sp_price, 2)
    max_loss   = round(short_put - credit, 2)   # stock goes to zero
    profit_tgt = round(credit * PROFIT_TARGET_PCT, 2)
    loss_limit = round(credit * LOSS_LIMIT_MULT, 2)
    pop        = round((1 - TARGET_DELTA) * 100, 1)
    cash_req   = round(short_put * 100, 0)      # per contract
    contracts  = max(1, int((portfolio_size * 0.10) / cash_req)) if cash_req > 0 else 1
    viable     = credit > 0.10 and short_put > 0

    return {
        "structure":    "Cash-Secured Put",
        "symbol":       symbol,
        "expiration":   expiration,
        "dte":          dte,
        "spot":         round(spot, 2),
        "bias":         "Bullish — willing to own stock",
        "legs": {
            "short_put": short_put
        },
        "credit":       round(credit, 2),
        "credit_per_contract": round(credit * 100, 0),
        "max_loss":     round(max_loss, 2),
        "cash_required": cash_req,
        "profit_target": profit_tgt,
        "loss_limit":   loss_limit,
        "pop":          pop,
        "breakeven":    round(short_put - credit, 2),
        "assignment_price": short_put,
        "contracts":    contracts,
        "total_credit": round(credit * 100 * contracts, 0),
        "total_risk":   round(max_loss * 100 * contracts, 0),
        "viable":       viable,
        "exit_dte":     EXIT_DTE,
        "note":         "Only use on stocks you are willing to own at the strike price"
    }

# ── IVR SCANNER ───────────────────────────────────────────────────────────────
def get_spot(symbol: str) -> float:
    # HM-OTASTY-GETSPOT-FIX 2026-05-27: original `data.get("results", {}).get("p")`
    # crashed with `'list' object has no attribute 'get'` — on the Polygon Starter
    # plan /v2/last/trade is HTTP 403, and _pg's error handler returns
    # results=[] (a LIST), so .get("p") blew up before the snapshot fallback.
    # Also: snapshot lastTrade.p is null on Starter. Price-source priority below
    # is plan-aware: real-time trade (if plan allows) → snapshot day close →
    # prev-day aggregate close. Each branch guards list-vs-dict shape.
    data = _pg(f"/v2/last/trade/{symbol}")
    res = data.get("results") if isinstance(data, dict) else None
    if isinstance(res, dict) and res.get("p"):
        return float(res["p"])

    snap = _pg("/v2/snapshot/locale/us/markets/stocks/tickers", {"tickers": symbol})
    tickers = snap.get("tickers", []) if isinstance(snap, dict) else []
    if tickers and isinstance(tickers[0], dict):
        t = tickers[0]
        price = (t.get("lastTrade", {}).get("p")
                 or t.get("day", {}).get("c")
                 or t.get("prevDay", {}).get("c") or 0)
        if price:
            return float(price)

    agg = _pg(f"/v2/aggs/ticker/{symbol}/prev")
    ares = agg.get("results", []) if isinstance(agg, dict) else []
    if ares and isinstance(ares[0], dict) and ares[0].get("c"):
        return float(ares[0]["c"])

    return 0.0

def _directional_lean(symbol: str) -> str:
    """HM-O-TASTY-DOCTRINE 2026-05-28: directional bias from price vs 20/50 SMA.
    bullish: price > SMA20 > SMA50; bearish: price < SMA20 < SMA50; else neutral.
    Drives structure selection (bullish→BPS, bearish→BCS, neutral→IC). Fail-safe → neutral."""
    try:
        end = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        bars = _pg(f"/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}",
                   {"adjusted": "true", "sort": "asc", "limit": "120"}).get("results", [])
        closes = [b["c"] for b in bars]
        if len(closes) < 50:
            return "neutral"
        price = closes[-1]
        sma20 = sum(closes[-20:]) / 20
        sma50 = sum(closes[-50:]) / 50
        if price > sma20 > sma50:
            return "bullish"
        if price < sma20 < sma50:
            return "bearish"
        return "neutral"
    except Exception:
        return "neutral"


def scan_ivr(symbols: list = None, portfolio_size: float = 52340) -> list:
    """
    Full IVR scan: for each symbol compute IVR, find expiration,
    build all applicable spread structures, rank by IVR.
    """
    syms    = symbols or UNIVERSE
    results = []

    for sym in syms:
        try:
            ivr_data = calc_ivr(sym)
            if ivr_data.get("error") or ivr_data["ivr"] is None:
                continue

            ivr  = ivr_data["ivr"]
            iv   = ivr_data["iv_current"]
            spot = get_spot(sym)
            if not spot:
                continue

            expiration = find_target_expiration(sym)
            if not expiration:
                continue

            dte = get_dte(expiration)

            entry = {
                "symbol":     sym,
                "spot":       round(spot, 2),
                "ivr":        ivr,
                "ivp":        ivr_data["ivp"],
                "iv_current": iv,
                "iv_high":    ivr_data["iv_high"],
                "iv_low":     ivr_data["iv_low"],
                "expiration": expiration,
                "dte":        dte,
                "sell_signal": ivr >= MIN_IVR,
                "structures": {}
            }

            if ivr >= MIN_IVR:
                # Build all four structures
                entry["structures"]["iron_condor"]     = build_iron_condor(sym, spot, iv, expiration, portfolio_size)
                entry["structures"]["bull_put_spread"]  = build_bull_put_spread(sym, spot, iv, expiration, portfolio_size)
                entry["structures"]["bear_call_spread"] = build_bear_call_spread(sym, spot, iv, expiration, portfolio_size)
                entry["structures"]["csp"]              = build_csp(sym, spot, iv, expiration, portfolio_size)

                # Structure recommendation — HM-O-TASTY-DOCTRINE 2026-05-28.
                # Direction picks the structure (bullish→BPS, bearish→BCS,
                # neutral→IC); CSP stays built as the discretionary "willing to
                # own" alternative (not auto-recommended without an own-it call).
                # Only consider viable structures with positive max_loss — this
                # retires the max(max_loss,0.01) ratio exploit that let a
                # negative/near-zero max_loss make IC win every time.
                lean = _directional_lean(sym)
                entry["directional_lean"] = lean
                _by_lean = {"bullish": "bull_put_spread",
                            "bearish": "bear_call_spread",
                            "neutral": "iron_condor"}
                viable = {k: v for k, v in entry["structures"].items()
                          if v.get("viable") and v.get("max_loss", 0) > 0}
                if viable:
                    preferred = _by_lean.get(lean, "iron_condor")
                    if preferred in viable:
                        entry["recommended"] = preferred
                    else:
                        # doctrine structure not viable → best return-on-risk among viable
                        entry["recommended"] = max(
                            viable, key=lambda k: viable[k]["credit"] / viable[k]["max_loss"])

            results.append(entry)

        except Exception as e:
            print(f"[IVR scan] {sym}: {e}")
            continue

    results.sort(key=lambda x: x["ivr"], reverse=True)
    return results

# ── TRADE MANAGEMENT CHECKER ──────────────────────────────────────────────────
def check_management(trade: dict, current_price: float) -> dict:
    """
    Given an open options trade, check if any management rule is triggered.
    Returns action recommendation.
    """
    opened  = datetime.fromisoformat(trade.get("opened_at", datetime.now().isoformat()))
    exp_str = trade.get("expiration", "")
    credit  = trade.get("credit", 0)
    current_value = current_price   # current mark of the spread

    actions   = []
    urgent    = False

    if exp_str:
        dte = get_dte(exp_str)
        if dte <= EXIT_DTE:
            actions.append(f"TIME EXIT: {dte} DTE reached — close now (21 DTE rule)")
            urgent = True

    if credit > 0:
        profit_pct = (credit - current_value) / credit * 100
        if profit_pct >= 50:
            actions.append(f"PROFIT TARGET: {profit_pct:.0f}% profit reached — close at 50% rule")
            urgent = True
        elif current_value >= credit * LOSS_LIMIT_MULT:
            actions.append(f"LOSS LIMIT: spread at {current_value:.2f} vs 2x credit {credit*2:.2f} — close now")
            urgent = True

    if not actions:
        if exp_str:
            actions.append(f"HOLD: {dte} DTE remaining, monitor for 50% profit")
        else:
            actions.append("HOLD: no management trigger")

    return {
        "action":  "CLOSE" if urgent else "HOLD",
        "urgent":  urgent,
        "reasons": actions
    }

if __name__ == "__main__":
    print("SwingDesk Options Engine — IVR Scanner")
    print(f"Polygon: {'OK' if POLYGON_KEY else 'MISSING'}")
    print("\nRunning quick IVR check on SPY, QQQ, NVDA...")
    for sym in ["SPY", "QQQ", "NVDA"]:
        ivr = calc_ivr(sym)
        print(f"  {sym}: IVR={ivr.get('ivr')} IV={ivr.get('iv_current')} signal={'SELL' if ivr.get('sell_signal') else 'WAIT'}")
