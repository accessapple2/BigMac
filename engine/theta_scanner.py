"""Theta Collection Scanner — premium-selling opportunity detection.

For each watchlist stock, scans for high-theta collection setups by evaluating:
  - IV Rank/Percentile: elevated IV = fatter premiums to sell
  - Range-bound price action: sideways market = theta-friendly
  - Earnings proximity: flags theta traps (< 7 days = avoid)
  - 30-45 DTE options: optimal theta decay zone

Strategy suggestions:
  - Iron Condor: range-bound + IV rank ≥ 40 (collect on both sides)
  - Credit Spread: directional lean + IV rank ≥ 30 (defined risk)
  - Short Strangle: high IV ≥ 60 + no earnings (wider, higher premium)
  - Covered Call: IV rank ≥ 25 (conservative, stock holders only)

Theta Score 1-10:
  - IV Rank contribution (0-4 pts)
  - IV Percentile bonus (0-2 pts)
  - Range-bound bonus (0-2 pts)
  - Near 200 SMA bonus (0-1 pt)
  - Earnings penalty (-2 to -4 pts)

Dashboard icon: ⏱
"""
from __future__ import annotations
import sqlite3
import threading
import time
import math
from datetime import datetime, date, timedelta
from rich.console import Console

console = Console()
DB = "data/trader.db"

_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 3600  # 1-hour cache — IV rank stable intraday


# ── Database ──────────────────────────────────────────────────────────────────

def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def ensure_theta_table():
    conn = _conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS theta_opportunities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            scan_date TEXT NOT NULL,
            iv_rank REAL,
            iv_percentile REAL,
            current_iv REAL,
            strategy_type TEXT,
            short_strike_call REAL,
            short_strike_put REAL,
            long_strike_call REAL,
            long_strike_put REAL,
            expiration TEXT,
            dte INTEGER,
            estimated_daily_theta REAL,
            max_risk REAL,
            theta_score INTEGER DEFAULT 0,
            is_range_bound INTEGER DEFAULT 0,
            earnings_warning INTEGER DEFAULT 0,
            earnings_date TEXT,
            spot_price REAL,
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


# ── IV Rank calculation ────────────────────────────────────────────────────────

def _calc_iv_rank(symbol: str) -> dict | None:
    """IV Rank + Percentile using 52-week realized vol as proxy.

    Since free Yahoo data doesn't provide historical IV snapshots, we use
    rolling 20-day realized vol as a proxy. This is a reasonable approximation
    because implied and realized vol are highly correlated.
    """
    from engine.market_data import _is_yf_limited, _set_yf_limited
    if _is_yf_limited():
        return None
    try:
        import yfinance as yf
        import numpy as np
        ticker = yf.Ticker(symbol)
        expirations = ticker.options
        if not expirations:
            return None

        hist = ticker.history(period="2d")
        if hist.empty:
            return None
        spot = float(hist["Close"].iloc[-1])

        # Get ATM IV from nearest expiry
        nearest = expirations[0]
        chain = ticker.option_chain(nearest)
        calls = chain.calls
        if calls.empty:
            return None

        calls = calls.copy()
        calls["distance"] = abs(calls["strike"] - spot)
        atm = calls.sort_values("distance").iloc[0]
        current_iv = float(atm.get("impliedVolatility", 0) or 0)
        if current_iv <= 0 or (current_iv != current_iv):  # NaN check
            return None
        current_iv_pct = round(current_iv * 100, 1)

        # 52-week realized vol for IV rank proxy
        yearly = ticker.history(period="1y", interval="1d")
        if yearly.empty or len(yearly) < 50:
            return None

        close = yearly["Close"]
        returns = close.pct_change().dropna()
        rolling_vol = returns.rolling(20).std() * np.sqrt(252)
        rolling_vol = rolling_vol.dropna() * 100  # as percentage

        vol_high = float(rolling_vol.max())
        vol_low = float(rolling_vol.min())
        vol_range = vol_high - vol_low

        if vol_range <= 0:
            iv_rank = 50.0
        else:
            iv_rank = min(100, max(0, (current_iv_pct - vol_low) / vol_range * 100))

        iv_percentile = float((rolling_vol < current_iv_pct).mean() * 100)

        return {
            "symbol": symbol,
            "spot": round(spot, 2),
            "current_iv": current_iv_pct,
            "iv_rank": round(iv_rank, 1),
            "iv_percentile": round(iv_percentile, 1),
            "vol_high_52w": round(vol_high, 1),
            "vol_low_52w": round(vol_low, 1),
            "expiration_nearest": nearest,
            "expirations": list(expirations),
        }

    except Exception as e:
        from engine.market_data import _set_yf_limited
        err = str(e)
        if "Too Many Requests" in err or "rate limit" in err.lower():
            _set_yf_limited()
        console.log(f"[yellow]Theta IV rank error {symbol}: {e}")
        return None


# ── Range-bound detection ──────────────────────────────────────────────────────

def _is_range_bound(symbol: str, spot: float) -> bool:
    """True if price has been sideways (< 8% range over last 10 trading days)."""
    try:
        from engine.market_data import get_intraday_candles
        candles = get_intraday_candles(symbol, interval="1d", range_="1mo")
        if len(candles) < 5:
            return False
        recent = candles[-10:]
        highs = [c["high"] for c in recent if c.get("high")]
        lows  = [c["low"]  for c in recent if c.get("low")]
        if not highs or not lows:
            return False
        range_high = max(highs)
        range_low  = min(lows)
        mid = (range_high + range_low) / 2
        range_pct = (range_high - range_low) / mid * 100
        return range_pct < 8.0  # < 8% range = sideways
    except Exception:
        return False


def _near_sma200(symbol: str, spot: float) -> bool:
    """True if price is within 2% of 200-day SMA."""
    try:
        from engine.sma_filter import get_sma_200_status
        status = get_sma_200_status(symbol)
        if status:
            dist = abs(status.get("distance_pct", 999))
            return dist <= 2.0
    except Exception:
        pass
    return False


# ── Options chain helpers ──────────────────────────────────────────────────────

def _days_until_expiry(expiry_str: str) -> int:
    """Days from today until expiry string 'YYYY-MM-DD'."""
    try:
        expiry = datetime.strptime(expiry_str, "%Y-%m-%d").date()
        return max(0, (expiry - date.today()).days)
    except Exception:
        return 0


def _unix_to_date(ts: int) -> str:
    """Unix timestamp → 'YYYY-MM-DD'."""
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d")


def _find_target_expiry(expirations: list[str], target_dte: int = 37,
                         min_dte: int = 21, max_dte: int = 55) -> str | None:
    """Find the expiry closest to target_dte within [min_dte, max_dte]."""
    candidates = []
    for exp in expirations:
        dte = _days_until_expiry(exp)
        if min_dte <= dte <= max_dte:
            candidates.append((abs(dte - target_dte), dte, exp))
    if not candidates:
        # Relax to any within 14-60 DTE
        for exp in expirations:
            dte = _days_until_expiry(exp)
            if 14 <= dte <= 60:
                candidates.append((abs(dte - target_dte), dte, exp))
    if not candidates:
        return None
    candidates.sort()
    return candidates[0][2]  # return the date string


def _find_otm_strike(strikes_premiums: list[tuple], spot: float,
                     target_moneyness: float, above: bool) -> tuple | None:
    """Find strike closest to target_moneyness.

    strikes_premiums: list of (strike, bid, ask, iv)
    target_moneyness: fraction above/below spot (e.g. 0.08 = 8% OTM)
    above: True for calls, False for puts
    """
    if above:
        target = spot * (1 + target_moneyness)
        candidates = [(abs(s - target), s, bid, ask, iv)
                      for s, bid, ask, iv in strikes_premiums if s > spot]
    else:
        target = spot * (1 - target_moneyness)
        candidates = [(abs(s - target), s, bid, ask, iv)
                      for s, bid, ask, iv in strikes_premiums if s < spot]
    if not candidates:
        return None
    candidates.sort()
    _, strike, bid, ask, iv = candidates[0]
    return strike, bid, ask, iv


def _extract_strikes(option_contracts: list) -> list[tuple]:
    """Extract (strike, bid, ask, iv) from Yahoo option contracts list."""
    out = []
    for c in option_contracts:
        try:
            s = float(c.get("strike", 0))
            b = float(c.get("bid", 0) or 0)
            a = float(c.get("ask", 0) or 0)
            iv = float(c.get("impliedVolatility", 0) or 0) * 100
            if s > 0:
                out.append((s, b, a, iv))
        except Exception:
            continue
    return out


def _estimate_daily_theta(mid: float, dte: int) -> float:
    """Estimate daily theta from option mid-price and DTE.

    Uses a simplified decay model: theta ≈ premium * sqrt(1/DTE) * 0.5
    For 30-45 DTE options, this gives a reasonable ballpark.
    """
    if dte <= 0 or mid <= 0:
        return 0.0
    # Theta accelerates as DTE decreases; at 30-45 DTE roughly 1.5-2% of premium/day
    daily_rate = 0.018 + (30 - min(dte, 30)) * 0.001  # slightly higher for shorter DTE
    return round(mid * daily_rate, 3)


# ── Strategy suggestion ────────────────────────────────────────────────────────

def _select_strategies(iv_rank: float, range_bound: bool,
                        earnings_warning: bool, earnings_days: int) -> str:
    """Return comma-separated list of applicable theta strategies."""
    strategies = []
    if earnings_warning and earnings_days <= 3:
        return "Avoid — Earnings Imminent"
    if iv_rank >= 40 and range_bound and not earnings_warning:
        strategies.append("Iron Condor")
    if iv_rank >= 60 and not earnings_warning:
        strategies.append("Short Strangle")
    if iv_rank >= 30 and not range_bound:
        strategies.append("Credit Spread")
    if iv_rank >= 25:
        strategies.append("Covered Call")
    if not strategies:
        strategies = ["Covered Call"] if iv_rank >= 20 else []
    return ", ".join(strategies) if strategies else "Monitor"


def _calc_theta_score(iv_rank: float, iv_percentile: float,
                       range_bound: bool, near_sma: bool,
                       earnings_warning: bool, earnings_days: int) -> int:
    """Calculate theta opportunity score 1-10."""
    score = 0

    # IV rank: primary driver (0-4 pts)
    if iv_rank >= 80:
        score += 4
    elif iv_rank >= 60:
        score += 3
    elif iv_rank >= 40:
        score += 2
    elif iv_rank >= 20:
        score += 1

    # IV percentile bonus (0-2 pts)
    if iv_percentile >= 70:
        score += 2
    elif iv_percentile >= 50:
        score += 1

    # Range-bound bonus (0-2 pts)
    if range_bound:
        score += 2

    # Near 200 SMA bonus (0-1 pt)
    if near_sma:
        score += 1

    # Earnings penalty
    if earnings_warning:
        if earnings_days <= 3:
            score -= 4
        elif earnings_days <= 7:
            score -= 2

    return min(10, max(1, score))


# ── Core scan ─────────────────────────────────────────────────────────────────

def scan_theta_symbol(symbol: str, earnings_map: dict | None = None) -> dict | None:
    """Scan one symbol for theta opportunities. Returns result dict or None."""
    iv_data = _calc_iv_rank(symbol)
    if not iv_data:
        return None

    spot = iv_data["spot"]
    iv_rank = iv_data["iv_rank"]
    iv_percentile = iv_data["iv_percentile"]
    current_iv = iv_data["current_iv"]

    # Minimum IV rank to bother scanning options chain
    if iv_rank < 20:
        return None

    # Earnings check
    earnings_warning = False
    earnings_days = 999
    earnings_date_str = None
    if earnings_map and symbol in earnings_map:
        info = earnings_map[symbol]
        earnings_days = info.get("days_until", 999)
        earnings_date_str = info.get("date")
        earnings_warning = 0 <= earnings_days <= 7

    # Range-bound check
    range_bound = _is_range_bound(symbol, spot)
    near_sma = _near_sma200(symbol, spot)

    # Get options chain for 30-45 DTE
    from engine.market_data import yahoo_options_chain
    raw = yahoo_options_chain(symbol)
    if not raw:
        return None

    # Find target expiry from list
    expiry_ts_list = raw.get("expirationDates", [])
    expiry_dates = [_unix_to_date(ts) for ts in expiry_ts_list]
    target_expiry = _find_target_expiry(expiry_dates)
    if not target_expiry:
        return None

    dte = _days_until_expiry(target_expiry)
    if dte <= 0:
        return None

    # Get chain for target expiry
    target_ts = next((ts for ts in expiry_ts_list
                       if _unix_to_date(ts) == target_expiry), None)
    if not target_ts:
        return None

    from engine.market_data import yahoo_options_chain_for_date
    exp_chain = yahoo_options_chain_for_date(symbol, target_ts)
    if not exp_chain:
        return None

    calls_raw = exp_chain.get("calls", [])
    puts_raw  = exp_chain.get("puts", [])
    if not calls_raw or not puts_raw:
        return None

    calls = _extract_strikes(calls_raw)
    puts  = _extract_strikes(puts_raw)

    # Select short strikes (~8% OTM)
    sc = _find_otm_strike(calls, spot, 0.08, above=True)
    sp = _find_otm_strike(puts,  spot, 0.08, above=False)

    # Select long strikes (~12% OTM, wings for Iron Condor)
    lc = _find_otm_strike(calls, spot, 0.12, above=True)
    lp = _find_otm_strike(puts,  spot, 0.12, above=False)

    if not sc or not sp:
        return None

    sc_strike, sc_bid, sc_ask, _ = sc
    sp_strike, sp_bid, sp_ask, _ = sp
    lc_strike = lc[0] if lc else None
    lp_strike = lp[0] if lp else None

    # Mid-prices
    sc_mid = (sc_bid + sc_ask) / 2 if sc_ask > 0 else sc_bid
    sp_mid = (sp_bid + sp_ask) / 2 if sp_ask > 0 else sp_bid

    # Skip if premiums are effectively zero (no options market)
    if sc_mid + sp_mid < 0.05:
        return None

    # Daily theta estimates (per contract = * 100 shares)
    call_theta_daily = _estimate_daily_theta(sc_mid, dte)
    put_theta_daily  = _estimate_daily_theta(sp_mid, dte)
    total_daily_theta = round((call_theta_daily + put_theta_daily) * 100, 2)

    # Max risk for Iron Condor
    total_credit = (sc_mid + sp_mid) * 100
    call_width = (lc_strike - sc_strike) * 100 if lc_strike else sc_strike * 100 * 0.05
    put_width  = (sp_strike - lp_strike) * 100 if lp_strike else sp_strike * 100 * 0.05
    max_risk = round(min(call_width, put_width) - total_credit, 2)

    # Strategy and score
    strategy_type = _select_strategies(iv_rank, range_bound, earnings_warning, earnings_days)
    theta_score = _calc_theta_score(iv_rank, iv_percentile, range_bound, near_sma,
                                     earnings_warning, earnings_days)

    result = {
        "ticker": symbol,
        "scan_date": date.today().isoformat(),
        "spot_price": spot,
        "current_iv": current_iv,
        "iv_rank": iv_rank,
        "iv_percentile": iv_percentile,
        "strategy_type": strategy_type,
        "short_strike_call": round(sc_strike, 2),
        "short_strike_put":  round(sp_strike, 2),
        "long_strike_call":  round(lc_strike, 2) if lc_strike else None,
        "long_strike_put":   round(lp_strike, 2) if lp_strike else None,
        "expiration": target_expiry,
        "dte": dte,
        "call_premium": round(sc_mid, 2),
        "put_premium":  round(sp_mid, 2),
        "estimated_daily_theta": total_daily_theta,
        "max_risk": max_risk,
        "theta_score": theta_score,
        "is_range_bound": range_bound,
        "near_sma200": near_sma,
        "earnings_warning": earnings_warning,
        "earnings_date": earnings_date_str,
        "earnings_days": earnings_days if earnings_days < 999 else None,
    }

    _save_theta_opportunity(result)
    return result


def _save_theta_opportunity(opp: dict):
    """Upsert theta opportunity (one row per ticker per scan_date)."""
    conn = _conn()
    try:
        conn.execute(
            "DELETE FROM theta_opportunities WHERE ticker=? AND scan_date=?",
            (opp["ticker"], opp["scan_date"])
        )
        conn.execute(
            "INSERT INTO theta_opportunities "
            "(ticker, scan_date, iv_rank, iv_percentile, current_iv, strategy_type, "
            "short_strike_call, short_strike_put, long_strike_call, long_strike_put, "
            "expiration, dte, estimated_daily_theta, max_risk, theta_score, "
            "is_range_bound, earnings_warning, earnings_date, spot_price) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                opp["ticker"], opp["scan_date"],
                opp["iv_rank"], opp["iv_percentile"], opp["current_iv"],
                opp["strategy_type"],
                opp["short_strike_call"], opp["short_strike_put"],
                opp["long_strike_call"], opp["long_strike_put"],
                opp["expiration"], opp["dte"],
                opp["estimated_daily_theta"], opp["max_risk"],
                opp["theta_score"],
                1 if opp["is_range_bound"] else 0,
                1 if opp["earnings_warning"] else 0,
                opp["earnings_date"],
                opp["spot_price"],
            )
        )
        conn.commit()
    except Exception as e:
        console.log(f"[red]Theta save error ({opp['ticker']}): {e}")
    finally:
        conn.close()


def scan_all_theta(symbols: list[str] | None = None) -> list[dict]:
    """Scan all watchlist symbols for theta opportunities. Returns sorted list."""
    if symbols is None:
        from config import WATCH_STOCKS
        symbols = WATCH_STOCKS

    with _cache_lock:
        cached = _cache.get("all")
        if cached and (time.time() - cached["ts"]) < _CACHE_TTL:
            return cached["opps"]

    # Pre-fetch earnings map for all symbols at once
    earnings_map: dict = {}
    try:
        from engine.earnings_calendar import fetch_earnings
        upcoming = fetch_earnings(symbols)
        for item in upcoming:
            earnings_map[item["symbol"]] = {
                "date": item["date"],
                "days_until": item["days_until"],
            }
    except Exception as e:
        console.log(f"[yellow]Theta: earnings fetch failed: {e}")

    from concurrent.futures import ThreadPoolExecutor, as_completed
    results = []

    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {ex.submit(scan_theta_symbol, sym, earnings_map): sym for sym in symbols}
        for fut in as_completed(futs, timeout=180):
            sym = futs[fut]
            try:
                r = fut.result()
                if r:
                    results.append(r)
                    console.log(
                        f"[cyan]⏱ Theta {sym}: score={r['theta_score']}/10 "
                        f"IV={r['iv_rank']:.0f}% rank · {r['strategy_type']}"
                    )
            except Exception as e:
                console.log(f"[yellow]Theta scan skip {sym}: {e}")

    # Sort by theta score desc, then by daily theta income desc
    results.sort(key=lambda x: (-x["theta_score"], -x["estimated_daily_theta"]))

    with _cache_lock:
        _cache["all"] = {"opps": results, "ts": time.time()}

    return results


# ── DB queries ────────────────────────────────────────────────────────────────

def get_theta_opportunities(limit: int = 50, min_score: int = 3) -> list[dict]:
    """Return recent theta opportunities from DB, sorted by score."""
    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM theta_opportunities "
        "WHERE theta_score >= ? "
        "ORDER BY scan_date DESC, theta_score DESC, estimated_daily_theta DESC "
        "LIMIT ?",
        (min_score, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_latest_theta(ticker: str | None = None) -> list[dict]:
    """Return today's theta opportunities, optionally filtered by ticker."""
    today = date.today().isoformat()
    conn = _conn()
    if ticker:
        rows = conn.execute(
            "SELECT * FROM theta_opportunities WHERE scan_date=? AND ticker=? "
            "ORDER BY theta_score DESC",
            (today, ticker)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM theta_opportunities WHERE scan_date=? "
            "ORDER BY theta_score DESC, estimated_daily_theta DESC",
            (today,)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_cached_theta() -> list[dict]:
    """Return in-memory cached theta results (most recent scan)."""
    with _cache_lock:
        cached = _cache.get("all")
        if cached:
            return cached["opps"]
    # Fall back to DB
    return get_latest_theta()


# ── AI prompt injection ───────────────────────────────────────────────────────

def build_theta_prompt_section(symbol: str, player_id: str = "") -> str:
    """Return theta context for AI prompt injection.

    For options-sosnoff: always inject when score >= 3.
    For other players: inject only when score >= 7 (very strong setups only).
    """
    try:
        today = date.today().isoformat()
        conn = _conn()
        row = conn.execute(
            "SELECT * FROM theta_opportunities WHERE ticker=? AND scan_date=? "
            "ORDER BY theta_score DESC LIMIT 1",
            (symbol, today)
        ).fetchone()
        conn.close()

        if not row:
            # Try from cache
            with _cache_lock:
                cached = _cache.get("all")
                if cached:
                    match = next((o for o in cached["opps"] if o["ticker"] == symbol), None)
                    if match:
                        row = match
            if not row:
                return ""

        r = dict(row) if hasattr(row, "keys") else row
        score = r.get("theta_score", 0)
        is_sosnoff = "sosnoff" in player_id.lower()

        # Threshold: Sosnoff sees all (score >= 3), others only see high scores (>= 7)
        if is_sosnoff and score < 3:
            return ""
        if not is_sosnoff and score < 7:
            return ""

        iv_rank = r.get("iv_rank", 0)
        strategy = r.get("strategy_type", "")
        theta = r.get("estimated_daily_theta", 0)
        sc = r.get("short_strike_call")
        sp = r.get("short_strike_put")
        exp = r.get("expiration", "")
        dte = r.get("dte", 0)
        max_risk = r.get("max_risk")
        earnings = r.get("earnings_warning", 0)
        earn_date = r.get("earnings_date", "")
        spot = r.get("spot_price", 0)

        lines = [f"\n=== THETA OPPORTUNITY ⏱ ==="]
        lines.append(f"  Score: {score}/10 | IV Rank: {iv_rank:.0f}% | IV: {r.get('current_iv', 0):.1f}%")
        lines.append(f"  Strategy: {strategy}")
        lines.append(f"  Expiry: {exp} ({dte} DTE)")
        if sc and sp:
            lines.append(f"  Short Call: ${sc:.2f} | Short Put: ${sp:.2f} | Spot: ${spot:.2f}")
        if max_risk:
            lines.append(f"  Est. Daily Theta: ${theta:.2f}/contract | Max Risk: ${max_risk:.0f}")
        if r.get("is_range_bound"):
            lines.append(f"  ✓ Range-bound price action — theta-friendly")
        if earnings:
            lines.append(f"  ⚠ EARNINGS WARNING: {earn_date} — premium is rich but risk is elevated")

        if is_sosnoff:
            lines.append(
                f"  Counselor: 'I sense {'anxiety and fear — the premium is rich' if iv_rank >= 60 else 'elevated uncertainty — premium is worth collecting'}. "
                f"The {'sideways energy tells me to sell on both sides' if r.get('is_range_bound') else 'directional lean is clear'}.'"
            )

        return "\n".join(lines) + "\n"

    except Exception:
        return ""
