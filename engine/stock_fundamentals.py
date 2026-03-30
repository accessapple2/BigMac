"""Stock Fundamentals — unified data layer using Yahoo direct HTTP.

Fetches and caches: P/E, EPS, revenue growth, margins, D/E, FCF, beta,
short interest, analyst targets, institutional ownership, sector, earnings dates.
Stores in stock_fundamentals SQLite table with daily refresh.
Computes Smart Score (A-F) and Portfolio Health Check.
"""
from __future__ import annotations
import json
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from rich.console import Console

console = Console()

DB = "data/trader.db"
_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 3600  # 1 hour in-memory, DB persists 24h


def _init_table():
    """Create stock_fundamentals table if not exists."""
    conn = sqlite3.connect(DB, check_same_thread=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_fundamentals (
            symbol TEXT PRIMARY KEY,
            data TEXT NOT NULL,
            smart_score INTEGER,
            grade TEXT,
            updated_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()


_init_table()


def _get_cached(symbol: str) -> dict | None:
    with _cache_lock:
        entry = _cache.get(symbol)
        if entry and time.time() - entry["ts"] < _CACHE_TTL:
            return entry["data"]
    # Check DB (24h TTL)
    try:
        conn = sqlite3.connect(DB, check_same_thread=False)
        row = conn.execute(
            "SELECT data, updated_at FROM stock_fundamentals WHERE symbol=?",
            (symbol,)
        ).fetchone()
        conn.close()
        if row:
            updated = datetime.fromisoformat(row[1])
            if (datetime.now() - updated).total_seconds() < 86400:  # 24h
                data = json.loads(row[0])
                with _cache_lock:
                    _cache[symbol] = {"data": data, "ts": time.time()}
                return data
    except Exception:
        pass
    return None


def _save_to_db(symbol: str, data: dict, score: int, grade: str):
    try:
        conn = sqlite3.connect(DB, check_same_thread=False)
        conn.execute("""
            INSERT OR REPLACE INTO stock_fundamentals (symbol, data, smart_score, grade, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, (symbol, json.dumps(data), score, grade, datetime.now().isoformat()))
        conn.commit()
        conn.close()
    except Exception as e:
        console.log(f"[red]DB save error for {symbol}: {e}")


def _safe_get(d: dict, *keys, default=None):
    """Safely traverse nested dicts, handling Yahoo's {raw, fmt} format."""
    val = d
    for k in keys:
        if isinstance(val, dict):
            val = val.get(k)
        else:
            return default
    if isinstance(val, dict) and "raw" in val:
        return val["raw"]
    return val if val is not None else default


def fetch_fundamentals(symbol: str, force: bool = False) -> dict | None:
    """Fetch comprehensive fundamentals from Yahoo Finance direct HTTP.

    Uses quoteSummary with multiple modules for: financials, valuation,
    short interest, analyst targets, institutional ownership, earnings.
    """
    if not force:
        cached = _get_cached(symbol)
        if cached:
            return cached

    from engine.market_data import yahoo_quote_summary

    # Fetch multiple modules in one call
    modules = ",".join([
        "financialData",
        "defaultKeyStatistics",
        "summaryDetail",
        "calendarEvents",
        "recommendationTrend",
        "majorHoldersBreakdown",
        "summaryProfile",
        "earningsTrend",
    ])
    summary = yahoo_quote_summary(symbol, modules=modules)
    if not summary:
        return None

    try:
        fin = summary.get("financialData", {})
        stats = summary.get("defaultKeyStatistics", {})
        detail = summary.get("summaryDetail", {})
        cal = summary.get("calendarEvents", {})
        rec_trend = summary.get("recommendationTrend", {})
        holders = summary.get("majorHoldersBreakdown", {})
        profile = summary.get("summaryProfile", {})
        earn_trend = summary.get("earningsTrend", {})

        # --- Core Valuation ---
        pe_trailing = _safe_get(detail, "trailingPE")
        pe_forward = _safe_get(stats, "forwardPE") or _safe_get(detail, "forwardPE")
        peg_ratio = _safe_get(stats, "pegRatio")
        price_to_book = _safe_get(stats, "priceToBook") or _safe_get(detail, "priceToBook")
        market_cap = _safe_get(detail, "marketCap")
        enterprise_value = _safe_get(stats, "enterpriseValue")

        # --- Earnings ---
        eps_trailing = _safe_get(stats, "trailingEps")
        eps_forward = _safe_get(stats, "forwardEps")

        # --- Growth ---
        revenue_growth = _safe_get(fin, "revenueGrowth")
        earnings_growth = _safe_get(fin, "earningsGrowth")

        # --- Margins ---
        gross_margin = _safe_get(fin, "grossMargins")
        operating_margin = _safe_get(fin, "operatingMargins")
        profit_margin = _safe_get(fin, "profitMargins")
        ebitda_margin = _safe_get(fin, "ebitdaMargins")

        # --- Returns ---
        roe = _safe_get(fin, "returnOnEquity")
        roa = _safe_get(fin, "returnOnAssets")

        # --- Financial Health ---
        debt_to_equity = _safe_get(fin, "debtToEquity")
        current_ratio = _safe_get(fin, "currentRatio")
        quick_ratio = _safe_get(fin, "quickRatio")
        free_cash_flow = _safe_get(fin, "freeCashflow")
        total_revenue = _safe_get(fin, "totalRevenue")
        total_debt = _safe_get(fin, "totalDebt")
        total_cash = _safe_get(fin, "totalCash")

        # --- Short Interest ---
        short_ratio = _safe_get(stats, "shortRatio")  # days to cover
        short_pct_float = _safe_get(stats, "shortPercentOfFloat")
        shares_short = _safe_get(stats, "sharesShort")
        shares_outstanding = _safe_get(stats, "sharesOutstanding") or _safe_get(detail, "sharesOutstanding")
        float_shares = _safe_get(stats, "floatShares")

        # --- Analyst Targets ---
        target_high = _safe_get(fin, "targetHighPrice")
        target_low = _safe_get(fin, "targetLowPrice")
        target_mean = _safe_get(fin, "targetMeanPrice")
        target_median = _safe_get(fin, "targetMedianPrice")
        recommendation = _safe_get(fin, "recommendationKey")
        num_analysts = _safe_get(fin, "numberOfAnalystOpinions")
        current_price = _safe_get(fin, "currentPrice")

        # Upside/downside to consensus
        analyst_upside = None
        if target_mean and current_price and current_price > 0:
            analyst_upside = round((target_mean - current_price) / current_price * 100, 1)

        # --- Recommendation Trend ---
        rec_summary = {}
        trends = rec_trend.get("trend", [])
        if trends:
            current = trends[0] if trends else {}
            rec_summary = {
                "strong_buy": _safe_get(current, "strongBuy", default=0),
                "buy": _safe_get(current, "buy", default=0),
                "hold": _safe_get(current, "hold", default=0),
                "sell": _safe_get(current, "sell", default=0),
                "strong_sell": _safe_get(current, "strongSell", default=0),
            }

        # --- Institutional / Insider Ownership ---
        insider_pct = _safe_get(holders, "insidersPercentHeld")
        institutional_pct = _safe_get(holders, "institutionsPercentHeld")
        institutions_count = _safe_get(holders, "institutionsCount")

        # --- Earnings Date ---
        earnings_dates = cal.get("earnings", {}).get("earningsDate", [])
        next_earnings = None
        days_to_earnings = None
        eps_estimate = None
        if earnings_dates:
            raw = earnings_dates[0].get("raw") or earnings_dates[0].get("fmt")
            if raw:
                if isinstance(raw, (int, float)):
                    next_earnings = datetime.fromtimestamp(raw).strftime("%Y-%m-%d")
                else:
                    next_earnings = str(raw)[:10]
                try:
                    days_to_earnings = (datetime.strptime(next_earnings, "%Y-%m-%d").date() - datetime.now().date()).days
                except (ValueError, TypeError):
                    pass

        # EPS estimate from earningsTrend
        et_trends = earn_trend.get("trend", [])
        for et in et_trends:
            period = _safe_get(et, "period")
            if period == "0q":
                eps_estimate = _safe_get(et, "earningsEstimate", "avg")
                break

        # --- Sector / Industry ---
        sector = profile.get("sector", "Unknown")
        industry = profile.get("industry", "Unknown")
        company_name = profile.get("longName") or profile.get("shortName") or symbol

        # --- Beta ---
        beta = _safe_get(stats, "beta") or _safe_get(detail, "beta")

        # --- 52-Week Range ---
        week52_high = _safe_get(detail, "fiftyTwoWeekHigh")
        week52_low = _safe_get(detail, "fiftyTwoWeekLow")
        week52_pct = None
        if week52_high and week52_low and current_price and (week52_high - week52_low) > 0:
            week52_pct = round((current_price - week52_low) / (week52_high - week52_low) * 100, 1)

        # --- Dividend ---
        dividend_yield = _safe_get(detail, "dividendYield")

        # Build result
        result = {
            "symbol": symbol,
            "company_name": company_name,
            "sector": sector,
            "industry": industry,
            "market_cap": market_cap,
            "enterprise_value": enterprise_value,
            "current_price": current_price,
            # Valuation
            "pe_trailing": _r(pe_trailing),
            "pe_forward": _r(pe_forward),
            "peg_ratio": _r(peg_ratio),
            "price_to_book": _r(price_to_book),
            # Earnings
            "eps_trailing": _r(eps_trailing),
            "eps_forward": _r(eps_forward),
            # Growth
            "revenue_growth": _pct(revenue_growth),
            "earnings_growth": _pct(earnings_growth),
            # Margins
            "gross_margin": _pct(gross_margin),
            "operating_margin": _pct(operating_margin),
            "profit_margin": _pct(profit_margin),
            "ebitda_margin": _pct(ebitda_margin),
            # Returns
            "roe": _pct(roe),
            "roa": _pct(roa),
            # Health
            "debt_to_equity": _r(debt_to_equity),
            "current_ratio": _r(current_ratio),
            "quick_ratio": _r(quick_ratio),
            "free_cash_flow": free_cash_flow,
            "total_revenue": total_revenue,
            "total_debt": total_debt,
            "total_cash": total_cash,
            # Short Interest
            "short_ratio": _r(short_ratio),
            "short_pct_float": _pct(short_pct_float),
            "shares_short": shares_short,
            "shares_outstanding": shares_outstanding,
            "float_shares": float_shares,
            # Analyst Targets
            "target_high": _r(target_high),
            "target_low": _r(target_low),
            "target_mean": _r(target_mean),
            "target_median": _r(target_median),
            "analyst_upside": analyst_upside,
            "recommendation": recommendation,
            "num_analysts": num_analysts,
            "rec_summary": rec_summary,
            # Ownership
            "insider_pct": _pct(insider_pct),
            "institutional_pct": _pct(institutional_pct),
            "institutions_count": institutions_count,
            # Earnings
            "next_earnings": next_earnings,
            "days_to_earnings": days_to_earnings,
            "eps_estimate": _r(eps_estimate),
            # Other
            "beta": _r(beta),
            "week52_high": _r(week52_high),
            "week52_low": _r(week52_low),
            "week52_pct": week52_pct,
            "dividend_yield": _pct(dividend_yield),
            "updated": datetime.now().isoformat(),
        }

        # Compute Smart Score
        score, grade, components = compute_smart_score(result)
        result["smart_score"] = score
        result["grade"] = grade
        result["score_components"] = components

        # Cache and persist
        with _cache_lock:
            _cache[symbol] = {"data": result, "ts": time.time()}
        _save_to_db(symbol, result, score, grade)

        return result

    except Exception as e:
        console.log(f"[red]Fundamentals error for {symbol}: {e}")
        return None


def fetch_all_fundamentals(symbols: list = None) -> list:
    """Fetch fundamentals for all symbols."""
    if symbols is None:
        from config import WATCH_STOCKS
        symbols = WATCH_STOCKS
    results = []
    for sym in symbols:
        data = fetch_fundamentals(sym)
        if data:
            results.append(data)
    return results


# ── Smart Score ──────────────────────────────────────────────────────

SECTOR_PE_AVERAGES = {
    "Technology": 30, "Communication Services": 22, "Consumer Cyclical": 25,
    "Consumer Defensive": 22, "Financial Services": 15, "Healthcare": 20,
    "Industrials": 22, "Energy": 12, "Basic Materials": 18,
    "Real Estate": 35, "Utilities": 18,
}


def compute_smart_score(data: dict) -> tuple:
    """Compute Smart Score (0-100) with A-F grade.

    Components (100 pts total):
    - Valuation (20): P/E vs sector, PEG
    - Growth (20): Revenue + earnings growth
    - Profitability (15): Margins, ROE
    - Financial Health (10): D/E, current ratio, FCF
    - Analyst Consensus (15): Target upside, recommendation
    - Short Interest (10): Low short = better (unless squeeze setup)
    - Insider/Institutional (10): High ownership = alignment
    """
    components = {}
    total = 0

    # --- Valuation (20 pts) ---
    val_score = 10  # neutral
    pe = data.get("pe_trailing")
    sector = data.get("sector", "")
    sector_avg = SECTOR_PE_AVERAGES.get(sector, 22)
    if pe and pe > 0:
        pe_rel = pe / sector_avg
        if pe_rel < 0.5:
            val_score = 20
        elif pe_rel < 0.8:
            val_score = 17
        elif pe_rel < 1.0:
            val_score = 14
        elif pe_rel < 1.3:
            val_score = 10
        elif pe_rel < 2.0:
            val_score = 5
        else:
            val_score = 2
    elif pe and pe < 0:
        val_score = 0
    peg = data.get("peg_ratio")
    if peg and 0 < peg < 1:
        val_score = min(20, val_score + 3)
    elif peg and peg > 3:
        val_score = max(0, val_score - 3)
    components["valuation"] = {"score": val_score, "max": 20, "pe": pe, "sector_avg_pe": sector_avg, "peg": peg}
    total += val_score

    # --- Growth (20 pts) ---
    grow_score = 10
    rev = data.get("revenue_growth")
    earn = data.get("earnings_growth")
    if rev is not None:
        if rev > 30:
            grow_score = 18
        elif rev > 15:
            grow_score = 15
        elif rev > 5:
            grow_score = 12
        elif rev > 0:
            grow_score = 8
        elif rev > -10:
            grow_score = 4
        else:
            grow_score = 1
    if earn is not None and earn > 20:
        grow_score = min(20, grow_score + 3)
    elif earn is not None and earn < -10:
        grow_score = max(0, grow_score - 3)
    components["growth"] = {"score": grow_score, "max": 20, "revenue_growth": rev, "earnings_growth": earn}
    total += grow_score

    # --- Profitability (15 pts) ---
    prof_score = 7
    pm = data.get("profit_margin")
    roe_val = data.get("roe")
    if pm is not None:
        if pm > 25:
            prof_score = 13
        elif pm > 15:
            prof_score = 11
        elif pm > 5:
            prof_score = 8
        elif pm > 0:
            prof_score = 5
        else:
            prof_score = 2
    if roe_val is not None and roe_val > 20:
        prof_score = min(15, prof_score + 3)
    elif roe_val is not None and roe_val < 0:
        prof_score = max(0, prof_score - 3)
    components["profitability"] = {"score": prof_score, "max": 15, "profit_margin": pm, "roe": roe_val}
    total += prof_score

    # --- Financial Health (10 pts) ---
    health_score = 5
    de = data.get("debt_to_equity")
    cr = data.get("current_ratio")
    fcf = data.get("free_cash_flow")
    if de is not None:
        if de < 30:
            health_score = 9
        elif de < 80:
            health_score = 7
        elif de < 150:
            health_score = 4
        else:
            health_score = 2
    if cr is not None and cr > 2:
        health_score = min(10, health_score + 2)
    elif cr is not None and cr < 1:
        health_score = max(0, health_score - 2)
    if fcf is not None and fcf > 0:
        health_score = min(10, health_score + 1)
    components["health"] = {"score": health_score, "max": 10, "debt_to_equity": de, "current_ratio": cr}
    total += health_score

    # --- Analyst Consensus (15 pts) ---
    analyst_score = 7
    upside = data.get("analyst_upside")
    rec = data.get("recommendation")
    if upside is not None:
        if upside > 30:
            analyst_score = 15
        elif upside > 15:
            analyst_score = 12
        elif upside > 5:
            analyst_score = 9
        elif upside > -5:
            analyst_score = 6
        elif upside > -15:
            analyst_score = 3
        else:
            analyst_score = 1
    if rec in ("strongBuy", "strong_buy"):
        analyst_score = min(15, analyst_score + 2)
    elif rec in ("sell", "strongSell", "strong_sell"):
        analyst_score = max(0, analyst_score - 3)
    components["analyst"] = {"score": analyst_score, "max": 15, "upside": upside, "recommendation": rec}
    total += analyst_score

    # --- Short Interest (10 pts) ---
    short_score = 5
    short_pct = data.get("short_pct_float")
    if short_pct is not None:
        if short_pct < 2:
            short_score = 9  # low short = bullish
        elif short_pct < 5:
            short_score = 7
        elif short_pct < 10:
            short_score = 5
        elif short_pct < 20:
            short_score = 3  # heavily shorted
        else:
            short_score = 1  # extreme short
    components["short_interest"] = {"score": short_score, "max": 10, "short_pct": short_pct}
    total += short_score

    # --- Insider/Institutional (10 pts) ---
    own_score = 5
    inst = data.get("institutional_pct")
    ins = data.get("insider_pct")
    if inst is not None:
        if inst > 70:
            own_score = 8  # strong institutional backing
        elif inst > 50:
            own_score = 6
        elif inst > 30:
            own_score = 4
        else:
            own_score = 3
    if ins is not None and ins > 10:
        own_score = min(10, own_score + 3)  # high insider = aligned
    elif ins is not None and ins > 5:
        own_score = min(10, own_score + 1)
    components["ownership"] = {"score": own_score, "max": 10, "institutional_pct": inst, "insider_pct": ins}
    total += own_score

    # Grade
    if total >= 80:
        grade = "A"
    elif total >= 65:
        grade = "B"
    elif total >= 50:
        grade = "C"
    elif total >= 35:
        grade = "D"
    else:
        grade = "F"

    return total, grade, components


# ── Portfolio Health Check ───────────────────────────────────────────

def portfolio_health_check(player_id: str) -> dict | None:
    """Analyze portfolio health: sector concentration, P/E avg, earnings exposure,
    beta, correlation to SPY.
    """
    try:
        conn = sqlite3.connect(DB, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        positions = conn.execute(
            "SELECT symbol, qty, avg_price, asset_type FROM positions WHERE player_id=?",
            (player_id,)
        ).fetchall()
        cash_row = conn.execute(
            "SELECT cash FROM players WHERE player_id=?",
            (player_id,)
        ).fetchone()
        conn.close()

        if not positions:
            return {"player_id": player_id, "status": "no_positions"}

        cash = cash_row["cash"] if cash_row else 0
        total_value = cash

        # Gather fundamentals for each position
        holdings = []
        for pos in positions:
            sym = pos["symbol"]
            qty = pos["qty"]
            avg = pos["avg_price"]
            mkt_val = qty * avg
            total_value += mkt_val

            fund = fetch_fundamentals(sym)
            holdings.append({
                "symbol": sym,
                "qty": qty,
                "market_value": mkt_val,
                "asset_type": pos["asset_type"] or "stock",
                "fundamentals": fund,
            })

        if total_value <= 0:
            return {"player_id": player_id, "status": "zero_value"}

        # --- Sector Concentration ---
        sectors = {}
        for h in holdings:
            f = h["fundamentals"]
            sector = f.get("sector", "Unknown") if f else "Unknown"
            sectors[sector] = sectors.get(sector, 0) + h["market_value"]

        sector_pcts = {s: round(v / total_value * 100, 1) for s, v in sectors.items()}
        max_sector = max(sector_pcts.items(), key=lambda x: x[1]) if sector_pcts else ("Unknown", 0)
        sector_concentration = "HIGH" if max_sector[1] > 50 else "MODERATE" if max_sector[1] > 30 else "LOW"

        # --- Average P/E ---
        pes = []
        for h in holdings:
            f = h["fundamentals"]
            if f and f.get("pe_trailing") and f["pe_trailing"] > 0:
                pes.append(f["pe_trailing"])
        avg_pe = round(sum(pes) / len(pes), 1) if pes else None

        # --- Weighted Beta ---
        betas = []
        weights = []
        for h in holdings:
            f = h["fundamentals"]
            if f and f.get("beta"):
                betas.append(f["beta"])
                weights.append(h["market_value"])
        total_weight = sum(weights)
        weighted_beta = round(sum(b * w for b, w in zip(betas, weights)) / total_weight, 2) if total_weight > 0 else None

        # --- Earnings Exposure ---
        earnings_this_week = []
        for h in holdings:
            f = h["fundamentals"]
            if f and f.get("days_to_earnings") is not None:
                dte = f["days_to_earnings"]
                if 0 <= dte <= 7:
                    earnings_this_week.append({
                        "symbol": h["symbol"],
                        "date": f["next_earnings"],
                        "days": dte,
                        "pct_of_portfolio": round(h["market_value"] / total_value * 100, 1),
                    })

        earnings_exposure = sum(e["pct_of_portfolio"] for e in earnings_this_week)

        # --- Average Smart Score ---
        scores = []
        for h in holdings:
            f = h["fundamentals"]
            if f and f.get("smart_score") is not None:
                scores.append(f["smart_score"])
        avg_score = round(sum(scores) / len(scores)) if scores else None
        avg_grade = _score_to_grade(avg_score) if avg_score else None

        # --- Cash Position ---
        cash_pct = round(cash / total_value * 100, 1) if total_value > 0 else 0

        # --- Short Interest Exposure ---
        high_short = []
        for h in holdings:
            f = h["fundamentals"]
            if f and f.get("short_pct_float") and f["short_pct_float"] > 10:
                high_short.append({
                    "symbol": h["symbol"],
                    "short_pct": f["short_pct_float"],
                })

        return {
            "player_id": player_id,
            "total_value": round(total_value, 2),
            "cash_pct": cash_pct,
            "num_positions": len(holdings),
            "sector_breakdown": sector_pcts,
            "sector_concentration": sector_concentration,
            "top_sector": {"name": max_sector[0], "pct": max_sector[1]},
            "avg_pe": avg_pe,
            "weighted_beta": weighted_beta,
            "earnings_this_week": earnings_this_week,
            "earnings_exposure_pct": round(earnings_exposure, 1),
            "avg_smart_score": avg_score,
            "avg_grade": avg_grade,
            "high_short_interest": high_short,
        }

    except Exception as e:
        console.log(f"[red]Portfolio health check error for {player_id}: {e}")
        return None


# ── Prompt Builders ──────────────────────────────────────────────────

def build_fundamentals_prompt(symbol: str) -> str:
    """Build enriched fundamentals section for AI scan prompt."""
    data = fetch_fundamentals(symbol)
    if not data:
        return ""

    lines = [f"\n--- Fundamentals: {symbol} ({data.get('company_name', symbol)}) [Smart Score: {data.get('grade', '?')} ({data.get('smart_score', '?')}/100)] ---"]

    # Valuation
    pe = data.get("pe_trailing")
    pe_fwd = data.get("pe_forward")
    if pe:
        lines.append(f"  P/E: {pe}" + (f" | Forward P/E: {pe_fwd}" if pe_fwd else ""))

    # EPS
    eps = data.get("eps_trailing")
    eps_fwd = data.get("eps_forward")
    if eps:
        lines.append(f"  EPS: ${eps}" + (f" | Forward: ${eps_fwd}" if eps_fwd else ""))

    # Growth
    rev = data.get("revenue_growth")
    earn = data.get("earnings_growth")
    if rev is not None:
        lines.append(f"  Revenue Growth: {rev:+.1f}%" + (f" | Earnings Growth: {earn:+.1f}%" if earn is not None else ""))

    # Margins
    pm = data.get("profit_margin")
    om = data.get("operating_margin")
    if pm is not None:
        parts = []
        if om is not None:
            parts.append(f"Operating: {om:.1f}%")
        parts.append(f"Net: {pm:.1f}%")
        lines.append(f"  Margins: {' | '.join(parts)}")

    # D/E
    de = data.get("debt_to_equity")
    if de is not None:
        label = "LOW" if de < 50 else "MODERATE" if de < 100 else "HIGH"
        lines.append(f"  Debt/Equity: {de:.1f} [{label}]")

    # Short Interest
    short_pct = data.get("short_pct_float")
    short_ratio = data.get("short_ratio")
    if short_pct is not None:
        label = "LOW" if short_pct < 5 else "MODERATE" if short_pct < 10 else "HIGH" if short_pct < 20 else "EXTREME"
        days_str = f", {short_ratio:.1f} days to cover" if short_ratio else ""
        lines.append(f"  Short Interest: {short_pct:.1f}% of float [{label}]{days_str}")

    # Analyst Targets
    target = data.get("target_mean")
    upside = data.get("analyst_upside")
    rec = data.get("recommendation")
    num = data.get("num_analysts")
    if target:
        lines.append(f"  Analyst Target: ${target:.2f} ({upside:+.1f}% upside) [{(rec or 'N/A').upper()}] ({num or '?'} analysts)")

    # Institutional/Insider
    inst = data.get("institutional_pct")
    ins = data.get("insider_pct")
    if inst is not None:
        lines.append(f"  Ownership: Institutional {inst:.1f}%" + (f" | Insider {ins:.1f}%" if ins is not None else ""))

    # Earnings Warning
    dte = data.get("days_to_earnings")
    if dte is not None and 0 <= dte <= 7:
        lines.append(f"  *** EARNINGS IN {dte} DAYS ({data.get('next_earnings')}) — consider trimming to reduce event risk ***")

    return "\n".join(lines)


def build_sell_fundamentals_prompt(symbol: str) -> str:
    """Build fundamentals context for sell decisions (earnings proximity, analyst downside)."""
    data = fetch_fundamentals(symbol)
    if not data:
        return ""

    warnings = []

    # Earnings proximity
    dte = data.get("days_to_earnings")
    if dte is not None and 0 <= dte <= 5:
        warnings.append(f"EARNINGS IN {dte} DAYS ({data.get('next_earnings')}) — high event risk, consider trimming")

    # Analyst downside
    upside = data.get("analyst_upside")
    if upside is not None and upside < -10:
        warnings.append(f"Analysts see {upside:.1f}% downside (target ${data.get('target_mean', '?')})")

    # High short interest
    short_pct = data.get("short_pct_float")
    if short_pct is not None and short_pct > 15:
        warnings.append(f"Short interest {short_pct:.1f}% of float — high bearish sentiment")

    if not warnings:
        return ""

    return "\n  ** SELL SIGNALS: " + " | ".join(warnings)


# ── Helpers ──────────────────────────────────────────────────────────

def _r(val, decimals=2):
    """Round a value safely."""
    if val is None:
        return None
    try:
        return round(float(val), decimals)
    except (ValueError, TypeError):
        return None


def _pct(val):
    """Convert ratio to percentage (0.15 -> 15.0)."""
    if val is None:
        return None
    try:
        return round(float(val) * 100, 2)
    except (ValueError, TypeError):
        return None


def _score_to_grade(score):
    if score >= 80:
        return "A"
    elif score >= 65:
        return "B"
    elif score >= 50:
        return "C"
    elif score >= 35:
        return "D"
    return "F"
