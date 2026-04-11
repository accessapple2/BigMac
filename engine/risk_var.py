"""
risk_var.py — Portfolio Value-at-Risk and stress testing for USS TradeMinds.

VaR methods:
  Parametric   — mean/std of daily returns × z-score × portfolio value
  Historical   — 5th/1st percentile of actual daily P&L distribution

Stress scenarios (pre-built):
  crash_N      — uniform market drop of N% across all equities
  tech_rotate  — Tech -15%, Defensives +5%
  vix_spike    — VIX→35 impact estimated via position beta
  rate_shock   — 10Y yield +50bps impact via sector sensitivity proxies
  custom       — CIC "stress test -15% tech" arbitrary pct + label

Safety: read-only calculations — no DB mutations except INSERT-only log tables.

Tables (INSERT only):
  var_snapshots   — daily VaR snapshot after calculation
"""
from __future__ import annotations

import logging
import math
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

import numpy as np

logger = logging.getLogger("risk_var")

DB_PATH = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)

# ── Sector classification ──────────────────────────────────────────────────────
# Used for sector-rotation and rate-shock scenarios
_TECH_TICKERS = {
    "NVDA","AMD","MSFT","AAPL","GOOGL","META","AMZN","TSLA","NOW","CRM",
    "AVGO","QCOM","MU","DELL","HPE","PLTR","ORCL","ANET","ADBE","INTC",
    "QQQ","QQQM","XLK","VGT","SMH",
}
_DEFENSIVE_TICKERS = {
    "JNJ","PG","KO","WMT","MCD","ABT","UNH","CVS","PFE","ABBV",
    "XLV","XLP","XLU","VDE","GLD","TLT","IEF","SHY",
}
_FINANCIAL_TICKERS = {
    "JPM","BAC","GS","MS","C","WFC","BLK","XLF","VFH",
}
_ENERGY_TICKERS = {
    "XOM","CVX","COP","SLB","OXY","XLE","VDE",
}

# Rate-shock sector beta proxies (sensitivity to +50bps yield)
# Negative = hurt by rising rates, Positive = benefit
_RATE_SENSITIVITY: dict[str, float] = {
    "TLT": -0.15, "IEF": -0.08, "SHY": -0.02,
    "XLU": -0.08, "REIT": -0.10,
    "XLK": -0.05, "NVDA": -0.05, "MSFT": -0.04, "AAPL": -0.03,
    "XLF": +0.04, "JPM": +0.05, "BAC": +0.05, "GS": +0.04,
    "XLE": +0.02, "XOM": +0.02,
}
_DEFAULT_RATE_SENSITIVITY = -0.02   # for unlisted tickers

# VIX-beta proxies
_VIX_BETA: dict[str, float] = {
    "SPY": 1.0, "QQQ": 1.25, "IWM": 1.3,
    "NVDA": 1.8, "TSLA": 2.0, "AMD": 1.7, "META": 1.4,
    "AAPL": 0.9, "MSFT": 0.85, "GOOGL": 1.1,
    "GLD": -0.3, "TLT": -0.5, "VIX": -1.0,
}
_DEFAULT_VIX_BETA = 1.2


# ── DB ─────────────────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def _init_tables() -> None:
    with _conn() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS var_snapshots (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                portfolio_value REAL    NOT NULL,
                var_95_param    REAL,
                var_99_param    REAL,
                var_95_hist     REAL,
                var_99_hist     REAL,
                daily_vol_pct   REAL,
                position_count  INTEGER,
                top_risk_ticker TEXT,
                snapshot_date   TEXT    DEFAULT (date('now')),
                created_at      TEXT    DEFAULT (datetime('now'))
            )
        """)
        db.commit()


# ── Position loading ───────────────────────────────────────────────────────────

def _get_positions() -> list[dict]:
    """
    Load all AI-player stock positions with live prices.
    Returns list of {player_id, symbol, qty, avg_price, live_price, market_value}.
    """
    db   = _conn()
    rows = db.execute(
        """SELECT p.player_id, p.symbol, p.qty, p.avg_price
           FROM positions p
           JOIN ai_players ap ON ap.id = p.player_id
           WHERE p.qty > 0 AND p.asset_type='stock' AND ap.is_human=0"""
    ).fetchall()
    db.close()

    positions = []
    for r in rows:
        positions.append({
            "player_id":  r["player_id"],
            "symbol":     r["symbol"].upper(),
            "qty":        float(r["qty"]),
            "avg_price":  float(r["avg_price"]),
            "live_price": float(r["avg_price"]),   # filled below
            "market_value": 0.0,
        })

    # Batch live-price fetch via yfinance
    symbols = list({p["symbol"] for p in positions})
    prices: dict[str, float] = {}
    try:
        import yfinance as yf
        tks = yf.Tickers(" ".join(symbols))
        for sym in symbols:
            try:
                info = tks.tickers[sym].fast_info
                p = getattr(info, "last_price", None) or getattr(info, "regular_market_price", None)
                if p:
                    prices[sym] = float(p)
            except Exception:
                pass
    except Exception:
        pass

    for pos in positions:
        lp = prices.get(pos["symbol"], pos["avg_price"])
        pos["live_price"]   = lp
        pos["market_value"] = pos["qty"] * lp

    return positions


def _aggregate_by_symbol(positions: list[dict]) -> dict[str, dict]:
    """Merge multi-player positions into fleet-level per-ticker holdings."""
    agg: dict[str, dict] = {}
    for pos in positions:
        sym = pos["symbol"]
        if sym not in agg:
            agg[sym] = {"symbol": sym, "qty": 0.0, "market_value": 0.0,
                        "live_price": pos["live_price"]}
        agg[sym]["qty"]          += pos["qty"]
        agg[sym]["market_value"] += pos["market_value"]
    return agg


# ── Historical return fetch ────────────────────────────────────────────────────

def _historical_returns(symbols: list[str], days: int = 30) -> dict[str, list[float]]:
    """
    Return dict of symbol → list of daily pct returns (most recent first).
    Falls back to DB market_snapshots if yfinance unavailable.
    """
    returns: dict[str, list[float]] = {}
    end   = datetime.now()
    start = end - timedelta(days=days + 10)

    # Try yfinance first
    try:
        import yfinance as yf
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            data = yf.download(
                symbols, start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
                auto_adjust=True, progress=False,
            )
        if not data.empty:
            close = data["Close"] if "Close" in data.columns else data
            pct   = close.pct_change().dropna()
            for sym in symbols:
                try:
                    col = pct[sym] if sym in pct.columns else pct.iloc[:, 0]
                    returns[sym] = list(col.dropna().values[-days:])
                except Exception:
                    pass
            if returns:
                return returns
    except Exception:
        pass

    # Fallback: DB market_snapshots
    db = _conn()
    cutoff = (datetime.now() - timedelta(days=days + 10)).strftime("%Y-%m-%d")
    for sym in symbols:
        try:
            rows = db.execute(
                "SELECT close FROM market_snapshots WHERE symbol=? AND date >= ? ORDER BY date ASC",
                (sym, cutoff),
            ).fetchall()
            closes = [float(r["close"]) for r in rows if r["close"]]
            if len(closes) >= 2:
                rets = [(closes[i] - closes[i-1]) / closes[i-1]
                        for i in range(1, len(closes))]
                returns[sym] = rets[-days:]
        except Exception:
            pass
    db.close()
    return returns


# ── VaR calculation ────────────────────────────────────────────────────────────

def calculate_var(days: int = 30) -> dict:
    """
    Calculate portfolio VaR (95% and 99%) using both parametric and historical.
    Returns full VaR dict with per-position breakdown.
    """
    _init_tables()
    positions  = _get_positions()
    if not positions:
        return {"error": "No positions found", "var_95_param": 0, "var_99_param": 0,
                "var_95_hist": 0, "var_99_hist": 0, "portfolio_value": 0}

    agg        = _aggregate_by_symbol(positions)
    symbols    = list(agg.keys())
    port_value = sum(p["market_value"] for p in agg.values())

    if port_value <= 0:
        return {"error": "Portfolio value is zero", "portfolio_value": 0}

    # Weights
    weights = {sym: agg[sym]["market_value"] / port_value for sym in symbols}

    # Historical returns
    hist_returns = _historical_returns(symbols, days)

    # ── Parametric VaR ────────────────────────────────────────────────────────
    # Weighted portfolio daily return series
    # For each day, port_ret = sum(weight * ticker_ret)
    n_days  = days
    all_dates: dict[int, float] = {}
    per_sym_vols: dict[str, float] = {}

    for sym, rets in hist_returns.items():
        if not rets:
            continue
        arr = np.array(rets)
        per_sym_vols[sym] = float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0
        w = weights.get(sym, 0)
        for i, r in enumerate(rets[-n_days:]):
            all_dates[i] = all_dates.get(i, 0) + w * r

    port_daily_rets = list(all_dates.values())

    if not port_daily_rets:
        port_vol = 0.01   # fallback 1% daily vol assumption
    else:
        port_vol = float(np.std(port_daily_rets, ddof=1)) if len(port_daily_rets) > 1 else 0.01

    z95 = 1.645
    z99 = 2.326
    var_95_param = port_value * z95 * port_vol
    var_99_param = port_value * z99 * port_vol

    # ── Historical VaR ────────────────────────────────────────────────────────
    if len(port_daily_rets) >= 5:
        sorted_rets = sorted(port_daily_rets)
        idx_95 = max(0, int(len(sorted_rets) * 0.05))
        idx_99 = max(0, int(len(sorted_rets) * 0.01))
        var_95_hist = abs(sorted_rets[idx_95]) * port_value
        var_99_hist = abs(sorted_rets[idx_99]) * port_value
    else:
        var_95_hist = var_95_param
        var_99_hist = var_99_param

    # ── Per-position risk contribution ────────────────────────────────────────
    pos_risks = []
    for sym, data in agg.items():
        vol   = per_sym_vols.get(sym, port_vol)
        w     = weights.get(sym, 0)
        risk_usd = data["market_value"] * z95 * vol
        pos_risks.append({
            "symbol":        sym,
            "market_value":  round(data["market_value"], 2),
            "weight_pct":    round(w * 100, 2),
            "daily_vol_pct": round(vol * 100, 2),
            "var_95":        round(risk_usd, 2),
        })
    pos_risks.sort(key=lambda x: x["var_95"], reverse=True)
    top_risk = pos_risks[0]["symbol"] if pos_risks else None

    # ── Store snapshot ────────────────────────────────────────────────────────
    with _conn() as db:
        db.execute(
            """INSERT INTO var_snapshots
               (portfolio_value, var_95_param, var_99_param,
                var_95_hist, var_99_hist, daily_vol_pct, position_count, top_risk_ticker)
               VALUES (?,?,?,?,?,?,?,?)""",
            (round(port_value, 2), round(var_95_param, 2), round(var_99_param, 2),
             round(var_95_hist, 2), round(var_99_hist, 2),
             round(port_vol * 100, 4), len(pos_risks), top_risk),
        )
        db.commit()

    # Risk gauge: green / yellow / red
    var_pct = var_95_param / port_value if port_value > 0 else 0
    if var_pct < 0.02:
        gauge = "green"
    elif var_pct < 0.04:
        gauge = "yellow"
    else:
        gauge = "red"

    return {
        "portfolio_value":  round(port_value, 2),
        "var_95_param":     round(var_95_param, 2),
        "var_99_param":     round(var_99_param, 2),
        "var_95_hist":      round(var_95_hist, 2),
        "var_99_hist":      round(var_99_hist, 2),
        "daily_vol_pct":    round(port_vol * 100, 2),
        "position_count":   len(pos_risks),
        "top_risk_ticker":  top_risk,
        "risk_gauge":       gauge,
        "positions":        pos_risks,
        "calculated_at":    datetime.now().isoformat()[:19],
    }


# ── Stress scenarios ───────────────────────────────────────────────────────────

def _classify_ticker(sym: str) -> str:
    s = sym.upper()
    if s in _TECH_TICKERS:        return "tech"
    if s in _DEFENSIVE_TICKERS:   return "defensive"
    if s in _FINANCIAL_TICKERS:   return "financial"
    if s in _ENERGY_TICKERS:      return "energy"
    return "equity"


def run_stress(scenario: str, param: float = 0.0,
               custom_label: str = "") -> dict:
    """
    Run a stress scenario.
    scenario: 'crash' | 'tech_rotate' | 'vix_spike' | 'rate_shock' | 'custom'
    param:    for 'crash' → drop% (positive means drop, e.g. 10 = -10%)
              for 'vix_spike' → target VIX level (e.g. 35)
              for 'rate_shock' → bps (e.g. 50)
              for 'custom' → pct change (e.g. -15 = -15%)
    """
    positions = _get_positions()
    if not positions:
        return {"error": "No positions"}

    agg        = _aggregate_by_symbol(positions)
    port_value = sum(p["market_value"] for p in agg.values())
    results    = []

    for sym, data in agg.items():
        mv   = data["market_value"]
        pct  = 0.0
        cls  = _classify_ticker(sym)

        if scenario == "crash":
            drop = -(abs(param) / 100.0)
            pct  = drop  # uniform across all

        elif scenario == "tech_rotate":
            if cls == "tech":
                pct = -0.15
            elif cls == "defensive":
                pct = +0.05
            else:
                pct = -0.05   # everything else mildly down

        elif scenario == "vix_spike":
            # VIX goes from ~15 baseline to target; each +1 VIX ≈ -0.5% on beta-1 stock
            current_vix = 15.0
            target_vix  = max(param, current_vix)
            vix_delta   = target_vix - current_vix
            beta        = _VIX_BETA.get(sym, _DEFAULT_VIX_BETA)
            pct         = -(vix_delta * 0.005) * beta   # -0.5% per VIX point × beta

        elif scenario == "rate_shock":
            bps         = abs(param)   # e.g. 50
            sensitivity = _RATE_SENSITIVITY.get(sym, _DEFAULT_RATE_SENSITIVITY)
            pct         = sensitivity * (bps / 50.0)   # normalised to 50bps baseline

        elif scenario == "custom":
            label_lower = custom_label.lower()
            req_cls = None
            if "tech" in label_lower:      req_cls = "tech"
            elif "defensive" in label_lower: req_cls = "defensive"
            elif "financial" in label_lower: req_cls = "financial"
            elif "energy" in label_lower:  req_cls = "energy"

            if req_cls is None or cls == req_cls:
                pct = param / 100.0
            else:
                pct = 0.0

        delta = mv * pct
        results.append({
            "symbol":     sym,
            "market_value": round(mv, 2),
            "sector":     cls,
            "shock_pct":  round(pct * 100, 2),
            "pnl":        round(delta, 2),
        })

    results.sort(key=lambda x: x["pnl"])
    total_pnl     = sum(r["pnl"] for r in results)
    worst_hit     = results[0]   if results else None
    best_hedge    = max(results, key=lambda x: x["pnl"]) if results else None
    if best_hedge and best_hedge["pnl"] <= 0:
        best_hedge = None  # no actual hedge

    scenario_label = {
        "crash":       f"Market Crash -{param:.0f}%",
        "tech_rotate": "Sector Rotation (Tech -15% / Defensives +5%)",
        "vix_spike":   f"VIX Spike to {param:.0f}",
        "rate_shock":  f"Rate Shock +{param:.0f}bps",
        "custom":      custom_label or f"Custom {param:+.1f}%",
    }.get(scenario, scenario)

    return {
        "scenario":        scenario_label,
        "portfolio_value": round(port_value, 2),
        "estimated_pnl":   round(total_pnl, 2),
        "pnl_pct":         round(total_pnl / port_value * 100, 2) if port_value else 0,
        "worst_hit":       worst_hit,
        "best_hedge":      best_hedge,
        "positions":       results,
        "run_at":          datetime.now().isoformat()[:19],
    }


def run_all_scenarios() -> dict:
    """Quick summary of all pre-built scenarios."""
    return {
        "crash_5":    run_stress("crash",       5.0),
        "crash_10":   run_stress("crash",      10.0),
        "crash_20":   run_stress("crash",      20.0),
        "tech_rotate": run_stress("tech_rotate", 0.0),
        "vix_35":     run_stress("vix_spike",   35.0),
        "rate_50bps": run_stress("rate_shock",  50.0),
        "generated_at": datetime.now().isoformat()[:19],
    }


# ── VaR history ───────────────────────────────────────────────────────────────

def get_var_history(days: int = 30) -> list[dict]:
    _init_tables()
    db  = _conn()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = db.execute(
        """SELECT snapshot_date, portfolio_value, var_95_param, var_99_param,
                  var_95_hist, daily_vol_pct, top_risk_ticker, created_at
           FROM var_snapshots
           WHERE snapshot_date >= ?
           ORDER BY snapshot_date ASC""",
        (cutoff,),
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]
