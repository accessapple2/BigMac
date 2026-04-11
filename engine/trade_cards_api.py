"""
TradeMinds — Trade Cards API
=============================
FastAPI APIRouter serving trade card data for the Command Center dashboard.

Endpoints
---------
GET /api/trade-cards          Latest debate + scenario data for all tickers
GET /api/trade-cards/{ticker} Single ticker card
GET /api/strategy-picks       Latest strategy signals
GET /api/fast-scan            Latest universe scan results + dynamic alerts

Register in dashboard/app.py:
    from engine.trade_cards_api import router as trade_cards_router
    app.include_router(trade_cards_router, tags=["Trade Cards"])
"""

import json
import math
import sqlite3
from datetime import datetime
from fastapi import APIRouter, Query
from pydantic import BaseModel
from typing import Optional

router = APIRouter()

DB_PATH    = "data/trader.db"
UOA_DB     = "trader.db"          # UOA tables live in the root-level DB
WEBULL_PID = "steve-webull"

# Confluence: min distinct alert_types in last 7d to mark as "actionable"
UOA_CONFLUENCE_THRESHOLD = 4


def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _uoa_db():
    conn = sqlite3.connect(UOA_DB)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Trade Cards  (debate_history_v2 + scenario_models)
# ---------------------------------------------------------------------------

@router.get("/api/trade-cards")
def get_trade_cards(limit: int = Query(50, le=200)):
    """
    Return the most recent debate card per ticker, joined with its latest
    scenario model when available.
    """
    conn = _db()
    try:
        rows = conn.execute("""
            SELECT
                d.id            AS debate_id,
                d.ticker,
                d.picard_decision,
                d.picard_conviction,
                d.adjusted_conviction,
                d.risk_rating,
                d.risk_override,
                d.picard_synthesis,
                d.spock_assessment,
                d.crusher_assessment,
                d.scotty_assessment,
                d.bull_avg_conviction,
                d.bear_avg_conviction,
                d.agent_count,
                d.created_at    AS debated_at,
                s.horizon_days,
                s.current_price,
                s.bull_probability,
                s.bull_target,
                s.bull_return_pct,
                s.bull_catalyst,
                s.base_probability,
                s.base_target,
                s.base_return_pct,
                s.base_thesis,
                s.bear_probability,
                s.bear_target,
                s.bear_return_pct,
                s.bear_catalyst,
                s.expected_value_pct,
                s.regime
            FROM debate_history_v2 d
            LEFT JOIN scenario_models s
                ON s.ticker = d.ticker
                AND s.id = (
                    SELECT id FROM scenario_models
                    WHERE ticker = d.ticker
                    ORDER BY created_at DESC LIMIT 1
                )
            WHERE d.id IN (
                SELECT MAX(id) FROM debate_history_v2 GROUP BY ticker
            )
            ORDER BY d.created_at DESC
            LIMIT ?
        """, (limit,)).fetchall()

        cards = []
        for r in rows:
            card = dict(r)
            # Conviction badge colour
            conv = card.get("adjusted_conviction") or card.get("picard_conviction") or 5
            if conv >= 8:
                card["conviction_color"] = "green"
            elif conv >= 6:
                card["conviction_color"] = "yellow"
            elif conv >= 4:
                card["conviction_color"] = "orange"
            else:
                card["conviction_color"] = "red"

            # Risk badge colour
            risk = (card.get("risk_rating") or "MEDIUM").upper()
            card["risk_color"] = {
                "LOW": "green", "MEDIUM": "yellow",
                "HIGH": "orange", "CRITICAL": "red",
            }.get(risk, "yellow")

            cards.append(card)

        return {"cards": cards, "count": len(cards)}
    finally:
        conn.close()


@router.get("/api/trade-cards/{ticker}")
def get_trade_card(ticker: str):
    """Single ticker: latest debate + all scenario models + agent verdicts."""
    ticker = ticker.upper()
    conn = _db()
    try:
        debate = conn.execute("""
            SELECT * FROM debate_history_v2
            WHERE ticker = ?
            ORDER BY id DESC LIMIT 1
        """, (ticker,)).fetchone()

        if not debate:
            return {"error": f"No debate found for {ticker}"}

        debate_dict = dict(debate)
        debate_id = debate_dict["id"]

        verdicts = conn.execute("""
            SELECT agent_name, side, lens, model, conviction, thesis, key_data_point
            FROM debate_agent_verdicts
            WHERE debate_id = ?
            ORDER BY side, conviction DESC
        """, (debate_id,)).fetchall()

        scenarios = conn.execute("""
            SELECT * FROM scenario_models
            WHERE ticker = ?
            ORDER BY created_at DESC LIMIT 5
        """, (ticker,)).fetchall()

        return {
            "ticker": ticker,
            "debate": debate_dict,
            "verdicts": [dict(v) for v in verdicts],
            "scenarios": [dict(s) for s in scenarios],
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Webull Command — supporting endpoints
# ---------------------------------------------------------------------------

@router.get("/api/rebalance-recs")
def get_rebalance_recs(limit: int = Query(20, le=100)):
    """Latest rebalance recommendations (most recent run only)."""
    conn = _db()
    try:
        latest_ts = conn.execute(
            "SELECT MAX(created_at) FROM rebalance_recommendations"
        ).fetchone()[0]
        if not latest_ts:
            return {"recs": [], "generated_at": None}

        # Allow a 60-second window so the full batch is captured
        rows = conn.execute("""
            SELECT symbol, action, shares, price, dollar_amount,
                   pct_of_portfolio, urgency, rationale, executed, created_at
            FROM rebalance_recommendations
            WHERE created_at >= datetime(?, '-60 seconds')
            ORDER BY
                CASE urgency WHEN 'HIGH' THEN 0 WHEN 'MEDIUM' THEN 1 ELSE 2 END,
                ABS(dollar_amount) DESC
            LIMIT ?
        """, (latest_ts, limit)).fetchall()
        return {"recs": [dict(r) for r in rows], "generated_at": latest_ts}
    finally:
        conn.close()


@router.get("/api/pipeline-health")
def get_pipeline_health():
    """Latest pipeline run status and timing."""
    conn = _db()
    try:
        row = conn.execute("""
            SELECT player_id, tickers_scanned, tickers_debated,
                   total_seconds, error, created_at
            FROM pipeline_runs ORDER BY created_at DESC LIMIT 1
        """).fetchone()

        # Last scan times for key tables
        scans = {}
        for table, ts_col in [
            ("universe_scan",   "created_at"),
            ("dynamic_alerts",  "triggered_at"),
            ("signals",         "created_at"),
            ("strategy_signals","created_at"),
            ("debate_history_v2","created_at"),
        ]:
            r = conn.execute(
                f"SELECT MAX({ts_col}) FROM {table}"
            ).fetchone()[0]
            scans[table] = r

        pending_alerts = conn.execute(
            "SELECT COUNT(*) FROM dynamic_alerts WHERE triggered_at >= datetime('now','-1 hour')"
        ).fetchone()[0]

        return {
            "last_run": dict(row) if row else None,
            "last_scan_times": scans,
            "pending_alerts_1h": pending_alerts,
        }
    finally:
        conn.close()


@router.get("/api/portfolio-history")
def get_portfolio_history(days: int = Query(30, le=365)):
    """Webull (steve-webull) portfolio value history for P&L chart."""
    conn = _db()
    try:
        rows = conn.execute("""
            SELECT DATE(recorded_at) AS date,
                   AVG(total_value)  AS total_value,
                   AVG(cash)         AS cash
            FROM portfolio_history
            WHERE player_id = ?
              AND recorded_at >= datetime('now', ? || ' days')
            GROUP BY DATE(recorded_at)
            ORDER BY date ASC
        """, (WEBULL_PID, f"-{days}")).fetchall()
        return {"history": [dict(r) for r in rows], "player_id": WEBULL_PID}
    finally:
        conn.close()


@router.get("/api/covered-calls")
def get_covered_calls(symbols: str = ""):
    """Theta opportunities for the given symbols (comma-separated)."""
    conn = _db()
    try:
        tickers = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        if not tickers:
            return {"opportunities": []}
        placeholders = ",".join("?" * len(tickers))
        rows = conn.execute(f"""
            SELECT ticker, strategy_type, short_strike_call, expiration, dte,
                   iv_rank, estimated_daily_theta, max_risk, theta_score,
                   spot_price, detected_at
            FROM theta_opportunities
            WHERE ticker IN ({placeholders})
              AND strategy_type LIKE '%covered%'
            ORDER BY detected_at DESC, theta_score DESC
            LIMIT 20
        """, tickers).fetchall()

        # Fall back to any strategy if no covered calls
        if not rows:
            rows = conn.execute(f"""
                SELECT ticker, strategy_type, short_strike_call, expiration, dte,
                       iv_rank, estimated_daily_theta, max_risk, theta_score,
                       spot_price, detected_at
                FROM theta_opportunities
                WHERE ticker IN ({placeholders})
                ORDER BY detected_at DESC, theta_score DESC
                LIMIT 20
            """, tickers).fetchall()

        return {"opportunities": [dict(r) for r in rows]}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Strategy Picker  (strategy_signals)
# ---------------------------------------------------------------------------

@router.get("/api/strategy-picks")
def get_strategy_picks(
    signal_type: Optional[str] = None,
    limit: int = Query(100, le=500),
):
    """
    Latest strategy signals, grouped by ticker. Optionally filter by
    signal_type (BUY / SELL).
    """
    conn = _db()
    try:
        where = "WHERE 1=1"
        params: list = []

        if signal_type:
            where += " AND signal_type = ?"
            params.append(signal_type.upper())

        rows = conn.execute(f"""
            SELECT
                ticker,
                strategy_name,
                signal_type,
                ROUND(confidence * 100) AS confidence_pct,
                entry_price,
                target_price,
                stop_price,
                ROUND((target_price - entry_price) / entry_price * 100, 2) AS upside_pct,
                ROUND((entry_price - stop_price) / entry_price * 100, 2)   AS risk_pct,
                notes,
                scan_date,
                created_at
            FROM strategy_signals
            {where}
            ORDER BY created_at DESC, confidence DESC
            LIMIT ?
        """, (*params, limit)).fetchall()

        # Group strategies per ticker
        ticker_map: dict = {}
        for r in rows:
            d = dict(r)
            t = d["ticker"]
            if t not in ticker_map:
                ticker_map[t] = {
                    "ticker": t,
                    "entry_price": d["entry_price"],
                    "target_price": d["target_price"],
                    "stop_price": d["stop_price"],
                    "upside_pct": d["upside_pct"],
                    "risk_pct": d["risk_pct"],
                    "scan_date": d["scan_date"],
                    "strategies": [],
                }
            ticker_map[t]["strategies"].append({
                "name": d["strategy_name"],
                "signal": d["signal_type"],
                "confidence_pct": d["confidence_pct"],
                "notes": d["notes"],
            })

        picks = sorted(
            ticker_map.values(),
            key=lambda x: len(x["strategies"]),
            reverse=True,
        )
        return {"picks": picks, "count": len(picks)}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Fast Scanner  (universe_scan + dynamic_alerts)
# ---------------------------------------------------------------------------

@router.get("/api/fast-scan")
def get_fast_scan(
    min_score: int = Query(0, ge=0, le=100),
    limit: int = Query(100, le=500),
):
    """
    Latest universe scan results ordered by score, with any recent dynamic
    alerts for the same tickers appended.
    """
    conn = _db()
    try:
        scan_rows = conn.execute("""
            SELECT
                u.ticker,
                u.scan_date,
                u.close      AS price,
                u.volume,
                ROUND(u.volume_ratio, 2) AS vol_ratio,
                ROUND(u.rsi, 1)          AS rsi,
                u.score,
                ROUND(u.gap_pct, 2)      AS gap_pct,
                u.signals
            FROM universe_scan u
            INNER JOIN (
                SELECT ticker, MAX(scan_date) AS latest
                FROM universe_scan
                GROUP BY ticker
            ) latest ON u.ticker = latest.ticker AND u.scan_date = latest.latest
            WHERE u.score >= ?
            ORDER BY u.score DESC, u.volume_ratio DESC
            LIMIT ?
        """, (min_score, limit)).fetchall()

        tickers = [r["ticker"] for r in scan_rows]

        # Fetch recent alerts for scanned tickers
        alerts_map: dict = {}
        if tickers:
            placeholders = ",".join("?" * len(tickers))
            alert_rows = conn.execute(f"""
                SELECT symbol, alert_type, message, severity, price, triggered_at
                FROM dynamic_alerts
                WHERE symbol IN ({placeholders})
                ORDER BY triggered_at DESC
            """, tickers).fetchall()
            for a in alert_rows:
                sym = a["symbol"]
                if sym not in alerts_map:
                    alerts_map[sym] = []
                if len(alerts_map[sym]) < 3:          # max 3 alerts per ticker
                    alerts_map[sym].append(dict(a))

        results = []
        for r in scan_rows:
            d = dict(r)
            try:
                d["signals"] = json.loads(d["signals"] or "[]")
            except (json.JSONDecodeError, TypeError):
                d["signals"] = []
            d["alerts"] = alerts_map.get(d["ticker"], [])

            # RSI zone label
            rsi = d.get("rsi") or 50
            if rsi >= 70:
                d["rsi_zone"] = "overbought"
            elif rsi <= 30:
                d["rsi_zone"] = "oversold"
            else:
                d["rsi_zone"] = "neutral"

            results.append(d)

        return {"results": results, "count": len(results)}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Options Flow helpers  (reads from UOA_DB = trader.db)
# ---------------------------------------------------------------------------

def _options_flow_for_tickers(tickers: list[str]) -> dict[str, dict]:
    """
    Batch-fetch 7-day UOA summary for a list of tickers.
    Returns {ticker: flow_dict}.
    """
    if not tickers:
        return {}

    conn = _uoa_db()
    ph   = ",".join("?" * len(tickers))
    try:
        # ── Aggregate from uoa_flow ─────────────────────────────────────
        flow_rows = conn.execute(f"""
            SELECT
                ticker,
                contract_type,
                SUM(CASE WHEN vol_oi_ratio >= 2 THEN volume ELSE 0 END) AS unusual_vol,
                SUM(premium_total)    AS total_premium,
                MAX(premium_total)    AS biggest_premium,
                AVG(implied_volatility) AS avg_iv,
                COUNT(*)              AS contract_count
            FROM uoa_flow
            WHERE ticker IN ({ph})
              AND created_at >= datetime('now', '-7 days')
            GROUP BY ticker, contract_type
        """, tickers).fetchall()

        # ── Top single flow (biggest contract) ─────────────────────────
        top_flow_rows = conn.execute(f"""
            SELECT f.ticker, f.contract_type, f.strike, f.expiration,
                   f.dte, f.premium_total, f.vol_oi_ratio,
                   f.implied_volatility, f.bid, f.ask, f.underlying_price,
                   f.moneyness, f.pct_otm, f.sentiment
            FROM uoa_flow f
            INNER JOIN (
                SELECT ticker, MAX(premium_total) AS max_prem
                FROM uoa_flow
                WHERE ticker IN ({ph})
                  AND created_at >= datetime('now', '-7 days')
                GROUP BY ticker
            ) top ON f.ticker = top.ticker AND f.premium_total = top.max_prem
            GROUP BY f.ticker
        """, tickers).fetchall()

        # ── Alert confluence (distinct alert_type count per ticker) ─────
        alert_rows = conn.execute(f"""
            SELECT ticker,
                   COUNT(DISTINCT alert_type) AS signal_count,
                   MAX(convergence_score)     AS max_convergence,
                   SUM(CASE WHEN severity IN ('HIGH','CRITICAL') THEN 1 ELSE 0 END) AS high_alerts,
                   MAX(description)           AS top_description,
                   MAX(created_at)            AS latest_alert
            FROM uoa_alerts
            WHERE ticker IN ({ph})
              AND created_at >= datetime('now', '-7 days')
            GROUP BY ticker
        """, tickers).fetchall()

    finally:
        conn.close()

    # ── Build per-ticker dicts ──────────────────────────────────────────
    flow_map:  dict[str, dict] = {}
    for r in flow_rows:
        t = r["ticker"]
        if t not in flow_map:
            flow_map[t] = {"call_unusual_vol": 0, "put_unusual_vol": 0,
                           "call_premium": 0.0, "put_premium": 0.0,
                           "avg_iv": None}
        side = r["contract_type"].lower()
        flow_map[t][f"{side}_unusual_vol"] = r["unusual_vol"] or 0
        flow_map[t][f"{side}_premium"]     = round(r["total_premium"] or 0, 0)
        if r["avg_iv"]:
            flow_map[t]["avg_iv"] = round(r["avg_iv"], 4)

    top_map: dict[str, dict] = {r["ticker"]: dict(r) for r in top_flow_rows}
    alrt_map: dict[str, dict] = {r["ticker"]: dict(r) for r in alert_rows}

    result: dict[str, dict] = {}
    for ticker in tickers:
        fm   = flow_map.get(ticker, {})
        top  = top_map.get(ticker)
        alrt = alrt_map.get(ticker, {})

        call_prem = fm.get("call_premium", 0) or 0
        put_prem  = fm.get("put_premium",  0) or 0
        total_unusual = (fm.get("call_unusual_vol", 0) or 0) + \
                        (fm.get("put_unusual_vol",  0) or 0)

        if call_prem + put_prem > 0:
            dominant = "CALLS" if call_prem >= put_prem else "PUTS"
            dom_ratio = round(
                max(call_prem, put_prem) / (call_prem + put_prem) * 100, 1
            )
        else:
            dominant  = None
            dom_ratio = None

        signal_count  = alrt.get("signal_count",    0) or 0
        max_conv      = alrt.get("max_convergence",  0) or 0
        actionable    = signal_count >= UOA_CONFLUENCE_THRESHOLD

        biggest = None
        if top:
            biggest = {
                "contract_type":  top["contract_type"],
                "strike":         top["strike"],
                "expiration":     top["expiration"],
                "dte":            top["dte"],
                "premium_total":  top["premium_total"],
                "vol_oi_ratio":   round(top["vol_oi_ratio"] or 0, 2),
                "implied_vol":    round(top["implied_volatility"] or 0, 4),
                "moneyness":      top["moneyness"],
                "pct_otm":        top["pct_otm"],
                "sentiment":      top["sentiment"],
                "underlying":     top["underlying_price"],
            }

        result[ticker] = {
            "ticker":           ticker,
            "total_unusual_vol": total_unusual,
            "call_unusual_vol":  fm.get("call_unusual_vol", 0),
            "put_unusual_vol":   fm.get("put_unusual_vol",  0),
            "call_premium_7d":   call_prem,
            "put_premium_7d":    put_prem,
            "dominant_direction": dominant,
            "dominant_pct":      dom_ratio,
            "avg_iv":            fm.get("avg_iv"),
            "biggest_flow":      biggest,
            "signal_count":      signal_count,
            "max_convergence":   round(max_conv, 1),
            "high_alert_count":  alrt.get("high_alerts", 0),
            "top_alert_desc":    alrt.get("top_description"),
            "latest_alert":      alrt.get("latest_alert"),
            "actionable":        actionable,
        }

    # Fill blanks for tickers with no data
    for ticker in tickers:
        if ticker not in result:
            result[ticker] = {
                "ticker": ticker, "total_unusual_vol": 0,
                "actionable": False, "dominant_direction": None,
            }

    return result


# ---------------------------------------------------------------------------
# Options Flow endpoints
# ---------------------------------------------------------------------------

@router.get("/api/options-flow/{ticker}")
def get_options_flow(ticker: str):
    """Full 7-day UOA flow summary for a single ticker."""
    ticker = ticker.upper()
    flow   = _options_flow_for_tickers([ticker])
    data   = flow.get(ticker, {"ticker": ticker})

    # Also pull the top 10 individual alerts for display
    conn = _uoa_db()
    try:
        alerts = conn.execute("""
            SELECT severity, alert_type, contract_type, strike, expiration,
                   vol_oi_ratio, premium_total, convergence_score, description,
                   created_at
            FROM uoa_alerts
            WHERE ticker = ?
              AND created_at >= datetime('now', '-7 days')
            ORDER BY
                CASE severity
                    WHEN 'CRITICAL' THEN 0 WHEN 'HIGH' THEN 1
                    WHEN 'MEDIUM'   THEN 2 ELSE 3 END,
                premium_total DESC
            LIMIT 10
        """, (ticker,)).fetchall()
        data["alerts"] = [dict(a) for a in alerts]
    finally:
        conn.close()

    return data


@router.get("/api/options-flow")
def get_options_flow_batch(tickers: str = Query(..., description="Comma-separated tickers")):
    """Batch UOA flow summary for multiple tickers."""
    ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    if not ticker_list:
        return {"flows": {}}
    flows = _options_flow_for_tickers(ticker_list)
    return {"flows": flows, "count": len(flows)}


# ---------------------------------------------------------------------------
# Wheel strategy (Counselor Troi) — covered call + CSP opportunities
# ---------------------------------------------------------------------------

def _bs_call_approx(S: float, K: float, T: float, iv: float) -> float:
    """
    Simplified Black-Scholes approximation for a European call.
    T  = time in years, iv = annualised implied volatility (e.g. 0.50 = 50%).
    Accurate to ~5% for near-ATM options; sufficient for screening.
    """
    if T <= 0 or iv <= 0 or S <= 0 or K <= 0:
        return 0.0
    vol_sqrt_T = iv * math.sqrt(T)
    d1 = (math.log(S / K) + 0.5 * iv ** 2 * T) / vol_sqrt_T
    d2 = d1 - vol_sqrt_T

    def _N(x: float) -> float:          # standard normal CDF approximation
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))

    return round(S * _N(d1) - K * _N(d2), 2)


def _wheel_for_ticker(ticker: str, price: float, iv: float) -> dict:
    """
    Compute best covered-call and CSP opportunities.

    Priority order:
    1. Use real bid prices from uoa_flow (most accurate).
    2. Fall back to Black-Scholes with uoa_flow IV.
    3. Fall back to B-S with a default 45% IV if no data exists.
    """
    DEFAULT_IV  = 0.45
    DTE_MIN, DTE_MAX = 14, 45
    CC_OTM_MIN, CC_OTM_MAX   = 0.03, 0.12   # 3–12% OTM for covered calls
    CSP_OTM_MIN, CSP_OTM_MAX = 0.05, 0.18   # 5–18% OTM for puts

    conn = _uoa_db()
    try:
        # ── Real contract data ──────────────────────────────────────────
        cc_rows = conn.execute("""
            SELECT strike, expiration, dte, bid, ask, implied_volatility,
                   pct_otm, vol_oi_ratio, underlying_price
            FROM uoa_flow
            WHERE ticker = ?
              AND contract_type = 'CALL'
              AND moneyness = 'OTM'
              AND dte BETWEEN ? AND ?
              AND pct_otm BETWEEN ? AND ?
              AND bid > 0
              AND created_at >= datetime('now', '-7 days')
            ORDER BY
                ABS(dte - 30) ASC,     -- prefer ~30 DTE
                bid DESC               -- then highest premium
            LIMIT 5
        """, (ticker, DTE_MIN, DTE_MAX,
              CC_OTM_MIN * 100, CC_OTM_MAX * 100)).fetchall()

        csp_rows = conn.execute("""
            SELECT strike, expiration, dte, bid, ask, implied_volatility,
                   pct_otm, vol_oi_ratio, underlying_price
            FROM uoa_flow
            WHERE ticker = ?
              AND contract_type = 'PUT'
              AND moneyness = 'OTM'
              AND dte BETWEEN ? AND ?
              AND pct_otm BETWEEN ? AND ?
              AND bid > 0
              AND created_at >= datetime('now', '-7 days')
            ORDER BY
                ABS(dte - 30) ASC,
                bid DESC
            LIMIT 5
        """, (ticker, DTE_MIN, DTE_MAX,
              CSP_OTM_MIN * 100, CSP_OTM_MAX * 100)).fetchall()
    finally:
        conn.close()

    eff_iv = iv or DEFAULT_IV

    def _enrich(row: sqlite3.Row, contract_type: str) -> dict:
        r        = dict(row)
        premium  = (r["bid"] + r["ask"]) / 2 if r.get("ask") else r["bid"]
        S        = r.get("underlying_price") or price
        K        = r["strike"]
        T        = r["dte"] / 365
        collateral = K * 100    # 1 contract = 100 shares

        ret_pct  = round(premium / (S if contract_type == "CALL" else K) * 100, 2)
        annlzd   = round(ret_pct / r["dte"] * 365, 1) if r["dte"] > 0 else None

        return {
            "source":       "market",
            "contract_type": contract_type,
            "strike":       K,
            "expiration":   r["expiration"],
            "dte":          r["dte"],
            "bid":          round(r["bid"], 2),
            "ask":          round(r["ask"], 2) if r.get("ask") else None,
            "mid":          round(premium, 2),
            "iv":           round(r["implied_volatility"] or eff_iv, 4),
            "pct_otm":      round(r["pct_otm"], 2),
            "vol_oi_ratio": round(r["vol_oi_ratio"] or 0, 2),
            "return_pct":   ret_pct,
            "annualized_pct": annlzd,
            "collateral":   round(collateral, 2),
            "premium_per_contract": round(premium * 100, 2),
        }

    def _estimate(contract_type: str) -> dict:
        """Fall back to B-S estimate when no live data."""
        if contract_type == "CALL":
            otm_pct = 0.07   # 7% OTM default
            K       = round(price * (1 + otm_pct), 2)
            dte     = 30
        else:
            otm_pct = 0.10
            K       = round(price * (1 - otm_pct), 2)
            dte     = 30
        T       = dte / 365
        premium = _bs_call_approx(price, K, T, eff_iv)
        if contract_type == "PUT":
            # put-call parity approximation (ignoring rates)
            premium = _bs_call_approx(price, K, T, eff_iv) + K - price
            premium = max(premium, 0)

        collateral = K * 100
        divisor    = (price if contract_type == "CALL" else K) or 0.01
        ret_pct    = round(premium / divisor * 100, 2)
        annlzd     = round(ret_pct / dte * 365, 1)
        return {
            "source":        "estimated",
            "contract_type": contract_type,
            "strike":        K,
            "expiration":    None,
            "dte":           dte,
            "bid":           None,
            "ask":           None,
            "mid":           round(max(premium, 0), 2),
            "iv":            round(eff_iv, 4),
            "pct_otm":       round(otm_pct * 100, 2),
            "return_pct":    ret_pct,
            "annualized_pct": annlzd,
            "collateral":   round(collateral, 2),
            "premium_per_contract": round(max(premium, 0) * 100, 2),
        }

    best_cc  = _enrich(cc_rows[0],  "CALL") if cc_rows  else _estimate("CALL")
    best_csp = _enrich(csp_rows[0], "PUT")  if csp_rows else _estimate("PUT")
    alt_cc   = [_enrich(r, "CALL") for r in cc_rows[1:3]]
    alt_csp  = [_enrich(r, "PUT")  for r in csp_rows[1:3]]

    return {
        "ticker":               ticker,
        "underlying_price":     price,
        "iv_used":              round(eff_iv, 4),
        "covered_call":         best_cc,
        "cash_secured_put":     best_csp,
        "alt_covered_calls":    alt_cc,
        "alt_csps":             alt_csp,
    }


@router.get("/api/wheel/{ticker}")
def get_wheel_opportunities(ticker: str, price: float = Query(0.0)):
    """
    Covered-call and cash-secured put opportunities for a ticker.
    Uses real market data from uoa_flow where available, B-S estimates otherwise.
    Pass ?price=<current_price> to override the underlying.
    """
    ticker = ticker.upper()

    # Resolve current price
    if price <= 0:
        try:
            from engine.market_data import get_stock_price
            data = get_stock_price(ticker)
            price = data.get("price", 0) if data else 0
        except Exception:
            pass

    # Resolve IV from UOA daily summary
    iv = 0.0
    conn = _uoa_db()
    try:
        row = conn.execute("""
            SELECT avg_iv FROM uoa_daily_summary
            WHERE ticker = ?
            ORDER BY scan_date DESC LIMIT 1
        """, (ticker,)).fetchone()
        if row and row["avg_iv"]:
            iv = float(row["avg_iv"])
    finally:
        conn.close()

    return _wheel_for_ticker(ticker, price, iv)


@router.get("/api/wheel")
def get_wheel_batch(tickers: str = Query(..., description="Comma-separated tickers")):
    """
    Wheel opportunities for multiple tickers (e.g. current Webull holdings).
    """
    ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    if not ticker_list:
        return {"wheels": []}

    # Batch price + IV fetch
    conn = _uoa_db()
    try:
        iv_rows = conn.execute(f"""
            SELECT ticker, avg_iv FROM uoa_daily_summary
            WHERE ticker IN ({','.join('?'*len(ticker_list))})
            GROUP BY ticker
            HAVING scan_date = MAX(scan_date)
        """, ticker_list).fetchall()
    finally:
        conn.close()

    iv_map = {r["ticker"]: float(r["avg_iv"] or 0) for r in iv_rows}

    results = []
    for ticker in ticker_list:
        price = 0.0
        try:
            from engine.market_data import get_stock_price
            data  = get_stock_price(ticker)
            price = data.get("price", 0) if data else 0
        except Exception:
            pass
        results.append(_wheel_for_ticker(ticker, price, iv_map.get(ticker, 0.0)))

    return {"wheels": results, "count": len(results)}


# ---------------------------------------------------------------------------
# Enhanced trade-cards: inject options_flow into each card
# ---------------------------------------------------------------------------

@router.get("/api/trade-cards-with-flow")
def get_trade_cards_with_flow(limit: int = Query(50, le=200)):
    """
    Same as /api/trade-cards but with UOA options-flow summary
    and Uhura's confluence score injected into every card.
    """
    conn = _db()
    try:
        rows = conn.execute("""
            SELECT
                d.id            AS debate_id,
                d.ticker,
                d.picard_decision,
                d.picard_conviction,
                d.adjusted_conviction,
                d.risk_rating,
                d.risk_override,
                d.picard_synthesis,
                d.spock_assessment,
                d.crusher_assessment,
                d.scotty_assessment,
                d.bull_avg_conviction,
                d.bear_avg_conviction,
                d.agent_count,
                d.created_at    AS debated_at,
                s.horizon_days,
                s.current_price,
                s.bull_probability,
                s.bull_target,
                s.bull_return_pct,
                s.bull_catalyst,
                s.base_probability,
                s.base_target,
                s.base_return_pct,
                s.base_thesis,
                s.bear_probability,
                s.bear_target,
                s.bear_return_pct,
                s.bear_catalyst,
                s.expected_value_pct,
                s.regime
            FROM debate_history_v2 d
            LEFT JOIN scenario_models s
                ON s.ticker = d.ticker
                AND s.id = (
                    SELECT id FROM scenario_models
                    WHERE ticker = d.ticker
                    ORDER BY created_at DESC LIMIT 1
                )
            WHERE d.id IN (
                SELECT MAX(id) FROM debate_history_v2 GROUP BY ticker
            )
            ORDER BY d.created_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
    finally:
        conn.close()

    cards = []
    tickers = []
    for r in rows:
        card = dict(r)
        conv = card.get("adjusted_conviction") or card.get("picard_conviction") or 5
        card["conviction_color"] = (
            "green" if conv >= 8 else "yellow" if conv >= 6
            else "orange" if conv >= 4 else "red"
        )
        risk = (card.get("risk_rating") or "MEDIUM").upper()
        card["risk_color"] = {
            "LOW": "green", "MEDIUM": "yellow",
            "HIGH": "orange", "CRITICAL": "red",
        }.get(risk, "yellow")
        cards.append(card)
        tickers.append(card["ticker"])

    # Batch-fetch options flow for all tickers
    flow_map = _options_flow_for_tickers(tickers) if tickers else {}
    for card in cards:
        flow = flow_map.get(card["ticker"], {})
        card["options_flow"] = {
            "total_unusual_vol":   flow.get("total_unusual_vol", 0),
            "dominant_direction":  flow.get("dominant_direction"),
            "dominant_pct":        flow.get("dominant_pct"),
            "call_premium_7d":     flow.get("call_premium_7d", 0),
            "put_premium_7d":      flow.get("put_premium_7d", 0),
            "signal_count":        flow.get("signal_count", 0),
            "max_convergence":     flow.get("max_convergence", 0),
            "actionable":          flow.get("actionable", False),
            "biggest_flow":        flow.get("biggest_flow"),
            "top_alert_desc":      flow.get("top_alert_desc"),
            "latest_alert":        flow.get("latest_alert"),
            "high_alert_count":    flow.get("high_alert_count", 0),
        }

    return {"cards": cards, "count": len(cards)}


# ---------------------------------------------------------------------------
# Kirk Advisory Log  (kirk_advisory_log in data/trader.db)
# ---------------------------------------------------------------------------

def _init_kirk_log():
    """Create kirk_advisory_log table if not exists."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS kirk_advisory_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker          TEXT NOT NULL,
            action          TEXT,
            message         TEXT,
            alert_type      TEXT DEFAULT 'advisory',
            fear_greed_score REAL,
            vix_level       REAL,
            dismissed_at    TIMESTAMP,
            acted_on        INTEGER DEFAULT 0,
            acted_at        TIMESTAMP,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


class KirkLogEntry(BaseModel):
    ticker: str
    action: str
    message: str
    alert_type: str = "advisory"
    fear_greed_score: Optional[float] = None
    vix_level: Optional[float] = None


@router.get("/api/kirk-advisory/log")
def get_kirk_advisory_log(limit: int = Query(100, le=500)):
    """Return Kirk Advisory alert history, newest first."""
    _init_kirk_log()
    conn = _db()
    try:
        rows = conn.execute("""
            SELECT id, ticker, action, message, alert_type,
                   fear_greed_score, vix_level,
                   dismissed_at, acted_on, acted_at, created_at
            FROM kirk_advisory_log
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return {"alerts": [dict(r) for r in rows], "count": len(rows)}
    finally:
        conn.close()


@router.post("/api/kirk-advisory/log")
def log_kirk_advisory(entry: KirkLogEntry):
    """Log a Kirk Advisory alert (deduplicates within 60-minute windows)."""
    _init_kirk_log()
    conn = _db()
    try:
        existing = conn.execute("""
            SELECT id FROM kirk_advisory_log
            WHERE ticker = ? AND action = ?
              AND created_at >= datetime('now', '-60 minutes')
            LIMIT 1
        """, (entry.ticker, entry.action)).fetchone()
        if existing:
            return {"skipped": True, "id": existing["id"]}

        cur = conn.execute("""
            INSERT INTO kirk_advisory_log
              (ticker, action, message, alert_type, fear_greed_score, vix_level)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (entry.ticker, entry.action, entry.message,
              entry.alert_type, entry.fear_greed_score, entry.vix_level))
        conn.commit()
        return {"ok": True, "id": cur.lastrowid}
    finally:
        conn.close()


@router.put("/api/kirk-advisory/{log_id}/dismiss")
def dismiss_kirk_advisory(log_id: int):
    """Mark a Kirk Advisory alert as dismissed."""
    _init_kirk_log()
    conn = _db()
    try:
        conn.execute("""
            UPDATE kirk_advisory_log SET dismissed_at = datetime('now')
            WHERE id = ?
        """, (log_id,))
        conn.commit()
        return {"ok": True, "id": log_id}
    finally:
        conn.close()


@router.put("/api/kirk-advisory/{log_id}/act")
def act_on_kirk_advisory(log_id: int):
    """Mark a Kirk Advisory alert as acted on."""
    _init_kirk_log()
    conn = _db()
    try:
        conn.execute("""
            UPDATE kirk_advisory_log
            SET acted_on = 1, acted_at = datetime('now')
            WHERE id = ?
        """, (log_id,))
        conn.commit()
        return {"ok": True, "id": log_id}
    finally:
        conn.close()
