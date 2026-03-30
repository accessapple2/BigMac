"""
TradeMinds UOA Tools for CrewAI
===============================
Gives Scout and other agents access to UOA data as tools.

Usage in crew/agents.py:
    from uoa.crew_tools import uoa_alerts_tool, uoa_flow_tool
    scout = Agent(
        role='Scout',
        tools=[uoa_alerts_tool, uoa_flow_tool, ...],
    )
"""

import sqlite3
import json
from datetime import datetime

try:
    from crewai.tools import tool
    HAS_CREWAI = True
except ImportError:
    # Fallback: define a simple decorator if crewai isn't installed
    HAS_CREWAI = False
    def tool(func):
        func.is_tool = True
        return func

DB_PATH = 'trader.db'


@tool
def uoa_alerts_tool(severity: str = "HIGH") -> str:
    """
    Get the latest Unusual Options Activity alerts.
    Severity options: CRITICAL, HIGH, MEDIUM, LOW.
    Returns the top 15 most recent alerts at or above the given severity.
    Use this to find stocks with unusual options positioning that may
    signal upcoming big moves.
    """
    sev_order = {'CRITICAL': 4, 'HIGH': 3, 'MEDIUM': 2, 'LOW': 1}
    min_sev = sev_order.get(severity.upper(), 3)
    allowed = [k for k, v in sev_order.items() if v >= min_sev]

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    placeholders = ','.join('?' * len(allowed))
    rows = conn.execute(f"""
        SELECT ticker, alert_type, severity, contract_type, strike,
               expiration, vol_oi_ratio, premium_total, description,
               convergence_score, alert_date
        FROM uoa_alerts
        WHERE severity IN ({placeholders})
        ORDER BY created_at DESC LIMIT 15
    """, allowed).fetchall()
    conn.close()

    if not rows:
        return f"No UOA alerts at {severity} level or above."

    results = []
    for r in rows:
        results.append({
            'ticker': r['ticker'],
            'type': r['alert_type'],
            'severity': r['severity'],
            'side': r['contract_type'],
            'strike': r['strike'],
            'expiry': r['expiration'],
            'vol_oi': r['vol_oi_ratio'],
            'premium': f"${r['premium_total']:,.0f}" if r['premium_total'] else 'N/A',
            'score': r['convergence_score'],
            'summary': r['description'],
            'date': r['alert_date'],
        })

    return json.dumps(results, indent=2)


@tool
def uoa_flow_tool(ticker: str) -> str:
    """
    Get detailed unusual options flow for a specific ticker.
    Shows the most unusual contracts by volume/OI ratio and premium size.
    Use this to dig deeper into WHY a stock was flagged by UOA alerts.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Get latest flow data
    rows = conn.execute("""
        SELECT contract_type, strike, expiration, dte, volume,
               open_interest, vol_oi_ratio, last_price, premium_total,
               moneyness, pct_otm, sentiment, implied_volatility
        FROM uoa_flow
        WHERE ticker = ?
        ORDER BY scan_date DESC, premium_total DESC
        LIMIT 20
    """, (ticker.upper(),)).fetchall()

    # Get daily summary
    summary = conn.execute("""
        SELECT * FROM uoa_daily_summary
        WHERE ticker = ?
        ORDER BY scan_date DESC LIMIT 1
    """, (ticker.upper(),)).fetchone()

    conn.close()

    if not rows:
        return f"No unusual options flow data for {ticker}."

    result = {
        'ticker': ticker.upper(),
        'summary': {
            'put_call_ratio': summary['put_call_ratio'] if summary else None,
            'total_put_premium': f"${summary['total_put_premium']:,.0f}" if summary else None,
            'total_call_premium': f"${summary['total_call_premium']:,.0f}" if summary else None,
            'max_vol_oi': summary['max_vol_oi_ratio'] if summary else None,
        } if summary else None,
        'unusual_contracts': [],
    }

    for r in rows:
        result['unusual_contracts'].append({
            'type': r['contract_type'],
            'strike': r['strike'],
            'expiry': r['expiration'],
            'dte': r['dte'],
            'volume': f"{r['volume']:,}",
            'oi': f"{r['open_interest']:,}",
            'vol_oi': r['vol_oi_ratio'],
            'price': f"${r['last_price']:.2f}",
            'premium': f"${r['premium_total']:,.0f}",
            'moneyness': r['moneyness'],
            'otm_pct': f"{r['pct_otm']:.1f}%",
            'sentiment': r['sentiment'],
            'iv': f"{r['implied_volatility'] * 100:.0f}%" if r['implied_volatility'] else 'N/A',
        })

    return json.dumps(result, indent=2)


@tool
def uoa_put_call_scan_tool(threshold: float = 2.0) -> str:
    """
    Find stocks with extreme put/call ratios today.
    High put/call ratio (>2.0) = heavy bearish positioning.
    Low put/call ratio (<0.4) = heavy bullish positioning.
    Returns tickers sorted by most extreme ratios.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    today = datetime.now().strftime('%Y-%m-%d')
    rows = conn.execute("""
        SELECT ticker, put_call_ratio, total_put_premium,
               total_call_premium, max_vol_oi_ratio, underlying_close
        FROM uoa_daily_summary
        WHERE scan_date = (SELECT MAX(scan_date) FROM uoa_daily_summary)
          AND put_call_ratio IS NOT NULL
          AND (put_call_ratio > ? OR put_call_ratio < ?)
        ORDER BY put_call_ratio DESC
    """, (threshold, 1.0 / threshold)).fetchall()
    conn.close()

    if not rows:
        return f"No extreme put/call ratios found (threshold: {threshold})."

    results = []
    for r in rows:
        bias = "BEARISH" if r['put_call_ratio'] > 1.0 else "BULLISH"
        results.append({
            'ticker': r['ticker'],
            'put_call_ratio': round(r['put_call_ratio'], 2),
            'bias': bias,
            'put_premium': f"${r['total_put_premium']:,.0f}",
            'call_premium': f"${r['total_call_premium']:,.0f}",
            'max_vol_oi': r['max_vol_oi_ratio'],
            'price': f"${r['underlying_close']:.2f}" if r['underlying_close'] else 'N/A',
        })

    return json.dumps(results, indent=2)
