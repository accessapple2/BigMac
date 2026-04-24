"""
TradeMinds UOA API Routes
=========================
FastAPI endpoints for the UOA scanner module.
Mount on your existing FastAPI app.

Usage in main.py:
    from uoa.routes import router as uoa_router
    app.include_router(uoa_router, prefix="/api/uoa", tags=["UOA"])
"""

import sqlite3
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, BackgroundTasks, Query
from typing import Optional

from uoa.scraper import UOAScraper

router = APIRouter()

DB_PATH = str(Path(__file__).resolve().parent.parent / "data" / "trader.db")


# ------------------------------------------------------------------
# Trigger scans
# ------------------------------------------------------------------

@router.post("/scan/quick")
async def scan_quick(background_tasks: BackgroundTasks, top_n: int = 50):
    """Quick scan: top N stocks from Chekov's universe."""
    background_tasks.add_task(_run_scan, 'quick', top_n)
    return {"status": "started", "scan_type": "quick", "top_n": top_n}


@router.post("/scan/full")
async def scan_full(background_tasks: BackgroundTasks):
    """Full scan: all 528 Chekov watchlist stocks. Takes 10-15 min."""
    background_tasks.add_task(_run_scan, 'full', None)
    return {"status": "started", "scan_type": "full",
            "note": "Full scan takes ~10-15 minutes"}


@router.post("/scan/tickers")
async def scan_tickers(tickers: str, background_tasks: BackgroundTasks):
    """Scan specific tickers. Pass comma-separated: META,NVDA,AAPL"""
    ticker_list = [t.strip().upper() for t in tickers.split(',') if t.strip()]
    background_tasks.add_task(_run_scan, 'custom', ticker_list)
    return {"status": "started", "tickers": ticker_list}


def _run_scan(mode, param):
    """Background task to run UOA scan."""
    scraper = UOAScraper(db_path=DB_PATH)
    if mode == 'quick':
        result = scraper.scan_quick(top_n=param or 50)
    elif mode == 'full':
        result = scraper.scan_watchlist()
    else:
        result = scraper.scan_tickers(param)

    # Post critical alerts to War Room
    for alert in result.get('alerts', []):
        if alert['severity'] in ('CRITICAL', 'HIGH'):
            _post_to_war_room(alert)


# ------------------------------------------------------------------
# Query alerts and flow data
# ------------------------------------------------------------------

@router.get("/alerts")
async def get_alerts(
    severity: Optional[str] = None,
    ticker: Optional[str] = None,
    date: Optional[str] = None,
    limit: int = Query(default=50, le=200)
):
    """Get UOA alerts. Filter by severity, ticker, date."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    query = "SELECT * FROM uoa_alerts WHERE 1=1"
    params = []

    if severity:
        query += " AND severity = ?"
        params.append(severity.upper())
    if ticker:
        query += " AND ticker = ?"
        params.append(ticker.upper())
    if date:
        query += " AND alert_date = ?"
        params.append(date)

    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("/alerts/critical")
async def get_critical_alerts(limit: int = 20):
    """Get only CRITICAL and HIGH alerts (the ones that matter)."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM uoa_alerts
        WHERE severity IN ('CRITICAL', 'HIGH')
        ORDER BY created_at DESC LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("/flow/{ticker}")
async def get_flow(ticker: str, date: Optional[str] = None, limit: int = 50):
    """Get raw unusual options flow for a ticker."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    query = "SELECT * FROM uoa_flow WHERE ticker = ?"
    params = [ticker.upper()]

    if date:
        query += " AND scan_date = ?"
        params.append(date)

    query += " ORDER BY premium_total DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("/summary")
async def get_daily_summary(date: Optional[str] = None, limit: int = 50):
    """Get daily put/call summary. Sorted by max vol/OI ratio."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    scan_date = date or datetime.now().strftime('%Y-%m-%d')
    rows = conn.execute("""
        SELECT * FROM uoa_daily_summary
        WHERE scan_date = ?
        ORDER BY max_vol_oi_ratio DESC
        LIMIT ?
    """, (scan_date, limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("/summary/{ticker}")
async def get_ticker_summary_history(ticker: str, days: int = 30):
    """Get historical daily summaries for a ticker (trend analysis)."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM uoa_daily_summary
        WHERE ticker = ?
        ORDER BY scan_date DESC LIMIT ?
    """, (ticker.upper(), days)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("/scan-log")
async def get_scan_log(limit: int = 20):
    """Get scan run history."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM uoa_scan_log
        ORDER BY created_at DESC LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("/scans")
async def get_scan_log_alias(limit: int = 20):
    """Compatibility alias for older clients expecting /scans."""
    return await get_scan_log(limit=limit)


@router.get("/dashboard")
async def uoa_dashboard():
    """
    Dashboard summary: latest scan stats + top alerts.
    For The Bridge (port 8080) to render.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Latest scan info
    scan = conn.execute(
        "SELECT * FROM uoa_scan_log ORDER BY created_at DESC LIMIT 1"
    ).fetchone()

    # Today's alerts by severity
    today = datetime.now().strftime('%Y-%m-%d')
    alert_counts = {}
    for sev in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
        count = conn.execute(
            "SELECT COUNT(*) FROM uoa_alerts WHERE alert_date = ? AND severity = ?",
            (today, sev)
        ).fetchone()[0]
        alert_counts[sev] = count

    # Top 10 alerts today
    top_alerts = conn.execute("""
        SELECT * FROM uoa_alerts
        WHERE alert_date = ?
        ORDER BY convergence_score DESC LIMIT 10
    """, (today,)).fetchall()

    # Tickers with highest put/call ratios today
    bearish_flow = conn.execute("""
        SELECT ticker, put_call_ratio, total_put_premium, max_vol_oi_ratio
        FROM uoa_daily_summary
        WHERE scan_date = ? AND put_call_ratio IS NOT NULL
        ORDER BY put_call_ratio DESC LIMIT 10
    """, (today,)).fetchall()

    conn.close()

    return {
        'scan_date': today,
        'last_scan': dict(scan) if scan else None,
        'alert_counts': alert_counts,
        'total_alerts_today': sum(alert_counts.values()),
        'top_alerts': [dict(r) for r in top_alerts],
        'bearish_flow': [dict(r) for r in bearish_flow],
    }


# ------------------------------------------------------------------
# War Room integration
# ------------------------------------------------------------------

def _post_to_war_room(alert: dict):
    """Post a UOA alert to the TradeMinds War Room."""
    try:
        # Import the war room posting function
        # Adjust this import to match your actual war room module
        from utils import post_war_room_message
    except ImportError:
        try:
            from main import post_war_room_message
        except ImportError:
            print(f"[UOA] War Room not available. Alert: {alert['description']}")
            return

    severity_icons = {
        'CRITICAL': '🚨',
        'HIGH': '⚠️',
        'MEDIUM': '📊',
        'LOW': 'ℹ️',
    }
    icon = severity_icons.get(alert['severity'], '📊')

    # Use Uhura as the UOA comms officer
    message = (
        f"{icon} UNUSUAL OPTIONS ACTIVITY DETECTED!\n"
        f"{alert['description']}\n"
        f"Score: {alert.get('convergence_score', 0):.0f}/100"
    )

    stance = 'BEAR' if alert.get('contract_type') == 'PUT' else 'BULL'

    post_war_room_message(
        player_id='uhura',
        display_name='Lt. Uhura',
        symbol=alert['ticker'],
        message=message,
        stance=stance,
        crew_prefix='📡 LT. UHURA [OPTIONS FLOW]'
    )
