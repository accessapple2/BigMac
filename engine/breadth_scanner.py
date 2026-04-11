"""
Chekov's Broad Scan — Market breadth scanner for USS TradeMinds.
Tracks sector ETF performance to measure advance/decline breadth,
SPY/RSP divergence, IWM confirmation, and rotation type.
"""

import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

load_dotenv()

from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

logger = logging.getLogger(__name__)

_DB = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))

SECTOR_ETFS = ["XLK", "XLF", "XLV", "XLE", "XLI", "XLC", "XLY", "XLP", "XLB", "XLRE", "XLU"]
BREADTH_ETFS = ["RSP", "IWM", "QQQ", "SPY"]
ALL_TICKERS = SECTOR_ETFS + BREADTH_ETFS

_CACHE_TTL = 300  # 5 minutes
_cache: dict = {}


def _init_db() -> None:
    conn = sqlite3.connect(_DB, timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS breadth_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_date TEXT NOT NULL,
                snap_time TEXT NOT NULL,
                breadth_score REAL,
                adv_count INTEGER,
                dec_count INTEGER,
                adv_decline_ratio REAL,
                spy_rsp_divergence REAL,
                iwm_confirmation TEXT,
                sector_leader TEXT,
                sector_laggard TEXT,
                rotation_type TEXT,
                sector_data_json TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.commit()
    finally:
        conn.close()


def _fetch_etf_changes() -> dict[str, float]:
    """
    Fetch today's % change for all tickers from Alpaca.

    Strategy: Fetch 2 days of daily bars, compare yesterday close to today's
    latest bar. If today has no bars yet (pre-market), return 0% change.

    Returns: {"XLK": 1.23, "XLF": -0.45, ...}
    """
    client = StockHistoricalDataClient(
        os.environ.get("ALPACA_API_KEY", ""),
        os.environ.get("ALPACA_SECRET_KEY", ""),
    )
    start = datetime.now(timezone.utc) - timedelta(days=5)
    req = StockBarsRequest(
        symbol_or_symbols=ALL_TICKERS,
        timeframe=TimeFrame.Day,
        start=start,
        limit=2,
        feed="iex",
    )
    bars = client.get_stock_bars(req)

    changes: dict[str, float] = {}
    for ticker in ALL_TICKERS:
        raw = bars.data.get(ticker, [])
        if len(raw) < 2:
            # Pre-market or no data — default to 0%
            changes[ticker] = 0.0
            continue
        yesterday_close = raw[-2].close
        today_bar = raw[-1]
        today_price = today_bar.close if today_bar.close else today_bar.open
        if yesterday_close and yesterday_close != 0:
            changes[ticker] = (today_price - yesterday_close) / yesterday_close * 100
        else:
            changes[ticker] = 0.0

    return changes


def _determine_rotation_type(sector_changes: dict[str, float]) -> str:
    ranked = sorted(SECTOR_ETFS, key=lambda t: sector_changes.get(t, 0.0), reverse=True)
    top3 = set(ranked[:3])

    risk_on_set = {"XLK", "XLY", "XLF"}
    risk_off_set = {"XLU", "XLP", "XLV"}

    if risk_on_set.issubset(top3):
        return "RISK_ON"
    if risk_off_set.issubset(top3):
        return "RISK_OFF"
    if ranked[0] == "XLE" or ranked[1] == "XLE":
        return "ENERGY_LED"
    return "MIXED"


def _determine_iwm_confirmation(changes: dict[str, float]) -> str:
    spy_pct = changes.get("SPY", 0.0)
    iwm_pct = changes.get("IWM", 0.0)
    if spy_pct > 0 and iwm_pct > 0:
        return "CONFIRMING"
    if spy_pct > 0 and iwm_pct <= 0:
        return "DIVERGING"
    return "NEUTRAL"


def _build_advisor_note(spy_rsp_divergence: float, breadth_score: float, iwm_confirmation: str) -> str:
    if spy_rsp_divergence > 0.5 and breadth_score < 20:
        return "⚠️ Narrow leadership — SPY up but breadth weak"
    if spy_rsp_divergence < -0.5:
        return "✅ Broad participation — RSP outperforming SPY (healthy)"
    if iwm_confirmation == "DIVERGING":
        return "⚠️ Small cap divergence — IWM lagging"
    return ""


def _store_snapshot(data: dict) -> None:
    now = datetime.now(timezone.utc)
    trade_date = now.strftime("%Y-%m-%d")
    snap_time = now.strftime("%H:%M:%S")
    sector_data_json = json.dumps(data.get("sectors", {}))

    conn = sqlite3.connect(_DB, timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(
            """
            INSERT INTO breadth_snapshots
                (trade_date, snap_time, breadth_score, adv_count, dec_count,
                 adv_decline_ratio, spy_rsp_divergence, iwm_confirmation,
                 sector_leader, sector_laggard, rotation_type, sector_data_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade_date,
                snap_time,
                data.get("breadth_score"),
                data.get("adv_count"),
                data.get("dec_count"),
                data.get("adv_decline_ratio"),
                data.get("spy_rsp_divergence"),
                data.get("iwm_confirmation"),
                data.get("sector_leader"),
                data.get("sector_laggard"),
                data.get("rotation_type"),
                sector_data_json,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_breadth_snapshot(force: bool = False) -> dict:
    """
    Main function. Fetches ETF data, computes breadth metrics, stores in DB,
    returns result dict with scores, signals, and Counselor Troi advisor note.
    """
    now_ts = datetime.now(timezone.utc).timestamp()
    cached = _cache.get("breadth_snapshot")
    if not force and cached and (now_ts - cached["ts"]) < _CACHE_TTL:
        return cached["data"]

    try:
        changes = _fetch_etf_changes()
    except Exception as exc:
        logger.error("breadth_scanner: Alpaca fetch failed: %s", exc)
        return {
            "breadth_score": None,
            "adv_count": None,
            "dec_count": None,
            "adv_decline_ratio": None,
            "spy_rsp_divergence": None,
            "iwm_confirmation": None,
            "sector_leader": None,
            "sector_laggard": None,
            "rotation_type": None,
            "sectors": {},
            "breadth_etfs": {},
            "advisor_note": "",
            "signal": "❌ Data unavailable",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "error": str(exc),
        }

    # Breadth score
    sector_changes = {t: changes.get(t, 0.0) for t in SECTOR_ETFS}
    adv_count = sum(1 for v in sector_changes.values() if v > 0)
    dec_count = len(SECTOR_ETFS) - adv_count
    breadth_score = (adv_count - dec_count) / len(SECTOR_ETFS) * 100
    adv_decline_ratio = adv_count / len(SECTOR_ETFS) * 100

    # Ranked sectors
    ranked_tickers = sorted(SECTOR_ETFS, key=lambda t: sector_changes[t], reverse=True)
    sectors = {
        ticker: {
            "pct_change": round(sector_changes[ticker], 4),
            "rank": ranked_tickers.index(ticker) + 1,
            "direction": "up" if sector_changes[ticker] > 0 else "down",
        }
        for ticker in SECTOR_ETFS
    }

    sector_leader = ranked_tickers[0]
    sector_laggard = ranked_tickers[-1]

    # Breadth ETF data
    breadth_etfs = {
        ticker: {"pct_change": round(changes.get(ticker, 0.0), 4)}
        for ticker in BREADTH_ETFS
    }

    spy_pct = changes.get("SPY", 0.0)
    rsp_pct = changes.get("RSP", 0.0)
    spy_rsp_divergence = round(spy_pct - rsp_pct, 4)

    iwm_confirmation = _determine_iwm_confirmation(changes)
    rotation_type = _determine_rotation_type(sector_changes)
    advisor_note = _build_advisor_note(spy_rsp_divergence, breadth_score, iwm_confirmation)

    # Human-readable signal
    if breadth_score >= 60:
        signal = "🟢 Strong breadth — broad market participation"
    elif breadth_score >= 20:
        signal = "🟡 Moderate breadth — mixed sector participation"
    elif breadth_score >= -20:
        signal = "🟠 Weak breadth — near even split"
    else:
        signal = "🔴 Negative breadth — broad market weakness"

    result = {
        "breadth_score": round(breadth_score, 2),
        "adv_count": adv_count,
        "dec_count": dec_count,
        "adv_decline_ratio": round(adv_decline_ratio, 2),
        "spy_rsp_divergence": spy_rsp_divergence,
        "iwm_confirmation": iwm_confirmation,
        "sector_leader": sector_leader,
        "sector_laggard": sector_laggard,
        "rotation_type": rotation_type,
        "sectors": sectors,
        "breadth_etfs": breadth_etfs,
        "advisor_note": advisor_note,
        "signal": signal,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "error": None,
    }

    try:
        _store_snapshot(result)
    except Exception as exc:
        logger.warning("breadth_scanner: DB store failed: %s", exc)

    _cache["breadth_snapshot"] = {"ts": now_ts, "data": result}
    return result


def get_breadth_advisor_note() -> str:
    """Quick call for Counselor Troi — returns just the advisor_note string."""
    snapshot = get_breadth_snapshot()
    return snapshot.get("advisor_note", "")


_init_db()
