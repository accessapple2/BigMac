"""
Stellar Cartography — Intermarket Correlation Monitor
Tracks TLT, GLD, UUP, HYG vs SPY to detect divergences as early warnings.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

_DB = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))

CORR_TICKERS = ["SPY", "TLT", "GLD", "UUP", "HYG"]

_cache: dict = {}
_cache_time: datetime | None = None
_CACHE_TTL = 300  # 5 minutes


def _init_db() -> None:
    try:
        conn = sqlite3.connect(_DB, timeout=30)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS correlation_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_date TEXT NOT NULL,
                snap_time TEXT NOT NULL,
                spy_pct REAL,
                tlt_pct REAL,
                gld_pct REAL,
                uup_pct REAL,
                hyg_pct REAL,
                alignment_score REAL,
                risk_mode TEXT,
                divergence_flags_json TEXT,
                signal TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error("correlation_monitor _init_db failed: %s", e)


def _fetch_pct_changes() -> dict[str, float]:
    """Fetch today's % change for CORR_TICKERS from Alpaca (2-day daily bars)."""
    try:
        from alpaca.data import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame

        client = StockHistoricalDataClient(
            os.environ.get("ALPACA_API_KEY", ""),
            os.environ.get("ALPACA_SECRET_KEY", ""),
        )
        req = StockBarsRequest(
            symbol_or_symbols=CORR_TICKERS,
            timeframe=TimeFrame.Day,
            start=datetime.now() - timedelta(days=5),
            limit=2,
            feed="iex",
        )
        bars = client.get_stock_bars(req)
        result: dict[str, float] = {}
        for ticker in CORR_TICKERS:
            raw = bars.data.get(ticker, [])
            if len(raw) >= 2:
                yesterday_close = raw[-2].close
                today_latest = raw[-1].close
                if yesterday_close and yesterday_close != 0:
                    pct = (today_latest - yesterday_close) / yesterday_close * 100
                    result[ticker] = round(pct, 4)
                else:
                    result[ticker] = 0.0
            elif len(raw) == 1:
                # Only one bar: open vs close
                bar = raw[0]
                if bar.open and bar.open != 0:
                    result[ticker] = round((bar.close - bar.open) / bar.open * 100, 4)
                else:
                    result[ticker] = 0.0
            else:
                result[ticker] = 0.0
        return result
    except Exception as e:
        logger.error("_fetch_pct_changes failed: %s", e)
        return {t: 0.0 for t in CORR_TICKERS}


def _compute_alignment(pcts: dict[str, float]) -> tuple[float, str, list[str], dict]:
    """Compute alignment score, risk mode, divergences, and per-indicator signals."""
    spy = pcts.get("SPY", 0.0)
    tlt = pcts.get("TLT", 0.0)
    gld = pcts.get("GLD", 0.0)
    uup = pcts.get("UUP", 0.0)
    hyg = pcts.get("HYG", 0.0)

    # SPY contribution: base score based on direction
    spy_contribution = 25 if spy > 0 else -25

    # TLT: bonds inverse relationship with equities
    if spy > 0:
        tlt_contribution = -15 if tlt > 0 else 15
    else:
        tlt_contribution = 15 if tlt > 0 else -15

    # HYG: high yield confirms equity direction
    if spy > 0:
        hyg_contribution = 20 if hyg > 0 else -20
    else:
        hyg_contribution = -20 if hyg > 0 else 20

    # GLD: spiking gold = hedging, bad when SPY up
    if spy > 0:
        gld_contribution = -15 if gld >= 2.0 else 5
    else:
        gld_contribution = 10 if gld > 0 else -5

    # UUP: strong dollar = headwind
    if uup > 0.3:
        uup_contribution = -15
    else:
        uup_contribution = 10

    raw_score = 50 + spy_contribution + tlt_contribution + hyg_contribution + gld_contribution + uup_contribution
    raw_score = max(0.0, min(100.0, raw_score))
    alignment_score = round((raw_score - 50) * 2, 2)

    # Risk mode
    if alignment_score > 40:
        risk_mode = "RISK_ON"
    elif alignment_score < -40:
        risk_mode = "RISK_OFF"
    elif abs(spy) > 0.5 and (
        (spy > 0 and tlt > 0.5 and gld > 0.5)
        or (spy < 0 and tlt < -0.5 and gld < -0.5)
    ):
        risk_mode = "DIVERGENT"
    else:
        risk_mode = "MIXED"

    # Divergences
    divergences: list[str] = []
    if spy > 0 and tlt > 0.3:
        divergences.append(
            f"TLT up +{tlt:.2f}% while SPY up — flight-to-safety bid in bonds, caution"
        )
    if spy > 0 and gld >= 2.0:
        divergences.append(
            f"GLD spiking +{gld:.2f}% — smart money hedging despite equity strength"
        )
    if hyg < -0.3:
        divergences.append(
            f"HYG down {abs(hyg):.2f}% — credit stress developing, may lead equities lower"
        )
    if uup > 0.3:
        divergences.append(
            f"UUP up +{uup:.2f}% — strong dollar headwind for stocks"
        )

    # Per-indicator details
    indicators = {
        "TLT": {
            "pct": tlt,
            "direction": "up" if tlt > 0 else "down",
            "confirms_spy": (spy > 0 and tlt < 0) or (spy < 0 and tlt > 0),
            "signal": "healthy" if (spy > 0 and tlt < 0) else "diverging",
        },
        "HYG": {
            "pct": hyg,
            "direction": "up" if hyg > 0 else "down",
            "confirms_spy": (spy > 0 and hyg > 0) or (spy < 0 and hyg < 0),
            "signal": "confirms" if (spy > 0 and hyg > 0) else "credit_stress",
        },
        "GLD": {
            "pct": gld,
            "direction": "up" if gld > 0 else "down",
            "confirms_spy": abs(gld) < 0.5,
            "signal": "spiking" if gld >= 2.0 else "neutral",
        },
        "UUP": {
            "pct": uup,
            "direction": "up" if uup > 0 else "down",
            "confirms_spy": uup < 0.1,
            "signal": "headwind" if uup > 0.3 else "neutral",
        },
    }

    return alignment_score, risk_mode, divergences, indicators


def _build_signal(alignment_score: float, risk_mode: str, divergences: list[str]) -> str:
    if risk_mode == "RISK_ON" and len(divergences) == 0:
        return "FULL_CONFIRM — all intermarket signals align bullish"
    elif risk_mode == "RISK_ON":
        return "MOSTLY_CONFIRM — risk-on with minor divergences"
    elif risk_mode == "RISK_OFF":
        return "RISK_OFF — intermarket signals confirm defensive posture"
    elif risk_mode == "DIVERGENT":
        return "DIVERGENT — conflicting signals, reduce size"
    else:
        return "MIXED — no strong directional confirmation"


def _save_snapshot(data: dict) -> None:
    try:
        conn = sqlite3.connect(_DB, timeout=30)
        conn.execute(
            """
            INSERT INTO correlation_snapshots
              (trade_date, snap_time, spy_pct, tlt_pct, gld_pct, uup_pct, hyg_pct,
               alignment_score, risk_mode, divergence_flags_json, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["trade_date"],
                data["snap_time"],
                data["spy_pct"],
                data["tlt_pct"],
                data["gld_pct"],
                data["uup_pct"],
                data["hyg_pct"],
                data["alignment_score"],
                data["risk_mode"],
                json.dumps(data["divergences"]),
                data["signal"],
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error("_save_snapshot failed: %s", e)


def get_correlations(force: bool = False) -> dict:
    """
    Main function. Returns intermarket correlation snapshot with alignment score,
    risk mode, divergences, and per-indicator signals.
    """
    global _cache, _cache_time

    now = datetime.now()
    if not force and _cache and _cache_time and (now - _cache_time).total_seconds() < _CACHE_TTL:
        return _cache

    try:
        pcts = _fetch_pct_changes()
        alignment_score, risk_mode, divergences, indicators = _compute_alignment(pcts)
        signal = _build_signal(alignment_score, risk_mode, divergences)

        trade_date = now.strftime("%Y-%m-%d")
        snap_time = now.strftime("%H:%M:%S")

        result = {
            "spy_pct": pcts.get("SPY", 0.0),
            "tlt_pct": pcts.get("TLT", 0.0),
            "gld_pct": pcts.get("GLD", 0.0),
            "uup_pct": pcts.get("UUP", 0.0),
            "hyg_pct": pcts.get("HYG", 0.0),
            "alignment_score": alignment_score,
            "risk_mode": risk_mode,
            "divergences": divergences,
            "indicators": indicators,
            "signal": signal,
            "fetched_at": now.isoformat(),
            "trade_date": trade_date,
            "snap_time": snap_time,
            "error": None,
        }

        _save_snapshot(result)

        _cache = result
        _cache_time = now
        return result

    except Exception as e:
        logger.error("get_correlations failed: %s", e)
        return {
            "spy_pct": 0.0,
            "tlt_pct": 0.0,
            "gld_pct": 0.0,
            "uup_pct": 0.0,
            "hyg_pct": 0.0,
            "alignment_score": 0.0,
            "risk_mode": "MIXED",
            "divergences": [],
            "indicators": {},
            "signal": "ERROR — could not fetch intermarket data",
            "fetched_at": datetime.now().isoformat(),
            "trade_date": datetime.now().strftime("%Y-%m-%d"),
            "snap_time": datetime.now().strftime("%H:%M:%S"),
            "error": str(e),
        }


# Initialize DB on module load
_init_db()
