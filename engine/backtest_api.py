#!/usr/bin/env python3
"""
Backtest API Routes — FastAPI router
Prefix: /api/backtest   (no collision with existing routes in app.py)
New endpoints:
  GET /api/backtest/strategies
  GET /api/backtest/strategies/top
  GET /api/backtest/strategies/worst
  GET /api/backtest/trades          (different from /api/backtests)
  GET /api/backtest/trades/summary
  GET /api/backtest/regime
  GET /api/backtest/bt-health
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query

try:
    from engine.strategy_breakdown import get_strategy_breakdown, get_top_strategies, get_worst_strategies
    from engine.trade_log import get_trade_log, get_trade_summary
    from engine.regime_analyzer import (
        get_regime_summary,
        get_best_agents_by_regime,
        get_regime_recommendations,
    )
    _AVAILABLE = True
except ImportError as _e:
    _AVAILABLE = False
    _IMPORT_ERR = str(_e)

router = APIRouter(prefix="/api/backtest", tags=["backtest-analytics"])


def _unavailable():
    return {"error": "Backtest analytics modules not available", "data": []}


@router.get("/strategies")
async def api_strategies(
    days: int = Query(30, ge=1, le=365),
    agent: Optional[str] = None,
    min_trades: int = Query(3, ge=1),
):
    """Strategy breakdown by timeframe × agent."""
    if not _AVAILABLE:
        return _unavailable()
    try:
        data = get_strategy_breakdown(agent_id=agent, days=days)
        data = [s for s in data if s["total_trades"] >= min_trades]
        return {"strategies": data, "count": len(data)}
    except Exception as e:
        return {"error": str(e), "strategies": []}


@router.get("/strategies/top")
async def api_top_strategies(
    n: int = Query(10, ge=1, le=50),
    days: int = Query(30, ge=1, le=365),
):
    """Top N strategies by profit factor."""
    if not _AVAILABLE:
        return _unavailable()
    try:
        return {"strategies": get_top_strategies(n=n, days=days)}
    except Exception as e:
        return {"error": str(e), "strategies": []}


@router.get("/strategies/worst")
async def api_worst_strategies(
    n: int = Query(5, ge=1, le=20),
    days: int = Query(30, ge=1, le=365),
):
    """Worst N strategies for review."""
    if not _AVAILABLE:
        return _unavailable()
    try:
        return {"strategies": get_worst_strategies(n=n, days=days)}
    except Exception as e:
        return {"error": str(e), "strategies": []}


@router.get("/trades")
async def api_trades(
    days: int = Query(30, ge=1, le=365),
    limit: int = Query(50, ge=1, le=500),
    agent: Optional[str] = None,
):
    """Trade-by-trade log with entry/exit detail."""
    if not _AVAILABLE:
        return _unavailable()
    try:
        return {
            "trades":  get_trade_log(agent_id=agent, days=days, limit=limit),
            "summary": get_trade_summary(days=days),
        }
    except Exception as e:
        return {"error": str(e), "trades": [], "summary": {}}


@router.get("/trades/summary")
async def api_trade_summary(days: int = Query(30, ge=1, le=365)):
    """Aggregate trade statistics."""
    if not _AVAILABLE:
        return _unavailable()
    try:
        return get_trade_summary(days=days)
    except Exception as e:
        return {"error": str(e)}


@router.get("/regime")
async def api_regime(days: int = Query(90, ge=1, le=365)):
    """Regime-aware performance split (joins with regime_history)."""
    if not _AVAILABLE:
        return _unavailable()
    try:
        return {
            "summary":         get_regime_summary(days=days),
            "best_agents":     get_best_agents_by_regime(days=days),
            "recommendations": get_regime_recommendations(),
        }
    except Exception as e:
        return {"error": str(e), "summary": {}, "best_agents": {}, "recommendations": []}


@router.get("/bt-health")
async def api_bt_health():
    """Health check for backtest analytics module."""
    return {
        "status":   "ok" if _AVAILABLE else "degraded",
        "available": _AVAILABLE,
    }
