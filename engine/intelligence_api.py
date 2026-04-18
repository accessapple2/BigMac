#!/usr/bin/env python3
"""
Intelligence API Routes — Uhura, Danelfin, Riker
FastAPI router mounted at /api/intelligence
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query

router = APIRouter(prefix="/api/intelligence", tags=["intelligence"])

# ── Optional imports ──────────────────────────────────────────────────────────
try:
    from engine.uhura_bridge_integration import (
        get_institutional_signal,
        get_institutional_summary,
        get_bulk_institutional_signals,
    )
    UHURA_OK = True
except ImportError:
    UHURA_OK = False

try:
    from engine.danelfin_parser import get_danelfin_score, get_top_danelfin_picks
    DANELFIN_OK = True
except ImportError:
    DANELFIN_OK = False

try:
    from engine.riker_synthesis import generate_synthesis
    RIKER_OK = True
except ImportError:
    RIKER_OK = False


# ── Health ────────────────────────────────────────────────────────────────────
@router.get("/health")
async def intelligence_health():
    return {"uhura": UHURA_OK, "danelfin": DANELFIN_OK, "riker": RIKER_OK}


# ── Uhura endpoints ───────────────────────────────────────────────────────────
@router.get("/uhura/signal/{ticker}")
async def api_uhura_signal(ticker: str, days: int = Query(30, ge=1, le=90)):
    if not UHURA_OK:
        return {"error": "Uhura module not available"}
    return get_institutional_signal(ticker, days=days) or {"ticker": ticker, "signal": None}


@router.get("/uhura/summary")
async def api_uhura_summary():
    if not UHURA_OK:
        return {"error": "Uhura module not available"}
    return get_institutional_summary()


# ── Danelfin endpoints ────────────────────────────────────────────────────────
@router.get("/danelfin/score/{symbol}")
async def api_danelfin_score(symbol: str):
    if not DANELFIN_OK:
        return {"error": "Danelfin module not available"}
    return get_danelfin_score(symbol) or {"symbol": symbol, "ai_score": None}


@router.get("/danelfin/top")
async def api_danelfin_top(
    n: int = Query(10, ge=1, le=50),
    min_score: int = Query(8, ge=1, le=10),
):
    if not DANELFIN_OK:
        return {"error": "Danelfin module not available", "picks": []}
    return {"picks": get_top_danelfin_picks(n=n, min_score=min_score)}


# ── Riker endpoints ───────────────────────────────────────────────────────────
@router.get("/riker/synthesis")
async def api_riker_synthesis(minutes: int = Query(10, ge=1, le=60)):
    if not RIKER_OK:
        return {"error": "Riker module not available"}
    return generate_synthesis(minutes=minutes)
