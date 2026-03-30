"""
FastAPI routers for CrewAI strategy pipeline and multi-portfolio management.

Include in main app:
    from crew.routes import crew_router, portfolio_router
    app.include_router(crew_router, prefix="/api/crew", tags=["CrewAI"])
    app.include_router(portfolio_router, prefix="/api/portfolios", tags=["Portfolios"])
"""

import json
import os
import sqlite3
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

from crew.pipeline import CrewPipeline
from portfolios.manager import PortfolioManager
from shared.matrix_bridge import (
    NEO_PLAYER_ID,
    annotate_player_payload,
    ensure_matrix_shared_records,
    sync_neo_from_native_portfolio,
    sync_neo_snapshot,
)

DB_PATH = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class CrewRunRequest(BaseModel):
    focus_area: str = "market opportunities"
    target_asset_class: str = "stock"
    target_portfolio_id: int = 1
    trigger: str = "api"


class ScoutRequest(BaseModel):
    focus_area: str = "market opportunities"


class ReviewRequest(BaseModel):
    strategy_id: int


class AddPortfolioRequest(BaseModel):
    name: str
    broker: str
    account_type: str = "paper"
    initial_balance: float = 100000.0
    is_human: int = 0
    notes: str = ""
    execution_mode: str = "auto"
    type: str = "paper"


class ActivatePortfolioRequest(BaseModel):
    active: bool = True


class OpenPositionRequest(BaseModel):
    portfolio_id: int
    ticker: str
    asset_class: str = "stock"
    direction: str = "long"
    quantity: float = 1.0
    entry_price: float = 0.0
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    option_type: Optional[str] = None
    strike_price: Optional[float] = None
    expiration_date: Optional[str] = None
    spread_type: Optional[str] = None
    spread_legs: Optional[str] = None
    metal_type: Optional[str] = None
    metal_oz: Optional[float] = None
    notes: str = ""


class ClosePositionRequest(BaseModel):
    position_id: int
    close_price: float
    notes: str = ""


# ---------------------------------------------------------------------------
# Crew router
# ---------------------------------------------------------------------------

crew_router = APIRouter()
_pipeline = CrewPipeline()
_run_results: dict = {}   # Background task results keyed by run_id
_run_counter = 0
_scheduler_state: dict = {}  # job_id → last run info, updated by main_crew scheduler


def _run_pipeline_bg(run_id: str, req: CrewRunRequest):
    result = _pipeline.run_full_pipeline(
        req.focus_area, req.target_asset_class, req.target_portfolio_id, req.trigger,
    )
    _run_results[run_id] = result


def _run_sunday_bg(run_id: str):
    result = _pipeline.run_sunday_review()
    _run_results[run_id] = result


@crew_router.post("/run")
def crew_run(req: CrewRunRequest, background_tasks: BackgroundTasks):
    """Launch full crew pipeline (runs in background)."""
    global _run_counter
    _run_counter += 1
    run_id = f"crew_run_{_run_counter}"
    background_tasks.add_task(_run_pipeline_bg, run_id, req)
    return {"status": "launched", "run_id": run_id, "message": "Pipeline running in background."}


@crew_router.post("/scout")
def crew_scout(req: ScoutRequest):
    """Run scout-only scan (synchronous)."""
    return _pipeline.run_scout_only(req.focus_area)


@crew_router.post("/review")
def crew_review(req: ReviewRequest):
    """Re-evaluate existing strategy through Critic + Commander."""
    return _pipeline.run_review_existing(req.strategy_id)


@crew_router.post("/sunday")
def crew_sunday(background_tasks: BackgroundTasks):
    """Launch Sunday review (runs in background)."""
    global _run_counter
    _run_counter += 1
    run_id = f"sunday_{_run_counter}"
    background_tasks.add_task(_run_sunday_bg, run_id)
    return {"status": "launched", "run_id": run_id, "message": "Sunday review running in background."}


@crew_router.get("/strategies")
def list_strategies(status: Optional[str] = None, asset_class: Optional[str] = None):
    """List crew strategies with optional filters."""
    conn = _db()
    try:
        query = "SELECT * FROM crew_strategies WHERE 1=1"
        params = []
        if status:
            query += " AND status = ?"
            params.append(status)
        if asset_class:
            query += " AND asset_class = ?"
            params.append(asset_class)
        query += " ORDER BY created_at DESC LIMIT 100"
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@crew_router.get("/strategies/{strategy_id}")
def get_strategy(strategy_id: int):
    """Get strategy details by ID."""
    conn = _db()
    try:
        row = conn.execute("SELECT * FROM crew_strategies WHERE id = ?", (strategy_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Strategy not found.")
        return dict(row)
    finally:
        conn.close()


@crew_router.get("/runs")
def list_runs(limit: int = 50):
    """List recent crew pipeline runs."""
    conn = _db()
    try:
        rows = conn.execute(
            "SELECT * FROM crew_runs ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@crew_router.get("/performance")
def get_performance():
    """
    Trade performance stats from the learning loop.
    Shows total trades, win rate, total P&L, best/worst trades,
    and breakdown by strategy type.
    """
    from crew.learning import get_performance_stats
    return get_performance_stats()


@crew_router.get("/lock/status")
def lock_status():
    """Current Ollama lock state — who holds it and for how long."""
    from shared.ollama_lock import get_lock_status
    return get_lock_status()


@crew_router.post("/lock/kill")
def kill_lock():
    """Force-release a stuck Ollama lock (removes the lock file)."""
    from shared.ollama_lock import OllamaLock, get_lock_status
    before = get_lock_status()
    OllamaLock.force_release()
    return {"status": "released", "was_locked": before.get("locked"), "was_held_by": before.get("caller")}


@crew_router.post("/learning/sync")
def sync_learning():
    """
    Manually trigger a sync of closed Alpaca positions → crew_trade_results.
    Called automatically before each pipeline run; this endpoint allows manual trigger.
    """
    from crew.learning import sync_closed_positions
    return sync_closed_positions()


# ---------------------------------------------------------------------------
# Portfolio router
# ---------------------------------------------------------------------------

portfolio_router = APIRouter()
positions_router = APIRouter()
_pm = PortfolioManager()
ensure_matrix_shared_records()


class NeoMirrorRequest(BaseModel):
    cash: Optional[float] = None
    total_value: Optional[float] = None
    season: Optional[int] = None
    replace_positions: bool = True
    positions: list[dict] = []
    trades: list[dict] = []
    chat_message: Optional[str] = None
    chat_context: Optional[str] = None
    war_room_take: Optional[str] = None
    war_room_symbol: str = "SPY"
    strategy_mode: Optional[str] = None
    symbol: Optional[str] = None


@positions_router.get("")
def list_arena_positions(player_id: Optional[str] = None,
                         asset_type: Optional[str] = None,
                         limit: int = 200):
    """Expose Arena positions from the legacy positions table on port 8000."""
    safe_limit = max(1, min(limit, 1000))
    conn = _db()
    try:
        query = (
            "SELECT p.id, p.player_id, ap.display_name, ap.provider, "
            "p.symbol, p.qty, p.avg_price, p.asset_type, p.option_type, "
            "p.strike_price, p.expiry_date, p.opened_at, p.high_watermark "
            "FROM positions p "
            "LEFT JOIN ai_players ap ON ap.id = p.player_id "
            "WHERE 1=1"
        )
        params = []
        if player_id:
            query += " AND p.player_id = ?"
            params.append(player_id)
        if asset_type:
            query += " AND p.asset_type = ?"
            params.append(asset_type)
        query += " ORDER BY datetime(p.opened_at) DESC, p.id DESC LIMIT ?"
        params.append(safe_limit)

        rows = conn.execute(query, params).fetchall()
        positions = []
        for row in rows:
            positions.append({
                "id": row["id"],
                "player_id": row["player_id"],
                "player_name": row["display_name"] or row["player_id"],
                "provider": row["provider"],
                "ticker": row["symbol"],
                "quantity": float(row["qty"] or 0),
                "entry_price": float(row["avg_price"] or 0),
                "current_price": float(row["high_watermark"] or row["avg_price"] or 0),
                "asset_type": row["asset_type"] or "stock",
                "option_type": row["option_type"],
                "strike_price": row["strike_price"],
                "expiration_date": row["expiry_date"],
                "opened_at": row["opened_at"],
                "high_watermark": row["high_watermark"],
            })
        return {
            "positions": positions,
            "count": len(positions),
            "source": "positions",
        }
    finally:
        conn.close()


@crew_router.get("/matrix/neo/status")
def neo_status():
    """Matrix Neo shared-state status on port 8000."""
    sync_neo_from_native_portfolio()
    info = ensure_matrix_shared_records()
    conn = _db()
    try:
        player = conn.execute(
            "SELECT id, display_name, provider, model_id, cash, is_active, season "
            "FROM ai_players WHERE id=?",
            (NEO_PLAYER_ID,),
        ).fetchone()
        positions = conn.execute(
            "SELECT COUNT(*) AS cnt FROM positions WHERE player_id=?",
            (NEO_PLAYER_ID,),
        ).fetchone()
        trades = conn.execute(
            "SELECT COUNT(*) AS cnt FROM trades WHERE player_id=?",
            (NEO_PLAYER_ID,),
        ).fetchone()
        war_room = conn.execute(
            "SELECT symbol, take, created_at FROM war_room WHERE player_id=? ORDER BY created_at DESC LIMIT 1",
            (NEO_PLAYER_ID,),
        ).fetchone()
        chat = conn.execute(
            "SELECT message, created_at FROM ai_chat WHERE player_id=? ORDER BY created_at DESC LIMIT 1",
            (NEO_PLAYER_ID,),
        ).fetchone()
        history = conn.execute(
            "SELECT total_value, cash, positions_value, recorded_at "
            "FROM portfolio_history WHERE player_id=? ORDER BY recorded_at DESC LIMIT 1",
            (NEO_PLAYER_ID,),
        ).fetchone()
        out = annotate_player_payload(dict(player)) if player else {"player_id": NEO_PLAYER_ID}
        out.update({
            "portfolio_id": info["portfolio_id"],
            "positions_count": int(positions["cnt"] or 0) if positions else 0,
            "trades_count": int(trades["cnt"] or 0) if trades else 0,
            "latest_war_room": dict(war_room) if war_room else None,
            "latest_chat": dict(chat) if chat else None,
            "latest_history": dict(history) if history else None,
        })
        return out
    finally:
        conn.close()


@crew_router.post("/matrix/neo/mirror")
def neo_mirror(req: NeoMirrorRequest):
    """Mirror Neo state from Matrix / 8000 into shared comparison surfaces."""
    return sync_neo_snapshot(req.dict())


@portfolio_router.get("/")
def list_portfolios(active_only: bool = False):
    """List all portfolios."""
    return _pm.get_portfolios(active_only=active_only)


@portfolio_router.get("/unified")
def unified_view():
    """Unified P&L view across ALL portfolios."""
    return _pm.get_unified_view()


@portfolio_router.get("/exposure")
def exposure(portfolio_id: Optional[int] = None):
    """Exposure breakdown for a portfolio (or all)."""
    return _pm.get_exposure(portfolio_id)


@portfolio_router.get("/{portfolio_id}")
def get_portfolio(portfolio_id: int):
    """Get specific portfolio."""
    result = _pm.get_portfolio(portfolio_id)
    if not result:
        raise HTTPException(status_code=404, detail="Portfolio not found.")
    return result


@portfolio_router.post("/add")
def add_portfolio(req: AddPortfolioRequest):
    """Add a new portfolio."""
    return _pm.add_portfolio(
        name=req.name, broker=req.broker, account_type=req.account_type,
        initial_balance=req.initial_balance, is_human=req.is_human, notes=req.notes,
        execution_mode=req.execution_mode, portfolio_type=req.type,
    )


@portfolio_router.post("/{portfolio_id}/activate")
def activate_portfolio(portfolio_id: int, req: ActivatePortfolioRequest):
    """Activate or deactivate a portfolio."""
    return _pm.activate_portfolio(portfolio_id, active=req.active)


@portfolio_router.get("/positions/open")
def open_positions(portfolio_id: Optional[int] = None, asset_class: Optional[str] = None):
    """List open positions with optional filters."""
    return _pm.get_open_positions(portfolio_id=portfolio_id, asset_class=asset_class)


@portfolio_router.post("/positions/open")
def open_position(req: OpenPositionRequest):
    """Open a new position. BLOCKS human-managed and tracking-only portfolios."""
    conn = _db()
    try:
        portfolio = conn.execute(
            "SELECT * FROM portfolios WHERE id = ?", (req.portfolio_id,)
        ).fetchone()
        if not portfolio:
            raise HTTPException(status_code=404, detail="Portfolio not found.")
        portfolio = dict(portfolio)
        execution = PortfolioManager.can_execute(portfolio)
        if not execution["allowed"]:
            raise HTTPException(
                status_code=403,
                detail=f"BLOCKED: Portfolio '{portfolio['name']}' is {execution['reason']}.",
            )
    finally:
        conn.close()

    return _pm.open_position(
        portfolio_id=req.portfolio_id, ticker=req.ticker, asset_class=req.asset_class,
        direction=req.direction, quantity=req.quantity, entry_price=req.entry_price,
        stop_loss=req.stop_loss, take_profit=req.take_profit,
        option_type=req.option_type, strike_price=req.strike_price,
        expiration_date=req.expiration_date, spread_type=req.spread_type,
        spread_legs=req.spread_legs, metal_type=req.metal_type, metal_oz=req.metal_oz,
        notes=req.notes,
    )


@portfolio_router.post("/positions/close")
def close_position(req: ClosePositionRequest):
    """Close a position and calculate P&L."""
    return _pm.close_position(req.position_id, req.close_price, req.notes)


@portfolio_router.get("/{portfolio_id}/closed_trades")
def closed_trades(portfolio_id: int, limit: int = 20):
    """Recent closed trades for a portfolio."""
    conn = _db()
    try:
        rows = conn.execute(
            "SELECT * FROM portfolio_positions WHERE portfolio_id = ? AND status = 'closed' ORDER BY closed_at DESC LIMIT ?",
            (portfolio_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
