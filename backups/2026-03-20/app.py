from __future__ import annotations
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import sqlite3
import json
import os
import threading
import uvicorn

app = FastAPI(title="TradeMinds Arena")
DB = "data/trader.db"

# Convert UTC timestamps to Arizona time (MST = UTC-7, no DST) in all API responses
import re
from datetime import datetime, timezone, timedelta
_AZ_TZ = timezone(timedelta(hours=-7))
_TS_RE = re.compile(r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}')

def _to_arizona(val):
    """Convert a UTC timestamp string to Arizona time string."""
    if not isinstance(val, str) or not _TS_RE.match(val):
        return val
    try:
        s = val.replace('T', ' ').split('.')[0]  # strip fractional seconds
        utc_dt = datetime.strptime(s, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        az_dt = utc_dt.astimezone(_AZ_TZ)
        return az_dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return val

def _convert_timestamps(obj):
    """Recursively convert all timestamp strings in a response to Arizona time."""
    if isinstance(obj, dict):
        return {k: _convert_timestamps(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_timestamps(item) for item in obj]
    if isinstance(obj, str):
        return _to_arizona(obj)
    return obj

from fastapi.routing import APIRoute
from fastapi.responses import JSONResponse
from starlette.requests import Request as StarletteRequest

class TimezoneRoute(APIRoute):
    """Custom route that converts UTC timestamps to Arizona time in all JSON responses."""
    def get_route_handler(self):
        original_handler = super().get_route_handler()
        async def handler(request: StarletteRequest):
            response = await original_handler(request)
            if isinstance(response, JSONResponse):
                try:
                    import json as _json
                    data = _json.loads(response.body)
                    converted = _convert_timestamps(data)
                    return JSONResponse(content=converted, status_code=response.status_code)
                except Exception:
                    pass
            return response
        return handler

app.router.route_class = TimezoneRoute

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


# --- Arena Endpoints ---

@app.get("/api/arena/leaderboard")
def leaderboard(season: int = 0):
    from engine.paper_trader import get_portfolio_with_pnl
    from engine.market_data import get_all_prices
    from config import WATCH_STOCKS

    conn = _conn()

    # Determine current season
    current_season = 2
    s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
    if s_row:
        current_season = int(s_row["value"])

    all_seasons = (season == -1)
    if season <= 0 and not all_seasons:
        season = current_season

    players = conn.execute("""
        SELECT p.id, p.display_name, p.provider, p.model_id, p.cash, p.is_active, p.is_halted, COALESCE(p.is_paused, 0) as is_paused
        FROM ai_players p WHERE p.is_active = 1 AND p.id NOT LIKE '%cto%'
        ORDER BY p.id
    """).fetchall()

    # Season-filtered trade counts
    trade_counts = {}
    if all_seasons:
        for row in conn.execute("SELECT player_id, COUNT(*) as cnt FROM trades GROUP BY player_id").fetchall():
            trade_counts[row["player_id"]] = row["cnt"]
    else:
        for row in conn.execute("SELECT player_id, COUNT(*) as cnt FROM trades WHERE season=? GROUP BY player_id", (season,)).fetchall():
            trade_counts[row["player_id"]] = row["cnt"]

    # Season-filtered win rate
    win_data = {}
    win_q = """
        SELECT player_id,
               COUNT(*) as total_sells,
               SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins
        FROM trades WHERE action='SELL' AND realized_pnl IS NOT NULL AND realized_pnl != 0"""
    if all_seasons:
        win_q += " GROUP BY player_id"
        win_rows = conn.execute(win_q).fetchall()
    else:
        win_q += " AND season=? GROUP BY player_id"
        win_rows = conn.execute(win_q, (season,)).fetchall()
    for row in win_rows:
        total = row["total_sells"]
        win_data[row["player_id"]] = round(row["wins"] / total * 100, 1) if total > 0 else 0

    # Season-filtered profit factor (sum of winning trades / sum of losing trades)
    profit_factor_data = {}
    pf_q = """
        SELECT player_id,
               COALESCE(SUM(CASE WHEN realized_pnl > 0 THEN realized_pnl ELSE 0 END), 0) as total_gains,
               COALESCE(SUM(CASE WHEN realized_pnl < 0 THEN ABS(realized_pnl) ELSE 0 END), 0) as total_losses
        FROM trades WHERE action='SELL' AND realized_pnl IS NOT NULL"""
    if all_seasons:
        pf_q += " GROUP BY player_id"
        pf_rows = conn.execute(pf_q).fetchall()
    else:
        pf_q += " AND season=? GROUP BY player_id"
        pf_rows = conn.execute(pf_q, (season,)).fetchall()
    for row in pf_rows:
        gains = row["total_gains"]
        losses = row["total_losses"]
        pf = round(gains / losses, 2) if losses > 0 else (999.0 if gains > 0 else 0.0)
        profit_factor_data[row["player_id"]] = {"profit_factor": pf, "realized_gains": round(gains, 2), "realized_losses": round(losses, 2)}

    # Day P&L from portfolio_history (season-filtered)
    day_pnl = {}
    if all_seasons:
        day_rows = conn.execute("""
            SELECT player_id, total_value FROM portfolio_history
            WHERE recorded_at >= datetime('now', '-24 hours')
            ORDER BY recorded_at ASC
        """).fetchall()
    else:
        day_rows = conn.execute("""
            SELECT player_id, total_value FROM portfolio_history
            WHERE recorded_at >= datetime('now', '-24 hours') AND season=?
            ORDER BY recorded_at ASC
        """, (season,)).fetchall()
    for row in day_rows:
        pid = row["player_id"]
        if pid not in day_pnl:
            day_pnl[pid] = {"first": row["total_value"], "last": row["total_value"]}
        day_pnl[pid]["last"] = row["total_value"]

    conn.close()

    is_current = (season == current_season) or all_seasons

    if is_current:
        # Live data for current season
        try:
            prices = get_all_prices(WATCH_STOCKS)
        except Exception:
            prices = {}
        result = []
        for p in players:
            try:
                pnl = get_portfolio_with_pnl(p["id"], prices)
                total_value = pnl["total_value"]
                positions_value = pnl["total_positions_value"]
                unrealized_pnl = pnl["total_unrealized_pnl"]
                return_pct = pnl["return_pct"]
            except Exception:
                total_value = round(p["cash"], 2)
                positions_value = 0
                unrealized_pnl = 0
                starting = 3500 if p["id"] == "dayblade-0dte" else (7021.81 if p["id"] == "steve-webull" else 7000)
                return_pct = round((total_value - starting) / starting * 100, 2)
            pnl_history = day_pnl.get(p["id"], {})
            day_change = pnl_history.get("last", total_value) - pnl_history.get("first", total_value)
            pf_info = profit_factor_data.get(p["id"], {})
            result.append({
                "player_id": p["id"],
                "name": p["display_name"],
                "provider": p["provider"],
                "model": p["model_id"],
                "cash": round(p["cash"], 2),
                "positions_value": positions_value,
                "total_value": total_value,
                "unrealized_pnl": unrealized_pnl,
                "return_pct": return_pct,
                "day_change": round(day_change, 2),
                "trades": trade_counts.get(p["id"], 0),
                "win_rate": win_data.get(p["id"], 0),
                "profit_factor": pf_info.get("profit_factor", 0),
                "realized_gains": pf_info.get("realized_gains", 0),
                "realized_losses": pf_info.get("realized_losses", 0),
                "is_active": bool(p["is_active"]),
                "is_halted": bool(p["is_halted"]),
                "is_paused": bool(p["is_paused"]),
            })
    else:
        # Historical season — reconstruct final values from last portfolio_history snapshot
        conn2 = _conn()
        result = []
        starting = 10000.0
        for p in players:
            pid = p["id"]
            # Season-aware starting capital: S1-S3 used $10k, S4+ uses $7k
            if pid == "steve-webull":
                s_starting = 7021.81
            elif pid == "dayblade-0dte":
                s_starting = 2000.0 if season == 1 else (5000.0 if season <= 3 else 3500.0)
            else:
                s_starting = 10000.0 if season <= 3 else 7000.0
            # Get last snapshot for this season
            snap = conn2.execute(
                "SELECT total_value, cash, positions_value FROM portfolio_history "
                "WHERE player_id=? AND season=? ORDER BY recorded_at DESC LIMIT 1", (pid, season)
            ).fetchone()
            # Get realized P&L sum for the season
            rpnl = conn2.execute(
                "SELECT COALESCE(SUM(realized_pnl), 0) as total FROM trades WHERE player_id=? AND season=? AND action='SELL' AND realized_pnl IS NOT NULL",
                (pid, season)
            ).fetchone()

            if snap:
                total_value = snap["total_value"]
            else:
                total_value = s_starting + (rpnl["total"] if rpnl else 0)

            return_pct = round((total_value - s_starting) / s_starting * 100, 2) if s_starting > 0 else 0
            pnl_history = day_pnl.get(pid, {})
            day_change = pnl_history.get("last", total_value) - pnl_history.get("first", total_value)

            result.append({
                "player_id": pid,
                "name": p["display_name"],
                "provider": p["provider"],
                "model": p["model_id"],
                "cash": round(snap["cash"], 2) if snap else round(total_value, 2),
                "positions_value": round(snap["positions_value"], 2) if snap else 0,
                "total_value": round(total_value, 2),
                "unrealized_pnl": 0,
                "return_pct": return_pct,
                "day_change": round(day_change, 2),
                "trades": trade_counts.get(pid, 0),
                "win_rate": win_data.get(pid, 0),
                "profit_factor": profit_factor_data.get(pid, {}).get("profit_factor", 0),
                "realized_gains": profit_factor_data.get(pid, {}).get("realized_gains", 0),
                "realized_losses": profit_factor_data.get(pid, {}).get("realized_losses", 0),
                "is_active": bool(p["is_active"]),
                "is_halted": False,
                "is_paused": False,
            })
        conn2.close()

    result.sort(key=lambda x: x["total_value"], reverse=True)
    return {"season": -1 if all_seasons else season, "current_season": current_season, "leaderboard": result}


@app.get("/api/arena/player/{player_id}")
def player_detail(player_id: str):
    from engine.paper_trader import get_portfolio_with_pnl
    from engine.market_data import get_stock_price

    conn = _conn()
    player = conn.execute("SELECT * FROM ai_players WHERE id=?", (player_id,)).fetchone()
    if not player:
        conn.close()
        return {"error": "Player not found"}

    positions = conn.execute(
        "SELECT symbol, qty, avg_price, asset_type, option_type, strike_price, expiry_date, opened_at "
        "FROM positions WHERE player_id=?", (player_id,)
    ).fetchall()

    # Get trade stats
    stats = conn.execute("""
        SELECT COUNT(*) as total_trades,
               SUM(CASE WHEN action='BUY' THEN 1 ELSE 0 END) as buys,
               SUM(CASE WHEN action='SELL' THEN 1 ELSE 0 END) as sells,
               SUM(CASE WHEN action LIKE 'BUY_%' THEN 1 ELSE 0 END) as options_trades
        FROM trades WHERE player_id=?
    """, (player_id,)).fetchone()

    # Realized P&L from completed sells
    realized = conn.execute("""
        SELECT COALESCE(SUM(
            CASE WHEN action='SELL' THEN qty * price ELSE 0 END -
            CASE WHEN action='SELL' THEN qty * (
                SELECT t2.price FROM trades t2
                WHERE t2.player_id=trades.player_id AND t2.symbol=trades.symbol
                AND t2.action='BUY' ORDER BY t2.executed_at DESC LIMIT 1
            ) ELSE 0 END
        ), 0) as total_realized
        FROM trades WHERE player_id=? AND action='SELL'
    """, (player_id,)).fetchone()

    # Look up data sources for each open position from its BUY trade
    position_sources = {}
    try:
        conn2 = _conn()
        conn2.execute("SELECT sources FROM trades LIMIT 1")  # test column exists
        for pos in positions:
            src_row = conn2.execute(
                "SELECT sources FROM trades WHERE player_id=? AND symbol=? AND action IN ('BUY','BUY_CALL','BUY_PUT') "
                "ORDER BY executed_at DESC LIMIT 1",
                (player_id, pos["symbol"])
            ).fetchone()
            if src_row and src_row["sources"]:
                position_sources[pos["symbol"]] = src_row["sources"]
        conn2.close()
    except Exception:
        pass

    conn.close()

    # Fetch live prices for positions
    prices = {}
    symbols = list(set(p["symbol"] for p in positions))
    for sym in symbols:
        data = get_stock_price(sym)
        if "error" not in data:
            prices[sym] = data

    pnl_data = get_portfolio_with_pnl(player_id, prices)

    # Attach sources to each position
    for pos in pnl_data["positions"]:
        pos["sources"] = position_sources.get(pos["symbol"], "")

    return {
        "player_id": player["id"],
        "name": player["display_name"],
        "provider": player["provider"],
        "model": player["model_id"],
        "cash": round(player["cash"], 2),
        "total_value": pnl_data["total_value"],
        "return_pct": pnl_data["return_pct"],
        "total_unrealized_pnl": pnl_data["total_unrealized_pnl"],
        "total_positions_value": pnl_data["total_positions_value"],
        "is_active": bool(player["is_active"]),
        "is_halted": bool(player["is_halted"]),
        "positions": pnl_data["positions"],
        "stats": {
            "total_trades": stats["total_trades"] if stats else 0,
            "buys": stats["buys"] if stats else 0,
            "sells": stats["sells"] if stats else 0,
            "options_trades": stats["options_trades"] if stats else 0,
        },
    }


@app.get("/api/arena/player/{player_id}/trades")
def player_trades(player_id: str, limit: int = 50):
    conn = _conn()
    # Check if sources column exists
    _has_src = False
    try:
        conn.execute("SELECT sources FROM trades LIMIT 1")
        _has_src = True
    except Exception:
        pass
    _sc = ", sources" if _has_src else ""
    trades = conn.execute(
        f"SELECT symbol, action, qty, price, asset_type, option_type, reasoning, confidence, executed_at{_sc} "
        "FROM trades WHERE player_id=? ORDER BY executed_at DESC LIMIT ?",
        (player_id, limit)
    ).fetchall()
    conn.close()
    return [dict(t) for t in trades]


@app.get("/api/arena/player/{player_id}/signals")
def player_signals(player_id: str, limit: int = 50):
    conn = _conn()
    _has_src = False
    try:
        conn.execute("SELECT sources FROM signals LIMIT 1")
        _has_src = True
    except Exception:
        pass
    _sc = ", sources" if _has_src else ""
    signals = conn.execute(
        f"SELECT symbol, signal, confidence, reasoning, asset_type, option_type, created_at{_sc} "
        "FROM signals WHERE player_id=? ORDER BY created_at DESC LIMIT ?",
        (player_id, limit)
    ).fetchall()
    conn.close()
    return [dict(s) for s in signals]


@app.get("/api/arena/player/{player_id}/history")
def player_history(player_id: str):
    conn = _conn()
    history = conn.execute(
        "SELECT total_value, cash, positions_value, recorded_at "
        "FROM portfolio_history WHERE player_id=? ORDER BY recorded_at ASC",
        (player_id,)
    ).fetchall()
    conn.close()
    return [dict(h) for h in history]


# --- General Endpoints ---

@app.get("/api/status")
def status():
    conn = _conn()
    # Get current season
    s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
    current_season = int(s_row["value"]) if s_row else 1

    players = conn.execute("SELECT COUNT(*) as cnt FROM ai_players WHERE is_active=1").fetchone()
    trades = conn.execute("SELECT COUNT(*) as cnt FROM trades WHERE season=?", (current_season,)).fetchone()
    signals = conn.execute("SELECT COUNT(*) as cnt FROM signals WHERE season=?", (current_season,)).fetchone()
    chat_count = conn.execute("SELECT COUNT(*) as cnt FROM ai_chat").fetchone()
    news_count = conn.execute("SELECT COUNT(*) as cnt FROM market_news").fetchone()

    # Total portfolio value
    total_val = conn.execute("""
        SELECT SUM(p.cash + COALESCE(pos_val, 0)) as total
        FROM ai_players p
        LEFT JOIN (SELECT player_id, SUM(qty * avg_price) as pos_val FROM positions GROUP BY player_id) pv
        ON p.id = pv.player_id
        WHERE p.is_active = 1
    """).fetchone()

    conn.close()
    return {
        "status": "running",
        "current_season": current_season,
        "active_players": players["cnt"],
        "total_trades": trades["cnt"],
        "total_signals": signals["cnt"],
        "total_chat_messages": chat_count["cnt"] if chat_count else 0,
        "total_news": news_count["cnt"] if news_count else 0,
        "total_portfolio_value": round(total_val["total"], 2) if total_val and total_val["total"] else 0,
    }


_trades_cache = {"data": None, "ts": 0, "key": ""}

@app.get("/api/trades/recent")
def recent_trades(limit: int = 30, season: int = 0):
    import time as _time

    # Cache key based on params
    cache_key = f"{limit}:{season}"
    if _trades_cache["key"] == cache_key and _time.time() - _trades_cache["ts"] < 15:
        return _trades_cache["data"]

    conn = _conn()
    # Determine season (-1 = all seasons)
    all_seasons = (season == -1)
    if season <= 0 and not all_seasons:
        s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        season = int(s_row["value"]) if s_row else 1
    # Check if sources column exists (migration may not have run yet)
    _has_sources = False
    try:
        conn.execute("SELECT sources FROM trades LIMIT 1")
        _has_sources = True
    except Exception:
        pass
    _src_col = ", t.sources" if _has_sources else ""

    if all_seasons:
        trades = conn.execute(
            "SELECT t.player_id, p.display_name, p.provider, t.symbol, t.action, t.qty, t.price, "
            "t.asset_type, t.option_type, t.reasoning, t.confidence, t.executed_at, "
            f"t.entry_price, t.exit_price, t.realized_pnl, t.strike_price, t.expiry_date{_src_col} "
            "FROM trades t JOIN ai_players p ON t.player_id = p.id "
            "ORDER BY t.executed_at DESC LIMIT ?", (limit,)
        ).fetchall()
    else:
        trades = conn.execute(
            "SELECT t.player_id, p.display_name, p.provider, t.symbol, t.action, t.qty, t.price, "
            "t.asset_type, t.option_type, t.reasoning, t.confidence, t.executed_at, "
            f"t.entry_price, t.exit_price, t.realized_pnl, t.strike_price, t.expiry_date{_src_col} "
            "FROM trades t JOIN ai_players p ON t.player_id = p.id "
            "WHERE t.season=? "
            "ORDER BY t.executed_at DESC LIMIT ?", (season, limit)
        ).fetchall()

    # Get current prices — use cached batch prices instead of individual calls
    open_symbols = set()
    for t in trades:
        if t["action"] in ("BUY", "BUY_CALL", "BUY_PUT"):
            open_symbols.add(t["symbol"])

    current_prices = {}
    if open_symbols:
        try:
            from engine.market_data import get_all_prices
            # Only fetch prices for symbols in the result set (not all WATCH_STOCKS)
            all_prices = get_all_prices(list(open_symbols))
            for sym in open_symbols:
                if sym in all_prices:
                    current_prices[sym] = all_prices[sym]["price"]
        except Exception:
            pass

    conn.close()

    result = []
    for t in trades:
        d = dict(t)
        # Add P&L for every trade
        if t["action"] == "SELL" and t["realized_pnl"] is not None:
            d["pnl"] = round(t["realized_pnl"], 2)
            d["pnl_pct"] = round(
                ((t["exit_price"] or t["price"]) - (t["entry_price"] or t["price"]))
                / (t["entry_price"] or t["price"]) * 100, 2
            ) if t["entry_price"] else None
        elif t["action"] in ("BUY", "BUY_CALL", "BUY_PUT"):
            # Unrealized P&L for open positions
            sym = t["symbol"]
            is_option = (t["asset_type"] == "option" or t["action"] in ("BUY_CALL", "BUY_PUT"))
            if is_option:
                # Estimate option value using intrinsic value
                from engine.paper_trader import estimate_option_price
                stock_price = current_prices.get(sym, 0)
                ot = (t["option_type"] if t["option_type"] else None) or ("call" if t["action"] == "BUY_CALL" else "put")
                strike = t["strike_price"] if t["strike_price"] else None
                est = estimate_option_price(ot, strike, stock_price, t["price"])
                d["pnl"] = round((est - t["price"]) * t["qty"], 2)
                d["pnl_pct"] = round((est - t["price"]) / t["price"] * 100, 2) if t["price"] > 0 else 0
                d["current_price"] = round(est, 2)
            elif sym in current_prices:
                cur = current_prices[sym]
                entry = t["price"]
                d["pnl"] = round((cur - entry) * t["qty"], 2)
                d["pnl_pct"] = round((cur - entry) / entry * 100, 2) if entry > 0 else 0
                d["current_price"] = round(cur, 2)
            else:
                d["pnl"] = None
                d["pnl_pct"] = None
        else:
            d["pnl"] = None
            d["pnl_pct"] = None
        result.append(d)

    _trades_cache["data"] = result
    _trades_cache["ts"] = _time.time()
    _trades_cache["key"] = cache_key
    return result


@app.get("/api/signals/recent")
def recent_signals(limit: int = 50, season: int = 0):
    conn = _conn()
    if season <= 0:
        s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        season = int(s_row["value"]) if s_row else 1
    # Check if sources column exists
    _has_src = False
    try:
        conn.execute("SELECT sources FROM signals LIMIT 1")
        _has_src = True
    except Exception:
        pass
    _sc = ", s.sources" if _has_src else ""
    signals = conn.execute(
        "SELECT s.player_id, p.display_name, p.provider, s.symbol, s.signal, s.confidence, "
        f"s.reasoning, s.asset_type, s.option_type, s.created_at{_sc} "
        "FROM signals s JOIN ai_players p ON s.player_id = p.id "
        "WHERE s.season=? "
        "ORDER BY s.created_at DESC LIMIT ?", (season, limit)
    ).fetchall()
    conn.close()
    return [dict(s) for s in signals]


@app.get("/api/arena/comparison")
def comparison_chart(season: int = 0):
    """Portfolio value history for all players, optionally filtered by season."""
    conn = _conn()
    all_seasons = (season == -1)
    if season <= 0 and not all_seasons:
        s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        season = int(s_row["value"]) if s_row else 1
    if all_seasons:
        # All seasons
        data = conn.execute(
            "SELECT h.player_id, p.display_name, h.total_value, h.recorded_at, h.season "
            "FROM portfolio_history h JOIN ai_players p ON h.player_id = p.id "
            "ORDER BY h.recorded_at ASC"
        ).fetchall()
    else:
        data = conn.execute(
            "SELECT h.player_id, p.display_name, h.total_value, h.recorded_at, h.season "
            "FROM portfolio_history h JOIN ai_players p ON h.player_id = p.id "
            "WHERE h.season = ? ORDER BY h.recorded_at ASC", (season,)
        ).fetchall()
    conn.close()

    by_player = {}
    for row in data:
        pid = row["player_id"]
        if pid not in by_player:
            by_player[pid] = {"name": row["display_name"], "history": []}
        by_player[pid]["history"].append({
            "value": row["total_value"],
            "time": row["recorded_at"],
        })
    return by_player


# --- Chat Endpoints ---

@app.get("/api/chat/recent")
def recent_chat(limit: int = 50):
    conn = _conn()
    messages = conn.execute(
        "SELECT c.id, c.player_id, p.display_name, p.provider, c.message, "
        "c.context, c.reply_to, c.created_at "
        "FROM ai_chat c JOIN ai_players p ON c.player_id = p.id "
        "ORDER BY c.created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(m) for m in messages]


@app.get("/api/chat/player/{player_id}")
def player_chat(player_id: str, limit: int = 20):
    conn = _conn()
    messages = conn.execute(
        "SELECT c.id, c.player_id, p.display_name, p.provider, c.message, "
        "c.context, c.reply_to, c.created_at "
        "FROM ai_chat c JOIN ai_players p ON c.player_id = p.id "
        "WHERE c.player_id = ? ORDER BY c.created_at DESC LIMIT ?",
        (player_id, limit)
    ).fetchall()
    conn.close()
    return [dict(m) for m in messages]


# --- News Endpoints ---

@app.get("/api/news/recent")
def recent_news(limit: int = 30):
    conn = _conn()
    news = conn.execute(
        "SELECT * FROM market_news ORDER BY fetched_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(n) for n in news]


@app.get("/api/news/{symbol}")
def symbol_news(symbol: str, limit: int = 10):
    conn = _conn()
    news = conn.execute(
        "SELECT * FROM market_news WHERE symbol=? ORDER BY fetched_at DESC LIMIT ?",
        (symbol.upper(), limit)
    ).fetchall()
    conn.close()
    return [dict(n) for n in news]


# --- P&L & Equity Endpoints ---

@app.get("/api/arena/player/{player_id}/pnl")
def player_pnl(player_id: str):
    """Get live unrealized P&L for a player's positions."""
    from engine.paper_trader import get_portfolio_with_pnl
    from engine.market_data import get_stock_price

    conn = _conn()
    positions = conn.execute(
        "SELECT symbol FROM positions WHERE player_id=?", (player_id,)
    ).fetchall()
    conn.close()

    prices = {}
    for p in positions:
        data = get_stock_price(p["symbol"])
        if "error" not in data:
            prices[p["symbol"]] = data

    return get_portfolio_with_pnl(player_id, prices)


@app.get("/api/arena/equity-curve")
def equity_curve(player_id: str = None, season: int = 0):
    """Get equity curve data, optionally filtered by season and player."""
    conn = _conn()
    all_seasons = (season == -1)
    if season <= 0 and not all_seasons:
        s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        season = int(s_row["value"]) if s_row else 1

    q = "SELECT player_id, total_value, cash, positions_value, recorded_at, season FROM portfolio_history"
    params = []
    clauses = []
    if player_id:
        clauses.append("player_id = ?")
        params.append(player_id)
    if not all_seasons:
        clauses.append("season = ?")
        params.append(season)
    if clauses:
        q += " WHERE " + " AND ".join(clauses)
    q += " ORDER BY recorded_at ASC"
    rows = conn.execute(q, params).fetchall()
    conn.close()

    result = []
    for r in rows:
        result.append({
            "player_id": r["player_id"],
            "timestamp": r["recorded_at"],
            "total_value": r["total_value"],
            "cash": r["cash"],
            "positions_value": r["positions_value"],
            "season": r["season"],
        })
    return result


# --- DayBlade Options Endpoints ---

@app.get("/api/dayblade/status")
def dayblade_status():
    """Get DayBlade live positions, P&L, stats, DTE breakdown, streak."""
    from engine.dayblade import (
        get_portfolio_with_pnl, get_dayblade_stats,
        is_dayblade_open_window, is_dayblade_close_window,
        is_market_hours_for_dayblade, is_power_hour,
        DAYBLADE_TICKERS, DAYBLADE_CASH, MAX_POSITIONS,
        get_win_streak,
    )
    from engine.market_data import get_stock_price

    prices = {}
    for sym in DAYBLADE_TICKERS:
        data = get_stock_price(sym)
        if "error" not in data:
            prices[sym] = data

    pnl = get_portfolio_with_pnl(prices)
    stats = get_dayblade_stats()
    streak = get_win_streak()

    window = "closed"
    if is_power_hour():
        window = "power_hour"
    elif is_dayblade_open_window():
        window = "open"
    elif is_dayblade_close_window():
        window = "closing"
    elif is_market_hours_for_dayblade():
        window = "monitoring"

    return {
        "portfolio": pnl,
        "stats": stats,
        "window": window,
        "starting_cash": DAYBLADE_CASH,
        "max_positions": MAX_POSITIONS,
        "tickers": DAYBLADE_TICKERS,
        "win_streak": streak,
    }


@app.get("/api/dayblade/trades")
def dayblade_trades(limit: int = 50):
    """Recent DayBlade trades."""
    conn = _conn()
    trades = conn.execute(
        "SELECT symbol, action, qty, price, asset_type, option_type, reasoning, confidence, executed_at "
        "FROM trades WHERE player_id='dayblade-0dte' ORDER BY executed_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return [dict(t) for t in trades]


@app.get("/api/dayblade/scanner")
def dayblade_scanner():
    """Live 0DTE options scanner — cheap tickets, premium plays, and scored opportunities."""
    from engine.dte_scanner import scan_0dte_opportunities
    return scan_0dte_opportunities()


# --- Market Data Endpoints ---

@app.get("/api/market/prices")
def market_prices():
    """Get current prices for watchlist stocks (parallel fetch)."""
    from engine.market_data import get_all_prices
    from config import WATCH_STOCKS
    return get_all_prices(WATCH_STOCKS)


@app.get("/api/market/candles/{symbol}")
def market_candles(symbol: str, interval: str = "5m", range: str = "1d"):
    """Get OHLCV candles for candlestick chart with configurable range."""
    from engine.market_data import get_intraday_candles
    candles = get_intraday_candles(symbol.upper(), interval, range)
    # Also get AI entry points for this symbol (recent BUY trades)
    conn = _conn()
    if range == "1d":
        date_filter = "AND date(t.executed_at)=?"
        date_val = __import__("datetime").datetime.now().strftime("%Y-%m-%d")
        params = (symbol.upper(), date_val)
    else:
        date_filter = ""
        params = (symbol.upper(),)
    entries = conn.execute(
        "SELECT t.player_id, p.display_name, t.action, t.price, t.qty, t.executed_at "
        "FROM trades t JOIN ai_players p ON t.player_id = p.id "
        f"WHERE t.symbol=? AND t.action LIKE 'BUY%' {date_filter} "
        "ORDER BY t.executed_at",
        params
    ).fetchall()
    conn.close()
    markers = [dict(e) for e in entries]
    return {"candles": candles, "markers": markers}


@app.get("/api/market/heatmap")
def market_heatmap():
    """Get watchlist heat map data: price, change%, position weight per model."""
    from engine.market_data import get_stock_price
    from config import WATCH_STOCKS

    conn = _conn()
    # Get all active positions grouped by symbol
    positions = conn.execute(
        "SELECT symbol, SUM(qty * avg_price) as cost_basis "
        "FROM positions WHERE player_id != 'dayblade-0dte' "
        "GROUP BY symbol"
    ).fetchall()
    conn.close()

    pos_weight = {row["symbol"]: row["cost_basis"] for row in positions}
    total_invested = sum(pos_weight.values()) or 1.0

    result = []
    for sym in WATCH_STOCKS:
        data = get_stock_price(sym)
        if "error" in data:
            continue
        weight = pos_weight.get(sym, 0) / total_invested
        result.append({
            "symbol": sym,
            "price": data["price"],
            "change_pct": data["change_pct"],
            "volume": data.get("volume", 0),
            "weight": round(weight, 4),
        })
    return result


@app.get("/api/arena/confidence")
def confidence_matrix():
    """AI confidence panel: each model's latest stance on each watchlist stock."""
    from config import WATCH_STOCKS

    conn = _conn()
    # Get all active players (exclude dayblade)
    players = conn.execute(
        "SELECT id, display_name FROM ai_players WHERE is_active=1 AND id != 'dayblade-0dte'"
    ).fetchall()

    result = {}
    for p in players:
        pid = p["id"]
        stances = {}
        for sym in WATCH_STOCKS:
            row = conn.execute(
                "SELECT signal, confidence, reasoning, created_at FROM signals "
                "WHERE player_id=? AND symbol=? ORDER BY created_at DESC LIMIT 1",
                (pid, sym)
            ).fetchone()
            if row:
                sig = row["signal"]
                conf = row["confidence"] or 0
                # Map signal to stance
                if sig in ("BUY", "BUY_CALL"):
                    stance = "bullish"
                elif sig == "BUY_PUT":
                    stance = "bearish"
                else:
                    stance = "neutral"
                stances[sym] = {
                    "stance": stance,
                    "signal": sig,
                    "confidence": round(conf, 2),
                    "reasoning": (row["reasoning"] or "")[:120],
                    "updated": row["created_at"],
                }
            else:
                stances[sym] = {"stance": "neutral", "signal": "HOLD", "confidence": 0, "reasoning": "", "updated": None}
        result[pid] = {"name": p["display_name"], "stances": stances}

    conn.close()
    return result


# --- GEX Endpoints ---

@app.get("/api/market/gex")
def gex_all():
    """Get GEX data for all supported tickers."""
    from engine.gex_scanner import get_all_gex
    return get_all_gex()


@app.get("/api/market/gex/{ticker}")
def gex_ticker(ticker: str):
    """Get GEX data for a specific ticker."""
    from engine.gex_scanner import get_gex
    result = get_gex(ticker.upper())
    if result is None:
        return {"error": f"No GEX data for {ticker.upper()}"}
    return result


# --- VIX & Earnings Endpoints ---

@app.get("/api/market/vix")
def vix_status():
    """Get current VIX price and change."""
    from engine.vix_monitor import get_vix_status, get_vix_history
    return {
        "current": get_vix_status(),
        "history": get_vix_history(),
    }


@app.get("/api/market/flow-lean")
def flow_lean():
    """Get current market directional lean from options flow."""
    from engine.market_flow import get_flow_lean, get_flow_lean_history
    current = get_flow_lean()
    return {
        "current": current,
        "history": get_flow_lean_history(50),
    }


@app.get("/api/cto/briefing")
def cto_briefing():
    """Get CTO Advisory briefings — today's briefings + history."""
    from engine.cto_advisor import get_latest_briefing, get_todays_briefings, get_briefing_history
    return {
        "latest": get_latest_briefing(),
        "today": get_todays_briefings(),
        "history": get_briefing_history(14),
    }


@app.get("/api/market/earnings")
def earnings_upcoming():
    """Get watchlist stocks with earnings in next 7 days."""
    from config import WATCH_STOCKS
    from engine.earnings_calendar import get_earnings_warnings
    return get_earnings_warnings(WATCH_STOCKS)


@app.get("/api/market/sectors")
def market_sectors():
    """Sector rotation tracker: performance by sector group."""
    from engine.market_data import get_stock_price
    from engine.sector_tracker import get_sector_rotation, get_sector_exposure
    from config import WATCH_STOCKS

    prices = {}
    for sym in WATCH_STOCKS:
        data = get_stock_price(sym)
        if "error" not in data:
            prices[sym] = data

    return {
        "rotation": get_sector_rotation(prices),
        "exposure": get_sector_exposure(),
    }


@app.get("/api/market/correlation")
def market_correlation():
    """Correlation matrix for watchlist stocks (30-day)."""
    from engine.correlation import get_watchlist_correlation
    return get_watchlist_correlation()


@app.get("/api/market/correlation/{player_id}")
def player_correlation(player_id: str):
    """Correlation matrix for a player's positions."""
    from engine.correlation import get_portfolio_correlation
    return get_portfolio_correlation(player_id)


@app.get("/api/arena/analytics")
def arena_analytics():
    """Performance analytics: Sharpe, max drawdown, win streak, best/worst trade, avg hold time."""
    from datetime import datetime, timedelta
    import math

    conn = _conn()

    # Get all players
    players = conn.execute(
        "SELECT id, display_name FROM ai_players WHERE is_active=1 AND id != 'dayblade-0dte'"
    ).fetchall()

    result = {}
    for p in players:
        pid = p["id"]

        # All trades for this player
        trades = conn.execute(
            "SELECT symbol, action, qty, price, executed_at, reasoning "
            "FROM trades WHERE player_id=? ORDER BY executed_at ASC",
            (pid,)
        ).fetchall()

        buys = {}   # symbol -> list of {qty, price, time}
        closed = []  # list of {symbol, pnl, pnl_pct, hold_seconds, buy_price, sell_price}

        for t in trades:
            sym = t["symbol"]
            if t["action"] in ("BUY", "BUY_CALL", "BUY_PUT"):
                if sym not in buys:
                    buys[sym] = []
                buys[sym].append({
                    "qty": t["qty"], "price": t["price"],
                    "time": t["executed_at"],
                })
            elif t["action"] == "SELL" and sym in buys and buys[sym]:
                buy_entry = buys[sym][0]
                pnl = (t["price"] - buy_entry["price"]) * t["qty"]
                pnl_pct = ((t["price"] / buy_entry["price"]) - 1) * 100 if buy_entry["price"] > 0 else 0
                try:
                    buy_dt = datetime.fromisoformat(buy_entry["time"].replace("Z", ""))
                    sell_dt = datetime.fromisoformat(t["executed_at"].replace("Z", ""))
                    hold_secs = (sell_dt - buy_dt).total_seconds()
                except Exception:
                    hold_secs = 0
                closed.append({
                    "symbol": sym, "pnl": pnl, "pnl_pct": pnl_pct,
                    "hold_seconds": hold_secs,
                    "buy_price": buy_entry["price"], "sell_price": t["price"],
                    "qty": t["qty"],
                })
                # Remove matched buy
                remaining = buy_entry["qty"] - t["qty"]
                if remaining <= 0.001:
                    buys[sym].pop(0)
                else:
                    buys[sym][0]["qty"] = remaining

        # Calculate metrics
        wins = [c for c in closed if c["pnl"] > 0]
        losses = [c for c in closed if c["pnl"] <= 0]
        total_closed = len(closed)
        win_rate = len(wins) / total_closed * 100 if total_closed > 0 else 0

        # Best / worst trade
        best_trade = max(closed, key=lambda x: x["pnl"]) if closed else None
        worst_trade = min(closed, key=lambda x: x["pnl"]) if closed else None

        # Win streak
        streak = 0
        max_streak = 0
        for c in closed:
            if c["pnl"] > 0:
                streak += 1
                max_streak = max(max_streak, streak)
            else:
                streak = 0

        # Average hold time
        hold_times = [c["hold_seconds"] for c in closed if c["hold_seconds"] > 0]
        avg_hold_secs = sum(hold_times) / len(hold_times) if hold_times else 0
        avg_hold_hours = avg_hold_secs / 3600

        # Max drawdown from portfolio history
        history = conn.execute(
            "SELECT total_value FROM portfolio_history WHERE player_id=? ORDER BY recorded_at ASC",
            (pid,)
        ).fetchall()
        max_dd = 0
        peak = 0
        for h in history:
            val = h["total_value"]
            if val > peak:
                peak = val
            if peak > 0:
                dd = (peak - val) / peak
                max_dd = max(max_dd, dd)

        # Sharpe ratio (from daily returns in portfolio_history)
        values = [h["total_value"] for h in history]
        daily_returns = []
        for i in range(1, len(values)):
            if values[i-1] > 0:
                daily_returns.append((values[i] - values[i-1]) / values[i-1])
        if daily_returns and len(daily_returns) > 1:
            avg_ret = sum(daily_returns) / len(daily_returns)
            std_ret = (sum((r - avg_ret)**2 for r in daily_returns) / (len(daily_returns) - 1)) ** 0.5
            sharpe = (avg_ret / std_ret) * (252 ** 0.5) if std_ret > 0 else 0
        else:
            sharpe = 0

        result[pid] = {
            "name": p["display_name"],
            "total_trades": len(trades),
            "closed_trades": total_closed,
            "win_rate": round(win_rate, 1),
            "wins": len(wins),
            "losses": len(losses),
            "sharpe_ratio": round(sharpe, 2),
            "max_drawdown_pct": round(max_dd * 100, 2),
            "longest_win_streak": max_streak,
            "avg_hold_hours": round(avg_hold_hours, 1),
            "best_trade": {
                "symbol": best_trade["symbol"],
                "pnl": round(best_trade["pnl"], 2),
                "pnl_pct": round(best_trade["pnl_pct"], 1),
            } if best_trade else None,
            "worst_trade": {
                "symbol": worst_trade["symbol"],
                "pnl": round(worst_trade["pnl"], 2),
                "pnl_pct": round(worst_trade["pnl_pct"], 1),
            } if worst_trade else None,
        }

    conn.close()
    return result


@app.get("/api/trades/export")
def export_trades(season: int = 0):
    """Export trades as CSV, optionally filtered by season."""
    from fastapi.responses import StreamingResponse
    import io, csv

    conn = _conn()
    all_seasons = (season == -1)
    if season <= 0 and not all_seasons:
        # Default: export all seasons
        all_seasons = True

    if all_seasons:
        trades = conn.execute(
            "SELECT t.player_id, p.display_name, t.symbol, t.action, t.qty, t.price, "
            "t.asset_type, t.option_type, t.strike_price, t.expiry_date, "
            "t.entry_price, t.exit_price, t.realized_pnl, "
            "t.reasoning, t.confidence, t.executed_at, t.season "
            "FROM trades t JOIN ai_players p ON t.player_id = p.id "
            "ORDER BY t.executed_at DESC"
        ).fetchall()
    else:
        trades = conn.execute(
            "SELECT t.player_id, p.display_name, t.symbol, t.action, t.qty, t.price, "
            "t.asset_type, t.option_type, t.strike_price, t.expiry_date, "
            "t.entry_price, t.exit_price, t.realized_pnl, "
            "t.reasoning, t.confidence, t.executed_at, t.season "
            "FROM trades t JOIN ai_players p ON t.player_id = p.id "
            "WHERE t.season=? ORDER BY t.executed_at DESC", (season,)
        ).fetchall()
    conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Season", "Player ID", "Player Name", "Symbol", "Action", "Qty", "Price",
                     "Entry Price", "Exit Price", "Realized P&L",
                     "Asset Type", "Option Type", "Strike", "Expiry",
                     "Reasoning", "Confidence", "Executed At"])
    for t in trades:
        writer.writerow([t["season"], t["player_id"], t["display_name"], t["symbol"], t["action"],
                        t["qty"], t["price"], t["entry_price"], t["exit_price"], t["realized_pnl"],
                        t["asset_type"], t["option_type"], t["strike_price"], t["expiry_date"],
                        t["reasoning"], t["confidence"], t["executed_at"]])

    output.seek(0)
    filename = f"trades_s{season}.csv" if not all_seasons else "trades_all.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.get("/api/market/sentiment")
def market_sentiment():
    """Get sentiment scores for all watchlist stocks."""
    from config import WATCH_STOCKS
    from engine.sentiment import get_watchlist_sentiment
    return get_watchlist_sentiment(WATCH_STOCKS)


@app.get("/api/market/sentiment/{symbol}")
def symbol_sentiment(symbol: str):
    """Get sentiment for a specific symbol."""
    from engine.sentiment import get_sentiment_for_symbol
    return get_sentiment_for_symbol(symbol.upper())


@app.get("/api/market/options-flow")
def options_flow():
    """Get options flow data for watchlist stocks with positions."""
    conn = _conn()
    symbols = conn.execute(
        "SELECT DISTINCT symbol FROM positions WHERE asset_type='option'"
    ).fetchall()
    conn.close()
    if not symbols:
        from config import WATCH_STOCKS
        syms = WATCH_STOCKS[:5]  # Limit to top 5 to avoid slow yfinance calls
    else:
        syms = [s["symbol"] for s in symbols]

    from engine.options_flow import get_flow_summary
    return get_flow_summary(syms)


@app.get("/api/market/options-alignment")
def options_alignment():
    """Check if recent AI options trades align with market flow."""
    from engine.options_flow import get_recent_ai_options_alignment
    return get_recent_ai_options_alignment()


@app.get("/api/journal")
def journal_entries(player_id: str = None, limit: int = 20, offset: int = 0):
    """Get AI journal entries."""
    from engine.ai_journal import get_journal_entries
    return get_journal_entries(player_id, limit, offset)


@app.get("/api/journal/today")
def journal_today():
    """Get today's journal entries."""
    from engine.ai_journal import get_today_journal
    return get_today_journal()


@app.get("/api/war-room")
def war_room(limit: int = 50):
    """Get recent War Room hot takes."""
    from engine.war_room import get_war_room_messages
    return get_war_room_messages(limit)


@app.post("/api/webull/sync")
def webull_sync(data: dict = None):
    """Manually sync Steve's Webull portfolio value."""
    if not data:
        return {"error": "No data provided"}
    total_value = data.get("total_value")
    if total_value is None:
        return {"error": "total_value is required"}
    try:
        total_value = float(total_value)
    except (ValueError, TypeError):
        return {"error": "total_value must be a number"}

    from engine.paper_trader import sync_webull_value, get_webull_synced
    sync_webull_value(total_value)
    synced = get_webull_synced()
    return {"ok": True, "synced": synced}


@app.get("/api/webull/synced")
def webull_synced():
    """Get the last manually synced Webull value."""
    from engine.paper_trader import get_webull_synced
    return get_webull_synced() or {"total_value": None, "synced_at": None}


@app.get("/api/system/ram")
def system_ram():
    """Get current RAM usage for dashboard display."""
    try:
        import psutil
        mem = psutil.virtual_memory()
        avail_gb = mem.available / (1024 ** 3)
        total_gb = mem.total / (1024 ** 3)
        used_gb = (mem.total - mem.available) / (1024 ** 3)
        pct = mem.percent
        if avail_gb >= 4:
            status = "green"
        elif avail_gb >= 2:
            status = "yellow"
        else:
            status = "red"
        return {
            "available_gb": round(avail_gb, 1),
            "used_gb": round(used_gb, 1),
            "total_gb": round(total_gb, 1),
            "percent_used": round(pct, 1),
            "status": status,
        }
    except ImportError:
        return {"error": "psutil not installed", "status": "unknown"}


@app.post("/api/war-room/post")
def war_room_post(data: dict = None):
    """Post a human message to the War Room as Steve."""
    # FastAPI parses JSON body into data
    if data is None:
        # Fallback: try to read raw
        return {"error": "No data provided"}

    message = (data.get("message") or "").strip()
    symbol = (data.get("symbol") or "").strip()
    strategy_mode = (data.get("strategy_mode") or "").strip().upper()

    # Validate strategy mode
    valid_modes = {"SIMONS", "DRUCKENMILLER", "PTJ", "COHEN", "ONEIL", "DALIO"}
    if strategy_mode and strategy_mode not in valid_modes:
        strategy_mode = ""

    if not message:
        return {"error": "Message is required"}

    # Default symbol to most recent war room topic
    if not symbol:
        conn = _conn()
        last = conn.execute(
            "SELECT symbol FROM war_room ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        conn.close()
        symbol = last["symbol"] if last else "SPY"

    # Tag the message with strategy mode if active
    tagged_message = message
    if strategy_mode:
        tagged_message = f"[{strategy_mode} MODE] {message}"

    # Save to war_room table (auto-migrate strategy_mode column if needed)
    conn = _conn()
    try:
        conn.execute(
            "INSERT INTO war_room (player_id, symbol, take, strategy_mode) VALUES (?, ?, ?, ?)",
            ("steve-webull", symbol, tagged_message, strategy_mode or None)
        )
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE war_room ADD COLUMN strategy_mode TEXT")
        conn.execute(
            "INSERT INTO war_room (player_id, symbol, take, strategy_mode) VALUES (?, ?, ?, ?)",
            ("steve-webull", symbol, tagged_message, strategy_mode or None)
        )
    conn.commit()
    conn.close()

    # Force the next War Room AI cycle to debate this symbol with strategy context
    try:
        from engine.war_room import set_forced_topic, set_strategy_mode
        set_forced_topic(symbol)
        if strategy_mode:
            set_strategy_mode(strategy_mode)
    except Exception:
        pass

    return {"ok": True, "symbol": symbol, "message": tagged_message, "strategy_mode": strategy_mode}


@app.post("/api/war-room/trigger")
def trigger_war_room():
    """Manually trigger a War Room cycle."""
    import threading
    from engine.war_room import run_war_room as _run_wr, get_most_volatile
    from engine.market_data import get_stock_price
    from config import WATCH_STOCKS

    def _run():
        try:
            # Get providers from DB
            conn = _conn()
            players = conn.execute(
                "SELECT id, provider, model_id, display_name FROM ai_players WHERE is_active=1"
            ).fetchall()
            conn.close()

            import os
            providers = {}
            for p in players:
                pid, prov, model, dname = p["id"], p["provider"], p["model_id"], p["display_name"]
                try:
                    if prov == "openai":
                        from engine.providers.openai_provider import OpenAIProvider
                        providers[pid] = OpenAIProvider(os.getenv("OPENAI_API_KEY"), pid, model, dname)
                    elif prov == "anthropic":
                        from engine.providers.claude_provider import ClaudeProvider
                        providers[pid] = ClaudeProvider(os.getenv("ANTHROPIC_API_KEY"), pid, model, dname)
                    elif prov == "google":
                        from engine.providers.gemini_provider import GeminiProvider
                        providers[pid] = GeminiProvider(os.getenv("GOOGLE_API_KEY"), pid, model, dname)
                    elif prov == "xai":
                        from engine.providers.grok_provider import GrokProvider
                        providers[pid] = GrokProvider(os.getenv("XAI_API_KEY"), pid, model, dname)
                    elif prov == "ollama":
                        from engine.providers.ollama_provider import OllamaProvider
                        providers[pid] = OllamaProvider(player_id=pid, model=model)
                except Exception:
                    pass

            prices = {}
            for sym in WATCH_STOCKS:
                data = get_stock_price(sym)
                if "error" not in data:
                    prices[sym] = data

            if prices and providers:
                _run_wr(providers, prices)
        except Exception as e:
            print(f"War Room trigger error: {e}")

    threading.Thread(target=_run, daemon=True).start()
    return {"status": "triggered"}


@app.get("/api/smart-money")
def smart_money(limit: int = 20):
    """Get recent Smart Money signals."""
    from engine.smart_money import get_recent_smart_money
    return get_recent_smart_money(limit)


@app.get("/api/autopilot/status")
def autopilot_status():
    """Get autopilot enabled/disabled status."""
    from engine.autopilot import is_autopilot_enabled
    return {"enabled": is_autopilot_enabled()}


@app.post("/api/autopilot/toggle")
def autopilot_toggle():
    """Toggle autopilot on/off."""
    from engine.autopilot import is_autopilot_enabled, set_autopilot
    current = is_autopilot_enabled()
    set_autopilot(not current)
    return {"enabled": not current}


_risk_radar_cache = {"all": None, "all_ts": 0, "prices": {}, "prices_ts": 0}

@app.get("/api/risk-radar")
def risk_radar(player_id: str = None):
    """Get risk radar spider chart data."""
    import time as _time
    from engine.risk_radar import get_risk_radar, get_all_risk_radars
    from engine.market_data import get_stock_price
    from config import WATCH_STOCKS

    # Cache all-players result for 5 minutes — check BEFORE fetching prices
    now = _time.time()
    if not player_id:
        if _risk_radar_cache["all"] and (now - _risk_radar_cache["all_ts"]) < 300:
            return _risk_radar_cache["all"]

    # Reuse cached prices if fresh (within 60s)
    if _risk_radar_cache["prices"] and (now - _risk_radar_cache["prices_ts"]) < 60:
        prices = _risk_radar_cache["prices"]
    else:
        prices = {}
        for sym in WATCH_STOCKS:
            data = get_stock_price(sym)
            if "error" not in data:
                prices[sym] = data
        _risk_radar_cache["prices"] = prices
        _risk_radar_cache["prices_ts"] = now

    if player_id:
        return get_risk_radar(player_id, prices)

    result = get_all_risk_radars(prices)
    _risk_radar_cache["all"] = result
    _risk_radar_cache["all_ts"] = now
    return result


@app.get("/api/backtest/{player_id}")
def backtest(player_id: str, days: int = 30,
             start_date: str = None, end_date: str = None):
    """Run Time Machine backtest for a player.

    Query params:
        days: Lookback in days (default 30, max 3650).
        start_date: Optional "YYYY-MM-DD" start (overrides days).
        end_date: Optional "YYYY-MM-DD" end (defaults to today).
    """
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
    from engine.backtester import backtest_player
    # Scale timeout with lookback length
    effective_days = days
    if start_date and end_date:
        from datetime import datetime as _dt
        effective_days = (_dt.strptime(end_date, "%Y-%m-%d") - _dt.strptime(start_date, "%Y-%m-%d")).days
    timeout = max(30, min(effective_days // 10, 120))
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            return ex.submit(backtest_player, player_id, days,
                             start_date, end_date).result(timeout=timeout)
    except FuturesTimeout:
        return {"error": f"Backtest timed out (>{timeout}s). Try a shorter date range."}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/regime")
def market_regime():
    """Get current market regime classification."""
    from engine.regime_detector import detect_regime
    return detect_regime()


@app.get("/api/whisper")
def whisper_network():
    """Get trending tickers from Whisper Network."""
    from engine.whisper_network import get_trending_tickers, check_watchlist_trending
    return {
        "trending": get_trending_tickers(),
        "watchlist_trending": check_watchlist_trending(),
    }


@app.get("/api/ghost-trades")
def ghost_trades(player_id: str = None, limit: int = 50):
    """Get ghost trades (missed opportunities)."""
    from engine.ghost_trades import get_ghost_trades
    return get_ghost_trades(player_id, limit)


@app.get("/api/ghost-trades/stats")
def ghost_stats():
    """Get aggregate ghost trade statistics."""
    from engine.ghost_trades import get_ghost_stats
    return get_ghost_stats()


@app.get("/api/alerts/recent")
def recent_alerts(limit: int = 20):
    """Get recent trades for browser notification polling."""
    conn = _conn()
    trades = conn.execute(
        "SELECT t.player_id, p.display_name, t.symbol, t.action, t.qty, t.price, "
        "t.reasoning, t.executed_at "
        "FROM trades t JOIN ai_players p ON t.player_id = p.id "
        "ORDER BY t.executed_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(t) for t in trades]


# --- Multi-Timeframe Analysis ---

@app.get("/api/market/mtf/{symbol}")
def multi_timeframe(symbol: str):
    """Get multi-timeframe analysis for a symbol."""
    from engine.multi_timeframe import get_multi_timeframe
    return get_multi_timeframe(symbol.upper())


# --- Options Greeks ---

@app.get("/api/options/greeks")
def options_greeks():
    """Get live Greeks for all options positions."""
    from engine.options_greeks import get_options_greeks
    from engine.market_data import get_stock_price

    conn = _conn()
    symbols = conn.execute(
        "SELECT DISTINCT symbol FROM positions WHERE asset_type='option'"
    ).fetchall()
    conn.close()

    prices = {}
    for s in symbols:
        data = get_stock_price(s["symbol"])
        if "error" not in data:
            prices[s["symbol"]] = data

    return get_options_greeks(prices)


@app.get("/api/options/theta-burn")
def theta_burn():
    """Get total theta burn summary."""
    from engine.options_greeks import get_total_theta_burn
    return get_total_theta_burn()


# --- Signal Tracker ---

@app.get("/api/signal-tracker")
def signal_tracker_all(limit: int = 100):
    """Get all tracked signals (active + resolved)."""
    from engine.signal_tracker import get_all_signals
    return get_all_signals(limit)


@app.get("/api/signal-tracker/active")
def signal_tracker_active():
    """Get active signals sorted by P&L."""
    from engine.signal_tracker import get_active_signals
    return get_active_signals()


@app.get("/api/signal-tracker/consensus")
def signal_tracker_consensus():
    """Get symbols with multiple model agreement."""
    from engine.signal_tracker import get_consensus_signals
    return get_consensus_signals()


@app.get("/api/signal-tracker/leaderboard")
def signal_tracker_leaderboard():
    """Best Signals leaderboard — model hit rates."""
    from engine.signal_tracker import get_model_leaderboard
    return get_model_leaderboard()


@app.get("/api/signal-tracker/second-chance")
def signal_tracker_second_chance():
    """Second Chance — stocks sold but now have fresh buy signals."""
    from engine.signal_tracker import get_reentry_opportunities
    return get_reentry_opportunities()


@app.get("/api/signal-tracker/reentry-leaderboard")
def signal_tracker_reentry_leaderboard():
    """Re-entry success rate per model."""
    from engine.signal_tracker import get_reentry_leaderboard
    return get_reentry_leaderboard()


# --- Pair Trades ---

@app.get("/api/pair-trades")
def pair_trades(limit: int = 20):
    """Get detected pair trade opportunities."""
    from engine.pair_trades import get_pair_trades
    return get_pair_trades(limit)


@app.get("/api/pair-trades/pnl")
def pair_pnl():
    """Get combined P&L for active pair trades."""
    from engine.pair_trades import get_pair_pnl
    from engine.market_data import get_stock_price
    from config import WATCH_STOCKS

    prices = {}
    for sym in WATCH_STOCKS:
        data = get_stock_price(sym)
        if "error" not in data:
            prices[sym] = data

    return get_pair_pnl(prices)


# --- Volatility Surface ---

@app.get("/api/market/vol-surface/{symbol}")
def vol_surface(symbol: str):
    """Get IV surface for a symbol."""
    from engine.vol_surface import scan_vol_surface
    result = scan_vol_surface(symbol.upper())
    if not result:
        return {"error": f"No vol surface data for {symbol.upper()}"}
    return result


@app.get("/api/market/vol-surfaces")
def all_vol_surfaces():
    """Get IV surfaces for DayBlade tickers."""
    from engine.vol_surface import get_all_vol_surfaces
    return get_all_vol_surfaces()


# --- Kill Switch ---

@app.post("/api/kill-switch")
def kill_switch():
    """EMERGENCY: Close ALL positions across ALL models."""
    from engine.kill_switch import kill_all_positions
    from engine.market_data import get_stock_price
    from config import WATCH_STOCKS

    prices = {}
    for sym in WATCH_STOCKS:
        data = get_stock_price(sym)
        if "error" not in data:
            prices[sym] = data

    return kill_all_positions(prices)


@app.get("/api/kill-switch/history")
def kill_switch_history():
    """Get kill switch activation history."""
    from engine.kill_switch import get_kill_switch_history
    return get_kill_switch_history()


# --- Model DNA ---

@app.get("/api/model-dna/{player_id}")
def model_dna(player_id: str):
    """Get behavioral fingerprint for an AI model."""
    from engine.model_dna import get_model_dna
    return get_model_dna(player_id)


@app.get("/api/model-dna")
def all_model_dna():
    """Get DNA for all models."""
    from engine.model_dna import get_all_model_dna
    return get_all_model_dna()


# --- P&L Attribution ---

@app.get("/api/pnl-attribution")
def pnl_attribution(days: int = 7):
    """Break down P&L by model, sector, trade type, entry time."""
    from engine.pnl_attribution import get_pnl_attribution
    return get_pnl_attribution(days)


# --- Gamma Environment ---

@app.get("/api/gamma-environment")
def gamma_environment():
    """Get current gamma environment (positive/negative) for SPY."""
    from engine.gamma_environment import detect_gamma_environment
    return detect_gamma_environment()


# --- Put/Call Skew ---

@app.get("/api/put-call-skew")
def put_call_skew():
    """Get put/call skew for SPY, QQQ, and watchlist stocks."""
    from engine.put_call_skew import get_all_skew
    return get_all_skew(["SPY", "QQQ"])


@app.get("/api/put-call-skew/{symbol}")
def put_call_skew_symbol(symbol: str):
    """Get put/call skew for a specific symbol."""
    from engine.put_call_skew import compute_put_call_skew
    result = compute_put_call_skew(symbol.upper())
    if not result:
        return {"error": f"No skew data for {symbol.upper()}"}
    return result


# --- High IV Scanner ---

@app.get("/api/high-iv")
def high_iv():
    """Get high IV opportunities across watchlist."""
    from engine.high_iv_scanner import scan_high_iv_opportunities
    from config import WATCH_STOCKS
    return scan_high_iv_opportunities(WATCH_STOCKS)


# --- Cross-Asset Monitor ---

@app.get("/api/cross-asset")
def cross_asset():
    """Get cross-asset monitor (SPY, VIX, DXY, Oil) with correlation signals."""
    from engine.cross_asset import get_cross_asset_monitor
    return get_cross_asset_monitor()


# --- Auto Trendlines (Support/Resistance) ---

@app.get("/api/trendlines/{symbol}")
def trendlines(symbol: str):
    """Get auto-detected support and resistance levels."""
    from engine.trendlines import detect_support_resistance
    result = detect_support_resistance(symbol.upper())
    if not result:
        return {"error": f"No trendline data for {symbol.upper()}"}
    return result


@app.get("/api/trendlines")
def all_trendlines():
    """Get S/R levels for all watchlist stocks."""
    from engine.trendlines import get_all_levels
    from config import WATCH_STOCKS
    return get_all_levels(WATCH_STOCKS)


# --- Fibonacci Levels ---

@app.get("/api/fibonacci/{symbol}")
def fibonacci(symbol: str):
    """Get Fibonacci retracement levels."""
    from engine.fibonacci import compute_fibonacci
    result = compute_fibonacci(symbol.upper())
    if not result:
        return {"error": f"No Fibonacci data for {symbol.upper()}"}
    return result


# --- Dynamic Alerts ---

@app.get("/api/dynamic-alerts")
def dynamic_alerts(limit: int = 50):
    """Get recent dynamic alerts."""
    from engine.dynamic_alerts import get_recent_alerts
    return get_recent_alerts(limit)


@app.get("/api/dynamic-alerts/active")
def active_alerts(minutes: int = 30):
    """Get active alerts (last N minutes) for banner display."""
    from engine.dynamic_alerts import get_active_alerts
    return get_active_alerts(minutes)


# --- S/R Heatmap (Volume Profile) ---

@app.get("/api/volume-profile/{symbol}")
def volume_profile(symbol: str):
    """Get volume-weighted price profile for S/R heatmap."""
    from engine.sr_heatmap import compute_volume_profile
    result = compute_volume_profile(symbol.upper())
    if not result:
        return {"error": f"No volume profile for {symbol.upper()}"}
    return result


# --- Chart Patterns ---

@app.get("/api/patterns/{symbol}")
def chart_patterns_symbol(symbol: str):
    """Get detected chart patterns for a symbol."""
    from engine.chart_patterns import detect_patterns
    return detect_patterns(symbol.upper())


@app.get("/api/patterns")
def chart_patterns_all():
    """Get detected chart patterns for all watchlist stocks."""
    from engine.chart_patterns import detect_all_patterns
    from config import WATCH_STOCKS
    return detect_all_patterns(WATCH_STOCKS)


# --- Raindrop Charts ---

@app.get("/api/raindrop/{symbol}")
def raindrop(symbol: str):
    """Get raindrop volume profile for intraday chart."""
    from engine.raindrop import compute_raindrop
    result = compute_raindrop(symbol.upper())
    if not result:
        return {"error": f"No raindrop data for {symbol.upper()}"}
    return result


# --- Relative Strength Scanner ---

@app.get("/api/strength")
def strength_index():
    """Get relative strength rankings for all watchlist stocks."""
    from engine.strength_scanner import scan_relative_strength, get_strength_rankings
    from config import WATCH_STOCKS
    rankings = get_strength_rankings()
    if not rankings:
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
        try:
            with ThreadPoolExecutor(max_workers=1) as ex:
                rankings = ex.submit(scan_relative_strength, WATCH_STOCKS).result(timeout=20)
        except (FuturesTimeout, Exception):
            rankings = []
    return rankings


# --- Smart Risk Levels ---

@app.get("/api/risk-levels")
def risk_levels():
    """Get smart risk levels (entry/stop/targets) for all open positions."""
    from engine.smart_levels import get_risk_levels
    from engine.market_data import get_stock_price
    from config import WATCH_STOCKS

    prices = {}
    for sym in WATCH_STOCKS:
        data = get_stock_price(sym)
        if "error" not in data:
            prices[sym] = data
    return get_risk_levels(prices)


@app.get("/api/risk-levels/{symbol}")
def risk_levels_symbol(symbol: str):
    """Get risk levels for a specific symbol."""
    from engine.smart_levels import get_levels_for_symbol
    from engine.market_data import get_stock_price

    prices = {}
    data = get_stock_price(symbol.upper())
    if "error" not in data:
        prices[symbol.upper()] = data
    return get_levels_for_symbol(symbol.upper(), prices)


# --- Strategy Race ---

@app.get("/api/strategy-race")
def strategy_race():
    """Compare AI strategy vs SPY buy-and-hold."""
    from engine.strategy_race import get_strategy_race
    return get_strategy_race()


# --- Weekly Picks ---

@app.get("/api/weekly-picks")
def weekly_picks():
    """Get the most recent weekly AI picks."""
    from engine.weekly_picks import get_weekly_picks
    return get_weekly_picks()


# --- Stock Race (heatmap animation data) ---

@app.get("/api/stock-race")
def stock_race():
    """Get real-time stock race data for animated bar chart."""
    from engine.market_data import get_stock_price
    from config import WATCH_STOCKS

    result = []
    for sym in WATCH_STOCKS:
        data = get_stock_price(sym)
        if "error" not in data:
            result.append({
                "symbol": sym,
                "price": data["price"],
                "change_pct": data["change_pct"],
                "volume": data.get("volume", 0),
            })
    result.sort(key=lambda x: x["change_pct"], reverse=True)
    return result


@app.get("/api/trend-forecast")
def trend_forecast():
    """Get trend predictions for all watchlist stocks."""
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
    from engine.trend_predictor import predict_all_trends
    from config import WATCH_STOCKS
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            return ex.submit(predict_all_trends, WATCH_STOCKS).result(timeout=20)
    except (FuturesTimeout, Exception):
        return []


@app.get("/api/trend-forecast/{symbol}")
def trend_forecast_symbol(symbol: str):
    """Get trend prediction for a specific symbol."""
    from engine.trend_predictor import predict_trend
    result = predict_trend(symbol.upper())
    return result or {"error": f"No prediction for {symbol}"}


@app.get("/api/pattern-alerts")
def pattern_alerts():
    """Get enriched pattern alert tiles with breakout/target/stop/win-rate."""
    from engine.pattern_alerts import get_pattern_alert_tiles
    return get_pattern_alert_tiles()


@app.get("/api/strategy-presets")
def strategy_presets():
    """Get strategy preset evaluations for all watchlist stocks."""
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
    from engine.strategy_presets import scan_strategies
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            return ex.submit(scan_strategies).result(timeout=20)
    except (FuturesTimeout, Exception):
        return []


@app.get("/api/strategy-presets/{symbol}")
def strategy_presets_symbol(symbol: str):
    """Get best strategy for a specific symbol."""
    from engine.strategy_presets import get_best_strategy
    result = get_best_strategy(symbol.upper())
    return result or {"error": f"No strategy fit for {symbol}"}


@app.get("/api/deals")
def active_deals():
    """Get active deals (grouped positions) with live P&L."""
    from engine.deal_tracker import get_deals_with_pnl
    from engine.market_data import get_stock_price
    prices = {}
    try:
        from config import WATCH_STOCKS
        for sym in WATCH_STOCKS:
            data = get_stock_price(sym)
            if "error" not in data:
                prices[sym] = data
    except Exception:
        pass
    return get_deals_with_pnl(prices)


@app.get("/api/deals/closed")
def closed_deals():
    """Get recently closed deals."""
    from engine.deal_tracker import get_closed_deals
    return get_closed_deals()


@app.get("/api/fundamentals")
def fundamentals():
    """Get enriched fundamentals for all watchlist stocks."""
    from engine.stock_fundamentals import fetch_all_fundamentals
    return fetch_all_fundamentals()


@app.get("/api/fundamentals/{symbol}")
def fundamentals_symbol(symbol: str):
    """Get enriched fundamentals for a specific symbol."""
    from engine.stock_fundamentals import fetch_fundamentals
    result = fetch_fundamentals(symbol.upper())
    return result or {"error": f"No fundamental data for {symbol}"}


@app.get("/api/fundamentals/score/{symbol}")
def fundamentals_score(symbol: str):
    """Get Smart Score (letter grade) for a specific symbol."""
    from engine.stock_fundamentals import fetch_fundamentals
    result = fetch_fundamentals(symbol.upper())
    if not result:
        return {"error": f"No fundamental data for {symbol}"}
    return {
        "symbol": symbol.upper(),
        "smart_score": result.get("smart_score"),
        "grade": result.get("grade"),
        "components": result.get("score_components"),
    }


@app.get("/api/fundamentals/scores")
def fundamentals_scores():
    """Get Smart Scores for all watchlist stocks."""
    from engine.stock_fundamentals import fetch_all_fundamentals
    results = fetch_all_fundamentals()
    return [{
        "symbol": r["symbol"],
        "company_name": r.get("company_name"),
        "smart_score": r.get("smart_score"),
        "grade": r.get("grade"),
        "sector": r.get("sector"),
    } for r in results]


@app.get("/api/portfolio-health/{player_id}")
def portfolio_health(player_id: str):
    """Get portfolio health check for an AI player."""
    from engine.stock_fundamentals import portfolio_health_check
    result = portfolio_health_check(player_id)
    return result or {"error": f"No data for {player_id}"}


@app.get("/api/insider/{symbol}")
def insider_activity(symbol: str):
    """Get SEC insider trading activity for a symbol."""
    from engine.openbb_data import get_insider_summary
    return get_insider_summary(symbol.upper())


@app.get("/api/insider")
def insider_all():
    """Get insider trading summaries for all watchlist stocks."""
    from engine.openbb_data import get_insider_summary
    from config import WATCH_STOCKS
    results = []
    for sym in WATCH_STOCKS:
        summary = get_insider_summary(sym)
        if summary:
            results.append(summary)
    return results


@app.get("/api/filings/{symbol}")
def sec_filings(symbol: str):
    """Get recent SEC filings for a symbol."""
    from engine.openbb_data import get_sec_filings
    return get_sec_filings(symbol.upper())


@app.get("/api/economic-calendar")
def economic_calendar():
    """Get macro economic data: CPI, unemployment, interest rates, GDP, FOMC."""
    from engine.openbb_data import get_economic_calendar
    return get_economic_calendar()


@app.get("/api/options-chain/{symbol}")
def options_chain(symbol: str, expiry: str = None):
    """Get full options chain with Greeks for a symbol."""
    from engine.openbb_data import get_options_chain
    result = get_options_chain(symbol.upper(), expiry)
    return result or {"error": f"No options data for {symbol}"}


# --- Paper-Trader Compatibility Endpoints ---
# These adapt the paper-trader's JSON-based API to the autonomous-trader's SQLite DB.

@app.get("/api/capital")
def get_capital():
    """Get current capital per AI player (cash + positions value)."""
    from engine.paper_trader import get_portfolio_with_pnl
    from engine.market_data import get_all_prices
    from config import WATCH_STOCKS

    conn = _conn()
    players = conn.execute("SELECT id, display_name, cash FROM ai_players WHERE is_active=1").fetchall()
    conn.close()

    try:
        prices = get_all_prices(WATCH_STOCKS)
    except Exception:
        prices = {}

    result = {}
    for p in players:
        pid = p["id"]
        starting = 3500.0 if pid == "dayblade-0dte" else (7021.81 if pid == "steve-webull" else 7000.0)
        try:
            pnl_data = get_portfolio_with_pnl(pid, prices)
            total = pnl_data["total_value"]
        except Exception:
            total = p["cash"]
        result[p["display_name"]] = {
            "cash": p["cash"],
            "total_value": round(total, 2),
            "starting": starting,
            "pnl": round(total - starting, 2),
        }
    return result


@app.get("/api/trades")
def get_all_trades(status: str = None, symbol: str = None, model: str = None, season: int = None):
    """Get trades with paired BUY/SELL and P&L for closed trades."""
    conn = _conn()
    if season is None:
        s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        season = int(s_row["value"]) if s_row else 2
    if season == -1:
        rows = conn.execute(
            "SELECT t.id, t.player_id, t.symbol, t.action, t.qty, t.price, t.reasoning, t.confidence, "
            "t.executed_at, t.asset_type, t.option_type, t.entry_price, t.exit_price, t.realized_pnl, "
            "p.display_name FROM trades t LEFT JOIN ai_players p ON t.player_id = p.id "
            "ORDER BY t.executed_at DESC LIMIT 500"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT t.id, t.player_id, t.symbol, t.action, t.qty, t.price, t.reasoning, t.confidence, "
            "t.executed_at, t.asset_type, t.option_type, t.entry_price, t.exit_price, t.realized_pnl, "
            "p.display_name FROM trades t LEFT JOIN ai_players p ON t.player_id = p.id "
            "WHERE t.season=? ORDER BY t.executed_at DESC LIMIT 500",
            (season,)
        ).fetchall()
    conn.close()
    trades = []
    for r in rows:
        action = r["action"]
        is_buy = action in ("BUY", "BUY_CALL", "BUY_PUT")
        is_sell = action == "SELL"

        if is_sell:
            # Closed trade — show entry/exit/P&L
            entry_p = r["entry_price"] or r["price"]  # entry_price column, fallback to price
            exit_p = r["exit_price"] or r["price"]  # exit_price column, fallback to price
            pnl = r["realized_pnl"]
            t = {
                "id": str(r["id"]),
                "symbol": r["symbol"],
                "side": "short" if r["option_type"] == "put" else "long",
                "entry_price": round(entry_p, 2) if entry_p else None,
                "exit_price": round(exit_p, 2) if exit_p else None,
                "quantity": r["qty"],
                "entry_date": r["executed_at"],
                "exit_date": r["executed_at"],
                "status": "closed",
                "model_source": r["display_name"] or r["player_id"],
                "signal_reasoning": r["reasoning"],
                "pnl": round(pnl, 2) if pnl is not None else None,
                "pnl_pct": round((exit_p - entry_p) / entry_p * 100, 2) if entry_p and entry_p > 0 else None,
            }
        else:
            # Open trade (BUY) — include unrealized P&L
            side = "long" if action in ("BUY", "BUY_CALL") else "short"
            entry_p = r["price"]
            unrealized_pnl = None
            unrealized_pnl_pct = None
            # Get current price for unrealized P&L
            try:
                from engine.market_data import get_stock_price
                cur_data = get_stock_price(r["symbol"])
                if "error" not in cur_data:
                    cur = cur_data["price"]
                    is_opt = (r["asset_type"] == "option" or action in ("BUY_CALL", "BUY_PUT"))
                    if is_opt:
                        from engine.paper_trader import estimate_option_price
                        ot = (r["option_type"] if r["option_type"] else None) or ("call" if action == "BUY_CALL" else "put")
                        strike = r["strike_price"] if r["strike_price"] else None
                        est = estimate_option_price(ot, strike, cur, entry_p)
                        unrealized_pnl = round((est - entry_p) * r["qty"], 2)
                        unrealized_pnl_pct = round((est - entry_p) / entry_p * 100, 2) if entry_p > 0 else 0
                    else:
                        unrealized_pnl = round((cur - entry_p) * r["qty"], 2)
                        unrealized_pnl_pct = round((cur - entry_p) / entry_p * 100, 2) if entry_p > 0 else 0
            except Exception:
                pass
            t = {
                "id": str(r["id"]),
                "symbol": r["symbol"],
                "side": side,
                "entry_price": round(entry_p, 2),
                "exit_price": None,
                "quantity": r["qty"],
                "entry_date": r["executed_at"],
                "exit_date": None,
                "status": "open",
                "model_source": r["display_name"] or r["player_id"],
                "signal_reasoning": r["reasoning"],
                "pnl": unrealized_pnl,
                "pnl_pct": unrealized_pnl_pct,
            }

        if status and t["status"] != status:
            continue
        if symbol and t["symbol"].upper() != symbol.upper():
            continue
        if model and t["model_source"] != model:
            continue
        trades.append(t)
    return trades


@app.get("/api/performance")
def get_performance(model: str = None, season: int = None):
    """Get overall performance statistics, filtered by season."""
    conn = _conn()
    # Default to current season
    if season is None:
        s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        season = int(s_row["value"]) if s_row else 2
    if season == -1:
        sells = conn.execute(
            "SELECT player_id, symbol, qty, price, reasoning, realized_pnl FROM trades WHERE action='SELL'"
        ).fetchall()
    else:
        sells = conn.execute(
            "SELECT player_id, symbol, qty, price, reasoning, realized_pnl FROM trades WHERE action='SELL' AND season=?",
            (season,)
        ).fetchall()
    conn.close()
    pnls = []
    for s in sells:
        if s["realized_pnl"] is not None:
            pnls.append(float(s["realized_pnl"]))
        else:
            import re
            m = re.search(r'PnL: \$([+-]?[\d.]+)', s["reasoning"] or "")
            if m:
                pnls.append(float(m.group(1)))
    winners = [p for p in pnls if p > 0]
    losers = [p for p in pnls if p < 0]
    return {
        "season": season,
        "total_trades": len(sells),
        "open_trades": 0,
        "closed_trades": len(sells),
        "winners": len(winners),
        "losers": len(losers),
        "win_rate": round(len(winners) / len(sells) * 100, 2) if sells else 0,
        "total_pnl": round(sum(pnls), 2),
        "avg_win": round(sum(winners) / len(winners), 2) if winners else 0,
        "avg_loss": round(sum(losers) / len(losers), 2) if losers else 0,
        "profit_factor": round(sum(winners) / abs(sum(losers)), 2) if losers else 0,
        "largest_win": round(max(winners), 2) if winners else 0,
        "largest_loss": round(min(losers), 2) if losers else 0,
        "avg_hold_time_hours": None
    }


@app.get("/api/unrealized")
def get_unrealized():
    """Get unrealized P&L for all open positions."""
    from engine.market_data import get_all_prices
    conn = _conn()
    positions = conn.execute(
        "SELECT player_id, symbol, qty, avg_price, asset_type, option_type, strike_price FROM positions"
    ).fetchall()

    # Sanity check: no positions = $0 unrealized
    if not positions:
        conn.close()
        return {"total_unrealized": 0.0, "positions": []}

    # Fetch prices for all symbols (needed for both stocks and option intrinsic value)
    all_symbols = list(set(p["symbol"] for p in positions))
    all_data = get_all_prices(all_symbols) if all_symbols else {}
    price_cache = {sym: d["price"] for sym, d in all_data.items()}
    conn.close()

    from engine.paper_trader import estimate_option_price

    results = []
    total = 0
    for pos in positions:
        entry = pos["avg_price"]
        if pos["asset_type"] == "option":
            stock_price = price_cache.get(pos["symbol"], 0)
            est_price = estimate_option_price(
                pos["option_type"], pos["strike_price"], stock_price, entry)
            pnl = round((est_price - entry) * pos["qty"], 2)
            pnl_pct = round((est_price - entry) / entry * 100, 2) if entry > 0 else 0
            total += pnl
            results.append({
                "symbol": pos["symbol"], "current_price": round(est_price, 2),
                "entry_price": entry, "qty": pos["qty"], "pnl": pnl, "pnl_pct": pnl_pct,
                "model": pos["player_id"], "type": pos["asset_type"],
                "option_type": pos["option_type"], "strike_price": pos["strike_price"]
            })
            continue
        price = price_cache.get(pos["symbol"])
        if price is None:
            continue
        pnl = round((price - entry) * pos["qty"], 2)
        pnl_pct = round((price - entry) / entry * 100, 2) if entry > 0 else 0
        total += pnl
        results.append({
            "symbol": pos["symbol"], "current_price": round(price, 2),
            "entry_price": entry, "qty": pos["qty"], "pnl": pnl, "pnl_pct": pnl_pct,
            "model": pos["player_id"], "type": pos["asset_type"]
        })
    return {"total_unrealized": round(total, 2), "positions": results}


@app.get("/api/performance/by-model")
def get_performance_by_model(season: int = None):
    """Get performance broken down by AI player, filtered by season."""
    conn = _conn()
    if season is None:
        s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        season = int(s_row["value"]) if s_row else 2
    players = conn.execute("SELECT id, display_name, cash FROM ai_players WHERE is_active=1").fetchall()
    result = {}
    for p in players:
        if season == -1:
            trades = conn.execute("SELECT action, reasoning FROM trades WHERE player_id=?", (p["id"],)).fetchall()
        else:
            trades = conn.execute("SELECT action, reasoning FROM trades WHERE player_id=? AND season=?", (p["id"], season)).fetchall()
        sells = [t for t in trades if t["action"] == "SELL"]
        import re
        pnls = []
        for s in sells:
            m = re.search(r'PnL: \$([+-]?[\d.]+)', s["reasoning"] or "")
            if m:
                pnls.append(float(m.group(1)))
        winners = len([x for x in pnls if x > 0])
        result[p["display_name"]] = {
            "total_trades": len(trades),
            "closed_trades": len(sells),
            "open_trades": len(trades) - len(sells),
            "win_rate": round(winners / len(sells) * 100, 2) if sells else 0,
            "total_pnl": round(sum(pnls), 2),
            "avg_pnl": round(sum(pnls) / len(pnls), 2) if pnls else 0
        }
    conn.close()
    return result


@app.get("/api/equity-curve")
def get_equity_curve(starting_capital: float = 10000, season: int = None):
    """Get equity curve from trade history, filtered by season."""
    conn = _conn()
    if season is None:
        s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        season = int(s_row["value"]) if s_row else 2
    if season == -1:
        sells = conn.execute(
            "SELECT executed_at, reasoning FROM trades WHERE action='SELL' ORDER BY executed_at"
        ).fetchall()
    else:
        sells = conn.execute(
            "SELECT executed_at, reasoning FROM trades WHERE action='SELL' AND season=? ORDER BY executed_at",
            (season,)
        ).fetchall()
    conn.close()
    import re
    curve = [{"date": "start", "equity": starting_capital, "trade": None}]
    equity = starting_capital
    for s in sells:
        m = re.search(r'PnL: \$([+-]?[\d.]+)', s["reasoning"] or "")
        if m:
            pnl = float(m.group(1))
            equity += pnl
            curve.append({
                "date": (s["executed_at"] or "")[:10],
                "equity": round(equity, 2),
                "pnl": round(pnl, 2)
            })
    return curve


@app.get("/api/models")
def get_models():
    """Get all AI players as models."""
    conn = _conn()
    players = conn.execute("SELECT id, display_name, provider, model_id, is_active FROM ai_players").fetchall()
    conn.close()
    return [
        {"name": p["display_name"], "description": f"{p['provider']} / {p['model_id']}",
         "type": p["provider"], "active": bool(p["is_active"]), "created_date": ""}
        for p in players
    ]


@app.post("/api/ai-chat")
def ai_chat(msg: dict):
    """AI chat endpoint for multi-model debate."""
    import requests as req
    message = msg.get("message", "")
    models = msg.get("models", ["gemma3:4b"])
    responses = []
    context = "You are an AI trading model in a debate. Be concise (2-3 sentences max). Topic: "

    for model_name in models:
        prompt = context + message
        response = None
        try:
            if "gemma" in model_name or "llama" in model_name:
                r = req.post("http://localhost:11434/api/generate",
                    json={"model": model_name, "prompt": prompt, "stream": False}, timeout=30)
                response = r.json().get("response", "").strip()[:300]
            elif "claude" in model_name:
                api_key = os.environ.get("ANTHROPIC_API_KEY", "")
                if api_key:
                    r = req.post("https://api.anthropic.com/v1/messages",
                        headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                        json={"model": "claude-haiku-4-5-20251001", "max_tokens": 150, "messages": [{"role": "user", "content": prompt}]},
                        timeout=20)
                    blocks = r.json().get("content", [])
                    response = blocks[0].get("text", "No response").strip()[:300] if blocks else "No response"
            elif "grok" in model_name:
                xai_key = os.environ.get("XAI_API_KEY", "")
                if xai_key:
                    r = req.post("https://api.x.ai/v1/chat/completions",
                        headers={"Authorization": "Bearer " + xai_key, "Content-Type": "application/json"},
                        json={"model": "grok-4-1-fast-reasoning", "max_tokens": 150, "messages": [{"role": "user", "content": prompt}]},
                        timeout=20)
                    choices = r.json().get("choices", [])
                    response = choices[0].get("message", {}).get("content", "No response").strip()[:300] if choices else "No response"
            elif "gemini" in model_name:
                gemini_key = os.environ.get("GEMINI_API_KEY", "")
                if gemini_key:
                    r = req.post(f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={gemini_key}",
                        headers={"Content-Type": "application/json"},
                        json={"contents": [{"parts": [{"text": prompt}]}]},
                        timeout=20)
                    cands = r.json().get("candidates", [])
                    response = cands[0]["content"]["parts"][0]["text"].strip()[:300] if cands else "No response"
        except Exception as e:
            response = f"Error: {str(e)[:80]}"
        if response:
            from datetime import datetime
            responses.append({"model": model_name, "response": response, "timestamp": datetime.now().isoformat()})
    return {"responses": responses}


_rec_cache = {}  # key: "player_id:symbol" -> {data, ts}
_REC_CACHE_TTL = 300  # 5 minutes


@app.get("/api/arena/player/{player_id}/recommendation/{symbol}")
def player_recommendation(player_id: str, symbol: str):
    """Get AI recommendation for a position — calls the owning model."""
    import time as _time
    import requests as req
    import re

    cache_key = f"{player_id}:{symbol}"
    if cache_key in _rec_cache and (_time.time() - _rec_cache[cache_key]["ts"]) < _REC_CACHE_TTL:
        return _rec_cache[cache_key]["data"]

    conn = _conn()
    player = conn.execute("SELECT provider, model_id, display_name FROM ai_players WHERE id=?",
                          (player_id,)).fetchone()
    if not player:
        conn.close()
        return {"rating": "HOLD", "grade": "C", "confidence": 0.5, "reasoning": "Player not found"}

    pos = conn.execute("SELECT qty, avg_price FROM positions WHERE player_id=? AND symbol=? AND asset_type='stock'",
                       (player_id, symbol)).fetchone()
    last_trade = conn.execute(
        "SELECT confidence, reasoning FROM trades WHERE player_id=? AND symbol=? ORDER BY executed_at DESC LIMIT 1",
        (player_id, symbol)).fetchone()
    conn.close()

    qty = pos["qty"] if pos else 0
    entry = pos["avg_price"] if pos else 0

    from engine.market_data import get_stock_price
    price_data = get_stock_price(symbol)
    current = price_data.get("price", 0)
    pnl_pct = round((current - entry) / entry * 100, 2) if entry > 0 else 0
    last_conf = last_trade["confidence"] if last_trade else 0.5

    # Get technicals + news
    rsi_val = "--"
    macd_val = "--"
    try:
        from engine.market_data import get_technical_indicators
        ti = get_technical_indicators(symbol)
        if ti:
            rsi_val = ti.get("rsi", "--")
            macd_val = ti.get("macd_histogram", "--")
    except Exception:
        pass

    headlines = ""
    try:
        from engine.news_fetcher import fetch_news
        news = fetch_news(symbol, limit=3)
        headlines = "; ".join([n.get("headline", "") for n in (news or []) if n.get("headline")])[:300]
    except Exception:
        headlines = "No recent news"

    prompt = (
        f"You are an AI trading advisor. You hold {qty:.2f} shares of {symbol} at ${entry:.2f}. "
        f"Current price is ${current:.2f} ({pnl_pct:+.1f}%). RSI is {rsi_val}. MACD histogram is {macd_val}. "
        f"Recent news: {headlines or 'None'}. "
        f"Rate this position with exactly one of: STRONG_BUY, BUY, HOLD, SELL, STRONG_SELL. "
        f"Also give a letter grade from A+ to F. "
        f"Reply in this exact format: RATING: <rating> GRADE: <grade> REASON: <1 sentence>"
    )

    response_text = ""
    provider = player["provider"]
    model_id = player["model_id"]

    try:
        if provider == "ollama":
            r = req.post("http://localhost:11434/api/generate",
                json={"model": model_id, "prompt": prompt, "stream": False}, timeout=30)
            response_text = r.json().get("response", "")
        elif provider == "anthropic":
            api_key = os.environ.get("ANTHROPIC_API_KEY", "")
            if api_key:
                r = req.post("https://api.anthropic.com/v1/messages",
                    headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                    json={"model": model_id, "max_tokens": 150, "messages": [{"role": "user", "content": prompt}]},
                    timeout=20)
                blocks = r.json().get("content", [])
                response_text = blocks[0].get("text", "") if blocks else ""
        elif provider == "openai":
            oai_key = os.environ.get("OPENAI_API_KEY", "")
            if oai_key:
                r = req.post("https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {oai_key}", "Content-Type": "application/json"},
                    json={"model": model_id, "max_tokens": 150, "messages": [{"role": "user", "content": prompt}]},
                    timeout=20)
                choices = r.json().get("choices", [])
                response_text = choices[0].get("message", {}).get("content", "") if choices else ""
        elif provider == "xai":
            xai_key = os.environ.get("XAI_API_KEY", "")
            if xai_key:
                r = req.post("https://api.x.ai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {xai_key}", "Content-Type": "application/json"},
                    json={"model": model_id, "max_tokens": 150, "messages": [{"role": "user", "content": prompt}]},
                    timeout=20)
                choices = r.json().get("choices", [])
                response_text = choices[0].get("message", {}).get("content", "") if choices else ""
        elif provider == "google":
            gemini_key = os.environ.get("GEMINI_API_KEY", "")
            if gemini_key:
                r = req.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/{model_id}:generateContent?key={gemini_key}",
                    headers={"Content-Type": "application/json"},
                    json={"contents": [{"parts": [{"text": prompt}]}]},
                    timeout=20)
                cands = r.json().get("candidates", [])
                response_text = cands[0]["content"]["parts"][0]["text"] if cands else ""
    except Exception as e:
        response_text = ""

    # Parse response
    rating = "HOLD"
    grade = "C"
    reason = ""

    if response_text:
        rt = response_text.upper()
        for r_val in ["STRONG_BUY", "STRONG_SELL", "BUY", "SELL", "HOLD"]:
            if r_val in rt:
                rating = r_val
                break
        grade_match = re.search(r'GRADE:\s*([A-F][+-]?)', response_text, re.IGNORECASE)
        if grade_match:
            grade = grade_match.group(1).upper()
        reason_match = re.search(r'REASON:\s*(.+)', response_text, re.IGNORECASE)
        if reason_match:
            reason = reason_match.group(1).strip()[:200]

    # Fallback: compute from signals if AI didn't respond
    if not response_text:
        if pnl_pct > 10:
            rating = "STRONG_BUY"
            grade = "A"
        elif pnl_pct > 3:
            rating = "BUY"
            grade = "B+"
        elif pnl_pct > -3:
            rating = "HOLD"
            grade = "C+"
        elif pnl_pct > -10:
            rating = "SELL"
            grade = "D"
        else:
            rating = "STRONG_SELL"
            grade = "F"
        reason = f"Heuristic: position at {pnl_pct:+.1f}%"

    # Confidence from composite
    conf_map = {"STRONG_BUY": 0.9, "BUY": 0.7, "HOLD": 0.5, "SELL": 0.3, "STRONG_SELL": 0.1}
    confidence = conf_map.get(rating, 0.5)

    result = {
        "rating": rating,
        "grade": grade,
        "confidence": confidence,
        "reasoning": reason,
        "model": player["display_name"],
        "cached": False,
    }
    _rec_cache[cache_key] = {"data": {**result, "cached": True}, "ts": _time.time()}
    return result


@app.post("/api/arena/player/{player_id}/buy")
def player_buy(player_id: str, body: dict):
    """Add to position (DCA) — buys at current market price."""
    from engine.paper_trader import buy
    from engine.market_data import get_stock_price
    symbol = body.get("symbol", "")
    qty = body.get("qty", 0)
    if not symbol or qty <= 0:
        return {"error": "symbol and qty > 0 required"}
    price_data = get_stock_price(symbol)
    price = price_data.get("price", 0)
    if not price:
        return {"error": f"Could not fetch price for {symbol}"}
    result = buy(player_id, symbol, price, qty=qty, reasoning="Manual DCA via dashboard")
    if not result:
        return {"error": "Buy failed — check cash balance"}
    return result


@app.post("/api/arena/player/{player_id}/trim")
def player_trim(player_id: str, body: dict):
    """Trim position — sells a fraction at current market price."""
    from engine.paper_trader import sell_partial
    from engine.market_data import get_stock_price
    symbol = body.get("symbol", "")
    fraction = body.get("fraction", 0.5)
    if not symbol:
        return {"error": "symbol required"}
    price_data = get_stock_price(symbol)
    price = price_data.get("price", 0)
    if not price:
        return {"error": f"Could not fetch price for {symbol}"}
    from engine.paper_trader import get_position
    pos = get_position(player_id, symbol)
    if not pos:
        return {"error": f"No position in {symbol}"}
    trim_qty = round(pos["qty"] * fraction, 4)
    if trim_qty <= 0:
        return {"error": "Nothing to trim"}
    result = sell_partial(player_id, symbol, price, trim_qty, reasoning=f"Manual trim {fraction*100:.0f}% via dashboard")
    if not result:
        return {"error": "Trim failed"}
    return result


@app.post("/api/arena/player/{player_id}/close")
def player_close(player_id: str, body: dict):
    """Close entire position at current market price."""
    from engine.paper_trader import sell
    from engine.market_data import get_stock_price
    symbol = body.get("symbol", "")
    if not symbol:
        return {"error": "symbol required"}
    price_data = get_stock_price(symbol)
    price = price_data.get("price", 0)
    if not price:
        return {"error": f"Could not fetch price for {symbol}"}
    result = sell(player_id, symbol, price, reasoning="Manual close via dashboard")
    if not result:
        return {"error": "Close failed — no position found"}
    return result


# --- Model Control Endpoints ---

# Cost estimates per scan
MODEL_COST_MAP = {
    "ollama-local": 0.0,
    "ollama-gemma27b": 0.0,
    "ollama-deepseek": 0.0,
    "ollama-qwen3": 0.0,
    "ollama-llama": 0.0,
    "ollama-glm4": 0.0,
    "ollama-kimi": 0.0,
    "ollama-plutus": 0.0,
    "claude-sonnet": 0.01,
    "claude-haiku": 0.002,
    "gpt-4o": 0.008,
    "gpt-o3": 0.015,
    "gemini-2.5-pro": 0.005,
    "gemini-2.5-flash": 0.001,
    "grok-3": 0.005,
    "grok-4": 0.005,
    "dayblade-0dte": 0.0,
    "steve-webull": 0.0,
    "cto-grok42": 0.005,
}


@app.get("/api/model-control")
def model_control():
    """Get model control panel data: pause state, costs, call counts."""
    conn = _conn()
    players = conn.execute("""
        SELECT id, display_name, provider, model_id, is_active, is_halted,
               COALESCE(is_paused, 0) as is_paused
        FROM ai_players ORDER BY provider, id
    """).fetchall()

    today = __import__("datetime").datetime.now().strftime("%Y-%m-%d")
    stats = conn.execute(
        "SELECT player_id, api_calls, total_cost FROM model_stats WHERE date=?",
        (today,)
    ).fetchall()
    stats_map = {r["player_id"]: {"api_calls": r["api_calls"], "total_cost": r["total_cost"]} for r in stats}

    pause_all = conn.execute("SELECT value FROM settings WHERE key='pause_all'").fetchone()
    conn.close()

    models = []
    grand_total = 0.0
    for p in players:
        pid = p["id"]
        st = stats_map.get(pid, {"api_calls": 0, "total_cost": 0.0})
        is_free = p["provider"] == "ollama" or pid in ("dayblade-0dte", "steve-webull")
        cost_per_scan = MODEL_COST_MAP.get(pid, 0.0 if is_free else 0.005)
        # Force $0 for all free/Ollama models regardless of what model_stats says
        display_cost = 0.0 if is_free else st["total_cost"]
        grand_total += display_cost
        models.append({
            "player_id": pid,
            "display_name": p["display_name"],
            "provider": p["provider"],
            "model_id": p["model_id"],
            "is_paused": bool(p["is_paused"]),
            "is_halted": bool(p["is_halted"]),
            "cost_per_scan": cost_per_scan,
            "api_calls_today": st["api_calls"],
            "total_cost_today": display_cost,
        })

    return {
        "pause_all": bool(pause_all and pause_all["value"] == "1"),
        "models": models,
        "grand_total_cost": grand_total,
    }


@app.post("/api/model-control/pause-all")
def toggle_pause_all():
    """Toggle global pause for all scanning."""
    conn = _conn()
    current = conn.execute("SELECT value FROM settings WHERE key='pause_all'").fetchone()
    new_val = "0" if (current and current["value"] == "1") else "1"
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('pause_all', ?)",
        (new_val,)
    )
    conn.commit()
    conn.close()
    return {"pause_all": new_val == "1"}


@app.post("/api/model-control/pause/{player_id}")
def toggle_pause_player(player_id: str):
    """Toggle pause for a specific AI model."""
    conn = _conn()
    current = conn.execute(
        "SELECT COALESCE(is_paused, 0) as is_paused FROM ai_players WHERE id=?",
        (player_id,)
    ).fetchone()
    if not current:
        conn.close()
        return {"error": "Player not found"}
    new_val = 0 if current["is_paused"] else 1
    conn.execute("UPDATE ai_players SET is_paused=? WHERE id=?", (new_val, player_id))
    conn.commit()
    conn.close()
    return {"player_id": player_id, "is_paused": bool(new_val)}


@app.post("/api/model-control/record-call/{player_id}")
def record_api_call(player_id: str):
    """Record an API call for cost tracking."""
    cost = MODEL_COST_MAP.get(player_id, 0.0 if player_id.startswith("ollama-") else 0.005)
    today = __import__("datetime").datetime.now().strftime("%Y-%m-%d")
    conn = _conn()
    conn.execute("""
        INSERT INTO model_stats (player_id, api_calls, total_cost, date)
        VALUES (?, 1, ?, ?)
        ON CONFLICT(player_id, date) DO UPDATE SET
            api_calls = api_calls + 1,
            total_cost = total_cost + ?
    """, (player_id, cost, today, cost))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.post("/api/admin/clean-stale-snapshots")
def clean_stale_snapshots():
    conn = _conn()
    season_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
    season = int(season_row[0]) if season_row else 3
    # Find models with $10k cash but portfolio_history showing losses (stale from pre-reset)
    stale = conn.execute("""
        SELECT DISTINCT ph.player_id FROM portfolio_history ph
        JOIN ai_players ap ON ap.id = ph.player_id
        WHERE ph.season=? AND ap.cash >= 9999
        AND ph.total_value < 9000
    """, (season,)).fetchall()
    deleted = {}
    for row in stale:
        pid = row["player_id"]
        cnt = conn.execute("SELECT count(*) FROM portfolio_history WHERE player_id=? AND season=?", (pid, season)).fetchone()[0]
        conn.execute("DELETE FROM portfolio_history WHERE player_id=? AND season=?", (pid, season))
        deleted[pid] = cnt
    conn.commit()
    conn.close()
    return {"ok": True, "deleted": deleted}


@app.post("/api/model-control/force-scan")
def force_scan():
    """Trigger a manual scan immediately, bypassing market hours check."""
    import threading
    import main as _main

    # Use main.py's scan lock so force scan and scheduled scan don't overlap
    if not _main._scan_lock.acquire(blocking=False):
        return {"ok": False, "message": "Scan already in progress"}

    def _do_scan():
        try:
            from config import WATCH_STOCKS

            arena = _main.arena
            if arena is None:
                arena = _main.initialize_arena()
                _main.arena = arena
            arena.run_scan(WATCH_STOCKS, force=True)
        except Exception as e:
            print(f"Force scan error: {e}")
        finally:
            _main._scan_lock.release()

    threading.Thread(target=_do_scan, daemon=True).start()
    return {"ok": True, "message": "Manual scan started"}


# --- Cost Dashboard Endpoints ---

@app.get("/api/costs/dashboard")
def cost_dashboard():
    """Full cost dashboard data: daily, cumulative, projections, grades."""
    from engine.cost_tracker import (
        get_daily_costs, get_cumulative_costs, get_cost_per_trade,
        get_projected_monthly_cost, get_token_efficiency,
        get_model_roi_ranking, get_model_efficiency_grades,
        get_free_vs_paid_pnl, get_dead_models, get_model_diversity,
        get_total_daily_cost, TOKEN_RATES,
    )
    today = __import__("datetime").datetime.now().strftime("%Y-%m-%d")
    daily = get_daily_costs(today)
    cumulative = get_cumulative_costs()
    cost_per_trade = get_cost_per_trade()
    projection = get_projected_monthly_cost()
    efficiency = get_token_efficiency()
    roi = get_model_roi_ranking()
    grades = get_model_efficiency_grades()
    free_vs_paid = get_free_vs_paid_pnl()
    dead = get_dead_models(48)
    diversity = get_model_diversity()
    daily_total = get_total_daily_cost(today)

    return {
        "daily_total": round(daily_total, 4),
        "daily_costs": daily,
        "cumulative_costs": cumulative,
        "cost_per_trade": cost_per_trade,
        "projection": projection,
        "token_efficiency": efficiency,
        "roi_ranking": roi,
        "efficiency_grades": grades,
        "free_vs_paid": free_vs_paid,
        "dead_models": dead,
        "diversity": diversity,
        "token_rates": {k: {"input": v[0], "output": v[1]} for k, v in TOKEN_RATES.items()},
    }


@app.get("/api/costs/daily-total")
def cost_daily_total():
    """Quick endpoint for nav bar daily cost display."""
    from engine.cost_tracker import get_total_daily_cost
    return {"daily_total": round(get_total_daily_cost(), 4)}


@app.get("/api/costs/history")
def cost_history(days: int = 30):
    """Daily cost totals for the last N days."""
    conn = _conn()
    rows = conn.execute("""
        SELECT date(timestamp) as day, SUM(cost_usd) as total_cost, COUNT(*) as num_calls
        FROM api_costs
        WHERE timestamp >= datetime('now', ? || ' days')
        GROUP BY date(timestamp)
        ORDER BY day ASC
    """, (f"-{days}",)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# Serve paper-trader static dashboard
_static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=_static_dir), name="static")


@app.get("/")
def serve_index():
    return FileResponse(os.path.join(_static_dir, "index.html"))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)

@app.get("/api/webull-portfolio")
def webull_portfolio():
    """Returns Steve's real Webull portfolio with live P&L"""
    from engine.paper_trader import get_portfolio_with_pnl
    from engine.market_data import get_stock_price

    conn = _conn()
    player = conn.execute("SELECT * FROM ai_players WHERE id='steve-webull'").fetchone()
    conn.close()

    if not player:
        return {"cash": 0, "positions": [], "recent_trades": [], "position_count": 0}

    # Fetch live prices for all of Steve's symbols
    prices = {}
    pos_conn = _conn()
    steve_positions = pos_conn.execute(
        "SELECT symbol FROM positions WHERE player_id='steve-webull'"
    ).fetchall()
    pos_conn.close()

    for row in steve_positions:
        try:
            prices[row["symbol"]] = get_stock_price(row["symbol"])
        except Exception:
            pass

    pnl = get_portfolio_with_pnl("steve-webull", prices)

    # Calculate total daily P&L % (weighted by market value)
    total_mkt = sum(p.get("market_value", 0) for p in pnl["positions"])
    total_day_pnl_pct = 0.0
    if total_mkt > 0:
        for p in pnl["positions"]:
            weight = p.get("market_value", 0) / total_mkt
            total_day_pnl_pct += weight * p.get("day_change_pct", 0)
    total_day_pnl_pct = round(total_day_pnl_pct, 2)

    return {
        "cash": pnl["cash"],
        "total_value": pnl["total_value"],
        "total_cost_basis": pnl["total_cost_basis"],
        "total_unrealized_pnl": pnl["total_unrealized_pnl"],
        "return_pct": pnl["return_pct"],
        "total_day_pnl_pct": total_day_pnl_pct,
        "starting_value": 7021.81,
        "positions": [
            {
                "symbol": p["symbol"], "qty": p["qty"], "avg_price": p["avg_price"],
                "current_price": p.get("current_price", p["avg_price"]),
                "market_value": p.get("market_value", p["qty"] * p["avg_price"]),
                "unrealized_pnl": p.get("unrealized_pnl", 0),
                "unrealized_pnl_pct": p.get("unrealized_pnl_pct", 0),
                "day_change_pct": p.get("day_change_pct", 0),
                "market": "webull",
            }
            for p in pnl["positions"]
        ],
        "recent_trades": [],
        "position_count": len(pnl["positions"]),
    }


@app.get("/api/webull/live")
def webull_live():
    """Fetch live portfolio from Webull OpenAPI."""
    from engine.webull_client import get_portfolio
    return get_portfolio()


@app.get("/api/price/{symbol}")
def get_price(symbol: str):
    """Get live price for a symbol via Yahoo Finance, with DB fallback."""
    from engine.market_data import get_stock_price
    data = get_stock_price(symbol.upper())
    if "price" in data:
        return {
            "symbol": symbol.upper(),
            "price": data["price"],
            "change": data.get("change_pct", 0),
            "change_pct": data.get("change_pct", 0),
            "prev_close": round(data["price"] / (1 + data.get("change_pct", 0) / 100), 2) if data.get("change_pct") else data["price"]
        }
    # Fallback: last trade price from DB
    conn = _conn()
    row = conn.execute(
        "SELECT price FROM trades WHERE symbol=? ORDER BY executed_at DESC LIMIT 1", (symbol.upper(),)
    ).fetchone()
    conn.close()
    if row:
        return {"symbol": symbol.upper(), "price": row["price"], "change": 0, "change_pct": 0, "prev_close": row["price"], "cached": True}
    return {"symbol": symbol.upper(), "price": 0, "change": 0, "change_pct": 0, "error": data.get("error", "No data")}


# --- Backtest Lab Endpoints ---

_backtest_status = {}  # run_id -> {progress, message, status, results}
_backtest_lock = threading.Lock()

@app.get("/api/backtest/models")
def backtest_available_models():
    """Return list of models available for backtesting."""
    from config import AI_PLAYERS
    return [{"id": p["id"], "name": p["name"], "provider": p["provider"]} for p in AI_PLAYERS]


@app.post("/api/backtest/run")
def backtest_run(payload: dict):
    """Start a backtest. Body: {date, model_ids, end_date?}"""
    from engine.historical_backtest import (
        run_single_day_backtest, run_multi_day_backtest,
        save_backtest_run, ensure_backtest_tables,
    )
    ensure_backtest_tables()

    date_str = payload.get("date")
    end_date = payload.get("end_date")
    model_ids = payload.get("model_ids", [])

    if not date_str or not model_ids:
        return {"error": "date and model_ids required"}

    # Generate run_id
    conn = _conn()
    run_type = "multi" if end_date and end_date != date_str else "single"
    cur = conn.execute(
        "INSERT INTO backtest_runs (run_type, start_date, end_date, model_ids, status) VALUES (?, ?, ?, ?, 'running')",
        (run_type, date_str, end_date or date_str, json.dumps(model_ids)),
    )
    run_id = cur.lastrowid
    conn.commit()
    conn.close()

    with _backtest_lock:
        _backtest_status[run_id] = {"progress": 0, "message": "Starting...", "status": "running", "results": None}

    def _run():
        def _progress(pct, msg):
            with _backtest_lock:
                _backtest_status[run_id]["progress"] = pct
                _backtest_status[run_id]["message"] = msg

        try:
            if end_date and end_date != date_str:
                results = run_multi_day_backtest(date_str, end_date, model_ids, _progress)
                save_backtest_run("multi", date_str, end_date, model_ids, results)
            else:
                raw = run_single_day_backtest(date_str, model_ids, _progress)
                results = {pid: r.to_dict() for pid, r in raw.items()}
                save_backtest_run("single", date_str, date_str, model_ids, results)

            with _backtest_lock:
                _backtest_status[run_id]["status"] = "complete"
                _backtest_status[run_id]["progress"] = 100
                _backtest_status[run_id]["message"] = "Complete"
                _backtest_status[run_id]["results"] = results

            # Update DB status
            c2 = _conn()
            c2.execute("UPDATE backtest_runs SET status='complete', completed_at=CURRENT_TIMESTAMP WHERE id=?", (run_id,))
            c2.commit()
            c2.close()
        except Exception as e:
            with _backtest_lock:
                _backtest_status[run_id]["status"] = "error"
                _backtest_status[run_id]["message"] = str(e)

            c2 = _conn()
            c2.execute("UPDATE backtest_runs SET status='error' WHERE id=?", (run_id,))
            c2.commit()
            c2.close()

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()

    return {"run_id": run_id, "status": "running"}


@app.get("/api/backtest/status/{run_id}")
def backtest_status(run_id: int):
    """Poll backtest progress."""
    with _backtest_lock:
        st = _backtest_status.get(run_id)
    if st:
        return st
    # Check DB for completed runs
    from engine.historical_backtest import get_backtest_run_results
    results = get_backtest_run_results(run_id)
    if results:
        return {"status": "complete", "progress": 100, "message": "Complete", "results": results}
    return {"status": "not_found", "progress": 0, "message": "Run not found"}


@app.get("/api/backtest/runs")
def backtest_runs(limit: int = 20):
    """Get recent backtest runs."""
    from engine.historical_backtest import get_backtest_runs
    return get_backtest_runs(limit)


@app.get("/api/backtest/run/{run_id}")
def backtest_run_detail(run_id: int):
    """Get detailed results for a specific run."""
    from engine.historical_backtest import get_backtest_run_results
    return get_backtest_run_results(run_id)


@app.get("/api/backtest/rankings")
def backtest_rankings():
    """Get model rankings aggregated across all backtest runs."""
    from engine.historical_backtest import get_model_rankings
    return get_model_rankings()


# ─── Strategy Lab ─────────────────────────────────────────────────────────────

_strategy_lab_status = {}
_strategy_lab_lock = threading.Lock()

@app.get("/api/strategy-lab/strategies")
def strategy_lab_strategies():
    """Return available strategies and their parameters."""
    from engine.strategy_lab import STRATEGIES
    return {k: {"name": v["name"], "description": v["description"],
                "params": v["params"], "optimize_grid": v.get("optimize_grid", {})}
            for k, v in STRATEGIES.items()}


@app.post("/api/strategy-lab/run")
def strategy_lab_run(payload: dict):
    """Run a single strategy backtest. Body: {strategy, symbol, start_date, end_date, params?}"""
    from engine.strategy_lab import run_strategy_backtest
    strategy = payload.get("strategy")
    symbol = payload.get("symbol", "AAPL").upper()
    start_date = payload.get("start_date")
    end_date = payload.get("end_date")
    params = payload.get("params", {})

    if not strategy or not start_date or not end_date:
        return {"error": "strategy, start_date, and end_date are required"}

    try:
        return run_strategy_backtest(strategy, params, symbol, start_date, end_date)
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/strategy-lab/optimize")
def strategy_lab_optimize(payload: dict):
    """Start optimization. Body: {strategy, symbol, start_date, end_date, grid?}
    Returns {run_id, status: "running"}. Poll /api/strategy-lab/status/{run_id}.
    """
    strategy = payload.get("strategy")
    symbol = payload.get("symbol", "AAPL").upper()
    start_date = payload.get("start_date")
    end_date = payload.get("end_date")
    custom_grid = payload.get("grid")

    if not strategy or not start_date or not end_date:
        return {"error": "strategy, start_date, and end_date are required"}

    import time as _time
    run_id = int(_time.time() * 1000) % 1_000_000_000

    with _strategy_lab_lock:
        _strategy_lab_status[run_id] = {
            "progress": 0, "message": "Starting...",
            "status": "running", "results": None,
        }

    def _run():
        from engine.strategy_lab import optimize_strategy

        def _progress(pct, msg):
            with _strategy_lab_lock:
                _strategy_lab_status[run_id]["progress"] = pct
                _strategy_lab_status[run_id]["message"] = msg

        try:
            result = optimize_strategy(strategy, symbol, start_date, end_date,
                                       custom_grid, progress_cb=_progress)
            with _strategy_lab_lock:
                _strategy_lab_status[run_id]["status"] = "complete"
                _strategy_lab_status[run_id]["progress"] = 100
                _strategy_lab_status[run_id]["message"] = "Complete"
                _strategy_lab_status[run_id]["results"] = result
        except Exception as e:
            with _strategy_lab_lock:
                _strategy_lab_status[run_id]["status"] = "error"
                _strategy_lab_status[run_id]["message"] = str(e)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return {"run_id": run_id, "status": "running"}


@app.get("/api/strategy-lab/status/{run_id}")
def strategy_lab_status(run_id: int):
    """Poll optimization progress."""
    with _strategy_lab_lock:
        st = _strategy_lab_status.get(run_id)
    if st:
        return st
    return {"status": "not_found"}


@app.post("/api/strategy-lab/deploy")
def strategy_lab_deploy(payload: dict):
    """Deploy winning params to trading_rules.txt. Body: {strategy, params, stats}"""
    from engine.strategy_lab import deploy_winning_params
    strategy = payload.get("strategy")
    params = payload.get("params", {})
    stats = payload.get("stats", {})
    if not strategy:
        return {"error": "strategy is required"}
    return deploy_winning_params(strategy, params, stats)


@app.get("/api/strategy-lab/latest")
def strategy_lab_latest():
    """Return the most recent auto-optimization report."""
    from engine.strategy_lab import get_latest_report
    report = get_latest_report()
    if report:
        return report
    return {"message": "No optimization reports yet. Run one manually or wait for Sunday auto-run."}


@app.get("/api/strategy-lab/history")
def strategy_lab_history(limit: int = 20):
    """Return summaries of recent optimization reports."""
    from engine.strategy_lab import get_report_history
    return get_report_history(limit)


@app.post("/api/strategy-lab/auto-optimize")
def strategy_lab_auto_optimize():
    """Manually trigger the full auto-optimization pipeline."""
    import time as _time
    run_id = int(_time.time() * 1000) % 1_000_000_000

    with _strategy_lab_lock:
        _strategy_lab_status[run_id] = {
            "progress": 0, "message": "Starting full auto-optimization...",
            "status": "running", "results": None,
        }

    def _run():
        from engine.strategy_lab import auto_optimize_all

        def _progress(pct, msg):
            with _strategy_lab_lock:
                _strategy_lab_status[run_id]["progress"] = pct
                _strategy_lab_status[run_id]["message"] = msg

        try:
            report = auto_optimize_all(progress_cb=_progress)
            with _strategy_lab_lock:
                _strategy_lab_status[run_id]["status"] = "complete"
                _strategy_lab_status[run_id]["progress"] = 100
                _strategy_lab_status[run_id]["message"] = "Complete"
                _strategy_lab_status[run_id]["results"] = report
        except Exception as e:
            with _strategy_lab_lock:
                _strategy_lab_status[run_id]["status"] = "error"
                _strategy_lab_status[run_id]["message"] = str(e)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return {"run_id": run_id, "status": "running"}


# ─── Realtime Monitor ─────────────────────────────────────────────────────────

@app.get("/api/realtime/alerts")
def realtime_alerts(limit: int = 20):
    """Get recent realtime spike alerts."""
    from engine.realtime_monitor import get_recent_alerts
    return get_recent_alerts(limit)


@app.get("/api/realtime/status")
def realtime_status():
    """Get realtime monitor connection status."""
    from engine.realtime_monitor import get_monitor_status
    return get_monitor_status()


@app.get("/api/news-sentiment/{symbol}")
def get_news_sentiment(symbol: str):
    """Get AI-powered sentiment analysis for a symbol's news"""
    try:
        import feedparser, requests as req
        url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
        feed = feedparser.parse(url)
        headlines = [e.get("title", "") for e in feed.entries[:5]]
        if not headlines:
            return {"symbol": symbol.upper(), "sentiment": "neutral", "score": 5, "headlines": []}
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            return {"symbol": symbol.upper(), "sentiment": "neutral", "score": 5, "headlines": headlines, "error": "No API key"}
        prompt = "Rate market sentiment for " + symbol + " based on these headlines. Respond ONLY as JSON: {\"sentiment\": \"bullish\" or \"bearish\" or \"neutral\", \"score\": 1-10, \"summary\": \"one sentence\"}\nHeadlines:\n" + "\n".join(headlines)
        res = req.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 200, "messages": [{"role": "user", "content": prompt}]},
            timeout=15)
        import re as re2
        text = res.json()["content"][0]["text"]
        m = re2.search(r'{[^{}]*}', text, re2.DOTALL)
        result = json.loads(m.group()) if m else {"sentiment": "neutral", "score": 5, "summary": "No data"}
        result["headlines"] = headlines
        result["symbol"] = symbol.upper()
        return result
    except Exception as e:
        return {"symbol": symbol.upper(), "sentiment": "neutral", "score": 5, "headlines": [], "error": str(e)}


# --- Chart Analyzer ---

@app.post("/api/chart-analyze")
def chart_analyze(payload: dict):
    """AI-powered chart technical analysis"""
    from engine.chart_analyzer import analyze_chart
    symbol = payload.get("symbol", "SPY")
    model = payload.get("model", "claude")
    return analyze_chart(symbol, model)


@app.get("/api/chart-analyses")
def chart_analyses(symbol: str = None):
    """Get saved chart analyses"""
    from engine.chart_analyzer import load_analyses
    analyses = load_analyses()
    if symbol:
        analyses = [a for a in analyses if a.get("symbol", "").upper() == symbol.upper()]
    return analyses


@app.get("/api/chart-analyses/{symbol}/compare")
def chart_analyses_compare(symbol: str):
    """Compare analyses across models for a symbol"""
    from engine.chart_analyzer import get_comparison
    return get_comparison(symbol)


# --- Pre-Market Gap Scanner ---

@app.get("/api/premarket-gaps")
def premarket_gaps():
    """Scan watchlist for pre-market price gaps > 2%"""
    from engine.premarket_scanner import scan_premarket_gaps
    return {"gaps": scan_premarket_gaps()}


@app.post("/api/premarket-analyze")
def premarket_analyze():
    """AI analysis of pre-market gaps across all models"""
    from engine.premarket_scanner import analyze_gaps_with_ai
    return {"responses": analyze_gaps_with_ai()}


@app.get("/api/dayblade/gap-candidates")
def dayblade_gap_candidates():
    """Pre-market gap candidates for DayBlade 0DTE plays"""
    from engine.premarket_scanner import get_dayblade_gap_candidates
    return {"candidates": get_dayblade_gap_candidates()}


# --- Stock Screener ---

@app.get("/api/screener")
def stock_screener(
    min_pe: float = None, max_pe: float = None,
    min_short_float: float = None, max_short_float: float = None,
    min_rel_volume: float = None, consensus: str = None,
    has_insider_buying: bool = None, earnings_within_days: int = None
):
    """Screen watchlist stocks by fundamental filters"""
    from engine.stock_screener import screen_stocks
    return {"results": screen_stocks(
        min_pe=min_pe, max_pe=max_pe,
        min_short_float=min_short_float, max_short_float=max_short_float,
        min_rel_volume=min_rel_volume, consensus=consensus,
        has_insider_buying=has_insider_buying, earnings_within_days=earnings_within_days
    )}


# --- Insider Trading ---

@app.get("/api/insider-trades/{symbol}")
def insider_trades(symbol: str):
    """Get insider trading data for a symbol"""
    import math
    try:
        from engine.insider_tracker import get_insider_trades
        trades = get_insider_trades(symbol)
        # Sanitize NaN/Inf floats that break JSON serialization
        for t in trades:
            for k, v in t.items():
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    t[k] = 0.0
        return {"trades": trades}
    except Exception:
        return {"trades": []}


@app.get("/api/insider-alerts")
def insider_alerts():
    """Scan watchlist for recent insider buying alerts"""
    from engine.insider_tracker import scan_insider_alerts
    return {"alerts": scan_insider_alerts()}


# --- S&P 500 Sector Heat Map ---

@app.get("/api/sectors/heatmap")
def sectors_heatmap():
    """S&P 500 sector ETF heat map with holdings"""
    from engine.premarket_scanner import get_sector_heatmap
    return {"sectors": get_sector_heatmap()}


# --- S&P 500 Treemap (top 50 by market cap) ---

_sp500_treemap_cache = {"data": None, "ts": 0}

@app.get("/api/market/sp500-treemap")
def sp500_treemap():
    """Top 50 S&P 500 stocks by market cap, grouped by sector, for treemap display."""
    import time as _time
    if _sp500_treemap_cache["data"] and _time.time() - _sp500_treemap_cache["ts"] < 55:
        return _sp500_treemap_cache["data"]

    # Top 50 S&P 500 by market cap with sectors
    SP500_TOP50 = [
        ("AAPL", "Technology"), ("MSFT", "Technology"), ("NVDA", "Technology"),
        ("AMZN", "Consumer Cyclical"), ("GOOGL", "Communication Services"),
        ("META", "Communication Services"), ("BRK-B", "Financial"),
        ("LLY", "Healthcare"), ("AVGO", "Technology"), ("JPM", "Financial"),
        ("TSLA", "Consumer Cyclical"), ("UNH", "Healthcare"), ("XOM", "Energy"),
        ("V", "Financial"), ("MA", "Financial"), ("COST", "Consumer Defensive"),
        ("JNJ", "Healthcare"), ("HD", "Consumer Cyclical"), ("PG", "Consumer Defensive"),
        ("ABBV", "Healthcare"), ("WMT", "Consumer Defensive"), ("NFLX", "Communication Services"),
        ("CRM", "Technology"), ("BAC", "Financial"), ("KO", "Consumer Defensive"),
        ("MRK", "Healthcare"), ("CVX", "Energy"), ("ORCL", "Technology"),
        ("AMD", "Technology"), ("PEP", "Consumer Defensive"), ("TMO", "Healthcare"),
        ("ACN", "Technology"), ("LIN", "Basic Materials"), ("ADBE", "Technology"),
        ("MCD", "Consumer Cyclical"), ("CSCO", "Technology"), ("ABT", "Healthcare"),
        ("PM", "Consumer Defensive"), ("WFC", "Financial"), ("NOW", "Technology"),
        ("IBM", "Technology"), ("GE", "Industrials"), ("ISRG", "Healthcare"),
        ("CAT", "Industrials"), ("INTU", "Technology"), ("VZ", "Communication Services"),
        ("TXN", "Technology"), ("QCOM", "Technology"), ("AMGN", "Healthcare"),
        ("SPGI", "Financial"),
    ]

    try:
        import yfinance as yf
        symbols = [s[0] for s in SP500_TOP50]
        tickers = yf.Tickers(" ".join(symbols))

        sectors = {}
        for sym, sector in SP500_TOP50:
            try:
                t = tickers.tickers.get(sym) or tickers.tickers.get(sym.replace("-", ""))
                if not t:
                    continue
                info = t.fast_info
                price = float(info.last_price) if hasattr(info, "last_price") else 0
                prev = float(info.previous_close) if hasattr(info, "previous_close") else price
                mcap = float(info.market_cap) if hasattr(info, "market_cap") else 0
                change_pct = ((price - prev) / prev * 100) if prev > 0 else 0

                if sector not in sectors:
                    sectors[sector] = {"sector": sector, "stocks": [], "total_mcap": 0}
                sectors[sector]["stocks"].append({
                    "symbol": sym,
                    "price": round(price, 2),
                    "change_pct": round(change_pct, 2),
                    "market_cap": mcap,
                })
                sectors[sector]["total_mcap"] += mcap
            except Exception:
                continue

        # Sort sectors by total market cap, stocks within each sector by market cap
        result = sorted(sectors.values(), key=lambda s: s["total_mcap"], reverse=True)
        for sec in result:
            sec["stocks"].sort(key=lambda s: s["market_cap"], reverse=True)

        _sp500_treemap_cache["data"] = result
        _sp500_treemap_cache["ts"] = _time.time()
        return result
    except Exception as e:
        return [{"sector": "Error", "stocks": [], "total_mcap": 0, "error": str(e)}]


# --- Finnhub Intelligence ---

@app.get("/api/finnhub/insider/{symbol}")
def finnhub_insider(symbol: str):
    """Get Finnhub insider transactions for a symbol."""
    from engine.finnhub_data import get_insider_transactions
    return {"transactions": get_insider_transactions(symbol)}


@app.get("/api/finnhub/insider-sentiment/{symbol}")
def finnhub_insider_sentiment(symbol: str):
    """Get aggregated insider sentiment for a symbol."""
    from engine.finnhub_data import get_insider_sentiment
    return get_insider_sentiment(symbol)


@app.get("/api/finnhub/earnings")
def finnhub_earnings():
    """Get upcoming earnings for watchlist stocks."""
    from engine.finnhub_data import get_earnings_calendar
    return {"earnings": get_earnings_calendar()}


@app.get("/api/finnhub/news-sentiment/{symbol}")
def finnhub_news_sentiment(symbol: str):
    """Get Finnhub news sentiment score for a symbol."""
    from engine.finnhub_data import get_news_sentiment
    return get_news_sentiment(symbol)


@app.get("/api/finnhub/filings/{symbol}")
def finnhub_filings(symbol: str, form: str = None):
    """Get SEC filings for a symbol."""
    from engine.finnhub_data import get_sec_filings
    return {"filings": get_sec_filings(symbol, form)}


@app.get("/api/finnhub/context/{symbol}")
def finnhub_context(symbol: str):
    """Get full Finnhub intelligence context for a symbol (for AI prompts)."""
    from engine.finnhub_data import build_ai_context
    return {"symbol": symbol, "context": build_ai_context(symbol)}


# --- Alpha Vantage Intelligence ---

@app.get("/api/alphavantage/technicals/{symbol}")
def av_technicals(symbol: str):
    """Get RSI, MACD, SMA from Alpha Vantage as cross-check."""
    from engine.alphavantage_data import get_rsi, get_macd, get_sma
    return {
        "symbol": symbol,
        "rsi": get_rsi(symbol),
        "macd": get_macd(symbol),
        "sma20": get_sma(symbol, time_period=20),
    }


@app.get("/api/alphavantage/overview/{symbol}")
def av_overview(symbol: str):
    """Get company fundamentals from Alpha Vantage."""
    from engine.alphavantage_data import get_company_overview
    return get_company_overview(symbol) or {"error": "No data available"}


@app.get("/api/alphavantage/earnings/{symbol}")
def av_earnings(symbol: str):
    """Get earnings surprises (last 4 quarters)."""
    from engine.alphavantage_data import get_earnings_surprises
    return {"surprises": get_earnings_surprises(symbol)}


@app.get("/api/alphavantage/context/{symbol}")
def av_context(symbol: str):
    """Get full Alpha Vantage intelligence context for a symbol."""
    from engine.alphavantage_data import build_ai_context
    return {"symbol": symbol, "context": build_ai_context(symbol)}


# --- FRED Macro Data ---

@app.get("/api/macro")
def macro_data():
    """Get FRED macro economic indicators."""
    from engine.alphavantage_data import get_macro_data
    return get_macro_data()


@app.get("/api/macro/context")
def macro_context():
    """Get macro context string for AI prompts."""
    from engine.alphavantage_data import build_macro_context
    return {"context": build_macro_context()}


# --- Combined Intelligence ---

@app.get("/api/intelligence/{symbol}")
def combined_intelligence(symbol: str):
    """Get combined intelligence from all data sources for a symbol."""
    parts = []
    try:
        from engine.finnhub_data import build_ai_context as fh_ctx
        fh = fh_ctx(symbol)
        if fh:
            parts.append(fh)
    except Exception:
        pass
    try:
        from engine.alphavantage_data import build_ai_context as av_ctx
        av = av_ctx(symbol)
        if av:
            parts.append(av)
    except Exception:
        pass
    try:
        from engine.alphavantage_data import build_macro_context
        macro = build_macro_context()
        if macro:
            parts.append(macro)
    except Exception:
        pass
    return {"symbol": symbol, "context": " | ".join(parts), "parts": parts}


# --- Trade Ideas: Smart Risk Levels ---

@app.get("/api/risk-levels/{symbol}")
def risk_levels(symbol: str, entry_price: float = None, side: str = "BUY"):
    """Calculate smart risk levels for a symbol."""
    from engine.smart_risk import calculate_risk_levels
    from engine.market_data import get_stock_price
    if not entry_price:
        p = get_stock_price(symbol)
        entry_price = p.get("price", 0) if "error" not in p else 0
    if not entry_price:
        return {"error": "Could not determine price"}
    return calculate_risk_levels(symbol, entry_price, side)


@app.get("/api/signals/with-risk")
def signals_with_risk(limit: int = 20):
    """Get recent signals with auto-calculated risk levels."""
    from engine.smart_risk import get_recent_signals_with_risk
    return {"signals": get_recent_signals_with_risk(limit)}


# --- Trade Ideas: Channel Bar ---

@app.get("/api/channels")
def all_channels():
    """Get all channel scan results with timeout protection."""
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
    from engine.channel_scanner import scan_channel

    channels = ["gap-and-go", "momentum-breakout", "reversal-bounce", "short-squeeze",
                "earnings-runner", "volatility-breakout"]
    results = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(scan_channel, ch): ch for ch in channels}
        for f in futures:
            ch = futures[f]
            try:
                results[ch] = f.result(timeout=15)
            except (FuturesTimeout, Exception):
                results[ch] = []
    return results


@app.get("/api/channels/{channel}")
def channel_scan(channel: str):
    """Run a specific channel scan."""
    from engine.channel_scanner import scan_channel
    return {"channel": channel, "results": scan_channel(channel)}


# --- Volatility Breakout Scanner ---

@app.get("/api/volatility-breakout")
def volatility_breakout():
    """Get active volatility breakout signals."""
    from engine.volatility_breakout import scan_all_breakouts
    return {"breakouts": scan_all_breakouts()}


@app.get("/api/volatility-breakout/history")
def volatility_breakout_history(limit: int = 50):
    """Get historical breakout signals with outcomes."""
    from engine.volatility_breakout import get_recent_breakouts
    return {"breakouts": get_recent_breakouts(limit)}


@app.get("/api/volatility-breakout/stats")
def volatility_breakout_stats():
    """Get breakout success rate statistics."""
    from engine.volatility_breakout import get_breakout_stats
    return get_breakout_stats()


# --- Discovery Scanner ---

@app.get("/api/discoveries")
def discoveries():
    """Get current discovery opportunities (outside watchlist)."""
    from engine.discovery_scanner import get_cached_discoveries
    return {"discoveries": get_cached_discoveries()}


@app.get("/api/discoveries/scan")
def discovery_scan():
    """Trigger a fresh discovery scan."""
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
    from engine.discovery_scanner import run_discovery_scan
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            results = ex.submit(run_discovery_scan).result(timeout=45)
        return {"discoveries": results, "count": len(results)}
    except (FuturesTimeout, Exception) as e:
        return {"discoveries": [], "error": str(e)}


@app.get("/api/discoveries/history")
def discovery_history(limit: int = 50):
    """Get historical discoveries."""
    from engine.discovery_scanner import get_recent_discoveries
    return {"discoveries": get_recent_discoveries(limit)}


# --- Trade Ideas: OddsMaker ---

@app.get("/api/oddsmaker/{symbol}")
def oddsmaker(symbol: str, signal: str = "BUY"):
    """Get OddsMaker win probability for a signal."""
    from engine.oddsmaker import calculate_odds
    return calculate_odds(symbol, signal)


@app.get("/api/signals/with-odds")
def signals_with_odds(limit: int = 20):
    """Get recent signals with OddsMaker probability."""
    from engine.oddsmaker import get_signals_with_odds
    return {"signals": get_signals_with_odds(limit)}


# --- Trade Ideas: Money Machine ---

@app.get("/api/money-machine/status")
def money_machine_status():
    """Get Money Machine status and current momentum leaders."""
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
    from engine.money_machine import get_status
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            return ex.submit(get_status).result(timeout=20)
    except (FuturesTimeout, Exception) as e:
        return {"active": False, "momentum_leaders": [], "positions": [], "error": f"Timeout: {e}"}


# --- Perplexity Finance: SEC EDGAR ---

@app.get("/api/sec/filings/{symbol}")
def sec_filings(symbol: str):
    """Get SEC EDGAR filings for a symbol."""
    from engine.sec_edgar import get_recent_filings
    return {"filings": get_recent_filings(symbol)}


@app.get("/api/sec/context/{symbol}")
def sec_context(symbol: str):
    """Get SEC filing context for AI prompts."""
    from engine.sec_edgar import build_ai_context
    return {"symbol": symbol, "context": build_ai_context(symbol)}


# --- Perplexity Finance: Earnings Hub ---

@app.get("/api/earnings/countdown")
def earnings_countdown(days: int = 7):
    """Get earnings countdown cards for watchlist."""
    from engine.earnings_hub import get_earnings_countdown
    return {"earnings": get_earnings_countdown(days)}


@app.get("/api/earnings/context/{symbol}")
def earnings_context(symbol: str):
    """Get earnings context for AI prompts."""
    from engine.earnings_hub import build_ai_context
    return {"symbol": symbol, "context": build_ai_context(symbol)}


# --- Perplexity Finance: Bull/Bear Analysis ---

@app.post("/api/bull-bear/{symbol}")
def bull_bear_analysis(symbol: str, model: str = "claude"):
    """Get AI bull/bear case analysis."""
    from engine.bull_bear import analyze_bull_bear
    return analyze_bull_bear(symbol, model)


@app.get("/api/bull-bear/all")
def bull_bear_all(model: str = "claude"):
    """Get bull/bear analysis for all held positions."""
    from engine.bull_bear import analyze_all_positions
    return {"analyses": analyze_all_positions(model)}


# --- Perplexity Finance: Market Movers ---

@app.get("/api/market-movers")
def market_movers():
    """Get top gainers, losers, and most active."""
    from engine.market_movers import get_market_movers
    return get_market_movers()


# --- Combined Intelligence Feed (enhanced) ---

@app.get("/api/intelligence/full/{symbol}")
def full_intelligence(symbol: str):
    """Get ALL intelligence from ALL data sources for AI prompt enrichment."""
    parts = []
    sources = [
        ("finnhub", lambda: __import__("engine.finnhub_data", fromlist=["build_ai_context"]).build_ai_context(symbol)),
        ("alphavantage", lambda: __import__("engine.alphavantage_data", fromlist=["build_ai_context"]).build_ai_context(symbol)),
        ("sec_edgar", lambda: __import__("engine.sec_edgar", fromlist=["build_ai_context"]).build_ai_context(symbol)),
        ("earnings", lambda: __import__("engine.earnings_hub", fromlist=["build_ai_context"]).build_ai_context(symbol)),
        ("macro", lambda: __import__("engine.alphavantage_data", fromlist=["build_macro_context"]).build_macro_context()),
        ("movers", lambda: __import__("engine.market_movers", fromlist=["build_ai_context"]).build_ai_context()),
    ]
    for name, fn in sources:
        try:
            ctx = fn()
            if ctx:
                parts.append({"source": name, "context": ctx})
        except Exception:
            pass
    combined = " | ".join(p["context"] for p in parts)
    return {"symbol": symbol, "context": combined, "sources": parts}
