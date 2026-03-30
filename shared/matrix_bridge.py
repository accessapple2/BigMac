from __future__ import annotations

import os
import sqlite3
from datetime import datetime

DB_PATH = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))

NEO_PLAYER_ID = "neo-matrix"
NEO_DISPLAY_NAME = "Neo"
NEO_PROVIDER = "matrix"
NEO_MODEL_ID = "8000 / Independent"
NEO_STARTING_CASH = 7000.0
NEO_PORTFOLIO_NAME = "Neo Matrix"
NEO_PORTFOLIO_NOTES = (
    "Matrix / 8000 / Independent. Shared conversation and comparison surfaces only. "
    "Arena must treat this participant as read-only."
)
INDEPENDENT_PLAYER_IDS = {NEO_PLAYER_ID}
MATRIX_TRADE_SOURCE = "matrix,8000,independent"


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def is_independent_player(player_id: str | None) -> bool:
    return bool(player_id and player_id in INDEPENDENT_PLAYER_IDS)


def player_meta(player_id: str | None, provider: str | None = None, model_id: str | None = None) -> dict:
    if is_independent_player(player_id):
        return {
            "provider": NEO_PROVIDER,
            "model": NEO_MODEL_ID,
            "provider_display": "Matrix",
            "source_label": "Matrix / 8000 / Independent",
            "source_port": 8000,
            "approval_scope": "matrix",
            "is_independent": True,
            "arena_governed": False,
            "ui_badge": "Matrix / 8000 / Independent",
            "can_pause": False,
        }

    return {
        "provider": provider,
        "model": model_id,
        "provider_display": (provider or "").upper() if provider else "",
        "source_label": "Arena / 8080",
        "source_port": 8080,
        "approval_scope": "arena",
        "is_independent": False,
        "arena_governed": True,
        "ui_badge": "",
        "can_pause": True,
    }


def annotate_player_payload(payload: dict) -> dict:
    data = dict(payload)
    current_equity = (
        data.get("current_equity")
        or data.get("total_value")
        or data.get("account_value")
        or data.get("cash_plus_market_value")
    )

    if not current_equity and data.get("name") == "Mr. Anderson":
        current_equity = (data.get("cash", 0) or 0) + (data.get("market_value", 0) or 0)

    if not current_equity:
        current_equity = data.get("starting_capital", 0) or 0

    data["current_equity"] = current_equity
    meta = player_meta(
        data.get("player_id") or data.get("id"),
        provider=data.get("provider"),
        model_id=data.get("model") or data.get("model_id"),
    )
    data.update(meta)
    if "model_id" in data and not data.get("model"):
        data["model"] = data["model_id"]
    if "model" in data and not data.get("model_id"):
        data["model_id"] = data["model"]
    return data


def ensure_matrix_shared_records() -> dict:
    conn = _db()
    try:
        season_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        current_season = int(season_row["value"]) if season_row and season_row["value"] else 1

        player = conn.execute(
            "SELECT id, cash FROM ai_players WHERE id=?",
            (NEO_PLAYER_ID,),
        ).fetchone()
        if player:
            conn.execute(
                "UPDATE ai_players "
                "SET display_name=?, provider=?, model_id=?, is_active=1, season=?, is_human=0 "
                "WHERE id=?",
                (NEO_DISPLAY_NAME, NEO_PROVIDER, NEO_MODEL_ID, current_season, NEO_PLAYER_ID),
            )
            neo_cash = float(player["cash"] or NEO_STARTING_CASH)
        else:
            neo_cash = NEO_STARTING_CASH
            conn.execute(
                "INSERT INTO ai_players "
                "(id, display_name, provider, model_id, cash, is_active, is_halted, is_human, season) "
                "VALUES (?, ?, ?, ?, ?, 1, 0, 0, ?)",
                (NEO_PLAYER_ID, NEO_DISPLAY_NAME, NEO_PROVIDER, NEO_MODEL_ID, neo_cash, current_season),
            )

        portfolio = conn.execute(
            "SELECT id FROM portfolios WHERE name=?",
            (NEO_PORTFOLIO_NAME,),
        ).fetchone()
        if portfolio:
            portfolio_id = int(portfolio["id"])
            conn.execute(
                "UPDATE portfolios "
                "SET broker='matrix', account_type='independent', is_active=1, is_human=0, "
                "notes=?, updated_at=CURRENT_TIMESTAMP "
                "WHERE id=?",
                (NEO_PORTFOLIO_NOTES, portfolio_id),
            )
        else:
            conn.execute(
                "INSERT INTO portfolios "
                "(name, broker, account_type, initial_balance, current_balance, is_human, is_active, notes) "
                "VALUES (?, 'matrix', 'independent', ?, ?, 0, 1, ?)",
                (NEO_PORTFOLIO_NAME, neo_cash, neo_cash, NEO_PORTFOLIO_NOTES),
            )
            portfolio_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

        has_history = conn.execute(
            "SELECT 1 FROM portfolio_history WHERE player_id=? LIMIT 1",
            (NEO_PLAYER_ID,),
        ).fetchone()
        if not has_history:
            conn.execute(
                "INSERT INTO portfolio_history (player_id, total_value, cash, positions_value, season) "
                "VALUES (?, ?, ?, 0, ?)",
                (NEO_PLAYER_ID, neo_cash, neo_cash, current_season),
            )

        conn.commit()
        return {"player_id": NEO_PLAYER_ID, "portfolio_id": portfolio_id, "season": current_season}
    finally:
        conn.close()


def get_neo_portfolio_id() -> int:
    info = ensure_matrix_shared_records()
    return int(info["portfolio_id"])


def _sync_open_positions(conn: sqlite3.Connection, open_rows: list[dict]) -> int:
    """Replace Neo's current open-position mirror only.

    The legacy `positions` table is a current-state table, not the source of historical record.
    """
    conn.execute("DELETE FROM positions WHERE player_id=?", (NEO_PLAYER_ID,))
    inserted = 0
    for row in open_rows:
        conn.execute(
            "INSERT INTO positions "
            "(player_id, symbol, qty, avg_price, asset_type, option_type, strike_price, expiry_date, opened_at, high_watermark) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                NEO_PLAYER_ID,
                row["symbol"],
                row["qty"],
                row["avg_price"],
                row["asset_type"],
                row["option_type"],
                row["strike_price"],
                row["expiry_date"],
                row["opened_at"],
                row["high_watermark"],
            ),
        )
        inserted += 1
    return inserted


def _insert_missing_trade(conn: sqlite3.Connection, trade: dict, season: int) -> bool:
    if _trade_exists(conn, trade):
        return False
    conn.execute(
        "INSERT INTO trades "
        "(player_id, symbol, action, qty, price, asset_type, option_type, strike_price, expiry_date, "
        "reasoning, confidence, executed_at, exit_price, realized_pnl, entry_price, season, sources, timeframe) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            NEO_PLAYER_ID,
            trade["symbol"],
            trade["action"],
            trade["qty"],
            trade["price"],
            trade["asset_type"],
            trade["option_type"],
            trade["strike_price"],
            trade["expiry_date"],
            trade["reasoning"],
            trade["confidence"],
            trade["executed_at"],
            trade["exit_price"],
            trade["realized_pnl"],
            trade["entry_price"],
            season,
            trade["sources"],
            trade["timeframe"],
        ),
    )
    return True


def sync_neo_from_native_portfolio() -> dict:
    """Mirror Neo's native 8000 portfolio state into shared 8080 comparison tables.

    Native source of truth:
    - portfolios.id = Neo Matrix
    - portfolio_positions rows for that portfolio

    Shared mirrored targets:
    - ai_players.cash
    - positions
    - trades
    - portfolio_history
    """
    info = ensure_matrix_shared_records()
    portfolio_id = int(info["portfolio_id"])

    conn = _db()
    try:
        portfolio = conn.execute(
            "SELECT id, initial_balance, current_balance FROM portfolios WHERE id=?",
            (portfolio_id,),
        ).fetchone()
        if not portfolio:
            return {"ok": False, "error": "Neo portfolio not found", "portfolio_id": portfolio_id}

        native_positions = conn.execute(
            "SELECT * FROM portfolio_positions WHERE portfolio_id=? ORDER BY created_at ASC, id ASC",
            (portfolio_id,),
        ).fetchall()

        season_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        season = int(season_row["value"]) if season_row and season_row["value"] else info["season"]

        initial_balance = float(portfolio["initial_balance"] or NEO_STARTING_CASH)
        total_realized = 0.0
        open_cost_basis = 0.0
        positions_value = 0.0

        open_rows = []
        trade_rows = []

        def _position_qty(row: sqlite3.Row) -> float:
            if (row["asset_class"] or "") == "metals" and row["metal_oz"]:
                return float(row["metal_oz"] or 0)
            return float(row["quantity"] or 0)

        for row in native_positions:
            qty = _position_qty(row)
            entry_price = float(row["entry_price"] or 0)
            current_price = float(row["current_price"] or entry_price or 0)
            created_at = row["created_at"]
            asset_class = row["asset_class"] or "stock"

            trade_rows.append({
                "symbol": row["ticker"],
                "action": "BUY",
                "qty": qty,
                "price": entry_price,
                "asset_type": asset_class,
                "option_type": row["option_type"],
                "strike_price": row["strike_price"],
                "expiry_date": row["expiration_date"],
                "reasoning": row["notes"] or "Matrix / 8000 portfolio open",
                "confidence": 0.0,
                "executed_at": created_at,
                "entry_price": entry_price,
                "exit_price": None,
                "realized_pnl": None,
                "sources": MATRIX_TRADE_SOURCE,
                "timeframe": "SWING",
            })

            if row["status"] == "open":
                open_cost_basis += qty * entry_price
                positions_value += qty * current_price
                open_rows.append({
                    "symbol": row["ticker"],
                    "qty": qty,
                    "avg_price": entry_price,
                    "asset_type": asset_class,
                    "option_type": row["option_type"],
                    "strike_price": row["strike_price"],
                    "expiry_date": row["expiration_date"],
                    "opened_at": created_at,
                    "high_watermark": current_price,
                })
            elif row["status"] == "closed":
                closed_pnl = float(row["closed_pnl"] or 0)
                total_realized += closed_pnl
                trade_rows.append({
                    "symbol": row["ticker"],
                    "action": "SELL",
                    "qty": qty,
                    "price": float(row["current_price"] or row["entry_price"] or 0),
                    "asset_type": asset_class,
                    "option_type": row["option_type"],
                    "strike_price": row["strike_price"],
                    "expiry_date": row["expiration_date"],
                    "reasoning": row["notes"] or "Matrix / 8000 portfolio close",
                    "confidence": 0.0,
                    "executed_at": row["closed_at"] or row["updated_at"] or created_at,
                    "entry_price": entry_price,
                    "exit_price": float(row["current_price"] or row["entry_price"] or 0),
                    "realized_pnl": closed_pnl,
                    "sources": MATRIX_TRADE_SOURCE,
                    "timeframe": "SWING",
                })

        shared_cash = initial_balance + total_realized - open_cost_basis
        total_value = shared_cash + positions_value

        conn.execute(
            "UPDATE ai_players SET cash=?, season=?, is_active=1 WHERE id=?",
            (round(shared_cash, 2), season, NEO_PLAYER_ID),
        )
        conn.execute(
            "UPDATE portfolios SET current_balance=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (round(total_value, 2), portfolio_id),
        )

        mirrored_positions = _sync_open_positions(conn, open_rows)
        inserted_trades = 0
        for trade in trade_rows:
            if _insert_missing_trade(conn, trade, season):
                inserted_trades += 1

        conn.execute(
            "INSERT INTO portfolio_history (player_id, total_value, cash, positions_value, season) "
            "VALUES (?, ?, ?, ?, ?)",
            (NEO_PLAYER_ID, round(total_value, 2), round(shared_cash, 2), round(positions_value, 2), season),
        )

        conn.commit()
        return {
            "ok": True,
            "player_id": NEO_PLAYER_ID,
            "portfolio_id": portfolio_id,
            "native_positions": len(native_positions),
            "open_positions": mirrored_positions,
            "mirrored_trades": inserted_trades,
            "cash": round(shared_cash, 2),
            "positions_value": round(positions_value, 2),
            "total_value": round(total_value, 2),
            "synced_at": datetime.now().isoformat(),
        }
    finally:
        conn.close()


def _trade_exists(conn: sqlite3.Connection, trade: dict) -> bool:
    row = conn.execute(
        "SELECT 1 FROM trades "
        "WHERE player_id=? AND symbol=? AND action=? AND COALESCE(qty, 0)=? "
        "AND COALESCE(price, 0)=? AND COALESCE(entry_price, 0)=? AND COALESCE(exit_price, 0)=? "
        "AND COALESCE(realized_pnl, 0)=? AND COALESCE(reasoning, '')=? "
        "AND COALESCE(executed_at, '')=? "
        "LIMIT 1",
        (
            NEO_PLAYER_ID,
            trade.get("symbol"),
            trade.get("action"),
            float(trade.get("qty") or 0),
            float(trade.get("price") or 0),
            float(trade.get("entry_price") or 0),
            float(trade.get("exit_price") or 0),
            float(trade.get("realized_pnl") or 0),
            trade.get("reasoning") or "",
            trade.get("executed_at") or "",
        ),
    ).fetchone()
    return bool(row)


def sync_neo_snapshot(payload: dict) -> dict:
    info = ensure_matrix_shared_records()
    conn = _db()
    try:
        season = int(payload.get("season") or info["season"])

        cash = payload.get("cash")
        if cash is not None:
            cash = float(cash)
            conn.execute(
                "UPDATE ai_players SET cash=?, season=? WHERE id=?",
                (cash, season, NEO_PLAYER_ID),
            )
            conn.execute(
                "UPDATE portfolios SET current_balance=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (cash, info["portfolio_id"]),
            )
        else:
            row = conn.execute("SELECT cash FROM ai_players WHERE id=?", (NEO_PLAYER_ID,)).fetchone()
            cash = float(row["cash"] or NEO_STARTING_CASH) if row else NEO_STARTING_CASH

        positions = payload.get("positions") or []
        if payload.get("replace_positions", True):
            conn.execute("DELETE FROM positions WHERE player_id=?", (NEO_PLAYER_ID,))

        positions_value = 0.0
        inserted_positions = 0
        for pos in positions:
            qty = float(pos.get("qty") or pos.get("quantity") or 0)
            avg_price = float(pos.get("avg_price") or pos.get("entry_price") or 0)
            current_price = float(pos.get("current_price") or avg_price or 0)
            positions_value += qty * current_price
            conn.execute(
                "INSERT OR REPLACE INTO positions "
                "(player_id, symbol, qty, avg_price, asset_type, option_type, strike_price, expiry_date, opened_at, high_watermark) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), ?)",
                (
                    NEO_PLAYER_ID,
                    (pos.get("symbol") or pos.get("ticker") or "").upper(),
                    qty,
                    avg_price,
                    pos.get("asset_type") or "stock",
                    pos.get("option_type"),
                    pos.get("strike_price"),
                    pos.get("expiry_date") or pos.get("expiration_date"),
                    pos.get("opened_at"),
                    current_price,
                ),
            )
            inserted_positions += 1

        inserted_trades = 0
        for trade in payload.get("trades") or []:
            normalized_trade = {
                "symbol": (trade.get("symbol") or "").upper(),
                "action": trade.get("action") or "BUY",
                "qty": float(trade.get("qty") or 0),
                "price": float(trade.get("price") or 0),
                "asset_type": trade.get("asset_type") or "stock",
                "option_type": trade.get("option_type"),
                "strike_price": trade.get("strike_price"),
                "expiry_date": trade.get("expiry_date"),
                "reasoning": trade.get("reasoning") or "",
                "confidence": float(trade.get("confidence") or 0),
                "executed_at": trade.get("executed_at") or datetime.now().isoformat(),
                "exit_price": trade.get("exit_price"),
                "realized_pnl": trade.get("realized_pnl"),
                "entry_price": trade.get("entry_price"),
                "sources": trade.get("sources") or MATRIX_TRADE_SOURCE,
                "timeframe": trade.get("timeframe") or "SWING",
            }
            if _insert_missing_trade(conn, normalized_trade, season):
                inserted_trades += 1

        chat_message = (payload.get("chat_message") or "").strip()
        if chat_message:
            conn.execute(
                "INSERT INTO ai_chat (player_id, message, context) VALUES (?, ?, ?)",
                (NEO_PLAYER_ID, chat_message, payload.get("chat_context")),
            )

        war_room_take = (payload.get("war_room_take") or "").strip()
        war_room_symbol = (payload.get("war_room_symbol") or payload.get("symbol") or "SPY").upper()
        if war_room_take:
            conn.execute(
                "INSERT INTO war_room (player_id, symbol, take, strategy_mode) VALUES (?, ?, ?, ?)",
                (NEO_PLAYER_ID, war_room_symbol, war_room_take, payload.get("strategy_mode")),
            )

        total_value = payload.get("total_value")
        if total_value is None:
            total_value = cash + positions_value
        total_value = float(total_value)

        conn.execute(
            "INSERT INTO portfolio_history (player_id, total_value, cash, positions_value, season) "
            "VALUES (?, ?, ?, ?, ?)",
            (NEO_PLAYER_ID, total_value, cash, positions_value, season),
        )

        conn.commit()
        return {
            "ok": True,
            "player_id": NEO_PLAYER_ID,
            "portfolio_id": info["portfolio_id"],
            "positions_synced": inserted_positions,
            "trades_inserted": inserted_trades,
            "chat_inserted": bool(chat_message),
            "war_room_inserted": bool(war_room_take),
            "total_value": round(total_value, 2),
            "cash": round(cash, 2),
            "positions_value": round(positions_value, 2),
            "synced_at": datetime.now().isoformat(),
        }
    finally:
        conn.close()
