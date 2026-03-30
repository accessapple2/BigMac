"""
Multi-portfolio manager and broker adapter stubs for USS TradeMinds.
"""

import json
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

DB_PATH = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))

try:
    from shared.matrix_bridge import get_neo_portfolio_id, sync_neo_from_native_portfolio
except Exception:
    get_neo_portfolio_id = None
    sync_neo_from_native_portfolio = None


# ---------------------------------------------------------------------------
# Enums & data classes
# ---------------------------------------------------------------------------

class AssetClass(str, Enum):
    STOCK = "stock"
    OPTION = "option"
    SPREAD = "spread"
    METALS = "metals"


class Direction(str, Enum):
    LONG = "long"
    SHORT = "short"


class PositionStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"
    PENDING = "pending"


@dataclass
class Position:
    portfolio_id: int
    ticker: str
    asset_class: str = "stock"
    direction: str = "long"
    quantity: float = 1.0
    entry_price: float = 0.0
    current_price: float = 0.0
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    # Options
    option_type: Optional[str] = None       # call / put
    strike_price: Optional[float] = None
    expiration_date: Optional[str] = None
    # Spreads
    spread_type: Optional[str] = None       # vertical / iron_condor / butterfly
    spread_legs: Optional[str] = None       # JSON string of leg details
    # Metals
    metal_type: Optional[str] = None        # gold / silver / platinum
    metal_oz: Optional[float] = None
    # Calculated
    unrealized_pnl: float = 0.0
    status: str = "open"
    notes: str = ""


# ---------------------------------------------------------------------------
# Portfolio Manager
# ---------------------------------------------------------------------------

class PortfolioManager:
    def __init__(self, db_path: str = None):
        self.db_path = db_path or DB_PATH
        self._ensure_schema()

    def _ensure_schema(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        try:
            conn.execute("PRAGMA busy_timeout=30000")
            for ddl in (
                "ALTER TABLE portfolios ADD COLUMN execution_mode TEXT NOT NULL DEFAULT 'auto'",
                "ALTER TABLE portfolios ADD COLUMN type TEXT NOT NULL DEFAULT 'paper'",
            ):
                try:
                    conn.execute(ddl)
                except sqlite3.OperationalError:
                    pass

            conn.execute(
                """UPDATE portfolios
                   SET execution_mode = CASE
                       WHEN broker = 'physical' OR lower(name) IN ('metals', 'enterprise computer')
                           THEN 'tracking'
                       WHEN is_human = 1
                           THEN 'manual'
                       WHEN execution_mode IS NULL OR execution_mode = ''
                           THEN 'auto'
                       ELSE execution_mode
                   END,
                       type = CASE
                           WHEN broker = 'physical' OR lower(name) IN ('metals', 'enterprise computer')
                               THEN 'physical'
                           WHEN lower(account_type) = 'paper'
                               THEN 'paper'
                           WHEN lower(account_type) IN ('live', 'independent')
                               THEN 'trading'
                           WHEN type IS NULL OR type = ''
                               THEN 'trading'
                           ELSE type
                       END,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE execution_mode IS NULL
                    OR execution_mode = ''
                    OR type IS NULL
                    OR type = ''
                    OR is_human = 1
                    OR lower(account_type) IN ('live', 'independent')
                    OR broker = 'physical'
                    OR lower(name) IN ('metals', 'enterprise computer')"""
            )
            conn.commit()
        finally:
            conn.close()

    def _db(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @staticmethod
    def can_execute(portfolio: Optional[dict]) -> dict:
        if not portfolio:
            return {"allowed": False, "reason": "portfolio not found"}
        execution_mode = str(portfolio.get("execution_mode") or "auto").lower()
        if execution_mode == "tracking":
            return {"allowed": False, "reason": "tracking-only portfolio"}
        if int(portfolio.get("is_human") or 0) == 1:
            return {"allowed": False, "reason": "human-managed portfolio"}
        return {"allowed": True}

    def _maybe_sync_neo(self, portfolio_id: int):
        if not sync_neo_from_native_portfolio or not get_neo_portfolio_id:
            return
        try:
            if int(portfolio_id) == int(get_neo_portfolio_id()):
                sync_neo_from_native_portfolio()
        except Exception:
            pass

    def get_portfolios(self, active_only: bool = False) -> list:
        conn = self._db()
        try:
            query = "SELECT * FROM portfolios"
            if active_only:
                query += " WHERE is_active = 1"
            query += " ORDER BY id"
            return [dict(r) for r in conn.execute(query).fetchall()]
        finally:
            conn.close()

    def get_portfolio(self, portfolio_id: int) -> Optional[dict]:
        conn = self._db()
        try:
            row = conn.execute("SELECT * FROM portfolios WHERE id = ?", (portfolio_id,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def add_portfolio(self, name: str, broker: str, account_type: str = "paper",
                      initial_balance: float = 100000.0, is_human: int = 0, notes: str = "",
                      execution_mode: str = "auto", portfolio_type: str = "paper") -> dict:
        conn = self._db()
        try:
            conn.execute(
                """INSERT INTO portfolios (name, broker, account_type, initial_balance,
                   current_balance, is_human, is_active, notes, execution_mode, type)
                   VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)""",
                (
                    name,
                    broker,
                    account_type,
                    initial_balance,
                    initial_balance,
                    is_human,
                    notes,
                    execution_mode,
                    portfolio_type,
                ),
            )
            conn.commit()
            pid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            return {"id": pid, "name": name, "status": "created"}
        finally:
            conn.close()

    def activate_portfolio(self, portfolio_id: int, active: bool = True) -> dict:
        conn = self._db()
        try:
            conn.execute(
                "UPDATE portfolios SET is_active = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (1 if active else 0, portfolio_id),
            )
            conn.commit()
            return {"id": portfolio_id, "is_active": active}
        finally:
            conn.close()

    def open_position(self, portfolio_id: int, ticker: str, asset_class: str = "stock",
                      direction: str = "long", quantity: float = 1.0, entry_price: float = 0.0,
                      stop_loss: float = None, take_profit: float = None,
                      option_type: str = None, strike_price: float = None,
                      expiration_date: str = None, spread_type: str = None,
                      spread_legs: str = None, metal_type: str = None,
                      metal_oz: float = None, notes: str = "") -> dict:
        conn = self._db()
        try:
            # Guard: check is_human
            portfolio = conn.execute(
                "SELECT * FROM portfolios WHERE id = ?", (portfolio_id,)
            ).fetchone()
            if not portfolio:
                return {"error": f"Portfolio {portfolio_id} not found."}
            portfolio = dict(portfolio)
            execution = self.can_execute(portfolio)
            if not execution["allowed"]:
                return {"error": f"BLOCKED: '{portfolio['name']}' is {execution['reason']}."}

            conn.execute(
                """INSERT INTO portfolio_positions (
                    portfolio_id, ticker, asset_class, direction, quantity, entry_price,
                    current_price, stop_loss, take_profit, option_type, strike_price,
                    expiration_date, spread_type, spread_legs, metal_type, metal_oz,
                    unrealized_pnl, status, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'open', ?)""",
                (portfolio_id, ticker, asset_class, direction, quantity, entry_price,
                 entry_price, stop_loss, take_profit, option_type, strike_price,
                 expiration_date, spread_type, spread_legs, metal_type, metal_oz, notes),
            )
            conn.commit()
            pos_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            result = {"id": pos_id, "ticker": ticker, "status": "opened"}
        finally:
            conn.close()
        self._maybe_sync_neo(portfolio_id)
        return result

    def close_position(self, position_id: int, close_price: float, notes: str = "") -> dict:
        conn = self._db()
        try:
            pos = conn.execute(
                "SELECT * FROM portfolio_positions WHERE id = ? AND status = 'open'",
                (position_id,),
            ).fetchone()
            if not pos:
                return {"error": f"Open position {position_id} not found."}

            pos = dict(pos)
            portfolio = conn.execute(
                "SELECT * FROM portfolios WHERE id = ?",
                (pos["portfolio_id"],),
            ).fetchone()
            portfolio = dict(portfolio) if portfolio else None
            execution = self.can_execute(portfolio)
            if not execution["allowed"]:
                name = portfolio["name"] if portfolio else pos["portfolio_id"]
                return {"error": f"BLOCKED: '{name}' is {execution['reason']}."}
            qty = pos["quantity"]
            entry = pos["entry_price"]
            direction = pos["direction"]

            # Calculate P&L
            if direction == "long":
                pnl = (close_price - entry) * qty
            else:
                pnl = (entry - close_price) * qty

            # For metals, multiply by oz
            if pos["asset_class"] == "metals" and pos["metal_oz"]:
                pnl = (close_price - entry) * pos["metal_oz"]

            conn.execute(
                """UPDATE portfolio_positions SET
                    status = 'closed', current_price = ?, closed_pnl = ?,
                    closed_at = CURRENT_TIMESTAMP, notes = COALESCE(notes || ' | ', '') || ?
                WHERE id = ?""",
                (close_price, pnl, notes, position_id),
            )

            # Update portfolio balance
            conn.execute(
                "UPDATE portfolios SET current_balance = current_balance + ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (pnl, pos["portfolio_id"]),
            )
            conn.commit()
            result = {"id": position_id, "closed_pnl": round(pnl, 2), "status": "closed", "portfolio_id": pos["portfolio_id"]}
        finally:
            conn.close()
        self._maybe_sync_neo(result["portfolio_id"])
        result.pop("portfolio_id", None)
        return result

    def get_open_positions(self, portfolio_id: int = None, asset_class: str = None) -> list:
        conn = self._db()
        try:
            query = """
                SELECT pp.*, p.name as portfolio_name, p.broker
                FROM portfolio_positions pp
                JOIN portfolios p ON pp.portfolio_id = p.id
                WHERE pp.status = 'open'
            """
            params = []
            if portfolio_id:
                query += " AND pp.portfolio_id = ?"
                params.append(portfolio_id)
            if asset_class:
                query += " AND pp.asset_class = ?"
                params.append(asset_class)
            query += " ORDER BY pp.created_at DESC"
            return [dict(r) for r in conn.execute(query, params).fetchall()]
        finally:
            conn.close()

    def get_unified_view(self) -> dict:
        """Single dict with total balance, P&L, per-asset and per-portfolio breakdown."""
        conn = self._db()
        try:
            portfolios = [dict(r) for r in conn.execute("SELECT * FROM portfolios ORDER BY id").fetchall()]

            total_balance = sum(p["current_balance"] for p in portfolios)
            total_initial = sum(p["initial_balance"] for p in portfolios)

            # Open positions
            positions = [dict(r) for r in conn.execute(
                "SELECT * FROM portfolio_positions WHERE status = 'open'"
            ).fetchall()]

            total_unrealized = sum(p.get("unrealized_pnl", 0) for p in positions)

            # Realized P&L from closed positions
            row = conn.execute(
                "SELECT COALESCE(SUM(closed_pnl), 0) as total_realized FROM portfolio_positions WHERE status = 'closed'"
            ).fetchone()
            total_realized = row["total_realized"]

            # By asset class
            by_asset_class = {}
            for p in positions:
                ac = p["asset_class"]
                if ac not in by_asset_class:
                    by_asset_class[ac] = {"count": 0, "unrealized_pnl": 0}
                by_asset_class[ac]["count"] += 1
                by_asset_class[ac]["unrealized_pnl"] += p.get("unrealized_pnl", 0)

            # Per-portfolio summary
            portfolio_summaries = []
            for pf in portfolios:
                pf_positions = [p for p in positions if p["portfolio_id"] == pf["id"]]
                pf_unrealized = sum(p.get("unrealized_pnl", 0) for p in pf_positions)
                portfolio_summaries.append({
                    "id": pf["id"],
                    "name": pf["name"],
                    "broker": pf["broker"],
                    "is_human": pf["is_human"],
                    "is_active": pf["is_active"],
                    "balance": pf["current_balance"],
                    "open_positions": len(pf_positions),
                    "unrealized_pnl": round(pf_unrealized, 2),
                })

            return {
                "total_balance": round(total_balance, 2),
                "total_initial": round(total_initial, 2),
                "total_unrealized_pnl": round(total_unrealized, 2),
                "total_realized_pnl": round(total_realized, 2),
                "total_return_pct": round(((total_balance - total_initial) / total_initial) * 100, 2) if total_initial else 0,
                "by_asset_class": by_asset_class,
                "portfolios": portfolio_summaries,
            }
        finally:
            conn.close()

    def get_exposure(self, portfolio_id: int = None) -> dict:
        """Exposure by ticker and direction for Risk Manager agent."""
        conn = self._db()
        try:
            query = "SELECT * FROM portfolio_positions WHERE status = 'open'"
            params = []
            if portfolio_id:
                query += " AND portfolio_id = ?"
                params.append(portfolio_id)

            positions = [dict(r) for r in conn.execute(query, params).fetchall()]

            exposure = {}
            for p in positions:
                key = f"{p['ticker']}_{p['direction']}"
                if key not in exposure:
                    exposure[key] = {
                        "ticker": p["ticker"],
                        "direction": p["direction"],
                        "total_quantity": 0,
                        "total_value": 0,
                        "positions": 0,
                    }
                exposure[key]["total_quantity"] += p["quantity"]
                exposure[key]["total_value"] += p["quantity"] * p["entry_price"]
                exposure[key]["positions"] += 1

            return {
                "portfolio_id": portfolio_id,
                "exposure": list(exposure.values()),
                "total_positions": len(positions),
            }
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Broker adapters (stubs)
# ---------------------------------------------------------------------------

class BrokerAdapter:
    """Base class for broker adapters."""

    def __init__(self, portfolio_id: int = None):
        self.portfolio_id = portfolio_id

    def get_account_info(self) -> dict:
        raise NotImplementedError

    def get_positions(self) -> list:
        raise NotImplementedError

    def submit_order(self, ticker: str, qty: float, side: str, order_type: str = "market") -> dict:
        raise NotImplementedError

    def cancel_order(self, order_id: str) -> dict:
        raise NotImplementedError


class AlpacaAdapter(BrokerAdapter):
    """Alpaca paper trading — wire to existing alpaca_bridge.py integration."""

    def get_account_info(self) -> dict:
        # TODO: Wire to engine/alpaca_bridge.py
        return {"broker": "alpaca", "status": "stub", "note": "Wire to alpaca_bridge.py"}

    def get_positions(self) -> list:
        return []

    def submit_order(self, ticker, qty, side, order_type="market"):
        return {"broker": "alpaca", "status": "stub", "ticker": ticker, "qty": qty, "side": side}

    def cancel_order(self, order_id):
        return {"broker": "alpaca", "status": "stub", "order_id": order_id}


class WebullAdapter(BrokerAdapter):
    """Steve's live human benchmark — READ ONLY. Never auto-trade."""

    def get_account_info(self) -> dict:
        # TODO: Wire to engine/webull_client.py for read-only data
        return {"broker": "webull", "status": "read_only", "is_human": True}

    def get_positions(self) -> list:
        return []

    def submit_order(self, ticker, qty, side, order_type="market"):
        raise PermissionError("BLOCKED: Webull is human-managed (is_human=1). No automated orders.")

    def cancel_order(self, order_id):
        raise PermissionError("BLOCKED: Webull is human-managed (is_human=1).")


class TradeStationAdapter(BrokerAdapter):
    """TradeStation — activate when ready. Supports options and futures."""

    def get_account_info(self) -> dict:
        return {"broker": "tradestation", "status": "inactive", "note": "Activate when ready"}

    def get_positions(self) -> list:
        return []

    def submit_order(self, ticker, qty, side, order_type="market"):
        return {"broker": "tradestation", "status": "inactive"}

    def cancel_order(self, order_id):
        return {"broker": "tradestation", "status": "inactive"}


class IBKRAdapter(BrokerAdapter):
    """Interactive Brokers — activate when ready. Most comprehensive."""

    def get_account_info(self) -> dict:
        return {"broker": "ibkr", "status": "inactive", "note": "Most comprehensive — activate when ready"}

    def get_positions(self) -> list:
        return []

    def submit_order(self, ticker, qty, side, order_type="market"):
        return {"broker": "ibkr", "status": "inactive"}

    def cancel_order(self, order_id):
        return {"broker": "ibkr", "status": "inactive"}


BROKER_ADAPTERS = {
    "alpaca": AlpacaAdapter,
    "webull": WebullAdapter,
    "tradestation": TradeStationAdapter,
    "ibkr": IBKRAdapter,
}


def get_adapter(broker: str, portfolio_id: int = None) -> BrokerAdapter:
    """Factory function to get the right broker adapter."""
    adapter_class = BROKER_ADAPTERS.get(broker.lower())
    if not adapter_class:
        raise ValueError(f"Unknown broker: {broker}. Available: {list(BROKER_ADAPTERS.keys())}")
    return adapter_class(portfolio_id=portfolio_id)
