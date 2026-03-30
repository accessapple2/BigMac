from __future__ import annotations
from .base import Broker, OrderResult
from engine.paper_trader import get_portfolio, buy, sell, get_position
import uuid


class PaperBroker(Broker):
    def get_portfolio(self, player_id: str) -> dict:
        return get_portfolio(player_id)

    def buy(self, player_id: str, symbol: str, price: float,
            qty: float = None, reasoning: str = "") -> OrderResult:
        result = buy(player_id, symbol, price, qty=qty, reasoning=reasoning)
        if result:
            return OrderResult(
                success=True, order_id=str(uuid.uuid4()),
                symbol=symbol, action="BUY",
                qty=result["qty"], price=price
            )
        return OrderResult(
            success=False, order_id="", symbol=symbol,
            action="BUY", qty=0, price=price, message="Order failed"
        )

    def sell(self, player_id: str, symbol: str, price: float,
             reasoning: str = "") -> OrderResult:
        pos = get_position(player_id, symbol)
        if not pos:
            return OrderResult(
                success=False, order_id="", symbol=symbol,
                action="SELL", qty=0, price=price, message="No position"
            )
        result = sell(player_id, symbol, price, reasoning=reasoning)
        if result:
            return OrderResult(
                success=True, order_id=str(uuid.uuid4()),
                symbol=symbol, action="SELL",
                qty=pos["qty"], price=price
            )
        return OrderResult(
            success=False, order_id="", symbol=symbol,
            action="SELL", qty=0, price=price, message="Sell failed"
        )

    def get_positions(self, player_id: str) -> list:
        return get_portfolio(player_id)["positions"]
