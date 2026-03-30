from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class OrderResult:
    success: bool
    order_id: str
    symbol: str
    action: str
    qty: float
    price: float
    message: str = ""


class Broker(ABC):
    @abstractmethod
    def get_portfolio(self, player_id: str) -> dict:
        pass

    @abstractmethod
    def buy(self, player_id: str, symbol: str, price: float,
            qty: float = None, reasoning: str = "") -> OrderResult:
        pass

    @abstractmethod
    def sell(self, player_id: str, symbol: str, price: float,
             reasoning: str = "") -> OrderResult:
        pass

    @abstractmethod
    def get_positions(self, player_id: str) -> list:
        pass
