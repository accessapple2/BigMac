from __future__ import annotations
from .base import Broker, OrderResult
import uuid


class WebullBroker(Broker):
    """Webull live broker integration using the Webull SDK."""

    INSTRUMENT_MAP = {
        "SPY": "913243251",
        "QQQ": "913243289",
        "NVDA": "913256135",
        "TSLA": "913255598",
        "AAPL": "913323898",
    }

    def __init__(self, app_key: str, app_secret: str, account_id: str):
        from webullsdktrade.api import API
        from webullsdkcore.client import ApiClient
        from webullsdkcore.common.region import Region

        self.client = ApiClient(app_key, app_secret, Region.US.value)
        self.api = API(self.client)
        self.account_id = account_id

    def get_portfolio(self, player_id: str) -> dict:
        # For live trading, fetch from Webull account
        try:
            account = self.api.account.get_account_profile(self.account_id)
            return {
                "cash": float(account.get("buying_power", 0)),
                "positions": self.get_positions(player_id),
            }
        except Exception as e:
            return {"cash": 0, "positions": [], "error": str(e)}

    def buy(self, player_id: str, symbol: str, price: float,
            qty: float = None, reasoning: str = "") -> OrderResult:
        instrument_id = self.INSTRUMENT_MAP.get(symbol)
        if not instrument_id:
            return OrderResult(False, "", symbol, "BUY", 0, price, f"Unknown symbol: {symbol}")

        if qty is None:
            portfolio = self.get_portfolio(player_id)
            qty = round((portfolio["cash"] * 0.10) / price, 0)

        try:
            response = self.api.order.place_order(
                account_id=self.account_id,
                qty=str(int(qty)),
                instrument_id=instrument_id,
                side="BUY",
                order_type="LIMIT",
                limit_price=str(price),
                tif="DAY",
                extended_hours_trading=False,
                client_order_id=str(uuid.uuid4())
            )
            if response.status_code == 200:
                return OrderResult(
                    True, response.json().get("order_id", ""),
                    symbol, "BUY", qty, price
                )
            return OrderResult(False, "", symbol, "BUY", qty, price, response.text)
        except Exception as e:
            return OrderResult(False, "", symbol, "BUY", qty, price, str(e))

    def sell(self, player_id: str, symbol: str, price: float,
             reasoning: str = "") -> OrderResult:
        instrument_id = self.INSTRUMENT_MAP.get(symbol)
        if not instrument_id:
            return OrderResult(False, "", symbol, "SELL", 0, price, f"Unknown symbol: {symbol}")

        positions = self.get_positions(player_id)
        pos = next((p for p in positions if p["symbol"] == symbol), None)
        if not pos:
            return OrderResult(False, "", symbol, "SELL", 0, price, "No position")

        qty = pos["qty"]
        try:
            response = self.api.order.place_order(
                account_id=self.account_id,
                qty=str(int(qty)),
                instrument_id=instrument_id,
                side="SELL",
                order_type="LIMIT",
                limit_price=str(price),
                tif="DAY",
                extended_hours_trading=False,
                client_order_id=str(uuid.uuid4())
            )
            if response.status_code == 200:
                return OrderResult(
                    True, response.json().get("order_id", ""),
                    symbol, "SELL", qty, price
                )
            return OrderResult(False, "", symbol, "SELL", qty, price, response.text)
        except Exception as e:
            return OrderResult(False, "", symbol, "SELL", qty, price, str(e))

    def get_positions(self, player_id: str) -> list:
        try:
            positions = self.api.account.get_account_positions(self.account_id)
            return [
                {"symbol": p.get("ticker", {}).get("symbol", ""),
                 "qty": float(p.get("position", 0)),
                 "avg_price": float(p.get("cost_price", 0)),
                 "asset_type": "stock"}
                for p in positions
            ]
        except Exception:
            return []
