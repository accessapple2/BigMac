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
        raise PermissionError(
            "BLOCKED: Webull is in MONITOR_ONLY mode per Admiral posture "
            "(2026-04-20). Memory rule #22. To un-mute, remove this "
            "guard AND confirm with Admiral."
        )

    def sell(self, player_id: str, symbol: str, price: float,
             reasoning: str = "") -> OrderResult:
        raise PermissionError(
            "BLOCKED: Webull is in MONITOR_ONLY mode per Admiral posture "
            "(2026-04-20). Memory rule #22. To un-mute, remove this "
            "guard AND confirm with Admiral."
        )

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
