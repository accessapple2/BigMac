"""Alpaca Paper Trading Bridge — connects to Alpaca's paper trading API."""
import os
from rich.console import Console

console = Console()


class AlpacaBridge:
    def __init__(self):
        from dotenv import load_dotenv
        load_dotenv()
        key = os.getenv('ALPACA_API_KEY', '')
        secret = os.getenv('ALPACA_SECRET_KEY', '')
        self.client = None
        if key and secret:
            try:
                from alpaca.trading.client import TradingClient
                self.client = TradingClient(key, secret, paper=True)
                console.log("[green]Alpaca Paper Trading bridge initialized")
            except Exception as e:
                console.log(f"[red]Alpaca init error: {e}")

    def status(self):
        if not self.client:
            return {'connected': False, 'reason': 'No API keys or client init failed'}
        try:
            a = self.client.get_account()
            return {
                'connected': True, 'equity': float(a.equity), 'cash': float(a.cash),
                'buying_power': float(a.buying_power), 'portfolio_value': float(a.portfolio_value),
                'status': a.status, 'currency': a.currency,
            }
        except Exception as e:
            return {'connected': False, 'reason': str(e)}

    def positions(self):
        if not self.client:
            return []
        try:
            return [{
                'symbol': p.symbol, 'qty': float(p.qty),
                'avg_entry': float(p.avg_entry_price), 'current_price': float(p.current_price),
                'market_value': float(p.market_value), 'unrealized_pl': float(p.unrealized_pl),
                'unrealized_plpc': round(float(p.unrealized_plpc) * 100, 2),
            } for p in self.client.get_all_positions()]
        except Exception as e:
            return [{'error': str(e)}]

    def orders(self, status='all'):
        if not self.client:
            return []
        try:
            from alpaca.trading.requests import GetOrdersRequest
            from alpaca.trading.enums import QueryOrderStatus
            m = {'open': QueryOrderStatus.OPEN, 'closed': QueryOrderStatus.CLOSED, 'all': QueryOrderStatus.ALL}
            return [{
                'id': str(o.id), 'symbol': o.symbol, 'side': o.side.value,
                'qty': str(o.qty), 'type': o.type.value, 'status': o.status.value,
                'filled_avg_price': str(o.filled_avg_price) if o.filled_avg_price else None,
                'submitted_at': str(o.submitted_at),
            } for o in self.client.get_orders(GetOrdersRequest(status=m.get(status, QueryOrderStatus.ALL), limit=50))]
        except Exception as e:
            return [{'error': str(e)}]

    def buy(self, symbol, qty, extended_hours: bool = False, limit_price: float = 0.0):
        if not self.client:
            return {'error': 'Not connected'}
        try:
            from alpaca.trading.enums import OrderSide, TimeInForce
            if extended_hours and limit_price > 0:
                # Alpaca requires limit orders for extended-hours trading.
                # Use a 0.5% premium above current price to favour a fill.
                from alpaca.trading.requests import LimitOrderRequest
                lp = round(limit_price * 1.005, 2)
                o = self.client.submit_order(LimitOrderRequest(
                    symbol=symbol, qty=float(qty), side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY, limit_price=lp,
                    extended_hours=True,
                ))
                console.log(f"[green]Alpaca BUY {qty} {symbol} EXTENDED limit=${lp:.2f} — order {o.id}")
            else:
                from alpaca.trading.requests import MarketOrderRequest
                o = self.client.submit_order(MarketOrderRequest(
                    symbol=symbol, qty=float(qty), side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY,
                ))
                console.log(f"[green]Alpaca BUY {qty} {symbol} — order {o.id}")
            return {'success': True, 'order_id': str(o.id), 'symbol': o.symbol, 'status': o.status.value}
        except Exception as e:
            return {'error': str(e)}

    def sell(self, symbol, qty, extended_hours: bool = False, limit_price: float = 0.0):
        if not self.client:
            return {'error': 'Not connected'}
        try:
            from alpaca.trading.enums import OrderSide, TimeInForce
            if extended_hours and limit_price > 0:
                # Alpaca requires limit orders for extended-hours trading.
                # Use a 0.5% discount below current price to favour a fill.
                from alpaca.trading.requests import LimitOrderRequest
                lp = round(limit_price * 0.995, 2)
                o = self.client.submit_order(LimitOrderRequest(
                    symbol=symbol, qty=float(qty), side=OrderSide.SELL,
                    time_in_force=TimeInForce.DAY, limit_price=lp,
                    extended_hours=True,
                ))
                console.log(f"[red]Alpaca SELL {qty} {symbol} EXTENDED limit=${lp:.2f} — order {o.id}")
            else:
                from alpaca.trading.requests import MarketOrderRequest
                o = self.client.submit_order(MarketOrderRequest(
                    symbol=symbol, qty=float(qty), side=OrderSide.SELL,
                    time_in_force=TimeInForce.DAY,
                ))
                console.log(f"[red]Alpaca SELL {qty} {symbol} — order {o.id}")
            return {'success': True, 'order_id': str(o.id), 'symbol': o.symbol, 'status': o.status.value}
        except Exception as e:
            return {'error': str(e)}

    def close_position(self, symbol):
        if not self.client:
            return {'error': 'Not connected'}
        try:
            self.client.close_position(symbol)
            console.log(f"[yellow]Alpaca CLOSED position: {symbol}")
            return {'success': True, 'message': f'{symbol} closed'}
        except Exception as e:
            return {'error': str(e)}

    def close_all(self):
        if not self.client:
            return {'error': 'Not connected'}
        try:
            self.client.close_all_positions()
            console.log("[red]Alpaca CLOSE ALL positions executed")
            return {'success': True, 'message': 'All positions closed'}
        except Exception as e:
            return {'error': str(e)}


alpaca = AlpacaBridge()
