"""Alpaca Paper Trading Bridge — connects to Alpaca's paper trading API."""
import os
from rich.console import Console
from engine.trade_gateway import check_trade

console = Console()


class AlpacaBridge:
    def __init__(self):
        from dotenv import load_dotenv
        load_dotenv()
        key = os.getenv('APCA_API_KEY_ID', '')
        secret = os.getenv('APCA_API_SECRET_KEY', '')
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


    def latest_prices(self, symbols):
        """Return {symbol: last_price} for a list of symbols (used for real-position P/L)."""
        if not symbols or not self.client:
            return {}
        try:
            from alpaca.data.historical import StockHistoricalDataClient
            from alpaca.data.requests import StockLatestTradeRequest
            key = os.getenv('APCA_API_KEY_ID', '')
            secret = os.getenv('APCA_API_SECRET_KEY', '')
            dc = StockHistoricalDataClient(key, secret)
            req = StockLatestTradeRequest(symbol_or_symbols=list(set(symbols)))
            trades = dc.get_stock_latest_trade(req)
            return {s: float(trades[s].price) for s in trades}
        except Exception as e:
            console.log(f"[yellow]latest_prices error: {e}")
            return {}

    def _poll_fill(self, order_id: str, timeout_s: float = 3.0,
                   poll_interval_s: float = 0.15):
        """HM-TRADES-PRICE-WRITEBACK-FIX 2026-05-21: poll an order to fill.

        Bridge.buy/sell submit and return immediately with status='new' — no
        filled_avg_price. To capture the actual broker fill (so trades.entry_price
        / trades.exit_price reflect reality not the trader's internal target),
        poll get_order_by_id until status is filled / partially_filled / a
        terminal-non-fill (canceled, rejected, expired). Returns
        (filled_avg_price, filled_qty, final_status). On timeout returns
        (None, None, last_status_seen).

        Live-market fills typically complete in <500ms; 3s timeout covers
        extended-hours/illiquid edges. Never raises.
        """
        import time
        deadline = time.time() + max(0.5, float(timeout_s))
        last_status = 'unknown'
        while time.time() < deadline:
            try:
                o = self.client.get_order_by_id(order_id)
                last_status = o.status.value if hasattr(o.status, 'value') else str(o.status)
                if last_status in ('filled', 'partially_filled'):
                    if o.filled_avg_price is not None:
                        return (float(o.filled_avg_price),
                                float(o.filled_qty) if o.filled_qty is not None else 0.0,
                                last_status)
                if last_status in ('canceled', 'cancelled', 'rejected', 'expired'):
                    return (None, None, last_status)
            except Exception:
                pass
            time.sleep(poll_interval_s)
        return (None, None, last_status)

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

    def _build_order_request(self, *, symbol: str, side, qty: float,
                             order_type: str, limit_price: float,
                             stop_price: float, extended_hours: bool,
                             notional: float = 0.0,
                             time_in_force: str = "DAY"):
        """HM-TRADE-DESK 2026-05-22: dispatch on order_type ∈
        {market, limit, stop, stop_limit}. Returns the appropriate
        alpaca-py request object. Extended-hours forces a limit-with-premium
        on otherwise-Market orders (Alpaca requirement).

        notional (dollar amount) is Alpaca-restricted to MarketOrderRequest
        on fractionable equities. Pass notional>0 with order_type='market'
        and qty=0; the helper builds MarketOrderRequest(notional=...).
        Limit/Stop orders REQUIRE qty (raises ValueError if notional set).

        HM-TRADE-DESK-AUTOPILOT 2026-05-22: time_in_force ∈ {"DAY","GTC","IOC","FOK"}
        — defaults to DAY to preserve legacy fleet behavior. The autopilot
        helper passes "GTC" so attached stop/target orders survive the close.
        """
        from alpaca.trading.enums import TimeInForce
        from alpaca.trading.requests import (
            MarketOrderRequest, LimitOrderRequest, StopOrderRequest,
            StopLimitOrderRequest,
        )
        _tif_map = {
            "day": TimeInForce.DAY, "gtc": TimeInForce.GTC,
            "ioc": TimeInForce.IOC, "fok": TimeInForce.FOK,
        }
        tif = _tif_map.get((time_in_force or "DAY").lower(), TimeInForce.DAY)
        # HM-ALPACA-BRIDGE-LIMIT-FIX (HM-NEXT-WAVE Phase 1) 2026-05-23:
        # Alpaca rule — extended-hours orders MUST be time_in_force=DAY
        # and MUST be limit orders. The DAY enforcement is canonical here
        # so a caller that passes time_in_force='gtc' on an extended-hours
        # bracket leg doesn't get silently rejected by the broker. The
        # market→limit coerce happens below (line ~155). Together these
        # form the "you can submit during XH" contract.
        if extended_hours and tif != TimeInForce.DAY:
            console.log(
                f"[yellow][ALPACA-BRIDGE] extended_hours=True forces "
                f"tif=DAY (caller passed {time_in_force!r}). Alpaca requirement."
            )
            tif = TimeInForce.DAY
        ot = (order_type or "market").lower()
        if notional and notional > 0 and ot != "market":
            raise ValueError(
                f"notional sizing is Market-only per Alpaca SDK; got order_type={ot!r}"
            )
        # Extended-hours override: if caller didn't explicitly pick limit/stop
        # and we're in extended-hours with a price, coerce to limit-with-premium
        # to satisfy Alpaca's extended-hours requirement.
        if extended_hours and ot == "market" and limit_price > 0:
            premium = 1.005 if side.value == "buy" else 0.995
            lp = round(limit_price * premium, 2)
            return LimitOrderRequest(
                symbol=symbol, qty=qty, side=side,
                time_in_force=tif, limit_price=lp,
                extended_hours=True,
            )
        if ot == "limit":
            return LimitOrderRequest(
                symbol=symbol, qty=qty, side=side,
                time_in_force=tif,
                limit_price=round(float(limit_price), 2),
                extended_hours=extended_hours,
            )
        if ot == "stop":
            return StopOrderRequest(
                symbol=symbol, qty=qty, side=side,
                time_in_force=tif,
                stop_price=round(float(stop_price), 2),
            )
        if ot == "stop_limit":
            return StopLimitOrderRequest(
                symbol=symbol, qty=qty, side=side,
                time_in_force=tif,
                stop_price=round(float(stop_price), 2),
                limit_price=round(float(limit_price), 2),
            )
        # Default: market — qty OR notional (not both)
        if notional and notional > 0:
            return MarketOrderRequest(
                symbol=symbol, notional=round(float(notional), 2),
                side=side, time_in_force=tif,
            )
        return MarketOrderRequest(
            symbol=symbol, qty=qty, side=side,
            time_in_force=tif,
        )

    def buy(self, symbol, qty, agent_id: str = "unknown", extended_hours: bool = False,
            limit_price: float = 0.0, order_type: str = "market", stop_price: float = 0.0,
            notional: float = 0.0, time_in_force: str = "DAY"):
        if not self.client:
            return {'error': 'Not connected'}
        try:
            # check_trade uses qty*price; when notional drives sizing, qty is 0
            # and we pass notional as the dollar value instead.
            _check_qty = float(qty) if (not notional or notional <= 0) else 0.0
            _check_price = float(limit_price or 0) if _check_qty > 0 else float(notional or 0)
            result = check_trade(agent_id, symbol, "BUY", _check_qty, _check_price)
            if not result["allowed"]:
                return {"error": f"Gateway blocked: {result['reason']}"}
            from alpaca.trading.enums import OrderSide
            req = self._build_order_request(
                symbol=symbol, side=OrderSide.BUY, qty=float(qty),
                order_type=order_type, limit_price=float(limit_price or 0),
                stop_price=float(stop_price or 0), extended_hours=extended_hours,
                notional=float(notional or 0), time_in_force=time_in_force,
            )
            o = self.client.submit_order(req)
            _size_log = f"${notional:.2f}" if notional and notional > 0 else f"{qty}"
            console.log(f"[green]Alpaca BUY {_size_log} {symbol} type={order_type} — order {o.id}")
            # HM-TRADES-PRICE-WRITEBACK-FIX 2026-05-21: poll for fill so callers
            # can persist filled_avg_price into trades.entry_price (not the
            # submit-time internal target). Fail-safe: returns None on timeout.
            fill_price, fill_qty, final_status = self._poll_fill(str(o.id))
            return {
                'success': True, 'order_id': str(o.id), 'symbol': o.symbol,
                'status': final_status, 'filled_avg_price': fill_price,
                'filled_qty': fill_qty,
            }
        except Exception as e:
            return {'error': str(e)}

    def sell(self, symbol, qty, agent_id: str = "unknown", extended_hours: bool = False,
             limit_price: float = 0.0, order_type: str = "market", stop_price: float = 0.0,
             notional: float = 0.0, time_in_force: str = "DAY"):
        if not self.client:
            return {'error': 'Not connected'}
        try:
            _check_qty = float(qty) if (not notional or notional <= 0) else 0.0
            _check_price = float(limit_price or 0) if _check_qty > 0 else float(notional or 0)
            result = check_trade(agent_id, symbol, "SELL", _check_qty, _check_price)
            if not result["allowed"]:
                return {"error": f"Gateway blocked: {result['reason']}"}
            from alpaca.trading.enums import OrderSide
            req = self._build_order_request(
                symbol=symbol, side=OrderSide.SELL, qty=float(qty),
                order_type=order_type, limit_price=float(limit_price or 0),
                stop_price=float(stop_price or 0), extended_hours=extended_hours,
                notional=float(notional or 0), time_in_force=time_in_force,
            )
            o = self.client.submit_order(req)
            _size_log = f"${notional:.2f}" if notional and notional > 0 else f"{qty}"
            console.log(f"[red]Alpaca SELL {_size_log} {symbol} type={order_type} — order {o.id}")
            # HM-TRADES-PRICE-WRITEBACK-FIX 2026-05-21: see buy() above.
            fill_price, fill_qty, final_status = self._poll_fill(str(o.id))
            return {
                'success': True, 'order_id': str(o.id), 'symbol': o.symbol,
                'status': final_status, 'filled_avg_price': fill_price,
                'filled_qty': fill_qty,
            }
        except Exception as e:
            return {'error': str(e)}

    def short_sell(self, symbol, qty, agent_id: str = "unknown"):
        """Open a short position via Alpaca paper. Submits a SELL order with no existing long.
        Alpaca paper accounts allow short selling — the position will show as negative qty."""
        if not self.client:
            return {'error': 'Not connected'}
        try:
            result = check_trade(agent_id, symbol, "SHORT", float(qty), 0.0)
            if not result["allowed"]:
                return {"error": f"Gateway blocked: {result['reason']}"}
            from alpaca.trading.enums import OrderSide, TimeInForce
            from alpaca.trading.requests import MarketOrderRequest
            o = self.client.submit_order(MarketOrderRequest(
                symbol=symbol, qty=float(qty), side=OrderSide.SELL,
                time_in_force=TimeInForce.DAY,
            ))
            console.log(f"[bold red]Alpaca SHORT {qty} {symbol} — order {o.id}")
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

    def submit_protective_orders(self, *, symbol: str, entry_side: str,
                                 qty: float, fill_price: float,
                                 sl_pct: float, tp_pct: float) -> dict:
        """HM-TRADE-DESK-AUTOPILOT 2026-05-22 — attach two separate GTC
        protective orders (stop-loss + take-profit) to a just-filled
        Trade Desk position.

        entry_side: 'BUY' (long) or 'SELL' (short). Protective orders
        trade the OPPOSITE side: long → sell-stop + sell-limit;
        short → buy-stop + buy-limit (to cover).

        sl_pct / tp_pct are positive percentages (e.g. 8.0 for 8%).
        Sign-aware: long stop = fill*(1 - sl/100); long target =
        fill*(1 + tp/100); short inverts both. Pass 0 to skip a leg.

        Bypasses check_trade — these are risk-management orders on an
        already-passed primary fill, not new trade intent.

        Returns {'stop_order_id', 'target_order_id', 'stop_price',
        'target_price', 'errors': list[str]}. Never raises; broker
        rejections are captured in errors for caller logging.
        """
        out: dict = {
            'stop_order_id': None, 'target_order_id': None,
            'stop_price': None, 'target_price': None, 'errors': [],
        }
        if not self.client:
            out['errors'].append('Not connected')
            return out
        qty_f = float(qty or 0)
        fill_f = float(fill_price or 0)
        if qty_f <= 0 or fill_f <= 0:
            out['errors'].append(
                f"invalid qty={qty_f} or fill_price={fill_f}"
            )
            return out

        is_long = (entry_side or '').upper() == 'BUY'
        from alpaca.trading.enums import OrderSide, TimeInForce
        from alpaca.trading.requests import (
            StopOrderRequest, LimitOrderRequest,
        )
        exit_side = OrderSide.SELL if is_long else OrderSide.BUY

        sl = float(sl_pct or 0)
        tp = float(tp_pct or 0)
        if is_long:
            stop_price = round(fill_f * (1.0 - sl / 100.0), 2)
            target_price = round(fill_f * (1.0 + tp / 100.0), 2)
        else:
            stop_price = round(fill_f * (1.0 + sl / 100.0), 2)
            target_price = round(fill_f * (1.0 - tp / 100.0), 2)
        out['stop_price'] = stop_price if sl > 0 else None
        out['target_price'] = target_price if tp > 0 else None

        # Stop-loss leg.
        if sl > 0 and stop_price > 0:
            try:
                req = StopOrderRequest(
                    symbol=symbol, qty=qty_f, side=exit_side,
                    time_in_force=TimeInForce.GTC, stop_price=stop_price,
                )
                o = self.client.submit_order(req)
                out['stop_order_id'] = str(o.id)
                console.log(
                    f"[yellow]Alpaca AUTOPILOT stop {qty_f} {symbol} "
                    f"@ ${stop_price} GTC — order {o.id}"
                )
            except Exception as e:
                msg = f"stop: {type(e).__name__}: {e!r}"
                out['errors'].append(msg)
                console.log(f"[red]AUTOPILOT {msg}")

        # Take-profit leg.
        if tp > 0 and target_price > 0:
            try:
                req = LimitOrderRequest(
                    symbol=symbol, qty=qty_f, side=exit_side,
                    time_in_force=TimeInForce.GTC, limit_price=target_price,
                )
                o = self.client.submit_order(req)
                out['target_order_id'] = str(o.id)
                console.log(
                    f"[cyan]Alpaca AUTOPILOT target {qty_f} {symbol} "
                    f"@ ${target_price} GTC — order {o.id}"
                )
            except Exception as e:
                msg = f"target: {type(e).__name__}: {e!r}"
                out['errors'].append(msg)
                console.log(f"[red]AUTOPILOT {msg}")

        return out

    def cancel(self, order_id: str):
        """Cancel an open Alpaca order by id. HM-TRADE-DESK 2026-05-22."""
        if not self.client:
            return {'error': 'Not connected'}
        if not order_id:
            return {'error': 'order_id required'}
        try:
            self.client.cancel_order_by_id(order_id)
            console.log(f"[yellow]Alpaca CANCEL order {order_id}")
            return {'success': True, 'order_id': order_id, 'status': 'canceled'}
        except Exception as e:
            return {'error': f"{type(e).__name__}: {e!r}"}


alpaca = AlpacaBridge()
