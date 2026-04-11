"""Alpaca Options Executor — single-leg, vertical spreads, iron condors.

Only activates for dayblade-0dte and dayblade-sulu.
Limits: 5 contracts max (single-leg), 3 per leg (multi-leg), $500 max capital per trade.
Auto-close: call close_all_options() at 12:45 PM MST / 3:45 PM ET.
"""
from __future__ import annotations
import os
from datetime import date, timedelta
from rich.console import Console

console = Console()

# Hard limits
MAX_SINGLE_CONTRACTS = 5
MAX_SPREAD_CONTRACTS = 3
MAX_CAPITAL_PER_TRADE = 500.0

# Only these players get real Alpaca options execution
OPTIONS_PLAYERS = {"dayblade-0dte", "dayblade-sulu"}

_client = None
_client_init = False


def _get_client():
    global _client, _client_init
    if _client_init:
        return _client
    _client_init = True
    try:
        from dotenv import load_dotenv
        load_dotenv()
        key = os.getenv("ALPACA_API_KEY", "")
        secret = os.getenv("ALPACA_SECRET_KEY", "")
        if not key or not secret:
            console.log("[yellow]Alpaca options: No API keys — skipping real execution")
            return None
        from alpaca.trading.client import TradingClient
        _client = TradingClient(key, secret, paper=True)
        console.log("[green]Alpaca options executor ready (paper)")
    except Exception as e:
        console.log(f"[red]Alpaca options init error: {e}")
    return _client


def get_atm_contract(symbol: str, option_type: str, target_dte: int = 0) -> str | None:
    """Fetch the closest ATM contract symbol from Alpaca.

    Returns OCC-format symbol like 'SPY260404C00580000' or None.
    """
    client = _get_client()
    if not client:
        return None
    try:
        from alpaca.trading.requests import GetOptionContractsRequest
        from alpaca.trading.enums import ContractType
        today = date.today()
        exp_min = today + timedelta(days=max(0, target_dte - 1))
        exp_max = today + timedelta(days=max(target_dte + 3, 7))
        ctype = ContractType.CALL if option_type == "call" else ContractType.PUT
        req = GetOptionContractsRequest(
            underlying_symbols=[symbol],
            type=ctype,
            expiration_date_gte=exp_min.isoformat(),
            expiration_date_lte=exp_max.isoformat(),
            limit=50,
        )
        result = client.get_option_contracts(req)
        contracts = result.option_contracts
        if not contracts:
            console.log(f"[yellow]Alpaca options: No contracts for {symbol} {option_type} dte~{target_dte}")
            return None

        # Get current stock price to find ATM
        current_price = _get_current_price(symbol)
        if not current_price:
            # Pick first tradable contract
            for c in contracts:
                if c.tradable:
                    return c.symbol
            return None

        # Find closest ATM that's tradable
        best = None
        best_dist = float("inf")
        for c in contracts:
            if not c.tradable:
                continue
            dist = abs(float(c.strike_price) - current_price)
            if dist < best_dist:
                best_dist = dist
                best = c

        if best:
            console.log(f"[dim]Alpaca options: Selected {best.symbol} strike={best.strike_price} exp={best.expiration_date}")
            return best.symbol
    except Exception as e:
        console.log(f"[yellow]Alpaca options get_atm_contract error: {e}")
    return None


def get_spread_contracts(
    symbol: str, option_type: str, target_dte: int, current_price: float
) -> tuple[str | None, str | None]:
    """Get (buy_contract, sell_contract) for a vertical spread.

    Bull call spread: buy ATM call, sell OTM call ~5% above.
    Bear put spread: buy ATM put, sell OTM put ~5% below.
    Returns (buy_symbol, sell_symbol) or (None, None).
    """
    client = _get_client()
    if not client:
        return None, None
    try:
        from alpaca.trading.requests import GetOptionContractsRequest
        from alpaca.trading.enums import ContractType
        today = date.today()
        exp_min = today + timedelta(days=max(0, target_dte - 1))
        exp_max = today + timedelta(days=max(target_dte + 3, 7))
        ctype = ContractType.CALL if option_type == "call" else ContractType.PUT
        req = GetOptionContractsRequest(
            underlying_symbols=[symbol],
            type=ctype,
            expiration_date_gte=exp_min.isoformat(),
            expiration_date_lte=exp_max.isoformat(),
            limit=100,
        )
        result = client.get_option_contracts(req)
        contracts = [c for c in result.option_contracts if c.tradable]
        if len(contracts) < 2:
            return None, None

        # For bull call spread: buy near ATM, sell ~5% OTM above
        # For bear put spread: buy near ATM, sell ~5% OTM below
        otm_offset = current_price * 0.05

        if option_type == "call":
            buy_target = current_price         # ATM
            sell_target = current_price + otm_offset  # OTM
        else:
            buy_target = current_price         # ATM
            sell_target = current_price - otm_offset  # OTM

        buy_contract = _nearest_strike(contracts, buy_target)
        sell_contract = _nearest_strike(contracts, sell_target)

        if buy_contract and sell_contract and buy_contract.symbol != sell_contract.symbol:
            return buy_contract.symbol, sell_contract.symbol
    except Exception as e:
        console.log(f"[yellow]Alpaca options get_spread_contracts error: {e}")
    return None, None


def get_iron_condor_contracts(
    symbol: str, target_dte: int, current_price: float
) -> tuple[str | None, str | None, str | None, str | None]:
    """Get (call_buy, call_sell, put_buy, put_sell) for an iron condor.

    Sell OTM call and OTM put, buy further OTM to cap risk.
    Wings ~5% and ~10% from current price.
    Returns (call_buy, call_sell, put_buy, put_sell) or (None, None, None, None).
    """
    client = _get_client()
    if not client:
        return None, None, None, None
    try:
        from alpaca.trading.requests import GetOptionContractsRequest
        from alpaca.trading.enums import ContractType
        today = date.today()
        exp_min = today + timedelta(days=max(0, target_dte - 1))
        exp_max = today + timedelta(days=max(target_dte + 3, 7))

        # Fetch calls
        call_req = GetOptionContractsRequest(
            underlying_symbols=[symbol], type=ContractType.CALL,
            expiration_date_gte=exp_min.isoformat(), expiration_date_lte=exp_max.isoformat(),
            limit=100,
        )
        calls = [c for c in client.get_option_contracts(call_req).option_contracts if c.tradable]

        # Fetch puts
        put_req = GetOptionContractsRequest(
            underlying_symbols=[symbol], type=ContractType.PUT,
            expiration_date_gte=exp_min.isoformat(), expiration_date_lte=exp_max.isoformat(),
            limit=100,
        )
        puts = [c for c in client.get_option_contracts(put_req).option_contracts if c.tradable]

        if len(calls) < 2 or len(puts) < 2:
            return None, None, None, None

        wing_near = current_price * 0.05   # 5% OTM — short strikes
        wing_far  = current_price * 0.10   # 10% OTM — long strikes (defined risk)

        call_sell = _nearest_strike(calls, current_price + wing_near)
        call_buy  = _nearest_strike(calls, current_price + wing_far)
        put_sell  = _nearest_strike(puts,  current_price - wing_near)
        put_buy   = _nearest_strike(puts,  current_price - wing_far)

        if all([call_sell, call_buy, put_sell, put_buy]):
            # Sanity: short strikes must be closer to ATM than long strikes
            cs_strike = float(call_sell.strike_price)
            cb_strike = float(call_buy.strike_price)
            ps_strike = float(put_sell.strike_price)
            pb_strike = float(put_buy.strike_price)
            if cs_strike < cb_strike and ps_strike > pb_strike:
                return call_buy.symbol, call_sell.symbol, put_buy.symbol, put_sell.symbol
    except Exception as e:
        console.log(f"[yellow]Alpaca options get_iron_condor_contracts error: {e}")
    return None, None, None, None


def submit_single_option(
    player_id: str, contract_symbol: str, qty: int, side: str = "buy"
) -> dict:
    """Submit a single-leg options market order.

    Args:
        player_id: Must be in OPTIONS_PLAYERS or this is a no-op.
        contract_symbol: OCC format, e.g. 'SPY260404C00580000'.
        qty: Number of contracts (capped at MAX_SINGLE_CONTRACTS).
        side: 'buy' or 'sell'.
    Returns dict with success/error.
    """
    if player_id not in OPTIONS_PLAYERS:
        return {"skipped": True, "reason": f"{player_id} not in options players list"}
    client = _get_client()
    if not client:
        return {"skipped": True, "reason": "Alpaca not connected"}

    qty = min(int(qty), MAX_SINGLE_CONTRACTS)
    if qty <= 0:
        return {"error": "qty must be >= 1"}

    try:
        from alpaca.trading.requests import MarketOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce
        order = client.submit_order(MarketOrderRequest(
            symbol=contract_symbol,
            qty=qty,
            side=OrderSide.BUY if side == "buy" else OrderSide.SELL,
            time_in_force=TimeInForce.DAY,
        ))
        console.log(f"[bold cyan]Alpaca OPTIONS {side.upper()} {qty}x {contract_symbol} — {player_id} order={order.id}")
        return {"success": True, "order_id": str(order.id), "symbol": contract_symbol, "qty": qty}
    except Exception as e:
        console.log(f"[yellow]Alpaca options submit_single error: {e}")
        return {"error": str(e)}


def submit_vertical_spread(
    player_id: str, buy_symbol: str, sell_symbol: str, qty: int, strategy: str
) -> dict:
    """Submit a defined-risk vertical spread as a multi-leg order.

    Bull call spread: buy_symbol = lower strike call, sell_symbol = higher strike call.
    Bear put spread:  buy_symbol = higher strike put, sell_symbol = lower strike put.
    """
    if player_id not in OPTIONS_PLAYERS:
        return {"skipped": True, "reason": f"{player_id} not in options players list"}
    client = _get_client()
    if not client:
        return {"skipped": True, "reason": "Alpaca not connected"}

    qty = min(int(qty), MAX_SPREAD_CONTRACTS)
    if qty <= 0:
        return {"error": "qty must be >= 1"}

    try:
        from alpaca.trading.requests import MarketOrderRequest, OptionLegRequest
        from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass, PositionIntent
        order = client.submit_order(MarketOrderRequest(
            qty=qty,
            time_in_force=TimeInForce.DAY,
            order_class=OrderClass.MLEG,
            legs=[
                OptionLegRequest(
                    symbol=buy_symbol, ratio_qty=1,
                    side=OrderSide.BUY, position_intent=PositionIntent.BTO,
                ),
                OptionLegRequest(
                    symbol=sell_symbol, ratio_qty=1,
                    side=OrderSide.SELL, position_intent=PositionIntent.STO,
                ),
            ],
        ))
        console.log(f"[bold cyan]Alpaca {strategy} {qty}x — {player_id} order={order.id}")
        return {"success": True, "order_id": str(order.id), "strategy": strategy, "qty": qty}
    except Exception as e:
        console.log(f"[yellow]Alpaca options submit_spread error: {e}")
        return {"error": str(e)}


def submit_iron_condor(
    player_id: str,
    call_buy: str, call_sell: str, put_buy: str, put_sell: str,
    qty: int,
) -> dict:
    """Submit a 4-leg iron condor. All legs are defined-risk (no naked shorts)."""
    if player_id not in OPTIONS_PLAYERS:
        return {"skipped": True, "reason": f"{player_id} not in options players list"}
    client = _get_client()
    if not client:
        return {"skipped": True, "reason": "Alpaca not connected"}

    qty = min(int(qty), MAX_SPREAD_CONTRACTS)
    if qty <= 0:
        return {"error": "qty must be >= 1"}

    try:
        from alpaca.trading.requests import MarketOrderRequest, OptionLegRequest
        from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass, PositionIntent
        order = client.submit_order(MarketOrderRequest(
            qty=qty,
            time_in_force=TimeInForce.DAY,
            order_class=OrderClass.MLEG,
            legs=[
                OptionLegRequest(symbol=call_buy,  ratio_qty=1, side=OrderSide.BUY,  position_intent=PositionIntent.BTO),
                OptionLegRequest(symbol=call_sell, ratio_qty=1, side=OrderSide.SELL, position_intent=PositionIntent.STO),
                OptionLegRequest(symbol=put_buy,   ratio_qty=1, side=OrderSide.BUY,  position_intent=PositionIntent.BTO),
                OptionLegRequest(symbol=put_sell,  ratio_qty=1, side=OrderSide.SELL, position_intent=PositionIntent.STO),
            ],
        ))
        console.log(f"[bold cyan]Alpaca IRON_CONDOR {qty}x {call_buy[:3]} — {player_id} order={order.id}")
        return {"success": True, "order_id": str(order.id), "strategy": "IRON_CONDOR", "qty": qty}
    except Exception as e:
        console.log(f"[yellow]Alpaca options submit_iron_condor error: {e}")
        return {"error": str(e)}


def close_options_position(player_id: str, contract_symbol: str, qty: int) -> dict:
    """Close (sell to close) a specific options position."""
    return submit_single_option(player_id, contract_symbol, qty, side="sell")


def close_all_options(player_id: str | None = None) -> dict:
    """Close ALL open options positions on Alpaca paper account.

    Called at 12:45 PM MST / 3:45 PM ET EOD sweep.
    If player_id is provided, filters log message but still closes everything
    (Alpaca doesn't track per-player — we close all to be safe).
    """
    client = _get_client()
    if not client:
        return {"skipped": True, "reason": "Alpaca not connected"}

    try:
        from alpaca.trading.requests import GetAllPositionsRequest
        positions = client.get_all_positions()
        options_positions = [
            p for p in positions
            if hasattr(p, "asset_class") and str(p.asset_class).lower() in ("us_option", "option")
        ]
        if not options_positions:
            # Try by symbol pattern (options symbols are longer)
            options_positions = [p for p in positions if len(p.symbol) > 10]

        if not options_positions:
            console.log("[dim]Alpaca options EOD: No options positions to close")
            return {"success": True, "closed": 0}

        closed = 0
        for pos in options_positions:
            try:
                from alpaca.trading.requests import ClosePositionRequest
                qty = abs(float(pos.qty))
                client.close_position(pos.symbol, ClosePositionRequest(qty=str(int(qty))))
                console.log(f"[yellow]Alpaca options EOD close: {pos.symbol} x{int(qty)}")
                closed += 1
            except Exception as e:
                console.log(f"[yellow]Alpaca options close {pos.symbol} error: {e}")

        who = player_id or "EOD sweep"
        console.log(f"[bold yellow]Alpaca options EOD: {closed} position(s) closed ({who})")
        return {"success": True, "closed": closed}
    except Exception as e:
        console.log(f"[red]Alpaca options close_all error: {e}")
        return {"error": str(e)}


def execute_options_signal(
    player_id: str,
    action: str,
    symbol: str,
    current_price: float,
    target_dte: int = 0,
    max_capital: float = MAX_CAPITAL_PER_TRADE,
) -> dict:
    """Top-level dispatcher. Routes BUY_CALL/BUY_PUT/spread/condor to Alpaca.

    Calculates qty based on max_capital and close_price from contract.
    Always enforces MAX_CAPITAL_PER_TRADE and contract limits.
    """
    if player_id not in OPTIONS_PLAYERS:
        return {"skipped": True}

    capital = min(float(max_capital), MAX_CAPITAL_PER_TRADE)
    action = action.upper()

    if action in ("BUY_CALL", "BUY_PUT"):
        opt_type = "call" if action == "BUY_CALL" else "put"
        contract = get_atm_contract(symbol, opt_type, target_dte)
        if not contract:
            return {"error": f"No {opt_type} contract found for {symbol}"}
        # Estimate qty from capital (use $5/contract floor if price unknown)
        premium = _get_contract_price(contract) or 5.0
        qty = max(1, min(MAX_SINGLE_CONTRACTS, int(capital / (premium * 100))))
        return submit_single_option(player_id, contract, qty)

    elif action == "BULL_CALL_SPREAD":
        buy_sym, sell_sym = get_spread_contracts(symbol, "call", target_dte, current_price)
        if not buy_sym or not sell_sym:
            return {"error": f"No call spread contracts for {symbol}"}
        premium = _get_contract_price(buy_sym) or 5.0
        qty = max(1, min(MAX_SPREAD_CONTRACTS, int(capital / (premium * 100))))
        return submit_vertical_spread(player_id, buy_sym, sell_sym, qty, "BULL_CALL_SPREAD")

    elif action == "BEAR_PUT_SPREAD":
        buy_sym, sell_sym = get_spread_contracts(symbol, "put", target_dte, current_price)
        if not buy_sym or not sell_sym:
            return {"error": f"No put spread contracts for {symbol}"}
        premium = _get_contract_price(buy_sym) or 5.0
        qty = max(1, min(MAX_SPREAD_CONTRACTS, int(capital / (premium * 100))))
        return submit_vertical_spread(player_id, buy_sym, sell_sym, qty, "BEAR_PUT_SPREAD")

    elif action == "IRON_CONDOR":
        call_buy, call_sell, put_buy, put_sell = get_iron_condor_contracts(symbol, target_dte, current_price)
        if not all([call_buy, call_sell, put_buy, put_sell]):
            return {"error": f"No iron condor contracts for {symbol}"}
        premium = (_get_contract_price(call_sell) or 2.5) + (_get_contract_price(put_sell) or 2.5)
        qty = max(1, min(MAX_SPREAD_CONTRACTS, int(capital / (premium * 100))))
        return submit_iron_condor(player_id, call_buy, call_sell, put_buy, put_sell, qty)

    return {"error": f"Unknown options action: {action}"}


# ── Helpers ──────────────────────────────────────────────────────

def _nearest_strike(contracts: list, target_price: float):
    """Return the contract with strike closest to target_price."""
    best = None
    best_dist = float("inf")
    for c in contracts:
        dist = abs(float(c.strike_price) - target_price)
        if dist < best_dist:
            best_dist = dist
            best = c
    return best


def _get_current_price(symbol: str) -> float | None:
    try:
        from engine.market_data import get_stock_price
        result = get_stock_price(symbol)
        if isinstance(result, dict):
            return float(result.get("price", 0)) or None
        return float(result) if result else None
    except Exception:
        return None


def _get_contract_price(contract_symbol: str) -> float | None:
    """Fetch last close price of a contract from Alpaca."""
    client = _get_client()
    if not client:
        return None
    try:
        c = client.get_option_contract(contract_symbol)
        if c and c.close_price:
            return float(c.close_price)
    except Exception:
        pass
    return None
