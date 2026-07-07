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
MAX_SPREAD_CONTRACTS = 10  # Raised 2026-04-22 to support scaleout (4ct)
MAX_CAPITAL_PER_TRADE = 500.0

# Only these players get real Alpaca options execution.
# Add new strategy player_ids here when registering a new strategy.
OPTIONS_PLAYERS = {
    "dayblade-0dte",
    "dayblade-sulu",
    # Strategy Registry players — added 2026-04-22 for bull_spread_v1
    "strategy:bull_spread_v1",
}

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
        key = os.getenv("APCA_API_KEY_ID", "")
        secret = os.getenv("APCA_API_SECRET_KEY", "")
        if not key or not secret:
            console.log("[yellow]Alpaca options: No API keys — skipping real execution")
            return None
        from alpaca.trading.client import TradingClient
        _client = TradingClient(key, secret, paper=True)
        console.log("[green]Alpaca options executor ready (paper)")
    except Exception as e:
        # HM-AA-broad: type+repr enrichment per HM-U posture.
        console.log(f"[red]Alpaca options init error: {type(e).__name__}: {e!r}")
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
        # HM-AA-broad: type+repr enrichment per HM-U posture.
        console.log(f"[yellow]Alpaca options get_atm_contract error: {type(e).__name__}: {e!r}")
    return None


def get_contract_at_strike(
    symbol: str, option_type: str, target_dte: int, target_strike: float
) -> str | None:
    """Fetch the contract symbol closest to a SPECIFIC target strike (not
    ATM). P0-A 2026-07-07: needed for wheel/CSP writers that pick a strike
    by OTM% before pricing it (e.g. wheel_strategy.py's 12%-OTM put), as
    opposed to get_atm_contract's current-price-relative matching.

    Returns OCC-format symbol or None. Same contract-fetch/tradable-filter
    shape as get_atm_contract, distance metric swapped to target_strike.
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

        best = None
        best_dist = float("inf")
        for c in contracts:
            if not c.tradable:
                continue
            dist = abs(float(c.strike_price) - target_strike)
            if dist < best_dist:
                best_dist = dist
                best = c

        if best:
            console.log(f"[dim]Alpaca options: Selected {best.symbol} strike={best.strike_price} (target {target_strike}) exp={best.expiration_date}")
            return best.symbol
    except Exception as e:
        console.log(f"[yellow]Alpaca options get_contract_at_strike error: {type(e).__name__}: {e!r}")
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
        # HM-AA-broad: type+repr enrichment per HM-U posture.
        console.log(f"[yellow]Alpaca options get_spread_contracts error: {type(e).__name__}: {e!r}")
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
        # HM-AA-broad: type+repr enrichment per HM-U posture.
        console.log(f"[yellow]Alpaca options get_iron_condor_contracts error: {type(e).__name__}: {e!r}")
    return None, None, None, None


# HM-AC-Option-A (2026-05-05): pre-flight buying-power check helpers.
# Defense-in-depth pairing with HM-AC-Option-B (commit 19c6746) — Option B
# fixes the architecture asymmetry (MLEG opens vs single-leg closes); Option A
# stops the noise generically by short-circuiting submits before Alpaca rejects
# them with insufficient buying power. Fail-open: any error reading buying_power
# returns ok=True so the existing exception handler still catches real rejects.
# Investigation: docs/HM-AC_BUYING_POWER_INVESTIGATION_2026-05-05.md.
def _occ_strike(occ_symbol: str) -> float:
    """Extract strike price from an OCC symbol (last 8 chars × 0.001).

    OCC format: 'SPY260515P00718000' → strike = 00718000 / 1000 = 718.0
    Returns 0.0 on parse failure (caller should treat as 'unknown').
    """
    try:
        return int(occ_symbol[-8:]) / 1000.0
    except Exception:
        return 0.0


def _preflight_buying_power(client, required_bp: float, label: str = "") -> dict:
    """Pre-flight buying-power check. Returns ok=True or skipped=True dict.

    Required ≤ 0.95 × available_bp (95% of available; leaves 5% headroom).
    Fail-open on any error reading the account — the real submit's exception
    handler still catches actual broker rejects via HM-AA-enriched logging.

    HM-AC-Option-A: defense-in-depth. Stops noise at the call site rather than
    letting every submit hit Alpaca, get rejected, and emit an APIError.
    """
    try:
        account = client.get_account()
        available = float(getattr(account, "options_buying_power", None)
                          or account.buying_power)
        if required_bp > 0.95 * available:
            return {
                "skipped": True,
                "reason": (
                    f"pre-flight ({label}): ${required_bp:,.0f} required > "
                    f"${0.95 * available:,.0f} (95% of ${available:,.0f} available)"
                ),
            }
        return {"ok": True}
    except Exception as e:
        # Fail-open — don't block on pre-flight error. Real submit will catch it.
        console.log(f"[yellow]pre-flight BP check error ({label}): {type(e).__name__}: {e!r}")
        return {"ok": True}


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

    # HM-AC-Option-A: pre-flight BP check. SELL of a put/call without held
    # position becomes a cash-secured short → strike × 100 × qty collateral.
    # BUY pays premium × 100 × qty (unknown without quote; conservative cap
    # at qty × $5000 = ~$50/contract worst case). Worst case wins.
    if side == "sell":
        required_bp = _occ_strike(contract_symbol) * 100 * qty
    else:
        required_bp = qty * 5000.0
    pf = _preflight_buying_power(client, required_bp, label=f"single-{side}")
    if pf.get("skipped"):
        return pf

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
        # HM-V: success-side NTFY (first occurrence per type per day).
        try:
            from engine.alert_channels import send_alert, AlertLevel
            send_alert(
                message=f"Alpaca fill: single {side.upper()} {qty}x {contract_symbol} ({player_id}) order={order.id}",
                level=AlertLevel.INFO,
                alert_type=f"hm-v-single-{side}",
                rate_limit_secs=86400,
            )
        except Exception:
            pass
        return {"success": True, "order_id": str(order.id), "symbol": contract_symbol, "qty": qty}
    except Exception as e:
        # HM-AA: enrich error log with exception type + repr (was just str(e),
        # often empty — see 08:56:21/23 logs 2026-05-05 with empty bodies).
        # type+repr surfaces the actual exception class in one log line.
        console.log(f"[yellow]Alpaca options submit_single error: {type(e).__name__}: {e!r}")
        # HM-U: NTFY first occurrence per error class per day (architecture-class broker-submit path).
        try:
            from engine.alert_channels import send_alert, AlertLevel
            send_alert(
                message=f"submit_single_option {type(e).__name__}: {e!r}",
                level=AlertLevel.WARNING,
                alert_type=f"hm-u-submit_single-{type(e).__name__}",
                rate_limit_secs=86400,
            )
        except Exception:
            pass
        return {"error": str(e), "error_type": type(e).__name__, "error_repr": repr(e)}


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

    # HM-AC-Option-A: pre-flight BP check. Vertical spread max-loss =
    # |strike_buy - strike_sell| × 100 × qty (defined-risk). Conservative
    # because Alpaca's actual margin requirement is usually slightly less
    # for credit spreads (max_loss − credit). Better to over-estimate.
    width_dollars = abs(_occ_strike(buy_symbol) - _occ_strike(sell_symbol)) * 100
    required_bp = width_dollars * qty
    pf = _preflight_buying_power(client, required_bp, label=f"spread-open-{strategy}")
    if pf.get("skipped"):
        return pf

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
                    side=OrderSide.BUY, position_intent=PositionIntent.BUY_TO_OPEN,  # HM-Z: was BTO (AttributeError; alpaca-py uses full names)
                ),
                OptionLegRequest(
                    symbol=sell_symbol, ratio_qty=1,
                    side=OrderSide.SELL, position_intent=PositionIntent.SELL_TO_OPEN,  # HM-Z: was STO
                ),
            ],
        ))
        console.log(f"[bold cyan]Alpaca {strategy} {qty}x — {player_id} order={order.id}")
        # HM-V: success-side NTFY for spread open (first per strategy per day).
        try:
            from engine.alert_channels import send_alert, AlertLevel
            send_alert(
                message=f"Alpaca spread open: {strategy} {qty}x ({player_id}) order={order.id}",
                level=AlertLevel.INFO,
                alert_type=f"hm-v-spread-open-{strategy}",
                rate_limit_secs=86400,
            )
        except Exception:
            pass
        return {"success": True, "order_id": str(order.id), "strategy": strategy, "qty": qty}
    except Exception as e:
        # HM-AA-extension: enrich error log + return dict per HM-U posture (CLAUDE.md).
        # This is the original BTO incident site (logged as alpaca_options.py:297
        # pre-HM-Z; line drifted to ~300 after the inline # HM-Z: comments landed).
        # Same Shape B as line 254 (HM-AA narrow-strict, commit a9d0649).
        console.log(f"[yellow]Alpaca options submit_spread error: {type(e).__name__}: {e!r}")
        # HM-U: NTFY first occurrence per error class per day (architecture-class broker-submit path).
        try:
            from engine.alert_channels import send_alert, AlertLevel
            send_alert(
                message=f"submit_vertical_spread {type(e).__name__}: {e!r}",
                level=AlertLevel.WARNING,
                alert_type=f"hm-u-submit_spread-{type(e).__name__}",
                rate_limit_secs=86400,
            )
        except Exception:
            pass
        return {"error": str(e), "error_type": type(e).__name__, "error_repr": repr(e)}


# HM-AC-Option-B (2026-05-05): close-side mirror of submit_vertical_spread.
# Closes a defined-risk vertical spread atomically as a single MLEG order
# instead of two single-leg submits. Single-leg closes fail because Alpaca
# interprets a SELL on a put-leg of an open MLEG spread as opening a fresh
# SHORT-PUT (cash-secured put), requiring strike × 100 × qty collateral
# rather than the much smaller defined-risk margin Alpaca already holds.
# Investigation: docs/HM-AC_BUYING_POWER_INVESTIGATION_2026-05-05.md.
def close_vertical_spread(
    player_id: str, long_symbol: str, short_symbol: str, qty: int, strategy: str
) -> dict:
    """Close a defined-risk vertical spread as a multi-leg order.

    Mirror of submit_vertical_spread for closes. Atomic — both legs in one
    MLEG order so Alpaca recognizes it as closing the existing spread, not
    opening a new naked-short.

    Bull call spread close: long_symbol = lower-strike call (BTO entry → STC),
                            short_symbol = higher-strike call (STO entry → BTC).
    Bull put spread close:  long_symbol = lower-strike put (BTO entry → STC),
                            short_symbol = higher-strike put (STO entry → BTC).
    Bear put spread close:  long_symbol = higher-strike put (BTO entry → STC),
                            short_symbol = lower-strike put (STO entry → BTC).

    HM-AC-Option-B (2026-05-05): closes spreads via MLEG order_class with
    SELL_TO_CLOSE on the long leg and BUY_TO_CLOSE on the short leg. Replaces
    the per-leg single-leg close path that Alpaca rejected with insufficient
    cash-secured-put collateral.
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
                # HM-AC-Option-B: SELL_TO_CLOSE the long leg (was BTO at open).
                OptionLegRequest(
                    symbol=long_symbol, ratio_qty=1,
                    side=OrderSide.SELL, position_intent=PositionIntent.SELL_TO_CLOSE,
                ),
                # HM-AC-Option-B: BUY_TO_CLOSE the short leg (was STO at open).
                OptionLegRequest(
                    symbol=short_symbol, ratio_qty=1,
                    side=OrderSide.BUY, position_intent=PositionIntent.BUY_TO_CLOSE,
                ),
            ],
        ))
        console.log(f"[bold cyan]Alpaca CLOSE {strategy} {qty}x — {player_id} order={order.id}")
        # HM-V: success-side NTFY for spread close (first per strategy per day).
        # Verifies HM-AC Option B's MLEG close path (commit 19c6746) when first
        # exit_manager close hits — closes the verification gap from Phase 5 of
        # that commit's session.
        try:
            from engine.alert_channels import send_alert, AlertLevel
            send_alert(
                message=f"Alpaca spread CLOSE: {strategy} {qty}x ({player_id}) order={order.id}",
                level=AlertLevel.INFO,
                alert_type=f"hm-v-spread-close-{strategy}",
                rate_limit_secs=86400,
            )
        except Exception:
            pass
        return {"success": True, "order_id": str(order.id), "strategy": strategy, "qty": qty}
    except Exception as e:
        # HM-AA / HM-U: enriched error log + NTFY (architecture-class broker-submit path).
        console.log(f"[yellow]Alpaca options close_vertical_spread error: {type(e).__name__}: {e!r}")
        try:
            from engine.alert_channels import send_alert, AlertLevel
            send_alert(
                message=f"close_vertical_spread {strategy} ({player_id}) {type(e).__name__}: {e!r}",
                level=AlertLevel.WARNING,
                alert_type=f"hm-u-close_vertical_spread-{type(e).__name__}",
                rate_limit_secs=86400,
            )
        except Exception:
            pass
        return {"error": str(e), "error_type": type(e).__name__, "error_repr": repr(e)}


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

    # HM-AC-Option-A: pre-flight BP check. Iron condor max-loss =
    # max(call_wing_width, put_wing_width) × 100 × qty (only one wing can
    # max out — defined-risk on both sides).
    call_width = abs(_occ_strike(call_buy) - _occ_strike(call_sell)) * 100
    put_width = abs(_occ_strike(put_buy) - _occ_strike(put_sell)) * 100
    required_bp = max(call_width, put_width) * qty
    pf = _preflight_buying_power(client, required_bp, label="iron-condor")
    if pf.get("skipped"):
        return pf

    try:
        from alpaca.trading.requests import MarketOrderRequest, OptionLegRequest
        from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass, PositionIntent
        order = client.submit_order(MarketOrderRequest(
            qty=qty,
            time_in_force=TimeInForce.DAY,
            order_class=OrderClass.MLEG,
            legs=[
                OptionLegRequest(symbol=call_buy,  ratio_qty=1, side=OrderSide.BUY,  position_intent=PositionIntent.BUY_TO_OPEN),   # HM-Z: was BTO
                OptionLegRequest(symbol=call_sell, ratio_qty=1, side=OrderSide.SELL, position_intent=PositionIntent.SELL_TO_OPEN),  # HM-Z: was STO
                OptionLegRequest(symbol=put_buy,   ratio_qty=1, side=OrderSide.BUY,  position_intent=PositionIntent.BUY_TO_OPEN),   # HM-Z: was BTO
                OptionLegRequest(symbol=put_sell,  ratio_qty=1, side=OrderSide.SELL, position_intent=PositionIntent.SELL_TO_OPEN),  # HM-Z: was STO
            ],
        ))
        console.log(f"[bold cyan]Alpaca IRON_CONDOR {qty}x {call_buy[:3]} — {player_id} order={order.id}")
        # HM-V: success-side NTFY for iron condor open (first per day).
        try:
            from engine.alert_channels import send_alert, AlertLevel
            send_alert(
                message=f"Alpaca iron condor: {qty}x {call_buy[:3]} ({player_id}) order={order.id}",
                level=AlertLevel.INFO,
                alert_type="hm-v-ic-open",
                rate_limit_secs=86400,
            )
        except Exception:
            pass
        return {"success": True, "order_id": str(order.id), "strategy": "IRON_CONDOR", "qty": qty}
    except Exception as e:
        # HM-U: enrich + NTFY first occurrence per error class per day (architecture-class broker-submit).
        console.log(f"[yellow]Alpaca options submit_iron_condor error: {type(e).__name__}: {e!r}")
        try:
            from engine.alert_channels import send_alert, AlertLevel
            send_alert(
                message=f"submit_iron_condor {type(e).__name__}: {e!r}",
                level=AlertLevel.WARNING,
                alert_type=f"hm-u-submit_iron_condor-{type(e).__name__}",
                rate_limit_secs=86400,
            )
        except Exception:
            pass
        return {"error": str(e), "error_type": type(e).__name__, "error_repr": repr(e)}


def close_options_position(player_id: str, contract_symbol: str, qty: int) -> dict:
    """Close (sell to close) a specific options position."""
    return submit_single_option(player_id, contract_symbol, qty, side="sell")


def close_all_options(player_id: str | None = None) -> dict:
    """Close ALL open options positions on Alpaca paper account.

    Called at 12:45 PM MST / 3:45 PM ET EOD sweep.
    If player_id is provided, filters log message but still closes everything
    (Alpaca doesn't track per-player — we close all to be safe).
    """
    # HM-AF-α 2026-05-06
    from config import SPREAD_CANNIBALIZATION_GUARD_ENABLED
    if SPREAD_CANNIBALIZATION_GUARD_ENABLED:
        console.log(f"[yellow][HM-AF-α] close_all_options blocked: spread cannibalization guard active (caller={player_id!r})")
        return {"skipped": True, "reason": "HM-AF-α spread cannibalization guard"}
    client = _get_client()
    if not client:
        return {"skipped": True, "reason": "Alpaca not connected"}

    try:
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
        skipped_legs = 0
        for pos in options_positions:
            # HM-AF-β 2026-05-06: skip legs of currently-open spread trades.
            # Fail-closed: any leg-filter error skips this position rather
            # than allowing a potential cannibalizing close through.
            try:
                from engine.options_utils import is_spread_leg
                if is_spread_leg(pos.symbol):
                    console.log(f"[cyan][HM-AF-β] EOD sweep skipping spread leg: {pos.symbol}")
                    skipped_legs += 1
                    continue
            except Exception as e:
                console.log(f"[red][HM-AF-β] leg filter error on {pos.symbol} (failing closed): {type(e).__name__}: {e!r}")
                skipped_legs += 1
                continue

            try:
                from alpaca.trading.requests import ClosePositionRequest
                qty = abs(float(pos.qty))
                client.close_position(pos.symbol, ClosePositionRequest(qty=str(int(qty))))
                console.log(f"[yellow]Alpaca options EOD close: {pos.symbol} x{int(qty)}")
                closed += 1
            except Exception as e:
                # HM-U: NTFY first occurrence per error class per day (per-position close failure).
                console.log(f"[yellow]Alpaca options close {pos.symbol} error: {type(e).__name__}: {e!r}")
                try:
                    from engine.alert_channels import send_alert, AlertLevel
                    send_alert(
                        message=f"close_options_position {pos.symbol} {type(e).__name__}: {e!r}",
                        level=AlertLevel.WARNING,
                        alert_type=f"hm-u-close_position-{type(e).__name__}",
                        rate_limit_secs=86400,
                    )
                except Exception:
                    pass

        who = player_id or "EOD sweep"
        console.log(f"[bold yellow]Alpaca options EOD: {closed} position(s) closed, {skipped_legs} spread leg(s) skipped ({who})")
        # HM-V: success-side NTFY for EOD-sweep aggregate (one per day if anything closed).
        if closed > 0:
            try:
                from engine.alert_channels import send_alert, AlertLevel
                send_alert(
                    message=f"Alpaca EOD sweep: {closed} option position(s) closed ({who})",
                    level=AlertLevel.INFO,
                    alert_type="hm-v-eod-sweep",
                    rate_limit_secs=86400,
                )
            except Exception:
                pass
        return {"success": True, "closed": closed, "skipped_legs": skipped_legs}
    except Exception as e:
        # HM-U: NTFY first occurrence per error class per day (close_all aggregate failure).
        console.log(f"[red]Alpaca options close_all error: {type(e).__name__}: {e!r}")
        try:
            from engine.alert_channels import send_alert, AlertLevel
            send_alert(
                message=f"close_all_options {type(e).__name__}: {e!r}",
                level=AlertLevel.RED_ALERT,
                alert_type=f"hm-u-close_all-{type(e).__name__}",
                rate_limit_secs=86400,
            )
        except Exception:
            pass
        return {"error": str(e), "error_type": type(e).__name__, "error_repr": repr(e)}


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

    HM-MARKET-HOLIDAY-CALENDAR Phase B 2026-05-25: options-spread fwd
    path (bypasses paper_trader). Gate against closed-market fires here.
    """
    # HM-MARKET-HOLIDAY-CALENDAR Phase B gate (third forward path per
    # CLAUDE.md two-book-bridge doctrine — bull_call_spread_v1,
    # bear_put_spread_v1, etc. route through here, NOT paper_trader).
    from engine.market_calendar import market_closed_reason as _mcr
    _r = _mcr()
    if _r is not None:
        return {"error": f"market_closed: {_r}", "skipped": True, "market_closed": True}
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
