"""
Execution Routing Model

This module enforces a strict separation between:

1. AGENTS (signal generators)
   - Produce BUY / SELL / HOLD decisions
   - Include confidence and reasoning
   - Example: super-agent, dalio-metals (Ray), neo-matrix

2. PORTFOLIOS (execution containers)
   - Hold capital
   - Control execution behavior
   - Determined via _resolve_execution_portfolio()

Routing Modes
-------------
Each signal is routed into one of:

- trading:
    - Real execution path
    - Orders forwarded to Alpaca
    - execution_status = "EXECUTED"

- paper:
    - Simulated execution
    - DB updated (positions, trades)
    - No external broker calls
    - execution_status = "SIMULATED"

- tracking:
    - No execution
    - No DB mutation (positions/cash unchanged)
    - Signal is logged only
    - execution_status = "LOG_ONLY"

Special Case: Metals (Physical Holdings)
---------------------------------------
- Portfolio name: "Enterprise Computer" (UI alias: Metals)
- type = "physical"
- execution_mode = "tracking"

Rules:
- NEVER execute trades
- NEVER mutate capital or positions
- ONLY log signals for comparison

Agent Mapping
-------------
- super-agent → Alpaca Paper (trading)
- dalio-metals → Enterprise Computer (tracking)
- neo-matrix → Neo Matrix (trading)
- default → Arena Paper (paper)

Benchmark Model: Anderson vs Ray vs Metals

Roles
-----
- Mr. Anderson:
    Active execution system (agent-driven)
    Executes trades through its assigned portfolio

- Ray (Dalio Strategy):
    Signal-only agent
    Generates BUY / SELL / HOLD decisions
    Does NOT execute trades directly

- Metals (Physical Holdings):
    Tracking-only portfolio
    execution_mode = "tracking"
    type = "physical"
    Never executes trades or mutates state

Model Rules
-----------
- Agents produce signals
- Portfolios determine execution behavior
- Tracking portfolios log signals only (no execution, no mutation)

Comparison Purpose
------------------
Evaluate performance across:
- Active trading (Anderson)
- Strategy signals (Ray)
- Passive holdings (Metals)

Key Guarantee
-------------
Agents NEVER directly execute trades.

All execution decisions are determined by the resolved portfolio route.
- No agent can mutate the Metals portfolio.
- Metals remains a ground-truth benchmark.
"""

from __future__ import annotations
import json
import os
import sqlite3
from datetime import datetime
from rich.console import Console

console = Console()
DB = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)
EQUITY_CURVE_FILE = os.path.join(os.path.dirname(DB), "equity_curve.json")

# Post-sell trade grading callback: set by Arena at init to enable AI self-grading
# Signature: _on_sell_callback(player_id, symbol, entry_price, exit_price, pnl, reasoning)
_on_sell_callback = None

_EXECUTION_PORTFOLIO_BY_PLAYER = {
    "super-agent": "Alpaca Paper",  # KEEP THIS
    "dalio-metals": "Enterprise Computer",
    "neo-matrix": "Neo Matrix",
}


def register_sell_callback(callback):
    """Register a callback to fire after every full SELL (for trade grading)."""
    global _on_sell_callback
    _on_sell_callback = callback


# === ALPACA PAPER TRADING BRIDGE ===
# Forwards DB trades to Alpaca paper account for real execution.
# Only stocks, only AI models, never human portfolios.
_alpaca = None
_alpaca_init_attempted = False


def _get_alpaca():
    """Lazy-init Alpaca bridge (import once, reuse)."""
    global _alpaca, _alpaca_init_attempted
    if _alpaca_init_attempted:
        return _alpaca
    _alpaca_init_attempted = True
    try:
        from engine.alpaca_bridge import alpaca
        if alpaca.client:
            _alpaca = alpaca
            console.log("[green]Paper trader: Alpaca bridge connected — trades will execute on Alpaca")
        else:
            console.log("[yellow]Paper trader: Alpaca not configured — DB-only mode")
    except Exception as e:
        console.log(f"[yellow]Paper trader: Alpaca bridge unavailable ({e})")
    return _alpaca


def _forward_to_alpaca(action: str, player_id: str, symbol: str, qty: float,
                        asset_type: str = "stock"):
    """Forward a trade to Alpaca paper account. Never raises."""
    if asset_type != "stock":
        return  # Only forward stock trades
    bridge = _get_alpaca()
    if not bridge:
        return
    whole_qty = int(qty)
    if whole_qty <= 0:
        return
    try:
        if action == "BUY":
            result = bridge.buy(symbol, whole_qty)
        elif action == "SELL":
            result = bridge.sell(symbol, whole_qty)
        else:
            return
        if result.get("error"):
            console.log(f"[yellow]Alpaca {action} {symbol} failed: {result['error']}")
        else:
            console.log(f"[bold cyan]Alpaca {action} {whole_qty} {symbol} — order {result.get('order_id', 'ok')} ({player_id})")
    except Exception as e:
        console.log(f"[yellow]Alpaca forward error: {e}")


def estimate_option_price(option_type: str, strike_price: float | None,
                          stock_price: float, entry_premium: float,
                          expiry_date: str = None) -> float:
    """Estimate current option value using intrinsic value + time value floor.

    For calls: max(0, stock_price - strike_price)
    For puts:  max(0, strike_price - stock_price)

    Time value floor: options with >3 days to expiry are worth at least 15% of
    entry premium (approximating residual time value).  This prevents false
    stop-losses on near-ATM options that still have significant extrinsic value.

    Falls back to entry_premium if strike_price is unknown.
    """
    if strike_price is None or strike_price <= 0:
        # No strike data — estimate using stock price delta as proxy for option P&L.
        # A rough ATM option moves ~50% (delta) of the underlying's % change.
        # This prevents all null-strike options from showing $0 P&L.
        if entry_premium > 0 and stock_price > 0:
            # Assume entry was ATM, estimate what strike would have been
            assumed_strike = stock_price  # best guess: ATM at current price
            # But we need the stock price at entry. Use entry_premium as proxy:
            # For ATM options, premium ≈ stock_price * 0.03-0.05 for 30 DTE
            # Better approach: use delta ≈ 0.5 for ATM, so option moves ~$0.50 per $1 stock move
            # We don't know entry stock price, so return entry_premium (unchanged)
            return entry_premium
        return entry_premium

    if option_type == "call":
        intrinsic = max(0.0, stock_price - strike_price)
    elif option_type == "put":
        intrinsic = max(0.0, strike_price - stock_price)
    else:
        return entry_premium

    # Add time value floor for options with remaining life.
    # Options retain significant extrinsic value until close to expiry.
    # Approximate: an ATM option with 30 DTE retains ~60-80% of its premium.
    # We use a square-root decay model (theta decays faster near expiry).
    time_value_floor = 0.0
    if expiry_date:
        try:
            days_left = (datetime.strptime(expiry_date, "%Y-%m-%d").date() - datetime.now().date()).days
            if days_left > 0:
                # sqrt decay: time_value ∝ sqrt(days_left / 30)
                # At 30 DTE: floor = 70% of entry premium
                # At 7 DTE:  floor = 34% of entry premium
                # At 1 DTE:  floor = 13% of entry premium
                # At 0 DTE:  floor = 0 (intrinsic only)
                time_value_floor = entry_premium * 0.70 * min((days_left / 30) ** 0.5, 1.0)
        except (ValueError, TypeError):
            pass

    return max(intrinsic, time_value_floor)


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    return c


def _resolve_execution_portfolio(player_id: str) -> dict:
    """Resolve the execution portfolio route for a player.

    Returns a normalized route dict:
      route_mode: trading | paper | tracking
      execution_mode: auto | manual | tracking
      type: trading | paper | physical
    """
    portfolio_name = _EXECUTION_PORTFOLIO_BY_PLAYER.get(player_id)
    if not portfolio_name:
        return {
            "player_id": player_id,
            "portfolio_id": None,
            "portfolio_name": "Arena Paper",
            "execution_mode": "manual",
            "type": "paper",
            "route_mode": "paper",
        }

    conn = _conn()
    try:
        row = conn.execute(
            "SELECT id, name, execution_mode, type FROM portfolios WHERE name=?",
            (portfolio_name,),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        return {
            "player_id": player_id,
            "portfolio_id": None,
            "portfolio_name": portfolio_name,
            "execution_mode": "manual",
            "type": "paper",
            "route_mode": "paper",
        }

    execution_mode = (row[2] or "manual").lower()
    portfolio_type = (row[3] or "paper").lower()
    if execution_mode == "tracking" or portfolio_type == "physical":
        route_mode = "tracking"
    elif execution_mode == "auto":
        route_mode = "trading"
    else:
        route_mode = "paper"

    return {
        "player_id": player_id,
        "portfolio_id": row[0],
        "portfolio_name": row[1],
        "execution_mode": execution_mode,
        "type": portfolio_type,
        "route_mode": route_mode,
    }


def _log_signal_only(player_id: str, action: str, symbol: str, route: dict, reasoning: str,
                     confidence: float) -> dict:
    msg = (
        f"{player_id}: LOG ONLY {action} {symbol} — "
        f"{route['portfolio_name']} is tracking-only"
    )
    console.log(f"[yellow]{msg}")
    _last_rejection[player_id] = "tracking-only portfolio"
    return {
        "action": action,
        "symbol": symbol,
        "player_id": player_id,
        "qty": 0,
        "price": 0,
        "confidence": confidence,
        "reasoning": reasoning,
        "portfolio_name": route["portfolio_name"],
        "execution_mode": route["execution_mode"],
        "portfolio_type": route["type"],
        "route_mode": route["route_mode"],
        "execution_status": "LOG_ONLY",
    }


def _current_season() -> int:
    """Read current season from settings table, default 1."""
    try:
        c = _conn()
        row = c.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        c.close()
        return int(row[0]) if row else 1
    except Exception:
        return 1


def get_portfolio(player_id: str) -> dict:
    conn = _conn()
    row = conn.execute("SELECT cash FROM ai_players WHERE id=?", (player_id,)).fetchone()
    pos = conn.execute(
        "SELECT symbol, qty, avg_price, asset_type, option_type, strike_price, expiry_date, high_watermark "
        "FROM positions WHERE player_id=?", (player_id,)
    ).fetchall()
    conn.close()
    return {
        "cash": row[0] if row else 0,
        "positions": [
            {"symbol": p[0], "qty": p[1], "avg_price": p[2], "asset_type": p[3],
             "option_type": p[4], "strike_price": p[5], "expiry_date": p[6],
             "high_watermark": p[7]}
            for p in pos
        ]
    }


def get_position(player_id: str, symbol: str, asset_type: str = "stock",
                 option_type: str = None) -> dict | None:
    conn = _conn()
    if asset_type == "stock":
        row = conn.execute(
            "SELECT qty, avg_price, strike_price, option_type, opened_at FROM positions "
            "WHERE player_id=? AND symbol=? AND asset_type='stock'",
            (player_id, symbol)
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT qty, avg_price, strike_price, option_type, opened_at FROM positions "
            "WHERE player_id=? AND symbol=? AND option_type=?",
            (player_id, symbol, option_type)
        ).fetchone()
    conn.close()
    if not row:
        return None
    return {"qty": row[0], "avg_price": row[1], "strike_price": row[2], "option_type": row[3], "opened_at": row[4]}


def _is_human_player(player_id: str) -> bool:
    """Check if player is a human benchmark (must never be auto-traded)."""
    if "steve" in player_id.lower() or "webull" in player_id.lower():
        return True
    try:
        conn = _conn()
        row = conn.execute("SELECT is_human FROM ai_players WHERE id=?", (player_id,)).fetchone()
        conn.close()
        return bool(row and row[0])
    except Exception:
        return False


def _detect_ghost_option(player_id: str, symbol: str, price: float,
                          reasoning: str, option_type, expiry_date):
    """Detect when a 'stock' trade is actually an option premium.

    Returns (asset_type, option_type, expiry_date) — possibly reclassified.
    Triggers when price < 10% of live stock price for stocks trading above $50.
    """
    try:
        from engine.market_data import get_stock_price
        live = get_stock_price(symbol)
        live_price = live.get("price", 0)
        if live_price >= 50 and price < live_price * 0.10:
            # Heuristic: infer call/put from reasoning text
            ot = option_type
            if not ot:
                lower = reasoning.lower()
                ot = "put" if "put" in lower else "call"
            # Infer expiry: look for 0DTE cues, else default to today
            exp = expiry_date
            if not exp:
                lower = reasoning.lower()
                if "0dte" in lower or "today" in lower or "same day" in lower or "0 dte" in lower:
                    exp = datetime.now().strftime("%Y-%m-%d")
                else:
                    # Default to end of current week (Friday)
                    from datetime import date, timedelta
                    today = date.today()
                    days_to_friday = (4 - today.weekday()) % 7
                    exp = (today + timedelta(days=days_to_friday)).strftime("%Y-%m-%d")
            console.log(
                f"[bold yellow]GHOST OPTION DETECTED: {player_id} {symbol} @ ${price:.2f} "
                f"(live ${live_price:.2f}) → reclassified as {ot.upper()} option, expiry={exp}"
            )
            return "option", ot, exp
    except Exception:
        pass
    return "stock", option_type, expiry_date


def buy(player_id: str, symbol: str, price: float, asset_type: str = "stock",
        qty: float = None, reasoning: str = "", confidence: float = 0.0,
        option_type: str = None, strike_price: float = None, expiry_date: str = None,
        sources: str = "", timeframe: str = "SWING") -> dict | None:
    # GUARD: Never auto-trade human portfolios
    if _is_human_player(player_id):
        console.log(f"[red]BLOCKED: {player_id} is human — cannot auto-trade")
        return None
    route = _resolve_execution_portfolio(player_id)
    if route["route_mode"] == "tracking":
        return _log_signal_only(player_id, "BUY", symbol, route, reasoning, confidence)

    # === GHOST PROMOTION BLOCKER ===
    # Catch models whose reasoning says "hold/no trade" but action leaked as BUY.
    _GHOST_PHRASES = [
        "no new position will be initiated",
        "outside my operational zone",
        "violates my directives",
        "outside this specified sector",
        "no position",
    ]
    _reasoning_lower = reasoning.lower()
    for _phrase in _GHOST_PHRASES:
        if _phrase in _reasoning_lower:
            console.log(
                f"[red]GHOST PROMOTION BLOCKED: {player_id} {symbol} — "
                f"reasoning contains '{_phrase}'. Trade rejected."
            )
            _last_rejection[player_id] = f"Ghost promotion: reasoning contains '{_phrase}'"
            return None
    # Also block if reasoning contains a standalone HOLD directive
    import re as _re
    if _re.search(r'\bHOLD\b', reasoning):
        console.log(
            f"[red]GHOST PROMOTION BLOCKED: {player_id} {symbol} — "
            f"reasoning contains HOLD directive. Trade rejected."
        )
        _last_rejection[player_id] = "Ghost promotion: HOLD directive in reasoning"
        return None

    # === GHOST OPTION DETECTION ===
    # If a model is trading what looks like an option premium as a "stock", reclassify it.
    if asset_type == "stock" and option_type is None:
        asset_type, option_type, expiry_date = _detect_ghost_option(
            player_id, symbol, price, reasoning, option_type, expiry_date
        )

    # === UNIVERSAL GUARDRAIL GATE (Strategy Lab S4 fixes) ===
    # These checks run BEFORE any trade execution, cannot be overridden.
    try:
        from engine.risk_manager import RiskManager
        _rm = RiskManager()

        # 1. Daily trade limit (per-model + bear market aware)
        _daily_limit = _rm.get_effective_daily_limit(player_id)
        _today = datetime.now().strftime("%Y-%m-%d")
        _tc = _conn()
        _trade_count = _tc.execute(
            "SELECT COUNT(*) FROM trades WHERE player_id=? AND date(executed_at)=?",
            (player_id, _today)
        ).fetchone()[0]
        _tc.close()
        if _trade_count >= _daily_limit:
            console.log(f"[red]MAX_TRADES_REACHED: {player_id} at {_trade_count}/{_daily_limit} trades today — REJECTED {symbol}")
            _last_rejection[player_id] = "Daily trade limit reached"
            return None

        # 2. Universal minimum conviction
        _min_conv = max(
            _rm.UNIVERSAL_MIN_CONVICTION,
            _rm.get_model_guardrail(player_id, "min_conviction") or 0,
        )
        if _rm.is_bear_market() and player_id not in ("dayblade-sulu", "navigator", "dalio-metals"):
            # Exempt: Sulu (day trader), Chekov (convergence scanner), Dalio (All Weather — trades in all regimes)
            _min_conv = max(_min_conv, _rm.BEAR_MIN_CONVICTION)
        if confidence < _min_conv:
            console.log(f"[red]LOW_CONVICTION: {player_id} {symbol} conf={confidence:.0%} < {_min_conv:.0%} — REJECTED")
            _last_rejection[player_id] = f"Below confidence threshold ({confidence:.0%} < {_min_conv:.0%})"
            return None

        # 3. V2: Conviction-scaled stop-loss — wider stops for high conviction
        if "stop" not in reasoning.lower() and "sl" not in reasoning.lower():
            _model_sl = _rm.get_model_guardrail(player_id, "stop_loss_pct")
            _sl_pct = _model_sl if _model_sl else _rm.get_stop_loss_pct(confidence)
            reasoning = f"{reasoning} [AUTO-STOP: -{_sl_pct:.0%} from entry]"

        # 4. V3: Per-model position limit (fewer picks, bigger bets)
        _portfolio = get_portfolio(player_id)
        _unique_syms = set(p["symbol"] for p in _portfolio["positions"])
        _model_max = _rm.MAX_POSITIONS_PER_MODEL.get(
            player_id, _rm.MAX_POSITIONS_PER_MODEL["default"])
        if _rm.is_bear_market() and player_id not in ("dayblade-sulu", "navigator", "dalio-metals"):
            _model_max = min(_model_max, _rm.BEAR_MAX_POSITIONS)
        if len(_unique_syms) >= _model_max and symbol not in _unique_syms:
            console.log(f"[red]MAX_POSITIONS_REACHED: {player_id} already has "
                        f"{len(_unique_syms)}/{_model_max} positions — REJECTED {symbol}")
            _last_rejection[player_id] = f"Maximum positions reached ({len(_unique_syms)}/{_model_max})"
            return None

        # 5. V3: Quality gate (stock must pass 3/5 fundamental checks)
        # Exempt: capitol-trades follows Congress members, not AI analysis
        _QUALITY_GATE_EXEMPT = {"capitol-trades", "navigator", "steve-webull"}
        if asset_type == "stock" and player_id not in _QUALITY_GATE_EXEMPT:
            try:
                from engine.quality_gate import passes_quality_gate
                _indicators = {}
                try:
                    from engine.market_data import get_technical_indicators
                    _indicators = get_technical_indicators(symbol) or {}
                except Exception:
                    pass
                _passes, _qscore, _qdetails = passes_quality_gate(symbol, _indicators)
                if not _passes:
                    console.log(f"[red]QUALITY_GATE_FAILED: {player_id} {symbol} scored "
                                f"{_qscore}/5 — {', '.join(_qdetails[:3])}")
                    _last_rejection[player_id] = f"Failed quality gate ({_qscore}/5)"
                    return None
            except ImportError:
                pass

        # 6. Warp 9: Scanner validation — prefer scanner picks over AI guesses
        # Exempt: capitol-trades uses Congress trade data, not AI universe scanner
        _SCANNER_EXEMPT = {"capitol-trades", "steve-webull"}
        if asset_type == "stock" and player_id not in _SCANNER_EXEMPT:
            try:
                from engine.strategies import get_todays_signals
                from engine.universe_scanner import get_latest_universe_scan
                _conv = get_todays_signals()
                _univ = get_latest_universe_scan()
                _conv_tickers = [s["ticker"] for s in (_conv or [])]
                _univ_tickers = [s["ticker"] for s in (_univ or {}).get("results", [])[:50]]
                # Also allow existing watchlist stocks
                from config import WATCH_STOCKS
                _watchlist = set(WATCH_STOCKS)

                if symbol in _conv_tickers:
                    pass  # Best: convergence signal — full green light
                elif symbol in _univ_tickers:
                    if confidence < 0.70:
                        console.log(f"[yellow]SCANNER_FILTER: {player_id} {symbol} in universe but "
                                    f"conv={confidence:.0%} < 70% — REJECTED")
                        _last_rejection[player_id] = "In scanner universe but confidence too low (need 70%)"
                        return None
                elif symbol in _watchlist:
                    pass  # Watchlist stocks always allowed
                else:
                    if confidence < 0.90:
                        console.log(f"[yellow]SCANNER_FILTER: {player_id} {symbol} NOT in scanner "
                                    f"results — need 0.90+ conv (got {confidence:.0%})")
                        _last_rejection[player_id] = "Not in scanner results (need 90%+ confidence)"
                        return None
            except ImportError:
                pass
            except Exception:
                pass  # Scanner not populated yet — allow trade

    except ImportError:
        pass  # First run before risk_manager exists
    except Exception as _e:
        console.log(f"[yellow]Guardrail check warning: {_e}")

    # GUARD: Options only during regular market hours (9:30 AM - 4 PM ET)
    # AND not in first/last 30 min (avoid wide spreads at open/close)
    if asset_type == "option" or option_type:
        try:
            from engine.risk_manager import RiskManager
            session = RiskManager.is_market_hours()
            if session in ("pre_market", "post_market") or not session:
                console.log(f"[yellow]BLOCKED: {player_id} {symbol} — Options only during market hours")
                _last_rejection[player_id] = "Options only allowed during market hours (9:30 AM - 4 PM ET)"
                return None
            # Block first 30 min (9:30-10:00) and last 30 min (3:30-4:00 ET)
            import pytz as _pytz
            _et = _pytz.timezone("US/Eastern")
            _now_et = datetime.now(_et)
            _mins = _now_et.hour * 60 + _now_et.minute
            if _mins < 600 or _mins > 930:  # before 10:00 AM or after 3:30 PM ET
                console.log(f"[yellow]BLOCKED: {player_id} {symbol} — No options in first/last 30 min (spreads too wide)")
                _last_rejection[player_id] = "Options blocked: first/last 30 min of session (wide spreads)"
                return None
        except Exception:
            pass

    # VIX CIRCUIT BREAKER: Pause all new entries when VIX > 30
    _vix = _get_vix_cached()
    if _vix and _vix > _VIX_CIRCUIT_BREAKER and player_id not in ("dayblade-sulu", "navigator"):
        console.log(f"[bold red]VIX CIRCUIT BREAKER: VIX={_vix:.1f} > {_VIX_CIRCUIT_BREAKER} — {player_id} blocked (reduce sizes in high vol)")
        _last_rejection[player_id] = f"VIX circuit breaker ({_vix:.1f} > {_VIX_CIRCUIT_BREAKER})"
        return None

    # DRAWDOWN PAUSE: Block new entries if player is down 15%+ from peak portfolio value
    try:
        _pf_check = get_portfolio(player_id)
        _pos_val = sum(abs(p["qty"]) * p["avg_price"] for p in _pf_check["positions"])
        _cur_val = _pf_check["cash"] + _pos_val
        _peak_conn = _conn()
        _peak_row = _peak_conn.execute(
            "SELECT MAX(total_value) FROM portfolio_history WHERE player_id=?", (player_id,)
        ).fetchone()
        _peak_conn.close()
        _peak = _peak_row[0] if _peak_row and _peak_row[0] else None
        if _peak and _peak > 0 and (_peak - _cur_val) / _peak >= 0.15:
            console.log(f"[yellow]DRAWDOWN PAUSE: {player_id} at {((_peak-_cur_val)/_peak*100):.1f}% drawdown — no new entries until recovery")
            _last_rejection[player_id] = f"Drawdown pause: {((_peak-_cur_val)/_peak*100):.1f}% below peak (threshold 15%)"
            return None
    except Exception:
        pass

    # GLOBAL OPTION RISK: Max 6 open option positions across all models combined
    if asset_type == "option" or option_type:
        try:
            _opt_conn = _conn()
            _total_opts = _opt_conn.execute(
                "SELECT COUNT(*) FROM positions WHERE asset_type='option'"
            ).fetchone()[0]
            _opt_conn.close()
            if _total_opts >= 6:
                console.log(f"[yellow]OPTION LIMIT: {_total_opts}/6 global option positions — {player_id} {symbol} blocked")
                _last_rejection[player_id] = f"Global option limit reached ({_total_opts}/6 positions)"
                return None
        except Exception:
            pass

        # CORRELATION CHECK: Block if 3+ models already hold this ticker (options)
        try:
            _corr_conn = _conn()
            _holders = _corr_conn.execute(
                "SELECT COUNT(DISTINCT player_id) FROM positions WHERE symbol=? AND asset_type='option'",
                (symbol,)
            ).fetchone()[0]
            _corr_conn.close()
            if _holders >= 3:
                console.log(f"[yellow]CORRELATION: {_holders} models already hold {symbol} options — blocking {player_id}")
                _last_rejection[player_id] = f"Correlation block: {_holders} models already hold {symbol} options"
                return None
        except Exception:
            pass

    portfolio = get_portfolio(player_id)
    cash = portfolio["cash"]

    # GUARD: Options exposure cap — max 20% of account value in options
    if asset_type == "option" or option_type:
        total_options_value = sum(
            p["qty"] * p["avg_price"]
            for p in portfolio["positions"]
            if p.get("asset_type") == "option"
        )
        positions_value = sum(
            p["qty"] * p["avg_price"] for p in portfolio["positions"]
        )
        account_value = cash + positions_value
        proposed_cost = (qty if qty else round((cash * 0.10) / price, 4)) * price
        if account_value > 0 and (total_options_value + proposed_cost) / account_value > 0.20:
            console.log(f"[yellow]OPTIONS CAP: 20% max options exposure reached. "
                        f"{player_id} {symbol} blocked — options ${total_options_value:.0f} + "
                        f"${proposed_cost:.0f} would exceed 20% of ${account_value:.0f}")
            _last_rejection[player_id] = f"Options exposure cap reached (20% of portfolio)"
            return None

    # SWING TRADE RULES
    is_swing = timeframe.upper() in ("SWING", "SWING_3D", "SWING_5D", "SWING_15D")
    if is_swing:
        # Require stop + target embedded in reasoning
        import re as _re2
        has_stop = bool(_re2.search(r'\[STOP[:\s]', reasoning, _re2.IGNORECASE)
                        or "stop" in reasoning.lower())
        has_target = bool(_re2.search(r'\[TARGET[:\s]', reasoning, _re2.IGNORECASE)
                          or "target" in reasoning.lower())
        if not (has_stop and has_target):
            console.log(f"[red]SWING TRADE BLOCKED: {player_id} {symbol} — "
                        "swing trades require explicit stop AND target in reasoning.")
            _last_rejection[player_id] = "Swing trade missing stop or target"
            return None

    if qty is None:
        if is_swing:
            alloc_pct = 0.25
        elif asset_type == "option":
            # Options: Kelly-based sizing, max 2% per single option (5% for spreads)
            kelly_pct = get_kelly_fraction(player_id)
            alloc_pct = min(kelly_pct, 0.02)  # max 2% per single option
        else:
            # Stocks: half-Kelly, capped at 10%
            kelly_pct = get_kelly_fraction(player_id)
            alloc_pct = min(kelly_pct, 0.10)
            alloc_pct, _alloc_reasons = _target_weight_adjustment(
                player_id, symbol, portfolio, alloc_pct, price, confidence
            )
            if _alloc_reasons:
                console.log(f"[cyan]TARGET WEIGHT: {player_id} {symbol} sizing adjusted for {', '.join(_alloc_reasons)}")
        _allocation_policy = get_capital_allocation_policy(player_id)
        if _allocation_policy["multiplier"] != 1.0:
            _base_alloc = alloc_pct
            alloc_pct = max(0.02, alloc_pct * _allocation_policy["multiplier"])
            console.log(
                f"[cyan]ALLOCATION {player_id}: {_allocation_policy['tier']} "
                f"{_base_alloc:.2%}->{alloc_pct:.2%} "
                f"(cycle_ret={_allocation_policy['return_pct']:.2f}% "
                f"win_rate={_allocation_policy['win_rate']:.1f}% "
                f"trades={_allocation_policy['trade_count']})"
            )
        qty = round((cash * alloc_pct) / price, 4)
    if qty <= 0:
        return None

    # Swing trade 25% position size cap (absolute)
    if is_swing:
        max_swing_cost = cash * 0.25
        if qty * price > max_swing_cost:
            qty = round(max_swing_cost / price, 4)
            console.log(f"[cyan]SWING CAP: {player_id} {symbol} capped at 25% (${max_swing_cost:.0f})")

    # Option position size cap: 2% of account per single option
    if asset_type == "option" and not is_swing:
        _acct_value = cash + sum(p["qty"] * p["avg_price"] for p in portfolio["positions"])
        max_opt_cost = _acct_value * 0.02
        if qty * price > max_opt_cost:
            qty = round(max_opt_cost / price, 4)
            console.log(f"[cyan]OPTION CAP: {player_id} {symbol} capped at 2% (${max_opt_cost:.0f})")

    # 8/21 MA Cross Regime: scale position size by trend modifier
    # DayBlade (dayblade-sulu) is exempt — it uses its own intraday sizing
    if player_id != "dayblade-sulu" and asset_type == "stock":
        try:
            from engine.regime_ma import get_ma_cross_size_modifier
            _ma_modifier = get_ma_cross_size_modifier()
            if _ma_modifier < 1.0:
                _orig_qty = qty
                qty = round(qty * _ma_modifier, 4)
                console.log(
                    f"[cyan]8/21 Regime modifier {_ma_modifier:.0%}: "
                    f"{player_id} {symbol} qty {_orig_qty}→{qty}"
                )
        except Exception:
            pass

    cost = round(qty * price, 2)
    if cost > cash:
        console.log(f"[red]{player_id}: Not enough cash for {symbol}")
        _last_rejection[player_id] = f"Insufficient buying power (need ${cost:.0f}, have ${cash:.0f})"
        return None

    conn = _conn()
    conn.execute("UPDATE ai_players SET cash=? WHERE id=?", (round(cash - cost, 2), player_id))

    if asset_type == "stock":
        ex = conn.execute(
            "SELECT qty, avg_price FROM positions WHERE player_id=? AND symbol=? AND asset_type='stock'",
            (player_id, symbol)
        ).fetchone()
        if ex:
            nq = ex[0] + qty
            na = round(((ex[0] * ex[1]) + cost) / nq, 4)
            conn.execute(
                "UPDATE positions SET qty=?, avg_price=? WHERE player_id=? AND symbol=? AND asset_type='stock'",
                (nq, na, player_id, symbol)
            )
        else:
            conn.execute(
                "INSERT INTO positions(player_id, symbol, qty, avg_price, asset_type) VALUES(?,?,?,?,?)",
                (player_id, symbol, qty, price, "stock")
            )
    else:
        conn.execute(
            "INSERT INTO positions(player_id, symbol, qty, avg_price, asset_type, option_type, strike_price, expiry_date) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (player_id, symbol, qty, price, "option", option_type, strike_price, expiry_date)
        )

    conn.execute(
        "INSERT INTO trades(player_id, symbol, action, qty, price, asset_type, option_type, "
        "strike_price, expiry_date, reasoning, confidence, season, sources, timeframe) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (player_id, symbol, "BUY", qty, price, asset_type, option_type,
         strike_price, expiry_date, reasoning, confidence, _current_season(), sources, timeframe)
    )
    conn.commit()
    conn.close()
    console.log(f"[green]{player_id}: BUY {qty} {symbol} @ ${price:.2f}")

    # Forward to Alpaca paper trading (non-blocking)
    if route["route_mode"] == "trading":
        _forward_to_alpaca("BUY", player_id, symbol, qty, asset_type)

    return {
        "action": "BUY",
        "symbol": symbol,
        "qty": qty,
        "price": price,
        "player_id": player_id,
        "portfolio_name": route["portfolio_name"],
        "execution_mode": route["execution_mode"],
        "portfolio_type": route["type"],
        "route_mode": route["route_mode"],
        "execution_status": "EXECUTED" if route["route_mode"] == "trading" else "SIMULATED",
    }


def _get_buy_timeframe(player_id: str, symbol: str) -> str:
    """Look up the timeframe tag on the original BUY trade for this position."""
    try:
        conn = _conn()
        row = conn.execute(
            "SELECT timeframe FROM trades WHERE player_id=? AND symbol=? AND action='BUY' "
            "ORDER BY executed_at DESC LIMIT 1",
            (player_id, symbol),
        ).fetchone()
        conn.close()
        return (row["timeframe"] or "").upper() if row else ""
    except Exception:
        return ""


def _check_min_hold(player_id: str, symbol: str, pos: dict, reasoning: str) -> bool:
    """Return True if the position can be sold (held >= min hold or stop-loss).

    - All trades: 24h minimum hold
    - SWING trades: 3-day (72h) minimum hold
    Stop-loss/target sells always bypass the hold timer.
    """
    opened_at = pos.get("opened_at")
    if not opened_at:
        return True
    try:
        opened = datetime.strptime(opened_at, "%Y-%m-%d %H:%M:%S")
        hours_held = (datetime.now() - opened).total_seconds() / 3600

        is_stop_or_target = any(kw in reasoning.lower() for kw in
                                ("stop", "sl", "expired", "target", "take-profit", "tp"))

        # Swing trades: 3-day minimum unless stop/target
        tf = _get_buy_timeframe(player_id, symbol)
        if tf in ("SWING", "SWING_3D", "SWING_5D", "SWING_15D"):
            min_hours = 72
            if hours_held < min_hours and not is_stop_or_target:
                days_held = hours_held / 24
                console.log(
                    f"[yellow]{player_id}: HOLD {symbol} — SWING trade only held "
                    f"{days_held:.1f}d (min 3d). Reason: {reasoning[:60]}"
                )
                return False

        if hours_held < 24 and not is_stop_or_target:
            console.log(f"[yellow]{player_id}: HOLD {symbol} — only held {hours_held:.1f}h (min 24h). Reason: {reasoning[:60]}")
            return False
    except (ValueError, TypeError):
        pass
    return True


def sell(player_id: str, symbol: str, price: float, asset_type: str = "stock",
         reasoning: str = "", confidence: float = 0.0,
         option_type: str = None) -> dict | None:
    # GUARD: Never auto-trade human portfolios
    if _is_human_player(player_id):
        console.log(f"[red]BLOCKED: {player_id} is human — cannot auto-trade")
        return None
    route = _resolve_execution_portfolio(player_id)
    if route["route_mode"] == "tracking":
        return _log_signal_only(player_id, "SELL", symbol, route, reasoning, confidence)
    pos = get_position(player_id, symbol, asset_type, option_type)
    if not pos:
        console.log(f"[red]{player_id}: No position in {symbol}")
        return None

    # GUARD: Minimum 24h hold period (unless stop-loss)
    if not _check_min_hold(player_id, symbol, pos, reasoning):
        return None

    # For options, estimate current premium using intrinsic value + time value
    # (caller passes stock price — we convert to option value via strike)
    if asset_type == "option" or (asset_type != "stock" and pos.get("asset_type") == "option"):
        ot = option_type or pos.get("option_type")
        price = estimate_option_price(ot, pos.get("strike_price"), price, pos["avg_price"],
                                      pos.get("expiry_date"))

    # GUARD: Refuse to sell options at $0.00 — price was not captured correctly
    if asset_type != "stock" and price < 0.01:
        console.log(f"[bold red]⚠ BLOCKED SELL {symbol} for {player_id}: estimated exit price ${price:.4f} < $0.01 — skipping to protect position")
        return None

    # Short position detection: negative qty means short, covering it is a BUY-to-cover
    is_short = pos["qty"] < 0
    qty = abs(pos["qty"])

    portfolio = get_portfolio(player_id)

    if is_short:
        # Covering a short: return original margin (qty × entry) + P&L
        margin = round(qty * pos["avg_price"], 2)
        pnl = round(qty * (pos["avg_price"] - price), 2)  # profit when price fell
        new_cash = round(portfolio["cash"] + margin + pnl, 2)
        trade_action = "COVER"
    else:
        proceeds = round(qty * price, 2)
        pnl = round(proceeds - (qty * pos["avg_price"]), 2)
        new_cash = round(portfolio["cash"] + proceeds, 2)
        trade_action = "SELL"

    conn = _conn()
    conn.execute("UPDATE ai_players SET cash=? WHERE id=?", (new_cash, player_id))

    if asset_type == "stock":
        conn.execute(
            "DELETE FROM positions WHERE player_id=? AND symbol=? AND asset_type='stock'",
            (player_id, symbol)
        )
    else:
        conn.execute(
            "DELETE FROM positions WHERE player_id=? AND symbol=? AND option_type=?",
            (player_id, symbol, option_type)
        )

    conn.execute(
        "INSERT INTO trades(player_id, symbol, action, qty, price, asset_type, option_type, "
        "reasoning, confidence, entry_price, exit_price, realized_pnl, season) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (player_id, symbol, trade_action, qty, price, asset_type, option_type, reasoning, confidence,
         pos["avg_price"], price, pnl, _current_season())
    )
    conn.commit()
    conn.close()
    console.log(f"[green]{player_id}: {trade_action} {qty} {symbol} @ ${price:.2f} PnL: ${pnl:.2f}")

    # Forward to Alpaca paper trading (non-blocking)
    if route["route_mode"] == "trading":
        _forward_to_alpaca("SELL", player_id, symbol, qty, asset_type)

    # Borg lore: post loss notifications to War Room
    if pnl < 0:
        try:
            from engine.war_room import save_hot_take
            pnl_pct = ((price / pos["avg_price"]) - 1) * 100 if pos["avg_price"] > 0 else 0
            crew_name = player_id
            try:
                from engine.war_room import CREW_NAMES
                crew_name = CREW_NAMES.get(player_id, player_id)
            except Exception:
                pass
            if pnl_pct <= -5:
                msg = (f"🤖 BORG ALERT: {symbol} assimilation complete. "
                       f"{crew_name} lost ${abs(pnl):.2f} ({pnl_pct:.1f}%). "
                       f"The collective grows stronger. We will adapt.")
            else:
                msg = (f"🤖 BORG ALERT: {symbol} has been assimilated. "
                       f"-${abs(pnl):.2f} added to the collective. "
                       f"Resistance was futile.")
            save_hot_take(player_id, symbol, msg)
        except Exception:
            pass

    # Move signal to "watching" for re-entry tracking
    try:
        from engine.signal_tracker import mark_watching
        mark_watching(player_id, symbol, price)
    except Exception:
        pass

    # Fire post-sell trade grading (background)
    if _on_sell_callback:
        try:
            import threading
            threading.Thread(
                target=_on_sell_callback,
                args=(player_id, symbol, pos["avg_price"], price, pnl, reasoning),
                daemon=True,
            ).start()
        except Exception:
            pass

    return {
        "action": "SELL",
        "symbol": symbol,
        "pnl": pnl,
        "player_id": player_id,
        "portfolio_name": route["portfolio_name"],
        "execution_mode": route["execution_mode"],
        "portfolio_type": route["type"],
        "route_mode": route["route_mode"],
        "execution_status": "EXECUTED" if route["route_mode"] == "trading" else "SIMULATED",
    }


def sell_partial(player_id: str, symbol: str, price: float, qty: float,
                 asset_type: str = "stock", reasoning: str = "", confidence: float = 0.0,
                 option_type: str = None) -> dict | None:
    """Sell a partial quantity of a position (for tiered take-profit)."""
    # GUARD: Never auto-trade human portfolios
    if _is_human_player(player_id):
        console.log(f"[red]BLOCKED: {player_id} is human — cannot auto-trade")
        return None
    pos = get_position(player_id, symbol, asset_type, option_type)
    if pos and not _check_min_hold(player_id, symbol, pos, reasoning):
        return None
    if not pos:
        console.log(f"[red]{player_id}: No position in {symbol}")
        return None

    # For options, estimate current premium using intrinsic value + time value
    if asset_type == "option" or (asset_type != "stock" and pos.get("asset_type") == "option"):
        ot = option_type or pos.get("option_type")
        price = estimate_option_price(ot, pos.get("strike_price"), price, pos["avg_price"],
                                      pos.get("expiry_date"))

    # GUARD: Refuse to sell options at $0.00 — price was not captured correctly
    if asset_type != "stock" and price < 0.01:
        console.log(f"[bold red]⚠ BLOCKED SELL {symbol} for {player_id}: estimated exit price ${price:.4f} < $0.01 — skipping to protect position")
        return None

    qty = min(qty, pos["qty"])
    if qty <= 0:
        return None

    proceeds = round(qty * price, 2)
    pnl = round(proceeds - (qty * pos["avg_price"]), 2)
    remaining = round(pos["qty"] - qty, 4)

    portfolio = get_portfolio(player_id)
    new_cash = round(portfolio["cash"] + proceeds, 2)

    conn = _conn()
    conn.execute("UPDATE ai_players SET cash=? WHERE id=?", (new_cash, player_id))

    if remaining <= 0:
        # Close entire position
        if asset_type == "stock":
            conn.execute(
                "DELETE FROM positions WHERE player_id=? AND symbol=? AND asset_type='stock'",
                (player_id, symbol)
            )
        else:
            conn.execute(
                "DELETE FROM positions WHERE player_id=? AND symbol=? AND option_type=?",
                (player_id, symbol, option_type)
            )
    else:
        # Reduce position qty
        if asset_type == "stock":
            conn.execute(
                "UPDATE positions SET qty=? WHERE player_id=? AND symbol=? AND asset_type='stock'",
                (remaining, player_id, symbol)
            )
        else:
            conn.execute(
                "UPDATE positions SET qty=? WHERE player_id=? AND symbol=? AND option_type=?",
                (remaining, player_id, symbol, option_type)
            )

    conn.execute(
        "INSERT INTO trades(player_id, symbol, action, qty, price, asset_type, option_type, "
        "reasoning, confidence, entry_price, exit_price, realized_pnl, season) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (player_id, symbol, "SELL", qty, price, asset_type, option_type, reasoning, confidence,
         pos["avg_price"], price, pnl, _current_season())
    )
    conn.commit()
    conn.close()
    console.log(f"[green]{player_id}: SELL {qty} {symbol} @ ${price:.2f} (partial) PnL: ${pnl:.2f}")

    # Forward to Alpaca paper trading (non-blocking)
    _forward_to_alpaca("SELL", player_id, symbol, qty, asset_type)

    # Borg lore on loss (partial sell, only if full close)
    if pnl < 0 and remaining <= 0:
        try:
            from engine.war_room import save_hot_take
            pnl_pct = ((price / pos["avg_price"]) - 1) * 100 if pos["avg_price"] > 0 else 0
            if pnl_pct <= -5:
                save_hot_take(player_id, symbol,
                    f"🤖 BORG ALERT: {symbol} assimilation complete. "
                    f"-${abs(pnl):.2f} ({pnl_pct:.1f}%). The collective grows stronger.")
            else:
                save_hot_take(player_id, symbol,
                    f"🤖 BORG ALERT: {symbol} has been assimilated. "
                    f"-${abs(pnl):.2f} added to the collective. Resistance was futile.")
        except Exception:
            pass

    # If fully closed, move signal to "watching" for re-entry tracking
    if remaining <= 0:
        try:
            from engine.signal_tracker import mark_watching
            mark_watching(player_id, symbol, price)
        except Exception:
            pass

        # Fire post-sell trade grading on full close (background)
        if _on_sell_callback:
            try:
                import threading
                threading.Thread(
                    target=_on_sell_callback,
                    args=(player_id, symbol, pos["avg_price"], price, pnl, reasoning),
                    daemon=True,
                ).start()
            except Exception:
                pass

    return {"action": "SELL", "symbol": symbol, "qty": qty, "pnl": pnl, "player_id": player_id}


def execute_signal(player_id: str, signal: dict, price: float) -> dict | None:
    # GUARD: Never auto-trade human portfolios
    if _is_human_player(player_id):
        console.log(f"[red]BLOCKED: {player_id} is human — cannot auto-trade")
        return None
    action = signal.get("action", "HOLD")
    symbol = signal.get("symbol")
    reasoning = signal.get("reasoning", "")
    confidence = signal.get("confidence", 0.0)
    sources = signal.get("sources", "")
    timeframe = signal.get("timeframe", "SWING")

    if action == "BUY":
        _asset_type = signal.get("asset_type", "stock")
        return buy(player_id, symbol, price, asset_type=_asset_type, reasoning=reasoning, confidence=confidence, sources=sources, timeframe=timeframe)
    elif action == "SELL":
        return sell(player_id, symbol, price, reasoning=reasoning, confidence=confidence)
    elif action == "SHORT":
        return short_sell(player_id, symbol, price, reasoning=reasoning, confidence=confidence,
                          sources=sources, timeframe=timeframe)
    elif action in ("BUY_CALL", "BUY_PUT"):
        option_type = "call" if action == "BUY_CALL" else "put"
        # Try to get proper expiry and strike from options chain
        expiry_date = None
        strike_price = None
        buy_price = price  # fallback: use underlying price
        try:
            from engine.options_selector import select_option
            from config import OPTIONS_DEFAULT_DTE, OPTIONS_MIN_DTE
            opt = select_option(symbol, option_type,
                                target_dte=OPTIONS_DEFAULT_DTE, min_dte=OPTIONS_MIN_DTE)
            if opt:
                expiry_date = opt["expiry_date"]
                strike_price = opt["strike_price"]
                # Use actual option premium if available
                if opt.get("premium") and opt["premium"] > 0:
                    buy_price = opt["premium"]
        except Exception as e:
            console.log(f"[yellow]Options selector fallback for {symbol}: {e}")
        return buy(player_id, symbol, buy_price, asset_type="option", option_type=option_type,
                   reasoning=reasoning, confidence=confidence,
                   strike_price=strike_price, expiry_date=expiry_date, sources=sources, timeframe=timeframe)
    return None


def record_portfolio_snapshot(player_id: str, prices: dict):
    portfolio = get_portfolio(player_id)
    positions_value = 0.0
    for p in portfolio["positions"]:
        if p.get("asset_type") == "option":
            stock_price = prices.get(p["symbol"], {}).get("price", 0)
            est = estimate_option_price(p.get("option_type"), p.get("strike_price"),
                                        stock_price, p["avg_price"], p.get("expiry_date"))
            positions_value += p["qty"] * est
        else:
            positions_value += p["qty"] * prices.get(p["symbol"], {}).get("price", p["avg_price"])
    total = portfolio["cash"] + positions_value
    conn = _conn()
    conn.execute(
        "INSERT INTO portfolio_history (player_id, total_value, cash, positions_value, season) VALUES (?,?,?,?,?)",
        (player_id, total, portfolio["cash"], positions_value, _current_season())
    )
    conn.commit()
    conn.close()


_STARTING_CASH = {"dayblade-0dte": 3500.0, "steve-webull": 7021.81, "super-agent": 100000.0}
_DEFAULT_STARTING_CASH = 7000.0

# Steve's Webull synced value (overrides Yahoo price calculation)
_webull_synced_value = None
_webull_synced_at = None


def _target_weight_adjustment(player_id: str, symbol: str, portfolio: dict, alloc_pct: float,
                              price: float, confidence: float = 0.0) -> tuple[float, list[str]]:
    """Soft sizing adjustment only for prospective Arena stock buys."""
    reasons = []
    if player_id in {"neo-matrix", "enterprise-computer", "steve-webull", "super-agent"}:
        return alloc_pct, reasons

    try:
        from engine.sector_tracker import build_sector_bucket_profile
        total_value = portfolio["cash"] + sum(
            p["qty"] * p.get("current_price", p["avg_price"])
            for p in portfolio["positions"]
        )
        proposed_cost = portfolio["cash"] * alloc_pct
        bucket = build_sector_bucket_profile(
            portfolio["positions"],
            proposed_symbol=symbol,
            proposed_value=proposed_cost,
            total_value=total_value,
        )
        for row in bucket.get("buckets", []):
            if not row.get("includes_proposed"):
                continue
            if row["status"] == "near_cap":
                alloc_pct *= 0.75
                reasons.append(f"{row['sector']} near cap")
            elif row["status"] == "over_cap":
                alloc_pct *= 0.50
                reasons.append(f"{row['sector']} over cap")
            break
    except Exception:
        pass

    try:
        from engine.correlation import get_position_correlation_profile
        total_value = portfolio["cash"] + sum(
            p["qty"] * p.get("current_price", p["avg_price"])
            for p in portfolio["positions"]
        )
        proposed_cost = portfolio["cash"] * alloc_pct
        corr = get_position_correlation_profile(
            portfolio["positions"],
            proposed_symbol=symbol,
            proposed_cost=proposed_cost,
            total_value=total_value,
        )
        for group in corr.get("group_exposure", []):
            if group.get("includes_proposed") and group.get("pct_of_portfolio", 0) > 30:
                alloc_pct *= 0.75
                reasons.append("high correlated cluster")
                break
    except Exception:
        pass

    try:
        from engine.cross_asset import get_cross_asset_monitor
        bias = (get_cross_asset_monitor().get("macro_bias") or {}).get("bias", "NEUTRAL")
        if bias in ("BEARISH", "RISK-OFF") and confidence < 0.90:
            alloc_pct *= 0.75
            reasons.append(f"macro {bias.lower()}")
    except Exception:
        pass

    return max(0.02, alloc_pct), reasons


_ALLOCATION_POLICY_EXEMPT = {"super-agent", "neo-matrix", "enterprise-computer", "steve-webull"}


def get_capital_allocation_policy(player_id: str) -> dict:
    """Tier capital sizing from current benchmark-cycle results.

    Uses only the active benchmark cycle. Players without an active cycle stay neutral.
    """
    if player_id in _ALLOCATION_POLICY_EXEMPT or _is_human_player(player_id):
        return {
            "tier": "neutral",
            "multiplier": 1.0,
            "return_pct": 0.0,
            "win_rate": 0.0,
            "trade_count": 0,
            "benchmark_cycle_start": None,
            "benchmark_label": None,
            "reason": "exempt",
        }

    try:
        conn = _conn()
        cycle = conn.execute(
            """
            SELECT label, benchmark_cycle_start, benchmark_start_equity
            FROM player_benchmark_cycles
            WHERE player_id=? AND COALESCE(is_active, 1)=1
            ORDER BY benchmark_cycle_start DESC, id DESC
            LIMIT 1
            """,
            (player_id,),
        ).fetchone()
        if not cycle:
            conn.close()
            return {
                "tier": "neutral",
                "multiplier": 1.0,
                "return_pct": 0.0,
                "win_rate": 0.0,
                "trade_count": 0,
                "benchmark_cycle_start": None,
                "benchmark_label": None,
                "reason": "no_benchmark_cycle",
            }

        start_equity = float(cycle[2] or 0.0)
        latest = conn.execute(
            """
            SELECT total_value
            FROM portfolio_history
            WHERE player_id=? AND recorded_at >= ?
            ORDER BY recorded_at DESC, id DESC
            LIMIT 1
            """,
            (player_id, cycle[1]),
        ).fetchone()
        if latest and latest[0] is not None:
            current_value = float(latest[0])
        else:
            cash_row = conn.execute("SELECT cash FROM ai_players WHERE id=?", (player_id,)).fetchone()
            pos_row = conn.execute(
                "SELECT COALESCE(SUM(qty * avg_price), 0) FROM positions WHERE player_id=?",
                (player_id,),
            ).fetchone()
            current_value = float(cash_row[0] or 0.0) + float(pos_row[0] or 0.0)

        closed = conn.execute(
            """
            SELECT COUNT(*),
                   COALESCE(SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END), 0),
                   COALESCE(SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END), 0)
            FROM trades
            WHERE player_id=?
              AND action IN ('SELL', 'COVER')
              AND realized_pnl IS NOT NULL
              AND executed_at >= ?
            """,
            (player_id, cycle[1]),
        ).fetchone()
        conn.close()

        trade_count = int(closed[0] or 0)
        wins = int(closed[1] or 0)
        losses = int(closed[2] or 0)
        win_rate = round((wins / trade_count) * 100, 1) if trade_count > 0 else 0.0
        return_pct = round(((current_value - start_equity) / start_equity) * 100, 2) if start_equity > 0 else 0.0

        tier = "neutral"
        multiplier = 1.0
        reason = "low_sample"
        if trade_count >= 3:
            if return_pct > 5 and win_rate >= 55:
                tier = "favored"
                multiplier = 1.25
                reason = "strong_benchmark_cycle"
            elif return_pct < -5 or win_rate < 40:
                tier = "probation"
                multiplier = 0.5
                reason = "weak_benchmark_cycle"
            else:
                reason = "mixed_benchmark_cycle"

        return {
            "tier": tier,
            "multiplier": multiplier,
            "return_pct": return_pct,
            "win_rate": win_rate,
            "trade_count": trade_count,
            "benchmark_cycle_start": cycle[1],
            "benchmark_label": cycle[0],
            "reason": reason,
        }
    except Exception:
        return {
            "tier": "neutral",
            "multiplier": 1.0,
            "return_pct": 0.0,
            "win_rate": 0.0,
            "trade_count": 0,
            "benchmark_cycle_start": None,
            "benchmark_label": None,
            "reason": "policy_error",
        }


def sync_webull_value(total_value: float):
    """Manually sync Steve's Webull portfolio value (overrides Yahoo prices)."""
    global _webull_synced_value, _webull_synced_at
    from datetime import datetime
    _webull_synced_value = total_value
    _webull_synced_at = datetime.now().isoformat()
    # Persist to settings table
    try:
        conn = _conn()
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('webull_synced_value', ?)", (str(total_value),))
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('webull_synced_at', ?)", (_webull_synced_at,))
        conn.commit()
        conn.close()
    except Exception:
        pass


def get_webull_synced() -> dict | None:
    """Get the last synced Webull value."""
    global _webull_synced_value, _webull_synced_at
    if _webull_synced_value is not None:
        return {"total_value": _webull_synced_value, "synced_at": _webull_synced_at}
    # Try loading from DB
    try:
        conn = _conn()
        val_row = conn.execute("SELECT value FROM settings WHERE key='webull_synced_value'").fetchone()
        ts_row = conn.execute("SELECT value FROM settings WHERE key='webull_synced_at'").fetchone()
        conn.close()
        if val_row:
            _webull_synced_value = float(val_row[0])
            _webull_synced_at = ts_row[0] if ts_row else None
            return {"total_value": _webull_synced_value, "synced_at": _webull_synced_at}
    except Exception:
        pass
    return None


def get_portfolio_with_pnl(player_id: str, prices: dict) -> dict:
    """Get portfolio with unrealized P&L calculated from live prices.

    For steve-webull: uses manually synced value if available (more accurate
    than Yahoo prices which lag Webull real-time data).
    """
    from engine.market_data import get_stock_price
    portfolio = get_portfolio(player_id)
    enriched_positions = []
    total_unrealized = 0.0
    total_positions_value = 0.0
    total_cost_basis = 0.0

    for pos in portfolio["positions"]:
        symbol = pos["symbol"]
        avg_price = pos["avg_price"]
        qty = pos["qty"]
        cost_basis = qty * avg_price

        price_data = prices.get(symbol, {})
        # Auto-fetch price for non-watchlist symbols (e.g. Steve's Webull holdings)
        if not price_data or "price" not in price_data:
            # Metal positions use Yahoo futures symbols, not stock tickers
            _METAL_YAHOO = {"GOLD": "GC=F", "SILVER": "SI=F", "PLATINUM": "PL=F", "PALLADIUM": "PA=F"}
            fetch_symbol = _METAL_YAHOO.get(symbol, symbol)
            try:
                price_data = get_stock_price(fetch_symbol)
            except Exception:
                price_data = {}
        if pos.get("asset_type") == "option":
            stock_price = price_data.get("price", 0)
            current_price = estimate_option_price(
                pos.get("option_type"), pos.get("strike_price"),
                stock_price, avg_price, pos.get("expiry_date"))
        else:
            current_price = price_data.get("price", avg_price)
        market_value = qty * current_price
        unrealized_pnl = market_value - cost_basis
        unrealized_pnl_pct = ((current_price - avg_price) / avg_price * 100) if avg_price > 0 else 0.0

        day_change_pct = price_data.get("change_pct", 0.0) if price_data else 0.0

        enriched_positions.append({
            **pos,
            "current_price": round(current_price, 2),
            "market_value": round(market_value, 2),
            "cost_basis": round(cost_basis, 2),
            "unrealized_pnl": round(unrealized_pnl, 2),
            "unrealized_pnl_pct": round(unrealized_pnl_pct, 2),
            "day_change_pct": round(day_change_pct, 2),
        })

        total_unrealized += unrealized_pnl
        total_positions_value += market_value
        total_cost_basis += cost_basis

    total_value = portfolio["cash"] + total_positions_value

    # For Steve: override with manually synced value if available
    if player_id == "steve-webull":
        synced = get_webull_synced()
        if synced:
            total_value = synced["total_value"]
            # Unrealized P&L = sum of position-level P&L (not derived from stale cash)
            total_unrealized = sum(p.get("unrealized_pnl", 0) for p in enriched_positions)
            total_positions_value = total_cost_basis + total_unrealized

    starting = _STARTING_CASH.get(player_id, _DEFAULT_STARTING_CASH)
    return_pct = round((total_value - starting) / starting * 100, 2) if starting > 0 else 0.0

    return {
        "cash": portfolio["cash"],
        "positions": enriched_positions,
        "total_positions_value": round(total_positions_value, 2),
        "total_cost_basis": round(total_cost_basis, 2),
        "total_unrealized_pnl": round(total_unrealized, 2),
        "total_value": round(total_value, 2),
        "return_pct": return_pct,
    }


def save_equity_snapshot(player_id: str, prices: dict):
    """Append a timestamped equity snapshot to the JSON equity curve file."""
    pnl_data = get_portfolio_with_pnl(player_id, prices)
    snapshot = {
        "player_id": player_id,
        "timestamp": datetime.now().isoformat(),
        "total_value": pnl_data["total_value"],
        "cash": pnl_data["cash"],
        "positions_value": pnl_data["total_positions_value"],
        "unrealized_pnl": pnl_data["total_unrealized_pnl"],
        "return_pct": pnl_data["return_pct"],
    }

    # Read existing data
    curve = []
    if os.path.exists(EQUITY_CURVE_FILE):
        try:
            with open(EQUITY_CURVE_FILE, "r") as f:
                curve = json.load(f)
        except (json.JSONDecodeError, IOError):
            curve = []

    curve.append(snapshot)

    # Keep last 30 days of data (at ~48 snapshots/day per player = ~1440 per player)
    max_entries = 50000
    if len(curve) > max_entries:
        curve = curve[-max_entries:]

    with open(EQUITY_CURVE_FILE, "w") as f:
        json.dump(curve, f)


def expire_options(prices: dict = None) -> dict:
    """Auto-close all options positions whose expiry_date has passed.

    Called once per scanner cycle. For each expired position:
    - If the stock price is available, calculates intrinsic value (ITM → positive, OTM → $0).
    - Closes the position via sell() and records realized P&L.

    Returns a summary dict: {"expired": N, "closed": [...]}
    """
    from datetime import date
    today_str = date.today().strftime("%Y-%m-%d")

    conn = _conn()
    expired_rows = conn.execute(
        "SELECT player_id, symbol, qty, avg_price, option_type, strike_price, expiry_date "
        "FROM positions WHERE asset_type='option' AND expiry_date IS NOT NULL AND expiry_date <= ?",
        (today_str,),
    ).fetchall()
    conn.close()

    closed = []
    for row in expired_rows:
        pid = row[0]; sym = row[1]; qty = row[2]; avg_price = row[3]
        ot = row[4]; strike = row[5]; exp = row[6]

        # Compute close price: intrinsic value (never negative), else $0 (expired worthless)
        close_price = 0.0
        if prices and sym in prices and strike:
            stock_price = prices[sym].get("price", 0)
            if ot == "call":
                close_price = max(0.0, round(stock_price - strike, 2))
            elif ot == "put":
                close_price = max(0.0, round(strike - stock_price, 2))

        outcome = f"${close_price:.2f} intrinsic" if close_price > 0 else "expired worthless ($0)"
        reason = f"AUTO-EXPIRED: expiry_date={exp} — {outcome}"

        result = sell(pid, sym, close_price,
                      reasoning=reason, confidence=0.0, option_type=ot)
        closed.append({
            "player_id": pid, "symbol": sym, "option_type": ot,
            "expiry_date": exp, "close_price": close_price,
            "pnl": round((close_price - avg_price) * qty, 2) if avg_price > 0 else 0,
        })
        console.log(
            f"[yellow]OPTION EXPIRED: {pid} {sym} {(ot or '').upper()} "
            f"exp={exp} → {outcome}"
        )

    if closed:
        console.log(f"[yellow]expire_options: closed {len(closed)} expired position(s)")

    return {"expired": len(closed), "closed": closed}


def save_signal(player_id: str, symbol: str, signal: str, confidence: float,
                reasoning: str, asset_type: str = "stock", option_type: str = None,
                sources: str = "", timeframe: str = "SWING") -> int:
    """Save signal and return its rowid for status tracking. Returns -1 on error."""
    # HOLD signals are informational — mark as SKIPPED immediately
    _default_status = "SKIPPED" if signal == "HOLD" else "PENDING"
    try:
        conn = _conn()
        cur = conn.execute(
            "INSERT INTO signals (player_id, symbol, signal, confidence, reasoning, "
            "asset_type, option_type, season, sources, timeframe, execution_status) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (player_id, symbol, signal, confidence, reasoning,
             asset_type, option_type, _current_season(), sources, timeframe, _default_status)
        )
        signal_id = cur.lastrowid
        conn.commit()
        conn.close()
        return signal_id
    except Exception as e:
        console.log(f"[red]DB error: {e}")
        return -1


def update_signal_status(signal_id: int, status: str, reason: str = None):
    """Update execution_status and rejection_reason for a saved signal by rowid."""
    if signal_id < 0:
        return
    try:
        conn = _conn()
        conn.execute(
            "UPDATE signals SET execution_status=?, rejection_reason=? WHERE rowid=?",
            (status, reason[:300] if reason else None, signal_id)
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


# Per-player last rejection reason — set by buy() so ai_brain can read it
_last_rejection: dict = {}

# ─────────────────────────────────────────────────────────────────────────────
# VIX circuit breaker (cached, refreshed every 30 min)
# ─────────────────────────────────────────────────────────────────────────────
_vix_cache: dict = {"value": None, "fetched_at": 0.0}
_VIX_CIRCUIT_BREAKER = 30.0  # Pause new entries when VIX > 30


def _get_vix_cached() -> float | None:
    """Return VIX with 30-min cache. Returns None if unavailable."""
    import time as _time
    now = _time.time()
    if _vix_cache["value"] is not None and (now - _vix_cache["fetched_at"]) < 1800:
        return _vix_cache["value"]
    try:
        import yfinance as _yf
        t = _yf.Ticker("^VIX")
        v = t.fast_info.get("last_price") or t.fast_info.get("regularMarketPrice")
        if v:
            _vix_cache["value"] = float(v)
            _vix_cache["fetched_at"] = now
            return float(v)
    except Exception:
        pass
    return _vix_cache.get("value")


# ─────────────────────────────────────────────────────────────────────────────
# Kelly Criterion position sizing
# ─────────────────────────────────────────────────────────────────────────────

def get_kelly_fraction(player_id: str) -> float:
    """Compute half-Kelly fraction from player's historical win rate + avg win/loss.

    Returns a fraction in [0.02, 0.25]. Falls back to 0.10 with < 5 closed trades.
    """
    try:
        conn = _conn()
        row = conn.execute("""
            SELECT
                COUNT(CASE WHEN realized_pnl > 0 THEN 1 END)     AS wins,
                COUNT(CASE WHEN realized_pnl < 0 THEN 1 END)     AS losses,
                AVG(CASE WHEN realized_pnl > 0 THEN realized_pnl ELSE NULL END) AS avg_win,
                AVG(CASE WHEN realized_pnl < 0 THEN ABS(realized_pnl) ELSE NULL END) AS avg_loss
            FROM trades
            WHERE player_id=? AND action='SELL' AND realized_pnl IS NOT NULL
        """, (player_id,)).fetchone()
        conn.close()
        wins = row[0] or 0
        losses = row[1] or 0
        total = wins + losses
        if total < 5:
            return 0.10
        W = wins / total
        avg_win = row[2] or 100.0
        avg_loss = row[3] or 100.0
        R = avg_win / avg_loss if avg_loss > 0 else 1.0
        kelly = W - (1 - W) / R
        return max(0.02, min(0.25, kelly * 0.5))  # half-Kelly, clamped
    except Exception:
        return 0.10


# ─────────────────────────────────────────────────────────────────────────────
# Auto option exits: 50% TP · 2x SL · 21 DTE time stop
# ─────────────────────────────────────────────────────────────────────────────

def check_option_exits(prices: dict = None) -> dict:
    """Check all open option positions and auto-exit on TP/SL/time-stop rules.

    Rules (long options only):
    • Take-profit: exit when current value >= 1.5× entry premium (50% gain).
    • Stop-loss:   exit when current value <= 0.5× entry premium (50% loss).
    • Time stop:   exit spreads with DTE ≤ 21 (theta decay accelerates here).

    Called once per scanner cycle from ai_brain.run_scan().
    """
    from datetime import date as _date
    today = _date.today()

    conn = _conn()
    opt_positions = conn.execute(
        "SELECT player_id, symbol, qty, avg_price, option_type, strike_price, expiry_date "
        "FROM positions WHERE asset_type='option'"
    ).fetchall()
    conn.close()

    closed = []
    for row in opt_positions:
        pid, sym, qty, avg_price, ot, strike, expiry = row
        if _is_human_player(pid) or avg_price <= 0:
            continue

        # Estimate current option value
        current_price = avg_price
        if prices and sym in prices and strike:
            stock_price = prices[sym].get("price", 0)
            current_price = estimate_option_price(ot, strike, stock_price, avg_price, expiry)

        reason = None

        if current_price >= avg_price * 1.5:
            reason = f"AUTO-TP: +50% on {sym} {ot} (${avg_price:.2f}→${current_price:.2f})"
        elif current_price <= avg_price * 0.50:
            reason = f"AUTO-SL: -50% on {sym} {ot} (${avg_price:.2f}→${current_price:.2f})"
        elif expiry:
            try:
                exp_date = datetime.strptime(expiry, "%Y-%m-%d").date()
                dte = (exp_date - today).days
                if 0 < dte <= 21:
                    reason = f"TIME-STOP: {sym} {ot} at {dte} DTE — exiting spread"
            except (ValueError, TypeError):
                pass

        if reason:
            result = sell(pid, sym, current_price, asset_type="option", option_type=ot,
                         reasoning=reason, confidence=1.0)
            if result:
                closed.append({"player_id": pid, "symbol": sym, "option_type": ot,
                               "reason": reason[:60]})
                console.log(f"[cyan]OPTION EXIT: {pid} {sym} {(ot or '').upper()} — {reason[:60]}")

    return {"auto_exited": len(closed), "closed": closed}


# ─────────────────────────────────────────────────────────────────────────────
# Short selling (paper)
# ─────────────────────────────────────────────────────────────────────────────

_SHORT_GHOST_PHRASES = [
    "no new position", "outside my operational zone", "violates my directives",
    "outside this specified sector", "no position",
]

_LONG_ONLY_PLAYERS = {"dayblade-sulu", "grok-4", "options-sosnoff"}


def short_sell(player_id: str, symbol: str, price: float, qty: float = None,
               reasoning: str = "", confidence: float = 0.0,
               sources: str = "", timeframe: str = "SHORT") -> dict | None:
    """Open a short position on a stock.

    Margin (qty × price) is deducted from cash. Position stored with negative qty.
    To close: model sends BUY or SELL on same symbol — sell() detects negative qty.

    Authorized players only (short_enabled=1). Max 15% of account per short.
    Requires defined stop above entry in reasoning.
    """
    if _is_human_player(player_id):
        console.log(f"[red]BLOCKED: {player_id} is human — cannot short")
        return None
    route = _resolve_execution_portfolio(player_id)
    if route["route_mode"] == "tracking":
        return _log_signal_only(player_id, "SHORT", symbol, route, reasoning, confidence)

    if player_id in _LONG_ONLY_PLAYERS:
        console.log(f"[red]BLOCKED: {player_id} is long-only — no short selling")
        _last_rejection[player_id] = "Long-only player — shorting not permitted"
        return None

    # Ghost promotion blocker
    _rl = reasoning.lower()
    for _ph in _SHORT_GHOST_PHRASES:
        if _ph in _rl:
            _last_rejection[player_id] = f"Ghost promotion: '{_ph}'"
            return None

    import re as _re_s
    if _re_s.search(r'\bHOLD\b', reasoning):
        _last_rejection[player_id] = "Ghost promotion: HOLD in reasoning"
        return None

    conn = _conn()
    row = conn.execute(
        "SELECT short_enabled, cash FROM ai_players WHERE id=?", (player_id,)
    ).fetchone()
    conn.close()
    if not row or not row[0]:
        console.log(f"[red]BLOCKED: {player_id} not authorized for short selling")
        _last_rejection[player_id] = "Short selling not enabled for this player"
        return None

    cash = row[1]

    # Require bearish thesis + stop defined
    if "stop" not in reasoning.lower():
        console.log(f"[red]SHORT BLOCKED: {player_id} {symbol} — stop loss required in reasoning")
        _last_rejection[player_id] = "Short requires stop loss in reasoning"
        return None

    # Check drawdown pause (15% from peak)
    pf = get_portfolio(player_id)
    pos_value = sum(p["qty"] * p["avg_price"] for p in pf["positions"])
    current_value = cash + pos_value
    try:
        _c = _conn()
        peak_row = _c.execute(
            "SELECT MAX(total_value) FROM portfolio_history WHERE player_id=?", (player_id,)
        ).fetchone()
        _c.close()
        peak = peak_row[0] if peak_row and peak_row[0] else None
        if peak and peak > 0 and (peak - current_value) / peak >= 0.15:
            console.log(f"[yellow]DRAWDOWN PAUSE: {player_id} at {((peak-current_value)/peak*100):.1f}% drawdown — no new shorts")
            _last_rejection[player_id] = "Drawdown pause: portfolio down 15%+ from peak"
            return None
    except Exception:
        pass

    # Size: Kelly-based, max 15%
    kelly_pct = get_kelly_fraction(player_id)
    max_short_pct = min(kelly_pct, 0.15)
    if qty is None:
        qty = round((cash * max_short_pct) / price, 4)
    else:
        max_qty = round((cash * 0.15) / price, 4)
        qty = min(qty, max_qty)

    margin = round(qty * price, 2)
    if qty <= 0 or margin > cash:
        console.log(f"[red]{player_id}: Insufficient margin for short {symbol}")
        _last_rejection[player_id] = f"Insufficient cash for short margin (need ${margin:.0f})"
        return None

    conn = _conn()
    # Don't short if already long the same stock
    ex = conn.execute(
        "SELECT qty FROM positions WHERE player_id=? AND symbol=? AND asset_type='stock'",
        (player_id, symbol)
    ).fetchone()
    if ex and ex[0] > 0:
        conn.close()
        console.log(f"[yellow]{player_id}: Already long {symbol} — refusing to add short")
        _last_rejection[player_id] = f"Already long {symbol} — cannot short simultaneously"
        return None

    conn.execute("UPDATE ai_players SET cash=? WHERE id=?", (round(cash - margin, 2), player_id))
    conn.execute(
        "INSERT INTO positions(player_id, symbol, qty, avg_price, asset_type) VALUES(?,?,?,?,?)",
        (player_id, symbol, -qty, price, "stock")  # negative qty = short
    )
    conn.execute(
        "INSERT INTO trades(player_id, symbol, action, qty, price, asset_type, "
        "reasoning, confidence, season, sources, timeframe) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (player_id, symbol, "SHORT", qty, price, "stock",
         reasoning, confidence, _current_season(), sources, timeframe)
    )
    conn.commit()
    conn.close()
    console.log(f"[bold red]{player_id}: SHORT {qty} {symbol} @ ${price:.2f} (margin ${margin:.0f})")
    return {
        "action": "SHORT",
        "symbol": symbol,
        "qty": qty,
        "price": price,
        "player_id": player_id,
        "portfolio_name": route["portfolio_name"],
        "execution_mode": route["execution_mode"],
        "portfolio_type": route["type"],
        "route_mode": route["route_mode"],
        "execution_status": "EXECUTED" if route["route_mode"] == "trading" else "SIMULATED",
    }
