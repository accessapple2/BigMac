"""
CrewAI specialist agents for USS TradeMinds.

5 agents form the strategy-writing crew:
  Scout → Architect → Backtester → Critic → Commander
"""

import json
import os
import sqlite3
from datetime import datetime, timedelta

from crewai import Agent
from crewai.tools import tool
from shared.finviz_scanner import scan_finviz
from uoa.crew_tools import uoa_alerts_tool, uoa_flow_tool, uoa_put_call_scan_tool

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_PATH = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))

LLM_CONFIG = {
    "scout": "ollama/gemma3:4b",
    "architect": "ollama/gemma3:4b",
    "backtester": "ollama/gemma3:4b",
    "critic": "ollama/gemma3:4b",
    "commander": "ollama/gemma3:4b",          # fallback from gemini-2.5-flash (spending cap hit)
}


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@tool
def query_news_sources(topic: str) -> str:
    """Query all market intelligence for a topic. Searches news headlines, AI signals,
    universe scans, discovery scanner, options flow, strategy signals, and fundamentals.
    Returns a combined brief the Scout can synthesize."""
    conn = _db()
    try:
        sections = []

        # 1. market_news — headlines matching topic or symbol
        rows = conn.execute(
            "SELECT symbol, headline, source, sentiment, fetched_at FROM market_news "
            "WHERE headline LIKE ? OR symbol LIKE ? ORDER BY fetched_at DESC LIMIT 15",
            (f"%{topic}%", f"%{topic.upper()}%"),
        ).fetchall()
        if rows:
            sections.append({"source": "market_news", "count": len(rows),
                             "data": [dict(r) for r in rows]})

        # 2. signals — recent AI buy/sell signals for matching symbols
        rows = conn.execute(
            "SELECT player_id, symbol, signal, confidence, reasoning, asset_type, "
            "option_type, created_at FROM signals "
            "WHERE (symbol LIKE ? OR reasoning LIKE ?) AND created_at > datetime('now', '-3 days') "
            "ORDER BY created_at DESC LIMIT 15",
            (f"%{topic.upper()}%", f"%{topic}%"),
        ).fetchall()
        if rows:
            sections.append({"source": "ai_signals", "count": len(rows),
                             "data": [dict(r) for r in rows]})

        # 3. universe_scan — nightly scan scores
        rows = conn.execute(
            "SELECT ticker, close, volume_ratio, rsi, score, signals, gap_pct, scan_date "
            "FROM universe_scan WHERE ticker LIKE ? ORDER BY scan_date DESC LIMIT 10",
            (f"%{topic.upper()}%",),
        ).fetchall()
        if rows:
            sections.append({"source": "universe_scan", "count": len(rows),
                             "data": [dict(r) for r in rows]})

        # 4. discoveries — short squeezes, unusual volume, momentum
        rows = conn.execute(
            "SELECT symbol, trigger_type, price, change_pct, volume, rel_volume, "
            "short_float, details, detected_at FROM discoveries "
            "WHERE (symbol LIKE ? OR details LIKE ?) AND detected_at > datetime('now', '-3 days') "
            "ORDER BY detected_at DESC LIMIT 10",
            (f"%{topic.upper()}%", f"%{topic}%"),
        ).fetchall()
        if rows:
            sections.append({"source": "discoveries", "count": len(rows),
                             "data": [dict(r) for r in rows]})

        # 5. flow_lean_history — latest options flow directional bias
        row = conn.execute(
            "SELECT lean, conviction, net_flow, total_call_premium, total_put_premium, "
            "recorded_at FROM flow_lean_history ORDER BY recorded_at DESC LIMIT 1"
        ).fetchone()
        if row:
            sections.append({"source": "options_flow_lean", "data": dict(row)})

        # 6. stock_fundamentals — if topic matches a symbol
        row = conn.execute(
            "SELECT symbol, smart_score, grade, data FROM stock_fundamentals "
            "WHERE symbol = ? LIMIT 1",
            (topic.upper(),),
        ).fetchone()
        if row:
            fund = dict(row)
            # Parse the JSON data field for key metrics only
            try:
                d = json.loads(fund["data"])
                fund["key_metrics"] = {
                    "price": d.get("current_price"), "pe_forward": d.get("pe_forward"),
                    "revenue_growth": d.get("revenue_growth"), "earnings_growth": d.get("earnings_growth"),
                    "profit_margin": d.get("profit_margin"), "analyst_upside": d.get("analyst_upside"),
                    "recommendation": d.get("recommendation"), "short_pct_float": d.get("short_pct_float"),
                    "next_earnings": d.get("next_earnings"), "sector": d.get("sector"),
                    "industry": d.get("industry"),
                }
                del fund["data"]  # Don't send the huge blob
            except (json.JSONDecodeError, TypeError):
                pass
            sections.append({"source": "fundamentals", "data": fund})

        # 7. strategy_signals — convergence signals from nightly strategy engine
        rows = conn.execute(
            "SELECT ticker, strategy_name, signal_type, confidence, entry_price, "
            "stop_price, target_price, notes, scan_date FROM strategy_signals "
            "WHERE ticker LIKE ? ORDER BY scan_date DESC LIMIT 5",
            (f"%{topic.upper()}%",),
        ).fetchall()
        if rows:
            sections.append({"source": "strategy_signals", "count": len(rows),
                             "data": [dict(r) for r in rows]})

        # 8. smart_money_signals — multi-AI convergence buys
        rows = conn.execute(
            "SELECT symbol, buyers, detected_at FROM smart_money_signals "
            "WHERE symbol LIKE ? ORDER BY detected_at DESC LIMIT 3",
            (f"%{topic.upper()}%",),
        ).fetchall()
        if rows:
            sections.append({"source": "smart_money_convergence", "count": len(rows),
                             "data": [dict(r) for r in rows]})

        if not sections:
            return f"No market intelligence found for '{topic}'."

        return json.dumps(sections, default=str)
    finally:
        conn.close()


@tool
def query_congress_trades(days: int) -> str:
    """Query smart money activity: reference trades from external arenas, multi-AI
    convergence signals, and congressional trades (when available). Returns trades
    and institutional signals from the last N days."""
    conn = _db()
    try:
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        sections = []

        # 1. reference_trades — external arena trades (rallies.ai etc)
        rows = conn.execute(
            "SELECT source, model_name, symbol, action, price, reasoning, confidence, "
            "outcome, pnl_pct, regime, traded_at FROM reference_trades "
            "WHERE traded_at >= ? ORDER BY traded_at DESC LIMIT 30",
            (cutoff,),
        ).fetchall()
        if rows:
            sections.append({"source": "reference_trades", "count": len(rows),
                             "data": [dict(r) for r in rows]})

        # 2. smart_money_signals — multi-AI convergence buys
        rows = conn.execute(
            "SELECT symbol, buyers, detected_at FROM smart_money_signals "
            "WHERE detected_at >= ? ORDER BY detected_at DESC LIMIT 10",
            (cutoff,),
        ).fetchall()
        if rows:
            sections.append({"source": "smart_money_convergence", "count": len(rows),
                             "data": [dict(r) for r in rows]})

        # 3. congress_trades — if table exists
        has_congress = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='congress_trades'"
        ).fetchone()
        if has_congress:
            rows = conn.execute(
                "SELECT * FROM congress_trades WHERE trade_date >= ? "
                "ORDER BY trade_date DESC LIMIT 50",
                (cutoff,),
            ).fetchall()
            if rows:
                sections.append({"source": "congress_trades", "count": len(rows),
                                 "data": [dict(r) for r in rows]})
        else:
            sections.append({"source": "congress_trades",
                             "note": "Not yet populated. Using reference_trades and smart_money as proxy."})

        # 4. weekly_picks — top conviction picks
        rows = conn.execute(
            "SELECT * FROM weekly_picks ORDER BY rowid DESC LIMIT 5"
        ).fetchall()
        if rows:
            sections.append({"source": "weekly_picks", "count": len(rows),
                             "data": [dict(r) for r in rows]})

        if not sections:
            return f"No smart money activity in the last {days} days."

        return json.dumps(sections, default=str)
    finally:
        conn.close()


@tool
def query_backtest_history(strategy_name: str) -> str:
    """Read backtest history for a strategy. READ ONLY — never deletes data. Returns past backtest results."""
    conn = _db()
    try:
        rows = conn.execute(
            "SELECT * FROM backtest_history WHERE notes LIKE ? ORDER BY run_date DESC LIMIT 10",
            (f"%{strategy_name}%",),
        ).fetchall()
        if not rows:
            return f"No backtest history found for '{strategy_name}'."
        return json.dumps([dict(r) for r in rows], default=str)
    finally:
        conn.close()


@tool
def get_portfolio_exposure(portfolio_id: int) -> str:
    """Check position exposure across a portfolio. Returns open positions grouped by ticker and direction."""
    conn = _db()
    try:
        rows = conn.execute(
            "SELECT ticker, asset_class, direction, quantity, entry_price, current_price, unrealized_pnl "
            "FROM portfolio_positions WHERE portfolio_id = ? AND status = 'open' ORDER BY ticker",
            (portfolio_id,),
        ).fetchall()
        if not rows:
            return f"No open positions in portfolio {portfolio_id}."
        return json.dumps([dict(r) for r in rows], default=str)
    finally:
        conn.close()


@tool
def get_metals_spot_prices() -> str:
    """Get current metals market data: Dalio's latest commentary from war_room,
    metals-related news, and cross-asset context (VIX, dollar, macro signals)."""
    conn = _db()
    try:
        sections = []

        # 1. Dalio's latest metals commentary from war_room
        rows = conn.execute(
            "SELECT player_id, symbol, take, created_at FROM war_room "
            "WHERE player_id LIKE '%dalio%' OR player_id LIKE '%metal%' "
            "ORDER BY created_at DESC LIMIT 5"
        ).fetchall()
        if rows:
            sections.append({"source": "dalio_metals_commentary", "count": len(rows),
                             "data": [dict(r) for r in rows]})

        # 2. Metals-related news
        rows = conn.execute(
            "SELECT symbol, headline, source, sentiment, fetched_at FROM market_news "
            "WHERE headline LIKE '%gold%' OR headline LIKE '%silver%' "
            "OR headline LIKE '%metal%' OR headline LIKE '%platinum%' "
            "OR headline LIKE '%copper%' OR headline LIKE '%inflation%' "
            "OR symbol IN ('GLD', 'SLV', 'GDX', 'GOLD', 'NEM') "
            "ORDER BY fetched_at DESC LIMIT 15"
        ).fetchall()
        if rows:
            sections.append({"source": "metals_news", "count": len(rows),
                             "data": [dict(r) for r in rows]})

        # 3. Latest CTO briefing for macro context
        row = conn.execute(
            "SELECT briefing, flow_lean, regime, created_at FROM cto_briefings "
            "ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if row:
            sections.append({"source": "cto_macro_context", "data": dict(row)})

        # 4. Options flow lean — macro sentiment indicator
        row = conn.execute(
            "SELECT lean, conviction, net_flow, recorded_at "
            "FROM flow_lean_history ORDER BY recorded_at DESC LIMIT 1"
        ).fetchone()
        if row:
            sections.append({"source": "options_flow_sentiment", "data": dict(row)})

        if not sections:
            return json.dumps({
                "note": "No metals data available yet. Dalio metals commentary runs at 7 AM MST weekdays.",
            })

        return json.dumps(sections, default=str)
    finally:
        conn.close()


@tool
def lookup_lynch_fundamentals(ticker: str) -> str:
    """Peter Lynch fundamental screener. Returns P/E, PEG, gross margin trend,
    cash position, debt ratio, and insider activity for a ticker. The Architect
    MUST call this before writing any strategy to fill the lynch_screen field."""
    conn = _db()
    try:
        row = conn.execute(
            "SELECT data FROM stock_fundamentals WHERE symbol = ? LIMIT 1",
            (ticker.upper(),),
        ).fetchone()
        if not row:
            return json.dumps({
                "ticker": ticker.upper(),
                "error": "No fundamentals data — ticker not in stock_fundamentals table.",
                "lynch_screen": {
                    "pe": "N/A", "peg": "N/A", "gross_margin": "N/A",
                    "cash_position": "N/A", "debt_ratio": "N/A", "insider_activity": "N/A",
                },
                "na_count": 6,
                "speculative": True,
            })

        d = json.loads(row["data"])

        def _val(key):
            v = d.get(key)
            if v is None or v == {} or v == "":
                return "N/A"
            return v

        screen = {
            "pe": _val("pe_trailing") if _val("pe_trailing") != "N/A" else _val("pe_forward"),
            "peg": _val("peg_ratio"),
            "gross_margin": _val("gross_margin"),
            "cash_position": _val("total_cash"),
            "debt_ratio": _val("debt_to_equity"),
            "insider_activity": _val("insider_pct"),
        }
        na_count = sum(1 for v in screen.values() if v == "N/A")

        result = {
            "ticker": ticker.upper(),
            "lynch_screen": screen,
            "na_count": na_count,
            "speculative": na_count > 3,
        }
        # Add bonus context Lynch would care about
        for extra in ("revenue_growth", "earnings_growth", "profit_margin",
                      "free_cash_flow", "current_ratio", "sector", "industry"):
            v = _val(extra)
            if v != "N/A":
                result[extra] = v

        return json.dumps(result, default=str)
    finally:
        conn.close()


@tool
def save_strategy(strategy_json: str) -> str:
    """Save a strategy to the crew_strategies table. Always increments version, never overwrites existing entries."""
    conn = _db()
    try:
        data = json.loads(strategy_json)
        name = data.get("name", "unnamed_strategy")

        # Get next version for this strategy name
        row = conn.execute(
            "SELECT MAX(version) as max_v FROM crew_strategies WHERE name = ?",
            (name,),
        ).fetchone()
        next_version = (row["max_v"] or 0) + 1

        conn.execute(
            """INSERT INTO crew_strategies (
                name, version, status, asset_class, direction, thesis,
                entry_rules, exit_rules, stop_loss_rule, position_size_rule,
                target_tickers, option_strategy, spread_config,
                conviction_score, critic_score, critic_notes,
                backtest_sharpe, backtest_max_drawdown, backtest_win_rate, backtest_profit_factor,
                scout_brief, architect_reasoning, commander_decision
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                name, next_version,
                data.get("status", "draft"),
                data.get("asset_class", "stock"),
                data.get("direction", "long"),
                data.get("thesis", ""),
                json.dumps(data.get("entry_rules", {})),
                json.dumps(data.get("exit_rules", {})),
                data.get("stop_loss_rule", ""),
                data.get("position_size_rule", ""),
                json.dumps(data.get("target_tickers", [])),
                data.get("option_strategy"),
                json.dumps(data.get("spread_config")) if data.get("spread_config") else None,
                data.get("conviction_score"),
                data.get("critic_score"),
                data.get("critic_notes"),
                data.get("backtest_sharpe"),
                data.get("backtest_max_drawdown"),
                data.get("backtest_win_rate"),
                data.get("backtest_profit_factor"),
                data.get("scout_brief"),
                data.get("architect_reasoning"),
                data.get("commander_decision"),
            ),
        )
        conn.commit()
        strategy_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        return json.dumps({"saved": True, "id": strategy_id, "name": name, "version": next_version})
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Live discovery feed — pulls from the arena's scanner tables
# ---------------------------------------------------------------------------

@tool
def get_live_discoveries(_: str = "") -> str:
    """Pull live market opportunities discovered by the arena's scanners RIGHT NOW.

    Combines four sources:
    - Discovery Scanner: unusual volume, short squeezes, gapping stocks (last 6h)
    - Universe Scan: AI-scored tickers from last nightly scan (top 20 by score)
    - AI Signals: recent BUY signals from the war room crew (last 24h)
    - Smart Money: multi-AI convergence buys (last 48h)

    Call this FIRST before scanning news to see what the ship's sensors are tracking.
    """
    conn = _db()
    try:
        sections = []

        # 1. Discovery Scanner — recent unusual activity
        rows = conn.execute(
            "SELECT symbol, trigger_type, price, change_pct, volume, rel_volume, "
            "short_float, details, detected_at FROM discoveries "
            "WHERE detected_at > datetime('now', '-6 hours') "
            "ORDER BY detected_at DESC LIMIT 25"
        ).fetchall()
        if rows:
            sections.append({
                "source": "discovery_scanner",
                "note": "Stocks flagged for unusual volume, gaps, or short squeeze in last 6h",
                "count": len(rows),
                "data": [dict(r) for r in rows],
            })

        # 2. Universe Scan — top AI-scored tickers from nightly scan
        rows = conn.execute(
            "SELECT ticker, close, volume_ratio, rsi, score, signals, gap_pct, scan_date "
            "FROM universe_scan ORDER BY score DESC, scan_date DESC LIMIT 20"
        ).fetchall()
        if rows:
            sections.append({
                "source": "universe_scan",
                "note": "Top AI-scored tickers from nightly universe scan",
                "count": len(rows),
                "data": [dict(r) for r in rows],
            })

        # 3. AI Signals — recent BUY signals from the war room
        rows = conn.execute(
            "SELECT player_id, symbol, signal, confidence, reasoning, asset_type, created_at "
            "FROM signals WHERE signal = 'BUY' "
            "AND created_at > datetime('now', '-24 hours') "
            "ORDER BY confidence DESC, created_at DESC LIMIT 20"
        ).fetchall()
        if rows:
            sections.append({
                "source": "ai_buy_signals",
                "note": "BUY signals from arena AI crew in last 24h",
                "count": len(rows),
                "data": [dict(r) for r in rows],
            })

        # 4. Smart Money — multi-AI convergence buys
        rows = conn.execute(
            "SELECT symbol, buyers, detected_at FROM smart_money_signals "
            "WHERE detected_at > datetime('now', '-48 hours') "
            "ORDER BY detected_at DESC LIMIT 10"
        ).fetchall()
        if rows:
            sections.append({
                "source": "smart_money_convergence",
                "note": "Tickers where 3+ AI models agree on BUY in last 48h",
                "count": len(rows),
                "data": [dict(r) for r in rows],
            })

        # 5. Strategy signals from nightly strategy engine
        rows = conn.execute(
            "SELECT ticker, strategy_name, signal_type, confidence, entry_price, "
            "stop_price, target_price, notes, scan_date FROM strategy_signals "
            "ORDER BY confidence DESC, scan_date DESC LIMIT 15"
        ).fetchall()
        if rows:
            sections.append({
                "source": "strategy_signals",
                "note": "Technical strategy signals from nightly scan",
                "count": len(rows),
                "data": [dict(r) for r in rows],
            })

        if not sections:
            return json.dumps({
                "note": "No live discoveries yet — scanners may not have run today.",
                "suggestion": "Use scan_finviz_elite with 'gainers' or 'unusual_volume' for live Finviz data.",
            })

        return json.dumps(sections, default=str)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Alpaca Paper Trade Executor — Commander's execution tool
# ---------------------------------------------------------------------------

@tool
def execute_paper_trade(trade_json: str) -> str:
    """Execute a paper trade on the Alpaca Paper Trading account ($100k account).

    trade_json fields:
      "ticker"          (required) — stock symbol, e.g. "NVDA"
      "direction"       (required) — "long" (buy) or "short" (sell short)
      "dollar_amount"   (optional) — USD to invest, default $2000 (2% of account)
      "qty"             (optional) — exact share count, overrides dollar_amount
      "stop_loss_pct"   (optional) — stop loss % below entry, e.g. 5.0 = 5%
      "take_profit_pct" (optional) — take profit % above entry, e.g. 15.0 = 15%
      "strategy_id"     (optional) — links trade to a crew_strategies row
      "notes"           (optional) — reasoning for the trade

    Safety: NEVER routes to human-managed or tracking-only portfolios.
    Default destination remains Alpaca Paper (portfolio_id=1).
    Position sizing: default $2000/trade. Max $5000/trade without explicit approval.

    Example:
      {"ticker": "NVDA", "direction": "long", "dollar_amount": 2000,
       "stop_loss_pct": 7, "take_profit_pct": 20, "notes": "AI momentum breakout"}
    """
    try:
        params = json.loads(trade_json)
    except (json.JSONDecodeError, TypeError) as e:
        return json.dumps({"error": f"Invalid JSON: {e}"})

    ticker = str(params.get("ticker", "")).upper().strip()
    direction = str(params.get("direction", "long")).lower().strip()
    if not ticker:
        return json.dumps({"error": "ticker is required"})
    if direction not in ("long", "short"):
        return json.dumps({"error": "direction must be 'long' or 'short'"})

    try:
        from portfolios.manager import PortfolioManager

        pm = PortfolioManager()
        target_portfolio = pm.get_portfolio(1)
        execution = pm.can_execute(target_portfolio)
        if not execution["allowed"]:
            portfolio_name = (target_portfolio or {}).get("name", "portfolio_id=1")
            return json.dumps({
                "executed": False,
                "error": f"BLOCKED: '{portfolio_name}' is {execution['reason']}.",
            })
    except Exception as e:
        return json.dumps({"executed": False, "error": f"Portfolio guard error: {e}"})

    # --- Get Alpaca account status and current price ---
    try:
        import sys
        sys.path.insert(0, os.path.expanduser("~/autonomous-trader"))
        from engine.alpaca_bridge import AlpacaBridge
        bridge = AlpacaBridge()
        acct = bridge.status()
        if not acct.get("connected"):
            return json.dumps({"error": "Alpaca Paper not connected. Check ALPACA_API_KEY / ALPACA_SECRET_KEY."})
        equity = acct.get("equity", 100000)
        buying_power = acct.get("buying_power", equity)
    except Exception as e:
        return json.dumps({"error": f"Alpaca init error: {e}"})

    # --- Validate ticker is tradeable on Alpaca before wasting LLM time ---
    try:
        asset = bridge.client.get_asset(ticker)
        if not asset.tradable:
            return json.dumps({
                "executed": False,
                "error": f"{ticker} is not tradeable on Alpaca (status={asset.status}). Try a different ticker.",
                "ticker": ticker,
            })
    except Exception as e:
        err_str = str(e)
        if "asset not found" in err_str.lower() or "40410000" in err_str:
            return json.dumps({
                "executed": False,
                "error": f"{ticker} not found on Alpaca — possibly delisted or OTC. Try a different ticker.",
                "ticker": ticker,
            })
        # Non-fatal: proceed and let the order attempt surface the real error

    # --- Get pre-order price for qty sizing ---
    try:
        from engine.market_data import get_stock_price
        price_data = get_stock_price(ticker)
        pre_price = float(price_data.get("price", 0) or 0)
    except Exception:
        pre_price = 0

    # --- Calculate share quantity ---
    qty = params.get("qty")
    if qty:
        qty = int(qty)
    else:
        dollar_amount = float(params.get("dollar_amount", 2000))
        dollar_amount = min(dollar_amount, 5000)  # Hard cap per trade
        if pre_price > 0:
            qty = max(1, int(dollar_amount / pre_price))
        else:
            qty = max(1, int(dollar_amount / 100))  # rough fallback

    if qty <= 0:
        return json.dumps({"error": f"Calculated qty={qty} — price lookup failed for {ticker}"})

    # Safety cap: no more than $5000 per trade
    if pre_price > 0 and qty * pre_price > 5000:
        qty = max(1, int(5000 / pre_price))

    # --- Execute on Alpaca Paper ---
    if direction == "long":
        order_result = bridge.buy(ticker, qty)
    else:
        order_result = bridge.sell(ticker, qty)

    if order_result.get("error"):
        return json.dumps({
            "executed": False,
            "error": order_result["error"],
            "ticker": ticker,
            "qty": qty,
        })

    order_id = order_result.get("order_id", "")

    # --- Poll for actual fill price (up to 10s) ---
    import time as _time
    entry_price = 0.0
    for _attempt in range(10):
        _time.sleep(1)
        try:
            filled = bridge.client.get_order_by_id(order_id)
            if filled.filled_avg_price:
                entry_price = float(filled.filled_avg_price)
                break
        except Exception:
            pass
    # Fallback to pre-order price if still unfilled (extended hours / illiquid)
    if entry_price == 0.0:
        entry_price = pre_price

    # --- Stop/take-profit — use Architect's values or sane defaults ---
    stop_loss_pct = float(params.get("stop_loss_pct", 0) or 0)
    take_profit_pct = float(params.get("take_profit_pct", 0) or 0)
    if stop_loss_pct == 0:
        stop_loss_pct = 3.0   # default -3%
    if take_profit_pct == 0:
        take_profit_pct = 6.0  # default +6%

    if entry_price > 0:
        stop_loss_price = round(entry_price * (1 - stop_loss_pct / 100), 4)
        take_profit_price = round(entry_price * (1 + take_profit_pct / 100), 4)
    else:
        stop_loss_price = None
        take_profit_price = None

    notes = params.get("notes", "")
    strategy_id = params.get("strategy_id")
    if strategy_id:
        notes = f"strategy_id={strategy_id} | {notes}"

    conn = _db()
    try:
        conn.execute(
            """INSERT INTO portfolio_positions
               (portfolio_id, ticker, asset_class, direction, quantity,
                entry_price, current_price, stop_loss, take_profit, notes, status)
               VALUES (1, ?, 'stock', ?, ?, ?, ?, ?, ?, ?, 'open')""",
            (ticker, direction, qty, entry_price, entry_price,
             stop_loss_price, take_profit_price,
             f"alpaca_order={order_id} | {notes}"),
        )
        conn.commit()
        pos_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    finally:
        conn.close()

    return json.dumps({
        "executed": True,
        "ticker": ticker,
        "direction": direction,
        "qty": qty,
        "entry_price": entry_price,
        "dollar_value": round(qty * entry_price, 2),
        "stop_loss": stop_loss_price,
        "take_profit": take_profit_price,
        "alpaca_order_id": order_id,
        "portfolio_position_id": pos_id,
        "account_equity": round(equity, 2),
        "account_buying_power": round(buying_power, 2),
    })


# ---------------------------------------------------------------------------
# Position sync — updates current_price + unrealized_pnl from live Alpaca data
# ---------------------------------------------------------------------------

def sync_positions_from_alpaca() -> dict:
    """Pull live prices from Alpaca Paper (stocks) and Yahoo Finance (metals) and
    update portfolio_positions + portfolios.current_balance.

    Called by the arena scanner on each cycle. Returns a summary dict.
    """
    import sys
    sys.path.insert(0, os.path.expanduser("~/autonomous-trader"))

    results = {"alpaca_synced": 0, "metals_synced": 0}

    # ── 1. Alpaca Paper stocks (portfolio_id=1) ─────────────────────────────
    try:
        from engine.alpaca_bridge import AlpacaBridge
        bridge = AlpacaBridge()
        if bridge.client:
            alpaca_positions = bridge.client.get_all_positions()
            live = {p.symbol.upper(): {
                "current_price": float(p.current_price or 0),
                "unrealized_pnl": float(p.unrealized_pl or 0),
            } for p in alpaca_positions}

            conn = _db()
            try:
                rows = conn.execute(
                    "SELECT id, ticker FROM portfolio_positions "
                    "WHERE status='open' AND portfolio_id=1"
                ).fetchall()
                for row in rows:
                    ticker = row["ticker"].upper()
                    if ticker in live:
                        conn.execute(
                            """UPDATE portfolio_positions
                               SET current_price=?, unrealized_pnl=?,
                                   updated_at=CURRENT_TIMESTAMP
                               WHERE id=?""",
                            (live[ticker]["current_price"],
                             live[ticker]["unrealized_pnl"], row["id"]),
                        )
                        results["alpaca_synced"] += 1
                conn.commit()
            finally:
                conn.close()
            results["alpaca_tickers"] = list(live.keys())

            # NOTE: Do NOT sync current_balance from Alpaca equity.
            # portfolios.id=1 current_balance stays at $100,000 (permanent benchmark).
            # P&L is tracked separately via portfolio_positions unrealized_pnl.
            # Sync account connection status only.
            try:
                acct = bridge.status()
                if acct.get("connected") and acct.get("portfolio_value"):
                    equity = round(float(acct["portfolio_value"]), 2)
                    results["alpaca_equity"] = equity  # log for debugging only, no DB write
                    conn2 = _db()
                    try:
                        conn2.execute(
                            "UPDATE portfolios SET updated_at=CURRENT_TIMESTAMP WHERE id=1",
                        )
                        conn2.commit()
                    finally:
                        conn2.close()
            except Exception as eq_err:
                results["alpaca_equity_error"] = str(eq_err)
    except Exception as e:
        results["alpaca_error"] = str(e)

    # ── 2. Physical Metals (portfolio_id=5, asset_class='metal') ────────────
    # Ticker map: XAUUSD → GOLD (GC=F), XAGUSD → SILVER (SI=F)
    METALS_TICKER_MAP = {"XAUUSD": "GOLD", "XAGUSD": "SILVER"}
    try:
        from engine.metals_tracker import get_spot_prices
        spot = get_spot_prices()  # {"GOLD": {"price": ...}, "SILVER": {"price": ...}}

        conn = _db()
        try:
            rows = conn.execute(
                "SELECT id, ticker, metal_oz, entry_price "
                "FROM portfolio_positions "
                "WHERE status='open' AND asset_class='metal'"
            ).fetchall()

            metals_market_value = 0.0
            for row in rows:
                metal_key = METALS_TICKER_MAP.get(row["ticker"].upper())
                if not metal_key or metal_key not in spot:
                    continue
                price = float(spot[metal_key]["price"])
                oz = float(row["metal_oz"] or 0)
                entry = float(row["entry_price"] or 0)
                unrealized = round((price - entry) * oz, 2) if entry > 0 else 0.0
                metals_market_value += price * oz
                conn.execute(
                    """UPDATE portfolio_positions
                       SET current_price=?, unrealized_pnl=?,
                           updated_at=CURRENT_TIMESTAMP
                       WHERE id=?""",
                    (round(price, 4), unrealized, row["id"]),
                )
                results["metals_synced"] += 1

            # Update portfolio balance = current market value of stack
            if metals_market_value > 0:
                conn.execute(
                    "UPDATE portfolios SET current_balance=?, updated_at=CURRENT_TIMESTAMP "
                    "WHERE id=5",
                    (round(metals_market_value, 2),),
                )
            conn.commit()
        finally:
            conn.close()
        results["metals_spot"] = {k: round(spot[k]["price"], 2)
                                   for k in ("GOLD", "SILVER") if k in spot}
    except Exception as e:
        results["metals_error"] = str(e)

    # ── 3. Super Agent (portfolio_id=6) — aggregate balance of non-human portfolios ──
    try:
        conn3 = _db()
        try:
            row = conn3.execute(
                "SELECT COALESCE(SUM(current_balance),0) as total "
                "FROM portfolios WHERE is_human=0 AND is_active=1 AND id != 6"
            ).fetchone()
            super_balance = round(float(row["total"]), 2)
            conn3.execute(
                "UPDATE portfolios SET current_balance=?, updated_at=CURRENT_TIMESTAMP WHERE id=6",
                (super_balance,),
            )
            conn3.commit()
            results["super_agent_balance"] = super_balance
        finally:
            conn3.close()
    except Exception as e:
        results["super_agent_error"] = str(e)

    results["synced"] = results["alpaca_synced"] + results["metals_synced"]
    return results


# ---------------------------------------------------------------------------
# Finviz Elite scan tool — thin CrewAI wrapper around shared/finviz_scanner.py
# ---------------------------------------------------------------------------

@tool
def scan_finviz_elite(scan_type: str) -> str:
    """Live Finviz Elite market scanner. Returns real-time screener data.

    scan_type options:
      "gainers"         — top % gainers today (USA, price > $1, vol > 100K)
      "losers"          — top % losers today
      "unusual_volume"  — stocks trading > 2x their average volume
      "oversold_rsi"    — stocks with RSI(14) below 30 (deeply oversold)
      "golden_cross"    — stocks where 50-day SMA crossed above 200-day SMA
      "insider_buys"    — latest insider purchase transactions
      "sector_heat"     — sector performance heat map (week/month/YTD)
      "news"            — latest market-moving headlines from Finviz
      "earnings_today"  — stocks reporting earnings today with high volume
    """
    return scan_finviz(scan_type)


# ---------------------------------------------------------------------------
# Agent factories
# ---------------------------------------------------------------------------

def create_scout(llm: str = None) -> Agent:
    """Market Scout — scans news, congress trades, sector momentum, metals. Bold contrarian plays."""
    return Agent(
        role="Market Scout",
        goal=(
            "Scan all available market data — news, congressional trades, sector momentum, "
            "metals prices — and surface the boldest, most contrarian opportunity with a clear thesis. "
            "Look for asymmetric risk/reward setups that the consensus is missing."
        ),
        backstory=(
            "You are Lt. Uhura, Communications Officer aboard the USS TradeMinds. "
            "You intercept every signal from the market — news feeds, congressional filings, "
            "sector rotations, metals flows. You have a nose for when the crowd is wrong "
            "and the courage to flag contrarian plays. Your scout briefs are legendary for "
            "surfacing gems before they move."
        ),
        tools=[get_live_discoveries, query_news_sources, query_congress_trades, get_metals_spot_prices, scan_finviz_elite, uoa_alerts_tool, uoa_flow_tool, uoa_put_call_scan_tool],
        llm=llm or LLM_CONFIG["scout"],
        verbose=True,
    )


def create_architect(llm: str = None) -> Agent:
    """Strategy Architect — writes executable strategies as structured JSON."""
    return Agent(
        role="Strategy Architect",
        goal=(
            "Take the Scout's opportunity brief and design a complete, executable trading strategy. "
            "BEFORE writing any strategy, you MUST run lookup_lynch_fundamentals for every target ticker "
            "to fill in the Lynch Fundamental Screen: P/E, PEG, gross margin trend, cash position, "
            "debt ratio, and insider activity. Include the lynch_screen object in your strategy JSON. "
            "If more than 3 of the 6 Lynch fields are N/A, mark the strategy as speculative=true. "
            "Output structured JSON with entry rules, exit rules, position sizing, stop loss, "
            "lynch_screen, speculative flag, and vehicle selection (stock long/short, options "
            "calls/puts, spreads including verticals/iron condors/butterflies, or metals allocation). "
            "Every strategy must have a clear edge and defined risk."
        ),
        backstory=(
            "You are Mr. Spock, Science Officer aboard the USS TradeMinds. "
            "Where the Scout finds opportunity, you engineer precision. You translate bold ideas "
            "into logically structured strategies with exact entry/exit conditions, position sizing "
            "rules, and risk parameters. You choose the optimal vehicle — sometimes a simple stock "
            "position, sometimes a complex options spread — always guided by logic, never emotion. "
            "You follow Peter Lynch's discipline: never commit capital without knowing the fundamentals. "
            "You ALWAYS call lookup_lynch_fundamentals for each ticker before designing the strategy."
        ),
        tools=[save_strategy, query_backtest_history, lookup_lynch_fundamentals],
        llm=llm or LLM_CONFIG["architect"],
        verbose=True,
    )


def create_backtester(llm: str = None) -> Agent:
    """Backtester — validates strategies via VectorBT Holodeck simulations."""
    return Agent(
        role="Backtester",
        goal=(
            "Take the Architect's strategy and validate it through rigorous backtesting. "
            "Report Sharpe ratio, max drawdown, win rate, and profit factor. "
            "ALL results MUST be saved to backtest_history — never skip, never delete. "
            "Flag any strategy with Sharpe < 0.5 or max drawdown > 25%."
        ),
        backstory=(
            "You are Lt. Chekov, running the Holodeck simulations aboard the USS TradeMinds. "
            "Every strategy gets tested against historical data before it touches real capital. "
            "You are meticulous — every backtest result is recorded in the ship's logs forever. "
            "You've seen too many strategies that looked great on paper blow up in practice."
        ),
        tools=[query_backtest_history],
        llm=llm or LLM_CONFIG["backtester"],
        verbose=True,
    )


def create_critic(llm: str = None) -> Agent:
    """Strategy Critic — scores strategies 1-10 on multiple dimensions."""
    return Agent(
        role="Strategy Critic",
        goal=(
            "Score the strategy 1-10 on: thesis strength, risk/reward ratio, backtest quality, "
            "market regime fit, vehicle choice appropriateness, and portfolio fit. "
            "Any dimension scoring below 6 means REJECT with specific revision requests. "
            "SPECULATIVE GATE: If the strategy has speculative=true (>3 Lynch fundamental "
            "fields are N/A), raise the approval threshold from 6 to 8 overall. Speculative "
            "strategies need exceptional conviction to pass — note which fundamentals are "
            "missing and justify why the edge holds despite data gaps. "
            "Maximum 2 revision cycles — after that, final verdict stands. "
            "Check portfolio exposure to avoid concentration risk."
        ),
        backstory=(
            "You are Commander Riker, First Officer aboard the USS TradeMinds. "
            "Your job is to challenge every strategy before it reaches the Captain. "
            "You've seen overconfident strategies wreck portfolios. You score honestly, "
            "reject mercilessly when warranted, but also recognize genuine edge when you see it. "
            "You follow Peter Lynch's rule: if you can't explain the fundamentals, you can't "
            "own the stock. Thin fundamental data is a red flag that demands higher conviction. "
            "You always check the ship's current exposure before approving new positions."
        ),
        tools=[get_portfolio_exposure, query_backtest_history],
        llm=llm or LLM_CONFIG["critic"],
        verbose=True,
    )


def create_commander(llm: str = None) -> Agent:
    """Commander — final go/no-go decision, saves strategy, and EXECUTES paper trade."""
    return Agent(
        role="Commander",
        goal=(
            "Make the final go/no-go decision on the strategy. "
            "If GO: (1) call save_strategy with status='approved', then "
            "(2) call execute_paper_trade to open a real position on Alpaca Paper. "
            "Use $2000 per trade unless the strategy specifies otherwise. "
            "Always set stop_loss_pct and take_profit_pct from the strategy's rules. "
            "CRITICAL: NEVER deploy to Webull (is_human=1) — Alpaca Paper ONLY. "
            "If NO-GO: call save_strategy with status='rejected' and explain why. "
            "A pipeline that ends without calling execute_paper_trade on a GO decision is INCOMPLETE."
        ),
        backstory=(
            "You are Captain Kirk, commanding the USS TradeMinds. "
            "The buck stops with you. You weigh the Scout's thesis, the Architect's design, "
            "the Backtester's data, and the Critic's assessment. You trust your crew but verify. "
            "You NEVER risk the Captain's personal Webull account on automated strategies — "
            "that's your money, your rules. Unproven strategies go to Alpaca Paper to earn their stripes. "
            "When you say GO, you mean it — you call execute_paper_trade and open the position. "
            "Words without action are worthless. Execute."
        ),
        tools=[get_portfolio_exposure, save_strategy, execute_paper_trade],
        llm=llm or LLM_CONFIG["commander"],
        verbose=True,
    )
