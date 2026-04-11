"""
TradeMinds — Rebalancer
========================
Final pipeline stage: converts portfolio optimizer recommendations into
specific executable trades (shares, dollar amounts) and optionally
sends them to Alpaca.

Pipeline:
  1. Load current holdings from positions table
  2. Load latest portfolio_optimizations result for player
  3. Compare holdings vs recommended actions (BUY/SELL/TRIM/HOLD)
  4. Use qwen3:8b to size each trade (shares, $, % of portfolio)
  5. Save to rebalance_recommendations table
  6. Optional --execute: send trades via paper_trader buy/sell/sell_partial

Usage:
    python -m engine.rebalancer steve-webull
    python -m engine.rebalancer claude-sonnet --execute
    python -m engine.rebalancer gpt-4o --dry-run   (default, same as no --execute)
"""

from __future__ import annotations

import json
import logging
import sqlite3
import sys
from datetime import datetime
from typing import Any

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

OLLAMA_BASE = "http://localhost:11434"
TRADER_DB = "data/trader.db"
DEFAULT_MODEL = "qwen3.5:9b"

# Max single-trade size as % of portfolio (safety cap)
MAX_TRADE_PCT = 0.20
# Min trade size — ignore recs smaller than this
MIN_TRADE_USD = 25.0
# Default TRIM reduces position by this fraction
DEFAULT_TRIM_FRACTION = 0.33

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [rebalancer] %(levelname)s: %(message)s",
)
logger = logging.getLogger("rebalancer")


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(TRADER_DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def init_db():
    with _conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS rebalance_recommendations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id TEXT NOT NULL,
                optimization_id INTEGER,
                symbol TEXT NOT NULL,
                action TEXT NOT NULL,
                shares REAL,
                price REAL,
                dollar_amount REAL,
                pct_of_portfolio REAL,
                urgency TEXT,
                rationale TEXT,
                executed INTEGER DEFAULT 0,
                execute_result TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.commit()


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def _load_positions(player_id: str) -> list[dict]:
    """Load current stock positions from DB."""
    with _conn() as c:
        rows = c.execute(
            """SELECT symbol, qty, avg_price
               FROM positions
               WHERE player_id=? AND asset_type='stock'""",
            (player_id,),
        ).fetchall()
    return [{"symbol": r["symbol"], "qty": r["qty"], "avg_price": r["avg_price"]} for r in rows]


def _load_latest_optimization(player_id: str) -> dict | None:
    """Load the most recent portfolio_optimizations row for player."""
    with _conn() as c:
        row = c.execute(
            """SELECT id, portfolio_value, actions_json, summary, created_at
               FROM portfolio_optimizations
               WHERE player_id=?
               ORDER BY id DESC LIMIT 1""",
            (player_id,),
        ).fetchone()
    if not row:
        return None
    actions = []
    try:
        actions = json.loads(row["actions_json"] or "[]")
    except json.JSONDecodeError:
        pass
    return {
        "id": row["id"],
        "portfolio_value": row["portfolio_value"],
        "actions": actions,
        "summary": row["summary"],
        "created_at": row["created_at"],
    }


def _enrich_prices(positions: list[dict]) -> list[dict]:
    """Add live last_price and market_value to each position."""
    symbols = [p["symbol"] for p in positions]
    if not symbols:
        return positions
    try:
        from engine.market_data import get_bulk_prices
        prices = get_bulk_prices(symbols)
        for p in positions:
            sym = p["symbol"]
            if sym in prices and prices[sym].get("price"):
                p["last_price"] = prices[sym]["price"]
                p["market_value"] = p["qty"] * p["last_price"]
                p["change_pct"] = prices[sym].get("change_pct", 0)
            else:
                p["last_price"] = p.get("avg_price", 0)
                p["market_value"] = p["qty"] * p["last_price"]
    except Exception as e:
        logger.warning(f"Price enrichment failed: {e}")
        for p in positions:
            p.setdefault("last_price", p.get("avg_price", 0))
            p["market_value"] = p["qty"] * p["last_price"]
    return positions


def _total_value(positions: list[dict], cash: float) -> float:
    return cash + sum(p.get("market_value", 0) for p in positions)


# ---------------------------------------------------------------------------
# Ollama
# ---------------------------------------------------------------------------

def _call_ollama(prompt: str, model: str) -> str:
    try:
        resp = requests.post(
            f"{OLLAMA_BASE}/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
                "think": False,
                "options": {"temperature": 0.4, "num_predict": 2048},
            },
            timeout=300,
        )
        resp.raise_for_status()
        return resp.json().get("response", "")
    except requests.Timeout:
        logger.error(f"Ollama timeout ({model})")
    except Exception as e:
        logger.error(f"Ollama error: {e}")
    return ""


def _parse_json(text: str) -> dict:
    import re
    cleaned = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
    if cleaned.startswith("```"):
        lines = [l for l in cleaned.split("\n") if not l.strip().startswith("```")]
        cleaned = "\n".join(lines)
    start = cleaned.find("{")
    end = cleaned.rfind("}") + 1
    if start != -1 and end > start:
        try:
            return json.loads(cleaned[start:end])
        except json.JSONDecodeError:
            pass
    logger.warning(f"Could not parse JSON: {text[:200]}...")
    return {}


# ---------------------------------------------------------------------------
# Trade sizing
# ---------------------------------------------------------------------------

def _fetch_buy_prices(actions: list[dict], positions: list[dict]) -> dict[str, float]:
    """Fetch live prices for BUY targets not already in the portfolio."""
    held = {p["symbol"] for p in positions}
    buy_symbols = [a["symbol"] for a in actions
                   if a.get("action", "").upper() == "BUY" and a["symbol"] not in held]
    if not buy_symbols:
        return {}
    try:
        from engine.market_data import get_bulk_prices
        prices = get_bulk_prices(buy_symbols)
        return {sym: data["price"] for sym, data in prices.items() if data.get("price")}
    except Exception:
        return {}


def _build_sizing_prompt(
    player_id: str,
    positions: list[dict],
    actions: list[dict],
    portfolio_value: float,
    cash: float,
    buy_prices: dict[str, float] | None = None,
) -> str:
    pos_lines = "\n".join(
        f"  {p['symbol']}: {p['qty']:.2f} shares @ ${p.get('last_price', p['avg_price']):.2f} "
        f"= ${p.get('market_value', 0):,.0f}"
        for p in positions
    )
    # Add live prices for BUY targets not in portfolio
    buy_price_lines = ""
    if buy_prices:
        buy_price_lines = "\nLIVE PRICES FOR BUY TARGETS\n" + "\n".join(
            f"  {sym}: ${price:.2f}" for sym, price in buy_prices.items()
        )
    action_lines = "\n".join(
        f"  {a['action']} {a['symbol']} [{a.get('urgency','?')}]: {a.get('rationale','')}"
        for a in actions
    )

    return f"""You are a trade execution specialist sizing rebalancing trades for {player_id}.

CURRENT PORTFOLIO (total ${portfolio_value:,.0f}, cash ${cash:,.0f})
{pos_lines}{buy_price_lines}

RECOMMENDED ACTIONS
{action_lines}

SIZE EACH TRADE. Rules:
- TRIM: sell {DEFAULT_TRIM_FRACTION:.0%} of position (or more if HIGH urgency)
- SELL: sell entire position
- BUY: use available cash, max {MAX_TRADE_PCT:.0%} of portfolio per trade
- HOLD: no trade, 0 shares
- Minimum trade size: ${MIN_TRADE_USD:.0f}
- Quantities must be whole shares (round down)

Respond with ONLY valid JSON:
{{
  "trades": [
    {{
      "symbol": "MSFT",
      "action": "TRIM",
      "shares": 2,
      "price": 415.50,
      "dollar_amount": 831.00,
      "pct_of_portfolio": 0.042,
      "rationale": "Trimming 33% of MSFT to reduce concentration"
    }}
  ]
}}"""


# ---------------------------------------------------------------------------
# Rule-based fallback sizer (used if Ollama fails)
# ---------------------------------------------------------------------------

def _rule_based_size(
    action_item: dict,
    positions: list[dict],
    portfolio_value: float,
    cash: float,
) -> dict | None:
    """Size a single trade using simple rules — no AI needed."""
    action = action_item.get("action", "").upper()
    symbol = action_item.get("symbol", "")
    pos = next((p for p in positions if p["symbol"] == symbol), None)
    price = pos["last_price"] if pos else action_item.get("_live_price", 0)

    if action == "HOLD" or not symbol:
        return None

    if action in ("SELL",) and pos:
        shares = pos["qty"]
        dollar = shares * price
    elif action == "TRIM" and pos:
        frac = 0.50 if action_item.get("urgency") == "HIGH" else DEFAULT_TRIM_FRACTION
        shares = max(1, int(pos["qty"] * frac))
        dollar = shares * price
    elif action == "BUY" and price > 0:
        budget = min(cash * 0.5, portfolio_value * MAX_TRADE_PCT)
        shares = max(1, int(budget / price))
        dollar = shares * price
    else:
        return None

    if dollar < MIN_TRADE_USD:
        return None

    return {
        "symbol": symbol,
        "action": action,
        "shares": shares,
        "price": price,
        "dollar_amount": round(dollar, 2),
        "pct_of_portfolio": round(dollar / portfolio_value, 4) if portfolio_value else 0,
        "rationale": action_item.get("rationale", ""),
    }


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

def _execute_trade(player_id: str, trade: dict) -> str:
    """Send a single trade to paper_trader. Returns result string."""
    action = trade["action"].upper()
    symbol = trade["symbol"]
    price = trade.get("price", 0)
    shares = trade.get("shares", 0)
    rationale = trade.get("rationale", "Rebalancer")

    if not price or not shares:
        return "SKIPPED: missing price or shares"

    try:
        from engine.paper_trader import buy, sell, sell_partial, get_position

        if action == "BUY":
            result = buy(player_id, symbol, price, qty=shares,
                         reasoning=rationale, confidence=0.6, timeframe="SWING")
        elif action == "SELL":
            result = sell(player_id, symbol, price, reasoning=rationale, confidence=0.6)
        elif action == "TRIM":
            pos = get_position(player_id, symbol)
            if pos and shares < pos["qty"]:
                result = sell_partial(player_id, symbol, price, qty=shares,
                                      reasoning=rationale, confidence=0.6)
            else:
                result = sell(player_id, symbol, price, reasoning=rationale, confidence=0.6)
        else:
            return "SKIPPED: action not executable"

        if result is None:
            return "BLOCKED: paper_trader returned None (human player or guard triggered)"
        return f"OK: {json.dumps(result)[:120]}"
    except Exception as e:
        return f"ERROR: {e}"


# ---------------------------------------------------------------------------
# Core runner
# ---------------------------------------------------------------------------

def run_rebalancer(
    player_id: str,
    execute: bool = False,
    model: str = DEFAULT_MODEL,
) -> dict:
    """Run full rebalancing pipeline. Returns result dict."""
    init_db()
    logger.info(f"Rebalancing {player_id} (execute={execute})...")

    # 1. Load holdings
    positions = _load_positions(player_id)
    if not positions:
        logger.warning(f"No positions for {player_id}")
        return {"error": f"No positions found for {player_id}"}

    # 2. Load latest optimization
    opt = _load_latest_optimization(player_id)
    if not opt:
        logger.warning(f"No optimization found for {player_id} — run portfolio_optimizer first")
        return {"error": "No optimization found. Run portfolio_optimizer first."}

    age_note = ""
    try:
        from datetime import timezone
        created = datetime.fromisoformat(opt["created_at"])
        age_minutes = (datetime.now() - created).total_seconds() / 60
        if age_minutes > 60:
            age_note = f" (⚠ optimization is {age_minutes:.0f} min old)"
    except Exception:
        pass

    logger.info(f"Using optimization #{opt['id']} from {opt['created_at']}{age_note}")

    # 3. Enrich with live prices
    positions = _enrich_prices(positions)

    # Get cash from paper_trader if possible
    cash = 0.0
    try:
        from engine.paper_trader import get_portfolio
        pf = get_portfolio(player_id)
        cash = pf.get("cash", 0) or pf.get("buying_power", 0) or 0
    except Exception:
        pass

    portfolio_value = _total_value(positions, cash)
    actions = opt["actions"]

    # 4. Size trades via AI
    buy_prices = _fetch_buy_prices(actions, positions)
    prompt = _build_sizing_prompt(player_id, positions, actions, portfolio_value, cash, buy_prices)
    raw = _call_ollama(prompt, model)
    parsed = _parse_json(raw) if raw else {}
    ai_trades = parsed.get("trades", [])

    # Merge AI sizing with optimizer action metadata
    trades: list[dict] = []
    for action_item in actions:
        sym = action_item.get("symbol", "")
        act = action_item.get("action", "").upper()
        urgency = action_item.get("urgency", "LOW")
        rationale = action_item.get("rationale", "")

        if act == "HOLD":
            trades.append({
                "symbol": sym, "action": "HOLD", "shares": 0,
                "price": 0, "dollar_amount": 0, "pct_of_portfolio": 0,
                "urgency": urgency, "rationale": rationale,
            })
            continue

        # Try to find matching AI trade
        ai = next((t for t in ai_trades if t.get("symbol") == sym), None)
        if ai:
            t = dict(ai)
            t["urgency"] = urgency
            t["rationale"] = ai.get("rationale") or rationale
            trades.append(t)
        else:
            # Fallback: rule-based sizing (inject live buy price if available)
            if act == "BUY" and sym in buy_prices:
                action_item = dict(action_item)
                action_item["_live_price"] = buy_prices[sym]
            sized = _rule_based_size(action_item, positions, portfolio_value, cash)
            if sized:
                sized["urgency"] = urgency
                trades.append(sized)
            else:
                logger.warning(f"Could not size trade for {act} {sym} — skipping")

    # 5. Save recommendations to DB
    opt_id = opt["id"]
    saved_ids = []
    with _conn() as c:
        for t in trades:
            row_id = c.execute(
                """INSERT INTO rebalance_recommendations
                   (player_id, optimization_id, symbol, action, shares, price,
                    dollar_amount, pct_of_portfolio, urgency, rationale)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    player_id, opt_id, t["symbol"], t["action"],
                    t.get("shares", 0), t.get("price", 0),
                    t.get("dollar_amount", 0), t.get("pct_of_portfolio", 0),
                    t.get("urgency", "LOW"), t.get("rationale", ""),
                ),
            ).lastrowid
            saved_ids.append(row_id)
        c.commit()
    logger.info(f"Saved {len(trades)} recommendations to DB")

    # 6. Optionally execute
    execute_results = {}
    if execute:
        logger.info(f"Executing {len(trades)} trades for {player_id}...")
        for t in trades:
            if t["action"] == "HOLD" or t.get("shares", 0) == 0:
                continue
            result_str = _execute_trade(player_id, t)
            execute_results[t["symbol"]] = result_str
            logger.info(f"  {t['action']} {t['symbol']}: {result_str[:80]}")

            # Update DB with execute result
            if saved_ids:
                row_id = saved_ids[trades.index(t)]
                with _conn() as c:
                    c.execute(
                        "UPDATE rebalance_recommendations SET executed=1, execute_result=? WHERE id=?",
                        (result_str[:500], row_id),
                    )
                    c.commit()

    return {
        "player_id": player_id,
        "optimization_id": opt_id,
        "portfolio_value": portfolio_value,
        "cash": cash,
        "position_count": len(positions),
        "trades": trades,
        "execute_results": execute_results,
        "executed": execute,
        "created_at": datetime.now().isoformat(),
    }


# ---------------------------------------------------------------------------
# Pretty printer
# ---------------------------------------------------------------------------

URGENCY_ICON = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}
ACTION_PAD = {"BUY": "BUY  ", "SELL": "SELL ", "TRIM": "TRIM ",
              "HOLD": "HOLD ", "REBALANCE": "REBAL"}


def _print_result(result: dict):
    pid = result["player_id"]
    val = result.get("portfolio_value", 0)
    cash = result.get("cash", 0)
    executed = result.get("executed", False)
    trades = result.get("trades", [])

    print(f"\n{'=' * 64}")
    print(f"REBALANCER: {pid}  {'[DRY RUN]' if not executed else '[EXECUTED]'}")
    print(f"Portfolio: ${val:,.0f}  |  Cash: ${cash:,.0f}  |  "
          f"Optimization #{result.get('optimization_id')}")
    print("=" * 64)

    if not trades:
        print("  No trades generated.")
    else:
        print(f"\n  {'ACTION':<8} {'SYMBOL':<8} {'SHARES':>8} {'PRICE':>9} "
              f"{'AMOUNT':>10} {'PCT':>6}  URGENCY")
        print(f"  {'-'*8} {'-'*8} {'-'*8} {'-'*9} {'-'*10} {'-'*6}  {'-'*7}")
        for t in trades:
            act = ACTION_PAD.get(t["action"], t["action"][:5].ljust(5))
            sym = t["symbol"].ljust(8)
            shares = f"{t.get('shares', 0):.0f}".rjust(8) if t.get("shares") else "      —".rjust(8)
            price = f"${t.get('price', 0):.2f}".rjust(9) if t.get("price") else "       —".rjust(9)
            dollar = f"${t.get('dollar_amount', 0):,.0f}".rjust(10) if t.get("dollar_amount") else "         —".rjust(10)
            pct = f"{t.get('pct_of_portfolio', 0):.1%}".rjust(6) if t.get("pct_of_portfolio") else "     —".rjust(6)
            urgency = t.get("urgency", "LOW")
            icon = URGENCY_ICON.get(urgency, "⚪")
            print(f"  {act} {sym} {shares} {price} {dollar} {pct}  {icon} {urgency}")
            if t.get("rationale"):
                print(f"  {'':>50}  ↳ {t['rationale'][:60]}")

    if executed and result.get("execute_results"):
        print(f"\n  Execution Results:")
        for sym, res in result["execute_results"].items():
            print(f"    {sym}: {res[:80]}")

    print("=" * 64)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="TradeMinds Rebalancer")
    parser.add_argument("player_id", help="Player ID (e.g. steve-webull, claude-sonnet)")
    parser.add_argument("--execute", action="store_true",
                        help="Send trades to paper_trader (default: dry-run)")
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="Dry run only — print trades, do not execute (default)")
    parser.add_argument("--model", default=DEFAULT_MODEL,
                        help=f"Ollama model (default: {DEFAULT_MODEL})")
    args = parser.parse_args()

    execute = args.execute and not args.dry_run if args.execute else False
    # If --execute explicitly passed, honour it
    if "--execute" in sys.argv:
        execute = True

    result = run_rebalancer(args.player_id, execute=execute, model=args.model)
    if "error" in result:
        print(f"Error: {result['error']}")
        sys.exit(1)
    _print_result(result)
