"""
TradeMinds — Portfolio Optimizer
==================================
Analyzes a player's current holdings and suggests rebalancing actions:
trim over-weight positions, add to under-weight, flag correlated clusters,
and surface sector concentration risks.

Uses Ollama (qwen3:8b) to synthesize findings into a ranked action list.
Results saved to portfolio_optimizations table in trader.db.

Usage (CLI):
    python -m engine.portfolio_optimizer steve-webull
    python -m engine.portfolio_optimizer claude-sonnet --top 5

Usage (import):
    from engine.portfolio_optimizer import run_optimizer
    result = run_optimizer("steve-webull")
"""

from __future__ import annotations

import json
import logging
import sqlite3
import sys
from datetime import datetime

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

OLLAMA_BASE = "http://localhost:11434"
TRADER_DB = "data/trader.db"
DEFAULT_MODEL = "qwen3.5:9b"

# Weight thresholds
MAX_SINGLE_WEIGHT = 0.25      # flag any position > 25% of portfolio
MIN_SINGLE_WEIGHT = 0.02      # ignore dust positions < 2%
MAX_SECTOR_WEIGHT = 0.40      # flag sector concentration > 40%
CORRELATION_THRESHOLD = 0.75  # flag pairs with r > 0.75

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [portfolio_optimizer] %(levelname)s: %(message)s",
)
logger = logging.getLogger("portfolio_optimizer")


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
            CREATE TABLE IF NOT EXISTS portfolio_optimizations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id TEXT NOT NULL,
                portfolio_value REAL,
                position_count INTEGER,
                concentration_flags TEXT,
                sector_flags TEXT,
                correlation_flags TEXT,
                actions_json TEXT,
                summary TEXT,
                model_used TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.commit()


# ---------------------------------------------------------------------------
# Portfolio data
# ---------------------------------------------------------------------------

def _get_portfolio(player_id: str) -> dict:
    """Get positions with live prices."""
    try:
        from engine.paper_trader import get_portfolio
        return get_portfolio(player_id)
    except Exception as e:
        logger.warning(f"paper_trader.get_portfolio failed: {e}")

    # Fallback: read directly from DB
    with _conn() as c:
        rows = c.execute(
            "SELECT symbol, qty, avg_price FROM positions WHERE player_id=? AND asset_type='stock'",
            (player_id,),
        ).fetchall()
    return {
        "positions": [
            {"symbol": r["symbol"], "qty": r["qty"], "avg_price": r["avg_price"],
             "market_value": r["qty"] * r["avg_price"]}
            for r in rows
        ],
        "cash": 0,
    }


def _enrich_prices(positions: list[dict]) -> list[dict]:
    """Add live prices where missing."""
    symbols = [p["symbol"] for p in positions if not p.get("last_price")]
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
                p["unrealized_pnl"] = (p["last_price"] - p.get("avg_price", 0)) * p["qty"]
                p["pnl_pct"] = ((p["last_price"] / p["avg_price"]) - 1) * 100 if p.get("avg_price") else 0
    except Exception as e:
        logger.warning(f"Price enrichment failed: {e}")
    return positions


# ---------------------------------------------------------------------------
# Analysis helpers
# ---------------------------------------------------------------------------

def _calc_weights(positions: list[dict]) -> tuple[list[dict], float]:
    """Add weight field to each position. Returns (positions, total_value)."""
    total = sum(p.get("market_value", 0) for p in positions)
    if total <= 0:
        return positions, 0
    for p in positions:
        p["weight"] = p.get("market_value", 0) / total
    return positions, total


def _concentration_flags(positions: list[dict]) -> list[str]:
    """Flag positions that are over- or under-weight."""
    flags = []
    for p in positions:
        w = p.get("weight", 0)
        sym = p["symbol"]
        if w > MAX_SINGLE_WEIGHT:
            flags.append(f"{sym} over-weight at {w:.0%} (limit {MAX_SINGLE_WEIGHT:.0%})")
    return flags


def _sector_flags(positions: list[dict]) -> list[str]:
    """Flag sectors that exceed MAX_SECTOR_WEIGHT."""
    try:
        from engine.sector_tracker import get_sector
    except ImportError:
        return []

    buckets: dict[str, float] = {}
    for p in positions:
        sector = get_sector(p["symbol"]) or "Unknown"
        buckets[sector] = buckets.get(sector, 0) + p.get("weight", 0)

    flags = []
    for sector, w in sorted(buckets.items(), key=lambda x: -x[1]):
        if w > MAX_SECTOR_WEIGHT:
            flags.append(f"{sector} sector at {w:.0%} (limit {MAX_SECTOR_WEIGHT:.0%})")
    return flags


def _correlation_flags(positions: list[dict]) -> list[str]:
    """Flag highly correlated pairs."""
    symbols = [p["symbol"] for p in positions if p.get("weight", 0) >= MIN_SINGLE_WEIGHT]
    if len(symbols) < 2:
        return []
    try:
        from engine.correlation import get_correlation_matrix
        data = get_correlation_matrix(symbols, period=60, threshold=CORRELATION_THRESHOLD)
        groups = data.get("high_correlation_groups", [])
        flags = []
        for g in groups:
            members = g.get("symbols", [])
            if len(members) >= 2:
                flags.append(f"Correlated cluster: {', '.join(members)} (r>{CORRELATION_THRESHOLD})")
        return flags
    except Exception:
        return []


def _regime_context() -> str:
    try:
        from engine.regime_detector import detect_regime
        r = detect_regime()
        return r.get("regime", "UNKNOWN")
    except Exception:
        return "UNKNOWN"


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
                "think": False,   # disable chain-of-thought for qwen3 — we want fast JSON
                "options": {"temperature": 0.5, "num_predict": 2048},
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
# Prompt
# ---------------------------------------------------------------------------

def _build_prompt(
    player_id: str,
    positions: list[dict],
    total_value: float,
    cash: float,
    regime: str,
    conc_flags: list[str],
    sect_flags: list[str],
    corr_flags: list[str],
) -> str:
    pos_lines = "\n".join(
        f"  {p['symbol']}: {p.get('weight', 0):.1%} weight, "
        f"${p.get('market_value', 0):,.0f} value, "
        f"pnl {p.get('pnl_pct', 0):+.1f}%"
        for p in sorted(positions, key=lambda x: -x.get("weight", 0))
        if p.get("weight", 0) >= MIN_SINGLE_WEIGHT
    )
    flag_lines = "\n".join(
        [f"  CONCENTRATION: {f}" for f in conc_flags]
        + [f"  SECTOR: {f}" for f in sect_flags]
        + [f"  CORRELATION: {f}" for f in corr_flags]
    ) or "  None detected"

    return f"""You are a portfolio manager reviewing {player_id}'s holdings.

PORTFOLIO SNAPSHOT
- Total value: ${total_value:,.0f}
- Cash: ${cash:,.0f}
- Market regime: {regime}
- Positions:
{pos_lines}

RISK FLAGS
{flag_lines}

Generate a prioritized list of up to 5 rebalancing actions. For each action specify:
- action: BUY | SELL | TRIM | HOLD | REBALANCE
- symbol: ticker
- rationale: 1 sentence
- urgency: HIGH | MEDIUM | LOW

Respond with ONLY valid JSON:
{{
  "actions": [
    {{"action": "TRIM", "symbol": "XYZ", "rationale": "...", "urgency": "HIGH"}},
    ...
  ],
  "summary": "1-2 sentence overall portfolio assessment"
}}"""


# ---------------------------------------------------------------------------
# Core runner
# ---------------------------------------------------------------------------

def run_optimizer(
    player_id: str,
    top_n: int = 5,
    model: str = DEFAULT_MODEL,
) -> dict:
    """Run portfolio optimization for player_id. Returns result dict."""
    init_db()
    logger.info(f"Optimizing portfolio for {player_id}...")

    portfolio = _get_portfolio(player_id)
    positions = portfolio.get("positions", [])
    cash = portfolio.get("cash", 0) or portfolio.get("buying_power", 0) or 0

    if not positions:
        logger.warning(f"No positions found for {player_id}")
        return {"error": f"No positions for {player_id}"}

    positions = _enrich_prices(positions)
    positions, total_value = _calc_weights(positions)

    conc_flags = _concentration_flags(positions)
    sect_flags = _sector_flags(positions)
    corr_flags = _correlation_flags(positions)
    regime = _regime_context()

    prompt = _build_prompt(
        player_id, positions, total_value, cash,
        regime, conc_flags, sect_flags, corr_flags,
    )

    raw = _call_ollama(prompt, model)
    if not raw:
        return {"error": "No response from model"}

    data = _parse_json(raw)
    actions = data.get("actions", [])[:top_n]
    summary = data.get("summary", "")

    result = {
        "player_id": player_id,
        "portfolio_value": total_value,
        "cash": cash,
        "position_count": len(positions),
        "regime": regime,
        "concentration_flags": conc_flags,
        "sector_flags": sect_flags,
        "correlation_flags": corr_flags,
        "actions": actions,
        "summary": summary,
        "model_used": model,
        "created_at": datetime.now().isoformat(),
    }

    # Save to DB
    try:
        with _conn() as c:
            c.execute(
                """INSERT INTO portfolio_optimizations
                   (player_id, portfolio_value, position_count,
                    concentration_flags, sector_flags, correlation_flags,
                    actions_json, summary, model_used)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    player_id, total_value, len(positions),
                    json.dumps(conc_flags), json.dumps(sect_flags),
                    json.dumps(corr_flags), json.dumps(actions),
                    summary, model,
                ),
            )
            c.commit()
        logger.info(f"Optimization saved for {player_id}")
    except Exception as e:
        logger.warning(f"DB save failed: {e}")

    return result


# ---------------------------------------------------------------------------
# Pretty printer
# ---------------------------------------------------------------------------

URGENCY_COLORS = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}
ACTION_LABELS = {"BUY": "BUY  ", "SELL": "SELL ", "TRIM": "TRIM ",
                 "HOLD": "HOLD ", "REBALANCE": "REBAL"}


def _print_result(result: dict):
    pid = result["player_id"]
    val = result.get("portfolio_value", 0)
    cash = result.get("cash", 0)

    print(f"\n{'=' * 62}")
    print(f"PORTFOLIO OPTIMIZER: {pid}")
    print(f"Value: ${val:,.0f}  |  Cash: ${cash:,.0f}  |  "
          f"Positions: {result['position_count']}  |  Regime: {result['regime']}")
    print("=" * 62)

    flags = result["concentration_flags"] + result["sector_flags"] + result["correlation_flags"]
    if flags:
        print("\n⚠  Risk Flags:")
        for f in flags:
            print(f"   • {f}")

    actions = result.get("actions", [])
    if actions:
        print(f"\n📋  Recommended Actions ({len(actions)}):")
        for a in actions:
            label = ACTION_LABELS.get(a.get("action", ""), a.get("action", "?"))
            sym = a.get("symbol", "?").ljust(6)
            urgency = a.get("urgency", "LOW")
            dot = URGENCY_COLORS.get(urgency, "⚪")
            print(f"   {dot} {label} {sym}  {a.get('rationale', '')}")
    else:
        print("\n  No actions recommended.")

    if result.get("summary"):
        print(f"\n💬  {result['summary']}")
    print("=" * 62)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="TradeMinds Portfolio Optimizer")
    parser.add_argument("player_id", help="Player ID (e.g. steve-webull, claude-sonnet)")
    parser.add_argument("--top", type=int, default=5, help="Max actions to return (default: 5)")
    parser.add_argument("--model", default=DEFAULT_MODEL,
                        help=f"Ollama model (default: {DEFAULT_MODEL})")
    args = parser.parse_args()

    result = run_optimizer(args.player_id, args.top, args.model)
    if "error" in result:
        print(f"Error: {result['error']}")
        sys.exit(1)
    _print_result(result)
