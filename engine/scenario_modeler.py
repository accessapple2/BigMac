"""
TradeMinds — Scenario Modeler
==============================
Generates Bull / Base / Bear scenarios for a ticker using local Ollama.
Pulls live price + technicals, recent debate history, and market regime
as context for the AI. Results saved to scenario_models table in trader.db.

Usage (CLI):
    python -m engine.scenario_modeler AAPL
    python -m engine.scenario_modeler AAPL --horizon 30

Usage (import):
    from engine.scenario_modeler import run_scenario_model
    result = run_scenario_model("AAPL", horizon_days=30)
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
DEFAULT_HORIZON = 30  # days

SCENARIOS = ["bull", "base", "bear"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [scenario_modeler] %(levelname)s: %(message)s",
)
logger = logging.getLogger("scenario_modeler")


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
            CREATE TABLE IF NOT EXISTS scenario_models (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                horizon_days INTEGER NOT NULL,
                current_price REAL,
                bull_probability INTEGER,
                bull_target REAL,
                bull_return_pct REAL,
                bull_catalyst TEXT,
                bull_invalidation TEXT,
                base_probability INTEGER,
                base_target REAL,
                base_return_pct REAL,
                base_thesis TEXT,
                base_invalidation TEXT,
                bear_probability INTEGER,
                bear_target REAL,
                bear_return_pct REAL,
                bear_catalyst TEXT,
                bear_invalidation TEXT,
                expected_value_pct REAL,
                regime TEXT,
                model_used TEXT,
                raw_response TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.commit()


# ---------------------------------------------------------------------------
# Context builders
# ---------------------------------------------------------------------------

def _get_price_context(ticker: str) -> dict:
    """Fetch current price, technicals, and RSI."""
    ctx: dict = {"price": None}

    try:
        from engine.market_data import get_stock_price
        data = get_stock_price(ticker)
        if data and data.get("price"):
            ctx["price"] = data["price"]
            ctx["change_pct"] = data.get("change_pct", 0)
            ctx["volume"] = data.get("volume", 0)
    except Exception as e:
        logger.warning(f"Price fetch failed: {e}")

    try:
        from engine.market_data import get_technical_indicators
        tech = get_technical_indicators(ticker)
        if tech:
            ctx["rsi"] = tech.get("rsi")
            ctx["sma_50"] = tech.get("sma_50")
            ctx["sma_200"] = tech.get("sma_200")
            ctx["above_sma50"] = tech.get("above_sma50")
            ctx["above_sma200"] = tech.get("above_sma200")
            ctx["volume_ratio"] = tech.get("volume_ratio")
            ctx["avg_volume_20d"] = tech.get("avg_volume_20d")
    except Exception as e:
        logger.warning(f"Technical indicators fetch failed: {e}")

    # RSI fallback from universe_scan cache
    if not ctx.get("rsi"):
        try:
            with _conn() as c:
                row = c.execute(
                    "SELECT rsi, volume_ratio FROM universe_scan "
                    "WHERE ticker=? ORDER BY id DESC LIMIT 1",
                    (ticker,),
                ).fetchone()
            if row:
                ctx["rsi"] = row["rsi"]
                if not ctx.get("volume_ratio"):
                    ctx["volume_ratio"] = row["volume_ratio"]
        except Exception:
            pass

    return ctx


def _get_regime_context() -> str:
    """Get current market regime as a string."""
    try:
        from engine.regime_detector import detect_regime
        r = detect_regime()
        return r.get("regime", "UNKNOWN")
    except Exception:
        return "UNKNOWN"


def _get_debate_context(ticker: str) -> str:
    """Pull the most recent debate result for this ticker, if any."""
    try:
        with _conn() as c:
            row = c.execute(
                """SELECT picard_decision, adjusted_conviction, picard_synthesis,
                          spock_assessment, crusher_assessment, scotty_assessment
                   FROM debate_history_v2
                   WHERE ticker = ?
                   ORDER BY id DESC LIMIT 1""",
                (ticker,),
            ).fetchone()
        if row:
            return (
                f"Recent debate verdict: {row['picard_decision']} "
                f"(conviction {row['adjusted_conviction']}/10). "
                f"Synthesis: {row['picard_synthesis'] or 'N/A'}. "
                f"Spock: {row['spock_assessment'] or 'N/A'}. "
                f"Crusher: {row['crusher_assessment'] or 'N/A'}."
            )
    except Exception:
        pass
    return "No recent debate data."


# ---------------------------------------------------------------------------
# Ollama call
# ---------------------------------------------------------------------------

def _call_ollama(prompt: str, model: str = DEFAULT_MODEL) -> str:
    """Synchronous Ollama call — returns raw text."""
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "think": False,   # disable chain-of-thought — we want fast JSON
        "options": {
            "temperature": 0.6,
            "num_predict": 2048,
        },
    }
    try:
        resp = requests.post(
            f"{OLLAMA_BASE}/api/generate",
            json=payload,
            timeout=300,
        )
        resp.raise_for_status()
        return resp.json().get("response", "")
    except requests.Timeout:
        logger.error(f"Ollama timeout for model {model}")
        return ""
    except Exception as e:
        logger.error(f"Ollama error: {e}")
        return ""


# ---------------------------------------------------------------------------
# JSON extraction (handles <think> blocks and markdown fences)
# ---------------------------------------------------------------------------

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
# Prompt builder
# ---------------------------------------------------------------------------

def _build_prompt(
    ticker: str,
    horizon_days: int,
    price_ctx: dict,
    regime: str,
    debate_ctx: str,
) -> str:
    price_str = f"${price_ctx['price']:.2f}" if price_ctx.get("price") else "unknown"
    chg_str = f"{price_ctx.get('change_pct', 0):+.2f}%" if price_ctx.get("price") else ""

    # Technicals
    rsi = price_ctx.get("rsi")
    rsi_str = f"{rsi:.1f}" if rsi is not None else "N/A"
    rsi_note = " (oversold)" if rsi and rsi < 30 else " (overbought)" if rsi and rsi > 70 else ""

    sma50 = price_ctx.get("sma_50")
    sma200 = price_ctx.get("sma_200")
    sma50_str = f"${sma50:.2f} ({'above' if price_ctx.get('above_sma50') else 'below'})" if sma50 else "N/A"
    sma200_str = f"${sma200:.2f} ({'above' if price_ctx.get('above_sma200') else 'below'})" if sma200 else "N/A"

    vol = price_ctx.get("volume", 0)
    vol_ratio = price_ctx.get("volume_ratio")
    vol_str = f"{vol:,}" if vol else "N/A"
    if vol_ratio:
        vol_str += f"  [{vol_ratio:.1f}x 20d avg]"

    return f"""You are a senior equity analyst. Build a {horizon_days}-day scenario model for {ticker}.

MARKET CONTEXT
- Current price: {price_str} ({chg_str})
- RSI(14):       {rsi_str}{rsi_note}
- SMA-50:        {sma50_str}
- SMA-200:       {sma200_str}
- Volume:        {vol_str}
- Market regime: {regime}
- {debate_ctx}

Generate exactly THREE scenarios. Probabilities must sum to 100.

Respond with ONLY valid JSON in this exact structure:
{{
  "bull": {{
    "probability": <integer 0-100>,
    "price_target": <float>,
    "return_pct": <float>,
    "catalyst": "<1-2 sentence bullish catalyst>",
    "invalidation": "<what would kill this thesis>"
  }},
  "base": {{
    "probability": <integer 0-100>,
    "price_target": <float>,
    "return_pct": <float>,
    "thesis": "<1-2 sentence base case>",
    "invalidation": "<what would break the base case>"
  }},
  "bear": {{
    "probability": <integer 0-100>,
    "price_target": <float>,
    "return_pct": <float>,
    "catalyst": "<1-2 sentence bearish catalyst>",
    "invalidation": "<what would kill the bear thesis>"
  }},
  "expected_value_pct": <probability-weighted average return as float>
}}"""


# ---------------------------------------------------------------------------
# Core runner
# ---------------------------------------------------------------------------

def run_scenario_model(
    ticker: str,
    horizon_days: int = DEFAULT_HORIZON,
    model: str = DEFAULT_MODEL,
    price_override: float | None = None,
) -> dict:
    """Run a full scenario model for ticker. Returns result dict."""
    init_db()
    ticker = ticker.upper()

    logger.info(f"Modeling scenarios for {ticker} over {horizon_days} days...")

    price_ctx = _get_price_context(ticker)
    if price_override is not None:
        price_ctx["price"] = price_override
        logger.info(f"Using price override: ${price_override:.2f}")
    regime = _get_regime_context()
    debate_ctx = _get_debate_context(ticker)

    prompt = _build_prompt(ticker, horizon_days, price_ctx, regime, debate_ctx)
    raw = _call_ollama(prompt, model)

    if not raw:
        logger.error("No response from Ollama")
        return {"error": "No response from model"}

    data = _parse_json(raw)
    if not data or "bull" not in data:
        logger.error("Failed to parse scenario JSON")
        return {"error": "Parse failure", "raw": raw[:500]}

    bull = data.get("bull", {})
    base = data.get("base", {})
    bear = data.get("bear", {})

    # Recalculate expected value if not provided or wrong
    ev = data.get("expected_value_pct")
    if ev is None:
        bp = bull.get("probability", 0) / 100
        bap = base.get("probability", 0) / 100
        bep = bear.get("probability", 0) / 100
        ev = round(
            bp * bull.get("return_pct", 0)
            + bap * base.get("return_pct", 0)
            + bep * bear.get("return_pct", 0),
            2,
        )

    result = {
        "ticker": ticker,
        "horizon_days": horizon_days,
        "current_price": price_ctx.get("price"),
        "regime": regime,
        "bull": bull,
        "base": base,
        "bear": bear,
        "expected_value_pct": ev,
        "model_used": model,
        "created_at": datetime.now().isoformat(),
    }

    # Save to DB
    try:
        with _conn() as c:
            c.execute(
                """INSERT INTO scenario_models (
                    ticker, horizon_days, current_price,
                    bull_probability, bull_target, bull_return_pct,
                    bull_catalyst, bull_invalidation,
                    base_probability, base_target, base_return_pct,
                    base_thesis, base_invalidation,
                    bear_probability, bear_target, bear_return_pct,
                    bear_catalyst, bear_invalidation,
                    expected_value_pct, regime, model_used, raw_response
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    ticker, horizon_days, price_ctx.get("price"),
                    bull.get("probability"), bull.get("price_target"), bull.get("return_pct"),
                    bull.get("catalyst"), bull.get("invalidation"),
                    base.get("probability"), base.get("price_target"), base.get("return_pct"),
                    base.get("thesis"), base.get("invalidation"),
                    bear.get("probability"), bear.get("price_target"), bear.get("return_pct"),
                    bear.get("catalyst"), bear.get("invalidation"),
                    ev, regime, model, raw[:2000],
                ),
            )
            c.commit()
        logger.info(f"Scenario model saved for {ticker}")
    except Exception as e:
        logger.warning(f"DB save failed: {e}")

    return result


# ---------------------------------------------------------------------------
# Pretty printer
# ---------------------------------------------------------------------------

def _print_result(result: dict):
    ticker = result["ticker"]
    price = result.get("current_price")
    price_str = f"${price:.2f}" if price else "N/A"

    print(f"\n{'=' * 60}")
    print(f"SCENARIO MODEL: {ticker}  |  Price: {price_str}  |  "
          f"Horizon: {result['horizon_days']}d  |  Regime: {result['regime']}")
    print("=" * 60)

    for name in SCENARIOS:
        s = result.get(name, {})
        prob = s.get("probability", "?")
        target = s.get("price_target")
        ret = s.get("return_pct")
        target_str = f"${target:.2f}" if target else "N/A"
        ret_str = f"{ret:+.1f}%" if ret is not None else "N/A"
        label = name.upper().ljust(5)
        thesis = s.get("catalyst") or s.get("thesis") or ""
        invalid = s.get("invalidation", "")
        print(f"\n  [{label}]  {prob}% probability  →  {target_str} ({ret_str})")
        print(f"    Thesis:       {thesis}")
        print(f"    Invalidation: {invalid}")

    ev = result.get("expected_value_pct")
    ev_str = f"{ev:+.1f}%" if ev is not None else "N/A"
    print(f"\n  Expected Value: {ev_str}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="TradeMinds Scenario Modeler",
        usage="scenario_modeler.py TICKER [--horizon N] [--price P] [--model MODEL]",
    )
    parser.add_argument("ticker", help="Stock ticker symbol")
    parser.add_argument("--horizon", type=int, default=DEFAULT_HORIZON,
                        help=f"Horizon in days (default: {DEFAULT_HORIZON})")
    parser.add_argument("--price", type=float, default=None,
                        help="Price override — uses live fetch if omitted")
    parser.add_argument("--model", default=DEFAULT_MODEL,
                        help=f"Ollama model (default: {DEFAULT_MODEL})")
    args = parser.parse_args()

    result = run_scenario_model(args.ticker, args.horizon, args.model, args.price)
    if "error" in result:
        print(f"Error: {result['error']}")
        sys.exit(1)
    _print_result(result)
