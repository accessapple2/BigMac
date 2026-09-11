#!/usr/bin/env python3
"""HM-XO-PLAN-2026-09 Phase 2 -- "McCoy Rank" bakeoff, first real measurement run.

XO directive (2026-09-11, Priority 3 item 9): run the three-arm bakeoff
(qwen3:8b, Fin-R1, MiniMax-M3) against today's REAL McCoy screened list,
using the REAL fleet prompt (engine.providers.base.build_prompt(), McCoy's
actual persona -- built via a throwaway provider with player_id=
'ollama-plutus' so MODEL_PERSONALITIES resolves correctly, then the exact
same prompt text is sent to each of the 3 candidate models). MEASUREMENT
ONLY -- no arm gets a fleet seat, nothing executes a trade, nothing writes
to any table a live decision path reads. Coordinated with olliemax
("Trip") via COORD_FROM_SCOTTY_mccoy_bakeoff.md before running (real GPU
load: ~10 symbols x 2 local arms = ~20 real generate calls, sequential).

Logs every call to a new, additive-only table (mccoy_bakeoff_log) with
enough context (symbol, price, regime, timestamp) that calibration and
forward-return-per-regime -- the two scoring dimensions that fundamentally
need FUTURE price data -- can be computed in a later pass once that data
exists. This run itself can only score: format validity, wall time, and
(MiniMax only) the reasoning-vs-visible token split + real cost.

Usage:
    .venv/bin/python3 scripts/mccoy_bakeoff_run.py [--n N] [--cost-cap USD]
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import config  # noqa: E402
from engine.providers.ollama_provider import OllamaProvider  # noqa: E402
from engine.providers.minimax_provider import MiniMaxProvider  # noqa: E402

DB_PATH = REPO_ROOT / "data" / "trader.db"
FIN_R1_MODEL_TAG = "hf.co/mradermacher/Fin-R1-GGUF:Q4_K_M"


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mccoy_bakeoff_log (
            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id                 TEXT NOT NULL,
            ts                     TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            symbol                 TEXT NOT NULL,
            price                  REAL,
            regime                 TEXT,
            arm                    TEXT NOT NULL,
            wall_s                 REAL,
            ok                     INTEGER,
            error                  TEXT,
            action                 TEXT,
            confidence             REAL,
            timeframe              TEXT,
            invalidation           TEXT,
            format_valid           INTEGER,
            raw_response           TEXT,
            input_tokens           INTEGER,
            visible_output_tokens  INTEGER,
            reasoning_tokens       INTEGER,
            cost_usd               REAL,
            source                 TEXT NOT NULL DEFAULT 'scripts/mccoy_bakeoff_run.py'
        )
    """)
    conn.commit()


def _build_arms(cost_cap: float) -> dict[str, object]:
    return {
        "qwen3:8b": OllamaProvider(player_id="bakeoff-qwen3-8b", model="qwen3:8b", url=config.OLLAMA_URL),
        "fin-r1": OllamaProvider(player_id="bakeoff-fin-r1", model=FIN_R1_MODEL_TAG, url=config.OLLAMA_URL),
        "minimax-m3": MiniMaxProvider(player_id="bakeoff-minimax-m3", daily_cost_cap=cost_cap),
    }


def _get_screened_symbols(n: int) -> list[str]:
    from engine.mccoy_screen import get_mccoy_screened_symbols
    screen = get_mccoy_screened_symbols()
    symbols = screen.get("symbols", [])
    print(f"[screen] {screen.get('n_found')}/{screen.get('n_requested')} symbols, "
          f"regime={screen.get('regime', {}).get('regime') if screen.get('regime') else '?'}, "
          f"sampling top {n}")
    return symbols[:n]


def _build_real_prompt(builder: OllamaProvider, symbol: str) -> tuple[str, float, str] | None:
    """Returns (prompt, price, regime) or None if data couldn't be gathered for this symbol."""
    from engine.market_data import get_stock_price, get_technical_indicators
    from engine.news_fetcher import get_news_for_symbol
    from engine.paper_trader import get_portfolio
    from engine.regime_router import get_current_regime

    price_data = get_stock_price(symbol)
    if not price_data or not price_data.get("price"):
        return None
    indicators = get_technical_indicators(symbol) or {}
    try:
        news = get_news_for_symbol(symbol, limit=7)
    except Exception:
        news = []
    portfolio_context = get_portfolio("ollama-plutus")
    regime = get_current_regime() or "UNKNOWN"

    prompt = builder.build_prompt(
        symbol, price_data["price"], price_data.get("change_pct", 0.0),
        price_data.get("high", price_data["price"]), price_data.get("low", price_data["price"]),
        portfolio_context, indicators, news,
    )
    return prompt, price_data["price"], regime


def _run_one(name: str, provider, prompt: str, symbol: str, price: float) -> dict:
    t0 = time.time()
    try:
        raw = provider.call_model(prompt)
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "wall_s": time.time() - t0, "raw": None}
    wall = time.time() - t0
    has_decision_line = any(line.strip().lower().startswith("decision:") for line in (raw or "").split("\n"))
    try:
        decision = provider.parse_decision(raw, symbol=symbol, price=price)
        parse_ok = decision.action in ("BUY", "BUY_CALL", "BUY_PUT", "SHORT", "HOLD")
    except Exception as e:
        return {"ok": False, "error": f"parse_decision {type(e).__name__}: {e}",
                "wall_s": wall, "raw": raw}
    return {
        "ok": True, "wall_s": wall, "raw": raw,
        "action": decision.action, "confidence": decision.confidence,
        "timeframe": decision.timeframe, "invalidation": decision.invalidation,
        "format_valid": bool(has_decision_line and parse_ok),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n", type=int, default=10, help="Number of symbols to sample (default 10)")
    parser.add_argument("--cost-cap", type=float, default=2.0, help="MiniMax daily cost cap USD (default 2.0)")
    args = parser.parse_args()

    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}"
    print(f"run_id={run_id}")

    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    _ensure_table(conn)

    symbols = _get_screened_symbols(args.n)
    if not symbols:
        print("[abort] no screened symbols returned -- nothing to run")
        return 1

    # Real McCoy persona, via a throwaway provider whose player_id matches
    # the live seat so base.py's MODEL_PERSONALITIES lookup resolves for
    # real (the bakeoff arms below use bakeoff-* player_ids deliberately,
    # so they DON'T pick up McCoy's persona a second time independently --
    # this builder is the single source of the prompt text, sent verbatim
    # to all three arms, matching the plan doc's "same prompt... run
    # through each" design).
    builder = OllamaProvider(player_id="ollama-plutus", model="qwen3:8b", url=config.OLLAMA_URL)
    arms = _build_arms(args.cost_cap)

    n_calls = 0
    for symbol in symbols:
        built = _build_real_prompt(builder, symbol)
        if built is None:
            print(f"[skip] {symbol}: no price data")
            continue
        prompt, price, regime = built
        print(f"\n=== {symbol} (price=${price:.2f} regime={regime}) ===")
        for arm_name, provider in arms.items():
            result = _run_one(arm_name, provider, prompt, symbol, price)
            n_calls += 1
            if result["ok"]:
                print(f"  [OK]   {arm_name:<12} wall={result['wall_s']:6.2f}s "
                      f"action={result['action']:<8} conf={result['confidence']} "
                      f"format_valid={result['format_valid']}")
            else:
                print(f"  [FAIL] {arm_name:<12} wall={result['wall_s']:6.2f}s error={result['error']}")

            row = {
                "run_id": run_id, "symbol": symbol, "price": price, "regime": regime,
                "arm": arm_name, "wall_s": result["wall_s"], "ok": int(result["ok"]),
                "error": result.get("error"), "action": result.get("action"),
                "confidence": result.get("confidence"), "timeframe": result.get("timeframe"),
                "invalidation": result.get("invalidation"),
                "format_valid": int(result.get("format_valid", False)) if result["ok"] else 0,
                "raw_response": result.get("raw"),
                "input_tokens": None, "visible_output_tokens": None,
                "reasoning_tokens": None, "cost_usd": None,
            }
            if arm_name == "minimax-m3":
                mm = arms["minimax-m3"]
                row["input_tokens"] = mm.total_input_tokens
                row["visible_output_tokens"] = mm.total_visible_output_tokens
                row["reasoning_tokens"] = mm.total_reasoning_tokens
                row["cost_usd"] = mm.total_cost_usd
            conn.execute(
                "INSERT INTO mccoy_bakeoff_log "
                "(run_id, symbol, price, regime, arm, wall_s, ok, error, action, confidence, "
                " timeframe, invalidation, format_valid, raw_response, input_tokens, "
                " visible_output_tokens, reasoning_tokens, cost_usd) "
                "VALUES (:run_id,:symbol,:price,:regime,:arm,:wall_s,:ok,:error,:action,:confidence,"
                " :timeframe,:invalidation,:format_valid,:raw_response,:input_tokens,"
                " :visible_output_tokens,:reasoning_tokens,:cost_usd)",
                row,
            )
            conn.commit()

    # --- Summary ---
    print("\n" + "=" * 100)
    print(f"SUMMARY (run_id={run_id}, {n_calls} calls)")
    print("=" * 100)
    for arm_name in arms:
        rows = conn.execute(
            "SELECT ok, wall_s, format_valid FROM mccoy_bakeoff_log WHERE run_id=? AND arm=?",
            (run_id, arm_name),
        ).fetchall()
        n = len(rows)
        n_ok = sum(r[0] for r in rows)
        avg_wall = sum(r[1] for r in rows if r[1] is not None) / n if n else 0.0
        n_valid = sum(r[2] for r in rows if r[0])
        print(f"  {arm_name:<12} n={n:<3} ok={n_ok:<3} format_valid={n_valid}/{n_ok if n_ok else 0} "
              f"avg_wall={avg_wall:.2f}s")

    minimax = arms["minimax-m3"]
    print(f"\n  minimax-m3 totals: in={minimax.total_input_tokens} "
          f"visible_out={minimax.total_visible_output_tokens} "
          f"reasoning_out={minimax.total_reasoning_tokens} "
          f"cost_usd=${minimax.total_cost_usd:.6f}")
    if minimax.total_input_tokens or minimax.total_reasoning_tokens:
        total_out = minimax.total_visible_output_tokens + minimax.total_reasoning_tokens
        reasoning_frac = (minimax.total_reasoning_tokens / total_out) if total_out else 0.0
        print(f"  minimax-m3 reasoning/visible split: reasoning={reasoning_frac:.1%} of output tokens")

    conn.close()
    print(f"\nLogged to data/trader.db:mccoy_bakeoff_log, run_id={run_id}")
    print("NOTE: calibration and forward-return-per-regime are NOT scored by this run -- "
          "both require future price/outcome data this run cannot have yet. price+regime "
          "are logged per row so a later pass can compute them once that data exists.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
