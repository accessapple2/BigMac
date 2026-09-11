#!/usr/bin/env python3
"""HM-XO-PLAN-2026-09 Phase 2 -- bakeoff readiness check (VERIFY ONLY, does not run the bakeoff).

Sends a real McCoy-shaped prompt (persona + market snippet + the fleet's
exact "Respond with EXACTLY this format" footer from
engine/providers/base.py) through each of the 4 arms built in
scripts/mccoy_bakeoff_arms.py, then runs the SAME parse_decision() the
live fleet uses on the raw response. Reports parse success/failure, wall
time, and (MiniMax only) the reasoning-vs-visible token split and cost.

Fin-R1 is tested twice: once with the plain prompt, once with an
English-pinning instruction prepended, since it code-switched into
Chinese on a plain English question in an earlier smoke test.

Usage:
    venv/bin/python3 scripts/mccoy_bakeoff_readiness_check.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import config  # noqa: E402
from scripts.mccoy_bakeoff_arms import build_arms  # noqa: E402

MCCOY_PERSONA = (
    "You are Dr. McCoy (Bones), Chief Medical Officer aboard USS TradeMinds. Rank: Commander. "
    "You diagnose trades like patients. Your 6 vital sign monitors: "
    "1) RSI divergence, 2) MACD histogram, 3) Volume spike >2x, 4) SMA position, "
    "5) Earnings surprise history, 6) Options flow alignment. Need 3/6 healthy to approve (2/6 in high VIX). "
    "Blunt, caring, occasionally frustrated. Pure math backed by medical metaphors."
)

MARKET_SNIPPET = """Symbol: NVDA
Current Price: $187.42
Change: +3.8%
Day High: $189.10
Day Low: $181.05

Technical Indicators for NVDA:
- [Yahoo] RSI(14): 68.2 [NEUTRAL]
- [Yahoo] MACD: 2.14, Signal: 1.62, Histogram: 0.52 [BULLISH]
- [Yahoo] SMA 50: $172.30 [Price ABOVE]
- [Yahoo] SMA 200: $151.80 [Price ABOVE, +23.47% away]
- [Yahoo] MA Cross: GOLDEN CROSS (50 vs 200)
- [Yahoo] Volume Ratio (vs 20d avg): 2.3x [HIGH]

News:
  *** [Reuters] NVDA beats Q3 earnings estimates, raises guidance on AI demand [HIGH IMPACT]
  - [Bloomberg] Analysts raise price targets following earnings beat

Analyze the news FIRST, then cross-reference with technical indicators. Major news catalysts (earnings, FDA, contracts, insider buying) should significantly increase conviction."""

FORMAT_FOOTER = """

Respond with EXACTLY this format (no extra text):
Decision: BUY or BUY_CALL or BUY_PUT or HOLD
Timeframe: SCALP or SWING or POSITION
Confidence: [number between 0.0 and 1.0]
Reasoning: [2-3 sentences. If BUY: state your THESIS — what is the catalyst, why now, and your exit plan. If HOLD: why this stock doesn't fit your strategy right now.]
Invalidation: [The specific price level that would prove this thesis wrong — a number, not a description. BUY/BUY_CALL: a price BELOW the current price ($187.42). HOLD: write N/A.]

Timeframe guide:
- SCALP: short-term trade (< 1 day), based on intraday/1hr signals, momentum or news play
- SWING: medium-term trade (2-10 days), based on daily/4hr setup, trend continuation
- POSITION: long-term trade (10+ days), based on weekly/daily fundamentals, trend or value"""

MCCOY_PROMPT = f"{MCCOY_PERSONA}\n\n{MARKET_SNIPPET}{FORMAT_FOOTER}"

ENGLISH_PIN_PREFIX = (
    "IMPORTANT: Respond ONLY in English. Do not use any other language "
    "under any circumstances, regardless of your training data.\n\n"
)


def run_one(name: str, provider, prompt: str) -> dict:
    t0 = time.time()
    try:
        raw = provider.call_model(prompt)
    except Exception as e:
        return {"name": name, "ok": False, "error": f"{type(e).__name__}: {e}",
                "wall_s": time.time() - t0}
    wall = time.time() - t0
    try:
        decision = provider.parse_decision(raw, symbol="NVDA", price=187.42)
        parse_ok = decision.action in ("BUY", "BUY_CALL", "BUY_PUT", "SHORT", "HOLD")
        # A raw fallback-default (HOLD, 0.5, reasoning==raw text) with no
        # recognizable "Decision:"/"Confidence:" line in the response is a
        # parser default, not real structured output -- flag it explicitly
        # per the HM-BM bakeoff-harness doctrine (identical fallback shapes
        # are not evidence of real inference).
        has_decision_line = any(
            line.strip().lower().startswith("decision:") for line in raw.split("\n")
        )
    except Exception as e:
        return {"name": name, "ok": False, "error": f"parse_decision {type(e).__name__}: {e}",
                "wall_s": wall, "raw_preview": raw[:300]}
    return {
        "name": name, "ok": True, "wall_s": wall,
        "action": decision.action, "confidence": decision.confidence,
        "timeframe": decision.timeframe, "invalidation": decision.invalidation,
        "has_decision_line": has_decision_line,
        "raw_preview": raw[:300].replace("\n", " | "),
    }


def report(result: dict) -> None:
    name = result["name"]
    if not result["ok"]:
        print(f"[FAIL] {name}: {result.get('error')}")
        if "raw_preview" in result:
            print(f"        raw: {result['raw_preview']!r}")
        return
    fmt_flag = "" if result["has_decision_line"] else "  ⚠ NO 'Decision:' LINE FOUND -- parser-default fallback, not real structured output"
    print(f"[OK]   {name} wall={result['wall_s']:.2f}s action={result['action']} "
          f"confidence={result['confidence']} timeframe={result['timeframe']} "
          f"invalidation={result['invalidation']!r}{fmt_flag}")
    print(f"        raw: {result['raw_preview']!r}")


def main() -> int:
    arms = build_arms()
    arms["minimax-m3"].reasoning_sample_every = 1  # sample every call -- this run is only ~1 call
    print("=" * 100)
    print("BAKEOFF READINESS CHECK -- real McCoy-shaped prompt, live parse_decision() -- VERIFY ONLY")
    print("=" * 100)

    for name in ("qwen3:8b", "plutus-v1-real", "minimax-m3"):
        result = run_one(name, arms[name], MCCOY_PROMPT)
        report(result)
        print()

    print("--- fin-r1: plain prompt ---")
    result_plain = run_one("fin-r1 (plain)", arms["fin-r1"], MCCOY_PROMPT)
    report(result_plain)
    print()

    print("--- fin-r1: English-pinned prompt ---")
    result_pinned = run_one("fin-r1 (english-pinned)", arms["fin-r1"], ENGLISH_PIN_PREFIX + MCCOY_PROMPT)
    report(result_pinned)
    print()

    minimax = arms["minimax-m3"]
    print("=" * 100)
    print(f"minimax-m3 token split this run: in={minimax.total_input_tokens} "
          f"visible_out={minimax.total_visible_output_tokens} "
          f"reasoning_out={minimax.total_reasoning_tokens} "
          f"cost_usd={minimax.total_cost_usd:.6f}")
    if minimax.reasoning_samples:
        s = minimax.reasoning_samples[-1]
        print(f"minimax-m3 reasoning sample (call {s['call_index']}): {s['reasoning_content'][:400]!r}")
    print("=" * 100)
    return 0


if __name__ == "__main__":
    sys.exit(main())
