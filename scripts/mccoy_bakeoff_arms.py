#!/usr/bin/env python3
"""HM-XO-PLAN-2026-09 Phase 2 -- "McCoy Rank" bakeoff arm registry + smoke test.

Builds the bakeoff arms (provider instances) and confirms each one answers a
one-token-style prompt. Does NOT run the bakeoff itself (no screened-list
iteration, no scoring, no top/bottom-20 basket) -- that's the next piece,
gated on the Phase 2 acceptance number per docs/XO_PLAN_2026-09.md.

Arms built here (4 of the 5 in the plan -- Claude/arm 2 already exists via
engine/xo_brief.py::call_claude and isn't part of this script):
  1. qwen3:8b            -- OllamaProvider, olliemax (today's McCoy baseline model)
  3. MiniMax-M3          -- engine/providers/minimax_provider.py, cloud, cost-tracked
  4. Fin-R1              -- OllamaProvider, olliemax, hf.co/mradermacher/Fin-R1-GGUF:Q4_K_M
  5. plutus-v1-real       -- OllamaProvider, olliemax, the restored HM-PLUTUS-V5-WIN checkpoint

None of these are wired to config.AI_PLAYERS / agent_routing.py -- bakeoff
arm only, no fleet seat reads this file.

Usage:
    venv/bin/python3 scripts/mccoy_bakeoff_arms.py            # smoke test all 4 arms
    venv/bin/python3 scripts/mccoy_bakeoff_arms.py --arm fin-r1
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import config  # noqa: E402 -- loads .env via dotenv (MINIMAX_API_KEY, OLLAMA_URL)
from engine.providers.ollama_provider import OllamaProvider  # noqa: E402
from engine.providers.minimax_provider import MiniMaxProvider  # noqa: E402

ONE_TOKEN_PROMPT = "Reply with exactly one word: PONG"

# Fin-R1 is a Chinese-origin finance model. 2026-09-10 live smoke (see the
# Captain's own parallel test) code-switched into Chinese mid-answer on a
# free-form finance prompt -- capability isn't in question, English format
# reliability is. This registry points at the base pulled tag
# (hf.co/mradermacher/Fin-R1-GGUF:Q4_K_M, per the original pull command);
# an "fin-r1-en" Modelfile variant pinning English output is being validated
# separately and can replace the tag below once confirmed stable. Whichever
# tag runs Phase 2, score its output-language/format-parse rate explicitly
# in parse_decision() -- a model that code-switches mid-answer fails parsing
# regardless of analysis quality, same posture as plutus-v1-real's
# SCORE:/VERDICT:-format risk already flagged in docs/XO_PLAN_2026-09.md.
FIN_R1_MODEL_TAG = "hf.co/mradermacher/Fin-R1-GGUF:Q4_K_M"


def build_arms() -> dict[str, object]:
    return {
        "qwen3:8b": OllamaProvider(
            player_id="bakeoff-qwen3-8b", model="qwen3:8b", url=config.OLLAMA_URL,
        ),
        "plutus-v1-real": OllamaProvider(
            player_id="bakeoff-plutus-v1-real", model="plutus-v1-real", url=config.OLLAMA_URL,
        ),
        "fin-r1": OllamaProvider(
            player_id="bakeoff-fin-r1", model=FIN_R1_MODEL_TAG, url=config.OLLAMA_URL,
        ),
        "minimax-m3": MiniMaxProvider(player_id="bakeoff-minimax-m3"),
    }


def smoke_test(name: str, provider) -> bool:
    t0 = time.time()
    try:
        response = provider.call_model(ONE_TOKEN_PROMPT)
    except Exception as e:
        print(f"[FAIL] {name}: {type(e).__name__}: {e}")
        return False
    wall = time.time() - t0
    preview = (response or "").strip().replace("\n", " ")[:200]
    ok = bool(preview)
    status = "OK" if ok else "FAIL (empty response)"
    print(f"[{status}] {name} wall={wall:.2f}s response={preview!r}")
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arm", choices=["qwen3:8b", "plutus-v1-real", "fin-r1", "minimax-m3"],
                        help="Smoke-test a single arm instead of all four")
    args = parser.parse_args()

    arms = build_arms()
    targets = {args.arm: arms[args.arm]} if args.arm else arms

    results = {name: smoke_test(name, provider) for name, provider in targets.items()}

    minimax = arms.get("minimax-m3")
    if minimax is not None and getattr(minimax, "total_cost_usd", 0):
        print(
            f"\nminimax-m3 spend this run: ${minimax.total_cost_usd:.6f} "
            f"(in={minimax.total_input_tokens} out={minimax.total_output_tokens} tokens)"
        )

    n_ok = sum(results.values())
    print(f"\n{n_ok}/{len(results)} arms answered.")
    return 0 if n_ok == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
