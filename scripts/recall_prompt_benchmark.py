#!/usr/bin/env python3
"""HM-XO-PLAN-2026-09 Phase 2, item B.1 -- recall-in-prompt benchmark.

Measures added tokens per prompt and added wall time for
engine/recall_prompt.py::build_recall_prompt_section(), standalone (NOT
through the live trading path -- see that module's docstring for why it
isn't wired into build_prompt() yet).

Usage:
    venv/bin/python3 scripts/recall_prompt_benchmark.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import config  # noqa: E402
config.RECALL_IN_PROMPT_ENABLED = True  # force on for this standalone benchmark only

from engine.recall_prompt import build_recall_prompt_section, _cached_corpus  # noqa: E402

CANDIDATES = [
    ("NVDA", "SWING", "Earnings beat with guidance raise, golden cross, high volume"),
    ("AMD", "SWING", "Breaking below 20-day SMA on above-average volume, bearish MACD cross"),
    ("TSLA", "SCALP", "Pre-market gap up on delivery numbers beat"),
]


def estimate_tokens(text: str) -> int:
    """Same rough estimator engine/cost_tracker.py uses (~4 chars/token) --
    consistent with how the rest of this repo already reports token counts."""
    return max(0, len(text) // 4)


def main() -> int:
    corpus = _cached_corpus()
    print(f"recall_corpus size: {len(corpus)} rows\n")
    if not corpus:
        print("EMPTY CORPUS -- nothing to benchmark against. Run scripts/recall_refresh.py first.")
        return 1

    # HM-RECALL-URL-STILL-LOCAL-2026-09-10: earlier tonight this benchmark
    # used a stand-in embedding because engine/setup_similarity_signal.py's
    # OLLAMA_EMBED_URL was silently still pointed at bigmac's decommissioned
    # local Ollama (RECALL_OLLAMA_URL was never actually set in the
    # 2026-09-09 migration to olliemax). Fixed -- now hits the real bge-m3
    # endpoint on olliemax via config.OLLAMA_URL. Real embed-call latency
    # below is the genuine end-to-end number, not a KNN-only partial.
    print("Using the real bge-m3 embed endpoint (config.OLLAMA_URL -> olliemax).\n")

    for symbol, tf, setup in CANDIDATES:
        t0 = time.time()
        section = build_recall_prompt_section(symbol, tf, setup)
        wall = time.time() - t0
        toks = estimate_tokens(section)
        print(f"[{symbol} {tf}] wall={wall:.3f}s added_tokens~={toks}")
        print(f"  section: {section!r}" if section else "  section: '' (abstained)")
        print()

    print("--- repeat pass (bge-m3 still resident -- within the 2min keep_alive window) ---")
    for symbol, tf, setup in CANDIDATES:
        t0 = time.time()
        section = build_recall_prompt_section(symbol, tf, setup)
        wall = time.time() - t0
        toks = estimate_tokens(section)
        print(f"[{symbol} {tf}] wall={wall:.3f}s added_tokens~={toks}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
