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

    # HM-OLLAMA-SERVE-DOWN-2026-09-10: com.ollama.serve (bge-m3 host,
    # 127.0.0.1:11434) is not running on bigmac right now -- confirmed no
    # process, not in launchctl list, plist present but unloaded. A live
    # embed_candidate() call abstains cleanly (by design) rather than
    # erroring, but that means it can't be benchmarked for real tonight.
    # Using a stored corpus embedding as a stand-in candidate_emb instead,
    # to still measure the KNN + formatting cost honestly. This is NOT the
    # full added-wall-time picture -- the missing piece is flagged below,
    # not estimated or faked.
    stand_in_emb = corpus[0]["emb"]
    print("NOTE: com.ollama.serve is down on bigmac right now (127.0.0.1:11434 unreachable) --")
    print("      live embed_candidate() calls abstain. Using a stored corpus embedding as a")
    print("      stand-in to still measure KNN + formatting cost. Real embed-call latency is")
    print("      UNMEASURED tonight, not included below -- see relay report.\n")

    for symbol, tf, setup in CANDIDATES:
        t0 = time.time()
        section = build_recall_prompt_section(symbol, tf, setup, candidate_emb=stand_in_emb)
        wall = time.time() - t0
        toks = estimate_tokens(section)
        print(f"[{symbol} {tf}] knn+format_wall={wall:.3f}s added_tokens~={toks}")
        print(f"  section: {section!r}" if section else "  section: '' (abstained)")
        print()

    print("--- repeat pass (warm corpus cache) ---")
    for symbol, tf, setup in CANDIDATES:
        t0 = time.time()
        section = build_recall_prompt_section(symbol, tf, setup, candidate_emb=stand_in_emb)
        wall = time.time() - t0
        toks = estimate_tokens(section)
        print(f"[{symbol} {tf}] knn+format_wall={wall:.3f}s added_tokens~={toks}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
