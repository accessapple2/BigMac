#!/usr/bin/env python3
"""HM-CN Track B.5 — Side-by-side diagnostic.

For each agent in config.AI_PLAYERS, compares:
  - main.py's hardcoded model (from the OllamaProvider list, lines 102-141)
  - what engine/agent_routing.py would return (reads ai_players.model_id)

Reports any disagreements with full context. Read-only — does NOT modify code
or DB.

Usage:
    venv/bin/python3 scripts/hmcn_side_by_side.py
    venv/bin/python3 scripts/hmcn_side_by_side.py --markdown > reports/HM-CN_side_by_side_initial.md

Design ref: reports/HM-CN_agent_routing_design.md
Inventory ref: reports/HM-CN_module_inventory.md
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from config import AI_PLAYERS  # noqa: E402
from engine.agent_routing import compare_against_main_py_inventory  # noqa: E402


# Hardcoded model mapping from main.py:102-141 (Arena providers list, post-HEAD 380c4d4).
# Excludes ollama-llama (GroqProvider) and dayblade-0dte (initialize_dayblade).
# 'ollama-local' is the player_id when no kwarg is passed at main.py:103.
MAIN_PY_INVENTORY = {
    "ollama-local":     "qwen3:14b",          # main.py:103 (no player_id arg → defaults)
    "ollama-gemma27b":  "ministral-3:3b",     # main.py:104
    "ollama-deepseek":  "deepseek-r1:14b",    # main.py:105
    "ollama-qwen3":     "ministral-3:3b",     # main.py:106
    "ollama-kimi":      "ministral-3:3b",     # main.py:107 (HM-CN Phase 1 patch)
    "ollama-glm4":      "ministral-3:3b",     # main.py:108
    "ollama-plutus":    "ministral-3:3b",     # main.py:109 (timeout=300)
    "dayblade-sulu":    "phi3:mini",          # main.py:111
    "mlx-qwen3":        "ministral-3:3b",     # main.py:116
    "qwen3-8b-4o":      "qwen3:8b",           # main.py:121
    "qwen3-8b-o3":      "qwen3:8b",           # main.py:122
    "qwen3-14b-pro":    "qwen3:8b",           # main.py:125 (HM-CN Phase 1 patch)
    "qwen3-8b-flash":   "qwen3:8b",           # main.py:126
    "options-sosnoff":  "qwen3:8b",           # main.py:127
    "energy-arnold":    "ministral-3:3b",     # main.py:128
    "ollama-coder":     "devstral-small-2",   # main.py:130
    "super-agent":      "qwen3:8b",           # main.py:132
    "dalio-metals":     "ministral-3:3b",     # main.py:134
    "qwen3-8b-sonnet":  "qwen3:8b",           # main.py:136
    "qwen-coder-haiku": "qwen2.5-coder:7b",   # main.py:137
    "qwen3-14b-grok3":  "qwen3:14b",          # main.py:139
    "deepseek-7b-grok4":"qwen3:8b",           # main.py:140
    "cto-grok42":       "qwen2.5-coder:7b",   # main.py:141
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--markdown", action="store_true",
                        help="Emit Markdown report instead of plain text")
    args = parser.parse_args()

    disagreements = compare_against_main_py_inventory(AI_PLAYERS, MAIN_PY_INVENTORY)

    if args.markdown:
        _emit_markdown(disagreements)
    else:
        _emit_plain(disagreements)

    # Exit non-zero if any disagreements (for cron use)
    sys.exit(0 if not disagreements else 1)


def _emit_plain(disagreements):
    print(f"=== HM-CN side-by-side: main.py vs agent_routing.py (DB-aware) ===")
    print(f"Compared {len(MAIN_PY_INVENTORY)} main.py entries against AI_PLAYERS + DB.")
    print()
    if not disagreements:
        print("NO DISAGREEMENTS. main.py and ai_players.model_id are in sync.")
        return
    print(f"FOUND {len(disagreements)} disagreement(s):")
    print()
    for d in disagreements:
        print(f"  agent_id:           {d['agent_id']}")
        print(f"  main.py model:      {d['main_py_model']}")
        print(f"  ai_players.model_id:{d['ai_players_model_id']}")
        print(f"  halt_mode:          {d['halt_mode']}")
        print(f"  AI_PLAYERS model:   {d['config_AI_PLAYERS_model']}")
        print()


def _emit_markdown(disagreements):
    print("# HM-CN Side-by-Side Initial Report")
    print()
    print(f"Compared **{len(MAIN_PY_INVENTORY)}** main.py entries against `config.AI_PLAYERS` + `ai_players.model_id`.")
    print()
    if not disagreements:
        print("**NO DISAGREEMENTS.** main.py and ai_players.model_id are in sync.")
        return
    print(f"## Found {len(disagreements)} disagreement(s)")
    print()
    print("| agent_id | main.py model | ai_players.model_id | halt_mode | config AI_PLAYERS model |")
    print("|---|---|---|---|---|")
    for d in disagreements:
        print(f"| `{d['agent_id']}` | `{d['main_py_model']}` | `{d['ai_players_model_id']}` | `{d['halt_mode']}` | `{d['config_AI_PLAYERS_model']}` |")
    print()
    print("Each row is a silent bypass — `main.py` constructs the OllamaProvider with one model while the DB canonical (set by setup_db or a runtime UPDATE) records a different model. Live trader follows `main.py`.")


if __name__ == "__main__":
    main()
