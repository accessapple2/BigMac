# HM-BE-suffix — Documented as scope-mismatch (no commit)

**Date:** 2026-05-12
**Status:** SKIP (HM-CLOSE-GAP W1.7)
**Auditor:** Scotty (Opus 4.7)

## Finding

Captain's W1.7 directive expected `qwen3-*` legacy variants to consolidate under canonical `qwen3-14b-pro`. The DB state shows the qwen3-prefixed IDs are **distinct players with distinct roles**, not aliases:

| id | display_name | model_id | trades |
|---|---|---|---:|
| `qwen3-8b-sonnet`  | Captain Sisko        | qwen3:8b  | 0 |
| `qwen3-8b-4o`      | Captain Janeway      | qwen3:8b  | 0 |
| `qwen3-8b-o3`      | Lt. Tuvok            | qwen3:8b  | 0 |
| `qwen3-14b-pro`    | "Qwen3 8B Pro"  ⚠️   | qwen3:8b  | 0 |
| `qwen3-8b-flash`   | Lt. Cmdr. Worf       | qwen3:8b  | 92 |
| `qwen3-14b-grok3`  | Ensign Hoshi         | qwen3:8b  | 0 |

Only `qwen3-8b-flash` has trade history (92 trades, recent 2026-04-25 → 2026-05-12). All others are 0-trade roster entries.

## Why a blind UPDATE is unsafe

`UPDATE ai_players SET player_id = 'qwen3-14b-pro' WHERE player_id IN ('qwen3-8b-sonnet', ...)` would collapse 6 distinct players into 1, breaking:
- `bridge_vote.py` references (each ID listed as a voter)
- `super_backtest_v4.py` AGENT_SPECS (each ID has distinct strategy hint)
- `crew_specialization.py` ALPHA_SQUAD members
- `cost_tracker.py` cost rows
- Phase-1/Phase-2/Phase-3 archived trade history (would all attribute to one player)

There is no clear "legacy → canonical" mapping Captain has established. Each ID has a Star Trek persona + distinct role.

## The real qwen3-14b-pro issue is HM-BE-historic Phase 2

The display_name "Qwen3 8B Pro" on the `qwen3-14b-pro` row is the visible artifact of HM-BE-historic's load-bearing finding (commit `8a031b5`):
- `qwen3-14b-pro` ID + display "Qwen3 8B Pro" + model `qwen3:8b` reflect partial cleanup
- `main.py:101` still loads `qwen3:14b` at runtime — the "downgrade" was never executed
- HM-BE-HISTORIC-PHASE2-ANSWER.md (committed `8a031b5`) has Captain's A.2 decision: truth-up to intent — execute the downgrade now

The proper ship venue for qwen3-14b-pro corrections is HM-BE-historic Phase 2, not HM-BE-suffix. Phase 2 is queued for the next session per its own atomic ship plan:
1. Verify qwen3:8b available on inference host
2. Backup trader.db
3. Edit main.py:101 + fallback.py:33 (qwen3:14b → qwen3:8b)
4. Optional super_backtest_v4.py 4 string replaces
5. Optional DB display_name UPDATEs (qwen3-14b-pro → "Dalio Macro 8B", gemini-2.5-pro → "Gemini 2.5 Pro")
6. Restart + verify + CLAUDE.md note + push

## Decision

Per HM-CLOSE-GAP doctrine (sacred-DB rule + each ticket independently revertable), **no commit for W1.7**. The qwen3 work belongs in HM-BE-historic Phase 2, where the rename is precisely scoped (single display_name update, not multi-player consolidation).

## Recommended follow-up

When HM-BE-historic Phase 2 ships (next session), include in its scope:
- Rename `qwen3-14b-pro` display_name from "Qwen3 8B Pro" → "Dalio Macro 8B" (per HM-BE-HISTORIC-ANSWERS.md original ship plan, ~~line~~ already encoded in PHASE2-ANSWER.md)
- Rename `gemini-2.5-pro` display_name from "Qwen3 14B Pro" → "Gemini 2.5 Pro"

Both are single-row UPDATEs, not multi-player consolidations. Safe.
