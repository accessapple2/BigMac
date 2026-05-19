# HM-ROUND-5 Item 2 — Bridge Consensus Producer Audit
Date: 2026-05-18
Decision required: Admiral

## Symptom
Bridge tab Consensus panel: 503/515 tickers stuck at ⚠️ SPLIT, summary line
reads "Spock ▲ BULLISH | Data ■ NEUTRAL | Uhura ■ NEUTRAL". Only Spock is
firing; Data and Uhura both NEUTRAL across the universe.

## Architecture (verified from `engine/consensus.py:168-296`)

| Officer  | Player ID            | Primary source                                           | Fallback                                       |
|----------|----------------------|----------------------------------------------------------|------------------------------------------------|
| Spock    | `deepseek-7b-grok4`  | `cto_advisor.get_latest_briefing()`                      | NEUTRAL on exception                           |
| Data     | `first-officer`      | `engine.first_officer._briefing_cache` (in-process)      | `signals` table WHERE `player_id='mlx-qwen3'` last 24h |
| Uhura    | `ollama-llama`       | `war_room` table WHERE `player_id='ollama-llama'` last 24h | `signals` table WHERE `player_id='ollama-llama'` last 24h |

## Per-producer findings

### Spock — FIRING ✅
- bridge_votes: 11 rows, last 2026-05-13 13:09:16 (5 days ago).
- cto_advisor briefing exists → Spock surfaces BULLISH per symptom.
- No action required.

### Data — DARK (finding c, "active but stale")
- `mlx-qwen3` last signal: **2026-05-07 12:22:22** (11 days ago).
- `_briefing_cache` is module-level state in `engine/first_officer.py`; cleared
  on every trader restart, refilled by `update_briefing()` cadence.
- Net: both primary (briefing_cache) and fallback (24h mlx-qwen3 signals) are
  empty → returns NEUTRAL across all tickers.
- **Diagnosis:** mlx-qwen3 producer halted/idle; not the Bridge UI's bug.
- **Decision class:** producer-revival epic.

### Uhura — DARK (finding a, "intentionally halted per zombie set")
- `ollama-llama` halt_mode=`exit_only` since 2026-04-25 (`halt_reason`: "S6
  review: routing zombie, retired 2026-04-25").
- Listed in CLAUDE.md "Zombie Candidates — Future Cleanup" section.
- Last signal: 2026-05-02 02:37:15 (16 days ago).
- `engine/consensus.py:267-269` explicitly documents this:
  > "ollama-llama (Uhura) is currently halted, so this fallback will return
  > empty until Uhura is reactivated. Bridge consensus will return NEUTRAL
  > where it used to surface Uhura's stance — that is correct."
- **Diagnosis:** working as designed. Bridge Consensus reflects fleet
  composition reality.
- **Decision class:** revival-vs-deprecation epic.

## Recommended bank items

1. **HM-DATA-PRODUCER-REVIVAL-DECISION** — Admiral picks: (a) revive
   mlx-qwen3 with a daily emit cadence and confirm `first_officer.update_briefing()`
   firing; (b) reassign Data's stance source to a different live producer
   (e.g. ollama-coder/devstral); (c) accept Data NEUTRAL as permanent and
   redesign the consensus header to drop the officer.

2. **HM-UHURA-PRODUCER-REVIVAL-DECISION** — Admiral picks: (a) revive
   ollama-llama (un-halt, accept routing-zombie reclassification); (b) port
   Uhura stance to a different LLM (llama3.1 per fleet roster aspirational
   model) — separate epic for SEC EDGAR 13F producer rebuild; (c) accept
   Uhura NEUTRAL as permanent and redesign the consensus header.

## Guardrails honored (per draft Item 2 HARD STOPS)
- No `ai_players.halt_mode` modifications.
- No producer restarts.
- No code changes to `engine/consensus.py`.
- This is audit-only — Bridge Consensus 503/515 SPLIT is the **correct
  rendering of fleet reality**, not a UI bug.

## Cross-refs
- `feedback_stale_docstring_misleads_discovery.md` — verified current state
  via DB + code before banking.
- CLAUDE.md "Zombie Candidates" section (ollama-llama listed).
- CLAUDE.md "Bench 4 — Ghost Trading" (Uhura's aspirational role).
