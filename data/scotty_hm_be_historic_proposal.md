# HM-BE-historic — Backtest Config Alignment (HALT FOR CAPTAIN)

**Author:** Scotty (Opus 4.7)
**Date:** 2026-05-12
**Status:** HALT — discovery exceeded "simple one-file rename" scope per HM-MONSTER3 directive guideline

## Why HALT

The directive said: "If discovery reveals simple config drift fix (one file, one rename): apply with `# === HM-BE-historic ===` anchor. **If deeper alignment work needed: HALT, document, defer.**"

Discovery reveals **multi-line, multi-spot drift across model strings AND DB display_names**, plus uncertainty about whether downstream backtest callers depend on the stale strings. This qualifies as deeper alignment work. Documenting for follow-up epic.

## Drift inventory

### Category 1 — config.py (clean, FYI)

`config.py:166` already documents the HM-BE downgrade with a comment:
```python
{"id": "qwen3-14b-pro",  "name": "Dalio Macro 8B",     "provider": "ollama", "model": "qwen3:8b", ...}
```
✅ Canonical: id stays `qwen3-14b-pro` (legacy), runtime model is `qwen3:8b`.

### Category 2 — engine/super_backtest_v4.py (NEEDS FIX)

`PRIMARY_FLEET_V4` (L82-110) and `SNIPER_FLEET_V4` (L113-119):

| Line | Player | Current `model` field | Should be | Source of truth |
|---|---|---|---|---|
| L93 | qwen3-14b-pro | `"qwen3:14b"` | `"qwen3:8b"` | config.py:166 (HM-BE 2026-04-20) |
| L116 | qwen3-8b-flash | `"qwen3:14b"` | `"qwen3:8b"` | config.py + display name; qwen3-8b-flash is an 8B model |
| L117 | qwen3-14b-pro | `"qwen3:14b"` | `"qwen3:8b"` | Same as L93 |
| L113 | deepseek-7b-grok4 | `"phi4:14b"` | `"deepseek-r1:7b"` | L85 (PRIMARY_FLEET_V4) has the correct string |

**4 string replacements in one file.** All cosmetic *iff* the `model` field is metadata-only. Need to verify before applying — see Risk 1 below.

### Category 3 — DB ai_players.display_name (NEEDS FIX, separate from backtest)

| id | Current display_name | Concern |
|---|---|---|
| `qwen3-14b-pro` | `"Qwen3 8B Pro"` | Honest — reflects the 8B downgrade. Mild inconsistency with config.py's `"Dalio Macro 8B"`. Cosmetic, low priority. |
| `gemini-2.5-pro` | `"Qwen3 14B Pro"` | **Cross-wired** — id says gemini, display says qwen3. Likely stale from a swap. Could mislead UI viewers. |

DB writes are SACRED per CLAUDE.md — any UPDATE here is an explicit one-shot maintenance migration, not a code change.

## Risks before fix

### Risk 1 — model string semantics

The `"model": "qwen3:14b"` strings in super_backtest_v4.py might be:
- (a) **Metadata-only** → safe to rename to `qwen3:8b`. Cosmetic.
- (b) **Load-bearing** → consumed by a callback that loads the actual Ollama model at runtime → renaming would change which model the backtest exercises.

Rough check before any change: grep `engine/super_backtest_v4.py` for `.get("model")` / `["model"]` references, and check `engine/proving_ground.py` and `engine/weekend_backtest.py` for the same `qwen3-14b-pro` IDs to confirm they read from `PRIMARY_FLEET_V4` vs. their own dicts. If load-bearing, the rename actually swaps models in the backtest — possibly invalidating prior OOS numbers documented in CLAUDE.md.

### Risk 2 — DB display_name change

`gemini-2.5-pro` → display rename touches whatever UI surface reads `display_name` (leaderboard, fleet roster, etc.). Low risk but requires a UI re-render check.

## Proposed scope for follow-up epic (HM-BE-historic-α)

**Phase 1 — Verify model-string semantics**
- Read super_backtest_v4.py model-field consumers
- Read proving_ground.py + weekend_backtest.py for crosslink
- Decide: cosmetic rename, or actual model swap that needs OOS re-run

**Phase 2 — super_backtest_v4.py rename (4 lines)**
- Replace 3× `"qwen3:14b"` → `"qwen3:8b"` at L93, L116, L117
- Replace `"phi4:14b"` → `"deepseek-r1:7b"` at L113
- Anchor `# === HM-BE-historic ===`

**Phase 3 — DB display cleanup (one-shot SQL)**
- `UPDATE ai_players SET display_name = 'Dalio Macro 8B' WHERE id = 'qwen3-14b-pro';`
- `UPDATE ai_players SET display_name = 'Gemini 2.5 Pro' WHERE id = 'gemini-2.5-pro';` (or whatever the canonical name should be)
- Run only with Captain approval; reversible via the old display_name shown above

## Captain Q

1. Approve Phase 1 verification next session?
2. If model strings are load-bearing (Risk 1b), is the right call: (a) rename anyway + accept that OOS numbers shift, or (b) leave drift in place and document the dual-name fact in CLAUDE.md?
3. Phase 3 DB cleanup — approve in same session as Phase 2, or separate ticket?

## Cross-references

- HM-BE (origin): config.py:166 comment "HM-BE: name aligned with model (was '14B'; downgraded to 8B on 2026-04-20 swap-storm cleanup)"
- engine/cost_tracker.py:49 already aligned with the downgrade
- HM-BE.BF report at data/scotty_hm_bebf_report.md (prior sibling, not re-reviewed in this audit)
