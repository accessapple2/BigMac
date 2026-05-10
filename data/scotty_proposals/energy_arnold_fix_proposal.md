# Energy-Arnold Parser — Investigation Proposal

**Filed:** 2026-05-10 by Scotty (loose-ends sweep)
**Status:** PROPOSAL ONLY. No code applied. Multi-file scope.
**Scope class:** Investigation → diff proposal → review → apply
**Linked context:** `CLAUDE.md` "energy-arnold — high-volume noise generator" section; `docs/XO_BACKLOG.md` Pending TODOs ("Investigate why `bridge_votes` collection stalled 2026-05-01 13:01").

## What's claimed (from CLAUDE.md)

- 9,632 total signals from `energy-arnold`. Confidence distribution **bimodal**: AVG 0.258. **6,643 (69%) at conf=0.0**, **1,209 (13%) at conf=1.0**. The remainder spreads across real values (0.85, 0.75, 0.65, 0.5, …).
- `bridge_votes` table: 216 votes, **MAX created_at = 2026-05-01 13:01:23**. Collection stalled.
- Audit conclusion: "IMPROVE decision pending parser investigation."

## What's observed today (2026-05-10)

- `data/trader.db::signals` distribution (all-time, all rows):
  - `0.0` → 6,691
  - `1.0` → 1,209
  - `0.85` → 1,020
  - `0.75` → 371
  - `0.5` → 351
  - `0.65` → 205
  - rest in the 0.30–0.95 range
- **Recent 5 NEW signals** (sample): 0.65, 0.65, 0.25, 0.85, 0.75 — **all valid real confidences**, no 0.0/1.0 in the head.

**Interpretation:** the bimodal collapse appears historical. Most or all of the 6,691 at-zero rows likely accumulated before some upstream change (model upgrade, prompt rev, or parser fix). Recent signals look clean. **Investigation required to confirm.**

## Code paths involved

1. **LLM output**: `engine/providers/ollama_provider.py:41-42` — `r.json().get("response", "")`. Returns raw text.
2. **Decision parsing**: extraction of `confidence` (float 0–1) from LLM text happens downstream. Candidate file: `engine/ai_brain.py` (many `decision.confidence` references); requires deeper trace to find the JSON/regex extractor.
3. **TradeDecision dataclass**: `engine/providers/base.py:13` — `class TradeDecision` defines the shape; default `confidence=0.0` is the prime suspect for the 0.0 spike (any parse path that falls through to default yields 0.0).
4. **bridge_vote**: `engine/bridge_vote.py` — collection writer. Stalled since 2026-05-01 13:01 (per audit).

## Hypothesis ladder (most → least likely)

1. **Default-0.0 sink** — LLM output occasionally fails JSON parse (qwen3:8b model drift, missing closing brace, narrative-style output not in JSON). Code defaults `confidence=0.0` instead of skipping the signal.
   - **Fix shape**: skip the signal entirely on parse failure rather than writing a 0.0 confidence row. Single-file in the parser; ≤ 20 lines.
2. **1.0 clamp on out-of-range** — LLM returns `confidence: 1.2` or string "100%"; code clamps to 1.0 instead of treating as parse error.
   - **Fix shape**: distinguish "well-formed but out-of-range" (clamp + flag) from "no confidence field" (skip). ≤ 30 lines.
3. **Historical artifact only** — the bimodal collapse stopped after some recent upstream change and current data is clean. **Fix shape: no code change; back-fill flag old rows as 'legacy' to exclude from analytics.**
4. **Bridge-voter stall is separate** — bridge_votes writer broken at 2026-05-01 13:01 but unrelated to confidence parse. Investigate independently.

## Required diagnostic steps (next session, before applying any fix)

```bash
# 1. Find the actual confidence extractor — likely a regex or json.loads in ai_brain.py
cd ~/autonomous-trader
grep -n "confidence" engine/ai_brain.py | head -30
grep -rn "decision.confidence\s*=\|TradeDecision(" engine/ --include="*.py" | head

# 2. Bucket signals by date to confirm bimodal is historical
sqlite3 data/trader.db "
  SELECT date(created_at), 
         SUM(CASE WHEN confidence=0.0 THEN 1 ELSE 0 END) AS at_zero,
         SUM(CASE WHEN confidence=1.0 THEN 1 ELSE 0 END) AS at_one,
         SUM(CASE WHEN confidence NOT IN (0.0, 1.0) THEN 1 ELSE 0 END) AS real,
         COUNT(*) AS total
  FROM signals 
  WHERE player_id='energy-arnold'
  GROUP BY date(created_at)
  ORDER BY 1 DESC
  LIMIT 30;
"

# 3. Sample raw LLM output for one current signal
# (requires service log inspection or one-shot manual call against ollie 192.168.1.166:11434)
curl -s http://192.168.1.166:11434/api/generate \
  -d '{"model":"qwen3:8b","prompt":"Return JSON: {action,confidence,reasoning}","stream":false}' \
  | jq '.response'

# 4. Diagnose bridge_vote stall (separate thread)
sqlite3 data/trader.db "
  SELECT date(created_at), COUNT(*) FROM bridge_votes
  GROUP BY date(created_at) ORDER BY 1 DESC LIMIT 30;
"
grep -rn "INSERT INTO bridge_votes\|bridge_vote_save\|save_bridge_vote" engine/ --include="*.py"
```

## Proposed fix (only after diagnostic confirms hypothesis 1 or 2)

**Single-file, ≤ 30 line patch shape** — parser-side guard. Exact target file pending step 1 of diagnostics.

```python
# Pattern: skip-on-default, not write-with-zero
if confidence is None or not isinstance(confidence, (int, float)):
    console.log(f"[yellow]{player_id}: confidence parse failed; raw={raw[:200]!r}")
    return  # do NOT write the signal at all

if not 0.0 <= confidence <= 1.0:
    console.log(f"[yellow]{player_id}: confidence out of range: {confidence}; clamping but flagging")
    confidence = max(0.0, min(1.0, confidence))
    # consider: tag signal with parse_warning='clamped_oor'
```

## Risk assessment

- **Sacred DB**: read-only diagnostics; no `UPDATE`, no `DELETE`, no `VACUUM`.
- **Service impact**: parser is per-signal-cycle, hot path. Restart required after fix (flag for Admiral).
- **Blast radius**: skips legitimate-but-unparseable signals → energy-arnold signal volume may drop. Acceptable; the dropped signals were already at 0.0 confidence and provided zero downstream value.
- **Rollback**: `git revert <commit>` + restart. No DB schema change.

## Why NOT applied today

- Cannot identify the single parser file with certainty without deeper tracing (`grep` returns wide surface — extractor not visible from one-line scan).
- "Multi-file" status per Task 3 directive: "If the fix is multi-file or > 50 lines: write `data/scotty_proposals/energy_arnold_fix_proposal.md` with the diagnosis, proposed diff, and risk assessment. Do NOT apply."
- Diagnostic step 2 may reveal the bimodal was healed in a prior commit and no fix is needed — premature application risks introducing a new bug for an already-closed problem.

## Recommended next action

1. Admiral runs diagnostic step 2 (the date-bucket query) and shares the result.
2. If hypothesis 3 confirmed (historical only) → close as documentation-only.
3. If 1 or 2 confirmed → schedule a 1–2h focused session, apply ≤ 30-line patch.
