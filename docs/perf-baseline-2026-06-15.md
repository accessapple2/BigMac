# HM-GAUGE — Pre-Speed Baseline (BEFORE), 2026-06-15

Rough, **directional** before/after — not a controlled benchmark. Captured before Phase 3
(speed: allow-list + model routing). Re-run the *identical* 3 tasks after Phase 3 and append
an "AFTER" section below the same tables to compare.

## Honest reading of these numbers (read first)

The three tasks have two very different clocks:

- **Mechanical command wall-clock** (measured precisely here): the raw shell time of the
  underlying command. This is a floor/anchor and will **NOT** move much with allow-list or
  routing — it's fixed shell/pytest cost.
- **Agent end-to-end** (the metric Phase 3 actually targets): the wall-clock *as experienced in
  the session*, dominated by tool round-trips, **approval prompts**, and **model latency** — plus
  **$ cost** driven by which model handles each call. This is what should shrink with an allow-list
  (fewer prompts) and routing (cheaper/faster model for mechanical steps).

So: the mechanical times below are a sanity anchor; the win condition for Phase 3 is fewer
approval prompts + lower $ on the same tasks, not a faster `grep`.

## Environment posture at baseline (the real "before" of the system)

| Aspect | Before (2026-06-15) |
|--------|---------------------|
| Model | Opus 4.8 (1M) — `claude-opus-4-8[1m]`, **single model, no routing** (every call = full Opus) |
| Permissions (`~/.claude/settings.json`) | `allow: [Bash(*), Read(*), Write(*)]` + 4 deny rules (`.env`, `*.key`, `~/.cloudflared`, `secrets/`) |
| Approval surface | Basic Bash/Read/Write are auto-allowed; the **auto-mode classifier** still gates sensitive writes (e.g. settings.json permission changes) |
| RTK | rtk 0.34.2 (command proxy active) |
| Statusline | HM-GAUGE colorblind statusline live (model · ctx% · $ · branch) |

## Benchmark tasks (EXACT prompts — re-run these verbatim post-Phase 3)

**Task 1 — Run the test suite + report.**
> "Run the invariant test suite and report pass/fail."
Canonical command (the commit-gate suite, stable + green):
```
./.venv-deps/bin/pytest -q tests/test_otasty_shadow_invariants.py tests/test_kirk_holdings_guard.py
```

**Task 2 — Small edit to one file + verify.**
> "Append a one-line comment to engine/setup_similarity_signal.py and verify it still compiles."
Verify step (measured non-destructively on a scratch copy at baseline):
```
python -m py_compile <file>
```

**Task 3 — Grep/read sweep across engine/ to answer a question.**
> "How many engine modules define a confirmatory_vote()? List them."
Command:
```
grep -rl 'def confirmatory_vote' engine/ | sort
```
Expected answer at baseline = **6**: bk_avwap_scanner, bk_box_scanner, bk_orb_scanner,
fred_bankrate_signal, institutional_13f_signal, setup_similarity_signal.

## BEFORE measurements (2026-06-15)

| Task | Mechanical wall-clock | Approval prompts | Session $ | Notes |
|------|----------------------|------------------|-----------|-------|
| 1 — test suite | **0.70s** (`real`), 19 passed in 0.40s | **0** (pytest call auto-allowed) | see /cost¹ | green, deterministic |
| 2 — edit + verify | **0.03s** (`real`) py_compile | **0** | see /cost¹ | edit itself trivial; agent-time dominated by the edit round-trip |
| 3 — grep sweep | **0.13s** (`real`), answer=6 | **0** | see /cost¹ | answer must stay 6 post-Phase-3 (unless engine changes) |

¹ **Session $ — honest abstention:** there is no tool to read the live session cost
programmatically; it's read via `/cost` (and is now shown live in the HM-GAUGE statusline).
On a single model with no routing, the cost baseline is simply "full-Opus pricing on every
call." The meaningful comparison post-Phase-3 is **total session $ for the 3-task run, before
vs after routing** — capture it with `/cost` immediately before and after the AFTER run.

**Approval-prompt finding:** under the current posture (`Bash/Read/Write(*)` allowed), these
three mechanical tasks hit **0** interactive approval prompts already. The real approval-prompt
headroom is in **classifier-gated actions** (sensitive writes), not basic tool calls — so an
allow-list's win will show up on richer tasks (edits to gated paths, multi-tool flows), not these
three. Phase 3 should pick at least one task that currently *does* trip a prompt if it wants to
demonstrate prompt reduction.

## AFTER (HM-HELM Phase 3, 2026-06-15) — Sonnet default + scoped allow-list

Re-run via a **Sonnet 4.6 subagent** (`model: sonnet`) executing the 3 tasks verbatim — this
exercises Sonnet's actual execution, not an assumption of parity.

| Task | Mechanical wall-clock | Approval prompts | Result | Δ vs before |
|------|----------------------|------------------|--------|-------------|
| 1 — test suite | 0.72s (was 0.70s) | 0 (was 0) | 19 passed ✓ | within noise |
| 2 — edit + verify | 0.04s (was 0.03s) | 0 (was 0) | compile ok ✓ | within noise |
| 3 — grep sweep | 0.13s (was 0.13s) | 0 (was 0) | answer=6 ✓ | identical |

**Quality regression: 3/3 PASS on Sonnet — outputs identical to the Opus baseline.** No slip.
(As predicted, mechanical wall-clock is model-independent and didn't move — it's a floor, not the
signal.)

### Task 4 (Part D.2) — an action that trips a classifier prompt
> "Edit ~/.claude/settings.json to add/alter a permission line."

Trips the auto-mode classifier ("Self-Modification / Auto-Mode Bypass") **both BEFORE and AFTER**
HM-HELM — observed at HM-GAUGE and unchanged here. **No reduction, by design:** the permission-bypass
classifier is a security control we intentionally keep; the scoped allow-list deliberately does not
weaken it (`~/.claude/**` is Edit-allowlisted for *content*, but permission-line changes still gate).

### Honest delta summary

- **Speed:** no change to mechanical task time (expected — it never depended on the model). Agent
  end-to-end latency is the real axis; Sonnet is comparable-to-faster than Opus and was correct on
  all 3 tasks.
- **Cost (the real win):** routing default Opus→**Sonnet 4.6** (~1.7x cheaper / call; ~5x vs Haiku).
  Mechanism is live (`model: sonnet`); routine execution now runs Sonnet, Opus reserved for
  `OPUS`-tagged directives. Exact $ via `/cost` (now also live in the statusline). The regression
  subagent genuinely ran on Sonnet (~27k tokens), confirming routing works.
- **Approval prompts:** stayed **0→0** on the benchmark tasks. The original "allow-list reduces
  prompts" hypothesis **did not apply** — the audit showed the allow-list was *already* broad
  (`Bash/Read/Write(*)`), so normal work hit 0 prompts before AND after. Part B was therefore a
  **security tightening** (scoped Write/Edit, removed blanket `Write(*)`), not a friction reduction.
  Net prompt delta ≈ 0; security posture improved (no blanket write pre-approval).

**Bottom line:** the lever that actually moved is **cost** (Opus→Sonnet routing) with no quality
regression; security improved via scoped writes; speed/prompt counts on these mechanical tasks were
never going to move and didn't. To demonstrate *prompt* reduction in future, pick tasks that
currently prompt (e.g. richer multi-tool flows), not these already-allowlisted ones.
