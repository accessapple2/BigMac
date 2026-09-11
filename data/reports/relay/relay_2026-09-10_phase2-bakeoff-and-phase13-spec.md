# Relay — Phase 2 bakeoff verification, recall/calibration builds, Phase 1.3 spec, 2026-09-10 (evening)

Continuation of tonight's earlier backlog-clearing session
(`relay_2026-09-10_bakeoff-minimax-provider-and-arms.md`,
`relay_2026-09-10_four-item-sweep-and-doc-revisit-expiry-check.md`).
Covers: the un-alias correction (item 7), Worf/Troi roster reconciliation
(item 8), the S8 rotation verification (item 9, execution held for
tomorrow), and the bakeoff readiness check + recall/calibration builds +
Phase 1.3 spec (items A/B/C).

## Item 7 — un-alias premise was false, did not shrink the alias set

Live digest check (`/api/tags` against olliemax, not just model size)
proved `plutus-v1`, `plutus-v1:latest`, `ministral-3:3b`, `qwen3:4b`, and
`qwen2.5-coder:7b` all still share `qwen3:8b`'s exact digest
(`f112024b4d65a1ec6a84...`) — the 2026-09-04 un-aliasing target passed
unrevisited for 6 days. **Did not shrink `_QWEN3_ALIAS_MODEL_IDS`** —
would have silently reintroduced the `<think>`-leak bug for
`ollama-plutus` (McCoy), the only still-active seat on that list. Both
CLAUDE.md copies (canonical + `ollietrades-lite`'s informational copy)
corrected from a false "resolved by 9/4" framing to open-ended-until-
verified, tagged `REVISIT-BY: 2026-09-17`. One concrete consequence:
McCoy's only alias-era trade (2026-07-19 boundary) is a single row dated
2026-09-03 — Season 6 was already cleared by an earlier relay doc.
Commits `0cd58b1`, `273db14` (ollietrades-lite).

## Item 8 — Worf/Troi are reassigned, not dead; the real stale one is elsewhere

`qwen3-8b-flash` (Worf) and `options-sosnoff` (Troi) were deliberately
moved off Arena signal-emission on 2026-05-29 into an Advisory Team role
(`run_team_advisor`, twice-daily, `portfolio_advice` table) — documented
in `main.py`'s own `_SCAN_TIER2` comments. Live-verified both fired today
(2026-09-10, 803/665 total rows). No code changed — nothing was broken.

**Real finding:** the third Advisory Team member, `grok`
(`engine/team_advisor_grok.py`, reads real Schwab holdings), hasn't
written a row since **2026-06-24 — 11 weeks stale**. Consumed by
`signal-center/server.py:1239`'s "Advisory Team consensus surface" panel,
which has silently shown 2/3 voices for 11 weeks with no freshness
indicator. Fourth dead-man's-switch instance this session. Filed to
`docs/XO_BACKLOG.md`. Commit `86e3636`.

## Item 9 — S8 rotation verified, execution held for tomorrow per the Captain

`rotate_season()`'s six-step sequence re-verified against current code:
dry-run unhalt-scope check → save S7 summary → bump season/timestamp →
reset cash → unhalt `halt_reason IS NULL` only → close positions + War
Room post. Live dry-run: `active_before=8, would_affect=8, safe=True`
(margin=10); zero halted-with-no-reason agents (no wrong-reactivation
risk). S7 started 2026-07-12 23:59 — genuinely 59-60 days old. `caller`
argument is mandatory since 2026-09-09 — tomorrow's run needs
`rotate_season(caller="s8-manual")`. **Not executed** — Captain's call,
"deserves a fresh session." Options premium restatement + dataset
exporter filed to `docs/XO_BACKLOG.md` with `REVISIT-BY: 2026-09-11`
tags, sequenced explicitly (Phase 1.1 read → S8 rotation → restatement →
exporter).

**Bug found and fixed while wiring those tags:** `check_doc_revisit_dates`
and the pre-existing `check_fleet_lifecycle_drift` both compared against
`datetime.now(timezone.utc).date()` — in Phoenix (UTC-7, no DST), "today"
flipped 5-7 hours early every day. Caught live: a tag dated "tomorrow"
(2026-09-11) set in the evening fired as overdue immediately. Both now use
Phoenix-local date. Commit `240a143`.

## Item A — bakeoff readiness check (verify only, nothing run)

Real McCoy-shaped prompt (persona + market snippet + the fleet's exact
output-format footer) through all 4 arms, live `parse_decision()`:

| Arm | Result |
|---|---|
| qwen3:8b | Clean pass |
| plutus-v1-real | **Reproducibly drops the Invalidation field** — live `INVALIDATION-IMPLAUSIBLE` warning fired for real during the test |
| MiniMax-M3 | Parsed, but wraps answers in in-character narrative prose rather than clean fields — risky, got lucky |
| Fin-R1 (plain) | **Silently misparsed as HOLD/0.5** — real content, correct format, but markdown-bolded field labels (`**Decision:**`) broke `parse_decision()`'s `startswith("decision:")` check. Did NOT code-switch to Chinese on this prompt. |
| Fin-R1 (English-pinned) | Parsed cleanly — but N=1, not a confirmed fix |

Scoring harness itself (the actual Phase 2 iterate-and-score runner) does
not exist as code yet — that, not arm readiness, is what blocks a real
run today. `parse_decision()` not modified — flagged the markdown-
stripping gap as a candidate fix, not applied without sign-off. Commit
`31e9e5a`.

## Item B — recall-in-prompt and calibration map built, both surfaced real gaps

**B.1** `engine/recall_prompt.py` (`RECALL_IN_PROMPT_ENABLED`, default
OFF) reuses `setup_similarity_signal`'s embedding/KNN primitives exactly,
no fork. **Not wired into `base.py::build_prompt()`** — held for a
deliberate follow-up, same posture as item 9. Found `recall_corpus` has
no sector/regime/forward-return columns (only symbol/outcome/pnl) — true
sector/regime cascading and forward-return neighbors wait on tomorrow's
dataset exporter. Found `com.ollama.serve` (bge-m3 host) is down on
bigmac entirely — no process, not in `launchctl list`, plist present but
unloaded; couldn't restart (`sudo` needs a password this session doesn't
have). Measured KNN+formatting cost using a stored embedding as a
stand-in: **~8ms, ~58 added tokens** — real embed-call latency unmeasured
tonight. Both findings filed to `docs/XO_BACKLOG.md`.

**B.2** `engine/calibration_map.py` — simple binned, not isotonic
(per-regime samples too thin: BULL_CROSS 131, CAUTIOUS_BEAR 32,
CAUTIOUS_BULL 27, BEAR_CROSS 3). Not wired into sizing. The exact
requested methodology (decision_audit trade_fire events joined to
realized trade outcomes, per regime) yielded only **3-5 usable
observations total** — most fired trades are still open. Supplementary,
regime-blind check on `trades.confidence` directly (474 clean closed
trades) found **305 (64%) have `confidence=0.0`** — a data placeholder,
not real predictions, filed as its own backlog item.

**The number worth its own line:** excluding the placeholder,
**169 real McCoy trades stated confidence in 0.9-1.0 (avg ~0.93);
realized hit rate was 78.6%.** McCoy is roughly **15 points overconfident**
at the top of his stated range, and every bucket below 0.9 has zero
coverage at all — the first honest number this program has on what his
confidence means. Commit `2b0cf2b`.

## Item C — Phase 1.3 spec finalized (Admiral decisions, 2026-09-10), nothing built

Confirmed the April Sniper gate (`engine/crew_scanner.py` Gate 7) has
drifted with no deliberate decision behind it: `SNIPER_ALPHA_THRESHOLD`
0.3→0.25, `SNIPER_MIN_CONFIDENCE` 65→55, Signal Center grade≥B check
dropped entirely — and a stale comment (fixed tonight) was still
claiming "0.3" against a live 0.25, plus a second stale comment
referencing `triple_threat.py`, archived since 2026-04-26.

**Decisions, written into `docs/XO_PLAN_2026-09.md`'s Phase 1.3 section
(not applied to the live gate yet — still blocked on 1.2):**
1. Phase 1.3 restores the original strict spec: `composite_alpha >= 0.3`
   AND `confidence >= 70%` AND Signal Center grade `>= B`. If it proves
   too tight once measured, that's a deliberate future decision, not a
   quiet loosening.
2. Sizing fails **closed** on missing calibration data — base allocation
   only, never the 0.6+ full tier, when a (regime, confidence-bucket) has
   no evidence. `get_calibrated_confidence()` itself stays fail-**open**
   (correct for the pass/fail gate); the sizing layer applies its own
   rule on top. Documented in both the plan doc and `calibration_map.py`'s
   own docstring, so a future implementer sees it at the source, not only
   in prose.
3. The 78.6%-vs-93% finding (see item B.2) is the concrete evidence
   behind decision 2 — most of the confidence range has never been
   checked against reality at all.

**Live code touched tonight: comments only, zero behavior change** —
`SNIPER_ALPHA_THRESHOLD`/`SNIPER_MIN_CONFIDENCE` values are unchanged
(0.25/55), verified post-edit. Commit (pending, see below).

## Files/state touched this session (evening continuation)

- `engine/crew_scanner.py` — Gate 7 comments corrected (no value change).
- `engine/calibration_map.py` — docstring note on fail-closed-for-sizing.
- `docs/XO_PLAN_2026-09.md` — Phase 1.3 section expanded, status log entry.
- `docs/XO_BACKLOG.md` — five new items filed across the session (stuck
  monday-check job, stale grok sub-advisor, com.ollama.serve down,
  confidence=0.0 contamination, plus the two 2026-09-11 held items).
- No `data/trader.db` writes this evening beyond the earlier
  `fleet_lifecycle_ledger` backfill row (item 4, prior relay doc).
