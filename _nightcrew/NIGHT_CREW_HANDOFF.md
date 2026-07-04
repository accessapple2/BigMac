# NIGHT CREW HANDOFF — 2026-06-29
**XO pre-built Tasks 1–2 in full. Task 3 is ready to run. Rung 4 OUT OF SCOPE (gated).**

## ROE (non-negotiable)
RULE #1 absolute (paper-only, Schwab read-only). No execution, no router, no trade fire. v1 UNTOUCHED. **No restart while unattended** — everything here is static-file, design-doc, or a read/compute job; if anything would need a restart, STAGE + report, don't restart. Branch exec-pipeline. Additive/reversible; commit each unit separately; push at end, 0 ahead. Don't trip the ntfy drain-stall monitor.

---

## TASK 1 — RUNG 1 (built): integrate `carrier_rung1_contact.html`
XO built the working crumb-trail + CONTACT card (deep-link + tiering + LCARS blue/amber, colorblind-correct). **Your job is integration, not design.** Search the file for `INTEGRATION:` — 5 markers:
1. **Tiering / feed** — replace `MOCK_ALERTS` with v2's live notification-poll payload; keep `ACTIONABLE_SOURCES`.
2. **openContact** — currently an overlay (least invasive). Wire to v2 `showTab()`/`loadResearchTab()` only if the CONTACT should live inside a tab.
3. **Context slots** — leave stubbed (that's Rung 2, design-only tonight).
4. **Deep-link** — `parseFocus()` reads `?focus=TICKER&src=SOURCE`; keep URL-reflects-focus.
5. **Chart link** — points at v1's chart (`/charts?symbol=`) per two-tier. Keep unless v2 grows its own.
**Self-verify (you have Chrome):** load `/bridge-v2?focus=AAPL&src=bk_avwap` → screenshot → CONTACT focuses AAPL w/ source+timestamp; confirm an INFORMATIONAL source (system/measurement) renders NO crumb. Put screenshots in the report. Commit as its own unit; reversible.

## TASK 2 — DESIGN SPECS (built, DO NOT BUILD)
Two finished specs — drop into `drafts/`, do not implement:
- `RUNG2_CONTEXT_ON_ARRIVAL_SPEC.md` — read-only context fill, endpoint map, per-source emphasis.
- `EMIT_TIME_ACTED_TAGGING_SPEC.md` — the proper fix for the dead-end join. **Touches the fire path → Admiral-review-only, never an overnight build.**
Just file them and note in the report they're queued for review.

## TASK 3 — BACKFILL (run this): `nightcrew_fwd_return_backfill.py`
The one thing only the box can do. It calls your EXISTING verified `evaluate_pending()` in a throttled, box-health-gated loop to drain the fwd_return backlog overnight.
1. **Adapt the import line** at top (`from engine.signal_evaluator import evaluate_pending`) and `DB_DEFAULT` to the real paths.
2. Dry-run first: `python3 nightcrew_fwd_return_backfill.py --dry-run` → confirm pending/filled counts look right.
3. Run: `python3 nightcrew_fwd_return_backfill.py --sleep 2 --max-load 6`
   - Idempotent (skips evaluated rows); safe alongside the live 30-min evaluator.
   - Auto-backs-off / aborts on sustained high load — protects the live process.
   - Does NOT touch the scheduler, config, or trader process.
4. If it aborts on load, that's fine — report it; the natural drain continues regardless.

## MORNING REPORT → `drafts/NIGHT_CREW_REPORT.md` + paste-back
- **Rung 1:** integrated Y/N, screenshots, deep-link examples, what's reversible.
- **Specs:** filed Y/N (links).
- **Drain:** final pending/filled + **the captured `/api/observations/summary`** (per-source `avg_fwd_*` — THE alpha read) + `/api/measurement-health`, with % drained.
- **Anything staged-not-shipped + why. Open questions for Admiral.**

The morning alpha snapshot is the prize: if the backfill cleared the backlog, the Admiral wakes to the first trustworthy per-source forward returns — the deployment authority for any future Rung 4.
