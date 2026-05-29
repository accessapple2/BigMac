# HM-AUDIT-T0 — Tier 0 Precursors (Starfleet Command Audit follow-up)
# XO 4.8 → Scotty · 2026-05-28 · Execute + self-verify + commit inline

CONTEXT: Scotty's verification report caught 6 stale facts in the audit. Two are
load-bearing for downstream work and must be corrected BEFORE Tier 1 F-4
reconciliation runs. This is that cleanup. No trader restart required — doc +
view-confirm only.

## T0-A — Correct GPU doctrine (CLAUDE.md was stale, drove this session's VRAM premise)
CLAUDE.md said "RTX 5060 8GB". Live /api/ps proved 16GB-class (qwen3:8b 5.98GB +
ministral-3:3b 4.62GB = 10.6GB co-resident — impossible on 8GB). The 8GB error
propagated into HM-WR-VRAM-THRASHING ("one 7B fits") and the navigator "too big
for 8GB" swap. Both rationales now suspect.

## T0-B — Confirm F-4 reconciliation targets trades_clean (NOT raw trades)
Raw `trades` carries the ~153% price-writeback inflation (235 known_contaminated
rows). Reconciliation must read `trades_clean` or it's worthless.

## T0-C — Ops register: log the SSH-to-Ollie-Max key gap.

---

## EXECUTED 2026-05-28 (Scotty) — T0 GREEN

- **T0-A:** SSH retry → `Permission denied` again (logged, T0-C). Proceeded on
  audit spec **RTX 5080 16GB**. Corrected CLAUDE.md GPU doctrine in BOTH places
  (RAM Discipline §, line ~101; HM-CD Doctrine Lesson §, line ~605): 8GB→16GB,
  "one 7B"→"two 7–8B co-resident", 14B-vs-14B still swaps, navigator-swap
  rationale flagged suspect, VRAM-thrashing fixes noted still-valid (only
  rationale changed). 16GB evidence-backed via /api/ps; exact model
  nvidia-smi-unconfirmed (SSH gap).
- **T0-B:** `trades_clean` view confirmed live. DDL:
  `SELECT * FROM trades WHERE execution_type='alpaca_paper' AND executed_at
  >= '2026-05-21' AND COALESCE(known_contaminated,0)=0`. Excludes the 235
  contaminated rows ✓. Pinned as F-4 source-of-truth in
  `drafts/HM-F4-RECONCILIATION.md` + noted the short clean window caveat + the
  OOS-2.692 comparison rung.
- **T0-C:** SSH gap logged → XO_BACKLOG (HM-OPS-SSH-OLLIE-MAX, LOW).
- No trader restart (doc + view-confirm only, as specified).
