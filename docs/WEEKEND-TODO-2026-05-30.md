# Corrected Canonical TODO — 2026-05-30 (weekend reconcile)

Reconciles the prior comprehensive to-do against the LIVE `docs/XO_BACKLOG.md` + `QUEUE_AUDIT_2026-05-29.md`
+ this session's commits. **Market-gated vs non-market split preserved.** This supersedes ad-hoc lists.

---

## ✅ CLOSED this session (verify before re-listing — these are DONE)
- **HM-RUN-SCAN-WATCHDOG §C** — indicators (Loop 3), catalyst/trending/quote_summary spikes (5B/5C/5D),
  deepseek + ollama-coder redundant arena paths. Clean-verified. *Remaining = the analyze-all FLOOR (Monday).*
- **HM-RESTART-ORPHAN-PREVENTION** — SHIPPED + proof-tested (`scripts/trader_restart.sh`, single-writer gate).
- **HM-WORF-DRIFT-RECONCILE** — COMPLETE (3rd residual `RULES_SCANNERS` removed).
- **HM-MODEL-CONFIG-STALENESS** — authoritative comment shipped (DB canonical; 10 mismatches documented).
- **Fleet decision matrix** — delivered; Gate 0 (no perf-assessment pre-5/14) is the standing constraint.
- **Doctrine banked** — measurement-contamination (×3), single-writer-verify, spikes-mask-floors,
  cheapest-fix-is-deletion, can't-validate-content→bound-quantity, role-conversions-leave-stale-paths.

## 🟡 DIAGNOSED this weekend — fix PROPOSED, awaiting go (non-market, but RED-ish fixes)
- **HM-DALIO-GOOGL-ZERO-EXIT** — root: manual SQL cleanup row (id 2539), not a code bug. Fix = correct the
  row (`realized_pnl=0, known_contaminated=1`) + make PnL aggregator tracking-route-aware. RED (DB write) → go-gated.
- **HM-NAVIGATOR-SIGNAL-PATH-DEAD** — root: by-design omission (re-homed to trade-only path 2026-04-12);
  fleet-wide rules-scanner blind spot. Fix = `save_signal` hook in crew_scanner rules path; **Admiral decides
  scope** (every eval floods `signals`). RED-ish → go-gated.
- **HM-AGENT-DUAL-PATH-AUDIT** — DONE: arena == {McCoy, Dax} only (orphan-hint refuted statically). No more
  free deletions; floor = those 2. (Staged 4-agent skip-set = harmless belt-and-braces.)
- **HM-CONTAMINATED-FLAG-INCOMPLETE** — investigation in flight (recommendation pending).

## 🔴 STAGED — bundle with Monday's floor restart (do NOT ship alone)
- **4 Tier3 redundant skip-set entries** (cto-grok42, ollama-deepseek, ollama-kimi, qwen3-8b-sonnet) —
  staged in `engine/ai_brain.py` working tree. *Now known belt-and-braces (tier-gated anyway) but harmless.*
- **HM-ADVISORY-CREW-DRIFT-SWEEP** (HIGH) — overlaps the above; reconcile into the Monday restart.
- **HM-BS-DAEMON-HEARTBEAT** (LOW one-liner) — next main.py restart.

## ⛔ MARKET-GATED — Monday/market-hours only (the hard wall)
- **§C floor build — Lever A bounded-rotation (Shape B)** — DESIGN DONE; BUILD Monday (needs clean arena
  set confirm {McCoy,Dax} + clean per-symbol cost for N-sizing). Sequence in HM-RUN-SCAN-WATCHDOG ticket.
- **HM-EXTERNAL-FETCH-DISCIPLINE-AUDIT — Phase 2 batch** (7 unbounded leaves) — needs market-hours confirm
  before fixing; build after floor closes.
- **HM-SIGNALS-V2-STALE-SWEEP** — bulk-UPDATE runs AFTER the floor fix (don't drain a still-filling pond).
- **Nightly-scanner confirm** (rs_rank/minervini 20:30/20:45 AZ under adjusted='all') — needs a live run.
- **battle_station daemon** — confirm firing Monday (0 fires on weekend = expected market-gate, verify).

## 🟢 NON-MARKET OPEN — doable anytime (LOW/hygiene)
- **HM-LOOP-1-LOG-VOLUME-ROTATION-CHECK** (LOW) · **HM-ADJUSTED-OHLCV-DOWNSTREAM-VERIFY** (LOW) ·
  **HM-MEMORY-DEEP-AUDIT-Q2** (quarterly) · **HM-FRONTEND-VISUAL-TEST-HARNESS** (conditional).
- **WAVE 7 frontend** (inline-style-sweep B6-B9, LCARS Tier 2, palette, AN-Bridge proxies, ollie-ai Step 7)
  — off scan-path, but each needs a **manual browser smoke** before "shipped" (HM-BJ.E2). *Agent can build +
  static-check but CANNOT browser-smoke from CLI → build + stage, human smokes before declaring shipped.*
- **HM-NOTIF-WAR-ROOM-PRODUCER** (MED) — BLOCKED on Captain trigger definition.

## 6 STANDING ITEMS (from fleet review — still open, not session-touched)
HM-RISK-MANAGER-CONVICTION-STOP · HM-SCHWAB-CROSS-MECHANISM-ALARM · HM-TRADES-MIRROR-GAP ·
HM-ALPACA-BRIDGE-LIMIT-FIX · HM-QG-FLOAT-TRUNCATION · HM-CONVICTION-TIER-BOUNDARY. (Most Admiral-gated or
maintenance-window; re-verify current state before touching — per verify-before-fix.)

## DEFERRED / bakeoff (don't force)
- **deepseek-7b-grok4 → HM-BM bakeoff** (~June 15) — operational-cost convergence banked.
- Plutus v6, IC Squadron validation, CAPITAL-LADDER, GRADE-B-FLEET-GATE, O-Tasty Stage 2, HM-AM, real-money automation.

---
### Corrections applied to the prior list
- **Already-closed but were still listed:** §C deepseek/coder, orphan-prevention, Worf-reconcile, model-staleness.
- **Refuted (don't act on):** "Worf/Seven/navigator scan the arena" (orphan-contaminated tally — static sweep
  shows they're tier-gated, arena=={McCoy,Dax}). The 4 staged removals are belt-and-braces, not load-bearing.
- **Priority correction:** §C floor is the only remaining HIGH on the scan path, and it's MARKET-GATED (Monday).
- **Missed in prior list, now surfaced:** the fleet-wide rules-scanner `signals`-table blind spot (navigator dx).
