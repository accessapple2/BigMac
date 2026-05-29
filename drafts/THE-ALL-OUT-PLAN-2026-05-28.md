# THE ALL OUT PLAN — v2 (whole-system) — 2026-05-28

> **STATUS: CLOSED / HISTORICAL — 2026-05-29.** WAVE 0-6 + 8 complete; WAVE 9.1
> wedge (tick-recorder cap fix) shipped 2026-05-29. Only WAVE 7 (frontend) +
> WAVE 9.x (event-pipeline follow-ups) remain. **This plan is spent — future
> sessions resume from `docs/XO_BACKLOG.md` + the gated list in
> `docs/QUEUE_AUDIT_2026-05-29.md`, NOT from this doc.**

## STATUS — end of session 2026-05-28
- **WAVE 0 (TIER3): ✅ infrastructure CLOSED** (two-lane scheduler + dead-model sweep + book-reconcile all proven). Per-agent tail → 5.9 / HM-AS-β.
- **WAVE 1 (data integrity): ✅ COMPLETE** — forward-fixes already shipped; Option B (`known_contaminated` + `trades_clean`) unblocks Plutus/PnL; historical backfill correctly STOPPED (unsafe to auto-match).
- **WAVE 2 (quick wins): ✅ COMPLETE** — 3 shipped (QG float / wheel regime / NTFY persist), 5 already-done, 2 not-real-quick-wins (ASGI deferred).
- **WAVE 3 (O-Tasty engine): ✅ COMPLETE** — strike solver (inverted put branch) + directional selection + same-origin tab; verified via live tunnel scan.
- **WAVE 4 (data pipeline): ✅ COMPLETE** — 3 already-done; 4.4 legacy scanner NOT retired (live code, stale "dead" premise).
- **WAVE 5 (fleet logic): ✅ COMPLETE** — dayblade Option-B shipped; conviction-scaled stops ENABLED (live); 6 Admiral decisions made.
- **WAVE 6 (infra): ✅ COMPLETE** — restart.sh fixed + CLAUDE.md staleness sweep + setup_db mirror; cloudflared/auth scaffolding prepped for Captain; HM-AS-β banked.
- **WAVE 7 (frontend): ⏭ CARRY-FORWARD** (last priority).
- **WAVE 8 (O-Tasty shadow loop): ✅ COMPLETE** (2026-05-28). 5 loops A–E + scheduler cadence, all zero-order verified by direct broker query (PA3YVDTUH5CB 0 orders). Live shadow daemon running in the O-Tasty backend (A/B/C 5-min RTH, E nightly 6 PM ET). Commits `b2755b9` `b3defcd` `2b537a0` `c9e81e4` `8eb0b5a` `e4b959a` `701a875`. **Live-paper promotion GATED on 30 shadow trades + Captain approval.**
- **WAVE 9 (events bus / event tape): ⏭ CARRY-FORWARD.**

**NEXT-SESSION ORDER:** HM-AS-β scheduler (`drafts/HM-AS-BETA-SCHEDULER-TOP-PRIORITY.md`) — TOP PRIORITY (systemic perf + TIER3 5.9 tail). Then carry-forward WAVE 7 (frontend) / WAVE 9 (events bus). WAVE 8 DONE.
**CAPTAIN QUEUE:** cloudflared sudo install + auth secret-gen (`drafts/WAVE6-CAPTAIN-RUNBOOK.md`). Signal Center :9000 = HOLD.

---

Built from Scotty's full backlog inventory (~45 items, 6 categories).
Sequenced by DEPENDENCY + RISK, not category. Build phase: market timing
yields to completion. Restart whenever needed.

Sacred rules: never rm .db/dirs (archive only); swingdesk.db isolation;
O-Tasty paper PA3YVDTUH5CB only; no real-money anything.

Work WAVE by WAVE. Report after each WAVE. Fail twice → pivot simpler.
Data-dependent verification = during market hours. High-blast infra/schema
= slot into lulls but DO it today.

SEQUENCING LOGIC:
- (D) data-integrity P0s gate everything downstream (PnL, Plutus corpus,
  fleet-perf). They go EARLY.
- Quick wins batched to clear noise.
- O-Tasty has real bugs (strike-selection, structure-select) — fix before
  any shadow promotion.
- Specs (F) + monitoring-only (L) items LAST or explicitly out-of-scope.

═══════════════════════════════════════════════════════════════════════════
WAVE 0 — TIER3 verdict (already in flight)
═══════════════════════════════════════════════════════════════════════════
Two-lane scheduler watcher (b7skppmtc) + 07:17 cron running.
Report: how many of 8 agents revived, swap rate by lane, deepseek alive?
WIN → TIER3 CLOSED. FAIL → revert two-lane commit + restart, note, continue.

═══════════════════════════════════════════════════════════════════════════
WAVE 1 — DATA INTEGRITY P0s (gates everything downstream)
═══════════════════════════════════════════════════════════════════════════
These corrupt PnL + block the Plutus corpus. Highest open severity. FIRST.

1.1 HM-TRADES-PRICE-WRITEBACK-BUG (P0) — 40% Alpaca trades wrong entry/exit,
    PnL overstated 153%. VERIFY current state first (memory says partially
    addressed before — may have regressed/incomplete), then fix writeback +
    backfill historical rows.
1.2 HM-TRADES-ALPACA-PROVENANCE — 100% rows mislabeled
    execution_type='simulated', 0% alpaca_order_id. Wire real order_id capture
    on fill + correct execution_type.
1.3 HM-TRADES-MIRROR-GAP — 23% Alpaca fills missing mirror row. Fix pipeline
    gap + backfill.

VERIFY: PnL sanity check — entry/exit match Alpaca fills, execution_type
correct, order_ids present. Report % clean before/after.
Commits: one per bug. Report WAVE 1 before WAVE 2.

═══════════════════════════════════════════════════════════════════════════
WAVE 2 — QUICK-WIN BATCH (clear the S-effort noise)
═══════════════════════════════════════════════════════════════════════════
2.1 HM-NEO-TRAIL-PERSIST — persist _neo_trail_highs (wiped on restart).
2.2 HM-ALPACA-BRIDGE-LIMIT-FIX — limit→market silent downgrade + hardcoded DAY
    tif. Honor limit + correct tif.
2.3 HM-QG-SCORE-FLOAT-TRUNCATION — int(score) drops +0.5 partials.
2.4 wheel_strategy.py:170 — wire regime from get_latest_briefing().
2.5 HM-WR-STALL-ALARM rate-limit — persist NTFY dedup.
2.6 NTFY rate-limit persist-to-settings — persist _rate_state.
2.7 HM-ASGI-MIDDLEWARE-EXCEPTION — cleanup app.py:1058/1060.
2.8 HM-ALERT-AUTH-STORM — alert poller rings on 401s; add auth gate.
2.9 HM-I-β — visually distinguish Arena Paper vs Alpaca Paper.
2.10 Webull → "Starfleet" label migration (keep internal id).
Report WAVE 2 (shipped list) before WAVE 3.

═══════════════════════════════════════════════════════════════════════════
WAVE 3 — O-TASTY real bugs (fix before any shadow promotion)
═══════════════════════════════════════════════════════════════════════════
3.1 Strike-selection bug — strikes far from spot (GOOGL 471–583 vs $388.83),
    IC max_loss negative. Fix solver to anchor near spot at target delta.
    ROOT bug — fix FIRST.
3.2 Structure-selection — no directional logic + max(max_loss,0.01) exploit →
    always IC. Implement bullish→BPS, bearish→BCS, neutral→IC, own-it→CSP,
    20Δ strikes.
3.3 Same-origin API fix — options_tab.html localhost:8889 → relative path so
    Test Kitchen shows live data via tunnel.
3.4 Rip out [MORE-DBG] from index.html.
3.5 VERIFY: re-scan → strikes near spot + structure variety + tab live data.
Commits per fix. Report WAVE 3 before WAVE 4.

═══════════════════════════════════════════════════════════════════════════
WAVE 4 — DATA PIPELINE
═══════════════════════════════════════════════════════════════════════════
4.1 HM-BULK-PRICES-FIXTURE-BUG — change_pct=0/vol=40 fixture for 508 symbols.
    VERIFY current state (memory says snapshot-addressed); re-fix if regressed.
4.2 HM-SLOW-FUNDAMENTALS Phase 2 — wire get_bulk_daily_ohlcv into 4 endpoints.
4.3 HM-DECISION-SUPPORT-OBSERVABILITY v1 — decision_audit table + 3 hook sites.
    HIGH: the observability layer that makes future debugging fast.
4.4 Legacy convergence scanner retirement — 8 readers of dead strategy_signals.
Report WAVE 4 before WAVE 5.

═══════════════════════════════════════════════════════════════════════════
WAVE 5 — FLEET LOGIC (dependency-gated trading items)
═══════════════════════════════════════════════════════════════════════════
5.1 HM-POSITIONS-CONVICTION-DENORM — resume from 52.4% NULL, finish denorm.
    PRECURSOR — unblocks 5.3 + 5.4. Do FIRST in this wave.
5.2 HM-DAYBLADE-OPTION-B — wire halt_mode into dayblade path + set
    dayblade-0dte halt_mode (the precursor), THEN remove legacy is_paused.
5.3 HM-RISK-MANAGER-CONVICTION-STOP-WIRE — unblocked by 5.1. Wire it.
5.4 HM-CONVICTION-TIER-BOUNDARY-CALIBRATION — report data for Admiral (don't
    auto-tune).
5.5 HM-DEEPSEEK-TRIAGE — gate-downgrade math (unblocked by 4.3).
5.6 SPREAD_CANNIBALIZATION_GUARD Phase 4 — report state, recommend lift/keep.
5.7 HM-TPOL-REMEDIATION — audit T'Pol halt logic, report.
5.8 HM-TRADE-DESK-AUTOPILOT Phase 2 — reconcile + mutual-cancel.
5.9 TIER3 per-agent behavioral diagnosis (deferred from WAVE 0, 2026-05-28) —
    diagnose why the 3 genuine equity agents (energy-arnold, ollama-qwen3,
    qwen3-8b-flash) are silent on INSTALLED models: HOLD/low-conviction vs a
    downstream gate. (TIER3 infrastructure CLOSED — scheduler + dead-model
    repointing + book-reconcile all proven; this is the remaining fleet-logic
    question, NOT infra.)
Report WAVE 5 before WAVE 6.

═══════════════════════════════════════════════════════════════════════════
WAVE 6 — INFRA / OPS HARDENING
═══════════════════════════════════════════════════════════════════════════
6.1 FIX restart.sh — wrong venv + misrouted logs (discovered this session).
    Canonical restart must use .venv + correct log paths. FOUNDATIONAL.
6.1b CLAUDE.md staleness-cleanup pass (added 2026-05-28) — correct provably
    stale claims so future sessions stop chasing ghost tickets: scan_strategies
    "dead since 04-07" (it's LIVE via Navigator endpoints), trades price-writeback
    + provenance (fixed 2026-05-21), dayblade-0dte halt_mode (now 'active' not
    'full'). Also relevant memory files. (Discovered across WAVES 1-4 verify-first.)
6.2 HM-WR-CYCLE-RCA Phase 2 — merge debug branch (jobs dump + 60s heartbeat);
    trader stops running debug bytecode.
6.3 HM-WR-LATENCY L2a v2 / L4 — deadline-check + L4-inner stack move.
6.4 HM-CLOUDFLARED-LAUNCHDAEMON — Scotty preps plist+runbook; Captain executes
    (sudo+FDA, Aqua session).
6.5 HM-PLUTUS-FINETUNING v1 — create scripts/learning/train_critic.py.
6.6 app.py auth Phase 1 (21268) — Scotty preps; Captain gens secret first.
6.7 HM-AW — reopen Signal Center :9000 to network (2FA+RBAC ready).
Report WAVE 6 before WAVE 7.

═══════════════════════════════════════════════════════════════════════════
WAVE 7 — DASHBOARD / FRONTEND
═══════════════════════════════════════════════════════════════════════════
SCOPE CORRECTED 2026-05-28 (SC-5 pre-scope, verified vs live code — 3 stale notes fixed).
Sequence by dependency-cleanliness; all are index.html/lcars.js/app.py → mandatory browser smoke.
7.2 HM-LCARS-COVERAGE Tier 1 — **6** SECTION_LABEL entries (NOT 13; labels added since).
    cockpit, trade-desk, live-trading, ollie-ai, real-portfolio, test-kitchen. S, ~15min. DO FIRST.
7.1 HM-AN-BRIDGE-AUTH Phase 1 — 5 Tier-1 SC READ-proxies in app.py (top5,
    intelligence-summary, scorecard, leaderboard, outcomes; SC localhost bypass
    already live). NOT the 11-route verify_admin_token auth (separate). S, ~2-3h.
7.3 HM-NOTIF-DEEPLINK-WAR-ROOM — needs BOTH a producer (no war_room notif_type
    emitted today) AND the deeplink case in _notifDestination. CONFIRM scope w/ Captain. S→S-M.
7.5 HM-OLLIE-AI-WORKSPACE — only **Step 7 (ollie-machine)** is a true placeholder
    (Steps 2-6 shipped). Step 7a scaffold M ~3-4h; + Backtest-Lab 5b / Wave-Scope 6b live-wire. NOT 5 panes.
7.6 HM-INLINE-STYLE-SWEEP — Batches 1-5 ALREADY SHIPPED to main. Real work =
    HM-V4.5-TOKEN-EXTENSION precursor (M ~1.5-2h) THEN Batches 6-9 (hard-blocked until tokens land). L. NOT "start batch 1".
7.4 HM-V4.4-PALETTE-EXTENSION + HM-THEME-V4.5-DEPRECATIONS — theme. (folds with 7.6 token work)
7.7 HM-LCARS-COVERAGE Tier 2 — LCARS cards. LARGE, as time allows.
Report WAVE 7 before WAVE 8.

═══════════════════════════════════════════════════════════════════════════
WAVE 8 — O-TASTY SHADOW LOOP (build the autonomous machine)
═══════════════════════════════════════════════════════════════════════════
Only after WAVE 3 bugs fixed. SHADOW ONLY — zero order submission.
Build 5 loops per HM-O-TASTY-AUTOPILOT: IVR scan, structure+entry (shadow),
position manager (shadow), kill-switch, nightly auditor. swingdesk.db only.
All 4 structures, dual IVR gate (IVR≥50 AND IV≥35%), 20Δ, 15-ETF universe,
3% BPR / 35% soft / 50% hard / 20 max / 3-per-sector. CSP special-cased.
VERIFY: shadow writes would-have-traded rows, ZERO orders on PA3YVDTUH5CB.
Report WAVE 8.

═══════════════════════════════════════════════════════════════════════════
WAVE 9 — EVENTS BUS + EVENT TAPE (foundation specs)
═══════════════════════════════════════════════════════════════════════════
SCOPE CORRECTED 2026-05-28 (SC-6 pre-scope, verified vs live code/DB).
NOT a build-from-scratch: the events bus AND the event tape are ALREADY SHIPPED + LIVE
(price_ticks 222k rows, event_tape firing, signals_v2 live, /api/events/health up). WAVE 9
is a bug-fix + incremental, not foundation work.
9.1 **WEDGE (confirmed LIVE BUG, HIGH)** HM-EVENT-TAPE-DYNAMIC-SUBSCRIPTION —
    tick_recorder `_MAX_SUBSCRIBED_SYMBOLS=28` < 34 active positions, so fleet
    positions consume the whole budget BEFORE convergence candidates (CRSR/QCOM/HOOD…)
    get a slot → detector never sees them. Fix: reorder _get_universe() to reserve
    convergence slots + probe real IEX cap. ~2-4h. SHIP NEXT SESSION.
9.2 HM-EVENTS-BUS-FOUNDATION — ✅ ALREADY SHIPPED (events/signals_v2/engine_allocation
    + consumer + /api/events/health all live). No work; verify-only.
9.3 HM-OLLIE-EVENT-TAPE-V2-REALTIME — ✅ ALREADY SHIPPED (Alpaca IEX pivot live; spec archived to docs/archive/).
9.x follow-ups (incremental, MEDIUM): tier-aware sampling C1.x; remaining ~4 event
    detectors; signals_v2 pending-backlog triage (1,142 pending vs 591 exec).
9.4 HM-PRODUCER-DECISIONS-CYCLE-1.   9.5 HM-OLLIE-LIVE-SCANNER-DASHBOARD-TILE.
Report WAVE 9.

═══════════════════════════════════════════════════════════════════════════
OUT OF SCOPE TODAY (real dependencies, not deferral graveyard)
═══════════════════════════════════════════════════════════════════════════
- HM-BM-BAKEOFF + HM-PLUTUS-V6-CORPUS — gated on Plutus v6 (mid-June calendar).
- HM-IC-SQUADRON 50-close shadow — needs trade accumulation over time.
- HM-CAPITAL-LADDER enforcement — gated on IC Squadron validation.
- HM-TIER-5-MEAN-REVERSION — gated after inline-style-sweep (7.6).
- HM-GRADE-B-FLEET-GATE — monitoring, due 2026-06-04.
- HM-AM-TOTAL-PORTFOLIO-UNIFICATION — Captain previously deferred.
- O-Tasty live paper submission — needs 30 shadow trades + Captain approval.
- Real money — never.

═══════════════════════════════════════════════════════════════════════════
EXECUTION RULES
═══════════════════════════════════════════════════════════════════════════
- Report after each WAVE: shipped list + commits + blockers + "next wave?".
- Blocked item: note blocker, do precursor if in-scope, else flag + move on.
- Restart as needed; build > market timing.
- Data-dependent verify during market hours; schema/infra in lulls but TODAY.
- NEW bugs discovered mid-wave → ADD to active wave, no new deferral bucket.
  Fix-it-today is the standing rule.

═══════════════════════════════════════════════════════════════════════════
COMPLETION REPORT (end of run)
═══════════════════════════════════════════════════════════════════════════
Per wave: shipped / blocked / discovered-new. Full commit list. Remaining
waves if not all 9 done. What's genuinely DONE vs in-flight.
