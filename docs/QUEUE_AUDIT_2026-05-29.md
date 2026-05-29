# Queue Audit — 2026-05-29 (end of warp-10 sweep)

**Resume point for future sessions.** THE-ALL-OUT-PLAN-2026-05-28 is CLOSED/HISTORICAL.
Work from `docs/XO_BACKLOG.md` + the gated list below.

20 commits landed today (not "30+"). Honesty notes: auth = 1-of-11 routes;
notif-deeplink = half (producer gated); HM-AS-β solved cadence *drift*, the §C
scan-lock *stall* is NOT solved (watchdog pending).

## ✅ Shipped today
- **Scheduler arc (HM-AS-β):** Loop 2 `_bg_autopilot`, Loop 3 battle_station daemon, §C HELD-INFLIGHT heartbeat, boundary-isolate doctrine.
- **WAVE 9.1 wedge (HIGH):** tick-recorder cap fix (reserve convergence + 28→30; JTAI ticks).
- **Roster hygiene:** Worf reconcile + advisory-crew sweep (5 agents off `_SCAN_TIER2`/`SNIPER_AGENTS`).
- **Morning ops fires (emergent):** O-Tasty `--reload` drop, cloudflared dedup-guard, dayblade flash-guard, push-health monitor.
- **Tier A sweep:** Archer-500 fix, auth-TOTP (B-1, 1 route), OAI-toast clarity; A-6 MEMORY trim (non-git, 36.6→23.5KB); A-4 = false positive.
- **WAVE 7 frontend:** LCARS-T1 (7.2), notif-deeplink half (7.3).
- Tier C restart verified 10/10.

## ⛔ Gated (blocker each)
- HM-RUN-SCAN-WATCHDOG — **data-ready**, needs focused design session.
- HM-NOTIF-WAR-ROOM-PRODUCER — needs Captain trigger definition.
- HM-BS-DAEMON-HEARTBEAT — next main.py restart.
- HM-RISK-MANAGER-CONVICTION-STOP — precursor met; gated on ~57% NULL backfill + flag-enable.
- §B Loop 3 #2 (gap/strategy wraps) — held, data-gated (only if squeeze-watchers spike).
- HM-MEMORY-DEEP-AUDIT-Q2 — quarterly (112 files).
- HM-FRONTEND-VISUAL-TEST-HARNESS — gated on WAVE 7 growth.
- HM-SIGNALS-V2-STALE-SWEEP — see below (filed today).
- review-2026-06-04 Worf — date + bear cycle.

## ➡️ Carry-forward (real scope, not done)
- **WAVE 7:** 7.1 AN-Bridge proxies (~2-3h), 7.5 ollie-ai Step 7 (~3-4h), 7.6 inline-style B6-9 + token precursor (L), 7.7 LCARS Tier 2 (12-16h), 7.4 palette.
- **WAVE 9.x:** tier-aware sampling, ~4 remaining detectors, signals_v2 pending-backlog triage, 9.4 producer-decisions, 9.5 live-scanner tile.
- **Standing:** HM-ALERT-AUTH-STORM, HM-ALPACA-BRIDGE-LIMIT-FIX, HM-DEEPSEEK-CONCENTRATION-CAP-V2, HM-CONVICTION-TIER-BOUNDARY (pending Admiral), HM-SCHWAB cross-mechanism alarm, HM-QG-FLOAT-TRUNCATION, HM-TRADES-MIRROR-GAP.

## 🔒 Out-of-scope (real dependencies)
- Plutus v6 (HM-BM-BAKEOFF, V6-CORPUS) — mid-June.
- HM-IC-SQUADRON 50-close shadow — trade accumulation.
- HM-CAPITAL-LADDER — gated on IC validation.
- HM-TIER-5-MEAN-REVERSION — gated after 7.6.
- HM-GRADE-B-FLEET-GATE — monitoring, due 2026-06-04.
- HM-AM-TOTAL-PORTFOLIO-UNIFICATION — Captain deferred.
- O-Tasty live paper — 30 shadow trades + approval (0/30).
- Real money — never.

## 🚩 Stale / rot / call-outs
- THE-ALL-OUT-PLAN-2026-05-28 — CLOSED today (header marked).
- HM-DATA-INTEGRITY-FORENSICS parent ticket (XO_BACKLOG, 2026-05-25) — sub-tickets shipped; **verify-for-closure** (possible queue-rot).
- §C scan-lock stall — the one active open wound; watchdog is data-ready, top real item.
- No invented work; A-4 was the only false positive.

## 🔬 signals_v2 stale-sweep diagnostic (read-only, 2026-05-29)
- status mix: stale 927 / pending 815 (was 1142, draining) / executed 601 / failed 289. No `expired` rows despite `events_bus.mark_signal_expired` writing that status (path inert).
- pending age: 168 (0-1h), 323 (1-6h), 239 (6-24h), 87 (1-7d).
- **123 pending are PAST their `stale_after` but still `pending`** (93 @ 6-24h, 30 @ >24h) — not expired.
- Newest *stale* signal created yesterday 16:14; newest *executed* today 15:27 → consumer still executes fresh pending but old past-stale ones accumulate.
- Mechanism: `events_bus_consumer` owns pending→stale (consumer-driven, NOT a scheduled sweep), reads `WHERE status='pending'`. The 123 stuck = signals the consumer hasn't reached.
- **Severity: MEDIUM (hygiene/bloat, NOT wrong-trade risk)** — `buy()` has an internal stale-gate, so stuck-pending can't be executed as fresh. Harm = pending-bucket bloat + status-column inaccuracy. → HM-SIGNALS-V2-STALE-SWEEP.
