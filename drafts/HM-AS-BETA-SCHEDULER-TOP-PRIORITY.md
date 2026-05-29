# HM-AS-β SCHEDULER — SCOPE CORRECTED (verify-before-fix, 2026-05-28 PM)

> **2026-05-28 PM correction (Option B investigation).** The original framing
> below conflated TWO unrelated problems and was WRONG about the root cause of
> the "5.9 tail." A read-only instrumentation pass refuted the starvation
> hypothesis. The corrected scope is in **§A / §B / §C**. The original writeup
> is preserved verbatim at the bottom under "ORIGINAL (REFUTED)" for the record.

## TL;DR of the correction
- The "5.9 tail = 3 scheduler-starved scan agents" premise is **REFUTED**.
- Two of the agents were **never silent** — they scan constantly and gate
  legitimately. Two others are **dormant for roster/cadence reasons**, not
  single-thread contention.
- There are **three distinct problems**, not one. Only §C is genuine new
  scheduler work; §B is the real (separable) cadence-drift work; §A is mostly
  already fixed.

---

## §A — "Silent scan agents" — MOSTLY ALREADY FIXED (not a scheduler problem)

Evidence (decision_audit + crew_decisions + signals tables, live 2026-05-28):

| Agent | Reality | Verdict |
|---|---|---|
| **ollama-qwen3** | Scanned constantly today via crew path. `crew_decisions` 2026-05-28: `SNIPER_ALPHA_GATE` ×130, `AGENT_PASS` ×124. Last `OLLAMA_TIMEOUT` 2026-05-01 (stale). | **NOT silent.** Legitimately gating (composite_alpha < 0.25 / no setup). Working as designed. |
| **ollama-plutus** | Scanned constantly. `crew_decisions` 2026-05-28: `MANDATE_BLOCKED` ×652 + `AGENT_PASS` ×2. | **NOT silent.** McCoy waiting for its high-VIX regime. Working as designed. |
| **energy-arnold** | Last `crew_decisions` 2026-04-02; not in crew union. Only in `_SCAN_TIER2` (arena path). No fresh `localhost:11434` error rows since 2026-05-07. Wiring now resolves to `OLLIE_URL` (`main.py:124 default_url=OLLIE_URL`); model remapped to installed `ministral-3:3b`. | **Dormant via §C (TIER2 cadence), not wiring/parser.** The HM-ENERGY-ARNOLD-BIMODAL-PARSER diagnostic's "localhost wiring" root cause is **stale/resolved.** |
| **qwen3-8b-flash** | Zero `crew_decisions` rows ever; not in crew union; only `_SCAN_TIER2`. `active` + `qwen3:8b` (installed) + full CREW_MANIFEST mandate. | **Dropped from scan roster.** See §A.1 — doctrine conflict. |

**Why decision_audit showed "last emit 2026-05-07":** `decision_audit.signal_emit`
only records *fired signals* (a rare terminal outcome). PASS/gate verdicts land
in `crew_decisions` (crew path) or the `signals` table (arena path). Querying
only decision_audit made working agents look dead. The per-scan telemetry the
original plan wanted to "add" **already exists** (`crew_scanner._log_decision →
crew_decisions`, with `gate_result` + `reason`).

### §A.1 — qwen3-8b-flash (Worf): doctrine-vs-reality drift — CAPTAIN DECISION
- `CLAUDE.md:391` lists Worf under **"Sniper Squad — Active Scouts," ~25 sigs/day.**
- `engine/crew_specialization.py:76` comments **"benched S6.1 (-0.36%)."**
- Reality: **not in the crew scan union** → silent since 2026-05-07. Sibling
  scout Spock (`deepseek-7b-grok4`) IS in `RULES_SCANNERS` and emits daily.
- **DECISION: REPOINTED — SHIPPED 2026-05-28 (commit `f99e2e2`).** Captain ruled
  the S6.1 −0.36% bench stale (pre TIER3/conviction-stops/two-lane/remap refactors);
  align reality to doctrine, re-bench later if it underperforms on current-system
  data. `'qwen3-8b-flash'` added to `RULES_SCANNERS` (crew_scanner.py:262, beside
  Spock). LLM agent — still subject to Sniper Alpha gate. Activated on the
  2026-05-28 16:24 restart (PID 42748). **SOAKING:** confirm Worf scans + emits
  next market session (crew_scanner is market-hours-only).

---

## §B — Cadence drift — THE REAL (SEPARABLE) SCHEDULER WORK

> **5th refutation (2026-05-28 PM): the victims are NOT the fix.** Wrapping
> `battle_station_monitor` + the squeeze watchers would have done nothing —
> `squeeze_watcher` was already `_bg`-wrapped since β.2 yet still logged 161 drift
> warnings, proving the loop-blocker is elsewhere. And "918 warnings = these 3
> jobs" is an **instrumentation-coverage artifact**: only those 3 functions
> contain drift-logging code; every other job drifts silently.

**Root cause (from `[HM-BQ-instr]` wall-time data):** the single-thread
`schedule.run_pending()` loop is blocked by long *synchronous* jobs. Ranked:

| Job | total | n | max | avg |
|---|---|---|---|---|
| **`run_whisper`** | 39,061s | 47 | **1194s** | **831s** |
| `run_autopilot` | 4,006s | 42 | 168s | 95s |
| `run_strategy_scan` | 335s | 2 | 254s | 167s |
| `run_gap_scan` | 232s | 2 | 119s | 116s |
| `run_imbalance_scan` | 471s | 7 | 81s | 67s |

`run_whisper` (registered `every(10).minutes`, runs ~14min avg / 20min max) is
the dominant blocker — its 1194s max ≈ `battle_station_monitor`'s 1183s max drift.

**Fix — data-ranked blockers, loop-by-loop:**
- **Loop 1 — SHIPPED + VERIFIED 2026-05-28 (commit `a31d365`, PID 42748):** wrapped
  `run_whisper` in `_bg_whisper` (skip-if-prior-running lock, max 1 in-flight;
  mirrors β.2 pilot). Registration `do(run_whisper)` → `do(_bg_whisper)`.
  **VERIFIED post-restart from raw log:** (1) scheduler holds `_bg_whisper`
  (`Whisper Network armed`); (2) skip-lock fired on overlap (`16:44:47 Whisper bg:
  prior tick still running — skip`) — expected, max-1-in-flight; (3) `run_whisper
  wall=1037s` ran in background; (4) **non-blocking PROVEN** — gex_refresh/flow_lean/
  gap_fill_check/squeeze_watcher all fired on cadence DURING the 17-min whisper run.
  **Still soaking:** drift-warning RATE drop for battle_station/squeeze needs a
  market-hours window (those jobs are market-gated) — mechanism already proven.
- **Loop 2 — STAGED, not shipped:** wrap `run_autopilot` (#2 blocker). Ship ONLY
  if drift persists after the Loop-1 soak. Data first.
- **Loop 3 — only if needed:** the infrequent heavies (`run_strategy_scan`,
  `run_gap_scan`, `run_imbalance_scan`).

Do NOT blanket-wrap all 145 jobs. **This will NOT revive any tail agent** — it
addresses cadence integrity only.

---

## §C — Arena/TIER2 scan starvation — NEW finding, the real "scheduler" link

> **INSTRUMENTATION LIVE + SOAKING — 2026-05-28 16:24 (commit `a31d365`, PID 42748).**
> Read-only telemetry shipped in `main.py::run_scanner`, ZERO behavior change. DO
> NOT re-instrument and DO NOT assume §C is unstarted — data is accumulating:
> - `[HM-AS-β-C] scan_lock held {N}s ({scan-only|scan+WR})` — per-scan lock-hold
>   duration + whether `run_war_room` ran inside the held lock.
> - `[HM-AS-β-C] due-but-skipped: T1,T2,T3 (cum …)` on each skipped tick — how
>   often each tier is starved because the lock was held.
>
> **Next session: read this data from a clean market day (no restart in window)
> BEFORE proposing any decouple.** It will confirm or KILL §C. NOTE: the original
> §C premise (WR holds lock 3–19min) was REFUTED — WR cycles are now 34–66s post
> VRAM-fixes; the dominant lock-holder is likely the arena scan itself (~150s),
> not WR. The telemetry settles which.

`main.py::run_scanner` runs the tiered scan (`_SCAN_TIER1/2/3`) in a background
thread holding `_scan_lock`, and calls `run_war_room()` **inline** every 3rd
cycle while still holding the lock. WR cycles run minutes-long (3–19 min under
VRAM thrashing). During that window every `run_scanner` tick hits
`Scan skipped — previous scan still running` (178 skips logged). TIER2
(`DeptHeads` — energy-arnold, qwen3-8b-flash, options-sosnoff, …) has a 2h
cadence and is the first casualty: today it fired **1×** vs TIER1 **9×**.

- Lock release IS in a `finally` (no leak) — this is contention, not a deadlock.
- **Candidate fix:** decouple `run_war_room()` from inside the `_scan_lock`
  critical section (run it on its own thread/job), so a long WR cycle stops
  blocking TIER2/TIER3 scans. This is the actual mechanism keeping
  energy-arnold/qwen3-8b-flash dormant — distinct from §B's single-thread drift.
- Optional follow-on: bring `crew_decisions`-style PASS/ERROR telemetry to the
  arena scan path (TIER2 agents currently write outcomes to the `signals` table,
  including the error-row-as-signal anti-pattern flagged in the energy-arnold
  diagnostic). Only the arena path lacks the clean telemetry the crew path has.

---

## Next-session order (updated 2026-05-28 PM — post-batch)
1. **§B Loop-1 soak verify** — from a clean market day, confirm `[HM-AS-β]` drift
   warnings for battle_station_monitor + squeeze watchers DROP after the
   `_bg_whisper` ship. If they DON'T → ship Loop 2 (`run_autopilot` wrap, staged).
2. **§C soak verify** — read the live `[HM-AS-β-C]` telemetry (lock-hold +
   due-but-skipped) from a clean market day. Confirm-or-KILL §C BEFORE any
   scan-loop decouple. (Premise already refuted — WR now 34–66s, not 3–19min.)
3. **§A.1 Worf soak verify** — confirm qwen3-8b-flash scans + emits via crew path.

**SHIPPED 2026-05-28 (PID 42748):** §A.1 Worf repoint (`f99e2e2`), §B Loop 1
`_bg_whisper` (`a31d365`), §C telemetry (`a31d365`). §A (ollama-qwen3/plutus,
energy-arnold wiring) needs **no code** — verified working / already fixed.

⚠️ **Data caveat:** the "TIER2 fired 1× vs TIER1 9×" + "178 skips" figures in §C
above were from a CONTAMINATED window (12:55 restart + log rotation ~05-27 13:30
+ rich-console line-wrap). Trust the NEW `[HM-AS-β-C]` telemetry from a clean day,
not those numbers.

Risk note still holds for any §C decouple: changes job timing/concurrency —
design carefully, soak, do NOT rush at a session tail.

## SC-3 held items (intentional non-correction — do NOT re-flag)
The 2026-05-28 CLAUDE.md staleness sweep (commit `98ac9f9`) applied 11 of 13
corrections. Two were deliberately HELD — known drift, intentional non-correction,
see SC-3 audit 2026-05-28:
1. **Dax config-vs-DB** — `config.AI_PLAYERS` says `qwen3:8b`, DB `model_id` says
   `ministral-3:3b`. This is the documented Drift-Catalog-#1 runtime-override
   ambiguity; "correcting" the doc could be wrong. Left as-is by design.
2. **war_room `~` row counts** (`~1,447`/`~272` vs actual 2,649/337) — tilde-
   approximate by design; would re-drift immediately. Low-value churn, skipped.

---
---

## ORIGINAL (REFUTED) — preserved for the record

> The text below was the 2026-05-28 AM bank. Its central hypothesis (scan jobs
> starved by single-thread contention) was refuted by the PM investigation
> above. Kept verbatim so future sessions see how the wrong root cause was
> reached and corrected.

**Problem (as originally stated):** `main.py` registers 145 `schedule.every()`
jobs on a single-thread `schedule` loop; long jobs drift others. Hypothesis:
energy-arnold / ollama-qwen3 / qwen3-8b-flash scan jobs are starved by
single-thread contention (the 5.9 trace). — *Refuted: ollama-qwen3 is not
starved (scans constantly, gates legitimately); energy-arnold/qwen3-8b-flash are
roster/cadence-dormant, not single-thread-starved; the drift only ever hit
battle_station_monitor + squeeze watchers.*

Partial work shipped: `[HM-AS-β]` cadence-drift observability; HM-AS-β.2 Option A
`_bg()` pilot on `run_squeeze_watcher` (2026-05-08).
