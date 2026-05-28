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
- **Decision needed:** if Worf should be active per CLAUDE.md, repoint —
  one line: add `'qwen3-8b-flash'` to `RULES_SCANNERS` (crew_scanner.py:256)
  or `ALPHA_SQUAD`. This starts a dormant agent firing live signals into the
  gate after a 3-week silence + a documented −0.36% bench — **signal-flow
  change, needs Captain sign-off.** If the bench is still intended, update
  CLAUDE.md:391 to reflect benched status instead (kill the drift either way).

---

## §B — Cadence drift — THE REAL (SEPARABLE) SCHEDULER WORK

This is the genuine HM-AS-β perf issue, but it is **narrowly scoped** and does
**not** touch signal flow. All 918 `[HM-AS-β]` drift warnings name exactly:

| Job | Drift warnings | Target cadence |
|---|---|---|
| `battle_station_monitor` (2-min options monitor) | 719 | 120s (drifts to 1183s) |
| `squeeze_watcher` | 161 | 1800s |
| `bbkc_squeeze_watcher` | 38 | 1800s |

No scan job ever appears in the drift log. The crew scanner completes every
cycle (~150s) with **0** "previous cycle still running" skips.

**Fix (HM-AS-β.3, surgical — matches shipped β.2 `_bg()` pilot):** wrap
`run_battle_station_monitor` + the two squeeze watchers in the `_bg()`
fire-and-forget wrapper (bounded by a small ThreadPoolExecutor). Do NOT
blanket-wrap all 145 jobs. APScheduler migration only if `_bg()` doesn't clear
the drift. Soak + confirm drift warnings drop. **This will NOT revive any tail
agent** — it addresses cadence integrity only.

---

## §C — Arena/TIER2 scan starvation — NEW finding, the real "scheduler" link

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

## Corrected next-session order
1. **§C** — decouple inline `run_war_room` from `_scan_lock` (revives TIER2 cadence).
2. **§B** — `_bg()` wrap on battle_station_monitor + squeeze watchers (cadence drift).
3. **§A.1** — Captain decision on Worf repoint vs CLAUDE.md drift fix.
- §A (ollama-qwen3/plutus, energy-arnold wiring) needs **no code** — verified working / already fixed.

Risk note from original still holds for §B/§C: changes job timing/concurrency —
design carefully, soak, do NOT rush at a session tail.

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
