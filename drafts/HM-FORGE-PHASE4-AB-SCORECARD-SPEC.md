# HM-FORGE Phase 4 — War Room Witness A/B Scorecard (SPEC ONLY, no execution)

**Status:** spec doc only — no code, no schema changes, no execution. Admiral-approved.
**Paste-order dependency:** only meaningful AFTER Phase 3 wires the 1.3 bake-off
winner as a War Room debate witness in A/B rotation vs `plutus-v1` and begins
tagging every debate record with `witness_model`. This document **pre-registers**
the 2-week evaluation BEFORE the data matures — no goalpost moves after day 1.

## 1. Purpose
Decide, on pre-registered evidence, whether the 1.3 winner (the `witness_model`
challenger) should become a co-equal War Room rotation witness alongside
`plutus-v1`, be extended for more data, or be reverted. The grader is a
**read-only** reporter over debate records — it touches no execution path; all
four gates and RULE #1 are unaffected.

## 2. Window & sample gate (pre-registered)
- **Window:** 14 calendar days, starting at the **first debate tagged with the
  new `witness_model`** (Phase 3's first rotation slot for the challenger).
- **Minimum sample gate:** **≥20 debates per arm** (`plutus-v1` arm and
  `witness_model` challenger arm) by day 14.
- **Automatic extension (not a judgment call):** if either arm is below 20 at
  day 14, extend in **7-day increments until both arms reach ≥20**. The
  extension is mechanical; it is NOT the EXTEND decision gate in §6.
  - (The §6 EXTEND gate is a one-time, Admiral-keyed decision about *mixed
    primary results*; this §2 extension is about *raw sample size*. Distinct.)

## 3. Primary metrics — decide the outcome
Each measured **per arm** over the window. Bars are pre-registered; no metric
is re-defined after collection begins.

| ID | Metric | Source | Pre-registered bar |
|----|--------|--------|--------------------|
| **M1** | JSON-validity rate (structured verdict parses clean AND is schema-complete against `{verdict, conviction, reason}`) | debate record structured-output field; same validity test as `hm_forge_bench.py::one_call` (`json_valid_pct`) | challenger **≥ `plutus-v1`** AND **≥ 95% absolute** |
| **M2** | Wall-clock per debate turn, **p50 + p95** | per-debate turn duration in the debate record | challenger **p95 ≤ `plutus-v1` p95**; tie to the `[WR-PROVIDER-DUR]` ~19m35s fan-out baseline (memory `project_hm_wr_provider_latency`) |
| **M3** | Timeout / discard rate against the **90 s ceiling** (the OllamaQueue starvation discard, `project_hm_wr_ollama_queue_starvation`) | count of debate turns that hit the 90 s timeout / were discarded ÷ turns, per arm | challenger **≤ `plutus-v1`**; **any arm discard rate > 5% = automatic FAIL** regardless of every other metric |

**Zero-discard consistency (self-verify §):** M3's discard framing matches the
HM-FORGE **Phase 2.1 vLLM PoC** success criterion of **zero 90 s-timeout
discards** (`drafts/HM-FORGE-PHASE2-1-VLLM-POC-SPEC.md`). The aspirational target
is **zero** discards per arm; the **> 5% auto-FAIL** here is the hard floor, not
a relaxation of the zero-discard goal. A challenger that introduces *any*
discards relative to a zero-discard `plutus-v1` arm fails M3.

## 4. Secondary metrics — context, never decisive alone
Reported for interpretation; **none can graduate or revert on its own.**

| ID | Metric | Source | Use |
|----|--------|--------|-----|
| **M4** | tok/s (mean) + VRAM peak | existing bench instrumentation — `hm_forge_bench.py` fields `tok_s`, `vram_peak_mib` (`nvidia-smi --query-gpu=memory.used`) | throughput/footprint context; ties M2 to the 16 GB ceiling |
| **M5** | Debate-outcome divergence: % of debates where the challenger's verdict differs from what `plutus-v1` produced in **adjacent rotation slots on comparable signals** | paired debate records by signal/ticker within the rotation | **report-only** — divergence is information, not failure |
| **M6** | Conviction calibration: where debates attach a `conviction` score to paper positions with realized outcomes **inside the window**, bucket conviction vs realized **R-multiple**, per arm | debate `conviction` ⟷ paper-position realized R | **sparse data expected** — report **n per bucket**; **draw no conclusion under n = 10** in a bucket |

## 5. Reflexion wiring — interface spec (build later)
Define the JSON the A/B grader emits **per debate** so `engine/self_improvement.py`
(`run_daily_reflection()` → finmem Layer 3, per its module doctrine) can consume
it as a FinMem input later. **No consumer is wired in this phase** — schema only,
exactly like the kirk_briefing R3 sidecar.

**Naming convention — one across both sidecars.** Align field naming with the
`kirk_briefing.py` R3 `.json` sidecar (`schema_version` int, `consumer: null`
until wired, snake_case keys, ISO `*_at` timestamps). Per-debate grader record:

```json
{
  "schema_version": 1,
  "consumer": null,
  "kind": "wr_witness_ab",
  "debate_id": "<debate record id>",
  "witness_model": "<challenger model id | plutus-v1>",
  "arm": "challenger | baseline",
  "signal_ref": "<ticker/signal id of the debated item>",
  "generated_at_az": "<ISO8601 America/Phoenix>",
  "m1_json_valid": true,
  "m2_turn_wall_s": 0.0,
  "m3_discarded": false,
  "m4_tok_s": 0.0,
  "m4_vram_peak_mib": 0,
  "m5_verdict": "SELL | PASS | WAIT",
  "m5_diverged_from_baseline": null,
  "m6_conviction": 0.0,
  "m6_realized_r": null
}
```

Window-level aggregates (the day-7 interim and final scorecards) are computed
from the per-debate records above — they are NOT a separate emitted schema; the
grader rolls up p50/p95, rates, and bucket counts at report time.

## 6. Decision gates at window close — pre-registered, Admiral keys the call
Exactly **one** of the three is stated in the final report. **No silent middle path.**

- **GRADUATE** — **all primary bars met** (M1 ✓ AND M2 ✓ AND M3 ✓, with both
  arms ≥20). → challenger becomes a **co-equal rotation witness**. This is a
  doctrine change executed under a **separate directive** (not by the grader).
- **EXTEND** — primary bars **mixed**, or sample still thin after the §2
  mechanical extension. → **+14 days, once only.** A second EXTEND is not
  available; the next close must be GRADUATE or REVERT.
- **REVERT** — **M3 auto-FAIL** (>5% discards on the challenger) **OR M1 below
  its bar.** → challenger **removed from rotation, archived (never deleted),
  `plutus-v1` sole witness**. Per sacred-data rule, the witness wiring + records
  are preserved for a future re-bake.

## 7. Reporting
- **Day-7 interim scorecard:** NTFY `ollietrades-admin` + archived md under
  `data/` (dated, never overwritten/deleted). Interim is **awareness-only** — it
  carries no gate decision and cannot move a goalpost.
- **Final scorecard (window close):** the full M1–M6 table per arm + the single
  §6 gate recommendation, NTFY `ollietrades-admin` + archived md.
- Both scorecards mirror the bench's markdown table shape where applicable
  (`| Model | tok/s | median wall (s) | JSON-valid % | VRAM peak (MiB) |`),
  extended with p95 wall and discard rate (the A/B-specific columns).

## 8. Safety posture (unchanged by this phase)
- The grader runs **read-only** against debate records — **zero execution-path
  touch.** All four gates and **RULE #1 (Schwab hands-off)** are unaffected.
- No `config.py` / agent-routing / doctrine change happens inside this window;
  GRADUATE's doctrine change is a separate, Admiral-keyed directive.
- The A/B is **report-only comparison** for the full 2 weeks — `plutus-v1`
  remains the load-bearing witness throughout; the challenger only adds rotation
  slots that are measured, never authoritative, until GRADUATE.

## 9. Known spec anomalies (flagged, not blockers)
1. **p95 is not in the bench's current output.** `hm_forge_bench.py` emits
   `wall_med` (median only). M2 requires p50 **and** p95 — the grader must
   compute both from **per-debate turn durations** in the debate records (the
   single-shot bench median is the 1.3 baseline reference, not the window
   measurement source). Capturing per-turn wall-clock on debate records is a
   Phase-3 wiring detail this spec depends on.
2. **`witness_model` store is Phase-3-defined.** The grader reads whichever
   debate record store Phase 3 tags (the War Room structured-debate path writes
   `debate_history_v2`; the exact column name + the per-turn duration field are
   set by Phase 3, consumed here).
3. **M5 pairing needs a comparability key.** "Adjacent rotation slots on
   comparable signals" requires a signal/ticker key shared across arms; if Phase
   3's rotation does not debate the same signal in both arms, M5 degrades to a
   distributional comparison (report-only either way — never decisive).

## 10. Out of scope (separate epics)
GRADUATE's doctrine change; multi-witness (>2-arm) rotation; the vLLM serving
cutover (Phase 2.1); any change to the 90 s ceiling itself. This spec covers the
measurement + decision protocol only.
