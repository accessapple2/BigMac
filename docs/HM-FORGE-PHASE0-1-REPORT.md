# EPIC HM-FORGE — Phase 0 + 1 Report (2026-06-10)

Model/serving upgrade path. Sequenced execution, verify-before-fix. Report-only
where flagged STOP. Anomalies surfaced; no live-risk action taken.

## Phase 0 — Gating Verification

| Step | Result | Decision |
|---|---|---|
| **0.1** Ollie Max VRAM | **RTX 5080, 16303 MiB (16 GB)**, ~3.9 GB idle | **27B = NO-GO** (Q4 needs ~18 GB). Refresh set = `gpt-oss:20b` + `gemma4:12b-it-qat` only. Matches `CLAUDE.md` (two-7–8B budget). |
| **0.2** Ollama versions | olliemax **0.24.0**, bigmac **0.23.3** | **No CVE-forced upgrade.** The May-2026 headline CVEs (`CVE-2026-42248/42249`) are **Windows auto-updater** flaws — N/A to this Linux+macOS fleet. Cross-platform "Bleeding Llama" OOB-read fix was untagged at disclosure → no clean "patched build" to chase. bigmac-behind = feature gap (MLX), not security. |
| **0.3** bigmac MLX runtime | Apple **M4 / arm64**, Ollama 0.23.3 | MLX engine was **augmented in 0.24** (Gemma-4-via-MLX, ~2× inference; enable `OLLAMA_USE_MLX=1`). bigmac at 0.23.3 is **below** that build. Report-only. Note: bigmac no longer co-hosts Ollama (per `CLAUDE.md`), so MLX upside is limited to ad-hoc/dev use. |

## Phase 1 — Zero-Risk Wins

### 1.1 FA + KV-q8_0 patch — **ALREADY APPLIED (no action)**
The systemd drop-in on .168 **already carries** the patch:
```
OLLAMA_HOST=0.0.0.0:11434
OLLAMA_FLASH_ATTENTION=1
OLLAMA_KV_CACHE_TYPE=q8_0
```
The backlog/memory "PENDING Captain sudo" was **stale**. No sudo to key.
Post-patch reference (captured this session, `scripts/ollama_tier1_baseline.sh`):
- `qwen3:8b` eval **147.0 tok/s**, total 808 ms, KV q8_0 active.
- Clean 2-model co-residency: `ministral-3:3b` 4.4 GB + `qwen3:8b` 5.7 GB = **10.1/16 GB**. The co-residency squeeze the patch targeted is resolved.

`scripts/ollama_tier1_perf.sh` working copy is **past commit `2117f81`** — current
HEAD includes the `OLLAMA_HOST` LAN-bind preservation fix. The `--revert` path
remains available if needed.

### 1.2 Model pulls
- `gpt-oss:20b` — **pulled OK** (13 GB, `17052f91a42e`). Additive; live fleet intact.
- `gemma4:12b-qat` — **wrong tag, does not exist** (registry 404, silently masked
  by exit-0 trailing `ollama list`). Correct tag is **`gemma4:12b-it-qat`** (7.2 GB).
- `gemma4:12b-it-qat` — **BLOCKED: HTTP 412 Precondition Failed.** olliemax Ollama
  `0.24.0` is **below the build required** to pull this model. → **Decision needed:**
  upgrading olliemax Ollama is a *live-fleet-restart action* (brief inference outage +
  re-verify 9 models reload clean). Not pre-authorized by the (unmet) CVE premise.

### 1.3 War-Room bake-off harness — **built, run deferred**
`scripts/hm_forge_bench.py` (compiles; frozen NVDA CSP debate prompt; captures
tok/s, median wall, JSON-validity %, VRAM peak via Ollama HTTP `format=json`).
**Run deferred** because: (a) loading 13 GB gpt-oss evicts the live fleet from
16 GB → must run in a **market-closed window** (after 13:00 MST); (b) the 3rd
model (`gemma4:12b-it-qat`) is blocked on the 1.2 upgrade decision. Run will be
**2-way** (`plutus-v1` vs `gpt-oss:20b`) until gemma4 unblocks. Output →
`docs/HM-FORGE-BENCH-SCORECARD.md`.

### 1.4 Conviction backfill + shadow-enable — **NOTHING TO EXECUTE**
- **Backfill = no-op (and a blind one would corrupt data).** `positions` NULL
  conviction = **47.1% (32/68), not the backlog's 57%**. All 32 NULLs are
  **categorical-by-design**: `alpaca-mirror` (30, broker-mirror) +
  `enterprise-computer` (2, metals). **Zero** belong to `AI_SIGNAL_PLAYERS`.
  Filling them would feed garbage conviction into the stop logic. The **writer is
  already canonical** — `paper_trader.py` stamps `conviction`+`conviction_source`
  on every real BUY (36 populated rows).
- **Shadow-enable = not achievable as specified.** The only toggle is
  `_CONVICTION_SCALED_STOPS_ENABLED` (env, default `False`). Per
  `risk_manager.py:904-930`, ON computes a conviction-scaled stop and **appends a
  live `SELL`** — there is **no shadow/log-only branch**. Flipping it is a *live
  stop-loss behavior change*, walled behind separate Admiral approval. **Flag left
  `False`.**

## Decisions for the Admiral
1. **Authorize olliemax Ollama upgrade?** Required to unblock `gemma4:12b-it-qat`
   (412). Live-fleet restart — sequence with a quiet window; `ollama list`
   before/after + smoke each model. Also unlocks 0.24+ feature parity.
2. **Benchmark run window** — confirm a market-closed slot for 1.3 (2-way now,
   3-way post-upgrade).
3. **Conviction-stop** is live-only (no shadow). Separate go/no-go if desired —
   note all current AI-signal positions already have populated conviction, so the
   flag would function cleanly, but it *acts live*.

## Phase 2 (spec-only)
- `drafts/HM-FORGE-PHASE2-1-VLLM-POC-SPEC.md`
- `drafts/HM-FORGE-PHASE2-2-MCCOY-V6-SPEC.md`

<!-- appended by hm_forge_phase15.sh 2026-06-10T21:02:28Z (3-way) -->
# HM-FORGE Phase 1.3 — War-Room Bake-off Scorecard

_Captured 2026-06-10T21:02:45Z on Ollie Max (.168, RTX 5080 16GB), 5 runs/model, frozen NVDA CSP debate prompt, format=json._

| Model | tok/s | median wall (s) | JSON-valid % | VRAM peak (MiB) |
|---|--:|--:|--:|--:|
| plutus-v1:latest | 166.3 | 0.25 | 100.0 | 4742 |
| gpt-oss:20b | 175.4 | 4.51 | 100.0 | 12512 |
| gemma4:12b-it-qat | 88.1 | 6.37 | 100.0 | 7726 |
