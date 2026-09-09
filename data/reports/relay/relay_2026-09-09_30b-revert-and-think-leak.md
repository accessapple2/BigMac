# 30B Revert to 8B + Think-Suppression Root Cause Confirmed — 2026-09-09

**VERDICT: 8B stays production (7.74s avg, improving further with olliemax tuning — see three-column table below). qwen3:30b-a3b (cut over last night, commit `b1ccddf`) reverted this morning after going queue-bound in production (last 200 calls avg 38.4s, max 100s, trending up: 43/55/72s).**

**Root cause of the think-leak found and fixed: `ollama show qwen3:30b-a3b` on olliemax shows `thinking` in capabilities and an `IsThinkSet` branch in the template — Ollama's `qwen3:30b-a3b` tag points at the **thinking-2507** build, which cannot be suppressed via either the API `think:false` field or the `/no_think` prompt token. Fix verified: `qwen3:30b-a3b-instruct-2507-q4_K_M` pulled and tested on 5 real McCoy prompts — 0/5 `<think>` leaks, clean in-character War Room takes, avg 18.60s/call (still ~3.7x slower than the tuned 8B's 5.08s, see three-column table). Fleet stays on 8B; instruct-2507 is a viable candidate on correctness now, not yet on latency.**

---

## Step 1 — revert McCoy/Worf to 8B, one restart, 10-call sample

**Code + DB changes** (config.py, ai_players table — DB is runtime-authoritative per `engine/agent_routing.py::_resolve_model_id`):
- `ollama-plutus` (McCoy): `qwen3:30b-a3b` → `plutus-v1` (qwen3:8b alias, per [[project_ollama_model_aliases_2026-08-25]])
- `qwen3-8b-flash` (Worf): `qwen3:30b-a3b` → `qwen3:8b`
- `options-sosnoff` (Troi): already `qwen3:8b` — untouched, was never actually migrated to 30B despite last night's commit message claiming all three
- `crew_specialization.py`'s `ollama-local`/Geordi advisory-pin dict entry: `qwen3:30b-a3b` → `gemma3:4b` (inert, seat is `halt_mode='full'`, reverted for reference-consistency only)

**Restart:** live trader.db write was starved by the running process (known pattern, see [[feedback_live_db_external_write_starvation]]) — resolved by stopping the single trader.log writer (pid 46143) cleanly, writing the DB update in the resulting zero-writer window, then relaunching via `scripts/trader_restart.sh` (single-writer gate passed, new pid 66575). One restart total, as instructed.

**Next-10-call sample, all landed on McCoy** (Worf/Troi didn't fire in the window):
```
8.67, 9.46, 6.72, 9.69, 10.56, 10.33, 3.90, 4.10, 1.91, 12.03  (seconds)
avg 7.74s | max 12.03s | min 1.91s
```
Matches last night's 7.39s step-1 8B benchmark almost exactly. vs. 30B production: avg 38.4s (~5x slower), max 100s.

## Step 2 — olliemax KV cache q8_0 + keep_alive=-1

Applied by Admiral directly on olliemax (`/etc/systemd/system/ollama.service.d/override.conf`, `sudo systemctl daemon-reload && restart`). Confirmed live via `systemctl show`: `OLLAMA_KV_CACHE_TYPE=q8_0`, `OLLAMA_KEEP_ALIVE=-1`. Restart completed ~07:48:44–07:48:53 MST (`journalctl -u ollama`, two `listening` lines — olliemax has 2x RTX 2080 Ti, one `llama_server` per card).

Splitting the 10-call sample at that restart:
- **Before (f16), n=4:** 8.67, 9.46, 6.72, 9.69 → avg **8.64s**
- **Boundary call (ambiguous, excluded):** 10.56s at 07:48:47, straddles the restart window
- **After (q8_0), n=5:** 10.33, 3.90, 4.10, 1.91, 12.03 → avg **6.45s**

Directionally positive for the 8B model too (no visible reload penalty), though n is small — not a controlled A/B, just what the live traffic happened to produce.

## Step 3 — 30B re-measured direct (not repointed)

5 real McCoy War Room prompts (`generate_hot_take`, same code path as production) fired directly at `qwen3:30b-a3b` on olliemax via a standalone parallel `OllamaProvider` instance — config/DB untouched, fleet never repointed. Real tickers pulled from today's actual `war_room_debates` rows (CASY, SUNB, SNAP, MEDP, META), real Alpaca snapshot quotes.

```
CASY 28.08s | SUNB 12.28s | SNAP 14.95s | MEDP 13.60s | META 15.74s
avg 16.93s | max 28.08s | min 12.28s
```
2-3x faster than the queue-bound production average (38.4s) — strongly suggests **queue contention**, not raw per-call model latency, was the dominant cost in production once other seats/War Room cycles were competing for the same `OllamaQueue` slots.

`/api/ps` after load: `qwen3:30b-a3b`, 19.29GB/19.67GB VRAM = **98.1% GPU** (up from ~94% at f16 last night), `context_length=16384` confirmed.

## Step 4 — olliemax NUM_PARALLEL=2 + queue-wait/model-time instrumentation

Admiral applied `OLLAMA_NUM_PARALLEL=2` on olliemax (5GB model comfortably fits the 11GB card's KV headroom at q8_0). Next-10-call sample under real load, all McCoy, several in rapid back-to-back succession (burst behavior, consistent with 2 concurrent slots actually being used):
```
5.66, 10.46, 4.51, 4.38, 4.29, 4.49, 4.65, 4.41, 3.66, 4.32  (seconds)
avg 5.08s | max 10.46s | min 3.66s
```

Then shipped direct measurement of contention itself rather than inferring it: `engine/ollama_queue.py`'s `_Task` now stamps `submit_ts`/`dispatch_ts`; `OllamaQueue.submit()` takes an optional `timing` dict populated with `queue_wait_s` and `model_time_s` (backward-compatible — the one other caller, `crew_scanner.py`, omits it and is unaffected). `ollama_call` log lines now read `wall=<s>s queue_wait=<s>s model_time=<s>s`. One trader restart to deploy (pid 72280, single-writer gate passed). Confirmed live: `queue_wait=0.00s` on the first 3 post-restart calls (McCoy solo, no contention in a quiet window) — a persistent watch is running for the rest of the session and will surface any call with real (>1s) queue_wait as it happens.

## Three-column summary — 8B tuning progression today

| Config | n | avg | max | min |
|---|---|---|---|---|
| 8B, f16 KV cache (baseline, pre-step-2) | 4 | 8.64s | 9.69s | 6.72s |
| 8B, q8_0 KV cache + keep_alive=-1 | 5 | 6.45s | 12.03s | 1.91s |
| 8B, q8_0 + `NUM_PARALLEL=2` | 10 | 5.08s | 10.46s | 3.66s |
| *(reference)* 30B thinking-2507 (`qwen3:30b-a3b`), direct calls | 5 | 16.93s | 28.08s | 12.28s |
| *(reference)* 30B instruct-2507 (`qwen3:30b-a3b-instruct-2507-q4_K_M`), direct calls | 5 | 18.60s | 20.60s | 14.69s |
| *(reference)* 30B (thinking build) in production, pre-revert | ~200 | 38.4s | 100s | — |

Not a controlled A/B (real production traffic, not synthetic load), but a consistent downward trend as each olliemax tuning step landed. 8B stays production regardless of what the instruct-2507 30B variant measures — this table is the "step 1 vs step 3" comparison the revert decision was made on.

## Root cause confirmed: qwen3:30b-a3b is the thinking-2507 build, cannot be suppressed

Last night's relay flagged the leak as "not blocking, worth a look later" (see [[relay 2026-09-08 olliemax cutover]] item 3). This morning: confirmed as a hard failure via both known suppression mechanisms —

1. **API `think:false` field** — already sent (`model_id.startswith("qwen3")`), confirmed via raw `/api/generate` probe: response body has **no separate `"thinking"` key at all**; full chain-of-thought is baked directly into `"response"` on every call.
2. **`/no_think` prompt-token** (the pattern `scout_critic.py`/`mlx_provider.py` already rely on for exactly this failure mode) — added to `ollama_provider.py` (`_NO_THINK_PROMPT_MODELS = {"qwen3:30b-a3b"}`, prepends `"/no_think\n"` to the prompt) and re-tested with a raw call. **Also failed** — full CoT monologue still generated, this time even closing with a literal `</think>` tag with no matching opening tag, before the real answer.

6/6 test calls that round (5 real McCoy prompts + 1 raw probe) leaked reasoning text into the visible response.

**Root cause, found via `ollama show qwen3:30b-a3b` on olliemax:** capabilities list `thinking`, and the template has an `IsThinkSet` branch — Ollama pre-fills an empty think block and the model reasons past it regardless of the `think` API field or in-prompt `/no_think` token. Cross-checked against Qwen's own repo: Ollama's `qwen3:30b-a3b` tag points at the **thinking-2507** build specifically, which has no non-thinking mode — this isn't a suppression bug, it's the wrong tag.

**Fix verified:** pulled `qwen3:30b-a3b-instruct-2507-q4_K_M` on olliemax (evicted the resident 8B for the pull/load, ~2min, accepted — paper day). 5 real McCoy War Room prompts, same direct-call methodology as the thinking-build test, config/DB untouched, fleet not repointed:
```
CASY 19.04s | SUNB 20.60s | SNAP 20.02s | MEDP 14.69s | META 18.67s
avg 18.60s | max 20.60s | min 14.69s | think_leaks: 0/5
```
Every response was a clean, in-character take (crew-name rival call-outs, price targets, no reasoning preamble, no `<think>`/`</think>` anywhere). The instruct-2507 tag genuinely fixes the correctness problem. It does not fix the latency gap: 18.60s avg is ~3.7x the tuned 8B's 5.08s (three-column table above) — so this is a real, viable model now on correctness grounds, but not a latency win over 8B as currently tuned.

## Taint window — decision output is suspect

**Any McCoy/Troi/Worf decision or War Room take generated by qwen3:30b-a3b between the 2026-09-08 21:18 MST cutover (commit `b1ccddf`) and the 2026-09-09 ~07:30 MST revert should be treated as suspect** — not clean in-character output, since think-suppression was silently non-functional the entire window. Do not bank any signal, take, or War Room debate result from that ~10h stretch as representative of the agents' actual reasoning quality.

## Code shipped
Commits `ea1269e` (revert + failed think-suppression attempt) and `ff420ce` (queue instrumentation), pushed to `exec-pipeline`:
- `config.py`: McCoy/Worf model fields reverted
- `ai_players` DB: McCoy/Worf model_id reverted (Troi untouched, already correct)
- `engine/crew_specialization.py`: Geordi advisory-pin reference reverted (inert)
- `engine/providers/ollama_provider.py`: `_NO_THINK_PROMPT_MODELS` + prompt-prefix added for qwen3:30b-a3b (does NOT fix the leak on the thinking-2507 tag — root cause is the tag itself, see above — kept as the theoretically-correct mechanism for any future qwen3moe model that does support it) + queue_wait/model_time split into the `ollama_call` log line
- `engine/ollama_queue.py`: `_Task.submit_ts`/`dispatch_ts`, `OllamaQueue.submit(timing=...)` optional output dict

## Open items
- `qwen3:30b-a3b-instruct-2507-q4_K_M` verified clean (0/5 think leaks) but not faster than tuned 8B (18.60s vs 5.08s avg). Not repointed. If a future case calls for 30B-class reasoning depth despite the latency cost, this is the tag to use — `qwen3:30b-a3b` (thinking-2507) should not be used for any fleet seat again.
- Queue-contention watch is live for the rest of the session (persistent Monitor, flags any `queue_wait` > 1s) — the step-3 "2-3x faster direct vs. production average" finding is now instrumented, not just inferred; no contention observed yet in today's quiet post-close-adjacent traffic.
