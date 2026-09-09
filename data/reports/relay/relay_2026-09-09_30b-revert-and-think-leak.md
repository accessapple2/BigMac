# 30B Revert to 8B + Think-Suppression Failure Confirmed — 2026-09-09

**VERDICT: 8B stays production. qwen3:30b-a3b (cut over last night, commit `b1ccddf`) reverted this morning after going queue-bound in production (last 200 calls avg 38.4s, max 100s, trending up: 43/55/72s). Admiral's call after seeing both step 1 (8B) and step 3 (30B) numbers side by side: tomorrow stays on 8B.**

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

## Critical finding: think-suppression is broken for qwen3:30b-a3b, on both known mechanisms

Last night's relay flagged this as "not blocking, worth a look later" (see [[relay 2026-09-08 olliemax cutover]] item 3). Today, confirmed as a hard failure, not a stylistic quirk:

1. **API `think:false` field** — already sent (model_id.startswith("qwen3")), confirmed via raw `/api/generate` probe: response body has **no separate `"thinking"` key at all**; full chain-of-thought is baked directly into `"response"` on every call.
2. **`/no_think` prompt-token** (the pattern `scout_critic.py`/`mlx_provider.py` already rely on for exactly this failure mode) — added to `ollama_provider.py` (`_NO_THINK_PROMPT_MODELS = {"qwen3:30b-a3b"}`, prepends `"/no_think\n"` to the prompt) and re-tested with a raw call. **Also failed** — full CoT monologue still generated, this time even closing with a literal `</think>` tag with no matching opening tag, before the real answer.

6/6 test calls today (5 real McCoy prompts + 1 raw probe) leaked reasoning text into the visible response. This is a qwen3moe (MoE) architecture; the dense qwen3:8b/14b tags this codebase relies on elsewhere were the ones the 2026-04-27 think:false fix was originally verified against — this MoE tag apparently isn't wired the same way in this Ollama build's Modelfile/template.

**Not attempted tonight:** post-hoc stripping (splitting the response on the literal `</think>` tag when present) as a workaround, or checking whether `ollama show qwen3:30b-a3b --modelfile` declares a `thinking` capability at all. Either would need a deliberate pass before this model could be considered viable at any latency, independent of the queue-contention finding above.

## Taint window — decision output is suspect

**Any McCoy/Troi/Worf decision or War Room take generated by qwen3:30b-a3b between the 2026-09-08 21:18 MST cutover (commit `b1ccddf`) and the 2026-09-09 ~07:30 MST revert should be treated as suspect** — not clean in-character output, since think-suppression was silently non-functional the entire window. Do not bank any signal, take, or War Room debate result from that ~10h stretch as representative of the agents' actual reasoning quality.

## Code shipped (uncommitted at time of writing, this doc + code changes ship together)
- `config.py`: McCoy/Worf model fields reverted
- `ai_players` DB: McCoy/Worf model_id reverted (Troi untouched, already correct)
- `engine/crew_specialization.py`: Geordi advisory-pin reference reverted (inert)
- `engine/providers/ollama_provider.py`: `_NO_THINK_PROMPT_MODELS` + prompt-prefix added for qwen3:30b-a3b (verified NOT sufficient on its own — see finding above; kept because it's the theoretically-correct mechanism and harmless, not because it currently works)

## Open items
- Think-suppression for qwen3:30b-a3b remains unsolved — needs a deliberate pass (Modelfile capability check, Ollama version check, or post-hoc `</think>` stripping) before this model is reconsidered.
- Queue-contention hypothesis (step 3 direct calls 2-3x faster than production average) is not yet proven — would need instrumentation of `OllamaQueue` wait time vs. inference time to confirm before it factors into any future re-cutover decision.
