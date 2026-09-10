# Relay — MiniMax M3 bakeoff provider + 4-arm smoke test, 2026-09-10

## What shipped

Per `docs/XO_PLAN_2026-09.md` Phase 2 ("McCoy Rank" model bakeoff), built
the two pieces that were open: a MiniMax provider (arm 3) and a registry
tying all four non-Claude arms together for a smoke test. **Bakeoff not
run** — this is arm-building only, as directed.

- `engine/providers/minimax_provider.py` — new `MiniMaxProvider(AIProvider)`.
  Same `call_model(prompt) -> str` interface as `OllamaProvider` so a future
  harness can swap arms by object. Hits MiniMax's native endpoint
  (`https://api.minimax.io/v1/text/chatcompletion_v2`) directly with
  `requests`, not the `openai` SDK — MiniMax's real path isn't the SDK's
  assumed `/v1/chat/completions`, so pointing the SDK's `base_url` there
  would 404. Mirrors `xo_brief.py::call_grok`'s shape (OpenAI-style message
  list, OpenAI-compatible non-OpenAI vendor, key from `.env`) per the
  plan's explicit instruction to follow that pattern.
  - Model string: `MiniMax-M3`. **`service_tier` deliberately never set** —
    confirmed live the account default is `"standard"` without it.
  - Cost: `$0.30/M in, $1.20/M out`, computed from the real `usage.
    prompt_tokens`/`completion_tokens` in each response (not the repo's
    char/4 estimator in `engine/cost_tracker.py` — this arm has no
    `ai_players` row so that tracker's DB-driven free/paid check doesn't
    apply cleanly to it). Kept the cost ledger in-process
    (`total_cost_usd`/`total_input_tokens`/`total_output_tokens` on the
    instance) rather than writing into `api_costs` — bakeoff-only,
    deliberately not touching fleet cost-tracking tables.
  - **MiniMax-M3 is a reasoning model** — usage splits `content` from
    `reasoning_content` cleanly (no `<think>` tag-stripping needed, unlike
    qwen3), but a too-small `max_tokens` can be entirely consumed by
    reasoning before any `content` is emitted. Live-verified 2026-09-10:
    `max_tokens=8` → `content=""` (all 8 tokens went to reasoning);
    `max_tokens=200` → `content="PONG"` (23 of 25 completion tokens were
    reasoning). Default budget set to 4096 (matches `call_claude`/
    `call_grok`'s convention in `xo_brief.py`) specifically to avoid this
    failure mode recurring silently.

- `scripts/mccoy_bakeoff_arms.py` — registry + smoke test for the 4 arms
  the directive named (qwen3:8b, plutus-v1-real, MiniMax-M3, Fin-R1). Arm 2
  (Claude, `xo_brief.py::call_claude`) already exists and isn't part of
  this script. Not wired to `config.AI_PLAYERS` — no fleet seat reads this
  file or the new provider.

## Fin-R1 — pulled, registered, format-validity flagged per the Captain's live finding

`ollama pull hf.co/mradermacher/Fin-R1-GGUF:Q4_K_M` was already run
(Captain, in parallel with this session) — confirmed present on olliemax
(`100.95.195.20:11434`, 4.68GB) alongside `fin-r1:latest` and
`fin-r1-en:latest`. Registered the **base pulled tag**
(`hf.co/mradermacher/Fin-R1-GGUF:Q4_K_M`) as the arm, per the original
instruction's exact pull command — not the `fin-r1-en` variant, which the
Captain flagged mid-session as still being validated.

**Live finding from the Captain, carried into this build:** first smoke
test on a free-form finance prompt code-switched into Chinese mid-answer
(Fin-R1 is Chinese-origin). Capability isn't the open question — English
format reliability is. Documented directly in
`scripts/mccoy_bakeoff_arms.py` next to `FIN_R1_MODEL_TAG`: whichever tag
runs Phase 2 for real, score output-language/format-parse rate explicitly
in `parse_decision()` — a model that code-switches mid-answer fails
parsing regardless of analysis quality. Same posture the plan doc already
applies to `plutus-v1-real`'s `SCORE:`/`VERDICT:`-format risk. If the
Captain's `fin-r1-en` variant validates as stable, swap `FIN_R1_MODEL_TAG`
in the registry — one line.

## Live-verified: all 4 arms answered a one-token prompt

`.venv/bin/python3 scripts/mccoy_bakeoff_arms.py`, prompt `"Reply with
exactly one word: PONG"`:

```
[OK] qwen3:8b         wall=0.09s response='PONG'
[OK] plutus-v1-real   wall=7.36s response='PONG'
[OK] fin-r1           wall=4.60s response='PONG'
[OK] minimax-m3       wall=5.43s response='PONG'

minimax-m3 spend this run: $0.000094 (in=184 out=32 tokens)

4/4 arms answered.
```

`plutus-v1-real` and `fin-r1` returned clean text with no leaked
chain-of-thought — neither model_id matches `OllamaProvider`'s
`think:False` qwen3 guard (`_QWEN3_ALIAS_MODEL_IDS` doesn't include
`plutus-v1-real`, and Fin-R1 isn't a qwen3 derivative), so this is real
inference behavior, not a suppressed leak. `py_compile` clean on both new
files.

## Follow-up (same day) — reasoning tokens tracked separately, suppression confirmed real

Captain's follow-up after the initial ship: score M3's reasoning cost
separately from visible-output cost, test whether `reasoning_content` can
actually be suppressed (vs. qwen3:30b-a3b's unsuppressible CoT), and log a
sample of the reasoning itself since it's the one arm in the bakeoff whose
thinking is observable at all.

- **Suppression is real and verified live** — unlike qwen3:30b-a3b,
  `"thinking": {"type": "disabled"}` genuinely turns reasoning off for M3:
  a call dropped from 25 completion tokens (23 reasoning) to 2, with
  `reasoning_content` absent from the response entirely. `MiniMaxProvider`
  now takes `thinking_mode: str | None` (default `None` = thinking on, the
  API's own default) — pass `"disabled"` per-instance to run the
  cheaper/faster path for comparison. Confirmed both ends live.
- **Reasoning tokens now tracked separately from visible-output tokens** —
  `usage.completion_tokens_details.reasoning_tokens` read per call;
  `total_reasoning_tokens` / `total_visible_output_tokens` accumulate
  alongside the existing `total_cost_usd`. Both are billed at the same
  output rate (no separate MiniMax price tier for reasoning vs. visible
  tokens), so this is a cost-*attribution* split, not a different rate.
  **Live magnitude, 3 trivial one-word-reply calls:** 199 total output
  tokens, 194 of them (97.5%) reasoning — a 1-2 token visible answer
  routinely cost 12-156 reasoning tokens underneath it. Worth watching at
  Phase 2 scale: the visible answer length says almost nothing about the
  real per-decision cost for this arm.
- **`reasoning_content` sampling added** — `reasoning_sample_every` (default
  5) keeps the full `reasoning_content` + `content` pair for every Nth call
  in `self.reasoning_samples`, and logs a 500-char preview at INFO for
  sampled calls only (unsampled calls stay cheap to log). Live-verified the
  captured text is genuine chain-of-thought (e.g., call 2's sample: *"The
  user is asking me to reply with exactly one word... I need to think
  about this more carefully..."* before answering `TWO`) — reads exactly
  like the qualitative signal the Captain was after, not a summary.
  Confirmed the field is correctly absent (empty string via `.get()`, not a
  KeyError) when `thinking_mode="disabled"`.

## NOT done — by design, per the directive

- **Bakeoff itself not run.** No screened-list iteration, no `-5..+5`
  scoring, no top/bottom-20 basket. That's the next piece, gated on the
  Phase 2 acceptance number (TBD, per the plan doc) once Phase 0's baseline
  distribution is known.
- **No fleet wiring.** Neither `MiniMaxProvider` nor the Fin-R1 arm touch
  `config.AI_PLAYERS`, `engine/agent_routing.py`, or any `ai_players` row.
  Bakeoff arm only, exactly as directed.
- **`fin-r1-en` not adopted** — the Captain's parallel validation of that
  variant is still in progress; registry points at the base tag until
  that's confirmed.
