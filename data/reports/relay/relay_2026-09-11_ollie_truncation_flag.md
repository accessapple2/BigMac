# Relay — 2026-09-11 afternoon. Ollama truncation fix + decision flagging + Riker gemma3 bug.

Triggered by olliemax's `~/modelworks/fleet_checks/ollama_churn/FINDINGS_FOR_SCOTTY.md`
(read live via `ssh olliemax`, updated 2026-09-11 12:25 MST). Three asks from
that doc + the Admiral, all closed this session. Restart to pick these up
is scheduled for after today's 13:00 MST close (see bottom).

## 1. Root cause + fix: Arena's num_ctx was silently truncating McCoy to ~30%

**What was happening:** `engine/providers/ollama_provider.py::_DEFAULT_NUM_CTX`
was `10240`, sized off a 2026-07-07 p95 that no longer reflects real traffic.
McCoy's (ollama-plutus) screened-scan prompts actually run 15,455-17,942
tokens (median 16,875). Ollama truncated 3,628 of these to 5,122 tokens
(median 30.4% kept) between 2026-09-09 07:42 MST and 2026-09-11 06:50 MST,
with **no error anywhere** — Ollama logs a WARN to its own journal, not
ours, and returns a normal 200. It stopped only because the large premarket
scans haven't run since this morning's restart; it returns Monday premarket
if not fixed.

**The "other caller" (not the Arena) sending 10,240:** confirmed to be
**Worf** (`qwen3-8b-flash`, `halt_mode='active'`). Config comments show
Worf's DB `model_id` is literally the tag `"qwen3:8b"` (not the `plutus-v1`
alias McCoy uses) — same weights (`_QWEN3_ALIAS_MODEL_IDS`), different tag
name, same shared client-side default. This matches olliemax's Priority-3
table exactly: `plutus-v1:latest` (McCoy) loaded 23x at ctx 10240 vs 1x at
24576; `qwen3:8b` (Worf) loaded 3x at 10240 vs 25x at 24576 — Worf's calls
are far less frequent than McCoy's screened scans, which is exactly the
23-vs-3 split. `scripts/mccoy_bakeoff_arms.py` + `mccoy_bakeoff_readiness_
check.py` (the fin-r1 arm, tested with McCoy's real full-size prompt) share
the same default and are the source of the 7 fin-r1 truncations.

**Fix:** `_DEFAULT_NUM_CTX` raised `10240 -> 24576` in `engine/providers/
ollama_provider.py`, matching what olliemax's qwen3-weight tags (plutus-v1,
qwen3:8b, qwen2.5-coder:7b, qwen3:4b, ministral-3:3b — all five
byte-identical) already carry as their own Modelfile `num_ctx`. This is a
single shared default, so it fixes McCoy, Worf, and both bakeoff scripts in
one change — an explicit value (not just omitting `num_ctx` and trusting
whatever the tag happens to be set to) keeps the original HM-PERF-FLEET-
THROUGHPUT intent of a known, predictable per-slot VRAM budget for 2-worker
co-residency. Also ends the runner-restart churn olliemax's Priority 3
documented (50 of 52 back-to-back restarts on the qwen3 weights were this
default fighting the tags' own 24576, not a real model swap).

The `qwen3:30b-a3b*` overrides in `_NUM_CTX_OVERRIDES` are left in place —
confirmed via `ai_players` that no active seat uses that model anymore
(McCoy and Worf both reverted to 8B same-day, HM-OLLIE-30B-BAKEOFF-REVERT
2026-09-09 after-hours) — inert today, harmless, left for a future 30B
re-bakeoff rather than deleted.

**Not yet live** — code change only; needs the trader restart scheduled
below.

## 2. Decision-row flagging (RULE #1 — additive only, verified no deletions)

Backed up `data/trader.db` to `data/backups/trader_pre_truncation_flag_
2026-09-11.db` before any schema change.

`scripts/hm_ollie_truncation_flag.py` (new):
- Loaded all 4,575 rows of olliemax's `truncated_prompts_2026-09-09_to_
  2026-09-11.csv` verbatim into a new table, `ollama_prompt_truncations` —
  raw source of truth, in-DB.
- Added two nullable columns to `decision_audit`: `prompt_truncation_flag`,
  `prompt_truncation_detail`. No existing column or row was touched;
  `decision_audit` row count confirmed unchanged (140,654 before and after).
- Flagged **9,601** `decision_audit` rows for `player_id='ollama-plutus'`
  whose `created_at` falls inside the documented per-day truncation windows
  (converted MST->UTC, +25min trailing buffer for observed generation/queue
  lag) from olliemax's Priority-1 table.

**Honest scope note — this is a time-window flag, not a per-call exact
match.** The CSV carries no symbol or player_id column, and I could not find
any table in this DB that logs McCoy's screened-scan calls with a token
count precise enough to join row-for-row:
- `decision_audit` itself has no prompt-size field (`prompt_text` is NULL
  on every recent row).
- `engine/cost_tracker.py`'s `api_costs` table — the one place that logs a
  token estimate per call — tops out at **10,415** estimated input tokens
  for McCoy on 2026-09-09, across every `call_type` including `scan`.
  That's nowhere near the CSV's real 15-18K tokenizer counts. **This means
  `run_mccoy_screened_scan`'s prompt-building path does not go through
  `cost_tracker.log_cost` at all — a separate, real observability gap**,
  distinct from today's three asks. Not fixed here (out of scope); worth
  its own ticket if McCoy's screened-scan cost/size should be visible in
  `api_costs` going forward.

Given that, the flag is windowed: any McCoy decision recorded during a day's
documented truncation window is flagged, with a `prompt_truncation_detail`
pointing back to this report and the source CSV table for the full data.

**The 940-row `78b329e716e7` (qwen3:30b-a3b) bucket (09-09 09:19-17:00,
shared per olliemax's doc between McCoy and Worf) has nothing flagged.**
Checked three candidate tables — `decision_audit` (Worf: zero rows since
2026-05-07), `crew_decisions` (Worf: zero rows since 2026-05-29), and
`plutus_shadow_critiques` (the report-only 30B/v7d shadow-witness table —
zero rows since 2026-07-11, i.e. not active during this window). McCoy's
own `decision_audit` rows in that sub-window already carry the
`qwen3_8b_default_ctx` flag from the broader same-day window (his 8B
calls were also truncated that day, per the CSV's 816-count for 09-09) —
so his real production decisions in that slice are flagged; there's no live
record of what the 30B *shadow* arm decided to flag separately. Not
papering over this: it's a genuine gap, not a silent drop.

`trades` table checked too (RULE #1's other flaggable table) — McCoy has
only 3 trades since 2026-09-08, all on 2026-09-11 09:17-10:58 MST, none
inside any truncation window. Nothing to flag there.

## 3. Riker's gemma3:4b runaway — real bug found and fixed, not just capped

olliemax's ask: "have the gemma3 caller set `num_predict`, or treat
`done_reason: 'length'` as a failure." Traced the caller: **`engine/
riker_xo.py::_do_riker_synthesis()`**, scheduled every 10 minutes by
`main.py` (`HM-RIKER-XO-SCHEDULE-2026-07-09`, `schedule.every(10).minutes.
do(_run_riker_xo_synthesis)`) — this is a *different*, still-live mechanism
from the 2026-06-24 Picard/Riker narrative-layer retirement CLAUDE.md
documents; that retirement removed a different in-process scheduler
(`run_riker_synthesis`/`_riker_startup`), not this one. Confirmed via
`main.py:4483-4500`'s own comment, which found and fixed the same
"doc says retired, code says live" gap back on 2026-07-09.

Confirmed live from olliemax's `journalctl -u ollama`, both 2026-09-11
`HTTP 500 after 3m0s` gemma3:4b failures (00:46:43 and 00:59:46 MST) came
from bigmac's own Tailscale IP (100.103.190.24), same IP as the `/api/tags`
polling this repo already does — genuinely ours, not a Model Works process.

**Root cause:** `num_predict` was a **top-level JSON key** in the
`/api/generate` payload, not inside `"options"` — Ollama only reads
generation params from `options`, so the cap was silently never applied.
The call ran unbounded until it hit the client's 180s timeout, generating
~28K tokens of context along the way (confirmed by two runaway ~20K-token
overnight completions in the same window). This is exactly why olliemax's
report could truthfully say "have the caller set num_predict" — from their
side, it looked unset, because it was, structurally.

**Fixed:** moved `num_predict` into `options` (so the 400-token cap is now
actually honored), and added a `done_reason == "length"` check that
discards (does not cache) a synthesis that still hits the cap — belt and
suspenders per olliemax's "either" framing, and specifically protects
against the failure mode they flagged: once they drop gemma3:4b's context
to 12288, an ex-runaway completion would return a *silent* truncated HTTP
200 instead of the loud 500 we're used to seeing today.

**Passed back to olliemax:** once this restart is live, ping them —
they're holding the gemma3:4b context reduction (32768 -> 12288) until
confirmation the caller now bounds output.

## Verification before restart

- `py_compile` clean on both modified files.
- `.venv/bin/python3 tests/pre_restart.py`: 29/32 pass. The 3 failures
  (`DalioFallbackProvider` `_ollama_lock` ImportError, Anderson not
  instantiated, power-hour convergence gate) are **pre-existing** —
  reproduced identically with today's changes `git stash`ed out. Not
  caused by this session's edits; not fixed here (out of scope for this
  directive). `dalio_provider.py` imports a symbol
  (`_ollama_lock`) that doesn't exist in `ollama_provider.py` — worth its
  own ticket since it means `DalioFallbackProvider` can't currently be
  constructed at all if anything tries to import it live.

## Restart

Code changes are not live until the trader restarts. Scheduled for right
after today's 13:00 MST close per the Admiral's own instruction this
session (also the window for the GPU thermal load test — see separate
follow-up). Restart via `zsh scripts/trader_restart.sh`, verify via a log
heartbeat (`ollama_call` line showing `qwen3:8b`/`plutus-v1` calls, confirm
no `[OLLAMA-CANCEL]` timeout spike immediately after).

## Files touched
- `engine/providers/ollama_provider.py` — `_DEFAULT_NUM_CTX` 10240 -> 24576
- `engine/riker_xo.py` — `num_predict` moved into `options`; `done_reason`
  guard added
- `scripts/hm_ollie_truncation_flag.py` — new, additive DB flag script (run
  once already against the live DB; idempotent on the raw-table load, not
  yet re-run-safe on the UPDATE step if run twice — see its `IS NULL` guard,
  which makes a second run a no-op rather than double-flagging)
- `data/backups/trader_pre_truncation_flag_2026-09-11.db` — pre-change
  backup, per doctrine
