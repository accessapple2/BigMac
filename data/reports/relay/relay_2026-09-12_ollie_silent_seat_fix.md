# Relay — 2026-09-12 morning. HM-OLLIE-SILENT-SEAT: fixed the num_ctx silent-reconfigure bug.

## Incident (Saturday audit, this session)

Nine `/api/generate` calls from bigmac (`100.103.190.24`) to olliemax hit
`qwen3:8b`, `phi3:mini`, `gemma3:4b` between 05:45:05-05:55:04. Traced via
olliemax's `journalctl -u ollama` (ground truth, not code inference) to
three independent, legitimate in-process schedulers inside the live trader
(`main.py`, PID 10282) colliding:

1. **Elder Council daily brief** (`agents/sarek.py`/`janeway.py`/`surak.py`,
   `main.py:6017` `schedule.every().day.at("05:45")`) — each hardcoded its
   own `num_ctx: 4096` locally.
2. **`scripts/hm_ops_sentinel.py::check_ollama_generate_probe()`** — cron
   `*/5 * * * *`, a real `/api/generate` probe (not `/api/tags`/`/api/ps` —
   deliberately, per the 2026-09-10 stale-socket incident) against
   `plutus-v1`/`qwen3:8b`, with no `num_ctx` at all. Its 5-min grid landed
   exactly on the Elder Council's 05:45 slot, causing a concurrent double-load
   of the same qwen3:8b blob and a genuine HTTP 499 (client-side cancel,
   32.9s) as one request aborted mid-load while the other held it.
3. **`engine/riker_xo.py::generate_riker_synthesis()`** — `main.py:4580`,
   every 10 minutes, no weekend/market-hours gate, **no `num_ctx` set at
   all** — silently inherited whatever context olliemax had resident for
   `gemma3:4b`. Fired 6 minutes after Surak had just loaded gemma3:4b at
   4096, so Riker's own prompt (crew-intelligence synthesis, observed up to
   ~8.2K tokens) ran on a 4,096-token seat by scheduling accident. No
   truncation this time only because that cycle's prompt happened to fit —
   exactly the failure class the Friday `done_reason=="length"` fix
   (`3daaa99`) exists to catch, one step upstream of where that guard bites.

Root cause: five different call sites independently decided (or didn't
decide) `num_ctx` for a single-loaded-model-at-a-time GPU box, instead of
routing through the one place that already tracked the right value per
model (`engine/providers/ollama_provider.py::_num_ctx_for`).

## Fix (HM-OLLIE-SILENT-SEAT-2026-09-12)

**Shared constant, made public + guarded** (`engine/providers/ollama_provider.py`):
- Added `_NUM_CTX_OVERRIDES["gemma3:4b"] = 12288` and `["phi3:mini"] = 12288`
  — sized for Riker's real ~8.2K-token prompt plus headroom. **Confirmed
  live against olliemax's own Modelfiles** (`ollama show <tag> --modelfile`):
  `gemma3:4b`'s own configured default is exactly `12288` — the override
  matches the tag's own truth, same philosophy as the Friday qwen3 fix.
  `phi3:mini`'s own default is only `2048`; 12288 is deliberately generous
  headroom, not a forced match, since phi3:mini only serves Janeway's
  once-daily thesis call, not high-frequency fleet traffic.
- New public `num_ctx_for(model_id)` — the one function every olliemax
  caller should use instead of a local literal.
- New `require_num_ctx(model_id, requested)` — the systemic guard asked
  for: raises `UndersizedNumCtxError` **before the request goes out** if a
  caller asks for less than the model's configured seat size. Same shape as
  the Friday `done_reason=="length"` guard — the bug isn't that a call
  *can* undersize a seat, it's that doing so was silent.

**Four callers fixed** to route through `num_ctx_for()` + wrap with
`require_num_ctx()`:
- `agents/sarek.py` (qwen3:8b) — was hardcoded 4096, now 24576.
- `agents/janeway.py` (phi3:mini) — was hardcoded 4096, now 12288.
- `agents/surak.py` (gemma3:4b) — was hardcoded 4096, now 12288.
- `engine/riker_xo.py` (gemma3:4b) — previously sent no `num_ctx` at all
  (the actual root cause of the 05:52 collision), now 12288 explicitly.

**Sentinel probe kept, not removed** (`scripts/hm_ops_sentinel.py`) — per
the Admiral's explicit instruction, the real-generate canary stays (it's
the only thing that caught the 2026-09-10 09:35-11:58 stale-socket outage
that `/api/tags`/`/api/ps` both missed for 3.5 hours). Now sends explicit
`num_ctx` (24576, via `num_ctx_for`) for both `plutus-v1` and `qwen3:8b` so
the probe can't undersize the seat it's checking.

**Cron staggered, not left colliding**: `hm_ops_sentinel.py`'s cron changed
from `*/5 * * * *` to `2-59/5 * * * *` (fires at :02/:07/.../:57 instead of
:00/:05/.../:45/:50/:55) so it no longer lands on the same wall-clock
minute as the Elder Council's fixed 05:30/05:35/05:40/05:45 daily schedule.
Edited per the repo's Cron Edit Safety Rule (dump to file, edit the file
not a pipe, `diff` + `wc -l` count-guard before install — both confirmed:
exactly one line changed, 172/172 lines before and after). Backups kept at
`archive/crontabs/crontab.backup_20260912_{pre,post}_sentinel_stagger.txt`.

## Verification (live, this session, market closed so low blast radius)

Ran one real call through each of the four fixed paths plus the sentinel
probe, checking olliemax's `/api/ps` (`context_length` field, ground truth
— not the code's own claim) after each:

| Call | Model | Expected ctx | Observed ctx | Result |
|---|---|---|---|---|
| `get_sarek_brief(force=True)` | qwen3:8b | 24576 | 24576 | ✓ picks=3 |
| `get_janeway_brief(force=True)` | phi3:mini | 12288 | 12288 | ✓ picks=6 |
| `get_surak_brief(force=True)` | gemma3:4b | 12288 | 12288 | ✓ picks=5 |
| `generate_riker_synthesis()` | gemma3:4b | 12288 | 12288 | ✓ 945-char synthesis, no `done_reason=length` |
| `check_ollama_generate_probe([])` | plutus-v1 + qwen3:8b | 24576 | 24576 | ✓ both `ok:true`, no alert, residency not undersized |

`require_num_ctx` unit-verified separately: `require_num_ctx("gemma3:4b", 4096)`
raises `UndersizedNumCtxError` with a message naming the configured seat
size and pointing at `num_ctx_for()` — confirms the guard actually fires on
exactly the value that caused this incident.

Side effect, not a concern: today's original 05:45-05:47 Elder Council run
had failed to persist (`ERROR persist signals: database is locked` in each
of `logs/{sarek,janeway,surak}.log`) — the verification calls above,
running with `force=True`, are the first successful persist of today's
real picks for all three, additive-only per RULE #1.

## Open / not done this pass

- Riker's synthesis scheduler (`main.py:4580`) still has no weekend/market-
  hours gate — it will keep ticking every 10 minutes regardless. Not a bug
  this pass fixes (out of scope: the ask was seat-sizing, not scheduling
  gates), but worth a future ticket if 24/7 ticking against a paper-only
  crew-intelligence synthesis isn't wanted.
- `phi3:mini`'s 12288 override is headroom, not a tag-default match (its
  own Modelfile default is 2048) — fine for its current once-daily caller,
  but flag if phi3:mini ever gets a second, higher-frequency caller with a
  different real prompt-size profile.
- No new automated test added for `require_num_ctx`/`num_ctx_for` — this
  pass was scoped to the four edits + guard + verification the Admiral
  asked for, not a new test suite. Worth adding to whatever covers
  `engine/providers/` if one exists.

## Restart + live verification (2026-09-12 06:40 MST, Admiral-authorized outside market hours)

Market closed until Monday 04:37 MST — restarted now rather than leaving
the old code in memory through the premarket window.

1. **Backup first**: ran `scripts/db_snapshot.sh` manually ahead of
   schedule — `data/backups/trader_2026-09-12.db` (1.3G, `integrity_check=ok`).
2. **Restart**: `zsh scripts/trader_restart.sh` — single-writer gate passed
   clean. Old PID 10282 killed, WAL checkpointed in the zero-reader window,
   new PID **71567** bound :8080, started `2026-09-12 06:40:42`, i.e. after
   commit `d639064` (`06:35:40`) — the running process reflects the fix.
3. **Live Riker call, through the running process, not a standalone
   script**: `curl -X POST http://127.0.0.1:8080/api/riker/synthesize` ->
   `{"ok":true,"length":891}`, confirmed in `logs/trader.log`: `[06:41:19]
   Commander Riker: Synthesis generated (891 chars)`. olliemax `/api/ps`
   before and after: `gemma3:4b` stayed at `context_length: 12288` both
   times.
4. **Live sentinel probe** (fresh interpreter, exactly as cron invokes it):
   `check_ollama_generate_probe([])` -> both `plutus-v1` and `qwen3:8b`
   `ok:true`, 0.0s (already warm at the right context), zero alerts.
   olliemax `/api/ps` before and after: `plutus-v1:latest` stayed at
   `context_length: 24576` both times, `gemma3:4b` unaffected at `12288`.
5. Post-restart `trader_error.log` tail: normal Saturday weekend-skip
   activity only (`[BRIDGE_VOTE] Skipping — weekend`, routine 0-result
   screener passes) — no new errors from this change. (One unrelated
   pre-existing line, `evaluate_realized_pending failed: no such column:
   realized_at`, is not touched by this fix — out of scope, noted not
   silently ignored.)

**HM-OLLIE-SILENT-SEAT is closed: code shipped, live in the running
process, verified end-to-end against olliemax's own `/api/ps`, not just
against the source file.**

## HM-OLLIE-SEAT-OVERSIZE-2026-09-12 — same-day correction

Trip found the other half of this fix: the first pass set BOTH gemma3:4b
and phi3:mini to a uniform 12288, which fixed undersizing but created
oversizing — phi3:mini at 12288 measures **7.7GB VRAM single-card** and was
evicting plutus-v1's seat. Same underlying bug (a caller's stated
requirement silently reconfigures a live seat for whoever runs next), just
in the opposite direction from the original incident.

**Confirmed before changing anything** (per the Admiral's explicit
instruction to check first): every real caller of phi3:mini measured well
under 3.5K:
- `agents/janeway.py`'s actual daily thesis prompt — **182 tokens**,
  measured live via `prompt_eval_count` in Ollama's own response, not an
  estimate.
- `engine/crew_scanner.py`'s `mlx-qwen3` (advisory tier, "Ensign Ro",
  currently halted) scan prompt — a similarly small templated
  market-context string.
- `engine/crew_scanner.py::_ensure_warm()` keep-alive ping — 5 tokens.
- `engine/chart_analyzer.py`/`bull_bear.py`/`premarket_scanner.py` each
  have a `model=="ollama"` branch reaching `config.OLLAMA_MODEL`
  (phi3:mini), but only via an explicit, non-default model selection
  nothing currently makes (`chart_analyze`'s default is `"codex"`) — dormant,
  flagged rather than sized against.

**Fix**: `phi3:mini` override dropped `12288 -> 4096` (3.7GB single-card,
clear headroom over every real prompt seen, stops competing with
plutus-v1 for a fleet seat). `gemma3:4b` unchanged at `12288` — Riker's
real prompts run 8.2-8.4K tokens and genuinely need it; also confirmed
this matches gemma3:4b's own Modelfile default exactly.

**Restart + live verification** (2nd restart this session, same
Admiral-authorized outside-market-hours posture): backup re-run (no-op,
today's snapshot already existed), `trader_restart.sh` clean restart, PID
74944 (was 71567). Live Janeway call: olliemax `/api/ps` showed
`phi3:mini` reload from stale `12288` to correct `4096`, `gemma3:4b`
untouched at `12288`. Live Riker call immediately after (dashboard
`/api/riker/synthesize`): `{"ok":true,"length":949}`, `gemma3:4b` still
`12288`, `phi3:mini` still `4096` — both seats stable and correct
together.
