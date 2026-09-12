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

## Restart note

Code changes are **not yet live** — same as the Friday `3daaa99` fix, this
needs `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader` to take
effect in the actual running trader process (verification above ran the
functions standalone via `.venv/bin/python3`, not through the live PID
10282 process, which still has the old code in memory). The cron change
(sentinel stagger) IS live immediately — cron reads the crontab fresh each
tick, no restart needed.
