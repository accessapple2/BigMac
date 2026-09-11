# Relay — C12: dr_crusher's stray halted-seat healthcheck, retired. 2026-09-11

## Target, precisely identified

Not `dr_crusher.sh` (the root-level bash script, LaunchDaemon-scheduled
every 360s, confirmed earlier today during the dry-dock breach hunt to be
alert-only and unrelated) — a **same-named but different** thing:
`main.py::dr_crusher_check()`, a Python function scheduled via
`schedule.every(15).minutes.do(dr_crusher_check)` (its own comment said
"every 5 min," stale — fixed in passing).

## What it did, and why it was wrong

"Check 2" of four fired an unconditional raw HTTP POST to `{OLLIE_URL}/
api/generate` with a hardcoded `"model": "0xroyce/plutus"` — bypassing
`OllamaProvider`, the DB, and any halt check entirely. That model is
nominally what `dayblade-0dte` (T'Pol, 0DTE options) is configured to
use — but **`dayblade-0dte` has been `halt_mode='full'` since
2026-07-13** (systematic options mispricing, reasoning/action inversion,
fractional-qty bugs — explicit `halt_reason` on file). No live seat has
used this model in ~2 months; this check kept it resident on olliemax
regardless, a real contributor to the GPU-residency contention that was
also starving `bge-m3` (the recall-in-prompt embedding model, see
`relay_2026-09-10_recall-url-fix-and-host-sweep.md`) before
`OLLAMA_MAX_LOADED_MODELS` was raised to 3.

**This was found and flagged twice before tonight** —
`relay_2026-09-08_olliemax-cutover-and-30b.md` and again in
`docs/XO_BACKLOG.md` on 2026-09-10 — and not actioned either time.
Tonight is the first time it's actually removed.

## What shipped

Removed Check 2 entirely (not retargeted to a different model — the
directive's explicit instruction is "retire it," and no live seat
currently needs a standalone infra healthcheck pinning a specific model
resident; Checks 3 and 4, DB liveness and recent-scan-activity, already
cover real health signal without a model-specific probe). Left Checks 3
and 4 byte-identical, untouched. Also fixed the stale "every 5 min"
comment to match the actual 15-minute schedule, and cleaned up the now-
unused `requests` import in the function's local scope.

## Verification

`python3 -m py_compile main.py` clean. The removed block was fully
self-contained (its own try/except, no shared state with Checks 3/4) —
confirmed by reading the surrounding code before and after the edit, not
just compiling it. Not functionally smoke-tested standalone (the function
is a closure nested inside `main.py`'s startup scope, not easily callable
in isolation without a full trader start) — the edit is small, surgical,
and leaves the untouched checks byte-identical, so this is judged
low-risk without a full restart-and-observe cycle; will be naturally
confirmed at D16's "clean restart" verification step.
