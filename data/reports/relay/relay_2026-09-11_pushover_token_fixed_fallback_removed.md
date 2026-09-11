# Relay — Pushover token fixed, fallback removed, live-verified. 2026-09-11

## Result

Real OllieTrades app token (`PUSHOVER_TOKEN`, 30 chars, starts with `a`)
now in `.env`. Re-ran the three-tier test: **both WARNING and RED_ALERT
succeeded on the first attempt, no fallback needed at all** — `pushover
sent: ...` with no preceding failure/retry line, versus every prior test
today showing `pushover failed via .env` before falling through.

## Which app each tier shows up under

| Tier | Pushover? | App identity |
|---|---|---|
| INFO | No (ntfy only, by design) | n/a |
| WARNING | Yes, quiet priority (-1) | **OllieTrades** — delivered via `.env`'s token directly, no fallback attempted |
| RED_ALERT | Yes, priority 1 | **OllieTrades** — same, direct delivery |

Both now land under the dedicated OllieTrades app, not the old shared
"GPU Watch" app the file-based fallback was borrowing.

## Fallback removed

`_send_pushover()` simplified back to reading only `PUSHOVER_TOKEN`/
`PUSHOVER_USER` from `.env` — the multi-source retry logic (added
temporarily while the token was bad) is gone. If these ever go missing
or invalid again, it now fails loud (logged warning, returns `False`)
rather than silently reverting to the GPU Watch identity — the right
failure mode once the primary path is confirmed reliable.

## Verification

Three-tier live dispatch with the fallback code already removed (not
just confirmed-then-removed-untested): all three tiers behaved
identically to the pre-removal test — `py_compile` clean, restarted the
live trader to pick up the change (was still running the pre-fix code in
memory), confirmed clean post-restart (`/api/status` responding, zero
tracebacks in the fresh log).
