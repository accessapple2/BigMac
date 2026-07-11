# Relay: HM-STATUS-PAGE-STALE-CACHE re-checked — currently fresh, narrowed not closed

**Date:** 2026-07-11
**Commit:** `e839a82`

## What was asked

"check the status page stale cache issue."

## Finding: not stale right now, but the original scenario is untested

Rather than repeat the filed ticket's diagnosis, fetched
`https://status.ollietrades.com/` live three times, ~3-4 seconds apart:

```
Last checked: 2026-07-11 13:58:32 UTC
Last checked: 2026-07-11 13:58:35 UTC
Last checked: 2026-07-11 13:58:39 UTC
```

Each timestamp matched real UTC time to the second. Response headers
explain why: `cache-control: no-cache, no-store, must-revalidate`
(the origin's own header) reaches the client unmodified, and
Cloudflare's own `cf-cache-status: DYNAMIC` header confirms the edge is
**not** caching this response during normal operation — every request
is passing through to the live origin.

**This narrows the ticket, doesn't close it.** The original observation
(2026-07-05, `docs/REBOOT_POSTURE.md`) was specific to a real physical
cold-start/power-cut test — Cloudflare served a stale cached page right
after the box rebooted, even though the fresh service was already up.
No cold-start test has been re-run since to confirm whether that specific
symptom still recurs; what I verified today only covers steady-state
operation, which was apparently always fine.

**A hypothesis worth recording, not confirmed:** Cloudflare's "Always
Online" feature (or similar stale-while-origin-down behavior) serves a
last-known-good cached copy specifically *while the origin looks
unreachable* — a different mechanism than normal `Cache-Control`-governed
caching, and one that would explain "fine in steady state, stale right
after reboot" precisely. Checked `.env` for a Cloudflare API token to
verify the zone's actual Cache Rules / Always Online setting
programmatically — only `CF_ACCESS_TEAM_DOMAIN` exists (scoped to
Cloudflare Access, not zone/cache config), so this can't be confirmed
without either Cloudflare dashboard access or another real cold-start
test.

## What this means practically

- No code change needed — the origin side is already correct and was
  already correct before today.
- Whether a Cloudflare dashboard change is still needed is genuinely
  unclear until either (a) someone checks the zone's Cache Rules /
  Always Online setting directly in the Cloudflare dashboard, or (b)
  another real cold-start test reproduces or fails to reproduce the
  original symptom.
- Not urgent to chase further right now — the underlying service is
  healthy and correctly configured; this is purely about what a human
  would see on `status.ollietrades.com` in the narrow window right after
  a reboot, which isn't a frequent event.

## Open items

`HM-STATUS-PAGE-STALE-CACHE` stays open, re-scoped from "confirmed
Cloudflare dashboard fix needed" to "unconfirmed hypothesis, needs either
dashboard access or a cold-start re-test to resolve." No other open
items from the XO-DEPARTURE-HARDENING thread remain — this was the last
one.
