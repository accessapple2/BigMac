# DRAFT — manifest.json CF Access bypass — FOR SIGN-OFF, NOT APPLIED

## Problem (confirmed earlier this session)
`bridge.ollietrades.com` is gated by Cloudflare Access (policy `bridge-allow`,
per CLAUDE.md's 2026-06-24 structural notes). The browser's own manifest
fetch (`<link rel="manifest" href="/static/manifest.json">`) doesn't carry
the CF Access session cookie the same way a normal page navigation does,
so Access challenges/redirects it → the browser reports a 503 fetching the
manifest, blocking PWA installability.

**Already shipped (app-layer half, this session):** added
`crossorigin="use-credentials"` to the manifest `<link>` tag in
`dashboard/static/index.html:39` — this makes the browser include
credentials on the manifest fetch, which may resolve it alone. The
CF Access bypass policy below is the belt-and-suspenders fix in case the
crossorigin attribute isn't sufficient by itself (browser support for
credentialed manifest fetches varies).

## What needs to change (Cloudflare Zero Trust → Access → Applications)

The existing `bridge-allow` Access policy currently gates the whole
`bridge.ollietrades.com` hostname (per CLAUDE.md, 3 emails allowed, 730h
session). Add a **path-exclusion / bypass rule** so these two paths never
hit the Access challenge:

1. `bridge.ollietrades.com/static/manifest.json` (exact path)
2. `bridge.ollietrades.com/test-kitchen/*` (per the original directive item — Test Kitchen sandbox)

**Exact steps (Zero Trust dashboard):**
1. Zero Trust → Access → Applications → find the app covering `bridge.ollietrades.com`
2. Add a new Policy on that application:
   - Name: `bypass-static-manifest`
   - Action: **Bypass**
   - Include: Everyone
   - Path rule (if the app supports path-scoped policies) restricted to `/static/manifest.json`
3. Repeat (or extend the same bypass policy's path list) for `/test-kitchen/*`
4. Policy **precedence matters** — the Bypass policy must be ordered/evaluated before the existing `bridge-allow` Allow policy, or Access may still challenge first

**If Access Applications in this account don't support path-scoped policies
directly** (varies by plan), the alternative is a **separate Access
Application** scoped specifically to the path `bridge.ollietrades.com/static/*`
(and another for `/test-kitchen/*`) with a Bypass policy, layered
underneath/alongside the existing whole-hostname app. Exact mechanism to
confirm against what this account's plan actually exposes — noted here as
the fallback rather than guessed at with false confidence.

## Note on browser access to apply this
Unlike the zone-level DNS/Rulesets pages (`dash.cloudflare.com/.../ollietrades.com`,
confirmed hanging indefinitely for both of us this session), Zero Trust
pages (`one.dash.cloudflare.com/...`) reportedly render fine on your end.
My own browser automation hit a *different* problem there — "Permission
denied for this action on this domain" on a fresh tab for
`one.dash.cloudflare.com/.../networks/tunnels` specifically (an
extension-level per-site permission gap, not the same "stuck loading"
issue as the zone pages). The Access → Applications path may or may not
have the same gap; untested this round. Worth a retry if you want me to
attempt this directly, or apply it yourself with the steps above.

## Verification plan (once applied)
```
curl -I https://bridge.ollietrades.com/static/manifest.json
```
Expect: `200` with the actual manifest JSON body, no redirect to a CF
Access login page — from an unauthenticated client (no session cookie),
confirming the bypass genuinely exempts this path rather than just
working because of an existing authenticated session.
