# network-bindings.md

> Relocated from CLAUDE.md by HM-PRIME Part C (move, not delete).

## Network Bindings
- **Trader dashboard (port 8080)**: uvicorn binds `0.0.0.0` (all interfaces) as
  of **HM-BRIDGE-BIND (2026-06-12, commit `d64e202`)** — env-overridable via
  `DASHBOARD_HOST` (set `127.0.0.1` to revert to loopback-only). LAN/Tailscale
  clients now reach `http://bigmac:8080` directly (no SSH tunnel); the Cloudflare
  tunnel at `bridge.ollietrades.com` (→ `localhost:8080`) keeps working unchanged
  since `0.0.0.0` includes loopback. The bind site is `main.py` `run_dashboard()`
  (~line 3526), NOT `main.py:3250` (that line ref was stale). **Auth review (done,
  not deferred):** network exposure is safe because the AuthMiddleware localhost
  bypass (`dashboard/app.py::_is_localhost`, line 1078) keys on
  `request.client.host == 127.0.0.1`, and `forwarded_allow_ips="127.0.0.1"` means
  uvicorn only honors `X-Forwarded-For` rewriting from genuinely-loopback
  connections. A LAN/Tailscale client arrives from a non-loopback source IP, so it
  can neither hit the bypass nor spoof `client.host` — it must authenticate (verified
  2026-06-12: LAN IP returns `303 → /login`, not a bypass). **Do NOT** set
  `proxy_headers=False` or widen `forwarded_allow_ips` — that invariant is what keeps
  the open bind safe. Caveat: `0.0.0.0` exposes `:8080` to the entire LAN, not just
  Tailscale; auth gates it, but rely on the macOS Application Firewall as a second
  layer on untrusted networks.
- **Tailscale node (HM-TS-DEDUP, 2026-06-12)**: bigmac's tailnet IP is
  **`100.103.190.24`** (MagicDNS `bigmac`). Reach the dashboard remotely at
  `http://100.103.190.24:8080` (or `http://bigmac:8080` once MagicDNS propagates).
  **Single daemon only**: the Tailscale.app system-extension
  (`io.tailscale.ipn.macsys.network-extension`, v1.96.5). The duplicate **Homebrew
  `tailscaled`** (v1.96.4, LaunchDaemon `homebrew.mxcl.tailscale.plist`) was booted
  out + keg-deleted on 2026-06-12 — it had registered a *second* node on this one
  Mac, which (a) caused the 1.96.4-client/1.96.5-server version mismatch and (b)
  owned a ghost identity that broke the remote TCP path (DERP pong OK, TCP dead).
  **Do NOT `brew install tailscale`** on this box — the GUI app owns the daemon;
  a second install re-creates the duplicate-node split. Two now-dead identities
  from the cleanup: `100.95.222.119` and `100.124.131.19` (both deleted/offline —
  do not reference). **Remote-client gotcha (root cause of Bonnie's PC failures):**
  a commercial VPN on the client can hijack the `100.64.0.0/10` CGNAT range —
  **NordVPN's NordLynx** routed `100.x` into its own tunnel, so Tailscale traffic
  to bigmac never reached the tailnet interface. Fix on the client: disable the
  VPN, or exclude `100.64.0.0/10` from its routes, so `100.103.190.24` resolves via
  the Tailscale adapter.
- **Cloudflare Access on `bridge.ollietrades.com` (HM-ACCESS-POLICY, 2026-06-12)**:
  the Access application **"bridge"** had **NO attached policy** — an orphaned
  *reusable* ALLOW policy (used-by 0, a migration artifact), so Access had no rule
  to evaluate and login was broken. Fixed by creating an **inline** policy
  **"bridge-allow"** (Action: Allow, Include: Emails, 1-month session) attached
  directly to the app. Verified end-to-end from iPhone on 5G (code delivered, login
  OK). **Lesson:** an Access app with zero attached policies fails closed — when
  auditing Access, confirm the app has an *attached* (preferably inline) policy, not
  just that a matching reusable policy exists somewhere in the account. This is the
  auth layer in front of the Cloudflare tunnel (→ `localhost:8080`); the tunnel is
  the `bridge.ollietrades.com` browser ingress, independent of the Tailscale path
  above (two separate remote-access routes: Cloudflare Access for browser, Tailscale
  for direct IP).
- **Signal Center (port 9000)**: bound to `127.0.0.1` from pre-2FA legacy
  posture. HM-AW (`docs/XO_BACKLOG.md`) tracks reopening to network now that
  2FA TOTP + multi-user auth (Captain, Bonnie observer, Dad charts) are in
  place. SSH tunnel required today for non-bigmac browser access.
- **Two distinct auth layers** (do not conflate): browser users → 2FA TOTP +
  RBAC at Signal Center server layer; automation/scripts → SSH keys + bigmac
  OS account. Both valid; protect different surfaces.
