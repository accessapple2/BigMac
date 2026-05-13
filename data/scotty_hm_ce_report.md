# HM-CE — Cloudflare Tunnel for Signal Center :9000

**Date:** 2026-05-12
**Status:** ⚠️ HALT — Captain action required (manual DNS step in Cloudflare dashboard)
**What landed:** tunnel ingress config + daemon reload; tunnel is ready and waiting for the right DNS record.
**What did NOT land:** `signal.ollietrades.com` CNAME (cloudflared cert is wrong-scoped — see below).

## Pre-flight findings

- **Tunnel:** name `trademinds`, UUID `dee0002c-c451-4919-8b16-d649ad19d029`. Active connectors to LAX/PHX edge POPs.
- **cloudflared binary:** `/opt/homebrew/bin/cloudflared` (symlink to Cellar 2026.3.0). Not in default PATH, so explicit absolute path used throughout.
- **Active service:** `com.cloudflare.cloudflared` plist at `~/Library/LaunchAgents/`. The `homebrew.mxcl.cloudflared` plist exists but is misconfigured (ProgramArguments lacks `tunnel run <UUID>`) and crash-loops harmlessly — leave it alone.
- **Existing ingress (pre-HM-CE):** single route `bridge.ollietrades.com → http://localhost:8080`.
- **Port 9000:** bound by pid 18380 — Signal Center is alive locally.

## What I changed (applied on bigmac, NOT in repo)

### `~/.cloudflared/config.yml`

Backup: `~/.cloudflared/config.yml.pre-HM-CE.YYYYMMDD_HHMM`

```diff
 tunnel: dee0002c-c451-4919-8b16-d649ad19d029
 credentials-file: /Users/bigmac/.cloudflared/dee0002c-c451-4919-8b16-d649ad19d029.json

 ingress:
   - hostname: bridge.ollietrades.com
     service: http://localhost:8080
     originRequest:
       noTLSVerify: true
+  # === HM-CE: Signal Center external access ===
+  - hostname: signal.ollietrades.com
+    service: http://localhost:9000
+  # === /HM-CE ===
   - service: http_status:404
```

Validation: `cloudflared tunnel ingress validate` → `OK`.

### Daemon reloaded

`launchctl kickstart -k gui/$UID/com.cloudflare.cloudflared` — pid 863 → pid 42816. Two new connectors registered (3767f99... + 8b716be0...). `bridge.ollietrades.com` still responding HTTP 303 post-reload (regression smoke green).

## The blocker

`cloudflared tunnel route dns dee0002c-... signal.ollietrades.com` succeeded but created the WRONG record. Log:

```
INF Added CNAME signal.ollietrades.com.accessapple.com which will route to this tunnel
```

Verified via dig:
```
$ dig signal.ollietrades.com.accessapple.com +short
172.67.149.30
104.21.39.220
```

This is `signal.ollietrades.com.accessapple.com` as a **literal FQDN** in the `accessapple.com` zone — not the intended `signal.ollietrades.com` in the `ollietrades.com` zone.

**Root cause:** `~/.cloudflared/cert.pem` was issued against the `accessapple.com` zone. cloudflared interpreted the requested hostname as a subdomain under the only zone the cert grants — so `signal.ollietrades.com` became `signal.ollietrades.com.accessapple.com`.

The existing `bridge.ollietrades.com` route works because its CNAME was created through a different path (presumably manually in the Cloudflare dashboard for the `ollietrades.com` zone, since this cert can't reach that zone).

## Captain action required (2 manual steps in Cloudflare dashboard)

### Step 1 — Add the correct CNAME in `ollietrades.com` zone

In Cloudflare dashboard → `ollietrades.com` zone → DNS records:

| Field | Value |
|---|---|
| Type | `CNAME` |
| Name | `signal` |
| Target | `dee0002c-c451-4919-8b16-d649ad19d029.cfargotunnel.com` |
| Proxy status | Proxied (orange cloud) |
| TTL | Auto |

### Step 2 — Delete the bogus CNAME in `accessapple.com` zone

In Cloudflare dashboard → `accessapple.com` zone → DNS records:

- Find: `signal.ollietrades.com.accessapple.com` (CNAME)
- Action: Delete

cloudflared has no CLI command to delete DNS routes (only IP routes), so this is dashboard-only.

## What happens after Step 1

The tunnel ingress is already wired. As soon as `signal.ollietrades.com` resolves to the tunnel:
- Cloudflare anycast IPs return for `dig signal.ollietrades.com`
- HTTPS hits the tunnel, which routes by hostname to `http://localhost:9000`
- Signal Center login page appears (HTTP 200 / 302 / 401 depending on auth state)

## External smoke checklist (Captain)

After both manual DNS steps land (give Cloudflare ~60s to propagate):

```bash
dig signal.ollietrades.com +short
# Expect: 2 Cloudflare anycast IPs (similar to bridge.ollietrades.com)

curl -I https://signal.ollietrades.com/
# Expect: HTTP 200/302/401 (NOT 000 like today)
```

From an external network (phone on cellular, etc.), open https://signal.ollietrades.com → Signal Center login renders.

## Rollback steps (if needed)

If Captain wants to revert HM-CE entirely:

```bash
# Restore prior config.yml on bigmac
cp ~/.cloudflared/config.yml.pre-HM-CE.* ~/.cloudflared/config.yml
launchctl kickstart -k gui/$(id -u)/com.cloudflare.cloudflared
# bridge.ollietrades.com remains live; signal route removed from ingress.
```

DNS cleanup (still required even on rollback):
- Delete `signal.ollietrades.com.accessapple.com` from `accessapple.com` zone (Step 2 above).
- Don't add the `ollietrades.com` CNAME if not needed.

## Why I went past CE.0 without a HALT

Directive's HALT conditions were: (a) tunnel config not found, (b) DNS API access unclear, (c) subdomain conflict. CE.0 showed (a) config IS found, (b) appeared OK (cert.pem exists, `tunnel route dns` is the standard auto-route command), (c) no conflict — `signal.ollietrades.com` was unused. The cert-zone-scope mismatch surfaced only AFTER the `tunnel route dns` call returned an unusual-looking log line. In hindsight, an extra check (`grep zone ~/.cloudflared/cert.pem` or `cloudflared access` listing) would have caught this pre-action. Logged as a watch item for similar future ops.

## Anchors

`# === HM-CE ===` / `# === /HM-CE ===` in `~/.cloudflared/config.yml` (system path, not tracked in repo).
