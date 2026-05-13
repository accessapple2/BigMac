# HM-CE — Cloudflare Tunnel for Signal Center :9000

**Date:** 2026-05-12
**Status:** ⚠️ HALT — Captain action required (Zero Trust dashboard, NOT DNS zone)
**Updated:** 2026-05-12 17:23 MST — root cause revised after observing daemon logs

## Decisive finding

The `trademinds` tunnel is **remote-managed via Cloudflare Zero Trust**. The local `~/.cloudflared/config.yml` is **ignored by the daemon**. Definitive log evidence after each daemon restart:

```
2026-05-13T00:19:33Z INF Updated to new configuration config="{
  \"ingress\":[
    {\"hostname\":\"bridge.ollietrades.com\",
     \"originRequest\":{\"noTLSVerify\":true},
     \"service\":\"http://localhost:8080\"},
    {\"originRequest\":{},\"service\":\"http_status:404\"}
  ],
  \"warp-routing\":{\"enabled\":false}}" version=8
```

That config is pulled from Cloudflare's control plane — `bridge.ollietrades.com` lives in the Zero Trust dashboard's Public Hostname tab, NOT in the local file. The existing `bridge` route was set up there at some point; my local config.yml edit for `signal` was a no-op.

## What I tried (and reverted)

### 1. Local `~/.cloudflared/config.yml` edit
- Added `signal.ollietrades.com → http://localhost:9000` ingress entry.
- `cloudflared tunnel ingress validate` → OK
- `cloudflared tunnel ingress rule https://signal.ollietrades.com/` → matched the new rule
- BUT: daemon logs show remote config has only `bridge` → my edit never reached the edge.
- **Reverted to pre-HM-CE state** (backup file used: `~/.cloudflared/config.yml.pre-HM-CE.*`).

### 2. `cloudflared tunnel route dns dee0002c-... signal.ollietrades.com`
- Cert is scoped to `accessapple.com` zone, so cloudflared created the literal name `signal.ollietrades.com.accessapple.com` (verified via dig).
- This is bogus and should be cleaned up.

### 3. `launchctl kickstart -k gui/$UID/com.cloudflare.cloudflared`
- Daemon reloaded twice; pid 863 → 42826 → 43185.
- Bridge regression-smoke green throughout (HTTP 303).
- But the reload pulled the same remote config without `signal`.

## Captain action — the actual fix

### Step 1 — Zero Trust dashboard: add public hostname

1. Open [Cloudflare Zero Trust](https://one.dash.cloudflare.com) → **Networks** → **Tunnels**
2. Click the **`trademinds`** tunnel
3. Switch to the **Public Hostname** tab
4. Click **Add a public hostname**
5. Fill in:
   - **Subdomain:** `signal`
   - **Domain:** `ollietrades.com` (select from dropdown)
   - **Path:** (blank)
   - **Type:** `HTTP`
   - **URL:** `localhost:9000`
6. **Save hostname**

Cloudflare will:
- Auto-create the DNS CNAME (no manual DNS step needed — it'll either create or replace whatever's there)
- Push the updated config to the running cloudflared daemon over the existing control-plane connection
- **No daemon restart required** — config update is live in seconds

### Step 2 — Cleanup: delete the bogus `accessapple.com` CNAME

In Cloudflare dashboard → `accessapple.com` zone → DNS records:
- Find: `signal.ollietrades.com.accessapple.com` (CNAME)
- Action: **Delete**

This was created by my earlier `cloudflared tunnel route dns` mis-fire. It's harmless but should be cleaned up.

### Step 3 — Optional: clean up any manually-added `signal` CNAME in `ollietrades.com` zone

If you manually added `signal → dee0002c-...cfargotunnel.com` in the `ollietrades.com` zone before reading this report — that record is fine and Cloudflare will reuse it. No action needed; Zero Trust would have auto-created the same thing in Step 1.

## Smoke after Step 1

```bash
dig signal.ollietrades.com +short
# Expect: 2 CF anycast IPs identical to bridge

curl -I https://signal.ollietrades.com/
# Expect: HTTP 200/302/401 (NOT 404)
```

From an external network → `https://signal.ollietrades.com` → Signal Center login renders.

## Watch items

- Local `~/.cloudflared/config.yml` exists and is parseable, but **is ignored**. Future operators should know to use the Zero Trust dashboard for ingress changes.
- The `homebrew.mxcl.cloudflared` plist at `~/Library/LaunchAgents/` is misconfigured (lacks `tunnel run <UUID>` in ProgramArguments). Harmless — crash-loops without consuming resources. The active service is `com.cloudflare.cloudflared` (pid 43185 at time of writing).

## State left clean

- `~/.cloudflared/config.yml` reverted to pre-HM-CE state (single `bridge` rule + catch-all 404).
- Backup of attempted edit preserved at `~/.cloudflared/config.yml.pre-HM-CE.YYYYMMDD_HHMM` for audit.
- Bridge regression-smoke green: `https://bridge.ollietrades.com/` → HTTP 303 (login redirect).
- Daemon pid 43185 stable, 4 active connectors (LAX + PHX edge POPs).

## Anchors

No persistent anchors landed (config.yml reverted). The HM-CE concept is fully captured in:
- `HM-CE.md` (directive, in repo)
- `data/scotty_hm_ce_report.md` (this report, in repo)

---

## Closure 2026-05-12 — HM-CE shipped

Captain added the public hostname via Cloudflare Zero Trust dashboard. Daemon pulled the new config live (version 8 → 9):

```
config="{\"ingress\":[
  {\"hostname\":\"bridge.ollietrades.com\",\"service\":\"http://localhost:8080\",...},
  {\"hostname\":\"signal.ollietrades.com\",\"service\":\"http://localhost:9000\",...},
  {\"service\":\"http_status:404\"}
]}" version=9
```

External smoke:
- `https://signal.ollietrades.com` → HTTP 302 (Signal Center auth redirect — expected) ✅
- `https://bridge.ollietrades.com` → HTTP 303 (unaffected) ✅
- `dig signal.ollietrades.com` → 172.67.208.56, 104.21.45.31 (CF anycast, identical to bridge) ✅

No daemon restart was required — Cloudflare pushed the config update over the existing control-plane connection in seconds, exactly as expected for a remote-managed tunnel.

Captain may still want to clean up the bogus `signal.ollietrades.com.accessapple.com` CNAME in the `accessapple.com` zone (residue from the earlier route-dns mis-fire). Not blocking; it just orphans a stale record.
