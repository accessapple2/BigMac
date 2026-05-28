# WAVE 6 — Captain Action Runbook (2026-05-28)

Three items need your hands (sudo / secret-gen / posture decision). Scotty has
prepped everything that doesn't require those. Signal Center :9000 is on HOLD
per your call.

---

## 1. cloudflared LaunchDaemon (needs sudo + Full Disk Access on Terminal)

Replaces the `@reboot` cron wrapper with a system-domain boot launcher that also
gives KeepAlive respawn-on-crash. Plist is prepped at
`drafts/com.trademinds.cloudflared.plist`.

```bash
# 1. Install the plist (system domain — needs sudo)
sudo cp ~/autonomous-trader/drafts/com.trademinds.cloudflared.plist /Library/LaunchDaemons/
sudo chown root:wheel /Library/LaunchDaemons/com.trademinds.cloudflared.plist
sudo chmod 644 /Library/LaunchDaemons/com.trademinds.cloudflared.plist

# 2. Stop the current cron-launched tunnel so they don't double-run
pkill -f "cloudflared tunnel" 2>/dev/null

# 3. Load + start the daemon
sudo launchctl bootstrap system /Library/LaunchDaemons/com.trademinds.cloudflared.plist
sudo launchctl kickstart -k system/com.trademinds.cloudflared

# 4. Verify
pgrep -fl "cloudflared tunnel"
curl -sI https://bridge.ollietrades.com | head -1   # expect HTTP 200/3xx
```
After confirming, retire the cron line + `scripts/cloudflared_reboot_start.sh`
(the daemon supersedes them — leaving both is the double-fire footgun).

---

## 2. auth Phase 1 enablement (needs secret-gen — Scotty must NOT generate)

Scaffolding is already in place: `dashboard/auth.py` helper (`verify_admin_token`
reads `OLLIETRADES_TOTP_SECRET`), runbook `docs/AUTH_SETUP.md`, and the stubbed
guard at `dashboard/app.py:21268-21269`.

```bash
# 1. Generate the TOTP secret (YOURS — do not paste into chat)
./venv/bin/python3 -c "import pyotp; print(pyotp.random_base32())"
#   → add to ~/autonomous-trader/.env (mode 600):  OLLIETRADES_TOTP_SECRET=<value>
#   (plus the service token + any other secrets per docs/AUTH_SETUP.md)

# 2. Un-stub the guard(s) — currently 1 route (squeeze/dismiss):
#    dashboard/app.py:21269 — uncomment:  _: str = Depends(verify_admin_token)
#    (grep for more if Phase 1 scope expands: "TODO Phase 1: enable after Admiral")

# 3. Restart + verify the route now 401s without a token, 200s with one.
```

---

## 3. Signal Center :9000 reopen — **HOLD** (your call)
Not reopening. You want to think on the network posture first. No action taken;
HM-AW stays parked.
