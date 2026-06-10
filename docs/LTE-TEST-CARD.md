# LTE Test Card — Remote Backbone Drill (Admiral-run, on cellular)

**Purpose:** prove the remote-access backbone works from **off-network** —
phone/laptop on **LTE/cellular with home Wi-Fi OFF** — so a real outage can be
worked from anywhere. Admiral runs this by hand; Claude does not execute it.

**Staged:** 2026-06-10 (D2 pull-forward). **Run window:** today or tomorrow AM.

> **D3 dependency:** the D3 disaster-recovery drill is **GATED on this card
> passing**. Do not schedule D3 until every box below is checked ✓.

---

## ⛔ PRECONDITION — bigmac tailnet is currently DOWN (MUST-FIX first)

As of 2026-06-10 09:1x AZ, `tailscale status` on **bigmac** reports **`Logged
out`** (tailscaled daemon PID 563 is running, but the node is **not
authenticated onto the tailnet**). The tailnet entry point is bigmac, so until
it is re-authed **every step below will fail at Step 1**.

**Re-auth requires Admiral action (do not let an agent attempt it):**

1. On bigmac, run: `tailscale up`
2. Open the printed `https://login.tailscale.com/a/...` URL in a browser and
   approve the node under the Admiral's Tailscale account.
3. Confirm: `tailscale status` shows bigmac **online** with a `100.x.y.z`
   address and a `*.ts.net` MagicDNS name (note the exact name — it fills the
   `<BIGMAC_TS>` placeholder below).
4. **Also** check **Key expiry** in the Tailscale admin console
   (https://login.tailscale.com/admin/machines). If bigmac's key expires within
   30 days, set **"Disable key expiry"** on the bigmac node so the backbone does
   not silently drop off mid-outage. *(Console toggle — Admiral only.)*

> `.168` (olliemax) has **no tailscale CLI** and does **not** need one: the
> architecture is **phone → tailnet → bigmac → LAN-SSH (192.168.1.168) → .168**.
> bigmac is the single tailnet hop; the nested hop to `.168` is plain LAN SSH.

---

## The drill (cellular, Wi-Fi OFF)

Replace `<BIGMAC_TS>` with bigmac's MagicDNS name from Precondition Step 3
(e.g. `steves-mac-mini.<tailnet>.ts.net`). All commands run from the
phone/laptop unless noted.

### ☐ Step 0 — confirm you are actually on cellular
```
# Wi-Fi OFF. Verify you are NOT on the home LAN:
ping -c1 192.168.1.168
```
- **Expected:** `100% packet loss` / `host unreachable` (LAN is gone — good;
  you are genuinely remote). If this *succeeds*, you are still on Wi-Fi — the
  test is invalid.

### ☐ Step 1 — tailnet SSH into bigmac
```
ssh bigmac@<BIGMAC_TS>
```
- **Expected:** shell prompt on `Steves-Mac-mini.local`. Confirm with `hostname`.
- **Fail =** backbone down → STOP, return to Precondition (tailnet not up).

### ☐ Step 2 — nested hop bigmac → .168 (LAN SSH)
```
# now ON bigmac (from Step 1):
ssh olliemax 'hostname; uptime'
```
- **Expected:** `host:olliemax` + an uptime line. Proves the nested LAN hop the
  DR runbook depends on.

### ☐ Step 3 — one curl of the public bridge
```
# from bigmac (or directly from the phone — both should work):
curl -s -o /dev/null -w "%{http_code}\n" https://bridge.ollietrades.com
```
- **Expected:** `200` or `302` or `303` (303 = Cloudflare Access auth redirect —
  healthy; the tunnel is up and serving). Anything `000`/`5xx` = tunnel down.

### ☐ Step 4 — one `make status` over SSH
```
# from bigmac (from Step 1):
cd ~/autonomous-trader && make status
```
- **Expected:** the read-only fleet snapshot prints (gates/regime/fleet/git/
  services/briefings). No errors. This is the "I can see the whole fleet from my
  phone" proof.

---

## Pass / fail

- **PASS** = Steps 0–4 all ✓ → **unblocks the D3 drill.** Record the date here.
- **FAIL at Step 1** = tailnet not authenticated → redo Precondition.
- **FAIL at Step 2** = LAN SSH / `.168` issue (not a backbone problem).
- **FAIL at Step 3** = cloudflared tunnel issue (independent of tailnet).
- **FAIL at Step 4** = app/`make status` issue (not connectivity).

**Result log:**
- [ ] Run on (date): __________  Result: PASS / FAIL at Step __  Notes: __________
