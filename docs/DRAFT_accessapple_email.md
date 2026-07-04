# DRAFT — accessapple.com email posture — FOR REVIEW, RECORDS LIVE IN A ZONE
# MY TOKEN CAN'T REACH

Investigated via read-only `dig` queries only (no API/dashboard access to
this zone — confirmed earlier this session: my token is scoped to
`ollietrades.com` Rulesets, and the CLI's cert scoping is a separate,
still-open issue, see `docs/DRAFT_cloudflared_reauth.md`). Everything below
is what's actually publicly resolvable right now, not guessed.

## 1. DMARC — already exists, correctly configured. No draft needed.

```
$ dig +short TXT _dmarc.accessapple.com
"v=DMARC1; p=none; rua=mailto:73b28e06653046de8e4ad42b82af807d@dmarc-reports.cloudflare.net"
```

This is **already** exactly the recommended starting posture: `p=none`
(monitor-only, doesn't affect delivery) with `rua` aggregate reporting
wired to Cloudflare's own DMARC report aggregator
(`dmarc-reports.cloudflare.net` — this is Cloudflare's free "Email
Security DMARC Management" feature, which parses the aggregate reports
for you rather than dumping raw XML). The directive's premise that this
needs drafting fresh doesn't hold — it's live and correct.

**Upgrade path to `quarantine`** (once confident no legitimate mail is
failing alignment — check the DMARC reports for a stretch of weeks first):
```
v=DMARC1; p=quarantine; rua=mailto:73b28e06653046de8e4ad42b82af807d@dmarc-reports.cloudflare.net; pct=25
```
Recommend adding `pct=25` (or similar) on the first move away from `p=none`
— quarantines only a sample of failing mail rather than all of it, so a
false positive doesn't suddenly vanish 100% of legitimate mail into spam
folders. Ramp `pct` up over successive weeks (25 → 50 → 100) once the
DMARC reports show the failure rate for legitimate senders is genuinely
zero, then finally move to `p=reject` once quarantine has run clean for a
few weeks.

## 2. SPF — syntactically correct. DKIM — completely missing. This is
## almost certainly the real "Fail" cause, not SPF.

```
$ dig +short TXT accessapple.com | grep spf
"v=spf1 include:spf.protection.outlook.com -all"
```

This is the standard, correct SPF record for Microsoft 365 / Outlook-hosted
mail: single include, hard-fail (`-all`, not the weaker `~all`). Checked
the DNS-lookup-count concern directly — `spf.protection.outlook.com`'s own
SPF resolves to a flat list of `ip4:`/`ip6:` mechanisms with **no further
nested `include:`**, so the full lookup chain is 1 (well under RFC 7208's
10-lookup limit, which would otherwise cause a hard PERMERROR treated as
Fail by most validators). Also confirmed only one `v=spf1` TXT record
exists on the apex (a second one would itself cause an immediate
PERMERROR) — so SPF's own syntax and lookup budget are both clean.

**DKIM CNAMEs do not exist:**
```
$ dig +short CNAME selector1._domainkey.accessapple.com
$ dig +short CNAME selector2._domainkey.accessapple.com
(both empty)
```

Without these two CNAMEs, Microsoft 365 cannot DKIM-sign outbound mail
with a domain-aligned signature — mail either goes out unsigned or signed
under Microsoft's own default domain (not aligned to `accessapple.com`).
**This is the more likely explanation for an observed "Fail"** than
anything wrong with SPF: DMARC passes on SPF-aligned OR DKIM-aligned (not
both required), but if whatever tool/report showed "Fail" was checking
DKIM specifically, or an overall DMARC-alignment summary where DKIM
contributes, this gap alone explains it — no SPF changes needed at all.

**Exact fix**: in the Microsoft 365 admin center → Exchange admin center →
"Protection" → DKIM (or `Enable-DkimSigningConfig` via PowerShell) → enable
DKIM signing for `accessapple.com`. This surfaces the **exact** two CNAME
target values to publish (they're tenant-specific, generated at enable
time — I can't predict them from outside; they follow the pattern
`selector1-accessapple-com._domainkey.<tenant>.onmicrosoft.com` based on
the tenant ID already visible in DNS, `NETORGFT6669011.onmicrosoft.com`,
but the admin center is the authoritative source, not a guess). Add both
as **DNS-only** CNAME records (not proxied — DKIM CNAMEs must resolve
directly, Cloudflare proxying a CNAME meant for mail-signing verification
would break it).

## 3. Origin-IP-exposure — methodology, not a specific finding

I could not identify the specific leaking record from outside the zone.
Checked the apex and ~20 common subdomain guesses (`www`, `ftp`, `direct`,
`origin`, `admin`, `cpanel`, `webmail`, `ns1`/`ns2`, `api`, `dev`,
`staging`, `cdn`, `mx`, `smtp`, etc.) — all either match Cloudflare's proxy
IP ranges (`104.21.x.x`/`172.67.x.x`, confirming they're correctly proxied)
or don't resolve at all. None of my guesses turned up a bare/DNS-only
record exposing a real origin IP — but I'm guessing blind; you have the
actual 18-record list in front of you and I don't.

**Exact methodology to find it yourself** (2 minutes in the dashboard):
1. DNS → Records. Sort or scan the **Proxy status** column.
2. Note every record set to **DNS only** (grey cloud) that has an A/AAAA
   value — these are the ones NOT hidden behind Cloudflare's edge.
3. For each DNS-only A/AAAA value found, check whether that **same IP**
   also appears as the target of any **proxied** record (orange cloud) —
   if so, that DNS-only record is leaking the real origin IP that the
   proxied record(s) are supposed to be hiding. An attacker who finds the
   DNS-only record can connect directly to that IP, bypassing Cloudflare's
   WAF/DDoS protection/access rules entirely for anything else pointed at
   the same origin.
4. **Fix**: either flip that record's proxy status to **Proxied** (orange
   cloud) if it's a web-facing service that should get the same
   protection, or — if it genuinely needs direct access (e.g., an
   SSH/admin endpoint that can't go through Cloudflare's HTTP(S) proxy) —
   move it to a **different IP** than the proxied services, so discovering
   it doesn't hand over the shared origin's address.

## Summary of what actually needs Admiral action
1. DMARC: no action needed now; revisit `p=quarantine` ramp after a
   monitoring period — informational only.
2. DKIM: enable in M365 admin center, publish the 2 CNAMEs it generates,
   as DNS-only.
3. Origin-IP-exposure: 2-minute dashboard scan per the methodology above —
   I don't have a specific record to point at.
