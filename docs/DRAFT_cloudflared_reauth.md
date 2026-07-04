# DRAFT — cloudflared re-authentication — PREP ONLY, NOT EXECUTED

Per Directive B item 4b: documents the exact steps + rollback for re-auth.
**I have not run `cloudflared tunnel login` and will not** — it opens a
browser for account selection, which needs the Admiral's own session.

## Why this is worth doing

Earlier this session (separate incident, see `docs/HANDOFF.md`'s "erroneous
DNS record" entry), a `cloudflared tunnel route dns` call meant for
`ollietrades.com` instead landed on `accessapple.com` — strong evidence
this box's cloudflared origin cert (`~/.cloudflared/cert.pem`) is currently
scoped to some account other than Bonnie's (the one that owns
`ollietrades.com`). That record turned out to not persist (confirmed
`NXDOMAIN` on recheck), so there's no known live exposure — but the cert
being logged into an unexpected account means any *future* CLI-driven
`route dns`/`route ip`/`route lb` call from this box risks the same
misfire. This doesn't affect the tunnel's actual data-plane operation
(the 5 live hostnames route correctly regardless of which account the CLI
happens to be logged into — that's governed by the tunnel ID + config.yml,
not the login cert) — it's specifically a risk for *future CLI-driven DNS
writes*, not current traffic.

## Exact steps

1. **Confirm current cert scope first** (informational, safe, no browser
   needed) — this alone might resolve the question without re-auth:
   ```
   cloudflared tunnel login --help
   ```
   Actually determining which account a cert is scoped to short of
   attempting an operation against a known zone isn't exposed by the CLI
   directly (no `cloudflared cert info` subcommand exists as of 2026.6.1) —
   the only reliable diagnostic is the kind of accidental-misfire evidence
   already observed. Skip to step 2 if this doesn't resolve it.

2. **Back up the current cert** before touching anything:
   ```
   cp ~/.cloudflared/cert.pem ~/.cloudflared/cert.pem.bak-$(date +%Y%m%d)
   ```

3. **Run the login** (Admiral, at the keyboard, browser required):
   ```
   cloudflared tunnel login
   ```
   This opens a browser to `https://dash.cloudflare.com/argotunnel` and
   prompts for **which account and zone** to authorize. **Select Bonnie's
   account, `ollietrades.com` zone** — this is the step that actually
   fixes the scoping; picking the wrong account here reproduces the exact
   same problem with extra steps.

4. **Verify the new cert's scope with a dry-run against a throwaway
   hostname** — do NOT test against a real hostname (would create a real,
   possibly-hard-to-clean-up record):
   ```
   cloudflared tunnel route dns trademinds cf-reauth-test-delete-me.ollietrades.com
   ```
   Expect: `Added CNAME cf-reauth-test-delete-me.ollietrades.com which will
   route to this tunnel` — critically, **check the exact hostname echoed
   back matches what you typed, not a nested/mangled variant** (this is
   exactly how the accessapple.com misfire was caught — a nested hostname
   in the confirmation output was the tell).

5. **Clean up the test record** — delete
   `cf-reauth-test-delete-me.ollietrades.com` from the `ollietrades.com`
   zone dashboard once confirmed (or ask me to verify + you delete, same
   division of labor as the accessapple.com incident: I can verify via
   `dig`, only you can delete from a zone your token doesn't reach for
   Rulesets-only access... actually DNS record deletion needs a
   DNS-scoped token or dashboard access — dashboard is simplest here).

6. **Re-run the same dry-run test again** after cert replacement is
   confirmed working, this time confirming it does NOT create anything
   under `accessapple.com` or any other unexpected zone — i.e., deliberately
   try to reproduce the original failure mode once, now that the cert
   should be correctly scoped, to close the loop with positive evidence
   rather than just assuming the fix worked.

## Rollback

If the new login somehow makes things worse (e.g., the new cert can't
route to the existing `trademinds` tunnel because it's now scoped to an
account that doesn't own that tunnel — possible if the account picker is
confusing or Bonnie's account isn't the one that owns the `trademinds`
tunnel resource itself, only the `ollietrades.com` zone):
```
cp ~/.cloudflared/cert.pem.bak-<date> ~/.cloudflared/cert.pem
```
No cloudflared restart is needed for a cert swap alone — the cert is only
read at `route`/`login`/tunnel-creation time, not by the running data-plane
`tunnel run` process (which authenticates via the tunnel's own credentials
file, `~/.cloudflared/<tunnel-id>.json`, a separate artifact from
`cert.pem`). This means the re-auth process itself carries **zero risk to
the live, running tunnel** — the 5 hostnames currently being served are
unaffected by cert.pem changes; this only affects future CLI-driven
`route`/`login`/management commands run from this box.

## What this does NOT need
- No cloudflared restart (per the rollback note above — separate credential files)
- No DNS downtime for the 5 live hostnames
- No tunnel re-creation — same tunnel ID (`dee0002c-c451-4919-8b16-d649ad19d029`) throughout
