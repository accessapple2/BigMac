# Relay — .env PUSHOVER_TOKEN still invalid after the length fix. 2026-09-11

## What was asked

Verify the corrected `.env` `PUSHOVER_TOKEN` is 30 characters, send one
live test at each tier, confirm delivery under the OllieTrades app
identity, then remove the file-based fallback if clean.

## Length is fixed. The value still isn't accepted.

`PUSHOVER_TOKEN`: now **30 characters** (was 120) — confirmed. `PUSHOVER_
USER`: 30 characters, unchanged. Both shaped correctly.

**Live API call still fails identically to before the length fix**:
`{"token":"invalid","errors":["application token is invalid, see
https://pushover.net/api"]}`. Not a shape problem this time — the string
is the right length and still rejected by Pushover's own servers as not
a registered application token.

**Checked the obvious alternate explanation** (token/user swapped in
`.env`): tried both orderings directly against Pushover's API. Both
fail with the identical "application token is invalid" error — ruling
out a simple swap.

**Also ruled out a caching/shell artifact**: confirmed `PUSHOVER_TOKEN`
is not already present in `os.environ` before `.env` loads (so a stale
shell-level export isn't shadowing the file), and `load_dotenv(override=
True)` still produces the same 30-char value that still fails. This is
the real, current `.env` content being tested, not a stale copy.

**Did not attempt to further guess or brute-force the value** — that's
not a diagnosis this session can usefully continue; it needs checking
directly against the Pushover dashboard (pushover.net → the OllieTrades
app → confirm/regenerate its API token) on your end.

## Fallback: staying, not removed

The file-based fallback (`/usr/local/etc/pushover.env`) is the **only**
credential source that currently delivers — confirmed with a direct
send: `.env` fails, falls through, file succeeds. Removing the fallback
right now would take RED_ALERT delivery from "working via the old
identity" to "broken outright." Keeping it until `.env`'s token is
confirmed valid by an actual successful send.

## Net effect right now

Every alert is still going out under the **old, shared** Pushover
identity (the file's app), not the dedicated OllieTrades one — same as
before this whole repoint effort, just now with automatic, logged
fallback instead of a silent single-source failure. Nothing is
regressed; the dedicated-identity goal just isn't realized yet.

## Next step (yours)

Check the OllieTrades app's token directly on pushover.net and re-paste
it into `.env` — likely a copy/paste error (transposed or dropped
character) given the shape is now right but the value isn't recognized.
Once updated, re-run the same three-tier test; I'll confirm delivery and
pull the fallback once it's clean.
