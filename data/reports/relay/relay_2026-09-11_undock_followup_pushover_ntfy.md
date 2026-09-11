# Relay — Undock follow-up: Pushover repoint + ntfy lift, live-verified. 2026-09-11

## 1. Pushover repointed to .env — and a real bad-value found along the way

Repointed `_send_pushover()` to read `PUSHOVER_TOKEN`/`PUSHOVER_USER` from
`.env` (the actual OllieTrades app credentials, added 2026-09-09 — no
separate app was ever needed; C11's earlier `PUSHOVER_OLLIETRADES_TOKEN`
env-var scheme was based on a wrong assumption and is now removed).

**Found live, not assumed**: `.env`'s `PUSHOVER_TOKEN` is **120
characters** — Pushover's application-token format is exactly 30. Their
own API rejects it outright: `{"token":"invalid","errors":["application
token is invalid"]}`. The older `/usr/local/etc/pushover.env` file's
token is correctly 30 chars and is the one that's actually been
delivering RED_ALERT pushes until now. **A presence-only fallback (env
set → never try the file) would have gone silently dark the moment this
repoint shipped** — rewrote it to fall back to the file on an actual send
failure, not just absence, and tried both credential sources live.

**Result, confirmed by real dispatch, not just code review**: all three
tiers now correctly route and deliver —
- INFO → ntfy only (confirmed `True`)
- WARNING → ntfy + browser + quiet-priority Pushover (confirmed `True`,
  delivered via the file fallback since `.env`'s token still fails)
- RED_ALERT → ntfy + Pushover + browser (confirmed `True`, same fallback)

**Still needs your attention**: `.env`'s `PUSHOVER_TOKEN` value itself is
wrong (wrong secret pasted in on 2026-09-09?) and should be corrected —
the code now tolerates it via fallback, but the dedicated-app intent
(distinct app icon/identity for OllieTrades) isn't actually realized
until that value is fixed, since every send is currently going out under
the old file's identity.

Also noted, not fixed (unrelated, pre-existing, off by config not by
bug): email delivery at RED_ALERT returns `False` because `SMTP_HOST`/
`SMTP_USER`/`SMTP_PASS` aren't set in `.env` — was already off before
today, not something this pass touched or was asked to touch.

## 2. ntfy DECOM-SILENCE lifted

Removed the unconditional `return False` in `_send_ntfy()`
(`engine/alert_channels.py`) that had suppressed every ntfy push since
2026-07-19. Confirmed live: all three test dispatches (INFO/WARNING/
RED_ALERT) show `ntfy: True` — real delivery restored, ~2 months after
Gate 2 (the condition the silence was tied to) landed.

## Live verification method

Two full three-tier dispatch rounds through the real `send_alert()`
production path (`bypass_rate_limit=True`, distinct `alert_type`s so
nothing collided with the storm breaker or rate limiter from the earlier
C11 testing). First round exposed the bad `.env` token; second round
(after the fallback-on-failure fix) confirmed full delivery at every
tier. Both rounds sent real notifications — by design, since "confirm
they route correctly" means confirming real delivery, not just a
code-path assertion.
