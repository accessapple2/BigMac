# Relay — 2026-08-28 — Signal Center restore + Kirk briefing ntfy fix

## Context

Directive: Signal Center down (signal.ollietrades.com 502, `signal_center_9000:false`
in fleet/status) — restart it, find why it died, then check why all four Kirk
briefing jobs (power_hour/after_close/premarket/open_check) stopped sending
around 2026-07-20/22, restore them, and add KeepAlive supervision to Signal
Center.

## Root causes found (two separate, unrelated failures under one symptom)

**Signal Center was never crashing — it was deliberately disabled and never
turned back on.** `launchctl print-disabled gui/501` showed
`com.trademinds.signal-center => disabled`, a persistent per-user launchd
override. Its plist already had `KeepAlive=true` and `RunAtLoad=true` — that
was never the gap. The `@reboot` cron fallback
(`scripts/signal_center_reboot_start.sh`, the documented belt-and-suspenders
for this exact "GUI LaunchAgent doesn't survive an SSH-only reboot" gotcha)
was *also* out of action — not missing, but renamed
`signal_center_reboot_start.sh.quietdown-disabled-2026-07-22`. Both point to
the same date: **2026-07-22, the OllieTrades fleet stand-down.** The box has
rebooted twice since (2026-08-27 10:41, 2026-08-28 20:08) and Signal Center
never came back either time, because both of its recovery paths were
intentionally turned off that day and nobody turned them back on.

`launchctl print-disabled` also shows **~23 other `com.ollietrades.*` /
`com.trademinds.*` jobs still disabled from the same stand-down** — see the
companion question doc. Not touched this pass; see below.

**The four Kirk briefing cron jobs never stopped running.** `crontab -l`
still has all four `kirk_briefing.py --mode <x>` lines, and
`logs/kirk_briefing_cron.log` shows all four building and archiving
successfully every single day, including today. The failure is downstream:
**every `ntfy push` has been returning `429 Too Many Requests`**, for every
mode, seemingly since the dates you quoted. `push_ntfy()` had no retry — one
429 and the message was just dropped (`push_ok=False`), which is
indistinguishable from "nothing happened" unless someone reads the log.
Manually POSTing to the same `ollietrades-admin` topic right now returns 200
immediately, so this isn't a hard daily quota exhaustion — anonymous
ntfy.sh's per-IP limit is a short-lived token bucket that this box's overall
ntfy volume (many senders, one shared home IP) is apparently pegging during
market hours.

## Shipped

- `launchctl enable gui/501/com.trademinds.signal-center` +
  `launchctl bootstrap gui/501 ~/Library/LaunchAgents/com.trademinds.signal-center.plist`
  — live again, `state=running`.
- Restored `scripts/signal_center_reboot_start.sh` (renamed back from the
  `.quietdown-disabled-2026-07-22` suffix, `chmod +x`) — the `@reboot` cron
  line pointing at it was never removed, so this closes the loop on both of
  Signal Center's recovery paths without touching anything else from the
  stand-down.
- `kirk_briefing.py::push_ntfy()` — added retry-on-429 (up to 4 attempts,
  backoff 5s/20s/60s, honors `Retry-After` if ntfy sends one). Tagged
  `HM-KIRK-NTFY-429-RETRY`. No change to the message content, archive
  behavior, or the `.sent` delivery-audit logic — only the push call itself
  now tries harder before giving up.

## NOT done (explicitly out of scope this pass)

- The ~23 other disabled fleet jobs from the 2026-07-22 stand-down — left
  disabled. See `QUESTION_fleet-standdown-reversal.md`, same relay batch.
- `com.trademinds.premarket` (a *different*, older job — `premarket-scan.sh`,
  4:00 AM AZ, last ran 2026-07-22 — not part of the Kirk pipeline) — also
  left disabled, also covered in the question doc.

## Live-verification results

- `curl -o /dev/null -w '%{http_code}' http://127.0.0.1:9000/` → `302` (login
  redirect — server up)
- `curl -o /dev/null -w '%{http_code}' https://signal.ollietrades.com` → `302`
  (matches local — tunnel/proxy routing correctly again)
- `launchctl print gui/501/com.trademinds.signal-center` → `state = running`
- `ps aux | grep signal-center` → real PID under
  `~/autonomous-trader/venv/bin/python3`
- `push_ntfy()` smoke-tested directly (not via the full briefing pipeline —
  that would have written a duplicate archive/.sent for today) — real POST
  to `ollietrades-admin`, returned `[200] attempt 1`. The retry branch itself
  (the 429 path) is code-reviewed but not live-fired — can't safely force a
  real 429 without spamming the shared topic, and the market's closed right
  now so no natural one occurred during the test.
- `py_compile` clean under `.venv/bin/python3` (3.14.6, the actual cron
  interpreter, not just system Python).

## Not yet observed

The next natural test is tomorrow's live `kirk_briefing.py --mode premarket`
cron fire (05:45 AZ) during market hours, when the 429s have actually been
happening — worth checking `logs/kirk_briefing_cron.log` afterward for
`ntfy push [200] attempt N` where N>1 (retry saved it) vs four back-to-back
attempt-4 failures (retry wasn't enough, ntfy volume needs a structural fix,
not just patience).
