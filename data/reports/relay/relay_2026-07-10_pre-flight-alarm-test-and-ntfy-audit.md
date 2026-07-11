# Relay: pre-flight alarm test COMPLETE + ntfy topic audit + 2 more IPv6 fixes (S6, work block 13)

**Date:** 2026-07-10
**Commits:** `2147771` (`long_range_sensors.py` fix), `ead1cac` (kill-test
complete + CLAUDE.md correction), `2400c7e` (`engine/ntfy.py` fix)
**Prior context:** "check the pre-flight alarm test" → discovered the
IPv6 root cause → "Do both" (fix `long_range_sensors.py` + run the
kill-test) → full kill-test run → "list all ntfy" → found a third
unprotected sender → "yes, fix it too."

## Summary of the whole arc

Started as a status check on one backlog item and ended up: fixing two
separate unprotected ntfy senders, closing out the one pre-flight-test
item that explicitly needed the Captain's own device, discovering an
undocumented (and better-than-documented) production safety net, and
closing a real, previously-hidden phone-subscription gap.

## Fix 1: `engine/long_range_sensors.py` (commit `2147771`)

Root-caused ~13,300 `ntfy failed` lines in `trader_error.log` this week
to this one file's own unprotected `requests.post()` — never given the
IPv4-force fix already applied to `engine/alert_channels.py` for this
box's confirmed lack of an IPv6 route to ntfy.sh. Verified the IPv6
condition is still live today via direct socket test. Fixed by routing
through the already-hardened `send_alert(audience="crew",
bypass_rate_limit=True)`. Also surfaced the topic-fragmentation issue
(below) while investigating.

## Topic-fragmentation finding (real, now resolved)

`watchdog.py`/`eod_report.py` resolve their "admin" topic from `.env`'s
`NTFY_ADMIN_TOPIC=Ollie-Alert-35` — a *different* topic than the literal
`"ollietrades-admin"` string hardcoded in four scripts wired in earlier
tonight and in `origin_healthcheck.sh`. **`Ollie-Alert-35` was not
subscribed on the Captain's phone until this session** — confirmed and
fixed directly by the Captain mid-session. This means watchdog/eod_report
alerts — arguably the most important ones — may not have been landing
before tonight, independent of the IPv6 bug.

## Full pre-flight kill-test (commit `ead1cac`)

Ran the actual disruptive test (with explicit confirmation at each
live-service-touching step, per the permission classifier's requirements
tonight):

| Service | Recovery mechanism | Downtime | Alert path exercised? |
|---|---|---|---|
| Tunnel (cloudflared) | Undocumented system LaunchDaemon, `KeepAlive` | <1s | No — too fast to catch |
| Dashboard (swingdesk) | Same undocumented LaunchDaemon pattern | <2s | No — same reason |
| Trader (main.py) | Cron keepalive (5-min) + watchdog 3-strike | ~2m19s | **Yes — Captain confirmed received** |
| `eod_report.py` manual run | Now on the fixed `send_alert` path | n/a | **Yes — Captain confirmed received** |

**Major undocumented-infrastructure finding:** cloudflared and swingdesk
are NOT on the cron+nohup fallback `CLAUDE.md`'s "LaunchAgent Reboot
Lifecycle" section describes as the current state — they're protected by
real, working, system-domain LaunchDaemons
(`/Library/LaunchDaemons/com.trademinds.cloudflared.plist`, dated
2026-06-11; `com.trademinds.swingdesk.plist`, 2026-06-17) that `CLAUDE.md`
still incorrectly calls "deferred." Confirmed live: killed each service
directly, watched a replacement appear in the same second, independent of
any of this repo's own watchdog/cron mechanisms. `status_page` has the
same pattern too (already correctly noted in `status_page_restart.sh`'s
own comment, just never propagated to `CLAUDE.md`). `CLAUDE.md` corrected.

Trader and signal-center are still on the slower cron+nohup fallback —
confirmed the difference matters: it's exactly why the trader's alert
path was observable (slow enough to catch) while the other two weren't
(too fast).

## ntfy topic inventory, compiled on request

Built a full table of resolved topics (`Ollie-Alert-35`/`-55` from
`.env`, `ollietrades-admin`/`-crew`/`-proving-ground` as literals,
`ollie-critical`/`-premarket`/`-signals`/`-pick` from other env vars),
which sender each uses, and IPv6-safety status for each. Also flagged a
long tail of ~15-20 individual one-off scripts (`dr_crusher.sh`,
`q_dissent_watch.py`, etc.) each hardcoding their own topic — not
individually audited, out of scope for tonight.

## Fix 2: `engine/ntfy.py` (commit `2400c7e`)

Compiling that inventory surfaced a **third** unprotected sender — same
disease as fix 1, this time in the module that owns the broader
`ollietrades-crew`/`Ollie-Alert-55` channel (BUY/TP/stop/regime-change/
spread-signal notifications) plus the Sniper Mode proving-ground topic.
Fixed the same way: delegate to the already-hardened `_send_ntfy()`
rather than a third separate implementation, reusing its existing
IPv4-lock-and-patch pair rather than risking two independent locks
racing to monkeypatch the same process-global `socket.getaddrinfo`.
Mapped the module's 1-5 integer priority scale to `_send_ntfy`'s string
labels. All ~10 public `notify_*` call signatures and the daemon-thread
fire-and-forget contract are unchanged.

## Testing (both fixes)

- `tests/test_long_range_sensors_ntfy_ipv6_fix.py` (4 tests) and
  `tests/test_engine_ntfy_ipv6_fix.py` (5 tests) — both passing.
- Full suite after both fixes: 990 passed, 21 failed — re-verified via
  `git stash` (twice, once per fix) that the bbkc-family failures are
  identical with each change fully reverted; same pre-existing flakiness
  confirmed three times tonight now, non-deterministic in exactly which
  tests fail run to run within that file.
- `py_compile` clean on all touched files.
- Trader restarted twice (once per fix), clean startup both times, module
  imports verified both times.

## Live verification

- Direct socket test confirmed the IPv6-no-route condition is still real
  and current (not historical).
- Both fixes restarted and confirmed clean, no new errors.
- Kill-test results verified end-to-end with Captain confirmation on four
  separate real pushes tonight (accidental test ping, TRADER REVIVED,
  EOD Report, plus the earlier subscription-gap fix itself).

## docs/XO_BACKLOG.md + CLAUDE.md

- `XO-DEPARTURE-HARDENING` Phase 3 item 10 marked **COMPLETED** with the
  full results table and the LaunchDaemon side-finding.
- `CLAUDE.md`'s "LaunchAgent Reboot Lifecycle" section corrected: which
  services have the LaunchDaemon (cloudflared, swingdesk, statuspage) vs.
  which are still on cron+nohup (trader, signal-center, watchdog), plus a
  flagged (not urgent) redundant cron line now superseded by its
  LaunchDaemon.

## Open items (carried forward, plus new)

1. **`engine/ntfy.py` was the last of the three found senders — but the
   long tail of ~15-20 one-off scripts wasn't audited.** If any of those
   matter for critical alerting, worth a future sweep.
2. Redundant `@reboot cloudflared_reboot_start.sh` cron line — harmless,
   not urgent, flagged for a future cleanup pass.
3. `HM-STATUS-PAGE-STALE-CACHE` — still needs a Cloudflare dashboard
   change only the Captain can make.
4. `HM-STRATEGIES-EXECUTOR-STATUS-NEVER-SET`/`HM-SWINGDESK-CLOSE-
   PHANTOM-ROW` pnl gap — on hold pending a live MLEG close.
5. `HM-DRAWDOWN-BLIND-TO-OPTIONS-PNL` — needs a dedicated design session,
   zero current urgency.
6. The `options_books` stored-counter drift — still harmless, still out
   of scope.
7. XO-DEPARTURE-HARDENING Phase 1 items 8/9 (weekly digest, error-filter
   consolidation) — still open, not part of tonight's scope.
