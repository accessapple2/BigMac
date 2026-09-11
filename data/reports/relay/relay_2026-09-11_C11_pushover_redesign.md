# Relay — C11: Pushover redesign. 2026-09-11

Four named sub-items. Two shipped and verified live, one flagged as a real
finding requiring an Admiral decision (not unilaterally acted on), one
scoped as a deliberate follow-up rather than a rushed merge.

## The finding underneath this item: ntfy has been fully dead since 2026-07-19

`engine/alert_channels.py::_send_ntfy()` has an unconditional early
`return False` at its top — **all code after it is unreachable, dead
code.** Comment: *"DECOM-SILENCE 2026-07-19 — all ntfy pushes silenced
ahead of Gate 2 full removal (Admiral wants phone quiet immediately)...
Revert by deleting this guard if silence needs to be lifted before Gate 2
lands."* Gate 2 (DECOM-MASTER) landed weeks ago. **Before this session,
INFO and WARNING-level alerts across the entire fleet had no working
delivery path to a phone at all** — only RED_ALERT (via Pushover + email)
ever reached anyone. This is almost certainly the concrete thing "alerts
need to be usable before we undock" was pointing at.

**Not reversed here.** The comment frames this as an explicit Admiral
directive ("phone quiet immediately"), and whether it was meant to lapse
automatically at Gate 2 or stay in effect until explicitly lifted isn't
something the code can decide — reversing a stated preference without
confirmation is a real overstep even though the surrounding comment
suggests it was meant to be temporary. **Flagged here for an explicit
decision; the tiered-Pushover work below closes the practical gap (a
working WARNING delivery path) without needing to touch this at all.**

## 1. Tiered routing — shipped, verified live

Pushover was RED_ALERT-only. WARNING now also reaches Pushover, at
**quiet priority (-1: shows in the app, no sound/vibration)** — a real
second tier instead of the previous hard cliff (silent vs. everything).
RED_ALERT stays priority 1, unchanged.

Verified with a live functional test (real `_send_pushover` mocked to
avoid an actual network call, everything else exercised for real):
single WARNING → one quiet push, confirmed `priority=-1`.

## 2. Storm breaker — shipped, verified live

New: `_storm_record_and_check()`/`_storm_notice_due()`, persisted the same
way the existing per-type rate limiter is (survives restarts). Tracks
loud-channel (WARNING+RED_ALERT) dispatch timestamps **globally across all
alert_types** — the specific gap the existing per-type rate limiter
doesn't cover (this repo's own real 07:33 lock-storm incident was exactly
this: several *different* alert types each individually under their own
rate limit, together still a flood). Threshold: 8 dispatches / 10 min
window. Once tripped, individual WARNING pushes stop reaching Pushover,
replaced by one rate-limited (30 min cooldown) "STORM: N alerts, check
dashboard" digest. RED_ALERT is **never** storm-suppressed outright (would
violate the "alarm can't share a failure mode with what it watches"
doctrine) — it always sends its own push, but contributes to the shared
counter and piggybacks a storm-context line when a notice is due.

Verified live: fired 11 WARNING alerts of distinct `alert_type`s in a
row — first 7 delivered individually, 8th+ correctly collapsed into a
single storm digest. A RED_ALERT fired immediately after (storm still
active) still delivered its own individual push, confirmed.

## 3. OllieTrades token — plumbing shipped, external step still needed

`_send_pushover()` now prefers `PUSHOVER_OLLIETRADES_TOKEN`/
`PUSHOVER_OLLIETRADES_USER` env vars (a dedicated app identity, so
OllieTrades alerts show under their own icon/name in the Pushover app)
over the existing shared `/usr/local/etc/pushover.env` creds, falling
back to the shared identity if the dedicated vars aren't set — which is
the case right now, so behavior is unchanged until the token exists.

**Cannot be completed by this session**: creating the actual dedicated
app is a pushover.net account action (Pushover → create an application,
get its token) — outside what this code can self-provision. Flagged as an
explicit Admiral to-do, not silently left half-built.

## 4. Kirk merge — investigated, scoped as a deliberate follow-up, not merged tonight

`kirk_briefing.py::push_ntfy()` is a **separate, standalone** ntfy
implementation — its own IPv4-workaround lock, its own 429-retry loop, own
delivery-confirmation semantics. Its own comment explains why: *"Distinct
from engine.ntfy._fire (fire-and-forget, swallows result): Job B's
delivery guarantee needs to KNOW the push landed."* Kirk's implementation
deliberately provides a **stronger** guarantee (retry-until-confirmed)
than `alert_channels.py`'s fire-and-forget dispatcher offers today.

A careless merge risks *weakening* the one notification path serving the
Admiral's stated "phone push, no hands-on" daily heartbeat — not touched
in the same pass as three other structural changes to the shared alert
module. The right design (not built tonight): Kirk keeps its
guaranteed-delivery retry loop, but calls into the same underlying
IPv4-hardened Pushover/ntfy primitives `alert_channels.py` now has,
instead of duplicating them — consolidation without weakening the
guarantee. Filed to `docs/XO_BACKLOG.md` as its own scoped item.

## Verification

`python3 -m py_compile engine/alert_channels.py` clean. `pytest -k
alert`: 134/135 pass — the one failure
(`test_hm_ops_sentinel_acks.py::test_main_dispatches_only_unsuppressed_
alerts`) is pre-existing and unrelated, confirmed via `git stash`
(identical failure on the pre-change tree; a live-data-fragile test
tripping on a real overdue `REVISIT-BY` tag in `docs/XO_BACKLOG.md` that
this particular test doesn't mock).
