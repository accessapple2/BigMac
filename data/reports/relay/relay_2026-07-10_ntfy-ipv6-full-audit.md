# Relay: full ntfy IPv6 audit — 2 more confirmed-broken senders fixed (S6, work block 14)

**Date:** 2026-07-10
**Commits:** `1e8411a` (watchdog.py + riker_synthesis.py fixes), `1ebd76f`
(backlog catalogue)
**Prior context:** "audit the remaining one-off ntfy scripts" (from the
list compiled in the prior work block) → found the risk is much larger
than estimated (~18 files, not ~15-20 vaguely) → "fix the 2 confirmed-
broken ones now."

## What was asked

Audit the ~15-20 one-off scripts flagged in the prior block's ntfy topic
inventory for the same IPv6-exposure pattern already found and fixed
twice in `long_range_sensors.py` and `engine/ntfy.py`.

## What the audit found

Systematically catalogued every file referencing `ntfy.sh`: 17 `.sh`
scripts (curl-based, confirmed safe via two separate live tests tonight)
and 42 `.py` files. Of those, 9 already used the hardened path
(`alert_channels.send_alert`/`_send_ntfy` or the now-fixed
`engine.ntfy`). The remaining ~32 each have their own separate,
hand-rolled implementation — a genuine, repeated anti-pattern across the
codebase, not a one-off.

**Important correction made mid-audit:** the bug is not unconditional.
Re-tested plain `requests.post` and raw `urllib.request` against the real
API right now — both succeeded instantly, because this resolver
currently orders IPv4 addresses first by default. But a forced-IPv6
connect still fails immediately with the same `[Errno 65] No route to
host` — confirming the underlying condition is real and current, just
address-family-ordering-dependent (matches the original 2026-07-07 fix's
own note that this is "context-dependent, interpreter/cron invocation
specific"). This means a clean interactive test cannot rule out risk for
a given file's actual cron/production invocation context — the only
reliable signal is each file's own historical log evidence.

**Checked several files' own logs directly rather than guessing from
code shape alone:**
- `watchdog.py` — **12 real failures**, as recently as 2026-07-09.
  Significant: this is the central mechanism tonight's whole
  departure-hardening effort depends on.
- `engine/riker_synthesis.py` — **21 real failures**.
- `engine/fleet_auditor.py` — 0 occurrences (clean, though not
  necessarily immune given the ordering-dependent nature of the bug).
- `orcl_gex_alerts.py`, `fred_data.py`, `universe_refresh.py`,
  `dayblade_scanner.py` — no dedicated log file found by name, not
  individually checked further.

## What shipped (commit `1e8411a`)

**`watchdog.py::push_alert()`** — fixed with a **self-contained** local
IPv4-force lock+monkeypatch, deliberately NOT importing
`engine.alert_channels`. Reasoning: watchdog.py's entire design point is
staying dependency-free from the `engine/` package it monitors, so it
keeps working even if that package has an import-time problem. Mirrors
`alert_channels.py`'s existing technique without creating a cross-module
dependency.

**`engine/riker_synthesis.py::_ntfy()`** — delegates to the already-
hardened `_send_ntfy()` instead. This file already imports from `engine/`
elsewhere, so this is consistent with its existing dependency posture
(unlike watchdog.py). Removed the now-unused `requests` import.

## Testing

- `tests/test_watchdog_ntfy_ipv6_fix.py` (3 tests): forces IPv4 during
  send and restores after; restores even when the send itself fails; the
  monkeypatch helper forces `AF_INET` correctly.
- `tests/test_riker_synthesis_ntfy_ipv6_fix.py` (3 tests): routes through
  `_send_ntfy` with correct topic/priority; default priority; an
  exception inside `_send_ntfy` doesn't propagate.
- Full suite: 996 passed, 21 failed — same pre-existing flakiness
  (bbkc/m5_allocator families) confirmed multiple times already tonight,
  unrelated to these changes.
- `py_compile` clean on both files.

## Live verification

- `engine/riker_synthesis.py` is cron-invoked fresh every 10 minutes, not
  imported into `main.py` (confirmed via grep — `main.py` uses the
  separate `engine/riker_xo.py` module instead). Picks up the fix
  automatically on its next run; no restart needed or performed.
- `watchdog.py` is a long-running standalone process with the old code
  held in memory — restarted it directly (kill + the same
  `watchdog_supervisor.sh` relaunch mechanism production uses). Its own
  startup fired a real "Watchdog Online" push through the newly-fixed
  code, and the log shows **"Push sent [200]"** — genuine live
  confirmation beyond the unit tests. Neither fix touches `main.py`/the
  trader process — no trader restart needed or performed this block.

## docs/XO_BACKLOG.md (commit `1ebd76f`)

Filed `HM-NTFY-IPV6-NOROUTE-SWEEP` — full inventory of all ~18 originally-
unprotected files, which 4 are now fixed (with their evidence trail),
which ~14 remain (each named individually), and a recommended fix
pattern + prioritization approach for a future pass (check each file's
own log for real evidence before fixing, same discipline used tonight).

## Open items (carried forward, plus the new catalogue)

1. **~14 more files** with the same unprotected-sender pattern, catalogued
   by name in `HM-NTFY-IPV6-NOROUTE-SWEEP` — not fixed tonight, scope was
   explicitly "the 2 confirmed-broken ones."
2. `HM-STATUS-PAGE-STALE-CACHE` — still needs a Cloudflare dashboard
   change only the Captain can make.
3. `HM-STRATEGIES-EXECUTOR-STATUS-NEVER-SET`/`HM-SWINGDESK-CLOSE-
   PHANTOM-ROW` pnl gap — on hold pending a live MLEG close.
4. `HM-DRAWDOWN-BLIND-TO-OPTIONS-PNL` — needs a dedicated design session,
   zero current urgency.
5. The `options_books` stored-counter drift — still harmless, still out
   of scope.
