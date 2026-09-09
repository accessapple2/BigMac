# Scotty Handoff — 2026-07-09 PM

**Session span:** ~07:50 AZ (original XO sweep handoff) → 14:55 AZ (this writeup).
**Live status at writeup:** trader process pid 10120, up 2h20m (started 12:35:09
AZ), HTTP 200 on `/api/status` at ~0.1s. 24 commits this session, all on
`exec-pipeline`. Test suite: 735 passing / 14 pre-existing failures (unrelated
to anything touched today — bbkc_squeeze_scanner, conviction_stop_shadow,
fleet_trail_conviction_scale, ollama_cancel_on_timeout, quality_gate_hold,
universe_filter, war_room_instrumentation).

---

## Part 1 — Original morning XO sweep (11 items) + immediate action

All investigated and verified against live code/logs/DB before touching
anything (per this repo's verify-first doctrine). 7 confirmed real bugs, 2
by-design items needing an Admiral call, 2 false alarms caught before wasting
time chasing them.

| # | Item | Verdict | Commit |
|---|---|---|---|
| A | McCoy COST position -15.8% — emergency flatten | Misattributed to McCoy (actually -7.98%, not breaching). Real position was `alpaca-mirror`, a broker-mirror row reflecting the REAL Alpaca paper account's own COST holding (-16.17%, "$73 dust" match exact). Closed via `AlpacaBridge.close_position`. Also closed `ollie-auto`/NUKZ (-8.83%). | `3814de8` |
| 1 | ntfy IPv6-no-route silently killing kirk_briefing sends | Confirmed + fixed. Ported the IPv4-force `getaddrinfo` monkeypatch already proven in `alert_channels.py`. | `aee01ba` |
| 2 | Hard-stop coverage gap (root cause of A) | `_check_hard_stops()` only covered 10 hardcoded players; 4 of 7 active players had zero stop-loss enforcement. Now queries actual position-holders. | `b175d39` |
| 3 | Dead Live Event Tape detector | Real root cause was deeper than suspected: an O(n²) correlated-subquery query that ran 3+ min and never completed at production tick volume — not the import-lock deadlock first suspected (that was also fixed, but wasn't the actual cause). | `3a39ec4`, `8baa851`, `e513aec` |
| 4 | riker_synthesis.py SQLite lock crash (06:40/06:50 cadence skip) | Retry + `db_conn` migration shipped. **See Part 3 — this had its own regression, now separately fixed.** | `dc52bde` (+ `224292b`, see below) |
| 5 | Capitol Trades MU alert dedup (15x re-fire) | Dedup now checks `crew_decisions` (signal-emission level), not just executed trades. | `7f7fdb9` |
| 6a | tax_harvester write-on-every-read | Init-once guard + 503 on lock contention. | `3c5d109` |
| 6b | Bare `/api/trades` unbounded serial price loop | Parallel batch fetch (reused `get_all_prices`, same as `/api/market/prices`) + lower default limit. | `cf0fd24` |
| 7 | BENCH should gate new entries | Shipped. Live-caught blocking real entries in production within minutes of restart (Dax on DRAM/SCHG, McCoy on SNAP/CNTA). | `527da99` |
| 8 | McCoy confidence-flood detection | Shipped. Caught its own false positive during dev (flagged `capitol-trades`, a rules-based non-LLM agent, on legitimate deterministic repeats) — excluded `RULES_SCANNERS`. Ollama-context root-cause investigation filed separately, not started. | `cf90bb5`, `b4c6572` |
| 9 | AuthMiddleware CF Access gap on page routes | Shipped — **this item grew a long tail, see Part 2.** | `a021437` |
| 10 | HIMS-style reasoning/action direction mismatch | Visibility-only flag, shipped. Reproduces the exact HIMS incident text in tests. | `5f62254` |

---

## Part 2 — Item 9's follow-up tail (Captain-reported, same-day)

Item 9's page-route fix let CF-authenticated browsers load `/classic` without
the PIN page, but surfaced a second-order gap: proving a valid CF JWT exists
only answers "can this request proceed," not "who is logged in" — `/api/me`
and `/api/active-users` kept 401ing, pausing alert pollers with a permanent
session-expired banner.

- **Session identity minting** (`e207fec`): `AuthMiddleware` now mints a real
  local session from the CF JWT's `email` claim — via `request.state`
  override so the *current* request's own handler sees it immediately, plus
  the normal cookie for future requests.
- **Poller auto-recovery** (`46744f2`): `onAuthLost` now runs a 30s recovery
  probe against `/api/me`; auto-resumes pollers + clears the banner on first
  200. Both `onAuthLost`/`onAuthRecovered` are idempotent. Verified via a
  16-check Node simulation harness (mocked DOM/fetch/timers) — no real
  CF-authenticated browser was reachable from this environment to verify
  end-to-end; **that verification is still owed to whoever has real browser
  access with CF Access credentials.**
- **Two console findings while in there, both real, both fixed:**
  - `riker-hero` "awaiting synthesis" forever: root-caused to a *completely
    separate* system (`engine/riker_xo.py`, not `engine/riker_synthesis.py`)
    that was never scheduled anywhere — only a manual button ever triggered
    it, in-memory cache resets every restart. Now scheduled every 10 min in
    `main.py` (`4ae94fe`). Live-verified generating real recommendations.
  - `uhura-insider-reads` stale since 2026-06-13 (26 days): `edgartools`
    package was missing from `.venv`, never pinned in `requirements.txt`,
    silently zeroing both 13F and Form-4 data sources every single day with
    no error surfaced. Installed + pinned (`4ae94fe`). Live-verified: Form 4
    import went from 0 → 181 real transactions, 8 real signals generated.
- **Cockpit panels stuck at "—" forever on fetch failure** (`62e834e`):
  explicit error state added to all 6 summary tiles + 3 table panels +
  refresh timestamp. Clears automatically on next successful render.
  - The reported "Started 1 intervals for cockpit" logging 2x on section
    switch was investigated but **not conclusively root-caused** — this
    codebase has two parallel, independently-built interval-management
    systems, and cockpit/`cpLoad` isn't registered in the newer one's
    `_sectionFetchMap` at all (which is itself odd, since cockpit's polling
    demonstrably works). Cockpit's data correctness is unaffected. Deferred
    as a minor efficiency/log-noise issue rather than risk a blind change to
    fragile, only-partially-understood polling infrastructure mid-incident.
  - A follow-up cpLoad() bug report ("second .then re-parses an already-
    parsed body") was investigated and **found not to reproduce** — the
    committed code (`62e834e`) already does check-then-parse-once correctly.
    Confirmed by the Captain as a mid-edit draft pasted to a reviewer by
    mistake, not a real issue with what shipped.
- **Navigator (Chekov's Station) UI fixes (4 items reported)** — sidebar
  grid-offset overlap, forced-6-column crowding, Entry/Target/Stop/R:R
  wrapping, HM-OPS-SENTINEL toast stacking — **could not be located.**
  `grep` for "Chekov's Station" only matches the older, simple
  `section-navigator` block in `index.html` (no sidebar, no forced grid, no
  toast system there); "HM-OPS-SENTINEL" doesn't appear in any frontend file
  at all. Likely a different UI surface (signal-center has its own separate
  frontend). **Still waiting on the exact URL/path — flagged mid-session,
  no reply yet as of this writeup.**

---

## Part 3 — Live incident: trader process down, then a recurring wedge

Independent of the above, a genuine production incident fired mid-session.

### 3a. Hard crash (service fully down, ~17:35–17:48 UTC / 10:35–10:48 AZ)

**Root cause:** `main.py`'s module-level `setup()` call (line ~4063, calling
`setup_db.py::setup()`) ran completely unguarded — one
`sqlite3.OperationalError: database is locked` during a startup-time WAL
contention window took down the **entire process** with an uncaught
exception, before dashboard/scanners/stop-enforcement could start.
`trader_error.log` showed this crash **3x in a row** — the watchdog kept
auto-restarting straight into the same failure. Fixed with a 3-attempt retry
+ short backoff (`setup()` is idempotent throughout, safe to retry); still
re-raises loudly if all 3 fail. **`07611ee`.**

### 3b. Recurring "wedge" (port bound, zero response, process alive)

Separate and more elusive: several times after the crash was fixed, the
dashboard would go fully unresponsive (bound socket, no response to
anything) while the trading engine kept running fine in other threads. Each
time, the watchdog self-healed it via restart before a `py-spy` dump could
be captured (no passwordless sudo available for `py-spy dump --pid` in this
session — **flagged for the Captain to either add a scoped NOPASSWD sudoers
entry for it, or run it manually next occurrence**).

**Important context found mid-investigation:** `logs/watchdog_cron.log`
shows "Bridge unresponsive" events going back to **May 31st**, 175+
occurrences, the large majority self-recovering in 1-3 min without ever
needing a restart. **This is a long-standing, chronic pattern that predates
today entirely** — not something introduced by today's work.

**Hardening shipped regardless** (`dd55d1a`): `AuthMiddleware.dispatch` is
`async def` but called `try_cf_access()`/`get_cf_identity()` directly and
synchronously — both validate the CF Access JWT via `PyJWKClient`, which
does a **synchronous** HTTP fetch of Cloudflare's certs endpoint (cached
after success, re-fetched on cache miss). Any network hang there — and this
box already hit unrelated network flakiness today (ntfy.sh, see Part 4) —
would block uvicorn's entire event loop. **Not confirmed as THE cause**
(the certs endpoint responded fine when tested live), but a real
architectural risk regardless. Wrapped both calls in `asyncio.to_thread` +
a hard 5s timeout; times out closed (401/redirect), never silently open.

**Result:** longest stable window went from ~5-15 min (observed 3x before
the fix) to 53 min, then the current instance has now run **2h20m+** with
zero wedges. Strongly suggestive the fix helped, but given the chronic
May-31-onward history, **cannot be called fully resolved** — recommend
continued monitoring, and getting real `py-spy` access wired up so the next
occurrence (whenever the CF-JWT fix's benefit wears off, if it does) gets an
actual stack dump instead of another guess.

---

## Part 4 — External, not this system's fault

**ntfy.sh has been unreachable from this box since ~09:00 AZ, still down as
of 14:55 AZ (5.5+ hours).** Confirmed via direct curl (both default and
`-4` forced IPv4) — full timeout, not a routing error. The `kirk_briefing.py`
IPv4-force fix (`aee01ba`) is confirmed **working correctly**: today's
12:45 power_hour failure was a plain timeout (destination unreachable), not
the IPv6 "No route to host" the fix targeted — that specific bug class is
resolved. Briefing content is safely archived either way (by design). This
is an external outage outside this system's control; worth checking ntfy.sh's
own status or considering a backup channel if it doesn't clear on its own.

---

## Part 5 — A regression caught and fixed during this handoff

While answering "did the riker_synthesis fix (`dc52bde`) actually produce a
fresh write" — it hadn't. `rikers_log`'s latest row was frozen at 09:21 AZ
despite it being mid-afternoon. Cause: `dc52bde` added
`from engine.db_conn import get_conn` as a **module-level** import. This
worked when smoke-tested interactively (`python3 -c "from
engine.riker_synthesis import ..."` from the repo root, which already has
the repo root on `sys.path`) but broke on **every real cron invocation**
with `ModuleNotFoundError: No module named 'engine'` — cron runs
`python3 /abs/path/to/engine/riker_synthesis.py`, and running a script BY
PATH sets `sys.path[0]` to the script's own directory (`engine/`), not the
repo root; the crontab's `cd .../autonomous-trader &&` before the python3
call doesn't change this. Broke the every-10-min cron silently for ~5.5h
before being caught by checking the actual cron log instead of trusting the
interactive test. Fixed with the same repo-root `sys.path` insertion pattern
`kirk_briefing.py` already uses (`224292b`). Live-verified by running the
exact cron invocation shape; new regression test invokes the script as a
subprocess by full path specifically (not a same-process import, which
wouldn't have caught this the first time).

**Lesson for future smoke tests on anything invoked by cron/launchd:** test
the actual invocation shape (full path, subprocess, correct cwd), not an
interactive shortcut from within the repo — they can have different
`sys.path` and this bug class is exactly the kind that hides there.

---

## Open items for next shift

1. **py-spy sudo access** — needed to get a real stack dump on the next wedge,
   if the CF-JWT fix's apparent improvement doesn't hold.
2. **Navigator (Chekov's Station) UI fixes** — 4 items reported, could not
   locate the actual file/URL. Need the exact path.
3. **CF Access real-browser round-trip** — session-minting + poller-recovery
   verified via unit tests/simulation only; needs an actual CF-authenticated
   browser to confirm end-to-end (hard reload → no PIN, zero 401s, pollers
   live; then simulate auth loss → banner + pause → restore → auto-resume).
4. **ntfy.sh external outage** — monitor for recovery; consider a backup
   notification channel if it persists.
5. **McCoy Ollama context/concurrency root cause** — filed as
   `HM-MCCOY-OLLAMA-CONTEXT-INVESTIGATION` in `docs/XO_BACKLOG.md`, not
   started (detection mechanism already shipped separately, this is just the
   underlying-cause dig).
6. **Cockpit double-interval-start** — not root-caused, deferred (see Part 2).
7. **Tomorrow's 06:40/06:50 riker_synthesis cadence** — genuinely can't be
   verified until it happens; today's regression (Part 5) makes this an
   important one to actually check tomorrow morning rather than assume fixed.
