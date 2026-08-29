# Relay — 2026-08-29 — status.ollietrades.com freeze investigation + fix

## Directive
Investigate why status.ollietrades.com's "Last checked" froze 2026-08-29
04:54:50 UTC (Fri 21:54:50 local) → 16:25:42 UTC (Sat 09:25:42 local),
~11.5h. Verification first, no changes until root cause named.

## Investigation (all steps run, all findings verified live — nothing guessed)

**1. What drives the checker.** `com.trademinds.statuspage`
(`/Library/LaunchDaemons/com.trademinds.statuspage.plist`) — a system
LaunchDaemon (`UserName=bigmac`, `KeepAlive=true`, `RunAtLoad=true`),
runs `scripts/status_page.py` on `:8090`, tunneled publicly by
cloudflared. **Not** a `com.steve.*` agent — those are
`com.steve.gpusweep`/`com.steve.wootsniper`, an unrelated GPU-sniper
stack. The Friday 429-remediation relay
(`relay_2026-08-28_429-remediation-A-and-D.md`) explicitly names its
scope fence as *excluding* the gpusweep/woot stack — i.e. the fence
protected those two daemons from being touched, and never referenced
`com.trademinds.statuspage` at all. **No causal path from the fence to
this outage.**

**2. Sleep/reboot ruled out.** `last reboot`: single boot at
`Fri Aug 28 20:08` (prior boot Thu 27 10:42 — one continuous uptime
spanning the entire outage window and beyond). `pmset -g log | grep -E
"Sleep|Wake"` for the window: zero genuine Sleep/Wake domain
transitions — every match was `PreventUserIdleSystemSleep` assertion
noise (substring collision on "Sleep"), not an actual state change.
`ps -p 310` confirmed the SAME PID had been running continuously since
that Fri 20:08:28 boot, straight through the entire outage and past its
resolution — the process itself never crashed or restarted.

**3. Checker's own logs.** Both `logs/status_page.log`
(StandardOutPath) and `logs/status_page_error.log`
(StandardErrorPath) are 0 bytes, untouched since July — the script
never logged anything, ever. Zero internal observability by design/
omission; contributed nothing to the investigation but is itself a
finding (see fix below).

**4. Lazy-refresh test.** Read the full source first —
`_build_status()` runs synchronously inside `do_GET()`, computing
`checked_at` fresh via `time.strftime()` on every request. No
background thread, no scheduled loop, no independent state of any kind
existed before this fix. Confirmed live rather than waiting the full
20 min: a single `curl https://status.ollietrades.com` produced
`Last checked: 2026-08-29 16:33:13 UTC` against a request sent at
`16:33:12 UTC` — instant, exact match. `cf-cache-status: DYNAMIC` on
the response rules out Cloudflare edge-caching as an alternate
explanation (the "no-cache, no-store, must-revalidate" header is
actually honored, not silently overridden by CF).

## Root cause

**The status checker had no independent heartbeat.** "Last checked" was
purely "the timestamp of the last HTTP request this process received."
The Fri 21:54 → Sat 09:25 freeze was an ~11.5h gap with zero incoming
requests to the page — nobody had a browser tab open, and nothing was
polling it automatically. Not a crash. Not the scope fence. Not sleep
or reboot. It resumed at 09:25 only because *something* (almost
certainly a manual check) sent it a request, which instantly
recomputed a live timestamp — the identical mechanism my own
verification curl just exercised.

## Fix (commit `379f84c`)

- `scripts/status_page.py`: added a background daemon thread
  (`_write_heartbeat`) that runs the same three checks on a fixed 5-min
  cadence, independent of HTTP traffic, persisting the result to
  `data/.status_page_heartbeat.json`. The public page's on-request
  behavior is unchanged — this is a purely internal, local-disk write,
  not an HTTP write path, so it doesn't change the "no auth, no
  secrets, no write paths" public-safety posture documented at the top
  of the file.
- `scripts/hm_ops_sentinel.py`: new `check_status_page_heartbeat()`.
  RED_ALERT if the heartbeat **file** — deliberately not the live page,
  which would trivially look fresh the instant anything (including the
  sentinel's own probe) requests it, the exact self-defeating check
  that would have caught nothing — is >15 min stale. Runs **any hours**
  (not market-hours-gated like the GEX/fred_carts check), matching the
  directive: the status page's whole purpose is being checkable 24/7.
  WARNING (not RED_ALERT) if the file is simply missing, since that's
  routine for the first 5 min after any restart.
- `tests/test_status_page_heartbeat_sentinel.py`: 5 tests (fresh, stale,
  just-under-threshold, missing-file, corrupt-JSON) against a temp
  heartbeat file via `unittest.mock.patch`.

**Live-verified end to end:** killed the old process (PID 310, no sudo
needed — it runs as `bigmac`, and `KeepAlive=true` respawned it
automatically). New process (PID 73680) wrote a heartbeat within
seconds. `hm_ops_sentinel.py --dry-run` immediately read a fresh
0.2-min age with no alert. Public page still serves correctly
post-restart (`Last checked` updates live, `cf-cache-status: DYNAMIC`
unchanged).

## 429-remediation Friday open-item — confirmed executable, now closed

The Friday-night 429-remediation work (dated Aug 28 22:51 across all
touched files) genuinely did execute and was live on disk the entire
time — it just wasn't committed to git until this session's earlier
unpushed/unmerged-work audit surfaced it. Confirmed via direct source
inspection (not just timestamps) and now committed + pushed:

- `e7c3e7d` — Olliemax decommission, Ollama routing consolidated to
  local `com.ollama.serve` (config.py, engine/providers/ollama_provider.py,
  engine/archer/brain.py, engine/phaser_lock.py, engine/reveille.py,
  scripts/trader_restart.sh, scripts/uhura/parse_signals.py,
  scripts/witness_ab_scorer.py, scripts/offhost_backup.sh,
  scripts/status_page.py's Ollie Max tile removal, CLAUDE.md doctrine).
- `2611d25` — db_snapshot.sh disk-emergency gzip fix.
- `932defe` — DECOM-SILENCE ntfy suppression catch-up (disk_space_alert.sh,
  backup_freshness_check.sh) — the specific commit named in this
  ticket's own context.
- `ccc1c55` — the `_tiers_triggered` restart-persistence fix
  (engine/crew_scanner.py) — bundled into the same uncommitted batch by
  coincidental timestamp, unrelated to 429s specifically, but the same
  "real work, never committed" pattern.

All four were already running live (main.py imports from the
filesystem, not from git) before this session ever touched them —
committing them changed no runtime behavior, only closed the gap
between "done" and "preserved in version control." **The 429-remediation
open-item can be closed as executed.**
