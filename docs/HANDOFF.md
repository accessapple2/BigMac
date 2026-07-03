# OLLIETRADES HANDOFF v2

Tier 1 = repo-verified. Tier 2 = carried from prior context, confirm before acting.

---

## PROVING GROUND — ollie-auto TERMINATED (Admiral-confirmed, 2026-07-02)

`scripts/proving_ground_admiral.py --kill --agent ollie-auto --confirm` run
at 2026-07-03 00:03:59 (AZ evening 2026-07-02 17:03:59). Result:
`{"ok": true, "from_state": "kill_warning", "to_state": "killed", "agent":
"ollie-auto", "ntfy_sent": true}`. Verified in `data/proving_ground.db`:
`running_scorecard` id 67 (2026-07-02) now shows `exit_status='killed'`;
`state_transitions` id 27 logs the clean `kill_warning → killed` edge.
State is now terminal-sticky — the evaluator will no-op on this trial from
here forward (per `_evaluate_state`'s sticky-terminal-states logic).

**Rationale on record (Admiral)**: max_drawdown -43% vs the -15% guard,
breached 5+ consecutive tracked days, 24 days past the day-60 forced-eval
boundary, corroborated by a bench grade of D. This is on top of the
K1 (`dd_past_day60`) kill condition that had been correctly firing (if
silently, due to the now-fixed escalation bug — see the entry above) since
trial day 61.

**Not touched by this action**: `ai_players.halt_mode` for `ollie-auto`
remains `exit_only`, unchanged (it's been that way since the unrelated
2026-06-19 Door-1 cut) — the Proving Ground kill is a trial-scorecard
decision, not itself a trading-permission change. `scripts/
proving_ground_admiral.py` doesn't touch `ai_players` at all; if a
different halt posture is wanted following this termination, that's a
separate, not-yet-requested action.

---

## PROVING GROUND — kill_warning escalation bug found + fixed (2026-07-02, `962ab3d`)

**What was actually happening** (root-caused via `data/proving_ground.db`,
not assumed): `ollie-auto`'s Proving Ground trial hit a real kill condition
(`dd_past_day60` — max_drawdown -43.03%, guard is -15%) on trial day 61
(2026-06-09). It has correctly remained in `kill_warning` every day since —
the underlying scorecard data was never wrong. But `ship_kill_evaluator`'s
`prev_state` read `history[0]`, which on every subsequent run IS today's own
just-inserted row (schema default `exit_status='pending'`, not yet
classified) — so every single day looked like a **fresh** "just transitioned
to kill_warning" event. Confirmed via `state_transitions`: **26 consecutive
identical transition rows**, 2026-05-26 through 2026-07-02, none of which
were actually new after the first. No mechanism could ever recognize "this
has been going on for N days," so no escalation was possible — not because
escalation logic was missing, but because the day-counting it would need to
run on was structurally broken.

The separate, unconditional "Day 60+ FORCED EVALUATION" daily nudge (a
different code path) genuinely was firing every day — it was just buried
under 26 duplicate "NEW transition!" alerts that each implied urgency
hadn't been building, exactly the shape of alert fatigue that gets
normalized and ignored.

**Fixed**: `prev_state` now excludes today's own unclassified row. Added
real escalation on top (was requested, and only possible once day-counting
actually works): `ESCALATION_DAYS=5` consecutive genuine kill_warning days
fires a `P_MAX` alert distinct from the routine nudge, repeating every
`ESCALATION_REPEAT_DAYS=5` thereafter, plus auto-halts new entries for the
trial agent (`halt_mode → exit_only`) if not already halted — the terminal
ship/kill decision stays a manual `scripts/proving_ground_admiral.py
--confirm` call either way. Verified this is currently a no-op against
production (`ollie-auto` has been `exit_only` since the unrelated
2026-06-19 Door-1 cut) via a dry-run against a **copy** of the real DB,
all side effects (NTFY, trader.db writes) mocked — zero live impact from
the verification itself.

18/18 tests pass (10 pre-existing `_evaluate_state` tests untouched, 8 new:
pure-function coverage for the day-counting helper + 4 integration tests
against an in-memory DB covering the exact regression, the "today's row
still gets classified" edge case, escalation-at-threshold-not-before, and
the already-halted no-op path).

**Not yet run**: the actual daily cron invocation of `ship_kill_evaluator`
already fired today (20:18, with the old buggy code — that's where today's
26th duplicate transition row came from). The fix takes effect on the next
scheduled run, not retroactively on today's already-logged row.

**Open decision, not yet executed**: `scripts/proving_ground_admiral.py
--kill --agent ollie-auto --confirm` — this is the terminal, sticky,
no-undo action that actually ends the trial. Flagged back to the Admiral
for explicit confirmation given the ambiguity in how it was raised and the
irreversibility of the action; not run.

---

## DIRECTIVE B — BACKLOG CLEARANCE (2026-07-02, Admiral sign-off) — ALL 5 ITEMS LANDED

1. **HM-ORPHAN-SEATS** — investigated fully, **no DB write needed**. 13 true
   orphan seats (tag-normalized diff vs live olliemax models — not 11, that
   figure was stale but structurally right), all already correctly
   retire-flagged with specific deliberate reasons (11 are intentional
   "bakeoff clone — audit trail only" records; 2 are documented dormant
   cleanups from May), all with real historical data across many tables
   (retire-flag was already the right call, already made). Verified zero
   orphan names visible in bridge-v2's rendered page; bridge/signal-center
   agent counts confirmed byte-identical (15/9/55/79) — signal-center
   proxies bridge's own endpoint, no drift possible by construction.

2. **V1 request consolidation** — Stage 1 (`docs/AUDIT_v1_fetches.md`,
   `349c5b0`): live network capture found 196 requests/138 distinct
   endpoints in a 60s window, not the reported 557 — that figure is
   accumulated repeat-poll volume over a longer session, not distinct call
   sites. Stage 2 (`8065dfe`): found **5** independent alert-polling loops
   (not the 4 assumed — a 5th, in the external `alert_speaker.js`, was only
   found by grepping that file separately) all hitting overlapping "alerts"
   data on 5 different uncoordinated timers; two of the original 4 backend
   endpoints (`/api/flash-alerts/active` and `/latest`) turned out to be
   the exact same underlying query called twice. Added one `/api/alerts/poll`
   aggregate endpoint + a 15s-TTL shared fetch cache; all 5 pollers now use
   it, keeping every poller's own interval and UI/safety logic untouched.
   **Verified live**: flash-alerts/active (was 8/55s) and /latest (was 6/55s)
   and dynamic-alerts/active (was 5/55s) all fully eliminated as independent
   calls; alerts/recent partially deduplicated (10s poller cadence exceeds
   the 15s cache TTL — a tuning nuance). Alert banner confirmed still
   rendering correctly (live MACD alert observed) — no UI regression. Stage 3
   (lazy-load ~40+ boot-time fetches for non-visible sections) **deferred**,
   clearly scoped in the audit doc — larger, more error-prone lift, 3 more
   directive items remained.

3. **SwingDesk header stats** (`626c10e`) — investigated what real data
   actually exists before wiring anything. `tb-daypnl` ← `risk-gate`'s
   already-computed real `daily_loss` (realized P&L from `daily_stats`),
   just never applied to that element. `tb-openrisk` ← new
   `SUM(risk_dollars)` over open trades — the per-trade value already
   existed (computed at plan time), just never aggregated; added the
   aggregate to the backend. `tb-portfolio` ← explicit **"n/a"** with a
   tooltip — genuinely no honest source exists (confirmed `alpaca_wired:
   False` on risk-gate itself, no local ledger tracks account/equity value
   anywhere in this backend). Verified live: all three render correctly
   given the current 0-open-positions state; zero console errors.

4. **Cloudflared hygiene**:
   - 4a (upgrade, executed + verified): 2026.6.0 → 2026.6.1 via brew.
     Baseline-checked all 5 hostnames before touching anything, killed the
     old process, restarted via the established `cloudflared_reboot_start.sh`
     (not a hand-rolled restart). **Unexplained but benign**: the process
     came back with a new PID faster than my own script's invocation (its
     own log shows "already running, skipping start" at the same timestamp)
     — no watchdog cron found that should explain this; flagging honestly
     rather than claiming a mechanism I didn't verify. Outcome confirmed
     regardless: `cloudflared tunnel info` shows the connector running
     **2026.6.1**, all 5 hostnames matched their pre-upgrade baseline codes
     exactly after.
   - 4b (reauth prep, `ff9ae8d`) — draft only, did not run
     `cloudflared tunnel login` myself (needs the Admiral's browser
     session). Includes a dry-run-against-a-throwaway-hostname verification
     step designed specifically to catch the same nested-hostname misfire
     that caused the accessapple.com DNS incident. Verified the doc's
     load-bearing safety claim before writing it: `cert.pem` and the
     tunnel's credentials file are confirmed-separate files on disk, so
     cert replacement carries zero risk to the live running tunnel.

5. **accessapple.com email posture** (`03838ac`) — draft only, zone my
   token can't reach; investigated via public `dig` only. **Corrected the
   directive's own premise**: DMARC already exists and is already correctly
   configured (`p=none` + `rua` via Cloudflare's own DMARC management) —
   not something to draft fresh; documented the quarantine ramp-up path
   instead. Real finding: DKIM CNAMEs are completely missing while SPF
   itself is syntactically clean (single lookup, hard-fail, no duplicate
   records) — flagged as the more likely explanation for an observed "Fail"
   than anything wrong with SPF, since DMARC only needs one of SPF/DKIM
   aligned. Origin-IP-exposure: could not find the specific leaking record
   from outside the zone (~20 subdomain guesses, no hits) — gave exact
   methodology for the Admiral to find it from their own direct zone view
   rather than guessing blind.

All 5 commits pushed and hash-verified individually per the standing
auto-push policy: `349c5b0`, `8065dfe`, `f7d50ae`, `626c10e`, `ff9ae8d`,
`03838ac` (6 commits — audit doc got a follow-up update after Stage 2).

### PARKED by Admiral decision (2026-07-02) — not open items, do not re-raise unprompted
- **DKIM setup + origin-IP-exposure review on accessapple.com** — both
  drafted (`docs/DRAFT_accessapple_email.md`) but explicitly parked:
  dormant email domain, no felt impact. DMARC stays at `p=none` (already
  correctly configured, no action pending). Revisit only if the domain
  becomes active for real mail flow or if asked directly.
- **cloudflared re-auth** — draft ready (`docs/DRAFT_cloudflared_reauth.md`),
  parked until the next time infra work touches this box. Not blocking
  anything currently live; the 5 real hostnames are unaffected regardless
  of cert.pem's account scoping (confirmed: cert.pem and the tunnel's
  credentials file are separate, data-plane traffic doesn't depend on
  cert.pem at all).

**No other open items as of this writing** — all INFRA sign-offs and all 5
Directive B items are closed or explicitly parked, not silently dropped.

---

## TIER 1 — VERIFIED THIS SESSION (confirmed against repo/live origin)

### Repo root
`/Users/bigmac/autonomous-trader` — confirmed via `pwd`, twice, in this session. (Not `~/ollietrades` or `~/OllieTrades` — those are legacy pre-rename paths still present on disk but not the live repo.)

### Cloudflare tunnel — ingress → service map
Tunnel `dee0002c-c451-4919-8b16-d649ad19d029`, config at `~/.cloudflared/config.yml`:

| Hostname | Service |
|---|---|
| bridge.ollietrades.com | http://localhost:8080 |
| signal.ollietrades.com | http://localhost:9000 |
| swingdesk.ollietrades.com | http://localhost:8889 |
| tour.ollietrades.com | http://localhost:8088 |
| (catch-all) | http_status:404 |

**Apex decision:** `ollietrades.com`/`www` are NOT in this ingress map — confirmed by direct read of the config. The apex-DNS fix was applied as a Cloudflare edge Redirect Rule (2 proxied DNS records + 1 rule), not a tunnel-ingress change.

### AUTHORITATIVE — local config.yml matches remote (RECONCILED)
Superseding the earlier "5 hostnames" note: XO read the Zero Trust dashboard directly and confirmed the tunnel `trademinds` has exactly the **4** routes listed above — same as local `config.yml`. `otasty` is gone (its DNS record was removed; not a tunnel route either before or after). The earlier "5 hostnames / otasty in the tunnel" note was stale, from before otasty's removal — corrected.

**`arena.ollietrades.com` — claim independently checked, does NOT hold up.** Reported as "resolves and 301s to signal.ollietrades.com, verified in-browser." Checked directly from this shell: `dig arena.ollietrades.com` returns **NXDOMAIN** (not a cached/stale answer — fresh query, full response confirmed `status: NXDOMAIN`). There is currently no DNS record for this hostname at all, proxied or otherwise — it cannot be serving anything right now. Not reconciled; flagging the direct contradiction rather than recording both claims as if compatible. If this was seen genuinely working in-browser, it may have been transient, cached (browser/OS DNS cache showing a stale resolution), or observed on a different session/state than what's live now.

**How the remote/local match was established:** the local `cloudflared tunnel list`/`tunnel info` CLI commands still don't expose ingress rules directly in this version, and my own browser access to the Cloudflare dashboard remained blocked all session (dashboard zone pages hang indefinitely — confirmed by both of us independently). This reconciliation rests on XO's direct dashboard read, not something I verified myself via an independent channel.

### DNS apex + redirect — APPLIED, independently verified live
Applied directly in the Cloudflare dashboard (browser automation on that page was non-functional all session — every attempt hung on a stuck "page never idle" state). I verified the result myself from this shell, not just from the report:

```
$ dig +short ollietrades.com A          → 172.67.208.56, 104.21.45.31 (Cloudflare proxy)
$ dig +short www.ollietrades.com        → 104.21.45.31, 172.67.208.56 (Cloudflare proxy)
$ curl -I http://ollietrades.com/       → 301, location: https://bridge.ollietrades.com/
$ curl -I https://www.ollietrades.com/  → 301, location: https://bridge.ollietrades.com/
$ curl -I ".../foo?bar=1&baz=2"         → 301, location: https://bridge.ollietrades.com/foo
```

Apex and www both resolve and both 301 to bridge with real `cf-ray` headers (not cached/local) — genuinely live.

**Gap found, not in the original report:** path is preserved but **query string is dropped** — `?bar=1&baz=2` did not survive the redirect, despite "preserve query string ON" being part of the original spec. Worth a follow-up if any inbound links to the apex ever carry query params (e.g. UTM tags, referral codes).

**Still blocked, both of us confirm it independently:** the zone dashboard (`dash.cloudflare.com` → ollietrades.com pages) hangs on the loading spinner indefinitely for both me and the Admiral — Zero Trust pages render, zone pages don't. Drafted (not applied) the Cloudflare API alternative:
1. `GET /zones/{zone_id}/rulesets` → find the ruleset with `phase: http_request_dynamic_redirect`
2. `GET /zones/{zone_id}/rulesets/{ruleset_id}` → get the exact current rule JSON
3. `PUT /zones/{zone_id}/rulesets/{ruleset_id}` → resend the full rules array with the target rule's `preserve_query_string` flipped true (whole-ruleset PUT — Cloudflare's Rulesets API doesn't document a single-rule PATCH)
4. Verify: `curl -I "http://ollietrades.com/foo?bar=1"` → location should end `?bar=1`

Full draft with exact request bodies at `/private/tmp/claude-501/-Users-bigmac/e4f1a08b-3760-4a9e-b859-43b284705dcd/scratchpad/cf_redirect_fix_draft.md` (session-scoped scratch path — copy it somewhere durable if it needs to survive past this session). Needs an API token scoped to Zone Rulesets edit, created by the Admiral (My Profile → API Tokens may load where zone pages don't). **Not applied** — held for sign-off per instruction, and the exact JSON field nesting for `preserve_query_string` should be confirmed against the real GET response (step 2) before constructing the PUT body, not blind-copied from the draft.

### Origin services — live, confirmed via `lsof`
- `:8080` — PID 72842, `main.py` (trader/dashboard, `.venv` Python 3.14)
- `:9000` — PID 60260, `signal-center/server.py` (separate `venv` Python 3.9)
- `:8889` — PID 94991, SwingDesk (`swingdesk/backend.py`) — restarted twice 2026-07-02 (first to clear the fd exhaustion, second after the schema+leak-guard fix), see incident below
- `:8088` — PID 417, `tour_api.py` — healthy, unaffected by the SwingDesk incident

### INCIDENT — SwingDesk 502, 2026-07-02 (RESOLVED)
`swingdesk.ollietrades.com` was returning 502 Bad Gateway. Diagnosed and fixed:

**Symptom:** the old process (PID 287, uptime 1d+) was still alive and still `LISTEN`ing on `:8889` per `lsof` — not crashed. But a direct `curl -v http://127.0.0.1:8889/` showed the TCP handshake completing, the HTTP request being sent, then `Recv failure: Connection reset by peer` — the OS-level socket accepted the connection but the application couldn't complete it.

**Root cause:** `logs/otasty_error.log` showed repeated `OSError: [Errno 24] Too many open files` (22 occurrences) from `asyncio`'s `socket.accept()`. `lsof -p 287` showed 303 open file descriptors against a 256 soft `maxfiles` limit. `logs/otasty.log` (stdout, not stderr — the `[otasty-shadow-scheduler]` prints go here) showed the actual leak source: **723** repeated occurrences of `OperationalError: table swingdesk_shadow_trades has no column named broker_order_id`.

**Root cause, traced to exact source (this round):** `run_loop_b()` (called every 5 min during RTH via `_shadow_cycle`) unconditionally `INSERT`s a `broker_order_id`/`live_status` pair for **every** gate-passing candidate — not gated by `LIVE_EXECUTION_ENABLED`, only the actual order-submission call is. `_ensure_shadow_schema()` (the idempotent migration helper, called at the top of every loop function) never included these two columns in its add-list — only `refused_reason`/`exit_reason`/`would_have_exit_at`/`exit_value`/`exit_pnl`. So every candidate, every cycle, hit the missing-column error. Confirmed live: `swingdesk_ivr` had **1,049** gate-passing candidates on 2026-07-02 alone — matching the high error count far better than the RTH-cycle cadence alone would. Worse: `run_loop_b()` opens its `sqlite3.connect()` with no `try/finally` — when the INSERT throws, the function exits without ever reaching its `conn.close()`, leaking one fd per candidate. The same unguarded-connection pattern exists in `run_loop_a`, `run_loop_c`, `run_loop_e`, and `run_loop_reconcile` (dormant currently, since `LIVE_EXECUTION_ENABLED=False` short-circuits it before opening a connection — but equally vulnerable to any *other* future exception).

**Fix applied (this round, Admiral-approved, backup taken first):**
1. `cp swingdesk.db backups/swingdesk_2026-07-02_104242_pre-broker_order_id-migration.db`
2. Added `broker_order_id TEXT` and `live_status TEXT` to `_ensure_shadow_schema()`'s idempotent column-add list (`swingdesk/shadow_autopilot.py`) — confirmed present in the live DB via `PRAGMA table_info` after restart.
3. Wrapped all 5 loop functions' connection lifecycle in `try/finally: conn.close()` (or `conn = None` + `finally: if conn is not None: conn.close()` for `run_loop_reconcile`, which already had an outer try/except) — the general "FD-leak guard," not just a point-fix for `run_loop_b`.
4. Ran the existing test suite (`venv/bin/python -m pytest tests/test_otasty_*.py`) — **all 18 tests pass**, including the safety-invariant tests (`test_flag_pinned_false`, `test_account_pinned_to_pa3yvdtuh5cb`, `test_no_ungated_submit`) — the refactor changed only resource cleanup, not behavior.
5. Restarted (`bash scripts/swingdesk_restart.sh`, new PID 94991/95021). Verified: local health check 200, live via tunnel 302 (same pattern as pre-incident), fresh FD count 52-59 (vs the 303 that caused the incident), **zero** new `broker_order_id` occurrences in `otasty.log` after the restart marker (checked precisely: `grep -c` on everything after the last "Shadow autopilot scheduler: started" line = 0, vs 723 before).

**Honest gap in verification:** I could not directly observe a full successful end-to-end write (the `swingdesk_shadow_trades` table still shows 0 total rows as of this writing, ~12 min post-restart, despite 1,049 real candidates being available and the scheduler having had time for 2+ ticks). One direct test hit `database is locked` against the live process — consistent with the live process actively writing at that moment, not with the bug recurring (different error entirely: lock contention, not a schema error) — but I did not manage to catch a clean successful write myself before running out of reasonable time to keep polling. The schema fix and leak guards are solidly verified (direct schema check, all tests passing, zero recurrence of the specific error); the *scheduler actually completing a cycle and persisting a row* is the one thing I'm flagging as not directly witnessed. Worth a spot-check in an hour.

**`tour` (:8088) does not share this fate** — checked directly: only 51 open FDs (vs SwingDesk's 303) despite similar ~1-day uptime, `logs/tour_api.log` has zero occurrences of either error pattern, and it responds locally in ~1ms. Different codebase (`tour_api.py`, standalone), no shared scheduler.

**SwingDesk root `/` returning `{"detail":"Not Found"}` — NOT a regression.** Checked `swingdesk/backend.py` source directly: there has never been an `@app.get("/")` route — only `/api/*` routes and a `StaticFiles` mount at `/static` (not `/`). `http://localhost:8889/static/index.html` → 200, real UI fully functional. The bare-root 404 is FastAPI's default catch-all for an undefined route, unchanged by this restart or any restart before it.

### DB inventory (repo-scoped `find`)
- **Canonical / sacred (never-delete per CLAUDE.md):** `data/trader.db`, `data/arena.db`, `signal-center/signals.db`
- **Secondary feature DBs:** `data/flow_gex.db`, `data/deep_scan.db`, `data/ghost_trades.db`, `data/backtest_results.db`, `data/backtest.db`, `data/alpha_signals.db`, `data/proving_ground.db`, `data/uhura_research.db`, `signal-center/signals_archive.db`
- **Stale/archival (NOT live):** root-level `trader.db`, `autonomous_trader.db`, `swingdesk.db`, everything under `backups/`

### Shipped fixes this session — commit hashes (branch `exec-pipeline`)
1. `75a3684` — canonical fleet P&L source of truth across bridge v1/v2/signal-center
2. `5cfdc97` — Fear & Greed null-guard (Ship's Log rendering unavailable as 0)
3. `2cf39da` — Battle Tab Trade Desk avg-cost field mapping
4. `a3df4e6` — leaderboard equity anchor hardcoded to season=5
5. `c23312b` — Riker confidence-scale bug + empty SCREENER intelligence rows
6. `8c85b78` — stop persisting risk_radar cold-start placeholder as a signal
7. `532573f` — predictions_snapshot skips incomplete rows instead of persisting them
8. `9fda946` — removed leftover LiveChart MutationObserver debug trap
9. `a6bfec6` — surface failed admin actions (Matrix tab: ntfy + red styling + honest status)
10. `511246a` — removed duplicate fetch calls on bridge-v2 initial load
11. `5d67c14` — GEX endpoints hang on cold cache — bound + serve-stale-while-revalidate
12. `13f0f17` — Chart.js 3.9.1 → 4.4.1
13. `ac92b38` — Fleet Report Card collapses dormant agents, sorts by P&L
14. `e23b3e0` — empty-state polish for Phaser-Lock (skeleton loader)
15. `4c19eb9` — v1/v2 P&L divergence fix + gex_all() cache-miss fix + StructureFix noise silenced
16. `0e1d485` — bridge-v2 is now the root view, v1 demoted to /classic
17. `3a6f102` — relocated 6 misplaced sections into `.main`, removed StructureFix shim entirely
18. `1d2bca2` — session handoff doc (DNS+redirect live, otasty removed)
19. `da7bb10` — SwingDesk 502 incident writeup, push-truth correction
20. `8ed5b17` — SwingDesk `broker_order_id` schema fix + fd-leak guard across all loop functions
21. `4965eaf` — bridge: `wbPortfolioSectorRow`/`wbPortfolioValueCard`/`wbSectorAllocationCard` bled into every section (Charts, Watchlist, confirmed) — added to the existing `_dashOrphans` mechanism in `showSection()` instead of a third one-off CSS patch. Also confirmed: `/static/index.html` (raw StaticFiles mount, no cache headers) can serve a stale copy after edits; `/classic` (explicit route, no-cache headers) is always fresh — prefer `/classic` for testing/bookmarks.
22. `20aab04` — drafted (not applied) origin-stability cron plan + manifest CF Access bypass plan, for sign-off
23. `cab3a60` — **SwingDesk domain mix-up fix.** Root cause of the reported "chart toast / watchlist stuck LOADING / all header stats dashed" was `swingdesk/index.html:405` hardcoding `const API = 'http://localhost:8889'` — every fetch on the page (watchlist, candles, scan, signals, positions, journal, stats, health) resolved against the *viewer's own machine* when loaded via the public domain, not the actual origin. Changed to `''` (same-origin relative). Also added an explicit `/` route to `swingdesk/backend.py` (there was none — bare FastAPI 404 before) with the same no-cache headers used on bridge's `/classic`. Verified live: watchlist shows real prices, chart renders, zero console errors. Note: `tb-portfolio`/`tb-daypnl`/`tb-openrisk` still show dashes — confirmed via grep these have **no backing JS anywhere in the file**, never implemented, not something this fix could touch (separate, pre-existing gap). `tb-winrate`/`tb-rr` now correctly fetch and legitimately show "—" given zero closed trades.
24. `22f90f7` — **origin-stability, cron-based, applied and verified.** New `scripts/origin_healthcheck.sh` (HTTP-level checks — `/api/status`, `/api/health` ×2 — not pgrep, specifically because the SwingDesk incident above was wedged-but-alive and a liveness check would have missed it) + new `scripts/signal_center_restart.sh` (didn't exist before, only the `@reboot`-only `signal_center_reboot_start.sh`). Cron: `*/5 * * * *`, installed live, coexists with the existing `HM-TRADER-KEEPALIVE`. **Verified for real, not just installed:** killed signal-center with `kill -9`, ran the healthcheck script manually to simulate the next tick — it detected the failure, restarted the process (new PID), confirmed responding `200` within seconds.
25. `f2d929c` — **status.ollietrades.com, live.** New `scripts/status_page.py` — minimal, no-auth, read-only, checks bigmac/Ollie Max/trader/tunnel, serves both HTML and `/api/status` JSON on `:8090`. Route added via the Admiral's own Zero Trust dashboard action (DNS auto-created that way, not by me — see the accessapple.com mistake below). Verified live: `https://status.ollietrades.com/` → `200`, no CF Access gate, all 4 checks report UP, JSON endpoint returns real data. Reboot persistence added (`scripts/status_page_reboot_start.sh`, `@reboot` cron).

Plus (not a git commit — crontab, verified live): Riker synthesis re-homed to `*/10 * * * *` cron (`engine/riker_synthesis.py`); manually run once, confirmed a fresh `rikers_log` row written 2026-07-02 (first update since 2026-05-23). Origin-healthcheck cron (`*/5 * * * *`) and status-page `@reboot` entry, both above, are likewise crontab-only.

### Push state
**Pushed to `origin/exec-pipeline`.** As of `f2d929c`, `git rev-parse HEAD` == `git rev-parse origin/exec-pipeline` — confirmed after every commit this session via explicit inline verification (not assumed). Standing policy as of 2026-07-02: **auto-push after verified batches** (Admiral-confirmed, superseding the earlier no-auto-push default) — each commit above was pushed and hash-verified individually, not batched into one bulk push.

### Open INFRA sign-offs (Admiral gate) — 4 of 4 DONE
1. ~~DNS apex record + redirect~~ — **DONE**, verified live, including the query-string sub-fix. Token provided by Admiral, but I could not use it directly (writing it into a Bash `export` was blocked by this session's own credential-leakage classifier — persists in shell history/tool logs); handed off the exact ready-to-paste `curl` sequence with `$CF_TOKEN` as a literal placeholder, Admiral ran it themselves. **Verified live, this token needed no use on my end**: `curl -I "http://ollietrades.com/foo?bar=1&baz=2"` → `location: https://bridge.ollietrades.com/foo?bar=1&baz=2` — full query string survives. Base apex/www redirects (no query) rechecked, no regression: both still `301` → `https://bridge.ollietrades.com/`.
2. ~~Origin stability~~ — **DONE**, applied and verified live (`22f90f7`, see above). Cron-based per repo doctrine, not systemd/launchd — this was explicitly re-requested as systemd once mid-session (systemd doesn't exist on this macOS box at all — flagged and corrected before executing).
3. ~~manifest.json CF Access bypass~~ — **DONE.** Applied by Admiral directly in the dashboard (not by me — I don't have Zero Trust Access-policy write access via my current token, scoped to Zone Rulesets only). Verified independently from an unauthenticated `curl` (no CF Access session cookie at all): `https://bridge.ollietrades.com/static/manifest.json` → `200`, real manifest JSON body. Confirmed scope is exact-path, not widened — three sibling checks (`/`, `/static/index.html`, `/static/app.js`) all still return `302` (CF Access login), so the email-allowlist lockdown on the rest of the app is intact.
4. ~~status.ollietrades.com~~ — **DONE**, live and verified (`f2d929c`, see above).

**All 4 INFRA sign-off items closed as of this writing.**

### INCIDENT — erroneous DNS record on a different zone — RESOLVED, no action needed
While attempting the status-page DNS route myself (before the Admiral did it via dashboard instead), `cloudflared tunnel route dns trademinds status.ollietrades.com` appeared to create `status.ollietrades.com.accessapple.com` — at the time, `dig` showed it resolving to real Cloudflare proxy IPs. The cloudflared CLI's login cert on this box had appeared scoped to a different zone/account (`accessapple.com`, matching the GitHub org `accessapple2` this repo pushes to) than `ollietrades.com`'s zone ("Bonnie's Account") — passing a hostname that doesn't end in `.accessapple.com` caused it to nest rather than error.

**Resolved without any deletion needed.** Admiral inspected Bonnie's `accessapple.com` zone directly in the dashboard: 18 records total, no `status.*` record anywhere. Re-ran `dig status.ollietrades.com.accessapple.com` myself — **NXDOMAIN**, confirmed via full response (`;; ->>HEADER<<- opcode: QUERY, status: NXDOMAIN`), not just an empty/cached result. The two independent checks agree: nothing to delete. Most likely explanation: a transient edge-propagation artifact that never became a persisted zone record, or `cloudflared`'s `route dns` call didn't actually commit before erroring — either way, confirmed gone now by both the account owner's direct zone read and a fresh DNS query.

**Still worth doing, lower urgency:** the cloudflared CLI cert on this box (`~/.cloudflared/cert.pem`) is seemingly scoped to some account other than Bonnie's `ollietrades.com` zone (unclear which one, since the erroneous record's target zone doesn't actually exist under that name in Bonnie's account — the exact account it's logged into is still unidentified). Since that record never persisted, there's no confirmed live exposure, but the cert being logged into an unexpected account means any *future* `cloudflared tunnel route dns` call from this box could misfire the same way. Worth a `cloudflared tunnel login` re-auth against Bonnie's account next time infra work touches this box, so future CLI-driven DNS operations target the right zone. Not urgent — no live route depends on this CLI path today (all 4+1 existing hostnames were set up via dashboard).

### O-Tasty — verified against repo source
The Admiral identified `otasty` as an internal codename for an in-house tastytrade-style options engine inside SwingDesk. Confirmed directly in the repo (not just from the report):
- `swingdesk/options_engine.py`, `swingdesk/shadow_autopilot.py`, `swingdesk/scanner.py`, `swingdesk/backend.py` all reference it; also `tests/test_otasty_shadow_invariants.py`, `tests/test_otasty_invariant_tamper.py`
- `swingdesk/shadow_autopilot.py:237` — `LIVE_EXECUTION_ENABLED = False   # PA3YVDTUH5CB paper only; never fleet account; never real money` — literal source, confirms both the isolated Alpaca paper account ID and the execution-disabled default
- Reached via SwingDesk's Test Kitchen tab, no dedicated subdomain needed — consistent with `otasty.ollietrades.com` being an orphaned/unused DNS record rather than a real routing target

### Cloudflare account details (Admiral-reported from the dashboard, not independently checkable by me)
- Zone `ollietrades.com`, Zone ID `0efaf9b4eba9ba61e16c4eec9e96740f`, Account ID `4925885a5424f7948d21d5e6ffaad234` ("Bonnie's Account"), Free plan, registrar Cloudflare, expires 2027-04-08
- **"Agent Lee" / "Enable Agent Lee access"** — flagged by me as suspicious when it first appeared (I hadn't seen it anywhere in this project). The Admiral identified it as Cloudflare's own first-party in-dashboard "Ask AI" feature, which auto-generates a read-only token if enabled, and left it **disabled**. I have no way to independently confirm this from outside the dashboard — noting it here as the Admiral's direct account-level observation, not something I verified myself. If anyone revisits this, worth a second look before trusting it's benign purely on the strength of this note.

### Ticket codes confirmed against CLAUDE.md
- `HM-ORPHAN-SEATS` — CLAUDE.md:312 ("11 `ai_players` seats referencing Ollama models absent from olliemax")
- `HM-SHELLY-WATCHDOG` — CLAUDE.md:225-226, 253 (Shelly plug watchdog scope, sketched not built, post-trip)

---

## TIER 2 — CARRIED OVER, NOT VERIFIED THIS SESSION (from prior/XO standing context; next session must confirm)

- **Ollie Max hardware:** MSI Aegis ZS2 / Ryzen 9 9900X / RTX 5080 16GB / Ubuntu 26.04 — not checked against any live system this session (only its Ollama endpoint, `192.168.1.168:11434`, is referenced in repo config/CLAUDE.md)
- **Machine roster:** Ollie Box (.166, RETIRED), laptop (.177), Bonnie's Mini = Minisforum — none of these appear anywhere in this session's tool output or in CLAUDE.md
- **bigmac IP** — provided as 192.168.1.248; CLAUDE.md's Shelly plug table instead lists bigmac at **192.168.1.245**. Discrepancy, not reconciled — confirm actual IP before relying on either.
- **Ticket backlog not found in repo:** `HM-DAX`, `HM-VOTE-AUDIT`, `HM-PY-CONSOLIDATE`, `HM-LESSON-SHADOW`, `HM-SCORECAP-REVISIT`, `HM-WAL-ROOTCAUSE` — zero grep hits in CLAUDE.md or docs/ this session
- **cloudflared launch mechanism** — carried-over text described it as "a system LaunchDaemon"; CLAUDE.md instead documents it as an `@reboot` cron entry (`scripts/cloudflared_reboot_start.sh`), specifically because LaunchAgent/LaunchDaemon bootstrap fails on this box over SSH-only sessions. Treat the cron-based description as the confirmed one; the LaunchDaemon framing is superseded/incorrect per repo doctrine.
- **Broker posture / naming conventions:** RULE #1 (Schwab read-only, no order path) and Alpaca-paper-only ARE backed by CLAUDE.md and should be treated as Tier 1 in spirit — restated here only because the rest of that section's phrasing ("Naming: Star Trek — Admiral/XO/Scotty") wasn't independently verified; this session's own directive used Admiral + Scotty (Chief Engineer), no "XO" role appeared.
