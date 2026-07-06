# XO Backlog — USS TradeMinds
# Riker's Standing Work Queue
# Updated: 2026-05-31 (BACKLOG RECONCILE — filed the 2026-05-31 session: Holly A/B, External-Intel, learning loops)

> **Session resume:** full state in `docs/QUEUE_AUDIT_2026-05-29.md` (shipped / gated / carry-forward / out-of-scope). THE-ALL-OUT-PLAN-2026-05-28 is CLOSED.

---
## 🔵 HM-ERROR-FILTER-CONSOLIDATION — filed 2026-07-05 (Phase 3, not urgent)

`scripts/daily_report.py::get_error_summary()` filters real errors with a
blanket `"[LRS]" not in line` exclusion — this hides ANY error tagged `[LRS]`
wholesale, including genuine ones (e.g. the "ntfy failed: <urlopen error
[Errno 65] No route to host>" lines are `[LRS]`-tagged and real). Contrast
with `scripts/eod_report.py::genuine_error_count()` (HM-EOD-REPORT-2026-07-05),
which uses an explicit, named false-positive allowlist instead of a blanket
tag exclusion — narrower and auditable (each exclusion is a specific,
verified string, not an entire log-source tag).

**Fix (Phase 3, not urgent):** migrate `daily_report.py` to the same explicit-
allowlist approach — ideally both scripts call one shared filter function
instead of maintaining two divergent definitions of "real error." Until then,
`daily_report.py`'s 10 PM error count is not exactly the same as
`eod_report.py`'s 2 PM error count for the same day, and that's expected
(different filters), not a bug.

---
## 🔵 HM-OLLIE-MACHINE-KILLGATE — filed 2026-07-05 (HM-ROSTER-RATIONALIZE follow-up)

`ollie-machine` ("Ollie Machine", rule-based/convergence-2of4, `halt_mode='active'`)
surfaced in the 2026-07-05 roster audit with 0 trades ever since creation
(2026-06-01) — 5+ weeks silent. Friday's roster session had already classed
it **structural non-competing** (sim/tracking-style seat, never fighting for
one of the 8 capped active slots), so the audit did not recommend an
immediate cut — deliberate prior decisions get a dated re-evaluation, not a
silent reversal from a fresh sweep.

**Admiral decision (2026-07-05):** no cut tonight. Dated trigger instead:

> **Kill gate: 2026-07-24.** If `ollie-machine` has recorded zero trades (in
> `trades` AND `options_trades` — check both; see HM-SWEEP-SIGNALS-TABLE-
> BLIND-SPOT re: options/CSP agents being invisible to a trades-only sweep)
> by this date, halt proposal goes to the Admiral. If it has traded,
> re-assess on the merits same as any other candidate.

Not to be confused with the pre-existing 2026-07-24 G1-G4 Door-1 kill-gate
(`project_door1_kill_gate` memory) — that's a separate, fleet-wide gate.
This is a single-agent trigger scoped only to `ollie-machine`.

---
## 🔵 HM-SHADOW-PIPELINE-COST-AUDIT — filed 2026-07-05 (HM-ROSTER-RATIONALIZE follow-up)

Surfaced while checking `api_costs` for q-witness's paid-xAI spend: five ids
NOT in `ai_players` show real metered API cost, last 30 days —

| id                      | calls | cost    |
|-------------------------|-------|---------|
| wr-shadow-v1            | 3,048 | $22.10  |
| wr-witness              | 2,052 | $15.54  |
| wr-shadow-v7d           | 2,997 | $11.22  |
| ab-witness-deepseek-r1  |   238 |  $0.72  |
| ab-witness-gpt-oss      |   229 |  $0.66  |
| **total**               | 8,564 | ~$50.24 |

4-10x q-witness's own $2.76/30d, and none of it is a roster seat, so it fell
entirely outside the ai_players-scoped leaderboard.

**Open, not chased tonight (own session when scoped):**
1. What are these — a shadow/witness scoring pipeline distinct from
   q-witness's War Room voice? Where invoked from?
2. What consumes their output — does anything read wr_shadow/wr_witness/
   ab_witness_* results downstream, or do they write and nobody reads?
3. Pure observation (like existing W0 forward-scoring shadow patterns) or
   does anything act on them?
4. If nothing reads it: that's the real dead spend on this box — bigger in
   dollars than anything found in the roster proper.

---
## 🔵 HM-DESK-CHAIN-PROVENANCE — filed 2026-07-05 (HM-DECISION-DESK-MVP Phase 1 follow-up)

Two data-integrity anomalies surfaced building the Desk's chain view
(`GET /api/desk/chain/<signal_id>`, commit `9bb56e6`), not fixed — the Desk
guards against ever *displaying* a wrong fill (only surfaces a trade when
`trade.symbol == signal.symbol`), but the underlying data problems are real
and unaddressed:

1. **`trades.signal_id` mislinked on 72% of sampled rows** — 47 of 65 sampled
   links point to a DIFFERENT symbol than the signal they claim to belong to.
   Same theme as CLAUDE.md's existing "acted_by_fleet... retrospective join
   is a DEAD END" note — this is the `trades` side of the identical disease.
2. **`execution_status` essentially never set to `'EXECUTED'` on real fleet
   trades** — not a single signal with a genuinely-matched trade link (by
   symbol) has `execution_status` literally `'EXECUTED'`. The status value
   appears to just not get written correctly for real fleet trades, full stop.

**Why this matters beyond the Desk:** both anomalies bear directly on chain
provenance, and provenance is exactly what the 2026-07-24 kill-gate reads
(both the fleet-wide Door-1 G1-G4 gate and the single-agent
`HM-OLLIE-MACHINE-KILLGATE` above) to decide what counts as a real, gate-
grade trade. A gate that reads a mislinked or never-marked-executed chain
risks the same "0% EXECUTED, 72% wrong-symbol-linked" blind spot the Desk had
to explicitly guard around. Filed for its own session — not actioned here.

---
## 🔵 HM-CLAUDE-TRADER-GHOST-DEFAULT — filed 2026-07-05 (historical finding, HM-DECISION-DESK-MVP Phase 1)

`signal-center/server.py`'s `/api/signals/<id>/execute` — the manual "SEND
IT" button a human clicks in signal-center to execute a fleet signal — has
defaulted `player_id` to `'claude-trader'` since it was written. `'claude-trader'`
**has never existed as an `ai_players` row.** Net effect: every manual-execute
click through that endpoint, for its entire history, silently went nowhere —
the `UPDATE trade_signals SET status='EXECUTING'` claim would succeed, but
the downstream `engine.paper_trader.buy(player_id='claude-trader', ...)` call
had no real player to execute against. **0 historical executions ever** via
this path, confirmed during the Phase 1 build.

Fixed in `9bb56e6` (default changed to `desk-manual`, the new dedicated
paper-only identity) — noted here as its own line because this is a real
historical finding about how long a core manual-execute control has been
silently inert, not merely a line in a commit message. Anyone auditing past
"why didn't the desk ever fire" reports should know the answer was this,
not a signal-quality or gating problem.

---
## 🔵 HM-STATUS-PAGE-STALE-CACHE — filed 2026-07-05 (bigmac cold-start test)

Surfaced during the bigmac cold-start test (`docs/REBOOT_POSTURE.md`):
`status.ollietrades.com` serves a Cloudflare-cached page with a stale "Last
checked" timestamp even though the underlying `scripts/status_page.py`
service is live and healthy.

**Correction before anyone re-attempts the obvious fix:** `status_page.py`
(both the `/api/status` JSON path and the main HTML path, `scripts/status_page.py:99,107`)
**already** sends `Cache-Control: no-cache, no-store, must-revalidate` on
every response — the origin is not the bug. Since `status.ollietrades.com`'s
route is dashboard-managed Cloudflare Tunnel config, not the local
`~/.cloudflared/config.yml` (per `docs/REBOOT_POSTURE.md` key posture fact
#4), the edge is most likely overriding origin cache directives via a
zone-level Cache Rule / Page Rule ("Cache Everything" or similar) or an Edge
Cache TTL setting that doesn't respect origin `Cache-Control` for this
hostname. Real fix is a Cloudflare Zero Trust dashboard change (a Cache Rule
that bypasses cache for `status.ollietrades.com`, or setting Edge Cache TTL
to "respect existing headers" / bypass), not a code change. Not actioned
this pass — dashboard-side, needs Admiral/XO to apply in the Cloudflare
dashboard.

---
## 🔵 HM-SWEEP-CADENCE — proposed 2026-07-05, cron not yet installed

**Approved earlier (XO-DECISIONS item 6, "sweep cadence"):** a manual clean-
window sweep run tonight after the 9:00 PM tuning crew, plus a standing
weekly cron so this stops depending on someone remembering to run it
manually — same "enforced at the door, not by periodic diligence" doctrine
already applied to the roster cap (see "Roster quality is enforced at the
door" in `docs/DOCTRINE.md`).

**Tonight's manual run:** scheduled via a session-scoped one-shot job (not
a real crontab entry — see caveat below) for ~9:50 PM MST, after the tuning
crew's 9:00-9:30 PM window. Runs `fleet_realism_sweep_clean_window.py`,
diffs the new report against this morning's `reports/fleet_realism_sweep_clean_20260705_065111.json`
baseline for the 5 agents with any clean-window signal, and reports whether
Tier 1 rankings held.

**Weekly cadence — proposed crontab line, NOT installed, needs your go-ahead
to add via `crontab -e`:**

```cron
10 22 * * 0 cd /Users/bigmac/autonomous-trader && .venv/bin/python -u fleet_realism_sweep_clean_window.py >> logs/fleet_sweep_clean.log 2>&1
```

Sunday 10:10 PM MST — ~40 min after the tuning crew's 9:00-9:30 PM window
closes, safe buffer against a slow tuning-crew run overlapping. Writes a
new timestamped `reports/fleet_realism_sweep_clean_*.json` each week (the
script never overwrites prior reports, per its own doctrine comment) and
appends to `logs/fleet_sweep_clean.log`. No notification/alerting wired —
purely a standing data point for the July 24 kill-gate read and future
roster-reconciliation passes; XO/Scotty would need to actually look at the
new report file each week, this doesn't push anything.

**⚠ Durability note, this is why this proposal lives here and not just in
chat:** the tonight-only run above was scheduled via this session's
`CronCreate` tool, which is **session-scoped — it is lost entirely if this
Claude Code session ends before it fires, with no warning.** The weekly
cadence proposed above is a REAL crontab line and, once installed, would
survive session death, reboots, and everything else a real cron job
survives — that durability gap is exactly why the weekly cron matters more
than it might look, and why this proposal is written here rather than left
as a one-off scheduled reminder.

---
## 🔴 HM-GATE-RESTART-HOLD — restart already occurred tonight (Desk session), verified harmless; Monday checklist collapsed

**Original hold (Admiral, 2026-07-05 evening):** do not restart the main
trader process (main.py) before Monday's close. Rationale: Monday is the
first live session under Friday's realism/staleness-fix config — its
numbers are the measurement we want, and a Sunday-night trader restart would
be a confound on that read. The auditioning gate (`crew_role='auditioning'`
checks in `paper_trader.buy()`/`short_sell()`/`RiskManager.check_buy()`, plus
`halt_gate.is_auto_tradeable()`'s `can_trade_live` enforcement) was committed
to the working tree, dormant until a restart, with nothing it guards against
able to occur before then (no auditioning candidates existed yet).

**⚠ Superseded by events, verified harmless (2026-07-05 ~18:38 MT
verification pass).** main.py restarted at **18:17:36 MST tonight** — a
**second, distinct event from the morning power-cycle**, several hours later,
not a re-triggering of it. **Traced to the closed HM-DECISION-DESK-MVP
session**, not launchd and not a crash: its own transcript records the exact
command, `zsh scripts/trader_restart.sh 2>&1 | tail -30`, run to live-verify
the new `/api/desk/*` endpoints before committing `9bb56e6`. `trader.log`
confirms a clean restart, not a crash — continuous heartbeats through
18:17:23, then a fresh-process init sequence at 18:17:36, no traceback
anywhere near the boundary. That session was unaware of this hold.

Four-point check run against the live restart, all clean:
1. **Restart source identified** — Desk session (above), not launchd/crash/Admiral.
2. **Gate code present in what's running** — `engine/halt_gate.py` +
   `setup_db.py` clean at `git status` (committed `f99df7e`), no working-tree drift.
3. **`[AUDITION-GATE]` fired correctly** — `trader.log` 18:17:36 verbatim:
   `[AUDITION-GATE] 0 auditioning seat(s); can_trade_live enforcement=OFF —
   backfill not detected (is_auto_tradeable() falls back to legacy
   is_human-only check; the crew_role='auditioning' checks in
   paper_trader.buy()/short_sell() and RiskManager.check_buy() are unaffected
   and still enforce independently)`. Exactly the designed fail-safe,
   confirmed from the log, not assumed.
4. **All 10 gated agents still tradeable** — queried directly: the 6
   executing (capitol-trades, neo-matrix, qwen3-8b-flash, ollama-qwen3,
   ollama-plutus, options-sosnoff) and 4 exit_only (gemini-2.5-flash,
   guardian-of-forever, navigator, ollie-auto) all have `is_human=0`, none in
   the passive-mirror set (`alpaca-mirror` only) — with enforcement OFF,
   `is_auto_tradeable()` falls back to `not is_human` for all ten. All can
   place/close normally tomorrow.

**Precision note — crew_dissent fix picked up early, harmless.**
`resolve_dissent_outcomes()`'s standalone run was filed above as "wouldn't be
picked up until Monday's restart either way." Tonight's restart picked it up
early (main.py's in-memory copy refreshed at 18:17:36) — harmless, since the
standalone run had already applied the identical fix to the same 22 rows
ahead of the restart. No discrepancy, just an earlier-than-planned no-op
re-application.

**⚠ Staleness-delta caveat for Monday's read.** The trader process itself
restarted Sunday evening 2026-07-05 (unplanned, Desk-session-triggered) —
distinct from the Monday-after-close event this hold exists to protect.
Gate code and config are unchanged between tonight's restart and Monday's,
so Monday's staleness-delta measurement is not confounded by config drift —
but the process's actual uptime clock reset Sunday night, not at Friday's
config landing. State this explicitly wherever Monday's read gets written up.

**What DID ship tonight (2026-07-05), already applied, not held:**
- `scripts/swingdesk_restart.sh` run — SwingDesk (:8889) now runs the new
  `SwingDeskAuthMiddleware` + startup auth-state log line. Isolated service,
  does not touch the trader process. Verified single process (PID 1675),
  no orphan, health checks passing.
- `resolve_dissent_outcomes()` run standalone — all 22 crew-dissent rows
  resolved live, `outcome_basis='price_pct'` tagged, 11/22 (50.0%) correct.
  Backup taken first: `data/backups/trader_2026-07-05_pre-crew-dissent-backfill.db`.

**Monday-after-close checklist (collapsed — the gate/config half already
proved clean tonight, so this is now purely the backfill + enforcement flip):**
1. Run the `can_trade_live` backfill SQL (see HM-AUDITION-ONBOARD-3 below for
   the exact statements).
2. Restart the trader (`scripts/trader_restart.sh`).
3. Verify `[AUDITION-GATE] active — ... can_trade_live enforcement=ON` (not
   the OFF/backfill-not-detected line seen tonight).
4. Confirm all 10 agents (6 executing + 4 exit_only, listed above) can still
   place/close real orders post-restart — the one that matters most, since a
   missed row in the backfill silently strands real positions.

---
## XO-DECISIONS 2026-07-05 — Admiral rulings on the Sunday systems-check, design still pending build

1. **Audition spend DENIED for now.** No paid-API candidates (Claude Sonnet 5,
   Grok 4.3) until the auditioning gate is proven live with real clean-window
   data. Qwen3.6-35B-A3B (free, local Ollie Max) audits the mechanism first.
2. **Gate design APPROVED WITH HARDENING** — 3 requirements before build:
   (a) second, independently-implemented enforcement check at dispatch level
   (not just inside `paper_trader.buy()`); (b) `can_trade_live` must get
   killed or genuinely wired up — no decorative flags; (c) a startup log line
   proving the gate is live. **Proposed design (not yet built — see chat for
   full detail):** primary check in `paper_trader.buy()`'s existing HALT GATE
   block (extend the inline SELECT to also read `crew_role`, reject if
   `'auditioning'`); independent second check in `RiskManager.check_buy()`
   (risk_manager.py — a different module, already the standard pre-flight
   gate called from `ai_brain.py` before `execute_signal()`/`buy()`, satisfies
   (a)); wire `can_trade_live` into the already-called `halt_gate.
   is_auto_tradeable()` (currently only checks is_human + passive-mirror) as
   a third layer, satisfying (b) — but this REQUIRES a backfill migration
   first (`can_trade_live=1` for every currently-legitimately-executing
   agent) since **all 79 `ai_players` rows currently have `can_trade_live=0`**,
   including every genuinely-executing agent — flipping enforcement on
   without the backfill would instantly halt the live fleet. Migration SQL
   needs Admiral review before running (case-by-case: tracking/sim/mirror/
   auditioning stay 0, guardian-of-forever needs judgment since it places
   real exit-only Alpaca orders despite being structural). (c): add
   `[AUDITION-GATE] active — N auditioning seat(s), can_trade_live
   enforcement=ON` to setup_db.py's roster-cap startup block.
3. **Build order confirmed:** gate built once, Qwen3.6-35B-A3B onboards
   first (new `ai_players` row, `crew_role='auditioning'`, mirrors an
   incumbent's mandate, doesn't touch the 6 current executing seats). Sonnet
   5 / Grok 4.3 rows are code-ready-shaped but not inserted/activated until
   item 1 flips.
4. **Crew-dissent fix APPROVED, propose-first.** Diff + backfill plan for
   the 22 stale rows required before anything runs (see chat for the
   proposed realized-price-return resolver replacing the `scored_predictions`
   dependency, and the backfill approach for the existing 22).
5. **Swingdesk auth backstop — proposed:** reuse `dashboard.app.AuthMiddleware`
   + its `/login` routes, mounted onto `swingdesk/backend.py` (currently only
   has `CORSMiddleware`, zero auth of its own) rather than reimplementing a
   parallel login system. Needs an import-safety check (circular imports,
   dashboard-specific globals) before landing.
6. **Sweep cadence — recommended:** do a manual `fleet_realism_sweep_clean_window.py`
   run tonight after the 9:30 PM tuning crew (immediate 2nd datapoint, matches
   this session's HM-ROSTER-CAP methodology precedent) AND add a weekly cron
   (Sunday ~10:00 PM MST, after the tuning crew) for ongoing cadence — matches
   the project's own "enforced at the door, not by periodic manual diligence"
   doctrine already applied to the roster cap.
7. **Report-back scheduled:** one-shot check at 9:38 PM MST tonight (session-
   scoped cron, does not survive a session end) to confirm `model_scores`
   populated and report options-sosnoff/qwen3-8b-flash's first live audition
   verdicts.

---
## HM-SUNDAY-SYSTEMS-CHECK — 2026-07-05, diagnosed, all propose-first (nothing applied)

**1. 🟢 tour.ollietrades.com 404 — NOT a bug, working as designed.** `tour_api.py`
(PID running since 2026-07-01, cron `@reboot` autostart, unrelated to the
07:24 trader restart) is a healthy headless JSON API — `/api/tour/health`,
`/api/tour/state`, `/api/tour/ticks`, `/api/paper/order` all respond fine.
It never defined a `/` route, so `GET /` 404ing is day-one behavior, not a
regression — confirmed real backend response (`server: uvicorn`), not a
Cloudflare edge 404 (CF Access still correctly gates the hostname).
Same pattern already documented for swingdesk's bare-`/` 404 in
`docs/HANDOFF.md`. **CLOSED 2026-07-05 (Admiral decision, re-verified same
day before closing — PID 417 unchanged, still running since 2026-07-01,
same 404 behavior): no action needed, API-only design accepted as final.**
Do not re-open this without new evidence of an actual regression (e.g. a
consumer starting to expect `/`).

**2. 🟡 CF Access auth-state — no drift found, but a real exposure gap surfaced.**
Live test (2026-07-05 ~14:47 UTC) shows all 4 subdomains correctly gated,
contradicting the "bridge open" report — likely a transient window, not a
standing regression. No repo/config change since 7/3 touches auth. Found a
plausible mechanism: `logs/cloudflared-daemon.log` shows a QUIC
reconnect/DNS-failure storm ~13:19-13:29 UTC today; a tunnel reconnect could
plausibly produce a brief Access-enforcement gap. **Real finding, not
speculative:** `swingdesk.ollietrades.com` (:8889) has **zero app-level auth
of its own** — `curl localhost:8889/` returns 200 directly, no login
redirect — it relies entirely on CF Access as its only auth layer, unlike
bridge/signal which both have an app-level login backstop even if CF Access
lapses. **Proposal:** (a) add an app-level auth check to swingdesk so it
isn't single-point-of-failure on CF Access, (b) pull the CF Access audit log
from the dashboard (not available locally) to check for actual policy
changes/lapses around today's reconnect storm — that's the one thing no
local artifact can answer.

**Item (a) SHIPPED same day** — see the swingdesk-auth commit `f99df7e`:
`SwingDeskAuthMiddleware` (`swingdesk/backend.py`) now requires a valid CF
Access JWT or the internal token for non-localhost/non-tunnel traffic, plus
a startup log line reporting configured/enforcing vs unconfigured/open.
Applied live via `scripts/swingdesk_restart.sh`, verified single process
(PID 1675), no orphan. Item (b) — the CF dashboard audit log for the
reconnect-storm window — was not pulled; superseded by the direct dashboard
read below, which answers different, higher-value questions instead.

### CF Access posture — full picture (local verification + Admiral's CF dashboard read, 2026-07-05 evening)

**Locally verified (this session, independently of the dashboard read):**
- `~/.cloudflared/config.yml` on this box lists exactly 4 ingress hostnames
  — `bridge.ollietrades.com`→:8080, `signal.ollietrades.com`→:9000,
  `swingdesk.ollietrades.com`→:8889, `tour.ollietrades.com`→:8088 — plus a
  catch-all `http_status:404`. **`status.ollietrades.com` is NOT in this
  file at all.** Its route was added directly via the CF Zero Trust
  dashboard (`docs/HANDOFF.md` commit `f2d929c`, 2026-07-02: "Route added
  via the Admiral's own Zero Trust dashboard action, DNS auto-created that
  way, not by me"), consistent with CLAUDE.md's "Remote config v11" note —
  this tunnel's ingress rules live partly in CF's remote config, not solely
  in this local file, so `config.yml` alone undercounts the real route set.
- **`status.ollietrades.com` having no Access app is confirmed intentional,
  not an oversight.** `scripts/status_page.py`'s own docstring: "Admiral-
  approved 2026-07-02. Deliberately minimal: no auth, no secrets, no write
  paths -- read-only health checks only, safe to expose publicly." Read the
  full script and hit its live `/api/status` just now: exactly 4 booleans
  (bigmac/ollie_max/trader/tunnel up-or-down) + a timestamp, nothing else —
  no account data, no DB reads, no secrets, no write path exists at all.
  Currently live (PID 6855, running since Thursday). This is a standard
  public status-page pattern and checks out as safe on its own facts, not
  just on the docstring's say-so.
- The `/static/manifest.json` CF Access bypass is real, already shipped
  (2026-07-02, `docs/HANDOFF.md`), and **scoped exactly as narrow as it
  sounds**: unauthenticated `curl https://bridge.ollietrades.com/static/manifest.json`
  → `200`, real body, no CF Access cookie needed — but three sibling paths
  (`/`, `/static/index.html`, `/static/app.js`) all still `302` to the CF
  Access login, confirming the bypass wasn't accidentally widened. Full
  history worth knowing: the manifest originally referenced two icon files
  (`icon-192.png`, `icon-512.png`) that were NOT covered by the bypass,
  which caused a real bug (bridge-v2 hanging on those gated icon fetches).
  The Admiral **declined** widening the Access bypass to cover the icons
  when that was proposed — instead the `icons` array was removed from
  `manifest.json` entirely (a PWA with no icons still installs, falls back
  to a generic icon). So today's actual public-bypass surface is exactly
  one file, with zero images/icons in it, by deliberate choice — not a
  partial fix that quietly left a wider hole.

**RESOLVED 2026-07-05, direct CF dashboard read (Admiral, policy column
inspected directly — this is now confirmed, not a local-evidence guess):**
5 Access applications = **bridge-full** (bridge.ollietrades.com, the
identity-requiring `bridge-allow` policy, 3-email allowlist / 730h session)
+ **bridge-manifest-bypass** (a SEPARATE, path-scoped app for
`bridge.ollietrades.com/static/manifest.json`, carrying its own distinct
`manifest-bypass` policy — never touches `bridge-allow`) + **tour** +
**signal** + **swingdesk** (each their own hostname-level app, presumably
also on `bridge-allow`, unconfirmed per-app but consistent with the shared-
policy pattern CLAUDE.md already documents). This fully explains the
earlier "how is the manifest path publicly `200` if the policy requires an
email login" tension — it doesn't share that policy at all, confirmed
directly rather than inferred. No contradiction; the local finding
(unauthenticated `200` on the manifest path, `302` on every sibling path)
was correct and is now backed by the dashboard's own policy assignment.

**`CF_ACCESS_AUD_EXTRA` — SHIPPED and verified end-to-end, 2026-07-05.** Real
64-hex-char AUD values for bridge and swingdesk's Access applications were
provided by the Admiral and saved into `swingdesk/.env` (gitignored, not
written into this file) — `CF_ACCESS_TEAM_DOMAIN`, `CF_ACCESS_AUD`=bridge's,
`CF_ACCESS_AUD_EXTRA`=swingdesk's (added this session to `dashboard/cf_auth.py`'s
`_valid_cf_jwt()` for exactly this multi-application-aud case). This
session had no Read/Edit access to any `.env` file, so the actual edit was
made by hand outside this session — confirmed the boundary rather than
routing around it.

SwingDesk restarted (`scripts/swingdesk_restart.sh`) — single process, no
orphan (same benign nohup-PID-echo quirk as the first restart: script
echoed one PID, a different one actually won the port-bind race and is the
real survivor; verified via `pgrep -f` + `lsof`). Startup log flipped from
"OPEN" to **"CF Access configured -- enforcing"**.

**End-to-end verified with a real browser session through the actual
Cloudflare tunnel (not a synthetic test):** navigated to
`https://swingdesk.ollietrades.com/` — full dashboard rendered live (chart,
watchlist, positions, journal). Network log: document + all 8 `/api/*`
calls (`health`, `watchlist`, `candles`, `positions`, `journal`, `stats`,
`risk-gate`, `signals`) → **200**, zero 401s, zero CF Access login
redirects. Cross-checked server-side in `logs/otasty.log`: the real
Cloudflare edge IP (`64.43.89.142`, not localhost, not a synthetic test IP)
hit every endpoint and got 200 across the board — genuine CF-signed JWT,
`aud` claim matched `CF_ACCESS_AUD_EXTRA`, validated correctly. Also
reconfirmed the synthetic pre-restart tests are visible in the same log for
contrast: a fake non-CF IP with no credentials correctly got 401 both times.
**Closed — no further action needed on this item.**

**3. 🔴 crew-dissent resolved=0/pending=22 — structural join mismatch, not a
transient/timing issue.** `resolve_dissent_outcomes()`
(`engine/crew_dissent.py:261-338`) runs nightly at 23:30 without error
(confirmed 4 consecutive clean nightly log lines, 07-01 through 07-04) and
correctly finds 0 resolvable rows every time — not stuck, not crashing,
genuinely computing zero. Root cause: it requires a same-symbol,
same-exact-date row in `signal-center/signals.db`'s `scored_predictions`
table with `horizon_days=5, closed=1` — but `scored_predictions` is
populated by individual agent-signal-generation events (chekov, danelfin_ai,
options_flow_scanner, etc.), not by the daily consensus/dissent cycle, so
the two pipelines' `(symbol, date)` keys essentially never coincide (verified:
0 matches at ANY horizon for all 22 rows; per-symbol `scored_predictions`
data predates every dissent by 2+ weeks). All 22 pending rows are 12-27 days
old — well past any resolution horizon, so this isn't "give it more time."
**Proposal:** resolve dissents against realized price action directly
(forward 5-trading-day return from `dissent_date` close via price history),
not via `scored_predictions` — decouples dissent-resolution from an
unrelated pipeline's incidental data.

**SHIPPED 2026-07-05 evening.** `engine/crew_dissent.py`'s
`resolve_dissent_outcomes()` now tries `scored_predictions` first (tagged
`outcome_basis='r_multiple'`), falls back to Polygon daily bars (tagged
`outcome_basis='price_pct'` — new column, old column's semantics never
silently reinterpreted). Local price tables turned out to have no usable
data either (`price_ticks` empty, `backtest_market_data` stale since
2026-04-02, `market_snapshots` covers only an unrelated fixed watchlist) —
Polygon (already-approved paid source) was the only option, bounded to one
call per distinct symbol per run via a pre-fetch, not per row.

**Bug found and fixed during verification, not after:** `PolygonData.get_bars()`
(`engine/providers/polygon_provider.py`) converted each bar's UTC millisecond
timestamp via `datetime.fromtimestamp()` — SYSTEM-LOCAL time. This box runs
`America/Phoenix` (UTC-7, no DST); Polygon's daily bars are stamped at
midnight ET (04:00-05:00 UTC), so every bar's date label rolled back one
calendar day. Two dissent rows (AVGO 06-18 and 06-19, the latter Juneteenth)
came back with identical resolved outcomes — the visible anomaly that
surfaced it: the real 06-18 close was mislabeled 06-17 and sorted before
BOTH dissent dates, so both incorrectly anchored to the same next bar.
Fixed to `fromtimestamp(tz=timezone.utc)`. Also affected (display-only, now
corrected) `dashboard/app.py`'s `/api/polygon/bars` chart endpoint — the
only other caller. **Doctrine: any UTC-millisecond timestamp conversion
must use an explicit UTC/exchange-timezone anchor, never system-local time
— the bug is invisible in UTC or Eastern-timezone environments and only
surfaces west of Eastern, which is exactly why it shipped unnoticed.**

Backfilled live 2026-07-05 (backup: `data/backups/trader_2026-07-05_pre-crew-dissent-backfill.db`):
22/22 resolved, 0 pending, 11/22 (50.0%) correct, all attributed to dissenter
Q — worth a line in the weekly tuning report per XO note.

**4. 🔵 Audition-pipeline onboarding design for 3 candidate models — proposal
below, nothing built.** See "HM-AUDITION-ONBOARD-3" ticket immediately below
for the full design (Claude Sonnet 5, Grok 4.3, Qwen3.6-35B-A3B).

---
## 🔵 HM-AUDITION-ONBOARD-3 — proposed 2026-07-05, no roster changes before Jul 24

**Ask:** onboard 3 candidates into shadow/audition per `AUDITION_CRITERIA`
(20 clean guarded trades in 6 weeks), competing for the 2 empty seats
reserved by HM-ROSTER-RECONCILE-8. Never seated on priors — must earn a
pass verdict from real clean-window data, same as options-sosnoff/
qwen3-8b-flash's current audition.

**Blocking gap found while scoping this:** there is no generic mechanism
today that lets a candidate scan and emit real signals while being
structurally blocked from executing a real order. `ai_players.can_trade_live`
looks like it should be that gate (used descriptively by ollie-machine,
q-witness, sell-the-news) but **it is checked NOWHERE in
`engine/paper_trader.py` or `engine/halt_gate.py`** — confirmed by grep,
zero hits. Confirmed further: **every single row in `ai_players` (all 79)
has `can_trade_live=0`**, including all 6 currently-really-executing agents
(capitol-trades, ollama-plutus, options-sosnoff, etc.) — the column carries
zero enforcement weight anywhere in the standard pipeline. The existing
`can_trade_live=0` agents (ollie-machine, sell-the-news) achieve real
shadow-safety only because they're **architecturally separate** — bespoke
scripts/loops with their own tracking-mode portfolio that never call the
shared `buy()` path at all, not because anything reads the flag.

**Proposed mechanism (not yet built):** add a real, minimal gate at the top
of `paper_trader.buy()` (and the options/short equivalents), keyed off
`crew_role='auditioning'` (no schema migration — reuses the existing free-text
column) rather than the already-meaningless `can_trade_live`: log the
decision to `signals` and run it through the same guardrail/quality-gate
checks as a real trade (so the audition is honest), then stop BEFORE the
Alpaca/cash-touching order and return a `shadow_logged` result instead —
architecturally the same shape as the existing ordered gate list in
`paper_trader.buy()` (HALT GATE → grade-B fleet gate → per-model max
positions → quality gate), just one more entry. This makes a candidate's
signals accumulate exactly like any other benched candidate's do for
`weekly_tuning_crew._run_auditions()` — no separate audition-scoring code
needed, it already generalizes.

**Per-candidate onboarding shape (3 new `ai_players` rows, `crew_role=
'auditioning'`, `halt_mode='active'` so they scan/emit, gated from execution
by the new check above — none touch or replace any of the 6 current seats):**

- **Claude Sonnet 5** (vs culled `claude-sonnet`, currently `halt_mode='full'`,
  locally-redirected to `ministral-3:3b` — no real Anthropic wiring exists
  today for this id). Real API model — **Free-Models-First doctrine requires
  explicit Admiral spend approval per agent** (`CLAUDE.md`: "Paid models are
  FORBIDDEN unless the Admiral approves the spend"). Needs a new provider
  wiring (no existing `ANTHROPIC_API_KEY`-based trading-agent path — grep
  found Anthropic SDK usage only in `lib/TradingAgents/` vendor code and
  test/dev scripts, not a live `ai_players` provider). Context, not a
  blocker: `docs/DOCTRINE.md`'s full-history sweep found the *prior*
  claude-sonnet posted <9% guarded-honest return / high spam, same as every
  other frontier cloud agent — exactly the kind of prior this audition is
  designed to test past, not be bound by.
- **Grok 4.3** (vs culled `grok-3`, `halt_mode='full'`, locally-redirected to
  `qwen3:14b`). Also a real paid API — same spend-approval requirement.
  `engine/team_advisor_grok.py`/`engine/providers/grok_provider.py` already
  have a working xAI integration (used by Archer/Q, not as a trading
  `ai_players` seat) — this is the more mechanical of the two integrations,
  reuse rather than build fresh.
- **Qwen3.6-35B-A3B** (Apache 2.0, local on Ollie Max — no spend approval
  needed, Free-Models-First compliant). Framed as a model-upgrade candidate
  for ollama-qwen3/qwen3-8b-flash rather than a new agent identity — proposed
  as its own auditioning seat (mirrors one incumbent's mandate, competes
  head-to-head) rather than mutating either live seat's `model_id`, so the
  6 current executing seats stay untouched per "no roster changes before
  Jul 24." Needs: confirm the model is pulled on Ollie Max
  (192.168.1.168:11434) and fits current VRAM co-residency (A3B = active
  ~3B-param MoE, lighter than the full 35B footprint, but verify against
  `docs/runbooks/ram-discipline.md` before pulling).

**Open for Admiral decision before any of this is built:** (1) approve/deny
paid-API spend for Sonnet 5 + Grok 4.3 candidates, (2) confirm the
`crew_role='auditioning'` gate design (vs. any alternative), (3) confirm
scope — build the gate + onboard all 3, or start with the free local
Qwen3.6 candidate only and defer the two paid ones.

**STATUS UPDATE 2026-07-05 evening — gate built, tested, committed; NOT yet
live (see HM-GATE-RESTART-HOLD above).** Decisions: (1) spend DENIED for now,
Qwen3.6 audits the mechanism first; (2) gate design approved with 3 hardening
requirements, all met — see `engine/paper_trader.py` (primary check in
`buy()`/`short_sell()`, both now also carry the `crew_role='auditioning'`
check; `short_sell()` additionally gained a halt_mode check it never had at
all before this), `engine/risk_manager.py` (independent second check in
`check_buy()`, different module/connection), `engine/halt_gate.py`
(`is_auto_tradeable()` now enforces `can_trade_live`, gated behind
`check_can_trade_live_backfill()` so it fails safe if the backfill below
hasn't run), `setup_db.py` (startup log line); (3) Qwen3.6 onboards first,
paid candidates code-ready-not-activated.

**can_trade_live backfill SQL — final, empirically derived (not guessed) by
tracing every `_is_human_player()`/`is_auto_tradeable()` call site in
`paper_trader.py`. Two groups — missing group 2 would silently strand real
open positions with no close path the moment enforcement goes live:**

```sql
-- Group 1: currently active/executing (6 agents)
UPDATE ai_players SET can_trade_live = 1
WHERE id IN ('capitol-trades','neo-matrix','ollama-plutus','ollama-qwen3',
             'options-sosnoff','qwen3-8b-flash');

-- Group 2: exit_only agents holding OPEN positions right now -- their real
-- closing sell() calls go through the same _is_human_player() gate inside
-- paper_trader.sell(); guardian-of-forever's entire purpose is placing real
-- exit-only Alpaca orders.
UPDATE ai_players SET can_trade_live = 1
WHERE id IN ('gemini-2.5-flash','guardian-of-forever','navigator','ollie-auto');
```

Everyone else (69 rows) stays `can_trade_live=0` — correct by construction:
`halt_mode='full'` agents are excluded from the scan roster and from
`can_close_position()` regardless, so the flag is moot for them; tracking/
sim/human rows are blocked by `is_human` or never reach this check via their
own separate code paths either way. **HELD until Monday after close,
bundled with the trader restart** — not run tonight (see HM-GATE-RESTART-HOLD).

All of the above dry-run tested against throwaway DB copies before writing
here: simulated auditioning candidate correctly blocked at all 3 layers
(gate_reject_log confirms `AUDITION_SHADOW`/`HALT`, zero positions created);
`check_can_trade_live_backfill()` correctly reports not-ready pre-backfill
and ready post-backfill; zero regression for real executing agents.

---
## 🟢 HM-ROSTER-RECONCILE-8 — Admiral decision recorded 2026-07-05, SQL pending final go-ahead

**Admiral decisions (2026-07-05):**
1. ollama-plutus / capitol-trades — keep as **measured core, under mitigation**
   (not "auto-keep clean," not "auto-cut negative") — trust the existing
   PROBATION_WATCH / tightened risk_manager caps already in place; their CIs
   are too wide to condemn on this sample.
2. options-sosnoff / qwen3-8b-flash — reclassified **ACTIVE-AUDITIONING**:
   stay executing (count against the cap), but their next 20 clean guarded
   trades are a formal audition against `AUDITION_CRITERIA`; fail or stall
   (no 20 trades within 6 weeks) → halt proposal, seat opens.
3. neo-matrix — candidate pending two checks: tail risk (worst loss/max DD/
   open-risk, since 91.2% WR with small wins reads like a premium-seller
   profile) and provenance (why is it sweep-invisible — deliberate shadow
   status or oversight).
4. Resulting roster: neo-matrix (pending #3), ollama-qwen3 [measured],
   ollama-plutus + capitol-trades [measured, mitigated], options-sosnoff +
   qwen3-8b-flash [auditioning]. 2 slots left EMPTY for audition graduates.
   dalio-metals / gemini-2.5-flash excluded from the cap count (tracking /
   exit_only-draining), not part of this list.

**Verification results (2026-07-05, this session):**
- **neo-matrix: PASSES both checks, confirmed auto-keep.** Worst clean-window
  closed-trade loss −$39.60 (GOOGL), second-worst −$35.92 (AVGO); max
  peak-to-trough drawdown $49.22 against +$90.58 cumulative; 0 open positions
  right now; trade sizing is fractional/small-notional throughout (not a
  premium-seller tail-risk shape). Provenance: `risk_manager.WARNING_ONLY_PLAYERS`
  only exempts it from the sector-concentration check, not from execution —
  it has been genuinely `halt_mode='active'` the whole time; this was a
  sweep-tooling blind spot (HM-SWEEP-SIGNALS-TABLE-BLIND-SPOT), not a
  deliberate bench.
- **options-sosnoff: confirmed NOT dormant, but surfaced a real sizing issue.**
  84 real closed CSP trades in `options_trades` since 2026-05-14 (`book_tag=
  'fleet'`, all `structure='csp'`, UPRO/SOXL/TQQQ/SPY/QQQ) — completely
  invisible to the `signals`-table sweep, exactly the same blind spot as
  neo-matrix. Aggregate P&L +$29,868.74 against a $12,880.20 account cash
  balance — checking individual legs, a single UPRO CSP in this book carries
  `max_loss=-$12,141` (≈$12-13K collateral) against that same account: one
  concurrent leg alone would nearly exhaust it, and dozens were recorded.
  Same disease already flagged in doctrine for this agent's backtest replay
  ("unbounded, not capital-constrained") now confirmed in real recorded
  trades. The fix (`75b63f1`, "CSP notional visibility + cap gate on new
  opens") already landed 2026-07-04 but is **not live** — needs the pending
  restart. **Action: options-sosnoff's ACTIVE-AUDITIONING trade count should
  only start counting from trades opened AFTER this weekend's restart**, once
  the cap-gate is actually enforcing; pre-restart CSP trades are not
  decision-grade for the audition.
- **qwen3-8b-flash: clean, no flags.** 16 real trades since clean cutoff (via
  the plain `trades` table, also `signals`-invisible), +$85.79, 16/16
  winners, modest sizing. Just short of the 20-trade audition floor.

**SQL drafted, NOT yet run** (7 cuts: archer, cto-grok42, energy-arnold,
holly-scanner, q-witness, quark-ic, sell-the-news → `full`; 1 reactivation:
ollama-qwen3 exit_only → active; label-only halt_reason updates on the other
5 kept seats). `enterprise-computer` (tracking-route), `ollie-machine`
(sim/tracking), and `trade-desk` (is_human=1, manual desk) deliberately left
untouched — they were never competing for one of the 8 to begin with, so
"apply the SQL for everything outside the list" was read as applying to
genuine executing candidates only, not these three structural categories.
Flag if that reading is wrong.

---
## Original open questions (2026-07-05, resolved above — kept for the record)

Context: HM-ROSTER-CAP (2026-07-04) built the mechanism (`MAX_ACTIVE_AGENTS=8`,
`AUDITION_CRITERIA`); this session shipped the two pieces the mechanism
needed to actually run (cap-exclusion fix in `setup_db.py`, audition scoring
in `weekly_tuning_crew.py` — see `docs/DOCTRINE.md` "Both mechanisms wired
live"). Applying the actual roster picks (which 8 seats hold `halt_mode=
'active'`) is **paused, not done** — real clean-window data contradicts part
of the proposed "measured core" list, and this is the Captain's call, not
mine to guess:

1. **capitol-trades and ollama-plutus are named auto-keeps and ARE
   negative on clean-window data — but this is already known and already
   partially mitigated, not fresh news.** `engine/crew_specialization.py`'s
   `PROBATION_WATCH` (HM-FLEET-REBASELINE-2026-07-04) already flags
   capitol-trades as "the only negative guarded agent (-3.51%)" with a
   60-day review date (currently 32d history); `engine/risk_manager.py`
   already tightened ollama-plutus's `max_daily_trades` 3→2 and
   `max_position_pct` ~0.25→0.15 specifically because its clean-window
   return flipped +21.67% (full-history) → −3.11% (n=22, 95% CI
   [−27.1%,+20.9%] — not distinguishable from zero on this sample) — noted
   explicitly as "tightening exposure while forward data accrues, not
   acting on the noisy point estimate." So the stated rule "auto-cut any
   active agent that flipped negative" was deliberately NOT applied to
   these two, in favor of probation-watch + reduced exposure. Question for
   the Captain: is that mitigation sufficient to justify "auto-keep" in the
   final 8, or should the roster-cap decision go further (cut/bench) now
   that a second, independent mechanism (the cap itself) is asking the same
   question?
2. **options-sosnoff and qwen3-8b-flash are named auto-keeps, but the
   numbers behind that pick are from the FULL-HISTORY sweep
   (`fleet_realism_sweep.py`), which `docs/DOCTRINE.md`'s own "Guarded+
   honest is the only decision-grade backtest number" entry already
   superseded** — the very next doctrine entry (clean-window re-run,
   2026-07-04) states "no fleet ranking is trustworthy — 17/22 agents have
   zero post-GATE-0 signals... nothing before it should be cited as a
   performance baseline." options-sosnoff (+25.23% guarded full-history)
   and qwen3-8b-flash (0% spam / 83.3% WR full-history) were exactly the
   top full-history performers that prompted that doctrine note — and on
   the clean-window standard the doctrine itself now says is the only
   trustworthy one, both currently show **zero** clean-window signals.
   qwen3-8b-flash may also still be carrying a stale/paused persona (see
   Item 18 above, "Worf persona check is a prerequisite"). This isn't noise
   — it's the project's own methodology having moved out from under the
   pick between the full-history sweep and the clean-window one. Keep them
   on trust pending forward data, or treat as ordinary unmeasured
   candidates (→ likely empty slots per the "empty slot beats an unmeasured
   agent" rule) until they accrue real clean-window signal?
3. **neo-matrix is a real, strong, currently-invisible candidate.** 34 clean
   (non-contaminated) closed trades since 2026-05-14, +$90.58 realized,
   91.2% win rate — better-measured than anything on the named "measured
   core" list except ollama-qwen3 — but it never shows up in the
   `signals`-table sweep (see DOCTRINE.md blind-spot note). Should it be
   ranked into the 8 on its real `trades`-table performance, or is there a
   reason (WARNING_ONLY risk-radar status, etc.) it's deliberately being
   held out that isn't captured in this data?
4. **Five currently-`halt_mode='active'` seats are self-declared
   non-executing in their own `halt_reason`** — archer ("never executes"),
   q-witness (debate witness, `can_trade_live=0`), sell-the-news ("no
   execution until Admiral go"), quark-ic ("Arena Paper book only, no Alpaca
   forward"), ollie-machine (sim/tracking, already excluded via crew_role=
   'sim' logic elsewhere). Fold these into the same cap-exclusion as
   TRACKING_PLAYERS (they don't compete for a seat either), or leave them
   counted?

**Full current numbers (clean_window_start=2026-05-14) for reference:**

| player_id | clean signals | guarded trades | return% | spam% | friction/pnl | note |
|---|---|---|---|---|---|---|
| ollama-qwen3 | 268 | 6 | **+14.96** | 2.9 | 0.043 | 3/4 bars, fails only trade-count (needs 20) |
| neo-matrix (trades-table) | n/a — 0 in `signals` | 34 (real) | n/a | n/a | n/a | +$90.58, 91.2% WR — sweep-invisible |
| ollama-plutus | 1483 | 22 | −3.11 | 3.6 | 0.438 | fails return + friction |
| capitol-trades | 127 | 12 | −3.50 | **67.7** | 0.179 | fails all 4 bars |
| options-sosnoff | 0 | — | — | — | — | unmeasured |
| qwen3-8b-flash | 0 | — | — | — | — | unmeasured |
| deepseek-7b-grok4 | 1745 | 13 | −2.72 | 19.6 | 0.289 | already `halt_mode='full'` |
| ollama-coder | 328 | 3 | −2.27 | 0.0 | 0.075 | already `halt_mode='full'` |
| cto-grok42 (trades-table) | n/a | 6 (real) | n/a | n/a | n/a | +$18.55, 100% WR, thin sample |

Do not run the `halt_mode` activation SQL or restart until this is resolved —
restart is scheduled for this weekend (market closed) specifically so this
doesn't become Monday 6:30 AM's first boot untested.

---
## 🟡 HM-SWEEP-SIGNALS-TABLE-BLIND-SPOT — MEDIUM (filed 2026-07-05)

`fleet_realism_sweep*.py` and the new audition scorer (`_run_auditions`,
`engine/crew/weekly_tuning_crew.py`) both measure activity by counting rows
in the `signals` table. Agents that route through the signal-center
bridge/consensus path instead of the standard scan→`signals` pipeline never
write a `signals` row even when they're genuinely trading — confirmed live:
neo-matrix (71 trades since 2026-05-14, 0 `signals` rows), cto-grok42 (6
trades, 0 `signals` rows), trade-desk (1 trade, 0 `signals` rows). They show
up as `clean_signals_in_db=0` / "cannot assess," which is wrong for
neo-matrix and cto-grok42 specifically (trade-desk is human/manual, correctly
out of scope). Fix: `clean_signal_count()`/audition candidate scoring should
also check the `trades` table directly (real executed trades don't need a
backtest replay — just realized-P&L rollup via `engine.trades_filter`,
excluding `known_contaminated`) and treat "has trades-table activity" as
equally "measured" as "has signals-table activity." Scoped, not urgent —
doesn't block HM-ROSTER-RECONCILE-8 (numbers above already computed by hand),
but the sweep script itself should stop silently mislabeling these agents
"unmeasured" going forward.

---
## 🆕 HM-AGENT-RULES-CONSOLIDATION — 2026-07-04, Admiral-decided batches A-F shipped

Source: `drafts/AGENT-RULES-REVIEW-2026-07-03.md` (21 inconsistencies) +
Admiral decisions 2026-07-04. Canonical numbers baked in across
trading_rules.txt/config.py/risk_manager.py/base.py/stops.py (max positions
5/3, cash floor 20%/35%, stops = engine/stops.py 12/15/18 tiers, options cap
10%, position cap 30%); Sulu persona retired to Iron Condor King; stale 0.08
conviction-stop staticmethod removed (was a LIVE bug via paper_trader.py,
not dead code); Tier-1 roster swept of halt_mode='full' entries; exit_only
stop coverage generalized to all agents holding positions (was
guardian-of-forever only, missed 15 positions across 4 other seats).
Commits: acd62d1, 2787efa, 9b3767f, f9e3a4c, a384667, 9d3e097 (see each for
detail). Document-only items and tickets below.

**Document-only (no code change, per Admiral instruction):**
- **Item 9 — ADVISORY_CREW kill-gated bridge voters silently out of WR
  vote.** The 06-19/20 Door-1 kill-gate moved qwen3-8b-sonnet, qwen3-14b-pro,
  deepseek-7b-grok4, ollama-kimi, dalio-metals, ollama-coder to `halt_mode=
  'full'`. war_room.py:1132-1133 excludes non-active players from the bridge
  vote. Those 6 were originally kept `active` specifically so they'd still
  bridge-vote (FLEET-ROSTER.md design intent) — the kill-gate silently
  removed their vote as a side effect of a decision made for a different
  reason. **Accepted as-is** — re-adding them to the vote while `full`
  would contradict the kill-gate's own intent (a runaway agent shouldn't
  get a vote either). Revisit only if/when any of these 6 are reopened.
- **Item 21 — TRADE_DESK_BYPASS_GATES=True (config.py:34).** Trade-desk
  manual orders bypass daily limits, MAX_POSITION_VALUE, kill switch, and
  Uhura veto. **Accepted as-is** — this is the manual human trade-desk path,
  not an AI agent; a human placing a deliberate order shouldn't be blocked
  by automated per-agent gates. Flagging here so any future "what can trade
  without rules" audit has this on record.

**Tickets (found during the audit, not fixed — separate scoped work):**
- 🔵 **Item 11 — model-id triage.** `config.AI_PLAYERS` is documented-wrong
  for ~10 agents (config.py:302-312); DB `ai_players.model_id` is runtime
  truth but some of those are themselves garbage placeholders (neo-matrix =
  `'8000 / Independent'`). `CREW_MANIFEST` model fields are a THIRD,
  independently-divergent source (e.g. crew_specialization.py:294 says McCoy
  = `0xroyce/plutus:latest` vs config's `plutus-v1`). Needs one pass that
  picks a single source of truth and reconciles the other two, not spot
  fixes.
- 🔵 **Item 12 — cto-grok42 dead model.** `crew_specialization.py:613`:
  `ai_players.model_id` still `devstral-small-2`, uninstalled since the MSI
  migration. War Room / debate calls 404 for this agent until the DB row is
  fixed. Sits in `_SCAN_TIER3` regardless (harmless — Tier 3 members mostly
  `halt_mode='full'` anyway) but the model-id fix itself is a one-line SQL
  UPDATE + verify, cleanly scoped.
- 🔵 **Item 13 — naming dedup.** Two agents both display as "Lt. Jadzia Dax"
  (crew_specialization.py:310, 465). `main.py`'s Tier-2 comment labels
  `ollama-qwen3` "Scotty" while `CREW_MANIFEST` calls it "Dax". `mlx-qwen3`
  is labeled "Chekov" in `main.py` roster comments but "Ensign Ro" in
  `CREW_MANIFEST`. `FLEET-ROSTER.md` still carries a stale 2026-06-01
  21/6/45 count vs `CLAUDE.md`'s current 15/9/55/79 (2026-07-01) waypoint.
  Reopening decisions made off display names alone will hit the wrong
  player_id — needs a single naming pass across main.py comments,
  CREW_MANIFEST, and FLEET-ROSTER.md.
- 🔵 **Item 18 — paused personas vs full mandates.** Nine ids have
  placeholder personas (`base.py`: `"Paused. Former quant specialist."`) while
  `CREW_MANIFEST` simultaneously defines real mandates for them (Sisko,
  Tuvok, Janeway, Q, Bashir, Hoshi, Seven, Reed, Odo). If any of these are
  reopened without persona restoration first, they'd scan with no identity/
  rules beyond the generic RULES block. **`qwen3-8b-flash` (Worf)'s persona
  check is a prerequisite for the Batch-1 reopening pass** (mlx-qwen3 is
  Batch-1's headline candidate; Worf shares the same drift-reconcile history
  — verify its persona isn't also stale before either seat flips).
- 🔵 **Sulu DayBlade-label sweep (found while retiring the persona, commit
  9d3e097).** ~15 files still reference `dayblade-sulu` with DayBlade-era
  assumptions: `main.py`'s EOD options sweep (`close_all_options`),
  `paper_trader.py`'s sizing/circuit-breaker/long-only exemptions,
  `crew_scanner.py`, `super_backtest_v4.py`, `weekend_backtest.py`, etc. Some
  of this may already be functionally correct for an options/spread trader
  and just mislabeled from before the S6.3 pivot to Iron Condor King; some
  may not be. `dayblade-sulu` is `halt_mode='exit_only'` today (no new
  entries), so nothing here is live-executing — needs its own review pass
  before touching behavior, not a spot-fix. See CLAUDE.md Archive Convention
  section for the persona-retirement record.

---
## 🔴 HM-TROI-MAXPOS-CAP-DEAD — HIGH (filed 2026-07-03) — read-only diagnostic done

**Finding (HM-TROI-DEEPDIVE-2026-07-03):** Troi's (`options-sosnoff`) CSP wheel strategy is carrying
**48 open positions / ~$1,315,399 notional cash-secured-put exposure** against a cash base that's
either $12,880.20 (`ai_players.cash`, explicitly decoupled from CSP accounting since HM-W1F4
2026-05-17) or a shared $73,380.21 fleet pool (`options_books.fleet.current_cash`, split across
`options-sosnoff` + `strategy:bull_spread_v1` + `swingdesk-manual` — no clean Troi-only slice).
Either way, notional secured is 16x+ the smaller figure. Trade-level performance itself is fine
(100% win rate closed, 36/36, +$7,972.21 realized) — this is a position-sizing control failure, not
a strategy problem.

**ROOT CAUSE:** `engine/wheel_strategy.py`'s `MAX_POSITIONS = 3` cap and its "skip if already held"
check both read `get_portfolio(PLAYER_ID)["positions"]` (`engine/paper_trader.py:540`), which queries
the stock `positions` table. But `open_options_trade()` (`engine/options_exec.py`) writes CSP legs
only to `options_trades`, never to `positions`. So every scan sees **zero** existing option
positions and **zero** held symbols, no matter how many are actually open — the cap and the dedup
are both silently dead code for options. `open_options_trade()` itself has no position-count or
already-held check of its own (only the HM-DOOR1 leveraged-ETF blocklist gate).

**Evidence — same-symbol, same-day stacking with no cap in sight:**
- 2026-06-11: 6 SOXL entries, 6 UPRO entries, 6 TQQQ entries — all in one day.
- 2026-06-12: 4 each (SOXL/UPRO/TQQQ). 2026-06-08 and 2026-06-09: 3 each.
- Current open book: 18 SOXL + 18 UPRO (all entered ≤2026-06-12, pre-Door1) + 6 QQQ + 6 SPY (all
  entered ≥2026-06-23, post-Door1 pivot to non-leveraged underlyings) = 48 total, vs the coded cap
  of 3.

**Severity: HIGH, not yet realized as loss** — all 48 open positions are currently well OTM (100%
win rate holds so far), so no live damage. But the control that's supposed to bound concentration
risk does not function, and nothing else in the path (`open_options_trade`, the scan loop) enforces
a ceiling. A VIX spike + a bad multi-day stretch could stack far more exposure than the strategy was
ever sized for.

**RECOMMENDED FIX (focused session):** `wheel_strategy.py`'s cap/dedup logic needs to read open CSP
count from `options_trades` (`WHERE agent_id='options-sosnoff' AND status='open'`, grouped by
symbol) instead of — or in addition to — the stock `positions` table. Until fixed, `MAX_POSITIONS=3`
is not a real constraint on this agent. No live risk currently (VIX-gated dormant since 2026-07-02,
zero new entries since 2026-06-29) but will resume stacking the moment VIX clears `MIN_VIX=18`.
Full diagnostic: HM-TROI-DEEPDIVE-2026-07-03 (this session).

---
## 🆕 2026-05-31 SESSION — filed retroactively (was git+memory only; backlog was stale-by-omission)

### ✅ HM-GEX-CANONICAL — single GEX source; 3 legacy GEX systems RETIRED (2026-05-31)
≥3 GEX displays disagreed (overlay walls 700/800 vs gamma-weighted 750/760; #1 self-contradicting regime).
**Canonical = `engine/options_flow_gex.py` (Polygon, gamma×OI, BS-re-gamma flip, ±20%/≤60DTE band).** All
Bridge GEX endpoints now reshape ONE source via `dashboard/app.py::_canonical_gex` (intraday cache →
flow_gex.db daily row → live compute): `/api/gex-snapshot`, `/api/gex/{symbol}` (#1/#5), `/api/gex-overlay/levels`
(#2), `/api/market/gex/{ticker}` (#4). Verified consistent: flip 754.01, walls 750/760, regime "stable (above
flip)". Intraday refresh: `main.run_gex_snapshot_refresh` every 15m RTH → in-process cache; daily-close
flow_gex.db write stays the validation series.
**RETIRED (dormant, code+DB tables PRESERVED — do NOT delete):**
- `engine/gex_scanner.py` (CBOE delayed) — job `run_gex_refresh` DISABLED (main.py).
- `engine/gex_calculator.py` (Alpaca) — job `run_alpaca_gex_refresh` DISABLED.
- `engine/gex_overlay.py` (CBOE OI / king-node DB) — job `run_gex_overlay_update` DISABLED; `/api/gex-overlay/heatmap`
  + the symbol-detail `gex_levels` (app.py) REPOINTED to `_canonical_gex` 2026-05-31. **gex_overlay now has ZERO live
  refs in app.py — fully dormant.** Consolidation 100%: every GEX route (heatmap included) resolves to the single source.
Commit 5f04271→(this). Browser-smoke to auth boundary only (dashboard 2FA-gated); Admiral does final visual.

### ✅ HM-PRODUCER-RETIRE — 2 legacy signal producers RETIRED (2026-05-31)
Diagnosis of the signals-feed silence (no new `trade_signals` since 2026-05-23) = **reboot-survival-gap**: the
2026-05-23 SSH-only reboot killed the last two live producers (`com.ollietrades.etfregime` @06:35 + `.optionsflow`
@07:00), launchd `gui/501` jobs that never re-bootstrapped. Consumer check proved the `signals` table is
**write-orphaned for trading** (neo-matrix consumes it observation-only via `exit_only`; short path + Holly A/B read
independent sources — nothing that trades goes dark). Both producers lived **only** in the deprecated
`/Users/bigmac/ollietrades` tree (no autonomous-trader copy). **Retired, not revived:**
- **options_flow RETIRED** — superseded by **HM-FLOW-NATIVE**.
- **etf_regime RETIRED** — legacy; **10d-edge rebuild candidate gated on HM-VALIDATION-RIGOR deflation** (W0 showed
  etf_regime_trader +0.997R @10d, n=33 — undeflated/thin, do not act on raw).
Actions (data preserved, archive-never-rm): plists → `~/Library/LaunchAgents/_archived_2026-05-31/`; scripts →
`/Users/bigmac/ollietrades/_archived/`; `trade_signals` + all .db **untouched** (W0 research substrate). W1 registry:
both marked `criticality=retired` → `/api/sources/health` reads **RETIRED** (not RED-fault); `signals` demoted
`live_decision→context` so consensus won't flag degraded on their absence.
**⚠️ HOLD:** consensus gate hook (`engine/consensus.py`) is coded but NOT live (trader not restarted). Activating it
needs the trader restart — **held for explicit go**. Note: once restarted, `riker_synthesis` (UNKNOWN, live_decision,
pre-existing) would still independently flag consensus degraded — separate W1 follow-up (riker ts_format resolution).

### 🟢 HM-HOLLY-WORKS — LIVE / racing (commits → cfe53bf)
The "faithful ~60 documented TI strategies, intraday-flat" frame is **DISPROVEN**. Rebuilt around what's
**OOS-validated**: works-set = **the_continuation** (OOS Sharpe 1.47, 58% WR, +5.6%/6wk; tuned 8%stop/6%tgt/
20d-hold) + **count_de_monet** (marginal, OOS Sharpe 0.59). Per-strategy exit regimes (`TI_EXIT_TYPE`:
momentum→swing, mean-rev→flat — the no-EOD-flat experiment proved momentum needs overnight holds). **180-day
regime test (Dec1→May29): the_continuation is BULL-ONLY** — +1.57%/trade 66%WR in bull, ~0 edge (+0.10%/trade)
& −88% drawdown path in bear; 95% of return from bull → motivated the regime gate. `engine/holly_intraday.py`
(HOLLY_WORKS), `engine/holly_live.py`, `scripts/holly_live_cron.sh` (live `*/15 13-20 * * 1-5`). Supersedes
the stale "HM-HOLLY-FAITHFUL Phase 1" task. **Status: LIVE A/B vs ollie-auto ($10k each, internal book).**

### 🟢 HM-HOLLY-REGIME-GATE — LIVE, awaiting first real bench (commit cfe53bf)
Entries-only gate on holly-scanner: benches the_continuation in CAUTIOUS_BEAR/BEAR_CROSS/CRISIS, trades in
BULL_CROSS/CAUTIOUS_BULL. Reads BOTH the fleet source (`_get_regime_from_8080`) AND regime_history (union —
catches CAUTIOUS_BEAR the fleet source can't distinguish). **Promoted shadow→LIVE.** Exits NEVER gated (no
position thrashing). NTFY to ollietrades-admin on the first real bench (deduped + 6h cooldown). Currently BULL
→ trading. **Status: LIVE; first real bench is the confirm trigger.**

### 🟢 HM-EXTERNAL-INTEL — live / capturing (commits b467491, f719e2f, 2683d18, 44b089c)
Captures the Admiral's pasted/forwarded intelligence. **Tier-1** (structured picks): TI Swing Picks → 32 in
external_picks; follow-TI shadow **+3.45%/pick, 53% WR** (tracked, not traded); watchlist; daily snapshot cron.
**Tier-2** (prose): 15 rows in external_intel_text; TrendSpider capture (ad-strip + theme/ticker extraction);
**eM Client forward-bug fixed** (was silently SKIP-dropping all forwards) + 14-row backfill of historical prose.
Dual ingestion: hourly email poller (OllieTradeMinds@gmail, eM Client auto-forward) + paste-box (`/api/intel/
paste`). Dashboard panels (browser-smoke passed). **Closes HM-TI-NEWSLETTER-CAPTURE** (its open ingestion
question is answered + built). **Status: live. Tier-2 features stay OUT of live gating until OOS-proven.**

### 🟡 HM-LESSON-VALIDATOR — SHADOW, awaiting first verdict (commits 8c9835e, 8de2ded)
Culling loop for the FinMem Reflexion lessons: parse → {ticker,regime,action}, scan decision_audit forward-only
→ followed-vs-ignored + counterfactual; CULL demonstrably-harmful (≥5 tests + significance margin), don't anoint
winners at N=5. **SHADOW-ONLY** (logs to lesson_validation_shadow, never touches agent_memory). Daily cron. NTFY
to ollietrades-admin on first verdict. **All 85 lessons PROVISIONAL** (n=0 forward tests yet — correct/conservative).
**Status: shadow; first non-provisional verdict → NTFY → Admiral promote decision.**

### 🟡 HM-OLLIE-LEARN — Phase 1 done (negative), Phase 2 shadow/parked (commits 0a9ebdc, 4872ad7)
Phase 1 rule-optimizer: OOS-validated, found **NO threshold change beats the static 2.0 gate** (OllieScore
clusters ≥2.0, re-thresholding inert) → 2.0 stays; kept as nightly-check infra. Phase 2 GB learned-gate: trained
on ~540 scoreable decisions, **OOS AUC 0.534 — no edge** (CV 0.674 was regime base-rate); SHADOW-only, never
gates live. **Status: both shadow/parked. Revisit Phase 2 only as regime diversity / corpus grows.**

### 🔵 HM-HOLLY-ENTRY-FIDELITY — DEFERRED (memory: project_hm_holly_entry_fidelity)
17/19 documented TI strategies fail OOS even with correct exit regimes (generic triggers — "close>20-bar-high" —
not real Holly setups; 33-37% WR). Rework entries toward real setup conditions to grow the works-set beyond the
2 validated. **Status: DEFERRED, hard + uncertain payoff. LOW-MED. Gated behind the the_continuation A/B baking.**

### 🟢 HM-SHORT-GUARD-ELITE — SHIPPED earlier 2026-05-31 (Stage-2 commit)
Stock-shorting activation with the Finviz Elite short guard: SI%>20 (authed Elite export) + DTC>5 (Polygon) +
earnings≤3d (Finnhub), fail-CLOSED, Option-B graceful degrade (Elite-down → DTC+earnings, never skip-and-allow).
8% hard buy-stop, 10%/position + 20% aggregate caps, 3 authorized agents. **Status: SHIPPED, SHORT_ENABLED=True.**

### 🔵 HM-BM-BAKEOFF — SPEC, gated behind Plutus v6 (spec d296e6c, `drafts/HM-BM-BAKEOFF-SPEC.md`)
One-shot 4-candidate Plutus **model-selection bakeoff** (stratified 100-trade corpus + outcome-aligned hybrid
scoring). **NEVER RAN — spec explicitly "do not execute until Plutus v6 lands (mid-June)."** NOT a recurring
monthly audit (earlier half-memory was wrong). Falls back to 4 candidates if v6 isn't ready. **Status: 🔵 spec,
mid-June, gated on HM-PLUTUS-V6. Blocked-on: v6 train.** (UNBLOCKED by HM-PLUTUS-PURPOSE 2026-05-31 — a fine-tune
now serves the witness, so the bakeoff has a live consumer to select for; still timing-gated on v6.)

### 🔵 HM-PLUTUS-V6 — SPEC, corpus not built (spec e5f46cf, `drafts/HM-PLUTUS-V6-CORPUS.md`)
Next-gen Plutus fine-tune on a substantially larger corpus; **target mid-June 2026 train.** Corpus NOT built
(`data/` tops out at `plutus_corpus_v5.jsonl`). Train on the RTX 5080 pinned env (NOT Ollie Max). **Status: 🔵
spec, not-started.** (UNBLOCKED by HM-PLUTUS-PURPOSE 2026-05-31 — the serve-path is no longer the question; v6's
output (register as `plutus-v2`) now has a live consumer: swap `ai_players.ollama-plutus.model_id` → `plutus-v2`.)
> ⚠️ **VERIFIED 2026-05-31 — the fine-tune is NOT serving (escalates HM-MODEL-CONFIG-STALENESS):** the v5-win doc
> claims "McCoy now runs the trained model instead of stock 0xroyce/plutus," but `ai_players.ollama-plutus.model_id
> = 0xroyce/plutus` (**stock**). Ollie Max `/api/tags` confirms the fine-tunes (`plutus-v2:latest` 4.68GB, modified
> 2026-05-27 23:33 + v1/v1-pinned) sit on the box **unwired** while stock `0xroyce/plutus:latest` (5.73GB) is what
> McCoy points at. So the v5 fine-tune we trained **is not deployed.**
> **CHASED 2026-05-31 — definitive, no override exists:** traced every Plutus path. (1) McCoy's **trading decisions
> are DETERMINISTIC** — scan routes to `crew_scanner.mccoy_rules` via `_scan_rules_agent` ("No Ollama call"), a
> VIX-tiered rule function. No LLM, so model_id is irrelevant to McCoy's trades. (2) The **only** actual Plutus LLM
> inference is `debate_engine.run_plutus_witness` (expert-witness step in 12-agent debates writing `debate_history_v2`)
> and it is **hardcoded** `call_ollama(..., "0xroyce/plutus", ...)` (debate_engine.py:622) — **stock**, doesn't even
> read `ai_players.model_id`. (3) The startup banner "McCoy=ministral-3:3b" (main.py:3348) is a **hardcoded stale
> literal**, not the resolved model. **CONCLUSION: nothing serves any fine-tune — the trained plutus-v1/v2 tags are
> fully unwired; the lone Plutus brain-call hardcodes stock.** v6 is **moot under current wiring**: McCoy trades on
> rules (a better model changes nothing) and the debate witness would need its hardcode repointed to even use one.
> **BEFORE spending on v6, decide what a fine-tuned Plutus is even FOR** — wire it into a decision path, or accept
> McCoy is a deterministic rules agent and retire the fine-tune track.

> **PHANTOM-CALENDAR RECONCILE (HM-PHANTOM-RECONCILE, 2026-05-31) — forensic-verified, all 4 resolved:**
> The 4 items I'd carried as "tracked" were **directionally real but stateful-wrong.** Verdicts: **HM-BM + Plutus-v6
> → FILED above** (real, spec-gated, mid-June — not recurring, not stale). **Polygon WS/VX → DROPPED/DONE:** resolved
> 2026-05-27 (realtime pivoted to Alpaca IEX; REST on current `v2/aggs` + `v3/reference`; **NO sunset deadline exists**
> in any doc — the "pre-Jun-22" deadline was misremembered; not at data-break risk). **PDT-rule 2026-06-04 → DROPPED:**
> MOOT — Alpaca **paper only**; PDT applies to real margin <$25k; no code keys off the date (only trace is a 6-wk-old
> `crew_scanner.py:188` comment). Only *filed* 06-04 item remains the *Worf bench review*.

> **AGGREGATOR STATUS (clarifies HM-DALIO + HM-TRACKING adoption):** the core fix **HM-TRACKING-AGGREGATOR IS
> SHIPPED** (eb2886e, ~22 rollup sites, `CLEAN_TRADES_WHERE` excludes tracking-route players incl. dalio-metals) —
> the clean aggregator used all this session IS that fix and it WORKS. The two "open" items are **residuals, NOT
> the last mile of the same fix**: (1) the dalio 18 polluted raw rows are already EXCLUDED by the live aggregator
> (don't surface in clean rollups) — only the raw DB rows are still wrong = a sacred-data correction (RED, staged-
> await-go), LOW urgency; (2) the stricter `alpaca_order_id`-boundary `trades_clean` view has zero readers = an
> optional refinement, not the core fix. **The aggregator fix is functionally complete.**

## 🆕 HM-BACKLOG-ADD — comparison candidates (2026-05-31, file-only · do not build yet)

### ✅ DECISION GATE — RESOLVED 2026-05-31 (ruled: WIRE)
- **HM-PLUTUS-PURPOSE** — ✅ **RESOLVED 2026-05-31 (e3c396d): wired, not retired.** `run_plutus_witness`
  (debate_engine.py:622) de-hardcoded → `_resolve_plutus_model()` reads `ai_players.model_id` (fail-safe → stock),
  so a model swap is now a DB change not a code edit. `ai_players.ollama-plutus.model_id` set to the canonical
  trained tag **`plutus-v1`** (digest 4bea908c0348 == plutus-v1-pinned; the HM-PLUTUS-V5-WIN production fine-tune —
  NOT `plutus-v2`, which is RESERVED as the v6-bakeoff slot). config.py:175 already = plutus-v1 → config+DB+witness
  agree. main.py:145 (T'Pol) left alone (separate path). **WATCH (not blocking):** plutus-v1 unvalidated in the
  witness role; verdicts log to `debate_history_v2.plutus_analysis` — compare fine-tune vs stock over next debates
  (HM-VALIDATION-RIGOR formalizes). **UNBLOCKS HM-PLUTUS-V6 + HM-BM-BAKEOFF** (a real fine-tune is now served, so
  both have a live consumer to improve/select for).

### 🟡 NEW EDGE (Polygon-native, data already owned)
- **HM-FLOW-NATIVE** (P2, HIGH) — unusual-options-activity classifier from Polygon options trades: sweep/block,
  opening/closing (OI delta), at-ask/bid, premium≥$250K; DROP spread legs (reuse `is_spread_leg` from HM-AF) +
  delta hedges. Feed crew as a scored signal; convergence = flow + technical confirm. #1 named retail edge —
  rivals show raw data, don't classify; classification is our advantage.
- **HM-GEX** (P2, HIGH 0DTE/SPY) — dealer gamma from options OI+greeks; gamma walls + flip point on dashboard.
  Polygon-native.

### 🟡 RIGOR (pure software; upgrades live selection systems)
- **HM-VALIDATION-RIGOR** (P2) — Deflated Sharpe + PBO via CPCV + trial-count penalty, wired into **BOTH** the
  Holly-race winner-selection **AND** HM-BM scoring (both are selection-bias today; raw Sharpe is in-sample-
  inflated). Guardrails: t-stat≥3.0, slippage stress 0.1–0.3%/round-trip, drawdown-clustering, size to P95
  drawdown. **SUBSUMES** the deferred cross-validation + Tier-2-OOS items.

### 🔵 AGENTIC (debate engine ALREADY EXISTS — file only the delta)
- **HM-CONSENSUS-WEIGHTING** (P3) — debate engine (bull/bear + Picard + `run_plutus_witness`, `debate_history_v2`)
  is already live. ADD ONLY: selective-consensus weighting — discount divergent / temporally-inconsistent inputs
  on confidence tiers. **Do NOT rebuild debate.**
- **HM-SIGNAL-PASSTHROUGH** (P3, small) — verify crew/debate → advisory handoff doesn't collapse a detailed
  scored report into a bland summary; add pass-through if needed.
- **HM-LESSON-GRADUATE** (P3, small) — define the shadow→live criterion for lesson-validator (N sessions of
  verdicts matching outcomes). Pairs with the shadow HM-LESSON-VALIDATOR filed above.

### 🔵 INFRA STANDARD
- **HM-HEARTBEAT-LAYERED** (P3) — generalize Dr. Crusher / Tractor monitoring beyond PID: broker-connected,
  LLM-connected, data-fresh, decision-recent. HM-RUN-SCAN-WATCHDOG is the first instance.

### ⏸ DEFERRED (cost-gated)
- **HM-DARKPOOL-FEED** (deferred) — true off-exchange print feed; only if the spend proves worth it.
  Flow + GEX stand alone without it.
---

> **HM-TRACKING-AGGREGATOR — ✅ SHIPPED 2026-05-30 (eb2886e).** Two-predicate clean-trades boundary
> (`executed_at >= '2026-05-14' AND player_id NOT IN tracking`) via new `engine/trades_filter.py`, adopted at ~22
> realized-PnL/WR rollup sites (proving_ground on date-floor-only `SIM_EVAL_WHERE`). **Verify-the-verifier PASSED
> post-restart (PID 78251):** fleet realized **$237,423 → $286 live** (was pure pre-05-14 garbage), dalio trades-PnL **0**,
> 10 sim agents preserved (202-trade fleet proves aoid-global would've wrongly erased them). Retires the dead
> `known_contaminated` flag + cosmetic `trades_clean` view as the canonical boundary. Map: `docs/TRACKING-AGGREGATOR-SITE-MAP-2026-05-30.md`.
> **Two orphans filed separately (NOT folded into the filter):**
> - **HM-EQUITY-CURVE-ORPHAN (LOW)** — ✅ **RETIRED 2026-05-30 (commit `9d5986f`).** Verified 0 fetch
>   consumers + 0 internal callers; removed `get_equity_curve`/`/api/equity-curve` from `dashboard/app.py`,
>   archived to `archive/retired/2026-05-30-equity-curve-orphan/get_equity_curve.py`. py_compile clean. The
>   separate live `/api/arena/equity-curve` (def `equity_curve`) is distinct and untouched.
> - **HM-BENCHMARK-DB-MISMATCH (MED)** — `engine/benchmark.py` writes `benchmark_snapshots` to `autonomous_trader.db`
>   but reads fleet PnL from `data/trader.db` → benchmark/Sharpe-vs-SPY data is silently wrong (snapshots land in a
>   different DB than the source). Fix the DB constant or document the split.

> **DAEMON GRAVEYARD (ALL-OUT-AUDIT-2026-05-30) — Phase 1 + 1b APPLIED 2026-05-30 (BOTH safety monitors restored):**
> Phase 1 = watchdog re-homed launchd→cron (`scripts/watchdog_supervisor.sh` `*/5`, plist retired, observed running,
> alarm layer BACK — caught cloudflared-down in 3 min). Phase 1b = healthcheck re-homed (plist retired, cron `0 6-13`,
> restart path HARDENED via trader_restart.sh d66b297, observed firing + correctly left healthy trader alone, single-writer 1).
> Both now route trader-restart through the orphan-proof path (healthcheck live; watchdog pending repoint — see flock blocker
> below). Full diagnosis + tiered plan: `docs/DAEMON-GRAVEYARD-REHOME-PLAN-2026-05-30.md`.
> **Two follow-ups filed (both = route restart through `trader_restart.sh`):**
> - **HM-HEALTHCHECK-RESTART-HARDEN (HIGH)** — `healthcheck.py::restart_server()` uses naive `pkill -9 main.py` +
>   `launchctl load` (headless→unreachable gui/501→can down-and-not-restore). **healthcheck stays DEFERRED/OFF until
>   this is rewritten to call `trader_restart.sh`** (orphan-proof single-writer gate). Falsifiable trigger: restart_server
>   invokes trader_restart.sh + a forced restart leaves exactly 1 writer.
> - **HM-TRADER-RESTART-FLOCK (HIGH) — ✅ SHIPPED 2026-05-30 (0f1c2cf).** flock absent on macOS → portable mkdir-atomic
>   mutex (`/tmp/uss_trader_restart.lock`, PID-liveness + age-guard staleness, trap-cleanup) at the top of
>   `trader_restart.sh`. 2nd concurrent caller ABORTS exit-4 BEFORE the kill step. **Thrash-test PROVEN:** loser-isolation
>   (lock pre-held by live pid) → restart aborts exit-4, kill never reached, trader PID UNCHANGED; 2×-concurrent →
>   one exit-0 (restart) + one exit-4 (abort, 0 kills), single-writer 1, exactly 1 trader, lock self-cleaned. The gate's
>   "admit one" is now ENFORCED. **This UNBLOCKS HM-WATCHDOG-RESTART-REPOINT.**
> - **HM-WATCHDOG-RESTART-REPOINT (MED) — ✅ SHIPPED 2026-05-30 (806ff99).** `watchdog.py` now calls `restart_trader()`
>   → `/bin/zsh scripts/trader_restart.sh` (orphan-proof, lsof-by-handle, single-writer gate, flock-serialized) instead of
>   the stale `launchctl_kickstart("com.trademinds.trader")`. Handles flock exit-4 gracefully (defers to a concurrent
>   healthcheck restart). **Verify-the-verifier PASSED:** killed the live trader → watchdog 3-strike grace (180s) →
>   "Restarting via trader_restart.sh" → flock lock acquired → RESTART OK, new PID, single-writer 1, orphan-free, HTTP 200.
>   Watchdog can now ACTUALLY heal the cron-launched trader (was alarm-only). **Safety arc complete:** both monitors
>   (watchdog + healthcheck) route trader-restart through the flock-mutexed orphan-proof path — two actors can't double-spawn.
> - **NEW (surfaced live):** cloudflared tunnel DOWN (no process; remote-access tunnel). Was a 05-23 cron @reboot
>   service — died mid-session, nothing restarted it (same reboot-survival-gap, mid-life variant). Captain decision: restart if remote access wanted.
> - **Phase 2 APPLIED 2026-05-30:** ✅ **ghost-advisor** re-homed (cron `*/10`, plist retired, observed firing — 172-decision
>   backlog pending its 1st live run after 7d dead) + ✅ **metals-sync** (cron `6:15`+`13:10`, plist retired, observed —
>   updated XAU/XAG live). Neither touches trader.log; single-writer stayed 1.
> - **Phase 2b APPLIED 2026-05-30:** ✅ **morningbriefing** re-homed (cron `0 6`, plist retired, import-smoke clean — full
>   audio/NTFY firing = next 0600 cron; fixes morning_brief.json stale-since-05-29 gap) + ✅ **sitrep** re-homed (cron
>   `6:30`/`10:00`/`13:30`, plist retired; reads trader.log via `tail` = single-writer safe; the "py3.9/PEP604 risk" was
>   FALSE — no PEP604 syntax, compiles under 3.9). **9 cron-managed daemons total; all script paths absolute** (caught +
>   fixed a relative-path bug: cron cwd=$HOME would've failed `engine/morning_briefing.py` at 6 AM).
> - **squeeze-scan + ollie-scan — RETIRED (plists renamed, verify-before-code catches):** both superseded by LIVE in-process
>   daemons — squeeze by `_bg_squeeze_watcher` (main.py:1731, 1103 hits, /api/squeeze serving); ollie-scan by `run_scanner`
>   (main.py:360, scheduled, `run_scanner wall=60s` actively running, the §C scan path). Cron-restoring either = double-scan
>   on rate-limited finviz/yfinance. NO cron; plists retired (kills latent double-run vector).
> - **Phase 2c APPLIED 2026-05-30 — ALL THREE RESTORED (observed-firing):** ✅ **uhura** (cron `5:30`; SEC 13F/Form-4
>   scraper — observed: 29 insider signals) · ✅ **real-portfolio-snapshot** (cron `13:45`; observed: snapshotted real
>   Schwab book $27,734.51) · ✅ **fleet-auditor** (cron `*/15`; manifest IS consumed → `dashboard/app.py:3469` endpoint +
>   NTFY transition-alerts, NOT cosmetic — observed: refreshed the 7-day-stale manifest). None touch trader.log; single-writer held 1.
> - **🏁 DAEMON GRAVEYARD — CORRECTED 2026-06-10 (HM-HARDEN A2, disk-verified).** The earlier
>   "ARC COMPLETE / 12 cron-managed daemons restored" line was **aspirational, not disk-true** —
>   the live `crontab -l` had NONE of the survival crons. HM-LEDGER (`docs/MASTER-LEDGER-2026-06-10.md`)
>   caught the claims-vs-disk gap; HM-HARDEN A2 closed the real gaps. Disk truth as of 2026-06-10:
>   - **Re-homed IN-PROCESS (N/A for cron — do NOT double-run):** morningbriefing (`main.py:4064` 06:00),
>     metals-sync→`run_metals_commentary` (`main.py:4597`), reveille (`main.py:4067` 05:45).
>   - **Re-homed to CRON 2026-06-10 (HM-HARDEN A2):** fleet-auditor (`*/15`), sitrep (06:30/10:00/13:30),
>     uhura (05:30), real-portfolio-snapshot (13:45). All `.venv` interpreter, absolute paths, `cd` to repo.
>   - **STILL DEFERRED (restart-capable; restarts held for post-window — install in a later batch):**
>     watchdog (`watchdog.py::restart_trader` can bounce the trader) and healthcheck (doctrine: OFF until
>     `restart_server()` is rewritten to call `trader_restart.sh`).
>   - **Retired (superseded/dead, unchanged):** ghost-advisor, squeeze-scan, ollie-scan, crusher, scanner,
>     optionsflow/etfregime/movers. Root cause banked: [[feedback_reboot_survival_gap]].
>   **py-version note:** venv/bin/python3 IS 3.9.6 (same as /usr/bin/python3); a venv swap does NOT fix PEP604.
>   HM-HARDEN A2 uses **.venv (3.14)** for the new crons — the canonical maintained interpreter, full deps.

> **Closure-sweep result 2026-05-29** (verify-before-fix audit of standing tickets):
> - **CLOSED (shipped, were queue-rot):** HM-ALERT-AUTH-STORM (90544a6, 2026-05-23), HM-DATA-INTEGRITY-FORENSICS (sub-tickets shipped 2026-05-25).
> - **RE-SPEC'd:** HM-DEEPSEEK-CONCENTRATION-CAP-V2 → standalone preventive cap LOW (deepseek already active, 0 positions; "prereq for unhalt" was stale).
> - **6 REAL standing items:** HM-RISK-MANAGER-CONVICTION-STOP (unblocked — precursor met, gated on ~57% NULL backfill + flag-enable), HM-SCHWAB-CROSS-MECHANISM-ALARM (still shared-cron fate), HM-TRADES-MIRROR-GAP (P0 prereq shipped → measure current gap), HM-ALPACA-BRIDGE-LIMIT-FIX (maintenance window), HM-QG-FLOAT-TRUNCATION (LOW), HM-CONVICTION-TIER-BOUNDARY (Admiral-gated decision).
> - ~33% of the standing backlog was not-actually-open.

---

## 🟢 HM-RESTART-ORPHAN-PREVENTION — SHIPPED 2026-05-30 (was HIGH) — operational hazard CLOSED

**The restart procedure can spawn ORPHAN traders.** 2026-05-29: a process started 15:15 froze the
listener-free but **kept running its scan loop** after a later restart took the port — two traders ran
in parallel **2.6 hours** (PID 29543 orphan + the live listener), double-scanning + double-signalling,
and the orphan (OLD code) polluted the shared `trader.log` with `deepseek:infer` lines that made a
*correct* §C-close fix look failed → triggered a multi-restart phantom chase. **Root cause:** the
restart pattern `kill $(lsof -tiTCP:8080 -sTCP:LISTEN)` only kills the **listener-holder** — an orphan
that already lost the listener survives. **"Port freed" ≠ "process dead."**

**Fix (harden the restart procedure — real fix, not just doctrine):**
1. Kill ALL trader processes: `pkill -f "main.py"` (or enumerate `ps | grep main.py` — note the binary
   is `Python` capitalized, so naive `grep python.*main.py` MISSES it; match on `main.py`).
2. After relaunch, **verify single-writer**: `lsof logs/trader.log` must show exactly ONE Python PID.
   If two → an orphan survived; kill it before declaring restart complete.
3. Bake into `scripts/trader_reboot_start.sh` + the manual restart runbook.

**SHIPPED 2026-05-30:** `scripts/trader_restart.sh` — kills ALL trader.log WRITE-holders (orphan-proof; write-mode filter spares `tail -f`/grep readers), SIGTERM→SIGKILL escalation, then a hard SINGLE-WRITER gate (fails loudly if >1, exit 2). **PROVEN**: test spawned a dummy orphan + a reader, ran the script → both real trader + orphan killed (DEAD-OK), reader survived (ALIVE-OK), single-writer gate passed (1 writer). Use this for ALL manual restarts; the @reboot script correctly bails-on-existing and is unchanged.

**Priority HIGH:** on any real-money posture a double-running trader = **duplicate orders**. On paper
it's account-harmless but it corrupted hours of measurement today. (Pairs with the restart-verification
doctrine already in CLAUDE.md — extend it from "new-PID-bound" to "single-writer-confirmed".)

## ⚠️ DATA SANITY-FLAG 2026-05-29: orphan double-run window 15:15-18:0x may have INFLATED magnitudes

A 2.6h orphan double-run (see HM-RESTART-ORPHAN-PREVENTION) means today's **absolute counts** may be
~2× inflated for that window: the 100%+ CPU readings, deepseek's "~100 sigs/day flood", and some
HELD-INFLIGHT magnitudes. **The CONCLUSIONS hold** (deepseek's redundant arena path is real; the
spike-bugs — catalyst/indicators/whisper/quote_summary — were real). But future analysis (esp. the
agent-review clean-window re-assessment) must NOT trust today's absolute magnitudes from the
15:15-18:0x window blindly; re-measure post-orphan-kill. Banked so the inflation isn't mistaken for signal.

## 🟡 HM-MODEL-CONFIG-STALENESS — MED (filed 2026-05-30) — config.py model fields stale vs canonical DB

**Finding (floor-math investigation 2026-05-30):** `config.py` AI_PLAYERS `model` fields are STALE vs the
canonical `ai_players.model_id` (DB), the runtime source-of-truth per HM-BN doctrine ("enforces canonical
model_ids on startup", main.py:121). Confirmed:
- `ollama-plutus` (McCoy): config `plutus-v1` → **DB/runtime `0xroyce/plutus`** (finance brain).
- `ollama-qwen3` (Dax): config `qwen3:8b` → **DB/runtime `ministral-3:3b`**.
**Verdict: DELIBERATE (DB canonical); config.py just never updated.** NOT a runtime bug — but it MISLED the
§C floor-math (trusting config.py gave wrong per-unit costs). **Harm:** any analysis reading config.py for
the live model is wrong. **Fix:** update config.py `model` to match the canonical DB, OR comment that
`ai_players.model_id` is authoritative and config.py `model` is informational-only. Likely >2 agents affected
(HM-BN made the DB canonical fleet-wide; config wasn't swept). Low-risk doc/config sync. (Drift Catalog #1.)

**Re-hit 2026-07-04 (HM-FLEET-REBASELINE):** same trap, different analysis. Drafting the fleet-core doctrine
claim off agent id/display_name ("ollama-qwen3", "mlx-qwen3") nearly banked "fleet core = Qwen family" —
both actually run `ministral-3:3b` per live `ai_players.model_id`; only options-sosnoff runs true `qwen3:8b`
among the top-5 guarded-honest performers. Caught before it was written to DOCTRINE.md, not after. **The
comment-only fix in config.py obviously isn't sufficient — it didn't stop this recurrence.** Escalating the
fix recommendation: LOW-priority follow-up to either (a) sync config.py `model` fields to match `model_id`
outright, or (b) add a small `get_live_model(player_id)` helper (reads `ai_players.model_id`) that analysis
scripts/doctrine-writing sessions are expected to call instead of reading `config.py`/id/display_name — a
comment nobody re-reads mid-analysis isn't load-bearing enough given it's now failed twice.

**HM-FLEET-REBASELINE-2026-07-04 follow-up items (file-only, do not build without Admiral review):**
- **gemini-2.5-flash full-halt deferred:** meets retirement criteria (guarded return 8.93% <9%, spam
  54.5%>48%) but has 1 open position (IREN) and `halt_mode='full'` blocks sells (`exit_only` doesn't).
  Flip `exit_only`→`full` once IREN closes (natural exit or guardian-of-forever sweep) — do NOT flip
  while a position is open, it would strand the position with no close path.
- **Kill-gate reminder:** the July 24 2026 G1-G4 Door-1 kill-gate verdict (`project_door1_kill_gate`
  memory) must be read against THIS sweep's guarded-honest baselines, not the pre-friction Season 6.3
  numbers (OOS Sharpe 2.692 etc. — those predate the reentry/cost-model guardrails and are stale for
  gate-decision purposes as of 2026-07-04).

## 🚨 GATE 0 — FLEET PERFORMANCE NOT ASSESSABLE PRE-2026-05-14 (data-integrity headline)

**Fleet-review 2026-05-29 finding (load-bearing for ALL roster decisions):** trade data before
**2026-05-14** is contaminated by the P0 price-writeback bug (internal price written, broker
`filled_avg_price` never read back). First real Alpaca fill: `2026-05-14 07:37:44`. **Trust the
`alpaca_order_id IS NOT NULL` boundary, NOT the `known_contaminated` flag** (the flag is incomplete
— caught 235 trades but missed ~$230K of garbage PnL in March alone, e.g. a `simulated` TSLA
$21.52→$396.83). **Only 2 agents have broker-real realized trades in the clean window: ollie-auto
(N=38), neo-matrix (N=18) — both low-N.** Every other agent has 0–17 clean trades (internal
`simulated` book by two-book design). **→ No perf-based keep/bench call is defensible this cycle
except WATCH; re-assess when the clean window grows past ~30 trades/agent.** Both clean agents show
+realized but −MTM (bank winners, hold losers) — realized WR is a selection artifact, not edge.

## 🟡 HM-CONTAMINATED-FLAG-INCOMPLETE — DIAGNOSED 2026-05-30 → recommend DEPRECATE

**ROOT CAUSE (2026-05-30 read-only):** `known_contaminated` has **NO detection logic** — the column exists
(setup_db.py:462-465 DDL) but **nothing computes/populates it**. The 235 flagged rows were set by ad-hoc
manual SQL scoped to 3 routed players (ollie-auto 190, neo-matrix 29, super-agent 16); one-time, no writer,
no anomaly logic. It missed ~$237K because the garbage is from **non-routed legacy/backtest agents**
(gemini-2.5-pro TSLA $21.52→$396.83 = id 179, +$117K, flag=0) — structurally out of scope. Quantified:
flag=0 simulated = **+$237,632** unflagged garbage vs flag=1 catching only −$333.
**RECOMMENDATION: DEPRECATE the flag, use `alpaca_order_id IS NOT NULL` as the authoritative clean/dirty
boundary** (first appears 2026-05-14 07:37:44, perfect pre/post separation: 0/2030 pre vs 118/342 post).
Broker ground-truth, auto-written on every fill, self-maintaining, one predicate — vs a manual one-time
player-scoped pass that misses 99.9% of garbage dollars. Already the stated guidance (XO_BACKLOG:65).
**Fix scope (RED — DB/view change, go-gated):** redefine the `trades_clean` view (DDL in
drafts/HM-F4-RECONCILIATION.md:18-22) to `WHERE alpaca_order_id IS NOT NULL` (subsumes the date+exec-type
filters; admits ~1 extra clean week since boundary 05-14 < the view's 05-21 floor). The dalio id=2539
tracking-route exclusion is ORTHOGONAL → handle via the `route_mode='tracking'`-aware aggregator (see
HM-DALIO-GOOGL-ZERO-EXIT), NOT by keeping the flag.

**⚠️ ALL-OUT-AUDIT-2026-05-30 CORRECTION — the view redefine already shipped, but it was COSMETIC.** The
`trades_clean` view now exists with the `alpaca_order_id IS NOT NULL` definition (118 rows) — **but it has ZERO
readers in code** (`rg trades_clean` → only the DDL/docs). Redefining a view nobody queries changed no PnL path.
**The REAL, still-OPEN work is ADOPTION:** repoint the actual realized-PnL rollups (brain_context.py, dashboard
WR%/PnL surfaces, scorecard) to key off the `alpaca_order_id` boundary (and exclude tracking-route players),
either by querying `trades_clean` or by inlining the predicate. Until a rollup READS the boundary, the
deprecation is paperwork. This is the same site-set as the dalio aggregator fix → **do them together.**
**Original ticket text below:**

## 🟡 HM-CONTAMINATED-FLAG-INCOMPLETE-orig — MEDIUM (filed 2026-05-29) — data-integrity-of-the-tool

The `trades.known_contaminated` flag is unreliable: it flagged 235 trades (ollie-auto 190, neo-matrix
29, super-agent 16) but **missed ~$230K of garbage PnL pre-2026-05-14** (March `simulated` shows
+$230,349 across 522 sells, 20 trades >$1K each, all `known_contaminated=0`). Either (a) fix it to
catch all pre-5/14 contamination, or (b) **formally deprecate it in favor of the `alpaca_order_id`
boundary** as the trustworthy clean/dirty discriminator. Recommend (b) — simpler + already proven.

## 🟢 HM-DALIO-GOOGL-ZERO-EXIT — ROW-FIX SHIPPED 2026-05-30; aggregator follow-up OPEN

**SHIPPED 2026-05-30:** row 2539 corrected (`realized_pnl=0, known_contaminated=1`; archived to
`data/archive/dalio_row_2539_pre-correction_2026-05-30.txt`). **BUT dalio total realized PnL is STILL −255.08.**

**ALL-OUT-AUDIT-2026-05-30 ENUMERATION (corrects the earlier "ONDS-sibling" guess):** the residual is **18
polluted rows**, NOT 2, and it is **AAPL-dominant, not ONDS**: AAPL id 1372 = **−229.48 (90% of the residual)**,
ONDS id 2545 = −91.05, GOOGL/QQQ small, partially offset by +DELL/+PLTR gains → nets −255.08. Neither AAPL nor
ONDS is "metals" — these are generic manual-cleanup sprays on a tracking-only player. **And `known_contaminated`
is INVERTED:** it is set on exactly ONE dalio row — id 2539, the one already fixed to 0 — and on NONE of the 18
rows still wrong. The flag points at the fixed row and ignores the pollution.

**→ This kills row-by-row whack-a-mole definitively (18 rows, AAPL-dominant, flag-inverted) and confirms the ONLY
sane fix = the `route_mode='tracking'`-aware aggregator** (exclude tracking-route players from realized-PnL rollups
— brain_context.py:273-275/452/530 + any dashboard sites). **OPEN follow-up (RED, multi-site, verified session):**
make the aggregator tracking-aware → zeroes ALL dalio/tracking pollution at once. See [[feedback_repeat_offender_bug_classes]]
(manual-SQL-cleanup-pollution class). Do NOT chase the 18 rows individually.

## 🟢 HM-DALIO-GOOGL-ZERO-EXIT-dx — DIAGNOSED 2026-05-30 (not a code bug; data + aggregator fix)

**ROOT CAUSE (2026-05-30 read-only):** `trades.id=2539` (dalio-metals GOOGL SELL, exit_price=0.0,
realized_pnl=−77.36) is a **MANUAL SQL worthless-expiry cleanup**, NOT a live code write. Its own
`reasoning` field is the provenance ("orphan expired option DTE-23... closed via worthless-expiry SQL
pattern... HM-MASTER-PLAN W2-D"). PROVEN not-a-code-bug: dalio-metals is `route_mode='tracking'`
(log-only) → `sell()`/`sell_partial()` short-circuit to `_log_signal_only` (paper_trader.py:1653-1655,
1922-1924, 486-512) which writes NO trades row. The $0.0 = deliberate "expired worthless" close; the
defect is it booked entry×qty (12.93×5.98≈77.36) as realized PnL against a **tracking-only player that
should carry ZERO realized PnL** (Two-Book doctrine). **PROPOSED FIX (not applied — RED, DB write):**
(1) `UPDATE trades SET realized_pnl=0, known_contaminated=1 WHERE id=2539` (sacred-data: correct, don't
delete); (2) make the realized-PnL aggregator **tracking-route-aware** (exclude route_mode='tracking'
players OR known_contaminated=1) — the durable fix; (3) audit the referenced ONDS legacy-shorts cleanup
for sibling rows. Other exit_price=0 rows (navigator ×4, dalio ×13) are legit OPEN entry rows (NULL exit),
not pollution. **Monday/Captain decision: the 2 SQL writes are RED → stage, await go.**

<details><summary>original</summary>

`dalio-metals` has a GOOGL trade with `exit_price=$0.0` polluting its realized PnL (−$91). Investigate
the `$0` write path (also seen on navigator ×4 exit_price=0 rows in spot-check). Likely a sell-price
fallback writing 0 when the fill price is unavailable. READ the write path before any fix.

## 🟢 HM-NAVIGATOR-SIGNAL-PATH-DEAD — DIAGNOSED 2026-05-30 (by-design omission, not a bug)

**ROOT CAUSE (2026-05-30 read-only):** navigator NOT-EMITTING into `signals` **by design** — not
emitting-but-not-recording. Commit `08cc0eb` (2026-04-12) re-homed navigator onto a TRADE-ONLY lineage
(`chekov_rules` in crew_scanner + `chekov_autotrade.py` convergence path), and the OLD `tractor_beam→
save_signal` emitter that wrote its 307 signals (all dated 2026-04-14, `sources='tractor_beam,...'`) was
**never carried into the imported codebase** → signals ceased 4-14. navigator still TRADES (37 since
5-14). `save_signal()` is called in **exactly ONE place: ai_brain.py:1282** (the arena LLM loop), and
navigator isn't in `_SCAN_TIER1/2/3` → never reaches it. **This is a FLEET-WIDE blind spot:** ALL
rules-scanners (capitol-trades, dalio-metals, holly-scanner, deepseek-as-rules, dayblade-0dte) trade but
write no `signals`. **PROPOSED FIX (not applied):** add a `save_signal()` hook to the crew_scanner rules
path (~crew_scanner.py:2766) mirroring ai_brain.py:1282 — restores `signals` coverage for ALL rules
agents. **CAVEAT (Admiral decision):** rules agents PASS far more than they trade → recording every eval
could FLOOD `signals`; decide scope (every eval vs only acted-on BUY/SELL) before wiring. RED-ish
(scan-path-adjacent + flood risk) → stage, await go.

<details><summary>original</summary>

`navigator` (Chekov) produces internal **trades** but has emitted **no signal since 2026-04-14**
(~6 weeks). Either it's emitting and signals aren't recording, or it stopped emitting while the trade
path lives. Investigate which — READ the signal-emission path vs the `signals` table writes before
concluding. (Note: navigator is in RULES_SCANNERS + crew_scanner — it IS scanning; the question is
the signal *recording*.)

---

## 🔴 HM-EXTERNAL-FETCH-DISCIPLINE-AUDIT — HIGH (filed 2026-05-29, promoted from MEDIUM/quarterly)

**Bug class: "unbounded external fetch on first cold caller, no caching, every caller re-pays."**
Now **6+ confirmed instances in two sessions** — promoted MEDIUM→HIGH on instance count:
1. Loop 1 — `get_technical_indicators` per-symbol Yahoo loop (552s).
2. Loop 3 — fixed via bulk Alpaca fetch + deadline.
3. Loop 5B — Finnhub `/calendar/earnings` (`_fh_get`, requests timeout ≠ total).
4. Loop 5B — other `_fh_get` callers (insider, news-sentiment, quote, company-news) shared the gap.
5. Loop 5D — catalyst earnings AV enrichment (per-symbol Alpha Vantage, 5/min).
6. Loop 5D — `get_trending_tickers` per-symbol `_yahoo_chart` over the 3,048-symbol universe.
**+ Loop 5C read — `base.build_prompt` inventory (accurate, ~15 per-symbol fetches):** MOST are
already BOUNDED — `market_data._is_yf_limited()` is hardwired `True` so all yfinance-routed
builders (mtf, fibonacci, trend, strategy, fundamental_score) short-circuit to empty before any
I/O; DB-backed ones (impulse, theta, gap, sentiment, strength) are DB reads. **4 LIVE unbounded-
on-cold paths remain:** (1) `build_whisper_prompt_section`→`get_trending_tickers()` **called with
NO `prices`** (base.py:658 — Loop 5D rewire MISSED this caller; legacy 3,048-sym Yahoo loop;
usually warm-cached but a latent re-hang); (2) `get_stock_price` loop over open positions
(base.py:1011 — Alpaca→Yahoo→Finnhub→AV per position); (3) `build_fundamentals_prompt` /
`build_sell_fundamentals_prompt`→`yahoo_quote_summary` (per-symbol Yahoo v10, timeout=10
single-attempt, NOT yf-gated); (4) `build_sr`/`build_pattern`→`_yahoo_chart` (bounded ~30s).
**Loop 5D follow-up:** thread `prices` into the whisper caller (one-liner, parks until 5C lands).

**COMPLETE engine/ SWEEP DONE 2026-05-29 (Explore inventory): 25 fetch-leaves — 13 BOUNDED,
8 UNBOUNDED (+OpenBB-SDK pair), 2 UNKNOWN.** Concrete fix list (ranked by hang risk):
- **TIER 1 (live scan-hot, reachable cold from `build_prompt`/catalyst — fix first):**
  1. `earnings_hub.py:13 get_earnings_countdown` → per-symbol `alphavantage_data.py:142
     get_earnings_surprises` (AV 5/min) + `market_data.py:550 get_stock_price`. No internal total
     deadline; `_CTX_CACHE` reduces re-pay but not a single cold build.
  2. `whisper_network.py:65 get_trending_tickers` legacy `_yahoo_chart` loop over ~3,048 syms —
     fixed when `prices=` passed (scan path does), but `check_watchlist_trending` +
     `build_whisper_prompt_section` callers still hit it (Loop 5D-miss).
  3. `market_data.py:550 get_stock_price` — 5-source serial cascade (Alpaca→Yahoo→Finnhub→AV→DB),
     no total deadline (~30s+ worst on Yahoo leg); the universal price leaf, called in many loops.
- **TIER 2 (lower-traffic unbounded — deadline/cache each):** `market_data.py:1022` Alpaca
  per-symbol bars fallback (N×10s, no cap); `market_data.py:509 get_all_prices` (8-worker pool, no
  pool-level timeout); `news_fetcher.py:20 fetch_news` (per-symbol Yahoo RSS, no cache); `sec_edgar.py:37/81`
  (per-symbol EDGAR, no cache); `openbb_data.py:145/236` insider/filings (OpenBB SDK, **no `timeout=`
  controllable at all**).
- **UNKNOWN (confirm):** `market_data.py:92 yahoo_quote_summary` (timeout=10, no cache — caller-frequency
  dependent).
- **ALREADY BOUNDED (no action):** `_fh_get` (15s thread-join), `_av_get` (timeout+1hr cache), Alpaca
  bulk/snapshot/bars-chunk, Polygon `_get`, FRED; yfinance paths inert via `_is_yf_limited()→True`.

**Fix shape:** total-deadline (thread-join like Loop 5B `_fh_get`) on each unbounded leaf + cache where
market-wide + prefer bulk/in-hand data over per-symbol loops (Loop 1→3, 5D-trending). **Priority:
SHIP BEFORE WAVE 7 weekend.** Tier 1 overlaps Loop 5C (in flight) — fold 5C's fix into Tier 1.

---

## 🟡 HM-SIGNALS-V2-STALE-SWEEP — MEDIUM (filed 2026-05-29) — read-only diagnostic done

**Finding (2026-05-29 diagnostic):** 123 `signals_v2` rows are `status='pending'` but
PAST their `stale_after` (93 @ 6-24h, 30 @ >24h) — not expired. Newest *stale*-marked
signal created yesterday 16:14 while *executed* continues today → the consumer-driven
sweep (`events_bus_consumer` owns pending→stale, reads `WHERE status='pending'`) reaches
fresh pending but old past-stale ones accumulate. Pending IS draining overall (1142→815).
No `expired` rows exist despite `events_bus.mark_signal_expired` writing that status (path
inert — worth checking).

**Severity: MEDIUM, not HIGH** — `buy()` has an internal stale-gate, so stuck-pending
canNOT be executed as fresh (no wrong-trade risk). Harm = pending-bucket bloat + status-
column inaccuracy + possible consumer-throughput lag.

**ROOT CAUSE (2026-05-29 deeper diagnosis):** the consumer (`run_events_bus_consumer`,
main.py:4048, every 1 min NYSE-hours) drains only `max_batch=10` pending/min, oldest-first,
and DOES mark past-stale at step (a) — but (1) 10/min < producer rate during heavy scanning
(navigator 368 + ollie-auto 240 fill faster), (2) it runs on the SHARED scheduler thread —
the same one §B/§C contention blocks — so it fires < every minute when scanners batch, and
(3) the no-price branch leaves signals `pending` (re-processed each tick). So staleness expiry
is gated behind rate-limited, contention-prone per-signal processing → past-stale accumulates.
`mark_signal_expired` is a CONFIRMED dead path (zero external callers; only its own warn-log
references it) → vestigial `expired` status, never written.

**RECOMMENDED FIX (focused session):** a **bulk stale-sweep decoupled from the consumer** —
`UPDATE signals_v2 SET status='stale' WHERE status='pending' AND stale_after IS NOT NULL AND
stale_after < datetime('now')` at the top of the consumer (or its own daemon). Expiry is a
bulk set-op, NOT a per-signal decision — shouldn't be rate-limited to 10/min or starved by
scheduler-thread contention. One cheap UPDATE clears the whole backlog each fire. Secondary:
delete the dead `mark_signal_expired`/`expired` path (`stale` is canonical); reconsider the
no-price reprocessing spin. NO wrong-trade risk (buy() stale-gate) → MEDIUM. Activation needs
a restart. Diagnostic detail in `docs/QUEUE_AUDIT_2026-05-29.md`.

**§C OVERLAP (bank 2026-05-29):** root-cause #2 (shared scheduler-thread contention)
directly overlaps the §C scan-lock stall. The run_scan watchdog (Loop 2+) may PARTIALLY
unblock the signals_v2 consumer via the same architectural fix. After the watchdog ships,
re-check whether the stale-sweep is still needed — but the **bulk UPDATE is the right
architecture regardless**: staleness is a set-property, not a per-signal decision. Expect the
watchdog to reduce (not eliminate) the accumulation.

---

## 🟢 HM-LOOP-1-LOG-VOLUME-ROTATION-CHECK — LOW (filed 2026-05-29)

Loop 1 instrumentation (HM-RUN-SCAN-WATCHDOG) adds ~10 `[SCAN-SUBCALL]` lines per scan to
trader.log. Not a problem now, but if Loop 1 stays long-term (post-watchdog ship), verify log
rotation handles the added volume. **Check after Loop 1 has soaked 24h+:** trader.log growth
rate + rotation config still sane. Either remove the instrumentation OR confirm rotation when
the watchdog ships.

---

## 🟢 HM-ADJUSTED-OHLCV-DOWNSTREAM-VERIFY — LOW (filed 2026-05-29)

Loop 3 set `adjustment='all'` GLOBALLY on `get_bulk_daily_ohlcv` (split-adjusted bars).
Residual flag from the Loop-2A analysis: not every consumer was exhaustively traced for
**adjusted-bar-vs-raw-live-quote mixing** on *recently-split* symbols (where adjusted ≠ raw).
Low risk (live quote sources are typically already adjusted; non-recently-split symbols see
raw≡adjusted no-op). **Read-only audit:** sweep all 6 bulk consumers (chart_patterns,
bbkc_squeeze_scanner, channel_scanner, minervini_filter, rs_rank, trendlines) + downstream
signal consumers for any place that compares an adjusted bulk-bar level to a raw live price.
Likely zero issues; deserves a clean closure. If a mix surfaces, narrow-fix that consumer.

---

## 🟢 HM-MEMORY-DEEP-AUDIT-Q2 — LOW / quarterly hygiene (filed 2026-05-29)

**Context:** A-6 Phase 1 done 2026-05-29 — MEMORY.md index trimmed 36.6KB→23.5KB (109 entries, hooks ≤~200 chars, under the 24.4KB load limit; backup in memory/archive/). 3 high-confidence stale entries fixed (all-out resume pointer, risk-manager-conviction precursor-met, conviction-tier denorm context).

**Deferred (this ticket):** full per-file verification of all **112 memory files** + 29 CLAUDE.md sections vs live code/DB. Multi-pass job, NOT between-soaks filler.
- 55 files carry BLOCKED/PENDING/queued/TODO markers — some likely resolved.
- 81 files carry RESOLVED/SHIPPED/CLOSED — mostly correct records, but any citing `file:line` refs are drift-prone (line-drift hit twice on 2026-05-29: auth 21269→21409, flash 4387→4896).
- Also: MEMORY.md has duplicate section headers (## Feedback ×2, ## Reference ×2) — consolidate during the deep pass.
- CLAUDE.md Fleet Roster "20 active" count needs reconcile vs the Worf bench.

**How:** dedicated focused session, or a multi-agent workflow (each agent verifies a batch vs live state) if Captain opts into orchestration.

---

## 🟡 HM-NOTIF-WAR-ROOM-PRODUCER — MED, BLOCKED on Captain trigger definition (filed 2026-05-29)

**Context:** A-3 deeplink half SHIPPED 2026-05-29 (`_notifDestination` `war_room`/`war-room` → `showSection('war-room')`, index.html:35392). Inert until a producer emits a `war_room`-typed notification.

**Blocked on Captain decision — what WR event fires the notif?**
- High-conviction debate consensus above a threshold (which threshold?)
- WR-surfaced actionable signal (which signal criteria?)
- Other?

**When trigger defined:** wire `_emit_notification(notif_type='war_room', ...)` at the WR event site (app.py helper at ~2189), with the agreed threshold to avoid every-9-min spam. Backend, S–M. Frontend deeplink already done.

---

## 🔴 HM-ADVISORY-CREW-DRIFT-SWEEP — NEW HIGH (filed 2026-05-29) — ~30 min, batch with next restart

**Trigger:** Worf reconcile (HM-WORF-DRIFT-RECONCILE) exposed the same disease in
its `_SCAN_TIER2` peers — agents in `ADVISORY_CREW` (bridge-vote-only, no scanning)
that are ALSO listed in `_SCAN_TIER2`, so the scanner roster claims they scan.

**Known offenders in `_SCAN_TIER2` ∩ `ADVISORY_CREW`:**
- `ollama-llama` (Uhura)
- `options-sosnoff` (Troi)
- `energy-arnold` (Trip)
- _(verify full intersection during the sweep — `ollama-local`/Geordi also appears
  in both; confirm against live `ADVISORY_CREW` before removing.)_

**Fix (same pattern as Worf):** remove each ADVISORY_CREW member from `_SCAN_TIER2`
(main.py) with a why-comment. Leave `ai_players` state alone unless verified
(active = required for WR bridge-vote, per HM-WORF-DRIFT-RECONCILE). Do NOT touch
`ollama-plutus`/`ollama-qwen3` (McCoy/Scotty — NOT benched).

**Activation:** `_SCAN_TIER2` change needs a restart; **batch with the run_scan
watchdog restart** — these agents emit nothing meaningful meanwhile.

---

## 🗓️ review-2026-06-04 — Worf (qwen3-8b-flash) bench re-evaluation

**Context:** Worf benched S6.1 (−0.36%), reconciled across all 6 state sources
2026-05-29 (HM-WORF-DRIFT-RECONCILE): removed from `_SCAN_TIER2` + `SNIPER_AGENTS`,
doc marked BENCHED, kept `ai_players` active for WR bridge-vote, `ADVISORY_CREW`
canonical. It's a **Bear Specialist** (bearish-only, stands down in confirmed
bulls / TRENDING_BULL).

**Review task (on/after 2026-06-04):** re-evaluate the −0.36% bench using
**current-system data** — conviction stops, scheduler fixes (Loop 1/2 + run_scan
watchdog), and model remaps have all landed since the S6.1 bench. The −0.36% was
measured under the old system.

**HARD GATE:** re-evaluate only during a **genuine BEAR cycle** — NOT a bull cross.
Regime on 2026-05-29 = BULL_CROSS, where a bear specialist is correctly dormant
and would show no edge regardless. Wait for RISK_OFF / confirmed bear, then
ghost-trade Worf for a window before deciding re-activate vs keep-benched.

---

## 🟢 HM-BS-DAEMON-HEARTBEAT — LOW / one-liner (filed 2026-05-29) — next main.py restart

Loop 3's battle_station daemon (`_battle_station_scheduler_thread`, main.py) is
silent-unless-error — no per-tick heartbeat (unlike the WR daemon's
`[WR-DAEMON-HB]`). Absence-of-drift is the only liveness proxy, which is ambiguous
(a dead daemon also produces no drift). Add a per-tick `[HM-BS-DAEMON-HB] tick=N`
log (mirror the WR daemon) so liveness is POSITIVELY verifiable. Same observability
lesson as §C: silence is ambiguous, presence is proof. One-liner; bundle with the
next main.py restart.

---

## 🔴 HM-RUN-SCAN-WATCHDOG — HIGH, IN PROGRESS (filed 2026-05-29) — multi-cause, Loops 1-5C

### ▶ MONDAY-RESUME (2026-05-30 weekend checkpoint) — §C floor = Lever A "bounded-rotation"
**§C causes CLOSED + clean-verified:** indicators (Loop 3) · catalyst/trending/quote_summary spikes
(5B/5C/5D) · **deepseek** redundant arena path (7d7caa8) · **ollama-coder** redundant path (7d7caa8).
**Remaining = the analyze-all FLOOR:** genuine LLM agents (McCoy/Dax, ±others) analyze all 307 symbols
→ ~85min TIER2 scan holds `_scan_lock`, starving TIER1 (BridgeCrew, 30-min cadence) ~2 slots/2h. NOT a
hang — a legitimately-long scan. (Arena scan is MARKET-HOURS-GATED → no weekend scans → floor dormant +
unconfirmable till Monday open.)
**FIX DECIDED = Lever A "bounded-rotation" (NOT a content screen):** verify-before-fix killed the
content-screen (Shape A) — McCoy/Dax signal on ~all 312 symbols (nothing to validate against) + no cached
options/IV universe (a CSP screen would need the per-symbol fetches we killed). Bounded-rotation bounds
QUANTITY (N≈50/cycle, rotate offset, full coverage over ~6 scans) with ZERO alpha loss; suits CSP's slow
cadence. See [[when-you-cant-validate-content-bound-quantity]] doctrine.
**STAGED (uncommitted, working tree `M engine/ai_brain.py`):** 4 Tier3 redundant removals (cto-grok42,
ollama-deepseek/Odo, ollama-kimi/Bashir, qwen3-8b-sonnet/Sisko) — bundle into the floor restart.
**DUAL-PATH FULL SWEEP DONE 2026-05-30 (static, uncontaminated) — orphan-hint REFUTED:** the arena
collapses to EXACTLY **{McCoy (ollama-plutus), Dax (ollama-qwen3)}** — both GENUINE. Worf/Seven/navigator
do NOT reach the arena (NOT in any scan tier — the orphan ran old bytecode w/ a pre-pruning roster). NO
additional free deletions beyond the staged skip-set (the other redundants are tier/DB-gated already).
So the floor is just McCoy+Dax; bounded-rotation applies to those 2.
**MONDAY BUILD SEQUENCE:** (1) first clean scan → CONFIRM arena set = {McCoy, Dax} (static expectation
set; should match). (2) build bounded-rotation on {McCoy, Dax}. (3) ditto step (2) — no extra deletions.
(4) bundle the 4 staged Tier3 removals. (5) live scan confirms TIER2 ≤15min + TIER1 starvation gone +
scans COMPLETE (post_processing>0) + full coverage over ~6 scans.
**VBC gates — STATIC ones DONE (cleared weekend):** offset → mirror `_ALPHA_PAIR_IDX` (crew_scanner:272)
BUT persist to `settings` (in-memory resets on restart → re-scans head, starves tail); modulo handles
universe-growth. No downstream assumes full-307/cycle (consumers drain what's emitted; bounded-rotation may
even HELP signals_v2 bloat). **VBC-2 (N sizing) = MONDAY** — needs clean per-symbol cost (the ~10.5s is
orphan-era; size N so N×agents×cost ≤ ~15min, margin under the 30-min TIER1 cadence).
**Also Monday:** signals_v2 stale-sweep runs AFTER the floor fix (don't drain a still-filling pond).

**STATUS 2026-05-29 PM: §C stall REDUCED from 3 causes → 1. Catalyst CLOSED; infer remains.**
- **Loop 1 (instrumentation): SHIPPED** — `[SCAN-SUBCALL]` + quiet per-phase/per-symbol telemetry.
- **Loop 3 (CAUSE #1 — indicators): SHIPPED + VERIFIED** (`befb327`) — per-symbol Yahoo
  loop (552s) → ONE bulk Alpaca call (`indicators wall=2.0s`). adjusted='all' global. **CLOSED.**
- **Loop 5A/5A.2 (instrumentation): SHIPPED** — setup-segment + 14 `build_scan_context`
  inner markers localized the 2nd cause to `ctx:catalyst`, then split it `:earnings`/`:trending`.
- **Loop 5B (Finnhub calendar): SHIPPED** (`9eb6e07`) — `_fh_get` cache + 15s thread-deadline.
  Valid hardening but NOT the catalyst hang (no-deadline-trip test proved it).
- **Loop 5D (CAUSE #2 — catalyst): SHIPPED + VALIDATED** (`0770c54`) — the real catalyst hang
  was `get_trending_tickers` looping `_yahoo_chart` over the **3,048-symbol** universe (Loop-1
  shape at scale). Fix: **trending-rewire** (derive movers from in-hand `prices`, 0 Yahoo) +
  **profile-keyed `build_scan_context` cache** (~4 builds/cycle, was 19). 16-min soak: **0
  `ctx:catalyst:trending` HELD samples; 10-min trending-TTL boundary passed with no hang.**
  **This cause is CLOSED.**
- **Loop 5C (CAUSE #3 — infer/analyze_chain): IN PROGRESS — now the SOLE remaining §C cause.**
  Post-5D, the dominant hang is `analyze_chain` wedging on specific symbols (TEAM 315s+, prior
  XOM/KLAC) — single-symbol, not cumulative (~10 other syms clear in 2-40s). Scan never
  completes (0 `post_processing` in 16 min; `_scan_lock` held 961s+). Read (Explore) found:
  deepseek path = `analyze_chain`→`analyze`→`build_prompt` (**~15 per-symbol fetches, several
  UNBOUNDED**: fundamentals/Yahoo, sell-fundamentals/analyst-ratings, sentiment/Finnhub, whisper,
  + a `get_stock_price` loop) → `call_model`→`get_queue().submit` (requests timeout=90s but
  queue `REQUEST_TIMEOUT=300s` is the operative outer bound; 315s ≈ 300s+slop). a/b/c AMBIGUOUS
  (queue-wait vs per-symbol build_prompt fetch) — **Loop 5C-A instrumentation next** to split
  `infer:{sym}:prompt` vs `:model` before fixing. Symbol-specificity hints at build_prompt data.

> **§C stall: 2 of 3 causes CLOSED (indicators + catalyst). 1 remains: infer/analyze_chain
> (Loop 5C). NOT declaring §C closed until 5C lands + soak holds zero-HELD>60s.**
> **Nightly-scanner follow-up:** rs_rank + minervini (20:30/20:45 AZ) under adjusted='all' —
> confirm output sane tonight (squeeze verified 131 rows/hr, 0 errors).

**STATUS 2026-05-29 PM: DATA-READY.** Two confirmed stalls reliably reproducing —
~14 min (AM) and 16+ min (PM, ongoing post-09:33 restart, HELD-INFLIGHT climbing
60s→960s+). The shipped HELD-INFLIGHT heartbeat is producing the duration
distribution; stalls appear effectively unbounded (hold the lock until restart).
Design against this real evidence in a focused session (signal-timeout vs
thread-kill vs per-subcall timeout) — needs design time, NOT a tail-patch.

**Trigger:** §C soak (HM-AS-β) found a **pre-existing scan-lock stall** — a scan
acquires `_scan_lock` (`main.py::run_scanner`) and `run_scan()` hangs unboundedly
(observed ~14 min, 06:05→06:16 on 2026-05-29, never completed) → every subsequent
tick `Scan skipped`, T1/T2 starved until restart. Same pattern in the old trader →
not new. The `[HM-AS-β-C] scan_lock held` line never fires because scans don't
complete; only `due-but-skipped` accumulates.

**Already shipped (read-only, 2026-05-29 PID 5299):** in-flight HELD-INFLIGHT
heartbeat (logs hold duration every 60s while a scan is in flight) — makes the
next stall visible within 60s. **Behavior-changing watchdog deferred to this item.**

**Scope:**
- Identify WHERE `run_scan()` hangs (which provider call / network with no timeout)
  — HELD-INFLIGHT heartbeat surfaces it on the next stall.
- Design options: (a) signal-based timeout returning after N min; (b) thread-kill +
  state cleanup; (c) per-subcall timeouts so the lock holder always returns.
- Risk: all touch the critical scan path → real testing before ship.
- **Soak first** with the heartbeat to learn stall frequency + duration distribution
  (once/day? once/hour?) before designing the fix.

Full context + design notes: `drafts/HM-AS-BETA-SCHEDULER-TOP-PRIORITY.md` §C.

---

## 🟢 HM-FRONTEND-VISUAL-TEST-HARNESS — LOW / conditional (filed 2026-05-29)

**Gate:** only if WAVE 7 frontend work keeps growing (AN-Bridge READ-proxies,
inline-style-sweep Batches 6-9, Ollie-AI workspace panes — 5+ more frontend
items). Below that threshold, eyeball verification during normal dashboard use
is sufficient and Playwright's ~300MB + tooling overhead isn't justified.

**Scope (when triggered):** `npm i -D playwright` + `tests/visual/` with headless
smoke per LCARS section (assert section-bar title/sub renders, no IIFE throw) +
a screenshot baseline. Makes the Frontend Ship Rule's browser smoke scriptable
instead of manual. Captain decision per the WAVE-7 frontend roadmap; separate
scope from any single patch.

**Why filed:** LCARS-T1 (2026-05-29) was data-only and verified via node --check
+ served-fresh + object-resolution, so no browser driver was needed — but the
next frontend items carry real runtime/closure risk where a harness pays off.

---

## ✅ HM-DATA-INTEGRITY-FORENSICS — CLOSED 2026-05-29 (all sub-tickets shipped)

> **CLOSED 2026-05-29 (closure sweep):** both sub-tickets shipped 2026-05-25 —
> HM-CATEGORY-C-EMERGENCY-LOCK (`45e57e1`, superseded) + HM-CLEAN-STALE-ARCHIVE-NOT-DELETE
> (merge `38a38e4`, pushed to main). No open sub-work remains; parent was queue-rot.
> Forensic record retained below.

### (CLOSED) HM-DATA-INTEGRITY-FORENSICS — PARENT TICKET (filed 2026-05-25)

**Trigger:** Forensic audit during HM-RISK-MANAGER-CONVICTION-STOP-WIRE Lane A
discovered an active "delete-without-archive" endpoint that wiped
`portfolio_history` rows pre-2026-03-11. All 9 DB backups inherit the
post-wipe state; data is unrecoverable from local sources.

**Sacred Data Rule violation:** "Trade data is gold. Never delete data."

### Sub-tickets

#### HM-CATEGORY-C-EMERGENCY-LOCK — ✅ **SHIPPED + SUPERSEDED** 2026-05-25

Initial emergency lock at `45e57e1` (returned HTTP 403, pushed direct-
to-main per Admiral authorization for active data-integrity violation).
Superseded by the proper fix HM-CLEAN-STALE-ARCHIVE-NOT-DELETE (merge
`38a38e4`), which replaces the 403 with the archive-then-delete pattern.
Lock commit retained in main history as forensic record of the doctrine
violation it closed.

#### HM-CLEAN-STALE-ARCHIVE-NOT-DELETE — ✅ **SHIPPED** 2026-05-25 (merge `38a38e4`, pushed to main)

Six-commit branch merged to main with end-to-end smoke + 5/5 tests:

  Phase 1 `04b00f3` — schema (portfolio_history_archived + 3 indexes)
  Phase 2 `8c9b942` — endpoint rewrite (archive-then-delete transaction)
  Phase 3 `a5054c0` — recovery endpoints (GET list + POST restore-from-archive)
  Phase 4 `b634dd0` — pattern tests (5/5 PASS, 0.04s wall)
  Phase 5 `c10eb06` — docs/DOCTRINE.md (Rules 1/2/3 codified)
  Phase 3.1 `c5a9d49` — typing.Optional fix (Py3.9 FastAPI runtime introspection)
  Merge `38a38e4` — merged to main + pushed

Post-merge verification: POST `/api/admin/clean-stale-snapshots` on
live DB (no-match case) returns 200 with `archived_count=0,
deleted_count=0, message="No stale snapshots to archive"`. The 403
short-circuit is gone; archive-then-delete pattern is the active code
path. Live trader continues running pre-merge bytecode until natural
restart — acceptable since the endpoint is admin-only with no
automated caller.

Sacred Data Rule restored: every gold-row removal is now traceable via
session_id + reversible via the restore endpoint.

Branch cleanup: keep `hm-clean-stale-archive-not-delete` 7 days, then
prune (standard).

#### HM-SIGNAL-WIPE-FORENSIC — ✅ **CLOSED AS NO-VIOLATION** 2026-05-25

Investigation result: `signals.earliest = 2026-03-11 18:18:16` is **NOT
a delete event**. Forensic searches confirmed:
- Zero `DELETE FROM signals` in code or git history.
- No clean-* endpoint touches signals.
- No retention/cleanup/prune cron for signals.
- Earliest timestamp matches fleet go-live; sibling tables (portfolio_
  history) started 24s later from same admin session.

Reclassified as Category B (never captured) — signal-emission code first
went live on that date; nothing was deleted. No further action required.

#### HM-PORTFOLIO-RECONSTRUCT-FROM-TRADES — optional, Admiral decides timing

Rebuild portfolio_history Jan 6 → Mar 11 from `trades` + Yahoo/Polygon
OHLCV. Caveat: most AI_SIGNAL_PLAYERS didn't exist pre-Mar-11; the
window is dominated by webull (liquidated) and 3 paid LLMs (now in
exit_only). Reconstruction is feasible but partial; ~6-8h scope. The
50d window we currently have IS the operational reality of the current
fleet — reconstruction adds historical color, not active calibration data.

#### HM-RECAP-TRIGGERS-DELETE-PATTERN — ✅ **AUDITED — NO ACTION REQUIRED** 2026-05-25

Full audit of `scripts/recapitalize_player.py` (137 lines, single source).
Findings:

- **Zero DELETE / DROP / TRUNCATE.** Only one mutation outside audit
  trail: `UPDATE ai_players SET cash=?, is_paused=? WHERE id=?` (L85).
- **Two gold-table INSERTs preserved per recap event:**
  `player_funding_events` (full delta audit) + `portfolio_history` (new
  snapshot at new equity level — prior rows untouched).
- **CLI-only:** no cron, no plist, no imports from other modules.
- **Lifetime usage:** 1 event in 76 days (dalio-metals 2026-03-28
  "restore baseline"). Audit mechanism rarely exercised.

Recap **arms** the precondition (cash >= 9999 + stale low-equity rows)
that the locked-and-fixed `clean_stale_snapshots` endpoint exploited.
Post-merge `38a38e4`, the endpoint archives-not-deletes, so the
script + endpoint now operate as a safe pair: recap sets new cash,
endpoint archives the now-stale prior snapshots to
`portfolio_history_archived`.

No code change needed. Recap is doctrine-compliant. Closing.

Banked adjacent (not actionable now):
- No NTFY on recap events (silent) — fleet observability gap, low priority.
- No 'unrecap' / refund path — one-way by design, acceptable.

#### HM-MARKET-HOLIDAY-CALENDAR — ✅ **SHIPPED** 2026-05-25 (Memorial Day arc)

Production trader fired 6 Alpaca orders + 2 simulated positions on
Memorial Day 2026-05-25 (market closed) because there was no holiday
calendar in production code. Full structural fix delivered same session.

Containment + structural commits (all pushed to main):
  `6cdf9d5`  Stage 1 — 6 Alpaca orders cancelled, 0/0 filled
  `c35aa51`  Stage 2 — 11 local rows archived per Doctrine Rule #2
  `02d3558`  Stage 3 — neo-matrix + ollie-auto halted
  `7d55d35`  Phase A — engine/market_calendar.py + tests (18/18)
  `3cd4838`  Phase B — 7 hard gates + 1 soft update (11/11 tests)
  `bf54ee8`  Phase C — dashboard banner holiday-aware
  `4588639`  Phase D — docs/DOCTRINE.md Rule #4

Doctrine Rule #4 codified: "Never trade on closed markets." Gates at
paper_trader buy/sell/short_sell + alpaca_bridge buy/sell/short_sell
+ alpaca_options.execute_options_signal + risk_manager.is_market_hours.

`engine/market_calendar.py` carries 2025-2027 NYSE holidays + early-
close days + DST-aware status enum. Annual extension required (bank
calendar-year-end ticket each December).

neo-matrix + ollie-auto held at `halt_mode='exit_only'` overnight
2026-05-25 → 2026-05-26 as conservative posture for first overnight
after fix; Admiral promotes to `active` Tuesday 09:30 ET after banner
verification.

#### HM-PROVING-GROUND-FORMALIZE-V2 — ✅ **SHIPPED** 2026-05-25 (merge `3b4bdac`)

Sniper Mode trial formalized after Memorial Day NTFY Proving Ground
review surfaced that the 30-day spec had run to Day 45 without formal
extension or exit criteria. Three-SUB structural ship:

  `e79a12a`  SUB-1 — dedicated `ollietrades-proving-ground` NTFY topic
                    (Admiral mobile receipt confirmed)
  `af22d32`  SUB-2 — Admiral-locked exit criteria + state evaluator
                    + Admiral CLI for terminal states (10/10 tests)
  `20d696c`  SUB-3 — TRIAL_DAYS 30 -> 60 + formalization rationale

Daily 13:18 AZ evaluator hook (`main.py::run_proving_ground_evaluator`).
State machine: pending → warning → ship_ready | kill_warning → shipped |
killed (terminal states require `scripts/proving_ground_admiral.py
--ship`/`--kill` --confirm --agent ollie-auto).

Dry-run at Day 46 (today): state='warning' (4/6 streak 5+ days).
**Heads-up:** at Day 60 boundary (2026-06-09) K1 kill_warning will fire
automatically because dd has held at -24% across the entire trial.
Admiral can preempt with --kill before Day 60 OR respond when the
auto-surfaced kill_warning emits.

#### HM-RISK-MANAGER-CONVICTION-STOP-WIRE — ✅ **SHIPPED** 2026-05-25 (merge `9b55466`)

Conviction-scaled stops shipped with feature flag default OFF. Tier
table at d41216b floor-fix state (0.18/0.15/0.12 — never tighter than
flat baseline).

Branch ship (11 commits merged):
  5655fb0 → f42c181  full HM-POSITIONS-CONVICTION-DENORM + WIRE arc
  d41216b           floor fix (eliminate 0.08 regression band)
  6971a91           Phase 5c 180d re-run (G1 +0.8%, G3 still 2/14)
  568cb81           Phase 6 feature flag default off
  51650ad           Opt 1 calibration trial (REJECTED — worse than floor)
  f42c181           Restore d41216b tier table per Admiral ship-as-is

Admiral flips `CONVICTION_SCALED_STOPS_ENABLED=True` in .env after
shadow-validating live trader behavior. Until then, production is
doctrine-equivalent to pre-wire (flat per-model guardrail for all
players).

HM-CONVICTION-TIER-BOUNDARY-CALIBRATION remains banked (separate
ticket) for post-shadow review — 3/4 regressors (dayblade-sulu,
options-sosnoff, partial ollama-llama) live in the 0.15 tier and would
benefit from calibration that the rejected Opt 1 trial failed to find.

#### HM-FLEET-TRAIL-CONVICTION-SCALE — ✅ **SHIPPED** 2026-05-25 (merge `ecc86b1`)

Symmetric counterpart to HM-RISK-MANAGER-CONVICTION-STOP-WIRE. Wires
conviction-scaled fleet trailing-stop width (3/4/5%) behind feature
flag `CONVICTION_SCALED_TRAIL_ENABLED` (default OFF). Codifies
Doctrine Rule #5 — symmetric conviction-scaling across all stop layers.

Branch ship (5 phases merged):
  Phase A `74116c5`  document current behavior baseline
  Phase B `21a5347`  engine.stops.get_trail_pct + flag + gate wiring
  Phase C `9bc22f2`  11/11 tier-table + gate-behavior tests
  Phase D `7f74c43`  targeted impact analysis (backtest harness mismatch
                     documented; 10/17 allow-list positions diverge under
                     flag-on; ollama-qwen3/neo-matrix/qwen3-8b-flash most
                     affected; AVGO/GOOGL/META/MSFT divergent tickers)
  Phase E `8d13ad6`  docs/DOCTRINE.md Rule #5 codified

Production behavior unchanged at merge — flag default OFF via code
(env entry omitted from .env per Admiral; code's empty-env-var fallback
to False is sufficient).

Admiral shadow-validation sequence (heads-up):
  1. Watch live trader under both flags=False through 2026-05-26+
  2. Flip CONVICTION_SCALED_STOPS_ENABLED=True first (smaller blast radius)
  3. Observe 5-10 trading days
  4. Then flip CONVICTION_SCALED_TRAIL_ENABLED=True
  5. Observe additional 5-10 days
  6. If either degrades behavior, flip back to False and revisit tiers

#### HM-OPTIONS-CONVICTION-STOP-WIRE — ✅ **SHIPPED** 2026-05-25 (merge `263c8dd`)

Third symmetric pairing — completes the conviction-scaling trio (entry
stop + fleet trail + options stop). Flag default OFF via code; same
ship-as-is + shadow-validate-live posture as Lane A + Lane C.

Branch ship (5 phases merged):
  Phase A `0124a70` baseline annotation
  Phase B `24332ea` engine.stops.get_options_stop_pct (30/40/50%) +
                    CONVICTION_SCALED_OPTIONS_STOP_ENABLED flag + gate
  Phase C `9bc5cbe` 11/11 tier-table + gate-behavior tests
  Phase D `eb6e432` targeted analysis (1 options position diverges —
                    navigator PLD call conv=0.75 would tighten 50% -> 30%)
  Phase E `5a81448` docs/DOCTRINE.md Rule #5 — options floor-invariant
                    EXCEPTION codified

INTENTIONAL DOCTRINE DEVIATION (Admiral-locked): unlike stops + trail
where the low tier matches the flat baseline, the options low-conv
tier (0.30) is TIGHTER than the current 0.50 baseline. Rationale:
theta decay + IV crush asymmetry. Documented as the only floor-
invariant exception in Rule #5; future stop layers must amend Rule #5
explicitly if they want to invert direction.

Three independent flags now exist (all default OFF):
  CONVICTION_SCALED_STOPS_ENABLED         (12/15/18% entry stop)
  CONVICTION_SCALED_TRAIL_ENABLED         (3/4/5% fleet trail)
  CONVICTION_SCALED_OPTIONS_STOP_ENABLED  (30/40/50% options stop)

Recommended shadow-validation order (least to most blast radius):
  1. STOPS  → observe 5-10 days
  2. TRAIL  → observe 5-10 days
  3. OPTIONS_STOP → observe closely; tier inverts floor + applies to
     small options surface; first divergent position is navigator PLD

#### HM-CONVICTION-SCALED-BACKTEST-HARNESS — banked (consolidates two prior)

Replaces and consolidates HM-FLEET-TRAIL-BACKTEST-HARNESS (banked
earlier this session). Both gaps share a root cause: engine.backtester
_simulate_guarded does not exercise the production fleet-trail or
options-stop layers — only the entry-stop layer that Lane A's
flat_stop_pct param covers.

Surfaced during Phase D of both HM-FLEET-TRAIL-CONVICTION-SCALE +
HM-OPTIONS-CONVICTION-STOP-WIRE. Targeted analyses in those tickets
provided directional input but not G1-G4 acceptance-gate validation.

Consolidated scope (~4-5h):
- Extend `_simulate_guarded` with TWO new optional callable parameters:
    `fleet_trail_pct_fn(conviction) -> float`
    `options_stop_pct_fn(conviction) -> float`
- Replace hardcoded V3 trail logic with the injected callable when set
- Add simulation of the options-stop branch (premium estimation from
  stock_price + entry_premium, then stop check)
- Wire `engine.stops.get_trail_pct` and `engine.stops.get_options_stop_pct`
  as the production-mirror callables
- Run 180-day A/B for each layer independently and as a combined ON
- Validate Phase D directional signals with G1-G4 gates

Priority: LOW. Bank for post-shadow-validation review. Per-layer Phase D
analyses are sufficient input for the initial flag-flip decisions.

#### HM-INLINE-STYLE-SWEEP — IN PROGRESS (5 of 9 batches complete)

Frontend dashboard inline-style → CSS-var migration. Multi-batch sprint
to consume ~1191 inline color literals across `dashboard/static/index.html`.
Conservative power-paste pattern via
`scripts/hm_inline_style_sweep_batch_migrate.py` — line-range-scoped
Python regex acting only inside `style="..."` attribute values, mapping
only the 5 hex codes whose canonical var exists in production :root.

  Batch 1 (b905935) bridge-tip-finish              —   6 migrated,  14 banked
  Batch 2 (f6ef2eb) bridge-rest      L7000-10000   —  58 migrated, 109 banked
  Batch 3 (4a56141) leaderboard-fleet L14000-18000 —  60 migrated, 119 banked
  Merge   (58c0ad6) 2+3 to main 2026-05-25
  Batch 4 (d9d7d65) cockpit          L18000-22000  —  24 migrated,  69 banked
  Batch 5 (db60387) squeeze-movers   L22000-26000  —  29 migrated,  25 banked
  Merge   (fast-fwd) 4+5 to main 2026-05-25

  Cumulative: 177 literals migrated, 336 banked-with-comment
  Site-wide hex-in-style-attr remaining: 1073

Migration rates so far:
  Batch 1: 30.0%      Batch 2: 34.7%     Batch 3: 33.5%
  Batch 4: 25.8%      Batch 5: 53.7%
  Cumulative: 34.5% (177 / 513 examined)

Batch 4's lower rate is a content-shape signal: cockpit zone is
accent-heavy (#2563eb, #ea580c, #fff family — the V4.5-token
candidates). Batch 5 is text-heavy (up/down/muted status indicators
on movers table) → highest single-batch migration rate.

Remaining batches **BLOCKED on HM-V4.5-TOKEN-EXTENSION** — the 336 banked
inventory cannot migrate further until canonical-var coverage expands:
  Batch 6 debate-oai L26000-30000 + L10000-14000 (~102)
  Batch 7a js-templates-static (helper class introduction)
  Batch 7b js-templates-dynamic (conditional class injection)
  Batch 8 class-bg-sweep (~57)
  Batch 9 svg-fills (162 fills + 100 strokes)

#### HM-V4.5-TOKEN-EXTENSION — banked, HIGH priority (blocks sweep batches 4-9)

The 242 banked literals across sweep batches 1-3 reveal a concrete
spec gap in the v4.4 unified theme module. Top-frequency hex codes
that DON'T have a canonical var:

  #2563eb + #3b82f6 family (26 occurrences) — needs `--accent-blue`
  #e5e7eb                  (16)              — needs `--border-light`
  #f59e0b                  (14)              — needs `--warning`
  #10b981                  (12)              — needs `--success-variant`
  #000                     (12, theme-blind) — needs `--text-absolute-black`
  #fff                     (11, theme-blind) — needs `--text-absolute-white`
  #ea580c                   (9)              — needs `--accent-orange-variant`
  #818cf8                   (8)              — needs `--accent-indigo`
  #ffd700                   (6)              — needs `--gold`

Scope (~1.5-2h):
- Add tokens to v4.4 unified theme module (4 theme cascades: dark, light,
  dark-cb, light-cb)
- Verify cascade specificity doesn't conflict with v4.4 base
- Smoke 4-theme render for new tokens
- Migrator script can then map these on next sweep batch

Priority: HIGH (blocks sweep progress past Batch 3). Greenfield-design
adjacent (token naming, cascade design) — work for fresh context, not
late-marathon execution.

#### HM-ADMIRAL-PREMARKET-CHECK — ✅ **SHIPPED** 2026-05-25 (3-phase merge `c0f3518`)

Final Memorial Day sprint. Two executable scripts that turn Tuesday
morning's "is it safe to unhalt the fleet?" question into one command
+ one safety-gated command.

`scripts/admiral_premarket_check.sh` (Phase A, commit `54b5a3e`)
  8 read-only checks, color-coded PASS/FAIL/WARN, exit 0 if clean:
    1. System health (PIDs + ports)
    2. Market calendar (engine.market_calendar direct call)
    3. Agent halt state (neo-matrix + ollie-auto)
    4. Conviction flags (.env)
    5. Alpaca state (equity + pending orders via local proxy)
    6. Scanner health (rs_rank + minervini_trend + squeeze_watch <24h)
    7. DB integrity (portfolio_history yesterday)
    8. Convergence smoke (strategy_names field — b76ea91 verifier)

`scripts/admiral_unhalt_agents.sh` (Phase B, commit `128fd46`)
  Safety-gated unhalt. Re-runs the premarket check as a gate. Without
  `--confirm` it prints the SQL it would run (dry-run). With `--confirm`
  it executes `UPDATE ai_players SET halt_mode='active' WHERE id IN
  ('neo-matrix','ollie-auto') AND halt_mode='exit_only'` and verifies.

`scripts/README.md` (Phase C, commit `c0f3518`)
  Usage notes for the suite + index of every existing script in
  `scripts/` grouped by purpose (Admiral pre-bell, backtests, ops,
  reboot wrappers, data export, archived).

Memorial Day evening smoke (live current-state):
  - 7/8 checks PASS
  - 1 FAIL on CHECK 5: 5 Memorial Day Neo QQQ orders still show
    "accepted" in Alpaca paper despite Stage 1 cancel arc. These
    will attempt to fill at 09:30 ET if not canceled at the broker
    UI side before bell. Admiral attention required.
  - Unhalt script correctly aborts when the check FAILs.

Tomorrow's runway: `scripts/admiral_premarket_check.sh` → fix any reds
(probable: cancel residual QQQ pendings at Alpaca UI) →
`scripts/admiral_unhalt_agents.sh --confirm`. Done.

Guard rails honored:
  - Premarket check is fully read-only
  - Unhalt is the only writer, behind --confirm
  - Only halt_mode column flipped (halted_at + halt_reason preserved
    as historical record per CLAUDE.md doctrine)
  - No schema changes
  - No live trader restart required (halt_mode is hot-read per cycle)

#### HM-CHEKOV-SEND-TO-HOLODECK-CONTEXT-WIRE — ✅ **SHIPPED** 2026-05-25 (commit `3852df5`)

The "Send to Holodeck" button on Chekov's convergence detail modal
navigated to the Holodeck section but never populated the Backtester
ticker field. User clicked alert on RGTX → landed on Holodeck with
NVDA (the default) still in the ticker field, forced to retype.

ROOT CAUSE: handler at `dashboard/static/index.html:33139` stashed the
symbol on `window._hmHolodeckPendingSymbol` and called
`showSection('holodeck')` — but nothing ever consumed the stashed
value. Dead state, unbroken UX gap since the convergence modal shipped.

FIX: defer one tick after `showSection()` (50ms — lets the
display:none → block transition complete so the field is real
in the DOM), then:
  1. write `sym` (uppercased) to `#holodeck-ticker`
  2. dispatch `input` + `change` events for downstream listeners
  3. smooth-scroll the field into view, block:'center'
  4. focus + select — Tab to strategy/days or type-replace

Bake-Off section intentionally untouched (no ticker input — fleet
replay, model + days only). Strategy + days selections preserved
per guard rail "don't be too aggressive".

Scope:
- Frontend-only, single-handler edit, 25 LOC
- No backend / no API contract changes
- `_hmHolodeckPendingSymbol` write preserved (in case any latent
  reader exists — none found via grep, but cheap to keep)

Admiral-verify-tomorrow (mirrors HM-INLINE-STYLE-SWEEP precedent):
click any active convergence alert toast → "🧪 Send to Holodeck" →
verify Backtester ticker shows the convergence symbol (not "NVDA").

#### HM-NAVIGATOR-CONVERGENCE-LIST-MISSING — ✅ **SHIPPED** 2026-05-25 (merge `b76ea91`)

Chekov convergence modal was showing "(N strategies — list not provided
by /api/navigator/convergence)" placeholder when the backend was ALWAYS
serving the data. One-line frontend field-name fix.

ROOT CAUSE: `engine/strategies.py::get_todays_signals()` at L898 returns
each signal with field `strategy_names` (snake_case GROUP_CONCAT). The
modal's lookup chain at `dashboard/static/index.html:33085` checked
`payload.strategies || payload.strategies_list || []` — never
`strategy_names`. Two other frontend sites (L18372 + L19795) already
used the correct field name; the modal was the outlier.

FIX: extend lookup chain to include `strategy_names`. Existing render
loop picks up the array.

Live smoke (2026-05-25 16:00 AZ) — 31 convergence signals today; the
bug report's sample matches production:
  RGTX 6 strategies conf 1.0
  [breakout_volume, macd_crossover, unusual_volume, ema_ribbon,
   trend_resumption, bull_momentum_breakout]

Single commit (`9b49b4f`) merged via `b76ea91`. ~10 min total.

#### HM-NTFY-ACK-CLICKTHROUGH — banked (Week 7 work)

Single click-through URL in NTFY body → logs `ntfy_ack` row to new
`ntfy_acknowledgements` table. ~30-min implementation. Enables real
engagement measurement instead of inferred-via-correlation. Low
priority — can ship anytime in the next 2 weeks.

#### HM-FINMEM-AGENT-MEMORY-ARCHIVE — defensive flag

`engine/finmem_writers.py:280` prunes `agent_memory` rows where
`decay_rate IS NOT NULL AND score < floor`. This is intentional memory
management (decay → prune below threshold), but agent memory IS partial
gold (learned context). Currently delete-without-archive.

Suggested fix: archive pruned rows to `agent_memory_pruned` with
timestamp + reason. Low priority — by design the pruned rows are low-
score memories the agent considered low-relevance; not the same severity
as portfolio_history.

#### HM-PORTFOLIO-POSITIONS-SYNC-ARCHIVE — defensive flag

`dashboard/app.py:10915` Alpaca-sync wipes `portfolio_positions WHERE
status='open'` before re-inserting current Alpaca state. Comment
explicitly preserves closed-history. State-sync is DEFENSIBLE but
worth adding a "last_synced_at" + audit log to make the sync events
queryable. Low priority.

### Audit complete — codebase delete-pattern inventory

Scanned: `dashboard/app.py`, `engine/`, `scripts/`, all `*.sh`,
`*.sql`, `migrations/`. Categories:

| Pattern | Count | Status |
|---|---|---|
| Active VIOLATION (delete gold without archive) | **1** | LOCKED via `45e57e1` |
| DEFENSIBLE state-sync (positions table on close, Alpaca sync, watchlist remove, season reset) | 13 | trades preserve history; positions are live-state |
| DEFENSIBLE computed-table refresh (rs_rank, minervini_trend, premarket_scan, backtest_extras, gex_levels) | 12 | nightly INSERT-replace pattern |
| Test scaffold cleanup (synthetic scripts) | 6 | not production paths |
| Watchlist user-driven removal | 3 | user intent, not data wipe |

No second active violation found. Sacred Data Rule integrity restored
(pending HM-CLEAN-STALE-ARCHIVE-NOT-DELETE proper fix).

---



**Reconciliation method**: every claim below verified against running code, DB state,
launchctl, trader.log post-PID-84968 startup (15:45 MST today), and on-disk files.
Items moved by category based on observed reality, not historical claim.

---

## Schwab Workflow

**Drop directory:** `/Users/bigmac/autonomous-trader/inbox/` (relocated 2026-05-07 from `/Users/bigmac/Downloads/` per HM-AT-β; previous: 2026-05-04 → `~/Downloads/`; pre-2026-05-04 → `/Users/Shared/schwab_inbox/`).

**How it works:** Admiral scps `Sc*Position*.csv` from Bonnie laptop into `~/autonomous-trader/inbox/`. The launchd watcher `com.ollietrades.schwab-watcher` polls every 60 seconds, finds the file via glob `Scwab*Positions*.csv` / `Schwab*Positions*.csv` / `schwab_*.csv` (case-insensitive), invokes `scripts/import_schwab_csv.py`, syncs via `scripts/sync_schwab_to_real_holdings.py`, archives the CSV to `data/schwab_csv_archive/`, and fires an NTFY notification to topic `ollietrades-admin`.

**Admiral's scp command** (PowerShell on Bonnie laptop):
```
scp "C:\Users\Bonnie\Downloads\Sc*Position*.csv" bigmac@192.168.1.248:~/autonomous-trader/inbox/
```

**Imports log:**
- 2026-05-07 09:14 MST — backlog drain (6 CSVs Apr 30 → May 7) imported during HM-AT diagnosis; archive count 2 → 13.
- 2026-05-04 09:35 MST — fresh snapshot 2026-05-04T12:15:00 imported, 24 rows. Resolved 4-day stale-data display issue (DELL day-change was showing -3.59% from 2026-04-30 snapshot; now correctly -0.77% from today's snapshot).
- 2026-04-30 09:21 MST — snapshot 2026-04-30T11:30:00, 16 rows
- 2026-04-28 09:39 MST — snapshot 2026-04-28T12:30:00, 14 rows
- 2026-04-24 09:54 MST — snapshot 2026-04-24T12:48:00, 8 rows

**Cadence note:** Imports are still manual-trigger (Admiral scps Schwab Positions CSV from Bonnie laptop into `~/autonomous-trader/inbox/`; watcher does the rest). No NTFY reminder added — revisit if drift recurs in 3 weeks.

---

## ⚠️ POST-GATE-FLIP MONDAY MORNING WATCH (2026-05-05 06:30 MST)

Gate flipped 2026-05-04 08:30 MST (commit `df7320c`). Service restarted PID 13734.
First live autonomous trades fire at NYSE open Monday 06:30 MST / 09:30 ET.

**Tier-2 execution gating:** Both `bull_call_spread_v1` and `bear_put_spread_v1`
filter `WHERE agent_name='tractor-beam'` on signal-center reads. **Tractor-beam
is the agent whose performance matters.** Pre-flip 30-day baseline: 268 signals,
34.3% hit_tp, PF 2.02, avg_pnl +1.74%.

### Pre-open (06:00 MST):
- [ ] Service running, PID stable (was 13734 at flip; may have rolled overnight)
- [ ] No overnight Errno 48 in trader.log (baseline = 6)
- [ ] Calibration query produces reasonable numbers (`signals.db::trade_signals` joined to `signal_outcomes`, agent_name='tractor-beam', last 24h)
- [ ] Halted players still 4 (ollama-llama, grok-3, dayblade-sulu, gemini-2.5-pro)
- [ ] All 3 gate sites still `_EXECUTION_ENABLED: bool = True`

### First-trade observation (06:30 - 10:30 MST):
- [ ] First fleet signal of the day fires cleanly
- [ ] First trade placed via Alpaca paper successfully
- [ ] Trade appears in `paper_trades` and `trades` tables
- [ ] Dashboard `/api/agents/scoreboard` reflects the new trade

### Kill-switch criteria — REVERT or HALT if ANY of:
1. **Tractor-beam delivering >5% loss on any single trade** → REVERT (SL discipline failure on the agent that drives execution)
2. **Aggregate paper P&L drawdown >5% from $99,931 starting balance in any 24h period** → HALT for review
3. **Tractor-beam placing >15 trades in first 4 hours** → HALT that signal source (suppress tractor-beam writes to `signals.db::trade_signals`)
4. **2+ service crashes in first 4 hours** → REVERT
5. **ANY real-broker (Schwab/Webull/IBKR) API call attempt** → REVERT IMMEDIATELY

### Recovery procedure (REVERT path):
```bash
cd ~/autonomous-trader
git reset --hard gate-flip-revert
git push --force-with-lease origin main
launchctl kickstart -k gui/$(id -u)/com.trademinds.trader
```

- Pre-flip main HEAD: `753f01a70f2a145a1f2cd70a41143d8188f0ae3d`
- Pre-flip backup: `backups/trader.db.pre-gate-flip-20260504_082909`
- Recovery doc: `/tmp/gate-flip-recovery.md`
- Local-only branch `gate-flip-revert` retained at least 1 week of clean operation

### HALT-tractor-beam procedure (kill-switch #3, less drastic than full revert):
The tractor-beam poster is external to this repo (posts via HTTP to signal-center port 9000). To halt it without reverting the gate:
```bash
# Option 1: Mark all tractor-beam NEW signals as DISMISSED (one-shot scrub)
sqlite3 signal-center/signals.db "
  UPDATE trade_signals
     SET status='DISMISSED', dismissed_at=datetime('now')
   WHERE agent_name='tractor-beam' AND status='NEW';
"
# Then identify and stop the upstream poster (separate investigation needed).
```

---

## ✅ HM-G COMPLETE — origin push unblocked (2026-05-04 07:25 MST)

5 fat files (1.32 GB total) removed from history via `git filter-repo` on a mirror clone, force-pushed to origin. Origin now at `50ef95c` (rewritten HM-C ship). Push 1 (rewrite): `50ef95c`. Push 2 (gitignore prevention): `f7181f0`. All 25 ahead-commits cleared.

Original archive preserved at `~/autonomous-trader-archive/2026-05-04-pre-hmg-rewrite/` (5 files, hash + byte verified). Surgery mirror at `~/git-surgery/autonomous-trader-mirror-20260504_070333/` retained for insurance.

`.gitignore` extended with fat-file prevention patterns: `backups/*.db.*`, `backups/*-shm`, `backups/*-wal`, `*.deprecated_*`, `.fuse_archive_*/`, `/trader.db`, `*.orig`, `*.swp`. Bare `*.bak` deliberately omitted to avoid silently shadowing 20+ tracked sprint backups.

---

## Retired Components

### Kirk Swing Desk — retired 2026-05-04
- Scaffolded but never wired (`agents/kirk.py::propose_swing()`, `agents/pike.py::second_opinion()`).
- Audit #6A investigation determined: drift between CLAUDE.md (claimed active in fleet roster) and code (zero callers, zero scheduler entries). Per `docs/AUDIT_6_INVESTIGATION_2026-05-04.md` Problem B.
- Decision: **RETIRE** rather than build the 6-8 hr feature. Manual swing-trading workflow no longer applies — fleet shifted to autonomous Alpaca-paper-only.
- Daily Kirk advisor (`engine/kirk_advisory.py`, `engine/kirk_grok_advisor.py`) is preserved and active. `kirk_advisory_log` continues to receive daily writes (272 rows, last write 2026-05-01).
- Code archived at `archive/retired/2026-05-04-kirk-swing-desk/` with restoration instructions in README.
- DB tables `kirk_signals` (0), `kirk_swing_trades` (0), `pike_votes` (0) preserved as empty schemas per SACRED-DATA discipline; can be dropped in a future schema-cleanup migration if approved.

---

## VERIFIED CLOSED (commit + reality both confirmed)

| ID  | Closed | Commit | Reality verification |
|-----|--------|--------|----------------------|
| B1  | 2026-05-03 | `8e06b5e` | `bull_spread_v1` `BULL_CROSS`→`BULL` mapping at `main.py:2610`; regime tick log confirms `BULL_CROSS` normalized to `BULL` at scheduler boundary |
| B2  | 2026-05-03 | `8e06b5e` | `bull_call_spread_v1` `get_regime` ImportError eliminated — replaced with `MarketContext` at `main.py:2685-2701`. Zero `get_regime` ImportErrors after PID 84968 startup |
| B3a | 2026-05-03 | `8e06b5e` | Edit 3 replaced broken `get_regime` import with `MarketContext` + regime normalization |
| B3b | 2026-05-03 | `8e06b5e` | Edit 2 regime normalization at `main.py:2648` — `bear_put_spread_v1` inverted block-list now correct |
| B4  | 2026-05-03 | `8e06b5e` | Same as B3b — Edit 2 closes inverted block-list (no separate `bear_put_spread_v1.py:366` edit needed) |
| B14 | 2026-05-03 | `cdc03d0` | `GetAllPositionsRequest` import removed from `engine/alpaca_options.py`. Symbol confirmed absent in alpaca-py 0.43.2; pure dead-code removal, zero behavioral change |
| B15 | 2026-05-03 | `17d40b4` | `OLLIE_URL` added to `initialize_dayblade()` import. **Verification: zero `OLLIE_URL` errors in `trader.log` after line 337403 (PID 84968 startup at 15:45)**. Pre-fix count 53,985, post-fix delta 0 |
| Task 3A | 2026-05-02 | `803c2db` | `engine/importers/ai4trade_importer.py` — `run_import()` alias added |
| Task 3B | 2026-05-02 | `803c2db` | `uoa/scraper.py:16` docstring path corrected |
| Task 3C | 2026-05-02 | `803c2db` | `premarket-scan.sh:46` defunct `launchctl start com.trademinds.crew` commented out |
| Item 5 | 2026-05-03 | `58c43f0` | ~60 lines dead crew-server polling removed from `premarket-scan.sh` |
| **AUDIT-#1** | 2026-05-03 | *pending commit* | **`halt_mode` enum added to `ai_players` (active/exit_only/full); `halt_gate` helper at `engine/halt_gate.py`; gates wired in `paper_trader.save_signal` (line 1870), `paper_trader.buy()` (line 547), `paper_trader.sell()` (line 1091; semantic: `exit_only` permits sells), `signal_tracker.record_signal` (line 35). Backfilled 1,156 leaked rows (`signals` 1,143 + `watchlist_signals` 13). Live gate-fire confirmed via direct exercise test.** |
| **AUDIT-HM#1** | 2026-05-03 | *pending commit* | **`healthcheck.py:43` `print(line)` removed; launchd plist already routes stdout → `logs/healthcheck.log`, eliminating 2× duplication. Truncated `logs/healthcheck.log` for clean post-fix verification window (next cron tick Mon 06:00 MST).** |
| **AUDIT-Open-Q#1** | 2026-05-03 | *pending commit* | **`ollama-llama` trapped positions flatted internally (NVDA 0.3748 sh @ $198.39, MSFT 0.175 sh @ $399.08; total realized -$7.16); `execution_type='manual_internal_mark'` for audit trail. No Alpaca round-trip per Admiral resolution Option B.** |

---

## PARTIALLY DONE (committed but not yet runtime-verified in production)

| Item | Status | Outstanding verification |
|------|--------|--------------------------|
| Edit 3 (`bull_call_spread_v1`) Monday verification | Code-level verified at `main.py:2685-2701` | Runtime verification needs Monday 06:30-13:00 MST market-hours window. Protocol at `/tmp/scotty_session_2026-05-03/b15_verification_protocol.md` |
| `bull_spread_v1` `BULL` normalization | Logged regime ticks confirm `BULL_CROSS`→`BULL` mapping fires | Need observation of an actual bull-spread signal generated post-fix during market hours (none yet — Sunday) |
| `bear_put_spread_v1` block-list correction | Code paths verified | Need market-hours observation that strategy correctly does NOT fire in BULL regime |
| OPS_LOG audit-trail bonus in `8e06b5e` | Healthcheck `backup_trader_db()` has `operation_name` param + writes to `docs/OPS_LOG.md` | Need next backup event to confirm trail writes |

---

## INTENTIONALLY PAUSED (deliberate dormancy, not a bug)

| Component | Pause mechanism | Verified state | Documentation |
|-----------|-----------------|----------------|---------------|
| `dayblade-sulu` (Lt. Sulu primary options trader) | `is_halted=1` in `ai_players` table, `halt_reason='S6.3 bench: R:R 0.10, dormant since 2026-03-31'` | DB-verified halted; `paper_trader.py` `buy()`/`sell()` both gate on `is_halted` (lines 547, 1091) | Drydock 2026-04-25 audit (CLAUDE.md) |
| `dayblade-0dte` (T'Pol on plutus) | Functionally idle: scheduler still runs `run_dayblade` every 5 min at `main.py:2554`, but no signals in DB since 2026-04-07 (26 days) | DB: MAX(`signals.created_at`) for `player_id LIKE '%dayblade%'` = `2026-04-07 15:41:46` | **Note: NOT commented out at main.py:1920 as previously claimed** — that line is `agent_ratings` code. DayBlade run path is live; dormancy is empirical (no trades emitted), not gated. Investigate before next iteration. |
| Battle Station feeders | Not in launchd | `launchctl list \| grep battle` returns 0 entries; `com.trademinds.battle*` does not exist | April 23 surgery, never re-added |
| Battle Station scheduler in main.py | Active: `run_battle_station_monitor` every 2 min at `main.py:2575`, `run_morning_briefing` daily 06:00 at line 2566 | Code-active but downstream feeders absent | "Pause" is partial: scheduler fires but feeders aren't running, so any signal pipeline is broken |
| `ollama-llama` | `is_halted=1`, `halt_reason='S6 review: routing zombie, retired 2026-04-25'` | DB-verified halted | Drydock 2026-04-25 |
| `grok-3` | `is_halted=1`, `halt_reason='S6 review: routing zombie, retired 2026-04-25'` | DB-verified halted | Drydock 2026-04-25 |

---

## ARCHITECTURALLY INCOMPLETE (code half-built, not fully wired)

| ID | Component | Reality | Severity |
|----|-----------|---------|----------|
| AI-1 | `signal_scorecard` table | Schema exists with 16 cols, **0 rows**. Writer never wired (April 7 Alpha Engine plan unfinished). Scoring pipeline can't run without source data. | MEDIUM — blocks gate-flip calibration (B5 dependency) |
| AI-2 | `ghost_trades` table | Only **9 rows total** (verified). Per-agent tables (`sarek_paper_trades`, `janeway_paper_trades`, `surak_paper_trades`, `kirk_signals`) appear to be the actual write paths, leaving `ghost_trades` mostly empty. CLAUDE.md describes Bench 4 ghost-recording every signal. Same import-drift family as B12-B15 likely. | MEDIUM — distorts ghost performance scoring |
| AI-3 | `is_active` flag is decorative | Verified: `paper_trader.py` enforces `is_halted` (lines 547, 1091) but `is_active` only appears once at line 1555 in a `SELECT ... WHERE COALESCE(is_active, 1)=1` filter. Halted players (`ollama-llama`, `grok-3`) still have `is_active=1`. Per April 25 audit, `is_paused`, `crew_role` are also decorative. **Document before any new agent wiring.** | DOC-CRITICAL — easy to mis-trust |
| AI-4 | `bridge_voter` collection | `bridge_votes` table has 216 rows total, MAX `created_at` = 2026-05-01 13:01:23 (2 days ago). Wired but not collecting daily. | LOW-MEDIUM — investigation needed |
| AI-5 | `energy-arnold` quality | `qwen3:8b` LLM, **9,632 signals** total, AVG confidence 0.258. Distribution: 6,643 at conf=0.0 (69%), 1,209 at conf=1.0 (13% over-confident), rest scattered. is_active=1, is_halted=0. Bridge_voter wired but not collecting. | NEEDS-DECISION — high noise volume; Phase 4 reframe |
| H1 | `engine/tiered_exits.py:check_spread_exits()` | Fully implemented, never called by any scheduler | HIGH — needed before first live spread trade |
| H2 | `_EXECUTION_ENABLED = False` | 3 independent copies in `executor.py:22`, `bull_call_spread_v1.py:63`, `bear_put_spread_v1.py:63`. Must flip atomically | DEFERRED — after 30 paper trades + positive expectancy |
| H3 | `/api/wheel/status` | Intermittent 500 at `dashboard/app.py:7592` | HIGH — before Wheel goes live |

---

## OPEN BOMBS (current severity, post-reconciliation)

### Production noise / latent
| ID | File | Severity | Status |
|----|------|----------|--------|
| B5 ✅ RESOLVED 2026-05-04 | `signal-center/server.py:2121` | — | **Audit #6X investigation cleared this.** Scorecard system at `signals.db::trade_signals + signal_outcomes` is healthy (1,147 signals, 100% outcome coverage, daemon writing every 15 min). Endpoint `/api/signals/scorecard` returns HTTP 200 in ~19ms. **NOT blocked on AI-1** — that was a different table in `data/trader.db` audit #6A flagged as separate work. Per Admiral verdict 2026-05-04: gate-flip ready at SQL-level review. Frontend calibration column = follow-up sprint, not blocker. See `docs/AUDIT_6X_INVESTIGATION_2026-05-04.md`. |
| B12 | `main.py:481/484` | MEDIUM | `check_vix_spike` ImportError — no commit yet, B12 status check on Monday per `b12_proposed_fix.md` |
| B13 | `main.py:3608` | LOW | Rallies scraper ImportError — 1 occurrence; deferred |
| B16 | `healthcheck.py:25,474` | MEDIUM (downgraded from CRITICAL) | `TUNNEL_URL` hardcoded to orphan `bridge.accessapple.com`. Real bridge `bridge.ollietrades.com` healthy. Part of accessapple rebrand sprint |
| B17 | unknown XML/lxml caller | MEDIUM | 49 `Filename too long: %3C!DOCTYPE…` in `trader_error.log` — passing HTML body as filename |
| B18 | `engine/fast_scanner.py:389/489-490` | MEDIUM | 34 `database is locked` in `scanner.err` — WAL contention with trader process |
| B19 | aladdin scraper write path | LOW-MEDIUM | 35 db-lock-adjacent entries in `aladdin.log`, same family as B18 |
| B20 | yfinance internal | LOW | 25 `HTTP 401 Invalid Crumb` self-recovers, ~9 retries per burst |
| B21 | iv_history pipeline | **LOW (downgraded from MEDIUM)** | "Day 5 missing 2026-05-02" was a Saturday — iv_history records weekdays only. MAX as_of_date = 2026-05-01 (Friday, 10 rows = healthy). Reframe: H4 ops check applies to the next Monday, not weekend |
| B27 | `healthcheck.py` (Ready Room + Red Alert) | LOW | Crusher weekend false-positives on Sat/Sun |
| B29 | `data/trader.db` `ghost_trades` table | MEDIUM | Folded into AI-2 above |

### Cleanup-eligible (Phase 2 candidates)
| ID | Description | Phase 2 action |
|----|-------------|----------------|
| B22 | Two 0B `arena.db` files (root + `data/`) | **CLOSED 2026-05-03** — archived to `arena.db.deprecated_20260503_182837` and `data/arena.db.deprecated_20260503_182837`. Filesystem-only (gitignored). Rollback: `mv ...deprecated_*` back. setup_db.py confirms files were dead artifacts |
| B23 | `tractor.db` referenced in CLAUDE.md SACRED DATA but file does not exist in `~/autonomous-trader` (lives in `~/ollietrades/tractor_beam/tractor.db` and `/Users/bigmac/G1_BACKUP/`) | Doc drift; address with CLAUDE.md update outside this directive |
| B24 | No log rotation policy. `trader.log` 27.5 MB / 337k lines, `trader_error.log` 13.7 MB / 142k lines | Phase 3 investigation report |
| B25 | 19 `.fuse_hidden*` zombie files (32KB each) | **CLOSED 2026-05-03** — archived to `data/.fuse_archive_20260503_182918/` (19 files, all `lsof`-empty pre-archive). Filesystem-only. Rollback: `mv data/.fuse_archive_20260503_182918/* data/` |
| B26 | `main.py:2554-2587` scheduler comment-vs-cadence drift (11 mismatches confirmed) | **CLOSED 2026-05-03** — commit `9ee1c5c`. py_compile clean. Rollback: `git revert 9ee1c5c` |
| B28 | 4 backup orphan WAL files (`trader_2026-04-07.db-shm/-wal`, `trader_2026-04-08.db-shm/-wal`) | **CLOSED 2026-05-03** — archived to `backups/orphan_wals_20260503_182933/` (4 files). Filesystem-only. Rollback: `mv backups/orphan_wals_20260503_182933/* backups/` |
| HM-COVERED-CALL-RECORDING | Covered-call writes recorded as `action=BUY, qty=+positive` instead of `SELL, qty=-N`. Reasoning column says *"Selling call @ $X.XX. Income generation on existing X position"* but the row debits cash as if it were a long-call buy. Surfaced 2026-05-23 during HM-MASTER-PLAN W2-C navigator review — found 4 orphan covered-calls (LITE/MRAM/COHR/MNST) where the stock leg auto-exited and left the misrecorded option leg behind. Caller site: `engine/chekov_autotrade.py::execute_covered_calls` (path that emits the `COVERED_CALL:` reasoning template). Impact: cash-flow direction reversed at write site; PnL accounting on covered calls is the opposite of reality (worthless-expiry is recorded as a loss when it should be a premium-kept gain). | **OPEN, MEDIUM.** Audit `execute_covered_calls` for the misrouted BUY-vs-SELL action. Fix forward + backfill historical rows (positions cleared via W2-C/W2-D pattern; no live rows remain other than navigator PLD which is still covered). Add a regression test that verifies short-call writes land as `action=SELL, qty<0` in `positions`. Cross-ref: backups/positions_navigator_orphan_covered_calls_20260523_075102.sql for the 4 cleared examples. | **✅ SHIPPED 2026-05-30 (commit `0c0e7c3`).** 4-row sign-only correction applied to `data/trader.db` trades 2540/2541/2542/2543 (LITE/MRAM/COHR/MNST): `realized_pnl` negated `−16.63 → +16.63`. **Ruling: SIGN-ONLY, no ×100** — the `trades` options book stores per-contract (multiplier applied at calc/display, not storage), so ×100 would have made these the only ×100 rows in the table. Clean fleet `270.35 → 303.61` (exactly +$33.26, gate-verified). Pre-state + forward SQL archived: `backups/trades_covered_call_signfix_{pre_,APPLY_}20260530_134402.sql`. NOTE: the *write-site* root cause (`execute_covered_calls` misrouted BUY-vs-SELL action + the regression test) is NOT in this fix — this was a data correction of the 4 historical rows only. Write-site fix + regression test remain OPEN if covered-call writes resume. |

| HM-NAVIGATOR-CONVICTION-BACKFILL | 5 navigator covered-call positions (1400 JTAI / 1401 LRCX / 1402 ON / 1403 QCOM / 1487 MNTS) carried NULL `conviction`/`conviction_source` after the denorm Phase-1 ship (mostly alpaca-mirror + pre-denorm rows). | **✅ SHIPPED 2026-05-30 (commit `0c0e7c3`).** Backfilled all 5 to `conviction=0.78`, `conviction_source='live_buy_backfill'`, each value **sourced from the originating stock BUY's real `trades.confidence`** (not a default/guess — all 5 genuinely 0.78). **0 live-stop changes**: `positions` has no persisted stop column (stop derives from conviction tier at runtime); 0.78 = bottom tier = the prior NULL-fallback tier, and none ≥0.80, so no stop widened. `navigator NULL-conviction remaining = 0`. Pre-state + forward SQL archived: `backups/positions_navigator_conviction_backfill_{pre_,APPLY_}20260530_134402.sql`. |

| HM-SIGNAL-TRADE-FK (rules-scanner path) | Rules-scanner BUYs (`crew_scanner.py::_scan_rules_agent`) dropped `trades.signal_id` AND `trades.prompt_version` — the path called `save_signal()` but discarded its returned id, so `buy()`'s inherited `signals.prompt_version` lookup (`paper_trader.py:1476`) had no id to resolve. (Arena path already threaded signal_id since 2026-05-20, `ai_brain.py:1632`.) | **CODE SHIPPED 2026-05-30 (commit `19e8a42`), behavioral confirm MONDAY-PENDING.** `_scan_rules_agent` now captures `_rules_sid = save_signal(...)` and passes `signal_id=_rules_sid` into `buy()`, so both columns populate via the inherited lookup. Forward-only, no historical backfill; `strategy_id` left NULL by design. Restart applied (live PID 85925). **⏳ A FUTURE SESSION MUST VERIFY:** on the first live rules-scanner BUY after a market-open session (earliest Mon 2026-06-01), confirm `trades.signal_id IS NOT NULL` AND `trades.prompt_version='<player>_rules_v1'` on that row. As of restart, 0 live BUYs had fired (market Sat-closed) so this is code-verified only, not behaviorally confirmed. |

---

## DEFERRED (planned sprints, out of scope tonight)

### TI NEWSLETTER LEARNING LOOP — filed 2026-05-30 (build AFTER current pipeline + Holly repair)

Admiral's vision: a 4-stage LEARNING LOOP where OllieTrades generates its own morning
swing picks, compares them to Trade Ideas (TI) newsletter picks (the "answer key"),
diagnoses misses to tune the scanner, and promotes both-lists-agree setups to a
high-confidence watchlist. **Dependency order matters — most parts need Holly's engine
producing picks first (Stage 3 repair is a prerequisite).** Filed now, build in order.

| Ticket | Depends on | What |
|---|---|---|
| **HM-TI-NEWSLETTER-CAPTURE** | none (parallel) | Parse the daily TI Swing Picks email → structured daily TI-picks table (ticker, entry-trigger price, stop, rationale, the "juice check" market-regime note). The answer-key feed. **OPEN QUESTION for Admiral: ingestion method** — does the email forward to an inbox OllieTrades reads? Confirm before build. |
| **HM-SWING-PICKS-GENERATOR** | Holly engine repaired (Stage 3) | OllieTrades produces its OWN morning swing watchlist — 5–10 tickers (start 10 to get a feel → tune to 5) with entry-trigger + stop + rationale, newsletter format. SWING side (multi-day, long-only, entry-triggered) — DISTINCT from the intraday Holly engine; likely uses Holly's strategy breadth, swing-configured. |
| **HM-PICKS-COMPARISON-LEARN** | both above | The learning core — daily diff our-picks vs TI-picks. For names TI flagged that we MISSED → diagnose WHY (which filter excluded it, which signal we underweighted) and LOG the lesson. Misses inform scanner tuning over time. |
| **HM-CROSS-VALIDATED-WATCHLIST** | comparison | Setups on BOTH lists → high-confidence → auto-add to watchlist; flag both-confirmed as auto-trade candidates IF they match our entry criteria. **Auto-trade execution = separately gated** — entry-triggered swing trades are a different execution model than the current market-order agents; NO auto-execute without Admiral's explicit go. |

**Sequence:** current pipeline (Stage 2 ship → Stage 3 Holly → Stage 4 A/B → Stage 5 launch)
→ HM-TI-NEWSLETTER-CAPTURE (can start parallel, independent) → HM-SWING-PICKS-GENERATOR
(needs Holly) → HM-PICKS-COMPARISON-LEARN → HM-CROSS-VALIDATED-WATCHLIST. Each its own
staged build. DO NOT build now — the loop needs Holly's engine producing picks to compare.

### HM-OLLIE-AI-WORKSPACE — Concept 5 Ollie AI Workspace

**North star:** `USS-Trademinds-Dashboard-Redesigns-v4.3-FINAL.{html,pdf}` (supersedes v4.2).
Lives on Admiral's Bonnie box at `C:\Users\Bonnie\Downloads\`; carry to bigmac via scp or
drop into `~/autonomous-trader/docs/design/` before continuing.

**Concept 5 has 6 sub-views** (was 3 in v4.2):

| # | Sub-view          | Sprint                    | Status        |
|--:|-------------------|---------------------------|---------------|
| 1 | Workspace         | HM-OLLIE-AI-WORKSPACE Step 2 | IN PROGRESS — first pass shipped against v4.2 verbal spec, **needs revision against v4.3** (uncommitted on disk) |
| 2 | Symbol Focus      | HM-OLLIE-AI-WORKSPACE Step 3 | Pending — OPAD-style cockpit + Trade Ticket / Flatten / ½ / Double / Reverse action bar |
| 3 | Signal Replay     | HM-OLLIE-AI-WORKSPACE Step 4 | Pending — FDMT/HE side-by-side + Ollie Signal stamps |
| 4 | Backtest Lab      | HM-OLLIE-AI-WORKSPACE Step 5 | Pending — equity curve + heatmap + filter optimizer |
| 5 | Ollie Wave Scope  | **HM-OLLIE-WAVE**         | Pending — adaptive EMA bands + gainers/losers + treemap |
| 6 | Ollie Machine     | HM-OLLIE-AI-WORKSPACE Step 7 | Pending — 2nd-gen automated momentum + Sim/Live toggle |

**Shipped to date:**
- Step 1 (commit `23d42be`, 2026-05-23) — sidebar 🧠 Ollie AI nav with purple NEW badge + empty `section-ollie-ai` shell.
- Step 2 v4.3 (commit `fb1e7b1`, 2026-05-23) — Concept 5 Workspace sub-view per v4.3 spec L310-374:
  Idea Surfing badge, 6-tab sub-view bar, 8-cell Channel Bar, SPDR sectors + Halts row,
  dual races, movers histogram + Top List Config. Smoke passed.

**Step 2 follow-ups (banked 2026-05-23 post-smoke):**

1. **HM-OLLIE-AI-MOVERS-FIXTURE** — Movers histogram renders "No movers" when
   `/api/movers` returns empty (off-hours, cold cache, or stale-filter excludes all
   rows). Same path also leaves Idea Surfing queue empty. Wire fixture fallback
   when `j.movers.length === 0`. Same fixture pattern as Halts feed. Priority: LOW.
2. **HM-OLLIE-AI-SECTION-ISOLATION** — Portfolio Value + Sector Allocation panels
   from `section-webull` (index.html L6354 region) bleed through above the Workspace
   when viewing `section-ollie-ai`. Persisted across v4.2-pass AND v4.3 rewrite even
   with `min-height:calc(100vh - 120px)` + opaque `background:#05080d` on the section
   wrapper. Diagnosis hypothesis: orphan `.card` elements between `section-ollie`
   close (L8746) and `section-ollie-ai` open (~L9558) OR a section's `display`
   style being overridden by JS elsewhere. Browser DevTools required — read
   `showSection()` flow + scan computed styles in production. Priority: MEDIUM.
3. **HM-OLLIE-AI-SURF-ANIM** — Idea Surfing countdown ring stays static; the
   `conic-gradient(--surf-deg)` CSS-var update from JS every 100ms isn't repainting
   the ring. Likely either: (a) conic-gradient with var() not re-evaluating on var
   change in Safari, OR (b) need to use `@property --surf-deg { syntax:'<angle>'; }`
   for animatable custom property, OR (c) swap to SVG arc/stroke-dashoffset for
   guaranteed cross-browser. Priority: LOW (cosmetic).

### HM-OAI-SIGNAL-REPLAY-POLISH — three deferred items from Step 4b

**Banked 2026-05-24 after Step 4b ship (commit `3fc9a83`).** Signal
Replay is functional with real per-card live wire; these three items
were intentionally deferred from the 4b scope:

1. **True BUY-date lookup for the signal-candle pivot.** Currently the
   "Ollie Signal · {date}" label uses `executed_at` from the SELL row
   (the close date), not the original BUY date. `_oaiPickPivotIdx`
   centers the candle window on the close, so the signal candle is
   effectively the close candle. To show the actual BUY entry pivot:
   - **Path A:** Join `trades` to itself (most-recent prior BUY for
     same symbol + player_id + asset_type) — add a SQL CTE in
     `dashboard/app.py::recent_trades`. ~10 LOC.
   - **Path B:** New endpoint `/api/trades/round-trips?limit=N` that
     returns matched BUY/SELL pairs with both timestamps. Cleaner
     separation, ~30 LOC.
   - **Path C:** Frontend two-fetch: when a SELL is picked, fire a
     second `/api/trades/recent?symbol=X&player_id=Y&before=Z&action=BUY`
     filtered call. Heavier per-pick latency.

   **Recommend Path A** — minimal backend touch.

2. **Short round-trip support.** Current dropdown filter is `action
   LIKE 'SELL%'`. Short trades open with `action='SHORT_SELL'` (or
   `SELL_TO_OPEN` for options) and close with `BUY_TO_COVER`. None
   present in current 200-trade window. When they appear, the "Buy
   Signal" stamp would mis-label them (short entry should show "Sell
   Signal"). Fix: detect direction from the matched OPEN row's action
   (after #1 lands), flip stamp + color logic.

3. **Filter UI for the dropdowns.** Today the dropdowns show all 106
   replayable trades; the Captain scrolls to find a specific signal.
   Add filter pills above the dropdowns:
   - Date range (today / 7d / 30d / 90d / all)
   - Player filter (ollie-auto / navigator / neo-matrix / all)
   - Outcome filter (winners / losers / all)
   - Min |pnl_pct| slider
   Lightweight client-side filter that re-populates the dropdown lists.
   ~2-3h frontend.

**Priority:** LOW. Signal Replay arc is production-ready; these are
ergonomic upgrades. Pick up alongside other HM-OAI-POLISH cluster
work or after Step 5 Backtest Lab ships.

### HM-SIGNALS-RECENT-ACTED-ON-FIELD — `/api/signals/recent` payload omits the `acted_on` column

**Surfaced 2026-05-24 during Step 4a Signal Replay build.**
`dashboard/app.py::recent_signals` (L3790) selects from the `signals`
table but the response payload does not include the `acted_on` column
even though it's defined in the schema (`signals.acted_on INTEGER
DEFAULT 0`). Confirmed via curl + inspection — payload contains
`player_id, display_name, provider, symbol, signal, confidence,
reasoning, asset_type, option_type, created_at, sources, timeframe,
execution_status, rejection_reason` — no `acted_on`.

**Impact:** Signal Replay (Step 4a/4b) can't filter signals by
"actually became a trade" using the canonical field. Step 4a worked
around it by using `execution_status !== 'REJECTED'` as a proxy
(includes EXECUTED + SKIPPED + any other non-rejection state) but the
semantic match is imperfect. Today's signal-db is dominated by
REJECTED rows (47/50 of most recent), so the workaround yields very
few replay candidates.

**Fix paths:**

1. **Add `acted_on` to the SELECT** in `recent_signals()` at
   `dashboard/app.py:3790-3840`. ~1 line change. Frontend filter then
   uses canonical field: `r.acted_on === 1`.

2. **Cross-reference with trades table** for richer replay data — join
   `signals` to `trades` on `(player_id, symbol, created_at)` so each
   signal in the payload includes the resulting trade's
   entry/exit/realized_pnl. Heavier but unblocks Step 4b's outcome %
   computation without separate per-signal trades queries.

3. **Compound — add `acted_on` AND a separate
   `/api/signals/replayable?limit=N` endpoint** that returns only
   signals with corresponding trades + computed outcome %. Cleanest
   for Signal Replay use case; isolates the query optimization from
   the general /api/signals/recent consumers.

**Priority:** MEDIUM. Blocks the broader-scope Step 4b query semantics.
Step 4b can still ship with the execution_status proxy; banking so
the proper fix lands once a backend window opens.

### HM-FUNDAMENTALS-COMPANY-NAME — `/api/fundamentals/{sym}.company_name` falls back to ticker for most symbols

**Surfaced 2026-05-23 during Step 3 Option 3 verification pass.** Tested
7 held Alpaca positions (WMB, INTU, AVGO, SPGI, LLY, F, COST) via
`/api/fundamentals/{sym}` — **all 7 returned `company_name == symbol`**
instead of "Williams Companies, Inc." / "Intuit Inc." / etc.

**Impact:** Symbol Focus cockpit header shows the ticker twice (logo +
ticker line + name line all show the same 4-letter string). Degraded
UX without breaking functionality.

**Root cause** at `engine/stock_fundamentals.py:301`:
```python
company_name = profile.get("longName") or profile.get("shortName") or symbol
```
Falls back to `symbol` when both `longName` and `shortName` are missing
from the yfinance profile dict. Other fields in the SAME endpoint response
(sector="Technology", industry="Software - Application", market_cap,
pe_trailing, target_high, etc.) ARE populated — so yfinance itself is
reachable and returning a profile — just not the name fields specifically.

**Hypotheses:**
1. yfinance API surface shifted; `longName` / `shortName` now under a
   different key (e.g. `name` / `displayName` / `quoteType.shortName`).
2. The fields are now in `Ticker.info` vs the older `Ticker.profile`
   path that `fetch_fundamentals` may be using.
3. Polygon Reference (`/v3/reference/tickers/{sym}`) returns a `name`
   field reliably — could be used as a fallback before falling all the
   way back to ticker.

**Fix paths:**
1. Inspect `engine/stock_fundamentals.py::fetch_fundamentals` to see
   which yfinance call provides `profile`. Compare against current
   yfinance docs.
2. Add Polygon Reference fallback: if `profile.get('longName')` empty,
   try Polygon's `/v3/reference/tickers/{sym}` (`branding` + `name`
   already used by HM-OAI-RACE-LOGOS proposal).
3. Cache resolved names in `data/ticker_metadata` table column
   `company_name` so a one-time backfill makes the issue invisible.

**Priority:** MEDIUM. Cosmetic — cockpit functional without it.

### HM-MARKET-DATA-PREV-CLOSE-INCONSISTENCY — `/api/price.change_pct` flips between 0 and stale-percent across symbols

**Surfaced 2026-05-23 during Step 3 Option 3 verification pass.** Same
sample of 7 held positions:

| Symbol | price | prev_close | change_pct |
|---|---|---|---|
| INTU | 374.44 | 374.44 | **0.00%** |
| LLY  | 1058.72 | 1058.72 | **0.00%** |
| F    | 14.93 | 13.67 | **+9.22%** |
| WMB  | 78.47 | (delta consistent) | +1.23% |

Market is closed (verified via dashboard log `[Market Closed] Active`).
Expected: all symbols should show `price == prev_close` and 0% change,
OR all should show last-trading-day change from prior-close. Mixed
behavior suggests `engine.market_data.get_stock_price` is pulling
`prev_close` from different time-anchors for different symbols (some
get yesterday's close as `prev_close`, others get the same day's close,
producing 0%).

**Impact:** Symbol Focus header sometimes shows `$XXX.XX (0.00 (0.00%))`
which looks like a quote bug; sometimes shows `+9.22%` which may be a
real intraday move or a 2-day-stale prev_close producing inflated
delta. Inconsistent → user can't tell signal from noise.

**Fix path:** audit `engine/market_data.py::get_stock_price`'s
prev_close resolution. Likely needs a unified prior-trading-day
anchor across all symbol sources (Polygon vs Alpaca vs yfinance).
**Priority:** LOW (cosmetic, doesn't affect trade execution).

### HM-SC-ATR-FEED-DISCREPANCY — residual investigation after partial fix

**Banked 2026-05-24 during XO power-run after HM-SC-ATR-INTU-ANOMALY
partial-fix ship (signal-center/server.py, commit pending).**

The outlier-robust mean (cap individual TR at 5× window median) ships
a real ATR robustness improvement and removes single-gap-bar distortion
(verified: INTU's 2026-05-21 −$73 gap-down would be clamped from $82
TR to $66 cap, ATR reduced ~6%).

But the live `/api/trade-levels/INTU` STILL returns ATR=$40 (atr_pct=12.51%)
even though Alpaca daily bars via engine.market_data.get_bulk_daily_ohlcv
show a 14-bar TR series of ~$13 median with one gap-day outlier of $82
→ a simple-mean ATR of $17.83 (clamped: $16.68). Math difference: $40
live API result implies the bars feed signal-center sees has multiple
high-TR bars (not just the one gap day), OR the time-bucket aggregation
in `_compute_trade_levels` is producing wider 'daily' OHLC than Alpaca's
official daily bars (possibly due to extended-hours inclusion in the
intraday-aggregated path).

**Fix path (not in power run):**
1. Add per-bar tracing to `_compute_trade_levels` to dump the actual
   TR list for INTU vs AAPL side-by-side.
2. Cross-check `_bridge_get('/api/charts/ohlcv?symbol=INTU&timeframe=1D&limit=60')`
   response against `engine.market_data.get_bulk_daily_ohlcv('INTU', '3mo')`
   to identify the feed divergence.
3. If extended-hours inclusion is the cause, add a regular-trading-hours
   filter to the bucket aggregator OR switch the source to the
   already-clean daily bars endpoint.

**Note:** The XO power-run partial fix (5× median cap) is correct on
its own merits and ships independently — gap-day distortion is a real
ATR issue regardless of the feed discrepancy. Requires signal-center
restart to activate (Captain decision; not part of trader restart).

### HM-SC-ATR-INTU-ANOMALY — signal-center reports ATR_PCT 12.51% on INTU vs 1-3% normal range

**Surfaced 2026-05-23 during Step 3 Option 3 verification pass.**
`signal-center:9000/api/trade-levels/INTU` returns `atr_pct: 12.51`
where the 6 other sampled symbols range 0.99% (SPY/AAPL) to 5.20% (F).

**Possible legitimate explanation:** INTU had recent rough sessions
(week52 range $302 - $814 per fundamentals payload) and the ATR window
includes a large drop bar.

**Possible bug:** ATR calc in `signal-center/server.py::_calc_trade_levels`
upstream of the trade-levels response may have a stale OR malformed
candle window producing inflated true-range values.

**Impact:** Symbol Focus on INTU shows wide supply/demand zones because
they're synthesized from `resistance ± atr*0.4`. The zones extend well
beyond the visible candle range, forcing the auto-scale to zoom out and
making the candles compress vertically.

**Fix path:** print the candle window signal-center is using for INTU
ATR calculation. Verify it's the right 14-day daily ATR vs intraday vs
fragmentary. Cross-check against ATR(14) on a charting platform.
**Priority:** LOW (cosmetic for INTU; only affects 1 symbol).

### HM-OLLIE-MACHINE — 6th Concept 5 sub-view (depends on backend agent build)

**Deferred sprint.** Banked 2026-05-24 after the Wave Scope arc shipped.
Last sub-view in the v4.3 Concept 5 map (`docs/design/v4.3-FINAL.html`
L621+) — `OLLIE MACHINE · 2ND GEN AUTOMATED MOMENTUM`. Spec shows:

  - Sim / Live toggle (mode pill switcher)
  - Top-3 momentum picks table with score columns
  - Auto-entry optimization settings (per-rank position sizing,
    confidence thresholds)
  - Machine activity log (last 7 actions)

**Why deferred:** the "Ollie Machine" agent **does not exist in
`config.AI_PLAYERS`** today. v4.3 spec describes the UI for a future
2nd-gen automated momentum agent. Shipping the UI now would render
against vapor data forever until the agent is built.

**Prerequisites before any UI work:**

1. **Build the Ollie Machine agent** in `engine/agents/` (or similar
   strategies path). Likely modeled on neo-matrix (rule-based momentum
   scout currently in active fleet). Specifically v4.3 spec mentions
   "2ND GEN" — implies improvements over neo-matrix:
   - Multi-timeframe confirmation (10m + D agreement)
   - Adaptive position sizing tied to momentum strength
   - Earnings-window awareness (skip pre-earnings setups)
2. **Register in `ai_players` DB row** with `halt_mode='full'`
   initially (cost-doctrine path) for backtesting.
3. **Run 30-day backtest pool** vs neo-matrix to confirm edge.
4. **Promote via Admiral approval gate** per CLAUDE.md Free Models
   First doctrine.
5. **THEN ship the UI** — Step 7a (visual scaffold matching spec) +
   Step 7b (live wire to agent's signals + Sim/Live config endpoint
   + auto-entry settings table).

**UI scope (post-agent ship):**

- Top of page: Sim ↔ Live toggle pill (defaults to Sim; flipping
  to Live requires confirmation modal — broker-state mutating)
- Top-3 picks card: sorted by confidence, columns for symbol /
  entry trigger / SL / TP / sizing / confidence score / "ARM" button
- Auto-entry config: position sizing % per rank, confidence floor,
  earnings blackout days
- Activity log: last 7 actions with timestamps + outcomes
- Performance summary: WR / avg R / total $ since Sim/Live flip

**Effort estimate (post-prerequisite):**

- Agent build + backtest: 8-12h (medium agent, leverages existing
  engine/momentum patterns from neo-matrix)
- UI Step 7a visual scaffold: 3-4h
- UI Step 7b live wire: 4-5h
- Sim/Live toggle + broker-submit confirmation: 2-3h (mirrors Step
  3c action-bar pattern)
- Total ≈ 17-24h split across at least 3 ship cycles

**Priority:** LOW — Concept 5 is 5/6 sub-views shipped and the
existing fleet (McCoy + Dax + Neo + Capitol) already covers
automated trading. Ollie Machine is a future strategic upgrade,
not a blocker on any current workflow.

### HM-OLLIE-AI-SYMBOL-FOCUS-HOVER — chart hover crosshair + candle tooltip

**Polish item explicitly deferred from Step 3b.2** (commit `718904c`,
2026-05-23). Step 3b.2 scope item #5 of 5 — hover crosshair / candle
tooltip — pulled out of the shipped scope so the AI line + Battle
Station + zones + monthly inset could land cleaner.

**Surface:** Symbol Focus cockpit main chart (`#oai-chart-svg` inside
`#section-ollie-ai`, populated by `_oaiRenderSvg()`).

**Target behavior:**
1. On `mousemove` over the SVG plot area, project mouse X to nearest
   candle index, render a vertical dashed crosshair line at that X +
   horizontal line at the mouse Y price level.
2. Floating tooltip near the cursor showing the hovered candle's
   OHLCV + date + delta-from-prior-close. SF Mono numerics, gold
   accent for ticker/date.
3. Show the implied price at mouse Y on the Y-axis (small tag pill).
4. On `mouseleave`, hide crosshair + tooltip.

**Implementation notes:**
- Cleanest path: add a transparent `<rect>` overlay covering the plot
  area to capture pointer events without blocking candle interactions.
- Candle index = `Math.round((mouseX - PAD_L) / barW)` clamped to
  `[0, n-1]`.
- Tooltip as a separate absolutely-positioned div outside the SVG
  (easier to style + position via offsetX/offsetY).
- Throttle mousemove handler with `requestAnimationFrame` so re-render
  doesn't tank scroll perf on 90 candles.

**Effort:** ~2-3h (overlay rect + index math + crosshair render +
tooltip DOM + RAF throttle + theme styling).

**Priority:** LOW (polish). Symbol Focus is functional and readable
without it; hover/tooltip is a power-user nicety for precise level
reading.

### HM-OAI-POLISH (post-Step 3c) — three reference-image gaps banked 2026-05-23

Captain reviewed three reference images vs the current Ollie AI build and
flagged three deltas. All are post-Step 3c polish — Step 3c (action-bar
broker wiring + confirmation modal) stays the priority until shipped.
Images not retained on bigmac; descriptions captured below.

**Status (XO power-run audit 2026-05-24):**
- ✅ HM-OAI-RACE-LOGOS — shipped commit `ff4c09f` (2026-05-23)
- ✅ HM-OAI-SYMBOL-FOCUS-TIMEFRAMES — shipped commit `e83e652` (2026-05-23)
- ⏸ HM-OAI-TOP-LIST-FILTER-DIALOG — **DEFERRED out of XO power run.**
  Scope (~5-7h: 9-field min/max input grid + new `/api/movers/filtered`
  backend endpoint + debounced refetch + localStorage persistence)
  exceeds power-run cadence and requires Captain decisions on filter
  defaults + universe scope. Banking for a dedicated session.

#### HM-OAI-RACE-LOGOS — real company logos in race rows
**Surface:** Workspace sub-view → Volatile Race + Large Cap Race rows
(`dashboard/static/index.html`, `_oaiRenderRaces` and the `<div class="oai-race-row">`
template). Currently `.rt` cell shows the bare ticker text.

**Target:** swap the bare-text ticker with a small (~18-22px) company logo
glyph alongside it, matching the reference image. Options:

1. **Polygon `/v3/reference/tickers/{symbol}` logo URLs** — already on the
   Starter plan ($29/mo Stocks + $29/mo Options, per CLAUDE.md "Polygon
   ACTIVE" line). Returns `branding.icon_url` and `branding.logo_url`.
   Cache locally to avoid request burst on race re-renders (8 symbols ×
   2 races every refresh = 16 calls/cycle if uncached).
2. **Logo.dev / Clearbit** free-tier proxy on `https://img.logo.dev/ticker/{SYM}?token=…`
   — easier integration, no Polygon dependency, but third-party rate
   limits + privacy review needed.
3. **Local logo set** in `dashboard/static/logos/{SYM}.png` for the top
   200 most-traded tickers; fallback to text for misses. Lowest runtime
   cost, manual maintenance.

**Recommended:** path 1 (Polygon — paid plan already active) with a
`dashboard/app.py` proxy `/api/logo/{symbol}` that caches `branding.icon_url`
to `data/logo_cache.json` with 30-day TTL. Frontend renders
`<img src="/api/logo/{SYM}" onerror="this.replaceWith(text fallback)">`.

Effort: ~3-4h (proxy + cache + race-row HTML/CSS rework + fallback).

#### HM-OAI-TOP-LIST-FILTER-DIALOG — interactive Min/Max filter inputs
**Surface:** Workspace sub-view → Top List Config card
(`dashboard/static/index.html` L9841-9850 ish, the `.oai-sec--filters`
card with `.oai-kv` rows). Currently each row shows a static
`label · value` pair (Earnings Date: any, Price: $5-$100, Volume Today:
400K-∞, etc.).

**Target:** convert each kv row into an editable Min/Max input pair
matching the reference image, so the Admiral can adjust filters live and
the Top List re-queries. Per-field UI:

| Field | Input shape |
|---|---|
| Earnings Date | dropdown: any / next 7d / next 14d / past 7d |
| Price | min `$X` + max `$Y` (number) |
| Volume Today | min `X` + max `Y` (number with M/K shorthand parser) |
| Float | min + max (M-shares) |
| Short Float % | min + max (0-100) |
| Position in Range | min + max (0-100) |
| Change from Close | min `X%` + max `Y%` |
| Consecutive Days | min `X` (integer) |
| Relative Volume | min `X.X` (float) |

**Backend:** new endpoint `/api/movers/filtered` accepting all 9 filter
params as query string, querying `mover_watchlist` joined to
`ticker_metadata` + `stock_fundamentals` with WHERE clauses. ~2-3h
backend, ~3-4h frontend (input grid + debounced refetch + apply/reset
buttons + persist to localStorage so filters survive reload).

#### HM-OAI-SYMBOL-FOCUS-TIMEFRAMES — multi-timeframe tabs (10m / D / W / M)
**Surface:** Symbol Focus cockpit chart
(`dashboard/static/index.html` `_oaiRenderSvg`, main `<svg id="oai-chart-svg">`
+ surrounding `.oai-chart` div). Currently locked to `timeframe=1Day` from
the `/api/chart-data?timeframe=1Day&bars=90` fetch.

**Target:** add a 4-tab strip above the chart matching the reference image:

| Tab | Backend param | Bars |
|---|---|---|
| **10m** | `timeframe=10Min`*  | ~78 (1 RTH session) |
| **D** | `timeframe=1Day` (default) | 90 |
| **W** | `timeframe=1Week`* OR client-side aggregate 5 daily → 1 weekly | 52 |
| **M** | `timeframe=1Month`* OR client-side aggregate 21 daily → 1 monthly | 36 |

*`/api/chart-data._TF_MAP` (dashboard/app.py:12534-12541) currently maps
`1m/5m/15m/30m/1h/1d` — needs `10m/1w/1M` added. For 1W/1M the cleanest
path is client-side aggregation from the existing 1Day candles (already
fetched for the monthly inset at bars=750) so no backend change required.
10m requires an Alpaca SIP feed call with the new TF; same `_TF_LOOKBACK`
table needs an entry (~2 days for 10m).

Sub-view title (`{SYM} · D` watermark) updates to `{SYM} · {TF}` on tab
switch. Battle Station overlays (PH/PL/VWAP/ORH/ORL) only make sense on
intraday → hide on D/W/M tabs (or recompute prior_high/low from the
selected timeframe).

Effort: ~3h backend (TF_MAP extension + 10m feed) + ~2-3h frontend
(tab strip + state + redraw + sub-view title).

### HM-TRENDSPIDER-INSPIRED — five tickets banked 2026-05-24 from TrendSpider scanner deep-dive

**Banked 2026-05-24 from TS scan menu cross-referenced against bridge.ollietrades.com.**
Key finding: the existing `section-squeeze` Ghost Watcher is a **short-interest squeeze**
scanner (Finviz/Polygon SI + low float + RSI + volume), NOT a Bollinger-inside-Keltner
volatility-compression squeeze. These are orthogonal concepts. Five tickets below close
the gap. Priorities call out the recommended ship order; none scoped yet.

#### HM-SQUEEZE-BBKC-COMPRESSION — Bollinger/Keltner volatility-compression scanner ✅ SHIPPED 2026-05-24

**Shipped 2026-05-24 commits `ecd2d1b` (core, +1002/-18) + follow-up `fed16de` (NTFY signature + per-run cap).**
First TS-inspired ticket from the cluster live. Default-ON via `BBKC_SQUEEZE_WATCHER_ENABLED=True`.

Sunday-bypass one-shot baseline (PID 36748, 4.66s wall, 3,009 symbols):
- **35 PRIORITY** (≥20d coil)
- **141 ALERT** (10–19d)
- **96 WATCH** (5–9d)
- Top by duration: VRE 45d, SHNY 44d, OII 43d, RNA 41d, AXS 39d

**Monday verification note (no separate ticket).** First live cycle at 06:30 AZ
2026-05-25 should:
1. Compare new-row count vs Sunday baseline (272 total). Dedupe should mean
   most rows skip — expect <50 new inserts, mostly tier upgrades.
2. Tier distribution drift: PRIORITY should grow modestly (yesterday's 19d
   ALERTs aging to 20d PRIORITY); WATCH should churn the most.
3. NTFY fired count should be ≤5 (per-run cap). Already-PRIORITY symbols
   from Sunday dedupe out — only fresh PRIORITY entries NTFY.
4. Query: `SELECT threshold_tier, COUNT(*) FROM squeeze_watch WHERE
   kind='bbkc' AND scan_ts > '2026-05-25T13:00:00' GROUP BY threshold_tier;`

#### HM-RS-RANK-VS-SPY — relative-strength rank vs SPY across universe ✅ SHIPPED 2026-05-24

**Shipped 2026-05-24 commit `b265ff7` (+805/-6, 6 files).** Default-OFF via
`RS_RANK_ENABLED`. Live one-shot baseline: 3,026 universe → 2,432 ranked rows in
3.42s wall; SPY 12wk = +8.17%; rank distribution ~24-25 symbols per slot
(clean percentile spread). NVDA rank 78, return +16.49% vs SPY.

Surface live: `GET /api/rs-rank?top=N&min_rank=M`, `GET /api/rs-rank/{symbol}`,
`/api/fundamentals/{sym}` augmented with rs_rank fields, section-fundamentals
cards show RS row + SPY benchmark badge.

#### HM-RS-RANK-OUTLIER-FILTER — gate IPO / penny-stock noise out of the rank

**Banked 2026-05-24 as a follow-up to HM-RS-RANK-VS-SPY.**

Top-10 from the first live scan included entries like ADV +7764% / AGL +14266%
— real returns but on discontinuous price histories (IPOs, reverse splits,
delisted-relisted symbols). The 60-bar lookback hits a near-zero starting
close and the percentage explodes. Outliers crowd the rank=99 slot with
unusable signal.

**Fix (single ~30-min ticket):**
- In `engine/rs_rank.py::_compute_window_return`, gate on
  `start_price >= 1.0` AND `abs(return_pct) < 500.0`. Symbols failing either
  return `(NaN, 0)` → unranked (rank=0).
- Also worth adding: filter `bars_used < 60` from the rankable set so the
  rank pool is apples-to-apples. Currently a 35-bar symbol's 35-bar return
  gets percentile-ranked against 60-bar returns — minor unfairness but
  noticeable on the edges.

**Impact:** ~30–80 symbols drop to unranked (rough estimate from the
+7000% / +14000% tail), tightening the 99-rank slot to genuine leaders.

**Priority:** LOW (current data is usable; filter is a quality tightening).
~30 min scope.

#### HM-OAI-MOVERS-RS-OVERLAY — color movers histogram bars by RS rank

**Banked 2026-05-24 as a deferred surface from HM-RS-RANK-VS-SPY scope.**
Ollie AI Workspace movers histogram currently colors bars by gain/loss sign.
Overlay rs_rank tier (≥70 deep green, 30–69 neutral, ≤29 deep red) as the
fill color instead, so the captain can see "this is up but it's a weak
RS=20 lagger" at a glance. Tooltip already shows symbol + gain%; add
"RS=N (12wk)" line.

**Priority:** LOW–MEDIUM (cosmetic but high-density signal). ~1–1.5h scope.

#### HM-SQUEEZE-PRE-BREAKOUT-COMPOSITE — multi-factor pre-breakout coil scan

Composite scan combining BB/KC squeeze ≥10 days **AND** price in top 25% of 20-day range
**AND** volume contracting (declining 20-day ATR/HV). This is TS's highest-conviction
"coil under the lid" signal — the BB/KC squeeze alone fires too often; the composite
filters to setups with directional bias.

**Depends on:** HM-SQUEEZE-BBKC-COMPRESSION (provides the squeeze input) +
HM-RS-RANK-VS-SPY (optional fourth factor: RS ≥ 80).

**Implementation:**
- Composite computed inside `engine/bbkc_squeeze_scanner.py` (4th tier above PRIORITY,
  call it `COMPOSITE` or `COILED`).
- Same persistence + NTFY pattern.
- Dashboard: third tab on `section-squeeze` ("Pre-Breakout Composite"), or filter chip
  on the BB/KC tab.

**Priority:** MEDIUM (sequential dep on the first two). ~3–4h scope.

#### HM-SQUEEZE-RELEASE-DETECT — alert when an existing squeeze breaks out

Companion alert: when a row already in `squeeze_watch` with `kind='bbkc'` sees BB expand
back outside KC AND a 2σ volume spike on the breakout candle, fire NTFY with
direction (BB upper break = bullish, BB lower break = bearish). Currently we only
alert on entry to the squeeze; the breakout is the actual tradeable moment.

**Implementation:**
- Add release-detect pass to `engine/bbkc_squeeze_scanner.py::run_scan()`: for every
  row in `squeeze_watch` with `tier IN ('ALERT','PRIORITY','COMPOSITE')` and
  `released_at IS NULL`, check current bar against the entry conditions; if released,
  flip `released_at` + NTFY.
- New columns on `squeeze_watch`: `released_at`, `release_direction` (`'up'`|`'down'`),
  `release_volume_ratio`.
- NTFY topic: `ollietrades-admin` (same as short-interest PRIORITY).
- Dashboard: add "Recently Released" subsection on `section-squeeze` showing rows with
  `released_at` within last 5 days.

**Priority:** MEDIUM (closes the loop on HM-SQUEEZE-BBKC). ~2–3h scope.

#### HM-MINERVINI-TREND-FILTER — Minervini Trend Template pass/fail tagging

Daily background job tagging every symbol in scan universe with Minervini Trend
Template pass/fail (8 conditions: price > 150/200 SMA, 150 > 200, 200 trending up
1mo+, price > 50 SMA, 50 > 150, price within 25% of 52wk high, price > 30% above
52wk low, RS ≥ 70). Cheap — all inputs already cached from Alpaca daily bars +
HM-RS-RANK-VS-SPY.

**Depends on:** HM-RS-RANK-VS-SPY (for the RS ≥ 70 condition).

**Implementation:**
- Daily job alongside RS-rank compute.
- New column `trend_template_pass` (boolean) + `trend_template_score` (0–8 count) on
  `stock_fundamentals`.
- Dashboard v1: add column to `section-fundamentals` + filter chip on Ollie AI
  Workspace movers histogram.
- v2 (deferred): use as a hard pre-filter for the Active 4 voters (Capitol, Neo,
  McCoy, Dax) — only buy candidates that pass the template. Requires fleet-side
  approval; v1 is observation-only.

**Priority:** LOW–MEDIUM (foundational filter for any "leader" composite scan; v1 is
data-only, no fleet behavior change). ~3h v1 scope.

### HM-CHART-DATA-EARNINGS-DATES-POPULATE — `/api/chart-data.earnings_dates` declared empty, never filled

**Backend bug surfaced during Step 3b.1 endpoint discovery 2026-05-23.**
`dashboard/app.py:12531` initializes the chart-data response skeleton with
`"earnings_dates": []` but no code path within `chart_data()` populates the
field. Every caller gets an empty list regardless of symbol.

**Impact:** Symbol Focus cockpit (Step 3b.1 / 3b.2) can only plot UPCOMING
earnings via `/api/earnings/countdown?days=14` — past earnings markers
across the 90-bar daily window (per v4.3 spec L417-418 "E" line at mid-
history) require a separate endpoint or this field finally being filled.

**Fix paths** (pick one):

1. **Populate in chart_data()** — fetch `yfinance.Ticker(symbol).earnings_dates`
   (a DataFrame indexed by datetime, columns include EPS estimate/actual),
   filter to the candle window's date range, return as a list of
   `{date: ISO, eps_estimate, eps_actual, surprise_pct}` dicts. ~15 LOC
   inside the existing try/except envelope.

2. **Separate endpoint** — add `GET /api/earnings/history/{symbol}?days=N`
   that returns the same shape. Cleaner separation of concerns; frontend
   makes one extra parallel call. ~25 LOC.

3. **Pull from existing earnings cache** — `data/earnings_cache.json`
   (per CLAUDE.md) is already loaded by `engine/earnings_hub.py` for the
   countdown endpoint; extend the cache schema to retain historical events
   and expose via either path 1 or 2.

**Why deferred:** Step 3b.1 wired upcoming earnings only (good enough for
v4.3 spec's typical day-of-earnings view). Past earnings markers are a
nice-to-have for Symbol Focus historical context but not blocking the
cockpit. Priority: LOW until a Captain workflow specifically needs past
earnings on the chart.

### HM-SIDEBAR-VAR-MIGRATION — Migrate hardcoded `.sidebar` background to `var(--sidebar-bg)`

**Part of the larger v4.4 migration.** The `.sidebar` selector at
`dashboard/static/index.html` L814 correctly uses `background: var(--sidebar-bg)`,
but a media-query override at **L29137** hardcodes
`.sidebar { background:#0a0e17 !important; ... }` for the mobile breakpoint.
The `!important` plus hardcoded color short-circuits the variable cascade —
light-mode + mobile = dark sidebar, dark-mode + mobile = same dark sidebar
but with the wrong shade vs. desktop. L154 also has an explicit
`[data-theme="light"] .sidebar { background:#ffffff; ... }` that re-hardcodes
the value the var should provide.

**Migration steps:**
1. Refactor L29137's mobile-media-query `.sidebar` rule — remove the
   `background:#0a0e17 !important` clause entirely; the desktop rule's
   `var(--sidebar-bg)` will cascade through.
2. Replace L154 explicit light-mode override with reliance on the
   `[data-theme="light"]` `--sidebar-bg` variable (set to `#ffffff` in the
   light-mode variable block at L134-150). Same for the `border-right`
   color which should pull from `var(--border)`.
3. Grep for any other `.sidebar` rules in the file (`grep -nE
   '\.sidebar\s*{[^}]*background' index.html`) and migrate each to the
   variable system.
4. Browser smoke at mobile breakpoint in both themes per Frontend Ship Rule.

**Why deferred:** sidebar background is sensitive — mobile drawer overlay
behavior needs careful testing across all 4 theme combinations
(dark, light, dark-cb, light-cb if [[hm-theme-cb-consolidate]] ships first).
Bundling with the v4.4 migration sprint avoids a one-off touchpoint.

### HM-DEEPSEEK-STOP-DISCIPLINE — ✅ CLOSED 2026-05-24, no action needed

**Closed 2026-05-24 after XO data review.** Initial diagnosis (30-day window:
81.4% WR, PF 0.53, net −$478, loser:winner 1.92×) appeared to indicate a
stop-discipline problem. Task 1 re-pull split the window pre/post the MU
disaster:

| Window | Closes | WR | PF | Net P&L |
|---|---:|---:|---:|---:|
| Full 30d | 118 | 81.4% | 0.53 | −$478 |
| Post-MU-disaster (5-04+) | 61 | 95.1% | **22.03** | **+$162** |

The 30-day report was poisoned by a SINGLE pre-cap MU concentration disaster
(−$671 on 2026-04-30, before HM-DEEPSEEK-STOP-CAP shipped 2026-05-23).
Post-disaster behavior is dramatically healthy. The agent was mis-calibrated
on CONCENTRATION (already fixed by HM-DEEPSEEK-CONCENTRATION-CAP 2026-05-20),
not on stops. Further % tightening risks killing +6% NOW/AMD-class winners
that are normal volatility band.

**XO decision:** no production stop changes. Wait for post-cap live data.

### HM-DEEPSEEK-30D-RECHECK — re-pull deepseek stats due 2026-06-07

**Reminder 2026-06-07 (~2 weeks post-cap).** Run Task 1 query again:
```sql
SELECT COUNT(*) AS closes,
  SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) AS wins,
  ROUND(SUM(realized_pnl), 2) AS net_pnl,
  ROUND(SUM(CASE WHEN realized_pnl > 0 THEN realized_pnl ELSE 0 END) /
        NULLIF(ABS(SUM(CASE WHEN realized_pnl < 0 THEN realized_pnl ELSE 0 END)),0), 2) AS pf
FROM trades
WHERE player_id='deepseek-7b-grok4'
  AND (action LIKE 'SELL%' OR action='COVER')
  AND executed_at >= '2026-05-23'
  AND realized_pnl IS NOT NULL;
```
**Gate**: if post-cap PF ≥ 1.0 AND WR ≥ 70% AND no losses > $150, hold steady.
If PF < 1.0 OR cap-escape occurs, re-open HM-DEEPSEEK-STOP-DISCIPLINE.

Also check W21+ signal volume — if concentration-cap continues to suppress
deepseek to <100 signals/week with 0 executions, the agent is effectively
muted and the post-cap denominator stays zero (no data to compare against).

### HM-RISK-MANAGER-CONVICTION-STOP-WIRE — wire conviction-scaled stop into live path

**Banked 2026-05-24 for a future power run.** Code-debt fix; orthogonal to
the deepseek decision.

`engine/risk_manager.py:115` already exposes `get_stop_loss_pct(conviction)`:
- conv ≥ 0.90 → 18%
- conv ≥ 0.80 → 15%
- conv ≥ 0.70 → 12%
- conv < 0.70 → 8%

The static helper is used by `engine/backtester.py:459` but **not** by the
live exit path. `engine/risk_manager.py:785` reads
`get_model_guardrail("stop_loss_pct", self.stop_loss_pct)` which returns
the per-model override OR the constructor default — never the conviction
scaler.

**Fix (~10 LOC):** at L785, prefer `get_model_guardrail` if present, else
fall back to `get_stop_loss_pct(pos.get('confidence') or 0.7)` instead of
`self.stop_loss_pct`. High-conviction trades get wider stops; low-conviction
get tighter (the existing wisdom that's currently buried in the static).

**Backtest required:** replay last 30 days with conviction-scaled stops,
compare to flat 12%. Acceptance: aggregate PF improves AND no single agent
sees a WR drop > 10 pts.

**Risk:** changes live exit behavior for every agent. Worth its own scoping
session — Captain decides activation per-agent or fleet-wide.

### HM-DOCTRINE-SHORT-INTEREST-READING — agent-prompt note: high SI is bullish (squeeze fuel), not bearish

**Banked 2026-05-24 during HM-ONDS-COVER trading review.** Two agents
opened ONDS shorts on 2026-03-30 citing high short interest as BEARISH
evidence:

- gemini-2.5-flash (signal_id 46119): "ONDS is experiencing a short squeeze
  (41% short float)... a short position with a stop-loss above $8.80
  appears prudent."
- dalio-metals (signal_id 46525): "short interest extremely high (34.1%
  of float), indicating significant bearish sentiment."

Both agents read high SI as confirming a downtrend. **Inverted read.**
Classically high short interest is **bullish-via-squeeze** — the trade
is crowded SHORT, exposing the symbol to violent upside as covers pile
in. The agents took the side they should have been against.

Outcome: both shorts held 55 days, closed −$106.95 combined via
HM-ONDS-COVER 2026-05-24. ONDS traded $9.07 vs entries $8.80 / $7.88.

**For future agent prompt tuning** (no immediate action — Admiral
decides timing):
- Add to gemini/dalio prompt context: "High short interest (>20% float)
  is a bullish squeeze setup, NOT bearish confirmation. Short the
  symbol only if SI is LOW and price is breaking down on volume."
- Consider a doctrine-level guardrail: reject SHORT signals where
  `short_float_pct > 20` AND reasoning mentions "short squeeze" as
  bearish evidence.
- Pattern is broader than ONDS — sample other historical SHORT signals
  to see how often this inversion appears in the corpus.

**Priority:** LOW (single-event harm so far). Watch for repeat pattern;
size of corrective action scales with frequency.

### HM-EXIT-TRAILING-STOP-TIER-DOCTRINE — pending Admiral decisions before build

**REVIEWED 2026-05-30 (Admiral):** trailing-tier philosophy **REAFFIRMED as
wider-high (NOT inverted).** `engine/stops.py::get_trail_pct` keeps the
Admiral-locked 5/4/3% tiers (≥0.90 → 5%, ≥0.80 → 4%, <0.80 → 3%). A
"tighter-high" inversion (lock profits on high-conviction winners) was
considered and **rejected** — the existing wider-high doctrine ("let winners
breathe past short-term pullbacks", matching `get_stop_loss_pct`) stands.
**Activation deliberately DEFERRED:** `CONVICTION_SCALED_TRAIL_ENABLED` stays
unset → **flat 3% trail is live by choice**; the 5/4/3 tiers remain
documented-but-off until live shadow data justifies flipping the flag. Zero
code changed this review. The runner-tier Q1–Q4 below stay OPEN/DEFERRED as a
**separate** future decision — NOT blocking, and orthogonal to the trail-%
tiering above.

**Banked 2026-05-24 after HM-EXIT-TRAILING-STOP-TIER scope surfaced a
critical reframe.** Scope analysis showed a 5th runner tier does NOT
recover the MU $1,916 miss because that move was intra-bar (+418% in
one minute — all 4 tiers ripped on the same bar). Runner helps grinder-
class trades that compound past +50% over weeks, not instant-rippers.

**Four decisions needed from Admiral before build:**

Q1: **What case are we optimizing for?**
  - (a) Capture intra-bar rippers like MU $533 — runner doesn't help,
    needs different solution (event detection + sell-rate damping that
    detects a tier-cascade and pauses the ladder)
  - (b) Capture multi-week compounders past +50% — runner helps
  - (c) Both — needs a 2-part solution

Q2: **Tier weight interpretation A or B?**
  - (a) Reduce tier 4 `sell_frac` only → runner = ~3% of original
    position (tiny tail; meaningful only on the largest winners)
  - (b) Restructure ladder weights (e.g. 0.4/0.4/0.4/0.5) →
    runner = ~22% of original (bigger spec change; affects every
    agent's tier shape, not just runner-enabled ones)

Q3: **Path A or Path B for the trailing stop?**
  - (a) Path A — existing 3% fleet trail (zero new code; trails
    catch fast; small contribution)
  - (b) Path B — wire V3 conviction-scaled trail (10% at +20% gain,
    `_get_trailing_stop_pct` already in `risk_manager.py:851` but
    only used by backtester). Parallel ticket
    HM-RISK-MANAGER-TRAILING-V3-WIRE; affects all fleet agents

Q4: **Which agents opt in?**
  - `ollama-plutus` is obvious (the MU $533 trade is its winner)
  - `super-agent` and `neo-matrix` are candidates
  - Capitol (data feed) and deterministic strategies probably not
  - Each agent's opt-in goes in `MODEL_GUARDRAILS["agent_id"]["runner_pct"]`

**Cross-reference:** scope report in this session (HM-EXIT-TRAILING-STOP-TIER
Captain-framed scope, 2026-05-24). Build is on HOLD until Admiral
answers the 4 questions in a clean session.

### HM-SHORT-RULES-PATH — wire the rules-scanner SHORT stub to short_sell() (deferred)

**Filed 2026-05-30 alongside HM-SHORT-ACTIVATION (LLM-path-only build).** The
crew_scanner rules path has a `SHORT_LOGGED` stub at `engine/crew_scanner.py:3542`
("SHORT — logged, not executed") that drops rules-driven short signals. HM-SHORT-
ACTIVATION wired only the LLM/arena path (agents emit `action="SHORT"` →
`execute_signal` → `short_sell()`), which is the proven path. This ticket wires the
rules stub to `short_sell()` too, so rules-scanner agents can short. **Gated on
HM-SHORT-ACTIVATION being proven live first** (don't add a 2nd execution surface
before the 1st is validated). When built, it inherits the same `short_guard`
safeguards automatically (they live inside `short_sell()`).

### HM-SHORT-EARNINGS-DATA-GAP — ✅ RESOLVED 2026-05-30 (all-paid-source repoint)

**Found then fixed same day.** Original gap: `short_guard._earnings_within` relied on
yfinance `.calendar` (empty under Yahoo throttle → earnings sub-guard always n/a).
**Fixed** in the Admiral-directed data-source audit: earnings now sourced from
**Finnhub `/calendar/earnings`** (paid, already wired at
`engine/finnhub_data.get_earnings_calendar`, used live by event_shield +
channel_scanner; probed HTTP 200, 122 rows/6d). **Proven on real data in the
re-run dry-run:** ULTA (reporting ≤3d) → `_earnings_within=True` → squeeze_block
BLOCK `[finnhub]`. The "no rows = fetch failed = fail-closed" rule distinguishes
"no earnings" (False) from "couldn't check" (None) since the market-wide window is
never genuinely empty. **No yfinance, no Finviz** in the earnings path.

### HM-FINVIZ-ELITE-FLEETWIDE — repoint the 4 free-scrape modules to authed Elite (MED, deferred)

**Filed 2026-05-30 alongside HM-FINVIZ-ELITE-AUTH STEP 2.** STEP 2 wired the authed
Elite export (`login_submit.ashx` → `.ASPXAUTH` → `export.ashx?v=131`, parses
`Short Float`) into `engine/short_guard.py` ONLY (the RED short-guard bundle). Four
other modules still use the `finvizfinance` FREE SCRAPE: `squeeze_scanner.py`,
`premarket_scanner.py`, `finviz_sectors.py`, `scripts/scotty_backtest.py`. This
ticket extracts the short_guard Elite-session helper into a shared client and
repoints those four. Deferred to a separate pass (not bundled into the RED short
change). Trigger: after the short-guard Elite bundle ships + bakes.

### HM-FINVIZ-ELITE-AUTH — wire the paid Finviz Elite API (pay-but-don't-use) (MED) — STEP 2 BUILT 2026-05-30, staged for eyes-on

**UPDATE 2026-05-30:** STEP 2 BUILT (staged, awaiting Captain dry-run approval).
Authed Elite export wired into `short_guard.py` (`export.ashx?v=131` → `Short
Float`); SI%>20 gate restored as the 3rd squeeze gate with **Option-B graceful
degrade** — Elite up = 3 gates (DTC+earnings+SI%), Elite down = 2 gates
(DTC+earnings still fail-closed), never 0 gates, never falls to free-scrape. Per-
verdict gate logging makes the degrade visible. Dry-run proved all 5 scenarios incl.
BYND 64% SI → BLOCK [finviz-elite] and Elite-down → DTC still blocks GME. NOTE: GME
real SI%=14.2% is BELOW the 20% floor → SI gate does NOT block GME (DTC 7.8>5 does);
"close the GME gap on SI%" needs SQUEEZE_SI_PCT_MAX lowered — an Admiral call. The
fleetwide repoint of the other 4 free-scrape modules is HM-FINVIZ-ELITE-FLEETWIDE.

### HM-FINVIZ-ELITE-AUTH (original filing) — wire the paid Finviz Elite API (pay-but-don't-use) (MED)

**Surfaced 2026-05-30 in the short-guard data-source audit.** We PAY for **Finviz
Elite (valid through Dec 2026)** — `FINVIZ_EMAIL`/`FINVIZ_PASSWORD` are in `.env` —
but the code uses the **`finvizfinance` free scraper** (no auth), which is the same
rate-limit/silent-degrade class as yfinance. **Probed live 2026-05-30:** the Elite
login works — `POST finviz.com/login_submit.ashx` → 200 + `.ASPXAUTH` cookie →
`elite.finviz.com/export.ashx?v=111&t=AAPL` returns CSV. So Elite is wireable today;
it's just not wired. Impact: (a) the short guard had to **DROP SI %-of-float** (no
reliable wired source — Polygon lacks float, free-Finviz is unreliable), leaving DTC
as the sole structural squeeze metric; (b) system-wide, every `finvizfinance` caller
(`squeeze_scanner`, `premarket_scanner`, `finviz_sectors`, `scotty_backtest`) is on
the free scrape. **Wiring Elite** (session-cookie or token export client) would
restore SI %-of-float to the squeeze guard (re-add `SI%>20` block alongside DTC>5)
AND give all agents reliable Finviz data. **Trigger:** after shorting ships.
**When done:** restore `SQUEEZE_SI_PCT_MAX` block to `short_guard.squeeze_block`
(the constant is still defined, unused, ready).

### HM-THEME-V4.5-DEPRECATIONS — remove legacy compat shims one release after V4.4

**Banked 2026-05-24 per 47's note.** After HM-THEME-CB-CONSOLIDATE v4.4
(Path C) bakes for one release and HM-INLINE-STYLE-SWEEP completes:

1. **Remove `html[data-theme]` legacy mirror** — V4.4 routes everything
   through `body[data-uss-theme]` (or whatever attribute 47's module
   picks). The legacy `html[data-theme="light"]` selectors + the JS that
   keeps `html.setAttribute('data-theme', ...)` in sync are compat-only.
   Once V4.4 ships and no consumer (CSS, JS, third-party) reads
   `html[data-theme]`, drop the mirror.
2. **Remove `--green` / `--red` / `--accent` aliases** — V4.4 introduces
   semantic names like `--up` / `--down` / `--brand` (47's diagnosis
   per the HM-CB-PATH-A history). The migration IIFE in Block 4 will
   set up alias bindings (`--green: var(--up)`) so legacy consumers keep
   rendering. After HM-INLINE-STYLE-SWEEP migrates 575 inline `style=""`
   color hardcodes to vars, audit which consumers still touch the
   aliases. Drop the unused ones.
3. **Remove `data-cb="true"` orthogonal attribute** — V4.4's single-axis
   `data-uss-theme` enum supersedes the orthogonal `data-theme` ×
   `data-cb` model. The Block 4 migration IIFE handles state migration;
   the old attribute can stay one release as read-only fallback then
   be dropped.

**Trigger:** ship V4.4, soak ≥1 week (one full RTH week minimum), confirm
no consumer is reading the legacy paths (grep + browser DevTools sweep),
then ship the deprecation removal.

**Note:** the exact attribute names / var names in this entry are
placeholders inferred from 47's HM-CB-PATH-A diagnosis ("v4.4 light-cb
selector model" + "v4.4 CSS variable names don't exist (--up, --down,
--up-bg)"). XO should patch this entry with 47's literal naming once
the V4.4 module lands.

### HM-INLINE-STYLE-SWEEP — replace 575 hardcoded inline style="" colors with CSS vars

**Banked 2026-05-24 during HM-CB-PATH-A.** 47 diagnosed 575 inline `style=""`
attributes carrying hex colors throughout `dashboard/static/index.html`. Each
is a theme-blind hardcode that the theme switcher can't lift. Mostly cluster
in:
- SVG inline backgrounds: `style="background:#0d1117"` on `.oai-chart-svg`,
  `.oai-sr-svg-{left,right}`, `.oai-bl-equity-svg`, `.oai-ws-svg`, etc.
- Card backgrounds: `style="background:var(--card-bg)"` or hex equivalents
- Color literals on text spans
Scope: too big for a single commit; needs a sweep script + per-pattern review.

**Approach:** scripted `sed -i` against documented patterns (e.g.
`background:#0d1117` → `background:var(--panel)`) followed by visual smoke
across all 12 sections. ~3-4h with careful testing. Not in HM-CB-PATH-A scope.

### HM-THEME-CB-V4.4-UNIFIED — full body[data-theme="light-cb"] migration

**Banked 2026-05-24 during HM-CB-PATH-A.** Path A kept the legacy orthogonal
model (data-theme=light/dark × data-cb=true/false). v4.4 design spec calls
for a unified single-axis `data-theme` enum: `dark | light | dark-cb | light-cb`.

Migration would require:
1. Renaming `[data-cb="true"]` selectors → `[data-theme$="-cb"]` (or split
   into `[data-theme="dark-cb"]` + `[data-theme="light-cb"]`)
2. Updating `toggleColorblind()` and `applyThemeUI()` to write composite values
3. Migrating existing localStorage `tm-theme` + `tm-cb` to a single `tm-theme`
   with composite value
4. Updating any consumer code that reads `data-cb` directly

Risk: high CSS churn; needs explicit Captain decision on default + back-compat.
Path A was the tactical fix to the visible regression. v4.4 unification is the
strategic rewrite — deferred for a dedicated session.

### HM-THEME-CB-CONSOLIDATE — Unify dual colorblind systems + migrate to data-theme axis

**Lands after v4.4 ships.** Two parallel colorblind systems exist in
`dashboard/static/index.html` today, each with its own button + storage key +
CSS target. Confusing, orthogonal, and the `data-cb` flag is independent of
`data-theme` which forces 4 css-rule matrices instead of a single theme axis.

**System A — KEEP:**
- Button: `#cbBtn` at L2908 (top nav, "CB" label)
- Function: `toggleColorblind()` at L23234
- Flag: `data-cb="true"` set on `<html>`
- Storage key: `tm-cb`
- CSS targets: `[data-cb="true"]` blocks at L403, L407-409, L523

**System B — REMOVE:**
- Button: `#cb-mode-btn` at L8476 (inside sniff-scan page, "CB" label)
- Function: `toggleCBMode()` at L31774
- Flag: `body.classList.add('cb-mode')` (uses a class, not an attribute)
- Storage key: `cb_mode`
- CSS targets: `.cb-mode` selectors (audit before deletion)

**Migration steps:**
1. Delete `#cb-mode-btn` button at L8476 + `toggleCBMode()` function at L31774-31787.
2. Grep for `.cb-mode` selectors — migrate any unique styling into the kept
   `[data-cb="true"]` blocks or fold into the new `data-theme="light-cb"`
   value (see step 4).
3. localStorage cleanup: write a one-time migration that, on page load, if
   `cb_mode === '1'` and `tm-cb` is unset, set `tm-cb = 'true'` then
   `localStorage.removeItem('cb_mode')`. Run once, remove the migration shim
   after a week.
4. Theme-axis migration: replace the orthogonal `data-cb` attribute with a
   composite `data-theme` value. New scheme:
   - `data-theme="dark"` (default)
   - `data-theme="light"` (current light mode)
   - `data-theme="dark-cb"` (dark + colorblind palette swap)
   - `data-theme="light-cb"` (light + colorblind palette swap)
   `toggleColorblind()` becomes a function that appends/strips `-cb` to the
   current theme value. localStorage stays at `tm-theme`. CSS selectors
   collapse from `[data-cb="true"]` + `[data-theme="light"]` cross-products
   into clean per-theme rule blocks.

**Why deferred to post-v4.4:** v4.3 → v4.4 sprint is touching the theme system
heavily already (HM-LIGHT-MODE-FIX 2026-05-23 just landed). Stacking another
theme-axis refactor on top risks visual regressions during smoke. Pick this
back up once v4.4 has soaked for 48h.

**Verification target:** all current `[data-cb]` and `.cb-mode` styling
continues to render correctly in all 4 theme combinations after migration.
Browser smoke required per Frontend Ship Rule.

### Accessapple rebrand cleanup sprint
**Verified count: 22 references across 6 files** (down from claimed 24):
- `healthcheck.py` (2)
- `main.py` (1)
- `dashboard/app.py` (11)
- `docs/G1_MIGRATION_INVENTORY.md` (5)
- `docs/SECURITY_AUDIT.md` (3)
- `docs/XO_BACKLOG.md` (this file, references)

Pre-sprint checklist (unchanged from prior version):
1. Confirm `bridge.ollietrades.com` is in CORS allow-list at `dashboard/app.py:1237` (don't just swap — *verify*)
2. `git remote -v` to confirm GitHub remote — is `accessapple2/BigMac.git` still valid or also renamed?
3. After fix: end-to-end test from external browser via `bridge.ollietrades.com` → dashboard → API call
4. Update `healthcheck.py:481-487` success criteria to accept 2xx/3xx (Cloudflare Access redirect = healthy)
5. Pair with B16 fix — fixing only the URL without success-criteria fix leaves Crusher still flagging stale on the 303

**Why deferred:** sprint touches CORS (security boundary) and external API docs (user-facing). Needs Admiral approval + a weekday window with browser at hand for verification.

### UX Sprint (`docs/UX_SPRINT_2026-04-28.md`)
All acceptance criteria unchecked — sprint never started.
- Priority 1: Risk-adjusted Leaderboard (Sharpe/Sortino/max DD/calibration columns)
- Priority 2: Today's Read Strip + Collapsible Cards
- Priority 3: Plain Mode Toggle

### Chekov Rehab
- Extract S5 version: `git show 859a4f0:engine/chekov_autotrade.py`
- Ghost-trade S5 vs current for 30 days, promote the better one
- Current threshold: 5.0 (muted)

### Bench 4 Ghost Runs (none started)
- Uhura-EDGAR: 60-day ghost run, promote if Sharpe > Capitol's
- Aladdin: wire iShares ETF flow → paper-trade sector rotation
- Spock-R1: 60-day A/B vs McCoy-alone (`ollama pull deepseek-r1:7b` first)
- Picard: convert weekly briefing → Ollie regime-table modifier

### Other deferred
- Phase 2 historical performance forensics across trader.db, signals.db, arena.db
- Phase 3 new backtests for orphaned strategies (`engine/options_agents.py` classes)
- Phase 4 spread strategy comparison report
- signals.db archival cron — first eligible 2026-05-05

### HM-GAMEPLAN-EARNINGS-NULL-FIX ✅ RESOLVED (shipped 2026-05-23 same day banked)

Fix landed via `fix(dashboard): null guard in _gpEarningsRows earnings
renderer` — see commit log. Filter now requires both `e.ticker` AND a
string `e.date`; belt-and-braces fallback on the .slice() call as well.

(Original banking preserved below for the audit trail.)

**Symptom:** `Uncaught (in promise) TypeError: Cannot read properties of
null (reading 'slice') at _gpEarningsRows`

**Location:** `dashboard/static/index.html:4911` inside
`function _gpEarningsRows(earnings)`. Pre-existing bug, NOT introduced
by the W5-Sidebar Consolidation — surfaced during smoke because the
checklist asked the Captain to verify a clean DevTools console.

**Cause:** the function guards `!earnings || !earnings.length` at
line 4905 and filters items with truthy `e.ticker` at 4906, but
never guards `e.date === null`. Any earnings record with a ticker
populated but a null date triggers the throw at:

```js
html += '...'+e.ticker+'</span> '+e.date.slice(5)+...
//                                  ^^^^^^^^^^^^^^^ throws
```

Upstream cause is likely Polygon / yfinance returning a record with
a ticker but no confirmed earnings date for a freshly-listed or
recently-IPO'd symbol — happens intermittently, hence why the bug
was latent rather than constant.

**Fix shape (when picked up):**
```js
var datePart = (e.date && typeof e.date === 'string') ? ' '+e.date.slice(5) : '';
html += '...'+e.ticker+'</span>'+datePart+...;
```

Defensive null + type check on `e.date`, fall back to empty string
so the ticker chip still renders without the date suffix.

**Sibling check:** `_gpCongressFlags` at line 4917 has similar
shape — guards `c.amount || c.amount_range || '—'` defensively
(D4 comment shows the same Pattern was caught earlier there).
Same null-guard discipline needs to land in `_gpEarningsRows` and
likely a few other `_gp*` formatters that consume backend-shape
records.

**Why deferred:** non-blocking — Game Plan card still renders other
sections; the earnings sub-row throws silently and falls through.
Fix is a one-liner but worth pairing with a broader `_gp*` audit
to catch sibling null-guard misses in one sweep.

### HM-LIVE-TRADING-WS-PUSH (banked 2026-05-23, Holly Live Trading scope close)

**Context:** the Holly Live Trading view shipped with **polled REST**
data path (5s candle poll + 10s trade-event poll) per Captain decision
during scoping. Push-based ticker stream deferred to this ticket.

**Build:** new backend Server-Sent Events endpoint wrapping the
existing `engine/realtime_monitor.py` Finnhub WebSocket (L228-234)
into a browser-consumable stream. Frontend `EventSource` subscribes,
gets `{symbol, price, ts}` events as they arrive. Same endpoint can
multiplex Alpaca trade events from the existing event bus so the
marker overlay also gets push delivery instead of poll.

**Shape sketch:**
```python
@app.get("/api/live/stream")
def live_stream(symbol: str = "SPY"):
    """SSE — emits {type:'tick'|'trade', ...} for the symbol."""
    def gen():
        while True:
            ev = next_event()  # blocking on Finnhub queue + trade bus
            yield f"data: {json.dumps(ev)}\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream")
```

**Frontend wiring:** replace `_ltPoll` loop in index.html with:
```js
var es = new EventSource('/api/live/stream?symbol=' + currentSymbol);
es.addEventListener('tick',  function(e) { _ltAppendTick(JSON.parse(e.data)); });
es.addEventListener('trade', function(e) { _ltAppendTrade(JSON.parse(e.data)); });
```

**Why deferred:** polling shipped tonight is the correct trade-off for
EOD scope — 10s marker freshness feels live enough; ship-tonight beats
ship-in-3h. Push upgrade is pure perf, no functional change.

**Affected files (est):**
- `dashboard/app.py` — new ~80 LOC SSE endpoint + queue subscription
- `engine/realtime_monitor.py` — expose internal event queue to FastAPI
- `dashboard/static/index.html` — swap `_ltPoll` for EventSource (~40 LOC)

### HM-ADMIN-ONLY-CSS-DEFAULT-DENY ✅ RESOLVED (shipped 2026-05-23 same day banked)

Fix landed via `feat(security): HM-ADMIN-ONLY-CSS-DEFAULT-DENY
default-deny gate` — see commit log. Changed line 329 from
`body.role-observer .admin-only { display: none !important; }` to
`body:not(.role-admin) .admin-only { display: none !important; }`.
Pre-auth load also benefits from the fail-safe (no role class yet
= treated as non-admin = hidden) at the cost of a brief flash on
slow networks when fetchMe() resolves the admin role.

(Original banking preserved below for the audit trail.)

**Finding:** the `.admin-only` CSS gate in `dashboard/static/index.html:329`
is one line and gates ONLY for `role=observer`:

```css
body.role-observer .admin-only { display: none !important; }
```

`role=admin` → gate doesn't fire → admin elements visible ✓
`role=observer` → gate fires → hidden ✓
**`role=charts` (or any future non-admin/non-observer role) → gate doesn't fire → admin elements visible** ✗

The system already has at least 3 roles per the auth middleware
(admin / observer / charts). Default-allow is the wrong posture for
admin-only — should be default-deny.

**Affected elements** (verified via grep of `.admin-only` in index.html):
- Sidebar nav: War Room (1 item + 1 group header — added during W5
  Sidebar Consolidation 2026-05-23)
- Model Control: 3 buttons (Pause All / Fallbacks / Force Scan)
- Ollie Fleet: ⚡ Send to Fleet button
- Trade Desk (or related panel): ⚡ Send to Fleet button
- Trade Desk Alpaca buttons: BUY / SELL / CLOSE ALL (3 buttons)
- Backtest panel: Run Backtest / Run Inverse Backtest (2 buttons)
- Whole-section gates: `section-strategy-lab` and `section-kill-switch`
  (both have `class="admin-only"` on the section div)

**Fix shape (when picked up):**
```css
/* Replace L329 with default-deny */
body:not(.role-admin) .admin-only { display: none !important; }
```

Or explicit per-role list if `.admin-only` needs to be visible to
some intermediate role. Verify no regression for the admin role
(should see everything as before).

**Why deferred:** simple one-line fix in isolation, but a security
boundary edit — wants a Captain-attended verification across all 3
roles (admin, observer, charts) before shipping. Pair with role
documentation in CLAUDE.md so future Claude sessions understand
the full role surface.

---

## ARCHITECTURAL ORPHANS (code exists, zero wiring to main.py)

| Agent/Class | File | Strategy | Wiring Status |
|-------------|------|----------|---------------|
| `QuarkIronCondor` | `engine/options_agents.py` | Iron condor | No scheduler entry |
| `McCoyBullPut` | `engine/options_agents.py` | Bull put spread | No scheduler entry |
| `AndersonBearCall` | `engine/options_agents.py` | Bear call spread | No scheduler entry |
| `CoveredCallAgent` | `engine/options_agents.py` | Covered call | No scheduler entry |
| `GhostKirkBullCall` | `engine/options_agents.py` | Ghost bull call | No scheduler entry |
| `GhostKirk0DTEBullCall` | `engine/options_agents.py` | Ghost 0DTE | No scheduler entry |
| `GhostLongCall` | `engine/options_agents.py` | Ghost long call | No scheduler entry |
| `GhostNakedPut` | `engine/options_agents.py` | Ghost naked put | No scheduler entry |
| `check_spread_exits()` | `engine/tiered_exits.py` | Model F exits | Imported at main.py:3952 but never called |
| `bear_call_spread()` | `engine/spread_trader.py` | Bear call spread | `SPREADS_ENABLED=False`, scaffolding only |
| `iron_condor()` | `engine/spread_trader.py` | Iron condor | `SPREADS_ENABLED=False`, scaffolding only |

---

## HIDDEN BOMBS (latent, not yet exploding)

| ID | File | Description | Trigger |
|----|------|-------------|---------|
| X3 | `strategies/bull_call_spread_v1.py:2691` | `ctx = {"regime": get_regime()}` — dict not MarketContext (note: regression check needed against `8e06b5e` Edit 3) | After import fix |
| X4 | `main.py:3952` | `MODEL_F_THRESHOLDS` imported at startup, `check_spread_exits()` never scheduled | When spreads go live |
| X5 | All 3 `_EXECUTION_ENABLED=False` | Three independent copies — must flip atomically | Gate-flip session |

---

## OPS UNVERIFIED

| Item | Check | When |
|------|-------|------|
| ~~Ghost scorecard calibration~~ ✅ CLEARED 2026-05-04 | Audit #6X verified endpoint healthy; 1,147 signals, 100% outcome coverage. Per Admiral verdict, SQL-level review sufficient — frontend column is follow-up sprint, not blocker. | (resolved) |
| Alpha threshold for `bull_spread_v1` first trade | Confirm threshold in strategy config | Before first trade |
| Chrome extension Profile 5 re-install | Manual check | Next session |

---

## FOLLOW-UPS FROM AUDIT-#1 (halt_mode introduction)

| ID | Task | Priority | Notes |
|----|------|----------|-------|
| HM-A ✅ FIXED 2026-05-04 | Migrate the ~22 `is_halted` read-sites to `halt_mode != 'active'` | MEDIUM | **Shipped 2026-05-04 (commit `a7e095a`). 14 production read sites migrated** (spec count "~22" was inflated; classification surfaced 14 actual reads after excluding write paths, drawdown-system reads, schema defines, and archived backups). Files: `dashboard/app.py` (9 sites including 2 `WHERE` filters + 7 SELECT/attr reads), `engine/paper_trader.py` (2 SELECTs — dropped unused column from buy/sell halt gate), `engine/morning_briefing.py:62`, `engine/war_room.py:835` (`WHERE is_halted=1` → `halt_mode != 'active'`), `reset_season2.py:64`. Every change tagged `# HM-A:`. API response shape preserved (`is_halted` JSON key, value derived from halt_mode). Drawdown-halt system at `ai_brain.py:817-848` + `risk_manager.py:868` + `post_earnings_drift.py` left alone — different concept (reads `agent_state.is_halted`, not `ai_players`). **Note (HM-S 2026-05-04):** the carve-out targets above were factually inaccurate. `ai_brain.py:817-848` and `risk_manager.py:868` do NOT read from `agent_state` (zero references confirmed by grep). The real drawdown-halt is `risk_manager.py::check_drawdown()` reading `portfolio_history` transiently. Only `post_earnings_drift.py:56` queries the phantom `agent_state` table (silent except). The carve-out *discipline* (don't touch drawdown-related code during halt-system migrations) was correct in spirit; the cited file:line targets were wrong. See HM-S report. |
| HM-B ✅ FIXED 2026-05-04 | Drop `is_halted` column from `ai_players` | RESOLVED | **Shipped 2026-05-04 (commit `9256890`).** Pre-flight (commit `a3a4cd0` HM-B-pre) migrated 4 unmigrated WRITE sites: `reset_season2.py:49,50`, `engine/season_manager.py:154,258`, `shared/matrix_bridge.py:114`, `setup_db.py:24` — all now use `halt_mode='active'` semantics. Live DB DDL: `ALTER TABLE ai_players DROP COLUMN is_halted` on SQLite 3.51. Backup at `backups/trader.db.pre-hmb-20260504_173026`. Service stable post-drop (PID 13734). Halt state now has single source of truth: `halt_mode`. |
| HM-C ✅ FIXED 2026-05-04 | Update read-path consumers of `signals` / `watchlist_signals` to filter `halted_emit = 0` for scoring queries | MEDIUM | **Shipped 2026-05-04. 22 files modified, 28 SQL sites filtered. Scope was broader than first scoped: `ai_brain.py:563` (TIER-1 escalation), `bull_call_spread_v1.py:251` / `bear_put_spread_v1.py:270` (tier-2 spread vote), `crew_scanner.py:3963` (autopilot fleet consensus), `risk_manager.py:312` (bear-mode gate) all consume signals for current-day execution decisions, not just calibration. Halted players were implicitly voting through pre-fix-#1 backlog rows. Helper `HALTED_EMIT_FILTER` constant added to `engine/halt_gate.py` for single-source-of-truth migration when HM-A/HM-B retire `is_halted`. Display/forensic paths preserved. `/v1/signals` external API also filtered — note in commit message under Behavior change visible to /v1/signals consumers** |
| HM-D ✅ INVESTIGATED 2026-05-04 | `watchlist_signals` halted-player rows decision | LOW | **Verdict α (Retain) recommended.** 165 halted-player rows total (62 ollama-llama + 41 sulu + 35 gemini-2.5-pro + 27 grok-3). 34 still in `status='active'` but bounded and self-resolving — `signal_tracker.py:124,133` ages them to `hit_target`/`expired` over time, and `halt_gate.can_emit_signal` blocks new active rows. Optional follow-up HM-D-fix (~30-45 min): add `JOIN ai_players halt_mode='active'` to 6 currently-unaware readers (5 in signal_tracker + crew_scanner.py:3965). Full report: `docs/HM-D_WATCHLIST_SIGNALS_VERDICT_2026-05-04.md`. |
| HM-E ✅ INVESTIGATED 2026-05-04 | Halted-player daily routines waste check | LOW | **Verdict B (modest waste).** Signal emission stopped naturally (last halted-player signal 3+ days ago). Trades all SELL action under exit_only — legitimate. **`ai_journal` runs daily for sulu + ollama-llama** — `main.py:520 run_journal()` and `engine/ai_journal.py:18 generate_journal_entry()` have zero halt-mode checks. ~2 LLM calls/day to Ollie Box for journals no one reads. Optional follow-up HM-E-fix (~5 min, low risk): add 3-line halt-mode check in `engine/ai_journal.py::generate_journal_entry()`. Full report: `docs/HM-E_HALTED_ROUTINES_VERDICT_2026-05-04.md`. |
| HM-S ✅ INVESTIGATED 2026-05-04 | `agent_state` table ghost — drawdown-halt source of truth question | MEDIUM | **Verdict C (dead but harmless) + documentation drift.** Drawdown halt protection IS functional but does NOT read from `agent_state` as CLAUDE.md claims — it's recomputed transiently every cycle from `portfolio_history` in `engine/risk_manager.py::check_drawdown()` (3,562 rows, 20% threshold). `agent_state` table never existed in any of 13 .db files searched. Only one reader exists (`agents/post_earnings_drift.py:56`) and it silently degrades to `False` via bare `except: return False`. PED is paper-only via separate `gated=True` flag — broken halt-check cannot cause real-money damage. **Live gate-flip soak is safe.** Recommended actions: (1) fix CLAUDE.md "Why both is_halted and halt_mode" section to describe transient drawdown computation, not phantom agent_state table, (2) optional PED cleanup — replace dead `is_halted()` with simpler `enabled` toggle (~10 min). Full report: `docs/HM-S_AGENT_STATE_GHOST_2026-05-04.md`. |
| HM-F ✅ RESOLVED 2026-05-04 | Add `halted_at` UPDATE to whatever code path sets `is_halted=1` going forward | RESOLVED | Audit found zero halt-write paths in current code. The four currently-halted players were halted via manual sqlite3 UPDATE; `season_manager.py` and `reset_season2.py` only UNHALT (set `is_halted=0`). Manual halt SQL is the only halt path; runbook documented in `CLAUDE.md` ## Manual halt SQL pattern. See `HM-F-future` for when a programmatic halt path appears. |
| HM-F-future | When a programmatic halt path appears (dashboard halt button, drawdown auto-halt, etc.), add `halt_player(conn, player_id, mode, reason)` helper to `engine/halt_gate.py` per HM-F Option 3 | LOW | **Do not pre-build.** YAGNI today — no caller exists. The helper should be written to fit whatever the real caller looks like (request handler? scheduled job? user-confirmation flow?), not in advance. |

---

## PATTERN NOTES

**Import-drift family (8+ items):** B12, B13, B14, B15, B17, AI-2, B29 share the
same family — "symbol moved, callers not updated, error swallowed by `except Exception`."
B14 + B15 closed today; remainder warrants a single disciplined import-drift sweep.

**Rebrand-drift family (B16, B23, accessapple sprint):** incomplete
`accessapple` → `ollietrades` rebrand left orphan domain references in code + docs.
22 refs across 6 files; sprint queued.

**Decorative-flag family (AI-3, AI-4):** `is_active`, `is_paused`, `crew_role` look
like state fields but don't gate execution. Only `is_halted` works. Document
before any new agent is wired.

---

## OPEN — Day-1 Soak Findings (2026-05-04 evening)

### HM-O — Ollie Box network outage (Scenario D, blocked at network layer)
- 192.168.1.166 unreachable: 100% ICMP loss + `nc: No route to host`. Not a stopped Ollama service — a network/power-layer failure that Scotty is not authorized to fix remotely.
- **Active impact during gate-flip soak:** three Ollie-Box-routed agents (`ollama-qwen3`, `ollama-coder`, `ollama-plutus`) emitting `HOLD, confidence=0.0` with `HTTPConnectionPool` error reasoning every signal cycle.
- **Action required from Captain/Admiral:** physically check power + network on Ollie Box. After it's back, re-run HM-O probe to verify all three models respond.
- **Follow-up (HM-X candidate):** circuit-breaker so unreachable Ollama doesn't keep emitting confidence-0.0 HOLDs into `signals` table.
- Full report: `docs/HM-O_OLLIE_BOX_HEALTH_2026-05-04.md`.

### HM-P — Confidence-scale audit (no urgent flag, deferred annotation pass)
- 42 production sites + 10 alt-named `conf` sites audited. **0 WRONG, 2 AMBIGUOUS (comments only), 49 CORRECT.**
- All gate-flipped strategy code (`bull_call_spread_v1`, `bear_put_spread_v1`, `executor`, `exit_manager`) verified: uses `TB_CONF_THRESHOLD = 85` against `trade_signals.confidence` (INT 0-100). **Soak may continue safely.**
- Implicit convention: `trade_signals` → INT 0-100; `signals`/`watchlist_signals`/`deep_scan_results`/`ghost_trades` + player decisions → REAL 0-1. Not documented anywhere central; one careless paste away from a silent bug.
- **HM-P-fix (deferred, low risk):** annotation pass adding `# scale: 0-100 INT` / `# scale: 0-1 REAL` at every comparison site. ~60-90 min one-shot. Optional rename `confidence` → `confidence_pct` in `engine/ollie_commander.approve_or_reject`.
- Full report: `docs/HM-P_CONFIDENCE_SCALE_AUDIT_2026-05-04.md`.

### HM-Q — execution_status vs halted_emit (Verdict A, no action)
- Both columns measure orthogonal things. `execution_status` = "what happened to this signal downstream"; `halted_emit` = "was the player allowed to act when emitted".
- HM-C is **not** redundant. `halted_emit` captures information (halt state at emission time) irrecoverable from `execution_status` or any join — `ai_players.halt_mode` is mutable and there is no halt-state audit log.
- **No schema change. No undo of HM-C.** Optional one-line annotation in `engine/halt_gate.py` near `HALTED_EMIT_FILTER` documenting the orthogonality.
- Open question worth chasing: **what writes `execution_status='EXPIRED'`?** 42,626 rows (69.5% of `signals`) and the audit found no writer — likely a sweeper job, but unverified.
- Full report: `docs/HM-Q_EXECUTION_STATUS_INVESTIGATION_2026-05-04.md`.

### HM-B — Drop ai_players.is_halted column (Day-1 evening, ✅ SHIPPED)
- HM-A read coverage was 100% clean, but pre-flight surfaced 4 unmigrated WRITE sites that would have SQL-errored post-drop. Migrated those first in HM-B-pre (`a3a4cd0`), then dropped column in HM-B (`9256890`).
- Live DB: `ALTER TABLE ai_players DROP COLUMN is_halted` on SQLite 3.51, backup at `backups/trader.db.pre-hmb-20260504_173026`. Service stable post-drop, no schema-related errors in trader.log.
- Halt state now has single source of truth: `halt_mode TEXT CHECK(halt_mode IN ('full','exit_only','active'))`.

### HM-D — watchlist_signals halted-player rows (Verdict α: Retain)
- 165 halted-player rows total. 34 still in `status='active'` but bounded and self-resolving — readers transition them out as price action plays out, and `halt_gate.can_emit_signal` blocks new active rows.
- Optional HM-D-fix (~30-45 min, deferred): add `JOIN ai_players halt_mode='active'` to 6 currently-unaware readers in `signal_tracker.py` + `crew_scanner.py:3965`. No urgency.
- Full report: `docs/HM-D_WATCHLIST_SIGNALS_VERDICT_2026-05-04.md`.

### HM-E — Halted-player daily routines (Verdict B: modest waste)
- Signal emission already stopped naturally; halted-player trades are all legitimate exit_only SELLs.
- **Active waste**: `ai_journal` daily routine runs for sulu + ollama-llama every market session — ~2 LLM calls/day to Ollie Box for journals no one reads. `main.py::run_journal()` + `engine/ai_journal.py::generate_journal_entry()` have no halt-mode check.
- Optional HM-E-fix (~5 min, low risk, deferred): add 3-line halt-mode check at the LLM-cost source in `generate_journal_entry()`.
- Full report: `docs/HM-E_HALTED_ROUTINES_VERDICT_2026-05-04.md`.

### HM-T — PED operational probe (Verdict B: silently inert, structurally unreachable promotion)
- PED is properly imported (main.py:3486) and scheduled every 15 min (main.py:3541) inside `if __name__ == "__main__":` block — the scheduler IS firing.
- **Lifetime activity: zero.** No row in `ai_players`, zero `signals` written, zero `trades`, zero log lines across all log files. Sitrep history (794 lines, 2026-05-01 onward) shows `PED signals: 0` every cycle.
- **Root cause:** `data/watchlist.txt` (PED's universe source at main.py:3496) does not exist. Falls back to 9 hardcoded ETF/mega-cap symbols. None have earnings in the 1-48hr post-earnings window today; effective trigger frequency is single-digit hours/year, single-digit signals/year after gap+vwap filters.
- **Gate-promotion criterion (30 trades + positive expectancy) is structurally unreachable.**
- Compute waste: negligible (rule-based, no LLM, ~0.1s/day total CPU).
- No other code reads `data/watchlist.txt` — the missing file is PED-specific. Was either deliberately abandoned or never wired.
- **Recommended: Option γ (formally retire).** Move to `archive/retired/`, remove schedule, document. Side benefit: closes HM-S-code by removing the phantom `agent_state` reference from active code paths. Option β (repair wiring with proper watchlist) is also viable if Captain sees PED research value.
- Full report: `docs/HM-T_PED_OPERATIONAL_PROBE_2026-05-04.md`.

### HM-T-fleet — Silent-Inertness Audit (Tuesday 2026-05-05)
- Extended HM-T's PED-class question fleet-wide. 49 ai_players + 130 schedule registrations classified.
- **7 PED-class inert agents identified:** anderson-bcs, mccoy-bps, quark-ic, covered-call (orphaned in `engine/options_agents.py`, file imported by nothing); qwen3-14b-pro (lab/backtest scaffold, never dispatched); red-alert (channel-mismatch — writes to non-existent `red_alert_log`); dayblade-0dte (was active until 2026-04-07, 28 days idle — watch list).
- **Halted-but-emitting confirmed:** ollama-llama leaked 947 post-halt signals (HM-A signal-emission gate gap). Earlier "2 post-halt trades NEW finding" claim was a query-window error; corrected in commit ee481fa — actual 7 post-halt trades, all clean exits, Verdict A (no trade-gate bug).
- **Orphan in signals not in roster:** `debate-pipeline` (1 row, 2026-03-31, vestigial).
- **Recommendations:** (1) ~~one bundled retirement commit for the 4 options_agents.py orphans (mirrors PED pattern)~~ **— APPLIED 2026-05-05 07:09 MST as Option 1 halt-only.** Pre-flight discovered `engine/options_agents.py` IS imported by `dashboard/app.py:17731` and contains 8 player_ids (4 targets + 4 ghosts), so the file was NOT archived. Instead 4 ai_players rows transitioned to `halt_mode='full'`. Code preserved per sacred-data rule. Open follow-ups: ghost-agents Option 4 investigation; HM-T-fleet doc has stale "imported by nothing" claim that needs a correction note; surgical file cleanup deferred until ghost investigation lands; (2) dispatch-loop investigation for qwen3-14b-pro; (3) clarify red-alert role; (4) signal-emission gate work (already in CLAUDE.md TODOs).
- 4 open Admiral questions: paid-model halting policy, options_agents retirement scope, dayblade-0dte timeline, mlx-qwen3/ollama-coder dispatch suppression.
- Full report: `docs/HM-T-fleet_SILENT_INERTNESS_AUDIT_2026-05-05.md`. No code/schema changes — investigation deliverable only.

### HM-I — Bridge Scope Investigation (Tuesday 2026-05-05)

**Status:** Admiral picked **Option β** (firm separation) 2026-05-05. Items 1+4 shipped same day; items 2/3/5 deferred.
**Priority:** Medium (architectural; running soak is stable)
**Investigation date:** 2026-05-05 morning (Scotty)

Inventoried the internal-book ↔ Alpaca-paper bridge. 3 books, 2 flows, 4-player routing table.

- **Active code-level finding:** `engine/paper_trader.py:1300` (partial-SELL path) called `_forward_to_alpaca` **without** the `route_mode == "trading"` gate that BUY (line 1015) and full-SELL (line 1167) both have. Source of ~181/day phantom-position skip log entries from legacy fleet players. **APPLIED 2026-05-05 commit `d06c33c`** (HM-I Option ε): added matching gate; all three forward paths now identical. Stale bytecode at PID 35155 means current process still emits skips until next restart.
- **Two-book policy formalized:** `CLAUDE.md` § "Architecture: Two-Book Bridge Policy" added 2026-05-05 commit `086a123`. Internal AI fleet book and Alpaca paper book are two separate ledgers by design. Routed players (super-agent, ollie-auto, neo-matrix, dalio-metals) + spread strategies forward to Alpaca; legacy fleet stays internal-only.
- **Phantom-reference fix:** `portfolios.id=5` renamed from "Dalio Metals" → "Enterprise Computer" 2026-05-05 to match `_EXECUTION_PORTFOLIO_BY_PLAYER` mapping. Resolution went from broken (fall-through paper) to correct (id=5, route_mode=tracking, log-only). Behavior change: dalio-metals no longer accumulates new internal-book trades — matches Option β log-only intent. Existing 37 trades + 2 positions preserved (FK on id, not name). DB-only change; no code/doc updates needed (refs were already correct).
- **Type 1 divergence count at investigation time:** 39 internal positions across 9 players that Alpaca paper doesn't have. Includes shorts (gemini-2.5-flash IREN/ONDS) and futures (enterprise-computer GC=F, SI=F) Alpaca paper can't accept. Stable post-β (legacy fleet stays internal by design).
- **5 options presented α/β/γ/δ/ε.** Admiral picked β. Item ε (decision-orthogonal) also applied.
- **β followups status:**
  - Item 2: Dashboard naming pass (Arena Paper vs Alpaca Paper visual distinction). **Deferred.**
  - Item 3: Webull dual-role split. **APPLIED 2026-05-05 07:56 MST** — code + service restart (PID 35155 → 59121) + DB migration atomic. New player `alpaca-mirror` (provider=alpaca-paper-broker, is_human=0). 3 positions migrated from webull to alpaca-mirror; webull retains 127 historical Webull-import trades + 73 portfolio_history rows. Kirk + first_officer + Q + cto_advisor + war_room + dashboard reads re-targeted to alpaca-mirror. SQL `!= 'webull'` exclusions in benchmark.py / war_room.py / holodeck_expansion.py rewritten as `is_human=0`. Stale-bytecode lockstep dictated atomic order: code → kickstart → DB. 18 files touched, 29 `# HM-I-β-Item3:` markers placed.
  - Item 5: Reconciliation report (replaces ε canary, daily NTFY on drift thresholds). **Deferred.**
- Full report: `docs/HM-I_BRIDGE_SCOPE_INVESTIGATION_2026-05-05.md`.

### Option 4 — Ghost Agents Investigation (Tuesday 2026-05-05)

**Status:** **CLOSED 2026-05-05 08:57 MST** — Admiral chose **Option B halt-only retirement**. 4 ghost agents transitioned `halt_mode='active' → 'full'` via DB UPDATE. File `engine/options_agents.py` untouched (sacred-data); `/api/options/scan-preview` endpoint continues serving 8 halted agents (4 production halted morning 06b5ce7 + 4 ghosts halted now). halt_gate API confirms all 4 ghosts return False on can_emit/open/close; active players (ollama-plutus, ollie-auto, super-agent) unaffected. Operationally a no-op (zero lifetime activity); DB now reflects behavioral reality. Pre-halt backup at `backups/trader.db.pre-ghost-retire-20260505_085718`.
**Original Status:** Open — awaiting Admiral A/B/C/D decision (no recommendation made).
**Priority:** Low (no current behavioral impact; either choice is reversible).
**Investigation date:** 2026-05-05 morning (Scotty)

Tested HM-T-fleet's ⚪ "by-design" classification of the 4 ghost agents (ghost-kirk-bc, ghost-kirk-0dte-bc, ghost-long-call, ghost-naked-put).

- **Verdict:** classification was **directionally correct**. All 4 ghosts are 🟡 half-wired — real classes with real scan logic, partitioned into a separate `options_books.ghost` research book ($2,500 starting capital) with drawdown gate, designed as A/B research framework. Not orphans.
- **But:** they share their dispatch path with the 4 production options agents we halted this morning (commit 06b5ce7). Both groups are preview-only — no scheduler entry, no execution step, only consumer is `dashboard/app.py:17731 /api/options/scan-preview`. The "separate confirm step" the run_scan_cycle docstring references doesn't exist in code.
- **4 options presented:** A leave alone (no action), B halt all 4 ghosts to mirror morning halt symmetrically, C activate (build the missing scheduler+confirm path), D retire entire options-engine subsystem.
- **Open Admiral questions:** was ghost activation always planned? should production+ghost halt status be symmetric? is the "separate confirm step" real or aspirational?
- Full report: `docs/OPTION-4_GHOST_AGENTS_INVESTIGATION_2026-05-05.md`.
- **Side observation:** morning halt of 4 production options agents (anderson-bcs/etc.) was effectively cosmetic — those agents had no path to fire either. Halt is still correct (marks them not-production), but didn't change behavior.

### HM-U — Silent-Failure Pattern Discussion (DISCUSSION ITEM, NOT A FIX)

**Status:** Open
**Priority:** Medium (architectural conversation, not a code change)
**Surfaced by:** HM-O / HM-S / HM-T / HM-E investigations on 2026-05-04, plus the stale-bytecode discovery during PED retirement verification

Today's audits found a recurring architectural anti-pattern across multiple subsystems:

| Subsystem | Silent-failure shape |
|---|---|
| HM-O (Ollie Box outage) | Connection-error reasoning text + `confidence=0.0` HOLD signals → treated as valid rows in `signals` table |
| HM-S (`agent_state` ghost) | `try/except Exception: return False` swallows missing-table error → drawdown-halt always says "not halted" |
| HM-T (PED inert) | Missing `data/watchlist.txt` → silently falls back to narrow universe → never qualifies → silently no-ops |
| HM-E (halted journals) | No halt-mode check on routines → continues running for halted players → wasted LLM calls |
| Stale-bytecode (PED-verification discovery) | `try/except: console.log(error)` at 4 call sites swallowed `no such column: is_halted` for 70 min before discovery |

**Common shape:** bare `except` / silent fallback / no-op success path / caught-and-logged-but-not-alerted. The codebase trades loud failure for quiet incorrectness in many spots, and the discipline of "don't crash the trader" has expanded to cover bugs that should be loud.

**Question for discussion (not for autonomous decision):**

1. Should bare `except Exception` blocks log the swallowed exception with stack trace by default (vs current pattern of `console.log(f"...: {e}")` losing the traceback)?
2. Should silent-fallback paths NTFY-alert when they fire (e.g., "PED couldn't load `data/watchlist.txt`, using fallback universe")?
3. Is there a project-wide error-handling philosophy worth writing down in CLAUDE.md (e.g., "data-layer SQL errors must NTFY-alert; LLM-API errors may be swallowed; config-fallback paths must log once-per-process")?
4. Are there other "wired-but-inert" agents we should fleet-audit (HM-T-fleet candidate)?
5. Should schema-change verification include a service restart in the verification phase, given the stale-bytecode trap from today (see Lessons section)?

**Recommended action:** Schedule a discussion-only session (Admiral + XO, no Scotty) to set posture. Then a follow-up sprint, if any, would write the explicit fix prompt.

**Not in scope here:** automatic refactor of all bare-except blocks. That's a code-philosophy decision, not a Scotty task.

### HM-S — agent_state table ghost (Verdict C: dead but harmless + docs drift)
- **`agent_state` does not exist in any of 13 .db files in the repo.** Confirmed via direct schema queries on every DB.
- **Only 1 reader** in production code: `agents/post_earnings_drift.py:56` — wrapped in bare `except Exception: return False`, so the missing table silently produces "not halted".
- **CLAUDE.md is factually wrong:** claims `engine/ai_brain.py` and `engine/risk_manager.py` read from `agent_state`. Neither file references `agent_state` at all. The actual drawdown-halt protection at `risk_manager.check_drawdown()` reads `portfolio_history` and recomputes `(peak - current) / peak >= 0.20` every cycle — transient, not flag-based, and FUNCTIONAL.
- **Safety implication for live gate-flip:** none. Drawdown halt + manual halt_mode runbook are both functional. PED's broken halt-flag is contained by separate `gated=True` paper-only gating.
- **Recommended:** (1) fix CLAUDE.md describe transient drawdown mechanism correctly (~5 min), (2) optional PED `is_halted()` cleanup (replace with `enabled` toggle, ~10 min). Both deferred.
- Full report: `docs/HM-S_AGENT_STATE_GHOST_2026-05-04.md`.

### HM-AB — bull_spread_v1 missing same-strategy self-skip check (2026-05-05)

**Status:** Open — strategy halted at commit `[this commit SHA]` pending fix.
**Priority:** High (was actively stacking positions; 18 open SPY bull_put_spreads accumulated <1 day post-gate-flip before halt).
**Surfaced by:** Admiral observation 2026-05-05 11:39 MST.

`strategies/bull_spread_v1.py` lacks a same-strategy self-skip check — the reciprocal of `strategies/bull_call_spread_v1.py:280-287` which queries `options_trades` for any open `bull_spread_v1` row on the same ticker and skips if found. Without the reciprocal, bull_spread_v1 is free to fire repeatedly on the same ticker (SPY) every signal tick (every 15 min per `main.py:2622` schedule), accumulating 18 open positions in <1 day.

**Halt applied 2026-05-05 11:39 MST (this commit):**
- `strategies/bull_spread_v1.py` `_EXECUTION_ENABLED = False` (module-level constant)
- `evaluate()` early-return checks the constant
- Auto-register call changed to `enabled=False`
- Belt-and-braces: either gate alone halts signal emission; both together provide redundant safety
- Stale-bytecode: PID 61083 has pre-halt bytecode in memory; halt takes effect on next service restart (planned ~13:00 MST per Admiral)
- Tag: `# HALT-2026-05-05:` markers in code

**Existing 18 positions ride** per Admiral directive — they're real Alpaca paper positions, max-loss-capped, same-expiration. `exit_manager` handles them on its scheduled cadence (TP / SL / expiration). **DO NOT close programmatically** during the halt window — closing while the underlying bug still exists risks stacking another bug on top.

**Fix shape (HM-AB session):**
1. Add `_already_open(ticker)` helper to `strategies/bull_spread_v1.py` mirroring `bull_call_spread_v1.py:275-290` — query `options_trades` for `WHERE strategy_id='bull_spread_v1' AND symbol=? AND exec_status='open'`.
2. Call it at the top of the per-ticker loop in `evaluate()`; skip ticker if already-open.
3. Once verified, flip both `_EXECUTION_ENABLED = True` and `enabled=True` to unhalt.

**Verification approach:**
- Pre-fix smoke: confirm a synthetic open row blocks signal emission for that ticker.
- Pre-unhalt: backlog audit of existing open positions; if any have already hit TP/SL/expiration, unhalt is safer because the strategy will see fewer "already open" hits naturally.
- Post-unhalt monitor: 1 hour soak with `tail -f logs/trader.log | grep bull_spread_v1` to confirm the strategy fires once per qualifying ticker per cycle, not stacking.

---


### HM-AF — dayblade-0dte spread cannibalization root cause (2026-05-06)

**Status:** **HALTED 2026-05-06 10:43:54 MST** via `UPDATE ai_players SET halt_mode='full'` (transaction took effect immediately, no service restart needed; halt-mode is read per-cycle).

**Surfaced by:** Day 3 morning observability check (Admiral + XO, 2026-05-06 10:00–10:45 MST), tracing the orphan SPY 732P short position visible in `positions` table after a clean MLEG fill.

**Root cause (the 2-day "spread positions vanish" mystery):** `dayblade-0dte` (T'Pol) was firing single-leg `submit_single_option(SELL)` calls on the LONG legs of bull_put_spread fills within minutes of the parent MLEG filling. Each fire dismantled a spread by selling its protective long leg, leaving an orphaned short PUT.

**Evidence chain:**
- 2026-05-05: 5 single-leg SELL fires across 4 timestamps (08:41 + 12:52–12:56 cluster) totaling 13 long-PUT contracts (1+3+5+1+3) — exactly matches the 13 spreads cleaned up by HM-AE Option B reconcile that evening. All fires logged at `engine/alpaca_options.py:251`.
- 2026-05-06 08:14:39 UTC: 1 single-leg SELL on SPY260515P00727000 (`order=4863f7fc-980d-4283-b30b-5fe89ae12ebb`) fired 2 minutes after the bull_put_spread MLEG `848ece89-...` filled at 08:12:41. Logged at `engine/alpaca_options.py:315` (different code path than yesterday's :251).
- Alpaca order data confirms both legs of every MLEG order today and yesterday filled cleanly with `filled_qty=1, status=filled` — Alpaca paper did NOT net-collapse the spreads as previously theorized; OUR code dismantled them.
- All firing entries attributed to `dayblade-0dte` in trader.log; one EOD-sweep log line confirms the cluster behavior: `(dayblade-sulu + dayblade-0dte EOD sweep)`.

**Halt SQL applied:**

    UPDATE ai_players
    SET halt_mode = 'full',
        halt_reason = '2026-05-06 spread cannibalization (closed long 727P leg of bull_put_spread_v1 2min after MLEG fill, alpaca_options.py:315; also responsible for 5 single-leg sells yesterday at alpaca_options.py:251 dismantling the 13-spread reconcile cohort)',
        halted_at = CURRENT_TIMESTAMP
    WHERE id = 'dayblade-0dte';

**Verification post-halt:**
- `halt_mode='full'`, `halted_at=2026-05-06 17:43:54 UTC` confirmed via SELECT
- No `Alpaca OPTIONS SELL` log entries from `dayblade-0dte` after 10:43 MST through 10:45 MST (~2 min observation window)
- `bull_spread_v1` self-skip continues to fire correctly on the orphan SPY 732P
- Stock-trading agents unaffected (ollama-plutus / qwen3 / capitol-trades / deepseek-7b-grok4 active throughout)

**Mis-attribution correction (HM-AE Option B):** Yesterday's HM-AE Option B reconcile marked 13 stale `options_trades` rows closed under the assumption that "Alpaca paper net-flat-collapsed the spreads." The cleanup was correct — the rows DID need to be marked closed because they WERE effectively closed — but the cause attribution was wrong. The actual cause was dayblade-0dte cannibalizing each spread's long leg, leaving naked shorts that subsequently closed via other paths or netted out. No corrective action needed on the 13 reconciled rows; this is a calibration note for future-XO.

**Open follow-ups (deferred to fresh-headed Scotty):**
1. **Investigate dayblade-0dte's two firing paths** — `alpaca_options.py:251` and `alpaca_options.py:315` are different code paths. Read both, trace callers, document trigger conditions. Why did `:315` fire on a 9-DTE option from an agent labeled `0dte`? Investigation only, no code changes. ~30 min.
2. **Architectural fix: spread-leg awareness for ALL agents** — long-term, ANY agent firing single-leg options closes should respect spread structure. Two approaches: (A) add `spread_id` + `is_spread_leg` columns to `positions`, populated when MLEG fills sync; (B) read-time check against `options_trades.legs_json` for matching open spread. Approach choice depends on Item 1's findings. ~60 min.
3. **Orphan SPY260515P00732000 short** (qty=-1, mv≈-$579, expires 2026-05-15) — Battle Station continues firing CLOSE_NOW every 2 min (legitimate panic on what looks like a naked short PUT) but the close routes through tracking-mode and never executes. Recommendation: let it expire on May 15. Paper money, no real risk. Set reminder for May 15 to verify expiration cleared the position.

**Reversal (if needed):**

    UPDATE ai_players SET halt_mode='active', halt_reason=NULL, halted_at=NULL WHERE id='dayblade-0dte';


**AMENDMENT 2026-05-06 11:00 MST (post-Scotty investigation):** Initial HM-AF writeup characterized `:251` and `:315` as two firing paths. Scotty investigation (`docs/diagnoses/dayblade_0dte_paths_2026-05-06.md`) corrected this: they are the same log line at different file offsets — commit `1eeff7d` (HM-V/HM-AA bundle, 2026-05-05 12:59 MST) inserted 147 lines above the success log inside `submit_single_option`. Pre-restart bytecode emitted `:251`; post-restart process emits `:315`. Single statement, single caller, single defect.

The actual contaminated code paths are THREE, all sharing the same root cause (no spread-leg awareness, no DTE filter, no agent-ownership filter):

- **P1 — Battle Station 2-min monitor** (`battle_station.py:684`): iterates ALL Alpaca options positions every 2 min, fires close on −50% pnl OR wrong-side-of-gamma-flip. Hardcodes `player_id="dayblade-0dte"` at `battle_station.py:668` for attribution but scope is global. Triggered today's `:315` fire on SPY 727P.
- **P2 — EOD sweep** (`main.py:2268` → `close_all_options` at `alpaca_options.py:590`): fires daily at 12:45 MST. Closes ALL options positions in the Alpaca book regardless of strategy/spread structure. Confirmed firing 2026-05-05 12:48:23.
- **P3 — dayblade.py:502 post-trade close_all_options**: fires `close_all_options` after every dayblade sell, NOT just EOD. Likely the highest-frequency leak; silently cannibalizing spreads since the 2026-05-04 gate flip.

**Halt of dayblade-0dte (`halt_mode='full'`) only stops P3.** P1 (Battle Station) and P2 (EOD sweep) remain active and will fire on any open options position regardless of dayblade-0dte's halt state.

**Additional finding — wrong-side-of-book bug:** `_get_alpaca_options_positions` strips qty sign at `battle_station.py:319`. Short positions get treated as longs in close logic, causing `submit_single_option(side="sell")` calls when the correct close action would be buy-to-close. Separate from cannibalization but compounds damage.

**Updated open follow-ups (supersedes original Items 1-3):**
1. **HM-AF-α** — Halt P1 + P2 + P3 via feature flag or guard (urgent, before Layer 1 ships). Scotty ~15-20 min.
2. **HM-AF-β** — Layer 1: Spread-leg awareness. `is_spread_leg(symbol)` helper cross-referencing `options_trades`/strategy_positions; applied to P1/P2/P3. Scotty ~60-90 min.
3. **HM-AF-γ** — Layer 2: Wrong-side-of-book correction in `_get_alpaca_options_positions`. Can ride with HM-AF-β.
4. **HM-AF-δ** — Layer 3: Remove hardcoded player_id in `battle_station.py:668`. Lower urgency.
5. Original Item 3 (orphan SPY 732P) unchanged — recommend let expire 2026-05-15.


---

### HM-AQ — Active Watchlist Coverage Decision (2026-05-07)

**Type:** Strategic scope decision (not a bug)
**Priority:** P3 — non-blocking, no execution risk
**Status:** **DECIDED 2026-05-07** — Captain approves broadening WATCH_STOCKS per criteria below. Implementation queued as HM-AQ-β. Spread-universe expansion deferred as HM-AQ-γ (out of scope, separate Captain decision).
**Origin:** 2026-05-07, "missed mover" investigation (DDOG +30.87%, FTNT +22.92%, MDB +14.19%, ZTS −21.37%, ARM −8.18%, TPR −8.14%)

#### Captain's decision (2026-05-07)

**WATCH_STOCKS expands** from 20 manually-curated mega-caps to a dynamically-refreshed universe matching:

| Criterion | Threshold |
|---|---|
| Market cap | ≥ $5B |
| Daily $ volume (20-day avg) | ≥ $50M |
| Refresh cadence | Weekly (Sunday pre-Monday-open) |
| Refresh source | Polygon screener API (Polygon Options Starter $29/mo activation under HM-AQ-β) |

**Expected size:** ~500-800 tickers.

**Risks acknowledged:** dashboard noise, scan-loop slowdown across 12+ iteration sites, more spread attempts (only relevant if HM-AQ-γ ships — for now, spread universes stay at 10 tickers).

**Catches:** all 6 missed movers from 2026-05-07 morning would have been in coverage under these criteria.

**Full criteria & roadmap:** `docs/UNIVERSE.md` (canonical reference; created in this commit).

#### Summary
The fleet's active iteration sources are locked to ~20 mega-cap names. Tickers outside that set are structurally invisible to every active scanner, dashboard surface, and spread engine — not filtered out by gates, simply never iterated.

#### Current state
| Source | Members | Used by |
|---|---|---|
| `config.py:24 WATCH_STOCKS` | 20 tickers (SPY, QQQ, TQQQ, NVDA, TSLA, AAPL, AMD, META, MSFT, GOOGL, AMZN, MU, ORCL, NOW, AVGO, PLTR, DELL, XLE, INTC, NUKZ) | dashboard (12+ iterations), `scripts/import_stooq.py` |
| Per-strategy `TIER_1+TIER_2` | 10 tickers (SPY, QQQ, IWM + 7 large-caps) | `bull_spread_v1`, `bull_call_spread_v1`, `bear_put_spread_v1` |
| `scan_universe` (DB) | 2,741 catalog rows | passive metadata only — no live readers |

Of the 6 candidates that triggered this investigation: 5 in `scan_universe` (catalog only), 0 in any active iteration source. ZTS not even catalogued.

#### Acceptance criteria (status post-decision 2026-05-07)
- [x] Coverage criteria documented — `docs/UNIVERSE.md`
- [x] CO decision logged — broaden, this commit + OPS_LOG 2026-05-07
- [x] Implementation ticket spawned — HM-AQ-β below
- [x] Spread-universe scope decision deferred — HM-AQ-γ marker below

#### Related
- `docs/UNIVERSE.md` — canonical universe doc
- HM-AQ-β — implementation ticket (Polygon screener + weekly refresh + storage migration)
- HM-AQ-γ — spread-universe expansion (deferred marker, not in active queue)
- `bull_call_spread_v1.py` TIER_1/TIER_2 definitions (out of scope; see HM-AQ-γ)
- HM-AP (closed no-op) — `bull_call_spread_v1` silence verdict
- HM-AR — `earnings_universe` observability (sibling finding from same investigation)

---

### HM-AQ-β — Implement dynamic WATCH_STOCKS refresh (2026-05-07)

**Type:** Implementation (active queue)
**Priority:** P3 → escalated and shipped same-day
**Status:** **SHIPPED 2026-05-07** — 5 commits (`5eb479c` schema → `dd43bab` accessor → `12ad22d` refresher → `404f0a2` consumer migration → commit 5 = bug-fix bundle + plist + wet refresh + perf fix). Universe at $100M floor: ~1,223 names (927 CS + 296 ETF). Bulk-endpoint perf fix at 9 fan-out sites makes 1,223-symbol snapshots ~1-2s instead of ~47s. Full narrative: `docs/UNIVERSE.md`.
**Origin:** HM-AQ decision 2026-05-07 (`docs/UNIVERSE.md`).

#### Scope

Replace the static `config.py:WATCH_STOCKS = [...20 tickers]` constant with a dynamically-refreshed universe of ~500-800 tickers matching the HM-AQ inclusion criteria (market cap ≥ $5B, daily $ volume ≥ $50M).

**Sub-decisions logged:**
- **Screener:** Polygon (not Alpaca). Rationale: Polygon Options Starter $29/mo is approved-in-principle (CLAUDE.md 2026-04-16) and offers a richer screener than Alpaca's. Activation cost ($29/mo) is part of HM-AQ-β implementation. First paid exception under Free-Models-First doctrine.
- **Spread universes (`TIER_1+TIER_2`):** NOT in scope. Tracked separately as HM-AQ-γ.

#### Components
1. **`engine/universe_refresh.py`** (new) — Polygon screener API client, cap/volume filter, output writer.
2. **Storage migration** — replace `config.py:WATCH_STOCKS` constant with one of:
   - DB table `universe_active(symbol, last_refreshed_at, market_cap, avg_daily_dollar_volume, included_reason)` — preferred; queryable
   - File `data/watch_stocks.json` — simpler; no schema migration
   - Decision: TBD during implementation; either preserves the import-as-list pattern via a getter helper.
3. **launchd plist** `com.ollietrades.universe-refresh` — fires Sunday 14:00 MST (post-close, pre-Monday-open). Per HM-AT-β lesson, watch dirs/paths owned by `~/autonomous-trader/` to avoid TCC issues.
4. **Polygon Options Starter activation** — first paid exception activated under Free-Models-First. Document the activation in OPS_LOG.
5. **Iteration-site audit** — 12+ sites in `dashboard/app.py` walk `WATCH_STOCKS` (per HM-AU). Each site must be retested for:
   - Rate-limit impact (Alpaca/Polygon API call fan-out at 25-40× rows)
   - Latency impact (single-threaded `schedule.run_pending()` blocking — relevant to HM-AS cadence tail)
   - Render performance (frontend table sizes 25-40×)
6. **Soak window** — ship to a non-prod-blocking surface first (e.g. dashboard read-only view) before flipping all callers.

#### Effort
~4-8 h Scotty (range reflects whether iteration-site audit surfaces rate-limit issues that require batching).

#### Acceptance criteria
- [ ] `universe_refresh.py` produces 500-800 tickers matching criteria
- [ ] Weekly refresh fires reliably via launchd
- [ ] All iteration sites retested; no rate-limit failures, no latency regression > 2× pre-ship
- [ ] OPS_LOG entry for Polygon Options Starter activation
- [ ] HM-AS-β cadence drift warning continues to fire normally (i.e. broadening doesn't dramatically push the tail)

#### Related
- HM-AQ — Captain decision (parent)
- HM-AQ-γ — spread-universe expansion (deferred)
- `docs/UNIVERSE.md` — criteria + rationale
- HM-AS-β — cadence drift warning (will detect any regression)
- HM-AU — Kirk advisory source routing audit (12+ iteration sites)

---

### HM-AQ-β.2 — Curated-tier ADR inclusion + `is_adrc` flag (2026-05-07, refined)

**Type:** Universe scope expansion (HM-AQ-β follow-up)
**Priority:** P3 — LOW (some liquid ADRs missed; curated tier is high-signal, not noise)
**Status:** Proposed (scope refined 2026-05-07)
**Origin:** HM-AQ-β v3 dry-run 2026-05-07 surfaced 79 type-skipped tickers, mostly ADRCs (BP, NIO, GGB, VIST, LEGN, ...). Many of the largest (TSM, ASML, BABA, SHOP, SE, NVO, NVS, AZN) have liquid options.
**Sequence:** After HM-AQ-β 24h soak (est. 2026-05-08 evening).

#### Captain's refined call (2026-05-07)
**Curated-tier inclusion, NOT blanket type=ADRC.** ADRCs are heterogeneous — TSM at $1T+ down to micro-cap reverse mergers. Blanket inclusion would add too much noise. Solution: apply the existing market_cap + dollar_volume filters to ADRCs (same thresholds as US CS); the filter naturally selects only the high-quality liquid tier.

Reasoning:
- Major-cap ADRs (TSM, ASML, BABA, SHOP, SE, NVO, NVS, AZN, etc.) are high-quality liquid names that AI agents can trade like US stocks.
- Smaller ADRs add currency complexity, regional risks, lower liquidity — without comparable quality benefit.
- Existing $5B cap + $100M dollar-volume filters do the right curation if applied to type=ADRC the same way as type=CS.

#### Refined scope

**1. Apply existing cap + volume filters to ADRCs:**
- market_cap ≥ $5B (same as US CS)
- dollar_volume ≥ $100M (same threshold)
- Filter naturally selects the high-quality tier
- Predicted addition: ~30-50 names (TSM, ASML, BABA, SHOP, SE, NVO, NVS, AZN, BHP, RIO, TM, SONY, ...)

**2. Add `is_adrc INTEGER DEFAULT 0` column to `scan_universe`:**
- Lets per-strategy code opt-in or opt-out of ADRs
- Spread strategies might want US-listed CS only (currency-aware concern; settlement timing on holidays differs)
- Currency-aware strategies could leverage the flag for FX-hedge logic
- Schema migration: `migrations/HM-AQ-β.2_universe_is_adrc_2026-05-XX.sql`

**3. Update `engine/universe_refresh.py`:**
- Step 2: include `type=ADRC` in the cap+volume filter pass (not skip)
- Set `is_adrc=1` on the row when type=ADRC
- Existing CS rows: `is_adrc=0` (default)
- Other types (ETV, ETN, BSKT, FUND, PFD, ...): still skipped as before
- Keep the audit log line `etf_included` style — add `adrc_included <SYM> cap=$X.XB dollar_volume=$Y.YM`

**4. Update `engine/universe.py`:**
- `get_active_universe()` continues to return ALL passing symbols (CS + ETF + ADRC) — drop-in for current consumers
- New helper: `get_us_only_universe()` returns rows with `is_adrc=0` AND `ticker_type='CS'` (for spread strategies, currency-sensitive consumers)
- `get_universe_with_metadata()` includes `is_adrc` in the returned dict
- `universe_health()` adds `adrc_passing` count split

**5. Document in `docs/UNIVERSE.md`:**
- New section: "ADR tier inclusion rationale" — explain why ADRCs ARE included but with a flag
- New section: "Per-strategy opt-out pattern" — explains the `is_adrc` flag and the `get_us_only_universe()` helper
- This sets a precedent for future flag columns: `is_etf` (already implicit via ticker_type), `is_leveraged`, etc.

#### Effort
~30 min Scotty:
- 5 min: schema migration (`ALTER TABLE scan_universe ADD COLUMN is_adrc INTEGER DEFAULT 0`)
- 10 min: refresher patch + `_write_universe` insert clause
- 10 min: `engine/universe.py` helper + SQL filter updates
- 5 min: docs/UNIVERSE.md + dry-run + Captain spot-check

#### Acceptance criteria
- [ ] `scan_universe.is_adrc` column added; ADRC rows correctly flagged
- [ ] Refresher includes ADRCs passing $5B/$100M filters; predicted +30-50 names
- [ ] `engine.universe.get_active_universe()` includes ADRCs (drop-in for existing consumers)
- [ ] `engine.universe.get_us_only_universe()` excludes ADRCs (new helper for spread strategies)
- [ ] Captain spot-check confirms presence of TSM, ASML, BABA, SHOP, SE, NVO and absence of micro-cap ADRs
- [ ] `docs/UNIVERSE.md` updated with ADR rationale + flag pattern

#### Related
- HM-AQ-β — parent (shipped 2026-05-07, commits `5eb479c` → `e333f63`)
- 79 ADRC/other-type symbols logged via `type_skipped` audit line during v3 dry-run
- Future flag columns (deferred marker): `is_leveraged`, `is_inverse`, etc. — same pattern this ticket establishes
- Spread strategies (`bull_spread_v1`, `bull_call_spread_v1`, `bear_put_spread_v1`) are likely consumers of `get_us_only_universe()` once HM-AQ-γ deferred ETF-spread question is revisited

---

### HM-AQ-γ — Spread-strategy universe expansion (deferred marker, 2026-05-07)

**Type:** Future Captain decision (NOT in active queue)
**Priority:** Deferred
**Status:** Marker only — kept so future-self knows the deferral was deliberate.
**Origin:** HM-AQ scope clarification 2026-05-07.

#### Why deferred
Spread strategies (`bull_spread_v1`, `bull_call_spread_v1`, `bear_put_spread_v1`) operate on options chains where **fill quality, bid-ask spread, and open interest dominate edge**. The 10-ticker `TIER_1+TIER_2` universe is curated for liquidity that supports defined-risk debit/credit spreads.

Expanding to mid-caps or thinly-traded names would introduce:
- Wider bid-ask spreads on options legs (eats edge)
- Lower OI / volume → fill risk on multi-leg orders
- Per-name option liquidity varies dramatically; coverage breadth doesn't translate to fill quality

**Captain principle (2026-05-07):** spread quality > spread coverage. Expanding spread universes requires its own analysis on per-name option-chain liquidity (avg daily option volume, OI floor, bid-ask spread floor) — separate Captain decision when surfaced.

#### When to revisit
- A specific mid/large-cap name with proven option liquidity becomes a high-conviction setup that current spread strategies miss
- A new options-liquidity-screener ships that can produce a vetted spread universe automatically
- Spread strategies' performance plateaus in a way that suggests universe-size limitation (currently they're tractor-beam-gate-limited per HM-AP, not universe-limited)

#### NOT a backlog item
This is a **deferred marker**, not an active ticket. Promote to a real ticket only when the trigger conditions above are met.

---

### HM-AR — earnings_universe Inject Observability (2026-05-07)

**Type:** Hygiene / observability
**Priority:** P4 — low, not safety-critical
**Status:** **AUDITED + DOCUMENTED 2026-05-07** — see `docs/EARNINGS.md`. Classified DEPRECATED. Cleanup queued as HM-AR-β below.
**Origin:** 2026-05-07, surfaced during HM-AQ investigation.

#### Audit findings (2026-05-07)

The original ticket framed `earnings_universe` as a single system. Audit revealed **three independent earnings code paths** that share nothing but the word "earnings":

1. **Options blackout (LIVE, safety-critical)** — `engine/options_selector.py::_next_earnings_date` reads `data/earnings_cache.json` + yfinance fallback. Independent of any SQLite table. **This is what actually protects options trades.**
2. **`main.py:679 run_earnings_universe_inject()` (LIVE)** — runs daily 06:00 AZ, but writes to **`scan_universe`** (via `engine.deep_scan.inject_earnings_tickers`), NOT `earnings_universe`. **Function name is a naming-drift lie.**
3. **`engine/earnings_injector.py` + `earnings_universe` table (DEAD ORPHAN)** — writer at line 78, reader at line 96, but **NO external caller**. The `__main__` block is the only entry point. Docstring says "Runs at 6:00 AM AZ" but no launchd/cron entry exists. Has been empty since creation.

**Classification: DEPRECATED.** Path 3 is dead code. Path 1 (the safety-critical one) is intact. Path 2 needs a rename to stop confusing investigators.

**No safety regression.** Options blackout enforcement is unaffected.

**Full path map:** `docs/EARNINGS.md`.

#### Acceptance criteria (status post-audit)
- [x] Audit + classification — `docs/EARNINGS.md`
- [x] SCHEMA.md row updated to point at audit
- [x] Cleanup ticket spawned — HM-AR-β below

---

### HM-AR-β — Retire `engine/earnings_injector.py` orphan + rename `run_earnings_universe_inject` (2026-05-07)

**Type:** Cleanup (HM-AR follow-up)
**Priority:** P4 — LOW (cosmetic; no functional change; eliminates naming-drift confusion)
**Status:** **SHIPPED 2026-05-07** — see commit and OPS_LOG. Path (a) formal retirement applied: orphan archived to `archive/earnings_injector.py.retired-20260507`; `main.py:679 run_earnings_universe_inject` renamed to `run_earnings_scan_inject` (4 sites: definition, error log, comment, schedule binding).
**Origin:** HM-AR audit 2026-05-07.

#### Recommended path: (a) formal retirement

Dead code is technical debt. The "run_earnings_universe_inject" naming-drift confusion alone justifies cleanup. Archive-not-delete honors the sacred-data rule. Effort small.

**Steps:**
1. Move `engine/earnings_injector.py` → `archive/retired/2026-05-07-earnings-injector/earnings_injector.py`. Per archive convention.
2. Leave the `earnings_universe` SQLite table in place (empty; no data to lose; sacred-data rule). Keep schema as forensic record. SCHEMA.md already documents it as deprecated.
3. **Rename `main.py:679 run_earnings_universe_inject()` → `run_earnings_scan_inject()`** to fix the naming-drift lie that confused HM-AR's initial framing. Update the schedule binding at `main.py:2585` accordingly.
4. Single commit + service restart.

#### Alternatives (not recommended)

- **(b) Wire the orphan to a scheduler** — theater without a consumer. `get_active_earnings_universe()` has no caller; populating the table doesn't help anything. Would need to also identify and ship a real consumer, doubling scope. Skip.
- **(c) Status quo** — kicks the can. Empty table + dormant script + lying function name continues to confuse future investigators. The HM-AR audit just spent time untangling exactly this. Don't pay that cost twice.

#### Effort
~15 min Scotty: file move + 2 small edits in `main.py` (function rename + schedule binding) + commit + service restart for the rename to take effect.

#### Acceptance criteria
- [ ] `engine/earnings_injector.py` archived to `archive/retired/2026-05-07-earnings-injector/`
- [ ] `main.py:679` function renamed to `run_earnings_scan_inject`
- [ ] `main.py:2585` schedule binding updated to call the new name
- [ ] `docs/EARNINGS.md` updated to reflect the retirement (path 2 rename + path 3 archive location)
- [ ] No new tracebacks post-restart
- [ ] OPS_LOG entry recording the archive + rename

#### Related
- HM-AR — audit (parent)
- `docs/EARNINGS.md` — three-path map
- `docs/SCHEMA.md` — earnings_universe deprecation note

---

### HM-AS-β — battle_station_monitor cadence-tail observability (2026-05-07)

**Type:** Observability
**Priority:** P3 — post-soak
**Status:** Proposed
**Origin:** HM-AS diagnosis 2026-05-07. Parent HM-AS closed as "diagnosed, deferred."

#### Diagnostic summary (HM-AS, see OPS_LOG 2026-05-07 09:30)
`run_battle_station_monitor` cadence median 2:01 (on target vs the `every(2).minutes` schedule binding at `main.py:2588`); p75 3:09; p95 5:07; max 11:00. Distribution: 69% on cadence, 17% in the 4-6 min tail, ~3% at 6+ min. Cause is architectural — `main.py:4036` runs a single-threaded `schedule.run_pending()` loop, and slow synchronous jobs (LLM calls, scans, backtests) periodically block subsequent ticks. Function itself (`main.py:1002`) is fast (flag check + early return when α guard active). Fire-count integrity for α-lift evidence preserved (80% recovery rate, 289 fires/12h matches histogram mean).

#### Shape
Add `logger.warning` when `run_battle_station_monitor` inter-fire interval exceeds 180s (3 min). Single-function add at `main.py:1002` (or wherever the monitor entry/exit points are). Tracks tail occurrences in production logs without changing scheduler architecture.

Sketch:
```python
_last_battle_station_run = 0.0
def run_battle_station_monitor():
    global _last_battle_station_run
    import time as _t
    now = _t.time()
    if _last_battle_station_run > 0 and (now - _last_battle_station_run) > 180:
        logger.warning(f"[HM-AS-β] battle_station cadence drift: {now - _last_battle_station_run:.0f}s since last fire (target 120s)")
    if now - _last_battle_station_run < 55:
        return
    _last_battle_station_run = now
    # ... existing body
```

#### Effort
~10 min Scotty. Single commit. No service restart required (function reload via natural restart cadence).

#### Acceptance criteria
- [ ] Warning fires in `trader_error.log` when next-tick gap >180s
- [ ] Historical pattern can be analyzed via `grep "[HM-AS-β]" logs/trader_error.log`
- [ ] No false positives on first-fire-after-startup (initial `_last_battle_station_run = 0.0` skipped)

#### Escalation path (if tail proves operationally relevant)
- Option (b) from HM-AS analysis: dedicated thread for battle_station — 15-30 min, isolated.
- Option (a): move all slow jobs to threaded execution — 30-60 min, touches every monitor.

#### Related
- HM-AS — diagnosed, deferred (2026-05-07 09:30)
- HM-AF-α — α-lift evidence integrity preserved by 80% fire-rate recovery
- `main.py:4036` — single-threaded scheduler architecture

---

### HM-AT-β — Schwab watcher: migrate watch dir off ~/Downloads to eliminate TCC dependency (2026-05-07)

**Type:** Workflow / robustness
**Priority:** P3 → P1 (escalated 2026-05-07: GUI fix path unavailable on headless Mini)
**Status:** **SHIPPED 2026-05-07** — see commit and OPS_LOG 2026-05-07.
**Origin:** HM-AT diagnosis 2026-05-07. Parent HM-AT closed via Full Disk Access GUI grant intent + `sleep 11` defense-in-depth (commit `e8b7f9e`); GUI grant proved infeasible on the headless Mini, so HM-AT-β became the actual fix.

#### Problem
Watch dir is currently `/Users/bigmac/Downloads/` (set 2026-05-04 to "meet downloads where the browser puts them"). macOS TCC restricts `~/Downloads/` access — the launchd audit session does not inherit Full Disk Access from Terminal/SSH, causing silent dormancy. HM-AT was resolved by manually granting `/bin/bash` Full Disk Access in System Settings. That grant is fragile: any TCC reset (macOS update, system reset, manual revoke) re-introduces the silent failure.

#### Shape
Migrate the watch dir from `~/Downloads/` to `~/autonomous-trader/inbox/`. The autonomous-trader directory is project-owned and not subject to TCC's user-data restrictions, so launchd-spawned agents can read it without any GUI grant.

Changes:
- Edit `scripts/schwab_csv_watcher.sh`: `WATCH_DIR="/Users/bigmac/autonomous-trader/inbox"` (was `/Users/bigmac/Downloads`).
- Create `~/autonomous-trader/inbox/` directory; add to `.gitignore` since the inbox holds transient CSVs.
- Update CLAUDE.md "Schwab Workflow" section to reflect new drop directory.
- Workflow change for Admiral: browser save target switches from Downloads to inbox/ (Chrome's "Ask where to save" or per-save dir change), OR add a one-liner cron / Hazel rule to move `~/Downloads/Sc[hw]ab*Positions*.csv` to inbox/.

#### Effort
~30 min Scotty (script edit + dir create + CLAUDE.md update + verify) + Admiral browser-config or Hazel rule.

#### Acceptance criteria
- [ ] `WATCH_DIR` constant moved off `~/Downloads/`
- [ ] launchd-driven watcher processes a test CSV without any TCC grant on `/bin/bash`
- [ ] Admiral workflow documented (browser save dir change OR Hazel rule)
- [ ] CLAUDE.md "Schwab Workflow" updated
- [ ] Bootout/bootstrap cycle in OPS_LOG showing TCC-free operation

#### Escalation path
If browser-save-dir change is unworkable, alternative: Hazel rule on `~/Downloads/` to move matching CSVs to `~/autonomous-trader/inbox/`. Hazel runs in user session and inherits TCC, so it can read Downloads even when launchd cannot.

#### Related
- HM-AT — closed via Full Disk Access GUI grant + `e8b7f9e` defense-in-depth
- OPS_LOG 2026-05-07 10:00 — TCC diagnosis + recovery path
- CLAUDE.md "Schwab Workflow" section — current drop dir documented

---

### HM-AU — Kirk advisory source routing audit (2026-05-07)

**Type:** Observability / documentation
**Priority:** P3 — low
**Status:** **AUDITED + DOCUMENTED 2026-05-07** — see `docs/KIRK_SOURCES.md`. One bug surfaced and queued as HM-AU-β.
**Origin:** 2026-05-07 morning Kirk paper-source check surfaced ambiguity in `/api/kirk/advisory?source=...` semantics.

#### Audit findings (2026-05-07)
1. **`?source=paper`** = engine path (`generate_kirk_advisory()`), reads `data/real_holdings.json`. **The name is post-Option A back-compat — actual data is Schwab/TradeStation, not Alpaca paper.** Per commit `e41ddb2` (2026-05-05), the engine was retargeted to `real_holdings.json`; the source name stayed for callers' back-compat.
2. **`?source=real`** = inline path at `dashboard/app.py:13422`, reads same `data/real_holdings.json` via `_read_real_positions_sync()`. Different output shape (regex-parsed action labels), bypasses rule engine + `kirk_advisory_log` writes.
3. **`?source=all`** = **bug** (HM-AU-β below). Both paper and real handlers read the same JSON file → returned positions are duplicated.
4. **Default source** = `"paper"` (function signature). Three of five front-end callers use the default; two use `_kirkSource` (typically `'real'`).
5. **Morning 23 → 11 position shift** explained: snapshot rewrite during HM-AT-β backlog drain at 09:14 MST; not a routing inconsistency.

Full behavior table: `docs/KIRK_SOURCES.md`.

#### Problem
Same endpoint (`/api/kirk/advisory`) returned different data depending on time of day, after a Schwab CSV import flipped intermediate state:
- 06:50 MST: `?source=paper` → 23 positions (Alpaca paper book)
- 10:50 MST: `?source=paper` → 11 positions (Schwab `real_holdings.json` after morning import)

Per HM-AJ-documented gotcha: `?source=real` **bypasses** `generate_kirk_advisory()` entirely and uses inline action-logic at `dashboard/app.py:13420`. Other `?source=` values' behavior is not documented — unclear which paths invoke the rule engine vs. inline logic, and what data file/table each one reads.

#### Open questions
1. What `?source=` values does the endpoint accept?
2. For each value: does it call `generate_kirk_advisory()` or use inline logic?
3. For each value: what is the underlying data source (Alpaca API, `real_holdings.json`, `paper_holdings.json`, schwab_holdings table, positions table)?
4. Which value does the dashboard front-end use by default? Does that match operator intent?
5. Is the source-name vs. data-source mapping intentional or accidental drift?

#### Shape
1. Read `dashboard/app.py:13420` (inline `?source=real` path) and `generate_kirk_advisory()` to enumerate accepted source values + branching logic.
2. Read each source's underlying data accessor.
3. Cross-reference dashboard front-end calls (search `kirk/advisory?source=` in HTML/JS).
4. Produce a behavior table mapping source value → code path → data source → typical row count.
5. Document in `CLAUDE.md` or `docs/SCHEMA.md` under a new "Kirk Advisory Routing" section.
6. If any source name contradicts its data source (e.g., `?source=paper` returning Schwab data), flag for follow-up rename or re-routing — but don't rename in this audit; document and surface to Admiral.

#### Effort
~30 min Scotty (read 4-6 code locations + 1 doc write).

#### Acceptance criteria
- [ ] Behavior table in `CLAUDE.md` or `docs/SCHEMA.md`: `?source=` value → code path → data source → expected row count
- [ ] HM-AJ gotcha note cross-linked
- [ ] Any naming/routing contradictions flagged with proposed renames (no actual renames in this audit)

#### Related
- HM-AJ — Kirk parse hardening + observability + alert hygiene (commit `796acbf`)
- 2026-05-07 morning observation: same endpoint returned 23 → 11 positions across the day
- `docs/KIRK_SOURCES.md` — full behavior table, snapshot data flow, naming-vs-data contradiction explained

---

### HM-AU-β — `?source=all` returns duplicate positions (2026-05-07)

**Type:** Bug
**Priority:** P3 — no front-end caller currently uses `?source=all` (per HM-AU audit grep), so user-visible impact is zero today; latent risk if a future caller adopts it.
**Status:** Proposed
**Origin:** HM-AU audit 2026-05-07. Bug surfaced when reading `dashboard/app.py:13488-13501` in light of post-Option A data routing (`paper` re-targeted to `real_holdings.json` in commit `e41ddb2`).

#### Bug
The `?source=all` branch concatenates `paper_positions + real_positions`:

```python
if source == "all":
    from engine.kirk_advisory import generate_kirk_advisory
    paper_result = generate_kirk_advisory()      # reads data/real_holdings.json
    paper_positions = paper_result.get("positions", []) or []
    for p in paper_positions:
        p["origin"] = "paper"
    paper_result["source"] = "all"
    paper_result["source_label"] = "Combined Paper + Real"
    paper_result["positions"] = paper_positions + real_positions  # ← BOTH come from real_holdings.json
    paper_result["real_cash_available"] = real_cash
    return paper_result
```

`paper_positions` (from `generate_kirk_advisory()` → `_load_real_holdings()`) and `real_positions` (from `_read_real_positions_sync()`) **both read `data/real_holdings.json`**. The concatenation produces each position twice, with one copy labeled `origin="paper"` and the other `origin="real"`. Pre-Option A this was correct (paper actually meant Alpaca paper book, real meant Schwab); post-Option A both sides resolve to the same file.

#### Reproduction
```bash
curl -s 'http://localhost:8080/api/kirk/advisory?source=all' \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); print(len(d.get("positions",[])))'
```
Expected (post-fix): 11. Actual today: 22.

#### Fix shape options

| Option | Effort | Behavior |
|---|---|---|
| (A) Drop `paper_positions` from the union; rename to "engine-output for `real`" | ~10 min | `?source=all` returns engine-path advisory + cash, no duplication |
| (B) De-dup by `symbol` after concat | ~10 min | Keeps both paths' enriched data; latest write wins per symbol |
| (C) Restore `paper` to truly mean Alpaca paper book; revert Option A retargeting in `e41ddb2` | ~30 min | Largest scope — undoes the 2026-05-05 Admiral decision, breaks current callers; not recommended |
| (D) Deprecate `?source=all` entirely; return 410 Gone | ~5 min | Cleanest if no caller needs union semantics today |

**Recommendation: (A)** — given no front-end caller uses `?source=all` (per audit grep at `docs/KIRK_SOURCES.md`), the union semantics are unused. Returning the engine path's output keeps the action labels + market context + alert dedup intact and stops the duplication.

#### Acceptance criteria
- [ ] `?source=all` returns N unique positions (N = active accounts in `real_holdings.json`)
- [ ] No duplicate `symbol` values in the response
- [ ] `docs/KIRK_SOURCES.md` updated with post-fix behavior

#### Related
- HM-AU — audit + behavior table
- `e41ddb2` — Option A retargeting that created the latent bug
- `docs/KIRK_SOURCES.md`

---

### HM-AK — Fleet roster cleanup (2026-05-07)

**Type:** DB hygiene
**Priority:** P3 → shipped same-day
**Status:** **SHIPPED 2026-05-07** — see `migrations/HM-AK_dormant_cleanup_2026-05-07.sql` and OPS_LOG 2026-05-07.
**Origin:** 2026-05-06 evening fleet roster check + 2026-05-07 morning HM-AK diagnosis. Surfaced 12 dormant zombies among 50 ai_players rows.

#### Outcome
12 dormant agents halted via UPDATE: 11 to `halt_mode='full'`, 1 (gemini-2.5-flash, 2 open positions) to `halt_mode='exit_only'`. Halt-mode census shifted from 37/9/4 (active/full/exit_only) to **25/20/5**.

**Halted (paid-API zombies, 6):**
- `claude-haiku`, `claude-sonnet`, `gpt-4o`, `gpt-o3`, `grok-4` → `halt_mode='full'`
- `gemini-2.5-flash` → `halt_mode='exit_only'` (had 2 open positions)

**Halted (dormant Ollama, 6):**
- `qwen-coder-haiku`, `qwen3-14b-grok3`, `qwen3-8b-4o`, `qwen3-8b-o3`, `ollama-glm4`, `ollama-gemma27b` → `halt_mode='full'`

**Side effect:** all three duplicate display name conflicts resolved (`Lt. Cmdr. Worf` × 3 → 1, `Lt. Cmdr. Spock` × 2 → 1, `Qwen3 14B Pro` × 2 → 1). The zombies leave active iteration, leaving only the canonical agent in each name slot.

**No service restart required** — halt_mode is read fresh per request via `engine/halt_gate.py`.

**Rollback:**
```sql
UPDATE ai_players SET halt_mode='active', halted_at=NULL, halt_reason=NULL
 WHERE halt_reason LIKE 'HM-AK 2026-05-07%';
```

#### Related
- `migrations/HM-AK_dormant_cleanup_2026-05-07.sql` — checked-in SQL artifact
- OPS_LOG 2026-05-07 — full diagnosis + outcome
- HM-AK-β below — architectural follow-up (scan loops still ignore halt_mode)

---

### HM-AK-β — Scan loops should filter by halt_mode, not is_active (2026-05-07)

**Type:** Architectural debt
**Priority:** P3 — escalated and shipped same-day
**Status:** **SHIPPED 2026-05-07** (commit `77de5be`) — Option A applied to the 3 known iteration sites (`main.py:1991`, `engine/risk_radar.py:168`, `engine/autopilot.py:63`). Iteration count drops ~49 → ~25 per cycle. Dashboard follow-up + dayblade-exclusion cleanup queued as HM-AK-β.2 + HM-AK-γ below.
**Origin:** HM-AK diagnosis 2026-05-07. Surfaced as a separate ticket because scope is too large for a same-day ship.

#### Problem
Multiple scan/iteration sites use `WHERE is_active=1` instead of `halt_mode='active'`:
- `main.py:1991` — `SELECT id, display_name FROM ai_players WHERE is_active=1 AND id != 'dayblade-0dte'`
- `engine/risk_radar.py:168` — same pattern
- `engine/autopilot.py:63` — same pattern
- `engine/cost_tracker.py:387` — `WHERE is_active=1`
- `engine/q_entity.py:224` — `WHERE is_active=1`
- `engine/providers/base.py:1119` — `WHERE is_active=1`
- ... and ~10 other sites (full inventory in HM-AK diagnosis logs)

After HM-AK, 25 rows are `halt_mode='active'` and 25 are halted (full or exit_only). But all 49 with `is_active=1` (only `webull` is `is_active=0`) still pass the iteration filter. Per-trade halt gates downstream block actual execution, so this is **not a safety issue** — it's just compute waste from iterating ~25 halted rows per cycle.

Per CLAUDE.md (2026-04-25 audit + HM-A migration): "halt_mode is now the only working per-player kill switch". The iteration sites haven't caught up.

#### Shape

**Option A (small, safe):** Replace `WHERE is_active=1` with `WHERE halt_mode='active'` at each iteration site. Touch ~17 SQL strings, one PR. ~1-2 h Scotty (read each site, verify call-site semantics, retest). Per-site analysis required because some callers may want to see halted agents (e.g. `cost_tracker` reporting historical costs).

**Option B (bigger, cleaner):** Introduce a single helper `engine/db_helpers.py::active_player_ids()` that returns agent IDs where `halt_mode='active'`, and migrate all iteration sites to call it. ~3 h Scotty. Future migrations only need to update the helper.

**Option C (defer):** No code change — accept that iteration is wider than execution. Compute waste is small (a few SELECTs per cycle).

#### Recommendation
**Option A or B post-soak.** Both are safe but neither is urgent. The execution-gate path is already correct via `halt_gate.py` per-trade checks; this is just iteration efficiency.

#### Acceptance criteria (if shipped)
- [ ] All `WHERE is_active=1` iteration sites replaced (or migrated to helper)
- [ ] Per-site verification that semantics are preserved (cost_tracker reports may want historical view)
- [ ] No regression in scan/trade/signal volume

#### Related
- HM-AK — parent (shipped 2026-05-07)
- HM-AK-β.2 — extend to dashboard sites (below)
- HM-AK-γ — drop redundant dayblade-0dte exclusion (below)
- 2026-04-25 audit notes — `is_active`, `is_paused`, `crew_role` are decorative; `halt_mode` is the kill switch
- HM-A — migrated production read paths from `is_halted` to `halt_mode`; iteration sites not migrated

---

### HM-AK-β.2 — Extend halt_mode filter to 3 dashboard iteration sites (2026-05-07)

**Type:** Architectural cleanup (HM-AK-β follow-up)
**Priority:** P4 — LOW (iteration efficiency only, not safety-critical)
**Status:** Proposed
**Origin:** HM-AK-β commit `77de5be` deferred 3 dashboard sites pending per-site read confirmation.

#### Problem
Three sites in `dashboard/app.py` use the identical scan-loop SQL pattern that HM-AK-β just patched in `main.py`/`engine/`, but were deferred because their use case (trade-iteration vs roster-display) wasn't confirmed at ship time:

- `dashboard/app.py:3904` — `SELECT id, display_name FROM ai_players WHERE is_active=1 AND id != 'dayblade-0dte'`
- `dashboard/app.py:4619` — same SQL
- `dashboard/app.py:12908` — same SQL

The `id != 'dayblade-0dte'` exclusion is the tell — it's the same pattern the scheduler scan loops use, suggesting these are also trade-iteration paths, not pure roster-display. But that needs to be **verified site-by-site** before applying the filter (display sites must show all halted agents).

#### Shape
Per site:
1. Read the surrounding function context
2. Classify as iteration (apply filter) or display (leave alone)
3. For iteration sites: add `AND halt_mode='active'` to the WHERE clause + tag `# HM-AK-β.2 2026-05-07`

#### Effort
~15-20 min Scotty (3 file reads + 0-3 edits depending on classification + commit + restart + verify).

#### Acceptance criteria
- [ ] Each of the 3 sites classified (iteration vs display) with rationale in commit message
- [ ] Iteration sites get `halt_mode='active'` filter
- [ ] Display sites left as-is, with comment explaining why
- [ ] Service restart + smoke verify

#### Related
- HM-AK-β — shipped 3-site fix (commit `77de5be`)
- HM-AK-γ — dayblade-exclusion cleanup (would touch the same sites; sequence HM-AK-β.2 first)
- `dashboard/app.py:5139, 5202` — already use `COALESCE(halt_mode,'active')='active'` (positive precedent in the same file)

---

### HM-AK-γ — Drop redundant `id != 'dayblade-0dte'` exclusion (2026-05-07)

**Type:** Cleanup (HM-AK-β follow-up)
**Priority:** P4 — LOW (no functional change)
**Status:** Proposed
**Origin:** HM-AK-β commit `77de5be` left the `id != 'dayblade-0dte'` clause in place for back-compat.

#### Problem
Post-HM-AK (commit `2b89651`) and HM-AF (earlier 2026-05-06), `dayblade-0dte` is `halt_mode='full'`. Once HM-AK-β added `halt_mode='active'` to the iteration filter, the explicit `id != 'dayblade-0dte'` exclusion became **redundant** — the halt_mode filter already excludes it.

Affected sites (all currently carry both clauses post-HM-AK-β):
- `main.py:1992`
- `engine/risk_radar.py:169`
- `engine/autopilot.py:64`
- `dashboard/app.py:3904, 4619, 12908` (after HM-AK-β.2 ships, if classified as iteration)

#### Shape
Drop the `AND id != 'dayblade-0dte'` clause from each site post-HM-AK-β.2. Tag `# HM-AK-γ 2026-05-07: removed redundant dayblade exclusion`.

**Constraint:** sequence HM-AK-β.2 BEFORE HM-AK-γ. If HM-AK-γ ships first and a future operator un-halts dayblade-0dte (e.g., to reactivate a 0DTE strategy), the iteration filter would no longer exclude it. The two-clause defense protects against that footgun until HM-AK-γ explicitly removes it as deliberate cleanup.

#### Effort
~5 min Scotty (after HM-AK-β.2 lands; then a single multi-site edit + commit + restart).

#### Acceptance criteria
- [ ] HM-AK-β.2 shipped first
- [ ] Redundant exclusion dropped at all confirmed iteration sites
- [ ] Service restart + smoke verify
- [ ] Re-confirm dayblade-0dte halt_mode='full' is the only protection (no rollback to active without explicit ticket)

#### Related
- HM-AK-β — shipped halt_mode filter (commit `77de5be`)
- HM-AK-β.2 — dashboard extension (sequence first)
- HM-AF — dayblade-0dte halt_mode='full' (the reason the exclusion is now redundant)

---

### HM-AW — Signal Center auth + network exposure review (HALTED 2026-05-07)

**Type:** Hygiene / network exposure
**Priority:** P3 — BLOCKED on HM-AW.3 (2FA enforcement) before LAN bind can ship
**Status:** **HALTED 2026-05-07** at Phase C — HARD STOP #10 fired. LAN bind shipped on local commit `0d3e5dc` (NOT pushed); Captain manual LAN verification surfaced that 2FA TOTP was advertised in code but NEVER WIRED — step-1 password match at `signal-center/server.py:658-665` sets `session["authenticated"]=True` directly without ever setting `totp_pending`, making step-2 (lines 624-652) dead code. Commit `0d3e5dc` was reset (`git reset --hard HEAD~1`); service restarted; bind verified back to `127.0.0.1:9000`. See `docs/HM-AW_PHASE_A_DIAGNOSE.md` Phase C section for the full diagnosis, rollback steps, and audit miss explanation. **Sequencing:** HM-AW.3 (2FA enforcement) MUST ship and verify before HM-AW (binding) can be re-attempted. HM-AW.2 (multi-user RBAC port) is a separate follow-on if Captain wants Bonnie/Dad on 9000.
**Origin:** 2026-05-07 14:55 MST Captain note post-HM-AQ-β ship close-out. Letter `HM-AT` was originally proposed but already used today (Schwab watcher TCC fix); rebadged as `HM-AW` to avoid collision (AV = HM-AV ALPACA→APCA simplification).
**Sequence:** Hold until HM-AW.3 ships AND HM-AQ-β stabilizes (24h soak after the 1,223-symbol universe lands in production, est. 2026-05-08 evening).

#### Background
Port 9000 (`signal-center` web UI) is currently bound to `127.0.0.1` from a legacy pre-2FA security posture. Browser access from non-bigmac devices requires SSH tunnel today. Captain wants to reopen 9000 to the network now that:
- 2FA TOTP enforcement is in place
- Multi-user auth (Captain, Bonnie observer mode, Dad charts permissions) is established
- The original justification for localhost-only (no auth layer) no longer applies

**Two distinct authentication paths to keep clear:**
- **Browser access** (humans) — 2FA TOTP + role-based access control via signal-center server
- **Automation access** (Scotty/scripts) — SSH keys + bigmac user account at the OS layer; does NOT go through Signal Center auth

These are NOT the same thing; CLAUDE.md should document them separately to prevent future-XO from conflating them.

#### Sub-questions for Phase 1 investigation (before any binding change)
1. **Network exposure path**:
   - (a) LAN-only via `0.0.0.0` (any 192.168.1.x device with auth) — simplest
   - (b) Cloudflare tunnel like `bridge.ollietrades.com:8080`, e.g. `signals.ollietrades.com` — broader
   - (c) Both — LAN + Cloudflare for full access redundancy
2. **2FA TOTP enforcement audit**: confirm 2FA is required on ALL sensitive routes (not just login page). Spot-check `/api/admin/*`, `/api/trades`, `/api/signals/post`, etc.
3. **RBAC verification**: confirm Bonnie observer mode (read-only) and Dad charts permissions (charts-only) still work post-network-exposure.
4. **Automation auth path documentation**: add a section to CLAUDE.md or `docs/AUTH.md` (new) clarifying the SSH-keys-vs-2FA distinction.

#### Investigation pre-flight (read-only, before any code change)
- `signal-center/server.py`: identify host argument and binding logic
- launchctl plist for `com.trademinds.signal-center`: verify no override of bind address
- Spot-check 2FA enforcement on a representative non-login route via `curl` without TOTP cookie
- Spot-check RBAC by simulating Bonnie/Dad auth tokens

#### Shape (after Phase 1 sub-questions resolved)
- One-line binding change in `signal-center/server.py` (or env var)
- launchctl restart of `com.trademinds.signal-center`
- Optional: Cloudflare tunnel config addition (separate ticket if pursued)

#### Effort
~30 min Scotty (Phase 1 investigation + binding change + restart + verify) once Captain greenlight.

#### Risk
LOW. Auth posture handles what bind-localhost was protecting. Reversal is a single-line revert + restart.

#### Acceptance criteria
- [x] Phase 1 sub-questions answered + documented (`docs/HM-AW_PHASE_A_DIAGNOSE.md`)
- [HALTED] Binding change shipped — was shipped on local commit `0d3e5dc`, then reset after HARD STOP #10
- [BLOCKED] All Signal Center routes confirmed under 2FA TOTP enforcement — **2FA never wired; tracked as HM-AW.3**
- [BLOCKED] Bonnie observer + Dad charts RBAC verified working — **RBAC never ported to port 9000; tracked as HM-AW.2**
- [ ] CLAUDE.md / docs/AUTH.md updated with SSH-keys-vs-2FA distinction (deferred until HM-AW re-attempt)

#### Related
- `docs/HM-AW_PHASE_A_DIAGNOSE.md` — full Phase A + C diagnose, rollback record, lessons
- HM-AW.2 (this file) — multi-user RBAC port (sequenced after HM-AW)
- HM-AW.3 (this file) — 2FA TOTP enforcement (prerequisite for HM-AW)
- HM-AT — Schwab watcher TCC fix (SHIPPED 2026-05-07; namespace collision avoided by using HM-AW here)
- HM-AT-β — Schwab watcher inbox migration (SHIPPED 2026-05-07, commit `5b87d69`)

---

### HM-AW.3 — Signal Center 2FA TOTP enforcement (2026-05-07)

**Type:** Auth / security gap
**Priority:** P2 — MUST ship before HM-AW (LAN bind) can re-attempt
**Status:** Proposed (filed 2026-05-07 from HARD STOP #10)
**Origin:** Captain manual Phase C verification of HM-AW (2026-05-07) discovered Sniff could log into 9000 from LAN with username + password only — no TOTP prompt. Investigation in `docs/HM-AW_PHASE_A_DIAGNOSE.md` F4 + Phase C section showed step-1 success branch sets `session["authenticated"] = True` directly; step-2 TOTP path is dead code.

#### Background
The TOTP infrastructure exists in `signal-center/server.py`:
- `_SC_TOTP_SECRET` env var (line 40), populated via `.env` inline loader (lines 21-29)
- `_sc_totp = pyotp.TOTP(_SC_TOTP_SECRET)` (line 41)
- TOTP HTML page (line 120) renders the 6-digit form
- Step-2 verification at lines 624-652 uses `_sc_totp.verify(code, valid_window=1)` against `session["totp_pending"]`

But step-1 success at lines 658-665 sets `session["authenticated"] = True` and redirects to `/` without ever setting `session["totp_pending"]`. The "2FA disabled — authenticate directly" comment is the smoking gun.

#### Shape
1. Edit step-1 success branch (lines 658-665) to:
   - If `_sc_totp` is configured (i.e. `TOTP_SECRET` is set): set `session["totp_pending"] = True`, set `session["totp_pending_user"] = username`, redirect to `/login?step=2` (do NOT set `authenticated`).
   - If `_sc_totp` is None: keep current direct-authenticate behaviour (degraded mode for environments without `TOTP_SECRET`).
2. Verify the existing step-2 path (lines 624-652) handles the `totp_pending` session key correctly when set from step-1 (it already does — it pops `totp_pending` and `totp_pending_user` and sets `authenticated` after `_sc_totp.verify` succeeds).
3. Smoke-test (localhost): POST username+password → expect 302 to `/login?step=2`; GET `/login?step=2` → expect TOTP page HTML.
4. Verify with the Sniff TOTP authenticator app from `.env` `TOTP_SECRET=4X6RT3GCI2CW5IJSZ76PO5QPIPPZRNDY`.
5. Captain manual verification from LAN device after HM-AW re-ship.

#### Effort
~20 min Scotty (small edit + restart + curl smoke + manual TOTP verification).

#### Risk
LOW for code change. Reversal is one-line revert. Risk of breaking login if `TOTP_SECRET` is invalid or pyotp version mismatch — mitigate by keeping the `if _sc_totp is None:` degraded branch so the service is never unloggable-into.

#### Acceptance criteria
- [ ] Step-1 success branch routes to step-2 when `_sc_totp is not None`
- [ ] curl smoke shows 302 → `/login?step=2` after correct password POST
- [ ] Captain manual TOTP verification from authenticator app succeeds
- [ ] HM-AW LAN bind change can be re-shipped after this lands

#### Related
- `docs/HM-AW_PHASE_A_DIAGNOSE.md` F4 — exact lines + diagnosis
- HM-AW (this file) — blocked on this ticket

---

### HM-AW.2 — Signal Center multi-user RBAC port (2026-05-07)

**Type:** Auth / RBAC
**Priority:** P3 — sequenced AFTER HM-AW (LAN bind, after HM-AW.3 2FA lands). Only required if Captain wants Bonnie/Dad on port 9000.
**Status:** Proposed (filed 2026-05-07 alongside HM-AW.3)
**Origin:** Phase A of HM-AW (2026-05-07) discovered that the multi-user RBAC config in `.env` (`DASHBOARD_USERS=Sniff:admin:..., Bonnie:observer:..., Dad:charts:...`) is consumed by `dashboard/app.py:557 _parse_users()` (port 8080) ONLY. `signal-center/server.py` (port 9000) reads only the singular `DASHBOARD_USER` / `DASHBOARD_PASS` env vars and accepts a single user. Captain elected to ship HM-AW with single-user posture (Sniff only on 9000); HM-AW.2 captures the optional follow-on if Bonnie/Dad need 9000 access too.

#### Shape
Port `_parse_users()` from `dashboard/app.py:557` into `signal-center/server.py`. Replace the singular `_SC_USER` / `_SC_PASS` check at line 658 with a registry lookup keyed on the submitted username. Preserve role attribution (admin / observer / charts). Wire role into `session["role"]` and add per-route role gating where Bonnie or Dad permissions differ from Sniff.

#### Effort
~30–60 min Scotty (port the function, swap the check, decide which signal-center routes admit observer/charts roles, smoke-test from each user's credentials).

#### Risk
LOW for the port itself; MEDIUM if signal-center routes need new role gates that don't exist in `dashboard/app.py` for analogous reasons. Reversal is straightforward (git revert).

#### Acceptance criteria
- [ ] `_parse_users()` ported and wired into `_auth_gate` / login flow
- [ ] Sniff, Bonnie, Dad all log in successfully with their own credentials
- [ ] Per-route role gating decisions documented (Bonnie read-only — what does that mean for `/api/signals/<id>/execute`? Dad charts-only — what does that mean for non-charts routes?)

#### Related
- HM-AW (this file) — single-user posture shipped under that ticket
- `dashboard/app.py:557 _parse_users()` — source of truth to port
- `.env` `DASHBOARD_USERS=Sniff:admin:ollietrades-admin,Bonnie:observer:ollietrades-crew,Dad:charts:none`

---

### HM-AM — Total Portfolio Unification (ALL PHASES SHIPPED 2026-05-07)

**Type:** Cross-source data layer (multi-phase epic)
**Priority:** P3 — closed; all four phases shipped 2026-05-07 (autonomous mode)
**Status:** **ALL PHASES SHIPPED 2026-05-07.** Phase 1 (`4f0bcff`) data layer · Phase 2 (`d338605`) Kirk envelope · Phase 3 (`d6c9647`) Advisory Team prompt · Phase 4 (`52d7298`) dalio-metals prompts. Captain intent ("metals are an extension of the total portfolio") closed end-to-end.
**Origin:** Captain mental-model 2026-05-06: "metals are an extension of the total portfolio." Schwab + Dilithium Reserve + Alpaca paper currently siloed across `data/real_holdings.json`, `metals_ledger` table, and `AlpacaBridge`. Kirk + Advisory Team see Schwab only. Goal: unified read-only API.

#### Phase 1 outcome

`engine/total_portfolio.py` provides:
- `get_total_portfolio() -> TotalPortfolio` — full unified view (positions + cash + totals + sources_loaded/failed)
- `get_portfolio_summary() -> dict` — lightweight summary
- 30s TTL cache (matches `engine/universe.py` precedent)
- Per-source resilience: each source loaded independently; failures recorded in `sources_failed` rather than raising

First smoke (2026-05-07): **22 positions, $138,371.20 total value**, all 3 sources loaded clean. See `docs/TOTAL_PORTFOLIO.md`.

#### Phase 2 (SHIPPED `d338605`) — Kirk advisory integration

`engine/kirk_advisory.py::generate_kirk_advisory()` now augments its return envelope with a `total_portfolio` key from `get_portfolio_summary()`. Per-Schwab-position executive action logic preserved (alert semantics + team_advisor_grok coupling intact). Defensive try/except: failure logs a warning and the envelope omits the key.

#### Phase 3 (SHIPPED `d6c9647`) — Advisory Team integration

`engine/team_advisor_grok.py::run_grok_subadvisor()` now injects a "## Total Portfolio Context" preamble into Grok's user prompt with cross-source totals (value/cash/invested + position count + sources_loaded). Stale `"~$22k notional + ~$2.2k cash"` hardcode removed. Per-Schwab-position breakdown loop (executive surface) preserved.

#### Phase 4 (SHIPPED `52d7298`) — `dalio-metals` strategy realign

Two `if self.player_id == "dalio-metals":` injection sites in `engine/providers/base.py` (single-shot prompt path + 3-step research/thesis/execute Step 2 thesis). Each appends a TOTAL PORTFOLIO CONTEXT block to `personality_block` so Mr. Dalio's All Weather reasoning sees Schwab + metals + Alpaca paper, not just metals. Other personas untouched.

#### Acceptance

- [x] `engine/total_portfolio.py` ships read-only data layer (Phase 1)
- [x] Standalone smoke succeeds (`venv/bin/python3 engine/total_portfolio.py`)
- [x] Per-source resilience verified (sources_failed pattern works)
- [x] 30s TTL cache + `force_refresh` flag
- [x] `docs/TOTAL_PORTFOLIO.md` documents module, data shape, deferred phases
- [x] Kirk advisory envelope includes `total_portfolio` (Phase 2)
- [x] Advisory Team prompt includes Total Portfolio Context preamble (Phase 3)
- [x] `dalio-metals` prompts include Total Portfolio Context preamble at both sites (Phase 4)
- [x] All consumers defensive (try/except, prompt builds without preamble on failure)

#### Cross-references

- `docs/TOTAL_PORTFOLIO.md` — full module reference
- `engine/alpaca_bridge.py::AlpacaBridge.status() / .positions()` — Alpaca source
- `data/real_holdings.json` — Schwab/TradeStation source (HM-AT-β pipeline)
- `metals_ledger` table — physical metals (`docs/SCHEMA.md`)
- HM-AT-β — Schwab CSV pipeline that feeds the real_holdings.json source
- HM-AU — Kirk advisory source routing audit (relevant when Phase 2 integration starts)

---

## Lessons

**2026-05-04 — Stale-bytecode trap from in-flight schema changes:** HM-B's `DROP COLUMN ai_players.is_halted` (commit `9256890`) created a stale-bytecode mismatch in the running trader process (PID 13734). The service was started at 08:32 MST — before HM-A's source migration shipped that morning — so the in-memory bytecode still had pre-HM-A SQL referencing the now-dropped column. Errors began at 17:36, but were caught by `try/except` blocks at the call sites and surfaced only as quiet `console.log` warnings: 15 occurrences across `War Room`, `ai_brain.py:286/295/533`, and three agents (ollama-coder, mlx-qwen3, energy-arnold) before discovery via log scan during PED retirement verification ~70 minutes later. Source code post-HM-A was clean; the issue was entirely in the long-running process's compiled module cache. **Future schema-change sessions should include a service restart in the verification phase OR a longer (30+ min) post-change soak window before declaring the change stable**, specifically to flush any pre-migration in-memory residue. This is also a HM-U datapoint: the silent-failure pattern (caught exceptions, swallowed errors) hid the issue from cursory checks — only a focused log scan surfaced it.

---

---

---

## SHIPPED 2026-05-06 19:40 MST — HM-AI Grok→Team rename (commit `b09d7a5`)

**Background:** "Grok" was legacy branding from the xAI Grok-4 era. The model has been qwen3:8b on Ollie Box since the 2026-04-17 RAM patch. HM-AG-β rewrote the scheduler docstring at `main.py:1718` to say "Advisory Team scheduler"; HM-AI continues that rename through the function, file, and variable layer so the code matches the docstring.

**Conceptual model (post-rename):**

    Team        = parent orchestrator   (run_team_advisor → run_team_scan)
    Grok-sub    = LLM-thesis sub-advisor (run_grok_subadvisor)         ← was run_grok_advisory
    Troi-sub    = sentiment sub-advisor  (run_troi_scan)
    Worf-sub    = tactical-risk sub-advisor (run_worf_scan)

The "grok" name now identifies the **sub-advisor role** (LLM-thesis sub-agent), not the model.

**Renames:**
- `engine/kirk_grok_advisor.py` → `engine/team_advisor_grok.py` (`git mv`, 95% similarity preserved)
- `run_grok_advisory()` → `run_grok_subadvisor()`
- `main.py def run_grok_advisor()` → `def run_team_advisor()`
- `main.py _grok_advisor_slots_done_today` → `_team_advisor_slots_done_today` (global flag)
- `engine/wb_advisory_team.py`: 1 import + 1 call + 1 docstring line
- `dashboard/app.py`: 1 import + 1 comment
- `engine/kirk_advisory.py`: 1 comment line
- Logger name in renamed file: `kirk_grok_advisor` → `team_advisor_grok`

**Preserved (intentionally not changed):**
- `portfolio_advice.advisor='grok'` DB rows — represents the sub-advisor role; preserves history
- Dashboard `🛸 Advisory Team` card with Grok/Worf tabs
- `[HM-AG-α]` log strings — "Grok" is the sub-advisor name, not the model
- `archive/retired/2026-05-04-kirk-swing-desk/` README and all `docs/*` historical references

**Verification matrix (all 9 GREEN, post-restart PID 75149):**
1. `git mv` rename history-preserving (95% similarity)
2. Zero orphan code refs to `kirk_grok_advisor` / `run_grok_advisory` (only self-documenting rename notes inside new file's docstring)
3. `import engine.team_advisor_grok` works; `from engine.team_advisor_grok import run_grok_subadvisor, get_scan_meta` resolves
4. Old `engine.kirk_grok_advisor` import path raises `ImportError`
5. Logger name updated to `team_advisor_grok`
6. Dashboard `/api/wb-team/advice` returns HTTP 200 with shape `{advisors:[grok,troi,worf], meta:{...}}`
7. Startup log line `"Advisory Team armed (Grok+Troi+Worf — fires 9:30 AM…)"` confirmed at `main.py:3879`
8. Manual `POST /api/wb-team/scan` returns `team_scan: true`; Troi + Worf each wrote 3 `portfolio_advice` rows under their advisor keys
9. `[HM-AG-α]` filter logs continue to fire under the renamed function

**Side observation (not a rename problem):** The post-rename trigger had Grok-sub return `parse_error: Expecting ',' delimiter: line 1 column 1514 (char 1513)` — qwen3:8b emitted malformed JSON on this run. The function ran end-to-end through the renamed path and hit the existing error-handling branch correctly. Pre-existing brittleness in `_parse_advice`'s strict `json.loads`. **Flagged as future HM-AJ candidate:** harden `_parse_advice` to recover from truncated/malformed LLM JSON (try-except `json.JSONDecodeError` with a salvage attempt that slices at the last complete `}` before the error position). Earlier 18:36 trigger saved 22/23 cleanly with 1 hallucination caught — proves filter + parse work when LLM behaves.

**Reversal:** `git revert b09d7a5` + `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader`. No DB state to roll back.


## SHIPPED 2026-05-06 18:00 MST — Kirk None-fix (commit `d2be8bb`)

**Root cause:** `engine/kirk_advisory.py:277` had a default-value bug:

    fg_score = fg.get("score", 50) if fg else 50

The `50` default only kicks in if `fg` is None OR the `score` key is missing. But Fear & Greed API can return `{"score": null}`, which makes `.get()` return None explicitly — bypassing the default. That None then flowed to line 357 (`if vix > 30 and fg_score < 35:`), throwing `TypeError: '<' not supported between instances of 'NoneType' and 'int'`.

**Fix:** Added explicit None-check:

    fg_score = fg.get("score") if fg else None
    fg_score = 50 if fg_score is None else fg_score

**Verified post-restart (PID 71272):** `generate_kirk_advisory()` returns clean dict with positions, cash=$2220.77 (matches Schwab snapshot), market_context, recommendations. No error key.

**Discovered along the way:**
- Kirk Advisory and "Advisory Team" (`engine/wb_advisory_team.run_team_scan`) are TWO separate systems with overlapping branding. The "Kirk Grok Swing Advisor" comment in main.py is misleading — that scheduler entry calls Advisory Team, not Kirk Advisory.
- Advisory Team has been working all along (10:40 MST today: 23 positions, 6 recommendations via qwen3:8b on Ollie Box). Kirk Advisory was the broken one.
- This was the root cause of the "Kirk silent" observability gap noted in HM-AF AMENDMENT — Kirk wasn't silent, it was crashing on every fire and only emitting the error log line.

**Open follow-ups:**
1. **Refresh `data/real_holdings.json`** — last updated 2026-05-04. Kirk now works but advises on stale positions until a fresh Schwab export is loaded.
2. **Add observability log lines** (original HM-AF Item #7-style) — Kirk currently logs only on error. Add success-side logging so we can verify daily fires.
3. **Investigate Advisory Team scope** — what's it advising on (23 positions ≠ Schwab ≠ Webull ≠ Alpaca counts), and is its output surfaced anywhere?


## SHIPPED 2026-05-06 11:53 MST — HM-AF-β + HM-AF-γ (commit `ca50d45`)

**HM-AF-β (Layer 1: spread-leg awareness):** New `engine/options_utils.py` (+143 new lines) with `parse_occ_symbol()` + `is_spread_leg(symbol)` + `has_open_spread_legs()`. 30s TTL in-memory cache to handle P1's 2-min loop performance. Match logic: parses OCC symbol → matches against `options_trades.legs_json` structured fields (underlying, expiration, option_type, strike) for rows WHERE `status='open' AND exec_status='open'`. Wired into all three contaminated paths:
- **P1** — `engine/battle_station.py::monitor_active_options` filters position list before the close-evaluation loop (+30/-3).
- **P2** — `engine/alpaca_options.py::close_all_options` per-position skip in EOD sweep (+17/-2).
- **P3** — `engine/dayblade.py` post-trade defense-in-depth observability log (+7).
Fail-closed: any leg-filter exception skips the close (conservative).

**HM-AF-γ (Layer 2: wrong-side-of-book correction):** `battle_station._get_alpaca_options_positions` now preserves qty sign via new `qty_signed` field (`qty` stays `abs()` for backcompat). `_auto_close` branches: `qty_signed < 0` (short) → `submit_single_option(side='buy')` for buy-to-close; `qty_signed > 0` (long) → `close_options_position` for sell-to-close. Fixes the bug where shorts were being treated as longs in close logic.

**HM-AF-α global guard remains ON** (`SPREAD_CANNIBALIZATION_GUARD_ENABLED=True` unchanged). β/γ are STAGED-AND-READY but DORMANT in production — every options close is intercepted by α before reaching β/γ. Lifting α requires a SEPARATE Phase 4 decision after 24h soak (review window opens 2026-05-07 ~11:53 MST).

**CLAUDE.md updated** with β/γ status row in the Feature Flags section, plus a note: "Lifting requires a separate Phase 4 decision; do not auto-lift" (+1/-1).

**Verification post-restart (PID 6633 → 7954, started 2026-05-06 11:53:52 MST):** All 7 deliverables green.
- New bytecode loaded ✅
- HM-AF-α outer guard still firing post-restart ✅ (11:53:59 first fire)
- `is_spread_leg` reachable via direct invocation ✅
- HM-AF-β code dormant under α (zero `[HM-AF-β]` log lines, exactly as designed) ✅
- CLAUDE.md updated with β/γ note + lift procedure ✅
- Zero `Alpaca OPTIONS SELL` post-restart ✅
- Zero `Alpaca options EOD close` post-restart ✅

**Unit test results (re-run against post-edit modules in venv Python):**
- `parse_occ_symbol("SPY260515P00732000")` → `{'underlying': 'SPY', 'expiration': '2026-05-15', 'option_type': 'put', 'strike': 732.0}` ✅
- `is_spread_leg("SPY260515P00732000")` → True ✅ (orphan from open spread id=27)
- `is_spread_leg("SPY260515P00727000")` → True ✅ (the cannibalized long leg, still in legs_json)
- `is_spread_leg("AAPL")` → False ✅
- `is_spread_leg("MSFT250517C00500000")` → False ✅
- `has_open_spread_legs()` → True ✅

The Test 5 result (`is_spread_leg("SPY260515P00727000") → True`) is the critical one — proves the helper correctly checks `options_trades.legs_json` (internal book) and not Alpaca positions. The 727P leg has been closed at Alpaca for hours but remains in the legs_json of the open spread row, and the helper finds it. Architecture is sound.

**Open items remaining (post-ship):**
1. **24h soak window** (opens 2026-05-07 ~11:53 MST) — monitor for unexpected `[HM-AF-β]` lines or any anomalies before deciding to lift α.
2. ~~**Today's 12:45 MST EOD sweep** — gated by HM-AF-α; verify post-12:46 with `grep "HM-AF-α.*close_all_options" logs/trader.log`.~~ ✅ **VERIFIED 2026-05-06 12:49:23 MST** — guard fired at `alpaca_options.py:600` blocking the sweep; zero actual EOD closes post-restart. P2 path now proven working in production alongside P1.
3. **HM-AF-δ** — remove hardcoded `player_id="dayblade-0dte"` in `battle_station.py:668` (lower priority).
4. **Orphan SPY260515P00732000 short** (qty=-1, expires 2026-05-15) — recommend let expire.

**Reversal:**

    git revert ca50d45
    launchctl kickstart -k gui/$(id -u)/com.trademinds.trader

(reverts both layers; α stays ON in either case)

To lift α (separate Phase 4 decision after 24h soak):

    # Edit config.py: SPREAD_CANNIBALIZATION_GUARD_ENABLED = False
    launchctl kickstart -k gui/$(id -u)/com.trademinds.trader


## SHIPPED 2026-05-03 — Sunday Morning Deploy

- **8e06b5e** regime fix deployed at 08:01 MST
- Manual `trader.db` backup taken: `backups/trader.db.pre_regime_fix_deploy_20260503_080141`
- 11 regime ticks verified post-restart (08:16:46 → 10:47:43, all `BULL_CROSS`)
- Edits 1, 2, 3 verified at code level (`main.py` lines 2610, 2656, 2685-2701)
- Runtime verification PENDING — Monday market-hours window 06:30-13:00 MST

## SHIPPED 2026-05-03 — Sunday Afternoon Deploy

- **d2ad748** B15 diagnostic patch (capture NameError traceback frames)
- **17d40b4** B15 fix — `OLLIE_URL` added to `initialize_dayblade()` import
- **cdc03d0** B14 fix — dead `GetAllPositionsRequest` import removed
- **58c43f0** Item 5 — ~60 lines dead crew-server polling removed from `premarket-scan.sh`
- PID 84968 deployed at 15:45 MST; 0 OLLIE_URL errors post-deploy (verified)

## SHIPPED 2026-05-02 (Saturday Night Drydock)

| Fix | File | Description |
|-----|------|-------------|
| Task 1 | git | Checkpoint commit `463c402` — 370 files, 8 drydock sessions |
| Task 3A | `engine/importers/ai4trade_importer.py` | Added `run_import()` alias → fixes nightly import crash |
| Task 3B | `uoa/scraper.py:16` | Fixed docstring example path |
| Task 3C | `premarket-scan.sh:46` | Commented out defunct `launchctl start com.trademinds.crew` |
| restart.sh | `restart.sh:11` | Split `qwen3.5:9b` across two vars to pass pre-commit hook |

## OPEN 2026-05-28 — HM-SCHWAB-ALARM-CROSS-MECHANISM (defense-in-depth follow-up)

**Context:** Schwab watcher + cadence alarm migrated launchd→cron 2026-05-28
(HM-SCHWAB-WATCHER-CRON, OPS_LOG) after both went silent 05-23→05-28 via the
same launchd-at-boot failure. The cron fix restores reboot-survival BUT puts the
watcher AND its staleness alarm on the SAME mechanism (cron) — still shared-fate.

**Lesson (CLAUDE.md doctrine):** an alarm that shares a failure mode with the
thing it monitors provides no defense.

**TODO:** relocate the Schwab staleness alarm to a DIFFERENT mechanism than the
watcher — e.g. a 48h-staleness check inside the always-on trader process
(`main.py` scheduler, which has independent monitoring + its own @reboot wrapper),
or an external uptime/dead-man's-switch monitor. Then a single cron failure can't
silence both the import and its watchdog. Priority: MEDIUM (cron is more robust
than the failed launchd, but the principle stands). Est: 1-2h.

## OPEN 2026-05-28 — HM-OPS-SSH-OLLIE-MAX (incident-response gap, LOW)

During HM-AUDIT-T0 GPU verification, `ssh bigmac@192.168.1.168` (Ollie Max) →
`Permission denied (publickey,password)` — no passwordless key from bigmac to
Ollie Max. Not urgent, but a latent incident-response gap: when the Ollama API
itself is the thing that's down, the dashboard/API can't tell you GPU state and
you'll need a shell on the box (nvidia-smi, systemctl, logs). Today it forced
the GPU spec to be inferred from `/api/ps` (10.6GB co-resident → 16GB-class)
rather than read directly. TODO (no fix now): install an SSH key bigmac→Ollie
Max (or document the manual access path) so shell is available when the API is
the outage. Also why the exact GPU model (RTX 5080 per XO audit) stays
nvidia-smi-unconfirmed.

## OPEN 2026-05-28 — HM-GIT-PUSH-HEALTH-MONITOR (defense-in-depth, ~30min, not someday)

87 commits of silent push failure (HM-PUSH-UNBLOCK) fired no alert. Build a
daily cron — INDEPENDENT of the push pipeline — that runs `git fetch && git
status` (or `git rev-list --count origin/main..HEAD`) and NTFYs ollietrades-admin
if local is more than N commits ahead of origin (suggest N=5). Must NOT live in
the same mechanism as pushing. Pairs with HM-SCHWAB-ALARM-CROSS-MECHANISM as the
2nd instance of the "monitor must fail independently of the monitored" principle.

## OPEN 2026-06-01 — LOW: two display items (Captain-observed, log-only)

Parked as lower-priority display bugs during the S6/0DTE/Troi diagnostic pass.
Diagnose-before-fix per repeat-offender rule (could be stale).

1. **Bridge-Kirk panel stuck F&G 50 / VIX 20.** The Bridge-side Kirk panel still
   renders Fear&Greed 50 / VIX 20 (looks like a hardcoded default/fallback). This
   is a DIFFERENT surface from the signal-center Morpheus Oracle, which was just
   fixed (HM-OVERNIGHT item 3: dedup + as_of stamp + kirk_advisory source_registry
   → /api/sources/health RED). Investigate which endpoint/default the bridge panel
   reads; likely wants the same as_of treatment or to read live VIX/F&G rather than
   a 50/20 placeholder. LOW.

2. **Scanner MU/DELL price-column mismatch.** In the scanner table, MU and DELL
   show a price that appears mis-mapped to the wrong column (price vs another
   numeric column). Verify against the source payload (could be a column-order /
   render-mapping drift, the DOM-shape-drift class). LOW.

## QUEUED 2026-06-01 — RISK-QUALITY: Battle Station 0DTE penny-premium stop (proper review)

NOT a stop-logic bug (the −66.7% SPY PUT was 2026-04-27, historical; stop fired
correctly as CLOSED_LOSS). Root cause = applying a −30% PERCENTAGE stop to a
penny-premium 0DTE option: from a $0.03 entry the option ticks $0.03→$0.02 (−33%)
→$0.01 (−66.7%) with no observable price near −30% (tick granularity wider than
the stop band). Queue for proper review (engine/battle_station_0dte.py):
1. **Min-premium entry gate** — reject contracts whose premium is so low a single
   $0.01 tick already blows the −30% stop (e.g. require entry premium ≥ ~$0.20–0.50).
2. **Absolute-dollar stop** alongside the % stop (STOP_DOLLARS) so cheap options
   exit on absolute risk, not an unrealizable percentage.
Cadence doc-drift (docstrings said "2 min" vs actual every(5).minutes) FIXED
2026-06-01 (doc-only). Tighter cadence reduces but cannot eliminate the overshoot;
the entry gate + dollar stop are the real fix. MED (risk quality), not live-bleeding.

## QUEUED 2026-06-01 — DASHBOARD UX: Strategy Lab "Auto-Optimize" button now no-ops a deploy

Follow-up to fix(safety) cf97b67 (disabled the Strategy Lab auto-deploy footgun —
`auto_optimize_all` now PROPOSES only, writes no files). The dashboard
"Auto-Optimize" button (`dashboard/app.py:13345` `strategy_lab_auto_optimize`)
still reports plain "Complete" — implying a deploy that no longer happens. Update
the panel to show **"Proposed — pending review"** and surface the
`report["proposed"]` list (param old→new + that it was NOT applied), so the next
person to click it isn't misled into thinking config changed.
- Cosmetic-honesty, not a safety issue (the dangerous write path is already gone).
- **Do before anyone relies on the button** — a misleading "success" could fool
  the next operator — but it must NOT jump ahead of a real build.
- Requires manual browser hover/click smoke test per the Frontend Ship Rule
  (single-file edit to `dashboard/static/index.html` + the app.py status text).
LOW (cosmetic), bounded by the "before button is trusted" caveat.

## OPEN 2026-07-01 — HM-DAX (POST-TRIP) — Dax role-degeneration decision

`ai_players` id `ollama-qwen3` (Lt. Jadzia Dax, model `ministral-3:3b`) has
role-degenerated: 100% stock scalps, ZERO options — the CSP role it was
originally scored on (+4.9 Sharpe backtest) now lives on a separate seat,
`shadow-qwen35-csp`. Post-swap realized read (executed_at ≥ 2026-05-15):
Sharpe 0.99 (n=33), net +$48.41, 93.9% win rate, one −$40.68 tail dominating —
net-green but economically negligible as a scalper, and the roster's +4.9
Sharpe figure is doubly stale (different strategy, different model). Prior
art: `docs/FLEET-ROSTER.md:16-18`, flagged 2026-06-15, "structural review
deferred post-trip." Decision needed: repurpose Dax back to CSP (retire
`shadow-qwen35-csp` as redundant), retire Dax's scalping role outright, or
formally migrate/rename the role split so both seats have distinct, non-stale
mandates. No action taken — filing only.

## OPEN 2026-07-01 — HM-VOTE-AUDIT (POST-TRIP) — vote-quality audit of ministral-3:3b War Room voters

~14 War Room voter seats run on `ministral-3:3b` (the same small model behind
the Dax role-degeneration above). Audit whether this model's votes carry
distinguishable signal at War Room scale, or whether ~14 seats voting off one
small, possibly-undifferentiated model dilutes the debate rather than adding
orthogonal perspective (see Duplicate Role Policy in `CLAUDE.md` — "bad
duplication" consolidates to one owner). Scope: pull each seat's vote-vs-outcome
accuracy, check pairwise vote correlation across the ~14, and compare against
larger/differentiated models in the same debates. No action taken — filing only.

## OPEN 2026-07-01 — HM-PY-CONSOLIDATE (POST-TRIP) — watchdog.py off system Python 3.9.6

`watchdog.py` (PID observed running today) executes under
`/Library/Developer/CommandLineTools/.../Python3.framework/Versions/3.9/...` —
macOS system CommandLineTools Python 3.9.6 — while the live trader (`main.py`)
and `scripts/trader_restart.sh`'s canonical launch target both run
`.venv/bin/python3` (Homebrew 3.14.3). This is real three-way interpreter drift
(flagged during today's HM-FULL-AUDIT-2026-07-01, section 3b). Low immediate
risk (watchdog is a thin supervisor with no exotic dependencies), but should be
repointed to `.venv/bin/python3` for consistency and so a future 3.9-only
stdlib quirk doesn't bite silently. No action taken — filing only.

## OPEN 2026-07-01 — HM-LESSON-SHADOW (POST-TRIP) — lesson_validation_shadow silent 25+ days

The `lesson_validation_shadow` table (Reflexion-style self-improvement
mechanism) has not received a write since 2026-06-06 (25+ days as of this
filing) and `lesson_validation_alerted` has zero rows ever. No cron entry
drives whatever process is supposed to populate it — found during
HM-FULL-AUDIT-2026-07-01 section 2e. Decision needed: rewire it to an active
scheduled job (if the self-improvement loop is still wanted) or formally retire
it (drop/archive the table, remove any dead references) so it stops reading as
an ambiguous "is this broken or intentionally off" signal on future audits. No
action taken — filing only.

## OPEN 2026-07-01 — HM-SCORECAP-REVISIT (dated 2026-07-07) — witness_ab SCORE_CAP=300 volume check

Dated follow-up to today's `witness_ab_scorer.py` SCORE_CAP bump 60→300
(commit `bfae596`), per the scorer's own in-file design note: "Option 1
(CURRENT): score everything — SCORE_CAP ≥ daily max, zero bias... revisit if
volume exceeds cap." Debate volume has been running 178–340/day and already
brushed the 300 cap on at least one recent day (338 on 6/24) — if volume holds
above 300/day, the scorer silently reverts to the same time-of-day sampling
bias the cap increase was meant to eliminate. **Watch line filed today:**
2026-07-01 A/B scoring ran imbalanced — deepseek-r1:14b scored 301 debates vs
gpt-oss:20b only 169 (yesterday, 6/30, both were even at 60/60). Escalate from
"watch" to "fix now" if the imbalance persists 3+ consecutive days. Revisit
2026-07-07 regardless. No action taken — filing only.

**GROUNDING FIX LANDED 2026-07-01 — all witness_ab rows on/before this date are
ungrounded-era; score the experiment on post-fix rows.**

Context (HM-CLOSEOUT Item 3): the originally-suspected "total bypass" (neither
ticker_context nor gamma_context ever reaching any witness prompt) turned out
NOT to be quite right on closer inspection — `generate_hot_take()`
(`engine/war_room.py:684`) has unconditionally built and prepended both blocks
for every caller since `ff1a920`/`c8c021d` (2026-06-22/23), regardless of
`prior_takes`. What was ACTUALLY, currently broken: `_record_witness`
(gemma4:12b-it-qat vs plutus-v1:latest live arm) had a separate, redundant
grounding-injection attempt with a closure/scoping bug — reassigning `_ctx`
inside a nested function made Python treat it as local for the whole function,
raising `UnboundLocalError` on every call. Confirmed firing every ~5min in
`logs/trader.log` since at least 14:37 today, silently swallowed by the
surrounding try/except — this arm produced **zero** witness takes (not
"ungrounded" takes — no takes at all) until the fix below. Separately, the
deferred `deepseek-r1:14b`/`gpt-oss:20b` arm (`_queue_ab_witness` →
`scripts/witness_ab_scorer.py`, scored hours-to-days later off-hours) DOES get
grounding from `generate_hot_take`'s built-in call, but computed at SCORING
time, not DEBATE time — a real temporal mismatch this fix also closes by
capturing debate-time grounding into the queued context.

Fix: new shared helper `_grounded_witness_ctx()` (`engine/war_room.py`, right
before `_record_witness`) used by all three witness paths — fixes the crash in
`_record_witness`, adds debate-time-accurate grounding to the `witness_queue`
context for the deepseek/gpt-oss arm, and (redundantly but harmlessly, since
`generate_hot_take` already grounds this arm live) also touches
`_record_shadow_witness` (plutus-v7d). Verified live: trader restarted
(`scripts/trader_restart.sh`, WAL checkpoint fired `0|0|0` clean per
HM-WAL-ROOTCAUSE interim mitigation), first post-restart witness call
succeeded (`logs/trader.log:1532`, `[WR-WITNESS] debate=MU_1782949327
witness_model=gemma4:12b-it-qat wall=34.412s` — no warning, no crash),
reconstructed the same MU prompt offline and confirmed `FACTUAL CONTEXT`
literally present.

**PRE-REGISTERED 2026-07-01 (before post-fix data matures):**
- Scoring window: post-grounding-fix rows only (see 3d stamp).
- Minimum n: 300 scored debates per arm within the window.
- Primary metric: directional accuracy vs realized next-day move on
  non-NEUTRAL verdicts.
- Secondary: agreement rate with McCoy (context, not victory condition).
- Win: one arm leads primary metric by >=5 percentage points at n>=300;
  else DRAW → decide on cost/latency (gpt-oss:20b vs deepseek-r1:14b
  tokens/sec on olliemax).
- Both arms must see the same debate stream; if daily scored counts diverge
  >25% for 3+ consecutive days, experiment is PAUSED-INVALID pending
  balance diagnosis (watch line from 2026-07-01: 301 vs 169).

## OPEN 2026-07-01 — HM-ORPHAN-SEATS (POST-TRIP) — 11 ai_players seats reference absent Ollama models

Cross-referencing `ai_players.model_id` (provider='ollama') against `ollama ls`
on olliemax (192.168.1.168) during HM-FULL-AUDIT-2026-07-01 section 6c found 11
orphan seats — model_id set but the model itself is no longer served on the
fleet host: `deepseek-r1:7b`, `devstral-small-2`, `gemma3:27b-it-qat`,
`gemma4:26b`, `gemma4:31b`, `llama3.1:latest`, `llama4:scout`, `qwen2.5:7b`,
`qwen3-coder:30b`, `qwen3.6:27b`, `qwen3.6:35b-a3b`. Decision needed per seat:
repoint to a currently-served model, or formally retire the seat (halt_mode
already may cover some — cross-check `halt_mode='full'` overlap before
deciding, see HM-FULL-AUDIT-2026-07-01 section F1). No action taken — filing
only.

## OPEN 2026-07-01 — HM-WAL-ROOTCAUSE — trader.db-wal structural bloat, no single leak found

`data/trader.db-wal` reached 642-645MB (vs. a normal near-zero checkpointed
size). `PRAGMA wal_checkpoint(PASSIVE)` on 2026-07-01 checkpointed only 11 of
164,302 WAL frames; `wal_checkpoint(TRUNCATE)` returned `SQLITE_BUSY` twice in
a row (stopped per fail-twice-stop rule, no forced kill/restart attempted).
`lsof` showed 15 concurrent open connections on the WAL file, all from the
trader's own PID — no external tool holding a lock. Root-cause suspect (not a
confirmed single leak): the codebase has no connection pool — ~953 ad-hoc
`sqlite3.connect()` call sites — combined with `main.py:run_scanner()`
background scan/War-Room cycles that can run 10-20+ minutes each (the function
has its own comment referencing a prior 14-min lock-hold stall). With scan and
War Room threads overlapping near-continuously, there may never be a moment
with zero open readers, so WAL checkpoint can never advance past whichever
reader is mid-cycle — a structural "always-a-reader" pattern rather than one
fixable leak, likely worsened by today's SCORE_CAP 300 change extending
witness debate runtime. Needs a deeper pass (e.g. instrumenting which specific
connection is oldest at checkpoint time) before a real fix can be scoped. No
action taken — filing only.

**INTERIM MITIGATION LANDED 2026-07-01 (HM-CLOSEOUT Item 1):**
`scripts/trader_restart.sh` now runs `PRAGMA wal_checkpoint(TRUNCATE);` (`|| true`)
immediately after confirming zero `trader.log` writers and before the new
process launches — the one guaranteed zero-reader window, so the checkpoint
is certain to fully truncate there regardless of the structural always-a-reader
problem during normal operation. This does NOT fix the root cause (WAL will
still grow unbounded between restarts) — it only guarantees the WAL resets to
near-zero on every restart rather than compounding across restarts indefinitely.
Arms on the next natural restart; no restart was forced to install it.

**Diagnostic note on the 642MB figure:** the PASSIVE checkpoint result triple
was `0|164302|11` (not busy, 164,302 total WAL frames, only 11 checkpointed) —
i.e. under normal running conditions almost none of the WAL is reclaimable, not
because it's dead/abandoned space but because some reader's snapshot pins
nearly the entire file. This confirms the 642MB is "live, unmerged" relative to
an open reader, not garbage a routine checkpoint should have already claimed —
consistent with the always-a-reader theory above, and it's why the interim fix
targets the restart's zero-reader window specifically rather than trying
another in-place checkpoint attempt.

## OPEN 2026-07-01 — HM-POLYGON-QUOTES — provider-order gaps + probe-informed recommendation

Six known gap sites where the codebase doesn't follow "Polygon primary" doctrine
(from HM-CLOSEOUT-2026-07-01 Item 4 trace, report-only at the time — no code
touched): `engine/market_data.py::get_stock_price` (25+ callers, Alpaca→Yahoo→
Finnhub→AlphaVantage, Polygon never referenced), `strategies/chain_lookup.py`
(`CHAIN_PROVIDER` defaults to `alpaca`), `engine/options_chain.py` (yfinance-only),
`engine/gamma_map.py` (Alpaca-only, feeds Ready Room GEX), and the dashboard's
two chart endpoints `dashboard/app.py` `/api/candles` + `/api/charts/ohlcv`
(both yfinance-only, the "+2" sites).

**HM-POLYGON-PROBE 2026-07-01 results** (`scripts/polygon_probe.py`, read-only,
SPY/NVDA/WDC, Stocks Starter + Options Starter tier):
- `/v2/last/nbbo/{ticker}` (live quotes): **403 NOT_AUTHORIZED on all 3 tickers**
  — "You are not entitled to this data." Confirmed: this plan tier has NO quote
  endpoint access at all, not delayed-but-usable — fully gated behind upgrade.
- `/v2/snapshot/.../tickers/{ticker}`: 200 OK, returns day/min/prevDay OHLC
  aggregates (o/h/l/c/v/vw) populated; `lastTrade`/`lastQuote` empty on all 3.
  Same gate as above — day-bar data works, quote data doesn't.
- `/v3/snapshot/options/{ticker}`: 200 OK, `open_interest` populated on 2/3
  tickers, but `last_quote`/`last_trade` NULL on all 3 and `greeks` NULL on 2/3
  (inconsistent across tickers — sample was the first 5 contracts returned,
  unfiltered by strike/expiry, so this may reflect which contracts got sampled
  more than a clean capability signal; a filtered re-probe would sharpen this).
  Confirms the existing `alpaca_chain_client.py` header comment ("Polygon
  Starter plan returns no quotes") is accurate for options too.
- No rate-limit headers present in any response on this tier.
- Delay could not be cleanly determined — probe ran after market close, so the
  ~31min gap observed between `ticker.updated` and wall-clock reflects
  time-since-close, not a live real-time-vs-delayed signal. Re-run during RTH
  for a clean delay read if it matters.

**Recommendation per site (probe-informed, no repointing done):**
- `get_stock_price` — **STAY on Alpaca/yfinance.** Polygon quote endpoint is
  403 on this plan; cannot supply live prices at all without a plan upgrade.
- `chain_lookup.py` — **STAY on Alpaca default.** Polygon options snapshot
  returns no usable last_quote/last_trade; matches the code's own prior
  assessment. Not a safe repoint.
- `options_chain.py` — **STAY on yfinance/Alpaca**, same reasoning as above.
- `gamma_map.py` — **TENTATIVE candidate for repoint, needs a build not a flip.**
  GEX computation leans on open_interest + chain structure more than live
  quotes, and `open_interest` DID come back populated in the probe. The
  platform already has a working Polygon-native GEX path
  (`engine/gamma_context.py` / `canonical_gex`) that doesn't depend on
  last_quote/last_trade either — `gamma_map.py` could plausibly follow the
  same pattern, but this needs an actual implementation + test, not a
  probe-only decision. Filed as follow-up, not actioned.
- `/api/candles` + `/api/charts/ohlcv` — **SAFE TO REPOINT.** Probe confirms
  Polygon's day-bar/snapshot data works cleanly (200 OK, real OHLC fields
  populated) on this plan, and `engine/market_data.py::get_polygon_bars` is
  already built, Polygon-primary, and proven working elsewhere in the
  codebase. These two endpoints could call it instead of `yfinance` directly
  with low risk — the only site of the six with a clear, low-risk path.

No repointing performed in this pass — probe results only. Actioning any of
the above (build the gamma_map.py repoint, or flip the two chart endpoints)
is separate follow-up work.

**2026-07-01 — chart repoints approved-in-principle, deferred post-trip by
Admiral order.**

## OPEN 2026-07-01 — HM-SHELLY-WATCHDOG (POST-TRIP) — box-plug auto-cycle design (design only, not built)

Context: HM-SHELLY-PREP-V2 (2026-07-01) shipped `scripts/plug_cycle.sh` (manual
control tool) and `scripts/shelly_net_watchdog.js` (on-device auto-cycle,
installed ONLY on the Allo router + Starlink Mini plugs — network gear, no
stateful DB to corrupt). The bigmac (.245) and olliemax (.246) box plugs stay
**manual-only** until this ticket is actioned — this entry is the design
sketch for eventually giving them a safe auto-cycle path, not an
implementation.

**Why boxes are harder than network gear:** cutting power to a box mid-write
can corrupt `trader.db` (or `signals.db`). Network gear has no such state —
worst case is a clean reboot. A box auto-cycler needs real DB-safety
reasoning that the network-gear watchdog didn't.

**Design sketch:**
1. **Cross-box, never self-monitoring.** A box that's down can't trigger its
   own recovery — bigmac's watchdog must monitor olliemax's reachability (and
   fire `scripts/plug_cycle.sh olliemax cycle` on trigger) and vice versa.
   This is already naturally enforced by `plug_cycle.sh`'s SAFETY RAIL 1
   (refuses off/cycle against the host it's running on) — the cross-box
   watchdog would just be a cron job on each box calling the *other* box's
   plug, which the existing tool already permits without modification.
2. **Independent failure mode (CLAUDE.md doctrine, "Alarms must not share a
   failure mode with what they watch").** The watchdog cron and the thing it
   watches must not share infrastructure — e.g. don't run the olliemax-watcher
   cron ON bigmac if bigmac's own uptime is what's in question elsewhere;
   consider running each box's watchdog from a third point if one becomes
   available, or at minimum keep it a plain cron entry (not tied to the
   trader process's own health).
3. **Conservative unresponsive-threshold, well past any legitimate slow
   restart/update** — a box that's merely slow to boot or mid-restart must
   never get power-cut. Needs to be long enough to rule out `trader_restart.sh`
   (which already waits up to 45x2s=90s for the listener) plus OS-level boot
   time plus margin — likely 10-15+ minutes unresponsive before even
   considering a trigger, not the 15-minute network-gear threshold reused
   verbatim (network gear reboots in seconds; a Mac Mini does not).
4. **DB-safety cannot be guaranteed at trigger time** — if a box is genuinely
   unresponsive, there's no way to ask it to checkpoint/quiesce first. The
   mitigations already shipped today reduce blast radius instead of
   preventing it outright: `scripts/trader_restart.sh` now checkpoints WAL in
   the zero-reader window on every *voluntary* restart (HM-WAL-ROOTCAUSE
   interim mitigation), and daily local (`scripts/db_snapshot.sh`) + off-host
   (`scripts/offhost_backup.sh`) snapshots exist if a forced cycle does
   corrupt something. An auto-cycle design should treat "accept the DB-crash
   risk of a forced cycle, mitigated by fresh backups" as the actual
   trade-off, not pretend a clean quiesce is achievable from outside.
5. **Manual override always available** — `plug_cycle.sh` already exists and
   works for a human to intervene immediately; this ticket only adds
   *unattended* recovery on top, doesn't replace the manual path.

No code written for this ticket — implementation is explicitly deferred
post-trip.
