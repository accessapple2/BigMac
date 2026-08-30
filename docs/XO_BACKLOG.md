# XO Backlog — USS TradeMinds
# Riker's Standing Work Queue
# Updated: 2026-07-06 (HM-BROKER-HISTORY-BACKFILL resolved — see start card below)

> **Older resume doc** (superseded by the start card below for current
> priority): `docs/QUEUE_AUDIT_2026-05-29.md`. THE-ALL-OUT-PLAN-2026-05-28 is
> CLOSED.

---
## 🟥 START CARD — read this first, cold, no other context needed (2026-07-05)

**Tonight's session, one paragraph:** a full roster audit (`HM-ROSTER-RATIONALIZE`)
found the dashboard's "78 active agents" counter reads a stale, unwired
column, corrected the real count, and reclassified two agents mid-audit.
That widened into `HM-EDGE-PROVENANCE` — tracing every trade's real venue —
which found the fleet's two ACTIVE-AUDITIONING seats (options-sosnoff,
qwen3-8b-flash) show 100% internal-simulation/unrouted activity, zero real
broker evidence, and that Alpaca's own paper account (the only ground truth)
shows +$1,064.08 lifetime, not the +$28.5k trader.db was read as claiming.
Both auditions are now SUSPENDED pending real broker routing (see
`HM-EDGE-PROVENANCE` entry below). Two features also shipped clean:
`scripts/eod_report.py` (2 PM daily ntfy heartbeat) and the incumbent-audition
+ signals-blind-spot wiring into `weekly_tuning_crew.py`. Everything committed
and pushed to `exec-pipeline` (`9bb56e6` through `b387027`).

**Update (2026-07-06 follow-on session):** `HM-BROKER-HISTORY-BACKFILL` is
now RESOLVED — reconciliation gap closed to $1.06 (true realized P&L
$1,138.31, reconstructed $1,139.37: +$907.37 real stock via direct FIFO
matching against Alpaca's own fills, +$232.00 real options). `HM-TUNING-
CREW-REPAIR` also shipped. See those entries below for full detail. Priority
order below is renumbered accordingly.

**Next session priority order — work top to bottom, do not reorder without
Admiral sign-off:**
1. **Per-agent attribution of the reconciled real stock/options P&L** —
   **UPDATE 2026-07-06:** mostly solved via timestamp+qty correlation
   against Alpaca's raw order history (see "Update (2026-07-06,
   per-agent attribution lead...)" note under `HM-BROKER-HISTORY-BACKFILL`
   below). `client_order_id` was a dead end (random UUID, never
   agent-stamped anywhere in this codebase). It's one continuous real
   position, not 15 independent trades — fragments matched to
   super-agent, ollie-auto, gemini-2.5-flash, neo-matrix, ollama-llama,
   and ollama-qwen3 by exact-second timestamp correlation. One clean gap
   remains: the closing 12.34sh/$224.39 liquidation (2026-05-20) has no
   local row under any agent at all. Still needs: a decision on whether to
   actually write these corrections back to `trades.execution_type`/
   `player_id`, or leave this as a documented read-only reconciliation.
2. **Route-to-broker proposal** — full ticket: `HM-ROUTE-TO-BROKER` entry
   below. Propose-first, do not implement without sign-off.
3. **Haircut test + evidence-tier re-grade** — full ticket:
   `HM-HAIRCUT-AND-EVIDENCE-TIERS` entry below. Its backfill dependency is
   now unblocked (see its own "Update 2026-07-06" note).
4. **Remaining XO-DEPARTURE-HARDENING phases** — filed (2026-07-06):
   `XO-DEPARTURE-HARDENING — Phase 1 remaining` / `Phase 2 remaining` /
   `Phase 3` entries below (all still `## 🔴` unstarted, propose-first, none
   built yet).
5. **Mistagging fix, proposed not built** — `paper_trader.py`'s Alpaca-fill
   writeback needs a path for `alpaca_options.py`'s forward calls AND
   apparently for some stock paths too (NVDA finding above suggests it's not
   only an options-side gap). See `HM-BROKER-HISTORY-BACKFILL`'s "Fix
   proposed, not applied" note below.

**Verify before acting on anything above:** this is a live production DB —
re-check current `ai_players`/`options_trades`/`trades` state before trusting
any specific number in this card; the card describes what was true as of
2026-07-06 ~22:20 MST, not a live view.

---
## 🟡 HM-TRADED-TODAY-UTC-BOUNDARY — filed 2026-08-29 (fleet-lifecycle / P0-emission session), QUEUED FOR NEXT SESSION, NOT YET FIXED

**Found by:** `tests/test_m5_allocator.py` failed tonight
(`test_traded_today_true_when_trade_exists`,
`test_run_m5_rebalance_skips_if_already_traded_today`) — not caused by
anything touched this session, just exposed by the wall-clock moment
the suite happened to run in.

**Root cause:** `engine/m5_allocator.py::_traded_today()` (line ~148)
compares `date.today()` — Python's local-system-timezone date (this box
is AZ, UTC-7, no DST) — against `date(executed_at)` on `trades` rows,
where `executed_at` defaults to SQLite's `CURRENT_TIMESTAMP` (UTC). AZ
local midnight is 07:00 UTC, so **every single day, from 00:00–07:00
UTC (16:00–23:00 the *previous* local evening in AZ), local date and UTC
date disagree by one calendar day.** During that ~7-hour window every
night, a trade that executed in the local evening gets a UTC timestamp
already stamped with *tomorrow's* UTC date — `_traded_today()` looks it
up under *today's* local date, finds nothing, and returns `False` even
though a trade already happened.

**This is a production-function bug, not a test bug.** Fix
`_traded_today()` itself; the failing tests are just the symptom.

**Failure-mode audit — what a wrong answer does (start here, may find
more):**
- **Only current caller:** `engine/m5_allocator.py::run_m5_rebalance()`
  (line ~205) — `if _traded_today(): _done_today=True; return` (skip)
  else fire `_execute_rebalance()`. The in-process `_done_today` flag is
  a *separate*, redundant guard that only protects against re-firing
  within the same running process — it resets on every `main.py`
  restart (which happens routinely per this session's other findings).
  **Concrete risk:** if a restart lands during the 00:00–07:00 UTC
  window on a day M5 already traded, `_traded_today()` false-negatives
  and `run_m5_rebalance()` fires a **duplicate same-day rebalance**.
  Direction confirmed: false-negative (says "no trade yet" when one
  happened), not false-positive — so the risk is duplicate execution,
  not a missed rebalance.
- Only one caller exists today, but audit for any NEW callers added
  since 2026-08-29 before assuming this scope is still accurate.

**Broader pattern, not just this one function:** a repo-wide grep the
night this was found shows `date.today()` used the same
naive-local-timezone way in `engine/alpaca_options.py`,
`engine/battle_station.py`, `engine/alpha_signals.py`, and likely
others — not confirmed whether any of those have a caller where the
UTC-boundary mismatch actually matters (most `date.today()` uses are for
labeling/lookback-window math, not a same-day dedup gate like this one),
but worth a scan while in this code, not just a single-function patch.

**Fix direction (as specified):**
1. Pin `_traded_today()` to a single canonical timezone — the codebase
   already has one: `engine.market_calendar.ET` /
   `engine.market_calendar._to_et()` (`America/New_York`, matches the
   actual trading-calendar clock everything else in this project uses).
   Compare `executed_at` and "today" both converted to ET, not local
   AZ system time vs. UTC-defaulted DB rows.
2. Audit every caller (currently just the one) for what a wrong answer
   near the boundary does before/after the fix — confirm the duplicate-
   execution risk above is fully closed, not just relocated to a
   different boundary.
3. Make the test deterministic: freeze the clock (e.g. `unittest.mock.
   patch` on `date.today`/`datetime.now`, or `freezegun` if already a
   dependency — check first) so the test suite can never again fail
   based on what hour it happens to run in. Add a dedicated boundary-
   case test (a trade executed at, say, 23:30 ET the previous day should
   read as "yesterday," not silently disappear or double-count).

---
## 🔵 HM-WORKTREE-DRIFT-TRIAGE — filed 2026-07-21 (Red Alert Yankee Echo Sierra / OllieTrades Lite session), LOW PRIORITY, POST-TRIP, NOT INVESTIGATED

During the Lite-build session, `git status` on `autonomous-trader` showed a
sizeable pre-existing dirty working tree, untouched by that session (which
only wrote this backlog entry + the Lite relay report):

**Modified, uncommitted:** `dashboard/app.py`, `data/agent_scoreboard.json`,
`docs/XO_BACKLOG.md` (this file — see the HM-WITNESS-AB-RETIRE-2026-07-18
and Counterfactual Report additions already sitting uncommitted at the tail
as of this entry), `drafts/daily_ledger.csv`, `engine/alert_channels.py`,
`scripts/backup_freshness_check.sh`, `scripts/db_snapshot.sh`,
`scripts/disk_space_alert.sh`, `scripts/offhost_backup.sh`,
`scripts/ollama_prewarm.sh`, `scripts/origin_healthcheck.sh`,
`scripts/recall_refresh_run.sh`.

**Untracked:** several `data/reports/relay/relay_2026-07-1[39]-*.md` files,
`backups/ai_players_pre_HM-GATE-RESTART-HOLD_20260706_165700.sql`,
`data/backups/hm_signals_v2_expire_*.ids`, `docs/handoffs/`,
`docs/model_watch/MODEL_WATCH_2026-07-1[29].md`,
`drafts/DAILY_REPORT_2026-07-{06..20}.md` (several dates),
`drafts/REALIZED_WEEKLY_2026-07-{10,17}.md`,
`scripts/com.ollietrades.hm-bridge-consensus-monday-check.system.plist`,
`scripts/hm_signals_v2_monday_check_verify.sh`,
`scripts/verify_repairs_20260713.py`.

**Not investigated tonight, by instruction** — the fleet owns bigmac through
tomorrow's close and the trip quiet-down, so no diffs were read and nothing
here was committed, reverted, or stashed. Flagging only that the tree is
this dirty, not diagnosing any individual file. **Next step (post-trip):**
review each modified file's diff and each untracked file to sort
work-in-progress worth committing from stale generated artifacts (daily
reports/relay files look like normal accretion; the code-file modifications
— `dashboard/app.py`, `engine/alert_channels.py`, the `scripts/*.sh`
health-check scripts — need an actual diff read before deciding keep/revert).

---
## 🔵 HM-ARCHER-PORTRAIT-REPEAT-FETCH — filed 2026-07-18 (HM-FULL-AUDIT-2026Q3 add-on), LOW PRIORITY, propose-first, NOT INVESTIGATED

Browser sweep observed `/classic` requesting `static/archer-portrait.png?v=4`
eighteen times on a single page load. Confirmed in source: `dashboard/static/
index.html` references `archer-portrait.png?v=4` **22 times**, of which 16
occurrences are inside JS template-string literals used to render each
Computer Chat message row (`'<div class="cc-row"><img src="/static/
archer-portrait.png?v=4" class="cc-avatar" alt="Archer">...'`, e.g. lines
~40120-41175) — one `<img>` tag gets minted per historical chat message
rendered on load, not one shared element. `dashboard/static/bridge-v2.html`
(the v2 tier) has zero references — this only affects the `/classic` (v1)
engineering console.

**Not yet determined:** whether these 18 are actually distinct network
fetches (a real caching/perf bug — identical URL should normally be served
from browser cache after the first fetch, so if it's really re-fetching over
the wire every time, either cache-control headers are wrong or something is
appending a cache-busting param per-render) or just 18 DOM `<img>` elements
correctly sharing one cached resource (in which case this is a non-issue,
cosmetic-in-appearance-only). No code was read/changed beyond the grep above.

**Next step if picked up:** check response headers on `/static/
archer-portrait.png` (cache-control/etag) and confirm via a real browser
network-tab capture whether repeat requests are cache hits or live fetches.
If real re-fetches: either dedupe the avatar into one shared DOM node reused
per message, or add appropriate caching headers server-side.

---
## 🟢 HM-NTFY-IPV6-NOROUTE-SWEEP — filed 2026-07-10, SHIPPED same night — all 31 unprotected senders fixed (4 + 27)

This box has no working IPv6 route to ntfy.sh (`HM-NTFY-IPV6-NOROUTE`,
`engine/alert_channels.py`, 2026-07-07) — confirmed still live tonight
via a direct forced-IPv6 socket test. **The failure is address-family-
ordering-dependent, not unconditional**: plain Python `requests`/
`urllib.request` succeed instantly right now under normal (non-forced)
resolution, because this resolver currently orders IPv4 first — but a
forced-IPv6 connect still fails immediately with `[Errno 65] No route to
host`. This matches the original fix's own note that it's "context-
dependent, interpreter/cron invocation specific" — a clean interactive
test cannot rule out a given cron invocation hitting it.

`engine/alert_channels.py::_send_ntfy()` has the fix (IPv4-force lock +
monkeypatch). Everything else in the codebase that sends ntfy pushes has
its OWN separate, hand-rolled implementation — a genuine, repeat
anti-pattern, not a one-off. Found while auditing "the remaining one-off
ntfy scripts" per Admiral request, following the earlier `long_range_
sensors.py`/`engine/ntfy.py` fixes from the same night.

**FIXED tonight (4 total, all with a real evidence trail before fixing):**
- `engine/long_range_sensors.py::send_ntfy()` — root cause of ~13,300
  `ntfy failed` lines in `trader_error.log` this week (though see note
  below — the exact historical split with `alert_channels.py`'s own
  pre-2026-07-07 contribution to that log isn't fully re-attributable,
  doesn't change that this file was genuinely unprotected until fixed).
- `engine/ntfy.py::_send()` — the default sender for the broader
  `ollietrades-crew`/`Ollie-Alert-55` channel (BUY/TP/stop/regime-change/
  spread-signal notifications) plus the Sniper Mode proving-ground topic.
- `watchdog.py::push_alert()` — **12 real failures in its own log**,
  2026-07-09. Self-contained fix (not importing `engine.alert_channels`
  — watchdog.py is deliberately dependency-free from the `engine/`
  package it monitors).
- `engine/riker_synthesis.py::_ntfy()` — **21 real failures in its own
  log**. Delegates to `_send_ntfy()` (this file already depends on
  `engine/` elsewhere).

**FIXED same night, second pass (27 more files, all catalogued in the
first pass) — same session, commits ready to push.** Two fix patterns,
picked per file's existing dependency posture:

- **Delegate to `engine.alert_channels._send_ntfy()`** (13 files — either
  already had an `engine/` dependency, or live inside `engine/` itself so
  a sibling-module import adds no new coupling): `agents/scotty/scotty.py`,
  `signal-center/server.py` (`_morpheus_log_action`'s FAILED-action
  alert), `engine/squeeze_scanner.py::_ntfy_priority_candidates()`,
  `engine/alpha_signals.py::_ntfy_send()`,
  `engine/morning_briefing.py::_push_admin_ntfy()`,
  `engine/dayblade_scanner.py::_push_ntfy()`,
  `engine/archer/alerts.py::run_alert_cycle()`,
  `engine/archer_morning_synthesis.py::send_briefing()`,
  `engine/universe_refresh.py::_ntfy()`,
  `engine/orcl_gex_alerts.py::send_ntfy()`,
  `engine/fred_data.py::_ntfy_carts_release()`,
  `engine/fleet_auditor.py::_push_ntfy()`,
  `engine/universe_scanner.py::_sp500_alert()`.
- **Self-contained local IPv4-force lock+monkeypatch** (14 files — zero
  existing `engine/` dependency, same reasoning as `watchdog.py`'s
  original fix): `swingdesk/shadow_autopilot.py::_ntfy_admin()`,
  `scripts/ghost_advisor.py::send_ntfy()`,
  `scripts/uhura_watch.py::send_ntfy()`,
  `scripts/q_market_open_ping.py::_ntfy()` (dead code — one-shot fired
  2026-06-09, fixed anyway for catalogue consistency),
  `scripts/q_dissent_watch.py::_ntfy()`,
  `scripts/import_schwab_csv.py::_ntfy()`,
  `scripts/model_watcher.py::ntfy_send()`,
  `scripts/model_sweep_v2.py::push_ntfy()`,
  `scripts/schwab_drawdown_alert.py::_ntfy()`,
  `scripts/fleet_heartbeat.py::_ntfy()`,
  `scripts/kimi_cut_watch.py::_ntfy()`,
  `scripts/iren_flip_watch.py::_ntfy()` (was never git-committed before
  this pass — added to git for the first time alongside its fix),
  `scripts/learning/check_pipeline.py::ping()`,
  `healthcheck.py::push_ntfy()`.

Test coverage: `tests/test_ntfy_ipv6_sweep_delegate_batch.py` (14 tests)
and `tests/test_ntfy_ipv6_sweep_standalone_batch.py` (16 tests, one
skipped — `model_sweep_v2` needs `vectorbt`, which only lives in
`.venv-backtest`, guarded via `pytest.importorskip`, consistent with the
vectorbt isolation rule; the fix itself is real, just untestable outside
that venv). Full suite: 1026 passed, 21 failed (the same pre-existing
bbkc/m5_allocator/quality_gate/etc. flakiness confirmed unrelated across
every fix session this week), 2 skipped. `py_compile` clean on all 27.

Live restart: `main.py`/trader (holds `squeeze_scanner`, `alpha_signals`,
`morning_briefing`, `dayblade_scanner`, `archer/alerts`, `fred_data`,
`fleet_auditor`, `universe_scanner` in memory via direct + `dashboard/
app.py` imports) restarted cleanly via `trader_restart.sh`, single PID
bound :8080, no import errors. `swingdesk/backend.py` (holds
`shadow_autopilot.py` in memory) restarted via `swingdesk_restart.sh`,
single PID bound :8889, HTTP 200 confirmed. `signal-center/server.py`
was not running at fix time — picks up the fix on next start
automatically. Everything else (scotty, the `scripts/*` one-shots,
`archer_morning_synthesis.py`, `universe_refresh.py`,
`orcl_gex_alerts.py`, `fred_data.py` CARTS job, `healthcheck.py`) is
cron/launchd-invoked fresh each run — no restart needed, self-heals on
next scheduled invocation.

**Lower priority, likely already safe:** ~17 `.sh` scripts (`dr_crusher.sh`,
`recall_refresh_run.sh`, `ollama_prewarm.sh`, `daily_watch_summary.sh`,
`morning_cd_instr.sh`, `morning_an2_observation.sh`,
`backtest_watcher.sh`, and others) all use `curl` directly, which has
been confirmed twice tonight (via direct live test) to handle this box's
IPv6 condition gracefully without the hang/failure Python's raw HTTP
clients exhibit.

**Closed.** Every Python ntfy sender catalogued in this ticket now routes
through the IPv4-forced path (either `engine.alert_channels._send_ntfy()`
directly, or a local copy of the same lock+monkeypatch technique). The
`.sh` scripts above remain out of scope (curl handles this box's IPv6
condition gracefully, confirmed via live test). Any *new* ntfy sender
added to this codebase going forward should use one of these two patterns
from the start rather than a third hand-rolled implementation.

---
## 🟢 HM-ARMED-DORMANT-SPREAD-STRATEGIES — filed 2026-07-10 (S6 P&L-reconciliation sweep), tb_active gate SHIPPED-DROPPED 2026-07-10, bull_spread_v1 self-healing in progress

`strategies/bull_call_spread_v1.py` and `strategies/bear_put_spread_v1.py`
are both imported and scheduled every tick in `main.py` (signal scan +
exit cycle, same as `bull_spread_v1`) but **have never fired a single
trade** — zero rows in `options_trades` for either `strategy_id`, ever.
`bull_spread_v1` itself (same `strategies/executor.py` framework) also
went dormant on 2026-05-14 (2 months silent) despite staying armed and
polled every cycle. Originally filed with "legitimately picky vs.
silently broken" left as an open question.

**UPDATE 2026-07-10 (closer look, per Admiral request): definitively
answered — all three are silently broken, not legitimately picky.**
Traced signal generation end-to-end for each. Two root causes found and
fixed today (`HM-OPTIONS-EXEC-CLOSE-EXEC-STATUS-NEVER-SET`,
`HM-EXECUTOR-STRUCTURE-WHITELIST-GAP`); a THIRD, more fundamental root
cause was found in this closer look and is NOT yet fixed:

1. **`bull_spread_v1` (SPY-only, `FIRST_TRADE_UNIVERSE=["SPY"]`,
   Admiral-approved 2026-04-22 scope limit — that part is intentional,
   not a bug):** `registry_signals` shows its last-ever generated signal
   was 2026-05-14 — 8 days BEFORE the `exec_status` bug even started
   (2026-05-22), so that bug alone doesn't explain the full silence.
   Traced the live per-ticker gate chain for SPY directly: `iv_history`
   for SPY has a **7-week recording gap, 2026-05-22 to 2026-07-10** —
   caused by the `exec_status` bug, since `_already_open('SPY')` was
   `True` the whole time and `bull_spread_v1.py`'s loop `continue`s on
   that check BEFORE ever calling `get_iv_rank(ticker, record=True)`.
   With SPY unblocked (today's fix), the scheduler's own 15-min cycles
   have started recording fresh IV again — but the first fresh reading
   landed as the new minimum of a stale/gapped comparison window, so
   `iv_rank` currently reads an artifactual `0.0`, routing to the debit
   `bull_call_spread` structure, which then fails its own $500-risk-cap/
   R:R quality gate (`[first_trade_width] no width fits risk_cap=$500
   for SPY bull_call_spread dte=21`, confirmed live). **Expected to
   self-heal over the next several days** as `iv_history` refills with
   regular readings and the comparison window's minimum naturally moves
   off today's single data point. No further code fix needed for this
   piece — it's an in-progress recovery from the already-fixed bug, not
   a new one.

2. **`bull_call_spread_v1` / `bear_put_spread_v1` (10-ticker universe:
   SPY, QQQ, IWM, AAPL, MSFT, NVDA, META, GOOGL, AMZN, TSLA):** `iv_history`
   for ALL 10 tickers shows the identical 7-week-plus gap (last row
   2026-05-22 for every ticker except SPY's fresh 2026-07-10 row from
   item 1). Traced why: both strategies only call `get_iv_rank(...)`
   AFTER their Tier-1 (`bmb_fired AND tb_active`) or Tier-2 (`buy_signal
   AND tb_active AND pc_ratio<0.7`) gate passes — and **both tiers
   require `tb_active=True` as a mandatory AND-condition.**
   `_get_tb_active()` queries `signal-center/signals.db`'s
   `trade_signals` table for `agent_name='tractor-beam'` rows within a
   lookback window. **That table has had ZERO `tractor-beam` rows since
   2026-04-14** (268 lifetime rows total, last one 2026-04-14 16:57:50)
   — while sibling signal sources in the same table (`shadow-bridge:*`,
   `long_range_sensors`) are actively firing as recently as today,
   confirming the signal-center pipeline itself is alive and this is
   specific to the `tractor-beam` source. **Root cause found in an
   existing code comment**, `engine/crew_scanner.py:3307-3312`
   (`HM-NAVIGATOR-SIGNAL-PATH-DEAD`, 2026-05-30): *"the old
   tractor_beam→save_signal emitter was dropped"* when agents were
   re-homed to a trade-only path on **2026-04-12** — a deliberate,
   documented architecture change. **Both `bull_call_spread_v1.py` and
   `bear_put_spread_v1.py` were created 2026-05-01 — nearly 3 weeks
   AFTER that emitter was already retired** — so `tb_active` has been
   `False` 100% of the time for the ENTIRE operational history of both
   strategies, for every one of their 10 tickers, blocking both tiers
   unconditionally. This is the true primary root cause of their
   dormancy — more fundamental than the whitelist-gap bug already fixed
   today (that fix only matters once a signal is ever generated, and
   none ever were, because of this). `engine/crew_scanner.py::
   chekov_rules()` reads the same dead table for its own TB-confidence
   boost (line 1624-1637) — a fourth confirmed consumer of the same
   stale dependency, not yet fixed either.

**UPDATE 2026-07-10 (Tractor Beam traced end-to-end, per Admiral
request): confirmed there is nothing live to repoint to.** Traced all
four files `CLAUDE.md`'s Fleet Roster doctrine names as "the live
Tractor Beam tiebreaker functionality":
- `engine/strategies.py::get_tb_direct_signals()` /
  `inject_tractor_beam_signals()` — both still query the same dead
  `agent_name='tractor-beam'` table, AND **neither function has a single
  caller anywhere in the codebase** — fully orphaned dead code, not just
  data-starved.
- `engine/crew_scanner.py::chekov_rules()` — reads the same dead table,
  IS called from a live scan path, but its only consumer agent
  (`navigator`, "Ensign Chekov") is `halt_mode='exit_only'` — not
  opening new positions — so this dependency has zero practical effect
  today regardless of the dead data.
- `engine/phaser_lock.py` — a completely unrelated system ("Highest-
  Conviction Setup Generator"). Computes its own conviction score from
  strategy-convergence/fleet-agreement data, writes to
  `data/phaser_lock_pick.json`. No functional connection to
  `agent_name='tractor-beam'` at all — its one "tractor" reference is a
  doctrine comment confirming it does NOT write to the archived
  `tractor.db`.
- `engine/reveille.py` — a pre-market briefing generator (ntfy/email
  delivery). Zero Tractor Beam references whatsoever.

**Conclusion: `CLAUDE.md`'s doctrine text describing this as "live... in-
repo today" is stale/inaccurate.** The old standalone Tractor Beam
process died 2026-04-17 (already documented in `CLAUDE.md`'s own sacred-
data section for `tractor.db`) and nothing has replaced it. Every
consumer of `agent_name='tractor-beam'` has been reading an empty well
for ~3 months. `CLAUDE.md`'s Fleet Roster section should be corrected in
a future pass to stop describing this as live.

**DECISION (Admiral, 2026-07-10): drop the `tb_active` gate.** No
rebuild attempted — building a genuine TB-signal replacement is a real
project of its own, out of scope here. **SHIPPED:** `tb_active` no
longer gates either Tier-1 or Tier-2 entry in `bull_call_spread_v1.py`
or `bear_put_spread_v1.py` — both tiers now fire on their other
conditions alone (BMB/BBD momentum, or Kirk/PED vote + P/C ratio).
`_get_tb_active()` is still called and the result still computed (cheap,
harmless single query) so this is a one-line revert if a real TB
replacement ever exists. Top-of-file docstrings and the `description`
strings (surfaced in the `strategies` DB table) updated to match. NOT
touched: `chekov_rules()`'s TB-confidence boost — moot today
(`navigator` is `exit_only`), left alone rather than bundled into this
fix; `engine/strategies.py`'s two orphaned TB functions — genuinely dead
code, not wired anywhere, left as-is (no live behavior to change).

---
## 🟡 HM-STRATEGIES-EXECUTOR-STATUS-NEVER-SET — filed 2026-07-10 (S6 P&L-reconciliation sweep, part 3)

`strategies/executor.py::_increment_closed()` — the only live close path
for `bull_spread_v1`/`bull_call_spread_v1`/`bear_put_spread_v1` (see
`HM-ARMED-DORMANT-SPREAD-STRATEGIES` above) and used nowhere else — sets
`exec_status='closed'` on full close but **never sets `status='closed'`,
and never writes a `pnl`**. Every P&L/win-rate query in the system
(including `options_book_summary()`, `strategy_pnl()`, etc.) filters on
`status='closed'`. Confirmed via the 21 legacy `bull_spread_v1` rows
currently `status='closed'` in `options_trades`: **100% of them got that
way from one-time manual reconciliation scripts** (`exit_reason` values
`HM-OPTIONS-TRADES-ZOMBIE-CLEANUP-reconcile-2026-05-18` and
`HM-AE-Option-B-reconcile-2026-05-05`), not from `_increment_closed()`
itself — the live code path has *never once* correctly closed a position
in this system's history. **SHIPPED (commit pending, this session):**
`_increment_closed()` now also sets `status='closed'` + `exit_date` on
full close — unambiguous, no broker-fill data needed, fixes the
"permanently invisible to reporting" failure mode.

**NOT shipped, needs a decision:** `pnl`/`exit_credit_debit` computation
on close. `close_vertical_spread()` (`engine/alpaca_options.py`) submits a
market order and returns immediately — no synchronous fill price. Getting
real P&L requires polling `client.get_order_by_id().filled_avg_price` for
the MLEG combo order (same mechanism `swingdesk/spread_executor.py::
poll_fill()` already uses for single-leg fills).

**UPDATE 2026-07-10 (sign convention resolved, not yet live-verified):**
the `alpaca-py` SDK's own docstring (`.venv/lib/python3.14/site-packages/
alpaca/trading/requests.py:436-438`, `LimitOrderRequest`/
`StopLimitOrderRequest.limit_price`) states verbatim: *"For the mleg
order class, this is specified such that a positive value indicates a
debit (representing a cost or payment to be made) while a negative value
signifies a credit (reflecting an amount to be received)."* This is
Alpaca's own documented convention (generated from their API schema),
not community guesswork, and independently matches
`swingdesk/spread_executor.py::build_mleg_order()`'s own docstring
("Debit → positive limit_price"), which arrived at the same convention
before this was found. `filled_avg_price` isn't separately documented
with the same caveat, but it's the same priced field concept as
`limit_price` (what you asked for vs. what you got) — a broker would not
flip sign convention between the two. This resolves the formula:
`close_cost = filled_avg_price * qty * 100`, `pnl = entry_credit_debit -
close_cost` — identical to the formula `engine/options_exec.py::
close_options_trade()` already uses, just with the previously-unverified
input now resolved.

**Residual gap, still real:** this is documented convention, not an
empirically-observed real fill — no live MLEG close has been captured
end-to-end to confirm it. Materially stronger evidence than before (an
official, schema-derived doc citation vs. nothing), but implementing the
pnl write should still ideally be checked against one real fill (or
explicitly accepted on doc-confidence alone) before shipping — Admiral's
decision, 2026-07-10: **hold.** Do not implement pnl computation on
doc-confidence alone. Wait until one of the three armed strategies (see
`HM-ARMED-DORMANT-SPREAD-STRATEGIES`) or a deliberate manual SwingDesk
close actually produces a real MLEG close, observe the real
`filled_avg_price` sign, confirm it matches this doc citation, THEN
implement `close_cost = filled_avg_price * qty * 100`, `pnl =
entry_credit_debit - close_cost` in `strategies/executor.py::
_increment_closed()` and `swingdesk/spread_executor.py::
_close_original_position()`.

---
## 🟡 HM-OPTIONS-EXEC-CLOSE-EXEC-STATUS-NEVER-SET — filed 2026-07-10 (S6 MLEG sweep), SHIPPED

`engine/options_exec.py::close_options_trade()` — the close path for
single-leg and MLEG-adjacent trades outside the `strategies/executor.py`
framework — updated `status`, `pnl`, `exit_date`, `exit_credit_debit`,
`pnl_pct`, `exit_reason` on close, but **never set `exec_status`**. Three
call sites gate on `exec_status='open'` ALONE (not `status`):
`strategies/exit_manager.py::fetch_open_strategy_positions()`, and the
self/cross-strategy dedup checks in `strategies/bull_spread_v1.py::
_already_open()` and `strategies/bull_call_spread_v1.py::
_bull_spread_v1_has_position()`. **Confirmed live, currently manifesting:**
`options_trades` id 28 (`bull_spread_v1`, SPY, closed 2026-05-22,
`exit_reason='expired_otm'`) stayed `exec_status='open'` for 7+ weeks,
silently blocking every new SPY entry for BOTH `bull_spread_v1` and
`bull_call_spread_v1` (cross-strategy check) the entire time. 19 total
rows found with `status='closed' AND exec_status='open'` (1 bull_spread_v1
+ 18 `options-sosnoff` CSP closes — the CSP ones don't cause a live
blocking bug since `paper_trader.py`'s CSP gating never checks
`exec_status`, just a data inconsistency).

**SHIPPED:** `close_options_trade()` now also sets `exec_status='closed'`
in the same UPDATE. **Also hand-corrected all 19 existing affected rows**
(`UPDATE options_trades SET exec_status='closed' WHERE status='closed'
AND exec_status='open'`) — verified `_already_open('SPY')` now returns
`False` post-fix. Tests in
`tests/test_options_exec_status_and_bear_spread_whitelist.py`.

---
## 🟡 HM-EXECUTOR-STRUCTURE-WHITELIST-GAP — filed 2026-07-10 (S6 MLEG sweep), SHIPPED

`strategies/executor.py::_execute_live()`'s structure whitelist was
hardcoded to `("bull_call_spread", "bull_put_spread")` — but
`strategies/bear_put_spread_v1.py` (live-scheduled every 15 min in
`main.py`) emits `"bear_put_spread"`/`"bear_call_spread"`, neither on the
list. **Every signal this strategy has ever generated was rejected
before submission**, since it was wired up — confirmed via
`options_trades`: zero rows for `strategy_id='bear_put_spread_v1'`, ever.
Verified this was purely a missing tuple entry, not a missing feature:
`engine/alpaca_options.py::submit_vertical_spread()`'s own docstring
already documents bear-put-spread support by name ("Bear put spread:
buy_symbol = higher strike put, sell_symbol = lower strike put"), and
`bear_put_spread_v1.py`'s payload shape (`long_leg`/`short_leg`/
`net_debit`/`net_credit`) is already identical to what the bull-spread
strategies this whitelist was written for use.

**SHIPPED:** whitelist expanded to include `"bear_put_spread"` and
`"bear_call_spread"`. No existing data to correct (zero rows ever). Tests
in `tests/test_options_exec_status_and_bear_spread_whitelist.py`.

---
## 🟡 HM-DRAWDOWN-BLIND-TO-OPTIONS-PNL — filed 2026-07-10 (S6 MLEG sweep), closer look done 2026-07-10, needs Admiral decision on the fix design, ZERO current impact

`engine/risk_manager.py::check_drawdown()` (called every scan cycle from
`engine/ai_brain.py:1065`, gates whether a player's new-trade scanning
proceeds — soft/transient per-cycle gate, not the persisted `halt_mode`
column) reads `MAX(total_value)`/latest `total_value` from
`portfolio_history`. Traced how that table is written
(`engine/paper_trader.py::record_portfolio_snapshot()` →
`get_portfolio()`, `ai_players.cash` + stock `positions` table only) and
confirmed it is **structurally blind to ALL options/CSP P&L, always, by
architecture** — CSP realized P&L is booked to `options_books.<book_tag>.
current_cash`, never to `ai_players.cash` (documented in
`engine/paper_trader.py` lines 2594-2609, `HM-CLEANUP-TRIO-2026-07-04`/
`HM-TROI-WHEEL-V2`). NOT the CSP-era-pricing-filter bug class from
earlier in this session's sweep — a separate, always-true architectural
gap.

**UPDATE 2026-07-10 (closer look, per Admiral request) — blast radius
and current impact:**

- `check_drawdown()` only ever runs for `ai_players` roster members
  (`ai_brain.py::_run_player()` iterates that table). Checked every
  options/CSP/spread-trading `agent_id` that appears in `options_trades`
  against `ai_players` directly: **`options-sosnoff` is the only one
  that's an actual `ai_players` row.** `shadow-qwen35-csp` (ghost-book
  CSP audition, `engine/shadow_csp.py`), `strategy:bull_spread_v1`
  (`strategies/executor.py`'s own scheduler), `swingdesk-manual`
  (SwingDesk manual API), and `test-door1-regression` are **all
  structurally outside this gate** — no `ai_players` row for
  `check_drawdown(player_id)` to even look up. They may have other,
  independent risk controls (not investigated here), but this specific
  20%-drawdown halt does not and cannot apply to them.
- **Current practical impact: zero.** `options-sosnoff` has exactly $0
  real-quotes-era CSP P&L (no real-quotes-era closes at all yet) — there
  is nothing for this gap to be masking today. (`shadow-qwen35-csp`,
  outside the gate anyway, is actually +$2,511.39 real, not a loss.) The
  gap is real and will matter the moment `options-sosnoff` — or any
  future CSP-trading `ai_players` roster addition — posts an actual real
  loss, but it is not hiding anything right now.

**UPDATE 2026-07-10 — fix design, and a correction to the original
filing:** the "double-counting risk against the CSP notional cap" noted
originally does not hold up. `get_csp_exposure()` (the notional/margin
cap) reads the shared `options_books.current_cash` pool for open-CSP
sizing; a fixed `check_drawdown()` would use `get_portfolio_with_pnl()`'s
`total_value_restated` (`= portfolio["cash"] + positions_value +
csp_pnl_real_quotes`, agent-scoped via `options_trades.agent_id`) —
fully independent computation paths, no overlap. Verified `csp_pnl_
real_quotes` is purely additive against `ai_players.cash` (per the
`HM-W1F4` decoupling doctrine already in the code), so folding it into a
drawdown calc would NOT double-count.

**The real obstacle is missing peak-tracking infrastructure**, not
double-counting. `check_drawdown()` needs a historical PEAK to compare
against. None of this codebase's three equity-tracking surfaces
currently persist a restated one: `portfolio_history` (SQL, raw
cash+stock only — what `check_drawdown` reads today), the JSON equity
curve file (`save_equity_snapshot()`, also raw — used for charting, a
totally separate mechanism, not read by `check_drawdown` either), or
`get_portfolio_with_pnl()` itself (has the restated figure, but live/
on-demand only, no history). Comparing a *restated* current value
against the *existing raw* peak would be apples-to-oranges and could
false-trigger a halt purely from the two figures being computed on
different bases, not from any real loss. A correct fix needs a new
persisted restated-equity history (new table or column), not a one-line
swap — genuinely a small design/build project, not folded into
mechanical fixes.

Two agent-sourced claims that assumed `portfolio_history` WAS polluted
by the synthetic-CSP-premium bug (an earlier sweep round flagged
`check_drawdown()` and `engine/rallies_intel.py::compare_crew_vs_
rallies()` on that premise) were checked directly against the live DB
and **do not hold** — for `options-sosnoff`, `portfolio_history`'s own
peak equals its own current value ($12,880.20 = $12,880.20, zero
synthetic inflation ever present in this specific table) since it was
never fed by CSP P&L in the first place, refuting the "true restated
equity is -$16,988" arithmetic those findings were based on. Not
shipping anything against `rallies_intel.py` — it's reading the same
architecturally-CSP-blind table, not a bug.

---
## 🟢 HM-SIGNALS-V2-FIFO-STARVATION — filed 2026-07-06 (HM-MONDAY-OPEN-WATCH), RESOLVED 2026-07-12

`engine/events_bus_consumer.py::consume_pending_signals()` dequeues with
`SELECT ... FROM signals_v2 WHERE status='pending' ORDER BY created_at ASC
LIMIT ?` — **no date filter, no priority lane, strict FIFO across all of
history.** Found a **14,936-row pending backlog dating back to 2026-06-24**
(12 days). As of 2026-07-06 ~06:33 AZ (market open), **14,842 of those rows
sit ahead of today's ~118 fresh signals** in queue order. Observed
throughput since Sunday's `main.py` restart: ~1,940 signals processed in
~12 hours (~161/hr) — at that rate today's signals won't be dequeued for
**3-4 days**.

**Why this matters specifically today:** today is the first live session
under the swing-surge staleness-budget fix (signals that used to expire at
30s now get a 3600s window before being marked stale). But the staleness
check (`stale_after` vs `now`) only evaluates once a row is actually
dequeued — a row buried behind 14,842 older rows never gets checked while
it's still within *either* budget. **The fix's effect on today's numbers
cannot be honestly measured until this is addressed** — whatever
executed/stale counts land today most likely reflect the 12-day-old backlog
draining, not this week's signals. The backlog itself predates today's
config change (starts 06-24), so it isn't caused by the fix — it just means
the fix can't be assessed while it's live.

**Secondary, lower-priority finding from the same investigation:** `navigator`
(`halt_mode='exit_only'`) is the source of 82 of the 118 fresh signals today
(vs. 6 each for ollama-qwen3/ollama-plutus) — `LONG` signals across dozens of
symbols it holds no position in, not exit-management of its actual 5 open
positions (JTAI, LRCX, MNTS, ON, QCOM). Verified this is **not a live risk**:
`events_bus_consumer` dispatches through the same shared `paper_trader.buy()`
that has the halt gate (`HALTED: navigator (exit_only)` confirmed firing
repeatedly in `trader.log`) — every one of these would be rejected if
dequeued. It's wasted compute/queue bloat (exit_only agents don't need a
full scan when they hold nothing to manage in most of the names they're
scanning), not an execution-safety gap.

**Propose-first, nothing built:** two directions worth considering —
(a) a priority lane / age cap so recent signals dequeue ahead of historical
backlog, or (b) a one-time bulk-archive of the pre-existing 06-24-and-older
backlog to `status='expired'` so the queue starts clean. Needs Admiral
sign-off before either.

**Fix proposal, 2026-07-06 post-incident cleanup (proposal only, no changes
without approval).** Re-ran the numbers with a halt_mode join, and the
picture is more clear-cut than the original filing suggested:

```
source              pending   halt_mode
navigator             13,063   exit_only
ollama-plutus            783   active
ollie-auto               599   exit_only
capitol-trades           282   active
ollama-qwen3              130   active
neo-matrix                 90   active
ollama-coder               85   full
qwen3-8b-sonnet            18   full
deepseek-7b-grok4          10   full
qwen36-audition-test        1   unknown
```
Total pending: **15,061**. Of that, **13,775 (91.5%) come from sources whose
`halt_mode` is `exit_only` or `full`** — meaning every one of those rows is
*structurally incapable of ever executing*: `paper_trader.buy()`'s halt gate
rejects any non-`active` source unconditionally (confirmed live in
`trader.log`, `HALTED: navigator (exit_only)` firing on every dequeue
attempt). These aren't "old but still viable" signals sitting in a slow
queue — they were dead on arrival the moment they were written, because
their source was already halted when they were created. `navigator` alone
(halted 2026-06-19, backlog starts 2026-06-24 — every single one of its
13,063 rows was generated *after* the halt) accounts for 86.7% of the total.
Only **1,285 pending rows (8.5%)** come from currently-`active` sources and
could theoretically still execute if reached — those are the only ones a
"we might lose something real" argument applies to. Today's fresh signals:
293 of the 15,061 pending (up from ~118 at market open — the backlog is
growing faster than it drains).

**Ranked recommendation:**
1. **Expire pending rows from non-`active` sources** (`status='expired'`,
   archive-not-delete, same pattern already used by `_expire_signal()` in
   this file for the re-entry guard) — clears 13,775 rows (91.5% of the
   backlog) with **zero loss of anything that could have executed**, since
   none of them could have. Lowest risk, highest impact, and it's a general
   rule (any halted source, not just navigator) so it doesn't need
   re-running by hand if another agent gets halted later.
2. **Priority lane / age-ordering for the remaining ~1,285 active-source
   rows** — these genuinely could execute, so a FIFO-vs-recency tradeoff is
   a real design choice here, not a freebie like #1. Options: reorder by
   `created_at DESC` within active sources (newest-first, matches the
   swing-surge intent), or a hybrid (drain a few oldest per tick alongside
   newest, so nothing starves completely).
3. **One-time blanket archive of everything before today** — simplest to
   implement, but throws away the ~1,285 active-source rows too, which #1+#2
   wouldn't need to. Only worth it if the Admiral would rather not carry any
   pre-existing backlog forward regardless of source.

Recommend **1 alone first** (it's nearly-zero-risk and clears the vast
majority on its own), then decide on 2 vs 3 for the small remainder once
it's clear how much daily "fresh" volume the queue actually needs to keep
up with. Nothing applied — needs sign-off before any UPDATE/write.

**Note (2026-07-06 post-incident cleanup):** confirmed the bridge login
prompt reappearing after today's `main.py` restart is expected, not a
regression. `dashboard/app.py:890` — `_active_sessions: dict[str, float] = {}`
— is a plain in-process Python dict (username → login timestamp), zero
persistence backing (no file/DB/redis anywhere in its read/write path,
checked directly). It lives entirely in main.py's process memory, so any
restart empties it, and every previously-authenticated browser session
gets bounced to `/login` on its next request. This is a real, known
limitation (session state doesn't survive a restart) rather than a bug —
worth knowing for any future restart (planned or incident-driven): expect
to re-authenticate afterward, that's not itself a sign something broke.

**RETRACTED 2026-07-06 (AFTER-CLOSE-WORK-ORDER P2.7 investigation) — the
above diagnosis was wrong.** Traced the actual auth gate before building the
requested "persist `_active_sessions`" fix, since that's the premise this
note rests on. `_active_sessions` is written in three places (both login
paths + `AuthMiddleware`'s per-request refresh) and read in exactly two
(`/api/active-users` — the "who's online" display — and the 30-min
inactivity sweep at `dashboard/app.py:1730-1732`). **Neither of those is the
auth gate.** The actual gate (`AuthMiddleware.dispatch`, `dashboard/app.py`
~line 1300) calls `_get_session_data(request)`, which only verifies the
signed `trademinds_session` cookie (`itsdangerous.URLSafeTimedSerializer`,
24h `max_age`, keyed by the stable `TRADEMINDS_SECRET` env var that fatal-
exits the process if unset rather than auto-regenerating) — it never touches
`_active_sessions`. Verified empirically: minted a token in one Python
process, decoded it successfully in a completely separate fresh process
(simulating a restart — no shared memory, `_active_sessions` empty in the
new process) — decode succeeded. **A previously-authenticated browser
session already survives a `main.py` restart** as long as the cookie hasn't
hit its own 24h expiry and `.env`'s `TRADEMINDS_SECRET` doesn't change.
`_active_sessions` resetting to `{}` on restart only blanks the "who's
online" count/list for up to 30 minutes until each active user's next
request repopulates it — cosmetic, not an auth-gate regression.

**Not built:** the requested `_active_sessions` persistence (SQLite/signed-
cookie) would have been solving a problem that doesn't exist at the layer it
was aimed at. If a real forced-relogin-after-restart is later reproduced
with concrete repro steps (specific browser, specific timing), the actual
cause is somewhere else — cookie natural 24h expiry lining up with the
restart by coincidence, a CF-tunnel/proxy session concern, or a browser-side
cookie-clear — and deserves its own targeted look rather than assuming
`_active_sessions` again.

**RESOLVED 2026-07-12 (XO directive, HM-SIGNALS-V2-FIFO-STARVATION
verification pass).** Re-checked this ticket cold against live
`data/trader.db` before touching anything, per doctrine (backlog/memory go
stale — confirm current state first, not just what the doc says). Finding:
**both original recommendations, plus the residual follow-up flagged
2026-07-09, were already applied** in two prior sessions — this entry was
just never marked closed.

Timeline, reconstructed from git + `data/backups/`:
- **2026-07-06, commit `aa55f1d`:** recommendation #1 (expire pending rows
  from halted/non-active sources, `scripts/hm_signals_v2_expire_halted_backlog.py`)
  + recommendation #2 (`engine/events_bus_consumer.py::consume_pending_signals()`
  switched to `ORDER BY created_at DESC`, newest-first) shipped together.
  Snapshot: `data/backups/hm_signals_v2_expire_20260706_083934.ids`.
- **2026-07-09, commit `b3e9ade`:** the residual gap this ticket's own 07-09
  note flagged (pre-reorder active-source rows permanently outranked under
  newest-first, keeping `hm_ops_sentinel.py`'s oldest-pending-age check
  perpetually WARNING) was closed via a new one-time script,
  `scripts/hm_signals_v2_expire_pre_reorder_active_backlog.py` — archived
  630 active-source rows dated before the 07-06 reorder (`status='expired'`,
  archive-not-delete). Snapshot:
  `data/backups/hm_signals_v2_expire_pre_reorder_20260709_204342.ids`.
  Verified in that commit: 841 pending → 211 pending.

**Live-verified 2026-07-12, no changes made — nothing left to fix:**
```
pending total:                              140
oldest pending age:                         45.7h   (< 48h WARNING threshold)
active-source rows predating the 07-06 reorder: 0
pending by source/halt_mode: ollama-plutus (active) 83, ollama-qwen3 (active) 57
```
`hm_ops_sentinel.py`'s `check_signals_v2_queue()` would not fire on either
sub-condition (pending≫3000, oldest-age≫48h) against these numbers. Closing
this ticket. If the queue climbs again (e.g. a currently-active source gets
halted later, or newest-first genuinely starves a batch of same-day signals
during a high-volume session), the same archive-not-delete pattern in the
two scripts above is the template; `hm_ops_sentinel.py`'s signals_v2 check
will surface it. See also `HM-SENTINEL-ACK` (same date) — an ack/ceiling
mechanism was added to `hm_ops_sentinel.py` so a known, understood recurrence
of this condition can be acknowledged without going permanently silent.

---
## 🟢 HM-SIGNALS-V2-STARVATION-RECURRENCE — filed 2026-07-12 (XO directive, post-fix watch item), RESOLVED 2026-08-29

Structural follow-on risk identified while verifying `HM-SIGNALS-V2-FIFO-STARVATION` closed (above): the newest-first ordering (`ORDER BY created_at DESC`, live since 07-06 commit `aa55f1d`) combined with the consumer's fixed drain cap (`max_batch=10` per 1-min tick, `engine/events_bus_consumer.py::consume_pending_signals()`) can permanently outrank *any* batch of pending rows that isn't the newest — not just the specific pre-07-06 and pre-07-09 backlogs already cleaned up. This same mechanism produced both prior one-time cleanups; nothing structural stops it from recurring with a third batch.

**Concretely observed today (2026-07-12, Sunday):** the 140 currently-pending rows (83 `ollama-plutus` + 57 `ollama-qwen3`, all dated 2026-07-10 evening/2026-07-11 early AM, all from currently-`active` sources) sat completely untouched all weekend — market closed, the consumer is NYSE-hours-only (`main.py::run_events_bus_consumer()` no-ops when `is_us_market_open()` is false). Whether Monday's fresh signal volume from these same two sources leaves enough drain-cap headroom to reach back to these 140 rows, or whether they get permanently outranked by same-day signals the same way the pre-07-06 batch was, is **unverified** — genuinely depends on Monday's volume, not derivable from today's data alone.

**Two candidate fixes for later evaluation — NOT implemented, no code changes made here:**
- **(a) TTL / age-cap auto-expiry:** expire pending rows older than N *market*-hours (using `engine.market_calendar.market_hours_elapsed`, added under `HM-SENTINEL-ACK` below) via the same archive-not-delete pattern as the two prior one-time scripts, but as a standing rule instead of a manual cleanup. Tradeoff: needs an Admiral-approved N (how old is "genuinely dead" vs. "still worth a shot") and turns a human archive decision into an automatic one.
- **(b) Hybrid ordering:** reserve a small slice of the consumer's per-tick drain cap (e.g. 1-2 of the 10) for the single oldest pending row regardless of source recency, so nothing can be *permanently* outranked even under sustained high newest-source volume — just slower. Tradeoff: touches the live consumer's dequeue logic directly (higher blast radius than (a), since it changes order-submission-adjacent behavior, not just an archive script).

**Monday verification task (queued, not yet run):** after 2026-07-13 market open, re-check whether these 140 rows are draining or being newest-first-outranked. That result decides whether this ticket becomes active work or closes as "didn't recur this time." Needs Admiral sign-off before either candidate fix is built.

**RESOLVED 2026-08-29 (Admiral directive, "OPS TRIAGE" item 1) — the
Monday verification task above never ran** (the 2026-07-22 fleet
stand-down and its aftermath intervened first). The predicted recurrence
happened at much larger scale before anyone checked: `hm_ops_sentinel.py`
(itself down since the same 07-22 stand-down, restored 2026-08-29 in the
HM-GEX-COLLECTOR-DEAD pass) surfaced pending=11,663 (>3000 cap), oldest
1199h on its first real run.

**Live breakdown at resolution time:**
```
source          halt_mode   pending   already past own stale_after
ollama-qwen3    active      4,039     4,013  (99.4%)
ollama-plutus   active      3,955     2,854  (72.2%)
mlx-qwen3       full        3,666     3,648  (99.5%)
ollie-auto      exit_only       3          0
```
Different shape than the 2026-07-06 incident (91.5% dead-letter via
halt_mode alone) — this time 68.5% of the backlog is from *active*
sources. But nearly all of it (6,867 of 7,994 active-source rows) is
independently dead via its own `stale_after` marker: the consumer already
refuses to execute a row past its own staleness budget regardless of
source status, so archiving loses nothing that could ever have run.

**Both candidate fixes applied, per Admiral direction "no more proposed":**
- **One-time cleanup** (`scripts/hm_signals_v2_expire_dead_letter_20260829.py`,
  archive-not-delete, same convention as the 07-06/07-09 scripts, snapshot
  at `data/backups/hm_signals_v2_expire_dead_letter_20260829_133052.ids`):
  archived the union of (non-active-source rows) ∪ (rows already past their
  own `stale_after`) = **10,536 rows**. Left the remaining **1,127** rows
  (1,101 `ollama-plutus` + 26 `ollama-qwen3`) untouched — all `NULL
  stale_after`, which `engine/events_bus_consumer.py`'s own comment
  documents as "never-stale" by explicit design, not missing data. Archiving
  them just for being old would have contradicted that design rather than
  fixed a bug.
- **Candidate (b), hybrid ordering** (`engine/events_bus_consumer.py::consume_pending_signals()`):
  every tick now reserves 2 of the 10 drain-cap slots for the single oldest
  pending row(s) regardless of source recency, before filling the rest
  newest-first as before. Guarantees forward progress on the tail of the
  queue under any sustained newer-source volume — the exact mechanism that
  produced this incident three times can't recur structurally, only slow
  the drain. Candidate (a) (TTL auto-expiry) was not built on top of this —
  (b) already guarantees eventual drainage for genuinely-live signals, and
  TTL would have fought the `NULL stale_after` = never-stale design
  decision from the previous paragraph.
- 4 new tests (`tests/test_events_bus_consumer_starvation_fix.py`) pin the
  hybrid-ordering behavior directly: an ancient row survives 20+ newer
  arrivals in the same tick, small queues aren't double-counted, the newest
  rows still dominate the non-reserved slots, empty queue doesn't crash.

**Not done:** the 1,127 remaining `NULL stale_after` rows (oldest:
2026-07-10) will drain naturally over the next few days under the new
2-per-tick oldest-reserve (390 market-minute ticks/day × 2 = up to 780
oldest-slot opportunities/day, comfortably clearing 1,127 within 1-2
trading days) — not force-cleared today, left to the mechanism this ticket
just shipped rather than a second manual sweep.

### 🟡 HM-SIGNALS-V2-FIFO-STARVATION — post-fix review, 2026-08-29 evening (scoping only, no code changes)

Requested after tonight's ops-sentinel triage (`relay_2026-08-29_ops-sentinel-triage.md`)
surfaced the still-1,127-pending queue and, on my first pass, was
misreported as evidence the structural fix above was "never built." **That
was wrong** — commit `61126bb` (today, 13:33, six hours before I checked)
already built, tested, and shipped it; the section immediately above this
one documents it as RESOLVED. Correcting the record here rather than
quietly fixing my own report. This entry is the requested review of that
already-shipped design against three specific questions, plus a same-night
interim-mitigation decision — not a new design proposal.

**1. Root cause of the recurrence — throughput or ordering?**
Ordering/deprioritization, confirmed three times over (2026-07-06, -09,
-29): pure `ORDER BY created_at DESC` can permanently outrank any batch
that isn't the newest arrival, regardless of aggregate daily drain
capacity — a queue with plenty of throughput headroom can still starve
its tail forever under sustained fresh-signal volume. It was never a
burst-arrival-vs-throughput problem; `max_batch=10`/minute during market
hours is not the bottleneck (see the ~780/day reserved-slot capacity
above against a 1,127-row queue).

**2. Design review + fairness/attribution concern.**
The shipped fix (`_OLDEST_RESERVE = min(2, max_batch)`,
`engine/events_bus_consumer.py:89-107`) reserves 2 of 10 slots per tick
for the single absolute-oldest pending row(s), `ORDER BY created_at ASC`,
with no source weighting, before filling the rest newest-first as before.
This guarantees forward progress (confirmed by the pinned test
`test_oldest_row_reached_despite_heavy_newer_volume`) without reverting to
strict FIFO (confirmed by `test_newest_rows_still_prioritized_for_remaining_slots`).

There's no "Honest Harness" doctrine by that name anywhere in this repo
(checked) — the closest real, load-bearing analog is the Backtest Rule
("always run ALL agents... never cite in-sample without matching OOS") and
`ollietrades_signal.py`'s own "verdict is computed, never prescribed"
posture — both about not letting selection/reporting mechanics quietly
bias what a number appears to say about an agent's skill. Read that way,
there is a real, currently-unaddressed version of that concern here: the
oldest-reserve selects by absolute age only, with **no per-source
fairness**. Right now 1,101 of the 1,127 pending rows (97.7%) belong to
one agent (`ollama-plutus`/McCoy), so nearly every reserved-oldest slot
for the next 1-2 trading days will be a McCoy signal generated **2026-07-10
through 07-11** — up to 7 weeks stale relative to current market
conditions and McCoy's current strategy state — executing under today's
date with no distinguishing tag. If those trades land in the same win-rate
/ P&L rollups used to compare agents or judge McCoy's *current* behavior
(`engine/agent_ratings.py`, the fleet report card, `/api/signals/compare`),
they will misrepresent what McCoy's live signal quality looks like right
now, purely as a side effect of queue mechanics, not trading skill. This
was true the moment the fix shipped and isn't new tonight, but no one
has flagged it before this doc. **Not fixed here** (out of scope: "no code
changes" was explicit) — options for the follow-up session below: (a) tag
signals executed from the reserved-oldest slot (e.g. a
`dispatch_reason='oldest_reserve'` column or reuse of existing metadata)
so performance rollups can exclude or footnote them, or (b) accept the
distortion as bounded and small enough not to matter (1-2 days of
anomalous fills against months of live trading) and just document it.
No recommendation forced here, per the same "report the numbers, don't
force a verdict" convention the comparison endpoint itself uses.

**3. Test plan + rollback.**
Already covered by the shipped commit, not something to newly design:
`tests/test_events_bus_consumer_starvation_fix.py` (4 tests, reran tonight,
all pass) pins oldest-row-reached-despite-volume, no-double-count on small
queues, newest-still-dominates-remaining-slots, and empty-queue-no-crash.
Rollback is a plain `git revert 61126bb` — the change is isolated to one
function's row-selection SQL plus the one-time archive script (already
executed, its own effect is separately reversible via the
`data/backups/hm_signals_v2_expire_dead_letter_20260829_133052.ids`
snapshot per the archive-not-delete convention). No schema changes, no
migration to unwind.

**Interim mitigation — evaluated, NOT run, skipping deliberately:**
considered manually invoking `consume_pending_signals()` tonight
(Saturday, market closed) to force-drain from id 67352 forward. Traced the
actual call path before touching anything: `_get_current_price()` typically
resolves a last-known price even off-hours (not a hard no-price skip), so
execution would proceed to `paper_trader.buy()` — which correctly refuses
to act on a closed market (`market_calendar.market_closed_reason()` gate,
`paper_trader.py:707-710`, checked first, before any side effect) and
returns `None`. The problem is what happens *after* that `None`:
`consume_pending_signals()` (`events_bus_consumer.py:192-198`) marks any
`buy()` rejection as `status='failed'` — permanently, indistinguishable
from a real trading-logic rejection. Running this manually tonight would
not "drain" these rows at all; it would falsely fail perfectly legitimate,
still-live active-source signals for the sole reason that it's Saturday,
denying them the genuine shot at real execution the already-shipped fix
guarantees them Monday. That's a real, provable side effect on the
harness's own trade/signal record (a corrupted `status='failed'` history,
not a live position), not a hypothetical one — skipping the manual drain
entirely. No action taken; the shipped mechanism runs on its own Monday.

**Proposed follow-up session:** not 2026-09-04 (told that date's taken).
Proposing **Wednesday 2026-09-03** instead — by then Monday 08-31 and
Tuesday 09-01 will have given the already-shipped fix two live trading
days against the real 1,127-row queue, so the fairness/attribution
question above can be scoped (or dropped) against actual drain data
instead of the zero-data guess available tonight. Small session: review
whether McCoy's reserved-oldest fills actually landed in any rollup in a
way that mattered, decide (a) vs (b) above, done.

---
## 🔴 HM-RIKER-SYNTHESIS-LOCK-CONTENTION — filed 2026-07-06 (HM-MONDAY-OPEN-WATCH), propose-first

`engine/riker_synthesis.py` (cron `*/10 * * * *`, re-homed from launchd
2026-07-01 per `HM-DIRECTIVE-2026-07-01` Deck2 #8) is **running correctly on
schedule and computing valid data every cycle** — confirmed via
`logs/riker_synthesis.log`: clean `[RIKER] Signals:X HC:X Trades:X
Positions:X` lines every 10 minutes straight through the current tail, no
gaps. **Not wedged on the `signals_v2` FIFO backlog above** — confirmed
`_get_recent_signals`/`_get_recent_trades` in this file query the `signals`/
`trades` tables directly, entirely independent of `signals_v2`.

**Real root cause: the final persist step is failing.**
`_save_synthesis()`'s `INSERT INTO rikers_log ...` has failed **14
consecutive times since ~06:50 AZ** (last successful write: 06:40 AZ,
matching the Admiral's own observation) with `sqlite3.OperationalError:
database is locked` — still failing as of the most recent attempt (07:20 AZ)
at time of filing. The connection (`sqlite3.connect(DB_PATH)`, line ~150) has
no explicit `timeout=` set, so it's on Python's 5s default before giving up;
evidently not enough under today's write load.

**Not isolated to this script** — the same `database is locked` string
appears in the same window across `ghost_advisor`, `aladdin`,
`movers_poller`, `regime_refresh`, `sarek`, `ti_picks_watcher`/`parser`, and
`trader.log`/`trader_error.log` itself (`War Room post failed: database is
locked`, as recently as 07:25 AZ) — reads as broad `trader.db` write
contention during the market-open surge, not a riker_synthesis-specific bug.
Plausibly the swing-surge config's higher write volume is now colliding with
everything else hitting the same SQLite file concurrently.

**Side effect worth noting:** MORPHEUS's intelligence-feed
(`_morpheus_load_riker_synthesis()` on signal.ollietrades.com) going quiet
since 06:40 is fully explained by this — there's simply nothing new in
`rikers_log` for it to show, not a second independent failure on the
MORPHEUS side.

**Propose-first, nothing built:** candidate fixes — raise `timeout=` on
these connections (cheap, low-risk), WAL-mode tuning, or reducing concurrent
writer count during the open surge. Needs Admiral sign-off; a longer
busy-timeout is the obvious minimal first move but touches a shared
connection pattern used elsewhere, so flagging rather than patching solo.

**APPLIED 2026-07-06 (Admiral approved):** all four `sqlite3.connect(DB_PATH)`
calls in `engine/riker_synthesis.py` now carry `timeout=30` (was Python's 5s
default). Live immediately — this script is a standalone cron invocation
(`*/10 * * * *`, fresh process per tick), not main.py-resident, so the edit
takes effect on the very next scheduled run with no restart required.
`py_compile` clean. **Timeout alone was insufficient** — the 07:50 attempt
still failed with `database is locked` after the full 30s wait, which is
what surfaced the real root cause below (255 leaked connections on main.py,
not a brief collision a longer wait could out-wait).

**RESOLVED 2026-07-06 08:12 AZ.** Root cause fixed at `HM-SQLITE-CONN-FD-LEAK`
(below) + `main.py` restart at 08:02:30. Verified clean on two independent
signals: (1) `engine/riker_synthesis.py`'s 08:10:01 attempt succeeded
(`rikers_log` id=5819) — first fully-post-restart tick, following the
08:00:00 success (id=5818, technically pre-restart but post-timeout-patch —
two consecutive clean writes bracketing the restart). (2) Zero
`database is locked` occurrences anywhere in `trader_error.log` since the
restart (checked the actual file tail through 08:11:24, not a date-scoped
grep — the earlier "1049 hits" figure from a first-pass check was a false
positive from an unscoped pattern matching historical log noise, not a real
post-restart count). Zero `War Room post failed` since restart too (last
occurrence 07:48:31, none since 08:02:30). Closed — no further action.

---
## 🔴 HM-SQLITE-CONN-FD-LEAK — filed 2026-07-06 (HM-MONDAY-OPEN-WATCH), fix proposal only, restart gated on Admiral's post-close window

Same anti-pattern as the historical bridge FD leak (memory:
`hm_bridge_wedge2_fd_leak.md`, fixed commit `7748aaf`) — recurring
elsewhere, unfixed. **Confirmed at two concrete sites so far:**

- `agents/aladdin.py::_ensure_tables()` (module-import-time, lines 95-128)
  and `::_save_signal()` (lines 146-165): both do
  `con = sqlite3.connect(...)` → one or more `con.execute()` → `con.commit()`
  → `con.close()`, all inside a single `try`, with only
  `except Exception: logger.error(...)` — **no `finally`.** If any
  `execute()` raises (e.g. `database is locked`, which is exactly what's
  been happening fleet-wide this morning — see `HM-RIKER-SYNTHESIS-LOCK-
  CONTENTION` above), `con.close()` is never reached and the connection
  leaks.
- `agents/sarek.py::_ensure_tables()` (lines 65-95): identical structure,
  identical gap. `get_sarek_brief()`/`run_monthly_dca()` also call
  `con.close()` inside a bare try (not yet individually confirmed
  leak-free, same file/author pattern — worth including in the same fix
  pass rather than assuming they're fine).

**Why this matters more than the historical bridge case:** the bridge leak
was in a single long-lived process (dashboard/app.py under main.py/uvicorn).
`agents/aladdin.py` and `agents/sarek.py` are **also main.py-resident**
(imported once, called repeatedly via `schedule.every(...)` for the life of
the process — currently ~13h and counting since Sunday's restart) — so
every lock-contention failure here leaks a connection for the remaining
life of the process, not just until the next cron tick (contrast
`engine/riker_synthesis.py`, a standalone cron script where a leaked
connection dies with the process every 10 minutes regardless). This is a
plausible *contributing* mechanism to why today's `database is locked`
storm has been sustained rather than self-resolving: each failed write
leaks a connection, leaked connections add contention, contention causes
more failures — a compounding cycle, not a one-off blip.

**Not yet done:** a full sweep of the other affected modules from the same
storm (`ghost_advisor`, `movers_poller`, `regime_refresh`,
`ti_picks_watcher`/`parser`) for the same pattern — only aladdin/sarek were
directly read and confirmed. `regime_refresh` is a standalone cron script
(`scripts/regime_refresh_runner.py`) like riker_synthesis, so lower urgency
there by the same short-process-lifetime logic; the other three weren't
checked.

**Fix proposal (not applied):** wrap each connection in `try/finally` (or
`with contextlib.closing(sqlite3.connect(...)) as con:`) so `close()` always
runs regardless of exception — same doctrine as the bridge fix, applied to
these call sites. **Admiral instruction: no restart until after market
close** (main.py must restart to pick up any change to these main.py-
resident modules) — this section is the fix *proposal* only; do not edit
`agents/aladdin.py`/`agents/sarek.py` or restart before the approved
post-close window.

**RESOLVED 2026-07-06 08:12 AZ (Admiral approved an earlier restart window,
option 3).** Applied `try/finally` to all 7 confirmed sites: `agents/
aladdin.py`'s `_ensure_tables()`, `_save_signal()`, the iShares-holdings
retry loop, and `get_fund_flows()`; `agents/sarek.py`'s `_ensure_tables()`,
`get_sarek_brief()`'s persist block, and `run_monthly_dca()`. Both files
`py_compile` clean. Audited `ghost_advisor.py`/`ti_picks_parser.py` too —
both already had proper `finally` blocks, no fix needed. `regime_refresh`
is a standalone cron script (same lower-urgency logic as riker_synthesis);
`movers_poller`'s source wasn't located, left unaudited rather than assumed
clean.

Restarted `main.py` via `scripts/trader_restart.sh` at 08:02:30 — clean
kill of PID 19805, **WAL checkpoint fully cleared during the zero-reader
window (`0|0|0`, zero pending frames)**, direct confirmation the leaked
connections were what blocked checkpoint. New PID 78010, single process,
orphan-free. Before/after: FD count on `trader.db` 256→11 (all new PID);
positions (46 rows/11 players) byte-identical hash before/after; `pause_all`/
`fallbacks_enabled` unchanged (0/0); Alpaca equity/cash/buying_power
unchanged modulo a few seconds' normal market drift. Confirmed fixed per
the two independent signals logged under `HM-RIKER-SYNTHESIS-LOCK-
CONTENTION` above.

**REOPENED 2026-07-06 08:55 AZ — the aladdin/sarek fix was real but was not
the dominant leak.** Admiral flagged that PID 78010 grew 11→119 FDs in the
37 minutes between the first restart (08:02:30) and the second (08:40:26,
done for the priority-lane change). Tracking the new PID 82694 the same
way: **7→85 FDs in ~12 minutes** — faster growth than before, immediately
past the "audit the remaining writers" trigger threshold.

**Root cause found: a much bigger, repo-wide pattern — `with
sqlite3.connect(...) as c:` (or `with _conn() as c:`) used as if it closes
the connection. It doesn't.** Verified empirically:
```python
with sqlite3.connect(':memory:') as c:
    c.execute('CREATE TABLE t(x)')
# c is still open here -- sqlite3.Connection.__exit__ commits/rollbacks
# the transaction, it does NOT call close(). This is a well-known Python
# sqlite3 gotcha, not a corner case.
```
**98 occurrences across 23 files**, concentrated in hot-path scanners that
run on every cycle — `engine/deep_scan.py` (9), `engine/bridge_vote.py` (9),
`engine/battle_station.py` (8), `engine/volume_scanner.py` (6),
`engine/volume_baselines.py` (6), `engine/strategy_rotator.py` (6),
`engine/dayblade_scanner.py` (6), `engine/rebalancer.py` (5),
`engine/gex_overlay.py` (5), plus 14 more files with 1-4 each (full list in
session transcript). Every one of these leaks unconditionally, on every
single call — not just on exception like the aladdin/sarek pattern, which
is exactly why growth is faster and more continuous than what the
try/finally fix addressed.

Traced the actual "War Room post failed" log line (which I'd previously
assumed came from `signal_poster.py` — it doesn't; that file is a pure
fire-and-forget HTTP poster with no DB code at all) to its real sources:
`engine/volume_scanner.py`, `engine/gex_overlay.py` (GEX regime variant),
`engine/season_manager.py` (season rotation variant) — all three touch
`trader.db` directly, `volume_scanner.py` via the leaking `with _conn() as
c:` pattern specifically (6 sites in that file alone). `engine/kirk_advisory.py`
has zero direct DB connection code (delegates elsewhere) — not a
contributor. `engine/paper_trader.py` has zero occurrences of the `with...as`
pattern — also not a contributor via this mechanism (may still have its own
missing-finally sites, not separately audited here).

**Not fixed — this is a full-repo pattern, not a 4-file patch, and needs its
own scoped session.** Proposal: a mechanical sweep converting every
`with sqlite3.connect(...) as c:` / `with _conn() as c:` site to either
`with contextlib.closing(sqlite3.connect(...)) as c:` (minimal diff, closes
correctly) or the explicit `c = ...; try: ...; finally: c.close()` form used
in the aladdin/sarek fix. Given the volume (98 sites), this should be
scripted/verified per-file rather than hand-edited, and reviewed before any
restart — each touched file is main.py-resident (these are all `engine/`
scan modules), so a fix pass means one more bundled restart when ready.

**Checkpoint (should have been at :15/:45, actually read at t+78min, 09:58):
179 FDs, no plateau** — confirms the trigger, no plateau to document.

**RESOLVED 2026-07-06 10:05 AZ — Admiral approved scripted sweep of all 98
sites.** Wrote `scripts/hm_sqlite_conn_leak_sweep.py`: regex-matches every
`with (_conn()|sqlite3.connect(...)) as VAR:` line (handles single-level
nested parens, e.g. `sqlite3.connect(str(DB_PATH))`), wraps in
`contextlib.closing(...)`, inserts `import contextlib` per file. Dry-run
posted for review first.

**Critical issue caught during review, fixed before applying:** `contextlib.
closing()` only calls `.close()` on exit — unlike the original `with conn:`,
it does **not** commit or rollback. A pre-apply audit found **7 of the 98
sites perform a write with no explicit `.commit()` anywhere in the block**,
relying entirely on the original pattern's implicit commit-on-exit:
`engine/wheel_strategy.py:91,281` (csp_wheel_scan_log insert, options_trades
max_loss/moneyness writeback), `engine/strategy_rotator.py:488` (strategy_
rotation results), `engine/dayblade_scanner.py:366,420,452,484` (flash_alerts
table creation, save/dismiss, session_grades). Naively applying the sweep to
these 7 would have **silently stopped persisting those writes** — worse than
the FD leak itself. Fixed by hand: added explicit `.commit()` at the correct
point in each (including before an early `return cur.lastrowid` in
`save_flash_alert`), verified compile-clean, then re-ran the audit
script — zero sites flagged. Also verified: no multi-context-manager
`with X() as a, Y() as b:` forms among the 98 (would need different
handling), and an automated post-hoc check confirmed every line touched in
the full 1203-line diff is either an `import contextlib` addition or a
`with` line rewrite — nothing else.

**Diff summary (98 sites / 24 files, all compiled clean):** `deep_scan.py`
9, `bridge_vote.py` 9, `battle_station.py` 8, `volume_baselines.py` 6,
`strategy_rotator.py` 6, `volume_scanner.py` 6, `dayblade_scanner.py` 6,
`rebalancer.py` 5, `gex_overlay.py` 5, `full_universe.py` 4,
`scenario_modeler.py` 4, `generated_assets.py` 4, `tax_harvester.py` 4,
`portfolio_optimizer.py` 3, `cash_manager.py` 3, `drift_rebalancer.py` 3,
`wheel_strategy.py` 2, `risk_var.py` 2, `universe.py` 2, `pipeline.py` 2,
`sub_portfolio.py` 2, `external_intel_signal.py` 1, `dashboard/app.py` 1,
`scripts/build_corpus_from_trader_db.py` 1.

**Bundled restart at 10:05:27** — clean kill of PID 82694, new PID 88763,
WAL checkpoint fully cleared (`0|0|0`) again. Verification: FD count
179→**6** (new PID 88763); positions (48 rows) byte-identical hash
before/after; `pause_all`/`fallbacks_enabled` unchanged (0/0); Alpaca
equity/cash/buying_power unchanged modulo normal drift.

**Gamma Map check (Admiral-requested, since `gex_overlay.py` was a named
culprit):** found `gex_levels`'s update scheduler is **deliberately
disabled** — `main.py:4405`, `# schedule.every(15).minutes.do(
run_gex_overlay_update) # DISABLED HM-GEX-CANONICAL`. This table won't
repopulate regardless of the FD-leak fix, by design — already-documented
canon (`CLAUDE.md`'s "2026-06-24 Structural Changes: Gamma grounding")
migrated the live Gamma Map to `engine/gamma_context.py`/`gex_snapshots`.
Checked that canonical replacement instead: **fresh**, most recent row
2026-07-06 10:05:38 AZ, right at the restart. The user-facing Gamma Map is
healthy; `gex_levels` staleness (last real row 2026-05-30, predating even
the formal disable) is expected and unrelated to today's leak.

**War Room posts:** zero `database is locked` or `War Room post failed`
occurrences anywhere from the 10:05:28 restart marker onward (checked the
actual log tail, not a stale grep match this time). Clean.

Closed — root cause (98-site repo-wide leak) fixed, verified, and the two
Admiral-requested post-restart confirmations both check out.

---
## 🟡 HM-FAST-SCAN-ORPHANED — filed 2026-07-06 (HM-MONDAY-OPEN-WATCH), left orphaned per Admiral instruction

`engine/fast_scanner.py` (a `run_daemon()`-style standalone scanner, not
main.py-resident, not in crontab) is currently serving results dated
**2026-06-28** — confirmed **not running at all** right now
(`pgrep -fl fast_scanner` returns nothing) and no dedicated log file exists
under that name. Reads as: was run manually or via some now-gone launchd/
process at some point, died or was stopped around 06-28, never restarted.
**Admiral decision: leave orphaned, no restart/fix action.** Documented here
so it doesn't get mistaken for a new regression in a future session — this
is a known, accepted-stale, deliberately-untouched scanner as of 2026-07-06.

---
## 🟢 HM-HIGH-IV-SCANNING-DISABLED — filed 2026-07-06 (HM-MONDAY-OPEN-WATCH), verified correct, not a fault

`engine/high_iv_scanner.py::scan_high_iv_opportunities()` reports
`"scanning_enabled": vix_level >= 25` — this is a **deliberate VIX-gate,
not a broken flag.** Live VIX checked directly via
`engine.vix_monitor.get_vix_status()` at time of filing: **16.04**, well
under the 25 threshold. `scanning_enabled=false` is exactly correct given
today's low-vol regime — high-IV credit-spread opportunities are
legitimately scarce when VIX is this low. No action needed; documented so
a future session doesn't re-flag this as a bug without checking VIX first.

---
## 🟡 HM-TRADE-SIGNALS-SOURCE-GAP — filed 2026-07-06 (HM-MONDAY-OPEN-WATCH), gap confirmed, root cause not yet identified

The "Trade Signals" source (`signal-center/w0_w1_seed_registry.py` registry
id `signals`, `(signals.db)`, daily-batch cadence, GREEN-within-~1-day /
RED->1-day) has been genuinely RED — **confirmed at the data level, not just
the dashboard display.** `signal-center/signals.db::trade_signals`
(the exact table/column the health check reads — `source_gate.py:41`'s
`SIGNALS_DB` and the registry's `DB_PATH` both point to this same file, so
this isn't a wrong-file/stale-copy issue like the decoy empty
`data/signals.db`) shows: last row before the gap was **2026-07-02
13:31:54** — matches the Admiral's observation to the minute — then
**zero rows 07-03 through 07-05**, then exactly **one row** at
2026-07-06 ~07:28 AZ (14:28:31 UTC), right around when this was being
investigated.

**Confirmed NOT related to the `signals_v2` FIFO backlog
(`HM-SIGNALS-V2-FIFO-STARVATION` above)** — completely different table,
different producer. The producer here is `engine/signal_bridge.py::
run_signal_bridge_job()`, scheduled `schedule.every(30).minutes.do(...)`
inside main.py (rides main.py's own scheduler, "inherits @reboot" per the
module's own docstring) — so it should have fired ~190 times between
07-02 13:31 and 07-06 07:28 and produced nothing every single time except
the one row just now.

**Root cause not yet found:** `grep`'d `trader_error.log` for
`signal_bridge`/`SIGNAL-BRIDGE`/`run_signal_bridge` — zero hits, meaning
either the job isn't erroring (silently short-circuiting on some internal
gate) or its errors aren't tagged in a way this search caught. Not
resolved — needs a closer read of `run_signal_bridge_job`'s wrapper in
`main.py` (~line 5740) and `engine/signal_bridge.py::run_signal_bridge()`'s
internal gates (e.g. an RTH/dedup/regime check that might be silently
true-for-skip since 07-02) before proposing a fix. The single row that just
appeared may be a one-off, not confirmation of recovery — worth re-checking
whether more rows land in the next 30-60 minutes.

---
## 🟢 HM-RED-SOURCE-TRIAGE — filed 2026-07-06 (HM-MONDAY-OPEN-WATCH post-incident cleanup)

Triaged the two remaining RED sources from `source_gate.py`. Different
verdicts for each — one is stale-by-design as documented, the other is a
real (if minor, self-recovering) gap:

**Execution Log — confirmed stale-by-design, no action needed.** The
registry's own note (`w0_w1_seed_registry.py:27`) already says this is
"EVENT-DRIVEN... sparse — 4 rows ever" — verified directly against
`execution_log`: **8 rows total, ever** (registry's "4" is itself slightly
stale documentation, minor correction), most recent **2026-06-19 22:35:51**
— matches the "16d RED" observation. This table only gets a row when a
human manually triggers a dispatcher action (`refresh-schwab`, `fire-kirk`,
tagged `by: ScottyTest`/`Sniff`/etc.) — it's not an automated pipeline at
all. 17 days since the last manual trigger is unremarkable given the whole
history is 8 rows since inception. Confirmed correct as documented; not a
broken producer.

**Kirk Advisory — genuinely stale, but self-explains and may self-recover
today; not "broken" so much as a missed window.** `data/kirk_advisory.
heartbeat` mtime is **2026-07-03 13:11** (confirmed the matching success
log line, `Kirk Advisory [0930]: computed + heartbeat`, is timestamped
2026-07-03 09:32:44 — so the heartbeat hasn't moved since Friday). The
underlying compute is NOT broken — `engine.kirk_advisory.
generate_kirk_advisory()` is running fine and constantly today via a
*separate* dashboard-triggered path (`dashboard/app.py:18476`/`18573`,
almost certainly the bridge frontend polling an advisory endpoint — logged
as `[Kirk] advisory run started` every ~2 minutes all morning, all
completing cleanly). But **only the scheduled path
(`main.py::run_kirk_advisory_job`, three fixed 10-minute windows at 06:35 /
09:30 / 13:05 AZ) stamps the heartbeat** — the dashboard-triggered runs
never touch it, by design (heartbeat = "the scheduled job is alive," not
"the compute works"). Today's 06:35 window closed before the 08:02:30
restart's in-memory `_kirk_slots_done_today` reset could catch it, so
that slot was silently missed for today — not logged as a failure, just
never attempted post-restart since its window had already passed. **Two
more chances today (09:30, 13:05)** — if either succeeds, the heartbeat
self-recovers with no code change needed. Worth a look after 09:40 AZ to
confirm; if BOTH today's remaining slots also miss, that would upgrade this
from "one restart-timing casualty" to a real recurring gap worth a fix
proposal (e.g., a wider catch-up window, or firing once immediately on
`main.py` startup if today's earliest slot was missed).

---
## 🟡 HM-ERROR-FILTER-CONSOLIDATION — filed 2026-07-05, DECIDED 2026-07-11 — KEEP for now, real follow-up identified but bigger than a "consolidation"

`scripts/daily_report.py::get_error_summary()` filters real errors with a
blanket `"[LRS]" not in line` exclusion — this hides ANY error tagged `[LRS]`
wholesale, including genuine ones (e.g. the "ntfy failed: <urlopen error
[Errno 65] No route to host>" lines are `[LRS]`-tagged and real). Contrast
with `scripts/eod_report.py::genuine_error_count()` (HM-EOD-REPORT-2026-07-05),
which uses an explicit, named false-positive allowlist instead of a blanket
tag exclusion — narrower and auditable (each exclusion is a specific,
verified string, not an entire log-source tag).

**Scoping pass 2026-07-11 — the `[LRS]` bug is real but currently masked by
a bigger, separate bug.** Read both functions in full and verified live
against today's logs (not guessed):

- `daily_report.py:204-218` reads only `logs/trader_error.log`, filters by
  `today.isoformat() in line` (substring match anywhere in the line), then
  `ERROR|Exception|Traceback` (case-sensitive) AND `"[LRS]" not in line`.
  Feeds the "Error log" row of `drafts/DAILY_REPORT_<date>.md` only — not
  `daily_ledger.csv` (no error column in `LEDGER_HEADER`).
- `eod_report.py:143-163` reads BOTH `trader.log` and `trader_error.log`,
  filters by `line.startswith(f"[{report_date} ")`, case-insensitive
  `error|exception|traceback|critical|fatal`, minus the named
  `KNOWN_FALSE_POSITIVE_PATTERNS` allowlist. Persisted to
  `eod_report_log.error_count`.
- **The masking bug:** `trader_error.log` lines have NO per-line date
  prefix at all (`HH:MM:SS [TAG] ...`, not `[YYYY-MM-DD ...` like
  `trader.log`) — confirmed via `grep -c '^\[2026-07-10' logs/
  trader_error.log` → **0**, out of 301,018 total lines. Consequence:
  `eod_report.py`'s own "genuine errors" count for today (465) comes
  **entirely from `trader.log`, zero contribution from
  `trader_error.log`**, even though that file is in its `LOG_PATHS` list —
  silently. `daily_report.py`'s substring-anywhere date match mostly only
  catches incidental date strings inside message bodies, not real
  timestamps, so its count is close to 0 regardless of the `[LRS]` bug.
- **Real risk confirmed, not hypothetical:** sampling the last 20,000
  lines of `trader_error.log` with eod_report.py's own case-insensitive
  pattern + the `[LRS]` tag: 1,513 matches, 721 after excluding known
  benign zero-count patterns — real ongoing items include `ntfy failed:
  <urlopen error timed out>` (218), `Ollama error (404): model not found`
  (69), `[OLLAMA-MODEL-FAIL] ... err=503` (64), `last-trade spot failed
  for` (85), Yahoo `HTTP Error 404` (29). (Also 202 `🔴🔴 CRITICAL: <ticker>
  volume spike` lines — a legitimate market alert, not a software error;
  would need its own allowlist entry to avoid false-positive inflation if
  ever counted.)

**Admiral decision 2026-07-11: KEEP, not FIX — but a real, bigger
follow-up was found underneath.** Went to implement the "shared
`engine/error_filter.py`" fix and traced `trader_error.log`'s origin
first, since the scoping pass flagged the date-format defect as needing
resolution "not guessed." Found it's deeper than a formatter tweak:

- `trader_error.log` is **plain shell stderr redirection**
  (`scripts/trader_restart.sh:98`: `nohup "$PYTHON" "$ENTRYPOINT" >>
  "$LOG" 2>> "$ERR" &`), not a single Python `logging` handler with one
  controllable format string. Whatever any code anywhere in the trader
  process writes to stderr — `console.log`/`logger` calls from dozens of
  different call sites each with their own ad-hoc prefix, PLUS raw
  Python tracebacks (which have no timestamp or tag prefix at all) —
  lands in this file verbatim, in whatever format that individual call
  site happens to use. There is no single source to patch.
- Making this file genuinely, correctly date-filterable would mean
  either (a) auditing and standardizing many independent logging call
  sites across the codebase to emit a consistent `[YYYY-MM-DD HH:MM:SS]`
  prefix (`trader.log`'s format) — a real, multi-file undertaking, not a
  quick fix, with ongoing risk that new call sites drift out of sync
  again — or (b) a fragile heuristic (file-position/rotation-boundary
  inference) that wouldn't produce a trustworthy "genuine error count."
  Neither is a same-session, low-risk change.
- Given that, extracting just `engine/error_filter.py` +
  `KNOWN_FALSE_POSITIVE_PATTERNS` right now would either (a) leave
  `daily_report.py`'s count near-zero regardless (since the shared
  function's date match still can't read `trader_error.log`, same
  problem as `eod_report.py` has today) — cosmetic, not a real fix — or
  (b) require doing the bigger date-format standardization in the same
  pass, which is genuinely out of scope for a "consolidate a filter
  function" ticket.

**Practical impact of KEEPing as-is:** `daily_report.py`'s error count
stays low-stakes — it's a 10 PM cron writing to `drafts/DAILY_REPORT_
<date>.md` only, **no ntfy push**, purely archival (confirmed via its
own docstring + cron entry, `0 22 * * 1-5`). `eod_report.py`'s ntfy-pushed
count (the one that actually matters day to day) is unaffected by this
decision — it already correctly counts everything from `trader.log`,
and its blind spot against `trader_error.log`-only errors (ntfy
timeouts, Ollama 503s, Yahoo 404s) is real but has existed unnoticed
this whole time; fixing it now would require the same bigger
log-standardization work.

**Real follow-up identified, not actioned:** if `trader_error.log`'s
errors should ever be visible in the actively-pushed `eod_report.py`,
that's its own dedicated project (standardize log line prefixes across
the codebase's stderr-writing call sites) — bigger than "error filter
consolidation," genuinely worth scoping separately if this becomes a
priority, not something to rush under a same-turn keep-vs-fix decision.

---
## 🟢 HM-TUNING-CREW-REPAIR — SHIPPED 2026-07-06 (from the degraded 2026-07-05 21:30 run)

Tonight's 21:30 tuning-crew read surfaced two silent LLM-agent failures, both
now fixed and tested.

**Agent 1 (Fleet Performance Officer) scored 0 models, no error logged.**
Root-caused by reproducing the exact prompt live: `engine.crew.
weekly_tuning_crew._ollama()` had `if r.ok: return r.json()...` with no
`else` — any non-2xx HTTP response (plausible under the heavy concurrent
Ollie-GPU load from the ~30 other jobs also firing at/near 21:30, per the
scheduler dump) fell through to a silent `return ""`, and an empty/
unparseable response upstream just produced "Scored 0 models" with zero
trace of why. Reproducing the real prompt against real data (79 active
agents fed into `spam_rates`, but only 4 with trade activity actually go
into the LLM prompt — the prompt itself was never large) parsed fine most
of the time, confirming this is an intermittent transport/response failure,
not a broken prompt. **Fixed:** `_ollama()` now logs loudly (`[ALERT]`) on a
non-2xx response and on an empty response body. `run_weekly_tuning()` now
fires a `send_alert()` (ntfy, `tuning_crew_zero_scored`) whenever
`scores_saved == 0` AND real trade activity existed that week — gated so a
genuine zero-trade week doesn't false-alarm.

**Agent 2 (Fleet Admiral / Gemini) — `ModuleNotFoundError: No module named
'google.generativeai'`.** `GEMINI_API_KEY` was already configured and the
package installs cleanly (`google-generativeai==0.8.6`) — installed it,
verified a real end-to-end Gemini call succeeds now. **Caveat the Admiral
should weigh:** the package prints its own `FutureWarning` — *"All support
for the `google.generativeai` package has ended… switch to `google.genai`."*
It works today but will never be patched again. The existing
`_ollama_fallback()` design (already falls back to local Ollama on any
Gemini error, tested and working) means the system was never truly "half-
dead" architecturally — only the specific combination of missing package +
(likely) the SAME `_ollama_fallback` silent-failure bug striking twice in a
row explains tonight's 0 adjustments. Fixed the fallback's silent failure
too (`engine.gemini_free_tier._ollama_fallback()`, same pattern as `_ollama()`
above). **Important discovery changing this decision:** `requirements.txt`
(the repo's own manifest) already lists `google-genai>=1.0.0` — the MODERN,
non-deprecated package — not `google-generativeai`. Neither was actually
installed in this venv before tonight (checked: `google-genai` isn't present
either). This means the project's own stated intent was already the modern
SDK; `gemini_free_tier.py`'s code just never got migrated to match, and this
venv was never fully synced to `requirements.txt` in the first place. **So
the real fix isn't "install the old package" (what I did to get Agent 2
working again tonight) vs. "retire Gemini" — it's migrating `call_gemini()`
to the `google.genai` API that the repo already declared it wants.** That's
a real code change (different client API surface), not a pip install, so
not done tonight — flagging so the decision isn't made on the "install vs.
retire" framing alone. Also: the old-package install downgraded `protobuf`
7.34.1→5.29.6 in this venv — verified no regression (`alpaca-py` + full test
suite green after), but `requirements.ollie.txt` still pins
`protobuf==7.34.1`, so this venv and that file now disagree; not reconciled.
Working today either way (verified a real end-to-end Gemini call succeeded
on the old package) — this is about the durable fix, not an active outage.

Also added the same loud-empty-loop guard to Agent 2's promotion-parsing
block (`adj_saved == 0` with real scores to work from → alert), gated to
not double-alert when Agent 1 already failed upstream.

13 new tests, all passing (`tests/test_weekly_tuning_crew_wiring.py`'s
`OllamaLoudFailureTests`/`ZeroScoreZeroAdjustmentGuardTests`,
`tests/test_gemini_free_tier.py`).

**Ticketed, not fixed — `database is locked` contention on
`alert_channels.save_setting`:** ~5 occurrences during the 21:33-21:36
window (heavy concurrent job load, same window as the above). `alert_channels
._conn()` already uses `timeout=20`; `trader.db` is already WAL-mode
file-wide (confirmed via `main.py`'s own boot log), so this isn't a missing-
WAL problem — it's genuine multi-writer contention exceeding 20s under peak
concurrent scheduler load. **Low real severity**: `_save_setting` only
persists the alerts-enabled toggle/email address, rarely-changing config,
not trade data — a failed write here just means a stale setting stays in
memory one cycle longer. Worth a look (whether OTHER, higher-stakes writes
hit the same contention during the same busy window) but not urgent enough
to interrupt anything for.

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
## 🟢 HM-SHADOW-PIPELINE-COST-AUDIT — filed 2026-07-05, KILLED 2026-07-11 — Admiral decision: kill, executed

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

**Scoped 2026-07-11, all four original questions answered with evidence:**

1. **What are these — confirmed a report-only shadow/witness scoring
   layer inside the War Room debate engine**, distinct from q-witness (a
   separately-tracked real paid-API player, `engine/ask_q.py:323-324`).
   `wr-witness` — A/B witness commentary (`gemma4:12b-it-qat` vs
   `plutus-v1:latest`), fired every debate cycle
   (`engine/war_room.py:294-330`, `_record_witness`). `wr-shadow-v1`/
   `wr-shadow-v7d` — plutus-v1 vs plutus-v7d critique pair written to
   `plutus_shadow_critiques` "for later grading" (`engine/war_room.py:
   356-402`, `_record_shadow_witness`), gated by `config.py:101
   SHADOW_WITNESS_ENABLED` (code default `False`). `ab-witness-
   deepseek-r1`/`ab-witness-gpt-oss` — a separate post-market batch judge
   scorer (`scripts/witness_ab_scorer.py:152`). All five run against local
   Ollama (192.168.1.168:11434) — `api_costs.call_type='war_room'` for
   all five, confirmed via direct SQL.

2. **Trigger mechanism:** `wr-witness`/`wr-shadow-*` fire in-process from
   `main.py`'s live scheduler (`main.py:505 run_war_room()` →
   `main.py:1576-1648`, throttled ~5min during market hours).
   `ab-witness-*` is a genuine crontab entry (`30 13 * * 1-5`,
   `HM-SHADOW-AB-WITNESS` 2026-06-29) — not launchd, no matching plist.

3. **Downstream consumer: none.** No `wr_shadow`/`wr_witness`/
   `ab_witness_*` tables exist by those names — the real tables are
   `plutus_shadow_critiques` (4,383 rows), `witness_ab` (1,648 rows),
   `witness_queue` (1,774 rows), all write-only. `plutus_shadow_critiques
   .realized_outcome` is 0/4,383 populated with zero `SELECT`s against it
   anywhere. `witness_ab.agreed_with_mccoy` is written
   (`scripts/witness_ab_scorer.py:195-198`) but never read — no dashboard
   route, report, or healthcheck references it.

4. **Is this dead spend? No — the $50.24 is 100% historical, already
   fixed, not a live leak.** `cost_usd = 0.00` confirmed for every
   `wr-shadow-*`/`wr-witness` call since **2026-07-06** (volume continues
   at ~200-230/day/id, cost stays exactly zero every day after). Root
   cause: `engine/cost_tracker.py:108-117`'s `wr-`/`ab-witness-` name-
   prefix allowlist, added under "COST-DISCIPLINE-IMPLEMENT 2026-07-05" —
   already known, already-remediated tech debt (`scripts/daily_report.py:
   389,399` has its own note: *"phantom billing from shadow CSP seats
   fixed 2026-06-29; historical rows left intact"*). **No cron/scheduler
   kill needed for cost.**

**Remaining open item was compute waste, not dollars.** Both pipelines
ran continuously (~200-230 Ollama calls/day/id) and wrote to tables
nobody read. Two anomalies had surfaced worth flagging rather than
silently fixing: `settings.SHADOW_WITNESS_ENABLED` was live-overridden
to `'true'` against `config.py`'s `False` code default (no audit trail
for who/when), and `scripts/witness_ab_scorer.log` showed a live
`errno 24 "Too many open files"` connection-failure bug against the
Ollama host.

**Admiral decision 2026-07-11: kill.** Given zero consumers on either
side, and `_record_witness` specifically overdue its own "2-week A/B"
window by 17+ days, chose to disable rather than build the missing
consumer. **Executed, all three steps confirmed live:**

1. Retired all three call sites (`_record_witness`,
   `_record_shadow_witness`, `_queue_ab_witness`) in-place inside
   `engine/war_room.py`'s debate-round function — commented out, not
   deleted, per this repo's Archive Convention; rehab path documented
   inline (wire a real consumer to `plutus_shadow_critiques`/
   `witness_ab`, then uncomment). Commit `2a59e0c`.
2. Flipped the live `settings.SHADOW_WITNESS_ENABLED` row to `'false'`
   (now matches `config.py`'s code default) — this also resolves the
   unexplained-override anomaly by returning it to the documented
   default rather than leaving it unexplained.
3. Removed the `HM-SHADOW-AB-WITNESS` crontab line
   (`witness_ab_scorer.py`, `30 13 * * 1-5`) — this stops both the
   compute waste and the live "too many open files" bug from recurring
   daily.
4. Restarted `main.py`/trader to pick up the code-level retirement
   (PID 61058, bound `:8080`, clean startup log, no import errors).

Fully reversible if ever needed: flip the settings flag back, re-add the
crontab line, uncomment the three calls in `war_room.py`, restart.

---
## 🟡 HM-DESK-CHAIN-PROVENANCE — filed 2026-07-05, DECIDED 2026-07-11 — KEEP, not FIX (re-scoped: one half isn't a bug, the other isn't gate-critical)

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
to explicitly guard around.

**Scoped 2026-07-11 — re-verified both figures live, not trusted blind:**

1. **72% mislink — confirmed exactly, root cause only partially found.**
   Only 81 of 2,692 total trades (~3%) carry any `signal_id` at all (a
   nullable FK added 2026-05-20, narrow/recent, not systemic historical
   corruption). Of those, 75 resolve to a real `signals` row; **54/75 =
   72.0%** symbol-mismatched — matches the ticket exactly. A fresh
   independent sample of the first 65 by trade id reproduces 47/65.
   `ollama-plutus` (1 matched/26 mismatched) and `ollie-auto` (0/14)
   account for 40 of the 54 mismatches. **The intended write path looks
   correct in code** — `engine/paper_trader.py:690` (`buy()`) inserts
   `signal_id` from a caller-supplied argument threaded through the same
   call scope (`engine/ai_brain.py:1300→1650`,
   `engine/crew_scanner.py:3316-3330`, `engine/execution_router.py:
   145-256`) — none of these, read in isolation, would produce a
   cross-player, cross-symbol mismatch. **But the actual corrupted rows
   don't fit that model at all**: e.g. trade 2729 (`ollama-plutus`, V,
   2026-06-01) links to signal 4695, which belongs to a DIFFERENT player
   (`gemini-2.5-flash`), DIFFERENT symbol (MSFT), created ~3 months
   earlier (2026-03-11) — and near-consecutive trade IDs map to
   near-consecutive signal IDs from unrelated players/symbols/dates. No
   backfill script or migration was found writing `trades.signal_id`
   (checked `migrations/apply_migration_003.py`,
   `scripts/hm_trades_writeback_backfill.py` — neither touches it).
   **Genuinely unresolved**: looks like a stale/shared-state or off-by-N
   bug in a scan cycle, but the exact write site producing these specific
   rows was not pinned this pass — needs its own dedicated trace.

2. **`execution_status` never `'EXECUTED'` — root cause fully found.**
   Lives on `signals`, not `trades` (`trades` has no such column).
   `engine/paper_trader.py:1759` sets it via `_resolve_execution_portfolio()`
   (`:410-464`), which only returns `route_mode="trading"` (→ `EXECUTED`)
   for a hardcoded 5-player whitelist in `_EXECUTION_PORTFOLIO_BY_PLAYER`
   (`:151-162` — `super-agent`, `dalio-metals`, `neo-matrix`, `ollie-auto`,
   `guardian-of-forever`) mapped to a `portfolios.execution_mode='auto'`
   row. Every other player hard-defaults to `route_mode="paper"` (`:426`)
   — `EXECUTED` is **architecturally unreachable** for the fleet's actual
   live trading loop, not a case-sensitivity or timing bug. Verified via
   direct query on genuinely-matched links: **16 PENDING, 5 SIMULATED, 0
   EXECUTED** — none of the 4 players producing real matched `signal_id`
   links (`ollama-plutus`, `deepseek-7b-grok4`, `ollama-qwen3`,
   `capitol-trades`) are in the legacy whitelist.

**Admiral decision 2026-07-11: KEEP, not FIX.** Re-examined both pieces
before implementing the "proposed fix" drafted in the first scoping
pass, and found that pass over-stated both the bug-ness and the urgency:

1. **`execution_status` is NOT a bug — verified the full write path.**
   `engine/paper_trader.py`'s own module docstring (lines 1-35) documents
   a deliberate three-tier routing model: `trading` (real Alpaca order,
   `EXECUTED`), `paper` (simulated, DB-only, **"No external broker
   calls"** by design, `SIMULATED`), `tracking` (log-only). Traced the
   full path live: `buy()`'s return dict (`:1759`) →
   `engine/ai_brain.py:1653` (`result.get("execution_status", ...)`) →
   `:1657` `update_signal_status(signal_id, _signal_status, _reason)` —
   this IS actually persisted to `signals.execution_status`, working
   exactly as the module documents. The "fix" drafted in the first pass
   (swap the whitelist gate for a check against
   `alpaca_status='filled'`) would have been **a no-op at best, actively
   misleading at worst** — `paper`-mode trades never call
   `_forward_to_alpaca`/`_persist_alpaca_fill` at all (confirmed at
   `:1732-1746`, gated `if route["route_mode"] == "trading":`), so they
   would never satisfy a fill-confirmation check either. Relabeling them
   `EXECUTED` would misrepresent trades that are genuinely, intentionally
   internal-simulation-only. **No code change warranted — this is
   correct, documented behavior, not a scoping gap.**

2. **The 2026-07-24 kill-gates do NOT actually read this data — verified
   directly, corrects the first pass's stated urgency.** Both
   `scripts/door1_kill_gate_check.py` and
   `scripts/ollie_machine_kill_gate_check.py` (built 2026-07-10, i.e.
   AFTER this ticket was filed 2026-07-05) were re-read line by line:
   neither references `signal_id` or `execution_status` anywhere. G1/G3
   read `options_trades` directly (`mtm_intrinsic`, `entry_credit_debit`,
   `pnl`); `ollie-machine`'s `trade_counts()` is a raw
   `SELECT COUNT(*) FROM trades/options_trades WHERE player_id=?` — no
   chain-provenance filter of any kind. **The "matters more now... gates
   read exactly this kind of chain data" framing from the original
   2026-07-05 filing (and repeated uncorrected in the first 2026-07-11
   scoping pass) was a reasonable anticipation at filing time that never
   got re-verified once the gate scripts actually shipped — it does not
   hold for the gates as built.** Correcting it here rather than letting
   it stand.

**The 72% `signal_id` mislink remains a real, open, UNFIXED data-quality
issue** — kept 🔵 open, deprioritized (not gate-critical per the above),
still needs its own dedicated write-site trace before any code change
(the actual corrupted rows don't match any currently-visible write
path, per the first pass's finding — a defensive insert-time assertion
would only prevent NEW instances, not explain or fix the existing
pattern, and touching `paper_trader.py`'s execution path without knowing
the real cause risks papering over a bug instead of catching it). The
Desk's own display already guards against ever showing a wrong fill
(`trade.symbol == signal.symbol` check), so the user-facing risk from
leaving this unfixed is already mitigated. **Do NOT backfill** — same
"retrospective join is a DEAD END" trap CLAUDE.md's ALPHA READ section
documents for `acted_by_fleet`; any backfill of the 47/65 historical
mismatches would just encode new guesswork, not truth.

No code touched this pass. Re-scoping only, correcting the record.

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
## 🟢 XO-DEPARTURE-HARDENING — Phase 1, filed 2026-07-06 (Admiral restated), all four gap-closing scripts WIRED LIVE 2026-07-10

Context: Admiral departs in a few days; after that the system runs
monitoring-only (phone push, no hands-on). All unattended automation in this
program must be **deterministic scripts, never LLM sessions**; **ntfy is the
alert channel throughout**; **propose-first per item**, same as every other
ticket in this file. This was previously only stated in conversation — the
2026-07-05 start card flagged it as missing from the document; this is that
restatement.

**UPDATE 2026-07-10 (status check + proposals, per Admiral request):**
re-verified all four Phase 1 items directly against the live crontab and
scripts rather than trusting this 4-day-old entry. Items 1 and 3 are in
much better shape than filed — most of both are already live:

- **Item 1 (service watchdog): largely already built.** `watchdog.py`
  (kept alive by `watchdog_supervisor.sh`, cron */5, reboot-survives)
  covers trader/bridge/signal-center/ollama/cloudflared with ntfy push.
  `origin_healthcheck.sh` (also */5) adds a second independent layer —
  actual HTTP response checks, not just process liveness — for bridge/
  signal-center/swingdesk/status_page, auto-restart + ntfy on failure.
- **Item 2 (health cron): partially built, two gaps confirmed** — (a)
  `tour_api`'s `/api/tour/health` checked nowhere; (b) the Cloudflare-
  edge-cache staleness for `status.ollietrades.com` unmonitored (checks
  hit localhost, bypassing the CDN layer where the staleness actually
  lives — see `HM-STATUS-PAGE-STALE-CACHE` below, still needs a
  Cloudflare dashboard change only the Admiral can make).
- **Item 3 (DB/log hygiene): largely already built, one gap confirmed** —
  daily DB backup w/ exact 7-day retention (`db_snapshot.sh`, `KEEP=7`),
  off-host nightly backup, a backup-freshness alarm, and weekly archived
  log rotation are all live. Gap: **no automated disk-space alert** —
  `scripts/vitals.sh` can report usage but isn't scheduled anywhere.
- **Item 4 (gate-day automation): not built at all** for the two dated
  fleet/agent gates. The Aug 15/16 incumbent-audition piece needs no new
  script — already computed and pushed daily via the existing
  `scripts/eod_report.py` (2pm weekdays), which already calls
  `track_incumbent_auditions()` and correctly reports both auditions as
  suspended (`HM-EDGE-PROVENANCE` ruling).

**WIRED IN LIVE 2026-07-10** (all four; syntax-checked and dry-run-verified
against live data before wiring, per-script behavior confirmed safe
before scheduling):
- `scripts/tour_api_restart.sh` — kills both tour_api's respawn-loop
  wrapper and the underlying process (it has its own self-respawn loop,
  unlike the other services, so a plain process kill would just get
  instantly re-spawned with the same wedge), relaunches detached. Added
  as a fifth `check_and_restart` line in `origin_healthcheck.sh`
  (live-scheduled cron `*/5`) — confirmed tour_api healthy (200) before
  wiring, then manually ran the updated script and confirmed a clean
  silent pass (no restart triggered, no false positive).
- `scripts/disk_space_alert.sh` — WARN 85% / ALARM 95% used, one push per
  severity per day, independent of the backup scripts by construction.
  Crontab: `0 6,13,20 * * *` (3x/day, every day). Dry-ran clean against
  the real box (29% used, 40GB free).
- `scripts/door1_kill_gate_check.py` — computes and pushes G1-G4 per
  `OLLIETRADES_KILL_GATE.md`'s pre-committed criteria. Compute-and-push
  only, never acts. G4 correctly reports inconclusive (no parallel-
  benchmark tracking exists anywhere in the codebase — the doc's own
  rules allow this). G1 uses the doc's own "~$500" absolute approximation
  since no queryable DAY-0 baseline was ever stored. G3 scoped to
  `structure='csp'` (an explicit interpretation call, flagged in the push
  itself, since the doc's own G3 SQL note doesn't state the filter
  explicitly). Dry-run against live data: G1 PASS ($2,511.39 vs $500), G3
  PASS (2.9% worst-loss ratio vs 20% threshold), G2 PASS post equity-curve
  fix (0.17% acct DD vs 0.55% SPY DD), G4 N/A. Crontab: `5 14 * * 1-5`
  (5min after `eod_report.py` to avoid an exact-minute collision).
- `scripts/ollie_machine_kill_gate_check.py` — checks both `trades` and
  `options_trades` for the single-agent 2026-07-24 gate
  (`HM-OLLIE-MACHINE-KILLGATE` below). Dry-run: still zero trades, 14
  days remaining as of today. Crontab: `10 14 * * 1-5`.

Crontab verified post-install: 77 → 87 lines, diff showed only the 3 new
additive blocks (disk-space-alert + both gate scripts), nothing else
touched or reordered.

**SHIPPED 2026-07-10, found while dry-running the Door-1 script:**
`dashboard/app.py::_get_alpaca_equity_and_spy_raw()` — the source behind
`/api/account/equity-curve`, which both G2 above and the Admiral-facing
Door-1 equity chart depend on — was silently stuck at 2026-05-23 for 7+
weeks. Root cause verified directly against the live Alpaca API (not
guessed): the `portfolio/history` endpoint defaults to a ~1-month window
measured FROM `start` when `period` is omitted, not "start to now" as
the code assumed — confirmed by moving `start` forward and watching the
window move with it. **This would never have self-healed, and worse, a
fixed `period` like "3M" would have resurfaced the identical bug right
around the real 2026-07-24 verdict date** (also confirmed via a live
test) — the kill-gate doc's own rule is "if a gate is ambiguous on the
day, it fails," so this was a real risk to Door-1's validity, not
cosmetic. Fixed: switched to `period="all"` (dropping `start`
entirely) — pre-season rows it returns are already filtered out for free
via the existing SPY-bars join. Live-verified post-restart:
`/api/account/equity-curve` now returns 42 dates through today
(2026-07-10), and the Door-1 script's G2 now resolves (PASS, 0.17% acct
DD vs 0.55% SPY DD) instead of reporting no data. Test added:
`tests/test_equity_curve_period_cap.py`.

**1. Service watchdog.** launchd keepalive coverage for trader/dashboards/
tunnel, ntfy on any unexpected death/restart with timestamp + last log
lines. The morning power-cycle (see `docs/REBOOT_POSTURE.md`) already proved
autostart works; this proves *liveness* — a separate claim. Per CLAUDE.md's
"Alarms must not share a failure mode with what they watch" doctrine — do
not run this watchdog on the same mechanism it's watching.

**2. Bigmac-native health cron.** curl-based check of all endpoints,
including `tour /api/tour/health` and the status-page timestamp-freshness
check (see `HM-STATUS-PAGE-STALE-CACHE` below for the Cloudflare-edge-cache
nuance already diagnosed there — don't re-diagnose it here). Every 30 min
during market hours, ntfy on failure. Second layer under whatever
Chrome/browser sweep already exists.

**3. DB/log hygiene.** Daily `trader.db` backup with 7-day rotation
(check first whether `scripts/db_snapshot.sh`/`scripts/offhost_backup.sh`,
referenced elsewhere in this file's backup-spine work, already cover this —
don't duplicate), disk-space alert threshold, log rotation (same check
against `scripts/rotate_logs.sh` if it already exists). Boring items kill
unattended boxes — this is precisely the "boring but load-bearing" category.

**4. Gate-day automation — COMPUTE AND PUSH, never act.** Scripts that
compute a verdict and ntfy it to the Admiral's phone; the decision stays
human. Three gates:
   - **2026-07-24 Door-1 G1-G4 kill-gate** (`project_door1_kill_gate` memory) —
     must read against the guarded-honest clean-window baselines, not the
     pre-friction Season 6.3 numbers (already noted stale elsewhere in this
     file).
   - **2026-07-24 `ollie-machine` kill gate** (`HM-OLLIE-MACHINE-KILLGATE`
     below) — no trade by this date (checking BOTH `trades` and
     `options_trades`) → halt proposal.
   - **Aug 15/16 incumbent audition gates** (`options-sosnoff`/
     `qwen3-8b-flash`, `engine.crew.audition_tracking`) — **now
     suspension-aware per HM-EDGE-PROVENANCE ruling 2**: both are currently
     SUSPENDED pending broker routing, so a gate-day script computed today
     would correctly report "suspended, not evaluable" rather than a false
     0/20 fail. Verify this stays true when `HM-ROUTE-TO-BROKER` eventually
     un-suspends them — the gate script needs to read `suspended`/
     `suspension_reason`, not just `clean_guarded_trades`.

---
## 🔴 XO-DEPARTURE-HARDENING — Phase 2 remaining, filed 2026-07-06

**5. Qwen3.6-35B-A3B audition onboarding — CLOSED, candidate dead on this
hardware (2026-07-06 evening).** Prep-checked per this item's own "confirm
the model is pulled... fits VRAM co-residency... verify against
ram-discipline.md before pulling" precondition, ahead of the planned
2026-07-07 after-close onboarding slot. Live `/api/tags` on Ollie Max
(192.168.1.168:11434) confirms `HM-ORPHAN-SEATS` (2026-07-01) is still
accurate — no qwen3.6 variant is pulled. Checked Ollama's library for every
`qwen3.6`/`35b-a3b` tag + real size: **the smallest quantization in the
entire Qwen3.6 family, anywhere, is 17GB** (`qwen3.6:27b-q4_K_M`); the
specific `35b-a3b` tag named here is 22-24GB. Ollie Max's real GPU (RTX
5080, confirmed via `nvidia-smi`: 16303 MiB total) cannot fit ANY Qwen3.6
tag solo, let alone alongside anything else — this is not a co-residency
squeeze like the existing 7-8B budget, it's a hard "doesn't fit at all."
"A3B = active ~3B-param MoE, lighter than the full 35B footprint" (this
ticket's original framing, see the candidate description below) describes
compute-per-token, not memory — MoE still needs all experts resident in
VRAM, so it doesn't help here. **Admiral decision 2026-07-06: substitute a
different free local model that actually fits** — see
`HM-QWEN36-SUBSTITUTE-2026-07-06` for the replacement pick + numbers.
Original riskiest-departure-item framing and `HM-EDGE-PROVENANCE`
broker-execution-required onboarding criteria below both carry over
unchanged to whatever model replaces Qwen3.6 — only the specific model_id
is dead, not the onboarding design.

**6. Shadow-pipeline verdict — KILLED 2026-07-11**, full ticket:
`HM-SHADOW-PIPELINE-COST-AUDIT` below. Cost was already $0/day since
2026-07-06, but zero consumers + one pipeline 17+ days overdue its own
"2-week A/B" window → Admiral decision: kill. Retired in
`engine/war_room.py` (code kept, commented out), live
`SHADOW_WITNESS_ENABLED` settings flag flipped false, `ab-witness-*`
cron removed, trader restarted. Fully executed, reversible.

**7. Desk provenance fix — DECIDED 2026-07-11: KEEP, not FIX**, full
ticket: `HM-DESK-CHAIN-PROVENANCE` below. Re-scoped on closer look:
`execution_status` "never EXECUTED" turned out to be correct, documented
behavior (a deliberate trading/paper/tracking routing model), not a bug
— no fix needed. The "gate-day scripts read this chain data" concern
from the original 2026-07-05 filing was **verified false** against the
actual shipped gate scripts (`door1_kill_gate_check.py`,
`ollie_machine_kill_gate_check.py`, built 2026-07-10) — neither
references `signal_id` or `execution_status` at all, both just do raw
trade counts. Urgency claim corrected. The 72% `signal_id` mislink
remains real and open but deprioritized (not gate-critical, write-site
still unknown, needs its own dedicated trace) — no code touched.

---
## 🔴 XO-DEPARTURE-HARDENING — Phase 3, filed 2026-07-06

**8. Weekly digest push — SHIPPED 2026-07-11, cron NOT installed
(propose-first).** `scripts/weekly_digest.py` — sweep summary (latest
`reports/fleet_realism_sweep_clean_*.json`, excluding `.INCOMPLETE_*`
files) + tuning results (reads `model_scores`/`model_adjustments` written
by the Sunday tuning crew, never re-invokes it — that makes LLM calls,
which unattended automation here must not do) + audition clocks (delegates
to `engine.crew.audition_tracking.track_incumbent_auditions`, already
suspension-aware) + 30-day spend (deliberately NOT scoped to `ai_players`
— see item 6 below, this is the direct fix for that blind spot). One
`send_alert()` push, `audience="admin"`, mirrors `eod_report.py`'s
structure exactly. 12 tests (`tests/test_weekly_digest.py`), dry-run
verified against real `trader.db`. **Proposed cron (not installed):**
`0 23 * * 0` (23:00 MST Sunday — after the tuning crew's ~21:00-21:30
window and the sweep's 22:10 kickoff both finish). Needs Admiral go-ahead
before adding to `crontab -e`, same durability caveat as `HM-SWEEP-
CADENCE` above (a manual dry-run doesn't survive session death).

**9. Error-filter consolidation — DECIDED 2026-07-11: KEEP, not FIX.**
See `HM-ERROR-FILTER-CONSOLIDATION` above for the full finding: went to
implement the fix and found `trader_error.log` is plain shell stderr
redirection with no single controllable format (dozens of independent
call sites, each with their own ad-hoc prefix, plus raw tracebacks with
no timestamp at all) — standardizing it into something genuinely
date-filterable is a real multi-file undertaking, not a quick
consolidation. `daily_report.py`'s broken count is low-stakes (archival
only, no ntfy push); `eod_report.py`'s actively-pushed count is
unaffected either way. Real follow-up identified (standardize log line
prefixes codebase-wide) but explicitly not actioned — bigger than this
ticket, worth its own dedicated session if ever prioritized.

**10. Pre-flight alarm test, day before departure.** Kill each service
(trader/dashboards/tunnel) one at a time and confirm the Phase-1-item-1
watchdog alert actually lands on the phone. Run `scripts/eod_report.py`
manually and confirm the push arrives. Confirm the Admiral's **phone**
specifically can pass Cloudflare Access to bridge/signal/desk — monitoring-
only mode is only real if the phone can actually see in; this is the one
item on the whole list that can't be verified by Scotty alone and needs the
Admiral's own device.

**COMPLETED 2026-07-10 — full kill-test run, Admiral-confirmed on-device
each time.** Root-caused and fixed a real ntfy delivery bug along the
way (see `HM-NTFY-IPV6-NOROUTE-LRS-FIX` — `engine/long_range_sensors.py`
was the source of ~13,300 "ntfy failed" lines this week, now fixed) and
confirmed/closed a real subscription gap (`Ollie-Alert-35`, the topic
`watchdog.py`/`eod_report.py` actually send to, was not subscribed on
the phone until tonight — now added). Results:

| Service | Recovery mechanism | Downtime | Alert path exercised? |
|---|---|---|---|
| Tunnel (cloudflared) | Undocumented system LaunchDaemon, `KeepAlive` | <1s | No — recovery too fast to catch (see below) |
| Dashboard (swingdesk) | Same undocumented LaunchDaemon pattern | <2s | No — same reason |
| Trader (main.py) | Cron keepalive (5-min) + watchdog 3-strike system | ~2m19s | **Yes — confirmed landed on phone** |
| `eod_report.py` manual run | `send_alert()`, now on the fixed path | n/a | **Yes — confirmed landed on phone** |

**Major side-finding, not previously documented anywhere:** tunnel and
swingdesk are NOT actually on the cron+nohup fallback `CLAUDE.md`'s
"LaunchAgent Reboot Lifecycle" section describes — they're protected by
real, working, system-domain LaunchDaemons (`/Library/LaunchDaemons/
com.trademinds.cloudflared.plist`, dated 2026-06-11;
`com.trademinds.swingdesk.plist`, 2026-06-17) that `CLAUDE.md`
incorrectly still describes as "deferred." Confirmed live by directly
killing each service and watching a replacement process appear in the
same second, independent of any of this repo's own watchdog/cron
mechanisms. `CLAUDE.md` updated to correct this. `status_page` has the
same LaunchDaemon pattern too (already correctly noted in
`scripts/status_page_restart.sh`'s own comment, just never propagated
to `CLAUDE.md`'s doctrine section).

**Phone/Cloudflare Access confirmed working** across four separate real
pushes tonight (a manual test ping, TRADER REVIVED, EOD Report) — no
remaining doubt on this piece.

**Residual, not urgent:** the old `@reboot cloudflared_reboot_start.sh`
cron line is now redundant (harmless — dup-guarded) given the
LaunchDaemon; worth removing in a future cleanup pass, not blocking.

---
## 🟡 HM-STATUS-PAGE-STALE-CACHE — filed 2026-07-05 (bigmac cold-start test), RE-CHECKED 2026-07-11 — currently fresh under normal operation, cold-start scenario untested since filing

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
to "respect existing headers" / bypass), not a code change.

**Re-checked live 2026-07-11 (normal operation, no reboot in progress):**
fetched `https://status.ollietrades.com/` three times, ~3-4 seconds apart.
The "Last checked" timestamp matched real UTC time to the second on every
fetch — the page is genuinely fresh right now, not stale. Response headers
confirm why: `cache-control: no-cache, no-store, must-revalidate` (the
origin header) is reaching the client unmodified, and Cloudflare's own
`cf-cache-status: DYNAMIC` header confirms the edge is NOT caching this
response during normal steady-state serving — it's passing every request
through to origin live.

**This does not close the ticket — it narrows it.** The original
observation was specific to a real physical cold-start (power-cut) test,
not steady-state operation; no cold-start test has been re-run since
2026-07-05 to confirm whether the specific symptom (edge serving a stale
cached copy right after/during an origin outage window) still recurs. A
plausible mechanism that would explain "fine in steady state, stale right
after a reboot" and wasn't previously considered: Cloudflare's "Always
Online" / stale-while-origin-is-down behavior serves a last-known-good
cached copy specifically WHILE the origin looks unreachable, independent
of `Cache-Control` headers (which only govern normal-operation caching,
not down-origin fallback behavior) — this is a hypothesis, not confirmed;
no Cloudflare API token exists in this repo's `.env` to inspect the zone's
actual Cache Rules / Always Online setting programmatically (only
`CF_ACCESS_TEAM_DOMAIN` is present, scoped to Access, not zone/cache
config), so this can't be verified without either Cloudflare dashboard
access or another real cold-start test. Not actioned this pass — still
dashboard-side if the hypothesis is right, needs Admiral/XO either way.

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

**⚠ TOMORROW (2026-07-07) AFTER-CLOSE BUNDLED RESTART — checklist, Admiral-confirmed 2026-07-06.**
Everything below rides ONE restart, not separate ones:
1. **Precondition gate (check first, before touching anything else):**
   confirm today's (07-07) open showed at least one live order placed/closed
   end-to-end under `can_trade_live` enforcement=ON. If any gate anomaly
   shows up instead, STOP — fix the anomaly first, hold both items below for
   a later window.
2. **Flip `config.ALERT_DEFS_ENABLED = True`** (currently `False` —
   see `HM-ALERT-COLLAB-LINKS` Phase 1, shipped code-complete 2026-07-06,
   gated off pending this flip).
3. **Qwen3.6-substitute onboarding** (`HM-QWEN36-SUBSTITUTE-2026-07-06`
   above) — ONLY if the Admiral has confirmed the pick (`qwen3:4b`
   recommended) and it's been pulled ahead of the restart. `ollama pull
   qwen3:4b` on Ollie Max first (separate step, doesn't need the trader
   restart), then the `ai_players` INSERT for the new auditioning seat
   (`crew_role='auditioning'`, `can_trade_live=0`) rides this same restart.
4. `zsh scripts/trader_restart.sh` — the one bundled restart.
5. **Post-restart verification (standard checklist):**
   - `trader.log` shows `[AUDITION-GATE] active — ... enforcement=ON` —
     **must still say ON, not OFF** (a regression here means the backfill
     or gate code got reverted somehow — stop and investigate, don't
     proceed with anything else).
   - Re-run the same `is_auto_tradeable()` check against all 10
     `can_trade_live=1` agents (6 executing + 4 exit_only) from
     2026-07-06 — confirm still all `True`.
   - If Qwen3.6-substitute onboarded this same restart: confirm the new
     seat shows up in `ai_players` with `crew_role='auditioning'`,
     `can_trade_live=0`, and is correctly BLOCKED at all 3 audition-gate
     layers (mirror the dry-run check already done 2026-07-05 — don't
     just assume the existing gate code handles a new row correctly,
     verify it).
   - Confirm `config.ALERT_DEFS_ENABLED` reads `True` in the running
     process and `engine.dynamic_alerts.run_user_alert_definitions()` is
     no longer a no-op (e.g. create one throwaway test definition, confirm
     it evaluates, then disable/remove it — don't leave test data live).
   - `logs/trader.log` clean of new tracebacks/errors in the first few
     minutes post-restart (same bar as tonight's restart).

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
  **DEAD, 2026-07-06 — see item 5 above + `HM-QWEN36-SUBSTITUTE-2026-07-06`
  below: no Qwen3.6 tag, at any quantization, fits Ollie Max's 16GB VRAM.
  "Lighter than the full 35B footprint" was true of active-compute, not
  memory footprint — wrong assumption, caught before pulling.**

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
## 🟢 HM-QWEN36-SUBSTITUTE-2026-07-06 — replacement pick, ONBOARDED 2026-07-07 (stale header corrected 2026-07-11)

**Stale-doc correction (2026-07-11):** this ticket's header said "NOT YET
PULLED," but the 2026-07-07 after-close bundled-restart checklist below
(item 3) already onboarded it. Verified live, not just trusted: `qwen3:4b`
confirmed pulled on Ollie Max (`/api/tags`), and `ai_players` has the
`qwen3-4b-audition` seat (`model_id='qwen3:4b'`, `crew_role='auditioning'`,
`halt_mode='active'`, `can_trade_live=0`) exactly as the checklist
specified. No further action needed on this ticket.

Qwen3.6-35B-A3B is dead (see item 5 in XO-DEPARTURE-HARDENING Phase 2 above)
— no tag in the entire family fits Ollie Max's 16GB VRAM. Admiral asked for
the strongest model/quant that actually co-resides with the currently-active
roster's real models, with headroom, no evictions during market hours.

**Real numbers (not tag names), checked 2026-07-06 evening:**
- Ollie Max GPU: RTX 5080, **16303 MiB (~15.9GB) total**, confirmed via live
  `nvidia-smi` (not just the ram-discipline.md doc figure).
- Cross-referenced `ai_players.model_id` for every non-`halt_mode='full'`
  ollama-provider agent against the roster (many player *names* are
  historical/misleading — e.g. `ollama-qwen3`'s real `model_id` is
  `ministral-3:3b`, not a qwen3 model). The real actively-serving model set:
  `ministral-3:3b` (2.95GB, backs ollama-qwen3 + gemini-2.5-flash),
  `qwen3:8b` (5.23GB, backs qwen3-8b-flash + ollama-llama + options-sosnoff +
  navigator + dayblade-sulu — 5 agents share it), `plutus-v1` (4.68GB, backs
  ollama-plutus). All three sizes are real `/api/tags` disk sizes (VRAM
  footprint tracks disk size closely for Q4 quants at short context — live
  `/api/ps` right now shows plutus-v1 resident at 4.57GB VRAM vs 4.68GB
  disk, confirming this).
- **Worst-case concurrent residency (all three loaded at once — realistic
  given the fleet runs multiple scan threads in parallel, not one agent at a
  time): 2.95 + 5.23 + 4.68 = 12.86GB.**
- **Headroom under that worst case: 16 − 12.86 ≈ 3.14GB.** (Live snapshot
  right now, market closed, shows only plutus-v1 resident at 4.57GB — i.e.
  ~11.3GB free at this exact moment — but that's not the market-hours
  worst case and shouldn't be the planning number.)

**Candidates checked (real Ollama library sizes, not parameter-count
labels):**
| Model | Disk size | Fits 3.14GB worst-case headroom? | Notes |
|---|---|---|---|
| qwen3.6:27b-q4_K_M (smallest Qwen3.6 exists) | 17GB | NO | ruled out above |
| gemma3:4b (already pulled) | 3.34GB | NO (exceeds headroom) | — |
| **qwen3:4b** | **2.5GB** | **YES, ~0.6GB margin** | **RECOMMENDED** — same Qwen3 lineage already deployed fleet-wide (qwen3:8b/14b, qwen3.5:9b), drop-in via the existing `OllamaProvider` wrapper, hybrid-thinking small model, benchmarks ahead of similarly-sized non-Qwen3 models on reasoning/math |
| qwen3:1.7b | 1.4GB | YES, ~1.7GB margin | more conservative fallback if 0.6GB margin is judged too tight |
| llama3.2:3b | 2.0GB | YES, ~1.1GB margin | weaker than qwen3:4b on reasoning benchmarks despite similar size; not recommended over qwen3:4b |

**Recommendation: `qwen3:4b` (2.5GB).** Real margin under the worst-case
concurrency assumption is ~0.6GB — genuine but not huge; flagging this
honestly rather than overstating confidence. `qwen3:1.7b` is the safer
fallback if more margin is wanted at the cost of some capability.

**✅ ADMIRAL GO 2026-07-06 — `qwen3:4b` approved, conditional on a measured
(not estimated) co-residency load test, since disk size ≠ runtime VRAM
(KV cache + context buffers + CUDA overhead sit on top of weights).**

**Load test performed 2026-07-06 ~18:40 MST:**
1. `ollama pull qwen3:4b` on Ollie Max — confirmed via live `/api/tags`:
   **2.497GB** actual pulled size (matches the 2.5GB estimate).
2. Built a REAL representative prompt from
   `engine.providers.base.AIProvider.build_prompt()` with realistic sample
   news/indicators/portfolio data — **13,747 chars (~3,436 tokens)**, not a
   trivial "hi" — matching actual production signal-generation prompt shape.
3. Fired concurrent `/api/generate` calls to all 4 models
   (`ministral-3:3b`, `qwen3:8b`, `plutus-v1:latest`, `qwen3:4b`,
   `keep_alive=3m` each) while polling `nvidia-smi --query-gpu=memory.used`
   on Ollie Max every 0.5s over a single persistent SSH session (avoids
   per-call SSH handshake overhead skewing the sampling). All 4 calls
   succeeded (wall times 1.5s–29.0s; `qwen3:4b` slowest, ~17.2k-char
   response).
4. **Measured nvidia-smi peak during concurrent use: 12,684 MiB**
   (transient, during simultaneous model-load); **settled/sustained value
   with all 4 within their keep_alive windows: 11,962 MiB** (held steady
   for the remainder of the polling window, confirmed plutus-v1's 3-min
   keep_alive hadn't yet expired at that point).
5. **Headroom at measured peak: 16,303 − 12,684 = 3,619 MiB (~3.5GB) — well
   above the 500 MiB threshold.** `qwen3:4b` **PASSES**. No fallback to
   `qwen3:1.7b` needed.

(Note for the record: individually-reported per-model `size_vram` via
`/api/ps`, checked ~3 min after the test once plutus-v1's keep_alive had
already expired, summed higher than the observed nvidia-smi total for the
3 remaining models — a real discrepancy between Ollama's self-reported
per-model figures and actual GPU-level accounting, not investigated further
since it doesn't change the pass/fail outcome and nvidia-smi is the
ground-truth instrument per the Admiral's instruction.)

**CONFIRMED for tomorrow's bundled window, no further approval needed per
Admiral's standing instruction.** `qwen3:4b` is pulled and ready; the
`ai_players` INSERT (auditioning seat) rides the 2026-07-07 after-close
restart per the checklist above, same precondition (today's open must show
a live order end-to-end under enforcement=ON first).

---
## 🟡 HM-GEX-OVERLAY-BATTLESTATION-MIGRATION — filed 2026-07-06, decision needed, NOT actioned

Companion to P4.13 (`AFTER-CLOSE-WORK-ORDER P4` above). `engine/
scan_context.py::get_gex_context_for_prompt()` (every agent's every scan
prompt) has been repointed tonight to `engine.canonical_gex`/
`engine.options_flow_gex` — that part is done. **Deliberately NOT migrated
tonight, per Admiral instruction:** `engine/battle_station.py`'s 5 direct
`engine.gex_overlay` call sites, all still reading the same frozen-since-
2026-05-30 `gex_levels` table:
- `battle_station.py:364-370` — `get_latest_gex`/`calculate_gex`/
  `_save_gex_levels`, inside the actively-scheduled (`main.py:4240`, every
  2 min) `run_battle_station_monitor`.
- `battle_station.py:574-575` — `_fetch_yahoo_chain` (a different helper,
  not GEX-snapshot-store-dependent — lower priority, just flagging it lives
  in the same file).
- `battle_station.py:817-818` and `:914-915` — two more `get_latest_gex`
  reads, same monitor.

**Recommendation:** migrate the three `get_latest_gex` read sites
(364, 817, 914) to the same `canonical_gex`/`options_flow_gex` read shape
used in tonight's `scan_context.py` fix — narrow, mechanical, same pattern
already proven tonight. The `calculate_gex`/`_save_gex_levels` write path
at 364-370 (Battle Station's own fresh-compute-and-persist fallback) is a
separate question: either let it keep writing to the legacy `gex_levels`
table as a Battle-Station-specific fallback (accepted as a deliberately
separate, narrower-blast-radius data path), or retire it too once Battle
Station also reads canonical data. Needs an explicit Admiral call — not
assumed either way. `_fetch_yahoo_chain` (574) is unrelated to this
migration question, no action needed there.

**Why this is safe to defer:** unlike `scan_context.py` (feeds every
agent's every decision), Battle Station's blast radius is narrower and
already-scoped to its own monitor — staleness here doesn't silently
contaminate the whole fleet's prompts the way P4.13's main finding did.

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
## 🔴 HM-EDGE-PROVENANCE — BOTH incumbent auditions SUSPENDED (2026-07-05, supersedes item 2 above)

**The "84 real closed CSP trades" / "16 real trades" language above was
accurate about the trades table but wrong about what those trades ARE.**
A same-day venue-provenance audit (HM-EDGE-PROVENANCE) traced every writer of
`options_trades`/`trades` and found:

- **options-sosnoff: 100% internal-simulation, zero broker contact, ever.**
  `engine.options_exec.py`'s own docstring: *"NO broker API is called."* Her
  "premium" is `estimated_premium = round(price * min(0.08, vix/500.0), 2)` —
  a VIX-scaled formula, not a fetched quote. None of her 84 rows has ever had
  a `broker_order_id`. The +$29,868.74 figure referenced above (and in
  HM-TROI-MAXPOS-CAP-DEAD) is what a formula produced against no real market
  at all — not evidence of edge.
- **qwen3-8b-flash: also 100% `execution_type='simulated'`, zero
  `alpaca_order_id`**, verified across all 16 clean-window trades. Same
  disease, different mechanism: the equity-routing path works for 163 *other*
  fleet trades (ollie-auto/neo-matrix/guardian-of-forever), she just isn't
  being routed through it. "Just short of the 20-trade audition floor" above
  was measuring a heuristic, not a trader.

**Admiral ruling (2026-07-05):** counting formula-priced or unrouted trades
toward an audition measures a heuristic, not a trader. Both audits are
**SUSPENDED as originally defined.** Redefined: an audition trade only counts
if it carries real broker evidence. Displayed count stays **0/20 for both**
— the number doesn't change, but its meaning does: not "quiet," but **"not
yet routable."** Each clock restarts once broker routing is confirmed live
for that agent's structure (HM-ROUTE-TO-BROKER, addendum item 8, promoted to
top of Phase 2 — not started as of this ruling). No new floor/deadline is
set until then; the original 2026-08-15/2026-08-16 dates above are
historical record of the pre-ruling design, not active deadlines.

Implemented in `engine/crew/audition_tracking.py` (now requires
`broker_order_id`/`alpaca_order_id` for any trade to count, for both
incumbents AND the bench-candidate blind-spot fallback — the "no broker
evidence, doesn't count toward anything" principle is fleet-wide, not
incumbent-specific) and wired into `weekly_tuning_crew.py`. `scripts/
eod_report.py` reports `SUSPENDED — 0/20 broker-executed (pending broker
routing)` for both going forward.

**Broker-history backfill (2026-07-05, new priority, outranks the haircut
test — do this before haircutting sim trades):** Alpaca's own order/fill
history showed REAL executed options fills (SPY/TSLA contracts) for
`dayblade-0dte`/`dayblade-sulu` that are locally mistagged
`execution_type='simulated'` (writeback bug — the broker fill happened, the
local ledger never learned about it), and `strategy:bull_spread_v1` has 15
rows with a real `broker_order_id` but `pnl` is NULL on every closed one
(never computed/written). Together with the ~$1,266 gap between Alpaca's
real +$1,064.08 lifetime equity and the −$202.56 currently locally
attributable, this is the recovery path to the fleet's only Tier-A per-agent
records. Ticketed as its own item — the mistagging bug gets a dedicated fix
proposal, not folded into this note.

Full report: this session's HM-EDGE-PROVENANCE interim findings (venue
classification method, full reconciliation numbers, headline answer) — ask
Scotty to reproduce if this doc entry isn't enough detail.

**Backfill progress (2026-07-05, follow-on session) — substantial, not complete:**
Corrected methodology first: MLEG (spread) orders carry `asset_class=None`
at the parent level, only legs are tagged `us_option` — a naive top-level
filter misses every real spread trade, which is exactly how last session's
first pass undercounted.

- **strategy:bull_spread_v1's 15 real spreads, priced:** all 15 local
  `broker_order_id`-carrying rows matched to real filled MLEG orders on SPY.
  Reconstructed entry credits from actual legs: **+$2,559.00 collected**,
  assuming OTM expiry (strikes 718-732 vs ~$744-748 spot — plausible, NOT
  independently verified against SPY's actual closing price each expiry
  date). The 4 local `failed_pre_fix` rows correctly match 4 real
  canceled/never-filled attempts — no correction needed there.
- **dayblade-0dte's TSLA trade: concrete proof of how bad the mistagging is,
  not just that it exists.** Local: entry $13.30, exit $3.80, same-day,
  `realized_pnl=-$29.00`. Real Alpaca fills: bought $9.55, sold the NEXT DAY
  at $1.34, real loss **-$821.00** — local ledger understated this one loss
  **28x** and got the exit date wrong by a full day.
- **Combined reconstructed options net: +$232.00** (+$2,559 spreads, -$2,327
  across 12 SPY + 2 TSLA single-leg orders). With the previously-found stock
  net (-$128.33), **≈+$104 now directly accounted for** against the real
  +$1,064.08 equity gain.
- **RESOLVED (2026-07-06 follow-on session) — gap closed to $1.06.** The
  April 22 lead was a dead end, correctly: checked `options_trades.strategy_id`
  (not just `agent_id`) — same 25 rows, but `exit_reason` (not previously
  queried) shows the 6 April 22 rows (one literally `symbol='TEST'`) all carry
  `exit_reason='HM-OPTIONS-TRADES-ZOMBIE-CLEANUP-reconcile-2026-05-18'` —
  confirmed zombie/test artifacts, already administratively cleaned up, not
  real unaccounted trades. All 25 local rows for `strategy:bull_spread_v1` are
  now fully classified (6 zombie, 4 `failed_pre_fix`/broker-never-received,
  15 real).
  The real gap was in **stock trades, not options**: computed the account's
  true realized P&L via clean accounting identity (equity − $100k base −
  unrealized = **$1,138.31**, can't be wrong regardless of local bookkeeping),
  then FIFO-reconstructed all 380 filled stock orders directly from Alpaca,
  bypassing local `execution_type` tagging entirely: **+$907.37 real stock
  P&L**, dominated by NVDA (+$842.22). Checked locally: every NVDA row across
  ~15 different agent IDs is tagged `execution_type='simulated'` — the exact
  same "broker fill happened, local ledger never learned about it" bug found
  in options, just far larger in dollars on the stock side.
  **Reconciliation: $907.37 (stock) + $232.00 (options) = $1,139.37 against
  the true $1,138.31 — a $1.06 residual.** Effectively closed. Per-agent
  attribution of the real stock P&L (which of the ~15 mistagged NVDA rows is
  the "real" one, and why none of the officially `ROUTED_PLAYERS`
  (`ollie-auto`/`neo-matrix`/`super-agent`) show a real NVDA row at all) is
  NOT done — separate, lower-priority follow-on if wanted. The aggregate
  reconciliation (does real edge exist, how much) is now solid.
- **Update (2026-07-06, per-agent attribution lead — client_order_id checked,
  dead end; timestamp/qty correlation instead cracked it):**
  Pulled all 31 real Alpaca NVDA orders directly (`b.client.get_orders`,
  same method as the FIFO reconstruction above) and printed each one's
  `client_order_id`. **Dead end as suspected worth ruling out:** every
  `client_order_id` is a random SDK-generated UUID with zero agent
  signature (e.g. `7edec3c9-2667-419c-b83f-4bb8501aad8e`) — confirmed by
  grepping the whole repo (`engine/alpaca_bridge.py`, `alpaca_options.py`):
  **no code path anywhere passes `client_order_id` on submission.** Alpaca
  auto-assigns it; there's nothing agent-derived to read back. Also ruled
  out the manual/trade-desk path: `trades` has zero `player_id IN
  ('trade-desk','captain-manual')` rows for NVDA, and the `_mirror_trade_desk_fill`
  writeback (`dashboard/app.py:12694`) correctly tags manual fills
  `execution_type='alpaca'` when it does fire — so manual desk orders are
  not the mystery here at all.
  **What worked instead: direct timestamp+qty correlation** between the
  31 real orders and the existing (mistagged `simulated`) local `trades`
  rows for NVDA. Matches are exact to the second on ~28 of 32 real fills:
  - **super-agent** — real BUY 3sh@$174.28 (03-24) + BUY 20sh@$179.13
    (03-25) = local BUY 23.0sh@$178.497391 (03-26 22:31) — the local price
    is the *exact* volume-weighted average of the two real fills
    ((3×174.28+20×179.13)/23 = 178.497391). super-agent's own local SELL
    of that 23sh @167.52 (03-28) has **no real counterpart** — the real
    position kept running in Alpaca past that date, so super-agent's local
    ledger and the broker diverged right there (local thought it flattened;
    broker never sold).
  - **ollie-auto** — 3 real fills 2026-04-09 18:31:07–25 (BUY 0.27, BUY
    0.27, SELL 0.55) match local rows at 18:31:07/08/25 to the second.
  - **gemini-2.5-flash** — 7 real SELLs 04-13→04-20 (1.99, 1, 0.5, 0.25,
    0.12, 0.06, 0.03 sh) match local SELL rows at the same timestamps
    to the second.
  - **neo-matrix** — 9 real BUYs + 3 real SELLs 04-20→04-22 match local
    rows to the second (this is the bulk of the position, ~9.7 net shares
    round-tripped).
  - **ollama-llama** — real SELL 2.25 (04-27 08:20) and 0.37 (04-27 20:24,
    limit, filled 3h later) match local rows at the same submit-timestamps.
  - **ollama-qwen3** — real SELLs 2.17/1.09/0.54/0.27 (04-29) match local
    rows within 5 seconds each.
  - **The final real SELL — 12.34sh @ $224.388103, 2026-05-20 18:16 — has
    NO local row anywhere.** `trades` has zero NVDA rows between 2026-05-13
    and 2026-07-01. Running the full 32-fill sequence in order nets to
    **exactly 0.00 shares** after this SELL (32.04sh bought = 32.04sh sold),
    confirming it's the true, total close of one single continuous real
    position — not a separate untracked position. FIFO P&L on the full
    31-order sequence: **+$842.23**, matching the ticket's cited +$842.22
    to the penny.
  **Conclusion:** this was never "which of 15 agents really made this
  trade" — it's **one real Alpaca position, and the local writeback bug
  scattered its fragments across whichever agent's simulated-ledger write
  happened to land at the same instant as each real fill** (consistent
  with the already-diagnosed "broker fill happened, local ledger never
  learned about it" bug, `HM-BROKER-HISTORY-BACKFILL` above). super-agent,
  ollie-auto, gemini-2.5-flash, neo-matrix, ollama-llama, and ollama-qwen3
  each own a slice by matched timestamp; **the closing 12.34sh/$224.39
  liquidation on 2026-05-20 is attributed to no agent at all** — a clean
  gap, not a mistag, and the largest single unattributed dollar chunk.
  Full per-order match table not transcribed here — reproducible by
  rerunning the same `get_orders` pull + join against `trades` on
  `(symbol, side, qty≈, |executed_at − submitted_at| < 5s)`.
- **BUILT (2026-07-06): `alpaca_real_fills` reconciliation table + venue tiering,
  per Admiral ruling.** Schema (additive, never overwrites local history):
  `scripts/hm_alpaca_real_fills_schema.py` — `alpaca_real_fills` table
  (broker truth: `broker_order_id`, `client_order_id`, symbol/asset_class,
  `matched_trade_id`/`matched_options_trade_id` nullable FKs, `match_method`,
  `match_confidence`, `notes` — a real fill with no local counterpart is a
  first-class row, not an omission) + `trades.venue` / `options_trades.venue`
  columns (`NULL`=unreviewed, `internal-sim`=reviewed/confirmed no broker
  fill, `broker-clean`=real fill, 1:1 attributable, `broker-commingled`=real
  fill fragmented across multiple agent_ids, excluded from both clean-sim
  evidence and Tier-A). **Admiral condition on `broker-clean`: Tier-A
  promotion must read P&L from `alpaca_real_fills`, not the local `pnl`/
  `realized_pnl` column** — clean attribution alone doesn't make the local
  number trustworthy (dayblade's TSLA fill was cleanly attributed AND still
  28x wrong locally). Pre-change snapshot: `data/trader.db.backup-2026-07-06-pre-alpaca-real-fills`.
  - **NVDA (61 real fills total across the session incl. this table): 31
    real orders reconciled.** 28 distinct local `trades` rows (super-agent,
    ollie-auto, gemini-2.5-flash, neo-matrix, ollama-llama, ollama-qwen3) ->
    `venue='broker-commingled'`. 3 rows reviewed and confirmed to have no
    real counterpart -> `venue='internal-sim'`. **Correction to the prior
    session's per-agent note:** precise second-level timestamp matching
    (not eyeballed) found **two** true orphan real fills, not one — the
    2026-05-20 12.34sh closing liquidation (already known) **and** a
    2026-04-14 08:00 real SELL of 1.99sh that has no local row within any
    plausible window (nearest qty-alike candidate is gemini-2.5-flash's
    `trades.id=1633`, but it's dated ~12h *before* the real order was even
    submitted — every other match in this set lands within 6 seconds, so
    this one was left unmatched rather than force-fit). Aggregate P&L
    unaffected either way (still reconciles to +$842.23 against the ticket's
    +$842.22).
  - **bull_spread_v1's 15 spreads: all 15 real MLEG opens confirmed 1:1**
    via their existing `broker_order_id` -> `venue='broker-clean'` on all 15
    `options_trades` rows. **Bonus find:** also reconstructed all 12 real
    single-leg CLOSE orders for these same 15 spreads (aggregated buybacks/
    sells across multiple rows at once, e.g. one real order buying back 4
    contracts closes 4 different rows' short legs simultaneously) — recorded
    as additional `alpaca_real_fills` rows (FK left null, notes list which
    `options_trades.id`s each closes; a genuine many-to-one relationship the
    single-FK schema doesn't try to force into 1:1).
  - **Correction to "dayblade's real fills" (2026-05-05/06/14/22 SPY, this
    was the "12 SPY + 2 TSLA single-leg orders, -$2,327" note from the prior
    backfill session):** re-pulling and matching by strike+qty shows **all
    12 SPY single-leg orders are actually bull_spread_v1's own close legs**
    (see above) — **zero** of them are dayblade. Only the **2 TSLA orders**
    (bought $9.55 2026-04-01, sold $1.34 2026-04-02) are genuinely
    dayblade-0dte — matched to local `trades.id=1565`/`1566`
    (`venue='broker-clean'`, single-agent, 1:1, no commingling). This is the
    exact TSLA trade already known from the prior session ("local realized_pnl
    -$29.00 vs real loss -$821.00, 28x understated") — now has concrete
    order IDs and the Admiral's P&L-from-`alpaca_real_fills` condition
    directly applies to it as the clearest example of why.
  - **Verification:** `alpaca_real_fills` has 60 rows (31 NVDA + 15
    bull_spread_v1 opens + 12 SPY closes + 2 TSLA). `PRAGMA integrity_check`
    = ok after each write. Scripts used (all dry-run-by-default, `--apply`
    to write): ad hoc, not yet consolidated into a single checked-in script —
    **follow-on TODO:** fold the three one-off match scripts into a single
    `scripts/hm_alpaca_real_fills_reconcile.py` if this reconciliation needs
    to run again (e.g. once `HM-ROUTE-TO-BROKER` ships and new real fills
    start accumulating).
  - **Not yet done:** `options-sosnoff`'s CSP wheel and any other
    `options_trades` strategy_id besides `bull_spread_v1` haven't been
    checked against Alpaca's real order history at all — this reconciliation
    only covered what the prior sessions had already flagged as suspect.
    Item 4 (haircut test) and the rest of item 5 (full leaderboard tier
    re-grade using this table) are still not started.
- **Fix proposed, not applied:** `paper_trader.py`'s
  `_update_trade_alpaca_fields`/`_persist_alpaca_fill` writeback exists for
  the stock path but has no equivalent for `alpaca_options.py`'s
  single-leg/mleg forward calls for `dayblade-0dte`/`dayblade-sulu`/
  `strategy:bull_spread_v1` — `execute_options_signal()`'s result dict isn't
  used to correct `execution_type`/prices the way the stock buy/sell path
  already does. Needs its own session to implement carefully, not a rushed
  patch.

---
## 🔴 HM-ROUTE-TO-BROKER — propose-first, promoted to top of Phase 2 (2026-07-05)

XO-DEPARTURE-HARDENING addendum item 8, promoted ahead of everything else in
Phase 2 by the HM-EDGE-PROVENANCE ruling above. Verbatim ask: **"route-to-
broker for every path that can reach Alpaca paper (options included if the
API supports the structures). From here forward, a trade that produces no
broker evidence doesn't count toward anything — auditions, gates,
leaderboards."**

Concretely: `options-sosnoff`'s CSP wheel (`engine/wheel_strategy.py` →
`engine/options_exec.py`, currently zero broker contact by design — see that
module's own docstring) and `qwen3-8b-flash`'s equity path (mechanism exists
generally — 163 other fleet trades already route real — she specifically
isn't being routed) both need a route to `engine/alpaca_options.py` /
`engine/alpaca_bridge.py`'s existing real `TradingClient`, the only code in
this repo that ever calls a genuine Alpaca order API. Check whether Alpaca's
paper options API actually supports single-leg CSP writes (it already
supports single options + verticals + iron condors per
`engine/alpaca_options.py`'s existing functions) before assuming the gap is
purely a wiring problem rather than an API-capability one.

**Do not implement without Admiral sign-off** — this changes what real (paper)
money the fleet places, not just what gets measured. Propose the design,
including how it composes with door1's leveraged-ETF CSP ban
(`LEVERAGED_ETF_TICKERS` in `engine/options_exec.py`) and the CSP notional
cap (`engine/risk_manager.py::csp_options_cap_breached`, commit `75b63f1`) --
both gates currently assume options never reach a broker; routing changes
that assumption and both need to keep holding.

---
## 🔴 HM-HAIRCUT-AND-EVIDENCE-TIERS — HM-EDGE-PROVENANCE items 4-5, not started (2026-07-05)

The interim report (this session) closed items 1/2/3/6 conclusively (venue
classification, Troi's answer, broker reconciliation, the no-hedging headline)
but explicitly deferred these two — flagged honestly rather than rushed:

**Item 4 — haircut test.** For every INTERNAL-SIM trade (the overwhelming
majority, per item 1's classification — see HM-EDGE-PROVENANCE entry above),
recompute P&L under conservative fills: options cross the spread (buy at
ask, sell at bid), equities fill at the NBBO touch, not mid. Report per-agent:
claimed P&L vs haircut P&L. Needs real per-trade repricing, not a rollup
query — the actual work not yet done.

**Item 5 — evidence-tier re-grade.** Re-grade the full leaderboard (all 80
`ai_players` rows, per `HM-ROSTER-RATIONALIZE`) into: **A** = broker-executed
(real `alpaca_order_id`/`broker_order_id`), **B** = internal-sim but
haircut-surviving (positive P&L even after item 4's conservative repricing),
**C** = internal-sim, unverifiable or haircut-killed. Propose explicitly how
the July 24 Door-1 G1-G4 kill gate should weight these tiers (a real, human
design decision — don't just pick a weighting silently).

**Update (2026-07-06): the backfill dependency is now resolved** —
`HM-BROKER-HISTORY-BACKFILL` above closed its reconciliation gap to $1.06.
Concretely for this ticket: `strategy:bull_spread_v1`'s 15 real spreads
(+$2,559, Tier-A candidates) and the real stock P&L hiding under ~15
mistagged agent IDs (dominated by a real +$842.22 NVDA position, currently
attributed to no specific agent) both need per-agent attribution before
this re-grade can assign tiers correctly — an agent currently reading
INTERNAL-SIM in the leaderboard may actually be Tier-A once that
attribution lands. `HM-ROUTE-TO-BROKER` is a separate, forward-looking
dependency (routing future trades) and still applies independently.

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

---
## 🟢 HM-OPS-SENTINEL — SHIPPED 2026-07-06 (AFTER-CLOSE-WORK-ORDER P1.1/P1.2, prevention build)

Independent watchdog for the failure mode behind today's morning lock storm
(`HM-SQLITE-CONN-FD-LEAK`/`HM-RIKER-SYNTHESIS-LOCK-CONTENTION` above): leaked
connections → FD growth → lock contention → more failed writes → more leaks.
Runs on its own cron cadence, not main.py-resident, so a main.py hang can't
also silence its own watchdog (doctrine: "alarms must not share a failure
mode with what they watch," CLAUDE.md).

`scripts/hm_ops_sentinel.py` — four independent checks, own alert_type/rate
limit each (via `engine.alert_channels.send_alert`, 1800s per-type cooldown):
1. **FD count** on `data/trader.db`(+`-wal`/`-shm`) for the main.py PID via
   `lsof`. WARNING >60, RED_ALERT >120 (Admiral-specified thresholds).
2. **rikers_log heartbeat age** — RED_ALERT if >25min stale during market
   hours (`RiskManager.is_market_hours()=='market'`).
3. **"database is locked" occurrences** in `trader_error.log` — WARNING on
   any. Uses a persisted byte-offset checkpoint (`data/.hm_ops_sentinel_state.json`),
   NOT a wall-clock time window — the log format is `HH:MM:SS` with no date
   and spans multiple days between weekly rotations, so a stale line from a
   prior evening (e.g. `21:31:58`) can read as numerically later than "now"
   and falsely look recent under a naive time-window comparison. First-run
   caught this live (reported 8 hits that were actually from a much earlier
   session); fixed before shipping.
4. **signals_v2 queue depth** (`HM-SIGNALS-V2-FIFO-STARVATION` above) —
   WARNING if pending>3000 or oldest-pending age>48h.

**Calibration finding from today's own verification run — worth the
Admiral's attention:** post-restart (10:05:27), FD count climbed from 6 to a
**plateau around 140-170** by ~12:10 (steady/fluctuating over repeated
30-90s samples, not monotonic) — well past the requested 120 red threshold,
during completely normal market-hours operation, with zero "database is
locked" errors in the same window. A literal `>120` check would RED-alert
on a healthy day. Rather than silently override the Admiral's specified
numbers, the FD check now also tracks a persisted per-PID growth rate and
includes it in the alert text (`+N over M min`) — a real leak reads as
sustained positive growth; healthy load reads as flat/oscillating. **Open
question for the Admiral:** keep 120 as-is (accept that RED will fire most
active trading sessions and rely on the trend annotation to triage), or
raise the red threshold to something nearer today's observed ~170 healthy
ceiling. Not changed unilaterally — flagging for a decision.

**Also found still-open, not fixed here (out of scope for this ticket):**
`navigator` (`halt_mode='exit_only'`) continues emitting new pending
`signals_v2` rows daily (it's a live source, not fixed by the one-time
`scripts/hm_signals_v2_expire_halted_backlog.py` cleanup) — the FIFO-
starvation backlog will partially re-accumulate day over day until the
priority-lane/age-ordering fix (option 2 in `HM-SIGNALS-V2-FIFO-STARVATION`
above) is actually built. The queue-depth check here will keep this visible
rather than let it go silently stale again.

Not yet added to crontab — held for the single bundled-changes review
alongside `HM-WAL-BUSY-TIMEOUT-HYGIENE` below, per tonight's sequencing.
Suggested line (5-min cadence, independent of main.py):
```
*/5 * * * * cd /Users/bigmac/autonomous-trader && /Users/bigmac/autonomous-trader/.venv/bin/python3 /Users/bigmac/autonomous-trader/scripts/hm_ops_sentinel.py >> /Users/bigmac/autonomous-trader/logs/hm_ops_sentinel_cron.log 2>&1
```

---
## 🔴 HM-WAL-BUSY-TIMEOUT-HYGIENE — filed 2026-07-06 (AFTER-CLOSE-WORK-ORDER P1.3), PROPOSAL ONLY, no changes applied

Per-connection SQLite hygiene audit, prompted by today's lock storm. Current
state on `data/trader.db`, checked directly:
```
journal_mode = wal        (already set -- persists in the DB file header,
                            every connection inherits it automatically)
synchronous  = 2 (FULL)   (per-connection setting, does NOT persist --
                            must be set on every connect() to take effect)
busy_timeout = 5000ms     (Python sqlite3's own default when no timeout= is
                            passed to connect(); also per-connection)
```

**Scope is much larger than the FD-leak sweep.** That sweep (98 sites/24
files) touched only the `with ... as c:` anti-pattern. This is a different
axis — every one of the **~602 raw `sqlite3.connect(...)` call sites**
across `engine/`, `dashboard/`, `agents/`, `scripts/`, `main.py` opens its
own connection with its own (often copy-pasted) settings, and there is no
shared connection helper today — **120 files each define their own local
`_conn()`.** Concretely:
- `PRAGMA synchronous` is set explicitly in only **5** files repo-wide
  (`main.py`, `engine/rallies_scraper.py`, `engine/alpha_signals.py`,
  `engine/portfolio_monitor.py`, plus one venv package) — everywhere else
  gets SQLite's FULL default.
- Of connect() calls that pass `timeout=`, values are inconsistent and in
  some cases *worse* than today's post-incident standard: 184 already use
  `30`, but 41 use only `5`, 22 use `15`, 9 use `20`, and a few outliers use
  `2`/`3`/`45`/`60`. ~200+ pass no `timeout=` at all (bare 5s default —
  exactly what was too short during this morning's storm before the
  `riker_synthesis.py` fix bumped it to 30).

**Proposed fix (NOT applied — needs Admiral sign-off):**
1. Add one shared helper, e.g. `engine/db_conn.py::get_conn(readonly=False)`,
   that opens with `timeout=30`, then issues `PRAGMA synchronous=NORMAL`
   (safe with WAL already on — NORMAL only relaxes the fsync that WAL mode's
   own commit protocol already makes optional; FULL is stricter than WAL
   mode needs and is the actual reason every writer pays extra fsync cost
   during the exact contention windows that caused today's storm).
   `journal_mode` does NOT need to be re-set per-connection (already durable
   on the DB file) — do not add a redundant `PRAGMA journal_mode=WAL` per
   call site, it's a no-op that only adds another statement to every hot path.
2. Migrate call sites incrementally, hot-path-first (same file list as the
   FD-leak sweep is the natural starting set — already known write-heavy),
   NOT a repo-wide mechanical sweep in one shot. 602 sites is 6x the FD-leak
   sweep's blast radius; a single scripted pass with the same review rigor
   (recall the FD-leak sweep caught 7/98 sites that needed a hand-added
   `.commit()`) is a much bigger single-session risk. Recommend phasing:
   Phase 1 = the 24 files already touched by the FD-leak sweep (known
   hot-path, already reviewed once this session), Phase 2 = everything else,
   on its own later schedule.
3. Each migrated file keeps its own local `_conn()` name (don't force an
   import-rename refactor across 120 files in the same pass) but has its
   body replaced to delegate to `engine.db_conn.get_conn()` — minimizes diff
   size per file while still centralizing the actual PRAGMA/timeout policy.

**Rollback:** trivial per-file (revert to the old bare `sqlite3.connect(...)`
call) since the helper is additive, not a schema or data change. No DB
migration involved — `synchronous=NORMAL` and `busy_timeout` are session-
scoped PRAGMAs, not persisted state, so there is nothing to "undo" at the
database level even without a code revert; the DB just reverts to its old
per-connection defaults.

**Gotchas flagged for the Admiral's review:**
- `synchronous=NORMAL` under WAL means a hard power loss (not a process
  crash — an OS-level outage) could lose the most recent commits that
  hadn't yet reached the WAL file's own durability point, though the DB
  stays structurally consistent either way (WAL mode's own guarantee, not
  affected by the synchronous level). Given this box already has UPS +
  power-loss-restore doctrine (`CLAUDE.md` Shelly Plug section) and nightly
  `scripts/db_snapshot.sh` + `scripts/offhost_backup.sh`, the residual risk
  window is a live power-loss during the exact multi-hour gap between
  snapshots — same risk class already accepted elsewhere in this project's
  backup posture, not a new category of exposure.
- `wal_autocheckpoint` (currently default 1000 pages) interacts with
  `synchronous=NORMAL` — checkpoints happening more/less often changes how
  much uncommitted WAL content could theoretically be at risk. Not proposing
  a change to `wal_autocheckpoint` in this pass; flagging that it's the
  related knob if the Admiral wants to revisit checkpoint cadence later.
- `scripts/db_snapshot.sh`/`scripts/offhost_backup.sh` back up the file
  directly — confirm (not verified in this pass) they either checkpoint
  first or copy `-wal`/`-shm` alongside the main file, since a raw copy of
  `trader.db` alone without its WAL sidecar could be missing recent commits
  regardless of the synchronous setting. Worth a quick read before or
  alongside this change, not blocking it.

**Nothing applied.** Waiting for Admiral sign-off before writing
`engine/db_conn.py` or touching any of the 602 call sites.

---
## 🟢 AFTER-CLOSE-WORK-ORDER P4 — ticket confirmations (2026-07-06), report-only

Four verify-only checks, no code changes except where noted.

**Item 10 — exit_only skips zero-position agents (Sulu/Uhura waste): CONFIRMED
RESOLVED, already fixed the day before this session.** DB-verified today:
`dayblade-sulu` and `ollama-llama` (Uhura) are both `halt_mode='exit_only'`
with **zero open positions**. Both waste vectors are already closed:
(1) both were removed from `main.py`'s `_SCAN_TIER1`/`_SCAN_TIER2` on
2026-07-04 per `HM-AGENT-RULES-CONSOLIDATION` (comment at `main.py:229-238`:
"exit_only never opens new positions... never scanned anyway"); (2)
`engine/guardian_sweep.py`'s exit-management sweep is explicitly scoped to
"every halt_mode='exit_only' player **currently holding a non-zero
position**" (`guardian_sweep.py:36`), so it correctly skips both. No action
needed.

**Item 11 — Fleet-tab Calls/day trading vs advisory/briefing split
(cto-grok42): CONFIRMED as a real, not-yet-built gap.** The Fleet tab's
"Calls/day" column (`dashboard/app.py` `/api/model-control`, field
`api_calls_today`) reads `model_stats.api_calls` — one undifferentiated
counter per player per day. But the finer-grained data already exists:
`api_costs.call_type` for `cto-grok42` shows a genuine mix —
`scan`=50, `trade_grade`=4 (trading), `cto_pre_market`=17,
`cto_pre_close`=13, `cto_post_open`=5, `cto_post_close`=17 (briefing),
`chat`=114, `journal`=141 (other) — all lumped into one number on the Fleet
tab today. Building the split itself (a Fleet-tab UI change) is out of
scope for tonight's report-only pass; flagging that the underlying data is
already there (`api_costs.call_type`), so the fix is a query/display change,
not a new instrumentation project.

**Item 12 — spam_rate_pct/model_scores visibility for the July 24 kill-gate:
CONFIRMED readable, but via direct SQL only — no dashboard surface.**
`model_scores.spam_rate_pct` is written ONLY by `engine/crew/
weekly_tuning_crew.py::run_weekly_tuning()` (fires Sunday ~21:30 MST only —
note: NOT literally "8:30", closest scheduled run this repo has to that
description; `engine/crew/daily_review_crew.py`'s daily ~20:1x run also
writes `model_scores` but its `_save_score()` INSERT doesn't include
`spam_rate_pct` at all, so daily-period rows are always NULL there by
design, not a bug). **Zero `period='weekly'` rows exist in `model_scores`
yet** — consistent with `HM-TUNING-CREW-REPAIR` (this session, 2026-07-06)
just having fixed the silent-zero-output bug that was likely eating prior
weekly runs; the first real post-fix data point lands this coming Sunday.
No dashboard/API endpoint reads `model_scores` or `spam_rate_pct` at all
(`grep` came back empty) — today, "readable" means direct `sqlite3 data/
trader.db` query only. Worth knowing before July 24 so nobody goes looking
for this on the Bridge and concludes the data doesn't exist.

**B6 (2026-07-06) re-check before July 24:** re-verified the "8:30" phantom
schedule doesn't appear uncorrected anywhere else in this file — every
other tuning-crew time reference (lines ~896, 904, 916, 1095-1097) already
consistently says 9:00-9:30 PM / ~21:30 MST, matching the correction above.
Nothing left to fix here; confirming rather than re-writing.

**Item 13 — gex_overlay.py/gex_levels official retirement per
HM-GEX-CANONICAL: NOT actually fully retired — correcting the record.**
The 2026-05-31 entry above is accurate for what it claims ("gex_overlay now
has ZERO live refs **in app.py**") but that scope was narrower than "fully
dormant" reads — two live, non-dashboard consumers remain, found today:
1. `engine/battle_station.py` (lines 363, 573, 816, 913) imports
   `get_latest_gex`/`calculate_gex`/`_save_gex_levels`/`_fetch_yahoo_chain`
   from `engine.gex_overlay` directly. `run_battle_station_monitor` is
   actively scheduled (`main.py:4240`, every 2 min) — this is live, not dead.
2. `engine/scan_context.py::get_gex_context_for_prompt()` (imports from
   `engine.gex_overlay` at line 109) injects a GEX regime block into **every
   AI agent's scan prompt**, every cycle — this is the per-player context
   builder used fleet-wide, not a narrow path.

**Concrete consequence, not just a dead-code technicality:** because
`run_gex_overlay_update`'s refresh scheduler is deliberately disabled
(`main.py:4405`, `# DISABLED HM-GEX-CANONICAL`), `gex_levels` has been
frozen since **2026-05-30**. `get_gex_context_for_prompt()`'s own fallback
("if no DB data exists, attempt a fresh calculation") never triggers,
because stale data still counts as present — so **every AI agent's trading
prompt has been fed a 5+-week-stale GEX regime label** this whole time,
while the Bridge UI shows fresh canonical data from a completely different
source (`engine.canonical_gex`/`options_flow_gex`). This is exactly the
cross-panel-disagreement failure mode `HM-GEX-CANONICAL` was meant to
eliminate — it just wasn't fully swept into this one prompt-building path
in May. **Not fixed tonight** (repointing a hot path that runs on every
single agent's every scan cycle deserves its own reviewed change, not a
same-night bundle) — filing as its own open item. Suggested fix shape:
repoint `get_gex_context_for_prompt()` to read `engine.canonical_gex` the
same way `dashboard/app.py::_canonical_gex_cached` does, and separately
decide whether `battle_station.py`'s `gex_overlay` calls should migrate too
(narrower blast radius, only affects Battle Station-specific features) or
are acceptable as a legacy, deliberately-separate data path — needs an
explicit decision either way rather than assuming "retired."

---
## 🟡 B3 — evaluator "on next restart" stale copy — FILED, not found in the 30-min budget (2026-07-06)

Per the WRAP-UP DIRECTIVE's own rule ("if any turns out >30 min or touches
more than you expect, STOP, file it, move on"): searched `dashboard/app.py`'s
`/api/measurement-health` endpoint (the one that reports `signal_evaluator`
status — the most likely home for this per `ALPHA READ`'s "measurement-
health→ntfy RED thresholds" being the adjacent open item), `engine/
signal_evaluator.py`, `engine/proving_ground.py`, both bridge HTML files, and
a repo-wide grep for "restart" near every evaluator-touching file. **Did not
find a literal "on next restart" (or close variant) string anywhere.**
Possibilities, not resolved: (1) it was already fixed/removed between the
2026-06-29 `ALPHA READ` note and now (real churn happened in between —
`HM-EDGE-PROVENANCE`, `HM-EXEC-PIPELINE`, this session's own measurement-
health work); (2) it's phrased differently than the note's paraphrase and
needs the Admiral (or whoever wrote the original `ALPHA READ` note) to point
at the exact string/file. Not fixed tonight — flagging rather than guessing
at the wrong target.

---
## 🟡 B5 — "GEX Snapshot" source-grid freshness card — FILED, not located (2026-07-06)

Same outcome as B3, same rule applied. Searched for a "Source Grid" /
"GEX Snapshot" card across `dashboard/static/index.html`,
`dashboard/static/bridge-v2.html`, and `dashboard/app.py`: checked every
`/api/gex*` endpoint's `source` field text (`"gex-snapshot canonical (...)"`
— the closest textual match found), the Metals/net-worth freshness renderer
(`fetchUnifiedNetWorth`, uses a `d.freshness` object — structurally the kind
of thing a "Source Grid" would be, but scoped to net-worth/metals only, no
GEX entry), and every `UNKNOWN`-label site in both HTML files and
`dashboard/app.py`. **Did not find a distinct multi-source freshness grid
with a "GEX Snapshot" card that goes UNKNOWN/green.** Likely named
differently than either "Source Grid" or "GEX Snapshot" verbatim, or lives
in a file not yet checked (`swingdesk/`, `signal-center/`, or a bridge panel
not grepped by these exact terms). What I CAN confirm instead: the new
collector (`scripts/hm_gex_daily_collect.py`, `HM-GEX-COLLECTOR-2026-07-06`)
writes into the exact same `gex_snapshots` table that
`engine.canonical_gex.latest_snapshot()` reads (verified — same table, same
schema, same file path) — so whatever freshness card exists, if it derives
from that read path (directly or via `/api/gex/{symbol}`), it WILL pick up
tomorrow's 13:05 write correctly by construction. The open question is
purely "where does the UI surface this," not "does the pipeline work."

---
## 🟢 AFTER-CLOSE-WORK-ORDER P3.8 — Kirk Advisory ALL THREE slots missed today, real bug found and fixed (2026-07-06)

Checked all three of today's fixed slots (06:35, 09:30, 13:05) against
`data/kirk_advisory.heartbeat` and `logs/trader.log`'s "Kirk Advisory: firing
slot" lines. **All three missed** — heartbeat mtime unchanged since
2026-07-03 13:11:17, zero "firing slot" log lines today at all. The prior
note above ("two more chances today") assumed 09:30/13:05 would self-recover
once the restart-timing miss on 06:35 was past; they didn't.

**Root cause found — not incident residue, a real scheduling bug.** Traced
`schedule.every(30).minutes.do(run_kirk_advisory_job)` (`main.py:4548`)
against the actual post-restart `[WR-DEBUG-INIT]` registration dump in
`logs/trader.log`: after the 10:05:27 restart, this job's first tick was
scheduled for **10:17:28** (not immediately at boot), then every 30 min
after — 10:47:28, 11:17:28, 11:47:28, 12:17:28, 12:47:28, **13:17:28**.
`run_kirk_advisory_job`'s own slot check only fires within a **10-minute
window** per slot (`sched_mins <= now_mins <= sched_mins+10`, e.g.
13:05-13:15 for the close slot) — and 13:17:28 lands **2 minutes past** that
window's close. Same story for 09:30 (would need a tick at :30-:40, the
actual ticks are :17/:47) and 06:35 (:35-:45 vs :17/:47). **A 30-minute
poll cadence checking a 10-minute window can systematically miss every
single slot, every day, if the post-restart phase offset happens to land
wrong** — and today it did, for all three, purely from the 10:05:27 restart
time's phase, with zero relation to this morning's lock storm. This would
have kept missing indefinitely (or until a future restart happened to
re-phase luckily) without a restart-independent fix.

**Fixed:** `main.py:4548` changed `schedule.every(30)` → `schedule.every(5)`.
A 5-minute poll is strictly less than the 10-minute window, so it's
mathematically guaranteed to land inside every slot window regardless of
restart phase. `py_compile` clean. Takes effect on tonight's bundled restart.

**Same latent risk found, NOT fixed tonight (out of scope for this
ticket):** `run_cto_advisory` (`main.py:4547`, `_CTO_SCHEDULE` at
`main.py:2906-2911`, 4 fixed daily slots) uses the **identical** 30-min-poll
pattern — same vulnerability class, not independently confirmed as
currently missing slots today, but structurally exposed the same way. Worth
the same `every(30)`→`every(5)` fix in a follow-up pass; flagging rather
than fixing blind tonight since it wasn't part of tonight's ask and deserves
its own confirm-then-fix pass like this one got.

**P3-8 REOPENED 2026-07-07 06:5x MST — real second bug found, distinct from
the scheduling fix above.** Admiral escalation: 06:35 slot appeared not to
self-heal, PLUS signal-center's Ship's Log showed `fire-kirk` TRIGGERED +
EXECUTED at 07-07 03:21/03:22 with zero heartbeat change — proving the
producer runs but its output doesn't always land.

**Two things resolved:**
1. **The 06:35 scheduled slot actually DID self-heal** — `trader.log` shows
   `[2026-07-07 06:39:50] Kirk Advisory: firing slot 0635...` /
   `[06:39:53] Kirk Advisory [0635]: computed + heartbeat` — the aa55f1d
   5-min-cadence fix worked exactly as designed (fired ~5 min into the
   06:35-06:45 window, the expected worst case). Heartbeat mtime confirmed
   fresh (`Jul 7 06:39:53`) before this investigation even started. The
   Admiral's escalation was likely a check that landed in the ~5-min gap
   before the next tick — the scheduling fix is NOT the reopened issue.
2. **`fire-kirk` is a REAL, separate bug — root cause found and fixed.**
   `signal-center/server.py`'s `/api/morpheus/action/fire-kirk` (admin
   manual trigger) POSTs to `GET http://127.0.0.1:8080/api/kirk/advisory`
   (`dashboard/app.py:18731`), which calls `engine.kirk_advisory.
   generate_kirk_advisory()` **directly** — a completely different code
   path from the scheduler's `run_kirk_advisory_job()` wrapper in
   `main.py`. The heartbeat touch was defined ONLY inside that scheduler
   wrapper (`_KIRK_HEARTBEAT` was a `main.py`-local constant), so any
   successful manual fire-kirk call computed and returned real data (hence
   "EXECUTED" in the Ship's Log) but never proved liveness to W1 or the
   source grid. Write path had silently diverged from what the read path
   checks — exactly the Admiral's hypothesis.

**Fix (code-complete, verified in isolation, rides today's bundled
restart):** moved heartbeat ownership into the shared compute function
itself. `engine/kirk_advisory.py` now defines its own
`_KIRK_HEARTBEAT_PATH` (same absolute path, computed the same way as the
existing `REAL_HOLDINGS_PATH` constant in that file) and a
`_touch_kirk_heartbeat()` helper, called at BOTH success-return points
inside `generate_kirk_advisory()` (the stale-holdings early return AND the
main advisory-result return) — NOT in the exception handler, preserving
the original "failure doesn't count as liveness" design intent. `main.py`'s
scheduler-side touch is untouched (now redundant on the scheduled path,
harmless). Verified: called `generate_kirk_advisory()` directly (exact
simulation of what fire-kirk does) — heartbeat mtime updated immediately,
no error. `tests/test_kirk_holdings_guard.py` (12 tests) + full suite both
still green. **Not yet live** — the running process (restarted 06:50 MST,
cause unrelated/not investigated) has the scheduling fix but not this
heartbeat-ownership fix yet; lands at today's single bundled restart per
instruction.

---
## 🟢 AFTER-CLOSE-WORK-ORDER P2.4-P2.6 — post-restart verification note (2026-07-06)

Verified live against the 13:21:30 bundled restart (PID 8170):
- **P2.4 GEX cache warm confirmed working** — `/api/market/gex/QQQ` and
  `/api/gex-overlay/levels?symbol=QQQ` returned real warmed data immediately
  post-restart, no "pending"/blank. **SPY specifically still shows "no
  cached GEX available"** — NOT a defect in the warm-cache fix; `engine.
  canonical_gex.latest_snapshot('SPY')` itself returns a degenerate snapshot
  (`call_wall==put_wall==king_node==740.0`, dated **2026-06-05**, a month
  stale) that the pre-existing collapsed-wall guard correctly refuses to
  serve (same guard `_canonical_gex_cached` already had). QQQ's snapshot
  from the same date is fine (`call_wall=710 != put_wall=707`). Since this
  restart happened after market close, Phase 2 (live background refresh)
  correctly skipped itself too — nothing to fetch live after hours. Net:
  the fix works; SPY's Gamma Map will stay blank until either the market
  reopens (Phase 2 will populate it live) or `flow_gex.db` gets a fresh
  non-degenerate SPY row. Not investigated further tonight (separate,
  pre-existing data-quality issue, not tonight's scope).
- **P2.6 confirmed** — `/api/account/equity-curve` returned correct data
  (17 dates, +1.49% account) on a cold post-restart cache, single Alpaca
  call pair, no 429.

---
## 🟢 HM-WAL-BUSY-TIMEOUT-HYGIENE — wave 1 SHIPPED 2026-07-06 (Admiral-approved, phased)

Admiral ruling: build `engine/db_conn.py` first, migrate hot paths (engine,
signal-center, dashboard) as wave 1, run one full trading day clean, then
sweep the remaining call sites as wave 2. Full backup before wave 1. Do not
touch all 602 sites in one pass. Also ruled on the FD sentinel threshold
(P1.1 above): keep 120 as a concept but raise the numbers — warn=150,
red=250 — since the observed healthy plateau today was 140-170; revisit
after a week of real sentinel data.

**Pre-wave-1 backup:** `data/backups/trader_pre_wal_migration_20260706_153104.db`
(SHA256 `144b9c17...`) + `data/backups/signals_pre_wal_migration_20260706_153104.db`
(SHA256 `b6ec39ff...`), taken before touching anything.

**`engine/db_conn.py`** — one function, `get_conn(db_path=None, *, timeout=30,
check_same_thread=True)`. Sets `busy_timeout=30000` (already applied process-
wide inside main.py via its own `sqlite3.connect` monkeypatch at
`main.py:69-74` — this helper sets it again anyway so callers OUTSIDE that
process, i.e. signal-center's separate py3.9 Flask process, aren't silently
depending on a patch that only exists in one of the two processes touching
these databases) + `synchronous=NORMAL` (the actual gap — not patched
anywhere, confirmed set explicitly in only 5 files repo-wide before this).
Does NOT re-set `journal_mode` (already durable on the DB file).

**Wave 1 scope (23 engine files + dashboard + signal-center's primary
factory):** the same 24-file list the FD-leak sweep already touched this
morning, minus `scripts/build_corpus_from_trader_db.py` (a manual/occasional
corpus-building script, not a hot-path scanner — deliberately deferred to
wave 2, not a hot path by the same "runs on every cycle" definition the rest
of this list uses) — `deep_scan.py`, `bridge_vote.py`, `battle_station.py`
(both its `_conn()` and one extra inline connect site), `volume_baselines.py`,
`strategy_rotator.py`, `volume_scanner.py`, `dayblade_scanner.py`,
`rebalancer.py`, `gex_overlay.py`, `full_universe.py`, `scenario_modeler.py`,
`generated_assets.py`, `tax_harvester.py`, `portfolio_optimizer.py`,
`cash_manager.py`, `drift_rebalancer.py`, `risk_var.py`, `universe.py`,
`pipeline.py`, `sub_portfolio.py`, `wheel_strategy.py` (5 sites, no shared
factory), `external_intel_signal.py`. Plus `dashboard/app.py`'s shared
`_conn()` — dashboard has **30 other** direct `sqlite3.connect()` sites
scattered through the file that were NOT part of the FD-leak sweep's list
and are NOT touched here; they're wave 2. Plus signal-center's `get_db()`
(36 call sites feeding off it — the file's actual hot-path factory; 11 other
scattered direct-connect sites there are also wave 2).

**Migration mechanics:** wrote `scripts/hm_db_conn_migration_wave1.py`
(dry-run by default, `--apply` to write) for the 20 files with a single
`_conn()` definition — regex-isolates the `_conn()` function body specifically
(so it can never touch an unrelated connect elsewhere in the same file,
e.g. battle_station.py's second site) and swaps only the
`sqlite3.connect(...)` call to `get_conn(...)`, preserving args
(`check_same_thread`, `timeout=N` — including `universe.py`'s non-default
`timeout=10`, verified passed through unchanged) and everything else
(`row_factory`, the redundant-but-harmless `PRAGMA journal_mode=WAL` lines,
`HM-BCE-broad` file-existence guards) untouched. Reviewed the full dry-run
diff for all 20 files before applying — every diff was exactly a 2-line
change (one import line, one connect-call line). The remaining files
(`wheel_strategy.py`'s 5 bare sites, `battle_station.py`'s extra site,
`external_intel_signal.py`, `dashboard/app.py`'s `_conn()`,
`signal-center/server.py`'s `get_db()`) were hand-edited individually —
same one-line-swap pattern, reviewed each.

**Verification:** all touched files `py_compile` clean (engine/dashboard
under the trader's Python 3.14, signal-center/server.py additionally
verified under its actual production interpreter, `venv/bin/python3`
== 3.9.6). Confirmed live post-restart: `engine.deep_scan._conn()` reports
`PRAGMA synchronous` = `1` (NORMAL) and `busy_timeout` = `30000`.

**Two restarts this session to get wave 1 fully live** (both clean, WAL
`0|0|0`, positions hash byte-identical `3cddd612...` 48 rows across both,
`pause_all`/`fallbacks_enabled` unchanged 0/0, zero lock errors either
time): the 13:21:30 restart (Kirk fix + FD-leak-sweep-era files already
in flight) predates these wave-1 edits, so a second restart at 15:39:17 was
needed to actually load wave 1 into the running main.py process. Signal-
center was separately killed+relaunched at ~15:32 (PID 19919→18747) since
it's a fully independent process — confirmed clean via `/api/sources/health`
returning correct data (including the `DORMANT` state from P2.5) with zero
new errors in `logs/signal-center.log`.

**Not done (deliberately, per the Admiral's phasing):** the ~578 remaining
call sites repo-wide (dashboard's other 30, signal-center's other 11,
`scripts/build_corpus_from_trader_db.py`, everything outside this list).
Wave 2 is gated on "one full trading day clean" — tomorrow, 2026-07-07
(Tuesday), is the first full live trading day under wave 1; revisit wave 2
after that reads clean (no new lock errors, no regression in the P1.1
sentinel's FD/lock-error checks).

## HM-SOURCE-HEALTH-FALSE-POSITIVES (2026-07-06, resolved)

**macro RED (66d)** - false positive. source_registry ts_format pointed at
bridge_iso:/api/macro:consumer_sentiment.date; UMich sentiment lags 1-2 months
at publisher, so macro re-REDs monthly forever regardless of pipeline health.
FIX (DB-only, no code/restart): repointed to treasury_10y.date (updates every
business day). Verified GREEN via /api/sources/health/macro (as_of 2026-07-02, 4d).
NOTE: change lives in signal-center/signals.db source_registry, not git.

**kirk_advisory RED (07-03 to 07-06)** - real outage, root cause = 30-min poll
phase drift missing all three 10-min persist slots (06:35/09:30/13:05 AZ).
Fixed in aa55f1d (5-min cadence); main.py restarted 07-06 16:58 with fix loaded.
Expected self-heal at first weekday slot 07-07 06:35. Fire Kirk outside slot
windows is a designed no-op (200 + skip, no write) - do not treat as fault.

**Steady state going forward:** 0 RED / 3 UNKNOWN (gex_snapshot, metals,
riker_synthesis - heartbeat wiring gaps, producers alive; follow-up candidate:
add as_of writes) / 4 RETIRED / 1 DORMANT.

---
## 🟢 HM-MACRO-503-DIAGNOSIS (2026-07-07, diagnosed + fix code-complete, held for after-close bundle)

**Reported:** bridge `/api/macro` returned 503 per the 14:36 UTC (07:36 MST)
health sweep. Signal's `macro` source itself was already GREEN
(`as_of` 2026-07-02) — underlying FRED data confirmed fine going in.

**Theory #3 (freshness gate) ruled out by code read, not guessed.** Read
both `dashboard/app.py::macro_data()` (`@app.get("/api/macro")`,
`@timed_cache(300)` wrapping `engine.alphavantage_data.get_macro_data()`)
and `get_macro_data()` itself line by line — **no freshness/staleness gate,
no explicit 503 raise, anywhere in either.** Live `curl localhost:8080/api/macro`
right now: 200 + full JSON (all 10 FRED indicators, including the expected
1-2 month publisher lag on `consumer_sentiment`/`cpi_yoy` dates — that lag is
real but was never gating anything).

**Actual root cause: trader.db lock contention, not macro-specific at
all.** `trader.log`'s own `[ENDPOINT-DUR]` lines show **~50 unrelated
endpoints** (`/api/status`, `/api/gex/SPY`, `/api/webull/positions`,
`/api/congress/trades`, `/api/risk/var`, `/api/macro`, dozens more) all
simultaneously stalling to **wall=57-58s** around 07:33-07:34 MST — a
process-wide freeze, not a per-endpoint bug. Correlates exactly with
multiple `OperationalError('database is locked')` entries in the same
window (`TICK-REC DB write failed`, `alert_channels: save_setting failed`,
`HM-EQ capitol-trades snapshot failed`) and with `hm_ops_sentinel.py`'s own
lock-error count climbing **0→4→10→18** across successive 5-min checks in
that exact span (FD count also crossed the 150 warn threshold, peaking at
154) — the sentinel (fixed last night, see `HM-OPS-SENTINEL` above) caught
this live and correctly alerted. One request (`/api/tax/history`) even
timed out to a real exception (`status=EXC`, wall=57.37s); a later,
separate spike hit 113s. This is the same `HM-SQLITE-CONN-FD-LEAK` /
lock-storm class already being worked (`HM-WAL-BUSY-TIMEOUT-HYGIENE` wave 1
shipped 2026-07-06, wave 2 still open) — not a new bug, a new symptom of
the known one.

**Why it showed as 503 specifically:** two client-side timeouts in this
codebase are well under the observed 57-113s stall duration —
`scripts/origin_healthcheck.sh` (`curl --max-time 8` against `/api/status`)
and `signal-center/server.py::_fetch_all_signals()`'s default **5-second**
timeout for any key not in `_SIGNALS_TIMEOUTS` (the `'economic'`/`/api/macro`
key had no override, unlike `dayblade`/`metals`/`risk_radar`, which already
carry higher timeouts for the identical reason — under-concurrent-load
slowness, not solo brokenness). Either one would report a failure during
the stall even though the backend eventually returned 200 to trader.log.

**Fix (code-complete, held for the after-close bundle per Admiral
constraint — NOT applied, no restart):** added `'economic': 15` to
`signal-center/server.py::_SIGNALS_TIMEOUTS`, same pattern as the three
existing overrides. This absorbs typical/moderate lock-contention windows
(the 33s/57s/58s spikes) without masking a genuinely broken endpoint
indefinitely — it does **not** fully solve the underlying lock contention
(the observed 113s outlier would still exceed a 15s timeout); that's wave 2
of `HM-WAL-BUSY-TIMEOUT-HYGIENE`, separately tracked, not boiled into this
ticket. `py_compile` clean.

**No `dashboard/app.py`/`engine/alphavantage_data.py` change proposed** —
there is no bug there to fix; a diff in the macro handler itself would be
solving the wrong layer.

**Verification after restart:** `curl -s -o /dev/null -w '%{http_code}'
http://localhost:8080/api/macro` (expect 200), MACRO panel renders on
`/classic`, and re-check `_SIGNALS_TIMEOUTS['economic']` is honored (no
easy black-box test for this without reproducing lock contention on demand
— acceptable to verify by code inspection post-restart rather than forcing
a live repro).

**cto_briefing secondary — NOT skipped, confirmed real + trivially fixed.**
Checked whether its daily run fired today per instruction: **it had not** —
zero "CTO Advisory firing" log lines for 2026-07-07 as of 07:44 MST, both
available slots today (`pre_market` 6:00, `post_open` 6:45) already past
their 10-min windows with no firing. This is exactly the "same latent risk,
not independently confirmed" flagged in the `P3.8` entry above — now
confirmed. Since the fix is the **identical one-line change** already
proven correct for Kirk (`schedule.every(30)` → `schedule.every(5)` for
`run_cto_advisory`, `main.py:4547`), it qualifies as "trivial" under this
ticket's own instruction — applied, `py_compile` clean, held for the same
bundle.

---
## 🟢 HM-LOCK-STORM-07-07-QUANTIFIED (deep-dive for WAL wave-2 scope, requested by Admiral)

**Duration — longer and in two waves, not a single ~57s blip.** Wave 1:
~07:29:56-07:34:xx MST (first `[HM-EQ]` per-agent snapshot failure through
the ~57-58s `[ENDPOINT-DUR]` cluster). Wave 2: 07:35:25-07:35:27 MST, worse
— `/api/tax/opportunities` and `/api/sub-portfolios` both hit **113+
seconds** before raising an exception (`status=EXC`, not even a slow 200).
`[HM-EQ]` per-agent snapshot failures continued through **07:37:40** (9
consecutive agents, roughly one per minute, EVERY one failed: ollama-plutus,
options-sosnoff, enterprise-computer, neo-matrix, capitol-trades, trade-desk,
ollie-machine, desk-manual, alpaca-mirror). Real span: **~8-10 minutes**,
not one moment.

**Quantified casualties:**
- **~1,968 real-time price ticks silently dropped.** `[HM-TICK-REC
  heartbeat]`'s `write=` counter was **frozen at 10101 for the entire
  07:30:46-07:36:12 window** (zero successful DB writes) while `recv=`
  climbed 17380→19354 and `drop=` climbed 6253→8221 in lockstep — every
  tick received during the stall was dropped, not queued or retried.
- **signals_v2 writes completely starved for ~19 minutes.** DB query:
  9 rows at 14:26 UTC, then **zero rows until 14:45 UTC** (07:27-07:44 MST)
  — no new pending/executed/failed signal rows of any kind during that
  entire window. Any trade opportunity in that span would have been
  silently missed at the signal-generation stage, not just delayed.
- **Every `[HM-EQ]` per-agent snapshot in the window failed** (9/9,
  `ai_brain.py:319`), War Room debate tracking failed at both start
  (`war_room.py:186`) and finish (`war_room.py:229`) more than once,
  `NewsPulse` DB store failed, `breadth_scanner` DB store failed, GEX
  snapshot stamps failed repeatedly, `alert_channels: save_setting failed`
  repeatedly (this is the SAME rate-limit-persistence write path touched
  by `HM-NTFY-IPV6-NOROUTE`/`HM-ALERT-RATE-ON-FAILURE` below — a second,
  independent way this exact incident degraded alerting, worth noting for
  the record even though unrelated to the network-routing fix).
- **No direct evidence of a lost/delayed order dispatch found** —
  no BUY/SELL/order-submit log lines appear in the window at all. Can't
  fully disambiguate "no trade opportunity happened to arise" from "the
  starved signal pipeline meant nothing ever got far enough to dispatch" —
  the signals_v2 starvation above is the more defensible, directly-evidenced
  claim; treat "order dispatch impact: unknown, plausibly zero because
  nothing reached that stage" rather than asserting a specific dropped order.

**What held the lock — no single smoking-gun query found; matches the
already-open `HM-WAL-ROOTCAUSE` (2026-07-01) finding, not a new cause.**
That ticket found 15 concurrent open connections on `trader.db-wal`, **all
from the trader's own single PID**, no external tool holding a lock — i.e.
structural over-concurrency from within one process, not one long
transaction. This incident's failure roster (tick recorder, HM-EQ ×9
agents, War Room ×2 sites, NewsPulse, breadth_scanner, GEX snapshots,
alert_channels settings) reads as the SAME pattern: many independent
call sites across the codebase each opening their own `sqlite3.connect()`
to the same file, all competing for WAL's single-writer slot during a
period of coincidentally-overlapping activity, rather than one identifiable
culprit holding the lock open.

**This is the quantified case for `HM-WAL-BUSY-TIMEOUT-HYGIENE` wave 2**
(wave 1 shipped 2026-07-06, migrated 23 engine files + dashboard +
signal-center's primary factory to the shared `engine.db_conn.get_conn()`
helper; wave 2 covers the remaining scattered direct-connect sites,
explicitly deferred at the time). Concrete numbers to carry into that
ticket's scoping: **~1,968 dropped ticks + ~19 min of zero signal
throughput + 9/9 agent-snapshot failure rate**, over an **8-10 minute
event**, driven by **no single query** but by the sheer count of
concurrent same-process connections — an argument for reducing connection
*count* (pooling/reuse), not just adding `busy_timeout` to more individual
call sites (which wave 1 already does, and this incident still happened
on top of wave-1-migrated code).

---
## 🟢 HM-NTFY-IPV6-NOROUTE + HM-ALERT-RATE-ON-FAILURE (2026-07-07) — SHIPPED + live-confirmed; exact historical failure rate stays UNVERIFIED (record corrected below, not re-guessed)

**Admiral asked: did the sentinel's ntfy ALERT path actually fire at
07:33, not just its own log counter?** Checked `logs/hm_ops_sentinel_cron.log`
for `_send_ntfy`'s own "ntfy sent"/"ntfy failed" log lines (distinct from
the sentinel script's own "[sentinel] ALERT dispatched" print, which fires
regardless of whether delivery succeeded). Found 20 "ntfy failed" lines,
zero "ntfy sent" lines — **initially wrote that up as "0 successes, 20
failures, 100% failure rate" here. That specific claim is WRONG, caught
and correcting before it stood as the record.** Neither this script nor
`main.py` ever calls `logging.basicConfig()` — Python's default logging
config drops INFO-level records below the WARNING threshold, and
`_send_ntfy`'s SUCCESS path logs at INFO (`logger.info("ntfy sent...")`)
while its FAILURE path logs at WARNING (`logger.warning("ntfy failed...")`,
visible via Python's stderr "handler of last resort"). **Zero "ntfy sent"
lines proves nothing** — successes were architecturally invisible in this
script's log the whole time, so the 20 visible failures could be the
entire population or a fraction of a much larger, mostly-successful one;
the log cannot distinguish those. Real, confirmed facts: the IPv6 routing
problem below is real (direct-tested, not log-inferred) and at least 20
genuine failures happened; a specific "100%" figure was an inference from
an observability gap, not a measurement — flagged rather than left
standing uncorrected. This is exactly the
"an alarm that only writes to its own log shares a failure mode with
silence" case the Admiral asked about — just one layer deeper than first
diagnosed: the silence extended to the SUCCESS side of the sentinel's own
diagnostic logging, not only to ntfy delivery.

**Root cause, confirmed by direct test:** this box has no working IPv6
route to ntfy.sh. `socket.create_connection()` to ntfy.sh's AAAA address
raises `OSError [Errno 65] No route to host`, 100% reproducible, on both
Python interpreters on this box; the IPv4 address always succeeds. Real-
world asymmetry: `scripts/hm_ops_sentinel.py` (`.venv`/py3.14, cron
`*/5`) got 0/20 successes; `scripts/git_push_health_check.py` (`venv`/
py3.9, cron daily) succeeded via the identical `_send_ntfy()` code path —
both interpreters fail the same direct IPv6 test, so the discrepancy is
specifically about cron execution context (not fully root-caused at the
OS level — plausibly launchd/cron network-entitlement differences from an
interactively-launched process), not Python version. Rather than depend on
`getaddrinfo()` address-family ordering being consistent across execution
contexts, **forced IPv4-only** for the ntfy call.

**Fix 1 (`engine/alert_channels.py::_send_ntfy`):** wraps the `urlopen`
call in a scoped, lock-protected monkeypatch of `socket.getaddrinfo` that
filters to `AF_INET` only, restored in a `finally` block. IPv6 doesn't work
anywhere on this box (confirmed), so the brief forced-IPv4 window carries
no real cost to other concurrent socket use. Verified: patch correctly
filters to IPv4-only, correctly restores original `getaddrinfo` after the
call (checked both before and after), and a real end-to-end `_send_ntfy()`
call (diagnostic topic, not the real admin channel) returned `True` —
confirmed real HTTP delivery, not just "didn't raise."

**Fix 2, found while verifying Fix 1 — a second, compounding gap:** the
rate-limit window (`_rate_ok()`) was being **consumed at CHECK time**,
before `_send_ntfy()` even ran — so a failed delivery ALSO burned the
30-min retry budget. Confirmed live: `sentinel_lock_errors` and
`sentinel_signals_v2_queue` both showed a `last_sent` timestamp in the
persisted rate-limit state despite 0 actual successful sends, meaning even
after Fix 1 lands, the NEXT real attempt for those specific alert types
wouldn't fire until their pre-fix-consumed windows separately expired.
Split `_rate_ok()` (now a pure read-only check) from a new
`_mark_rate_limit_sent()` (called only after `send_alert()` confirms
`results.get("ntfy") or results.get("email")` — deliberately NOT
`any(results.values())`, since `results["browser"]` is hardcoded `True`
unconditionally for WARNING/RED_ALERT and would have silently defeated
this exact fix for the two levels the sentinel actually uses). Verified
with a 3-step test: simulated failure → rate window NOT consumed →
simulated success → rate window consumed → third attempt correctly
skipped as rate-limited.

**Fix 3, the observability gap itself:** `scripts/hm_ops_sentinel.py` now
calls `logging.basicConfig(level=logging.INFO, ...)` at import time --
scoped to this standalone script's own process only, deliberately NOT
touching `engine/alert_channels.py`'s logging behavior (shared with
`main.py`, which must not be affected). This is what makes it possible to
actually verify Fix 1/Fix 2 going forward instead of reasoning from an
architecturally-blind log.

**Status:** all three fixes are in files re-loaded fresh on every
cron-invoked script's next tick (`engine/alert_channels.py` +
`scripts/hm_ops_sentinel.py`, both cron-only, not `main.py`-resident) —
**no restart needed for any of these**, unlike the other fixes tonight;
they self-apply on the next real cron invocation. `py_compile` clean on
both files. Full test matrix (IPv4 patch correctness, getaddrinfo
restore-after-call, live diagnostic-topic delivery, failure-doesn't-
consume-rate-limit, success-does-consume, third-attempt-correctly-rate-
limited) all passed.

**✅ LIVE-CONFIRMED, real cron context, not a test harness.** Waited for
the next genuine (non-rate-limited) dispatch with Fix 3's visibility in
place. `logs/hm_ops_sentinel_cron.log`, 2026-07-07 08:30:04 MST:
```
2026-07-07 08:30:04,058 [engine.alert_channels] INFO: ntfy sent [200]: ⚠️ TradeMinds Warning
2026-07-07 08:30:04,072 [engine.alert_channels] INFO: Alert dispatched [warning/sentinel_signals_v2_queue]: ...
```
Real HTTP 200 to ntfy.sh, under actual crontab invocation (`.venv`/py3.14,
`*/5` cadence) — **the first confirmed-successful ntfy delivery from this
sentinel since it was created.** Cross-checked the rate-limit state
persisted correctly on this genuine success: `sentinel_signals_v2_queue`
`last_sent` = 0.41 min after the send (vs. showing 25-30+ min stale before
Fix 2). All three fixes (IPv4 force, rate-limit-on-success-only, INFO-level
visibility) verified working together, live, without a restart. Closing
this out — the sentinel's alert path is now genuinely trustworthy, not
just quiet.

---
## 🟡 HM-WAL-BUSY-TIMEOUT-HYGIENE wave 2 — SCOPED TONIGHT, NOT EXECUTED (Admiral: own dedicated after-close window tomorrow)

**Explicit non-goal, stated up front:** nothing below rides tonight's
bundle. This is a plan, informed by today's `HM-LOCK-STORM-07-07-QUANTIFIED`
data, for tomorrow's own dedicated after-close window.

**Cross-referenced every file the 07:33-07:40 storm actually implicated
against wave 1's migrated-file list.** Wave 1 covered 23 specific engine
files + dashboard's shared `_conn()` + signal-center's `get_db()` factory
(exact list in the wave-1 entry above). **None of today's worst offenders
were in it:**

| File | Raw `sqlite3.connect()` sites | Storm role |
|---|---|---|
| `engine/ai_brain.py` | **4** | `[HM-EQ]` per-agent snapshot — 9/9 failed today, worst offender |
| `engine/war_room.py` | 1 | debate tracking start+finish, both failed multiple times |
| `engine/tick_recorder.py` | 1 | real-time tick writes — ~1,968 ticks dropped, write counter frozen 6 min |
| `engine/news_pulse.py` | 1 | `NewsPulse` DB store failed |
| `engine/alert_channels.py` | 1 | `save_setting failed` (rate-limit persistence — same file `HM-NTFY-IPV6-NOROUTE`/`HM-ALERT-RATE-ON-FAILURE` already touched tonight for the ntfy code path; this is a DIFFERENT, still-open gap in the same file) |
| `engine/gex_overlay.py` | 1 (line 602) | **partial-migration gap**: this file IS in wave 1's list and its `_conn()` factory correctly uses `get_conn()` — but `_log_snapshot()` at line 602 has its own separate raw `sqlite3.connect(TRADER_DB, check_same_thread=False)` that bypasses the factory entirely. Proves wave 1 alone doesn't guarantee coverage even for "migrated" files — a leftover unconverted site inside an otherwise-migrated file is invisible unless checked directly. |
| `main.py` (`run_breadth_sector_corr`, ~line 5615) | uses main.py's own process-wide `busy_timeout` monkeypatch (not `get_conn()`) | `breadth_scanner: DB store failed` — has `busy_timeout` via the existing monkeypatch but likely NOT `synchronous=NORMAL` (get_conn's actual documented gap over the monkeypatch) |

**9 confirmed raw/gap sites directly tied to a real, quantified incident** —
not a guess at priority order, an empirical one.

**Recommendation for tomorrow's window:**
1. Migrate the 6 files above to `engine.db_conn.get_conn()` first — proven
   storm participants, highest-confidence value, small tight list.
2. Fix `gex_overlay.py:602` specifically as a reminder to **grep every
   "wave-1-complete" file for leftover raw `sqlite3.connect(` before
   declaring wave 2 done** — the factory existing doesn't mean every site
   uses it.
3. **Re-open the "reduce connection count" question, don't just repeat
   wave 1's pattern.** Wave 1 already added `busy_timeout`+
   `synchronous=NORMAL` to 23+ files and today's storm still happened on
   top of that — `HM-WAL-ROOTCAUSE` (2026-07-01) already found 15+
   concurrent same-PID connections with no external holder. Busy_timeout
   makes each individual wait survivable; it does not reduce how many
   writers pile up at once. Worth asking, per implicated file: does
   `ai_brain.py`'s HM-EQ loop need 4 separate connections per cycle, or
   could per-cycle work share one? Same question for any file with >1 site.
4. Then continue the already-documented remaining scope (dashboard's other
   30 scattered sites, signal-center's other 11, `scripts/
   build_corpus_from_trader_db.py`) at whatever pace fits the window —
   lower priority than the 6 above since none were directly implicated by
   a real measured incident.

**Not decided here — needs the Admiral's call at execution time:** whether
tomorrow's window does the storm-implicated 6 only (tight, fast, evidence-
driven) or the full remaining wave-2 scope in one pass (matches wave 1's
own "sweep the remaining call sites as wave 2" framing, but bigger diff,
same "full backup before touching anything" discipline wave 1 used).

---
## 🟢 EOD WRAP — 2026-07-07, after-close bundle SHIPPED

### Bundle execution summary
Pre-restart GEX check: SPY `gex_snapshots` newest row confirmed fresh
(asof `2026-07-07 20:05:25` UTC = 13:05 MST, matching the collector's
schedule exactly) — call_wall=$750, gamma_flip=$750.53, put_wall=$617,
king_node=$740, all distinct (not degenerate). Served correctly end-to-end
through `canonical_gex.latest_snapshot()`.

Bundle applied: `ALERT_DEFS_ENABLED` flipped True; `qwen3:4b` onboarded as
`qwen3-4b-audition` ("Cadet Worf," mirrors `qwen3-8b-flash`/Worf's Bear
Specialist mandate in `crew_specialization.py`, `crew_role='auditioning'`,
`can_trade_live=0`); sentinel cron formalized (crontab confirmed durable,
thresholds confirmed warn=150/red=250, **and a real gap caught+fixed**:
`hm_ops_sentinel_cron.log` had no rotation mechanism at all — extended
`rotate_logs.sh` to a multi-target loop, added it at a 10MB threshold,
tested against synthetic files before touching the real ones); Kirk
fire-kirk heartbeat fix; CTO Advisory scheduling fix; P4-13 GEX repoint;
Fleet-tab split/79-active mislabel/navigator gate repair-sweep items;
`/api/macro` timeout fix. One trader restart (13:42:24 MST) + one
signal-center restart, both clean/orphan-free. All 14 modified Python
files + 2 modified HTML files compiled/syntax-checked clean; full test
suite run immediately before restart — same 14 pre-existing, unrelated
failures as every other check tonight, 633 passed.

### Post-restart verification — all 10 checklist items confirmed
1. **Audition gate**: `[AUDITION-GATE] active — 1 auditioning seat(s),
   can_trade_live enforcement=ON` — correctly counts the new seat.
2. **All 10 original gated agents**: `is_auto_tradeable()` = True for all,
   re-verified against the live post-restart process.
3. **New seat blocked at all 3 gate layers**: `halt_gate.is_auto_tradeable()`
   → False (live-tested); `RiskManager.check_buy()` → `(False,
   "AUDITIONING: shadow mode, no live execution...")` (live-tested);
   `paper_trader.buy()`'s HALT GATE → confirmed via direct code-path match
   against the row's exact `halt_mode='active'`/`crew_role='auditioning'`
   values (not live-invoked — `buy()` has too many side effects to safely
   call standalone; no real order attempt has fired yet since market is
   closed, so this one is code-verified, not live-observed like the other
   two).
4. **Alert-defs reader live**: `config.ALERT_DEFS_ENABLED=True` confirmed
   in the running process; `run_user_alert_definitions()` executes a real
   query (not short-circuiting as a no-op).
5. **P4-13 fresh GEX label**: `get_gex_context_for_prompt()` now returns
   "updated 20:05" (today's collector run) with SPY showing LONG GAMMA /
   QQQ showing SHORT GAMMA — genuinely distinct regimes, proving real data,
   not a stale default.
6. **Gamma Map walls populated**: `/api/gex/spy` and `/api/gex/qqq` both
   return full strike-level data, non-empty walls, today's timestamp.
7. **Sentinel firing with real ntfy**: confirmed tracking the new PID
   (1479) correctly within one tick of the restart; ntfy delivery already
   proven working multiple times earlier today (mechanism lives in a
   cron-reloaded file untouched by this restart, so it was never at risk).
8. **Clean logs**: zero traceback/critical lines in the first 300 log
   lines post-restart.
9. **Positions**: 33 open positions, $17,229.10 total notional,
   SHA256 `5bca62fe9003...` as the current reference hash. No pre-restart
   snapshot was taken to diff against (not set up in advance) — integrity
   instead backed by the restart script's own pre-restart WAL checkpoint
   completing fully (`0|0|0`, zero frames remaining) before the process
   transition, which is the actual data-loss guarantee, not a hash match.
10. **WAL checkpoint**: healthy — `0|230|51` post-restart (busy=0, 51/230
    frames checkpointed; PASSIVE mode partial-checkpoints by design, not a
    failure).

### Swing-surge clean-day breakdown, storm window annotated
Today (`signals_v2`): 128 signals → 7 trades (5.5% conversion), guarded
P&L **-$111.40** fleet-wide (capitol-trades +$8.95, gemini-2.5-flash
-$91.19, ollama-plutus +$0.78, ollama-qwen3 -$28.37, ollie-auto -$1.57).
Status breakdown for the day: 1,609 failed / 211 pending / 9 stale / 1
expired.

**Storm window (07:27-07:45 MST) isolated: zero signals_v2 rows of ANY
status** — not a contributor to the 1,609 "failed" count, a clean 18-minute
gap where nothing was recorded at all, good or bad. All 1,609 failures
happened outside the storm window, across the rest of the day — a
separate, pre-existing pattern not investigated as part of tonight's work
(no evidence tying it to the storm specifically).

### WAL wave-1 verdict
Today was the first full live trading day under wave 1 (shipped
2026-07-06). Verdict: **wave 1 works for what it covers, and coverage was
incomplete — confirmed by a real incident, not a guess.** Sentinel's own
history across the day: lock_errors > 0 in only 14/228 five-minute samples
(~6% of the time) — the steady-state baseline wave 1 protects is
genuinely clean most of the time. But the 07:33 storm (max 18 lock errors
in one 10-min window) broke through anyway, and tracing its actual
participants (`ai_brain.py`, `war_room.py`, `tick_recorder.py`,
`news_pulse.py`, `alert_channels.py`, plus a leftover raw-connect site
inside the already-migrated `gex_overlay.py`) showed **none of them were
in wave 1's scope** — this isn't wave 1 failing at its job, it's wave 1
correctly protecting the 23+dashboard+signal-center files it touched while
the storm happened entirely in files it never reached. `HM-WAL-BUSY-
TIMEOUT-HYGIENE` wave 2 (scoped tonight, not executed — see the entry
above) is the direct, evidence-backed next step, not a speculative one.

### Day-1 gated-live-order confirmation
**Confirmed**: `ollie-auto` (halt_mode='exit_only', one of the 10
`can_trade_live=1` gated agents) placed a real Alpaca order — SELL PM,
order ID `abb28f18-8665-454f-95af-66aed92ad90a`, status `filled` — at
**06:39:34 MST, 9 minutes after today's 6:30 AM open**. This is the
precondition `HM-GATE-RESTART-HOLD` was held for: a live order completing
end-to-end under `can_trade_live` enforcement=ON, observed on the actual
first trading day, not simulated. (Correction to an earlier same-day
report: this was first quoted as "13:39:34 MST" — that was the raw UTC
`trades.executed_at` value read without converting; `executed_at` uses
SQLite's `CURRENT_TIMESTAMP`, always UTC. Corrected here with the
subtraction shown, not just asserted.)

---
## 🔴 P0-A: OPTIONS FILL INTEGRITY — DIAGNOSIS COMPLETE, DIFFS PROPOSED, HELD FOR BUNDLE

Read-only diagnosis per Admiral directive. No code shipped, no restart.
Confirmed against live code + DB, not the audit doc alone.

### 1. Inventory — every site that invents an option price

| File | Function | Verdict | Detail |
|---|---|---|---|
| `engine/wheel_strategy.py:227` | `run_wheel_scan()` | **BROKEN — no real-quote attempt at all** | `premium_pct = min(0.08, vix / 500.0)`. Pure VIX-scaled formula. Zero chain calls. |
| `engine/shadow_csp.py:177` | `_build_candidates()` | **BROKEN — identical formula** | `premium = round(price * min(0.08, vix / 500.0), 2)`. Byte-identical to wheel_strategy's, deliberately (docstring: "builds the SAME deterministic candidate set Troi would"). Taints the bakeoff, not just the live seat. |
| `engine/dayblade.py:296,320-346` | `_estimate_atm_premium()` / `buy_option()` | **PARTIALLY BROKEN** | DOES try a real quote first (OpenBB `get_options_chain`, uses bid/ask mid), only falls back to the `_estimate_atm_premium()` formula (`stock_price * 0.30 * sqrt(dte/365)`) on a bare `except: pass` or no strike match. Correct pattern, but: (a) silent fallback — no metric on how often OpenBB actually returns data vs. how often it silently degrades to synthetic, (b) `get_portfolio_with_pnl()` (line 228-232) marks OPEN option positions using `paper_trader.estimate_option_price()` (a decay heuristic) rather than trying a live quote first — the displayed unrealized P&L for dayblade's option book never attempts a real mark. |
| `engine/battle_station_0dte.py` | `_get_option_price()` | **CLEAN** | Real Alpaca quote only (`alpaca_options._get_contract_price`, `client.get_option_contract(...).close_price`). Returns 0.0 on failure; caller gates on `price >= MIN_PREMIUM` and skips the trade. No synthetic fallback exists. |
| `engine/battle_station.py` | n/a | **CLEAN** | No option-price invention found. `avg_entry_price` reads are from existing position/order data (real fills), not price generation. |
| `engine/options_exec.py` | `open_options_trade()` / `close_options_trade()` | **MECHANISM-LEVEL GAP** | Trusts whatever `entry_price`/`exit_price` the caller supplies with zero independent validation — by design, it's a generic accounting helper, not a pricing source. Credits `options_books.current_cash` permanently at entry (line 145) based on that unvalidated number. **No mark-to-market function exists anywhere in this file** — confirmed by the `options_books` schema itself (`book_tag, starting_capital, current_cash, max_drawdown_pct, total_trades, wins, losses` — no unrealized/mark column of any kind). |
| `engine/paper_trader.py:4231` | `_csp_current_premium()` | **MOSTLY CORRECT, already exists** | Tries Polygon `get_option_quote(occ).mid` first; falls back to `estimate_option_price()` (intrinsic + a `entry_premium * 0.70 * sqrt(days_left/30)` time-value-floor heuristic) only if Polygon fails. Used by the CSP TP/SL/time-stop exit check (`_check_option_exits_canonical_short_premium`, line 4263) — this is the **existing real-quote infrastructure the entry-side fix should reuse**, not reinvent. Caveat: the BSM-style fallback is anchored on `entry_premium` — if entry was synthetic, a Polygon-miss on the exit side still inherits some contamination, just decayed by a real (if approximate) time curve rather than pure fantasy. |
| `engine/wheel_assignment_ledger.py::assign_csp()` | ITM assignment | **CORRECT, not synthetic** | Uses `spot_at_expiry` (real stock price) for intrinsic-value assignment mechanics — this is legitimate; assignment intrinsic is deterministic math (`strike - spot`), not a quote that needs fetching. |

**Scale**: `options-sosnoff` (Troi) has 84 closed CSPs, $29,868.74 total realized P&L, **95.2% win rate** — matches the audit's cited baseline exactly. Given the entry premium is fantasy and OTM-expiry-at-$0 is the dominant close path, this win rate is close to tautological (a fake "already collected" premium that's rarely given back isn't evidence of skill).

### 2-3. Proposed diffs: real-quote wiring + mark-to-market

**Reuse the existing `_csp_current_premium()` pattern (Polygon quote → BSM-floor fallback) rather than building a new pricing path.** Concretely:

- **New shared helper** `engine/options_pricing.py::get_real_csp_premium(symbol, expiration, strike, stock_price) -> float | None` — extracted/generalized from `_csp_current_premium`'s Polygon-quote branch (drop the `entry_premium`-anchored BSM fallback for the ENTRY case specifically — there's no `entry_premium` to anchor to yet, so entry-side failure should mean **skip the trade**, matching `battle_station_0dte`'s pattern, not fabricate a number).
- **`wheel_strategy.py:225-228`**: replace the `premium_pct = min(0.08, vix/500.0)` block with a call to `get_real_csp_premium(ticker, expiry, put_strike, price)`; `if premium is None: continue` (skip the ticker this cycle, same as battle_station_0dte's `MIN_PREMIUM` gate).
- **`shadow_csp.py:177`**: identical replacement — same helper, same skip-on-None behavior. Keeps both seats and the baseline on one real pricing source, so the bakeoff stays apples-to-apples.
- **`dayblade.py`**: add Polygon as a second real-quote attempt alongside the existing OpenBB chain call (try OpenBB, then Polygon, then skip — never fall through to `_estimate_atm_premium()` silently); instrument the fallback/skip path with a counter or log line so the real-vs-synthetic ratio is visible going forward (closes the current blind spot). For `get_portfolio_with_pnl()`'s open-option marks: try `get_real_csp_premium`-equivalent first, keep `estimate_option_price()` only as the final fallback (matches `_csp_current_premium`'s own existing hierarchy — consistency, not a new pattern).
- **Mark-to-market (item 3)**: since `options_books` has no unrealized column, add a **computed-on-read** function `engine/options_exec.py::book_equity(book_tag) -> dict` returning `{current_cash, unrealized_pnl, total_equity}` — iterates `options_trades WHERE book_tag=? AND status='open'`, marks each short-premium leg via `get_real_csp_premium` (or the existing `_csp_current_premium` for CSPs specifically), sums `(entry_credit_debit - current_market_cost)` per open trade. Computed-on-read matches this codebase's existing convention (e.g. `canonical_gex.latest_snapshot()`, `get_portfolio_with_pnl()`) over a synced/stored column that can drift stale. Wire this into wherever `options_books.current_cash` is currently displayed as "book equity" (dashboard/`app.py` options-book endpoints — not yet located precisely; needs a follow-up grep for `current_cash` display sites before the bundle window, not done here since it's read-only display wiring, not new financial logic).

### 4. Era-fencing — TROI_V2_ERA_START does NOT mean what the directive assumes

**Important correction before implementing anything**: `TROI_V2_ERA_START = "2026-07-06"` (`paper_trader.py:2541`) already exists, but it marks the **`TROI_CSP_CAP_GATE` position-sizing boundary** (HM-TROI-WHEEL-V2, "v1 blind/uncapped" → "v2 gated"), NOT a pricing-methodology boundary. As of right now (2026-07-07), **every CSP trade ever written, including everything after this era start, still uses the synthetic `vix/500` entry formula** — there is no real-quote era yet to fence into "v2." Using the existing constant as the directive literally describes would mislabel post-07-06 cap-gated-but-still-synthetic trades as "real-quote v2," which is false.

**Proposed**: introduce a **separate, new constant** `TROI_REAL_QUOTES_ERA_START` (distinct name, to avoid conflating the cap-gate axis with the pricing axis — they're independent and shouldn't share one boundary marker), set to whatever date item 2's fix actually ships and goes live, not retroactively. Until then, the restated leaderboard should label **100% of existing CSP history — v1 cap-gate era AND v2 cap-gate era alike — as "synthetic fills — restated,"** and grade Troi from zero real-quote trades until the fix ships and accumulates some. This is a forward-only fix, same shape as the `acted_by_fleet` emit-time-tagging lesson already banked in `CLAUDE.md` ("Fix = emit-time 'acted' tagging... FORWARD-ONLY build") — no retroactive repricing of historical rows, just an honest label going forward.

### 5. Shadow CSP bakeoff — confirmed same-root-cause

`shadow_csp_scorecard.py::compute()` scores both the Troi baseline (`options-sosnoff`/`fleet` book) and both ghost seats (`ghost` book) purely from `options_trades.pnl` — the same corrupted chain (fake entry premium → mostly-OTM-expiry-at-$0 close). Confirmed 6 closed CSP trades exist for `shadow-%` agents so far, same formula. **The bakeoff cannot be validly re-run on historical data — it must restart clean once item 2 ships**, comparing baseline vs. ghosts on real-quote trades only, going forward. Re-running the SAME scorer against the SAME tainted rows would just reproduce the same false 95% number with extra steps.

### Nothing shipped
All of the above is diagnosis + proposed diffs only, per the Admiral's explicit hold. Implementation (the `options_pricing.py` helper, the 3 call-site rewires, `book_equity()`, the new era constant, and the bakeoff restart) is scoped for a dedicated bundled window — not attempted inline here given the blast radius (touches live P&L accounting for the fleet's single best-looking performer).

---
## 🔴 P0-B: SIGNAL HYGIENE — DIAGNOSIS COMPLETE, DIFFS PROPOSED, HELD FOR BUNDLE

Read-only diagnosis per Admiral directive. No code shipped, no restart.

### 1. Idempotency — dedup key for ALL agents

Confirmed and **worse than reported**: `signals_v2` has zero unique constraints.
Capitol-trades' existing dedup (`crew_scanner.py:3081`) checks only `positions`
(already-held) and `trades` (already-executed-today) — a **blocked** signal has
no protection at all and re-emits every cycle. Live measurement: **1,286
duplicate `IBM`/`capitol-trades`/`direct_buy_intent` rows** in `signals_v2`
since 2026-07-06 (1,183 `failed` + 103 `pending`), plus **85 duplicate
`TRADE_REJECTED` rows** in `crew_decisions` for the same symbol today alone —
some emitted multiple times within the same second (`19:59:26` ×5), meaning
this isn't purely a cross-cycle re-emission problem, there's same-pass
duplication too.

**Proposed diff**: `emit_signal_v2()` (`engine/events_bus.py:111`, the single
chokepoint every caller already uses — matches this codebase's existing
centralization pattern, e.g. `open_options_trade`'s door1 gate) gets a
schema-level unique index:
```sql
CREATE UNIQUE INDEX idx_signals_v2_dedup
ON signals_v2(source, symbol, direction, date(created_at))
WHERE signal_type = 'direct_buy_intent';
```
`emit_signal_v2()` switches its INSERT to `INSERT OR IGNORE`; when
`cur.rowcount == 0` (blocked by the index), return the existing row's id
instead of a fresh one. Scoped to `direct_buy_intent` only for now — that's
the signal_type with demonstrated pathological duplication; `momentum`
signals may legitimately re-fire intraday on new setups, so blanket-applying
without the same evidence risks silently dropping real signals. Expand the
`WHERE` clause to additional signal_types only after confirming each has the
same problem, not preemptively.

### 2. Blacklist instrumentation + auto-expire

Confirmed: `learning_engine.py:110`'s 3-loss/14-day block (`recent_losses`
query, line 46) only appends a string to an in-memory `blocked_reasons` list
— **nothing is persisted**. Cross-checked against `gate_reject_log` (the
system's actual existing rejection-logging table, used by 10 other gates —
HALT, MARKET_CLOSED, GRADE_B, MAX_TRADES_REACHED, MAX_POSITIONS_REACHED,
PRICE_SANITY, LOW_CONVICTION, SCANNER_FILTER, QUALITY_GATE_FAILED,
DRAWDOWN_PAUSE): the ticker-blacklist gate writes to **none of them**. It's
invisible to any existing report.

**Proposed diff**: make the blacklist block also call `_log_gate_reject`-style
write into `gate_reject_log` (gate_name=`TICKER_BLACKLIST`), reusing existing
infrastructure rather than building new. This alone closes the "log every
block with the would-be trade" ask (row already carries `symbol`, `price`,
`confidence`).

**Auto-expire — a real design nuance, not just a checkbox**: this isn't a
persisted blacklist table to expire rows from — it's a **rolling 14-day
window computed fresh every call** (`recent_losses` groups `trades` by
`symbol HAVING COUNT(*)>=3` within the trailing 14 days). Natural time-based
expiry already happens by construction — a symbol falls off automatically
once its losing trades age past 14 days. TRUE early-expiry ("test
profitable, remove before the natural rolloff") requires the NEW
`gate_reject_log` rows above to feed a hypothetical-outcome tracker (what
would this rejected signal's symbol have returned over the following N days)
and a feedback check in the blacklist logic itself. That tracker is the same
shape as item 3's counterfactual report — natural to build once, together.

### 3. Weekly rejected-signal counterfactual report

**Correction**: the directive names `gate_rejects` — that table doesn't
exist. The real table is **`gate_reject_log`** (confirmed schema:
`id, ts, player_id, symbol, gate_name, reason, signal_id, price,
confidence`). Live counts today: HALT 23,864, MARKET_CLOSED 14,733, GRADE_B
12,366, MAX_TRADES_REACHED 288, MAX_POSITIONS_REACHED 215, PRICE_SANITY 123,
LOW_CONVICTION 21, SCANNER_FILTER 19, QUALITY_GATE_FAILED 6, DRAWDOWN_PAUSE 4.

**Design note before building**: HALT and MARKET_CLOSED dominate by 3+ orders
of magnitude and aren't really "does this gate earn its keep" questions —
they're structural (an agent that's halted or a market that's closed isn't a
tunable decision). The discretionary gates worth counterfactual-testing are
GRADE_B, MAX_TRADES_REACHED, MAX_POSITIONS_REACHED, PRICE_SANITY,
LOW_CONVICTION, SCANNER_FILTER, QUALITY_GATE_FAILED, DRAWDOWN_PAUSE (plus the
new TICKER_BLACKLIST from item 2). Proposed report: weekly cron, per
gate_name in that discretionary set, join `gate_reject_log.symbol` +
`gate_reject_log.ts` against subsequent real price action (N-day forward
return from `ts`) and compare against the executed-signal baseline return
over the same window — same "forward-only, don't retrofit" shape as the
`acted_by_fleet` lesson already banked in `CLAUDE.md`. Not built tonight;
scoped for the bundle window alongside item 2's tracker since they share the
same join logic.

### 4. Single GEX source of truth — root cause confirmed, it's a regime-rule mismatch, not (only) a data mismatch

Traced both numbers. The bridge's `-5.76B` total_gex (`/api/gex/spy`,
confirmed fresh tonight after the P4-13/P2-4 fixes) and whatever the signal
page displays as "negative gamma -5.76B" are likely **the same underlying
number**, given the exact match — but two independently-computed, separately
labeled pipelines exist:

- `engine/options_flow_gex.py` → `flow_gex.db.gex_snapshots` →
  `engine/canonical_gex.py::latest_snapshot()` — regime computed from
  **spot-vs-gamma_flip** (the standard options-market convention: `"LONG
  GAMMA · stable (spot above flip)"`). This is the one collecting fresh data
  today and feeding the bridge + agent prompts (fixed tonight).
- `engine/gamma_context.py` → **a *different* `gex_snapshots` table, same
  name, different database (`trader.db`, not `flow_gex.db`)** — regime
  computed at line ~277 as `"positive" if net_gex >= 0 else "negative"` — the
  **raw sign of aggregate dealer GEX**, a materially different (and, per
  standard options terminology, less correct) rule than spot-vs-flip. This
  feeds War Room prompt injection (`build_gamma_block`, per CLAUDE.md's
  "Gamma grounding" doctrine) and is the likely source of whatever
  "negative gamma" label the signal page inherited.

A market can show negative aggregate GEX while spot sits above the local
flip — the two conventions genuinely disagree in that case, which is exactly
what today's numbers show (total_gex negative, but spot above flip → bridge
correctly says LONG GAMMA, naive sign-check says "negative"). **Proposed
fix**: retire `gamma_context.py`'s own regime computation; have it consume
`canonical_gex.latest_snapshot()`'s regime label directly (or, at minimum,
rename its own field from `regime` to something unambiguous like
`net_gex_sign` so nothing downstream mistakes it for a trading-regime
classification). One canonical regime source, one table, everything else
reads it — not rebuilt tonight given it touches a live War-Room-prompt path,
but the fix is small and well-scoped for the bundle window.

### 5. Small fixes

**"25 of 25 models" display bug** (`signal-center/index.html:2112-2114`):
not a counting bug — a **category mismatch**. `bullCount` counts analyses
with a non-empty `bull_case`; `bearCount` counts non-empty `bear_case`.
Traced the producer (`engine/bull_bear.py`): every model is prompted for
`"the strongest bull case AND the strongest bear case"` (`bull_bear.py:100`)
— **every properly-functioning model returns both fields populated, always**
— there is no directional/lean field anywhere in this data. Counting
non-empty text was never going to produce a real consensus split. Two fix
options: (a) real fix — add a `VERDICT: BULLISH/BEARISH/NEUTRAL` line to the
prompt and parse it into a new field (touches the data-generation side,
correct long-term); (b) fast interim — stop labeling this "Bull/Bear
Consensus" since the data doesn't support a consensus claim; relabel to "N
models provided pro/con analysis" until (a) ships. Recommend (a) for the
bundle, (b) is a one-line label change if a same-night patch is wanted
first.

**Insider feed 0 buys / 29 sells** (`engine/insider_tracker.py:34,114`):
confirmed root cause via a live (read-only) yfinance check on a real ticker.
`get_insider_trades()` reads `row.get("Transaction", ...)` for the
classification text — **yfinance's `Transaction` column is empty on every
row** in current output. The actual transaction description lives in a
different column, `Text` (e.g. `"Sale at price 295.14 per share."`,
`"Stock Gift at price 0.00 per share."`). Since `transaction_type` is always
`""`, the buy/sell keyword match (`"purchase"/"buy"/"acquisition"` for buys)
can never fire on this path — for anything, buy or sell — meaning whatever
"29 sells" the audit observed came through a different code path (not fully
traced here) rather than this one. **Proposed fix**: change line 34 to read
`row.get("Text", row.get("Transaction", ""))` and confirm the keyword sets
still match real `Text` phrasing (`"Sale at price"`, `"Purchase at price"` —
unconfirmed whether genuine purchases render exactly that way; verify against
a ticker with a known recent insider purchase before shipping, not assumed).
Caveat worth noting: insider purchases are also just genuinely rarer than
sales in real markets, so 0-vs-29 may be partly real signal, not purely a
parsing artifact — the fix should be verified against known-purchase cases,
not just declared done once the column read is corrected.

### Nothing shipped
Diagnosis + proposed diffs only, per the Admiral's hold. Item 1's unique
index is small/safe enough to be a strong bundle-window candidate as-is;
items 2+3 share a tracker and should land together; item 4 touches a live
prompt-injection path (War Room) and item 5's insider fix needs a
known-purchase verification step before shipping — none attempted inline
here.

---
## 🟢 P0-B.4 GEX unification — SHIPPED, backfill disagreement answer

Fixed `engine/gamma_context.py::_compute()`'s regime rule to match the
canonical spot-vs-flip convention (same precedence as
`engine/options_flow_gex.py`: flip-based when a flip exists, raw-sign only
as fallback when none found). Verified live: this table's own historical
`total_gex`-sign vs `spot_price`-vs-`gamma_flip` disagreement, by day
(SPY, `source='polygon'` rows only):

| Day | Total rows | Disagreed |
|---|---|---|
| 2026-07-07 | 1 | 1 (100%) |
| 2026-06-29 | 5 | 5 (100%) |
| 2026-06-24 | 24 | 4 (17%) |

**Did any real decision key off the wrong sign?** Checked every consumer of
`gamma_context.py`'s `.regime` field — only `build_gamma_block()`'s War
Room prompt text (`"({regime} gamma) -- {regime_note}"`). No sizing or halt
logic reads it. **Blast radius: War Room's qualitative LLM reasoning was
periodically fed a wrong volatility-regime framing on at least these 3
known days — never a mechanical mis-sizing or wrongly-triggered halt.**

**Important — did NOT touch `engine/gamma_environment.py`.** This is a
THIRD, separate GEX-derived module, and it also uses raw total_gex sign —
but its own comment (`HM-DRYDOCK A1 DOCTRINE 2026-06-09`) explicitly says
this is **intentional**: sign-based read drives DayBlade's real
`sizing_factor` (1.5x up / 0.5x down) as "the correct conservative
volatility input," while display/narrative deliberately uses the
flip-based canonical instead — a considered prior Admiral decision, not a
bug twin of tonight's finding. Confirmed via `git diff --stat` this file
has zero changes from tonight's work.

---
## 🟢 P0-A/P0-B BUNDLE — SHIPPED 2026-07-07 15:35 MST, verification in progress

### Tier 1 (all shipped, live-verified)
1. **Signal dedup**: application-level check in `emit_signal_v2()`
   (`direct_buy_intent` only — schema-level UNIQUE index rejected outright,
   1,286 pre-existing duplicate rows violate it and never-delete-data rules
   out cleaning them first). Verified in isolation (4/4 assertions:
   dup-same-day returns existing id, diff-symbol inserts fresh, non-
   direct_buy_intent types unaffected). **Corrected 7-day conversion trend**
   (deduped denominator vs raw):

   | Day | Raw signals | Deduped | Trades | Raw conv% | Deduped conv% |
   |---|---|---|---|---|---|
   | 07-07 | 1,702 | 37 | 7 | 0.41% | **18.92%** |
   | 07-06 | 2,440 | 285 | 11 | 0.45% | **3.86%** |
   | 07-02 | 2,538 | 374 | 8 | 0.32% | **2.14%** |
   | 07-01 | 2,044 | 389 | 4 | 0.20% | **1.03%** |
   | 06-30 | 2,243 | 467 | 5 | 0.22% | **1.07%** |

   The raw metric was crushed by duplicate-signal noise by roughly an order
   of magnitude — actual conversion is meaningfully healthier than it
   looked. No NEW duplicates observed since restart, but market is closed
   and zero new `direct_buy_intent` signals have fired yet to genuinely
   exercise the live path — re-check during tomorrow's market hours for a
   true live confirmation, not just the isolated test.

2. **Insider feed**: `Text` column fix, live-verified against IBM (7 buys
   now correctly classified, was 0) plus TSLA/CVS/INTC purchase-phrasing
   confirmation.
3. **Bull/bear card**: replaced N-of-N count with explicit `VERDICT` field
   (new prompt line + parser), frontend shows real bullish/bearish/neutral
   split, old-format cache entries correctly excluded (not miscounted)
   until the 1h TTL rolls over.
4. **Blacklist logging**: `_log_learning_block()` now writes to
   `gate_reject_log` (gate_name=`LEARNING_BLOCK`), live-tested end-to-end.

### Tier 2 (all shipped)
5. **Real-quote pricing**: new `engine/options_pricing.py`
   (`get_real_csp_premium`) — **Alpaca primary, not Polygon** (a mid-build
   discovery: Polygon's snapshot endpoint returns populated Greeks but
   zero bid/ask across every strike/expiry/type tested on this account's
   tier, systemic not contract-specific; Alpaca confirmed live-working for
   both SPY and QQQ, the only 2 wheel tickers not already blocked by
   door1). Wired into `wheel_strategy.py:227` and `shadow_csp.py:177`, no
   fallback (None quote = skip the trade, never fabricate).
   **Consequential finding, surfaced not buried**: real ATM-ish 30-45DTE
   put premiums measured 0.12%-0.21% ROC vs. the strategy's own 3.0%
   MIN_PREMIUM_RETURN threshold — roughly 15-25x below. Under real
   pricing, wheel_strategy.py and shadow_csp.py will not clear their own
   entry bar under current parameters (12% OTM / 30 DTE / 3% min ROC).
   This is the honest structural reality the fix reveals, not a bug in the
   fix — the strategy's parameters were implicitly calibrated against
   fantasy premiums. Re-tuning those parameters is a separate decision,
   not made here.
   New era constant `TROI_REAL_QUOTES_ERA_START = "2026-07-07"`
   (`paper_trader.py`), deliberately independent of `TROI_V2_ERA_START`
   (cap-gate axis).
6. **Mark-to-market**: discovered genuinely dead infrastructure —
   `options_trades.mtm_intrinsic` column + `scripts/refresh_mtm_intrinsic.py`
   existed but were never cron-scheduled nor consumed anywhere. Ran once
   (populated all 6 open CSPs, currently $0.00 — all OTM), scheduled going
   forward (weekdays 15:55 ET), wired `unrealized_pnl`/`total_equity` into
   `/api/options/book-summary`.
7. **GEX unification**: fixed `gamma_context.py`'s regime rule to match
   canonical spot-vs-flip precedence. Backfill: SPY rows disagreed
   2026-07-07 (1/1), 06-29 (5/5), 06-30 (24, 4 disagreed). Blast radius
   confirmed: War Room prompt text only, zero sizing/halt consumers.
   **Did NOT touch `gamma_environment.py`** — its raw-sign rule driving
   DayBlade's real `sizing_factor` is an explicit, documented 2026-06-09
   Admiral decision (`HM-DRYDOCK A1 DOCTRINE`), confirmed a different,
   intentional divergence, not a bug twin.
8. **Leaderboard era-fence**: found the ACTUAL leaderboard-grading call
   site (`dashboard/app.py`'s `season_realized["options-sosnoff"]`
   computation) was using `_csp_realized_pnl_v1` (cap-gate-fenced only,
   still 100% synthetic) — switched to new `_csp_realized_pnl_real_quotes`
   (pricing-integrity-fenced). **Troi's season-anchored return_pct: this
   fix removes the full $29,868.74 synthetic CSP contribution from her
   graded figure — it will now read $0.00 CSP contribution until a
   real-quote CSP closes. A dramatic drop in her displayed return is the
   fix working, not a regression — her old ~500%+ return was a
   synthetic-fill artifact.** `get_portfolio_with_pnl()` also gained
   parallel `csp_pnl_synthetic_restated`/`csp_pnl_real_quotes`/
   `return_pct_restated`/`csp_pricing_note` fields (raw `total_value`/
   `return_pct` left untouched for backward compat). Bakeoff scorer
   (`shadow_csp_scorecard.py::_closed_csps`) same fence — baseline + both
   ghost seats now correctly show 0 closes, not the tainted 95%-WR-style
   verdict.

### Post-restart verification results

**GEX same-minute check: RULE now agrees, DATA FRESHNESS does not (yet) —
honest finding, not glossed over.** Live comparison just now:
- `gamma_context.py` (live-computed, fixed rule): regime=negative, spot=747.71, flip=748.41
- Bridge `canonical_gex` (last collector run, 13:05 MST — market now closed): regime=stable/above-flip, spot=751.28, flip=750.53

Both correctly apply spot-vs-flip precedence — the DISAGREEMENT here is
because they're reading spot at DIFFERENT MOMENTS (gamma_context computes
fresh on every call; the canonical collector only runs once daily at 13:05
AZ), and SPY genuinely moved from 751.28 to 747.71 (crossing the ~748.4
flip) between those two samples. **This is a separate, pre-existing
cadence-mismatch limitation, not a re-emergence of the rule bug just
fixed.** Tonight's fix guarantees the two sources compute the SAME regime
FROM THE SAME SPOT/FLIP pair; it does not guarantee they sample spot at
the same moment. Closing that gap fully would mean either running the
canonical collector far more frequently, or having `gamma_context.py` stop
computing live and only read the (then-stale) canonical snapshot — the
latter would make War Room's intraday gamma grounding meaningfully staler
than it is today. Flagged as a follow-up decision, not solved here;
re-check "same regime, same minute" during live market hours tomorrow,
when the canonical collector's most recent 13:05 snapshot will be much
closer to current price.

---
## Follow-ups from 1311da3 (Admiral, no urgency)

**1. Wheel re-tune: HOLD.** wheel_strategy.py's 12%-OTM/30-DTE/3%-min-ROC
parameters stay as-is. The strategy will sit idle under real pricing (see
1311da3's measured 0.12-0.21% real ROC vs. the 3.0% bar) until a deliberate
re-tune, which is a **separate design exercise against real chain data**
— not a quick parameter tweak, and not done as part of the fill-integrity
fix. door1 (leveraged-ETF CSP ban) stays in force regardless. Proposal to
the Admiral when scoped; no code touched here.

**4. Guard on the 84% figure**: `return_pct_restated`/the leaderboard's
84.0% for options-sosnoff is **corrected accounting on synthetic-era
fills — not evidence the strategy has edge.** It reflects removing a
known-fake $29,868.74 CSP contribution from a display figure, nothing
more. Real performance grading starts from zero real-quote CSP closes
(TROI_REAL_QUOTES_ERA_START, 2026-07-07 onward) and needs its own sample
before anyone should read it as a track record. Guard note added to the
leaderboard's `book_status` field (see dashboard/app.py) so this isn't
just a backlog-only caveat — see F4 below for the exact wording shipped.

**2. GEX interim (shipped)**: added visible "as of" timestamps to both the
bridge (`dashboard/static/bridge-v2.html`'s `gexMeta` line) and
signal-center (`signal-center/index.html`'s GEX card expand, new "As Of"
row) — transparency only, not a fix for the underlying cadence mismatch.
Intraday recompute (making the canonical collector run frequently enough
that gamma_context.py's live computation and the canonical snapshot are
never far apart) is the real fix, tracked separately as its own P2 window,
not attempted here.

**3. PENDING — tomorrow at open (2026-07-08, ~06:30 MST)**: confirm the
`emit_signal_v2()` dedup (commit `1311da3`) holds under genuine live
signal flow (not just the pre-restart isolated test) — check for any new
`direct_buy_intent` duplicate rows in `signals_v2` once real scan activity
resumes, and post the corrected 7-day signal→trade conversion series
(deduped denominator) as the new tracked baseline going forward. Not
actionable tonight — market closed, zero new `direct_buy_intent` signals
have fired since the restart to genuinely exercise the live path.

**4. Guard strengthened (code-complete-and-held)**: `dashboard/app.py`'s
`book_status` note for options-sosnoff now reads "displayed return is
corrected accounting on synthetic-era fills, NOT edge evidence · real-quote
grading starts 2026-07-07, needs its own sample first" — explicit per the
Admiral's exact framing. This is a Python string literal inside the live
trader process; needs the next restart to go live (F2's two HTML/JS
changes are static files, no restart needed, already live). No urgency
noted, so held rather than forcing an out-of-band restart for a wording
change alone.


---
## ⚠️ SUPERSEDED — see the corrected re-run further below

**Do not trust the numbers in this first run.** MARKET_CLOSED showed
avg fwd_1d=+115.030% and MAX_TRADES_REACHED showed +13.258% — both
impossible for real stock/ETF moves at this horizon. Traced live: ~1.5%
of `gate_reject_log.price` values for MARKET_CLOSED are sub-$1 for
normally-priced stocks (e.g. HIMS logged at $0.91 against a real
~$30-60 range) — an upstream data-quality bug (not yet root-caused to
the exact caller), not a bars/return-calc bug. A handful of these produce
+1000%+ "returns" that dominate a naive mean. Caught before this stood as
the record; script fixed (outlier guard: entry price >= $1, |return| <=
100%; median reported alongside mean) and re-run — see the corrected
table below this one for the trustworthy numbers.

## Counterfactual Report — 2026-07-07 (P1 measurement layer, scripts/counterfactual_report.py) — SUPERSEDED, KEPT FOR THE RECORD

# Counterfactual Report — 2026-07-07
Window: last 30 days (2026-06-07 to 2026-07-07)

Executed baseline (BUY signals that went live): n=29/26/26 (1d/3d/5d with bars available)
  avg fwd_1d = -1.074%
  avg fwd_3d = -0.409%
  avg fwd_5d = 0.355%

| Gate | Blocked (deduped) | Bars avail | avg fwd_1d | avg fwd_3d | avg fwd_5d | Structural? |
|---|---|---|---|---|---|---|
| HALT | 3803 | 2736 | -0.058% | 0.446% | 0.952% | yes |
| MARKET_CLOSED | 1704 | 1219 | 115.030% | 121.125% | 130.853% | yes |
| GRADE_B | 1532 | 1145 | -0.390% | 0.868% | 0.782% |  |
| MAX_TRADES_REACHED | 232 | 141 | 13.258% | 12.596% | 12.795% |  |
| MAX_POSITIONS_REACHED | 128 | 82 | -0.733% | 0.344% | 0.717% |  |
| LOW_CONVICTION | 21 | 10 | -1.016% | 0.076% | 0.451% |  |
| SCANNER_FILTER | 17 | 9 | -2.715% | -2.486% | -2.486% |  |
| QUALITY_GATE_FAILED | 5 | 3 | 1.916% | 3.547% | 3.547% |  |
| DRAWDOWN_PAUSE | 2 | 1 | -10.128% | n/a | n/a |  |
| LEARNING_BLOCK | 1 | 0 | n/a | n/a | n/a |  |

---
## 🟢 P1.3 REPORT CARD METRIC SWAP — code-complete-and-held for next window

`engine/agent_ratings.py::calculate_rating()` — win_rate was worth up to
**50/100 points** in the composite score (0-40 base scaled at `WR*0.5`,
explicitly commented "THE most important metric," plus +5/+5 "elite"/
"legendary" bonus tiers at 70%/80% WR). Replaced with:
- **Expectancy** (`avg_win*WR - avg_loss*(1-WR)`, the standard formula —
  WR still appears INSIDE it, correctly weighted by win/loss magnitude,
  just no longer ALSO a separate stacked bonus): 0-50 pts (was WR's slot).
- **Profit factor**: 0-25 pts (was 20 — absorbed some of WR's old weight).
- **Max drawdown**: 0-25 pts, penalty-based, computed from a reconstructed
  chronological equity curve (peak-to-trough over the rating sample,
  normalized against the same $7k reference `_MAX_SANE_PNL` already uses).
- Trade-count bonus and consecutive-loss penalty kept as-is.
- `win_rate` stays in the result dict as a **display-only** field
  (`win_rate_is_display_only: True` added so no future consumer mistakes
  it for a grade input again).

**Era-fence check**: this scorer was ALREADY options-exclusive before
tonight (`calculate_rating` filters `asset_type in ("option","options")`
and rejects non-stock-ticker symbols at line ~115) — options-sosnoff's CSP
P&L was never part of this specific report card to begin with. The
Admiral's "era-fence applies" instruction is satisfied by this pre-existing
exclusion; no additional filtering was needed here. (Troi's era-fencing
lives in the leaderboard's `season_realized` computation and
`get_portfolio_with_pnl()`, already shipped in commit `1311da3`.)

Live-tested against an isolated DB copy (capitol-trades: A/80.2,
WR=90.9% shown for display, score actually driven by expectancy=$6.44/trade,
PF=7.82, maxDD=0.52%). Held — this is a Python function change inside the
live trader process, needs the next restart to take effect, per the
Admiral's explicit "rides the next bundled restart" sequencing.

---
## 🟢 P1.2 TRIPLE-BARRIER LABELING BACKFILL — shipped, ran tonight

`scripts/label_signals.py` (new, resumable via `signal_labels.signal_id`
UNIQUE constraint + checkpointed commits every 200 rows). v1 params:
2.0x ATR(14) profit-take, 1.0x ATR(14) stop, 5-day time barrier, stop
checked first on same-day ambiguity (conservative/worst-case ordering,
documented in `params_json` per row for traceability if re-labeled later
with different params).

**Result**: 13,465 deduped historical signals (all of `signals_v2`,
2026-05-23 to 2026-07-07, 917 distinct symbols) → 8,085 labeled, 5,362
skipped (no bars available or insufficient prior ATR history — mostly the
earliest signals in the window, which don't have 14 prior trading days of
bars before them). Label distribution: 4,271 time-barrier (52.8%), 2,815
stop (34.8%), 1,017 profit-take (12.6%) — the stop being hit more often
than the profit-take is the expected/correct asymmetric-barrier behavior
(1x ATR stop is a narrower target than a 2x ATR profit-take), not a bug.

This produces labels ONLY — explicitly NOT the meta-label gate or
per-agent IC per the Admiral's sequencing ("do not build those yet, labels
first"). New `signal_labels` table: `signal_id, symbol, entry_date,
entry_price, atr, label, barrier_hit, fwd_return, params_json,
computed_at`.

---
## 🟡 NEW FINDING: gate_reject_log has occasional garbage price values

Discovered while building the P1 counterfactual report (2026-07-07).
~1.5% of `MARKET_CLOSED` rejections in the last 30 days carry a `price`
under $1.00 for symbols that don't trade anywhere near that (HIMS logged
at $0.91, real range ~$30-60). `_log_gate_reject`'s MARKET_CLOSED call
sites in `engine/paper_trader.py` (lines ~669, ~1774, ~4494) just pass
through whatever `price` the caller already computed — this isn't a
logging-layer bug, the bad value is coming from further upstream (which
specific caller/scanner computes a sub-$1 "price" for an otherwise
normally-priced symbol, not traced tonight). Low volume (~135/8,686
MARKET_CLOSED rows in 30 days) but each one can produce a wildly wrong
counterfactual return if not filtered, which is exactly what happened in
the first (superseded) counterfactual-report run above. Worth a targeted
grep for anywhere `price` gets computed before reaching `buy()`'s
MARKET_CLOSED gate — not scoped or attempted here, filed for a future
window.

**Note**: the corrected script accidentally ran twice back-to-back (a
background invocation that appeared stalled, so it was re-run in the
foreground — both actually completed). Both appends below are valid,
outlier-guarded runs; the small differences between them (e.g. slightly
different executed-baseline sample sizes) are real run-to-run variance in
Alpaca's bars response a few minutes apart, not a bug. Kept both rather
than deleting either.

---
## Counterfactual Report — 2026-07-07 (P1 measurement layer, scripts/counterfactual_report.py)

# Counterfactual Report — 2026-07-07
Window: last 30 days (2026-06-07 to 2026-07-07)
Outlier guard: entry price >= $1.00, |forward return| <= 100% (see script docstring for why)

Executed baseline (BUY signals that went live): n=24/22/22 (1d/3d/5d with bars available)
  fwd_1d: mean=-0.020%, median=0.457%
  fwd_3d: mean=-0.877%, median=-0.196%
  fwd_5d: mean=-0.603%, median=0.823%

| Gate | Blocked (deduped) | Bars avail | mean/median fwd_1d | mean/median fwd_3d | mean/median fwd_5d | Structural? |
|---|---|---|---|---|---|---|
| HALT | 3803 | 2491 | -0.11/0.11% | 0.22/0.53% | 0.55/0.61% | yes |
| MARKET_CLOSED | 1706 | 1067 | 0.97/0.25% | 1.59/0.79% | 2.10/1.36% | yes |
| GRADE_B | 1532 | 994 | -0.41/-0.70% | 1.10/0.16% | 1.13/0.00% |  |
| MAX_TRADES_REACHED | 232 | 131 | -0.21/0.06% | -0.76/0.71% | -0.72/0.53% |  |
| MAX_POSITIONS_REACHED | 128 | 73 | -0.84/-0.41% | 0.47/0.77% | 0.42/-0.20% |  |
| LOW_CONVICTION | 21 | 10 | -0.44/-1.20% | 0.80/1.54% | 1.51/1.39% |  |
| SCANNER_FILTER | 17 | 9 | -1.82/-1.81% | -0.80/0.11% | -0.80/0.11% |  |
| QUALITY_GATE_FAILED | 5 | 4 | 0.47/0.23% | 2.03/1.11% | 2.03/1.11% |  |
| DRAWDOWN_PAUSE | 2 | 0 | n/a | n/a | n/a |  |
| LEARNING_BLOCK | 1 | 0 | n/a | n/a | n/a |  |


---
## Counterfactual Report — 2026-07-07 (P1 measurement layer, scripts/counterfactual_report.py)

# Counterfactual Report — 2026-07-07
Window: last 30 days (2026-06-07 to 2026-07-07)
Outlier guard: entry price >= $1.00, |forward return| <= 100% (see script docstring for why)

Executed baseline (BUY signals that went live): n=22/20/20 (1d/3d/5d with bars available)
  fwd_1d: mean=-1.873%, median=-0.317%
  fwd_3d: mean=-1.429%, median=-0.089%
  fwd_5d: mean=-0.647%, median=-0.089%

| Gate | Blocked (deduped) | Bars avail | mean/median fwd_1d | mean/median fwd_3d | mean/median fwd_5d | Structural? |
|---|---|---|---|---|---|---|
| HALT | 3803 | 2186 | -0.24/0.09% | 0.29/0.51% | 0.66/0.59% | yes |
| MARKET_CLOSED | 1706 | 907 | 0.73/0.34% | 1.58/0.69% | 1.91/1.27% | yes |
| GRADE_B | 1532 | 943 | -0.30/-0.66% | 0.90/0.07% | 0.99/-0.02% |  |
| MAX_TRADES_REACHED | 232 | 144 | -0.70/-0.20% | -0.56/0.57% | -0.46/0.53% |  |
| MAX_POSITIONS_REACHED | 128 | 71 | -0.27/-0.31% | 0.68/0.97% | 1.19/0.38% |  |
| LOW_CONVICTION | 21 | 11 | 0.04/-0.19% | 2.26/2.47% | 4.39/4.26% |  |
| SCANNER_FILTER | 17 | 9 | -0.55/-0.19% | 1.52/0.45% | 1.84/0.45% |  |
| QUALITY_GATE_FAILED | 5 | 3 | -1.14/-1.39% | -0.10/-0.73% | -0.10/-0.73% |  |
| DRAWDOWN_PAUSE | 2 | 1 | -10.13/-10.13% | n/a | n/a |  |
| LEARNING_BLOCK | 1 | 0 | n/a | n/a | n/a |  |

---
## 🟢 signal_labels audit — clean, does NOT share gate_reject_log's price corruption

Checked per Admiral follow-up. **No shared exposure, confirmed both
structurally and empirically:**

- **Structural**: `scripts/label_signals.py` never reads `gate_reject_log`
  or any DB-stored price field at all. `entry_price` comes exclusively
  from freshly-fetched Alpaca OHLC bars (`entry_bar["c"]`) — a completely
  independent data path from whatever upstream caller corrupted
  `gate_reject_log.price`.
- **Empirical**: `SELECT COUNT(*) FROM signal_labels WHERE entry_price <
  1.0` = **0**. The lowest entries that DO exist (ARTL ~$1.08-1.31, SKYQ
  ~$1.13-1.20) are consistent across many separate dates for the same
  symbol — the signature of a genuinely low-priced security, not a
  one-off corruption artifact (which would show as an anomalous outlier
  against that symbol's own normal range, the way HIMS at $0.91 did
  against its real ~$30-60 range).

No re-labeling needed. **Correction to this same entry**: initially
planned to add a defensive $1-floor guard to `label_signals.py` anyway
"since it's cheap" — checked first and reverted that plan. 22 distinct
symbols in the existing 8,085 labels legitimately trade under $5 with
*consistent* pricing across many dates (SNAP alone: 47 rows, $4.35-4.85 —
its real recent range; ARTL: 23 rows, $1.09-1.71), not the anomalous
single-point drop HIMS showed in `gate_reject_log`. A blanket $1 floor
would have silently excluded real signals for legitimately low-priced
securities going forward — the wrong fix for a bug that isn't present
here. No guard added; `label_signals.py`'s entry_price stays sourced
purely from Alpaca bars, unguarded, because the empirical check already
shows nothing to guard against.

---
## 🟢 signal_labels backfill — finished, 98.97% complete

Re-ran `scripts/label_signals.py` (resumable, picked up exactly where the
first run left off). **5,231 of the remaining 5,362 labeled this pass**
(97.6%) — the first run's skips were mostly transient: it ran concurrently
with `counterfactual_report.py` hitting the same Alpaca bars endpoint,
which likely rate-limited/timed-out some fetches (both scripts fail-open
to an empty bars dict on any error, so a transient hiccup silently became
a permanent-looking skip). Running alone resolved almost all of it.

**Final: 13,334 of 13,465 deduped signals labeled (98.97%)**. Distribution
held steady: 52.3% time-barrier, 35.2% stop, 12.5% profit-take (matches
the first run's proportions almost exactly, confirming the extra labels
didn't skew the distribution).

**Genuinely remaining 131**: 128 are `APLS` (Apellis Pharmaceuticals, a
real, liquid biotech — not a data-coverage or delisting issue). Diagnosed:
Alpaca's IEX feed returns only 13 daily bars for APLS in the relevant
early-window date range, one short of the 14 needed for a full ATR(14)
calc — a genuine sparse-data gap for that specific ticker/window, not a
script bug (confirmed live: `_fetch_ohlc('APLS', ...)` returns real,
correctly-priced bars, just fewer than the period requires). The other 3
(`AFAXX` — likely a money-market-fund symbol, not an equity; `SMOKE` — the
literal smoke-test row, signal_id=1, not a real signal) are expected
non-labelable rows, not gaps. Not chased further — genuinely small, well-
understood residual, not worth the time against "no urgency."

---
## Standing constraint: meta-label gate + IC dashboard stay UNBUILT

Per Admiral: not started until `signal_labels` has a clean week of
forward-generated labels (not just tonight's one-time backfill) —
explicitly citing the evaluator lesson (the `acted_by_fleet` retrospective-
join dead end banked in `CLAUDE.md`: don't build the consumer on top of
data that hasn't been proven to accumulate correctly under real, ongoing
conditions first). Tonight's backfill populated history; it does not by
itself establish that the labeling pipeline behaves correctly under live,
day-to-day operation (new signals arriving, ATR windows rolling forward,
bars fetched without the concurrent-script contention that caused this
session's transient skips). No code for either the meta-label gate or the
IC dashboard is scoped or started — this section exists purely so a future
session doesn't start building on the assumption the labels are
immediately trustworthy for that purpose.

---
## 🟢 HM-PERF-FLEET-THROUGHPUT — code-complete-and-held for bundled restart

Admiral-approved client-side performance work, companion to server-side
flags (FLASH_ATTENTION, KV q8_0, NUM_PARALLEL=2, MAX_LOADED_MODELS=2)
applied separately on olliemax — confirmed live via
`systemctl show ollama --property=Environment`.

### 1. OllamaQueue: 1 worker → OLLAMA_QUEUE_WORKERS (default 2)

`engine/ollama_queue.py` redesigned for N concurrent workers with two-tier
model-affinity routing, replacing the single `_resident_model` string with
`_active_models` (Counter — models a worker is executing RIGHT NOW, the
tier that actually captures the NUM_PARALLEL=2 win) and `_resident_models`
(LRU set capped at `num_workers`, matching MAX_LOADED_MODELS=2 — models
recently dispatched and probably still loaded). Swap detection updated to
match: a swap is dispatching a model outside BOTH sets while the resident
set is already at capacity (not just "differs from the last task," which
would over-log once >1 model can legitimately co-reside). Two-lane
scan/WR fairness and the `WR_ANTI_STARVE_K` anti-starvation cap are
unchanged in spirit — the lock still serializes every scheduling
*decision* even though execution now happens concurrently, so the
existing counter logic generalizes without modification.

**Rollback parity, deliberately engineered**: resident-set capacity is
tied to `num_workers` (`max(1, num_workers)`), so `OLLAMA_QUEUE_WORKERS=1`
collapses to tracking exactly one resident model — byte-for-byte matching
the pre-2026-07-07 single-string design. Verified by a dedicated test
(`test_num_workers_1_collapses_resident_capacity_to_one`).

`config.py`: `OLLAMA_QUEUE_WORKERS = int(os.environ.get(..., "2"))`.
Rollback: set to `1`, no other code changes needed.

**Tests** (`tests/test_ollama_queue_fairness.py`, rewritten + extended —
11/11 passing): the 5 original fairness/anti-starvation tests updated to
seed the new `_resident_models`/`_active_models` state (same test intent,
new internal shape) — confirms no regression in the HM-TIER3-SIGNAL-DROP
fix. 6 new tests: active-model affinity beats resident-only affinity;
resident set correctly holds 2 distinct models at `num_workers=2` without
flagging a swap; a 3rd distinct model at capacity IS correctly flagged a
swap; `num_workers=1` rollback parity; **genuine concurrent execution**
verified via a `threading.Barrier` (two slow calls provably overlap in
wall-clock time, not just complete quickly in sequence); `num_workers=1`
verified to genuinely serialize (max concurrent execution count = 1 across
4 submitted calls) — the rollback path is provably NOT silently still
parallel-capable.

### 2. num_ctx cap — measured, not guessed

Directive suggested "likely 4096-6144." **Measured against real production
traffic instead**: `api_costs.input_tokens`/`output_tokens`,
`call_type='scan'`, the 5 currently-active Ollama agents, last 30 days
(n=5,845). Input-only: p50=7,624, p95=8,394, p99=8,541, max=8,769.
Input+output total: p50=7,698, **p95=8,719**, p99=31,977 (an outlier
tail — almost certainly qwen3 thinking-mode leakage past the `think:False`
guard, not the normal distribution).

**The directive's own suggested range (4096-6144) would have truncated
the MEDIAN real prompt**, let alone p95 — confirms measuring first,
rather than shipping the guessed range, mattered here. Set
`num_ctx=10240` (engine/providers/ollama_provider.py, in the `options`
payload) — ~17% headroom above the real p95, deliberately not sized to
the p99 tail (would cost 3-4x the per-slot VRAM to protect <1% of calls
that are already anomalous).

### 3. Stale hardware references — fixed

`engine/ollama_queue.py` docstring and `engine/providers/ollama_provider.py`
Fix-4 comment both corrected: RTX 5060/8GB/"one model fits" →
RTX 5080/16GB/"two 7-8B co-resident" (matches the already-corrected
`docs/runbooks/ram-discipline.md` and `config.py:241`, which had this
right since 2026-05-30 — only these two files were still stale).
`healthcheck.py:24` also had a stale "RTX 5060" comment — fixed. **Found
and flagged, not fixed**: that same line's IP is `192.168.1.166`, but
every other live reference to Ollie Max (`config.py`'s `OLLIE_URL`, this
whole ticket's own testing) uses `.168` — possible stale/wrong IP,
out of scope for a hardware-comment fix, not verified or touched.

### 4. Load test — corrected after two self-caught methodology bugs

First pass reported a suspicious "6.63x speedup" (2-worker scan_p95=0.25s)
and an alarming multi-hour wall of VRAM eviction/reload log lines. Neither
survived scrutiny as originally interpreted:

- **Speedup confound**: the first pass used an IDENTICAL fixed prompt for
  every call. Ollama's prompt-prefix KV-cache meant the second (2-worker)
  run's repeated, byte-identical prompts on an already-warm model
  re-processed near-zero input tokens — a cache-warming artifact, not a
  concurrency measurement. Fixed: every call now gets a unique nonce
  appended, forcing a genuine cold-prefix generation each time.
- **Journal false alarm**: the journal check used an open-ended `--since`
  with no `--until`. Because this script's own OUTPUT was read much later
  in wall-clock time than the test itself ran, the grep swept up ~2.5
  HOURS of subsequent, ordinary journal activity from the STILL-LIVE,
  unrestarted (old single-worker code) trader process — easily misread as
  "the test caused this thrashing" when it was pre-existing production
  traffic, unconnected to a test that took under 2 seconds total. Fixed:
  both `--since` and `--until` now bound the window tightly to the actual
  test duration.

**Corrected, trustworthy numbers** (verified against Ollama's own
server-reported `total_duration`/`prompt_eval_count`, which matched the
client-measured wall time almost exactly — 150ms server vs 156ms client
for one independently-verified sanity call, confirming these are real
generations, not silently short-circuited):

| | wall (6 calls: 4 scan + 2 WR) | scan_times | scan_p95 | errors |
|---|---|---|---|---|
| 1 worker (baseline) | 0.91s | [0.19, 0.14, 0.13, 0.13] | 0.22s | 0 |
| 2 workers (new) | 0.49s | [0.16, 0.16, 0.16, 0.17] | 0.17s | 0 |

**Speedup: 1.86x** — close to the ~2x theoretical ceiling for doubling
worker capacity, the gap being thread-coordination/lock overhead. Model:
`qwen3:8b`, warm (10m keep_alive), FLASH_ATTENTION + KV q8_0 active —
individual calls this fast (hundreds of ms for ~1,300-token prompts) are
genuinely plausible on an RTX 5080 with flash attention on an
already-resident model, not a bug; confirmed via the independent
sanity-check call's matching server/client timing.

**Journal, correctly scoped to the actual ~2s test window**: `NO_MATCHES`
— clean, no eviction/offload/OOM events during the test itself.

**Not directly tested (would require the live restart)**: sustained,
larger-scale concurrent load matching real production WR-burst volume,
and the num_ctx=10240 cap's actual VRAM footprint under genuine 2-slot
concurrent 8k+-token prompts (this test used ~1,300-token synthetic
prompts for practicality — see script docstring). The mechanics
(concurrency, fairness, rollback, swap detection) are unit-tested
exhaustively; the absolute-scale production validation is the real
post-restart verification this ticket's item 4 asks for.

### Verification checklist for after the restart (not run yet — held)
- [OLLAMA-QUEUE-SWAP] frequency unchanged or lower vs. pre-restart baseline
- No new REQUEST_TIMEOUT skips
- Sentinel green
- Fleet scan cycle wall time before/after — to be posted here once measured
  live (this ticket's synthetic test above is the pre-restart substitute,
  not a replacement for the real post-restart number)

New scripts: `scripts/hm_perf_queue_loadtest.py` (this load test, rerunnable
anytime, standalone, does not touch the live trader process).

---
## 🟡 HM-PERF-FLEET-THROUGHPUT — POST-RESTART VERIFICATION (2026-07-07 18:32:06 restart)

Restart executed 18:32:06 MST. Standard checklist first, then the 4
HM-PERF-specific items — **honest result: 2 of 4 look good, 2 show a
concerning early signal that needs continued monitoring, not declared
resolved.**

### Standard checklist — all pass
Audition gate active (1 auditioning seat, enforcement=ON), all 10 gated
agents + new audition seat correctly gated (backfill ready=True), WAL
checkpoint clean (`0|0|0`), sentinel green and correctly tracking new
PID 37400, zero tracebacks/critical in post-restart logs.

### 1. [OLLAMA-QUEUE-SWAP] frequency — HIGHER, not lower (+19.8%)

Used the queue's own internal `swap#N` counter (resets to 0 per process
boot — confirmed the only reliable boundary marker; `trader_error.log`
has no date prefix, only HH:MM:SS, so naive timestamp-string comparison
silently corrupted an early attempt at this analysis by conflating
today's entries with prior days' at the same clock time — corrected
before trusting it).

- **Before** (1-worker, previous restart-to-restart window 15:35:51-18:32:06,
  2.93h): 141 swaps → **48.1 swaps/hr**.
- **After** (2-worker, first 25 min post-restart, 18:32:06-18:57): 24 swaps
  → **57.6 swaps/hr**.
- **Change: +19.8%.**

**Working theory, not confirmed**: the fleet actively rotates through 6+
distinct models (plutus-v1, plutus-v7d, gemma4:12b-it-qat, qwen3:4b,
qwen3:8b, ministral-3:3b observed in this window alone) against a
resident capacity of only 2. With 2 concurrent workers, both slots can
independently pick up DIFFERENT models at once — filling 2 execution
slots with 2 different models is easier to trigger than serially cycling
through them 1 at a time, which may increase apparent model diversity in
flight and thus eviction pressure. The isolated synthetic load test
(same model both lanes, controlled) showed a clean 1.86x win — this
LIVE, mixed-model result does not replicate that cleanly, which is itself
the finding: **the fleet's real bottleneck may be model-diversity-vs-
resident-capacity, not raw worker concurrency**, and 2 workers doesn't
fix that on its own.

### 2. REQUEST_TIMEOUT skips — improved (1 vs 34)

Corrected the same date-ambiguity bug (see above) before trusting this
too. 34 timeouts in the comparable prior window, only 1 since restart (a
single isolated WR-lane qwen3:8b timeout at 19:53:48 — WR calls are
documented latency-tolerant, occasional timeouts under load are expected
and this rate is a clear improvement). **This item passes.**

### 3. Sentinel green — confirmed, passes.

### 4. Fleet scan cycle wall time (WR-DUR), before vs after — WORSE in this small sample

| | n | mean | median |
|---|---|---|---|
| Before (1-worker, last 10 pre-restart cycles) | 10 | 21.7s | 13.2s |
| After (2-worker, first 5 post-restart cycles) | 5 | 43.5s | 46.1s |

**Also worse, not better, in this sample.** Two honest caveats before
reading too much into this: (a) n=5 post-restart is a genuinely small
sample — WR cycle wall time is highly variable (this same "before" data
ranges 1.4s-60.7s cycle-to-cycle depending on queue depth at cycle
start), and (b) the first several post-restart cycles include cold-start
model-loading overhead (nothing was resident immediately after the
restart) that a longer window would dilute out. Both of these numbers
are also consistent with, and plausibly explained by, the elevated swap
rate in item 1 — if swaps are genuinely more frequent, and each swap
costs real load time, worse cycle times would be the expected downstream
consequence, not a separate coincidence.

### Verdict: DO NOT DECLARE THIS TICKET'S SUCCESS CRITERIA MET YET

The isolated, controlled load test (docs/XO_BACKLOG.md, same-model
concurrent dispatch) is real and its 1.86x speedup stands on its own
merits — that mechanism works as designed. But the two production
metrics this ticket explicitly asked to verify (swap frequency, cycle
wall time) both moved the WRONG direction in the first 25 minutes of
live mixed-model traffic, and the working theory (resident-capacity
vs. model-diversity being the actual constraint) is a real, substantive
possibility that would mean the throughput ceiling here isn't
worker-count, it's how many distinct models the fleet calls per cycle
relative to MAX_LOADED_MODELS=2. **Recommend**: let this run longer
(a few hours, ideally spanning market-open real WR-burst volume, not
just after-hours quiet traffic) before drawing a final verdict, and if
the elevated swap rate persists at scale, the next investigation is
fleet model diversity vs. resident capacity — not a workers-count
tweak. Rollback path (`OLLAMA_QUEUE_WORKERS=1`) remains available and
verified byte-parity-safe if the extended window confirms a regression
rather than settling out.

---
## HM-PERF decision rule for tomorrow (2026-07-08 market open)

**Admiral ruling**: judge `OLLAMA_QUEUE_WORKERS=2` on the **market-hours
scan window only** (same-model qwen3:8b burst — the workload the isolated
synthetic load test actually modeled, and the workload most representative
of the fleet's real hot path), **not** the after-hours evening WR mix this
first verification pass happened to catch (6+ distinct models, only 2
resident slots — a much harder case for any worker-count change to help
with, and not what tonight's mixed 1.86x-synthetic-vs-worse-live-mix
result should be read as a verdict on).

**Decision tree**:
- **Scan wall time improves AND swaps stay flat during the scan
  window** → keep `OLLAMA_QUEUE_WORKERS=2` as-is. No further action.
- **Swaps rise even during the scan window** (same-model burst, where a
  rise would mean something is wrong beyond the evening mixed-model
  effect already suspected) → **patch strict-affinity mode** (pre-built
  below, held) **before** considering rollback to 1. Strict-affinity is
  the more surgical fix: it directly targets the theory from tonight's
  verification (worker 2 may be introducing model diversity rather than
  pure same-model concurrency) without giving up the 2-worker throughput
  gain entirely.
- Either branch: **post scan-cycle wall time before/after to this file**
  once measured, using tomorrow's real market-hours data — tonight's
  numbers were after-hours only and are not a substitute.

### Strict-affinity mode — pre-built, held, ready if triggered

Design: workers with index > 0 (i.e., every worker beyond the first) only
dispatch a task whose model_id is ALREADY in `_active_models` (something
another worker is executing that exact model RIGHT NOW) — if no such task
exists in either lane, that worker idles rather than picking up a
different-model task, even if one is waiting. Worker 0 is unrestricted
(normal fairness + two-tier affinity, unchanged). This makes worker
1+'s ENTIRE role "pure NUM_PARALLEL=2 exploitation for whatever worker 0
is already running" — it can never be the thing that introduces a 3rd
model or fills a 2nd resident slot with something different, which is
exactly the mechanism tonight's verification flagged as the likely cause
of the +19.8% swap-rate increase.

Feature-flagged: `OLLAMA_QUEUE_STRICT_AFFINITY` (config.py, default
False — current shipped behavior unchanged unless explicitly triggered).

**BUILT AND TESTED, held**: `engine/ollama_queue.py`'s `_take_next_locked`
now accepts `worker_index`; workers 1+ under strict mode only dispatch a
task whose model_id is already in `_active_models`, else return `None`
(idle) even with other work queued — worker 0 is always unrestricted.
`_worker_loop` threads `worker_index` through from `Thread(args=(i,))`.
`status()` exposes `strict_affinity` for observability.

6 new tests in `tests/test_ollama_queue_fairness.py` (17/17 total
passing): worker 1 idles with no active-model match even when other work
exists; worker 1 correctly takes a matching task; worker 1 correctly
REFUSES a non-matching task even when it's the only thing queued (the
core guarantee — proves it won't reproduce the model-diversity-in-flight
behavior strict mode exists to prevent); worker 0 stays unrestricted; the
flag defaults off and worker 1 behaves normally when it's False; and a
full end-to-end test with real concurrent threads (not seeded internals)
confirms a mixed same-model-burst + different-model-scan-call workload
completes cleanly under strict mode within a 10s timeout — proof against
a worker-1-idles-forever deadlock, the main risk this design introduces.

**Incidentally found**: the pre-strict-affinity full test suite run
turned up 5 test failures (`test_bbkc_squeeze_release.py`,
`test_bbkc_pre_breakout_composite.py`) not in tonight's established
14-failure baseline. Confirmed via `git stash` — same 5 fail identically
with ALL of tonight's changes reverted, so definitively pre-existing and
unrelated (likely date-sensitive), not a regression from this work.
Not investigated further — filed here as an observation, not a task.

Not currently enabled — this is the "ready if needed" patch, not a
change to today's shipped default. Deploy tomorrow only if the market-
hours scan-window judgment (above) triggers it.

---
## 🟢 HM-TELEGRAM-NTFY-UNIFY (filed 2026-07-07, from HM-ALERT-COLLAB-LINKS §7.4) — SHIPPED 2026-07-07, commit `5bed053`

**SHIPPED 2026-07-07, commit `5bed053`.** Executed per
`directives/hm_telegram_ntfy_unify_execution.md`. All 7 `_send_telegram`
call sites replaced with `_notify()` (routes through
`engine.alert_channels.send_alert`, ntfy-first). Zero telegram references
remain (`grep -n "telegram" engine/dynamic_alerts.py` — zero hits,
confirmed). 3 new tests (`tests/test_dynamic_alerts_notify.py`), 16/16
existing Phase 1 tests still green, full suite at the established
baseline (no new regressions). Restarted clean; verified via a direct
non-mocked live invocation against a real current SPY price (zero
exceptions) rather than waiting indefinitely for the natural fleet
cycle's infrequent after-hours cadence. Full detail below (original
filing, kept for the record).

`engine/dynamic_alerts.py` still sends via a legacy `_send_telegram` path
(silent-catch wrapper, `engine/telegram_alerts.py`) instead of the unified
severity-routed `alert_channels.py` (ntfy-first doctrine). Unify: route
`dynamic_alerts` emissions through `alert_channels.py`, retire the bespoke
telegram call. Deliberately kept OUT OF SCOPE of the alert-share-links work
to keep that diff small. Low priority; no user-visible breakage today.

**Verified before filing** (2026-07-07): `engine/dynamic_alerts.py:61-67`
—

```python
def _send_telegram(message: str):
    """Send alert via Telegram."""
    try:
        from engine.telegram_alerts import send_alert
        send_alert(message)
    except Exception:
        pass
```

Confirmed genuine silent-catch (bare `except Exception: pass`, zero
logging) — violates the no-silent-catch error handling posture. Confirmed
it bypasses `alert_channels.py` entirely (direct import of
`engine.telegram_alerts`, not routed through the severity/rate-limit/
multi-channel logic the rest of the alerting stack uses). 8 call sites in
`dynamic_alerts.py` (trendline breaks, RSI oversold/overbought, volume
spike, MACD crossover, plus others).

**Bonus, same pass**: fix the silent-catch violation while unifying —
route failures through the same `logger.warning` + telemetry pattern
`alert_channels.py`'s own send paths already use, rather than swallowing
silently.

---
## 🟡 HM-SILENT-CATCH-SWEEP (2026-07-07) — code-complete-and-held for next bundled restart

Systematic follow-up to the `_db_notification` `created_at` bug found earlier
tonight (HM-DYNALERTS-HYGIENE, commit `1a12286`): "the created_at find is the
third organ dead behind a bare except (ntfy delivery, notifications INSERT)."
Full sweep of `engine/`, `scripts/`, `dashboard/` for the same disease class.

### Inventory

- **930** total `except: pass`-shaped silent-catch sites (AST-scanned, not
  regex — catches multi-line `except Exception:\n    pass` the naive grep
  in tonight's earlier ad-hoc check missed).
- **92** candidate sites where the guarded try-block contains a WRITE
  operation (DB insert/update/delete, HTTP POST/PUT, file write) — the
  scope this ticket asked for.
- **8** of those 92 were false positives on manual triage (pure
  `SELECT`/read blocks the write-pattern regex over-matched on) — discarded.
  **84 genuine write-guarding sites**, triaged read-only via 5 parallel
  investigation passes (live DB row-count/freshness queries, file mtime
  checks, endpoint verification) before any code was touched.

### Classification (of the 84 genuine sites)

- **45 (a) PROVABLY WORKING TODAY** — live evidence (fresh matching rows,
  correct schema, fresh file mtimes) confirms the write succeeds regularly.
  No changes made — the directive scoped logging additions to (b)/(c) only.
- **33 (b) CAN'T TELL** — insufficient evidence either way (rare/conditional
  triggers, HTTP POSTs with no independent confirmation, features that may
  simply be unused rather than broken). **Logging added to all 33** —
  `logger.warning`/`console.log` with file+context, matching each file's
  existing convention. No behavior changes beyond observability.
- **6 (c) DEAD** — clear evidence the write has never succeeded or has
  stopped succeeding. See below.

### Dead write paths found (6)

1. **`engine/alert_channels.py::_db_notification`** — already fixed
   tonight, commit `1a12286`, before this sweep started (the seed case
   that prompted the sweep).
2. **`engine/paper_trader.py::_update_trade_alpaca_fields`** —
   **FIXED, this commit.** `UPDATE trades ... ORDER BY executed_at DESC
   LIMIT 1` is not valid SQLite syntax without the
   `SQLITE_ENABLE_UPDATE_DELETE_LIMIT` compile flag (absent from Python's
   bundled sqlite3) — has raised `OperationalError: near "ORDER": syntax
   error` on **every single call since the function was written**,
   silently swallowed. Confirmed empirically: zero `trades` rows anywhere
   have `alpaca_status='submitted'`. Affects SHORT trades and multi-leg
   spread trades specifically (their `execution_type` never updates from
   `'simulated'` to `'alpaca_paper'` after a real Alpaca order confirms).
   **This is the highest-stakes fix in this sweep** — it touches live
   order-tracking metadata, not just a log line. Fixed with the standard
   SQLite "target the most recent row via a rowid subquery" idiom, tested
   directly (`tests/test_update_trade_alpaca_fields.py`, 2 new tests
   proving the fix targets exactly the intended row and leaves others
   untouched), and the except block now logs loudly on any future failure
   instead of swallowing it.
3. **`engine/ollama_watchdog.py::_post_war_room`** — **FIXED, this
   commit.** POSTed to `/api/war-room`, which is GET-only
   (`dashboard/app.py:7404`) — every call has 405'd and been silently
   swallowed since written. Real POST route is `/api/war-room/post`
   (`:7618`). Fixed and **live-verified against the running server**
   (`curl -X POST .../api/war-room/post` → HTTP 200, `{"ok":true}` — a
   real, clearly-tagged `[Ollama Watchdog]` test row landed in `war_room`,
   left per never-delete-data doctrine). Every recycle-failure/circuit-
   breaker notification to the war room had been silently dropped until
   now.
4. **`engine/ghost_scoring.py::capture_new_signals`** — **NOT fixed,
   ticketed.** Its `except sqlite3.IntegrityError: pass` is itself
   *correct* (a dedup guard against re-capturing the same signal_id) —
   the real problem is the function has **no caller anywhere in the
   codebase** (no cron, no `main.py` wiring, CLI-only `__main__`). Its
   target DB (`ghost_trades.db`) has been frozen since 2026-04-29 despite
   the upstream source (`signal-center/signals.db`) actively producing
   qualifying signals through today. Deciding whether to wire this back up
   or formally retire it is a product call, not a bug fix — flagging for
   Admiral decision, not touched.
5–6. **`engine/holly_nightly_backtest.py` + `engine/holly_intraday.py`**
   — **NOT fixed, ticketed.** Both write to `data/backtest.db` tables
   that are frozen at exactly 2026-06-05, because `holly_nightly_cron.sh`
   (the shared driver for both) is **absent from the current crontab
   entirely** — not a code bug, an operational/scheduling gap. Restoring
   a month-dead nightly job is a scope/behavior decision, not a pure
   observability fix — flagging for Admiral go/no-go rather than silently
   re-adding a cron line. Logging added to both except blocks regardless
   (observability if/when the job runs again).

Also confirmed **not bugs** (dead by explicit design, already documented
elsewhere): `engine/gex_overlay.py`'s prune (scheduler intentionally
commented out, `HM-GEX-CANONICAL` 2026-05-31 migration) and
`engine/discovery_scanner.py`'s discoveries insert (scheduler retired in
`main.py`, "replaced by Volume Radar"). Logging still added to both in
case either is ever re-enabled without someone rediscovering this history.

### Architectural landmine flagged (same failure shape, not yet triggered)

`engine/vix_monitor.py`, `engine/momentum_tracker.py`, `engine/red_alert.py`,
and `engine/benchmark.py` all connect via a **bare relative path**
(`"autonomous_trader.db"`) that resolves to a *different* file at the repo
root — NOT the canonical `data/trader.db`. Works today only because these
processes happen to run with cwd = repo root. If invocation cwd ever
changes (different cron/launchd working directory, run from a
subdirectory), every write would silently start hitting a fresh, empty,
uncommitted DB file — same failure shape as the `created_at` bug, just not
triggered yet. `dashboard/app.py`'s trade-desk order provenance writeback
(site 2 in the fix list above) has the same relative-path pattern
independently. **Not fixed this pass** (changing a DB path touches more
than logging) — filed here for a dedicated hygiene pass.

### Other observation (not a silent-catch bug, different flavor)

`dashboard/app.py`'s `trade_explanations` table initializes correctly and
has the right schema, but **nothing in the codebase ever inserts into
it** — only a `CREATE TABLE` and one `SELECT ... WHERE trade_id=?`. The
"explain a trade" feature appears to read from a table nothing writes to.
Not touched (deciding whether to wire up a writer or retire the read path
is a feature decision), noted for whoever owns trade explanations.

### Bonus fix (incidental, found while adding logging)

`engine/ollie_commander.py` had a **pre-existing dead reference**:
`logger.warning(...)` at line 300 (Scout→Critic pipeline error path) with
no `logger` ever defined anywhere in the file — would have raised
`NameError` on any actual scout/critic pipeline failure, converting a
warning-log call into an unhandled crash. Fixed as a side effect of adding
`import logging` / `logger = logging.getLogger(__name__)` for this
sweep's own logging additions to the same file.

### Verification

- `py_compile` clean across all 28 touched files.
- Full suite: 646 passed (+2 from the new `_update_trade_alpaca_fields`
  regression test), 19 failed — identical to tonight's established
  baseline, no new regressions.
- `_update_trade_alpaca_fields` fix directly tested
  (`tests/test_update_trade_alpaca_fields.py`): confirms the UPDATE
  targets exactly the most recent matching trade, leaves older/other-
  symbol trades untouched, and a no-match case is a legitimate silent
  no-op (not an error).
- `_post_war_room` fix live-verified against the running server (HTTP 200
  confirmed, real row landed).

### Disposition

**Code-complete-and-held** for the 27 resident `engine/`/`dashboard/`
files (all imported into the live `main.py` process — need a restart to
take effect, per HM-CONSOLE-INIT doctrine). `scripts/witness_ab_scorer.py`
is the one non-resident file touched (standalone cron script) — its fix
takes effect automatically on the next cron invocation, no restart needed.

Ride the next bundled after-close restart window. The `paper_trader.py`
fix (#2 above) is the one item worth flagging for expedited treatment —
it's the only fix in this batch that changes live trade-metadata
correctness rather than pure observability.

---
## 🟡 HM-SCAN-LIVENESS-WATCHDOG (2026-07-08) — SHIPPED (commits `471d656`, `153c954`, restarted 10:26 MST), two follow-ups open

Context: Phase 1 alert-defs dogfood (5 real `alert_definitions` created via
`/api/alert-defs`) needed live confirmation that `run_user_alert_definitions`
evaluates cleanly. That investigation surfaced a 09:16→09:55 MST gap with no
log evidence either way — root-caused to `run_scanner()`'s global cooldown
gate (`_last_scan_time`) resetting on every scheduler tick that clears the
interval check, even when zero tiers are due (`main.py:449-451`, was fully
silent). Shipped same-session, ahead of the 11:00 ET FOMC minutes:
- `_last_scan_complete_ts` — stamped only on real scan completion.
- `check_scan_liveness()` — every 2 min, warning-severity `alert_channels.send_alert`
  if age > 2×T1 interval (60 min), edge-triggered, auto-clears.
- One log line on the previously-silent no-op path so "not due" and
  "stalled" read differently in `trader.log` going forward.

**Open item 1 — watchdog itself only verified by isolated logic check**
(threshold math sanity-tested standalone: 10min age → no fire, 65min → fires),
never live-fire tested against the running process — restarting mid-test to
force a real 60-min stall wasn't worth the risk under the deadline. Needs
either: (a) an after-hours window where the alert threshold is temporarily
dropped to ~3 min to observe a real fire+clear cycle, or (b) a proper unit
test on `check_scan_liveness()` with a mocked `_last_scan_complete_ts` /
stubbed `send_alert`. Do this before trusting it silently through a real
incident.

**Open item 2 — RESCOPED 2026-07-08 ~10:59 MST.** Root cause of "why 68
minutes" found: a live in-flight cycle (started ~10:30 MST, still running as
of this edit) showed the scan_lock heartbeat's phase marker moving
symbol-by-symbol through sequential Ollama inference for `ollama-plutus`
(MU → META → NVDA → AVGO → XLE → ORCL → NOW → CNTA...), each symbol taking
anywhere from tens of seconds to ~2.5 min. **Per-symbol LLM inference time is
unbounded and scan duration is just the sum of however many symbols are in
that cycle's universe** — the 04:06 4090.9s stall fits this exactly; it
wasn't a deadlock, it was the same mechanism running long on a bad day
(slow model, large universe, or both). No separate wedge/deadlock bug to
chase.

Rescoped from "lock hold-timeout" to three parts, in priority order:

**(a) PRIORITY — decouple `run_user_alert_definitions` from the LLM scan
into a fast lane on every 2-min scheduler tick.** RSI/volume/price-level/
trendline checks need only plain market data, zero LLM inference — there is
no reason they should be hostage to however long `ollama-plutus` takes to
grind through its symbol list. Concretely: today's dogfood (`alert_definitions`
IDs 1-5) sat blind through the entire 10:30→11:00+ MST FOMC-minutes reaction
window because their only evaluation path is inside the same `_arena_scan_thread`
that the slow LLM scan monopolizes via `_scan_lock`. That coupling is the
actual bug — fix it by evaluating user alert-defs (and probably the other
`check_*` dynamic-alert functions, all pure-data too) on their own fast,
lock-independent cadence, separate from `arena.run_scan()`.

**(b) Per-symbol inference timebox + skip-and-log.** Cap how long any single
symbol's inference phase can run inside a scan; if it blows the budget, skip
that symbol (log which one, and why), don't let it hold the whole cycle
hostage. This is what actually bounds worst-case scan duration — a lock
timeout alone would just abort the whole cycle's work, losing every symbol
already processed.

**(c) Keep a `_scan_lock` hold-timeout, but demoted to backstop only** — a
safety net for a genuine future deadlock (not the expected case anymore),
not the primary fix.

Evidence to attach once the in-flight cycle (started ~10:30 MST) completes:
total cycle duration, and confirmation that alert-defs 1-5 evaluated cleanly
once it finally does.

**Organic live-fire watch armed 11:05 MST** — this same cycle running past
60 min is backlog item #1's (watchdog verification) real-world test, for
free. `_last_scan_complete_ts` was seeded at process import (~10:26:00 MST,
this morning's second restart) and hasn't moved since — no scan has
completed. Threshold trips at seed-time + 3600s ≈ **11:26 MST** (a few min
before the ~11:30 estimate based on scan-start time, since the watchdog
clocks from last-completion not last-start). Watching for: (1) `[HM-SCAN-
LIVENESS] ALERT` log line + a real `alert_channels.send_alert` call with no
`send_alert failed` in trader_error.log: fires clean → **close item #1 as
production-verified**. (2) Silent past 11:32 MST → treat as a watchdog bug,
investigate before EOD. (3) Once the cycle finally completes → confirm
`[HM-SCAN-LIVENESS] recovered` auto-clear fires. Outcome to be logged here
when observed.

**OUTCOME (11:21:14 MST) — cycle completed at 3078.1s (51.3 min), UNDER
the 3600s/60min alert threshold.** `[HM-AS-β-C] scan_lock held 3078.1s
(scan-only)`. The organic live-fire test did **not** occur — the cycle
self-resolved before crossing the alarm line, so item #1 (watchdog
verification) is still open, unverified by a real fire event. No
`[HM-SCAN-LIVENESS] ALERT` line appeared (correctly — it shouldn't have,
math checks out: 3078s < 3600s) and no `send_alert failed` errors either.
Immediately after, a second scan ran and completed in 13.8s — confirming
the system returns to normal cadence once the long cycle clears.

**Item #2 evidence, confirmed:** 51.3 min for this cycle (vs. 68 min for
the 04:06 incident) is a second independent data point for the same
unbounded-per-symbol-inference diagnosis — not a one-off. Reinforces (a)/(b)/(c)
priority order above.

**Alert-defs 1-5 evaluated cleanly on this cycle:** `Dynamic alerts: 8
triggered` logged 11:20:53 with zero errors (built-in checks fired
normally — `dyn_macd_crossover_NVDA`, `dyn_rsi_oversold_ORCL`,
`dyn_volume_spike_APLS`, `dyn_rsi_overbought_CNTA`, etc. all dispatched
clean via alert_channels). All 5 user defs (IDs 1-5) remained
`enabled=1, last_triggered_at=null` post-cycle — correct, since none of
their specific conditions (SPY RSI≤30, NVDA RSI≥70, TSLA vol≥3x, BABA≥110,
QQQ trendline break) were actually met this pass. No `alert_def`-specific
errors anywhere in `trader_error.log`.

**Item #1 still needs its live-fire test** — next opportunity is whenever
a future cycle organically exceeds 60 min, or force it via the two options
already listed above (temp-lower threshold in an after-hours window, or a
mocked-timestamp unit test). Not closed.

## HM-STOP-COVERAGE-GAP-2026-07-09 — manual emergency flatten + coverage gap found

**Manual intervention log (2026-07-09 ~08:27-08:29 MST, Captain-approved):**
Investigating a Scotty-handoff report of "COST position -15.8% unrealized,
no stop fired" surfaced that the original attribution (McCoy) was wrong —
live-verified McCoy's (`ollama-plutus`) actual COST position was -7.98%,
not yet breaching its -8% floor. A full sweep of all 42 open stock
positions against the -8% hard-stop threshold (live Alpaca prices) found
the real breaching position was `alpaca-mirror` — a `crew_role='mirror'`
bookkeeping row that reflects the literal real Alpaca paper broker
account's own COST holding (qty=0.08, unrealized -16.17%, market value
$72.76 — matches the handoff's "$73 dust" exactly), plus a second breach:
`ollie-auto` / NUKZ at -8.83%.

Root cause: `engine/crew_scanner.py::_check_hard_stops()` only iterates
`ACTIVE_SCANNERS + RULES_SCANNERS + ALPHA_SQUAD` (10 named players) — it
never looks at any other player's open stock positions. 4 of the 7 players
currently holding open stock positions (`alpaca-mirror`, `ollie-auto`,
`guardian-of-forever`, `qwen3-8b-flash`) are outside that set and have
**zero stop-loss coverage**. This is the real defect behind the "why did
the stop not fire" question — not a threshold/tiering bug, a membership
gap in the safety-net function itself.

**Actions taken (Captain-approved, before root-cause fix shipped):**
- `ollie-auto` NUKZ: closed via `engine.paper_trader.sell()` (the normal
  gated path), qty=0.348 @ $65.50, realized PnL -$2.21. Internal ledger
  updated; Alpaca-side leg was already flat (0 qty on the real account —
  pre-existing internal/broker ledger drift for this player, not caused by
  this close).
- `alpaca-mirror` COST: `engine.paper_trader.sell()` correctly refused
  (`_is_human_player` guard — this row is a broker-state mirror, not an
  AI-tradeable book). Closed instead via `AlpacaBridge.close_position('COST')`
  directly against the real Alpaca **paper** account (RULE #1 Schwab-hands-off
  N/A — this is Alpaca paper only). Verified: account position count 14→13,
  COST absent from `client.get_all_positions()` post-close.
- Re-swept all 41 remaining open stock positions post-close: zero remaining
  violators of the -8% threshold.

**Not yet done:** fix `_check_hard_stops()`'s player coverage (tracked as
the P0 "position-monitoring loop" item in the same session) and confirm
whether `alpaca-mirror`'s internal `positions` table row self-corrects via
its normal broker-sync job or needs a manual row cleanup.

## HM-MCCOY-OLLAMA-CONTEXT-INVESTIGATION-2026-07-09 — follow-up, not yet started

**Open question:** what caused McCoy's (`ollama-plutus`) local model to
collapse to emitting exactly `confidence=0.85` on 100% of BUY_CALL signals
starting 2026-07-08 (73/76 that day, 45/45 on 07-09), after previously
varying naturally (0.8/0.82/0.87/0.89/0.9/0.95)? Timing coincides with an
`engine/providers/ollama_provider.py` context/concurrency change dated
2026-07-07 (`num_ctx=10240`, 2 concurrent workers) — plausible but
**unconfirmed** root cause, flagged during the same-day investigation that
also shipped `HM-DEGENERATE-CONFIDENCE-2026-07-09` (the detection mechanism
for this pattern going forward, in `engine/crew_scanner.py`).

**Scope for the follow-up:**
- Diff the exact `ollama_provider.py` change from 2026-07-07 — what
  changed about context window size, concurrency, or request batching.
- Check whether other Ollama-backed agents sharing the same model/pair
  (Dax/`ollama-qwen3`, since McCoy runs paired with Dax per
  `SCAN_PAIRS`) show any correlated confidence-variance change, or if
  this is isolated to McCoy's specific model (plutus).
- Determine whether `num_ctx=10240` or the 2-worker concurrency setting
  is more likely to cause a model to degenerate toward a single stable
  output token sequence (context truncation cutting off the reasoning
  that normally varies confidence, vs. two concurrent requests
  interleaving/corrupting KV-cache state are different failure
  mechanisms with different fixes).
- Not urgent — `is_confidence_reliable('ollama-plutus')` now surfaces
  the symptom automatically; this ticket is about the underlying cause.

## HM-CF-502-BLIP-2026-07-09 — investigated, no origin-side cause found

**Reported:** Cloudflare returned 502 error pages for every `/api/*` fetch
for ~2 minutes starting ~16:43 MST (23:43 UTC — the audit's "16:43 ET" is
the known TZ-display-mislabel, not a real ET timestamp; see
`feedback_trader_process_clock_skew_utc` doctrine). Self-recovered.

**Checked and ruled out:**
- **`trader.log`** shows fully continuous `[ENDPOINT-DUR]` traffic through
  16:40-16:46 MST with no gap and no abnormal `wall=` time — the origin
  process (`main.py`, pid 10120 at the time) was never unresponsive or
  blocked in that window.
- **`trader_error.log` / `hm_ops_sentinel.py`'s own byte-offset-tracked
  lock check** show zero `database is locked` occurrences since 10:33 MST
  today — this morning's lock storm (06:50-10:33 MST, confirmed real: many
  `sqlite3.OperationalError: database is locked` across `paper_trader.py`,
  `war_room.py`, `main.py` Autopilot, and LRS rating writes) had already
  ended almost 6 hours before tonight's blip and has **not recurred**.
  **Tonight's 502 is a different incident from this morning's lock storm,
  not a repeat of it.**
- **`logs/cloudflared-daemon.log`** (the tunnel's own connection log — quic
  dial failures, stream errors, reconnects) has no entries at all in the
  16:43-16:45 MST window; the nearest logged reconnect activity was
  16:51-16:57 MST, 46-52 minutes earlier, and nothing since. The tunnel's
  own edge connection was stable through the reported blip.

**Conclusion:** no origin-side (app process, SQLite, or our own tunnel
client) cause correlates with the timing. Most likely a transient
Cloudflare-edge-side routing blip external to our infrastructure, which
self-recovered as reported. Nothing actionable found — flagging as
investigated-and-closed rather than leaving it open with no lead.

**Sentinel conditions checked in the same pass:**
- **(a) `rikers_log` heartbeat staleness (153→198 min stale):** RESOLVED.
  Root cause was the `ModuleNotFoundError: No module named 'engine'`
  crash-loop in `riker_synthesis.py`'s cron invocation (missing repo-root
  `sys.path` insertion), fixed in commit `224292b` (2026-07-09 14:55 MST —
  landed before tonight's audit). Verified live: `rikers_log` has persisted
  every 10 minutes with zero gaps since ~15:00 MST (latest row
  `2026-07-10 00:30:00`, 3 min old at check time); `hm_ops_sentinel.py`'s
  own heartbeat `age_min` reads 0-10 continuously, well under the 25-min
  RED_ALERT threshold. No further action needed.
- **(b) `signals_v2` FIFO starvation (oldest-pending ~360h):** still open,
  confirmed live (`hm_ops_sentinel_cron.log` shows `oldest_age_hours`
  climbing 362.8→364.9 tonight, WARNING firing every cycle, rate-limited to
  ~1 ntfy push/30min — the sentinel is working correctly, the underlying
  condition is real). **Recommendation #1 from `HM-SIGNALS-V2-FIFO-STARVATION`
  above (expire pending rows from halted/non-active sources) was already
  applied** via the one-time `scripts/hm_signals_v2_expire_halted_backlog.py`
  cleanup — pending dropped from 15,061 to 739, `navigator`'s 13,063 dead
  rows are gone. **Recommendation #2 (priority-lane / age-ordering for the
  remaining active-source rows) is still not built** — the oldest pending
  row is still the same `2026-06-24 19:42:20` signal, now blocking strict
  FIFO ahead of every fresh active-source signal. Confirmed the consumer
  itself (`run_events_bus_consumer`, `main.py:3441`) is healthy, not
  stalled — it drained normally all day and stopped exactly at 16:00 ET
  market close (correct by-design `is_us_market_open()` gating, 1-min
  cadence), so the queue will resume strict-FIFO draining tomorrow at open,
  still behind the same 15-day-old row. **Deferring, not applying #2** —
  this is a live-trading dequeue-order policy change (FIFO-age-cap vs.
  newest-first vs. hybrid-drain are genuinely different behaviors, not a
  single obviously-correct bug fix) and the backlog entry above already
  explicitly gates it on Admiral sign-off. If asked to pick one: option
  (a) — reorder by `created_at DESC` within active sources only — matches
  the swing-surge intent with the least behavioral surprise for the other
  ~1,285 legacy active-source rows already in queue.

**CORRECTION (2026-07-09, same evening, HM-BUG-BATCH cleanup pass):** the
"Recommendation #2 is still not built" line above is **wrong** — verified by
reading `engine/events_bus_consumer.py::consume_pending_signals()` directly
instead of trusting the ops-sentinel's oldest-age metric as a proxy. The
query has been `ORDER BY created_at DESC` (newest-first) since **commit
`aa55f1d`, 2026-07-06 15:29:58, "Admiral Steve"** — the exact same commit
that shipped recommendation #1's halted-source cleanup. Both recommendations
from the original filing were applied together that day; my earlier
same-night entry above re-diagnosed this as still-open without checking the
actual consumer code. Apologies for the noise — corrected now, not silently
edited above, so the trail of what was believed when stays intact.

**What the ops-sentinel WARNING is actually catching, now correctly
understood:** newest-first ordering means a handful of very-old
active-source rows (the `2026-06-24 19:42:20` one, and however many others
predate the 07-06 fix) structurally can **never** win priority against
same-day signals — they're not stuck in a slow FIFO, they're just
permanently outranked. That's the intended tradeoff of newest-first, not a
bug. But it does mean `oldest_age_hours` will climb forever and the sentinel
will warn forever on rows that will genuinely never dequeue, which is noise
once you understand the mechanism. That's a much smaller, lower-risk
residual problem than "the priority fix isn't built" — it's "a few dozen
ancient rows from before 07-06 should be archived (same archive-not-delete
`hm_signals_v2_expire_halted_backlog.py`-style pattern, extended to
active-source rows older than some age cutoff like 7 days, not just halted
sources) so the sentinel stops alerting on rows that structurally can't
execute under the ordering that's already live." Not applied tonight —
flagging as a small, separate, low-risk follow-up rather than assuming scope
that wasn't explicitly asked for.

## 🔵 HM-CRONTAB-EINTR — filed 2026-07-12, not urgent, diagnosis deferred

`crontab -e`/`crontab -l`-style installs are failing machine-wide on bigmac
with `crontab: tmp/tmp.49301: Interrupted system call` — reproduced twice by
XO from two different sessions (once local, once a fresh SSH session from
another machine), same error both times, so it isn't a one-off signal/session
fluke. Root cause not yet investigated (candidates: `EDITOR`/`VISUAL`
misconfigured to something that mishandles signals, a stale/locked
`/usr/lib/cron/tabs` tmp file, or a `vipw`-style file-locking race — none
confirmed).

**Impact today:** blocked the planned cron installation of the
`HM-SIGNALS-V2-STARVATION-RECURRENCE` Monday-morning check
(`scripts/hm_signals_v2_monday_check.py`). Worked around by installing it as
a one-shot `launchd` LaunchAgent instead
(`~/Library/LaunchAgents/com.ollietrades.hm-signals-v2-monday-check.plist`,
`StartCalendarInterval` pinned to Year/Month/Day/Hour/Minute = 2026/7/13/7/0
MST so it fires exactly once and never recurs) — bootstrapped into `gui/501`
and confirmed via `launchctl print` (event trigger registered, `runs = 0`,
armed for the target timestamp). The script's own `_remove_own_cron()`
self-cleanup no-ops harmlessly if crontab is still broken when it fires
(catches the non-zero `crontab -l` exit and just skips, per its existing
error handling — verified by reading the function, not assumed).

**Not urgent** because the one job that needed scheduling today is covered
by the launchd fallback. Still worth root-causing before the next time
something needs a recurring (not one-shot) cron entry, since launchd
one-shots don't generalize to that case, and per
`docs/CLAUDE.md`'s "LaunchAgent Reboot Lifecycle" note, `launchd` has its
own known gaps (SSH-run `launchctl bootstrap` domain errors, RunAtLoad not
firing without a logged-in Aqua session) that make it an imperfect
universal substitute for cron, not just a drop-in replacement.

**Next step (not done):** reproduce with `crontab -l` alone (isolate whether
it's specific to `-e`/write-with-tmpfile or hits read-only listing too),
check `echo $EDITOR $VISUAL`, and check for a stale lockfile under
`/usr/lib/cron/tabs/` or `/var/at/tabs/`.


---
## Counterfactual Report — 2026-07-12 (P1 measurement layer, scripts/counterfactual_report.py)

# Counterfactual Report — 2026-07-12
Window: last 30 days (2026-06-12 to 2026-07-12)
Outlier guard: entry price >= $1.00, |forward return| <= 100% (see script docstring for why)

Executed baseline (BUY signals that went live): n=35/29/25 (1d/3d/5d with bars available)
  fwd_1d: mean=-0.965%, median=0.045%
  fwd_3d: mean=-0.943%, median=-0.234%
  fwd_5d: mean=-1.244%, median=-0.720%

| Gate | Blocked (deduped) | Bars avail | mean/median fwd_1d | mean/median fwd_3d | mean/median fwd_5d | Structural? |
|---|---|---|---|---|---|---|
| HALT | 4376 | 4103 | -0.05/0.14% | 0.23/0.42% | 0.73/0.74% | yes |
| MARKET_CLOSED | 1948 | 1652 | 0.90/0.17% | 1.19/0.46% | 1.56/0.77% | yes |
| GRADE_B | 1024 | 1008 | -0.78/-0.93% | -0.11/-0.66% | -0.53/-1.17% |  |
| BENCH | 256 | 201 | -0.25/0.04% | n/a | n/a |  |
| MAX_TRADES_REACHED | 245 | 232 | 0.02/0.02% | -0.50/0.53% | -0.35/0.44% |  |
| MAX_POSITIONS_REACHED | 156 | 143 | -0.59/-0.35% | 0.42/0.55% | 0.64/0.07% |  |
| LEARNING_BLOCK | 71 | 43 | 1.26/0.69% | n/a | n/a |  |
| LOW_CONVICTION | 33 | 29 | -0.12/-0.16% | 0.66/0.32% | 2.56/2.12% |  |
| SCANNER_FILTER | 18 | 18 | -1.22/-0.30% | 0.10/0.11% | 0.17/-0.04% |  |
| QUALITY_GATE_FAILED | 6 | 3 | -1.14/-1.39% | -0.10/-0.73% | -0.10/-0.73% |  |
| DRAWDOWN_PAUSE | 5 | 4 | -4.72/-4.70% | -6.71/-6.71% | n/a |  |

---
## 🔵 HM-UNEXPLAINED-ARTIFACTS — filed 2026-07-12 (XO EOD status check), low priority, investigate-later, document-only

Three loose ends surfaced during the 2026-07-12 EOD systems check, none
blocking, none touched — captured here so they don't get re-discovered cold.

**1. `bot-code.tar.gz` (repo root, 15MB, dated 2026-07-03) — EXPLAINED, no
further action needed.** `tar -tzf` (read-only) shows a full source snapshot
(`engine/`, `scripts/`, `docs/`, `lib/`, `reports/`, `strategies/`,
`swingdesk/`, `tests/`, `training_data/`, including some stale `.bak_*`
files). `drafts/OLLIETRADES-MEMORY.md` documents it: pulled 2026-07-03 by
Bonnie onto her Windows machine (`C:\Users\Bonnie\Documents\bigmac-bot\`) to
work on the code in Cowork. So it's a known, intentional export artifact,
not a mystery — just left untracked in the repo root since. No cleanup
action taken (not asked for); mentioning in case it should eventually be
`.gitignore`'d or moved out of the repo root.

**2. `db_snapshot.sh`'s 2026-07-12 run fired at 10:47am MST instead of its
normal 20:15 MST cron slot** (the 20:15 run that evening correctly `[SKIP]`'d
as a same-day duplicate once it saw the 10:47 snapshot already existed, so no
data was lost or double-counted). Checked `crontab -l` (no 10:4x AZ entry
exists), `backup_freshness_check.sh`/`disk_space_alert.sh`/`offhost_backup.sh`
(none of them invoke `db_snapshot.sh`), and `.zsh_history` (nothing recorded
for that window) — no trigger source identified quickly, so per XO
instruction this is documented rather than chased further. Likely an ad hoc
manual/interactive invocation (e.g. a Claude Code session verifying
something) rather than a scheduling bug, but genuinely unconfirmed.

**3. Intermittent `ntfy failed: <urlopen error timed out>` window,
~21:43–22:36 MST on 2026-07-12** — recur-watch only, not treated as an
incident. Failures were interleaved with successful `ntfy sent [200]` lines
in the same window (including one right at the end, 22:36:33), and a live
`curl` to `ntfy.sh` from this box during the EOD check also timed out once
(8s) before succeeding on retry — consistent with transient network
flakiness rather than the box-wide IPv6-routing class of bug already fixed
under `HM-NTFY-IPV6-NOROUTE-SWEEP`. No code change proposed. If this
recurs on a subsequent night, escalate to an actual investigation; a single
one-hour blip is not enough signal to act on.

**Disposition:** item 1 is closed (explained). Items 2 and 3 stay open as
recur-watch only — re-check if either happens again before spending more
time on them.

## HM-SEASON-ROTATION-CASH-RESET-2026-07-18 — accepted as-is, backlog note only

The 2026-07-12 season-rotation bug (`HM-SEASON-ROTATION-BLANKET-REACTIVATE`,
see relay reports `relay_2026-07-18_season-rotation-halt-reset-correction.md`
and `relay_2026-07-18_season-rotation-blanket-reactivate-fix.md`) reset all
65 wrongly-reactivated agents' `cash` column to the $7000 season default.
When `halt_mode`/`halt_reason` were restored to their pre-rotation truth,
cash was deliberately **not** restored — the Admiral accepted the $7000
values as-is (2026-07-18), since the correct pre-rotation figures aren't
simply "whatever it was on 07-12" if any legitimate trading occurred for
these agents between their original halt date and 07-12. No further action
planned; noting here only so it isn't re-discovered as a fresh discrepancy.

**Related, closed same day:** `ollie-auto`'s two Alpaca-side orphaned
positions from the same rotation bug (`BLK` qty 0.15, `PM` qty 0.14 — its
internal `positions` rows were deleted by the rotation's blanket
`DELETE FROM positions`, while the real broker-side Alpaca positions
survived) were re-seeded into `ai_players`/`positions` from live Alpaca
truth on 2026-07-18, restoring ollie-auto's own exit/stop-loss logic's
visibility into them. Verified byte-for-byte match against a fresh
`get_alpaca_positions()` call post-write. See
`relay_2026-07-18_season-rotation-blanket-reactivate-fix.md` §6 for the
original finding and the follow-up relay for the actual reconciliation.

## HM-UHURA-NONBLOCK-UNCOMMITTED-2026-07-18 — needs a decision Monday, not touched

Found while inventorying pending repo changes over the weekend:
`dashboard/app.py`'s `uhura_signal()` has an **uncommitted** working-tree-only
change, tagged `HM-UHURA-NONBLOCK 2026-07-18` in its own docstring, author
unknown (not attributed to any session in this conversation). Diffstat:
+21/-1, one function.

**What it does:** converts `uhura_signal()` from a blocking
`@timed_cache(120)`-wrapped call into a non-blocking wrapper — returns the
last cached result (or a `"warming"` placeholder on true cold start)
instantly, and kicks off a background daemon thread to refresh via a new
`_uhura_signal_compute()` (which keeps the original `@timed_cache(120)`)
when the cache is stale and no refresh is already in flight. Stated purpose:
fixes a >10s cold-call hang on `/classic` and `archer`'s `intel_sources`
callers.

**Known issue, not fixed:** the staleness/`_busy` check uses plain
`getattr`/attribute-on-function state, not a lock — two near-simultaneous
stale requests can both read `_busy=False` before either sets it `True`,
spawning two concurrent refresh threads. Likely harmless (both compute the
same thing, last write wins) but not verified, and not the right pattern to
reuse elsewhere if a refresh ever gets a side effect.

**Likely already live:** `main.py` imports `dashboard.app` directly, and the
trader was restarted 2026-07-18 16:12:18 MST (for the unrelated
`HM-SEASON-ROTATION-BLANKET-REACTIVATE` fix) — restarts load whatever's on
disk regardless of commit state, so this fix is probably already running in
production despite never having been committed or reviewed.

**Action needed Monday:** either (a) test it properly (specifically probe
the double-refresh race under concurrent load), fix the race with a real
lock, and commit; or (b) deliberately revert it if it's not wanted. Left
untouched per instruction — do not silently commit or revert without a
decision.

---
## 🔵 HM-WITNESS-AB-RETIRE-2026-07-18 — `scripts/witness_ab_scorer.py` RETIRED, do not re-schedule

**Directive (DECOM-PHASE-2, 2026-07-18):** Ollie Max is being decommissioned.
`witness_ab_scorer.py` scores queued war-room debates against McCoy
(plutus-v1) using models hosted on Ollie Max (`.168`) — deepseek-r1:14b and
gpt-oss:20b. With the host going away, the script has no home to run on.

**Status at retirement:** never had a cron/launchd entry — always run
manually/ad hoc. Last write to `witness_ab` table: 2026-07-10 (8 days stale
as of this entry). Lifetime tally: deepseek-r1:14b 1,196 scored / 64.8%
agreement with McCoy; gpt-oss:20b 452 scored / 47.3% agreement.

**Disposition:** code kept in place (`scripts/witness_ab_scorer.py`), NOT
deleted, per Archive Convention. `witness_ab` table and its data preserved.
**Do not re-add to cron/launchd.** Rehab path, if ever revisited: needs a
non-Ollie-Max inference target (or its own box) before it can run again —
not a same-day fix.


---
## Counterfactual Report — 2026-07-19 (P1 measurement layer, scripts/counterfactual_report.py)

# Counterfactual Report — 2026-07-19
Window: last 30 days (2026-06-19 to 2026-07-19)
Outlier guard: entry price >= $1.00, |forward return| <= 100% (see script docstring for why)

Executed baseline (BUY signals that went live): n=33/31/23 (1d/3d/5d with bars available)
  fwd_1d: mean=-1.035%, median=0.045%
  fwd_3d: mean=-1.774%, median=-0.094%
  fwd_5d: mean=-1.584%, median=0.200%

| Gate | Blocked (deduped) | Bars avail | mean/median fwd_1d | mean/median fwd_3d | mean/median fwd_5d | Structural? |
|---|---|---|---|---|---|---|
| HALT | 4835 | 4796 | -0.05/0.15% | 0.15/0.44% | 0.47/0.56% | yes |
| MARKET_CLOSED | 3010 | 2166 | 0.69/0.26% | 1.15/0.52% | 1.59/0.59% | yes |
| BENCH | 1941 | 1392 | -0.38/-0.08% | -1.33/-0.26% | -0.81/-0.17% |  |
| MAX_TRADES_REACHED | 245 | 236 | 0.03/0.02% | -0.42/0.54% | -0.34/0.32% |  |
| MAX_POSITIONS_REACHED | 156 | 149 | -0.51/-0.33% | 0.10/0.34% | 0.51/0.23% |  |
| MANDATE_BLOCKED | 119 | 77 | 3.31/-0.21% | 2.56/-0.74% | n/a |  |
| LEARNING_BLOCK | 71 | 65 | 0.12/0.35% | -1.33/-0.54% | -0.64/0.24% |  |
| GRADE_B | 34 | 32 | -0.17/0.36% | 0.48/0.92% | 0.23/1.53% |  |
| LOW_CONVICTION | 33 | 29 | -0.12/-0.16% | 0.11/0.11% | 1.12/0.50% |  |
| SCANNER_FILTER | 19 | 19 | -1.55/-0.40% | -0.72/-0.19% | -0.60/-0.23% |  |
| QUALITY_GATE_FAILED | 6 | 4 | 0.47/0.23% | 2.03/1.11% | 2.03/1.11% |  |
| DRAWDOWN_PAUSE | 5 | 5 | -3.56/0.39% | -2.04/1.07% | -5.00/0.26% |  |


---
## Counterfactual Report — 2026-07-26 (P1 measurement layer, scripts/counterfactual_report.py)

# Counterfactual Report — 2026-07-26
Window: last 30 days (2026-06-26 to 2026-07-26)
Outlier guard: entry price >= $1.00, |forward return| <= 100% (see script docstring for why)

Executed baseline (BUY signals that went live): n=32/32/30 (1d/3d/5d with bars available)
  fwd_1d: mean=-0.739%, median=0.074%
  fwd_3d: mean=-1.048%, median=-0.072%
  fwd_5d: mean=-1.582%, median=-0.077%

| Gate | Blocked (deduped) | Bars avail | mean/median fwd_1d | mean/median fwd_3d | mean/median fwd_5d | Structural? |
|---|---|---|---|---|---|---|
| BENCH | 3752 | 3411 | -0.16/-0.13% | -0.73/-0.47% | -0.81/-0.53% |  |
| HALT | 3417 | 3393 | 0.17/0.25% | -0.15/0.34% | -0.41/0.24% | yes |
| MARKET_CLOSED | 2762 | 1981 | 0.79/0.00% | 0.60/-0.03% | 0.59/-0.10% | yes |
| MAX_TRADES_REACHED | 245 | 236 | 0.03/0.02% | -0.42/0.54% | -0.34/0.32% |  |
| MANDATE_BLOCKED | 119 | 84 | 3.33/-0.24% | 1.80/-0.94% | 1.59/-1.19% |  |
| LEARNING_BLOCK | 71 | 65 | 0.12/0.35% | -1.33/-0.54% | -0.64/0.24% |  |
| MAX_POSITIONS_REACHED | 38 | 33 | 0.49/0.10% | -1.10/-0.10% | -0.28/0.27% |  |
| LOW_CONVICTION | 22 | 18 | -0.22/-0.17% | -0.98/0.01% | -0.77/-0.01% |  |
| GRADE_B | 19 | 17 | 2.34/2.74% | 0.94/0.92% | 0.94/2.22% |  |
| SCANNER_FILTER | 19 | 19 | -1.55/-0.40% | -0.72/-0.19% | -1.13/-0.27% |  |
| QUALITY_GATE_FAILED | 6 | 4 | 0.47/0.23% | 2.03/1.11% | 2.03/1.11% |  |
| DRAWDOWN_PAUSE | 5 | 5 | -3.56/0.39% | -2.04/1.07% | -5.00/0.26% |  |


---
## Counterfactual Report — 2026-08-02 (P1 measurement layer, scripts/counterfactual_report.py)

# Counterfactual Report — 2026-08-02
Window: last 30 days (2026-07-03 to 2026-08-02)
Outlier guard: entry price >= $1.00, |forward return| <= 100% (see script docstring for why)

Executed baseline (BUY signals that went live): n=23/23/23 (1d/3d/5d with bars available)
  fwd_1d: mean=-0.551%, median=0.122%
  fwd_3d: mean=-0.984%, median=-0.094%
  fwd_5d: mean=-1.751%, median=-0.265%

| Gate | Blocked (deduped) | Bars avail | mean/median fwd_1d | mean/median fwd_3d | mean/median fwd_5d | Structural? |
|---|---|---|---|---|---|---|
| BENCH | 3752 | 3411 | -0.16/-0.13% | -0.76/-0.39% | -0.63/-0.26% |  |
| MARKET_CLOSED | 2344 | 1570 | 0.53/-0.18% | 0.16/-0.33% | 0.32/-0.46% | yes |
| HALT | 1541 | 1533 | -0.03/0.07% | -0.31/0.08% | -0.57/0.08% | yes |
| MANDATE_BLOCKED | 119 | 84 | 3.33/-0.24% | 1.80/-0.94% | 1.59/-1.19% |  |
| LEARNING_BLOCK | 71 | 65 | 0.12/0.35% | -1.33/-0.54% | -0.64/0.24% |  |
| MAX_POSITIONS_REACHED | 32 | 27 | 0.67/0.10% | -1.48/-0.69% | -0.49/0.28% |  |
| MAX_TRADES_REACHED | 22 | 20 | 0.98/0.98% | 0.14/0.14% | -0.59/0.19% |  |
| GRADE_B | 16 | 15 | 2.60/2.99% | 0.68/0.92% | 0.57/0.92% |  |
| LOW_CONVICTION | 14 | 11 | 0.11/0.78% | -1.58/0.00% | -1.24/-0.04% |  |
| DRAWDOWN_PAUSE | 5 | 5 | -3.56/0.39% | -2.04/1.07% | -5.00/0.26% |  |
| SCANNER_FILTER | 3 | 3 | -1.31/0.08% | -4.50/-7.46% | -8.06/-7.46% |  |
| QUALITY_GATE_FAILED | 1 | 0 | n/a | n/a | n/a |  |


---
## Counterfactual Report — 2026-08-09 (P1 measurement layer, scripts/counterfactual_report.py)

# Counterfactual Report — 2026-08-09
Window: last 30 days (2026-07-10 to 2026-08-09)
Outlier guard: entry price >= $1.00, |forward return| <= 100% (see script docstring for why)

Executed baseline (BUY signals that went live): n=13/13/13 (1d/3d/5d with bars available)
  fwd_1d: mean=-1.016%, median=-0.059%
  fwd_3d: mean=-2.150%, median=-0.734%
  fwd_5d: mean=-2.912%, median=-0.813%

| Gate | Blocked (deduped) | Bars avail | mean/median fwd_1d | mean/median fwd_3d | mean/median fwd_5d | Structural? |
|---|---|---|---|---|---|---|
| BENCH | 3541 | 3205 | -0.15/-0.14% | -0.71/-0.40% | -0.63/-0.27% |  |
| MARKET_CLOSED | 1825 | 1066 | 0.37/-0.19% | -0.11/-0.48% | -0.10/-0.47% | yes |
| HALT | 747 | 742 | 0.18/0.20% | -0.45/0.19% | -0.32/0.17% | yes |
| MANDATE_BLOCKED | 119 | 84 | 3.33/-0.24% | 1.80/-0.94% | 1.59/-1.19% |  |
| LEARNING_BLOCK | 24 | 22 | -2.10/-1.25% | -2.10/-1.25% | -1.42/-1.22% |  |
| GRADE_B | 2 | 2 | -0.68/-0.68% | -2.03/-2.03% | -2.83/-2.83% |  |
| SCANNER_FILTER | 1 | 1 | -7.55/-7.55% | -7.91/-7.91% | -10.68/-10.68% |  |
| DRAWDOWN_PAUSE | 1 | 1 | 1.07/1.07% | 1.07/1.07% | 4.29/4.29% |  |


---
## Counterfactual Report — 2026-08-23 (P1 measurement layer, scripts/counterfactual_report.py)

# Counterfactual Report — 2026-08-23
Window: last 30 days (2026-07-24 to 2026-08-23)
Outlier guard: entry price >= $1.00, |forward return| <= 100% (see script docstring for why)

Executed baseline (BUY signals that went live): n=0/0/0 (1d/3d/5d with bars available)
  fwd_1d: mean=n/a, median=n/a
  fwd_3d: mean=n/a, median=n/a
  fwd_5d: mean=n/a, median=n/a

| Gate | Blocked (deduped) | Bars avail | mean/median fwd_1d | mean/median fwd_3d | mean/median fwd_5d | Structural? |
|---|---|---|---|---|---|---|

## HM-LIFECYCLE-CRON-GAP (2026-08-29)
check_fleet_lifecycle_drift covers launchd jobs + ai_players only.
Crontab entries have no ledger coverage: 16 quietdown-disabled scripts
were restored and re-armed on 2026-08-29 with job_drift=[] reported.
check_cron_missing_scripts catches entry->dead-path; nothing catches
a quietdowned script coming back. Doctrine says manual edits become a
sentinel finding — not true for cron.

## HM-FALSE-RED-ALERT (2026-08-29)
A RED ALERT was received describing a Season 2 rotation abort
(31 rows vs 1 active, margin=10). No rotation ran: fleet is Season 7,
trigger is Sunday-only, zero rotation lines in trader.log, and no
working Pushover path exists. Text reproduces engine/season_manager.py:176
verbatim including margin=10. Source unidentified.

---
## 🟡 HM-QUIETDOWN-STALE-JOBS-CLASSIFICATION — 2026-08-30 (report-only, no revives executed)

Requested: classify the 16 cron-scheduled scripts still sitting as
`*.quietdown-disabled-2026-07-22` (renamed away during the fleet
stand-down, never reversed — see [[project_ollietrades_final_quietdown_2026-07-22]]
and `HM-LIFECYCLE-CRON-GAP` above, which is the same population: these
scripts sit entirely outside `fleet_lifecycle_ledger` coverage — only
`job` [launchd] and `agent` [ai_players] target types exist there, no
`cron` type, confirmed by a full 2026-08-29 ledger scan turning up zero
rows for any of these 16 names). Two required columns per job:
**current-writer** (is something else already producing this output
today?) and **stale-ref screen** (olliemax / direct-Polygon / per-run
ntfy-curl, all pre-dating the 2026-08-28 429-remediation pass —
`relay_2026-08-28_429-remediation-A-and-D.md`). **No revives executed
this pass** — report only, Admiral picks.

**Worked examples that seeded the method** (both confirmed live,
neither is one of the 16 — they're separate `job`-type launchd entries
that already went through a proper fleet_lifecycle revive 2026-08-29
22:02:55/56 UTC): `uhura-watch` (`scripts/uhura_watch.py`, a *fleet
health monitor*, unrelated to `agents/uhura_agent.py` despite the name)
hasn't fired since revival (next trigger Mon 08-31, weekday-only
schedule) — but the "uhura signal" a viewer actually sees today comes
from a third, fully independent system: `/api/uhura/signal`
(`dashboard/app.py:18511`, backed by `engine.uhura`'s live GEX/IV/skew/
congress/arena confluence compute, cached 120s) — real-time, dashboard-
poll-driven, no relation to either disabled Uhura job. `archer-briefing`
(`engine/archer_morning_synthesis.py`) fired clean at its 06:25 AZ slot
this very morning — `archer_briefings` id 114, `created_at 2026-08-30
13:25:03` (UTC) confirms it live.

### Classification table

| # | Script | Historical output | Current-writer check | Stale-ref screen | Verdict |
|---|--------|-------------------|----------------------|-------------------|---------|
| 1 | `agents/uhura_agent.py` | SEC EDGAR 13F/Form-4 intel officer → `institutional_holdings`/`institutional_signals`/`insider_trades` | **No.** Both `institutional_signals` and `insider_trades` last wrote 2026-07-22 (12:46:49 / 12:30:39), nothing since. **Not** the same "Uhura" as the live `/api/uhura/signal` endpoint above — different data, different code, name collision only. Genuinely dead pipeline, no substitute. | Clean — no ntfy, olliemax, or Polygon calls at all. | **Revive candidate** |
| 2 | `scripts/q_dissent_watch.py` | NTFY on Q's (Grok War Room voice) first live crew dissent | No current watcher, but also no fresh event to watch: `crew_dissent_log` last Q dissent 2026-07-18 — pre-dates even the quietdown. | **Fails** — direct `urllib.request.urlopen()` straight to `ntfy.sh`, own ad hoc IPv4 lock; bypasses the hardened `engine.alert_channels._send_ntfy()` (DECOM-SILENCE guard + 429 backoff). | Needs ntfy migration before revival |
| 3 | `scripts/kimi_cut_watch.py` | NTFY when `ollama-kimi` crosses cut threshold (Sharpe<0, ≥25 closed) | **Moot.** `ollama-kimi` is `halt_mode='full'` — cut already executed 2026-06-19 ("door1-cut: -$1368 realized, negative-expectancy bleeder", backfilled into `fleet_lifecycle_ledger` 2026-08-29). The decision this script exists to trigger happened two months ago. | Fails — same direct-`urlopen` pattern as #2. | **Do not revive** — purpose already fulfilled |
| 4 | `engine/fleet_auditor.py` | `data/health_manifest.json` + ntfy on UP/DOWN transitions | **Ambiguous, leaning no.** The manifest file shows `generated_at: 2026-08-30T02:30:01Z` (genuinely fresh) but `from engine.fleet_auditor import run_audit` fails live right now (`ModuleNotFoundError` — confirmed by direct import test), no commit touched the file today, and the dashboard's own `/api/health-manifest` lazy-refresh path (triggers `run_audit()` when the manifest is >20min stale) is silently broken by the same missing import. Most likely explanation: a one-off manual audit run during tonight's earlier ops-triage, not a standing substitute. | Fails — direct `urlopen()` `_push_ntfy`. | Needs ntfy migration; note the dashboard's silent self-heal breakage regardless of revive decision |
| 5 | `engine/riker_synthesis.py` | Riker XO narrative synthesis | **N/A — already decided, not a candidate.** CLAUDE.md documents code-level retirement 2026-06-24 (`main.py` scheduler removed, not paused); the `riker-synthesis` **job** was formally `retire`d again in `fleet_lifecycle_ledger` 2026-08-29 22:03:04 ("code-retired 06-24, tombstoned"). Bridge live intel layer is the documented substitute. | Was already clean (routed through `alert_channels._send_ntfy()`), moot either way. | **Off this list** — only the file-rename bookkeeping is left, not a revive decision |
| 6 | `scripts/origin_healthcheck.sh` | Actual HTTP-response checks (not just process liveness) for bridge/signal-center/swingdesk/status_page/tour_api, auto-restart + ntfy | **No.** `hm_ops_sentinel.py` (live, `*/5`) checks heartbeat files and launchd status, not live HTTP responses — doesn't replicate the "wedged-but-alive, passes pgrep, fails HTTP" case this script exists for. `watchdog.py` covers process liveness only. Real, uncovered gap while this stays dark. | Fails — direct curl to `ntfy.sh`. | Needs ntfy migration; genuine monitoring gap in the meantime |
| 7 | `scripts/situation_report.py` | Twice-daily 3-line fleet status via ntfy | **Partial overlap, not a strict substitute.** `kirk_briefing.py` (revived, 4x/day: premarket/open/power-hour/after-close) covers similar territory but is a different persona/format. | **Clean** — already routes through `engine.ntfy._fire()` → hardened `alert_channels._send_ntfy()`. | Clean screen; flag possible redundancy with `kirk_briefing` for Admiral's call |
| 8 | `scripts/ollama_prewarm.sh` | Cron pre-warm before ~06:51 AZ market-open cold-start (bridge-wedge fix) | **Likely moot, not confirmed.** `com.ollama.serve`'s `OLLAMA_KEEP_ALIVE=-1` (set 2026-08-27) means the shared model no longer unloads — the cold-start failure mode this script exists to prevent may no longer occur at all. No live cold-start test run to confirm. | Fails — direct curl to `ntfy.sh`. Also targets a specific model/host that predates the 2026-08-25/27 Ollama alias situation — needs re-verification against the current model roster before any revival regardless of the KEEP_ALIVE question. | Low priority either way |
| 9 | `scripts/regime_refresh_runner.py` | Decoupled intraday regime refresh (fixes in-trader scheduler starvation after 06:30 open) | **No — and the bug it fixes is confirmed still live.** `regime_history` gets exactly one row per date (~06:55–06:59 AZ, at/near open) and zero updates the rest of the session, every day checked 2026-08-27 through 08-30. Matches the original failure description exactly. | **Clean** — no ntfy, olliemax, or Polygon calls. | **Strongest revive candidate on the list** |
| 10 | `scripts/recall_refresh_run.sh` | Incremental recall-corpus embed refresh (`.venv-recall` only) | **Not fully characterized.** `recall_corpus` holds 252 rows; didn't verify whether that's caught up to closed-trade volume or badly behind — needs a 5-minute check before deciding, not resolved here. | Fails — direct curl to `ntfy.sh`. | Needs ntfy migration regardless; current-writer status inconclusive |
| 11 | `scripts/daily_report.py` | `drafts/DAILY_REPORT_<date>.md` + `drafts/daily_ledger.csv`, 22:00 UTC | **No** — nothing has produced these files since 07-21. | Clean — no ntfy at all. | **Collides with #13** — see note below |
| 12 | `fleet_realism_sweep_clean_window.py` | Weekly Sunday clean-window backtest sweep → `reports/fleet_realism_sweep_clean_*.json` | **No.** No report newer than 2026-07-05; nothing else runs this backtest. | **Clean** — no ntfy, olliemax, or Polygon calls. | **Revive candidate** |
| 13 | `scripts/eod_report.py` | Same `drafts/DAILY_REPORT_<date>.md` + `daily_ledger.csv`, 14:00 UTC | **No** — same as #11. | Clean — already uses `engine.alert_channels.send_alert`/`AlertLevel` (hardened). Note: its own header claims "no ntfy push" while the code imports `send_alert` — minor doc/behavior mismatch, not a blocker. | **Collides with #11** — see note below |
| 14 | `scripts/iren_flip_watch.py` | IREN ticker flip-watch, "self-removing" | **No.** | Fails — direct `urlopen` to `ntfy.sh`. Also independently re-flagged 2026-08-28 (HM-429-REMEDIATION-D) as an active error source, with a crontab comment incorrectly claiming the file "no longer exists on disk (deleted, not renamed)" — it's present, just renamed; that comment was never actually checked. | Lowest-priority candidate — needs the ntfy fix and a look at the pre-existing error before reviving |
| 15 | `scripts/door1_kill_gate_check.py` | Fleet-wide Door-1 G1-G4 kill-gate, compute-and-push only, never acts | **No, and the decision date it served may have been silently missed.** The G1-G4 verdict was dated **2026-07-24**, but the quietdown disabled this script on **2026-07-22** — two days before it could fire even once at the decision date. Nothing in this file records a verdict ever being rendered. Last live dry-run (2026-07-10): G1/G2/G3 PASS, G4 N/A. | Fails — `subprocess.run(["curl", ...])` direct to `ntfy.sh`. | Needs ntfy migration; **the missed 07-24 verdict is a separate, bigger question than routine revival** — flagging for its own Admiral call |
| 16 | `scripts/ollie_machine_kill_gate_check.py` | Single-agent `ollie-machine` kill gate, same compute-and-push pattern, dated **2026-07-24** | **Same gap as #15.** `ollie-machine`'s only ledger entry (`active`, 2026-08-29) is an unrelated backfilled baseline, not a 07-24 ruling. Verdict never computed. | Fails — identical direct-curl pattern to #15. | Same as #15 |

**Notes:**
- **#11/#13 file collision** is a real structural bug pre-dating the
  quietdown (different cron times, same output path) — reviving both
  would recreate it. Pick one, not both.
- **#4's dashboard self-heal breakage** (`/api/health-manifest` silently
  no-ops on a missing import) exists independent of whether
  `fleet_auditor.py` gets revived — worth its own look either way.
- **#15/#16's missed 07-24 verdict dates** are the one finding here that
  isn't really a "revive y/n" question — the Admiral may want either
  gate computed retroactively against current data before deciding
  anything about the cron line.
- Full per-script investigation (DB queries, live import tests, launchd
  plist reads, ledger scans) in the session transcript if more detail
  than this table is ever needed.

---
## 🟢 HM-DOOR1-OLLIE-MACHINE-KILLGATE-VERDICT — verdicts rendered 2026-08-30 for the 2026-07-24 window (5-week overrun)

Both `door1_kill_gate_check.py` and `ollie_machine_kill_gate_check.py`
were disabled by the 2026-07-22 quietdown two days before their
pre-committed **2026-07-24** verdict date, so neither ever fired — see
`HM-QUIETDOWN-STALE-JOBS-CLASSIFICATION` above. Rendered both verdicts
tonight against the same historical data the scripts would have used,
scoped to the original window, not today's live state. **Rendered
2026-08-30 for the 2026-07-24 window; delayed by the 07-22 stand-down.**

### Door-1 G1-G4 (fleet-wide, `OLLIETRADES_KILL_GATE.md`) — KEEP-eligible, all gates PASS

Replicated `compute_g1`/`g2`/`g3` from `door1_kill_gate_check.py` directly
(live DB + read-only Alpaca `GET`s), scoping each to DAY_0=2026-06-24 →
DAY_30=2026-07-24 instead of "today":

- **G1 Money — PASS.** Realized CSP P&L (era-filtered per the same
  `TROI_REAL_QUOTES_ERA_START=2026-07-07` standard `/api/strategy/pnl`
  already applied by 07-24) = **+$3,154.73** (3 trades, `shadow-qwen35-csp`).
  Open-position MTM = **$0** — confirmed zero CSP positions were open on
  2026-07-24 (all 13 window trades had already closed, last exit
  2026-07-20). Total **$3,154.73 ≥ $500 floor.** (Unfiltered across all 13
  window trades, ignoring the era standard: +$14,161.38 — also a PASS;
  the era-filter choice isn't load-bearing here either way.)
- **G2 Risk — PASS.** Account max drawdown over the window = **2.29%**
  vs. SPY max drawdown 2.24% (SPY return +0.76%, flat by the doc's own
  <±1% definition → 3.0% threshold applies). 2.29% < 3.0%.
- **G3 Tail — PASS.** 13 closed CSP trades in-window, $33,720 premium
  collected, worst single row was **+$643.34 — a profit, not a loss** (no
  realized losses occurred in the window at all). Ratio 1.9% ≪ 20%
  threshold.
- **G4 vs Paid — N/A**, unchanged: no JEPI/JEPQ/NANC/KRUZ parallel-
  benchmark tracking exists anywhere in the codebase, same finding as the
  2026-07-10 dry run.
- **KEEP-eligible on G1+G2+G3: TRUE.**

**Consequence (fleet-wide halt of other strategies on KEEP) WAIVED
2026-08-30 after re-argument.** Grounds: (1) pre-commitment text is the
crowd-out reading, but the present-reason test fails — fleet dark
07-22→08-29, 4/6 candidate seats zero trades since 07-15, remaining two
immaterial (-$116.89 / +$17.49), and Door1's own CSP book dormant since
06-29: no active edge exists to protect from no active competition; (2)
the adjacent 06-19 precedent never enforced crowd-out in practice.
Verdict KEEP unaffected. This waiver retires THIS consequence instance
only — any future gate wanting crowd-out semantics must state it
explicitly with a data-freshness condition (consequence expires if not
rendered within N days of window close), so no standing order can fire
stale again. Case memo: `docs/HM-DOOR1-KEEP-CONSEQUENCE-MEMO-2026-08-30.md`.

**Systemic check, same session:** audited the rest of `docs/XO_BACKLOG.md`
for other pending pre-committed consequences that could fire stale the
same way. Found three, none need a patch:
- **`options-sosnoff`/`qwen3-8b-flash` incumbent-audition gate** (Aug
  15/16 verdict date, `engine.crew.audition_tracking`, and the earlier
  2026-07-05 "6 weeks, no 20 trades → halt proposal" clause under
  `HM-ROSTER-RECONCILE-8` — same two agents, same mechanism). **Already
  safe by construction, not by luck:** the suspension
  (`"suspended": True` in `audition_tracking.py`) is a hardcoded
  code-level flag gated on `HM-ROUTE-TO-BROKER` shipping, not a
  wall-clock check — confirmed `HM-ROUTE-TO-BROKER` is still 🔴
  unstarted, so any gate-day script would correctly report "suspended,
  not evaluable" today regardless of how stale its last run was. No
  expiry condition needed; it can't fire on a date at all.
- **Sniper Mode / `ollie-auto` proving-ground Day-60 `kill_warning`**
  (2026-06-09 boundary, `main.py::run_proving_ground_evaluator`).
  Different architecture — a live daily in-process evaluator with its
  own state machine, not a static standing order in a doc. `ollie-auto`
  is currently `halt_mode='shadow'`, not one of the evaluator's own
  terminal states (`shipped`/`killed`), so its fate was resolved through
  general fleet reclassification rather than the evaluator's CLI — worth
  a look in its own session, but not the same bug class; adding a
  doc-level expiry condition to live code would be the "new architecture"
  this memo was told to avoid.
- No other "if not checked by \[date\], consequence \[X\]" pattern found
  in a search across the rest of the file.

**Would the intervening 5 weeks change this verdict?** No — the CSP book
made zero trades of any kind after 2026-06-29 (last entry) and has zero
open positions today, same as on 07-24. The window's data hasn't changed
since the day the verdict was due; today's re-check is numerically
identical to what a same-day run on 07-24 would have shown.

### ollie-machine single-agent gate — FAIL, gate consequence applied

Zero rows in `trades` (player_id) and `options_trades` (agent_id) as of
2026-07-24 (creation 2026-06-01) — the pre-committed 2026-07-05 trigger
("if ollie-machine has recorded zero trades... by this date, halt
proposal goes to the Admiral," `HM-OLLIE-MACHINE-KILLGATE`) fires.

**Consequence applied 2026-08-30 per direct Admiral authorization:**
`scripts/fleet_lifecycle.py halt ollie-machine --type agent` —
`ai_players.halt_mode` → `full`, `fleet_lifecycle_ledger` id 114, order
doc `docs/orders/ORDER_2026-08-30_halt_agent_ollie-machine.md`,
review-by `2026-09-29`. Reversible via `fleet_lifecycle.py revive
ollie-machine --reason "..."` if the Admiral revisits.

**Would the intervening 5 weeks change this verdict?** No — still zero
trades in either table as of 2026-08-30, over 3 months after creation.
The 5-week gap added no new information; the agent never traded before
or after the missed gate date.

---
## 🟢 HM-IREN-WATCH-CRONTAB-COMMENT-FIX — 2026-08-30

The 2026-08-28 HM-429-REMEDIATION-D crontab comment for
`iren_flip_watch.py` claimed the file "no longer exists on disk (deleted,
not renamed)" — wrong, per `HM-QUIETDOWN-STALE-JOBS-CLASSIFICATION`
above: it's present as
`scripts/iren_flip_watch.py.quietdown-disabled-2026-07-22`, renamed not
deleted, same as its 15 siblings. Corrected the comment in the live
crontab (backed up first:
`~/backups/cron/crontab.bak-20260830-071451-pre-iren-comment-fix`).

**Note on "see commit `<sha>`":** no commit exists for the rename
itself — the 2026-07-22 stand-down renamed all 23 scripts via a plain
filesystem `mv`, not a git operation (crontab writes were EINTR-blocked
that day too; see [[project_ollietrades_final_quietdown_2026-07-22]]).
The corrected comment cites the last commit that touched the file at its
original path (`92a24d9`, unrelated content — an ntfy-hardening sweep)
plus the memory doc as the actual source for the rename event, since a
rename-commit genuinely doesn't exist to cite.

---
## 🟢 HM-QUIETDOWN-FINAL-DISPOSITIONS — 2026-08-30, Admiral decisions executed

Final disposition for all 16 items in `HM-QUIETDOWN-STALE-JOBS-CLASSIFICATION`
above, executed one at a time in the Admiral-specified order, each with a
manual clean-tick verification before moving to the next. Crontab backed up
first: `~/backups/cron/crontab.bak-20260830-090553-pre-revive-batch`.

### REVIVED (files restored, crontab line uncommented)

1. **`regime_refresh_runner.py`** — FIRST, per order. Manual tick: correctly
   no-ops on a non-trading day (exit 0, `skip: not a trading day`). Real
   verification (intraday `regime_history` rows during RTH) due Monday 08-31.
2. **`agents/uhura_agent.py`** — manual tick ran the full real scan (6 min,
   live EDGAR): 19,676 holdings across 7 funds, 227 insider transactions
   across 26 tickers, 10 signals generated, confirmed landed in
   `institutional_signals`/`insider_trades` with fresh 2026-08-30 timestamps.
3. **`fleet_realism_sweep_clean_window.py`** — launched per its own
   documented `nohup ... &` pattern; ran to completion clean, ~10 minutes,
   all 22 agents, incremental crash-safe saves throughout, no errors beyond
   expected/harmless per-symbol Yahoo delisting misses (APLS, MASI).
   `reports/fleet_realism_sweep_clean_20260830_091015.json` — valid, 22/22
   agents present.
4. **`scripts/eod_report.py`** — "wins the collision" per directive, but the
   collision itself was re-checked and doesn't exist: `eod_report.py` writes
   `eod_report_log` (DB) + ntfy only, never `drafts/DAILY_REPORT_*.md` or
   `daily_ledger.csv` — that path belongs entirely to `daily_report.py`, only
   mentioned in `eod_report.py`'s own header as a comparison aside. No doc
   mismatch to fix either (the "no ntfy push" phrase in that header describes
   `daily_report.py`, correctly). Revived anyway per explicit directive.
   Manual `--dry-run` tick: real Alpaca equity read ($98,704.61), guarded P&L,
   both incumbent auditions correctly reported SUSPENDED, "Genuine errors: 0."

### NTFY-MIGRATED THEN REVIVED

5. **`scripts/origin_healthcheck.sh`** — first, per order (monitoring gap).
   Raw `curl` to `ntfy.sh` replaced with a shellout to
   `engine.alert_channels.send_alert` (DECOM-SILENCE/Pushover/rate-limit
   aware). Real run: all 4 services + tour-api passed, no restart triggered.
   Isolated failure-path test: `send_alert` returned
   `{'ntfy': False, 'browser': True}` — correctly suppressed by DECOM-SILENCE
   rather than firing a raw, unfiltered POST.
6. **`scripts/q_dissent_watch.py`** — raw `urllib` + IPv4-monkeypatch replaced
   with `engine.alert_channels.send_alert` (which already carries the same
   IPv4 fix, HM-NTFY-IPV6-NOROUTE-ENGINE-NTFY-FIX). Clean no-op verified:
   state file (`last_id=9285`) matches `crew_dissent_log`'s real max Q-dissent
   id exactly, zero rows since 2026-07-18, script exits 0 silently.

### `scripts/recall_refresh_run.sh` — ENABLED, repointed off decommissioned olliemax, live-verified

7. Raw `curl` replaced with `engine.alert_channels.send_alert` (via the main
   `.venv`, not `.venv-recall`). **Admiral approved enabling; crontab line
   uncommented 2026-08-30.**

   **CORRECTION (same night, superseding the entry below):** the first
   attempt to run the catch-up failed with `[Errno 65] No route to host`
   against the embedding backend, `OLLAMA_EMBED_URL` = Ollie Max
   (`192.168.1.168:11434`, `engine/setup_similarity_signal.py:56`) —
   initially read as "Ollie Max is down, will resume when it's back." **That
   was wrong.** Ollie Max was decommissioned 2026-07 (`e7c3e7d`,
   2026-08-29, "consolidate routing to local com.ollama.serve, retire
   olliemax") — it is never coming back. `setup_similarity_signal.py:56`
   (and, found in the same sweep, `scripts/recall_bakeoff.py:10`, the
   one-time bake-off script, not in the live call chain but fixed for
   consistency) simply **evaded that consolidation's grep** and kept the old
   hardcoded default. Both repointed to `http://127.0.0.1:11434`, matching
   the same `OLLAMA_URL`-env-var-with-127.0.0.1-default convention e7c3e7d
   already established everywhere else.

   `bge-m3` (the embedding model, 1024-dim, matching `EMBED_DIM`) was not
   yet pulled locally — pulled (`ollama pull bge-m3`, ~1.2GB). Confirmed
   embedding-only (`family: bert`, `capabilities: ["embedding"]`) — cannot
   load as a second chat model alongside the qwen3:8b alias set, satisfying
   the "mind the 16GB box" constraint.

   **Live-verified, 3 runs:** run 1: `new=3, embedded=3` (252→255). Runs 2
   and 3: `new=0, embedded=0`, stable. **Final embedded count: 255, not the
   full 1,595.** The ~3-run full-catch-up estimate from the original
   characterization was wrong in a different way than the outage error was:
   `recall_refresh.py` always considers the **500 most-recent-by-date**
   closed trades (`N_CORPUS=500`, newest-first), not "the next 500 unembedded
   ones" — so once the handful of genuinely-recent-and-uncovered trades (3,
   as it turned out) are embedded, the run stabilizes at zero new,
   permanently, because the remaining ~1,340-row gap consists of trades
   *older* than the 500-most-recent-by-date window and are structurally
   unreachable by this script's normal incremental operation, not a bug or a
   remaining backlog to wait out. Closing that gap (if wanted) would need a
   deliberate one-time wider sweep (e.g. a temporary `RECALL_N_CORPUS`
   override) — not attempted here, out of scope of "confirm the count moves
   past 252," flagged as a separate future decision.

   **Widened stale-ref sweep, as ordered:** checked the config/env layer
   (not just script text) of all 6 other scripts revived tonight
   (`uhura_agent.py`, `regime_refresh_runner.py`, `eod_report.py`,
   `origin_healthcheck.sh`, `q_dissent_watch.py`,
   `fleet_realism_sweep_clean_window.py`) and every module they import
   (`engine.halt_gate`, `engine.market_calendar`, `engine.regime_ma`,
   `engine.trades_filter`, `config`, `engine.crew.audition_tracking`,
   `engine.alert_channels`, `engine.backtester`) — zero olliemax/dead-host
   references anywhere. `origin_healthcheck.sh` specifically: its full host
   list (bridge :8080, signal-center :9000, swingdesk :8889, status_page
   :8090, tour-api :8088) is 100% localhost, no olliemax entry to alert on
   forever. `.env` also clean — `OLLAMA_BASE_URL`/`OLLAMA_URL`/
   `ADVISORY_OLLAMA_URL`/`OLLIE_URL` all already `127.0.0.1:11434`.

   **Fleet LLM backend claim, verified rather than repeated:** built the
   real provider list via `engine.agent_routing.build_all_providers()` (the
   exact function `main.py` calls at startup) against the live DB. All 5
   currently-active LLM-routed seats (`ollama-plutus`, `ollama-qwen3`,
   `options-sosnoff`, `qwen3-4b-audition`, `qwen3-8b-flash`) resolve to
   `http://localhost:11434/api/generate`. The other 5 active seats
   (`capitol-trades`, `desk-manual`, `enterprise-computer`, `m5-allocator`,
   `trade-desk`) correctly build no Ollama provider at all (non-LLM/manual/
   data-feed). **The earlier "Ollie Max down = fleet LLM backend at risk"
   flag was an overclaim, not a verified fact — retracted.** The live fleet
   was never on olliemax; nothing to check before Monday open on this front.

### RETIRED (Admiral-approved, permanent, crontab comment updated)

- **`scripts/kimi_cut_watch.py`** — purpose fulfilled: `ollama-kimi` was
  already cut 2026-06-19 (confirmed via `fleet_lifecycle_ledger` backfill).
- **`scripts/daily_report.py`** — **correction on record**: retired per
  explicit Admiral directive, but the stated "superseded by eod_report"
  reason doesn't hold (see item 4 above — no actual collision). Retired on
  consolidation grounds instead (one daily archival mechanism). Revive
  candidate if the markdown-archive/CSV-ledger output is wanted back
  specifically — its own functionality was never actually broken or
  duplicated.
- **`scripts/iren_flip_watch.py`** — Admiral dropped IREN alerts; not fixing
  the pre-existing 429-era error, retiring outright.
- **`engine/fleet_auditor.py`** — STAYS retired, v2 direction decided. No
  file/crontab change (already dark); crontab comment updated to reflect the
  permanent decision instead of the generic stand-down text.

### STAYS DARK, no ledger action (crontab comment refreshed for accuracy only)

- **`scripts/situation_report.py`** — revisit ~2026-09-06 against
  `kirk_briefing.py`'s live coverage before deciding if it's redundant.
- **`scripts/ollama_prewarm.sh`** — revisit after the 2026-09-04 qwen3:8b
  un-aliasing; `OLLAMA_KEEP_ALIVE=-1` (set 08-27) may have already mooted the
  cold-start failure mode it exists to prevent.

### AFTER: hm_ops_sentinel repoint check

**No code change needed — already correct.** `check_launchd_jobs_health()`
(the launchd-side registry check) already skips any ledger-retired/halted
target live; confirmed zero drift (`lifecycle_drift={'job_drift': [],
'agent_drift': [], 'overdue': []}`). The cron-side check
(`check_cron_missing_scripts()`) has no hardcoded per-script registry at all
— it re-reads live crontab every run — so retiring a script by leaving its
crontab line commented is automatically sufficient, no repointing needed.

**Verified the "broken" cron list shrank as intended**, with one honest
caveat found along the way: before tonight's fixes, 6 entries were flagged
broken (`eod_report_cron.log`, `gex_collector.log`,
`origin_healthcheck_cron.log`, `q_dissent_watch.log`,
`regime_refresh_cron.log`, `uhura_cron.log`). After: down to 4
(`q_dissent_watch.log`, `origin_healthcheck_cron.log`, `uhura_cron.log`,
`gex_collector.log`). `eod_report.py` and `regime_refresh_runner.py` cleared
immediately because they always log visible content. **`origin_healthcheck.sh`
and `q_dissent_watch.py` will NOT clear from a healthy/no-op run** — both are
silent-on-success by design, so the sentinel's last-line-of-log check keeps
seeing their old pre-fix error content until either a real alert-worthy event
occurs or someone adds an explicit success heartbeat line. Not a regression,
not fixed here (would be new architecture) — flagging as a known limitation
of the generic check for silent scripts. `uhura_cron.log` will self-clear at
its next real cron fire (05:30 AZ tomorrow; tonight's manual verification ran
outside the crontab's own log redirect). `gex_collector.log` remains the
pre-existing, separately-tracked unverified item (first real post-fix cron
firing also Monday).

---
## 🟢 HM-SENTINEL-BANNER-REGRESSION-INVESTIGATED — 2026-08-30, evidence-based correction

Admiral flagged "3 of today's revives fail under cron despite passing
manual verification," hypothesis: the ntfy migration's `engine.alert_channels`
import works interactively but not under cron's env (PYTHONPATH/venv/cwd).
Per sentinel doctrine, read each log's actual last lines before assuming
the cause — the evidence did not support the hypothesis.

### Item 1 — origin_healthcheck_cron.log / q_dissent_watch.log / uhura_cron.log

**Not a cron-env import bug — verified, not assumed:**
- All three logs' last-write mtimes predate tonight's revival work entirely
  (`origin_healthcheck_cron.log` 08-29 21:40, `q_dissent_watch.log` 08-28
  13:40, `uhura_cron.log` 08-29 05:30) — the stale content is pre-existing,
  not a new post-revival failure.
- **`origin_healthcheck.sh` is not failing — it's running fine, every 5
  minutes, silently.** Cross-checked `trader.log`'s `/api/status` access
  log: hits land every 5 minutes matching the cron schedule exactly
  (11:35:01, 11:40:00/01, 11:45:02, ...), all fast (<1s), confirming real
  healthchecks succeeding. The script is silent-on-success by design (no
  restart needed = no log line at all), which is the SAME blind spot
  already documented earlier tonight, not a regression.
- `q_dissent_watch.py`'s schedule (weekdays 6-13 AZ) has had **zero**
  scheduled opportunities since revival — today is Sunday. `uhura_agent.py`
  (daily 05:30 AZ) also had zero — today's 05:30 slot passed hours before
  the file was revived. Neither has failed; neither has been asked to run
  yet.
- **Directly tested the PYTHONPATH/venv/cwd hypothesis anyway**, since it's
  a real class of bug worth ruling out explicitly rather than just
  asserting: ran both scripts' exact crontab invocation strings under a
  fully stripped environment (`env -i PATH=/usr/bin:/bin:/opt/homebrew/bin
  HOME=/Users/bigmac`, matching real cron's minimal env) — both the
  `engine.alert_channels` import and the full script logic succeed cleanly.
  No PYTHONPATH/venv/cwd issue exists; both scripts insert the repo root
  into `sys.path` themselves.

**Fixed anyway — the real, adjacent problem: the sentinel can't tell
"silently healthy" from "still broken."** Added an unconditional one-line
heartbeat to both `origin_healthcheck.sh` (stdout, after all checks,
captured by the crontab's own redirect) and `q_dissent_watch.py` (the two
`return 0` no-op paths) so every real tick — success, no-op, or failure —
leaves fresh, distinguishable log content. **Verified with a real cron
tick, not a manual run** (per the Admiral's standing instruction going
forward): polled `origin_healthcheck_cron.log`'s mtime, caught the real
11:55:01 tick firing live, confirmed the new heartbeat line landed as the
last line, re-ran the sentinel — `origin_healthcheck_cron.log` cleared from
`broken`. `q_dissent_watch.py`'s next real tick is tomorrow (Monday,
weekday-gated) — cannot be verified by a real tick tonight; not faked with
another manual run. `uhura_agent.py` doesn't need this fix (never
silent — always logs full scan output) — just needs its 05:30 AZ tick
tomorrow.

### Item 2 — gex_collector.log

Same pattern, read directly: log's last write (08-28 13:05) predates the
script's 08-29 06:50 restoration (a separate, earlier session's fix, not
part of tonight's 16-script batch — correcting the "it was in the dropped
batch" framing). Crontab schedule is weekday-only 06:05 AZ; today is
Sunday. No code defect found — nothing to repoint. Resolves at Monday's
real tick, same as items above.

### Item 3 — the 11 stale launchd entries

Checked each individually (StandardOutPath/ErrorPath vs the sentinel's
`LAUNCHD_JOB_REGISTRY`, actual file mtimes, and each plist's real
`StartCalendarInterval`) rather than accepting the "self-adapting" claim
at face value a second time:

- **`hm-wr-dur-monday-check` — genuinely dead, retired.** Its
  `StartCalendarInterval` is a **one-shot hardcoded date, `{Year: 2026,
  Month: 7, Day: 20, Hour: 9, Minute: 0}`**, `RunAtLoad=false` — confirmed
  via direct plist read. Never fires again regardless of enabled state.
  Same dead-one-shot pattern as `hm-signals-v2-monday-check`/`-verify`
  (retired earlier tonight, `8bed0fc`) — missed in that pass, caught now.
  Retired via `fleet_lifecycle.py` (ledger confirms `retire` superseding
  the 08-29 `revive`; `launchctl print-disabled` confirms `disabled`) and
  removed from `LAUNCHD_JOB_REGISTRY` — the ledger-skip alone would have
  been sufficient going forward, removed from the dict too since it's no
  longer part of the 08-29-reactivated set the registry's own docstring
  describes.
- **`archer-briefing` — real log-path bug, repointed.** Registry pointed
  at `logs/archer_briefing.log` (the plist's `StandardOutPath`), which is
  **permanently 0 bytes** — `engine/archer_morning_synthesis.py`'s real
  output goes through Python's `logging` module, which defaults to
  **stderr**, not stdout. `archer_briefing_err.log` had today's real 06:25
  briefing content the whole time. Repointed the registry entry.
  **Found and fixed a real bug along the way**: the same stderr routing
  meant `from engine.alert_channels import _send_ntfy` (no `sys.path`
  setup in the file — launchd invokes it by direct script path, which
  puts `engine/` on `sys.path[0]`, not the repo root) was failing with
  `No module named 'engine'` on **every single run**, silently caught by a
  broad `except`, logged as a mere warning — the briefing itself always
  saved fine, but ntfy delivery never actually fired, ever. Added the same
  `sys.path.insert(0, repo_root)` pattern used elsewhere; verified the
  import succeeds under a simulated direct-invocation + stripped-env test
  matching launchd's real behavior, and confirmed `py_compile` clean under
  the actual runtime this job uses (`venv/bin/python3`, Python 3.9 — not
  `.venv`).
- **`uhura-watch` — is NOT the same thing as tonight's `uhura_agent.py`
  cron revival; correcting that premise.** `scripts/uhura_watch.py`
  ("Uhura-Watch — Fleet Health Monitor," launchd) and
  `agents/uhura_agent.py` (SEC EDGAR 13F/insider intel, cron, revived
  tonight) are two unrelated scripts that happen to share a name — same
  name-collision trap flagged in the original classification report.
  Left `uhura-watch` in the registry unchanged — it's correctly scheduled
  (weekdays 6:30 AM–1 PM AZ), just hasn't had a real tick since the 08-29
  22:02 revive (today's Sunday). Not dropped, not repointed — there was
  nothing wrong with it.
- **The other 8** (`universe-refresh`, `iv-backfill`, `danelfin-update`,
  `enrichment-poller`, `scotty`, `daily-watch`, `morning-an2-observation`,
  `stale-trim-obs`) — checked every plist's real `StandardOutPath` against
  the registry: all match exactly. Checked every real `StartCalendarInterval`:
  all are legitimate recurring weekly/weekday/daily schedules, none yet
  due since the 08-29 22:02 revive. 4 fire later **today** (`universe-refresh`
  14:00, `daily-watch` 13:30 daily, `danelfin-update` 20:00, `enrichment-poller`
  23:00 — all Sunday-inclusive schedules); the rest are weekday-only, next
  real tick Monday. No registry changes needed — these will self-clear on
  their own normal schedule, which is correct behavior, not a bug.

### Verification

`hm_ops_sentinel.py --dry-run` after all fixes: `launchd` stale count
**11 → 9** (both real fixes confirmed: `hm-wr-dur-monday-check` no longer
checked at all, `archer-briefing` cleared). `cron` broken count **6 → 3**
(origin_healthcheck cleared via a real verified tick; the remaining 3 have
no real tick available until Monday, not fixable tonight without faking
verification). `lifecycle_drift` stays `{[], [], []}` throughout — no
new drift introduced.

**Not achieved tonight, and not fakeable per the new standard:** `banner 2`
(cron) at true zero and the 9 remaining launchd entries all require ticks
that are hours-to-a-day away on their own real schedules. Recommend a
follow-up check Monday afternoon once weekday schedules have had a full
cycle, rather than treating tonight's 9/3 remainder as unresolved.
