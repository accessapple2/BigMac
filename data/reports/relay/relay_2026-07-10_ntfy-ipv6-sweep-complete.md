# Relay: HM-NTFY-IPV6-NOROUTE-SWEEP — all 27 remaining files fixed (S6, work block 15)

**Date:** 2026-07-10
**Commit:** `92a24d9`
**Prior context:** earlier this session, 4 senders fixed (`long_range_
sensors.py`, `engine/ntfy.py`, `watchdog.py`, `engine/riker_synthesis.py`)
and the remaining ~26-27 files catalogued in `docs/XO_BACKLOG.md`
(`HM-NTFY-IPV6-NOROUTE-SWEEP`). Captain: "fix the remaining 14 files too"
— corrected in-session to the actual catalogued count (27, not 14) before
proceeding, per the transparency doctrine.

## What shipped

All 27 remaining catalogued files, using the two patterns already proven
on the first 4 tonight:

**Delegate to `engine.alert_channels._send_ntfy()` (13 files)** — files
already coupled to `engine/`, or living inside `engine/` itself so a
sibling-module import adds no new dependency:
`agents/scotty/scotty.py`, `signal-center/server.py`,
`engine/squeeze_scanner.py`, `engine/alpha_signals.py`,
`engine/morning_briefing.py`, `engine/dayblade_scanner.py`,
`engine/archer/alerts.py`, `engine/archer_morning_synthesis.py`,
`engine/universe_refresh.py`, `engine/orcl_gex_alerts.py`,
`engine/fred_data.py`, `engine/fleet_auditor.py`,
`engine/universe_scanner.py`.

**Self-contained local IPv4-force lock+monkeypatch (14 files)** — zero
existing `engine/` dependency, same reasoning as `watchdog.py`'s own fix
(stay independent of the package being monitored/orchestrated):
`swingdesk/shadow_autopilot.py`, `scripts/ghost_advisor.py`,
`scripts/uhura_watch.py`, `scripts/q_market_open_ping.py`,
`scripts/q_dissent_watch.py`, `scripts/import_schwab_csv.py`,
`scripts/model_watcher.py`, `scripts/model_sweep_v2.py`,
`scripts/schwab_drawdown_alert.py`, `scripts/fleet_heartbeat.py`,
`scripts/kimi_cut_watch.py`, `scripts/iren_flip_watch.py`,
`scripts/learning/check_pipeline.py`, `healthcheck.py`.

**Notable finds along the way:**
- `scripts/iren_flip_watch.py` had never been added to git before —
  fixed and committed for the first time in this same commit.
- `scripts/q_market_open_ping.py` is permanently dead code (one-shot
  guarded to fire only on 2026-06-09, already past) — fixed anyway for
  catalogue consistency, zero live risk either way.
- `dayblade_scanner.py::_push_ntfy()` is gated on an unset `NTFY_URL` env
  var — currently a no-op in production; fixed so it's safe the moment
  someone sets that var.

## Testing

- `tests/test_ntfy_ipv6_sweep_delegate_batch.py` — 14 tests, one per
  delegate-pattern file. Mocks `engine.alert_channels._send_ntfy` and
  asserts title/message/priority/topic on each call.
- `tests/test_ntfy_ipv6_sweep_standalone_batch.py` — 16 tests (14 files +
  2 restore-on-failure spot-checks). Asserts `socket.getaddrinfo` is
  forced to the module's own `_ipv4_only_getaddrinfo` during the send and
  restored to the true original afterward, including on failure.
  `model_sweep_v2`'s test is guarded with `pytest.importorskip("vectorbt")`
  — that module only lives in `.venv-backtest`, never the main test venv,
  per existing doctrine; confirmed it py_compiles clean regardless.
- `signal-center/server.py`'s test runs under its own Python 3.9 `venv/`
  (flask + pyotp aren't in the main `.venv`) — guarded with
  `pytest.importorskip` so it skips gracefully under the main suite, and
  **confirmed genuinely passing** when run directly under `venv/bin/python3`.
- Full suite under `.venv`: **1026 passed, 21 failed, 2 skipped** — the 21
  failures are the same pre-existing bbkc/m5_allocator/quality_gate/
  universe_filter/war_room flakiness confirmed unrelated to these changes
  (this exact set has now been re-confirmed unrelated across 3+ separate
  fix sessions this week).
- One real bug caught by my own test suite before it shipped: the
  `squeeze_scanner` test initially hardcoded `topic == "ollietrades-admin"`,
  but the real `.env` has `NTFY_ADMIN_TOPIC=Ollie-Alert-35` — another
  test's `dotenv` load earlier in the full-suite run leaked that into
  `os.environ` for the whole process, and my test's *code* was correct
  (topic genuinely resolves from env at call time) but my *assertion* was
  wrong. Fixed by pinning the env var explicitly via `monkeypatch.setenv`
  rather than relying on ambient process state.
- `py_compile` clean on all 27 touched files.

## Live verification

- **`main.py`/trader** — holds 8 of the fixed modules in memory
  (`squeeze_scanner`, `alpha_signals`, `morning_briefing`,
  `dayblade_scanner`, `archer/alerts`, `fred_data`, `fleet_auditor`,
  `universe_scanner`, via direct imports plus `dashboard/app.py`, same
  process). Restarted via `trader_restart.sh`: old PID killed, WAL
  checkpointed, new PID `35742` bound `:8080`, `RESTART OK` confirmed,
  clean startup log (no import errors, Guardian sweep + HM-EQ snapshot
  both fired normally).
- **`swingdesk/backend.py`** — holds `shadow_autopilot.py` in memory
  (imports `start_shadow_scheduler` at module load). Restarted via
  `swingdesk_restart.sh`: new PID `35976` confirmed alive, bound `:8889`,
  HTTP 200 on a direct curl. (Log briefly showed an "address already in
  use" line from the `com.trademinds.swingdesk.plist` LaunchDaemon losing
  a startup race against the manual restart — harmless, single live
  process confirmed via `ps`/`lsof`.)
- **`signal-center/server.py`** — not running at fix time (no process
  found). Will pick up the fix automatically whenever it's next started.
- **Everything else** (scotty, all `scripts/*` one-shots,
  `archer_morning_synthesis.py`, `universe_refresh.py`,
  `orcl_gex_alerts.py`, `fred_data.py`'s CARTS job, `healthcheck.py`) is
  cron/launchd-invoked fresh per run — no long-lived process holds stale
  bytecode, self-heals on next scheduled invocation, no restart needed.

## docs/XO_BACKLOG.md

`HM-NTFY-IPV6-NOROUTE-SWEEP` updated from 🟡 (4 of ~18 fixed) to 🟢
(closed — all 31 total Python senders now IPv4-hardened). Full file/
pattern breakdown recorded in the ticket. The ~17 `.sh` scripts remain
out of scope (curl handles this box's IPv6 condition gracefully,
confirmed via live test twice this week).

## Open items (carried forward)

Same 5 items as the prior relay (`relay_2026-07-10_ntfy-ipv6-full-audit.md`)
— none touched this block:
1. `HM-STATUS-PAGE-STALE-CACHE` — needs a Cloudflare dashboard change only
   the Captain can make.
2. `HM-STRATEGIES-EXECUTOR-STATUS-NEVER-SET`/`HM-SWINGDESK-CLOSE-PHANTOM-
   ROW` pnl gap — on hold pending a live MLEG close.
3. `HM-DRAWDOWN-BLIND-TO-OPTIONS-PNL` — needs a dedicated design session.
4. The `options_books` stored-counter drift — still harmless, still out
   of scope.
5. `HM-ARMED-DORMANT-SPREAD-STRATEGIES` — `bull_spread_v1` self-healing in
   progress (per the entry immediately below this one in `XO_BACKLOG.md`),
   unrelated to tonight's ntfy work.

**This closes the entire HM-NTFY-IPV6-NOROUTE-SWEEP thread** — all 31
Python ntfy senders in the codebase now force IPv4, tested, restarted
where live, and catalogued.
