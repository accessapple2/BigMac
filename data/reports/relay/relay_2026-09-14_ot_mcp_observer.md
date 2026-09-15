# Relay — 2026-09-14 night — read-only MCP observer (HM-OT-MCP-OBSERVER)

Top priority after the stale-GEX batch. The build is committed (`12eccb9`) and tested, but
not deployed: the LaunchDaemon install needs sudo, and the Cloudflare side is applied by
hand. The Captain's spec file isn't on bigmac yet, so this was built from the decisions in
the session, to be diffed against the spec on 2026-09-15.

## What's built

- **`ot_observer/`** is its own process, started by `scripts/ot_mcp_observer.py`. It serves
  MCP streamable HTTP on `127.0.0.1:8765` and is reached only through the tunnel behind
  Access.
- **The ten tools are a proposal.** The spec named `scheduler_health`; the other nine are
  mine: `observer_status`, `read_log`, `recent_decisions`, `recent_trades`, `open_positions`,
  `fleet_roster`, `lifecycle_ledger`, `recent_notifications`, `recent_trade_signals`.
- **Nothing accepts SQL, a path or a shell command.** Queries are fixed, filters are
  validated identifiers, and logs are chosen from a fixed name list: `trader`,
  `trader_error`, `ops_sentinel`, `origin_healthcheck`, `signal_center`, `crusher`,
  `watchdog_cron`.

## Decisions built in

| Decision | Where |
|---|---|
| Separate hostname `ot-mcp.ollietrades.com`; Access with Managed OAuth | `docs/runbooks/ot-mcp-cloudflare.md` (staged) |
| Kill switch returns "disabled", not empty | `data/ot_observer.disabled`; every tool checks it first |
| `prompt_text` off by default; one row behind the flag | `recent_decisions(include_prompt_text=true)` → `limit_applied: 1` |
| Audit every call | `logs/ot_observer.log`: one JSON line with timestamp (UTC+MST), tool, args, status, row count, duration. Covers disabled, rejected and unavailable calls too |
| Never the lock holder | read-only (`mode=ro`) handle opened per request, plus `query_only`, 200 ms busy timeout, 2 s query budget that aborts the query; a counter proves every handle closed |
| Started-without-done is explicit | `scheduler_health.started_without_done`, each entry `in_flight` or `orphaned_by_restart` (restart marker `main.py:4330`) |
| Every timestamp in UTC and MST, both labelled | every timestamp field is `{utc, mst, stored_as, raw}` |
| Separate process, not inside the trader | `main.py` replaces `sqlite3.connect` globally; the observer also uses `sqlite3.dbapi2.connect` |
| "Can't read" is distinct from "no rows" | status is `ok`, `no_rows`, `unavailable` (with reason), `disabled`, `rejected` or `error` |
| Wrapped continuation lines | a rich log line with no trailing `file.py:N` is a continuation; one with it is a new same-second record |

**Correction recorded:** I first said a stopped trader leaves the read-only open failing on
the missing `-shm`. That's wrong.
- **What I probed** (SQLite 3.53): a missing `-shm` reads fine when the directory is writable.
  The open fails only when the WAL index can't be created at all (`-wal` present, `-shm`
  missing, directory not writable).
- **Why it matters:** the trader closes its connections between operations, so a stopped
  trader normally leaves the DB readable.
- **What the observer does instead:**
  - It reports trader liveness separately, as `trader_log_last_write` with its age.
  - The WAL test simulates the condition that actually fails.

## Timestamp bases (verified against the writers)

| Column | Stored as | Evidence |
|---|---|---|
| `decision_audit.created_at` | UTC | all 6 INSERTs leave the `datetime('now')` default |
| `trades.executed_at` | UTC | relay 2026-09-12 TZ gate; `tz_bucket_suspect` flag returned per row |
| `positions.opened_at`, `notifications.timestamp` | UTC | latest values are 2026-09-15 03:1x (now 20:xx MST) |
| `fleet_lifecycle_ledger.created_at` | UTC | both writers leave the default (backfilled rows carry the backfill time) |
| `trade_signals.created_at` | UTC | `server.py:3118` explicit `datetime.now(timezone.utc)` |
| `trade_signals.executed_at` / `dismissed_at` | MST local | 4 writers use `datetime.now().isoformat()`; no stored values yet |
| `ai_players.halted_at` | **per value** | `fleet_lifecycle.py:269` writes local ISO (`T` + fraction); `proving_ground` and manual halts write `CURRENT_TIMESTAMP` (UTC, space) |
| `trader.log` / Python-logging / werkzeug / `crusher` logs | MST local | line formats |
| `origin_healthcheck` | explicit offset | `-0700` in each line |
| `trader_error.log` | time only | returned with a note that the line has no date; no date is guessed |

## Verification

- **Tests:** 100 in `tests/test_ot_observer_*.py`. All five files failed at import before
  any observer code existed; after the build they pass. They cover:
  - INSERT, UPDATE, DELETE and CREATE on the read-only handle all raise, and the row count is
    unchanged.
  - An unopenable WAL index gives `unavailable`; a missing DB gives `unavailable` and isn't
    created.
  - A lock fails in under 1 s; a runaway recursive query is aborted by the budget.
  - No handle stays open after a request (counter at 0, TRUNCATE checkpoint not busy).
  - The kill switch gives `disabled` for all 10 tools, and the call is still audited.
  - Every row tool's default limit, cap clamp, `no_rows`, and `unavailable` when the DB is
    missing.
  - `prompt_text` is excluded by default and capped at one row with the flag.
  - Invalid filters and unknown log names are `rejected`.
  - Parser: carried timestamps, joined continuations, same-second records, redaction.
  - `scheduler_health`: `in_flight`, `orphaned_by_restart`, no activity, window edges.
  - Exactly 10 tools are registered, all `readOnlyHint`.
- **Live smoke** (in-process, real `trader.db` / `signals.db`):
  - All 10 tools returned `ok` in 0.5–188 ms, with 0 handles left open.
  - `scheduler_health` correctly reported `run_ollie_extended_scan` as `orphaned_by_restart`:
    it started 20:08:56 and was killed by the 20:12:11 restart.
  - These smoke calls are in `logs/ot_observer.log`.
- **Over real HTTP** (server run briefly on :8765, then stopped):
  - An MCP client initialized, listed 10 tools (all read-only) and `scheduler_health`
    returned `ok`.
  - `Host: evil.example.com` got `421 Invalid Host header`; `Host: ot-mcp.ollietrades.com`
    got `200`.
  - The server log shows a clean start and a graceful shutdown; exit code 144 was the stop
    signal.

## For the Captain to apply (staged)

1. **LaunchDaemon:** install steps are in the header of `infra/launchd/com.trademinds.ot-mcp.plist`
   (sudo; a system LaunchDaemon with KeepAlive, running as bigmac).
2. **Cloudflare:** `docs/runbooks/ot-mcp-cloudflare.md`. Create the Access app first (Managed
   OAuth under Advanced settings, 5–15 min token, 1–2 week grant), then the tunnel route to
   `localhost:8765`, then run the verify steps.
3. **Two decisions are in that runbook:**
   - `bridge-allow` is recorded as an **inline** policy on the bridge app, and it may not be
     attachable to a second app. If not, recreate the same rule as `ot-mcp-allow`.
   - That rule includes a non-Captain address. Decide whether the observer should have the
     same audience as the bridge.

## Found along the way (not fixed)

1. **The watchdog has been dead for about a week.**
   - `scripts/watchdog_supervisor.sh` runs from cron every 5 minutes and relaunches
     `watchdog.py`, which raises `RuntimeError: OLLAMA_URL not set` at line 63. That's the
     fail-loud check from the 9/11 hardcoded-host fix.
   - The error appears 2,026 times in `watchdog_cron.log` (about 7 days at that cadence), and
     no `watchdog.py` process is running.
   - The supervisor script doesn't load `OLLAMA_URL` or `.env`.
   - Not tracked in XO_BACKLOG.
   - So the trader's 60-second watchdog recovery hasn't been running; only
     `HM-TRADER-KEEPALIVE` / `origin_healthcheck` (every 5 min) remain.
2. **`logs/ot_observer.log` isn't in `scripts/rotate_logs.sh`,** which rotates an explicit
   file list. Growth is about 200 bytes per call.
3. **Reading a cleanly closed WAL DB read-only leaves empty `-wal` / `-shm` files** in
   `data/`. That's harmless; the trader creates the same files.
4. **`recent_decisions(include_prompt_text=true)` with no `decision_id`** returns the newest
   decision, and the newest row can have a null `prompt_text`. Pass `decision_id` to inspect
   a specific prompt.

## Not built (options)

- Validating Access's `Cf-Access-Jwt-Assertion` inside the observer as a second layer.
- An observer self-heartbeat monitored from a different mechanism (alarms must not share a
  failure mode).

## Open

- Diff against the Captain's spec file on 2026-09-15.
- Apply the staged LaunchDaemon and Cloudflare settings, then run the runbook verify steps.
- Watchdog `OLLAMA_URL` crash: needs a decision and a fix.
