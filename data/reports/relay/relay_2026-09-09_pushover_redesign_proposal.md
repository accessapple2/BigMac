# Relay — 2026-09-09 (late) — Pushover redesign: inventory + proposal

Proposal only, nothing built. RULE #1 applies here too even though this
touches no `trades` data — no `notifications` rows are ever deleted;
everything below is new routing logic and new columns/tables, never a
rewrite or purge of history. Build starts after tomorrow's four RULE #1
enforcement layers (`docs/XO_PLAN_2026-09.md`), per instruction.

## Correction up front: no existing tier-design doc found

Searched every relay doc and `docs/*.md` for an existing Pushover/alert
priority-sound-app-cooldown tier design — none exists. The closest real
thing is the `AlertLevel` class already live in `engine/alert_channels.py`
(`INFO` / `WARNING` / `RED_ALERT`, mapped today to ntfy priority/tags
only — Pushover is wired to `RED_ALERT` alone, one hardcoded
`priority=1` call). If a design doc existed elsewhere, it isn't in this
repo, its git history, or the relay doc set — the routing table below is
built from scratch against the inventory, not a citation. Flag it back if
I've missed the real source.

## Current-state facts that shape the design

- **ntfy is fully silenced** (`DECOM-SILENCE 2026-07-19`, `alert_channels.py::_send_ntfy` returns `False` before attempting delivery) — but `kirk_briefing.py` has its **own, separate, unsilenced** direct-to-ntfy.sh `push_ntfy()`, firing 4x/day (05:46, 06:59, 12:45, 13:15 MST). It never routes through `alert_channels.py`, so it was never touched by the silence decision — whether it's actually landing on the phone depends only on whether that ntfy topic is still subscribed there, independent of the deliberate platform silence.
- **Pushover today = `RED_ALERT` lane only**, one call site (`alert_channels.py:521`), fixed `priority=1`, no per-type sound/app/cooldown, credentials from `/usr/local/etc/pushover.env` (not `.env`).
- **`kirk_briefing.py --mode after_close` already runs at 13:15 MST, and `--mode premarket` at 05:46 MST.** These are market-commentary briefings (Kirk's advisory voice), not an alerts/notifications digest. **Decision (2026-09-09 night): merge, not offset** — see the digest section below. No new cron times; Kirk's own separate ntfy path retires and he routes through the same OllieTrades delivery as everything else.

## Inventory — last 30 days, `notifications` table (18,456 rows)

Raw `type` values include per-symbol and per-date suffixes (e.g.
`bk_avwap_bull_ORLA`, `hm-i-b-item5-drift-2026-09-09`) that would make a
raw GROUP BY nearly useless (4,935 distinct combos) — normalized by
stripping trailing `_TICKER` and `-YYYY-MM-DD` suffixes to get the real
semantic alert type. Split by whether it reached Pushover — under today's
code, that's exactly `severity='critical'` (the only value that triggers
`_send_pushover`), nothing more nuanced.

### Reached Pushover (severity=critical) — 8 types, 357 pushes in 30 days
| Alert type | Count | /day avg |
|---|---|---|
| `sentinel_launchd_mass_outage` | 159 | 5.3 |
| `sentinel_mlx_qwen3_heartbeat_stale` | 84 | 2.8 |
| `long_range_sensors_whale` | 74 | 2.5 |
| `sentinel_disk_space_critical` | 19 | 0.6 |
| `hm-season-rotation-aborted` | 9 | 0.3 |
| `polygon_limiter_fail_loud` | 5 | 0.2 |
| `backup_freshness_check` | 5 | 0.2 |
| `sentinel_main_py_down` | 1 | — |
| `sentinel_source_health_watcher_heartbeat_stale` | 1 | — |

`sentinel_launchd_mass_outage` at 5.3/day and `sentinel_mlx_qwen3_heartbeat_stale`
at 2.8/day are the clearest storm-breaker candidates — see below.
`hm-season-rotation-aborted` is the false-alert investigated earlier today
(this afternoon's relay doc) — 8 of its 9 occurrences are the same
unresolved recurrence, now structurally closed off going forward
(`rotate_season()` requires a caller).

### Written to `notifications`, never reached Pushover — 35 types, 18,099 rows
| Alert type | Count | Severity |
|---|---|---|
| `bk_avwap_bear` | 8,109 | info |
| `bk_avwap_bull` | 6,429 | info |
| `sentinel_disk_space` | 965 | warning |
| `sentinel_signals_v2_queue` | 388 | warning |
| `sentinel_lifecycle_drift` | 371 | warning |
| `sentinel_launchd_job_stale` | 308 | warning |
| `sentinel_cron_missing_script` | 307 | warning |
| `source-health-watcher-stale` | 217 | warning |
| `bk_box_bull` | 209 | info |
| `dyn_macd_crossover` | 170 | info |
| `dyn_rsi_oversold` | 143 | info |
| `sys_scan_liveness` | 129 | warning |
| `dyn_volume_spike` | 109 | info |
| `dyn_rsi_overbought` | 57 | info |
| `dyn_resistance_break` | 31 | warning |
| `dyn_support_break` | 21 | warning |
| `sys_confidence_reliability` | 20 | warning |
| `hm-i-b-item5-drift` | 16 | warning |
| `bbkc_squeeze_prewatch` | 16 | warning |
| `sentinel_lock_errors` | 16 | warning |
| `sentinel_mlx_qwen3_unhealthy` | 13 | warning |
| `bk_orb_bull` | 11 | info |
| `origin_healthcheck_restart` | 9 | warning |
| `eod_report` | 7 | info |
| `recall_refresh_failed` | 6 | warning |
| `guardian_sweep_sells` | 4 | warning |
| `user_price_level` | 4 | info |
| `sentinel_fd_warn` | 3 | warning |
| `hm-u-alpaca_sync-ConnectionError` | 2 | warning |
| `deployment_floor` | 2 | warning |
| `tuning_crew_zero_scored` | 2 | warning |
| `q_dissent` | 2 | info |
| `sys_event_tape_liveness` | 2 | warning |
| `strategy_lab_proposal` | 1 | warning |

`bk_avwap_bear`/`bull` alone are **79% of all 30-day notification volume**
(14,538 of 18,456) — these are per-signal pattern hits, correctly never
individually pushed today. They're the strongest case for the digest
mechanism below rather than any per-event routing.

## Proposed routing table

Keyed by normalized alert type (or a sensible group where types clearly
share a risk profile — no reason to hand-tune 43 rows individually when
most fall into one of five natural buckets). `priority`/`sound` are
Pushover's own vocabulary (-2..2, and Pushover's named sounds); `app`
is which Pushover application token delivers it (see the OllieTrades
token note below); `cooldown` matches `alert_channels.RATE_LIMIT_SECS`'s
existing per-`alert_type` mechanism, just tuned per row instead of one
global 300s; `quiet-hours bypass` = fires even during whatever quiet-hours
window gets configured (proposed: 22:00–05:30 MST, i.e. after Kirk's
premarket brief through end of prior evening).

| Tier | Alert types (examples) | Priority | Sound | App | Cooldown | Quiet-hours bypass |
|---|---|---|---|---|---|---|
| **T0 — Emergency** | `sentinel_main_py_down`, kill-switch fires, broker-submit hard failures | 2 (emergency, requires ack) | `siren` | OllieTrades | 0 (never suppress) | Yes |
| **T1 — Critical** | `sentinel_launchd_mass_outage`, `sentinel_disk_space_critical`, `polygon_limiter_fail_loud`, `backup_freshness_check` fail, `hm-season-rotation-aborted` | 1 (high) | `persistent` | OllieTrades | 900s (15min) — was effectively 0 today, this is the storm-breaker's main lever | Yes |
| **T2 — Actionable warning** | `sentinel_signals_v2_queue`, `sentinel_lifecycle_drift`, `sentinel_launchd_job_stale`, `sentinel_cron_missing_script`, `sys_scan_liveness`, `guardian_sweep_sells`, `recall_refresh_failed` | 0 (normal) | `pushover` (default) | OllieTrades | 1800s (30min) | No |
| **T3 — Heartbeat/health, low urgency (silent, in-app)** | `source-health-watcher-stale`, `sentinel_mlx_qwen3_heartbeat_stale`, `sentinel_mlx_qwen3_unhealthy`, `sentinel_lock_errors`, `sentinel_fd_warn`, `origin_healthcheck_restart`, **`long_range_sensors_whale`** | -1 (low, no sound/vibrate, Pushover shows quietly) | none | OllieTrades | 3600s (1hr) | No |
| **T4 — Digest-only, never a standalone push** | `bk_avwap_bull`/`bear`, `bk_box_bull`, `bk_orb_bull`, `dyn_*` pattern hits, `user_price_level`, `q_dissent`, `hm-i-b-item5-drift`, everything else info-level and high-volume | n/a — not pushed individually | n/a | n/a | n/a | n/a |

**Decision (2026-09-09 night):** `long_range_sensors_whale` → **T3**, not
T4 — still lands as its own silent, in-app entry (74 in 30 days, 2.5/day),
just never makes a sound or interrupts. Confirmed, applied to the table
above.

Notes on remaining reclassifications versus today:
- `hm-season-rotation-aborted` stays T1 (it's a real structural-integrity
  alarm) even though its root cause was never found — the September fix
  making `rotate_season()` refuse to run anonymously means any *future*
  fire now self-identifies its caller, which is exactly what a T1 alert
  should do.

## Storm breaker

Two real storm patterns in the inventory motivate this, not a hypothetical:
`sentinel_launchd_mass_outage` at 5.3/day and `sentinel_mlx_qwen3_heartbeat_stale`
at 2.8/day are both symptoms of the SAME underlying condition repeating
(a flapping health check), not 5-8 distinct incidents/day.

Proposed mechanism (new table, additive, no existing data touched):
```sql
CREATE TABLE alert_storm_state (
    alert_type TEXT PRIMARY KEY,
    window_start TEXT,
    count_in_window INTEGER DEFAULT 0,
    last_pushed_at TEXT,
    storm_active INTEGER DEFAULT 0
)
```
Rule: if the same `alert_type` fires **≥3 times inside 10 minutes**, the
4th+ occurrence in that window is written to `notifications` as always
(RULE #1 — every event still gets a durable row) but is **not** pushed;
instead a single "STORM: `<type>` fired N times in the last 10 min, first
at `<ts>`" push replaces the individual pushes, capped at one storm-push
per `alert_type` per 30 minutes. Storm state clears itself once a full
window passes with the count back under 3. This is a genuine dedup layer
on top of the existing per-type cooldown, not a replacement for it — the
per-tier cooldowns above handle steady low-rate repeats; the storm breaker
handles bursts.

## Per-event timestamp + Bridge URL

Timestamp: already fixed today (`HM-PUSHOVER-TIMESTAMP-2026-09-09`,
explicit `timestamp` field on every Pushover send) — carries forward
unchanged into the redesign, nothing new needed here.

Bridge URL: propose appending a deep link to every push's message body,
e.g. `https://bridge.ollietrades.com/classic?notif=<notifications.id>`,
resolving (new, small endpoint) to the relevant context — a signal's
`/api/desk/chain/{signal_id}` view when `agent_id`/a linked signal exists,
otherwise the raw notification. Needs a few minutes' design on exactly
what "context" means per alert type (a sentinel alert has no signal to
link to) — proposing a generic `/classic?notif=<id>` fallback that at
minimum jumps to a notification detail view, richer linking where a
signal/trade id is available.

## Close-of-day digest + premarket summary — merged into Kirk (decision, 2026-09-09 night)

**Decision: merge, not offset.** No new 05:50/13:20 pushes. The digest
becomes the **final paragraph** of `kirk_briefing.py`'s existing 05:46
(`--mode premarket`) and 13:15 (`--mode after_close`) pushes — same
content/shape as originally proposed (counts by type since the prior
digest, storms flagged, any T0/T1 from the window surfaced again in
summary so an overnight critical isn't lost), just appended to Kirk's
message body instead of sent as a second push.

**Kirk's own ntfy path retires.** `kirk_briefing.py::push_ntfy()` (the
separate, direct-to-ntfy.sh function that never routed through
`alert_channels.py` and was therefore never touched by DECOM-SILENCE)
goes away. Kirk's 4 daily briefings (premarket/open_check/power_hour/
after_close) instead send through the same unified OllieTrades routing as
every other alert type here — one delivery path for everything, no more
shadow channel whose delivery depended on whatever ntfy subscription
state happened to still exist on the phone.
- Premarket (05:46 MST): Kirk's brief + digest paragraph appended.
- Open_check (06:59) / power_hour (12:45): unaffected, no digest paragraph
  (mid-session, not a natural summary boundary) — routes through the same
  unified path, just without the appended digest.
- After_close (13:15 MST): Kirk's brief + digest paragraph appended.

## `scripts/alert_test.py` (proposed shape, not built)
```
python3 scripts/alert_test.py                # fires one of each tier (T0-T4-digest-preview), real Pushover send
python3 scripts/alert_test.py --tier T1      # just one tier
python3 scripts/alert_test.py --dry-run      # print what would send, no network call
```
One real push per tier (T0-T3) so the phone shows the actual
priority/sound/app difference live; T4 prints a sample digest line instead
of pushing (it's never a standalone push by design). Bypasses cooldown and
storm-breaker state (test sends must never get swallowed by the mechanism
being tested).

## OllieTrades app token — landed, precedence decided (2026-09-09 night)

`PUSHOVER_TOKEN` / `PUSHOVER_USER` (the OllieTrades application) are now
in `.env`. Decided: `alert_channels.py` reads them from the trader's
environment **first**, falling back to `/usr/local/etc/pushover.env`
**only if absent** — not a replacement of that file, the GPU daemons keep
reading it as-is, unaffected. Concretely: `_send_pushover()`'s credential
resolution becomes `tok, usr = os.environ.get("PUSHOVER_TOKEN"),
os.environ.get("PUSHOVER_USER")`, then only opens
`/usr/local/etc/pushover.env` if either is still `None` after that —
env-first, file-fallback, not the reverse and not env-only.

**Push titles say "OllieTrades"**, not the "TradeMinds" phrasing used
elsewhere in this doc's earlier draft (corrected throughout, including
the routing table's "App" column) and not literally what `send_alert()`'s
current title fallback produces today (`f"{prefix} TradeMinds
{level...}"` — that string needs the same correction in the build, it
wasn't just this doc's wording).

## Build order (unchanged: after tomorrow's RULE #1 layers, before any other data task)
1. `alert_storm_state` table + storm-breaker logic in `alert_channels.py`,
   as specified above.
2. Routing table as data (a dict or a small table — leaning dict, 43 rows
   doesn't need a DB table) replacing the current 3-bucket `AlertLevel` map
   — `long_range_sensors_whale` → T3.
3. Timestamp (already done) + Bridge URL append.
4. Retire `kirk_briefing.py::push_ntfy()`; route Kirk's 4 daily calls
   through the same unified OllieTrades delivery path as everything else.
5. Append the digest paragraph to Kirk's premarket (05:46) and after_close
   (13:15) sends specifically — no new cron entries, no cron edits at all
   for this piece.
6. `scripts/alert_test.py`.
7. `_send_pushover()` credential resolution: `PUSHOVER_TOKEN`/`PUSHOVER_USER`
   from `.env` first (now present), fall back to
   `/usr/local/etc/pushover.env` only if absent — GPU daemons keep reading
   that file untouched. Push titles: `"OllieTrades"`, fixing
   `send_alert()`'s current fallback title too, not just new copy.
