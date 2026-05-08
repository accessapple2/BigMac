# HM-AS-β Scheduler Diagnostic — 2026-05-08

**Author:** Scotty Phase 6 follow-up
**Trigger:** squeeze watcher's scheduled first fire (`run_squeeze_watcher`,
`main.py:1400`) never executed in the 37 minutes after trader restart at
06:56:44 MST, despite all static checks passing. Hand-fire of the same
scanner function from a sibling Python returned in 34 s. The scheduler,
not the watcher, is the issue.
**Mode:** **read-only diagnostic.** No fixes applied. Proposed surgical
patches noted but NOT staged.

---

## TL;DR — root cause

OllieTrades runs **129** `schedule.every(...).do(...)` registrations on
the **single-threaded synchronous `schedule` Python library**. Tick loop:

```python
# main.py:4115-4120
while True:
    try:
        schedule.run_pending()    # runs every pending job IN SEQUENCE, in this thread
        ...
        time.sleep(1)
    except Exception: ...
```

`schedule.run_pending()` iterates pending jobs and runs each to
completion before returning. **One slow job head-of-line-blocks every
later job.** With 129 jobs (many of them network-bound LLM calls,
Polygon REST fetches, Finviz scrapes, multi-symbol scans), serial
execution is fundamentally insufficient.

**Drift evidence is consistent with this model.** The
`battle_station_monitor` (every 2 min, target 120 s) records actual
intervals from 180 s up to **8,240 s (137 min)** in the last 24 h.
137 min = at least one prior job ran for two-plus hours synchronously.

**Squeeze watcher's missing first fire** is a clean case study: a
30-min registration sat behind the queue and never got its turn in
37 minutes of wall clock.

---

## 1. Inventory — every scheduled task + claimed cadence

`grep -nE "schedule\\.every\\(" main.py` → **129 registrations.**
A representative slice (full list in the inventory pass):

### Sub-2-minute — none (the lowest cadence is 2 min for `battle_station_monitor`).

### Every 2 min (3 registrations)
- `run_battle_station_monitor` — line 2668. Has explicit drift logging
  at `main.py:1014-1020`; this is the dominant drift-source in
  `trader_error.log`.
- `run_scanner` — line 2641
- `run_crew_scanner_job` — line 3056

### Every 3 min (1)
- `run_war_room` — line 2669

### Every 5 min (~12)
- `run_dayblade`, `run_volume_red_alert`, `run_eod_scorecard_job`,
  `run_portfolio_monitor`, `run_oi_morning_snapshot`, `run_battle_station_0dte_job`,
  `run_alpaca_portfolio_sync`, `run_sulu_autoclose`, `run_earnings_day_scan`,
  `run_bull_spread_exits`, `run_bear_put_spread_exits`,
  `run_bull_call_spread_exits`, `run_bridge_vote_job` (gated)

### Every 10 min (3)
- `run_whisper`, `run_chekov_stoploss`, `run_riker_synthesis`,
  `run_ollie_extended_scan`

### Every 15 min (~25)
- Includes the spread-strategy entry signals, `run_gex_overlay_update`,
  `run_volume_market_scan`, `run_strength_scan`, `run_premarket_scanner`,
  `run_capitol_scan`, `run_cost_monitor`, `run_finviz_premarket_scan`,
  `run_recovery_scan`, `run_wheel_scan`, `run_uoa_premarket`, etc.

### Every 30 min (~30)
- Includes the new `run_squeeze_watcher` (line 3028 — never fired yet),
  plus `run_daily_summary`, `run_journal`, `run_alpaca_gex_refresh`,
  `run_ah_scanner`, `run_autopilot`, `run_weekly_picks`, `run_cto_advisory`,
  `run_ready_room`, `run_team_advisor`, `run_indicator_bench`,
  `run_trend_forecast`, `run_strategy_presets`, `run_sma_scan`,
  `run_theta_scan`, `run_universe_scan`, `run_strategy_scan`,
  `run_metals_commentary`, `run_picard_briefing`, `run_archer_frontier`,
  `run_season_rotation`, `run_market_history_backfill`,
  `run_insider_fetch`, `run_strategy_lab_auto`, `run_crew_strategy`,
  `run_daily_review`, `run_reference_import`, `run_weekly_tuning`,
  `run_aladdin_brief`, `run_q_daily_quote`, `run_daily_reflection`

### Hourly (5)
- `run_earnings_check`, `run_strategy_race`, `run_impulse_check`,
  `run_fundamental_scan`, `run_signal_scorecard`, `check_wheel_assignments`

### Every 2 hours (1)
- `run_imbalance_scan`

### Once-daily / weekly clock-aligned (~50)
- `every().day.at("HH:MM").do(...)` — morning briefings, EOD
  reconciliations, Sunday refreshes, weekly elimination, Picard /
  Surak / Sarek / Janeway / Archer time-locked tasks

**Total: 129 registrations.**

---

## 2. Cadence-drift evidence — last ~24h from `trader_error.log`

The HM-AS-β observability hook at `main.py:1014-1020` logs whenever
`battle_station_monitor`'s actual interval exceeds 180 s. Sampling
the last day:

| Time      | Drift (s) | Drift (min) | × target (120s) |
|-----------|----------:|------------:|----------------:|
| 09:58:06  | 519.8     | 8.7         | 4.3×            |
| 10:50:29  | 665.4     | 11.1        | 5.5×            |
| 11:40:33  | 472.0     | 7.9         | 3.9×            |
| 13:39:12  | 701.8     | 11.7        | 5.8×            |
| 16:41:52  | **5342**  | **89**      | **44×**         |
| 18:36:09  | **6857**  | **114**     | **57×**         |
| 20:53:29  | **8240**  | **137**     | **69×**         |
| 21:40:38  | 2153      | 36          | 18×             |
| 22:36:55  | 2424      | 40          | 20×             |
| 00:16:30  | **5975**  | **100**     | **50×**         |
| 03:22:17  | **8090**  | **135**     | **67×**         |

**5 separate spikes ≥ 89 minutes** in 24h. The 137-minute spike at
20:53 is the largest. During those windows **the entire scheduler
queue is wedged on one slow job**.

Pattern: drift compounds during low-traffic hours (afterhours / overnight
when LLM-advisory and reconciliation jobs run). Drift is meaningful but
contained during the 06:30-13:00 MST market-hours window (180-700 s
range).

The squeeze-watcher missing-first-fire happened in market hours
(06:56:44-07:32 MST). Drift was modest (200-700 s) but enough to push a
+30 min registration past its first window.

---

## 3. Scheduler implementation

```
main.py:13   import schedule
```

**Library:** [`schedule`](https://github.com/dbader/schedule) — pure-Python,
synchronous, single-threaded by default. It has no concept of:

- Concurrent job execution
- Job timeouts
- Background-thread dispatch
- Queueing semantics
- Per-job parallelism

It is intentionally minimal — Vincent Driessen's "schedule.every(N).do(fn)
is friendlier than cron" library. Production-suitable for low-job-count
schedulers; **not designed for 129 concurrent jobs that include
network-bound LLM calls**.

No APScheduler. No Celery. No asyncio task graph. No threading.Timer.
Just `schedule.run_pending()` in a `while True` loop with a 1-second
sleep.

---

## 4. Blocking calls in `main.py`

`grep -nE "time\\.sleep|requests\\.|\\.read\\(|input\\(\\)" main.py`:

```
4120:            time.sleep(1)
```

**Only one blocking call directly in `main.py`** — the 1-second tick
sleep, which is correct and expected.

The blocking is NOT in `main.py`. The blocking is in the **scheduled
functions themselves**:

- `run_capitol_scan` → calls `engine.capitol_fund.run_capitol_scan` which
  scrapes web pages (multi-second I/O)
- `run_picard_briefing`, `run_archer_frontier`, `run_team_advisor`,
  `run_aladdin_brief` → LLM calls to Ollama on Ollie Box (30-180 s each
  per CLAUDE.md S6.3 timing data)
- `run_universe_scan`, `run_strategy_scan` → multi-thousand-symbol
  iteration; per HM-AQ-β notes can take 47 s pre-fix, 1-2 s post-fix —
  but only after the bulk-endpoint perf fix landed
- `run_indicator_bench`, `run_trend_forecast`, `run_strategy_presets`,
  `run_signal_scorecard` → SQL aggregation across `signals` /
  `portfolio_history` tables (signals.db is 644 MB, trader.db is 257 MB)
- `run_market_history_backfill`, `run_insider_fetch`,
  `run_trade_outcomes_backfill` → multi-API backfill jobs
- `run_squeeze_watcher` (the new one) → 34 s for one Finviz fetch +
  ~40 yfinance lookups (measured during today's hand-fire)

**Many of these are individually fine.** The problem is they run on the
**same thread, in series**.

---

## 5. Concurrency posture

`grep -nE "ThreadPoolExecutor|max_workers|concurrent" main.py` →
**0 matches.** No thread pool exists for scheduler dispatch.

However, the codebase **does** use `threading.Thread(target=..., daemon=True).start()`
ad-hoc for some scheduled tasks. From `grep -nE "threading\\." main.py`:

| Line | Worker thread |
|---:|---|
| 421  | `_arena_scan_thread` — daemon |
| 1116 | `_war_room_thread` — daemon, `_war_room_running` event guard |
| 2485 | `_scan_thread` (crew_scanner) — daemon, named "crew_scanner" |
| 2517 | `battle_station_0dte` — daemon, named |
| 2602 | `_startup_market_backfill` — daemon (one-shot) |
| 3924 | `dash_thread` — daemon (uvicorn) |
| 4010 | `_warmup` — daemon (one-shot) |
| 4078 | `_warmup_ollama` — daemon (one-shot) |
| 4096 | `_riker_startup` — daemon (one-shot) |

**Pattern observed:** when authors knew a job was slow, they
hand-spawned a `Thread`. This **partially** insulates the scheduler from
those specific jobs (arena scan, war room, crew scanner, battle station
0dte). But it's done case-by-case, not as a uniform discipline.

**Other 120+ scheduled jobs run synchronously in the scheduler thread.**

Locks observed (`_scan_lock`, `_war_room_running`, `_crew_scanner_lock`,
`_bs0dte_lock`) — these are *good* (prevent overlap of long-running
jobs spawning their own threads), but they don't help the scheduler
itself: any slow synchronous job still blocks the queue.

---

## 6. Root-cause summary

| Layer | Status |
|---|---|
| **Library choice** | `schedule` — synchronous, single-threaded by design |
| **Tick loop** | `schedule.run_pending(); time.sleep(1)` — runs all due jobs serially in main thread |
| **Job count** | 129 — far above the comfortable range for this library |
| **Per-job latency** | mixed; several jobs in 30-180 s range, some multi-minute |
| **Concurrency mitigation** | partial — 9 jobs hand-spawn threads; **120+ run inline** |
| **Drift outcome** | spikes to 89-137 min during heavy-job windows |
| **Squeeze watcher symptom** | +30 min registration queued behind backlog and didn't fire in 37 min |

**Root cause:** **scheduler architecture, not any single offending job.**
The drift will keep happening — and grow — as long as 120+ slow jobs
share a single execution thread. Adding a 130th job (or a 200th)
guarantees more drift, not less.

---

## 7. Proposed surgical patches (NOT APPLIED)

Three options ranked by reward÷risk. **None applied this sprint.**

### Option A — Fire-and-forget thread wrapper (small, 1-line per job)

Add a helper in `main.py`:

```python
import threading
from functools import wraps

def _bg(fn):
    """Wrap a scheduled function so schedule.run_pending() never blocks.
    Each invocation spawns a daemon thread; if the prior invocation is
    still running, a per-job lock can be added via a decorator arg."""
    @wraps(fn)
    def _wrapper():
        threading.Thread(target=fn, daemon=True, name=f"sched_{fn.__name__}").start()
    return _wrapper
```

Then change selected high-value lines from
`schedule.every(30).minutes.do(run_squeeze_watcher)` to
`schedule.every(30).minutes.do(_bg(run_squeeze_watcher))`.

**Pros:** trivially small change. Per-job opt-in. Doesn't touch the
existing `threading.Thread(...)` ad-hoc pattern.
**Cons:** unbounded thread count if a slow job's previous invocation
hasn't finished by next cadence — needs a per-job lock guard
(several jobs already have these). No global parallelism cap.
**Where to apply first:** the 6 fastest cadences (every 2/3/5 min) and
network-bound jobs (Finviz, LLM advisories, Polygon refresh).

### Option B — ThreadPoolExecutor with `max_workers=8` (medium)

```python
from concurrent.futures import ThreadPoolExecutor
_sched_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="sched")

def _pool(fn):
    @wraps(fn)
    def _wrapper():
        _sched_pool.submit(fn)
    return _wrapper
```

**Pros:** bounded thread count; same per-job opt-in syntax.
**Cons:** 8 workers may be insufficient if many slow LLM jobs queue
up simultaneously; queue-backpressure semantics inside the pool need
thinking through.
**Where to apply first:** same as Option A.

### Option C — Move time-sensitive watchers to dedicated launchd plists (large but cleanest)

The pattern is already in use for `com.ollietrades.model-watcher`
(HM-AY-α #6) which sidesteps the in-process scheduler entirely.

**Move out of `schedule`:**
- `run_squeeze_watcher` → `com.ollietrades.squeeze-watcher.plist`
  (every 30 min)
- `run_battle_station_monitor` → `com.ollietrades.battle-station.plist`
  (every 2 min) — biggest drift offender
- `run_capitol_scan` → already has a tendency to delay; move to plist

**Pros:** OS-level scheduler; drift becomes a non-issue; per-job
isolation (one slow scrape doesn't slow another).
**Cons:** needs a small CLI entry-point per moved job; 30+ min ship
per job. Not a one-sprint move; would scope to **HM-AS-γ** as a
multi-week migration starting with the highest-priority watchers.

### Recommended sequence (proposed, not applied)

1. **Option A on `run_squeeze_watcher` only** (1-line change) — unblocks
   the immediate Phase 6 activation symptom
2. **Option A on the 5 biggest current drift offenders** (next sprint)
   — battle_station_monitor (every 2 min), capitol_scan (15 min,
   web-scraping), the 4 LLM advisory jobs (Picard / Archer / Team /
   Aladdin)
3. **Option C migration** of those same jobs to plists once the in-process
   wrapper has soaked for 1-2 weeks — long-term clean architecture

---

## 8. What this diagnostic does NOT cover

- Per-job timing measurement (would require instrumenting each
  `do(fn)` registration with a wrapper that records execution time
  to a new `scheduler_perf` table — separate ticket)
- Identifying the specific 130+ minute job in the 20:53 spike — would
  require correlating with trader.log timestamps at that window
- Whether `signal-center/server.py` (the Flask app on :9000) has its
  own scheduler concerns — out of scope; HM-AS-β has been bigmac-trader
  only

---

## 9. Halt condition

**No code changes performed.** Diagnostic only. The proposed Option A
1-line patch for `run_squeeze_watcher` is documented (§7) but not
applied — Admiral go required before any scheduler change.

If Admiral approves, the **single-line surgical fix** to unblock Phase
6's squeeze activation is:

```diff
- schedule.every(30).minutes.do(run_squeeze_watcher)
+ schedule.every(30).minutes.do(_bg(run_squeeze_watcher))
```

(plus the ~5-line `_bg` helper definition near the top of `main.py`).

That's the smallest possible fix that proves Option A works. After
soak, broader rollout is the proper HM-AS-β follow-up (call it
HM-AS-β.2 or similar).
