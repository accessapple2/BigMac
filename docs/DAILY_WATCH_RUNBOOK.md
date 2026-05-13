# Daily Watch Runbook

**Built:** HM-CLOSE-GAP W3 (2026-05-12)
**Schedule:** 13:30 AZ daily (right after US market close)
**Dispatch:** ntfy `ollietrades-admin` with traffic-light tags
**Script:** `scripts/daily_watch_summary.sh`
**Plist:** `infra/launchd/com.ollietrades.daily-watch.plist` (deployed at `~/Library/LaunchAgents/`)

## What it measures (10 checks)

| Check | Metric | Threshold |
|---|---|---|
| 1 | HM-EQ snaps last 24h | 🟢 >=200, 🟡 100-200, 🔴 <100 (expect ~288) |
| 2 | Polygon fallback events today | 🟡 if >500 (degraded), 🟢 otherwise |
| 3 | `$ATH` delisted errors today | 🔴 if >0, 🟢 if 0 (HM-BL territory) |
| 4 | Adaptive tuning weights (latest run) | Reports snapshot only — drift is visible via repeated daily reads |
| 5 | HM-AS-β cadence drift count today | 🟢 <=5, 🟡 6-20, 🔴 >20 (scheduler stall) |
| 6 | HM-BB new positions written today | Snapshot count |
| 7 | HM-AN2.C consume + HALTED gate count | Both reported; HALTED proves gate working |
| 8 | neo-matrix halt_mode current state | 🟢 active, 🟡 exit_only/full |
| 9 | Squeeze surface (watch + candidates) | Snapshot counts |
| 10 | [red]/Traceback lifetime count | Cumulative; new-today grep needed for delta |

## ntfy priority

- `default` priority by default
- `high` if any 🔴 in the summary

## False-alarm patterns

- **Weekend**: HM-EQ counts naturally lower (no market hours = fewer snapshots in liquid windows)
- **Holidays**: All counts lower; logs may show 0 trades but trader still alive
- **Daylight savings transitions** (March/November): one extra/missing hour in the 24h window
- **First few days after a fresh launchd load**: counts start from 0 and ramp up

## How to disable temporarily

```bash
launchctl unload ~/Library/LaunchAgents/com.ollietrades.daily-watch.plist
```

Re-enable:

```bash
launchctl load ~/Library/LaunchAgents/com.ollietrades.daily-watch.plist
```

## How to run on-demand

```bash
bash ~/autonomous-trader/scripts/daily_watch_summary.sh
```

ntfy will fire as if it were the scheduled run.

## Where logs go

- `logs/daily_watch.log` — full summary text, append-only, one section per fire
- `logs/daily_watch_stdout.log` — launchd captured stdout (single-line dispatched-at marker)
- `logs/daily_watch_stderr.log` — launchd captured stderr (curl errors, sqlite errors)

## Tuning

Thresholds are at the top of `daily_watch_summary.sh` (search for `TAG_*` lines). Edit + reload plist for changes to take effect.

## Cross-references

- HM-EQ: `engine/ai_brain.py:303 start_equity_snapshot_daemon` (module-level start; fires every 5 min)
- HM-BL: `engine/yf_safe.py:_DELISTED_CACHE` (the cache that suppresses repeat $ATH warnings)
- HM-AS-β: `main.py:1006-1024` (battle_station_monitor cadence detector)
- HM-AN2.C: `engine/crew_scanner.py:_hm_an2_consume_signal_center` (neo-matrix Signal Center consumer)
- HM-DASH.4: `engine/squeeze_scanner.py + squeeze_candidates table` (squeeze surface backend)
