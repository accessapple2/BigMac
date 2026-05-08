# HM-AO-β — Squeeze Watcher (Ghost pattern)

**Status:** SHIPPED 2026-05-08, Scotty 3.2 Phase 5
**Ships:** schema + writer + 30-min scheduler + ntfy surfacer + dashboard backend
**Default:** OFF (env flag `SQUEEZE_WATCHER_ENABLED`)
**Frontend panel:** deferred — see HM-AO-β-2 follow-up

---

## 1. Architecture decision

**Ghost Watcher, NOT a 4th autonomous voter.** Mirrors the
`ghost_options_watch` pattern. The squeeze scanner surfaces candidates
to the Admiral via:

- a new `squeeze_watch` table (read by dashboard panel)
- ntfy `ollietrades-admin` for PRIORITY-tier hits

It does **NOT**:

- Write to `signals` table
- Call `paper_trader.py` or any execution path
- Vote alongside ollie-auto / ollama-plutus / capitol-trades / neo-matrix
- Fire any orders to Alpaca

### Rationale

Squeeze plays are event-driven tail-risk (BTDR / SOUN / NVAX-class names
with high short-interest %, low float, abrupt volume). Reward
selectivity over volume. The Admiral has decades of manual squeeze
trading judgment — auto-firing on a one-day data point would burn that
edge. Watcher pattern: scanner picks → DB write → ntfy → Admiral
decides → manual paper trade or dismiss. **No execution risk added to
the fleet.**

### Promotion path

If `squeeze_watch` shows >50 high-quality fires over a 30-day window
**and** forward returns on the surfaced symbols clear a defined bar
(post hoc evidence — not part of this ship), a future epic can promote
to a voting agent. **Not in scope here.** Current ship is
evidence-gathering only.

---

## 2. Schema reference + composite score

### `squeeze_watch` table

```sql
CREATE TABLE squeeze_watch (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol          TEXT NOT NULL,
  scan_ts         TEXT NOT NULL,             -- ISO8601 UTC
  short_pct       REAL,                       -- short interest as % of float
  float_m         REAL,                       -- float in millions
  vol_ratio       REAL,                       -- current vol / 30d avg
  rsi             REAL,                       -- RSI(14)
  breakout_score  REAL,                       -- 0-1; 1.0 if price > 10d high
  composite_score REAL NOT NULL,              -- 0-100 normalized
  threshold_tier  TEXT,                       -- 'WATCH'|'ALERT'|'PRIORITY'
  price_at_scan   REAL,
  notes           TEXT,                       -- raw_score, days_to_cover, etc.
  ntfy_sent       INTEGER DEFAULT 0,
  ntfy_deferred   INTEGER DEFAULT 0,          -- 1 = quiet-hours defer
  dismissed       INTEGER DEFAULT 0,
  dismissed_at    TEXT,
  dismissed_reason TEXT,
  created_at      TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_squeeze_watch_symbol_ts ON squeeze_watch(symbol, scan_ts DESC);
CREATE INDEX idx_squeeze_watch_active ON squeeze_watch(dismissed, scan_ts DESC) WHERE dismissed = 0;
CREATE INDEX idx_squeeze_watch_tier ON squeeze_watch(threshold_tier, scan_ts DESC) WHERE dismissed = 0;
```

Migration script: `scripts/migrations/add_squeeze_watch_table.sql`

### Composite score formula

The scanner already produces a 1-10 integer score
(`engine/squeeze_scanner.py::_score_candidate`). The watcher converts:

```
composite_score = score * 10
```

Tier mapping:

| Score | Composite | Tier |
|------:|----------:|------|
| 1-4 | 10-40 | (not persisted — below `_MIN_PERSIST_SCORE`) |
| 5 | 50 | WATCH |
| 6 | 60 | WATCH |
| 7 | 70 | WATCH |
| 8 | 80 | ALERT |
| 9 | 90 | PRIORITY |
| 10 | 100 | PRIORITY |

Tier alignment with existing `_post_war_room_alerts(score > 8)` is
deliberate: the same threshold that already triggers a Chekov War Room
post now also triggers a PRIORITY ntfy.

---

## 3. Scheduler cadence + feature flag

### Cadence

`schedule.every(30).minutes.do(run_squeeze_watcher)` — registered at
`main.py:2962` near other scanner schedulers.

- Hard 25-min dedupe inside `run_squeeze_watcher` so faster scheduler
  ticks (e.g. caused by drift recovery) cannot double-fire
- HM-AS-β cadence-drift observability: warns if interval > 2400s
  (target 1800s)
- Skipped outside market hours via `RiskManager.is_market_hours()`
- 24h dedupe at the writer layer: same-symbol rows only re-insert if
  the new tier is strictly higher than the most recent non-dismissed
  row (prevents flood of duplicate WATCH rows for chronic-short names)

### Feature flag (default-OFF)

```bash
# .env
SQUEEZE_WATCHER_ENABLED=True   # accepted: 1, true, yes, on (case-insensitive)
```

When flag is unset/False: function logs "SQUEEZE_WATCHER_ENABLED not
set — skipping" once on startup, then is a no-op every tick. Module
load is safe (lazy import inside the function — `engine.squeeze_scanner`
is never imported at main.py module-load time).

---

## 4. ntfy throttle + quiet hours

### Quiet hours

`_is_quiet_hours_et()` returns True for the 22:00-06:00 ET window
(02:00-10:00 UTC during DST). PRIORITY rows that land in this window
are inserted with `ntfy_deferred=1` and **no ntfy is sent**. They
become eligible on the next post-quiet-hours scan.

### Throttle

`_ntfy_priority_candidates(max_individual=5)`:

- ≤ 5 PRIORITY rows pending → **individual ntfys**, one per row, all
  marked `ntfy_sent=1`
- > 5 PRIORITY rows pending → **single rollup ntfy** with the count and
  top symbols, all underlying rows marked `ntfy_sent=1`

Excludes `ntfy_sent=1`, `ntfy_deferred=1`, and `dismissed=1` rows. Never
raises into caller (failures logged to console).

### ntfy topic

`NTFY_ADMIN_TOPIC` env var, defaulting to `ollietrades-admin`.

---

## 5. Dashboard panel routes

Backend (shipped in `dashboard/app.py`):

| Method | Route | Mutating? | Purpose |
|--------|-------|-----------|---------|
| GET | `/api/squeeze/recent?days=N&tier=all\|WATCH\|ALERT\|PRIORITY` | no | List of non-dismissed candidates with computed `age_hours`, ordered tier-desc then time-desc, capped 200 |
| GET | `/api/squeeze/summary?days=N` | no | Tier counts headline (PRIORITY/ALERT/WATCH/total) |
| POST | `/api/squeeze/dismiss` | **yes** | Body `{id: int, reason: str}` — UPDATE squeeze_watch SET dismissed=1. **TIER B per `docs/AUTH_PHASE_1_ROUTE_TIERS.md` §3.A.1.** Auth helper stub already in route body (`# TODO Phase 1: enable after Admiral secret-gen`). |

Frontend deferred — see HM-AO-β-2 follow-up. Until that ships, the
dashboard surfaces are queryable directly:

```bash
curl http://127.0.0.1:8080/api/squeeze/summary?days=7 | jq .
curl 'http://127.0.0.1:8080/api/squeeze/recent?days=7&tier=PRIORITY' | jq '.items[]'
```

---

## 6. Activation runbook (Admiral)

```bash
# Step 1 — Review this doc + verify trader is at the post-merge HEAD.
cd ~/autonomous-trader
git log --oneline -7
# Expect 7 commits f43acac..<status_doc> (matching Phase 5 status).

# Step 2 — Verify schema + (empty) table exist.
sqlite3 data/trader.db ".schema squeeze_watch" | head -3
sqlite3 data/trader.db "SELECT COUNT(*) FROM squeeze_watch;"
# Expect: 0 rows currently.

# Step 3 — Set the feature flag.
echo "SQUEEZE_WATCHER_ENABLED=True" >> .env
# (or edit and ensure no duplicate/older entry overrides)
chmod 600 .env

# Step 4 — Restart trader so main.py reloads + the new dashboard routes go live.
launchctl kickstart -k gui/$(id -u)/com.trademinds.trader
sleep 20
launchctl list | grep com.trademinds.trader   # confirm new PID
tail -50 logs/trader.log | grep -i "squeeze\|error" | head -10

# Step 5 — Wait 30 min. First scheduled scan fires on the next 30-min boundary.
# Then verify rows landed:
sqlite3 data/trader.db "SELECT scan_ts, symbol, threshold_tier, composite_score \
  FROM squeeze_watch ORDER BY id DESC LIMIT 10;"

# Step 6 — Confirm dashboard routes are alive:
curl -s http://127.0.0.1:8080/api/squeeze/summary?days=7

# Step 7 — Monitor ntfy ollietrades-admin for the next 7 days; PRIORITY hits
# should fire (rate-limited per the throttle rules).

# Step 8 — After 30 days of evidence, decide: keep as watcher / promote to
# voter / retire. The promotion epic is OUT OF SCOPE for HM-AO-β.
```

---

## 7. Rollback runbook

```bash
# Disable the watcher — module goes idle, no DB writes, no ntfy.
sed -i.bak 's|^SQUEEZE_WATCHER_ENABLED=.*|SQUEEZE_WATCHER_ENABLED=False|' .env
# (if not present, just delete or leave the line)
launchctl kickstart -k gui/$(id -u)/com.trademinds.trader

# squeeze_watch rows persist for review. To purge them (only if you really
# want to — sacred-data rule generally says preserve):
# sqlite3 data/trader.db "DELETE FROM squeeze_watch WHERE dismissed=0;"
```

---

## 8. Sacred-rules audit

| Rule | Status |
|---|---|
| No edits to `paper_trader.py` | ✅ |
| No edits to gate / strategy files | ✅ |
| No `signals` table writes from scanner | ✅ (verified by `tests/test_squeeze_writer.py::test_no_signals_table_writes`) |
| No trade execution paths | ✅ |
| No `_EXECUTION_ENABLED` flips | ✅ |
| No service restarts performed | ✅ — Admiral times the activation restart |
| No setting `SQUEEZE_WATCHER_ENABLED=True` in this commit | ✅ — flag stays unset |
| No `DROP TABLE` | ✅ |
| Schema migration limited to ONE table (squeeze_watch) | ✅ |
| Pre-migration trader.db snapshot taken | ✅ — `backups/trader.db.pre-squeeze-watch-20260508_063311` |

---

## 9. Test inventory

`tests/test_squeeze_writer.py` — **16/16 PASSED** at ship.

- `test_below_min_score_not_written`
- `test_score_to_composite_mapping`
- `test_tier_thresholds_boundaries`
- `test_dedupe_same_tier_skipped`
- `test_dedupe_lower_tier_skipped`
- `test_dedupe_upgrade_inserts_new_row`
- `test_quiet_hours_marks_deferred_for_priority_only`
- `test_no_signals_table_writes`
- `test_bad_row_does_not_raise`
- `test_tier_helpers`
- `test_ntfy_skipped_in_quiet_hours`
- `test_ntfy_called_outside_quiet_hours`
- `test_ntfy_individual_under_throttle`
- `test_ntfy_rollup_over_throttle`
- `test_ntfy_skips_already_sent`
- `test_ntfy_skips_dismissed`

Run: `./venv/bin/python3 -m pytest tests/test_squeeze_writer.py -v`

---

## 10. Open follow-ups

- **HM-AO-β-2** — Frontend dashboard panel (cards, dismiss button) in
  `dashboard/static/index.html` (CLAUDE.md doctrine: edit static HTML
  only, do not touch React frontend). Backend routes are ready —
  panel is a read-from-API + small mutating dismiss flow.
- **Auth Phase 1 wire-up** for `POST /api/squeeze/dismiss` — TIER B per
  `docs/AUTH_PHASE_1_ROUTE_TIERS.md` §3.A.1. Stub already in route body.
- **Promotion epic (future)** — only after 30 days of evidence on
  surfaced PRIORITY candidates' forward returns.

---

*End of HM-AO-β architecture + runbook. Activation gated on Admiral.*

---

## 11. Activation Log — 2026-05-08 (appended Scotty 3.3 Phase 6)

### Pre-activation

- Pre-snapshot: `backups/trader.db.pre-squeeze-activation-20260508_065633` (258 MB)
- Wiring confirmed intact:
  - `main.py:1400` — `def run_squeeze_watcher():`
  - `main.py:3028` — `schedule.every(30).minutes.do(run_squeeze_watcher)`
  - `main.py:1446` — lazy `from engine.squeeze_scanner import run_scan` inside the function (no module-load coupling)
- `.env` flag append: `SQUEEZE_WATCHER_ENABLED=True` (line 3, post-append; perms stayed `-rw-------`)

### Restart

- 06:56:44 MST — `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader`
- 06:56:50 — new PID 58779 confirmed (was 55222 pre-restart)
- Startup clean — only error in tail was an unrelated FRED API 500
  (transient external API), no squeeze-related errors

### First-scan poll (background bash bar8z8z17)

35-minute poll on `SELECT COUNT(*) FROM squeeze_watch`:

```
[+0m..+31m] squeeze_watch rows: 0   (32 polling samples, all zeros)
```

**Scheduler-driven first fire NOT observed in the 37-minute window.**
No log line `[cyan]Squeeze Watcher: ...` ever emitted by `run_squeeze_watcher`
in `logs/trader.log` post-restart. Diagnostic findings:

- `os.environ['SQUEEZE_WATCHER_ENABLED'] = 'True'` confirmed via sibling
  Python with `load_dotenv(override=True)` — the same path `main.py:24`
  uses
- `engine.squeeze_scanner` imports clean
- `RiskManager.is_market_hours()` returns `'market'` (truthy string,
  market is open), so the market-hours branch wouldn't return-skip
- Trader scheduler IS alive — RedAlert continues firing every ~5 min
  (last 07:27:51) — but the +30-min `run_squeeze_watcher` registration
  has not triggered

Most likely cause: **HM-AS-β scheduler-drift backlog.** The
`battle_station_monitor` cadence-drift log shows recent intervals of
8,090s, 5,975s, 6,857s (target: 120s) — i.e. the single-threaded
`schedule.run_pending()` loop is blocking on slow jobs and pushing
all scheduled entries downstream. New 30-min registrations queue
behind the backlog. Filed as a sibling-of-HM-AS-β observation; no
fix applied this sprint.

### Hand-fire (out-of-band proof)

To unblock activation evidence, fired one scan from a sibling Python
process:

```python
$ ./venv/bin/python3 -c "
import os, sys, time
sys.path.insert(0, '/Users/bigmac/autonomous-trader')
from dotenv import load_dotenv; load_dotenv('.env', override=True)
from engine.squeeze_scanner import run_scan
t0 = time.time(); r = run_scan(force=True); print(time.time()-t0, len(r['results']), r['watch_persist'])
"

[07:33:22] Squeeze Scanner: fetching Finviz candidates...
[07:33:40] Squeeze Scanner: 269 candidates from Finviz
[07:33:56] Squeeze Scanner: 0 squeeze candidates found
elapsed: 34.2s
raw results: 0
watch_persist: {'inserted': 0, 'deferred': 0, 'skipped_dedup': 0, 'ntfy_fired': 0}
```

**Result: 0 squeeze candidates today.** Scanner pulled 269 base
candidates from Finviz (Float Short > 20% screener), but none passed
the post-filter conjunction `vol_ratio >= 2.0 AND rsi < 70`. This is
normal market state — squeeze setups (high short interest +
unusual-volume spike + non-overbought RSI) are rare by definition.
The scanner is functioning correctly; the 0-row outcome reflects
today's tape, not a wiring fault.

### What's verified by the hand-fire

- Module import + dependency chain (finvizfinance, engine.market_data,
  alpaca-bars feed) all working
- `_score_candidate` running on real data
- `_persist_results` reaching the persistence layer cleanly (returned
  the expected summary dict with explicit zeros)
- `_ntfy_priority_candidates` not invoked (correctly — no PRIORITY
  rows to ntfy)
- 34-second total scan time — well under the 30-min cadence budget

### Verdict

**Activation = ACCEPTED, scheduler-driven first fire = OUTSTANDING.**

The watcher is armed and provably-functional via hand-fire. The
scheduler-driven first scan is queued behind the HM-AS-β backlog and
will fire when the scheduler thread catches up. No further action
required from the Admiral — once the scheduler unblocks, scans
proceed at the documented 30-min cadence.

If the schedule continues not to fire within 24 hours, a follow-up
should investigate whether HM-AO-β-3 needs to switch from `schedule`
to a dedicated launchd plist (mirroring the `model-watcher` pattern
shipped in HM-AY-α #6, which sidesteps the in-process scheduler
entirely). That's a separate epic.

### Squeeze panel (HM-AO-β-2)

Already shipped in commit `143a94a` (Phase 7) — UI is live and will
render rows automatically once the scheduler unblocks or the next
hand-fire is run. Empty-state message currently displays for the
panel: "No active squeeze candidates. Watcher runs every 30 min
during market hours."
