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
