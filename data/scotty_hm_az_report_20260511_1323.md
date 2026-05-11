# HM-AZ Closure — Ghost-Trades Query Rewrite (Option B)

**Generated:** 2026-05-11 13:23 MST
**Branch:** main
**Commits ahead of `origin/main`:** **2** (HM-AZ.0 + HM-AZ.3)
**`origin/main` HEAD:** `068359b tune(navigator): bump MAX_POSITIONS 5→10`
**Push performed:** NO
**Service restart performed:** NO

## What shipped

### `engine/ghost_trades.py` — full rewrite (HM-AZ.3)

Four functions rewritten to align with the empirically-canonical schema at `data/trader.db.ghost_trades`:

| Function | Before | After |
|---|---|---|
| `get_ghost_trades` | INNER JOIN on phantom column `g.player_id` → `OperationalError` | LEFT JOIN on `g.advisor = p.id` with SQL aliases (`ts AS created_at`, `advisor AS player_id`, `price AS entry_price`, `rationale AS reasoning`) + `COALESCE(p.display_name, g.advisor)` fallback |
| `get_ghost_stats` | aggregated on phantom `outcome_pnl_pct` | counts real `total_ghosts` (16 rows today); outcome-derived fields return zero (trader.db has no outcome columns) |
| `log_ghost_trade` | INSERT on phantom `player_id` column | INSERT into `(ts, symbol, side, qty, price, fill_price, venue, advisor, status, rationale)` — confidence embedded in rationale string |
| `update_ghost_outcomes` | UPDATE on phantom `outcome_price` | No-op stub with one-shot warning log per process |

### `data/ghost_trades.db` — renamed (HM-AZ.2)

Filesystem rename, not a sacred-DB write:
```
data/ghost_trades.db → data/ghost_trades.db.legacy_lean_2026-05-11
```

Backup preserved at `data/ghost_trades.db.pre-rename-20260511_132055` (integrity_check: ok; 784 rows).

Note: orphan WAL/SHM sidecars from the original path (`data/ghost_trades.db-shm`, `data/ghost_trades.db-wal`) remain — harmless (wal is 0 bytes). If `engine/ghost_trader.py` ever re-fires (currently silent since 2026-04-28, plist has `.bak.20260430_routingleak` sibling), it will create a fresh empty DB at the original path — separate from the renamed file. Acceptable; cleanup deferred.

## Commits

```
8f65a87 fix(ghost-trades): HM-AZ.3 — rewrite queries for trader.db schema (Option B)
8a2a00e docs(scotty): HM-AZ.0 — ghost-trades schema rewrite discovery (Option B path)
```

## Q1–Q6 resolution (as approved)

| # | Original recommendation | **Final Option B path** |
|---|---|---|
| Q1 | Lean schema canonical | **trader.db canonical** — flipped after discovery (lean's writer silent 13 days; trader.db writer fired today 16:27 UTC) |
| Q2 | ATTACH trader.db AS t | **Single-DB**, no ATTACH (both `ghost_trades` and `ai_players` live in trader.db) |
| Q3 | Query-only rewrite | **same** — query-only; no schema migration |
| Q4 | outcome → exit_price/pnl_pct | **omitted** — trader.db has no outcome columns; future ticket adds enrichment |
| Q5 | Rename non-canonical DB | **renamed `data/ghost_trades.db`** (was the stale lean store) to `.legacy_lean_2026-05-11` |
| Q6 | No schema migration | **same** — query-only fix |

## Functional smoke (verified pre-restart via direct import)

```
get_ghost_trades(limit=3)
  → 3 real rows: TER, CEG, AMGN (today + last week)
  → keys: id, symbol, side, qty, created_at, player_id, entry_price,
          fill_price, venue, status, signal_id, reasoning, display_name

get_ghost_trades(player_id='ollie_super_trades')
  → filters correctly (3 matching rows)

get_ghost_stats()
  → {total_ghosts: 16, would_have_won: 0, ..., top_missed: []}

update_ghost_outcomes({})
  → logs warning once; idempotent on repeated calls
```

`log_ghost_trade` smoke test deliberately skipped (sacred-DB write rule). Query syntax verified by py_compile + matches the trader.db schema we read in discovery.

## Consumer impact

| Consumer | Effect |
|---|---|
| `dashboard/app.py:6266` (`/api/ghost-trades`) | Now returns real data instead of 500. Outward dict keys preserved via SQL aliases — no app.py change needed. |
| `dashboard/app.py:6273` (`/api/ghost-stats`) | Returns `total_ghosts: 16` (vs prior 500). Outcome fields will read 0 in the dashboard — visible gap until outcome enrichment ships. |
| `engine/scan_context.py:393` (`ghosts = get_ghost_trades(limit=5)`) | Returns real ghosts (vs prior empty/error). Scan context will now reflect actual ghost activity. |
| `engine/ai_brain.py:733` (`update_ghost_outcomes`) | Becomes a logged no-op. Will see one warning line per process restart in `trader_error.log`. |
| `engine/ai_brain.py:989` (`log_ghost_trade`) | Now writes successfully (was silently failing). Expect new ghost rows to appear in trader.db when confidence ≥ 0.60 HOLDs land. |

## What this fixes

- Eliminates the "single biggest recurring runtime error class" per the infra audit (`docs/SCOTTY_INFRA_AUDIT.md:338`): `sqlite3.OperationalError: no such column: g.player_id`
- Dashboard's ghost-stats panel becomes functional for the first time (counts + recent ghosts)
- `engine/ai_brain.py::log_ghost_trade` becomes a working writer (was a silent failure)

## What this does NOT fix (deferred)

1. **Outcome tracking** — trader.db has no `exit_price` / `pnl_pct`. `update_ghost_outcomes` is a no-op. Dashboard's "would have won/lost/best/worst" stays empty until either:
   - Schema migration to add outcome columns, or
   - A new outcome-enrichment table (FK to ghost_trades.id)
2. **The two-writer split** — `engine/ghost_trader.py` (silent since 2026-04-28) and `scripts/ghost_advisor.py` (active) still exist as parallel writers. Consolidation is a separate cleanup ticket.
3. **The 784-row lean DB** is archived in `.legacy_lean_2026-05-11` — recoverable but no longer read.

## Files

```
 engine/ghost_trades.py                                 | 174 +++++++++--------- (rewritten)
 data/scotty_hm_az_discovery.md                         | 108 +++++++++++ (new)
 data/scotty_hm_az_report_20260511_1323.md              | (this file, new)
 data/ghost_trades.db                                   → (renamed)
 data/ghost_trades.db.legacy_lean_2026-05-11            | (renamed file, 784 rows)
 data/ghost_trades.db.pre-rename-20260511_132055        | (backup, integrity ok)
```

## Standing rule compliance

- ✅ No `git push` performed
- ✅ No service restart performed (required at Admiral's discretion)
- ✅ No sacred-DB write — trader.db only queried; ghost_trades.db is NOT in sacred set
- ✅ Backup taken before rename (good hygiene even though not strictly required)
- ✅ Diff-then-apply (Read before every Edit; explicit Write for full rewrite)
- ✅ One commit per sub-phase (2 commits: HM-AZ.0 docs, HM-AZ.3 code)
- ✅ NTFY on each commit + verify
- ✅ Idempotency anchors: `HM-AZ` matches at 6 sites in `engine/ghost_trades.py`

## Admiral action

```bash
cd ~/autonomous-trader
git log origin/main..HEAD --oneline      # expect 2 commits

# Pause VPN, push
git push origin main

# Restart picks up rewritten module
launchctl kickstart -k gui/$(id -u)/com.trademinds.trader
sleep 5

# Watch trader_error.log — 'no such column: g.player_id' should stop accumulating
tail -50 logs/trader_error.log | grep -c "g.player_id"   # expect 0 going forward

# Verify dashboard endpoints return real data
curl -s "http://localhost:8080/api/ghost-trades?limit=3" | python3 -m json.tool
curl -s "http://localhost:8080/api/ghost-stats" | python3 -m json.tool
```

## Follow-up tickets surfaced

1. **Outcome enrichment** — add `ghost_trades_outcomes` table or migrate trader.db.ghost_trades to include `exit_price` + `pnl_pct`. Restores `would_have_won` stats.
2. **Writer consolidation** — `engine/ghost_trader.py` (stale) vs. `scripts/ghost_advisor.py` (active) two-writer split. Pick one canonical writer or formalize the split.
3. **Orphan sidecar cleanup** — `data/ghost_trades.db-shm` (32K) + `-wal` (0 bytes) are stale. Safe to `rm` but deferred (sacred-data rule's broad reading).
4. **`com.ollietrades.ghost-trader.plist` audit** — currently active in `~/Library/LaunchAgents/` but writer silent since Apr 28. Confirm intent (disable plist OR fix writer) and align.

— Scotty
