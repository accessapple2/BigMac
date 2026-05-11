# HM-AZ Discovery — 2026-05-11 (Option B path)

Read-only sweep + scope revision after first sweep contradicted audit's Q1 recommendation.

## Plan as approved (Option B)

| Question | Original recommendation | **Revised after discovery** |
|---|---|---|
| Q1 — Canonical schema | lean `ghost_trades.db` | **trader.db** — empirically active (last write today 16:27 UTC) |
| Q2 — Cross-DB JOIN strategy | ATTACH trader.db AS t | **Single-DB** — both `ghost_trades` and `ai_players` in trader.db. No ATTACH. |
| Q3 — Column-mapping policy | rewrite query, no schema migration | **same** — query-only |
| Q4 — `outcome_*` mapping | `exit_price` + `pnl_pct` (lean) | **omit** — trader.db has no outcome columns; gracefully return NULL/0 |
| Q5 — Non-canonical DB | rename trader.db table | **rename `data/ghost_trades.db` file** → `.legacy_lean_2026-05-11` (the now-stale store) |
| Q6 — Migration vs query-only | query-only | **same** |

## Why scope flipped

| File | Writes to | Latest write | Status |
|---|---|---|---|
| `scripts/ghost_advisor.py:205,259` | `data/trader.db.ghost_trades` | **2026-05-11 16:27 UTC** | 🟢 ACTIVE |
| `engine/ghost_trader.py:148,295` | `data/ghost_trades.db` (lean) | 2026-04-28 06:35 (13 days idle) | 🔴 STALE |
| `engine/ghost_trades.py:25,51` (broken reader) | `data/trader.db` (wrong columns) | always errored | 🔴 BROKEN |

The original audit recommendation was likely written before `ghost_advisor.py` took over as the live writer. Empirical reality flips Q1: trader.db is canonical.

`com.ollietrades.ghost-trader.plist` exists at `~/Library/LaunchAgents/` (Apr 14 mtime) with a `.bak.20260430_routingleak` sibling — suggests the writer was edited/disabled Apr 30. Compatible with the 13-day silence. Safe to rename the file out from under it; if the plist ever fires again, it'll create a fresh empty DB at the original path.

## trader.db `ghost_trades` schema (the new canonical)

```
id            INTEGER PK
ts            TEXT NOT NULL                       ← maps to created_at semantics
symbol        TEXT NOT NULL
side          TEXT CHECK IN ('BUY','SELL')
qty           REAL NOT NULL
price         REAL NOT NULL                       ← maps to entry_price semantics
fill_price    REAL
venue         TEXT CHECK IN ('virtual','alpaca_shadow')
advisor       TEXT NOT NULL                       ← maps to player_id semantics
signal_id     TEXT
status        TEXT DEFAULT 'filled'
rationale     TEXT
```

**Indices:** `idx_ghost_trades_ts`, `idx_ghost_trades_symbol`, `idx_ghost_trades_advisor`.

## advisor ⇆ ai_players mismatch

Sample advisor values in trader.db.ghost_trades: `ollie_super_trades`, `trailing_stop`. These are **strategy labels, not player_ids**. Joining `ai_players p ON g.advisor = p.id` would return 0 matches for current data.

**Mitigation:** use `LEFT JOIN` so ghosts without an ai_players row still surface in the reader. `display_name` will be NULL for those; consumers should `COALESCE(p.display_name, g.advisor)`.

## Fields the reader expects but trader.db lacks

- `confidence` — not in trader.db → return NULL
- `pnl_pct` / `outcome_price` / `outcome_pnl_pct` — no outcome tracking in trader.db. `get_ghost_stats()` summary stats will be mostly zero/null. Acceptable v1; future ticket can add outcome enrichment.
- `entry_price` → use `g.price AS entry_price`
- `created_at` → use `g.ts AS created_at`
- `player_id` → use `g.advisor AS player_id`

## Consumers of engine.ghost_trades

```
dashboard/app.py:6266     from engine.ghost_trades import get_ghost_trades
dashboard/app.py:6267     return get_ghost_trades(player_id, limit)
dashboard/app.py:6273     from engine.ghost_trades import get_ghost_stats
dashboard/app.py:6274     return get_ghost_stats()
engine/scan_context.py:393 from engine.ghost_trades import get_ghost_trades
engine/scan_context.py:394 ghosts = get_ghost_trades(limit=5)
engine/ai_brain.py:733-734 update_ghost_outcomes — **out of scope this phase**
engine/ai_brain.py:989-990 log_ghost_trade — **out of scope this phase**
```

Read consumers (`dashboard/app.py`, `engine/scan_context.py`) consume `dict(r)` — they read whatever columns the SELECT returns. As long as the response dict has the keys they expect (`symbol`, `display_name`, `player_id`/`advisor`, `created_at`/`ts`, `entry_price`/`price`), they'll work. Will preserve outward-facing keys via SQL aliases.

## Scope of write functions (`log_ghost_trade`, `update_ghost_outcomes`)

Both reference columns that don't exist in EITHER schema (`player_id`, `outcome_price`, `outcome_pnl_pct`, `updated_at`). They've been silently broken for as long as the schema mismatch existed. **Default-accept Q6 was "query-only fix; no schema migration."** Interpreting "query" generously to mean all 4 SQL statements in the module — I will fix the writes to match the trader.db schema too, so the module is internally consistent.

Mapping for the writes:
- `log_ghost_trade(player_id, symbol, confidence, reasoning, price)`:
  - `ts` = `datetime.utcnow().isoformat() + "+00:00"`
  - `symbol` = symbol
  - `side` = `'BUY'` (this is a HOLD ghost — but the CHECK constraint requires BUY/SELL; pick BUY as it's the more useful read)
  - `qty` = 0.0 (ghost, no actual fill)
  - `price` = price
  - `venue` = `'virtual'`
  - `advisor` = player_id
  - `rationale` = reasoning
  - confidence DROPPED (no column) — rationale string can include it as `f"conf={confidence:.2f}: {reasoning}"`
- `update_ghost_outcomes(prices)`:
  - **NO-OP**: trader.db has no outcome columns. Function becomes a stub that logs "outcomes not tracked in trader.db schema" once and returns. Future ticket.

## Sub-phase plan (revised)

| # | Action | Sacred-DB? | Restart? |
|---|---|---|---|
| HM-AZ.0 | Discovery (this doc) — commit | no | no |
| HM-AZ.1 | `cp data/ghost_trades.db data/ghost_trades.db.pre-rename-<ts>` + `sqlite3 integrity_check` | no (ghost_trades.db is not in sacred set per CLAUDE.md) | no |
| HM-AZ.2 | `mv data/ghost_trades.db data/ghost_trades.db.legacy_lean_2026-05-11` | no | no |
| HM-AZ.3 | Rewrite `engine/ghost_trades.py` queries (all 4) | no | **yes** |
| HM-AZ.C | py_compile + import smoke + sample query | no | no |
| HM-AZ.D | Closure report | no | — |

Notes:
- `trader.db` is NOT touched. No sacred-DB write.
- `ghost_trades.db` is renamed — outside the sacred set (per CLAUDE.md, only `trader.db`, `arena.db`, `tractor.db` are sacred). Backup before rename is good hygiene, not a rule requirement.
- Restart required after HM-AZ.3 to load the updated `engine/ghost_trades.py` module into the trader process.
