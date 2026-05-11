# HM-BB Discovery

**Phase:** BB.0 (read-only inventory)
**Date:** 2026-05-11
**Scotty:** Claude Code · Opus 4.7 · `claude-opus-4-7[1m]`
**Pre-flight:** PASS — HM-AZ commits in origin (`2ee11fa`, `8f65a87`, `8a2a00e`), service PID 75002, schema confirmed thin, 16 rows.

---

## 1. Existing columns (`trader.db.ghost_trades`)

```
cid name        type    notnull dflt        pk
0   id          INTEGER 0       —           1
1   ts          TEXT    1       —           0
2   symbol      TEXT    1       —           0
3   side        TEXT    1       —           0
4   qty         REAL    1       —           0
5   price       REAL    1       —           0
6   fill_price  REAL    0       —           0
7   venue       TEXT    1       —           0
8   advisor     TEXT    1       —           0
9   signal_id   TEXT    0       —           0
10  status      TEXT    1       'filled'    0
11  rationale   TEXT    0       —           0
```

Indexes: `idx_ghost_trades_ts`, `idx_ghost_trades_symbol`, `idx_ghost_trades_advisor`.

---

## 2. Missing columns

All four targets are absent — full migration set required:

- `entry_price` REAL — **missing**
- `confidence` REAL — **missing**
- `exit_price` REAL — **missing**
- `pnl_pct` REAL — **missing**

Note: existing `price` column already holds an entry-price-shaped value at INSERT (passed as `price` in `execute_buy`/`execute_sell`). `entry_price` is semantically redundant on the BUY side but distinct on the SELL/close side, where today there's no separate exit-price field. Captain decision needed (see §8).

---

## 3. Writer files — TWO active writers into `trader.db.ghost_trades`

### Writer A — `scripts/ghost_advisor.py` (the main writer per directive)

| Line | Function | Side | INSERT scope |
|-----:|----------|------|--------------|
| 199–209 | `execute_buy(sym, qty, price, advisor, signal_id, rationale, tiers, cur, dry)` | BUY | INSERT at L205-209 |
| 243–263 | `execute_sell(sym, qty_to_sell, price, advisor, signal_id, rationale, cur, dry, partial=False)` | SELL | INSERT at L259-263 |

**Variables in scope at INSERT (both sites):** `sym, qty, price, advisor, signal_id, rationale`. **No `confidence` field in scope** — confirmed by `grep "confidence\|conf\b" scripts/ghost_advisor.py` returning 0 matches.

### Confidence-thread feasibility (per signal source)

The `apply_decision()` dispatcher (L430) consumes `decision` dicts assembled by signal readers (L322–427). To populate `confidence`, each reader needs to write a `confidence` key:

| Reader | Source table | Confidence-shaped column available |
|--------|--------------|-----------------------------------|
| `read_super_trades` (L322) | `ollie_super_trades` | `success_prob` — direct map to `confidence` |
| `read_smart_money` (L346) | `smart_money_signals` | none — could synthesize from `len(buyers)` (e.g. `min(buyers*0.25, 1.0)`) |
| `read_advisories` (L372) | `trade_advisories` | `multiplier` (semantic mismatch — alpha weight, not probability) — leave NULL |
| `read_kirk` (L408) | `kirk_advisory_log` | none — leave NULL |
| (trailing stop call at L311) | synthetic | none — leave NULL |

**Recommendation:** populate `confidence` where directly available (super_trades only), NULL elsewhere. Document the partial coverage in commit message.

### Writer B — `engine/ghost_trades.py` (HOLD-decision writer)

| Line | Function | Call site | Confidence in scope? |
|-----:|----------|-----------|---------------------|
| 63–74 | `log_ghost_trade(player_id, symbol, confidence, reasoning, price)` | `engine/ai_brain.py:989` | **YES — function param** |

Currently embeds confidence in the rationale string (`f"conf={confidence:.2f}: {reasoning}"`) because there was no column. After BB.2 this writer should populate the new `confidence` column directly. The rationale-embedded confidence can stay (informational) or be cleaned up; recommend leaving it for backward compatibility with existing 16 rows.

### Other ghost_trades INSERTs (NOT in trader.db scope)

- `engine/ghost_trader.py:148` — INSERTs into `data/ghost_trades.db` (legacy lean DB, renamed `data/ghost_trades.db.legacy_lean_2026-05-11` per HM-AZ.2). Its native schema **already has** `entry_price`, `confidence`, `exit_price`, `pnl_pct`. **Out of HM-BB scope** unless the directive intends to revive this writer — see §8.

---

## 4. Close hook / UPDATE statements

### Status: **NO close-side UPDATE exists** in either canonical writer for `trader.db.ghost_trades`.

`scripts/ghost_advisor.py` records the SELL as a separate INSERT row (L259), not an UPDATE of the BUY row. The DB carries the BUY and SELL as independent rows; they're correlated only by `symbol`, `advisor`, `signal_id`. There is no `mark_closed`, `update_ghost`, or "close" function in `scripts/ghost_advisor.py`.

`engine/ghost_trades.py::update_ghost_outcomes` (L82–97) is a logged no-op stub per HM-AZ.

### Implication for BB.3

`exit_price` / `pnl_pct` populated on close requires a design choice:

**Option α — SELL-row population (simple):**
On `execute_sell`, populate the SELL row's `exit_price` (= the SELL price) and `pnl_pct` (computed by looking up the most recent matching BUY row for the same `symbol+advisor`, computing `(sell_price - buy_price) / buy_price * 100`). Two SELECTs + the existing INSERT. No schema-shape change.

**Option β — UPDATE-the-BUY (paired):**
On `execute_sell`, UPDATE the matched BUY row's `exit_price` and `pnl_pct`, AND insert the SELL row with those same values for symmetry. Adds a new UPDATE statement. More work, but the BUY row becomes self-describing.

**Option γ — Add an outcome table:**
Defer outcome to a new `ghost_outcomes` table referencing `ghost_trades.id`. Out of HM-BB scope per directive (BB.2 specifies ALTER TABLE on `ghost_trades`).

**Recommendation:** Option α (SELL-row population). Minimal schema impact, matches existing two-row-per-trade pattern, and `pnl_pct` on the SELL row is the conventional read shape. The BUY row gets `exit_price=NULL, pnl_pct=NULL` (still-open); the SELL row gets both populated. Reader filters `WHERE status='filled' AND side='SELL'` for closed-trade stats.

---

## 5. Reader files — Directive ambiguity, must resolve before BB.4

### `engine/ghost_trades.py` (plural, HM-AZ canonical) — reads `trader.db`

- L100–132 `get_ghost_trades(player_id, limit)` — list view
- L135–158 `get_ghost_stats()` — aggregate (currently returns hardcoded zeros for outcome stats)

Dashboard endpoints that hit this:

- `GET /api/ghost-trades` → `dashboard/app.py:6263-6267` → `get_ghost_trades`
- `GET /api/ghost-trades/stats` → `dashboard/app.py:6270-6274` → `get_ghost_stats`

Used by `engine/scan_context.py:393` as well.

### `engine/ghost_trader.py` (singular, legacy lean) — reads `data/ghost_trades.db`

- `get_scorecard(days)` at L316
- `get_recent_trades(limit, agent, status)` (referenced by dashboard L16989)
- `capture_new_signals()`, `check_outcomes()` at L101 / L168

Native schema **already has** `entry_price, confidence, exit_price, pnl_pct, hit_target, hit_stop, max_gain_pct, max_loss_pct, exit_time`. Its INSERT and UPDATE statements (L148, L295) are already wired for these fields.

Dashboard endpoints:

- `GET /api/ghost/scorecard` → L16977 → `get_scorecard`
- `GET /api/ghost/trades` → L16989 → `get_recent_trades`
- `POST /api/ghost/refresh` → L17000 → `capture_new_signals` + `check_outcomes`

These three endpoints **read from the legacy lean DB**, which was renamed in HM-AZ.2. Live file `data/ghost_trades.db` (zero-byte WAL, 32K SHM) still exists alongside the `.legacy_lean_*` snapshot, so they may or may not be functional — needs verification (out of BB.0 read-only scope).

### **Directive question (BB.4):**

The directive says *"Update `engine/ghost_trader.py` (post HM-AZ) to surface the new columns."* But:

1. `engine/ghost_trader.py` (singular) targets a different DB (`data/ghost_trades.db`) and its schema already has all four target columns.
2. The reader that actually consumes `trader.db.ghost_trades` post-HM-AZ is `engine/ghost_trades.py` (plural).

**Highest-likelihood reading:** BB.4 intends `engine/ghost_trades.py` (plural). The `get_ghost_stats()` function at L135 currently returns zeros for `would_have_won`, `avg_pnl_pct`, `best_ghost_pct`, `worst_ghost_pct` — explicit HM-AZ note says these are deferred until outcome columns exist. BB.2 supplies those columns. BB.4 swaps the zeros for live `SUM(CASE WHEN pnl_pct > 0 …)`, `AVG(pnl_pct)`, `MAX/MIN(pnl_pct)` aggregates. `get_ghost_trades()` also gains the four fields in its SELECT for list views.

**Captain confirmation needed before BB.4** — see §8.

---

## 6. Safe to insert?

**Yes** for BB.2 — `ALTER TABLE ADD COLUMN` is non-destructive in SQLite. Each ALTER is idempotent-guarded via `PRAGMA table_info` check per BB.2 directive. Existing 16 rows will have NULL for new columns (expected; no data corruption). Backup snapshot in BB.1 covers rollback.

---

## 7. Restart impact

**Yes** — service must restart for:

1. `scripts/ghost_advisor.py` BB.3 changes to take effect on next signal cycle (it's invoked by a separate runner, but the process running it needs reload).
2. `engine/ghost_trades.py` BB.4 reader changes — `dashboard/app.py` uses lazy import (`from engine.ghost_trades import …` inside the endpoint), but module is cached after first load. Restart clears.
3. `engine/ai_brain.py:989` call to `log_ghost_trade` (HOLD writer) — same module-cache rule.

Captain will run `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader` after push per HM-BB standing rule #7.

---

## 8. Open questions for Captain (block BB.1+ until resolved)

### Q1 — Confirm BB.4 reader target
Edit `engine/ghost_trades.py` (plural, HM-AZ canonical, `trader.db` reader) and treat the directive's "engine/ghost_trader.py" as a transposition typo? **Recommended: YES** — this is the reader actually wired to the migrated DB.

### Q2 — Confirm `entry_price` semantics
On a BUY INSERT, populate `entry_price = price` (duplicate of existing `price` column for read-shape symmetry with outcome stats)? Or leave NULL on BUY and only populate on SELL (i.e., `entry_price` = "the BUY price this SELL is closing against")? **Recommended: populate `entry_price = price` on BUY**, populate `exit_price = price` on SELL, populate `pnl_pct` on SELL only. Matches the most common ghost-trade read pattern.

### Q3 — Confirm close-hook approach (Option α vs β)
Per §4 — recommended Option α (SELL-row carries `exit_price` + `pnl_pct`, computed against most-recent matching BUY by `symbol+advisor`). BB.3 implementation cost: ~15 LOC. Option β (also UPDATE the BUY row): ~30 LOC.

### Q4 — `engine/ghost_trades.py::log_ghost_trade` confidence handling
This writer (called from `ai_brain.py:989`, HOLD-decision path) has `confidence` as a function param. Populate the new `confidence` column directly AND keep the `f"conf={confidence:.2f}: …"` prefix in `rationale` for the 16 legacy rows? Or strip the prefix going forward? **Recommended: populate column, keep prefix for one release cycle** (back-compat for legacy rows pre-BB.2).

### Q5 — `confidence` coverage on the trade-execution path
Per §3, only `read_super_trades` has a directly mappable `confidence` (= `success_prob`). The other 3 readers leave it NULL. Acceptable, or want a synthesized confidence (e.g. smart_money: `min(buyers/4, 1.0)`)? **Recommended: NULL where unmapped; document in commit**.

---

## 9. Phase sequencing (proposed)

After Captain answers Q1–Q5:

- **BB.1** — snapshot `trader.db` → `trader.db.pre-hm-bb-<TS>`, row-count verify.
- **BB.2** — `ALTER TABLE` for 4 cols, idempotent-guarded. No commit (DB-only).
- **BB.3** — patch `scripts/ghost_advisor.py` (both INSERTs) + `engine/ghost_trades.py::log_ghost_trade` (column population + remove HOLD-conf-rationale prefix per Q4). Compile check. One commit.
- **BB.4** — patch `engine/ghost_trades.py::get_ghost_stats` (replace zeros with live aggregates) + `get_ghost_trades` (add 4 fields to SELECT). Compile check. One commit.
- **BB.C** — static verify (anchors, compile, schema present, smoke SELECT).
- **BB.D** — closure report.

---

## 10. Files NOT changing (verified by directive scope)

- `engine/ghost_trader.py` (legacy lean, unless Captain rules Q1 the other way)
- `data/ghost_trades.db` and `data/ghost_trades.db.legacy_lean_2026-05-11` (untouched)
- `dashboard/app.py` (lazy imports + dict-passthrough, no schema-aware code on the trader.db path)
- `engine/scan_context.py:393` (uses dict keys, transparent to new fields)

---

**HALT.** Ready for Captain direction on Q1–Q5 before BB.1.

---

## HM-BB Closure (2026-05-11)

Captain green-lit all 5 recommendations. BB.1 → BB.4 → BB.C executed without halts.

### Commits staged (not pushed)

```
3d9f5ee feat(ghost): HM-BB.4 — surface new columns in ghost_trader reader
8fc1aeb feat(ghost): HM-BB.3 — populate entry_price/confidence/exit_price/pnl_pct in writers
```

(BB.2 is DB-only — no commit, per directive.)

### Backup

```
data/trader.db.pre-hm-bb-20260511_1343    269.4M    16 ghost_trades rows (parity OK)
```

### Schema diff

**Before (BB.0):** 12 columns — `id, ts, symbol, side, qty, price, fill_price, venue, advisor, signal_id, status, rationale`

**After (BB.2):** 16 columns — added `entry_price REAL`, `confidence REAL`, `exit_price REAL`, `pnl_pct REAL` at cid 12-15. Indexes unchanged. Existing 16 rows preserve all original column values; new columns are NULL on those rows (expected).

### Smoke test result

```
Reader can SELECT new columns: [(1, None, None, None, None)]
get_ghost_stats() → total_ghosts=16, would_have_won=0, ..., top_missed=[]
get_ghost_trades(limit=2) → both legacy rows return entry_price via COALESCE
  (e.g. id=16 TER: entry_price=365.985 from g.price fallback;
        confidence/exit_price/pnl_pct all null as expected for pre-BB.3 rows)
```

### Anchors landed (8 sites)

- `scripts/ghost_advisor.py` — L199/218, L253/263, L272/282, L362/364, L517/520
- `engine/ghost_trades.py` — L66/83 (BB.3), L123/137 (BB.4), L162/200 (BB.4)

### Restart needed

**Yes** — Captain will run `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader` after push. Until restart:
- `engine/ghost_trades.py` import is cached in the running dashboard/main process
- `scripts/ghost_advisor.py` is invoked by a separate runner, picks up changes on next signal cycle

### What lights up after first post-restart trade cycle

1. New ghost_advisor BUY rows: `entry_price` = price, `confidence` = success_prob (super_trades) or NULL.
2. New ghost_advisor SELL/TRIM rows: `entry_price` = avg_cost, `exit_price` = price, `pnl_pct` computed.
3. New HOLD writes via `engine/ai_brain.py:989 → log_ghost_trade`: `entry_price` + `confidence` columns populated (rationale prefix retained one cycle per Q4).
4. `/api/ghost-trades/stats` aggregates start incrementing as SELL rows land.
5. `/api/ghost-trades` list endpoint exposes the 4 new fields per row.

### Out-of-scope follow-ups (HM-BB.E candidates)

- Backfill of NULL `entry_price`/`confidence` on the 16 pre-migration rows. `entry_price` is recoverable from `g.price` (already handled at read via COALESCE; could be a one-shot UPDATE). `confidence` is irrecoverable for the 16 legacy rows (the 4 HOLD rows have it embedded in `rationale` as `conf=0.XX:`, parseable; the 12 ghost_advisor rows never had it captured).
- Strip the legacy `conf=…:` prefix from `log_ghost_trade` rationale after one release cycle (Q4 sunset clause).
- Synthesize confidence for `read_smart_money` (e.g. `min(buyers/4, 1.0)`) so smart-money signals contribute to confidence-weighted stats. Decision deferred.
- Dashboard surface — expose `confidence`, `exit_price`, `pnl_pct` in the ghost panel UI (HM-BA scope).
- `engine/ghost_trader.py` (singular legacy) reads `data/ghost_trades.db` which was renamed by HM-AZ.2 — three dashboard endpoints (`/api/ghost/scorecard`, `/api/ghost/trades`, `/api/ghost/refresh`) may now be 500-prone. Out of HM-BB scope; separate triage ticket.

### Phase ledger

| Phase | Outcome | Artifact |
|-------|---------|----------|
| BB.0  | Discovery report + 5 captain-questions | `data/scotty_hm_bb_report.md` |
| BB.1  | DB backup (parity OK) | `data/trader.db.pre-hm-bb-20260511_1343` |
| BB.2  | 4 ALTER TABLE ADD COLUMN (no commit per directive) | schema in trader.db |
| BB.3  | Writer wired (ghost_advisor + ghost_trades.log_ghost_trade) | commit `8fc1aeb` |
| BB.4  | Reader wired (ghost_trades.get_ghost_trades + get_ghost_stats) | commit `3d9f5ee` |
| BB.C  | Static verify: anchors + compile + schema + SELECT smoke | all PASS |
| BB.D  | This closure section | — |

Ready for Captain push + service restart.
