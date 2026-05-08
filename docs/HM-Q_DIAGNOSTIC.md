# HM-Q Diagnostic — `signals.execution_status` vs `signals.halted_emit`

**Author:** Scotty 2.9 (Phase 4)
**Date:** 2026-05-08
**Source ticket:** HM-C audit follow-up — overlap between two newer columns
on `signals` flagged as potentially redundant
**Hard rule:** **No fix applied.** Diagnostic + recommendation only.

---

## TL;DR — recommendation

**KEEP-AS-IS.** The two columns are **not redundant** — they answer
different questions about a signal's life. Recommendation is to add
documentation (`docs/SCHEMA_NOTES.md`) and a single unit test asserting
the invariant `halted_emit=1 ⇒ execution_status IN ('SKIPPED','REJECTED')`.
A *future* migration that replaces `halted_emit` with a richer
"player halt-state at emit time" view is possible but premium-cost
relative to the current correctness — defer.

---

## 1. Schema

```
CREATE TABLE signals (
  id                INTEGER PRIMARY KEY,
  player_id         TEXT NOT NULL REFERENCES ai_players(id),
  symbol            TEXT NOT NULL,
  signal            TEXT NOT NULL,
  confidence        REAL,
  reasoning         TEXT,
  asset_type        TEXT DEFAULT 'stock',
  option_type       TEXT,
  acted_on          INTEGER DEFAULT 0,
  created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  ...
  execution_status  TEXT     DEFAULT 'PENDING',   -- column 13
  rejection_reason  TEXT     DEFAULT NULL,
  halted_emit       INTEGER  DEFAULT 0            -- column 15
);
CREATE INDEX idx_signals_status ON signals(execution_status);
```

---

## 2. Writers

### `execution_status` — actively written from `engine/paper_trader.py`

| Writer site | Sets value | Trigger |
|---|---|---|
| `paper_trader.py:1051` | `'EXECUTED'` if `route_mode='trading'`, else `'SIMULATED'` | Buy fill |
| `paper_trader.py:1244` | Same shape | Sell fill |
| `paper_trader.py:2337` | Same shape | Spread fill |
| `paper_trader.py:442` | `'LOG_ONLY'` | Tracking-only routes (Enterprise Computer) |
| `paper_trader.py:1943` | `INSERT INTO signals (..., execution_status)` (initial PENDING) | Signal save |
| `paper_trader.py:1964` | `UPDATE signals SET execution_status=?, rejection_reason=? WHERE rowid=?` | Result writeback |

5+ active writer sites in `engine/paper_trader.py`. The column is the
"trade lifecycle status" of the signal.

### `halted_emit` — **no active writer in the runtime**

```
$ grep -rn "halted_emit\s*=" --include="*.py" .
engine/halt_gate.py:105:  HALTED_EMIT_FILTER = "halted_emit = 0"   # filter, not write
```

The column is **historically backfilled** (per `halt_gate.py:98-100`):

> `watchlist_signals` were backfilled with `halted_emit=1` by HM-C fix #1.
> This constant is the single source of truth for the read-side filter so
> that a future migration (e.g. when `is_halted`/`halted_emit` is replaced
> by a `halt_mode` join) only has to change one line.

So `halted_emit` is a **frozen-in-time provenance flag**: at backfill,
rows from players whose `halt_mode != 'active'` at emit time were
marked `halted_emit=1`. It is not updated in the live runtime.

---

## 3. Readers

### `execution_status` — read in 4 distinct surfaces
- `dashboard/app.py:3127+3133` — feeds the Signals dashboard panel
  (`s.execution_status, s.rejection_reason`)
- `engine/ai_brain.py:1306` — `_signal_status = result.get("execution_status", "EXECUTED")`
- `engine/paper_trader.py` — internal handoff
- `setup_db.py` indexed at create time

### `halted_emit` — read only via `engine/halt_gate.py`
- `HALTED_EMIT_FILTER = "halted_emit = 0"` — single source of truth
- `with_halted_filter(where_clause)` helper — composed into scoring /
  calibration / leaderboard read paths
- The comment explicitly says: "Use ONLY in scoring/calibration/
  leaderboard read paths — not in raw signal-feed display panels,
  diagnostic counts, or per-player forensic views."

---

## 4. Distinct values + counts

```sql
SELECT execution_status, halted_emit, COUNT(*) FROM signals GROUP BY 1,2;
```

| execution_status | halted_emit |     n |
|------------------|------------:|------:|
| EXPIRED          |           0 | 42,626|
| SKIPPED          |           0 | 11,525|
| REJECTED         |           0 |  9,275|
| SKIPPED          |           1 |  1,064|
| PENDING          |           0 |    317|
| SIMULATED        |           0 |    102|
| REJECTED         |           1 |     79|
| EXECUTED         |           0 |     17|
| | | **65,005** |

**Total `halted_emit=1` rows: 1,143** — all of them have
`execution_status IN ('SKIPPED','REJECTED')`. **Zero** have EXECUTED,
SIMULATED, EXPIRED, PENDING, or LOG_ONLY. That's the invariant a unit
test should encode.

**Coverage:** 1,143 / 65,005 ≈ 1.76% of signals are halted-emit
artifacts. The HM-C backfill covered the historical exposure window.

---

## 5. Are they redundant? complementary? in conflict?

### Complementary, not redundant.

They answer different questions about a signal's life:

| Question | Answered by |
|---|---|
| Did the trade fire? (lifecycle) | `execution_status` |
| Was the player halted when this was emitted? (provenance) | `halted_emit` |

A signal can be `SKIPPED` for many reasons — low conviction, off-hours,
position limits, kill switch, halt — and only the *halt* reason is what
`halted_emit=1` flags. The cardinality data above shows this clearly:
**11,525 of 12,589 SKIPPED rows are halted_emit=0** (i.e. skipped for a
non-halt reason).

### No conflict observed.

The 17 EXECUTED rows all have `halted_emit=0` — consistent with "halted
players don't fire trades." If any EXECUTED row had `halted_emit=1` it
would be a real bug; none do.

### One quirk worth noting

`acted_on` is the older lifecycle column (`acted_on INTEGER DEFAULT 0`).
It survives in the schema. Its writer/reader inventory wasn't part of
the HM-Q ticket but probably overlaps with `execution_status` —
candidate for a separate diagnostic. Not in scope here.

---

## 6. Recommendation — **KEEP-AS-IS** + minor doc additions

### Action 1 — add `docs/SCHEMA_NOTES.md` (or expand existing)

```
## signals table

execution_status (TEXT, default 'PENDING')
    Forward-looking trade-lifecycle status set by paper_trader.py
    Values: PENDING, EXECUTED, SIMULATED, LOG_ONLY, SKIPPED, REJECTED, EXPIRED
    Read by: dashboard/app.py:3127+, engine/ai_brain.py:1306

halted_emit (INTEGER, default 0)
    Frozen provenance flag: was player_id's halt_mode != 'active' at
    emit time? Backfilled by HM-C fix #1; not updated in the runtime.
    Read only via engine/halt_gate.py::HALTED_EMIT_FILTER for scoring
    and calibration read paths.
    Invariant: halted_emit=1 → execution_status IN ('SKIPPED','REJECTED')
```

### Action 2 — add one unit test

```python
# tests/test_signals_invariants.py
def test_halted_emit_implies_no_fill():
    rows = sqlite3.connect("data/trader.db").execute(
        "SELECT execution_status, COUNT(*) FROM signals "
        "WHERE halted_emit=1 GROUP BY 1"
    ).fetchall()
    statuses = {r[0] for r in rows}
    assert statuses <= {"SKIPPED", "REJECTED"}, \
        f"halted_emit=1 leaked into {statuses - {'SKIPPED','REJECTED'}}"
```

### Action 3 (deferred) — replace `halted_emit` with a join

Once `ai_players_halt_history` (or similar audit table) exists and
captures every halt transition with timestamps, the read-time filter
becomes:

```sql
... WHERE NOT EXISTS (
    SELECT 1 FROM ai_players_halt_history h
    WHERE h.player_id = s.player_id
      AND h.halted_at <= s.created_at
      AND (h.unhalted_at IS NULL OR h.unhalted_at > s.created_at)
      AND h.halt_mode != 'active'
)
```

…and `halted_emit` becomes redundant. **Do not do this now** — it
requires a halt-history table that doesn't exist (CLAUDE.md confirms
"there is no schema default and no trigger" for halt timestamps;
HM-F investigation 2026-05-04 found no programmatic halt paths).
Building the history capture is its own multi-week project.

---

## 7. Halt condition

**No fix applied.** Document only. The two columns continue to coexist.
The `halt_gate.py::HALTED_EMIT_FILTER` constant remains the single
source of truth for read-side halt-emit exclusion until a future
sprint.
