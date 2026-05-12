# HM-BL.E2 — Stale Positions Audit (DISCOVERY ONLY)

**Author:** Scotty (Opus 4.7)
**Date:** 2026-05-12
**Status:** DISCOVERY — no cleanup applied; proposal for follow-up epic

## TL;DR

**Zero stale 0-qty rows in `positions` table today.** The DELETE-on-full-SELL pattern is correctly implemented across all close paths. The dashboard queries defensively with `WHERE qty > 0` regardless, so even a future regression that left 0-qty rows would not corrupt the UI. **No urgent action.** Optional canary monitor proposed.

## Current state

```
SELECT COUNT(*) FROM positions WHERE qty = 0 OR qty IS NULL;  -- 0
SELECT COUNT(*) FROM positions WHERE qty > 0;                 -- 45
```

No rows with `qty=0` or `qty IS NULL` exist as of audit (2026-05-12 13:33 MST).

## How cleanup currently happens (verified)

### Full-close paths — DELETE

| Site | Path | Pattern |
|---|---|---|
| `engine/paper_trader.py:1170` | Stock full-sell | `DELETE FROM positions WHERE player_id=? AND symbol=? AND asset_type='stock'` |
| `engine/paper_trader.py:1175` | Option full-sell | `DELETE … AND option_type=?` |
| `engine/paper_trader.py:1301` | Stock partial that drains | Same DELETE — runs when remaining qty would be 0 |
| `engine/paper_trader.py:1306` | Option partial that drains | Same DELETE |
| `engine/dayblade.py:465` | 0DTE close | DELETE |
| `engine/metals_tracker.py:435` | Metals close | DELETE |
| `engine/webull_client.py:301` | Webull sync full refresh | DELETE all webull rows before re-insert |
| `engine/season_manager.py:163,266` | Season reset | DELETE all non-broker rows |
| `scripts/close_player_positions.py:95` | Manual close tool | DELETE by id |

### Partial-close paths — UPDATE qty

| Site | Path | Pattern |
|---|---|---|
| `engine/paper_trader.py:1313` | Stock partial | `UPDATE positions SET qty=? …` (qty=remaining) |
| `engine/paper_trader.py:1318` | Option partial | Same |
| `engine/dayblade.py:470` | 0DTE partial | Same |
| `engine/metals_tracker.py:438` | Metals partial | Same |

**The partial-UPDATE path is the only theoretical vector to leave a `qty=0` row** — if `remaining` is computed as 0 but the code doesn't fall into the DELETE branch. Reviewing `paper_trader.py:1296-1322`, the branching is `if abs(remaining) < threshold → DELETE; else → UPDATE qty=remaining`, so a tiny-but-nonzero remainder could in theory leave `qty=0.00001`. Today's DB shows zero such rows, so either the threshold logic is sound or fleet sizing avoids the edge.

## Why this is not currently a problem

Every consumer query in `dashboard/app.py` filters `WHERE qty > 0`:

- L2558, L4551, L4588, L4637, L7303, L12443, L14058, L14119, L14134

So even if a 0-qty row appeared, it would not corrupt fleet rosters, portfolio totals, the leaderboard, or the symbols-watched list. The downstream cost would be limited to:

- Slightly larger `positions` table → negligible at fleet scale
- `UNIQUE(player_id, symbol, asset_type, option_type, strike_price, expiry_date)` constraint preventing legitimate re-buy of the same symbol+leg without manual cleanup → real but rare

## Proposed cleanup approach (deferred — open for Captain)

### Option A — Canary monitor (recommended, low effort)

Add a query to `scripts/health_check.py`:
```python
stale = c.execute("SELECT COUNT(*) FROM positions WHERE qty = 0 OR qty IS NULL").fetchone()[0]
if stale > 0:
    ntfy.send(f"positions table has {stale} stale 0-qty rows", topic="ollietrades-admin")
```
Catches future regressions without preemptive deletion. Zero current value, no behavior change.

### Option B — Defensive UPDATE-to-DELETE rewrite

Replace the `UPDATE positions SET qty=?` blocks at paper_trader L1313/L1318, dayblade L470, metals_tracker L438 with `if remaining <= epsilon: DELETE; else: UPDATE`. Hardens the only theoretical zero-leak vector. ~20 lines.

### Option C — One-shot maintenance script

Already exists implicitly: `DELETE FROM positions WHERE qty = 0 OR qty IS NULL;` is safe to run any time. Could be added to the season-rollover script if a real audit ever finds drift.

## Recommendation

**Option A only**, as part of the existing healthcheck cadence. Options B and C are pre-emptive cleanups of a vector that is not currently leaking. Per CLAUDE.md "Don't design for hypothetical future requirements" — defer until evidence of drift.

## Captain Q for follow-up epic (HM-BL.E2 successor)

1. Approve Option A canary monitor only, or also Option B defensive rewrite?
2. If A: should `ollietrades-admin` NTFY be daily (with floor of 0 → no NTFY) or only on `> 0`?
3. Threshold for healthcheck noise: NTFY at `stale > 0` or `stale > N` (e.g., N=5)?

## Cross-references

- HM-BL closure (root): prior thread that surfaced the concern
- Schema: positions table has UNIQUE constraint; zero-qty row would block re-buy without DELETE
- HM-BL.E1: related sibling (if it exists in archive) — not located in this audit
