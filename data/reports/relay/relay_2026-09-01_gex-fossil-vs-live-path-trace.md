# Relay — GEX fossil-vs-live path trace (investigate only, no fix applied)
**2026-09-01, evening. Follow-up on the options-data correction. Report only per instruction.**

---

## 1. What does `/api/market/gex` actually read from? (not a query bug)

Traced the full call chain: `gex_all()` → `_canonical_gex_cached()` →
`_canonical_gex()` (`dashboard/app.py:18491`) → `engine.canonical_gex.
canonical_gex()` (`engine/canonical_gex.py:69`). That function has a
documented 3-tier priority, and I read all three tiers directly:

1. **In-process intraday cache** (`engine.options_flow_gex._LATEST`,
   refreshed every ~15min RTH by `main.py`'s `run_gex_snapshot_refresh`).
   This tier calls Polygon live (`fetch_chain`/`fetch_spot` in
   `options_flow_gex.py`, confirmed 100% Polygon, no fallback provider).
   Every refresh cycle 403s, `compute_gex` returns an `{"error": ...}`
   dict, and `canonical_gex()`'s own guard (`not d["gex"].get("error")`)
   correctly skips this tier — it never serves a bad/stale in-process
   value. Fails safe.
2. **Latest row in `data/flow_gex.db`'s `gex_snapshots` table** — a
   **separate SQLite file** from `data/trader.db`'s `gex_snapshots` (same
   table name, different file, different schema, different writer). This
   file's last real row is 2026-07-21 13:05, because the daily collector
   that writes it (`scripts/hm_gex_daily_collect.py` → `options_flow_gex.
   collect()`) has been 403'ing since the Polygon subscription cancelled,
   and its crontab line was formally retired 2026-08-30 (HM-GEX-RETIRED).
   This row is **not collapsed** (call_wall ≠ put_wall ≠ king_node), so it
   passes `canonical_gex()`'s validity check and is returned immediately.
3. **Live compute** — same Polygon path as tier 1, same 403. Never
   reached today, because tier 2 already returns successfully.

**Answer: not a query/join bug.** `/api/market/gex` is correctly reading
its designed source — a distinct, durable SQLite file
(`data/flow_gex.db`) whose writer has been dead since 2026-07-21. The
confusing part is purely the identical table name (`gex_snapshots`) across
two unrelated files — there's no cross-contamination, no wrong-table read,
just two same-named tables in different files that happen to diverge.

---

## 2. Two-column consumer list

Traced every caller of both the fossil path (`engine.canonical_gex` /
`data/flow_gex.db`) and the live path (`gex_calculator.py` /
`data/trader.db`'s `gex_snapshots`, `source='alpaca'`) across `engine/`
and `dashboard/`.

### READS-FOSSIL (`engine.canonical_gex` → `data/flow_gex.db`, frozen 07-21)

| Consumer | What it is | Staleness-aware? |
|---|---|---|
| `dashboard/app.py` `/api/market/gex` (`gex_all`) | Dashboard Gamma Map, multi-ticker | **Yes** — fixed `0e14e77` |
| `dashboard/app.py` `/api/market/gex/{ticker}` (`gex_ticker`) | Dashboard, single-ticker | **Yes** — fixed `0e14e77` |
| `dashboard/app.py` `/api/gex-overlay/levels` | GEX overlay panel | **No** — missed by `0e14e77` |
| `dashboard/app.py` `/api/gex-overlay/heatmap` | Per-strike heatmap panel | **No** — missed by `0e14e77` |
| `dashboard/app.py` `/api/chart-data` (embeds `gex_levels` inline) | Chart-data panel | **No** — no staleness field at all |
| `dashboard/app.py` `/api/gex-snapshot` (`gex_latest`) | "Observation-only" intraday-fresh endpoint | **No** field, but see note below — usually hits tier 1/3 not tier 2 |
| `engine/ready_room.py` (`generate_ready_room_briefing`) | SPY session briefing, "Troi's read" per its own comment | **No** — see finding below |
| `engine/dynamic_advisor.py` (`generate_advisory`, `/api/ready-room/advisory`) | Human-facing advisory panel | **No** — see finding below |

### READS-LIVE (`gex_calculator.py` → `data/trader.db`'s `gex_snapshots`, Alpaca)

| Consumer | What it is | Gates a real decision? |
|---|---|---|
| `engine/risk_manager.py:797-813` | Position-size gate: 25% cut on negative GEX, hard block near call wall | **Yes — real trade gate** |
| `engine/ready_room.py` (legacy primary, before canonical overlay) | SPY briefing base values | Overridden by fossil, see below |
| `engine/kirk_advisory.py:_get_gex_context()` | Kirk's advisory context | Display |
| `engine/providers/base.py:919-927` (`build_alpaca_gex_prompt_section`) | **Fleet-wide LLM prompt injection** — every AI player's context block gets a live "Alpaca GEX" section | Feeds every agent's LLM prompt |
| `dashboard/app.py` `/api/gex/{symbol}/history` (`gex_alpaca_history`) | Historical GEX display | Display |
| `dashboard/app.py::_build_computer_context()` → `/api/computer/chat` | Captain Archer chat LLM prompt | Feeds an LLM prompt |

### Separate, live-or-nothing third pipeline (not implicated either way)

`engine/gex_scanner.py` (CBOE-direct, on-demand fetch, own short-TTL
cache, **no fallback to stale data** — returns `None` on failure, same
fail-safe shape as the intraday-cache tier above). Consumers:
`engine/spy_wall_strategy.py` (walls only — via `crew_scanner.py`, a real
scan/signal path, but confirmed its spot comes from live
`market_data.get_stock_price` and its VIX from a non-GEX briefing field,
so it's not exposed to the fossil issue), `engine/dayblade.py` (LLM prompt
injection, but `dayblade-sulu` is `halt_mode='exit_only'` per CLAUDE.md —
no new positions), `engine/dte_scanner.py`, `engine/gamma_environment.py`,
`engine/scan_context.py`. None of these touch the fossil `flow_gex.db`
path — flagging for completeness, not because it's part of this
investigation's disagreement.

---

## 3. Does anything reason from the July 21 numbers as current?

**Correction to what I told you last turn.** I said "nothing live gates
on this... it's purely a display-layer fix" — that was wrong for one
specific path, and I want to be precise about where.

**`engine/ready_room.py:506-520`** (the canonical-overlay block, called
every 30min market-hours via `run_ready_room`) unconditionally overwrites
**`spot`, `call_wall`, `put_wall`, `gamma_flip`, `max_gamma`
(king_node), and `total_gex`** with the fossil `flow_gex.db` row, whenever
`canonical_gex()` doesn't return an `error` key — which it never does
today, since the fossil row is valid and non-collapsed. There is no
staleness check anywhere in this block. Read the exact code:

```python
if _c and not _c.get("error"):
    if _c.get("spot") is not None:       spot       = _c["spot"]
    if _c.get("call_wall") is not None:  call_wall  = _c["call_wall"]
    if _c.get("put_wall") is not None:   put_wall   = _c["put_wall"]
    if _c.get("gamma_flip") is not None: gamma_flip = _c["gamma_flip"]
    if _c.get("king_node") is not None:  max_gamma  = _c["king_node"]
    if _c.get("total_gex") is not None:  total_gex  = _c["total_gex"]
```

That means **SPY's spot price in today's Ready Room briefing is silently
being replaced with whatever SPY closed at around 2026-07-21** — not just
the gamma levels, the underlying price itself. This briefing is saved to
`ready_room_briefings`, served at `/api/ready-room/condition`, and read by
`engine/dynamic_advisor.py::_gather()` as `cond` — which then **re-applies
the exact same fossil override a second time**, directly, at
`dynamic_advisor.py:791-804` (identical shape, identical lack of a
staleness gate). The result renders to a human at `/api/ready-room/advisory`
as "Troi's read" (the panel's own branding, per `ready_room.py`'s inline
comment) — a human reading that panel today has no way to know the walls,
flip, and possibly the SPY price itself are six weeks old. This is not an
LLM prompt (confirmed — grepped `engine/providers/` and `war_room.py`,
neither `ready_room.py` nor `dynamic_advisor.py` feeds any AI-player
prompt), but it is a real "stale number rendered as current" case, exactly
the shape your question was worried about.

**Everything else, checked and confirmed clean:**
- `engine/risk_manager.py`'s trade-sizing gate — Alpaca-only, never
  touches `canonical_gex`/`flow_gex.db`. Unaffected.
- `engine/kirk_advisory.py`, `engine/providers/base.py`'s fleet-wide LLM
  prompt injection, `dashboard/app.py::_build_computer_context()` (Archer
  chat) — all Alpaca-only (`gex_calculator.py`), unaffected.
- `engine/war_room.py::build_gamma_block()` (the OTHER "Troi" touchpoint I
  cited last turn) — that one uses `gamma_context.py` (Polygon-direct,
  different pipeline again), and does fail safe to `""` since it's
  actively 403'ing right now. My prior statement was correct **for that
  specific path** — I'd conflated it with the `ready_room.py`/
  `dynamic_advisor.py` path above, which is a different pipeline with a
  different (and real) gap.
- `engine/spy_wall_strategy.py` / `engine/crew_scanner.py` — a genuine
  live scan/signal path, but its walls come from live CBOE
  (`gex_scanner.py`) and its spot from live `market_data`, not from the
  fossil canonical path. Clean.
- `engine/pattern_matcher.py` (`/api/ready-room/similar-days`) — does read
  the tainted `ready_room_briefings` table (so its "similar day" spot/wall
  fingerprints are polluted by the same fossil override), but this
  appears to be a display/analytics feature, not an order-generating
  path. Not traced further than that — flagging in case it matters more
  than it looks.

**Three dashboard endpoints also serve the fossil data with zero
staleness signal**, missed by the `0e14e77` fix because I only touched
`gex_all()`/`gex_ticker()`: `/api/gex-overlay/levels`,
`/api/gex-overlay/heatmap`, and `/api/chart-data`'s embedded `gex_levels`
block.

---

## 4. Are the two sources even comparable?

**Structurally yes, numerically no — and they will keep disagreeing even
after a Polygon restore, in most of the system.**

Read both compute functions directly:
- `engine/options_flow_gex.py::compute_gex()` (Polygon) — gamma × OI ×
  contract-multiplier × spot² × 0.01 per contract, aggregated per strike,
  ±STRIKE_BAND of spot, expiries ≤60 DTE, wall = strongest same-sign GEX
  strike, gamma flip via a Black-Scholes zero-cross scan across ±15% of
  spot.
- `gex_calculator.py::compute()` (Alpaca) — same core formula
  (`gamma × OI × ...`) using **Alpaca's own live per-contract Greeks**
  (not self-computed BS), same 0-60 DTE window, ±15% of spot for display,
  put wall = most-negative put GEX below spot, call wall = most-positive
  call GEX above spot, flip via `_find_zero_gamma` (didn't trace this
  one's internals in depth, but same dealer-GEX concept).

Same methodology family, **different options-chain data providers**
(Polygon vs Alpaca) feeding different Greeks into the same formula — they
will not produce identical numbers even on the same day. This isn't
speculation: `ready_room.py`'s own inline comment already documents a
measured, live discrepancy ("put_wall 740 / flip 749 vs canonical
750 / 755").

**If you restore the $29/mo Polygon tier:** the canonical (`flow_gex.db`)
path starts updating again, and the ONE place in the codebase that's
actually wired to prefer it (`ready_room.py`/`dynamic_advisor.py`'s
overlay block, plus the dashboard's `/api/market/gex` family) would start
showing fresh, Polygon-sourced numbers again — genuinely fixed.

**But it does NOT touch `risk_manager.py`, `kirk_advisory.py`,
`providers/base.py`'s fleet-wide prompt injection, or `/api/gex/{symbol}/
history`.** None of those four have any code path that even checks
whether Polygon/canonical data exists — they call `gex_calculator.py`
directly, unconditionally, always. So restoring Polygon does **not**
create one unified GEX truth. It creates a permanent split: the
Ready-Room/dashboard-display corner of the system prefers Polygon
(fresher, more precise), while the trade-gating and fleet-prompt corner
of the system stays on Alpaca forever, by construction, with the two
disagreeing by the amount `ready_room.py`'s comment already measured
(single-digit dollars on SPY walls, in the one case checked). That's a
real, standing "two live sources" situation either way — restoring
Polygon just moves it from "one side is frozen data pretending to be
live" to "both sides are live and quietly disagree," which is a
different, smaller problem, but not zero.

---

## Is `/api/market/gex` a dead path nobody reads?

**No.** It's read by the dashboard's Gamma Map (the thing you originally
flagged), and the wider fossil-serving code (`_canonical_gex`/
`_canonical_gex_cached`) is read by at least 6 dashboard endpoints plus
two backend advisory paths, one of which (`ready_room.py`/
`dynamic_advisor.py`) actively reasons over the fossil numbers as current
today. Not a candidate for deletion.

## Proposed fixes (not applied — your call)

1. **Extend the `0e14e77` staleness marker** to the three endpoints it
   missed: `/api/gex-overlay/levels`, `/api/gex-overlay/heatmap`,
   `/api/chart-data`'s embedded `gex_levels`. Same `_gex_age_days()`
   helper, same pattern, small.
2. **Gate `ready_room.py`'s and `dynamic_advisor.py`'s canonical-overlay
   blocks on freshness** — e.g. skip the override entirely (fall back to
   the live Alpaca legacy values, which is what the code already does on
   an explicit `error`) when `_gex_age_days(_c.get("_asof")) >= 1.0`,
   reusing the same threshold and helper as the dashboard fix. This is
   the fix for the actual "reasoning from stale data" finding — two files,
   still backend-only, no restart-sensitive frontend changes.
3. **The bigger decision, not a code fix:** either restore Polygon (buys
   back precision in the Ready-Room/dashboard corner specifically) or
   formally retire the canonical/`flow_gex.db` path everywhere it's still
   wired (items 1-2 above plus `ready_room.py`/`dynamic_advisor.py`) and
   let Alpaca be the sole number displayed and reasoned over — removing
   the two-source situation entirely instead of managing it. Both are
   legitimate; this doc doesn't recommend one over the other.

Nothing in this list has been applied. Awaiting direction on 1-3 before
touching any of it.
