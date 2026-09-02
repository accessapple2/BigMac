# Relay — GEX freshness gate (fix 2) + staleness-marker extension (fix 1)
**2026-09-01, evening. Applying the two fixes proposed in the fossil-vs-live trace. Restore decision explicitly NOT actioned — see closing section.**

---

## Fix 2 — freshness-gate the Polygon/canonical override (the real fix)

**Root cause, restated plainly:** `engine/ready_room.py` and
`engine/dynamic_advisor.py` each independently ran "if the canonical
(Polygon) GEX read didn't error, trust it over the live Alpaca value" —
with no age check. A dead daily collector (HM-GEX-RETIRED, 07-21) still
returns a structurally valid, error-free row forever, so both kept
overwriting today's live data with a frozen one, unconditionally, since
07-21.

**Freshness, defined and visible.** `engine/canonical_gex.py` (the single
source-of-truth module both files already imported from) now carries:

```python
CANONICAL_GEX_MAX_AGE_DAYS = 1.0
```

Named, top-level, documented in place — not a magic number buried in an
`if` statement two call sites deep. Comment states the reasoning
explicitly: gamma is intraday regime data, a day old is already stale for
decisioning (your framing, verbatim, because it's the correct one — this
isn't a "keep it around for a week" kind of staleness).

**One shared gate, not two duplicated checks.** Also new in
`engine/canonical_gex.py`:
- `snapshot_age_days(as_of)` — the age-parsing primitive (handles both
  `' '`/`'T'` separators, returns `None` on missing/unparseable, never
  silently `0`).
- `canonical_gex_if_fresh(symbol)` — calls `canonical_gex()`, returns
  `None` if it errored, is missing an `_asof`, or is older than
  `CANONICAL_GEX_MAX_AGE_DAYS`; returns the dict otherwise.

Both `ready_room.py:506-531` and `dynamic_advisor.py:791-806` now call
`canonical_gex_if_fresh("SPY")` instead of the raw `canonical_gex()` +
inline `not error` check, and treat `None` exactly like they already
treated an explicit fetch error — fall through to the legacy Alpaca/
red_alert values untouched. No new "is it stale" logic lives in either
file anymore; there's one implementation to get right, not two to keep in
sync.

**The `mic_advisor.py` file you named doesn't exist** — closest match in
the repo is `dyna`**`mic`**`_advisor.py`, which is the file already named
earlier in your message, so I've treated "the duplicate" as the
`dynamic_advisor.py` copy of `ready_room.py`'s block (which is exactly
what it is — read both, confirmed straight-copy shape, same bug,
independently). Flagging the name mismatch here in case you meant a third
file I haven't found — nothing else in the repo imports `canonical_gex`
except `dashboard/app.py`, `engine/ready_room.py`, `engine/dynamic_advisor.py`,
`engine/gex_overlay.py` (a display-only reader, not an overlay-with-
fallback pattern), `engine/options_flow_gex.py`, and `engine/canonical_gex.py`
itself — grepped fresh to be sure.

**`war_room.py` deliberately left alone, and now says why in the code
itself** (not just this doc, so a future pass doesn't "fix" it into
brokenness):

```python
# HM-GEX-FRESHNESS-GATE-2026-09-01: intentionally NOT freshness-gated like
# ready_room.py/dynamic_advisor.py's canonical_gex overlay was. This is a
# different pipeline (engine.gamma_context, Polygon-direct, no flow_gex.db
# fossil in the loop) that already fails safe to "" the moment its live
# Polygon call errors (which it does today, HM-GEX-RETIRED) -- there is no
# "stale-but-error-free" row for it to silently serve, so no gate is needed
# here. Do not "fix" this by pointing it at canonical_gex_if_fresh() --
# that would trade a fail-safe empty string for a fallback to the same
# fossil source this gate exists to guard against elsewhere.
```

**`dashboard/app.py` consolidated too, not just extended.** Its
`_gex_age_days()` (added in `0e14e77`) is now a one-line delegator to
`engine.canonical_gex.snapshot_age_days` instead of its own copy of the
same parsing logic — a third instance would have existed otherwise. New
`_gex_is_stale()` reuses `CANONICAL_GEX_MAX_AGE_DAYS` instead of the
hardcoded `>= 1.0` that was duplicated at both `gex_all()` and
`gex_ticker()`; both now call the shared function.

### Verification

**Live, against real current data** (no mocking, ran directly against
today's actual `flow_gex.db`):
```
raw canonical_gex asof: 2026-07-21 20:05:20   error: None
age_days: 42.26
threshold: 1.0
canonical_gex_if_fresh result: None
```
Confirms the gate correctly rejects the real fossil row today — this is
the exact case that was silently passing before.

**Unit tests** (`tests/test_gex_freshness_gate.py`, 13 tests, all passing):
- `snapshot_age_days`: none/unparseable → `None`; fresh → ~0; the literal
  frozen `2026-07-21T13:05:00` → >40 days.
- `canonical_gex_if_fresh`: fresh → returns dict; stale → `None`; errored
  → `None`; missing `_asof` → `None`. `CANONICAL_GEX_MAX_AGE_DAYS == 1.0`
  asserted directly (not re-derived) so a future accidental edit fails
  loudly.
- **`ready_room.py` end-to-end** (`generate_ready_room_briefing`, mocked
  `gex_calculator.compute_gex_sync` for a known legacy profile, mocked
  `canonical_gex_if_fresh`, real tmp-path DB — never touches
  `trader.db`): fresh canonical → `spot_price`/`call_wall`/`put_wall`/
  `gamma_flip`/`max_gamma_strike` all take the canonical values. Stale
  (`canonical_gex_if_fresh` returns `None`, matching today's real state)
  → all five fields stay at the legacy Alpaca profile's values —
  **this is the exact regression test for the incident**: before this
  fix, this test would have failed (the fossil values would have won).
- **`dynamic_advisor.py` end-to-end** (`generate_advisory`, mocked
  `_gather()` and `canonical_gex_if_fresh`): fresh → the "Key levels"
  sentence in `market_read.summary` cites the canonical walls. Stale →
  cites `red_alert`'s own (live) walls instead.
- `dashboard/app.py`'s `_gex_age_days`/`_gex_is_stale` proven to delegate
  to the same canonical implementation (not a fourth copy).

**Verified `/api/ready-room/advisory` stops rendering July 21 numbers as
Troi's read**, per your explicit ask — done via the `dynamic_advisor.py`
end-to-end test above rather than a live curl, because the running
dashboard process (port 8080) is serving pre-fix bytecode and this fix is
deliberately not being deployed tonight (see close). The stale-path test
reproduces today's real condition (`canonical_gex_if_fresh` returns
`None` for real, confirmed live above) and asserts the advisory falls
through to live `red_alert` walls — which is the same code path
`/api/ready-room/advisory` runs in production. Will re-verify against the
live endpoint after the next restart if you want that extra check.

---

## Fix 1 (companion) — extend the staleness marker to the 3 missed endpoints

`0e14e77` covered `gex_all()`/`gex_ticker()` only. Added `age_days`/
`stale` (and `as_of` where missing) to:

- **`/api/gex-overlay/levels`** — checked closely as asked. Live
  `trader.log` sample: **147 hits vs 2 for `/api/market/gex` and 10 for
  `/api/gex-snapshot`** in the same window. Traced the consumer:
  `dashboard/static/index.html`'s SPY/QQQ King-Node/Gamma-Flip/Put-Wall/
  Call-Wall panel, `setInterval(loadGexOverlay, 900000)` — every 15 min,
  per open tab. The volume isn't a bug or a runaway loop; it's simply the
  longest-lived, most continuously-polled GEX consumer (a monitoring tab
  left open for hours), which is exactly why it was the most exposed —
  every 15 minutes for however many hours that tab's been up, it was
  rendering the frozen row as current with zero signal. Now carries the
  same fields as the other two.
  - **Found but NOT fixed, flagging separately, out of scope for tonight:**
    this endpoint returns `regime: "stable (above flip)" / "volatile
    (below flip)"`, but the consuming JS checks for
    `d.regime === 'positive_gamma' | 'negative_gamma'` — neither string
    ever matches, so the regime badge has been stuck at the neutral "— γ"
    state regardless of actual regime. Unrelated to staleness, a real
    bug, small fix, but touches frontend JS (needs the browser smoke test
    per the Frontend Ship Rule) — didn't fold it into tonight's
    backend-only change. Also noting in passing: that badge's color
    scheme is green/red (`#22c55e`/`#f87171`), which conflicts with the
    standing colorblind-UI rule (BLUE+AMBER only) — pre-existing, not
    something I introduced, flagging since I was already in the code.
- **`/api/gex-overlay/heatmap`** — same fields added, same pattern.
- **`/api/chart-data`**'s embedded `gex_levels` block — same fields added.
  No dedicated endpoint-level test (that endpoint's other sections make
  live Alpaca/DB calls unrelated to GEX; full invocation would be
  expensive and flaky for a one-field addition) — verified by code
  inspection, and it reuses the same `_gex_age_days`/`_gex_is_stale`
  functions already covered by unit tests elsewhere.

**Tests:** `tests/test_gex_staleness_marker.py` extended with 4 new tests
(fresh/stale for `gex_overlay_levels`, staleness fields present on
`gex_overlay_heatmap`, error path unchanged for both) — 23 tests total in
that file + `test_gex_freshness_gate.py` combined, all passing.

---

## Full verification

- `py_compile` clean on all 5 touched files (`dashboard/app.py`,
  `engine/canonical_gex.py`, `engine/ready_room.py`,
  `engine/dynamic_advisor.py`, `engine/war_room.py`).
- New/modified test files: 23 passed (13 freshness-gate + 10 staleness-marker).
- Full suite: 1200 passed, 2 skipped, 19 pre-existing failures — all
  confirmed unrelated (grepped: none reference `canonical_gex`,
  `ready_room`, `dynamic_advisor`, `war_room`, or `dashboard.app`).
  Spread across `bbkc_*`, `m5_allocator`, `ntfy_ipv6_sweep_*`,
  `riker_synthesis_*` (Riker was stood down per CLAUDE.md — these tests
  reference a retired module), `universe_filter`. Two more files fail to
  even collect (`test_holodeck_drawdown_sign.py` — missing `vectorbt`,
  the known `.venv` vs `.venv-backtest` isolation boundary;
  `test_riker_synthesis_lock_retry.py` — same retired-module issue).
  None of this is new; all pre-dates tonight's change.

**Deploy status:** committed, not restarted. Ships with the next natural
restart alongside `337407c` and `0e14e77`, per instruction.

---

## On the $29 Polygon restore — holding, reasoning recorded here

You're holding the restore decision; this is the note so it isn't
re-derived from scratch later.

Before tonight's fix, restoring Polygon would have been closer to a
*repair* than a *choice* — the canonical path was silently winning over
live data by construction (any non-error read beat Alpaca, and a dead
collector never errors), so bringing Polygon back would have been fixing
a bug as a side effect of paying for it. That's not a clean way to make a
$29/mo decision.

**Now that the override is freshness-gated, restoring Polygon becomes
what it always should have been: a choice about wall/flip precision, not
a repair.** With the gate in place, a restored Polygon feed would:
- Start winning the overlay again in `ready_room.py`/`dynamic_advisor.py`
  (Troi's read) and the dashboard's `/api/market/gex` family, genuinely
  fresh, genuinely more precise than Alpaca (the codebase's own comment
  measures the gap: put_wall 740 vs 750, flip 749 vs 755).
- **Still not touch** `risk_manager.py`'s trade-sizing gate,
  `kirk_advisory.py`, or `providers/base.py`'s fleet-wide LLM prompt
  injection — none of those three have any code path that checks for
  Polygon/canonical data at all; they call the Alpaca pipeline directly,
  unconditionally, restore or no restore.

So the decision in front of you, once this ships, is genuinely just:
*is the wall/flip precision gain in the Ready-Room/dashboard corner of
the system worth $29/mo*, with the freshness/staleness failure mode this
whole investigation was about now closed either way (fresh Polygon data
displays and is reasoned over as fresh; stale or absent Polygon data
correctly falls through to live Alpaca everywhere, dashboard included).
Restoring doesn't unify the two sources — that was true before tonight
and stays true after — it only makes the Polygon side trustworthy again
when it's there.
