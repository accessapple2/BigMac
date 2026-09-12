# `/api/bridge/facts` — a single source of truth for cross-panel display facts

**Status: SPEC ONLY, NOT BUILT.** Written 2026-09-12 per the Admiral's
directive, Phase 3 of the Bridge correctness pass. Do not implement
without explicit go-ahead — the Admiral reads this first.

## The problem, with this session's own evidence

Phase 1 and Phase 2 of this pass (see the two companion relay docs,
`relay_2026-09-12_bridge_correctness_phase1_trace.md` and
`..._phase2_fixes.md`) found the same shape of bug five separate times:
a fact that's computed or fetched independently by 2-4 different call
sites, each with its own idea of freshness, definition, or field name,
with no shared "as of" timestamp a panel could use to know it's showing
something stale or simply different from its neighbor.

| Fact | Computed here | Computed here too | Genuinely the same thing? |
|---|---|---|---|
| Market regime | `engine/regime_detector.py` (50/200-day MA golden-cross, Riker's only source) | `engine/regime_ma.py` (8/21-day MA cross, feeds `regime_router.py`'s real strategy-fit gate) | **No** — different timeframes, both legitimate, never labeled as such |
| Season number | Was hardcoded "Season 5" in 3 places (`index.html` glance row, `/api/v1/docs` footer, Archer chat context) while `bridge-v2.html` read it dynamically since 2026-08-29 | `engine/season_manager.get_current_season()`, `/api/fleet/pnl`'s `current_season`, `/api/arena/leaderboard`'s `current_season` | Yes, but 3 call sites didn't know a shared source already existed |
| Agent counts | `/api/systems-status`'s `ai_players GROUP BY halt_mode` (82 total, matches live DB) | `/api/arena/leaderboard`'s curated ~11-row named-crew subset | **No** — different definitions (full roster vs. curated leaderboard), never labeled as such |
| Sector performance | `/api/sectors/heatmap` (Finviz -> Yahoo -> stale-disk -> 0.0, was short-circuiting on a flat-zero Finviz response) | `/api/ready-room/briefing`'s cached LLM narrative (real numbers baked into text from whenever it last generated) | Same underlying fact, two different "as of" times, no comparison possible |
| Metals P&L | `/api/metals/portfolio`'s `return_pct` (unrealized vs. cost basis) | The same response's `spot_prices.GOLD.change_pct` (today's spot move) | **No** — different metrics, same panel family, no label distinguishing them |

The doctrine this violates is the one already named in this repo for a
different context (`docs/DOCTRINE.md`'s "coverage that looks complete but
silently excludes what matters"): a panel that shows a confident number
with no visible caveat is worse than one that visibly says "I don't
know" — the reader has no way to tell a trustworthy number from a stale
or mismatched one just by looking at it. Every bug in the table above was
invisible until someone (the Admiral, this session) went and looked at
two panels side by side.

## Design

### The endpoint

`GET /api/bridge/facts` returns a flat, versioned map of named facts.
Every fact carries the same envelope:

```json
{
  "facts": {
    "regime.ma_cross": {
      "value": "BEAR_CROSS",
      "as_of": "2026-09-12T14:10:39Z",
      "source": "engine.regime_ma.detect_ma_cross_regime",
      "stale_after_s": 300,
      "stale": false,
      "label": "8/21-day MA cross (drives real strategy-fit gating)"
    },
    "regime.golden_cross": {
      "value": "BULL_TREND",
      "as_of": "2026-09-12T14:10:38Z",
      "source": "engine.regime_detector.detect_regime",
      "stale_after_s": 300,
      "stale": false,
      "label": "50/200-day MA golden cross (long-term trend, narrative only)"
    },
    "season.current": {
      "value": 8,
      "as_of": "2026-09-12T14:00:00Z",
      "source": "engine.season_manager.get_current_season",
      "stale_after_s": 3600,
      "stale": false,
      "label": "Current season"
    },
    "fleet.agent_count.full_roster": {
      "value": {"active": 8, "exit_only": 3, "full": 71, "total": 82},
      "as_of": "...", "source": "ai_players GROUP BY halt_mode", ...
    },
    "fleet.agent_count.leaderboard_roster": {
      "value": 11,
      "as_of": "...", "source": "/api/arena/leaderboard curated subset", ...
    }
  },
  "generated_at": "2026-09-12T14:10:40Z"
}
```

Every fact is independently cacheable and independently stale-able — a
caller can request a subset (`?keys=regime.ma_cross,season.current`) so a
light panel isn't paying for the whole bundle every poll.

### Staleness is a first-class property, not a per-panel afterthought

Each fact's `stale_after_s` is defined once, next to the fact's own
computation (not re-invented per panel, which is how `_trades_today()`'s
UTC/local mismatch and the sector-heatmap zero-fill both happened — two
different pieces of "how fresh is fresh enough" logic, in two different
files, that nobody could audit together). `stale: true` is computed
server-side at request time (`now - as_of > stale_after_s`), so the
frontend never has to know a threshold value at all — it just greys out
on `stale: true`, same rule everywhere.

### Two genuinely-different facts don't get merged into one

The regime and metals rows above are NOT a bug to unify away — they're
two real, different numbers that happen to share a vague English name
("regime", "the metals number"). The fix isn't to pick a winner; it's to
give each a distinct, labeled key (`regime.ma_cross` vs.
`regime.golden_cross`) so a panel that shows one can say which one, and a
panel that wants to show both can show both without them looking like a
contradiction. This is the direct fix for Phase 1 Item 1 and Phase 2 Item
12, generalized.

### What it wraps, not replaces, on day one

`/api/bridge/facts` is a thin aggregator over the *existing* computation
functions (`regime_ma.detect_ma_cross_regime`, `season_manager.get_current_season`,
the `ai_players` count query, etc.) — it does not reimplement any of
them. This matters for risk: the actual math that produces each fact
doesn't move or change, only where a panel goes to read the labeled,
timestamped result.

## Panels this would replace (call sites, not business logic)

Direct replacements — panels currently doing their own fetch + inline
"is this fresh enough" reasoning that would instead read one key each:

- **Regime**: bridge-v2 Macro tab's `macroRegimeLbl` (currently
  `/api/macro/dashboard`'s own `regime` field — a third regime concept,
  worth folding in as `regime.macro_risk_on` once audited), the Riker
  widget (implicitly, via whatever regime it narrates), any future
  "Tactical Display" panel that wants the MA-cross regime explicitly.
- **Season**: all 4 places fixed by hand in Phase 2 (`bridge-v2.html`'s
  two Season tiles, `index.html`'s glance row, `/api/v1/docs` footer,
  Archer chat context) — Phase 2's fixes each independently re-derive the
  season from whatever fetch was already nearby; this endpoint would let
  them all read the same key instead of 4 slightly-different code paths
  that all happen to be correct today but could drift again.
- **Agent counts**: Systems Status, the Fleet Agents tile, the
  leaderboard's own count line, the "Show All N Agents" toggle label —
  four call sites, at least two genuinely different definitions, all of
  which should be explicit about which one they're showing
  (`fleet.agent_count.full_roster.total` vs.
  `fleet.agent_count.leaderboard_roster`).
- **Sector data**: `/api/sectors/heatmap` and `/api/ready-room/briefing`'s
  baked-in narrative numbers — the facts endpoint wouldn't replace the
  narrative text itself, but would let the Gameplan panel show "as of
  Friday 06:00" next to its own numbers instead of presenting them as if
  current.
- **Metals**: the glance-row header and the Metals Exposure panel, made
  explicit about which metric each is (`metals.unrealized_pnl_pct` vs.
  `metals.spot_change_pct.gold`).
- **GEX age**: named by the Admiral as a fifth example of this same
  disease. Not traced this session (the GEX CBOE repoint itself is
  explicitly out of scope) — but "how stale is the current gamma
  snapshot" is exactly the `as_of`/`stale_after_s` shape this endpoint is
  built for, and it should get a `gex.snapshot` key alongside the others
  whenever that work happens, rather than its own bespoke staleness check
  (which is roughly what caused the 2026-09-01 GEX incident this repo's
  own history already documents — a third, independent instance of the
  same disease, one this spec would have prevented rather than just
  papering over).

## What it would NOT touch

- No trading decision code. `regime_router.py`, `paper_trader.py`, and
  every other real gate keep reading their own sources directly, exactly
  as today — this is a *display* aggregator. A fact key mirrors a
  decision-path value for display purposes; it never becomes the
  decision path's own source.
- No change to how any individual fact is computed. `detect_ma_cross_regime()`,
  `get_current_season()`, etc. all keep their current logic, callers, and
  cache TTLs. The endpoint reads their *existing* return values and adds
  the envelope.
- Item 9's full cross-panel unification isn't "solved" by building this
  endpoint alone — panels still have to be migrated to read from it one
  at a time (see Phasing below). Building the endpoint is necessary but
  not sufficient; the payoff is in the migration.

## Phasing (if approved)

1. **Endpoint + 5-8 core facts** (regime x2, season, agent counts x2,
   metals x2, sector as-of): a few hundred lines, no frontend changes
   yet. Testable and reviewable in isolation.
2. **One pilot panel migrated** (suggest the Season tiles — lowest risk,
   already proven-correct logic in 4 places to consolidate into 1) to
   validate the pattern end to end, including the staleness grey-out
   behavior, before committing to a wider rollout.
3. **Remaining panels migrated incrementally**, prioritized by how often
   this session's audit actually found them wrong (regime, metals,
   agent counts, sector data — the four families with confirmed real
   incidents above) over panels that were merely *slow* to converge but
   not visibly wrong.
4. **GEX key added** if/when the CBOE repoint work happens — deliberately
   sequenced after, not blocking it, since that work is explicitly
   out of scope for this pass.

## Effort estimate

- Endpoint + 5-8 facts, with tests: **~1 focused session** (comparable in
  size to this pass's Phase 2, which touched 5 files across similar
  ground). Low risk — additive, no existing code path changes.
- Pilot panel migration + staleness UI (grey-out CSS/behavior, one
  panel): **~half a session**, mostly frontend, includes a manual
  browser smoke test per this repo's Frontend Ship Rule.
- Full rollout across all identified panels: **2-4 more sessions**,
  spread out, each one small and independently reviewable rather than a
  single large migration — matches how Phase 2 of this pass went (11
  independent, individually-verified fixes rather than one big patch).
- Not estimated here: whether "grey out on stale" is the right UX for
  every panel (a stale season badge probably should just look normal
  until it's *very* stale; a stale regime badge during market hours
  probably should scream). That's a design decision for whoever builds
  Phase 2 of this spec, not assumed here.
