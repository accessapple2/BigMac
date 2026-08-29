# Question: Polygon rate limiter — BENCH-gated / GEX-staleness-sensitive module list

**Date:** 2026-08-28
**Context:** Part C of the 429-remediation directive (Polygon free tier, 5
calls/min) explicitly gates building/enabling `engine/rate_limiter.py` +
consolidating all Polygon calls through `engine/providers/polygon_provider.py`
on an Admiral review of which callers feed BENCH-gated execution and can't
tolerate stale GEX. GO was given on the investigation only — **nothing built
or enabled yet.**

## The directive undercounts the real surface

"12 engine modules + 5 scripts" undercounts what's actually calling
`api.polygon.io` / importing `polygon_provider` today: **34 files**, not 17 —
15 in `engine/`, 7 in `scripts/`, and 12 more spread across `swingdesk/`,
`scrapers/`, `strategies/`, `dashboard/app.py`, `signal-center/`, and
`mread_server.py` that weren't named at all. Whatever the limiter's actual
scope ends up being, it needs to account for all 34, or it'll just move the
429s to whichever callers got left out.

## Triage of the `engine/` callers (highest-signal for the staleness question)

**Can't tolerate stale GEX — feed live/BENCH-gated decisions:**
- `engine/gamma_context.py` — native GEX grounding fed directly into
  options/0DTE agent decisions. This is the central "GEX" case the directive
  means.
- `engine/options_pricing.py` — quote-based entry pricing for short-premium
  CSP writers. Stale quote = wrong entry price on a paper fill.
- `engine/paper_trader.py` — the execution routing model itself.
- `engine/bk_orb_scanner.py` — intraday opening-range-breakout confirmatory
  scanner; timing-sensitive by construction.
- `engine/squeeze_scanner.py` — feeds the Squeeze Score signal directly.
- `engine/ollie_machine_universe.py` — tradeable-universe filter applied
  PRE-VOTE; stale data here changes which symbols agents can even consider.

**Safe to serve cached/stale data — not on the execution path:**
- `engine/options_flow_gex.py` — explicitly self-documented
  "OBSERVATION-ONLY BY CONSTRUCTION... imports NO order/execution path."
- `engine/universe_refresh.py` — weekly cadence already; low rate-pressure
  regardless.
- `engine/ticker_context.py` / `engine/ticker_names.py` — company-name /
  narrative grounding for LLM briefings, not price/trading data.
- `engine/holly_intraday.py` — a backtest engine, not live execution.
- `engine/total_portfolio.py` — read-only external-account reporting (RULE #1
  territory — those accounts stay read-only regardless), not a trading input.
- `engine/crew_dissent.py` — retrospective "was the dissenter right"
  scoring, evaluated after the fact, not gating a live decision.

**Infrastructure, not a consumer:**
- `engine/providers/polygon_provider.py` — this is what Part C proposes
  making the sole caller; not itself a staleness-sensitivity question.

## Not yet triaged to the same depth

The 7 `scripts/` files and the 12 files outside `engine/`/`scripts/`
(`swingdesk/*` — a whole separate scanner+options-engine+autopilot stack,
`scrapers/*`, `strategies/*`, `dashboard/app.py`, `signal-center/
expectancy_engine.py`, `mread_server.py`) weren't individually triaged this
pass — flagging their existence and rough grouping is as far as read-only
investigation went before stopping to bring this back for review, per the
explicit gate. `swingdesk/` in particular looks like it could have its own
live-decision-sensitive callers (`options_engine.py`, `shadow_autopilot.py`
by name alone) and probably deserves the same depth of triage before any
limiter goes live, not an assumption either way.

## What this means for the limiter design (not built)

A single shared `engine/rate_limiter.py` wrapping `polygon_provider.py` at
4 calls/min with an on-disk cache (as the directive proposes) would need
either: (a) a TTL/freshness tier per caller — GEX/gamma callers get little
to no caching (or an explicit "reject if stale beyond N seconds during market
hours" behavior), while the observation-only/backtest/reporting group can
happily serve a cache that's minutes or hours old; or (b) two separate
budgets against the same 5-calls/min ceiling, one reserved for the
live-decision group. Either is a real design decision, not just an
implementation detail — bringing it here rather than picking one unilaterally.

## Options

- **Approve the `engine/` triage as scoped, proceed to design the limiter
  around it, and separately triage `scripts/`+the other 12 files before
  anything outside `engine/` gets migrated onto the limiter** — narrows the
  first pass to what's already investigated.
- **Hold the whole limiter until all 34 files are triaged to the same
  depth** — safer, slower.
- **Something else** — e.g. limiter scope should be per-directory, or
  `swingdesk/` should be excluded/handled separately as its own subsystem.
