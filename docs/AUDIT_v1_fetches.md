# AUDIT — v1 (`/classic`) request volume

Method: `mcp__claude-in-chrome__read_network_requests`, fresh `/classic` load,
captured for ~60s post-load (long enough to see 2-3 poll cycles of the
tightest-interval loops, not just the one-time boot fetches).

## Headline finding: it's not 557 distinct call sites — it's 138 distinct
## endpoints, several polled by independent, uncoordinated loops

Raw captured requests in the 60s window: **196**. Distinct URLs: **138**.
The remaining ~58 are repeat polls from a handful of tight-interval loops —
extrapolated over a longer session (the reported 557 baseline is plausible
over several minutes), the repeat-poll volume dominates, not the number of
distinct things the page actually needs on boot.

**This changes where the leverage is.** Consolidating "the top 5 highest-count
blocks" isn't about merging call *sites* — the top offenders are the same
handful of endpoints firing repeatedly. The fix is deduplicating overlapping
polling loops, not restructuring one-time boot fetches (those are already
each called once).

## Top repeat-offenders in the 60s window (sourced to exact JS)

| Endpoint | Count/60s | Interval | Source function | Line |
|---|---|---|---|---|
| `/api/alerts/recent?limit=1` | 9 | 30s (`setInterval`, comment says "was 10s") | `pollTradeAlerts()` | 29093 |
| `/api/flash-alerts/active` | 8 | 15s (self-`setTimeout` loop) | `_pollFlashAlerts()` IIFE | 40820 |
| `/api/flash-alerts/latest` | 6 | 20s (self-`setTimeout` loop) | `_pollFab()` IIFE | 40859 |
| `/api/dynamic-alerts/active?minutes=30` | 5 | 20s (`setInterval`) | `checkAlertBanner()` | 32683/32733 |
| `/api/navigator/convergence` | 4 | ~15-20s (not yet sourced) | — | — |
| `/api/arena/leaderboard` | 4 | ~15-20s (not yet sourced) | — | — |
| `/api/unrealized` | 3 | ~20s (not yet sourced) | — | — |
| `/api/regime` | 3 | ~20s (not yet sourced) | — | — |
| `/api/ready-room/alerts` | 3 | ~20s (not yet sourced) | — | — |
| `/api/performance?season=6&fleet_only=true` | 3 | ~20s (not yet sourced) | — | — |
| `/api/kirk/advisory` | 3 | mixed one-time + poll | — | — |

**The clearest, lowest-risk win: the top 4 rows are all "alerts" data**,
fetched by 4 completely independent polling loops that were evidently added
at different times without anyone noticing they overlap:
- `pollTradeAlerts` — trade-specific alert banner
- `checkAlertBanner` — general dynamic alert banner
- `_pollFlashAlerts` — flash alert modal
- `_pollFab` — flash alert thin top-strip banner

All four poll every 15-30 seconds, independently, forever, whether or not
the user is looking at anything alert-related. This cluster alone accounts
for 28 of the 58 repeat-poll requests in the 60s window (~48%).

## Boot-time (one-time) fetches — not a consolidation target by count,
## but a lazy-load target by relevance

Of the 138 distinct endpoints, a large fraction are for sections that are
`display:none` on initial paint (per `showSection()` — only one `section-*`
is visible at a time) but whose data is fetched unconditionally on load
regardless of which tab is active. Examples spotted in this capture:
`/test-kitchen/api/health`, `/test-kitchen/api/positions`,
`/api/backtest/filter-contribution`, `/api/tax/history`,
`/api/tax/opportunities`, `/api/tax/wash-sales`, `/api/congress/trades`,
`/api/congress/overlap`, `/api/uhura/institutional`, `/api/scanner/events*`
— these are Stage 3 candidates (lazy-load on tab-open, not on boot), not
Stage 2 candidates (they're each called once already, so consolidating
them doesn't reduce request count the way deduplicating the alert-poll
cluster does).

## Stage 2 — DONE, verified live (commit `8065dfe`)
Found a 5th independent poller while implementing this stage: `alert_speaker.js`
(external file, not in `index.html` itself) has its own separate 10s poll to
`/api/alerts/recent`, on top of `pollTradeAlerts`'s 30s poll to the same
endpoint — a second, previously-undiscovered duplicate. All 5 now share one
`/api/alerts/poll` aggregate endpoint via a 15s-TTL cache helper
(`_fetchAlertsPoll`); each poller's own interval and UI logic left untouched.

**Verified live**, clean network-buffer capture, ~55s window, before/after:
- `/api/flash-alerts/active` — was 8, now **0** (fully eliminated as an independent call)
- `/api/flash-alerts/latest` — was 6, now **0** (fully eliminated)
- `/api/dynamic-alerts/active` — was 5, now **0** (fully eliminated)
- `/api/alerts/recent` — partially deduplicated (the 10s-cadence poller is
  tighter than the 15s cache TTL, so it doesn't fully collapse to zero
  independent calls — a tuning nuance, not a functional gap; could drop the
  cache TTL to 10s in a follow-up if this specific residual matters)
- Alert banner confirmed still rendering correctly end-to-end (live MACD
  cross alert toast observed on screen after the change) — no UI regression

## Stage 3 — deferred, not started this session
Gate the boot-time fetches for non-visible sections behind `showSection()`
lazy-init (fetch on first activation of that tab, not unconditionally on
page load). Requires mapping each of the ~40+ remaining boot-time fetches
(test-kitchen, tax, congress, uhura-institutional, scanner/events, etc.) to
its owning section, then verifying each affected section still loads
correctly on first open — a materially larger, more error-prone change than
Stage 2 (which was a contained, mechanical swap with an easy before/after
network-count check). Per the directive's own instruction to stop rather
than push through if a stage looks too risky, and given 3 more major items
remain in this same directive, this is deferred as a clearly-scoped
follow-up rather than rushed this session.
