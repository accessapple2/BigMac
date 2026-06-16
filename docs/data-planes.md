# data-planes.md

> Relocated from CLAUDE.md by HM-PRIME Part C (move, not delete).

## strategy_signals (convergence scanner data plane)

`strategy_signals` is the canonical table for multi-strategy convergence
candidates feeding Phase 1/2/2.5 of HM-OLLIE-LIVE-SCANNER and the live event
tape. Columns: `ticker`, `strategy_name`, `confidence`, `entry_price`,
`stop_price`, `target_price`, `created_at`, `scan_date`. Read paths:

- `dashboard/app.py::api_scanner_convergence()` — `/api/scanner/convergence`
  tier-counts strategies over a 90-min window. Tier 1: ≥5 strategies. Tier 2:
  4. Tier 3: 3.
- `dashboard/app.py::api_scanner_events()` and `api_scanner_events_realtime()`
  — decorate events with `in_scanner_tier` from the same 90-min window.
- `engine/event_tape.py::_scanner_tier_for()` — Phase 2.5 detector
  cross-reference.

**Legacy convergence scanner — NOT dead (corrected 2026-05-28, HM-CLAUDE-STALENESS):**
The earlier "scan_strategies write path silently dead since 2026-04-07" note is
**STALE/WRONG**. `engine/strategies.py::scan_strategies` is LIVE: the Navigator
endpoints `/api/navigator/strategies/scan` and `/api/navigator/scan-now`
(dashboard/app.py) call it with default `save=True` → writes `strategy_signals`,
which is read back by `/api/navigator/convergence`, `get_todays_signals`,
`event_tape`, `trade_cards_api`, `tick_recorder`. Do NOT retire it without first
deprecating those Navigator endpoints (a product decision) + a coordinated
reader migration. Verified live during WAVE 4 (2026-05-28).

**Window doctrine:** the canonical convergence window is 90 minutes. Match it
when adding new readers — windows shorter than 30 min miss premarket scan
batches; longer than 120 min mixes regimes and produces stale tier flags.
