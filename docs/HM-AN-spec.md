# HM-AN — Morpheus Port 9000 Reframe

## Status: SCOPED 2026-05-26, ready to ship

## Dependency: HM-AM ✅ (confirmed live)

## Objective
Reframe port 9000 from a signal-intelligence SPA into Morpheus — the Matrix Operator view.
New default landing = tab-matrix layout with 6 sections.

## Current state (surveyed 2026-05-26)
- signal-center/index.html: 3,266 lines monolithic, 3 section-title divs, no tab DOM
- Morpheus routes: 7 (3 GET state reads, 4 POST admin actions) — all exist, none surfaced in UI
- execution_log: 18-col schema, 0 rows ever — Morpheus action POSTs never fired in prod
- daily_snapshot: 7-col schema, 4 rows, stale since 2026-05-24
- Auth: /api/me → 401 if unauthenticated; admin POSTs gate on same

## Target layout (5 tabs)
1. RED ALERT — kill switch, halt controls, emergency actions
2. MATRIX — signal intelligence (current default content migrated here)
3. INTELLIGENCE — daily_snapshot data, crew performance
4. ORACLE — Kirk advisory log, portfolio advice
5. FLEET — agent roster, model toggle
6. SHIP'S LOG — execution_log, trade history

## Build phases
Phase 1 (~2h): Tab navigation shell + migrate existing sections into MATRIX tab
Phase 2 (~1h): Wire /api/morpheus/awareness + /api/morpheus/operator-info into INTELLIGENCE tab
Phase 3 (~1h): Wire Kirk advisory log + portfolio_advice into ORACLE tab
Phase 4 (~1h): Wire execution_log writer (first live test of Morpheus action POSTs)

## Auth model
Admin POSTs require /api/me authentication. UI should show auth status + login prompt if 401.
Captain authenticates via existing 2FA TOTP flow.

## Sacred rules
- Never touch dashboard/static/index.html (port 8080) — port 9000 only
- Never rm signal-center/signals.db
- Phase 1 must be browser smoke-tested before Phase 2

## Estimated effort: 5h Scotty
