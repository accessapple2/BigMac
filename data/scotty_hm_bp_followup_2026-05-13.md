# HM-BP-FOLLOW-UP — gemini-2.5-pro March 12 entry_price corruption trace

**Date:** Investigation 2026-05-13
**Trigger:** HM-BP (commit e2a59c4) reject filter caught 5+ trades with abs(pnl_pct) > 50%
**Root corruption sample:** gemini-2.5-pro AMZN entry=$8.88 exit=$211.23 (2278%)

## Anomaly profile

- All offending trades dated 2026-03-12
- All on gemini-2.5-pro player_id
- Symbols: AMZN, AAPL, TSLA (mega-caps with prices in $200-$400 range)
- Entry prices: $8.88-$21.52 (single-digit/low double-digit)
- Exit prices: $200-$400 (correct mega-cap prices)
- Pattern: entry is ~1/24th to 1/19th of actual price (looks like NMS error or split-adjust glitch)

## Hypotheses to investigate

1. **Split-adjustment glitch** — AMZN 20:1 split was 2022; AAPL 4:1 was 2020; TSLA 3:1 was 2022. All before 2026-03-12. Possible stale split adjustment in seed data.
2. **Currency conversion error** — $8.88 × ~24 ≈ $211. JPY:USD ratio at the time was ~145. EUR:USD ~1.08. Neither maps cleanly. INR:USD ~83 — closer but still wrong.
3. **Test data contamination** — gemini-2.5-pro running on backtest seed data that bled into live trades table.
4. **Manual/seeded position** — someone (or a migration script) inserted demo positions with wrong prices.

## Next steps for HM-BP-FOLLOW-UP Phase 2

1. Pull trade rows with full context (executed_at timestamp, reasoning, side, qty)
2. Check ai_brain.py for any test-mode flag that could have triggered 2026-03-12
3. Audit any migration scripts/seed-data fixtures that ran ~March 11-12
4. Verify entry_price column is REAL not stored as cents (no obvious unit mismatch evidence)

## Resolution status

HM-BP reject filter (commit e2a59c4) handles this gracefully — but the corruption itself remains in trades table. Park as data-cleanup follow-up:
- Option A: leave as-is (filter handles it)
- Option B: SQL UPDATE the 5 rows with corrected entry_price (computed from realized_pnl backward)
- Option C: Soft-delete with a `corrupt_data=1` flag column

Captain decision required before any data mutation.
