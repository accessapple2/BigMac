-- HM-POSITIONS-CONVICTION-DENORM Phase 1 migration (2026-05-24)
-- Denormalizes signal/trade confidence onto the positions table so the
-- live exit-evaluation site (engine/risk_manager.py:785) can read conviction
-- without threading new state through the get_portfolio → check_stop_loss
-- → exit-action call chain.
--
-- Precursor for HM-RISK-MANAGER-CONVICTION-STOP-WIRE — conviction-scaled
-- stops require this denorm because positions.conviction does not exist
-- and the read path SELECT in paper_trader.get_portfolio doesn't currently
-- carry signal confidence forward from BUY time.
--
-- Backfill from trades.confidence (most-recent BUY per player_id+symbol)
-- handled by hm_positions_conviction_denorm_backfill.py (Phase 2).
-- Live BUY-path write handled by paper_trader.py edit (Phase 3).
--
-- Pre-flight verified 2026-05-24: all 9 INSERT INTO positions sites use
-- explicit column lists, so ALTER ADD COLUMN with DEFAULT NULL is safe
-- against the running trader without restart.
--
-- Backup at: data/trader.db.backup-2026-05-24-pre-HM-POSITIONS-CONVICTION-DENORM
-- Rollback: ALTER TABLE positions DROP COLUMN conviction;
--           ALTER TABLE positions DROP COLUMN conviction_source;
--           (SQLite < 3.35 requires table-rebuild dance; macOS bigmac
--           SQLite is 3.43+ per Homebrew, DROP COLUMN works in-place.)

ALTER TABLE positions ADD COLUMN conviction REAL DEFAULT NULL;
ALTER TABLE positions ADD COLUMN conviction_source TEXT DEFAULT NULL;
-- conviction_source values: 'live_buy' (Phase 3 write path) |
--                            'backfill' (Phase 2 historical) |
--                            'manual' (future hand-fix) | NULL (legacy/edge)
