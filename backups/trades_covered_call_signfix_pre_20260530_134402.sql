-- ① covered-call sign-only correction pre-state archive 20260530_134402
-- id=2540 LITE: realized_pnl=-12.08
UPDATE trades SET realized_pnl=-12.08 WHERE id=2540; -- rollback
-- id=2541 MRAM: realized_pnl=-0.6
UPDATE trades SET realized_pnl=-0.6 WHERE id=2541; -- rollback
-- id=2542 COHR: realized_pnl=-3.76
UPDATE trades SET realized_pnl=-3.76 WHERE id=2542; -- rollback
-- id=2543 MNST: realized_pnl=-0.19
UPDATE trades SET realized_pnl=-0.19 WHERE id=2543; -- rollback
