-- ① covered-call sign-only correction (negate realized_pnl; NO x100 — matches trades book convention)
-- 2540 LITE -12.08 -> +12.08
-- 2541 MRAM  -0.60 ->  +0.60
-- 2542 COHR  -3.76 ->  +3.76
-- 2543 MNST  -0.19 ->  +0.19
UPDATE trades SET realized_pnl = -realized_pnl WHERE id=2540 AND realized_pnl=-12.08;
UPDATE trades SET realized_pnl = -realized_pnl WHERE id=2541 AND realized_pnl=-0.6;
UPDATE trades SET realized_pnl = -realized_pnl WHERE id=2542 AND realized_pnl=-3.76;
UPDATE trades SET realized_pnl = -realized_pnl WHERE id=2543 AND realized_pnl=-0.19;
