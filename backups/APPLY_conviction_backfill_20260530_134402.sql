-- ② navigator conviction backfill (sourced 0.78 from originating stock BUY trades.confidence)
-- 1400 JTAI  ← stock BUY id=2560 conf=0.78
-- 1401 LRCX  ← stock BUY id=2563 conf=0.78
-- 1402 ON    ← stock BUY id=2561 conf=0.78
-- 1403 QCOM  ← stock BUY id=2562 conf=0.78
-- 1487 MNTS  ← stock BUY id=2584 conf=0.78
UPDATE positions SET conviction=0.78, conviction_source='live_buy_backfill' WHERE id=1400 AND conviction IS NULL;
UPDATE positions SET conviction=0.78, conviction_source='live_buy_backfill' WHERE id=1401 AND conviction IS NULL;
UPDATE positions SET conviction=0.78, conviction_source='live_buy_backfill' WHERE id=1402 AND conviction IS NULL;
UPDATE positions SET conviction=0.78, conviction_source='live_buy_backfill' WHERE id=1403 AND conviction IS NULL;
UPDATE positions SET conviction=0.78, conviction_source='live_buy_backfill' WHERE id=1487 AND conviction IS NULL;
