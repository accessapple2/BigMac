-- ② conviction backfill pre-state archive 20260530_134402
-- navigator NULL-conviction positions before live_buy_backfill UPDATE
-- id=1400 JTAI: conviction=None source=None
UPDATE positions SET conviction=None, conviction_source=None WHERE id=1400; -- rollback
-- id=1401 LRCX: conviction=None source=None
UPDATE positions SET conviction=None, conviction_source=None WHERE id=1401; -- rollback
-- id=1402 ON: conviction=None source=None
UPDATE positions SET conviction=None, conviction_source=None WHERE id=1402; -- rollback
-- id=1403 QCOM: conviction=None source=None
UPDATE positions SET conviction=None, conviction_source=None WHERE id=1403; -- rollback
-- id=1487 MNTS: conviction=None source=None
UPDATE positions SET conviction=None, conviction_source=None WHERE id=1487; -- rollback
