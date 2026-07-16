-- HM-REALIZED-RETRY migration (2026-07-15)
--
-- Splits realized-return tracking off from evaluated_at so a temporarily
-- unavailable Alpaca daily bar (expiry date's close not yet published)
-- doesn't get permanently locked in as a lost observation. See
-- engine/signal_evaluator.py::evaluate_realized_pending() module docstring.
--
--   realized_at              TEXT      -- set once resolved (success OR
--                                          permanent-fail after max attempts)
--   realized_attempts        INTEGER   -- fetch attempts made so far
--   realized_next_retry_at   TEXT      -- earliest time to retry after a
--                                          transient failure
--   realized_fail_reason     TEXT      -- last failure reason code
--                                          (no_keys/http_NNN/no_bars/
--                                           same_bar/bad_close/error:*/
--                                           not_directional/same_day_expiry)
--
-- Backfill: the 5,729 bk_avwap + 61 bk_box rows that already have a
-- fwd_return_1d_realized value (all from the 2026-06-29 02:10-04:26 UTC
-- window, before this split existed) are marked realized_at=evaluated_at,
-- realized_attempts=1 so evaluate_realized_pending() doesn't re-fetch them.

ALTER TABLE signal_observations ADD COLUMN realized_at TEXT;
ALTER TABLE signal_observations ADD COLUMN realized_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE signal_observations ADD COLUMN realized_next_retry_at TEXT;
ALTER TABLE signal_observations ADD COLUMN realized_fail_reason TEXT;

CREATE INDEX IF NOT EXISTS ix_sigobs_realized_pending
    ON signal_observations(realized_at, evaluated_at);

UPDATE signal_observations
   SET realized_at = evaluated_at,
       realized_attempts = 1
 WHERE fwd_return_1d_realized IS NOT NULL
   AND realized_at IS NULL;
