-- HM-PROVING-GROUND-FORMALIZE-V2 SUB-2 migration (2026-05-25)
--
-- Adds the formal exit-state machinery for the Sniper Mode trial:
--   1. running_scorecard.exit_status  TEXT DEFAULT 'pending'
--      (back-compat: NULL/missing rows default to 'pending' per Phase A
--       brief guard rail — older rows without this column existed in 25
--       prior scorecard entries; ALTER ADD COLUMN with DEFAULT 'pending'
--       backfills them transparently.)
--   2. state_transitions  NEW TABLE
--      Doctrine Rule #1 compliance — never deleted, append-only audit
--      log of every state machine edge fired by ship_kill_evaluator.
--
-- States:
--   pending       (initial)
--   warning       (go_count 3 or 4 for 5+ consecutive days)
--   ship_ready    (SHIP conditions met for 10 consecutive days)
--   kill_warning  (any KILL condition fires)
--   shipped       (final — set only by Admiral CLI --ship)
--   killed        (final — set only by Admiral CLI --kill)
--
-- Storage: data/proving_ground.db (NOT trader.db). Matches the existing
-- proving_ground.py separation of scorecard data from production trader DB.

ALTER TABLE running_scorecard
  ADD COLUMN exit_status TEXT DEFAULT 'pending';

CREATE TABLE IF NOT EXISTS state_transitions (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    transition_at         TEXT NOT NULL DEFAULT (datetime('now')),
    from_state            TEXT NOT NULL,
    to_state              TEXT NOT NULL,
    trigger_metrics_json  TEXT NOT NULL,
    ntfy_sent             INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_state_transitions_at
    ON state_transitions(transition_at DESC);
