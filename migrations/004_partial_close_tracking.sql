-- Migration 004: partial close tracking for scaleout exit rules
-- Adds contracts_closed_so_far so exit_manager doesn't re-fire
-- the same threshold on subsequent evaluation cycles.
--
-- contracts column was added ad-hoc via ALTER TABLE in Task 6.
-- This migration formalises it with a comment and adds the new column.
-- Both are guarded with IF NOT EXISTS / ignore logic in the apply script.

-- Track how many contracts have been partially closed so far.
-- Starts at 0; incremented each time executor.close_position() runs.
ALTER TABLE options_trades ADD COLUMN contracts_closed_so_far INTEGER DEFAULT 0;
