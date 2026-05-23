-- Migration 005 — HM-TRADE-DESK-AUTOPILOT 2026-05-22
-- Adds nullable stop_loss_order_id + take_profit_order_id to trades so the
-- Trade Desk's auto-attached protective Alpaca GTC orders link cleanly back
-- to the primary mirror row in trades. NULL on all existing rows (fleet trades
-- + pre-autopilot Captain trades) by design — sacred-data rule preserved.
--
-- Apply via migrations/apply_migration_005.py (idempotent guard there).

ALTER TABLE trades ADD COLUMN stop_loss_order_id TEXT;
ALTER TABLE trades ADD COLUMN take_profit_order_id TEXT;
