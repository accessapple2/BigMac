-- HM-RS-RANK-IBD-BLENDED migration (banked 2026-05-24)
-- Adds rs_rank_blended column to rs_rank table — IBD-style multi-period
-- weighted blend (0.40·3mo + 0.20·6mo + 0.20·9mo + 0.20·12mo) percentile-
-- ranked across the universe.
--
-- The existing rs_rank column (single 12wk window) stays as the primary
-- view; rs_rank_blended is the v2 alternative for downstream consumers
-- that prefer the IBD-blended formula (Minervini, leader composites).
--
-- Idempotent: engine/rs_rank.py::_ensure_schema mirrors this.

ALTER TABLE rs_rank ADD COLUMN rs_rank_blended INTEGER;
