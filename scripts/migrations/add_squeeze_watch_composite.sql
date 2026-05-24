-- HM-SQUEEZE-PRE-BREAKOUT-COMPOSITE migration (banked 2026-05-24)
-- Extends squeeze_watch with composite-pass + range-position +
-- vol-contracting columns so the BB/KC scanner can flag the directional-
-- bias subset (coil + top-of-range + contracting volume). RS-pass
-- bonus column tracked separately so the composite still works when
-- RS_RANK_ENABLED is off.
--
-- Idempotent: engine/bbkc_squeeze_scanner.py::_ensure_schema mirrors
-- this for fresh-DB / drift handling.

ALTER TABLE squeeze_watch ADD COLUMN composite_pass INTEGER DEFAULT 0;
ALTER TABLE squeeze_watch ADD COLUMN range_position_pct REAL;
ALTER TABLE squeeze_watch ADD COLUMN vol_contracting_pct REAL;
ALTER TABLE squeeze_watch ADD COLUMN composite_rs_pass INTEGER DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_squeeze_watch_composite
    ON squeeze_watch(kind, composite_pass DESC, scan_ts DESC)
    WHERE composite_pass = 1 AND dismissed = 0;
