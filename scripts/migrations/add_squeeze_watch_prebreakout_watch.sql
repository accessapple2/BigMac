-- HM-SQUEEZE-PRE-BREAKOUT-WATCH migration (banked 2026-05-24)
-- Adds a tighter precursor flag on squeeze_watch: BB/KC squeeze still ON
-- (duration >= 10d) AND price within ±2% of trailing 10-day high AND
-- volume in neutral range (0.7-1.3× 20d mean). Catches the moment before
-- the breakout — distinct from HM-SQUEEZE-PRE-BREAKOUT-COMPOSITE (broader,
-- top-25% range + contracting vol) and HM-SQUEEZE-RELEASE-DETECT (after).
--
-- Idempotent: engine/bbkc_squeeze_scanner.py::_ensure_schema mirrors this.

ALTER TABLE squeeze_watch ADD COLUMN pre_breakout_watch INTEGER DEFAULT 0;
ALTER TABLE squeeze_watch ADD COLUMN dist_to_10d_high_pct REAL;
ALTER TABLE squeeze_watch ADD COLUMN neutral_vol_ratio REAL;

CREATE INDEX IF NOT EXISTS idx_squeeze_watch_prebreakout
    ON squeeze_watch(kind, pre_breakout_watch DESC, scan_ts DESC)
    WHERE pre_breakout_watch = 1 AND dismissed = 0;
