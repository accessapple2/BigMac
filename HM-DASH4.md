# 🔧 SCOTTY — HM-DASH.4: Parameterize Squeeze Threshold
### Opus 4.7 · Small follow-up to Session 1 · ~30 min

> **Captain's orders, Mr. Scott:** O1 from Session 1 closure resolved. Make /api/squeeze/candidates return score>=3 (broader) WITHOUT changing squeeze_watch table behavior (still >=5 for high-confidence persistence). Frontend filters by score. Today's ATYR (DTC=25) and ABR (DTC=20) would be surfaced.

## Constraints

- squeeze_watch table semantics UNCHANGED — existing consumers (scanner persist threshold, any other readers) must see exactly the same data they see today
- /api/squeeze/candidates endpoint exposes score>=3 with score field visible (the field exists per DASH.3 closure — just need lower threshold to be populated)
- ?min_score=N query param accepted, default 3
- Each returned row tagged with `tier`: 'watch' for >=5, 'candidate' for 3-4
