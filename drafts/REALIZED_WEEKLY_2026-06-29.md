# REALIZED WEEKLY SNAPSHOT — 2026-06-29
_Generated 2026-06-29 16:12 UTC · exec-pipeline · restart pending Admiral's return_

## Coverage
- First day: 2026-06-22  Last day: 2026-06-26
- Distinct trading days: 5  Total real-bar rows: 6325
- Sufficiency gate: n≥400 AND days≥20 per source/bucket

## Per-Source (real bars only, excl exact-0.0 artifacts)
```
source                      n    mean%     wr%   days  status
------------------------------------------------------------------------
bk_avwap                 4829    0.052    54.7      5  COLLECTING — need 15 more days
deep_scan                1438   -0.416    44.6      5  COLLECTING — need 15 more days
bk_box                     50    0.013    66.0      5  COLLECTING — need 350 more obs; need 15 more days
fred_bankrate               8   -0.318    25.0      5  COLLECTING — need 392 more obs; need 15 more days
```

## Per-Source × Bucket (strict definitions)
```
source                 bucket                      n    mean%     wr%   days  status
------------------------------------------------------------------------------------------
bk_avwap               a_proj_under10            196     0.79    68.4      5  COLLECTING — need 204 more obs; need 15 more days
bk_avwap               b_proj_over10             107     -1.8    36.4      5  COLLECTING — need 293 more obs; need 15 more days
bk_avwap               c_proj_null              4526    0.064    54.5      5  COLLECTING — need 15 more days
bk_box                 a_proj_under10              6    -0.46    50.0      5  COLLECTING — need 394 more obs; need 15 more days
bk_box                 c_proj_null                44    0.077    68.2      5  COLLECTING — need 356 more obs; need 15 more days
deep_scan              a_proj_under10            912    0.187    53.3      5  COLLECTING — need 15 more days
deep_scan              b_proj_over10             526    -1.46    29.7      5  COLLECTING — need 15 more days
fred_bankrate          c_proj_null                 8   -0.318    25.0      5  COLLECTING — need 392 more obs; need 15 more days
```

## Sufficiency Gate Summary
**No source/bucket has crossed the sufficiency gate yet.**
COLLECTING — not yet testable. Return when gate trips (n≥400, days≥20).

_Bucket definitions (strict):_
- `a_proj_under10` — fwd_return_1d IS NOT NULL AND < 0.10
- `b_proj_over10`  — fwd_return_1d IS NOT NULL AND >= 0.10
- `c_proj_null`    — fwd_return_1d IS NULL (no deep_scan projection)
- NULL projections are NEVER folded into proj_under10.
