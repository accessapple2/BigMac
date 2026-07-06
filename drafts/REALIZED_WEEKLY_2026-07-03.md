# REALIZED WEEKLY SNAPSHOT — 2026-07-03
_Generated 2026-07-04 04:00 UTC · exec-pipeline · restart pending Admiral's return_

## Coverage
- First day: 2026-06-22  Last day: 2026-06-26
- Distinct trading days: 5  Total real-bar rows: 7225
- Sufficiency gate: n≥400 AND days≥20 per source/bucket

## Per-Source (real bars only, excl exact-0.0 artifacts)
```
source                      n    mean%     wr%   days  status
------------------------------------------------------------------------
bk_avwap                 5674   -0.007    54.5      5  COLLECTING — need 15 more days
deep_scan                1484   -0.351    45.4      5  COLLECTING — need 15 more days
bk_box                     52    0.012    65.4      5  COLLECTING — need 348 more obs; need 15 more days
fred_bankrate               8   -0.318    25.0      5  COLLECTING — need 392 more obs; need 15 more days
grok_kirk_scan              7    0.568    57.1      1  COLLECTING — need 393 more obs; need 19 more days
```

## Per-Source × Bucket (strict definitions)
```
source                 bucket                      n    mean%     wr%   days  status
------------------------------------------------------------------------------------------
bk_avwap               a_proj_under10            221    0.774    67.4      5  COLLECTING — need 179 more obs; need 15 more days
bk_avwap               b_proj_over10             124   -1.788    37.1      5  COLLECTING — need 276 more obs; need 15 more days
bk_avwap               c_proj_null              5329    0.002    54.4      5  COLLECTING — need 15 more days
bk_box                 a_proj_under10              6    -0.46    50.0      5  COLLECTING — need 394 more obs; need 15 more days
bk_box                 c_proj_null                46    0.074    67.4      5  COLLECTING — need 354 more obs; need 15 more days
deep_scan              a_proj_under10            948    0.259    54.2      5  COLLECTING — need 15 more days
deep_scan              b_proj_over10             536   -1.429    29.9      5  COLLECTING — need 15 more days
fred_bankrate          c_proj_null                 8   -0.318    25.0      5  COLLECTING — need 392 more obs; need 15 more days
grok_kirk_scan         a_proj_under10              2    0.647    50.0      1  COLLECTING — need 398 more obs; need 19 more days
grok_kirk_scan         b_proj_over10               1   -6.423     0.0      1  COLLECTING — need 399 more obs; need 19 more days
grok_kirk_scan         c_proj_null                 4    2.276    75.0      1  COLLECTING — need 396 more obs; need 19 more days
```

## Sufficiency Gate Summary
**No source/bucket has crossed the sufficiency gate yet.**
COLLECTING — not yet testable. Return when gate trips (n≥400, days≥20).

_Bucket definitions (strict):_
- `a_proj_under10` — fwd_return_1d IS NOT NULL AND < 0.10
- `b_proj_over10`  — fwd_return_1d IS NOT NULL AND >= 0.10
- `c_proj_null`    — fwd_return_1d IS NULL (no deep_scan projection)
- NULL projections are NEVER folded into proj_under10.
