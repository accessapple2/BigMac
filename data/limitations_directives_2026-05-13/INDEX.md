# Limitations Directives — 2026-05-13

Five outstanding limitations identified during today's session, each with
investigation directives ready for next-session execution.

## Priority order

| # | Limitation | Priority | Captain consult? | Est effort |
|---|---|---|---|---|
| L1 | GPU recovery resilience | HIGH | Quick decision | 15 min |
| L4 | CPU-era data contamination | HIGH | No (Scotty) | 20 min |
| L3 | Polygon ReadTimeout pattern | MEDIUM | Maybe (capital) | 30-60 min |
| L5 | run_squeeze_watcher 48.8s | MEDIUM | No (Scotty) | 60-120 min |
| L2 | 8GB VRAM constraint | LOW | After measurement | 30 min |

## Sequencing

1. **L1 first** — single SSH session to Ollie Box, 3 apt-mark commands. Eliminates a recurring failure mode permanently.

2. **L4 second** — adds data_epoch column to scorecards. Enables clean GPU-era baselines for L2 and L5 measurement.

3. **L3 + L5 in parallel** — both are read-only diagnostic Tier 1 work that Scotty can run.

4. **L2 last** — measurement may show "no action needed" (rotation cost acceptable).

## What gets unblocked

- L1 done → HM-CD-MIGRATE doctrine permanently safe against kernel auto-update
- L4 done → All subsequent perf measurements are honest
- L3 done → Tuned data fetch resilience
- L5 done → Squeeze watcher no longer the slowest handler
- L2 done → Conscious choice about model concurrency strategy

After all five, the platform's foundational layer is fully understood and tuned.
