# HM-BQ Emit Audit 2026-05-13

## Counts by sink
- trader_error.log: 2
- trader.log:       31

## Decorated functions in main.py
- Total `@_hm_bq_instr`: 64

## Decorator implementation
```python
def _hm_bq_instr(name):
    def _deco(fn):
        @_hm_bq_functools.wraps(fn)
        def _wrapper(*args, **kwargs):
            _t0 = _hm_bq_time.perf_counter()
            try:
                return fn(*args, **kwargs)
            finally:
                _wall = _hm_bq_time.perf_counter() - _t0
                if _wall > 1.0:
                    console.log(f'[HM-BQ-instr] {name} wall={_wall:.3f}s')
        return _wrapper
    return _deco
# === /HM-BQ-instr ===
```

## Sample emissions
```
07:33:32 [LRS] [HM-BQ-instr] battle_station_monitor wall=2.133s
08:07:29 [LRS] [HM-BQ-instr] run_squeeze_watcher wall=48.825s
── trader.log ──
           [HM-BQ-instr] run_impulse_check wall=56.250s               main.py:53
           [HM-BQ-instr] run_scanner wall=172.860s                    main.py:53
[17:19:10] [HM-BQ-instr] run_whisper wall=1627.088s                   main.py:53
[17:19:16] [HM-BQ-instr] run_chekov_stoploss wall=6.592s              main.py:53
[17:20:27] [HM-BQ-instr] run_gex_refresh wall=3.170s                  main.py:53
[17:20:30] [HM-BQ-instr] run_gex_overlay_update wall=2.801s           main.py:53
[17:20:37] [HM-BQ-instr] run_flow_lean wall=7.088s                    main.py:53
[17:20:41] [HM-BQ-instr] run_ai_saas_disruption wall=3.927s           main.py:53
           [HM-BQ-instr] run_volume_market_scan wall=1.944s           main.py:53
[17:24:04] [HM-BQ-instr] run_gap_fill_check wall=134.841s             main.py:53
```

## Hypothesis ranking
1. **Threshold-only logging (most likely):** decorator gates emission on wall > N seconds. Read decorator body above to confirm threshold value.
2. **Wrong sink:** emissions go to console.log (trader.log) instead of logger.info (trader_error.log).
3. **Decorator not actually applied** to most paths.

## Recommended next-session actions
- A. Lower threshold to capture more samples (noisier)
- B. Add periodic stats summary (every N cycles emit p50/p95/max per handler)
- C. Write walls to dedicated SQL table for analytical surface

Most likely best: B + C combined.
