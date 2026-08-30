# ORDER — REVIVED: mlx-qwen3-probe (job)

**Date:** 2026-08-29
**Action:** revive
**Reason:** New heartbeat prober for the mlx-qwen3 revival (scripts/mlx_qwen3_probe.py, StartInterval 300s) -- mlx_lm.server is a third-party binary with no heartbeat of its own, this is the external prober hm_ops_sentinel.py watches.
