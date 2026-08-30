# ORDER — REVIVED: mlx-qwen3 (job)

**Date:** 2026-08-29
**Action:** revive
**Reason:** Revived after an unsupervised death 2026-07-18 (6 weeks dark, no launchd/cron ever watched it). New com.ollietrades.mlx-qwen3.plist (KeepAlive=true) + heartbeat probe + hm_ops_sentinel RED_ALERT coverage. Reconstructed startup invocation from engine/providers/mlx_provider.py's own defaults (shell history had zero mlx/8899 mentions); model confirmed cached locally, server verified live via /v1/chat/completions.
