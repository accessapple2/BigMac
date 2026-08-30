#!/usr/bin/env python3
"""scripts/mlx_qwen3_probe.py — HM-MLX-QWEN3-REVIVAL-2026-08-29.

mlx-qwen3's local MLX server (engine/providers/mlx_provider.py, port 8899)
died 2026-07-18 -- four days before the 07-22 stand-down, a fully
independent incident -- and stayed dead six weeks with ZERO supervision:
no launchd plist, no cron entry, nothing watching it at all. Revived via
com.ollietrades.mlx-qwen3.plist (KeepAlive=true so a crash self-heals).

This script is the missing piece: mlx_lm.server is a third-party binary
with no heartbeat-writing of its own, so an external prober is the only
way to get a heartbeat file for hm_ops_sentinel.py to watch (same "alarm
on a different mechanism than what it watches" doctrine as
source_health_watcher.py). HTTP-checks the server's /v1/models endpoint
and writes a heartbeat JSON every run.

Runs via com.ollietrades.mlx-qwen3-probe.plist (StartInterval, every 5 min).

Exit codes:
  0 - probe ran (heartbeat written regardless of health/unhealthy)
  1 - error before the heartbeat could be written
"""
from __future__ import annotations

import json
import sys
import time
import urllib.request
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_HEARTBEAT_PATH = _ROOT / "data" / "mlx_qwen3_heartbeat.json"
MLX_URL = "http://127.0.0.1:8899/v1/models"


def main() -> int:
    now = time.time()
    healthy = False
    detail = None
    latency_ms = None
    try:
        t0 = time.time()
        with urllib.request.urlopen(MLX_URL, timeout=10) as r:
            body = r.read()
            latency_ms = round((time.time() - t0) * 1000, 1)
            if r.status == 200:
                healthy = True
                try:
                    data = json.loads(body)
                    detail = [m.get("id") for m in data.get("data", [])]
                except Exception:
                    detail = "200 OK, unparseable body"
            else:
                detail = f"HTTP {r.status}"
    except Exception as e:
        detail = f"{type(e).__name__}: {e}"

    heartbeat = {
        "watcher": "mlx_qwen3_probe",
        "last_run": now,
        "last_run_iso": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now)) + "Z",
        "healthy": healthy,
        "latency_ms": latency_ms,
        "detail": detail,
    }
    try:
        _HEARTBEAT_PATH.parent.mkdir(parents=True, exist_ok=True)
        _HEARTBEAT_PATH.write_text(json.dumps(heartbeat, indent=2))
    except Exception as e:
        print(f"[mlx-qwen3-probe] failed to write heartbeat: {e}", file=sys.stderr)
        return 1

    print(f"[mlx-qwen3-probe] healthy={healthy} latency_ms={latency_ms} detail={detail}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
