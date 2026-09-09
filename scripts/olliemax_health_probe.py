#!/usr/bin/env python3
"""olliemax_health_probe.py — HM-OLLIEMAX-PROBE (2026-09-08)

Cron every 5 min: curl -sf --max-time 5 http://100.95.195.20:11434/api/tags.
On failure, fires a RED_ALERT (ntfy is a no-op since 2026-07-19's
DECOM-SILENCE guard — RED_ALERT is the only channel that still actually
pages, via pushover + email). This is the first thing that should fire if
olliemax drops overnight; nothing else in the fleet currently checks it.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

OLLIEMAX_URL = "http://100.95.195.20:11434/api/tags"


def main() -> int:
    try:
        result = subprocess.run(
            ["curl", "-sf", "--max-time", "5", OLLIEMAX_URL],
            capture_output=True, timeout=10,
        )
        if result.returncode == 0:
            print("[olliemax-probe] OK")
            return 0
        detail = f"curl exit {result.returncode}: {result.stderr.decode(errors='replace')[:200]}"
    except Exception as e:
        detail = f"{type(e).__name__}: {e}"

    print(f"[olliemax-probe] FAILED — {detail}")
    try:
        from engine.alert_channels import send_alert, AlertLevel
        send_alert(
            message=f"olliemax ({OLLIEMAX_URL}) unreachable -- {detail}",
            level=AlertLevel.RED_ALERT,
            alert_type="hm-olliemax-probe",
            rate_limit_secs=1800,
        )
        print("[olliemax-probe] RED_ALERT dispatched")
    except Exception as e:
        print(f"[olliemax-probe] send_alert unavailable: {e}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
