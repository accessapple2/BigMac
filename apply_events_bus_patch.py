#!/usr/bin/env python3
"""HM-BACKTEST-REALISM 2026-07-03 — apply the swing stale-budget fix in place.

Edits engine/events_bus.py: "swing": 30  ->  "swing": 3600 (with comment).
Takes a timestamped .bak first. Idempotent: refuses to run twice.
Run from ~/autonomous-trader:  python3 apply_events_bus_patch.py
"""
import shutil
import sys
from datetime import datetime
from pathlib import Path

TARGET = Path("engine/events_bus.py")
OLD = '    "swing": 30,'
NEW = (
    '    # HM-BACKTEST-REALISM 2026-07-03 (XO audit): swing was 30s — shorter than\n'
    '    # intraday (900s) and shorter than the 60s consumer poll, so a swing signal\n'
    '    # born just after a poll tick expired before it could EVER be dispatched.\n'
    '    # Swing setups (5-30 day holds) stay valid for hours; 3600s clears the 60s\n'
    '    # poll + 120s emit cycle with margin while still expiring same-session.\n'
    '    "swing": 3600,'
)

def main() -> int:
    if not TARGET.exists():
        print(f"ERROR: {TARGET} not found — run from ~/autonomous-trader")
        return 1
    src = TARGET.read_text()
    if '"swing": 3600' in src:
        print("Already patched — nothing to do.")
        return 0
    if OLD not in src:
        print('ERROR: expected \'"swing": 30,\' not found — file has drifted, patch manually.')
        return 1
    bak = TARGET.with_suffix(f".py.bak_pre_realism_{datetime.now():%Y%m%d_%H%M%S}")
    shutil.copy2(TARGET, bak)
    TARGET.write_text(src.replace(OLD, NEW, 1))
    import py_compile
    py_compile.compile(str(TARGET), doraise=True)
    print(f"Patched OK. Backup: {bak}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
