#!/usr/bin/env python3
"""patch_qg_hold_rating.py — Draft Patch 2 for HM-QG-CALIBRATION.

Currently the analyst-recommendation branch awards 0 (FAIL) for
recommendation="hold", which is the median Wall Street rating and the
single biggest reason INTC (696 lifetime rejects) is blocked. This patch
makes "hold"/"neutral" earn 0.5 partial credit (same as SKIP), while
"sell"/"underperform"/"strong_sell" remain outright FAIL.

INDEPENDENT of Patch 1. Captain may land both, just Patch 1, or neither.

DEFAULT: dry-run. Use --apply to write. --revert restores from backup.

Invoke via venv/bin/python3 scripts/patch_qg_hold_rating.py [--apply|--revert]
"""
from __future__ import annotations

import argparse
import datetime as _dt
import difflib
import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TARGET = PROJECT_ROOT / "engine" / "quality_gate.py"
TODAY = _dt.date.today().isoformat()
BACKUP = TARGET.with_suffix(f".py.bak.hold.{TODAY}")

# Old analyst block (post-Patch-1 indentation — 8 spaces, inside `if fund:`).
OLD_BLOCK = '''        # 4. Analyst consensus Buy or Strong Buy
        rec = fund.get("recommendation", "")
        if rec and rec.lower() in ("buy", "strongbuy", "strong_buy", "overweight"):
            score += 1
            details.append(f"analyst={rec}")
        elif rec:
            details.append(f"FAIL analyst={rec}")
        else:
            details.append("SKIP analyst=N/A")
'''

# New analyst block — "hold"/"neutral" earn 0.5 partial credit.
NEW_BLOCK = '''        # 4. Analyst consensus Buy or Strong Buy
        # Patch 2 (HM-QG-CALIBRATION): "hold"/"neutral" — the median
        # Wall Street rating — now earn 0.5 partial credit instead of
        # being treated as outright FAIL. Outright sells still FAIL.
        rec = fund.get("recommendation", "")
        _rec_low = (rec or "").lower()
        if _rec_low in ("buy", "strongbuy", "strong_buy", "overweight"):
            score += 1
            details.append(f"analyst={rec}")
        elif _rec_low in ("hold", "neutral"):
            score += 0.5
            details.append(f"analyst={rec} (partial)")
        elif rec:
            details.append(f"FAIL analyst={rec}")
        else:
            score += 0.5
            details.append("SKIP analyst=N/A")
'''


def _read_target() -> str:
    return TARGET.read_text(encoding="utf-8")


def _build_patched(src: str) -> str:
    if OLD_BLOCK not in src:
        raise SystemExit(
            f"FATAL: expected OLD_BLOCK not found verbatim in {TARGET}. "
            "Source has drifted from the V3 snapshot this patch was drafted "
            "against. Refusing to apply.\n"
            "Note: if Patch 1 has already been applied, this OLD_BLOCK is "
            "still present untouched — both patches edit disjoint regions."
        )
    return src.replace(OLD_BLOCK, NEW_BLOCK, 1)


def _unified_diff(old: str, new: str) -> str:
    return "".join(
        difflib.unified_diff(
            old.splitlines(keepends=True),
            new.splitlines(keepends=True),
            fromfile=f"a/engine/quality_gate.py",
            tofile=f"b/engine/quality_gate.py",
            n=3,
        )
    )


def _py_compile_check() -> None:
    res = subprocess.run(
        [sys.executable, "-m", "py_compile", str(TARGET)],
        capture_output=True, text=True,
    )
    if res.returncode != 0:
        raise SystemExit(
            f"FATAL: py_compile failed post-write:\nstdout={res.stdout}\nstderr={res.stderr}"
        )
    print(f"[OK] py_compile clean: {TARGET}")


def cmd_dry_run() -> int:
    src = _read_target()
    new = _build_patched(src)
    diff = _unified_diff(src, new)
    print("=" * 72)
    print("DRY-RUN — Patch 2 (hold/neutral analyst rating → partial credit)")
    print(f"Target : {TARGET}")
    print(f"Backup would be written to : {BACKUP}")
    print("=" * 72)
    print(diff if diff else "(no diff — already patched?)")
    print("=" * 72)
    print("Re-run with --apply to actually write the patch.")
    return 0


def cmd_apply() -> int:
    src = _read_target()
    new = _build_patched(src)
    if src == new:
        print("[NOOP] target already matches patched form. Nothing to do.")
        return 0
    if BACKUP.exists():
        print(f"[WARN] backup already exists at {BACKUP} — leaving it untouched.")
    else:
        shutil.copy2(TARGET, BACKUP)
        print(f"[OK] backup -> {BACKUP}")
    TARGET.write_text(new, encoding="utf-8")
    print(f"[OK] wrote patched {TARGET}")
    _py_compile_check()
    return 0


def cmd_revert() -> int:
    if not BACKUP.exists():
        candidates = sorted(TARGET.parent.glob("quality_gate.py.bak.hold.*"))
        if not candidates:
            raise SystemExit(f"FATAL: no Patch-2 backup found for {TARGET}")
        latest = candidates[-1]
        print(f"[INFO] today's backup {BACKUP} not found; using latest {latest}")
        shutil.copy2(latest, TARGET)
    else:
        shutil.copy2(BACKUP, TARGET)
        print(f"[OK] restored {TARGET} from {BACKUP}")
    _py_compile_check()
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--apply", action="store_true",
                     help="Actually write the patch (default is dry-run).")
    grp.add_argument("--revert", action="store_true",
                     help="Restore engine/quality_gate.py from backup.")
    grp.add_argument("--dry-run", action="store_true",
                     help="Print the unified diff (this is the default).")
    args = parser.parse_args(argv)

    if args.revert:
        return cmd_revert()
    if args.apply:
        return cmd_apply()
    return cmd_dry_run()


if __name__ == "__main__":
    raise SystemExit(main())
