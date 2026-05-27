#!/usr/bin/env python3
"""patch_movers_meta_predicate.py — Draft Lane B2 patch for HM-DASH-MOVERS.

The movers endpoint at `dashboard/app.py:1423` computes the "with_metadata"
counter using `market_cap is not None` as the enrichment predicate. ETFs
have no market_cap by design (per feedback_etf_market_cap_lookup.md —
Polygon/yfinance return None for ETF market_cap; use AUM, not cap).
The result: every ETF in the movers feed is counted as "metadata_pending"
even when its ticker_metadata row is fully written.

This patch flips the predicate to `ticker_type is not None` — a field
that ticker_metadata enrichment writes for both CS and ETF rows.

DEFAULT: dry-run. Use --apply to write. --revert restores from backup.

Invoke via venv/bin/python3 scripts/patch_movers_meta_predicate.py [--apply|--revert]
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
TARGET = PROJECT_ROOT / "dashboard" / "app.py"
TODAY = _dt.date.today().isoformat()
BACKUP = TARGET.with_suffix(f".py.bak.movers_meta.{TODAY}")

# Exact line to flip (currently at dashboard/app.py:1423).
OLD_LINE = '        with_meta = sum(1 for m in movers if m.get("market_cap") is not None)\n'
NEW_LINE = '        with_meta = sum(1 for m in movers if m.get("ticker_type") is not None)\n'


def _read_target() -> str:
    return TARGET.read_text(encoding="utf-8")


def _build_patched(src: str) -> str:
    if OLD_LINE not in src:
        raise SystemExit(
            f"FATAL: expected OLD_LINE not found verbatim in {TARGET}.\n"
            f"Source may have drifted; refusing to apply. OLD_LINE was:\n"
            f"  {OLD_LINE!r}"
        )
    if src.count(OLD_LINE) > 1:
        raise SystemExit(
            f"FATAL: OLD_LINE matches {src.count(OLD_LINE)}× in {TARGET}; "
            "single replacement would be ambiguous. Refusing to apply."
        )
    return src.replace(OLD_LINE, NEW_LINE, 1)


def _unified_diff(old: str, new: str) -> str:
    return "".join(
        difflib.unified_diff(
            old.splitlines(keepends=True),
            new.splitlines(keepends=True),
            fromfile="a/dashboard/app.py",
            tofile="b/dashboard/app.py",
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
    print("DRY-RUN — Lane B2 movers-meta predicate (market_cap → ticker_type)")
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
        candidates = sorted(TARGET.parent.glob("app.py.bak.movers_meta.*"))
        if not candidates:
            raise SystemExit(f"FATAL: no movers-meta backup found for {TARGET}")
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
                     help="Restore dashboard/app.py from backup.")
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
