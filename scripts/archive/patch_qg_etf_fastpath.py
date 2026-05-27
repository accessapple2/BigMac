#!/usr/bin/env python3
"""patch_qg_etf_fastpath.py — Draft Patch 1 for HM-QG-CALIBRATION.

Replaces the dead-code ETF fast-path in engine/quality_gate.py with a
shape-based ETF detector. Yahoo quoteSummary returns a truthy dict for
ETFs but with earnings_growth=None, revenue_growth=None,
recommendation=None and sector="Unknown" — so the existing
`if fund is None:` branch never matches and ETFs route through the
stock-fundamentals path (mathematically capped at 2/5).

DEFAULT: dry-run. Use --apply to write. --revert restores from backup.

Invoke via venv/bin/python3 scripts/patch_qg_etf_fastpath.py [--apply|--revert]
"""
from __future__ import annotations

import argparse
import datetime as _dt
import difflib
import os
import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TARGET = PROJECT_ROOT / "engine" / "quality_gate.py"
TODAY = _dt.date.today().isoformat()
BACKUP = TARGET.with_suffix(f".py.bak.{TODAY}")

# --- The exact OLD block we replace (lines ~36-71 of quality_gate.py V3). ---
OLD_BLOCK = '''    # 1. Earnings beat (positive earnings growth = recent beat)
    try:
        from engine.stock_fundamentals import fetch_fundamentals
        fund = fetch_fundamentals(symbol)
        if fund:
            eg = fund.get("earnings_growth")
            if eg is not None and eg > 0:
                score += 1
                details.append(f"earnings_growth={eg:+.1f}%")
            elif eg is not None:
                details.append(f"FAIL earnings_growth={eg:.1f}%")
            else:
                details.append("SKIP earnings_growth=N/A")
                score += 0.5  # Partial credit for missing data

            # 2. Revenue growing (positive YoY)
            rg = fund.get("revenue_growth")
            if rg is not None and rg > 0:
                score += 1
                details.append(f"revenue_growth={rg:+.1f}%")
            elif rg is not None:
                details.append(f"FAIL revenue_growth={rg:.1f}%")
            else:
                details.append("SKIP revenue_growth=N/A")
                score += 0.5

            # 4. Analyst consensus Buy or Strong Buy
            rec = fund.get("recommendation", "")
            if rec and rec.lower() in ("buy", "strongbuy", "strong_buy", "overweight"):
                score += 1
                details.append(f"analyst={rec}")
            elif rec:
                details.append(f"FAIL analyst={rec}")
            else:
                details.append("SKIP analyst=N/A")
        else:
            # No fundamentals available — give partial credit
            score += 1.5
            details.append("fundamentals unavailable — partial pass")
    except Exception as e:
        score += 1.5
        details.append(f"fundamentals error: {e}")
'''

# --- The NEW block (Patch 1 — shape-based ETF detector). ---
NEW_BLOCK = '''    # 1. Earnings beat (positive earnings growth = recent beat)
    try:
        from engine.stock_fundamentals import fetch_fundamentals
        fund = fetch_fundamentals(symbol)
        # Patch 1 (HM-QG-CALIBRATION): shape-based ETF detector.
        # Yahoo quoteSummary returns a truthy dict for ETFs with all
        # numeric/text fundamental fields None — so the historical
        # `if fund is None:` ETF fast-path was dead code. Detect ETFs
        # by signature instead and short-circuit to an RSI-only gate.
        if fund is not None:
            _eg = fund.get("earnings_growth")
            _rg = fund.get("revenue_growth")
            _rec = fund.get("recommendation")
            _sector = fund.get("sector")
            is_etf_like = (
                _eg is None
                and _rg is None
                and (_rec is None or _rec == "")
                and (_sector is None or _sector == "" or _sector == "Unknown")
            )
            if is_etf_like:
                rsi = indicators.get("rsi")
                if rsi is not None and rsi >= 70:
                    return False, 0, [f"ETF-shape FAIL: RSI={rsi:.0f} (overbought)"]
                if rsi is not None:
                    return True, 5, [f"ETF-shape (RSI={rsi:.0f} OK), exempt"]
                return True, 5, ["ETF-shape (no RSI), exempt"]
        if fund:
            eg = fund.get("earnings_growth")
            if eg is not None and eg > 0:
                score += 1
                details.append(f"earnings_growth={eg:+.1f}%")
            elif eg is not None:
                details.append(f"FAIL earnings_growth={eg:.1f}%")
            else:
                details.append("SKIP earnings_growth=N/A")
                score += 0.5  # Partial credit for missing data

            # 2. Revenue growing (positive YoY)
            rg = fund.get("revenue_growth")
            if rg is not None and rg > 0:
                score += 1
                details.append(f"revenue_growth={rg:+.1f}%")
            elif rg is not None:
                details.append(f"FAIL revenue_growth={rg:.1f}%")
            else:
                details.append("SKIP revenue_growth=N/A")
                score += 0.5

            # 4. Analyst consensus Buy or Strong Buy
            rec = fund.get("recommendation", "")
            if rec and rec.lower() in ("buy", "strongbuy", "strong_buy", "overweight"):
                score += 1
                details.append(f"analyst={rec}")
            elif rec:
                details.append(f"FAIL analyst={rec}")
            else:
                details.append("SKIP analyst=N/A")
        else:
            # No fundamentals available — give partial credit
            score += 1.5
            details.append("fundamentals unavailable — partial pass")
    except Exception as e:
        score += 1.5
        details.append(f"fundamentals error: {e}")
'''


def _read_target() -> str:
    return TARGET.read_text(encoding="utf-8")


def _build_patched(src: str) -> str:
    if OLD_BLOCK not in src:
        raise SystemExit(
            f"FATAL: expected OLD_BLOCK not found verbatim in {TARGET}. "
            "The source has drifted from the snapshot this patch was drafted "
            "against (engine/quality_gate.py V3, 108 LOC, 2026-05-14). "
            "Refusing to apply."
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
    print("DRY-RUN — Patch 1 (ETF shape-based fast-path)")
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
        # Try the most recent .bak.* file for this script's target.
        candidates = sorted(TARGET.parent.glob("quality_gate.py.bak.*"))
        if not candidates:
            raise SystemExit(f"FATAL: no backup found for {TARGET}")
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
