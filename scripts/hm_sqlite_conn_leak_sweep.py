#!/usr/bin/env python3
"""HM-SQLITE-CONN-FD-LEAK mechanical sweep (2026-07-06, Admiral-approved).

Fixes the repo-wide `with _conn() as c:` / `with sqlite3.connect(...) as c:`
anti-pattern: a raw sqlite3.Connection used as a context manager only
commits/rolls back the transaction on exit -- it never calls close(). Every
site leaks a connection on every single call, unconditionally.

Fix: wrap the connection expression in contextlib.closing(...), which DOES
call close() on exit while preserving identical behavior inside the block.
Minimal diff -- one line changed per site, no reindentation.

Dry-run by default: prints a unified diff + per-file site count, does NOT
write. Pass --apply to write the changes, then py_compile every touched
file and report pass/fail per file.
"""
import sys
import re
import difflib
import subprocess
from pathlib import Path

ROOT = Path.home() / "autonomous-trader"

TARGET_FILES = [
    "engine/full_universe.py", "engine/gex_overlay.py", "engine/scenario_modeler.py",
    "engine/wheel_strategy.py", "engine/volume_baselines.py", "engine/deep_scan.py",
    "engine/portfolio_optimizer.py", "engine/risk_var.py", "engine/rebalancer.py",
    "engine/cash_manager.py", "engine/universe.py", "engine/battle_station.py",
    "engine/drift_rebalancer.py", "engine/external_intel_signal.py",
    "engine/strategy_rotator.py", "engine/bridge_vote.py", "engine/volume_scanner.py",
    "engine/generated_assets.py", "engine/tax_harvester.py", "engine/pipeline.py",
    "engine/sub_portfolio.py", "engine/dayblade_scanner.py", "dashboard/app.py",
    "scripts/build_corpus_from_trader_db.py",
]

# Matches: <indent>with <expr> as <var>:
#   <expr> is _conn(...) or sqlite3.connect(...) -- single-line calls only
#   (confirmed no multi-line variants exist via a pre-check).
PATTERN = re.compile(
    r'^(?P<indent>\s*)with\s+'
    r'(?P<expr>_conn\(\)|sqlite3\.connect\((?:[^()]|\([^()]*\))*\))'
    r'\s+as\s+(?P<var>\w+)\s*:\s*$'
)


def fix_file(path: Path):
    """Returns (new_lines, n_sites) or (None, 0) if no changes."""
    lines = path.read_text().splitlines(keepends=True)
    n_sites = 0
    out = []
    for line in lines:
        m = PATTERN.match(line)
        if m:
            n_sites += 1
            newline = "\n" if line.endswith("\n") else ""
            out.append(
                f"{m.group('indent')}with contextlib.closing({m.group('expr')}) "
                f"as {m.group('var')}:{newline}"
            )
        else:
            out.append(line)
    if n_sites == 0:
        return None, 0

    has_contextlib = any(
        re.match(r'^\s*import contextlib\s*$', l) for l in out
    )
    if not has_contextlib:
        # Anchor after 'import sqlite3' if present, else after the last
        # top-of-file `from __future__ import ...` line, else at the top.
        inserted = False
        for i, l in enumerate(out):
            if re.match(r'^\s*import sqlite3\s*$', l):
                out.insert(i + 1, "import contextlib\n")
                inserted = True
                break
        if not inserted:
            for i, l in enumerate(out):
                if not re.match(r'^\s*(from __future__ import|#|""").*$', l) and l.strip() != "":
                    out.insert(i, "import contextlib\n")
                    inserted = True
                    break
        if not inserted:
            out.insert(0, "import contextlib\n")

    return out, n_sites


def main():
    apply = "--apply" in sys.argv
    total_sites = 0
    results = []

    for rel in TARGET_FILES:
        path = ROOT / rel
        original = path.read_text()
        new_lines, n_sites = fix_file(path)
        if new_lines is None:
            results.append((rel, 0, None))
            continue
        total_sites += n_sites
        new_content = "".join(new_lines)
        diff = list(difflib.unified_diff(
            original.splitlines(keepends=True), new_lines,
            fromfile=f"a/{rel}", tofile=f"b/{rel}",
        ))
        results.append((rel, n_sites, (new_content, diff)))

    print(f"{'FILE':45} {'SITES':6}")
    for rel, n_sites, payload in results:
        if n_sites:
            print(f"{rel:45} {n_sites:6}")
    print(f"\nTOTAL SITES: {total_sites}")

    print("\n" + "=" * 70)
    print("FULL DIFF")
    print("=" * 70)
    for rel, n_sites, payload in results:
        if not n_sites:
            continue
        _, diff = payload
        print(f"\n--- {rel} ---")
        sys.stdout.writelines(diff)

    if not apply:
        print("\n[DRY RUN] pass --apply to write + compile-check")
        return

    print("\n" + "=" * 70)
    print("APPLYING")
    print("=" * 70)
    compile_results = []
    for rel, n_sites, payload in results:
        if not n_sites:
            continue
        new_content, _ = payload
        path = ROOT / rel
        path.write_text(new_content)
        r = subprocess.run(
            [sys.executable, "-m", "py_compile", str(path)],
            capture_output=True, text=True,
        )
        ok = r.returncode == 0
        compile_results.append((rel, ok, r.stderr))
        status = "OK" if ok else "FAIL"
        print(f"  [{status}] {rel} ({n_sites} sites)")
        if not ok:
            print(f"    {r.stderr}")

    n_fail = sum(1 for _, ok, _ in compile_results if not ok)
    print(f"\n{len(compile_results)} files written, {n_fail} compile failures")


if __name__ == "__main__":
    main()
