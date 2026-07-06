#!/usr/bin/env python3
"""HM-WAL-BUSY-TIMEOUT-HYGIENE wave 1 (2026-07-06, Admiral-approved, phased).

Migrates each target file's single `_conn()` factory to route its
`sqlite3.connect(...)` call through `engine.db_conn.get_conn(...)`, which
adds `PRAGMA synchronous=NORMAL` (the one setting not already covered by
main.py's process-wide busy_timeout monkeypatch). Only touches the connect()
call line inside `_conn()` -- row_factory / PRAGMA journal_mode / guard
clauses above it are left exactly as-is (minimal diff, same doctrine as the
morning's FD-leak sweep).

Dry-run by default (prints per-file diff preview). Pass --apply to write.
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# path_var mirrors whatever local module-level constant each file already
# uses for its DB path -- captured, not invented, so get_conn() is called
# with the exact same argument the original sqlite3.connect() call used.
TARGETS = [
    "engine/deep_scan.py",
    "engine/bridge_vote.py",
    "engine/battle_station.py",
    "engine/volume_baselines.py",
    "engine/strategy_rotator.py",
    "engine/volume_scanner.py",
    "engine/dayblade_scanner.py",
    "engine/rebalancer.py",
    "engine/gex_overlay.py",
    "engine/full_universe.py",
    "engine/scenario_modeler.py",
    "engine/generated_assets.py",
    "engine/tax_harvester.py",
    "engine/portfolio_optimizer.py",
    "engine/cash_manager.py",
    "engine/drift_rebalancer.py",
    "engine/risk_var.py",
    "engine/universe.py",
    "engine/pipeline.py",
    "engine/sub_portfolio.py",
]

# Matches "sqlite3.connect(ARGS)" -- single-level-nested parens tolerant
# (same regex shape as the morning's leak-sweep script).
CONNECT_RE = re.compile(r"sqlite3\.connect\(((?:[^()]|\([^()]*\))*)\)")
IMPORT_RE = re.compile(r"^import sqlite3\s*$", re.MULTILINE)


def migrate_file(path: Path) -> tuple[str, int]:
    text = path.read_text()
    if "from engine.db_conn import get_conn" in text or "from engine import db_conn" in text:
        return text, 0  # already migrated

    # Only touch the connect() call that lives inside _conn()'s body -- find
    # the function, then substitute within that slice only, so this can never
    # accidentally touch an unrelated sqlite3.connect() elsewhere in the file
    # (e.g. battle_station.py / gex_overlay.py have a second, different-purpose
    # connect site outside _conn() that wave 1 deliberately does not touch).
    m = re.search(r"^def _conn\(.*?\n(?:.*\n)*?", text, re.MULTILINE)
    if not m:
        return text, 0
    func_start = m.start()
    # Function body ends at the next top-level "def "/"class " or EOF.
    next_def = re.search(r"\n(?:def |class )", text[func_start + 4:])
    func_end = func_start + 4 + next_def.start() if next_def else len(text)
    func_slice = text[func_start:func_end]

    conn_match = CONNECT_RE.search(func_slice)
    if not conn_match:
        return text, 0

    new_func_slice = (
        func_slice[:conn_match.start()]
        + f"get_conn({conn_match.group(1)})"
        + func_slice[conn_match.end():]
    )
    new_text = text[:func_start] + new_func_slice + text[func_end:]

    # Insert the import right after "import sqlite3" (every target file has one).
    new_text, n_sub = IMPORT_RE.subn(
        "import sqlite3\nfrom engine.db_conn import get_conn",
        new_text,
        count=1,
    )
    if n_sub == 0:
        # No bare "import sqlite3" line found (unexpected) -- bail rather than
        # silently write an import-less get_conn() call.
        return text, 0

    return new_text, 1


def main() -> int:
    apply = "--apply" in sys.argv
    changed = 0
    for rel in TARGETS:
        path = ROOT / rel
        new_text, n = migrate_file(path)
        if n == 0:
            print(f"[skip] {rel} -- no _conn()/connect() match or already migrated")
            continue
        changed += 1
        if apply:
            path.write_text(new_text)
            print(f"[applied] {rel}")
        else:
            import difflib
            old_lines = path.read_text().splitlines(keepends=True)
            new_lines = new_text.splitlines(keepends=True)
            diff = difflib.unified_diff(old_lines, new_lines, fromfile=rel, tofile=rel, n=1)
            print(f"[dry-run] {rel}")
            print("".join(diff))

    print(f"\n{'Applied' if apply else '[DRY RUN] Would migrate'}: {changed}/{len(TARGETS)} files")
    if not apply:
        print("Pass --apply to write.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
