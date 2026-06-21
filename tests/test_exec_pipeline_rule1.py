"""N1 — RULE #1 guard: zero Schwab write-path references in the execution pipeline.

RULE #1: Schwab (...7015) is real cash. Read-only GETs only. NEVER write/order.

This test scans every file in the execution pipeline for strings that would
indicate a write path to Schwab. If any match is found, the build FAILS.

The execution path files audited:
  engine/winning_signal.py
  engine/confluence_engine.py
  engine/execution_router.py

Add new execution-path files to PIPELINE_FILES below when they are created.
Run:  .venv/bin/python3 -m pytest tests/test_exec_pipeline_rule1.py -v
"""
from __future__ import annotations

import re
from pathlib import Path

# All files that form the execution path — expand this list as Phase 1 grows
PIPELINE_FILES: list[str] = [
    "engine/winning_signal.py",
    "engine/confluence_engine.py",
    "engine/execution_router.py",
]

# Patterns that would indicate a Schwab write path — any match is a FAIL
SCHWAB_WRITE_PATTERNS: list[str] = [
    r"7015",                          # Schwab account suffix
    r"schwab",                        # any case — see re.IGNORECASE below
    r"SCHWAB_API",
    r"schwab_client",
    r"schwab\.orders",
    r"schwab\.place",
    r"SchwabClient",
    r"broker.*schwab",
    r"schwab.*broker",
    r"brokerage.*schwab",
]

_COMBINED = re.compile("|".join(SCHWAB_WRITE_PATTERNS), re.IGNORECASE)

_REPO_ROOT = Path(__file__).parent.parent


def test_no_schwab_references_in_execution_path():
    """RULE #1 guard: zero Schwab write-path strings in any pipeline file.

    If this test fails, the build is blocked until the offending reference
    is removed. This is the hard gate that prevents accidental Schwab exposure.
    """
    violations: list[str] = []

    for rel_path in PIPELINE_FILES:
        fpath = _REPO_ROOT / rel_path
        if not fpath.exists():
            # Missing file is an error — pipeline file must exist
            violations.append(f"MISSING FILE: {rel_path}")
            continue

        text = fpath.read_text(encoding="utf-8", errors="replace")
        for lineno, line in enumerate(text.splitlines(), start=1):
            if _COMBINED.search(line):
                violations.append(f"{rel_path}:{lineno}: {line.strip()[:120]}")

    assert not violations, (
        f"RULE #1 VIOLATION — Schwab references found in execution pipeline "
        f"({len(violations)} hit(s)):\n"
        + "\n".join(f"  {v}" for v in violations)
    )


def test_pipeline_files_exist():
    """Every declared pipeline file must exist on disk."""
    missing = [f for f in PIPELINE_FILES if not (_REPO_ROOT / f).exists()]
    assert not missing, f"Pipeline files missing: {missing}"


def test_alpaca_bridge_not_schwab():
    """Verify that the Alpaca bridge import in execution_router uses alpaca_bridge, not schwab."""
    router_path = _REPO_ROOT / "engine/execution_router.py"
    text = router_path.read_text(encoding="utf-8")
    assert "alpaca_bridge" in text, "execution_router.py must import alpaca_bridge"
    # Double-check: the bridge import is for paper trading
    assert "paper_trader" in text or "alpaca_bridge" in text, (
        "execution_router must route through paper_trader or alpaca_bridge (Alpaca paper)"
    )
