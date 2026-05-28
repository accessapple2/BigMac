"""SC-7 #1 + #4 — O-Tasty shadow-machine safety invariants.

Two load-bearing properties of swingdesk/shadow_autopilot.py that currently
have ZERO test coverage:

  1. ZERO-ORDER INVARIANT (highest value): the module exists to SHADOW O-Tasty
     premium-selling against PA3YVDTUH5CB without ever submitting an order. A
     future refactor that adds a broker submit must fail CI, not ship silently.

  4. EXIT_DTE / VALID_EXIT_REASONS coupling: Loop C emits `f"time_{EXIT_DTE}dte"`
     and Loop E audits exit_reason against VALID_EXIT_REASONS. If EXIT_DTE is
     ever retuned, the generated reason would fall out of the valid set and
     Loop E would flag every time-exit as a compliance violation + NTFY-storm
     the admin. Pin the coupling so the trap can't reopen.
"""
import ast
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
_SWINGDESK = _REPO / "swingdesk"
if str(_SWINGDESK) not in sys.path:
    sys.path.insert(0, str(_SWINGDESK))

_SHADOW_SRC = _SWINGDESK / "shadow_autopilot.py"

# Function names that would actually place/route a broker order. The module may
# legitimately reference build_*/calc_*/get_* (read-only compute) — only these
# submit-path names are forbidden.
_FORBIDDEN_CALLS = {
    "submit_order", "place_order", "create_order", "submit_market_order",
    "execute_options_signal", "execute_signal", "_forward_to_alpaca",
}
# Any import whose module path contains one of these is a broker-submit surface.
_FORBIDDEN_IMPORT_SUBSTR = ("alpaca", "alpaca_options", "alpaca_bridge")


def _shadow_tree() -> ast.Module:
    return ast.parse(_SHADOW_SRC.read_text(), filename=str(_SHADOW_SRC))


def test_zero_order_invariant_no_broker_calls():
    """No call to any order-submission function anywhere in the module."""
    tree = _shadow_tree()
    offenders = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            fn = node.func
            name = None
            if isinstance(fn, ast.Name):
                name = fn.id
            elif isinstance(fn, ast.Attribute):
                name = fn.attr
            if name in _FORBIDDEN_CALLS:
                offenders.append(f"{name} @ line {node.lineno}")
    assert not offenders, (
        "shadow_autopilot.py must NEVER call an order-submit function "
        f"(SHADOW only). Found: {offenders}"
    )


def test_zero_order_invariant_no_broker_imports():
    """No import of any alpaca/broker submission module."""
    tree = _shadow_tree()
    bad = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            if any(s in node.module for s in _FORBIDDEN_IMPORT_SUBSTR):
                bad.append(f"from {node.module} @ line {node.lineno}")
        elif isinstance(node, ast.Import):
            for a in node.names:
                if any(s in a.name for s in _FORBIDDEN_IMPORT_SUBSTR):
                    bad.append(f"import {a.name} @ line {node.lineno}")
    assert not bad, (
        "shadow_autopilot.py must not import a broker-submit module "
        f"(it is SHADOW only). Found: {bad}"
    )


def test_exit_dte_reason_in_valid_set():
    """Loop C's generated time-exit reason must be in Loop E's valid set.

    Guards the NTFY-storm trap: retuning EXIT_DTE without updating
    VALID_EXIT_REASONS would make every time-exit audit as a violation.
    """
    import options_engine
    import shadow_autopilot
    generated = f"time_{options_engine.EXIT_DTE}dte"
    assert generated in shadow_autopilot.VALID_EXIT_REASONS, (
        f"Loop C emits {generated!r} (from EXIT_DTE={options_engine.EXIT_DTE}) "
        f"but it is NOT in VALID_EXIT_REASONS={shadow_autopilot.VALID_EXIT_REASONS}. "
        "Loop E would flag every time-exit as a compliance violation. "
        "Update VALID_EXIT_REASONS when EXIT_DTE changes."
    )
