"""Phase-4 tamper harness for the O-Tasty Phase-2 safety invariants.

Proves the gate logic in test_otasty_shadow_invariants.py actually goes RED when
the safety properties are violated — a clean current source proves the invariant
HOLDS, but only a tamper proof shows the guard would CATCH a regression.

Every case mutates an IN-MEMORY copy of swingdesk/shadow_autopilot.py (the real
file is never written) and re-runs the EXACT helper that gates CI. Covers the
three the Admiral named explicitly — ungated submit / flag True / fleet creds —
plus the remaining guards for completeness.
"""
import ast
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_otasty_shadow_invariants as inv

_SRC = inv._SHADOW_SRC.read_text()


def _tree(src: str) -> ast.Module:
    return ast.parse(src)


def _assert_real_is_clean():
    """Guard the harness itself: mutations must start from a green baseline."""
    tree = _tree(_SRC)
    assert inv.flag_assignment_value(tree).value is False
    assert inv.ungated_submit_calls(tree) == []
    assert inv.non_paper_trading_clients(tree) == []
    assert inv.fleet_cred_reads(tree) == []
    assert inv.schwab_references(tree) == []
    assert inv.forbidden_module_imports(tree) == []
    assert inv.account_ids(_SRC) == {inv._ACCOUNT}


# ── the three the Admiral named ──────────────────────────────────────────────
def test_tamper_flag_true_is_caught():
    _assert_real_is_clean()
    bad = _SRC.replace("LIVE_EXECUTION_ENABLED = False",
                       "LIVE_EXECUTION_ENABLED = True", 1)
    assert bad != _SRC, "tamper precondition: flag assignment must exist"
    value = inv.flag_assignment_value(_tree(bad))
    # test_flag_pinned_false asserts `value.value is False`; flipping to True
    # makes that assertion fail.
    assert not (isinstance(value, ast.Constant) and value.value is False), (
        "flag flipped True must break the pinned-False invariant"
    )


def test_tamper_ungated_submit_is_caught():
    """Remove the flag guard around the live submit (classic refactor bleed).

    Mutates the actual code gate line (matched by stripped content) — NOT the
    docstring, which also mentions `if LIVE_EXECUTION_ENABLED:` as prose.
    """
    _assert_real_is_clean()
    lines = _SRC.splitlines(keepends=True)
    mutated = False
    for i, line in enumerate(lines):
        if line.strip() == "if LIVE_EXECUTION_ENABLED:":
            indent = line[: len(line) - len(line.lstrip())]
            lines[i] = f"{indent}if True:\n"
            mutated = True
            break
    assert mutated, "tamper precondition: a code-level positive gate must exist"
    bad = "".join(lines)
    offenders = inv.ungated_submit_calls(_tree(bad))
    assert any("submit_order" in o for o in offenders), (
        f"ungating the submit must be flagged; got {offenders}"
    )


def test_tamper_fleet_creds_is_caught():
    """Swap OTASTY_-scoped creds for the bare fleet cred name."""
    _assert_real_is_clean()
    bad = _SRC.replace("OTASTY_APCA_API_KEY_ID", "APCA_API_KEY_ID")
    assert bad != _SRC, "tamper precondition: OTASTY cred read must exist"
    offenders = inv.fleet_cred_reads(_tree(bad))
    assert any("APCA_API_KEY_ID" in o for o in offenders), (
        f"a non-OTASTY alpaca cred must be flagged; got {offenders}"
    )


# ── remaining guards (completeness) ──────────────────────────────────────────
def test_tamper_live_trading_client_is_caught():
    _assert_real_is_clean()
    # Anchor on the call-site `paper=True)` — the docstring says "paper=True and",
    # so this uniquely targets the TradingClient construction, not prose.
    bad = _SRC.replace("paper=True)", "paper=False)", 1)
    assert bad != _SRC, "tamper precondition: paper=True) call-site must exist"
    offenders = inv.non_paper_trading_clients(_tree(bad))
    assert offenders, "a non-paper TradingClient must be flagged"


def test_tamper_foreign_account_is_caught():
    _assert_real_is_clean()
    bad = _SRC.replace(inv._ACCOUNT, "PA0000000000", 1)
    assert bad != _SRC, "tamper precondition: account id must exist"
    ids = inv.account_ids(bad)
    assert ids != {inv._ACCOUNT} and "PA0000000000" in ids, (
        f"a foreign Alpaca account id must be flagged; got {sorted(ids)}"
    )


def test_tamper_schwab_code_use_is_caught():
    _assert_real_is_clean()
    bad = _SRC + "\n_TAMPER = schwab_client_submit()\n"
    offenders = inv.schwab_references(_tree(bad))
    assert offenders, "a schwab code identifier must be flagged"


def test_tamper_schwab_in_docstring_is_allowed():
    """The schwab guard must NOT false-positive on prose documenting the ban —
    the real module's own docstring says 'never Schwab'."""
    assert inv.schwab_references(_tree(_SRC)) == [], (
        "documenting the Schwab prohibition in a docstring must stay green"
    )


def test_tamper_fleet_wrapper_import_is_caught():
    _assert_real_is_clean()
    bad = _SRC.replace("from __future__ import annotations",
                       "from __future__ import annotations\nfrom alpaca_bridge import buy", 1)
    assert bad != _SRC, "tamper precondition: anchor import must exist"
    offenders = inv.forbidden_module_imports(_tree(bad))
    assert any("alpaca_bridge" in o for o in offenders), (
        f"a fleet broker-submit wrapper import must be flagged; got {offenders}"
    )
