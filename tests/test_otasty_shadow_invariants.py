"""SC-7 #1 + #4 — O-Tasty shadow-machine safety invariants.

Reconciles c7c36ff (SwingDesk Phase 2 — Alpaca execution wiring, default-OFF)
with the original zero-order guard. The module is now DUAL-ROLE:

  * ALWAYS-ON shadow emit — every Loop-B row is written to
    swingdesk_shadow_trades regardless of any flag (unchanged behavior).
  * DEFAULT-OFF paper executor — a broker submit path exists but is inert
    unless LIVE_EXECUTION_ENABLED is flipped, and it may ONLY ever route to
    the isolated PA3YVDTUH5CB *paper* account — never the fleet, never Schwab,
    never real money.

The old absolute "zero broker call / zero alpaca import" invariant is replaced
by the property that actually keeps the Captain safe post-Phase-2:

  1a. FLAG PINNED FALSE — LIVE_EXECUTION_ENABLED is a literal False at module
      scope. Flipping it live must be a deliberate, reviewable one-line diff
      that trips this gate.
  1b. NO UNGATED SUBMIT — every order-submission call is lexically nested under
      `if LIVE_EXECUTION_ENABLED:`. A submit added outside the gate fails CI.
  1c. PAPER-ONLY CLIENT — every TradingClient(...) is constructed paper=True.
  1d. OTASTY-SCOPED CREDS — alpaca creds are read only from OTASTY_-prefixed
      env vars; a bare fleet cred name (APCA_*/ALPACA_*) fails CI.
  1e. ACCOUNT PINNED — the only Alpaca account id referenced is PA3YVDTUH5CB.
  1f. NO SCHWAB / NO FLEET CLIENT — no schwab surface and no fleet broker-submit
      wrapper (alpaca_bridge / alpaca_options / paper_trader) is imported.

  4.  EXIT_DTE / VALID_EXIT_REASONS coupling: Loop C emits `f"time_{EXIT_DTE}dte"`
      and Loop E audits exit_reason against VALID_EXIT_REASONS. If EXIT_DTE is
      ever retuned, the generated reason would fall out of the valid set and
      Loop E would flag every time-exit as a compliance violation + NTFY-storm
      the admin. Pin the coupling so the trap can't reopen.

The checks are factored into pure (tree -> offenders) helpers so the Phase-4
tamper harness can re-run the EXACT gate logic against mutated source and prove
it goes red.
"""
import ast
import re
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
_SWINGDESK = _REPO / "swingdesk"
if str(_SWINGDESK) not in sys.path:
    sys.path.insert(0, str(_SWINGDESK))

_SHADOW_SRC = _SWINGDESK / "shadow_autopilot.py"

_FLAG = "LIVE_EXECUTION_ENABLED"
_ACCOUNT = "PA3YVDTUH5CB"

# Function names that actually place/route a broker order. Read-only calls
# (get_*/build_*/calc_*/get_order_by_id) are intentionally NOT here.
_FORBIDDEN_SUBMIT = {
    "submit_order", "place_order", "create_order", "submit_market_order",
    "execute_options_signal", "execute_signal", "_forward_to_alpaca",
}
# Project-local fleet broker-submit wrappers. The first-party alpaca.* SDK
# (alpaca.trading.* / alpaca.data.*) is the SANCTIONED paper path and allowed;
# these wrappers route fleet/real orders and must never appear here.
_FORBIDDEN_IMPORT_SUBSTR = ("alpaca_bridge", "alpaca_options", "paper_trader")
# Substrings marking a credential surface; non-OTASTY ones risk the fleet acct.
_FLEET_CRED_MARKERS = ("APCA", "ALPACA")


def _shadow_tree() -> ast.Module:
    return ast.parse(_SHADOW_SRC.read_text(), filename=str(_SHADOW_SRC))


def _shadow_text() -> str:
    return _SHADOW_SRC.read_text()


# ── pure helpers (source -> offenders) — reused by the tamper harness ────────
def _call_name(node: ast.Call):
    fn = node.func
    if isinstance(fn, ast.Name):
        return fn.id
    if isinstance(fn, ast.Attribute):
        return fn.attr
    return None


def flag_assignment_value(tree: ast.Module):
    """The AST value node assigned to LIVE_EXECUTION_ENABLED at module scope,
    or None if unassigned."""
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == _FLAG:
                    return node.value
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.target.id == _FLAG:
                return node.value
    return None


def positive_gate_if_nodes(tree: ast.Module):
    """If-nodes whose test is exactly `LIVE_EXECUTION_ENABLED` (the positive
    gate). `if not LIVE_EXECUTION_ENABLED:` early-returns are NOT gates."""
    return [
        node for node in ast.walk(tree)
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.Name)
        and node.test.id == _FLAG
    ]


def ungated_submit_calls(tree: ast.Module):
    """Forbidden-submit calls NOT lexically inside a positive gate."""
    gated = set()
    for gate in positive_gate_if_nodes(tree):
        for n in ast.walk(gate):
            if isinstance(n, ast.Call) and _call_name(n) in _FORBIDDEN_SUBMIT:
                gated.add((_call_name(n), n.lineno))
    offenders = []
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and _call_name(n) in _FORBIDDEN_SUBMIT:
            key = (_call_name(n), n.lineno)
            if key not in gated:
                offenders.append(f"{key[0]} @ line {key[1]}")
    return offenders


def non_paper_trading_clients(tree: ast.Module):
    """TradingClient(...) constructions lacking an explicit paper=True."""
    bad = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _call_name(node) == "TradingClient":
            kw = {k.arg: k.value for k in node.keywords if k.arg}
            paper = kw.get("paper")
            ok = isinstance(paper, ast.Constant) and paper.value is True
            if not ok:
                bad.append(f"TradingClient(paper!=True) @ line {node.lineno}")
    return bad


def _env_read_literals(tree: ast.Module):
    """String-literal names passed to os.environ.get / os.getenv (skips
    non-literal lookups like os.environ.setdefault(k.strip(), ...))."""
    names = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr in ("get", "getenv") and node.args:
                first = node.args[0]
                if isinstance(first, ast.Constant) and isinstance(first.value, str):
                    names.append((first.value, node.lineno))
    return names


def fleet_cred_reads(tree: ast.Module):
    """Alpaca-cred env reads that are NOT OTASTY_-scoped."""
    bad = []
    for name, lineno in _env_read_literals(tree):
        if any(m in name for m in _FLEET_CRED_MARKERS) and not name.startswith("OTASTY_"):
            bad.append(f"{name} @ line {lineno}")
    return bad


def forbidden_module_imports(tree: ast.Module):
    """Fleet broker-submit wrapper imports (alpaca.* SDK is allowed)."""
    bad = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            if any(s in node.module for s in _FORBIDDEN_IMPORT_SUBSTR):
                bad.append(f"from {node.module} @ line {node.lineno}")
        elif isinstance(node, ast.Import):
            for a in node.names:
                if any(s in a.name for s in _FORBIDDEN_IMPORT_SUBSTR):
                    bad.append(f"import {a.name} @ line {node.lineno}")
    return bad


def _docstring_const_ids(tree: ast.Module):
    """id()s of Constant nodes that are module/class/function docstrings — prose
    that may legitimately *document* a prohibition (e.g. "never Schwab")."""
    ids = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef,
                             ast.ClassDef)):
            body = getattr(node, "body", None)
            if body and isinstance(body[0], ast.Expr) \
                    and isinstance(body[0].value, ast.Constant) \
                    and isinstance(body[0].value.value, str):
                ids.add(id(body[0].value))
    return ids


def schwab_references(tree: ast.Module):
    """Any schwab CODE surface (import / identifier / non-docstring string).
    Docstrings and comments may discuss the prohibition; code may not use it."""
    doc_ids = _docstring_const_ids(tree)
    hits = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and "schwab" in node.module.lower():
            hits.append(f"from {node.module} @ line {node.lineno}")
        elif isinstance(node, ast.Import):
            for a in node.names:
                if "schwab" in a.name.lower():
                    hits.append(f"import {a.name} @ line {node.lineno}")
        elif isinstance(node, ast.Name) and "schwab" in node.id.lower():
            hits.append(f"name {node.id} @ line {node.lineno}")
        elif isinstance(node, ast.Attribute) and "schwab" in node.attr.lower():
            hits.append(f"attr {node.attr} @ line {node.lineno}")
        elif isinstance(node, ast.Constant) and isinstance(node.value, str) \
                and id(node) not in doc_ids and "schwab" in node.value.lower():
            hits.append(f"string @ line {node.lineno}")
    return hits


def account_ids(text: str):
    """Distinct Alpaca-style account ids (PA + 10 alnum) referenced in source."""
    return set(re.findall(r"\bPA[0-9A-Z]{10}\b", text))


# ── invariants (1a–1f) ──────────────────────────────────────────────────────
def test_flag_pinned_false():
    """1a — LIVE_EXECUTION_ENABLED is a literal False at module scope."""
    value = flag_assignment_value(_shadow_tree())
    assert value is not None, f"{_FLAG} must be assigned at module scope."
    assert isinstance(value, ast.Constant) and value.value is False, (
        f"{_FLAG} must be pinned to a literal False (Phase-2 default-OFF). "
        "Flipping it live must be a deliberate, reviewed one-line diff."
    )


def test_no_ungated_submit():
    """1b — every broker-submit call is nested under `if LIVE_EXECUTION_ENABLED:`."""
    offenders = ungated_submit_calls(_shadow_tree())
    assert not offenders, (
        "shadow_autopilot.py has an order-submit call OUTSIDE the "
        f"`if {_FLAG}:` gate. Every submit must be flag-gated. Found: {offenders}"
    )


def test_trading_client_is_paper_only():
    """1c — every TradingClient is constructed paper=True."""
    bad = non_paper_trading_clients(_shadow_tree())
    assert not bad, (
        "shadow_autopilot.py must construct TradingClient with paper=True "
        f"(PA3YVDTUH5CB paper only — never a live trading client). Found: {bad}"
    )


def test_alpaca_creds_are_otasty_scoped():
    """1d — alpaca creds come only from OTASTY_-prefixed env vars."""
    bad = fleet_cred_reads(_shadow_tree())
    assert not bad, (
        "shadow_autopilot.py reads an Alpaca credential that is NOT "
        "OTASTY_-scoped — that risks binding to the fleet account. "
        f"Use OTASTY_APCA_* only. Found: {bad}"
    )


def test_account_pinned_to_pa3yvdtuh5cb():
    """1e — the only Alpaca account id referenced is the isolated paper one."""
    ids = account_ids(_shadow_text())
    assert _ACCOUNT in ids, (
        f"Expected the pinned paper account {_ACCOUNT} to be referenced."
    )
    assert ids == {_ACCOUNT}, (
        "shadow_autopilot.py references an Alpaca account id other than the "
        f"isolated paper account {_ACCOUNT}: {sorted(ids - {_ACCOUNT})}"
    )


def test_no_schwab_or_fleet_client():
    """1f — no schwab surface and no fleet broker-submit wrapper import."""
    tree = _shadow_tree()
    schwab = schwab_references(tree)
    assert not schwab, (
        "shadow_autopilot.py must never use Schwab in code — it is Alpaca "
        f"paper (PA3YVDTUH5CB) only. Found: {schwab}"
    )
    bad = forbidden_module_imports(tree)
    assert not bad, (
        "shadow_autopilot.py must not import a fleet broker-submit wrapper "
        f"({'/'.join(_FORBIDDEN_IMPORT_SUBSTR)}). The alpaca.* SDK is the "
        f"sanctioned paper path. Found: {bad}"
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
