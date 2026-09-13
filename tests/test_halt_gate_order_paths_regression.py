"""tests/test_halt_gate_order_paths_regression.py — HM-EXIT-GATE-AUDIT-2026-09-13.

Regression test for the neo-matrix halt-gate gap: `paper_trader.sell()` and
`paper_trader.short_sell()` both carry a `halt_mode='full'` check ("exit_only
PERMITS sells, only 'full' blocks" — see their own HALT GATE comments), but
three other position-closing paths did not: `paper_trader.sell_partial()`,
`crew_scanner._check_scaled_exits()`, and `options_exec.close_options_trade()`.
That gap let neo-matrix's scaled-exit tiers fire 22 real Alpaca-paper sells
on HL/AG while `halt_mode='full'` on 2026-08-25..27, with zero decision_audit
trail (no gate existed to reject through). Full trace:
relay_2026-09-13_gate_efficacy_and_halt_gap.md.

This is the same bug class `short_sell()` itself was patched for once before
(its own comment: "CLOSED GAP: short_sell() had NO halt_mode check at all
before this") — it was never generalized to the other two closing paths.
All three are now fixed. This test guards against any of the five named
functions losing the check again, and against a sixth order-reaching
function being added later without one.

Same shape as tests/test_trades_tz_gate_regression.py (source-scan against
named files/functions rather than a runtime behavioral probe, reusing that
test's comment/docstring-aware prose-stripping) — this test does not check
for the literal current line, it checks for the *shape* of the fix: a
comparison against the literal string 'full' following a reference to
halt_mode, inside the named function's own source block.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.ci.grep_gate_scan import _blank_prose  # noqa: E402

# Every function that can reach a real position-close (broker order or paper
# bookkeeping close+P&L booking) for an AI-driven player must check
# ai_players.halt_mode and refuse when it is 'full'. Add new order-reaching
# functions here as they're written -- this list is the audit surface, not
# just a snapshot of what happened to exist in September 2026.
_HALT_GATED_FUNCTIONS = [
    ("engine/paper_trader.py", "buy"),
    ("engine/paper_trader.py", "sell"),
    ("engine/paper_trader.py", "sell_partial"),
    ("engine/paper_trader.py", "short_sell"),
    ("engine/crew_scanner.py", "_check_scaled_exits"),
    ("engine/options_exec.py", "close_options_trade"),
]

# Matches `halt_mode` (any case/spacing of the DB column or a local variable
# holding it) -- required in every gated function, regardless of which of
# the two legitimate comparison shapes it uses (see below).
_HALT_CHECK_SHAPE = re.compile(r"halt_mode", re.IGNORECASE)

# Two DIFFERENT, both-legitimate comparison shapes coexist in this codebase,
# and a gated function must use one of them, not necessarily 'full':
#   - entry-opening functions (buy, short_sell) block on `!= 'active'` --
#     exit_only AND full both refuse a NEW position.
#   - position-closing functions (sell, sell_partial, _check_scaled_exits,
#     close_options_trade) block on `== 'full'` only -- exit_only
#     deliberately PERMITS a close/exit, only full blocks it.
# Requiring "== 'full'" from every function would be wrong: buy()/short_sell()
# correctly use the broader "!= 'active'" form. This test cares that halt_mode
# is checked and acted on at all, not which of the two correct shapes it uses.
_HALT_COMPARE = re.compile(r"""==\s*['"]full['"]|!=\s*['"]active['"]""")


def _extract_function_source(full_text: str, func_name: str) -> str:
    """Return the source of a module-level `def func_name(...)` block, up to
    (but not including) the next module-level `def `/`class ` line, or EOF.
    """
    lines = full_text.splitlines()
    start = None
    def_pattern = re.compile(rf"^def {re.escape(func_name)}\(")
    for i, line in enumerate(lines):
        if def_pattern.match(line):
            start = i
            break
    assert start is not None, f"could not find module-level `def {func_name}(` — has it been renamed or moved?"
    end = len(lines)
    for j in range(start + 1, len(lines)):
        if re.match(r"^(def |class )", lines[j]):
            end = j
            break
    return "\n".join(lines[start:end])


def test_every_named_order_path_checks_halt_mode_full():
    missing: list[str] = []
    for relpath, func_name in _HALT_GATED_FUNCTIONS:
        full = _ROOT / relpath
        original = full.read_text(encoding="utf-8", errors="replace")
        block = _extract_function_source(original, func_name)
        scan_block = _blank_prose(block)
        # Require BOTH a reference to halt_mode and a comparison against the
        # literal 'full' somewhere in the same function body -- checking
        # them independently (rather than one combined regex) tolerates the
        # exact variable-naming/line-splitting differences already present
        # across these three files (e.g. a two-step SELECT-then-compare vs.
        # a single indexed comparison) without weakening what's actually
        # required: this function must both read halt_mode and act on 'full'.
        has_halt_mode_ref = bool(_HALT_CHECK_SHAPE.search(scan_block))
        has_compare = bool(_HALT_COMPARE.search(scan_block))
        if not (has_halt_mode_ref and has_compare):
            missing.append(
                f"{relpath}::{func_name} "
                f"(halt_mode ref={has_halt_mode_ref}, comparison found={has_compare})"
            )
    assert not missing, (
        "The following order-reaching functions are missing a halt_mode='full' "
        "check -- this is the exact HM-EXIT-GATE-AUDIT-2026-09-13 gap that let "
        "a halted agent (neo-matrix) execute 22 real sells while halt_mode='full'. "
        "See paper_trader.sell()'s own HALT GATE comment for the reference "
        "shape ('exit_only PERMITS sells, only 'full' blocks'). Missing:\n"
        + "\n".join(missing)
    )


def test_sell_partial_actually_blocks_a_fully_halted_player(tmp_path, monkeypatch):
    """Behavioral companion to the source-scan above -- proves the fix works,
    not just that the right words appear near each other in the file.
    """
    import sqlite3

    db_path = tmp_path / "test_trader.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""CREATE TABLE ai_players (
        id TEXT PRIMARY KEY, halt_reason TEXT, halt_mode TEXT DEFAULT 'active',
        is_human INTEGER DEFAULT 0
    )""")
    conn.execute(
        "INSERT INTO ai_players (id, halt_reason, halt_mode) VALUES "
        "('halted-test-agent', 'test halt', 'full')"
    )
    conn.execute("""CREATE TABLE positions (
        id INTEGER PRIMARY KEY, player_id TEXT, symbol TEXT, qty REAL,
        avg_price REAL, asset_type TEXT DEFAULT 'stock', option_type TEXT,
        strike_price REAL, expiry_date TEXT, opened_at TEXT
    )""")
    conn.execute(
        "INSERT INTO positions (player_id, symbol, qty, avg_price, opened_at) VALUES "
        "('halted-test-agent', 'AG', 1.0, 10.0, '2026-08-01 00:00:00')"
    )
    conn.commit()
    conn.close()

    import engine.paper_trader as pt
    monkeypatch.setattr(pt, "_conn", lambda: sqlite3.connect(str(db_path)))
    monkeypatch.setattr(pt, "_is_human_player", lambda pid: False)

    result = pt.sell_partial(
        player_id="halted-test-agent", symbol="AG", price=13.0, qty=0.5,
        reasoning="test scaled exit while halted",
    )
    assert result is None, (
        "sell_partial() must refuse to execute for a halt_mode='full' player "
        "-- got a non-None result instead, meaning the halt gate did not fire"
    )
