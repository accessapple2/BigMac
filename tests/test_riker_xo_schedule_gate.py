"""HM-RIKER-CADENCE-GATE-2026-09-13 — the 10-min Riker synthesis scheduler tick must
skip outside market hours (weekends/holidays/overnight) using the same gate as the
other market-hours jobs, and still fire inside them.

The wrapper is a closure inside main.py's scheduler setup, so it's extracted by source
and exec'd against fake engine modules rather than importing main.py (which starts the
whole trader)."""
from __future__ import annotations

import re
import sys
import textwrap
import types
from pathlib import Path

import pytest

_MAIN = Path(__file__).resolve().parent.parent / "main.py"
_SRC = _MAIN.read_text()


def _load_wrapper(monkeypatch, market_hours, calls):
    start = _SRC.index("    def _run_riker_xo_synthesis():")
    end = _SRC.index("    schedule.every(10).minutes.do(_run_riker_xo_synthesis)")
    block = textwrap.dedent(_SRC[start:end])

    rm = types.ModuleType("engine.risk_manager")

    class RiskManager:
        @staticmethod
        def is_market_hours():
            if isinstance(market_hours, Exception):
                raise market_hours
            return market_hours

    rm.RiskManager = RiskManager
    rx = types.ModuleType("engine.riker_xo")
    rx.generate_riker_synthesis = lambda: calls.append(1)
    monkeypatch.setitem(sys.modules, "engine.risk_manager", rm)
    monkeypatch.setitem(sys.modules, "engine.riker_xo", rx)

    ns = {"logger": types.SimpleNamespace(warning=lambda *a, **k: None, debug=lambda *a, **k: None)}
    exec(block, ns)
    return ns["_run_riker_xo_synthesis"]


@pytest.mark.parametrize("session", ["pre_market", "market", "post_market"])
def test_fires_inside_market_hours(monkeypatch, session):
    calls: list = []
    _load_wrapper(monkeypatch, session, calls)()
    assert calls == [1]


def test_skips_when_market_closed(monkeypatch):
    calls: list = []
    _load_wrapper(monkeypatch, False, calls)()
    assert calls == []


def test_skips_when_gate_check_raises(monkeypatch):
    calls: list = []
    _load_wrapper(monkeypatch, RuntimeError("calendar down"), calls)()
    assert calls == []


def test_interval_still_ten_minutes():
    assert re.search(r"schedule\.every\(10\)\.minutes\.do\(_run_riker_xo_synthesis\)", _SRC)
