"""ot_observer's ten fixed read-only tools: shape, caps, explicit states, audit.

Decisions under test (Captain, 2026-09-14):
- kill-switch file -> "disabled", never empty results;
- "cannot read" (unavailable) is never confused with "no rows";
- prompt_text off by default, and capped at ONE row behind an explicit flag;
- every call appended to logs/ot_observer.log (timestamp, tool, args, row count, duration);
- every timestamp returned as UTC and MST, both labelled.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ot_observer_testkit import RICH_TRADER_LOG, build_root, execute_many, seed_all, write_log  # noqa: E402

from ot_observer import config, tools  # noqa: E402

CALLS = {
    "observer_status": {},
    "scheduler_health": {},
    "read_log": {"log": "trader"},
    "recent_decisions": {},
    "recent_trades": {},
    "open_positions": {},
    "fleet_roster": {},
    "lifecycle_ledger": {},
    "recent_notifications": {},
    "recent_trade_signals": {},
}
ROW_TOOLS = ["recent_decisions", "recent_trades", "open_positions", "fleet_roster",
             "lifecycle_ledger", "recent_notifications", "recent_trade_signals"]
TIMESTAMP_FIELD = {
    "recent_decisions": "created_at", "recent_trades": "executed_at", "open_positions": "opened_at",
    "lifecycle_ledger": "created_at", "recent_notifications": "timestamp",
    "recent_trade_signals": "created_at",
}


def _call(paths, name, **args):
    return tools.TOOLS[name](paths, **{**CALLS[name], **args})


def _audit_lines(paths):
    return [json.loads(line) for line in paths.audit_log.read_text().splitlines()]


@pytest.fixture
def seeded(tmp_path):
    paths = build_root(tmp_path)
    seed_all(paths, 150)
    write_log(paths, "trader.log", RICH_TRADER_LOG, age_seconds=30)
    return paths


def test_exactly_ten_fixed_tools():
    assert tuple(tools.TOOLS) == tuple(CALLS)
    assert tools.TOOL_NAMES == tuple(CALLS)


@pytest.mark.parametrize("name", list(CALLS))
def test_kill_switch_returns_disabled_for_every_tool(tmp_path, name):
    paths = build_root(tmp_path, dbs=False)
    paths.kill_switch.parent.mkdir(parents=True, exist_ok=True)
    paths.kill_switch.write_text("off\n")
    result = _call(paths, name)
    assert result["status"] == "disabled"
    assert str(paths.kill_switch) in result["reason"]
    assert result.get("rows") is None and result.get("data") is None
    assert _audit_lines(paths)[-1]["status"] == "disabled"


@pytest.mark.parametrize("name", list(CALLS))
def test_every_tool_returns_the_envelope(seeded, name):
    result = _call(seeded, name)
    for key in ("tool", "status", "reason", "generated_at", "trader_log_last_write"):
        assert key in result, key
    assert result["tool"] == name
    assert set(result["generated_at"]) >= {"utc", "mst"}
    assert result["status"] in {"ok", "no_rows"}, result


@pytest.mark.parametrize("name", ROW_TOOLS)
def test_row_tools_default_limit(seeded, name):
    result = _call(seeded, name)
    assert result["status"] == "ok"
    assert result["row_count"] == len(result["rows"]) == config.DEFAULT_LIMIT
    assert result["truncated"] is True


@pytest.mark.parametrize("name", ROW_TOOLS)
def test_row_tools_clamp_to_cap(seeded, name):
    result = _call(seeded, name, limit=1000)
    assert result["row_count"] == len(result["rows"]) == config.MAX_LIMIT
    assert result["limit_applied"] == config.MAX_LIMIT
    assert result["limit_clamped"] is True
    assert result["truncated"] is True


@pytest.mark.parametrize("name", ROW_TOOLS)
def test_empty_table_is_no_rows_not_ok(tmp_path, name):
    paths = build_root(tmp_path)
    result = _call(paths, name)
    assert result["status"] == "no_rows"
    assert result["rows"] == [] and result["row_count"] == 0


@pytest.mark.parametrize("name", ROW_TOOLS)
def test_missing_db_is_unavailable_never_empty(tmp_path, name):
    paths = build_root(tmp_path, dbs=False)
    result = _call(paths, name)
    assert result["status"] == "unavailable"
    assert result["reason"]
    assert result["rows"] is None and result["row_count"] is None


@pytest.mark.parametrize("name", list(TIMESTAMP_FIELD))
def test_row_timestamps_are_labelled_utc_and_mst(seeded, name):
    row = _call(seeded, name)["rows"][0]
    stamp = row[TIMESTAMP_FIELD[name]]
    assert set(stamp) >= {"utc", "mst", "stored_as", "raw"}
    assert stamp["utc"].endswith("Z") and stamp["mst"].endswith(" MST")


def test_trade_signal_execution_times_are_labelled_as_local_mst(tmp_path):
    """signal-center/server.py writes executed_at / dismissed_at as datetime.now().isoformat()
    (naive local) and created_at as explicit UTC -- verified from the writers 2026-09-14; the
    live table has no executed/dismissed values yet."""
    paths = build_root(tmp_path)
    execute_many(
        paths.signals_db,
        "INSERT INTO trade_signals (symbol, action, created_at, executed_at, dismissed_at) VALUES (?,?,?,?,?)",
        [("SPY", "BUY", "2026-09-14 13:36:04", "2026-09-14T06:40:00.123456", "2026-09-14T07:00:00.500000")],
    )
    [row] = _call(paths, "recent_trade_signals")["rows"]
    assert row["created_at"]["mst"] == "2026-09-14 06:36:04 MST"
    assert row["executed_at"] == {"utc": "2026-09-14T13:40:00Z", "mst": "2026-09-14 06:40:00 MST",
                                  "stored_as": "mst_local", "raw": "2026-09-14T06:40:00.123456"}
    assert row["dismissed_at"]["utc"] == "2026-09-14T14:00:00Z"


def test_decisions_exclude_prompt_text_by_default(seeded):
    result = _call(seeded, "recent_decisions")
    assert all("prompt_text" not in row for row in result["rows"])


def test_prompt_text_flag_is_capped_at_one_row(seeded):
    result = _call(seeded, "recent_decisions", include_prompt_text=True, limit=100)
    assert result["row_count"] == 1 and len(result["rows"]) == 1
    assert result["limit_applied"] == 1
    assert result["prompt_text_cap_applied"] is True
    assert result["rows"][0]["prompt_text"].startswith("PROMPT ")


def test_prompt_text_for_a_specific_decision(seeded):
    result = _call(seeded, "recent_decisions", include_prompt_text=True, decision_id=3)
    [row] = result["rows"]
    assert row["id"] == 3 and row["prompt_text"] == "PROMPT 2"


def test_invalid_filter_is_rejected_explicitly(seeded):
    result = _call(seeded, "recent_trades", player_id="x'; DROP TABLE trades; --")
    assert result["status"] == "rejected"
    assert result["rows"] is None


def test_read_log_rejects_names_outside_the_enum(seeded):
    result = _call(seeded, "read_log", log="../../.env")
    assert result["status"] == "rejected"
    assert "trader" in result["reason"]


def test_read_log_parses_trader_log_with_labelled_timestamps(seeded):
    result = _call(seeded, "read_log", log="trader", lines=50, contains="SCHED-JOB")
    assert result["status"] == "ok"
    messages = [r["message"] for r in result["rows"]]
    assert "[SCHED-JOB] done name=_run_ollietrades_signal_cycle wall=0.002s" in messages
    first = result["rows"][0]
    assert first["ts"]["mst"] == "2026-09-14 05:01:45 MST" and first["ts"]["utc"] == "2026-09-14T12:01:45Z"


def test_read_log_missing_file_is_unavailable(tmp_path):
    paths = build_root(tmp_path)
    result = _call(paths, "read_log", log="trader")
    assert result["status"] == "unavailable" and result["rows"] is None


def test_scheduler_health_through_the_tool(seeded):
    result = _call(seeded, "scheduler_health", lookback_minutes=1440)
    assert result["status"] == "ok"
    assert "summary_state" in result["data"] and "started_without_done" in result["data"]


def test_observer_status_reports_each_source(seeded):
    data = _call(seeded, "observer_status")["data"]
    assert data["trader_db"]["status"] == "available"
    assert data["signals_db"]["status"] == "available"
    assert data["kill_switch"] == {"present": False, "path": str(seeded.kill_switch)}


def test_observer_status_reports_unavailable_db(tmp_path):
    paths = build_root(tmp_path, dbs=False)
    data = _call(paths, "observer_status")["data"]
    assert data["trader_db"]["status"] == "unavailable" and data["trader_db"]["reason"]


def test_trader_log_last_write_age_and_missing_log(seeded, tmp_path):
    stamp = _call(seeded, "observer_status")["trader_log_last_write"]
    assert stamp["status"] == "available" and 20 <= stamp["age_seconds"] <= 120
    assert set(stamp) >= {"utc", "mst"}
    bare = build_root(tmp_path / "bare")
    assert _call(bare, "observer_status")["trader_log_last_write"]["status"] == "unavailable"


def test_every_call_is_audited(seeded):
    _call(seeded, "recent_trades", limit=5, player_id="ollama-plutus")
    entry = _audit_lines(seeded)[-1]
    assert entry["tool"] == "recent_trades"
    assert entry["args"] == {"limit": 5, "player_id": "ollama-plutus"}
    assert entry["status"] == "ok" and entry["row_count"] == 5
    assert isinstance(entry["duration_ms"], (int, float)) and entry["duration_ms"] >= 0
    assert set(entry["ts"]) >= {"utc", "mst"}


def test_rejected_and_unavailable_calls_are_audited_too(tmp_path):
    paths = build_root(tmp_path, dbs=False)
    _call(paths, "recent_trades")
    _call(paths, "read_log", log="nope")
    statuses = [e["status"] for e in _audit_lines(paths)]
    assert statuses == ["unavailable", "rejected"]
