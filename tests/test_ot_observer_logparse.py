"""ot_observer log parsing and scheduler_health.

- rich wraps long records onto continuation lines (this morning's [SCREENED-HB] did);
  a wrapped line has no trailing `file.py:N`, a new same-second record does.
- scheduler_health reports "started without a matching done" as an explicit state:
  Monday's stall was invisible because a missing line looks like nothing.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ot_observer_testkit import RICH_TRADER_LOG  # noqa: E402

from ot_observer import logparse as lp  # noqa: E402

NOW_UTC = datetime(2026, 9, 15, 3, 30, 0, tzinfo=timezone.utc)  # 20:30 MST


def test_rich_records_join_wraps_and_carry_timestamps():
    recs = lp.parse_rich_lines(RICH_TRADER_LOG)
    assert [r.message for r in recs] == [
        "[SCHED-JOB] start name=_run_riker_xo_synthesis",
        "[ENDPOINT-DUR] GET /api/bridge/consensus wall=0.01s status=200",
        "[SCHED-JOB] start name=_run_ollietrades_signal_cycle",
        "[SCHED-JOB] done name=_run_ollietrades_signal_cycle wall=0.002s",
        "Database ready with 14 AI players",
        "HM-CB Polygon candles stale for DPZ:",
        "HM-CA Alpaca candles fallback to Yahoo",
    ]
    assert [r.source for r in recs] == [
        "main.py:110", "app.py:1640", "main.py:110", "main.py:116", None,
        "market_data.py:960", "market_data.py:1095",
    ]
    assert recs[3].ts_mst == datetime(2026, 9, 14, 5, 1, 45) and recs[3].ts_inferred is False
    assert recs[4].ts_mst == datetime(2026, 9, 14, 5, 1, 45) and recs[4].ts_inferred is True
    assert recs[6].ts_mst == datetime(2026, 9, 14, 5, 1, 46)


def test_lines_before_any_timestamp_have_no_timestamp():
    recs = lp.parse_rich_lines(["plain startup line"] + RICH_TRADER_LOG[:1])
    assert recs[0].ts_mst is None and recs[0].ts_inferred is True


def test_redact_masks_credentials():
    text = ("auth failed token=abc123XYZ Authorization: Bearer eyJhbGciOi.payload "
            "key sk-ant-api03-ABCDEFGHIJKL api_key=AKIA1234567")
    out = lp.redact(text)
    for secret in ("abc123XYZ", "eyJhbGciOi.payload", "sk-ant-api03-ABCDEFGHIJKL", "AKIA1234567"):
        assert secret not in out
    assert "[REDACTED]" in out


def test_tail_lines_returns_last_n(tmp_path):
    path = tmp_path / "big.log"
    path.write_text("".join(f"line {i}\n" for i in range(10000)))
    assert lp.tail_lines(path, 3, byte_cap=4096) == ["line 9997", "line 9998", "line 9999"]


def _recs(*items: tuple[str, str]) -> list:
    return [lp.LogRecord(ts_mst=datetime.fromisoformat(ts), ts_inferred=False, message=msg,
                         source="main.py:110", line_no=i) for i, (ts, msg) in enumerate(items)]


def test_start_without_done_is_an_explicit_state():
    h = lp.scheduler_health_from_records(_recs(
        ("2026-09-14 20:10:00", "[SCHED-JOB] start name=run_scanner"),
        ("2026-09-14 20:10:01", "[SCHED-JOB] start name=_bg_whisper"),
        ("2026-09-14 20:10:01", "[SCHED-JOB] done name=_bg_whisper wall=0.002s"),
    ), now_utc=NOW_UTC, lookback_minutes=60)
    assert h["summary_state"] == "job_started_without_done"
    [job] = h["started_without_done"]
    assert job["name"] == "run_scanner"
    assert job["state"] == "in_flight"
    assert job["started_at"] == {"utc": "2026-09-15T03:10:00Z", "mst": "2026-09-14 20:10:00 MST",
                                 "stored_as": "mst_local"}
    assert job["age_seconds"] == 1200
    assert job["exceeds_warn_seconds"] is True
    assert h["completed"]["count"] == 1


def test_start_before_a_restart_is_orphaned_not_in_flight():
    h = lp.scheduler_health_from_records(_recs(
        ("2026-09-14 20:10:00", "[SCHED-JOB] start name=run_scanner"),
        ("2026-09-14 20:12:11", "WAL mode enabled: trader.db"),
        ("2026-09-14 20:16:00", "[SCHED-JOB] start name=run_events_bus_consumer"),
        ("2026-09-14 20:16:00", "[SCHED-JOB] done name=run_events_bus_consumer wall=0.500s"),
    ), now_utc=NOW_UTC, lookback_minutes=60)
    [job] = h["started_without_done"]
    assert job["state"] == "orphaned_by_restart"
    assert h["last_restart_at"]["mst"] == "2026-09-14 20:12:11 MST"
    assert h["summary_state"] == "ok"


def test_all_paired_is_ok_with_an_explicit_empty_list():
    h = lp.scheduler_health_from_records(_recs(
        ("2026-09-14 20:20:00", "[SCHED-JOB] start name=run_scanner"),
        ("2026-09-14 20:21:35", "[SCHED-JOB] done name=run_scanner wall=95.443s"),
    ), now_utc=NOW_UTC, lookback_minutes=60)
    assert h["summary_state"] == "ok"
    assert h["started_without_done"] == []
    assert h["completed"]["count"] == 1
    assert h["completed"]["slowest"][0] == {"name": "run_scanner", "wall_seconds": 95.443,
                                            "done_at": {"utc": "2026-09-15T03:21:35Z",
                                                        "mst": "2026-09-14 20:21:35 MST",
                                                        "stored_as": "mst_local"}}


def test_no_job_lines_in_window_is_explicit():
    h = lp.scheduler_health_from_records(_recs(
        ("2026-09-14 20:20:00", "[SCREENED-HB] player=ollama-plutus"),
    ), now_utc=NOW_UTC, lookback_minutes=60)
    assert h["summary_state"] == "no_job_activity_in_window"
    assert h["last_job_start_at"] is None
    assert h["started_without_done"] == []


def test_records_outside_the_window_are_ignored():
    h = lp.scheduler_health_from_records(_recs(
        ("2026-09-14 18:00:00", "[SCHED-JOB] start name=run_scanner"),
        ("2026-09-14 20:20:00", "[SCHED-JOB] start name=_bg_whisper"),
        ("2026-09-14 20:20:00", "[SCHED-JOB] done name=_bg_whisper wall=0.002s"),
    ), now_utc=NOW_UTC, lookback_minutes=60)
    assert h["started_without_done"] == []


def test_wrapped_sched_job_lines_pair_after_parsing():
    recs = lp.parse_rich_lines(RICH_TRADER_LOG)
    h = lp.scheduler_health_from_records(
        recs, now_utc=datetime(2026, 9, 14, 12, 30, 0, tzinfo=timezone.utc), lookback_minutes=60)
    names = [j["name"] for j in h["started_without_done"]]
    assert names == ["_run_riker_xo_synthesis"]
    assert h["completed"]["count"] == 1
