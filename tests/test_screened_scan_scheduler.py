"""HM-SCREENED-SCAN-HB 2026-09-14 — engine/screened_scan_scheduler.py plus its
main.py wiring.

Covers: one [SCREENED-HB] line per tick with every slot's status, the
rotation-proof JSON heartbeat, the seat gate (skip without scanning, not marked
done), slot/window semantics carried over unchanged from the old
run_mccoy_screened_scan body, and rich-markup escaping of "[pre-open]" (which
used to render as nothing).

main.py isn't importable in tests (it starts the whole trader), so the seat
gate is extracted by source and exec'd, same approach as
test_riker_xo_schedule_gate.py."""
from __future__ import annotations

import io
import json
import sqlite3
import types
from datetime import datetime
from pathlib import Path

import pytz
from rich.console import Console

from engine.screened_scan_scheduler import ScreenedScanScheduler

ET = pytz.timezone("US/Eastern")
_MAIN_SRC = (Path(__file__).resolve().parent.parent / "main.py").read_text()


def _et(day: int, hour: int, minute: int) -> datetime:
    # 2026-09-12 is a Saturday, 09-14 a Monday, 09-15 a Tuesday.
    return ET.localize(datetime(2026, 9, day, hour, minute))


class _Harness:
    def __init__(self, tmp_path, *, now, seat=(True, "active"), screen=None, scan_error=None, heartbeat_path=None):
        self.now = now
        self.seat = seat
        self.screen = screen if screen is not None else {
            "symbols": ["AAA", "BBB"], "regime": {"regime": "BEAR_CROSS"}, "n_found": 2, "n_requested": 100,
        }
        self.scan_error = scan_error
        self.scans: list = []
        self.screen_calls = 0
        self.buf = io.StringIO()
        console = Console(file=self.buf, width=300, color_system=None, log_time=False, log_path=False)
        self.path = heartbeat_path or tmp_path / "hb.json"
        self.sched = ScreenedScanScheduler(
            player_id="ollama-plutus",
            label="McCoy",
            get_screen=self._get_screen,
            run_scan=self._run_scan,
            seat_status=self._seat_status,
            log=console.log,
            heartbeat_path=self.path,
            now_et=lambda: self.now,
        )

    def _get_screen(self):
        self.screen_calls += 1
        return self.screen

    def _run_scan(self, symbols, player_ids):
        if self.scan_error:
            raise self.scan_error
        self.scans.append((list(symbols), player_ids))

    def _seat_status(self, player_id):
        if isinstance(self.seat, Exception):
            raise self.seat
        return self.seat

    @property
    def out(self) -> str:
        return self.buf.getvalue()

    def heartbeat(self) -> dict:
        return json.loads(self.path.read_text())


def test_fires_once_in_window_then_done_today(tmp_path):
    h = _Harness(tmp_path, now=_et(14, 9, 35))
    assert h.sched.tick() == {"pre-open": "fired", "midday": "outside_window"}
    assert h.scans == [(["AAA", "BBB"], frozenset({"ollama-plutus"}))]
    h.now = _et(14, 9, 36)
    assert h.sched.tick()["pre-open"] == "done_today"
    assert len(h.scans) == 1


def test_slot_tag_survives_rich_markup(tmp_path):
    h = _Harness(tmp_path, now=_et(14, 9, 35))
    h.sched.tick()
    assert "McCoy screened scan [pre-open]: 2/100 symbols (regime=BEAR_CROSS)" in h.out


def test_heartbeat_line_on_every_tick_including_skips(tmp_path):
    h = _Harness(tmp_path, now=_et(14, 8, 0))
    h.sched.tick()
    h.now = _et(14, 8, 1)
    h.sched.tick()
    assert "[SCREENED-HB] player=ollama-plutus tick=1 et=Mon 08:00 pre-open=outside_window midday=outside_window" in h.out
    assert "tick=2 et=Mon 08:01" in h.out
    assert h.scans == [] and h.screen_calls == 0


def test_heartbeat_file_records_thread_start_and_slot_outcome(tmp_path):
    h = _Harness(tmp_path, now=_et(14, 9, 35))
    h.sched.mark_thread_started()
    started = h.heartbeat()
    assert started["thread_started_at_utc"] and started["tick"] == 0 and started["pid"]
    h.sched.tick()
    hb = h.heartbeat()
    assert hb["player_id"] == "ollama-plutus" and hb["tick"] == 1
    assert hb["last_tick_et"].startswith("2026-09-14T09:35")
    assert hb["slots"]["pre-open"]["status"] == "fired"
    assert hb["slots"]["pre-open"]["last_fired_status"] == "fired"
    assert hb["slots"]["pre-open"]["n_symbols"] == 2
    assert hb["slots"]["midday"]["status"] == "outside_window"
    assert [p.name for p in tmp_path.iterdir()] == ["hb.json"]  # atomic replace, no temp leftovers


def test_inactive_seat_skips_without_screen_or_scan_and_is_not_marked_done(tmp_path):
    h = _Harness(tmp_path, now=_et(14, 9, 35), seat=(False, "halt_mode=full"))
    assert h.sched.tick()["pre-open"] == "skipped_seat:halt_mode=full"
    assert h.screen_calls == 0 and h.scans == []
    assert "pre-open=skipped_seat:halt_mode=full" in h.out
    h.seat = (True, "active")
    h.now = _et(14, 9, 36)
    assert h.sched.tick()["pre-open"] == "fired"


def test_seat_check_exception_fails_open(tmp_path):
    h = _Harness(tmp_path, now=_et(14, 9, 35), seat=RuntimeError("db locked"))
    assert h.sched.tick()["pre-open"] == "fired"
    assert len(h.scans) == 1
    assert "seat check seat_check_failed_open:RuntimeError -- firing anyway" in h.out


def test_late_recovery_window_edges(tmp_path):
    h = _Harness(tmp_path, now=_et(14, 9, 56))
    assert h.sched.tick()["pre-open"] == "fired_late"
    assert "McCoy screened scan [pre-open] [LATE — same-day recovery]: 2/100" in h.out
    last_minute = _Harness(tmp_path / "b", now=_et(14, 10, 55))
    assert last_minute.sched.tick()["pre-open"] == "fired_late"
    too_late = _Harness(tmp_path / "c", now=_et(14, 10, 56))
    assert too_late.sched.tick()["pre-open"] == "outside_window"
    assert too_late.scans == []


def test_empty_screen_marks_done_without_scan(tmp_path):
    h = _Harness(tmp_path, now=_et(14, 12, 30), screen={"symbols": [], "regime": None, "n_found": 0, "n_requested": 100})
    assert h.sched.tick()["midday"] == "fired_empty_screen"
    assert h.scans == []
    assert "McCoy screened scan [midday]: 0 symbols from screen — skipping" in h.out
    h.now = _et(14, 12, 31)
    assert h.sched.tick()["midday"] == "done_today"


def test_scan_error_marks_done_and_logs_escaped(tmp_path):
    h = _Harness(tmp_path, now=_et(14, 9, 35), scan_error=RuntimeError("boom [x]"))
    assert h.sched.tick()["pre-open"] == "error:RuntimeError"
    assert "McCoy screened scan [pre-open] error: boom [x]" in h.out
    h.now = _et(14, 9, 36)
    assert h.sched.tick()["pre-open"] == "done_today"


def test_weekend_skip_and_midnight_reset(tmp_path):
    h = _Harness(tmp_path, now=_et(12, 9, 35))
    assert h.sched.tick() == {"pre-open": "weekend", "midday": "weekend"}
    h.now = _et(14, 9, 35)
    h.sched.tick()
    h.now = _et(15, 0, 30)
    assert h.sched.tick() == {"pre-open": "reset", "midday": "reset"}
    h.now = _et(15, 9, 35)
    assert h.sched.tick()["pre-open"] == "fired"
    assert len(h.scans) == 2


def test_heartbeat_write_failure_logged_once_and_tick_survives(tmp_path):
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory")
    h = _Harness(tmp_path, now=_et(14, 8, 0), heartbeat_path=blocker / "hb.json")
    assert h.sched.tick() == {"pre-open": "outside_window", "midday": "outside_window"}
    h.now = _et(14, 8, 1)
    h.sched.tick()
    assert h.out.count("heartbeat write failed") == 1


# --- main.py wiring --------------------------------------------------------

def _load_seat_status(arena):
    start = _MAIN_SRC.index("def _screened_scan_seat_status(")
    end = _MAIN_SRC.index("def _screened_get_screen(")
    ns = {"sqlite3": sqlite3, "arena": arena}
    exec(_MAIN_SRC[start:end], ns)
    return ns["_screened_scan_seat_status"]


def _seed_db(tmp_path, rows):
    (tmp_path / "data").mkdir()
    with sqlite3.connect(tmp_path / "data" / "trader.db") as c:
        c.execute("CREATE TABLE ai_players (id TEXT PRIMARY KEY, halt_mode TEXT)")
        c.executemany("INSERT INTO ai_players VALUES (?, ?)", rows)


def test_seat_gate_requires_provider_and_active_halt_mode(tmp_path, monkeypatch):
    _seed_db(tmp_path, [("ollama-plutus", "active"), ("ollama-qwen3", "full"), ("orphan", "active")])
    monkeypatch.chdir(tmp_path)
    arena = types.SimpleNamespace(providers={"ollama-plutus": object(), "ollama-qwen3": object(), "ghost": object()})
    seat_status = _load_seat_status(arena)
    assert seat_status("ollama-plutus") == (True, "active")
    assert seat_status("ollama-qwen3") == (False, "halt_mode=full")
    assert seat_status("orphan") == (False, "no_provider_built")
    assert seat_status("ghost") == (False, "halt_mode=None")
    assert _load_seat_status(None)("ollama-plutus") == (False, "no_provider_built")


def test_seat_gate_fails_open_on_db_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # no data/trader.db here
    seat_status = _load_seat_status(types.SimpleNamespace(providers={"ollama-plutus": object()}))
    assert seat_status("ollama-plutus") == (True, "halt_check_failed_open:OperationalError")


def test_main_wrappers_and_threads_delegate_to_schedulers():
    for fn, sched in (("run_mccoy_screened_scan", "_mccoy_screened_scheduler"),
                      ("run_qwen3_screened_scan", "_qwen3_screened_scheduler")):
        body = _MAIN_SRC[_MAIN_SRC.index(f"def {fn}():"):]
        assert f"{sched}.tick()" in body[: body.index("\n\n\n")]
    for thread_fn, sched in (("_mccoy_scheduler_thread", "_mccoy_screened_scheduler"),
                             ("_qwen3_scheduler_thread", "_qwen3_screened_scheduler")):
        body = _MAIN_SRC[_MAIN_SRC.index(f"    def {thread_fn}():"):]
        assert f"{sched}.mark_thread_started()" in body[: body.index("while True:")]
    assert "_mccoy_screened_slots_done_today" not in _MAIN_SRC
    assert "_qwen3_screened_slots_done_today" not in _MAIN_SRC
