"""engine/screened_scan_scheduler.py — HM-SCREENED-SCAN-HB 2026-09-14.

One ScreenedScanScheduler per screened-scan seat (McCoy = ollama-plutus,
Scotty = ollama-qwen3), replacing the two copy-pasted run_*_screened_scan()
bodies that lived in main.py. main.py's dedicated daemon threads
(HM-SCHED-STALL-FIX / HM-EXIT-GATE-AUDIT-2026-09-13) still call tick() every
60s; only what a tick does changed.

Why (data/reports/relay/relay_2026-09-14_screened_scan_silence_trace.md):
  * A healthy tick logged nothing, and the only proof the thread existed was a
    startup line that scripts/rotate_logs.sh archives out of trader.log every
    morning at 05:00. Confirming both threads were alive on 2026-09-14 took a
    native `sample` of the live PID. tick() now logs one [SCREENED-HB] line per
    tick (every slot's status: fired / skipped and why) AND rewrites a small
    JSON heartbeat under data/, which log rotation never touches.
  * ollama-qwen3 sits at halt_mode='full' with no provider built, yet its
    thread ran a full arena.run_scan (price fetch for the whole screen) at both
    slots every weekday. A slot now fires only if seat_status() says the seat
    can actually run; a skipped slot is NOT marked done, so the skip shows up
    on every tick of the window.
  * Rich console markup silently swallows lowercase square-bracketed text, so
    "McCoy screened scan [pre-open]" rendered as "McCoy screened scan :".
    Every interpolated value is escaped here.

Slot/window semantics are unchanged from HM-XO-PLAN-2026-09 Phase 1.2 +
HM-SCHED-STALL-FIX: 9:35 and 12:30 ET, a 20-min on-time window plus 60-min
same-day late recovery, at most one slot fired per tick, done-set reset during
ET hour 0, weekends skipped, and a slot that errors or gets an empty screen is
still marked done for the day (no retry storm).
"""
from __future__ import annotations

import contextlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from rich.markup import escape

SLOTS: tuple[tuple[str, int, int], ...] = (("pre-open", 9, 35), ("midday", 12, 30))
NORMAL_WINDOW_MIN = 20
LATE_RECOVERY_MIN = 60


def _default_now_et() -> datetime:
    import pytz

    return datetime.now(pytz.timezone("US/Eastern"))


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class ScreenedScanScheduler:
    """Twice-daily screened scan for one seat. Not thread-safe by design: each
    instance is driven by exactly one daemon thread."""

    def __init__(
        self,
        *,
        player_id: str,
        label: str,
        get_screen: Callable[[], dict],
        run_scan: Callable[[list, frozenset], None],
        seat_status: Callable[[str], tuple[bool, str]],
        log: Callable[[str], None],
        heartbeat_path: Path | str,
        now_et: Callable[[], datetime] = _default_now_et,
    ) -> None:
        self.player_id = player_id
        self.label = label
        self._get_screen = get_screen
        self._run_scan = run_scan
        self._seat_status = seat_status
        self._log = log
        self._heartbeat_path = Path(heartbeat_path)
        self._now_et = now_et
        self._done_today: set[str] = set()
        self._tick = 0
        self._thread_started_at: str | None = None
        self._heartbeat_write_failed = False
        self._slots: dict[str, dict] = {
            slot_id: {
                "status": None,
                "status_since_utc": None,
                "last_fired_at_utc": None,
                "last_fired_status": None,
                "n_symbols": None,
            }
            for slot_id, _, _ in SLOTS
        }

    def mark_thread_started(self) -> None:
        """Call from inside the daemon thread itself, so the heartbeat file
        proves the thread body actually ran, not just that .start() returned."""
        self._thread_started_at = _utc_iso()
        self._write_heartbeat(None)

    def tick(self) -> dict[str, str]:
        now = self._now_et()
        self._tick += 1
        if now.hour < 1:
            self._done_today.clear()
            statuses = {slot_id: "reset" for slot_id, _, _ in SLOTS}
        elif now.weekday() >= 5:
            statuses = {slot_id: "weekend" for slot_id, _, _ in SLOTS}
        else:
            statuses = self._evaluate_slots(now)
        self._record(now, statuses)
        return statuses

    def _evaluate_slots(self, now: datetime) -> dict[str, str]:
        statuses: dict[str, str] = {}
        now_mins = now.hour * 60 + now.minute
        fired_this_tick = False
        for slot_id, target_h, target_m in SLOTS:
            if slot_id in self._done_today:
                statuses[slot_id] = "done_today"
                continue
            target_mins = target_h * 60 + target_m
            if not target_mins <= now_mins <= target_mins + NORMAL_WINDOW_MIN + LATE_RECOVERY_MIN:
                statuses[slot_id] = "outside_window"
                continue
            if fired_this_tick:
                statuses[slot_id] = "deferred_next_tick"
                continue
            can_run, reason = self._check_seat()
            if not can_run:
                statuses[slot_id] = f"skipped_seat:{reason}"
                continue
            if reason != "active":
                self._log(
                    f"[yellow][SCREENED-HB] player={escape(self.player_id)} "
                    f"seat check {escape(reason)} -- firing anyway (fail-open)"
                )
            is_late = now_mins > target_mins + NORMAL_WINDOW_MIN
            statuses[slot_id] = self._fire(slot_id, is_late)
            fired_this_tick = True
        return statuses

    def _check_seat(self) -> tuple[bool, str]:
        # Efficiency gate, not a safety gate: run_scan and paper_trader keep
        # their own halt checks, so a broken check must not cost an active
        # seat its slot.
        try:
            return self._seat_status(self.player_id)
        except Exception as exc:
            return True, f"seat_check_failed_open:{type(exc).__name__}"

    def _fire(self, slot_id: str, is_late: bool) -> str:
        label = escape(self.label)
        slot_tag = escape(f"[{slot_id}]")
        late_tag = escape(" [LATE — same-day recovery]") if is_late else ""
        n_symbols = None
        try:
            screen = self._get_screen()
            symbols = screen["symbols"]
            n_symbols = len(symbols)
            if not symbols:
                self._log(f"[yellow]{label} screened scan {slot_tag}{late_tag}: 0 symbols from screen — skipping")
                status = "fired_empty_screen"
            else:
                self._run_scan(symbols, frozenset({self.player_id}))
                regime = screen.get("regime")
                regime_name = regime.get("regime") if regime else "?"
                self._log(
                    f"[green]{label} screened scan {slot_tag}{late_tag}: "
                    f"{screen['n_found']}/{screen['n_requested']} symbols (regime={escape(str(regime_name))})"
                )
                status = "fired_late" if is_late else "fired"
        except Exception as exc:
            self._log(f"[red]{label} screened scan {slot_tag} error: {escape(str(exc))}")
            status = f"error:{type(exc).__name__}"
        finally:
            self._done_today.add(slot_id)
        slot = self._slots[slot_id]
        slot["last_fired_at_utc"] = _utc_iso()
        slot["last_fired_status"] = status
        slot["n_symbols"] = n_symbols
        return status

    def _record(self, now: datetime, statuses: dict[str, str]) -> None:
        now_utc = _utc_iso()
        for slot_id, status in statuses.items():
            slot = self._slots[slot_id]
            if slot["status"] != status:
                slot["status"] = status
                slot["status_since_utc"] = now_utc
        body = " ".join(f"{slot_id}={status}" for slot_id, status in statuses.items())
        self._log(
            f"[SCREENED-HB] player={escape(self.player_id)} tick={self._tick} "
            f"et={now:%a %H:%M} {escape(body)}"
        )
        self._write_heartbeat(now)

    def _write_heartbeat(self, now: datetime | None) -> None:
        payload = {
            "player_id": self.player_id,
            "label": self.label,
            "pid": os.getpid(),
            "thread_started_at_utc": self._thread_started_at,
            "tick": self._tick,
            "last_tick_at_utc": _utc_iso() if now is not None else None,
            "last_tick_et": now.isoformat(timespec="seconds") if now is not None else None,
            "slots": self._slots,
        }
        try:
            self._heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(
                dir=self._heartbeat_path.parent, prefix=f".{self._heartbeat_path.name}.", suffix=".tmp"
            )
            try:
                with os.fdopen(fd, "w") as fh:
                    json.dump(payload, fh, indent=2)
                os.replace(tmp, self._heartbeat_path)
            except BaseException:
                with contextlib.suppress(OSError):
                    os.unlink(tmp)
                raise
        except Exception as exc:
            if not self._heartbeat_write_failed:
                self._log(
                    f"[red][SCREENED-HB] player={escape(self.player_id)} heartbeat write failed: "
                    f"{escape(repr(exc))} -- retrying every tick, logging again only on recovery"
                )
            self._heartbeat_write_failed = True
            return
        if self._heartbeat_write_failed:
            self._log(f"[green][SCREENED-HB] player={escape(self.player_id)} heartbeat write recovered")
            self._heartbeat_write_failed = False
