"""Log reading for the observer: tail reads, rich-log records, per-format timestamps,
credential redaction, and scheduler_health.

rich (trader.log) layout, from real lines 2026-09-14:
- a record's first line starts with "[YYYY-MM-DD HH:MM:SS] " (local MST) or, when the
  second has not changed, 22 blank columns -- and ends with a "file.py:N" source column;
- a wrapped continuation line starts with 22 blank columns and has no source column;
- plain print() output starts at column 0 with neither; it carries the last timestamp,
  marked inferred.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import config, timefmt

_RICH_TS = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] ")
_RICH_TS_BYTES = re.compile(rb"(?m)^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] ")
_BLANK_TS_COLUMN = " " * 22
_SOURCE_SUFFIX = re.compile(r"^(.*?)\s+([\w.\-]+\.py:\d+)\s*$")

_SCHED_START = re.compile(r"\[SCHED-JOB\] start\s+name=(\S+)")
_SCHED_DONE = re.compile(r"\[SCHED-JOB\] done\s+name=(\S+)(?:\s+wall=([\d.]+)s)?")
RESTART_MARKER = "WAL mode enabled: trader.db"  # main.py:4330, logged once per trader start


@dataclass(frozen=True)
class LogRecord:
    ts_mst: datetime | None
    ts_inferred: bool
    message: str
    source: str | None
    line_no: int


# ── redaction ────────────────────────────────────────────────────────────────

_KEYED_SECRET = re.compile(
    r"(?i)\b(api[_-]?key|apikey|access[_-]?token|token|secret|password|passwd|authorization)"
    r"\s*[=:]\s*(?:bearer\s+)?[^\s,;'\"]+"
)
_BARE_SECRETS = (
    re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._\-+/=]+"),
    re.compile(r"\bsk-[A-Za-z0-9_\-]{8,}"),
    re.compile(r"\bxox[abprs]-[A-Za-z0-9\-]+"),
    re.compile(r"\bAKIA[0-9A-Z]{8,}"),
)


def redact(text: str) -> str:
    text = _KEYED_SECRET.sub(lambda m: f"{m.group(1)}=[REDACTED]", text)
    for pattern in _BARE_SECRETS:
        text = pattern.sub("[REDACTED]", text)
    return text


# ── reading ──────────────────────────────────────────────────────────────────

def tail_lines(path: Path, max_lines: int, byte_cap: int) -> list[str]:
    """The last max_lines lines, reading at most byte_cap bytes from the end.
    Raises FileNotFoundError / OSError."""
    size = os.path.getsize(path)
    data = b""
    pos = size
    with open(path, "rb") as handle:
        while pos > 0 and data.count(b"\n") <= max_lines and (size - pos) < byte_cap:
            step = min(65536, pos, byte_cap - (size - pos))
            pos -= step
            handle.seek(pos)
            data = handle.read(step) + data
    lines = data.decode("utf-8", errors="replace").splitlines()
    if pos > 0 and lines:
        lines = lines[1:]  # first line is partial
    return lines[-max_lines:]


def read_recent_rich(path: Path, *, lookback_minutes: int, now_utc: datetime,
                     byte_cap: int) -> tuple[list[LogRecord], bool, int]:
    """Records covering the lookback window. Returns (records, window_truncated, bytes_read);
    window_truncated means byte_cap was reached before the window start."""
    window_start = timefmt.mst_naive(now_utc - timedelta(minutes=lookback_minutes))
    size = os.path.getsize(path)
    chunks: list[bytes] = []
    pos = size
    covered = False
    with open(path, "rb") as handle:
        while pos > 0 and (size - pos) < byte_cap:
            step = min(1 << 20, pos, byte_cap - (size - pos))
            pos -= step
            handle.seek(pos)
            chunk = handle.read(step)
            chunks.append(chunk)
            first = _RICH_TS_BYTES.search(chunk)
            if first and datetime.fromisoformat(first.group(1).decode()) < window_start:
                covered = True
                break
    lines = b"".join(reversed(chunks)).decode("utf-8", errors="replace").splitlines()
    if pos > 0 and lines:
        lines = lines[1:]
    return parse_rich_lines(lines), (pos > 0 and not covered), size - pos


# ── parsing ──────────────────────────────────────────────────────────────────

def _split_source(body: str) -> tuple[str, str | None]:
    match = _SOURCE_SUFFIX.match(body)
    if match:
        return match.group(1).strip(), match.group(2)
    return body.strip(), None


def parse_rich_lines(lines: list[str]) -> list[LogRecord]:
    records: list[LogRecord] = []
    current: dict | None = None
    last_ts: datetime | None = None

    def flush() -> None:
        if current is not None:
            records.append(LogRecord(**current))

    for index, raw in enumerate(lines):
        line = raw.rstrip("\n")
        if not line.strip():
            continue
        stamped = _RICH_TS.match(line)
        if stamped:
            flush()
            last_ts = datetime.fromisoformat(stamped.group(1))
            message, source = _split_source(line[stamped.end():])
            current = dict(ts_mst=last_ts, ts_inferred=False, message=message, source=source, line_no=index)
            continue
        if line.startswith(_BLANK_TS_COLUMN):
            body = line[len(_BLANK_TS_COLUMN):]
            message, source = _split_source(body)
            if source is not None or current is None:
                flush()
                current = dict(ts_mst=last_ts, ts_inferred=last_ts is None, message=message,
                               source=source, line_no=index)
            else:
                current["message"] = f"{current['message']} {body.strip()}"
            continue
        flush()
        current = dict(ts_mst=last_ts, ts_inferred=True, message=line.strip(), source=None, line_no=index)
    flush()
    return records


_LINE_FORMATS = {
    "python_logging": (re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d{3}\b"), "%Y-%m-%d %H:%M:%S"),
    "werkzeug": (re.compile(r"\[(\d{2}/[A-Za-z]{3}/\d{4} \d{2}:\d{2}:\d{2})\]"), "%d/%b/%Y %H:%M:%S"),
}
_ISO_OFFSET = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{4})")
_DATE_CMD = re.compile(r"^[A-Z][a-z]{2} ([A-Z][a-z]{2} +\d{1,2} \d{2}:\d{2}:\d{2}) MST (\d{4})")
_TIME_ONLY = re.compile(r"^(\d{2}:\d{2}:\d{2}) ")


def _line_timestamp(line: str, kind: str) -> tuple[dict | None, str | None]:
    """(labelled timestamp or None, note explaining a missing one)."""
    try:
        if kind in _LINE_FORMATS:
            pattern, fmt = _LINE_FORMATS[kind]
            match = pattern.search(line)
            if match:
                return timefmt.from_log_mst(datetime.strptime(match.group(1), fmt)), None
        elif kind == "iso_offset":
            match = _ISO_OFFSET.match(line)
            if match:
                return timefmt.from_aware(datetime.strptime(match.group(1), "%Y-%m-%dT%H:%M:%S%z")), None
        elif kind == "date_cmd":
            match = _DATE_CMD.match(line)
            if match:
                stamp = " ".join(match.group(1).split()) + " " + match.group(2)
                return timefmt.from_log_mst(datetime.strptime(stamp, "%b %d %H:%M:%S %Y")), None
        elif kind == "time_only":
            match = _TIME_ONLY.match(line)
            if match:
                return None, f"line records time {match.group(1)} MST only; no date in this log"
    except ValueError:
        return None, "timestamp in line could not be parsed"
    return None, "no timestamp in line"


def parse_log_lines(lines: list[str], kind: str) -> list[dict]:
    """Entries for read_log, oldest first, messages redacted."""
    if kind == "rich":
        return [
            {"tail_index": r.line_no,
             "ts": timefmt.from_log_mst(r.ts_mst) if r.ts_mst else None,
             "ts_inferred": r.ts_inferred,
             "ts_note": None if r.ts_mst else "no timestamp before this line in the window read",
             "source": r.source,
             "message": redact(r.message)}
            for r in parse_rich_lines(lines)
        ]
    entries = []
    for index, line in enumerate(lines):
        if not line.strip():
            continue
        ts, note = _line_timestamp(line, kind)
        entries.append({"tail_index": index, "ts": ts, "ts_inferred": False, "ts_note": note,
                        "source": None, "message": redact(line.rstrip())})
    return entries


# ── scheduler_health ─────────────────────────────────────────────────────────

def scheduler_health_from_records(records: list[LogRecord], *, now_utc: datetime,
                                  lookback_minutes: int) -> dict:
    """Pair [SCHED-JOB] start/done lines inside the window.

    A start with no matching done is reported, never dropped: "in_flight" if it began after
    the last trader restart in the window, "orphaned_by_restart" if a restart came after it.
    """
    window_start = timefmt.mst_naive(now_utc - timedelta(minutes=lookback_minutes))
    now_mst = timefmt.mst_naive(now_utc)

    open_starts: dict[str, list[LogRecord]] = {}
    orphaned: list[LogRecord] = []
    completed: list[tuple[str, float | None, datetime]] = []
    done_without_start = 0
    last_start: datetime | None = None
    last_restart: datetime | None = None
    restarts = 0

    for record in records:
        if record.ts_mst is None or record.ts_mst < window_start:
            continue
        if RESTART_MARKER in record.message:
            restarts += 1
            last_restart = record.ts_mst
            for starts in open_starts.values():
                orphaned.extend(starts)
            open_starts = {}
            continue
        started = _SCHED_START.search(record.message)
        if started:
            open_starts.setdefault(started.group(1), []).append(record)
            last_start = record.ts_mst
            continue
        done = _SCHED_DONE.search(record.message)
        if done:
            name, wall = done.group(1), done.group(2)
            if open_starts.get(name):
                open_starts[name].pop(0)
            else:
                done_without_start += 1
            completed.append((name, float(wall) if wall else None, record.ts_mst))

    def job(record: LogRecord, state: str) -> dict:
        age = int((now_mst - record.ts_mst).total_seconds())
        return {
            "name": _SCHED_START.search(record.message).group(1),
            "state": state,
            "started_at": timefmt.from_log_mst(record.ts_mst),
            "age_seconds": age,
            "exceeds_warn_seconds": age > config.IN_FLIGHT_WARN_SECONDS,
        }

    in_flight = [job(r, "in_flight") for starts in open_starts.values() for r in starts]
    without_done = sorted([job(r, "orphaned_by_restart") for r in orphaned] + in_flight,
                          key=lambda j: j["started_at"]["utc"])

    if in_flight:
        summary = "job_started_without_done"
    elif last_start is None and not completed:
        summary = "no_job_activity_in_window"
    else:
        summary = "ok"

    slowest = sorted((c for c in completed if c[1] is not None), key=lambda c: -c[1])[:5]
    return {
        "summary_state": summary,
        "window": {
            "lookback_minutes": lookback_minutes,
            "from": timefmt.from_log_mst(window_start),
            "to": timefmt.from_log_mst(now_mst),
        },
        "started_without_done": without_done,
        "completed": {
            "count": len(completed),
            "done_without_start_in_window": done_without_start,
            "slowest": [{"name": n, "wall_seconds": w, "done_at": timefmt.from_log_mst(t)} for n, w, t in slowest],
        },
        "last_job_start_at": timefmt.from_log_mst(last_start) if last_start else None,
        "seconds_since_last_job_start": int((now_mst - last_start).total_seconds()) if last_start else None,
        "last_restart_at": timefmt.from_log_mst(last_restart) if last_restart else None,
        "restarts_in_window": restarts,
    }
