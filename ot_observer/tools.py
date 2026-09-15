"""The ten fixed read-only tools of the ot-mcp observer.

Every call:
- checks the kill-switch file first and returns status "disabled" -- never empty results;
- reads through ot_observer.db (read-only handle per request, fail fast);
- reports one explicit status: ok | no_rows | unavailable (could not read -- never means
  nothing happened) | disabled | rejected | error;
- labels every timestamp in UTC and MST with its stored basis;
- is appended to the audit log.

Tables, columns and queries are fixed here. Filters are validated identifiers; logs are a
fixed name enum. Nothing accepts SQL, a path, or a shell command.

Timestamp bases (verified against the writers, 2026-09-14):
  decision_audit.created_at, trades.executed_at, positions.opened_at,
  fleet_lifecycle_ledger.created_at, notifications.timestamp,
  trade_signals.created_at ............................ utc
  ai_players.halted_at ................................. per value (see timefmt)
  trade_signals.executed_at / dismissed_at ............. mst_local (signal-center/server.py
      writes datetime.now().isoformat(); no stored values yet on 2026-09-14)
"""
from __future__ import annotations

import functools
import inspect
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from . import audit, config, db, logparse, timefmt
from .config import ObserverPaths

TOOLS: dict[str, Callable[..., dict]] = {}
_IDENTIFIER = re.compile(r"^[A-Za-z0-9_.:\-]{1,%d}$" % config.FILTER_MAX_LEN)
_HALT_MODES = ("active", "exit_only", "full")


class Rejected(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


# ── dispatch ─────────────────────────────────────────────────────────────────

def _tool(name: str) -> Callable[[Callable[..., dict]], Callable[..., dict]]:
    def register(body: Callable[..., dict]) -> Callable[..., dict]:
        signature = inspect.signature(body)

        @functools.wraps(body)
        def run(paths: ObserverPaths, **kwargs: Any) -> dict:
            return _run(name, paths, kwargs, signature, body)

        TOOLS[name] = run
        return run

    return register


def _unreadable(status: str, reason: str) -> dict:
    return {"status": status, "reason": reason, "rows": None, "row_count": None, "data": None}


def _run(name: str, paths: ObserverPaths, kwargs: dict[str, Any],
         signature: inspect.Signature, body: Callable[..., dict]) -> dict:
    started = time.monotonic()
    result: dict[str, Any] = {"tool": name, "status": None, "reason": None,
                              "generated_at": timefmt.now_label()}
    if paths.kill_switch.exists():
        result["trader_log_last_write"] = None
        result.update(_unreadable("disabled", f"kill switch file present: {paths.kill_switch} (remove it to re-enable)"))
    else:
        result["trader_log_last_write"] = _file_last_write(paths.log_dir / "trader.log")
        try:
            try:
                signature.bind(paths, **kwargs)
            except TypeError as exc:
                raise Rejected(f"invalid arguments: {exc}") from exc
            result.update(body(paths, **kwargs))
        except Rejected as exc:
            result.update(_unreadable("rejected", exc.reason))
        except db.Unavailable as exc:
            result.update(_unreadable("unavailable", exc.reason))
        except Exception as exc:  # an explicit error state, never silence
            result.update(_unreadable("error", f"{type(exc).__name__}: {exc}"))
    result["duration_ms"] = round((time.monotonic() - started) * 1000, 1)
    failure = audit.append(paths, tool=name, args=kwargs, status=result["status"],
                           row_count=result.get("row_count"), duration_ms=result["duration_ms"])
    if failure:
        result["audit"] = f"audit write failed: {failure}"
    return result


# ── shared helpers ───────────────────────────────────────────────────────────

def _limit(requested: Any, *, label: str = "limit", default: int = config.DEFAULT_LIMIT,
           cap: int = config.MAX_LIMIT) -> tuple[int, bool]:
    if requested is None:
        return default, False
    if isinstance(requested, bool) or not isinstance(requested, int):
        raise Rejected(f"{label} must be an integer")
    applied = max(1, min(requested, cap))
    return applied, applied != requested


def _identifier(label: str, value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not _IDENTIFIER.match(value):
        raise Rejected(f"{label} must match {_IDENTIFIER.pattern}")
    return value


def _file_last_write(path: Path) -> dict:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return {"status": "unavailable", "reason": f"not found: {path}"}
    except OSError as exc:
        return {"status": "unavailable", "reason": f"{type(exc).__name__}: {exc}"}
    return {
        "status": "available",
        **timefmt.label(datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)),
        "age_seconds": int(time.time() - stat.st_mtime),
        "size_bytes": stat.st_size,
    }


def _select(db_path: Path, *, table: str, columns: tuple[str, ...], order_by: str,
            limit_requested: Any, limit: int, clamped: bool, cap: int = config.MAX_LIMIT,
            filters: tuple[tuple[str, Any], ...] = (), where: str | None = None,
            timestamps: dict[str, str] | None = None, truncate: dict[str, int] | None = None) -> dict:
    clauses = [where] if where else []
    params: list[Any] = []
    for column, value in filters:
        if value is not None:
            clauses.append(f"{column} = ?")
            params.append(value)
    sql = f"SELECT {', '.join(columns)} FROM {table}"
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += f" ORDER BY {order_by} LIMIT ?"
    rows = db.fetch(db_path, sql, (*params, limit + 1))
    truncated = len(rows) > limit
    rows = rows[:limit]
    for row in rows:
        for column, basis in (timestamps or {}).items():
            row[column] = timefmt.from_db(row[column], basis)
        for column, max_chars in (truncate or {}).items():
            value = row.get(column)
            if isinstance(value, str) and len(value) > max_chars:
                row[column] = value[:max_chars]
                row[f"{column}_truncated_at"] = max_chars
    return {
        "status": "ok" if rows else "no_rows",
        "rows": rows,
        "row_count": len(rows),
        "limit_requested": limit_requested,
        "limit_applied": limit,
        "limit_clamped": clamped,
        "cap": cap,
        "truncated": truncated,
    }


def _db_probe(path: Path) -> dict:
    try:
        [row] = db.fetch(path, "SELECT count(*) AS tables FROM sqlite_master WHERE type = 'table'")
    except db.Unavailable as exc:
        return {"status": "unavailable", "reason": exc.reason, "path": str(path)}
    return {"status": "available", "path": str(path), "tables": row["tables"]}


# ── the ten tools (order is the published order) ─────────────────────────────

@_tool("observer_status")
def observer_status(paths: ObserverPaths) -> dict:
    """Kill switch, readability of each database, and freshness of each named log."""
    return {
        "status": "ok",
        "row_count": None,
        "data": {
            "kill_switch": {"present": False, "path": str(paths.kill_switch)},
            "trader_db": _db_probe(paths.trader_db),
            "signals_db": _db_probe(paths.signals_db),
            "named_logs": {name: {"file": filename, **_file_last_write(paths.log_dir / filename)}
                           for name, (filename, _kind) in config.LOGS.items()},
            "limits": {
                "default_rows": config.DEFAULT_LIMIT, "max_rows": config.MAX_LIMIT,
                "prompt_text_rows": config.PROMPT_TEXT_LIMIT, "log_lines_max": config.LOG_LINES_MAX,
                "busy_timeout_ms": config.BUSY_TIMEOUT_MS, "query_budget_ms": config.QUERY_BUDGET_MS,
            },
        },
    }


@_tool("scheduler_health")
def scheduler_health(paths: ObserverPaths, lookback_minutes: int | None = None) -> dict:
    """[SCHED-JOB] start/done pairing from trader.log, with started-without-done explicit."""
    minutes, clamped = _limit(lookback_minutes, label="lookback_minutes",
                              default=config.SCHED_LOOKBACK_DEFAULT_MIN, cap=config.SCHED_LOOKBACK_MAX_MIN)
    path = paths.log_dir / "trader.log"
    now = datetime.now(timezone.utc)
    try:
        records, window_truncated, bytes_read = logparse.read_recent_rich(
            path, lookback_minutes=minutes, now_utc=now, byte_cap=config.SCHED_BYTE_CAP)
    except FileNotFoundError as exc:
        raise db.Unavailable(f"trader log not found: {path}") from exc
    except OSError as exc:
        raise db.Unavailable(f"trader log unreadable: {type(exc).__name__}: {exc}") from exc
    health = logparse.scheduler_health_from_records(records, now_utc=now, lookback_minutes=minutes)
    health["window"].update(lookback_clamped=clamped, log_window_truncated=window_truncated,
                            bytes_read=bytes_read)
    return {"status": "ok", "data": health, "row_count": len(health["started_without_done"])}


@_tool("read_log")
def read_log(paths: ObserverPaths, log: str, lines: int | None = None,
             contains: str | None = None) -> dict:
    """Tail of one named log, parsed into records with labelled timestamps, redacted."""
    if not isinstance(log, str) or log not in config.LOGS:
        raise Rejected(f"log must be one of: {', '.join(config.LOGS)}")
    limit, clamped = _limit(lines, label="lines", default=config.LOG_LINES_DEFAULT, cap=config.LOG_LINES_MAX)
    if contains is not None and (not isinstance(contains, str) or not 1 <= len(contains) <= config.CONTAINS_MAX_LEN):
        raise Rejected(f"contains must be a string of 1-{config.CONTAINS_MAX_LEN} characters")
    filename, kind = config.LOGS[log]
    path = paths.log_dir / filename
    scan = config.LOG_LINES_MAX * 4 if contains else limit * (4 if kind == "rich" else 1)
    try:
        raw = logparse.tail_lines(path, scan, config.LOG_BYTE_CAP)
    except FileNotFoundError as exc:
        raise db.Unavailable(f"log file not found: {path}") from exc
    except OSError as exc:
        raise db.Unavailable(f"log file unreadable: {type(exc).__name__}: {exc}") from exc
    entries = logparse.parse_log_lines(raw, kind)
    if contains:
        entries = [e for e in entries if contains in e["message"]]
    truncated = len(entries) > limit
    entries = entries[-limit:]
    return {
        "status": "ok" if entries else "no_rows",
        "rows": entries,
        "row_count": len(entries),
        "limit_requested": lines,
        "limit_applied": limit,
        "limit_clamped": clamped,
        "cap": config.LOG_LINES_MAX,
        "truncated": truncated,
        "log": log,
        "scanned_lines": len(raw),
    }


@_tool("recent_decisions")
def recent_decisions(paths: ObserverPaths, limit: int | None = None, player_id: str | None = None,
                     event_type: str | None = None, decision_id: int | None = None,
                     include_prompt_text: bool = False) -> dict:
    """decision_audit, newest first. prompt_text only with include_prompt_text, one row."""
    if not isinstance(include_prompt_text, bool):
        raise Rejected("include_prompt_text must be true or false")
    if decision_id is not None and (isinstance(decision_id, bool) or not isinstance(decision_id, int)):
        raise Rejected("decision_id must be an integer")
    columns = ("id", "created_at", "event_type", "player_id", "symbol", "signal_id", "trade_id",
               "regime", "confidence", "gate_verdict", "reasoning_snippet", "prompt_truncation_flag")
    if include_prompt_text:
        columns += ("prompt_text",)
        applied, clamped = config.PROMPT_TEXT_LIMIT, limit not in (None, config.PROMPT_TEXT_LIMIT)
        cap = config.PROMPT_TEXT_LIMIT
    else:
        applied, clamped = _limit(limit)
        cap = config.MAX_LIMIT
    result = _select(
        paths.trader_db, table="decision_audit", columns=columns, order_by="id DESC",
        limit_requested=limit, limit=applied, clamped=clamped, cap=cap,
        filters=(("id", decision_id), ("player_id", _identifier("player_id", player_id)),
                 ("event_type", _identifier("event_type", event_type))),
        timestamps={"created_at": "utc"}, truncate={"reasoning_snippet": 600},
    )
    result["prompt_text_included"] = include_prompt_text
    result["prompt_text_cap_applied"] = include_prompt_text
    return result


@_tool("recent_trades")
def recent_trades(paths: ObserverPaths, limit: int | None = None, player_id: str | None = None,
                  symbol: str | None = None) -> dict:
    """trades, newest first. Row flags tz_bucket_suspect / pnl_basis_invalid are returned."""
    applied, clamped = _limit(limit)
    return _select(
        paths.trader_db, table="trades",
        columns=("id", "executed_at", "player_id", "symbol", "action", "qty", "price", "entry_price",
                 "exit_price", "realized_pnl", "asset_type", "option_type", "strike_price", "expiry_date",
                 "execution_type", "alpaca_order_id", "alpaca_status", "signal_id", "known_contaminated",
                 "pnl_basis_invalid", "tz_bucket_suspect", "reasoning"),
        order_by="id DESC", limit_requested=limit, limit=applied, clamped=clamped,
        filters=(("player_id", _identifier("player_id", player_id)), ("symbol", _identifier("symbol", symbol))),
        timestamps={"executed_at": "utc"}, truncate={"reasoning": 300},
    )


@_tool("open_positions")
def open_positions(paths: ObserverPaths, limit: int | None = None, player_id: str | None = None) -> dict:
    """positions with nonzero quantity, newest first."""
    applied, clamped = _limit(limit)
    return _select(
        paths.trader_db, table="positions",
        columns=("id", "opened_at", "player_id", "symbol", "qty", "avg_price", "asset_type", "option_type",
                 "strike_price", "expiry_date"),
        where="qty != 0", order_by="id DESC", limit_requested=limit, limit=applied, clamped=clamped,
        filters=(("player_id", _identifier("player_id", player_id)),),
        timestamps={"opened_at": "utc"},
    )


@_tool("fleet_roster")
def fleet_roster(paths: ObserverPaths, limit: int | None = None, halt_mode: str | None = None) -> dict:
    """ai_players seats with halt state."""
    if halt_mode is not None and halt_mode not in _HALT_MODES:
        raise Rejected(f"halt_mode must be one of: {', '.join(_HALT_MODES)}")
    applied, clamped = _limit(limit)
    return _select(
        paths.trader_db, table="ai_players",
        columns=("id", "display_name", "provider", "model_id", "halt_mode", "halt_reason", "halted_at",
                 "crew_role", "role", "is_active", "is_paused", "season"),
        order_by="id", limit_requested=limit, limit=applied, clamped=clamped,
        filters=(("halt_mode", halt_mode),),
        timestamps={"halted_at": "halted_at"}, truncate={"halt_reason": 300},
    )


@_tool("lifecycle_ledger")
def lifecycle_ledger(paths: ObserverPaths, limit: int | None = None, target_name: str | None = None) -> dict:
    """fleet_lifecycle_ledger, newest first. Backfilled rows carry the backfill time."""
    applied, clamped = _limit(limit)
    return _select(
        paths.trader_db, table="fleet_lifecycle_ledger",
        columns=("id", "created_at", "target_type", "target_name", "action", "reason", "order_doc",
                 "resume_by", "review_by", "backfilled", "created_by"),
        order_by="id DESC", limit_requested=limit, limit=applied, clamped=clamped,
        filters=(("target_name", _identifier("target_name", target_name)),),
        timestamps={"created_at": "utc"}, truncate={"reason": 600},
    )


@_tool("recent_notifications")
def recent_notifications(paths: ObserverPaths, limit: int | None = None, severity: str | None = None,
                         type: str | None = None) -> dict:  # noqa: A002 -- mirrors the column name
    """notifications (alerts), newest first."""
    applied, clamped = _limit(limit)
    return _select(
        paths.trader_db, table="notifications",
        columns=("id", "timestamp", "type", "severity", "title", "body", "agent_id", "acknowledged"),
        order_by="id DESC", limit_requested=limit, limit=applied, clamped=clamped,
        filters=(("severity", _identifier("severity", severity)), ("type", _identifier("type", type))),
        timestamps={"timestamp": "utc"}, truncate={"body": 500},
    )


@_tool("recent_trade_signals")
def recent_trade_signals(paths: ObserverPaths, limit: int | None = None, symbol: str | None = None,
                         agent_name: str | None = None) -> dict:
    """signals.db trade_signals, newest first."""
    applied, clamped = _limit(limit)
    return _select(
        paths.signals_db, table="trade_signals",
        columns=("id", "created_at", "type", "symbol", "action", "entry_price", "stop_loss", "take_profit",
                 "confidence", "agent_name", "model_used", "status", "executed_at", "dismissed_at",
                 "timeframe", "w3_gex_regime", "reasoning"),
        order_by="id DESC", limit_requested=limit, limit=applied, clamped=clamped,
        filters=(("symbol", _identifier("symbol", symbol)), ("agent_name", _identifier("agent_name", agent_name))),
        timestamps={"created_at": "utc", "executed_at": "mst_local", "dismissed_at": "mst_local"},
        truncate={"reasoning": 300},
    )


TOOL_NAMES = tuple(TOOLS)
