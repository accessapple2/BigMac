"""MCP wiring for the observer: FastMCP over streamable HTTP, bound to 127.0.0.1, exactly
the ten tools from ot_observer.tools, all annotated read-only.

Reached remotely only through the cloudflared tunnel (ot-mcp.ollietrades.com) behind
Cloudflare Access. DNS-rebinding protection stays on; only the loopback and tunnel host
names are accepted.
"""
from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations

from . import config, tools
from .config import ObserverPaths

INSTRUCTIONS = """Read-only observer of the OllieTrades paper-trading system.
Every response has a status:
- ok: rows or data returned.
- no_rows: the read succeeded and matched nothing.
- unavailable: the source could not be read (reason says why). It does NOT mean nothing happened.
- disabled: the observer's kill switch is on.
- rejected: an argument was invalid.
- error: an unexpected failure (reason says what).
Every timestamp carries utc and mst labels and how it was stored.
scheduler_health lists jobs that started without a matching done explicitly.
prompt_text is only returned with include_prompt_text=true, and then for one decision."""

_READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)


def _given(**kwargs: Any) -> dict[str, Any]:
    return {k: v for k, v in kwargs.items() if v is not None}


def build_server(paths: ObserverPaths | None = None) -> FastMCP:
    paths = paths or ObserverPaths.default()
    security = TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=[f"127.0.0.1:{config.PORT}", f"localhost:{config.PORT}", config.PUBLIC_HOSTNAME],
        allowed_origins=[f"https://{config.PUBLIC_HOSTNAME}", f"http://127.0.0.1:{config.PORT}",
                         f"http://localhost:{config.PORT}"],
    )
    server = FastMCP("ot-observer", instructions=INSTRUCTIONS, host=config.HOST, port=config.PORT,
                     stateless_http=True, json_response=True, transport_security=security)

    def call(name: str, **kwargs: Any) -> dict:
        return tools.TOOLS[name](paths, **kwargs)

    @server.tool(name="observer_status", annotations=_READ_ONLY,
                 description="Kill switch, readability of trader.db and signals.db, freshness of each named log.")
    def observer_status() -> dict:
        return call("observer_status")

    @server.tool(name="scheduler_health", annotations=_READ_ONLY,
                 description="Trader scheduler job starts/dones from trader.log; jobs started without a "
                             "matching done are listed explicitly (in_flight or orphaned_by_restart).")
    def scheduler_health(lookback_minutes: int | None = None) -> dict:
        return call("scheduler_health", **_given(lookback_minutes=lookback_minutes))

    @server.tool(name="read_log", annotations=_READ_ONLY,
                 description=f"Tail of one named log ({', '.join(config.LOGS)}), parsed, credentials redacted.")
    def read_log(log: str, lines: int | None = None, contains: str | None = None) -> dict:
        return call("read_log", **_given(log=log, lines=lines, contains=contains))

    @server.tool(name="recent_decisions", annotations=_READ_ONLY,
                 description="decision_audit rows, newest first. prompt_text only with include_prompt_text=true, "
                             "capped at one row.")
    def recent_decisions(limit: int | None = None, player_id: str | None = None, event_type: str | None = None,
                         decision_id: int | None = None, include_prompt_text: bool = False) -> dict:
        extra = {"include_prompt_text": True} if include_prompt_text else {}
        return call("recent_decisions", **_given(limit=limit, player_id=player_id, event_type=event_type,
                                                 decision_id=decision_id), **extra)

    @server.tool(name="recent_trades", annotations=_READ_ONLY, description="trades rows, newest first.")
    def recent_trades(limit: int | None = None, player_id: str | None = None, symbol: str | None = None) -> dict:
        return call("recent_trades", **_given(limit=limit, player_id=player_id, symbol=symbol))

    @server.tool(name="open_positions", annotations=_READ_ONLY, description="positions with nonzero quantity.")
    def open_positions(limit: int | None = None, player_id: str | None = None) -> dict:
        return call("open_positions", **_given(limit=limit, player_id=player_id))

    @server.tool(name="fleet_roster", annotations=_READ_ONLY, description="ai_players seats with halt state.")
    def fleet_roster(limit: int | None = None, halt_mode: str | None = None) -> dict:
        return call("fleet_roster", **_given(limit=limit, halt_mode=halt_mode))

    @server.tool(name="lifecycle_ledger", annotations=_READ_ONLY,
                 description="fleet_lifecycle_ledger rows, newest first.")
    def lifecycle_ledger(limit: int | None = None, target_name: str | None = None) -> dict:
        return call("lifecycle_ledger", **_given(limit=limit, target_name=target_name))

    @server.tool(name="recent_notifications", annotations=_READ_ONLY,
                 description="notifications (alerts), newest first.")
    def recent_notifications(limit: int | None = None, severity: str | None = None,
                             type: str | None = None) -> dict:  # noqa: A002
        return call("recent_notifications", **_given(limit=limit, severity=severity, type=type))

    @server.tool(name="recent_trade_signals", annotations=_READ_ONLY,
                 description="signals.db trade_signals rows, newest first.")
    def recent_trade_signals(limit: int | None = None, symbol: str | None = None,
                             agent_name: str | None = None) -> dict:
        return call("recent_trade_signals", **_given(limit=limit, symbol=symbol, agent_name=agent_name))

    return server
