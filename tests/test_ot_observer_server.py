"""ot_observer MCP server wiring: exactly the ten tools, all read-only, localhost bind,
and the tunnel hostname allowed through DNS-rebinding protection."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ot_observer_testkit import build_root  # noqa: E402

from ot_observer import config, tools  # noqa: E402
from ot_observer.server import build_server  # noqa: E402


def test_server_registers_exactly_the_ten_read_only_tools(tmp_path):
    server = build_server(build_root(tmp_path))
    listed = asyncio.run(server.list_tools())
    assert sorted(t.name for t in listed) == sorted(tools.TOOL_NAMES)
    assert all(t.annotations is not None and t.annotations.readOnlyHint is True for t in listed)


def test_server_call_reaches_the_tool(tmp_path):
    paths = build_root(tmp_path)
    server = build_server(paths)
    asyncio.run(server.call_tool("observer_status", {}))
    assert paths.audit_log.exists()


def test_server_binds_localhost_and_allows_only_known_hosts(tmp_path):
    server = build_server(build_root(tmp_path))
    assert server.settings.host == "127.0.0.1"
    assert server.settings.port == config.PORT
    security = server.settings.transport_security
    assert security.enable_dns_rebinding_protection is True
    assert "ot-mcp.ollietrades.com" in security.allowed_hosts
