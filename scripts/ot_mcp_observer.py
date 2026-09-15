#!/usr/bin/env python3
"""ot-mcp read-only observer entrypoint (streamable HTTP on 127.0.0.1:8765).

Runs as its own process under the LaunchDaemon com.trademinds.ot-mcp -- never inside
main.py, which replaces sqlite3.connect globally. Kill switch: create
data/ot_observer.disabled; every tool then answers "disabled".
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ot_observer.server import build_server  # noqa: E402

if __name__ == "__main__":
    build_server().run(transport="streamable-http")
