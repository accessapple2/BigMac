"""OllieTrades read-only MCP observer (ot-mcp).

Runs as its own process: main.py replaces sqlite3.connect globally, so the observer must
never be imported into, or run inside, the trader. Nothing here writes to a database,
runs a shell, or accepts SQL or paths from the caller.
"""
