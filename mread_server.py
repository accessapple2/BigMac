#!/usr/bin/env python3
"""
M-READ — OllieTrades read-only MCP server
=========================================

The XO's cockpit read-layer. Exposes OllieTrades state to Claude (and any MCP
client) as READ-ONLY tools. There are NO write tools on this server, by design —
RULE #1 (Schwab read-only / Alpaca paper-only) and the never-DELETE/DROP/TRUNCATE
sacred-DB rules are enforced at the protocol boundary: the capability to mutate
simply does not exist here.

Defense in depth:
  - The SQLite connection is opened mode=ro (the OS/SQLite itself refuses writes).
  - query_trader_db additionally rejects anything that isn't a single SELECT/WITH.
  - Every external call (Alpaca, Polygon) hits read endpoints only.

Transport: streamable-http (works as a remote Claude custom connector).
Auth: a shared secret token, supplied via ?token=, X-MCP-Token header, or
      Authorization: Bearer. Intended to sit BEHIND your cloudflared tunnel +
      Cloudflare Access. Read-only + paper-only means blast radius is near-zero
      even if the token leaks, but don't publish it.

Run:
    pip install "mcp[cli]" uvicorn
    # set env (see DEPLOY.md), then:
    python3 mread_server.py
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import urllib.parse
import urllib.request
from typing import Any

import uvicorn
from mcp.server.fastmcp import FastMCP
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse

# ──────────────────────────────────────────────────────────────────────────────
# Config (all secrets from env — nothing hardcoded)
# ──────────────────────────────────────────────────────────────────────────────
TRADER_DB        = os.environ.get("TRADER_DB", "/Users/bigmac/autonomous-trader/data/trader.db")
POLYGON_KEY      = os.environ.get("POLYGON_API_KEY", "")
ALPACA_KEY       = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET    = os.environ.get("ALPACA_SECRET_KEY", "")
ALPACA_BASE      = os.environ.get("ALPACA_PAPER_BASE", "https://paper-api.alpaca.markets")
SCOREBOARD_TABLE = os.environ.get("SCOREBOARD_TABLE", "scoreboard")     # adjust to canonical source
SCHWAB_TABLE     = os.environ.get("SCHWAB_TABLE", "schwab_holdings")    # adjust to sync_schwab_live target
MCP_URL_TOKEN    = os.environ.get("MCP_URL_TOKEN", "")                  # shared secret; empty = no auth (local only)
HOST             = os.environ.get("MREAD_HOST", "127.0.0.1")
PORT             = int(os.environ.get("MREAD_PORT", "8790"))
MAX_ROWS         = int(os.environ.get("MREAD_MAX_ROWS", "500"))

mcp = FastMCP("ollietrades-read", instructions=(
    "Read-only access to OllieTrades state: Alpaca paper positions, Polygon "
    "quotes, the trader.db (SELECT only), signal_observations, the scoreboard, "
    "and the Schwab read-only display mirror. No tool here can modify anything."
))

# ──────────────────────────────────────────────────────────────────────────────
# DB helpers (read-only connection — the hard guarantee)
# ──────────────────────────────────────────────────────────────────────────────
def _ro_conn() -> sqlite3.Connection:
    """Open trader.db strictly read-only. SQLite refuses any write on this handle."""
    con = sqlite3.connect(f"file:{TRADER_DB}?mode=ro", uri=True, timeout=10)
    con.row_factory = sqlite3.Row
    return con


_FORBIDDEN = (
    "insert", "update", "delete", "drop", "alter", "create", "replace",
    "truncate", "attach", "detach", "pragma", "vacuum", "reindex", "grant",
)


def _is_read_only_sql(sql: str) -> tuple[bool, str]:
    s = sql.strip().rstrip(";").strip()
    if not s:
        return False, "empty query"
    if ";" in s:
        return False, "multiple statements are not allowed"
    low = s.lower()
    if not (low.startswith("select") or low.startswith("with")):
        return False, "only SELECT / WITH queries are allowed"
    for kw in _FORBIDDEN:
        if re.search(rf"\b{kw}\b", low):
            return False, f"keyword '{kw}' is not allowed on this read-only layer"
    return True, ""


def _rows_to_dicts(cur: sqlite3.Cursor, rows: list) -> list[dict]:
    cols = [d[0] for d in cur.description] if cur.description else []
    return [dict(zip(cols, r)) for r in rows]


def _table_columns(con: sqlite3.Connection, table: str) -> list[str]:
    """Internal schema introspection (runs on our own read-only handle)."""
    try:
        cur = con.execute(f"SELECT * FROM {table} LIMIT 0")
        return [d[0] for d in cur.description]
    except sqlite3.Error:
        return []


# ──────────────────────────────────────────────────────────────────────────────
# HTTP helper for external read APIs
# ──────────────────────────────────────────────────────────────────────────────
def _get_json(url: str, headers: dict | None = None, timeout: int = 10) -> Any:
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


# ──────────────────────────────────────────────────────────────────────────────
# TOOLS — all read-only
# ──────────────────────────────────────────────────────────────────────────────
@mcp.tool()
def describe_schema(table: str = "") -> str:
    """List tables in trader.db, or the columns of one table.

    Call with no argument to list all tables. Pass a table name to see its
    columns. Use this to navigate the DB before writing query_trader_db calls.
    """
    try:
        con = _ro_conn()
        if not table:
            cur = con.execute(
                "SELECT name, type FROM sqlite_master "
                "WHERE type IN ('table','view') ORDER BY name"
            )
            rows = _rows_to_dicts(cur, cur.fetchall())
            con.close()
            return json.dumps({"objects": rows}, indent=2)
        cols = _table_columns(con, table)
        con.close()
        if not cols:
            return json.dumps({"error": f"no such table/view: {table}"})
        return json.dumps({"table": table, "columns": cols}, indent=2)
    except sqlite3.Error as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def query_trader_db(sql: str) -> str:
    """Run a READ-ONLY (SELECT / WITH) query against trader.db.

    Multiple statements, and any write/DDL keyword, are rejected. The underlying
    connection is opened read-only, so writes are impossible even if the guard is
    bypassed. Results are capped at MREAD_MAX_ROWS rows.
    """
    ok, why = _is_read_only_sql(sql)
    if not ok:
        return json.dumps({"error": f"rejected: {why}"})
    try:
        con = _ro_conn()
        cur = con.execute(sql)
        rows = cur.fetchmany(MAX_ROWS + 1)
        truncated = len(rows) > MAX_ROWS
        out = _rows_to_dicts(cur, rows[:MAX_ROWS])
        con.close()
        return json.dumps(
            {"row_count": len(out), "truncated": truncated, "rows": out},
            indent=2, default=str,
        )
    except sqlite3.Error as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_positions() -> str:
    """Current Alpaca PAPER account positions with unrealized P&L (read-only)."""
    if not (ALPACA_KEY and ALPACA_SECRET):
        return json.dumps({"error": "ALPACA_API_KEY / ALPACA_SECRET_KEY not set"})
    headers = {"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET}
    try:
        data = _get_json(f"{ALPACA_BASE}/v2/positions", headers=headers)
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": f"alpaca: {e}"})
    keep = ("symbol", "qty", "avg_entry_price", "current_price", "market_value",
            "unrealized_pl", "unrealized_plpc", "side")
    positions = [{k: p.get(k) for k in keep} for p in data]
    return json.dumps({"count": len(positions), "positions": positions}, indent=2)


@mcp.tool()
def get_quote(ticker: str) -> str:
    """Latest Polygon snapshot for a ticker: last price, day change, prev close.

    NOTE: on the Stocks Starter plan, data is ~15-minute delayed. Treat as
    indicative, not real-time tick.
    """
    if not POLYGON_KEY:
        return json.dumps({"error": "POLYGON_API_KEY not set"})
    t = urllib.parse.quote(ticker.upper().strip())
    url = (f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/"
           f"tickers/{t}?apiKey={POLYGON_KEY}")
    try:
        data = _get_json(url)
    except Exception as e:  # noqa: BLE001
        return json.dumps({"error": f"polygon: {e}"})
    tk = data.get("ticker") or {}
    last = (tk.get("lastTrade") or {}).get("p")
    day = tk.get("day") or {}
    prev = tk.get("prevDay") or {}
    return json.dumps({
        "ticker": ticker.upper(),
        "last": last,
        "day_open": day.get("o"), "day_high": day.get("h"),
        "day_low": day.get("l"), "day_close": day.get("c"), "day_volume": day.get("v"),
        "prev_close": prev.get("c"),
        "change": tk.get("todaysChange"),
        "change_pct": tk.get("todaysChangePerc"),
        "note": "Starter plan data may be ~15 min delayed",
    }, indent=2, default=str)


@mcp.tool()
def get_signal_observations(since: str = "", source: str = "", limit: int = 200) -> str:
    """Rows from signal_observations, newest first.

    Args:
      since:  optional ISO date/datetime lower bound (e.g. '2026-06-22').
      source: optional exact source filter (e.g. 'uhura', 'bk_orb').
      limit:  max rows (hard-capped at MREAD_MAX_ROWS).

    Schema is auto-detected (timestamp + source columns) so this survives minor
    column-name differences. If detection fails, fall back to query_trader_db.
    """
    limit = min(limit, MAX_ROWS)
    try:
        con = _ro_conn()
        cols = _table_columns(con, "signal_observations")
        if not cols:
            con.close()
            return json.dumps({"error": "signal_observations table not found"})
        ts_col = next((c for c in
                       ("observed_at", "created_at", "captured_at", "ts", "timestamp", "inserted_at")
                       if c in cols), None)
        src_col = next((c for c in ("source", "feed", "signal_source") if c in cols), None)

        where, params = [], []
        if since and ts_col:
            where.append(f"{ts_col} >= ?"); params.append(since)
        if source and src_col:
            where.append(f"{src_col} = ?"); params.append(source)
        clause = (" WHERE " + " AND ".join(where)) if where else ""
        order = f" ORDER BY {ts_col} DESC" if ts_col else ""
        sql = f"SELECT * FROM signal_observations{clause}{order} LIMIT ?"
        params.append(limit)

        cur = con.execute(sql, params)
        out = _rows_to_dicts(cur, cur.fetchall())
        con.close()
        return json.dumps(
            {"row_count": len(out), "ts_col": ts_col, "source_col": src_col, "rows": out},
            indent=2, default=str,
        )
    except sqlite3.Error as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_scoreboard() -> str:
    """Realized P&L per agent from the canonical scoreboard table.

    Reads SCOREBOARD_TABLE (env, default 'scoreboard'). Explicitly ignores the
    corrected_pnl column (known NULL dead code, flagged for audit). If the table
    name is wrong, set SCOREBOARD_TABLE or use describe_schema to find it.
    """
    try:
        con = _ro_conn()
        cols = _table_columns(con, SCOREBOARD_TABLE)
        if not cols:
            con.close()
            return json.dumps({
                "error": f"table '{SCOREBOARD_TABLE}' not found",
                "hint": "set SCOREBOARD_TABLE env, or call describe_schema to locate it",
            })
        id_col = next((c for c in ("player_id", "agent", "agent_id", "name") if c in cols), cols[0])
        pnl_col = next((c for c in ("realized_pnl", "realized_pl", "pnl", "total_pnl") if c in cols), None)
        if pnl_col:
            sql = (f"SELECT {id_col} AS agent, {pnl_col} AS realized_pnl "
                   f"FROM {SCOREBOARD_TABLE} ORDER BY {pnl_col} DESC")
        else:
            sql = f"SELECT * FROM {SCOREBOARD_TABLE}"
        cur = con.execute(sql)
        out = _rows_to_dicts(cur, cur.fetchall())
        con.close()
        return json.dumps(
            {"source_table": SCOREBOARD_TABLE, "row_count": len(out), "rows": out},
            indent=2, default=str,
        )
    except sqlite3.Error as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_schwab_holdings() -> str:
    """Schwab read-only display mirror (RULE #1: display only, never traded).

    Reads SCHWAB_TABLE (env, default 'schwab_holdings'), the canonical target of
    sync_schwab_live.py. Set SCHWAB_TABLE if the name differs.
    """
    try:
        con = _ro_conn()
        cols = _table_columns(con, SCHWAB_TABLE)
        if not cols:
            con.close()
            return json.dumps({
                "error": f"table '{SCHWAB_TABLE}' not found",
                "hint": "set SCHWAB_TABLE env, or call describe_schema to locate it",
            })
        cur = con.execute(f"SELECT * FROM {SCHWAB_TABLE}")
        out = _rows_to_dicts(cur, cur.fetchmany(MAX_ROWS))
        con.close()
        return json.dumps(
            {"source_table": SCHWAB_TABLE, "row_count": len(out),
             "note": "display-only mirror; never an execution target", "rows": out},
            indent=2, default=str,
        )
    except sqlite3.Error as e:
        return json.dumps({"error": str(e)})


# ──────────────────────────────────────────────────────────────────────────────
# Auth middleware + app assembly
# ──────────────────────────────────────────────────────────────────────────────
class TokenAuth(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path == "/health":
            return await call_next(request)
        if MCP_URL_TOKEN:
            supplied = (
                request.query_params.get("token")
                or request.headers.get("x-mcp-token")
                or request.headers.get("authorization", "").removeprefix("Bearer ").strip()
                or None
            )
            if supplied != MCP_URL_TOKEN:
                return JSONResponse({"error": "unauthorized"}, status_code=401)
        return await call_next(request)


def build_app():
    app = mcp.streamable_http_app()          # mounts MCP at /mcp
    app.add_middleware(TokenAuth)
    async def health(_req):                   # unauthenticated, for cloudflared
        return PlainTextResponse("ok")
    app.add_route("/health", health, methods=["GET"])
    return app


if __name__ == "__main__":
    if not MCP_URL_TOKEN and HOST not in ("127.0.0.1", "localhost"):
        print("WARNING: MCP_URL_TOKEN is empty and host is not loopback — "
              "anyone who can reach this port gets read access. Set a token.")
    print(f"[M-READ] trader.db = {TRADER_DB}")
    print(f"[M-READ] serving on http://{HOST}:{PORT}/mcp   (health: /health)")
    uvicorn.run(build_app(), host=HOST, port=PORT)
