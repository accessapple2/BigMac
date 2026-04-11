"""
engine/mcp_server.py — TradeMinds MCP Server (port 8081)

Exposes TradeMinds as an MCP (Model Context Protocol) server so any
AI tool — Claude Code, Cursor, other agents — can query signals,
check agent status, and interact with the arena.

Transport : HTTP JSON-RPC 2.0  (POST /mcp)
Auth      : Bearer token via TRADEMINDS_MCP_KEY env var (optional locally)
Port      : 8081 (separate from main dashboard on 8080)

Usage from Claude Code:
    Add to ~/.claude/mcp_servers.json:
    {
      "trademinds": {
        "type": "http",
        "url": "http://127.0.0.1:8081/mcp",
        "headers": {"Authorization": "Bearer <TRADEMINDS_MCP_KEY>"}
      }
    }
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

load_dotenv(override=True)

logger = logging.getLogger("mcp_server")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [MCP] %(message)s")

# ── Config ─────────────────────────────────────────────────────────────────────
MCP_PORT   = 8081
TRADER_URL = "http://127.0.0.1:8080"
MCP_KEY    = os.getenv("TRADEMINDS_MCP_KEY", "")

# ── Tool definitions ───────────────────────────────────────────────────────────
TOOLS: list[dict] = [
    {
        "name": "trademinds_signals",
        "description": (
            "Get the latest trading signals from the TradeMinds fast scanner. "
            "Returns ticker, direction (BUY/SELL/HOLD), confidence score, and reasoning."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "ticker": {
                    "type": "string",
                    "description": "Optional ticker symbol to filter (e.g. 'NVDA').",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max signals to return (default 20).",
                    "default": 20,
                },
            },
        },
    },
    {
        "name": "trademinds_leaderboard",
        "description": (
            "Get the current AI agent performance leaderboard. "
            "Shows P&L, return %, win rate, and rank for every crew member."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "trademinds_bridge_vote",
        "description": (
            "Get the latest Bridge Vote — the AI crew's consensus on market direction. "
            "Returns BULLISH/BEARISH/NEUTRAL conviction with individual agent votes."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "detail": {
                    "type": "boolean",
                    "description": "If true, return individual agent votes. Default false (summary only).",
                    "default": False,
                },
            },
        },
    },
    {
        "name": "trademinds_portfolio",
        "description": (
            "Get the Webull Portfolio (Captain Kirk) current positions, "
            "cash balance, and unrealized P&L."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "trademinds_health",
        "description": (
            "Get TradeMinds system health: scanner status, Ollama uptime, "
            "last scan time, active agents, and any active alerts."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "trademinds_ask_archer",
        "description": (
            "Send a question or command to Archer (the TradeMinds CIC AI) "
            "and get a natural-language response. Use for market analysis, "
            "trade ideas, or fleet status questions."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "message": {
                    "type": "string",
                    "description": "The question or command for Archer CIC.",
                },
            },
            "required": ["message"],
        },
    },
    {
        "name": "trademinds_create_agent",
        "description": (
            "Create a new AI trading agent in the TradeMinds fleet. "
            "The agent will immediately start trading with its assigned strategy."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Display name for the new agent.",
                },
                "model": {
                    "type": "string",
                    "description": (
                        "LLM model ID (e.g. 'ollama/llama3', 'claude-sonnet-4-6', "
                        "'gemini-2.5-flash')."
                    ),
                },
                "strategy": {
                    "type": "string",
                    "description": "Trading strategy description.",
                    "default": "Balanced growth with risk management",
                },
                "cash": {
                    "type": "number",
                    "description": "Starting capital in USD (default 7000).",
                    "default": 7000,
                },
            },
            "required": ["name", "model"],
        },
    },
    {
        "name": "trademinds_risk",
        "description": (
            "Get current VaR (Value at Risk) and optional stress test results "
            "for the fleet portfolio. Parametric + historical VaR at 95%/99%."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "scenario": {
                    "type": "string",
                    "description": (
                        "Optional stress scenario to run: "
                        "crash_5, crash_10, crash_20, tech_rotate, vix_35, rate_50bps. "
                        "Omit for VaR snapshot only."
                    ),
                },
            },
        },
    },
]

# ── FastAPI app ────────────────────────────────────────────────────────────────
app = FastAPI(title="TradeMinds MCP Server", version="3.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Auth ───────────────────────────────────────────────────────────────────────
def _check_auth(request: Request) -> None:
    """Enforce Bearer token if TRADEMINDS_MCP_KEY is set."""
    if not MCP_KEY:
        return  # Open — local dev with no key configured
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer ") or auth[7:].strip() != MCP_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing TRADEMINDS_MCP_KEY")

# ── Trader API proxy helpers ───────────────────────────────────────────────────
async def _get(path: str, params: dict | None = None) -> Any:
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"{TRADER_URL}{path}", params=params or {})
        r.raise_for_status()
        return r.json()

async def _post(path: str, body: dict) -> Any:
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(f"{TRADER_URL}{path}", json=body)
        r.raise_for_status()
        return r.json()

# ── Tool dispatch ──────────────────────────────────────────────────────────────
async def _call_tool(name: str, args: dict) -> Any:
    if name == "trademinds_signals":
        limit  = int(args.get("limit", 20))
        ticker = args.get("ticker", "").upper().strip()
        # Try fast-scan results first, fall back to recent signals
        try:
            data = await _get("/api/recent-signals", {"limit": limit})
            if ticker and isinstance(data, list):
                data = [s for s in data if (s.get("ticker") or "").upper() == ticker]
            elif ticker and isinstance(data, dict):
                rows = data.get("signals", data.get("results", []))
                data = [s for s in rows if (s.get("ticker") or "").upper() == ticker]
            return data
        except Exception:
            return await _get("/api/signals/recent", {"limit": limit})

    if name == "trademinds_leaderboard":
        return await _get("/api/arena/leaderboard")

    if name == "trademinds_bridge_vote":
        detail = bool(args.get("detail", False))
        if detail:
            votes     = await _get("/api/bridge/votes")
            consensus = await _get("/api/bridge/consensus")
            return {"consensus": consensus, "votes": votes}
        return await _get("/api/bridge/consensus")

    if name == "trademinds_portfolio":
        return await _get("/api/webull-portfolio")

    if name == "trademinds_health":
        return await _get("/api/operations")

    if name == "trademinds_ask_archer":
        message = str(args.get("message", "")).strip()
        if not message:
            return {"error": "message is required"}
        result = await _post("/api/computer/chat", {
            "message":   message,
            "player_id": "mcp-client",
            "provider":  "auto",
        })
        return result

    if name == "trademinds_create_agent":
        return await _post("/api/agents/create", {
            "name":     args.get("name"),
            "model":    args.get("model"),
            "strategy": args.get("strategy", "Balanced growth with risk management"),
            "cash":     float(args.get("cash", 7000)),
        })

    if name == "trademinds_risk":
        scenario = args.get("scenario", "").strip()
        if scenario:
            return await _post("/api/risk/stress", {"scenario": scenario})
        return await _get("/api/risk/var")

    return {"error": f"Unknown tool: {name}"}

# ── JSON-RPC helpers ───────────────────────────────────────────────────────────
def _ok(req_id: Any, result: Any) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}

def _err(req_id: Any, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}

# ── MCP JSON-RPC endpoint ──────────────────────────────────────────────────────
@app.post("/mcp")
async def mcp_endpoint(request: Request):
    """Main MCP JSON-RPC 2.0 endpoint."""
    _check_auth(request)

    try:
        body = await request.json()
    except Exception:
        return JSONResponse(_err(None, -32700, "Parse error"), status_code=400)

    method  = body.get("method", "")
    params  = body.get("params") or {}
    req_id  = body.get("id")

    logger.info("MCP %s (id=%s)", method, req_id)

    # ── Handshake ──
    if method == "initialize":
        return JSONResponse(_ok(req_id, {
            "protocolVersion": "2024-11-05",
            "serverInfo":      {"name": "trademinds", "version": "3.1.0"},
            "capabilities":    {"tools": {}},
        }))

    if method in ("notifications/initialized", "initialized"):
        return JSONResponse({"jsonrpc": "2.0"})

    # ── Tool list ──
    if method == "tools/list":
        return JSONResponse(_ok(req_id, {"tools": TOOLS}))

    # ── Tool call ──
    if method == "tools/call":
        tool_name = params.get("name", "")
        tool_args = params.get("arguments") or {}
        try:
            result = await _call_tool(tool_name, tool_args)
            return JSONResponse(_ok(req_id, {
                "content": [{"type": "text", "text": json.dumps(result, indent=2, default=str)}],
                "isError": False,
            }))
        except httpx.ConnectError:
            return JSONResponse(_err(req_id, -32000, "TradeMinds dashboard offline (check port 8080)"))
        except httpx.HTTPStatusError as e:
            return JSONResponse(_err(req_id, -32000, f"Trader API {e.response.status_code}: {e.response.text[:200]}"))
        except Exception as e:
            logger.exception("Tool call failed: %s", tool_name)
            return JSONResponse(_err(req_id, -32000, str(e)))

    if method == "ping":
        return JSONResponse(_ok(req_id, {}))

    return JSONResponse(_err(req_id, -32601, f"Method not found: {method}"))


# ── Info endpoint ──────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {
        "server":       "TradeMinds MCP",
        "version":      "3.1.0",
        "mcp_endpoint": "/mcp",
        "port":         MCP_PORT,
        "tools":        [t["name"] for t in TOOLS],
        "auth":         "required (Bearer token)" if MCP_KEY else "open (set TRADEMINDS_MCP_KEY to enable)",
        "dashboard":    TRADER_URL,
    }


@app.get("/health")
async def health():
    try:
        async with httpx.AsyncClient(timeout=3) as c:
            r = await c.get(f"{TRADER_URL}/api/status")
            dashboard_ok = r.status_code == 200
    except Exception:
        dashboard_ok = False
    return {
        "mcp":       "running",
        "dashboard": "online" if dashboard_ok else "offline",
        "tools":     len(TOOLS),
    }


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    logger.info("🚀 TradeMinds MCP Server — port %d", MCP_PORT)
    logger.info("   Tools: %s", ", ".join(t["name"] for t in TOOLS))
    if MCP_KEY:
        logger.info("   Auth: Bearer token required")
    else:
        logger.info("   Auth: OPEN — set TRADEMINDS_MCP_KEY in .env to secure")
    uvicorn.run(app, host="127.0.0.1", port=MCP_PORT, log_level="info")
