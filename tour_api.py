"""TOUR-API — read-only state/ticks + Alpaca-PAPER order passthrough for the Bridge Tour page.

Standalone service on :8088. DOES NOT import or restart the :8080 dashboard.

RAILS (enforced here):
  * All market/book reads use SQLite opened ``mode=ro`` (read-only handle; the OS
    refuses writes on it). No DROP/DELETE/TRUNCATE/UPDATE/INSERT in any read path.
  * NOTHING in this module reaches a Schwab/IBKR/real-account module or creds.
    The ONLY broker entrypoint is Alpaca PAPER (engine.alpaca_bridge.AlpacaBridge,
    constructed paper=True), imported LAZILY inside the POST handler so a bare
    ``import tour_api`` loads zero engine code and zero broker creds.
  * Alpaca PAPER only. POST asserts route=="alpaca_paper" AND the resolved client
    is the paper client (paper base_url + sandbox flag) before any submit.

Diagnostics that shaped this file (TOUR-API STEP 1, verified 2026-06-12):
  * Paper book store .......... data/trader.db -> positions (OPEN) + trades (log)
  * price_ticks ............... data/trader.db (NOT swingdesk.db — directive was stale)
                                cols: id, symbol, price, volume, ts(TEXT iso-Z)
  * Candidate sources ......... data/trader.db: squeeze_candidates, rs_rank,
                                minervini_trend, external_picks, ghost_options_watch
  * Mood ...................... data/trader.db: regime_history (latest .regime)
  * Alpaca PAPER submit ....... engine.alpaca_bridge.AlpacaBridge.buy(symbol, qty, ...)
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

# --------------------------------------------------------------------------- #
# Paths (read-only)                                                           #
# --------------------------------------------------------------------------- #
_HOME = os.path.expanduser("~/autonomous-trader")
DB_PATH = os.path.join(_HOME, "data", "trader.db")

# Symbols surfaced on the Tour "watch" strip (active watchlist, capped).
_WATCH_LIMIT = 8
_POSITIONS_LIMIT = 40
_CANDIDATE_LIMIT_PER_SOURCE = 6
_CALLLOG_LIMIT = 20
_TICKS_LIMIT = 60

app = FastAPI(title="TOUR-API", docs_url=None, openapi_url=None, redoc_url=None)


# --------------------------------------------------------------------------- #
# CORS — Access-Control-Allow-Origin: * on EVERY response (incl. errors)      #
# --------------------------------------------------------------------------- #
@app.middleware("http")
async def _cors_everywhere(request: Request, call_next):
    if request.method == "OPTIONS":
        resp: Response = Response(status_code=204)
    else:
        resp = await call_next(request)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "content-type"
    return resp


# --------------------------------------------------------------------------- #
# Read-only DB helpers                                                        #
# --------------------------------------------------------------------------- #
def _ro_conn() -> sqlite3.Connection:
    """Open trader.db in read-only mode. The OS rejects any write on this handle."""
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, check_same_thread=False, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def _q(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    try:
        return conn.execute(sql, params).fetchall()
    except sqlite3.Error:
        return []


def _q1(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> sqlite3.Row | None:
    rows = _q(conn, sql, params)
    return rows[0] if rows else None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _last_px(conn: sqlite3.Connection, sym: str) -> float | None:
    r = _q1(conn, "SELECT price FROM price_ticks WHERE symbol=? ORDER BY ts DESC LIMIT 1", (sym,))
    return round(float(r["price"]), 4) if r and r["price"] is not None else None


def _session_base_px(conn: sqlite3.Connection, sym: str) -> float | None:
    """Session 'base' (open) = first tick on the symbol's most-recent tick date.

    Anchoring on the latest tick's own calendar date (not UTC-now midnight) is
    timezone-robust: after UTC rollover the US session's ticks still share one
    date, so we don't miss the open. ts is ISO with a 'T' at offset 10.
    """
    r = _q1(
        conn,
        """SELECT price FROM price_ticks
            WHERE symbol=?
              AND substr(ts,1,10) = (SELECT substr(MAX(ts),1,10) FROM price_ticks WHERE symbol=?)
            ORDER BY ts ASC LIMIT 1""",
        (sym, sym),
    )
    return round(float(r["price"]), 4) if r and r["price"] is not None else None


# --------------------------------------------------------------------------- #
# State assembly                                                              #
# --------------------------------------------------------------------------- #
def _build_mood(conn: sqlite3.Connection) -> str:
    r = _q1(conn, "SELECT regime FROM regime_history ORDER BY date DESC LIMIT 1")
    return r["regime"] if r and r["regime"] else "UNKNOWN"


def _build_main(conn: sqlite3.Connection) -> dict[str, Any]:
    """Main chart symbol = the most-streamed name on the latest tick date.

    Picking the highest tick-count symbol (QQQ in practice) guarantees the Tour
    chart ticks continuously, and self-heals if the live stream set changes.
    """
    r = _q1(
        conn,
        """SELECT symbol FROM price_ticks
            WHERE substr(ts,1,10) = (SELECT substr(MAX(ts),1,10) FROM price_ticks)
            GROUP BY symbol ORDER BY COUNT(*) DESC LIMIT 1""",
    )
    sym = r["symbol"] if r else "QQQ"
    return {"sym": sym, "px": _last_px(conn, sym), "base": _session_base_px(conn, sym)}


def _build_watch(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = _q(
        conn,
        "SELECT symbol FROM watchlist WHERE is_active=1 ORDER BY added_at ASC LIMIT ?",
        (_WATCH_LIMIT,),
    )
    out = []
    for r in rows:
        sym = r["symbol"]
        out.append({"sym": sym, "px": _last_px(conn, sym), "base": _session_base_px(conn, sym)})
    return out


def _build_positions(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Paper book OPEN positions (data/trader.db::positions, stock legs)."""
    # Aggregate across fleet players so the book shows one row per symbol
    # (qty summed, entry = qty-weighted average cost).
    rows = _q(
        conn,
        """SELECT symbol,
                  SUM(qty) AS qty,
                  SUM(qty * COALESCE(avg_price,0)) / NULLIF(SUM(qty),0) AS avg_price
             FROM positions
            WHERE asset_type='stock' AND qty IS NOT NULL AND qty != 0
            GROUP BY symbol
            HAVING SUM(qty) != 0
            ORDER BY ABS(SUM(qty * COALESCE(avg_price,0))) DESC
            LIMIT ?""",
        (_POSITIONS_LIMIT,),
    )
    out = []
    for r in rows:
        sym = r["symbol"]
        out.append(
            {
                "sym": sym,
                "qty": round(float(r["qty"]), 4),
                "entry": round(float(r["avg_price"]), 4) if r["avg_price"] is not None else None,
                "last": _last_px(conn, sym),
            }
        )
    return out


def _votes(yes: int, no: int, wait: int) -> dict[str, int]:
    return {"yes": int(yes), "no": int(no), "wait": int(wait)}


def _build_candidates(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Squeeze / RS Rank / Minervini / external-intel / ghost-watch convergence.

    candidate_id encodes <source>:<key> so POST /api/paper/order can resolve it
    read-only back to a symbol before submitting an Alpaca PAPER buy.
    """
    cands: list[dict[str, Any]] = []

    # --- squeeze_candidates (active, highest composite first) ---------------
    for r in _q(
        conn,
        """SELECT symbol, composite_score, threshold_tier, price_at_scan, short_pct, days_to_cover
             FROM squeeze_candidates
            WHERE dismissed=0
            ORDER BY composite_score DESC LIMIT ?""",
        (_CANDIDATE_LIMIT_PER_SOURCE,),
    ):
        score = float(r["composite_score"] or 0)
        ready = score >= 40 or (r["threshold_tier"] or "").upper() == "EXECUTE"
        cands.append(
            {
                "id": f"squeeze:{r['symbol']}",
                "sym": r["symbol"],
                "kind": "squeeze",
                "status": "execute_ready" if ready else "on_watch",
                "legs": [{"txt": f"BUY {r['symbol']} (short-squeeze long)"}],
                "net_debit": None, "max_gain": None, "max_loss": None, "breakeven": None,
                "trigger": r["price_at_scan"], "stop": None, "target": None,
                "signal": f"squeeze composite {score:.0f} · short {r['short_pct']}% · DTC {r['days_to_cover']}",
                "votes": _votes(min(5, round(score / 15)), 0, max(0, 3 - round(score / 15))),
            }
        )

    # --- rs_rank (relative-strength leaders) --------------------------------
    for r in _q(
        conn,
        "SELECT symbol, rs_rank, rs_return_pct, rs_vs_spy_pct FROM rs_rank ORDER BY rs_rank DESC LIMIT ?",
        (_CANDIDATE_LIMIT_PER_SOURCE,),
    ):
        rank = int(r["rs_rank"] or 0)
        cands.append(
            {
                "id": f"rs:{r['symbol']}",
                "sym": r["symbol"],
                "kind": "rs_rank",
                "status": "execute_ready" if rank >= 90 else "on_watch",
                "legs": [{"txt": f"BUY {r['symbol']} (RS-leader long)"}],
                "net_debit": None, "max_gain": None, "max_loss": None, "breakeven": None,
                "trigger": None, "stop": None, "target": None,
                "signal": f"RS rank {rank} · 12wk {r['rs_return_pct']:.1f}% · vs SPY {r['rs_vs_spy_pct']:+.1f}pp",
                "votes": _votes(min(5, round(rank / 20)), 0, max(0, 5 - round(rank / 20))),
            }
        )

    # --- minervini_trend (trend-template pass) ------------------------------
    for r in _q(
        conn,
        """SELECT symbol, template_score, template_pass, rs_pass, price_at_scan
             FROM minervini_trend
            WHERE template_pass=1
            ORDER BY template_score DESC LIMIT ?""",
        (_CANDIDATE_LIMIT_PER_SOURCE,),
    ):
        cands.append(
            {
                "id": f"minervini:{r['symbol']}",
                "sym": r["symbol"],
                "kind": "minervini",
                "status": "execute_ready" if (r["template_pass"] and r["rs_pass"]) else "on_watch",
                "legs": [{"txt": f"BUY {r['symbol']} (Minervini trend long)"}],
                "net_debit": None, "max_gain": None, "max_loss": None, "breakeven": None,
                "trigger": r["price_at_scan"], "stop": None, "target": None,
                "signal": f"Minervini {r['template_score']}/8 conds · RS {'pass' if r['rs_pass'] else 'fail'}",
                "votes": _votes(int(r["template_score"] or 0), 8 - int(r["template_score"] or 0), 0),
            }
        )

    # --- external_picks (external-intel) ------------------------------------
    for r in _q(
        conn,
        """SELECT id, ticker, source, action, entry, stop, note
             FROM external_picks
            ORDER BY pick_date DESC, id DESC LIMIT ?""",
        (_CANDIDATE_LIMIT_PER_SOURCE,),
    ):
        act = (r["action"] or "BUY").upper()
        cands.append(
            {
                "id": f"intel:{r['id']}",
                "sym": r["ticker"],
                "kind": "external_intel",
                "status": "on_watch",
                "legs": [{"txt": f"{act} {r['ticker']} ({r['source']})"}],
                "net_debit": None, "max_gain": None, "max_loss": None,
                "breakeven": None, "trigger": r["entry"], "stop": r["stop"], "target": None,
                "signal": f"intel:{r['source']} — {(r['note'] or '')[:80]}",
                "votes": _votes(1, 0, 1),
            }
        )

    # --- ghost_options_watch (open ghost option positions) ------------------
    for r in _q(
        conn,
        """SELECT id, symbol, option_type, strike, expiry, contracts, entry_price,
                  tp_price, sl_price, source, rationale
             FROM ghost_options_watch
            WHERE outcome='open'
            ORDER BY ts DESC LIMIT ?""",
        (_CANDIDATE_LIMIT_PER_SOURCE,),
    ):
        right = "C" if (r["option_type"] or "").upper() == "CALL" else "P"
        net_debit = None
        if r["entry_price"] is not None and r["contracts"]:
            net_debit = round(float(r["entry_price"]) * 100 * int(r["contracts"]), 2)
        cands.append(
            {
                "id": f"ghost:{r['id']}",
                "sym": r["symbol"],
                "kind": "ghost_option",
                "status": "on_watch",
                "legs": [
                    {
                        "side": "BUY",
                        "qty": int(r["contracts"] or 1),
                        "strike": r["strike"],
                        "right": right,
                        "exp": r["expiry"],
                    }
                ],
                "net_debit": net_debit, "max_gain": None,
                "max_loss": net_debit, "breakeven": None,
                "trigger": None, "stop": r["sl_price"], "target": r["tp_price"],
                "signal": f"ghost-watch:{r['source']} — {(r['rationale'] or '')[:80]}",
                "votes": _votes(1, 0, 1),
            }
        )

    return cands


def _build_calllog(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = _q(
        conn,
        """SELECT symbol, action, price, entry_price, exit_price, realized_pnl, executed_at
             FROM trades
            ORDER BY executed_at DESC LIMIT ?""",
        (_CALLLOG_LIMIT,),
    )
    out = []
    for r in rows:
        realized = r["realized_pnl"]
        closed = realized is not None
        outcome = None
        if closed:
            outcome = "win" if float(realized) > 0 else ("loss" if float(realized) < 0 else "scratch")
        # OPEN rows get a current mark for unrealized P/L (equity = last tick;
        # option/unstreamed = None). Closed rows keep realized untouched.
        mark = None if closed else _last_px(conn, r["symbol"])
        out.append(
            {
                "sym": r["symbol"],
                "type": r["action"],
                "status": "closed" if closed else "open",
                "entry": r["entry_price"] if r["entry_price"] is not None else r["price"],
                "exit": r["exit_price"],
                "mark": mark,
                "realized": round(float(realized), 2) if realized is not None else None,
                "outcome": outcome,
                "opened": r["executed_at"],
                "closed": r["executed_at"] if closed else None,
            }
        )
    return out


# --------------------------------------------------------------------------- #
# Routes                                                                      #
# --------------------------------------------------------------------------- #
@app.get("/api/tour/state")
def tour_state():
    conn = _ro_conn()
    try:
        return JSONResponse(
            {
                "asof": _utc_now_iso(),
                "mood": _build_mood(conn),
                "main": _build_main(conn),
                "watch": _build_watch(conn),
                "positions": _build_positions(conn),
                "candidates": _build_candidates(conn),
                "calllog": _build_calllog(conn),
            }
        )
    finally:
        conn.close()


@app.get("/api/tour/ticks")
def tour_ticks(sym: str):
    sym = (sym or "").upper().strip()
    conn = _ro_conn()
    try:
        rows = _q(
            conn,
            "SELECT price, volume, ts FROM price_ticks WHERE symbol=? ORDER BY ts DESC LIMIT ?",
            (sym, _TICKS_LIMIT),
        )
    finally:
        conn.close()
    ticks = [{"price": r["price"], "volume": r["volume"], "ts": r["ts"]} for r in reversed(rows)]
    return JSONResponse({"sym": sym, "ticks": ticks})


def _resolve_candidate(conn: sqlite3.Connection, candidate_id: str) -> dict[str, Any]:
    """Read-only resolve candidate_id -> {sym, equity}. equity=False for option legs."""
    if ":" not in candidate_id:
        return {"error": "bad candidate_id format (expected <source>:<key>)"}
    source, key = candidate_id.split(":", 1)
    source = source.lower()
    if source == "squeeze":
        r = _q1(conn, "SELECT symbol FROM squeeze_candidates WHERE symbol=? AND dismissed=0 LIMIT 1", (key,))
        return {"sym": r["symbol"], "equity": True} if r else {"error": "squeeze candidate not found"}
    if source == "rs":
        r = _q1(conn, "SELECT symbol FROM rs_rank WHERE symbol=? LIMIT 1", (key,))
        return {"sym": r["symbol"], "equity": True} if r else {"error": "rs candidate not found"}
    if source == "minervini":
        r = _q1(conn, "SELECT symbol FROM minervini_trend WHERE symbol=? LIMIT 1", (key,))
        return {"sym": r["symbol"], "equity": True} if r else {"error": "minervini candidate not found"}
    if source == "intel":
        r = _q1(conn, "SELECT ticker FROM external_picks WHERE id=? LIMIT 1", (key,))
        return {"sym": r["ticker"], "equity": True} if r else {"error": "intel candidate not found"}
    if source == "ghost":
        r = _q1(conn, "SELECT symbol FROM ghost_options_watch WHERE id=? LIMIT 1", (key,))
        if not r:
            return {"error": "ghost candidate not found"}
        # Option leg — the confirmed Alpaca PAPER entrypoint here is the equity
        # buy() path only; refuse options submit from the tour passthrough.
        return {"sym": r["symbol"], "equity": False}
    return {"error": f"unknown candidate source '{source}'"}


@app.post("/api/paper/order")
async def paper_order(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "reason": "invalid JSON body"}, status_code=400)

    candidate_id = str(body.get("candidate_id", ""))
    route = str(body.get("route", ""))

    # HARD ASSERT: Alpaca PAPER route only.
    if route != "alpaca_paper":
        return JSONResponse({"ok": False, "reason": "route must be 'alpaca_paper'"}, status_code=403)

    # Self-verify / harmless ping — never submits.
    if candidate_id in ("", "__noop__"):
        return JSONResponse({"ok": False, "reason": "noop — no candidate resolved (route assertion passed)"})

    conn = _ro_conn()
    try:
        resolved = _resolve_candidate(conn, candidate_id)
    finally:
        conn.close()

    if "error" in resolved:
        return JSONResponse({"ok": False, "reason": resolved["error"]}, status_code=404)
    if not resolved.get("equity"):
        return JSONResponse(
            {"ok": False, "reason": "options paper submit not supported via tour (equity buy() only)"},
            status_code=400,
        )

    sym = resolved["sym"]

    # LAZY import — keeps `import tour_api` free of broker creds/engine code,
    # so the schwab-isolation self-check passes by construction.
    try:
        from engine.alpaca_bridge import AlpacaBridge
    except Exception as e:
        return JSONResponse({"ok": False, "reason": f"alpaca bridge unavailable: {type(e).__name__}"}, status_code=503)

    bridge = AlpacaBridge()
    if not getattr(bridge, "client", None):
        return JSONResponse({"ok": False, "reason": "alpaca paper client not connected"}, status_code=503)

    # HARD ASSERT: the resolved client is the Alpaca PAPER client.
    base_url = str(getattr(bridge.client, "_base_url", "")).lower()
    is_sandbox = bool(getattr(bridge.client, "_sandbox", False))
    if "paper" not in base_url or not is_sandbox:
        return JSONResponse(
            {"ok": False, "reason": "refused: broker client is not the Alpaca PAPER client"},
            status_code=403,
        )

    # Submit a small notional PAPER buy via the confirmed entrypoint.
    try:
        result = bridge.buy(sym, 0, agent_id="tour-api", notional=100.0)
    except Exception as e:
        return JSONResponse({"ok": False, "reason": f"submit error: {type(e).__name__}: {e}"}, status_code=502)

    if isinstance(result, dict) and result.get("error"):
        return JSONResponse({"ok": False, "reason": str(result["error"])})

    order_id = None
    if isinstance(result, dict):
        order_id = result.get("order_id") or result.get("id") or result.get("alpaca_order_id")
    return JSONResponse({"ok": True, "order_id": order_id, "sym": sym})


@app.get("/api/tour/health")
def health():
    return {"ok": True, "service": "tour-api", "asof": _utc_now_iso()}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8088, log_level="info")
