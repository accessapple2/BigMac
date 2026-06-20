"""
engine/stream.py — live tick relay for the cockpit (SSE)
========================================================

Reuses Holly's existing feed. The Alpaca IEX WebSocket recorder already writes
every tick to the `price_ticks` table; this just TAILS that table and fans the
ticks out to browsers over Server-Sent Events. One DB, one upstream WS — the
cockpit never opens its own Alpaca connection (IEX free = one connection).

Mount it on the SwingDesk app alongside fire_control:
    from engine.stream import router as stream_router
    app.include_router(stream_router)

Cockpit connects with:
    new EventSource("/api/stream/NVDA")

Adjust the three SCHEMA constants below to match your real price_ticks columns.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

# --- SCHEMA: point these at your real price_ticks table -------------------
DB_PATH    = os.getenv("OT_DB", os.path.join(
    os.getenv("OT_ROOT", "/Users/bigmac/autonomous-trader"), "data", "trader.db"))
TABLE      = os.getenv("OT_TICKS_TABLE", "price_ticks")
SYMBOL_COL = os.getenv("OT_TICKS_SYMBOL_COL", "symbol")
PRICE_COL  = os.getenv("OT_TICKS_PRICE_COL", "price")
TS_COL     = os.getenv("OT_TICKS_TS_COL", "ts")
VOLUME_COL = os.getenv("OT_TICKS_VOLUME_COL", "volume")
ID_COL     = os.getenv("OT_TICKS_ID_COL", "id")   # confirmed PK: id,symbol,price,volume,ts
# --------------------------------------------------------------------------

POLL_SEC = float(os.getenv("OT_STREAM_POLL_SEC", "0.5"))   # tail cadence
BATCH    = 500                                             # max ticks per poll

router = APIRouter()


def _conn():
    # read-only; tolerant if the recorder holds a write lock
    c = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, check_same_thread=False, timeout=2)
    c.row_factory = sqlite3.Row
    return c


def _max_id(conn) -> int:
    try:
        r = conn.execute(f"SELECT MAX({ID_COL}) AS m FROM {TABLE}").fetchone()
        return int(r["m"]) if r and r["m"] is not None else 0
    except Exception:
        return 0


def _ticks_since(conn, sym: str, after_id: int):
    q = (f"SELECT {ID_COL} AS rid, {PRICE_COL} AS price, {TS_COL} AS ts, {VOLUME_COL} AS vol "
         f"FROM {TABLE} WHERE {SYMBOL_COL}=? AND {ID_COL}>? ORDER BY {ID_COL} LIMIT {BATCH}")
    return conn.execute(q, (sym, after_id)).fetchall()


@router.get("/api/stream/{symbol}")
def stream(symbol: str):
    sym = symbol.upper()

    def gen():
        # SSE preamble: client reconnect hint
        yield "retry: 3000\n\n"
        try:
            conn = _conn()
        except Exception as e:
            yield f"event: error\ndata: {json.dumps({'detail': str(e)})}\n\n"
            return
        cursor = _max_id(conn)             # start at the live edge, not history
        idle = 0
        while True:
            try:
                rows = _ticks_since(conn, sym, cursor)
            except Exception as e:
                yield f"event: error\ndata: {json.dumps({'detail': str(e)})}\n\n"
                time.sleep(1.0)
                continue
            if rows:
                idle = 0
                for r in rows:
                    cursor = r["rid"]
                    yield f"data: {json.dumps({'price': r['price'], 'ts': r['ts'], 'v': r['vol']})}\n\n"
            else:
                idle += 1
                if idle % 20 == 0:          # ~every 10s keep proxies from timing out
                    yield ": keepalive\n\n"
            time.sleep(POLL_SEC)

    return StreamingResponse(gen(), media_type="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",          # disable nginx/cloudflared buffering
    })
