"""
engine/events.py — SSE relay of event_tape for the cockpit reticle flash
=========================================================================

event_tape is written by engine/event_tape.py (Holly-style realtime events:
running_up_fast, volume_burst, new_session_high, gap_fill_complete). This tails
new rows for a symbol from the live edge and pushes them to the cockpit, which
pulses the matching station's lock bracket.

GET /api/events/{symbol}  → SSE: data: {"type":"volume_burst","ts":"..."}

Columns are auto-detected from event_tape at import (id / symbol / type / ts),
so it adapts to the exact names without hand-config.

Mount:  from engine.events import router as events_router
        app.include_router(events_router)
"""
from __future__ import annotations

import json
import os
import sqlite3
import time

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

DB_PATH = os.getenv("OT_DB", os.path.join(
    os.getenv("OT_ROOT", "/Users/bigmac/autonomous-trader"), "data", "trader.db"))
TABLE = os.getenv("OT_EVENT_TABLE", "event_tape")


def _ro():
    return sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, check_same_thread=False, timeout=5)


def _detect_cols():
    """Pick id/symbol/type/ts columns from event_tape's real schema."""
    try:
        c = _ro()
        names = [r[1] for r in c.execute(f"PRAGMA table_info({TABLE})").fetchall()]
        c.close()
    except Exception:
        return ("id", "symbol", "event_type", "detected_at")

    def pick(cands, default):
        low = {n.lower(): n for n in names}
        for cand in cands:                       # exact match first
            if cand in low:
                return low[cand]
        for cand in cands:                       # then substring
            for n in names:
                if cand in n.lower():
                    return n
        return default

    idc  = "id" if "id" in names else "rowid"
    symc = pick(["symbol", "ticker", "sym"], "symbol")
    typc = pick(["event_type", "type", "event", "kind"], "event_type")
    tsc  = pick(["detected_at", "ts", "created_at", "time"], "detected_at")
    return (idc, symc, typc, tsc)


ID_COL, SYM_COL, TYPE_COL, TS_COL = _detect_cols()

router = APIRouter()


def _max_id(conn):
    try:
        r = conn.execute(f"SELECT MAX({ID_COL}) FROM {TABLE}").fetchone()
        return r[0] or 0
    except Exception:
        return 0


def _since(conn, sym, cursor):
    q = (f"SELECT {ID_COL} AS rid, {TYPE_COL} AS etype, {TS_COL} AS ts "
         f"FROM {TABLE} WHERE {ID_COL} > ? AND {SYM_COL} = ? ORDER BY {ID_COL} ASC")
    return conn.execute(q, (cursor, sym)).fetchall()


@router.get("/api/events/{symbol}")
def events(symbol: str):
    sym = symbol.upper()

    def gen():
        try:
            conn = _ro()
            conn.row_factory = sqlite3.Row
        except Exception as e:
            yield f"event: error\ndata: {json.dumps({'detail': str(e)})}\n\n"
            return
        cursor = _max_id(conn)          # live edge — only new events, not history
        idle = 0
        while True:
            try:
                rows = _since(conn, sym, cursor)
            except Exception as e:
                yield f"event: error\ndata: {json.dumps({'detail': str(e)})}\n\n"
                return
            if rows:
                idle = 0
                for r in rows:
                    cursor = r["rid"]
                    yield f"data: {json.dumps({'type': r['etype'], 'ts': r['ts']})}\n\n"
            else:
                idle += 1
                if idle % 20 == 0:      # keepalive ~every 10s
                    yield ": keepalive\n\n"
            time.sleep(0.5)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
