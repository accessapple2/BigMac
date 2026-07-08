"""HM-TRADE-DESK-AUTOPILOT-PHASE2 (HM-NEXT-WAVE Phase 3) — 2026-05-23.

Fractional / notional Trade Desk autopilot via attach-after-fill +
OCO daemon. Captain spec: Phase 2 (Bracket) handles whole-share
market/limit BUYs natively via Alpaca's BracketOrderRequest. Phase 3
covers the cases Alpaca rejects from BracketOrderRequest:

  * notional (dollar) sizing
  * fractional qty (e.g. 0.5 sh, 12.34 sh)

For those, we cannot submit a bracket atomically. Instead we:
  1. Submit the parent BUY (already-filled or pending)
  2. Poll for fill (parent fill is required to know actual filled_qty)
  3. Once filled: attach a SELL STOP @ fill*(1-sl/100) + SELL LIMIT @
     fill*(1+tp/100), both GTC, qty=filled_qty (NOT requested_qty —
     partial-fill safe)
  4. Write sl_order_id + tp_order_id back to the trades row
  5. OCO daemon monitors the two children — when one transitions to
     a terminal state (filled, canceled, rejected, expired), cancel
     the sibling

DAEMON LIFECYCLE
================
Per CLAUDE.md HM-EQ lesson: daemon binds at module-level startup, NEVER
lazy. main.py calls start_trade_desk_autopilot_daemon() at boot. The
daemon is a single threading.Thread(daemon=True) with a 30s sleep loop
that polls `autopilot_oco_watch` table for active (parent, sl, tp) sets
and reconciles them against Alpaca order status.

IDEMPOTENCY
===========
attach_children_after_fill checks if sl_order_id is already set on the
trades row before attaching — prevents double-attach if the caller
retries.

FAIL-SAFE
=========
Every external call (Alpaca submit, DB write, NTFY) wrapped. Errors log
`[TDA-PHASE2-WARN]` and continue. A daemon crash logs
`[TDA-PHASE2-CRASH]` + NTFY ollietrades-admin then re-raises (so
launchd / systemd restart visibility is preserved at the process
level — the inner daemon stays inside its loop).

SCHEMA
======
New table `autopilot_oco_watch`:
  parent_order_id TEXT PRIMARY KEY
  sl_order_id     TEXT
  tp_order_id     TEXT
  symbol          TEXT
  agent_id        TEXT
  attached_at     TEXT
  status          TEXT   -- 'active' | 'resolved' | 'failed'
  resolved_at     TEXT
  resolution      TEXT   -- 'sl_filled' | 'tp_filled' | 'parent_canceled' | 'manual'
"""
from __future__ import annotations

import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path

from rich.console import Console

console = Console()

_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "trader.db"

# Daemon configuration
_OCO_POLL_INTERVAL_S = 30  # Captain spec: 30s
_DAEMON_THREAD: threading.Thread | None = None
_DAEMON_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def init_schema() -> None:
    """Create autopilot_oco_watch table idempotently."""
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS autopilot_oco_watch (
                  parent_order_id TEXT PRIMARY KEY,
                  sl_order_id     TEXT,
                  tp_order_id     TEXT,
                  symbol          TEXT,
                  agent_id        TEXT,
                  attached_at     TEXT DEFAULT (datetime('now')),
                  status          TEXT DEFAULT 'active',
                  resolved_at     TEXT,
                  resolution      TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_oco_watch_status "
                "ON autopilot_oco_watch(status)"
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        console.log(
            f"[red][TDA-PHASE2-SCHEMA] init failed: "
            f"{type(e).__name__}: {e!r}"
        )


# ---------------------------------------------------------------------------
# Attach helper — called after parent fill confirmed
# ---------------------------------------------------------------------------


def attach_children_after_fill(
    *,
    parent_order_id: str,
    side: str,                    # 'BUY' for long entries (children are SELL stops/limits)
    symbol: str,
    filled_qty: float,            # NOT requested_qty — partial-fill safe
    fill_price: float,
    sl_pct: float,
    tp_pct: float,
    sl_kind: str = "fixed",       # HM-TRADE-DESK-AUTOPILOT-PHASE3: 'fixed' | 'trailing'
    agent_id: str = "trade-desk",
    trade_id: int | None = None,
) -> dict:
    """Submit SL + TP children after a parent fill. Idempotent — re-runs
    are no-ops when the parent's sl_order_id is already populated on
    the trades row.

    Returns {stop_order_id, target_order_id, stop_price, target_price,
    errors[], skipped: bool}.
    """
    out: dict = {
        "stop_order_id": None, "target_order_id": None,
        "stop_price": None, "target_price": None,
        "errors": [], "skipped": False,
    }
    if not parent_order_id:
        out["errors"].append("parent_order_id required")
        return out
    if filled_qty is None or float(filled_qty) <= 0:
        out["errors"].append(f"invalid filled_qty={filled_qty}")
        return out
    if fill_price is None or float(fill_price) <= 0:
        out["errors"].append(f"invalid fill_price={fill_price}")
        return out

    # IDEMPOTENCY GUARD — has this parent already had children attached?
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            row = conn.execute(
                "SELECT 1 FROM autopilot_oco_watch "
                " WHERE parent_order_id=? LIMIT 1",
                (parent_order_id,),
            ).fetchone()
            if row:
                out["skipped"] = True
                console.log(
                    f"[yellow][TDA-PHASE2] skip duplicate attach for "
                    f"parent {parent_order_id} (already in oco_watch)"
                )
                return out
        finally:
            conn.close()
    except Exception as e:
        console.log(
            f"[yellow][TDA-PHASE2-WARN] idempotency check failed "
            f"(continuing): {type(e).__name__}: {e!r}"
        )

    # Submit the two children via the existing bridge helper.
    try:
        from engine.alpaca_bridge import alpaca
        result = alpaca.submit_protective_orders(
            symbol=symbol, entry_side=side,
            qty=float(filled_qty), fill_price=float(fill_price),
            sl_pct=float(sl_pct or 0), tp_pct=float(tp_pct or 0),
            sl_kind=(sl_kind or "fixed"),
        )
        out["stop_order_id"] = result.get("stop_order_id")
        out["target_order_id"] = result.get("target_order_id")
        out["stop_price"] = result.get("stop_price")
        out["target_price"] = result.get("target_price")
        if result.get("errors"):
            out["errors"].extend(result["errors"])
    except Exception as e:
        out["errors"].append(
            f"submit_protective_orders crash: {type(e).__name__}: {e!r}"
        )
        return out

    # Record into oco_watch so the daemon can monitor.
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            conn.execute(
                "INSERT OR REPLACE INTO autopilot_oco_watch "
                "(parent_order_id, sl_order_id, tp_order_id, symbol, "
                " agent_id, attached_at, status) "
                "VALUES (?,?,?,?,?,datetime('now'),'active')",
                (
                    parent_order_id, out["stop_order_id"],
                    out["target_order_id"], symbol, agent_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        out["errors"].append(
            f"oco_watch persist failed: {type(e).__name__}: {e!r}"
        )

    # Write child IDs back to the trades row.
    if trade_id is not None:
        try:
            conn = sqlite3.connect(str(_DB_PATH), timeout=10)
            try:
                conn.execute(
                    "UPDATE trades SET stop_loss_order_id=?, "
                    " take_profit_order_id=? WHERE id=?",
                    (out["stop_order_id"], out["target_order_id"],
                     int(trade_id)),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            out["errors"].append(
                f"trades back-write failed: {type(e).__name__}: {e!r}"
            )

    return out


# ---------------------------------------------------------------------------
# Cancel cascade — used by the dashboard cancel endpoint
# ---------------------------------------------------------------------------


def cancel_with_children(parent_order_id: str) -> dict:
    """When canceling a parent, cancel its OCO children first.
    Resolves the oco_watch row to 'parent_canceled'.
    """
    from engine.alpaca_bridge import alpaca
    out: dict = {"parent": None, "sl": None, "tp": None, "errors": []}
    if not parent_order_id:
        out["errors"].append("parent_order_id required")
        return out

    # Look up children from oco_watch
    sl_id = tp_id = None
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            row = conn.execute(
                "SELECT sl_order_id, tp_order_id FROM autopilot_oco_watch "
                " WHERE parent_order_id=?",
                (parent_order_id,),
            ).fetchone()
            if row:
                sl_id, tp_id = row[0], row[1]
        finally:
            conn.close()
    except Exception as e:
        out["errors"].append(f"oco_watch lookup: {type(e).__name__}: {e!r}")

    # Cancel children first per Captain spec
    if sl_id:
        try:
            out["sl"] = alpaca.cancel(sl_id)
        except Exception as e:
            out["errors"].append(f"sl cancel: {type(e).__name__}: {e!r}")
    if tp_id:
        try:
            out["tp"] = alpaca.cancel(tp_id)
        except Exception as e:
            out["errors"].append(f"tp cancel: {type(e).__name__}: {e!r}")

    # Cancel the parent
    try:
        out["parent"] = alpaca.cancel(parent_order_id)
    except Exception as e:
        out["errors"].append(f"parent cancel: {type(e).__name__}: {e!r}")

    # Resolve oco_watch row
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            conn.execute(
                "UPDATE autopilot_oco_watch "
                "   SET status='resolved', "
                "       resolution='parent_canceled', "
                "       resolved_at=datetime('now') "
                " WHERE parent_order_id=?",
                (parent_order_id,),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        out["errors"].append(
            f"oco_watch resolve: {type(e).__name__}: {e!r}"
        )

    return out


# ---------------------------------------------------------------------------
# Replace protective orders — HM-TRADE-DESK-AUTOPILOT-PHASE3 2026-05-23
# ---------------------------------------------------------------------------


def replace_protective_orders(
    parent_order_id: str,
    *,
    sl_pct: float,
    tp_pct: float,
    sl_enabled: bool = True,
    tp_enabled: bool = True,
    sl_kind: str = "fixed",
) -> dict:
    """HM-TRADE-DESK-AUTOPILOT-PHASE3 — cancel-and-resubmit cycle for
    an existing OCO pair. Alpaca does not support inline-edit on stop
    or limit orders, so to change a protective price the only path is:

      1. Cancel current SL + TP children
      2. Submit new children with the requested pcts
      3. Update oco_watch row with new child IDs

    There is a brief window between (1) and (2) where the position is
    unprotected. Caller surfaces this in the UI.

    Reconstructs symbol / side / fill_price / filled_qty from the parent
    order on Alpaca — no schema dependency on oco_watch storing those.

    Returns {parent_order_id, sl_order_id, tp_order_id, stop_price,
    target_price, sl_kind, canceled[], errors[]}.
    """
    from engine.alpaca_bridge import alpaca

    out: dict = {
        "parent_order_id": parent_order_id,
        "sl_order_id": None, "tp_order_id": None,
        "stop_price": None, "target_price": None,
        "sl_kind": (sl_kind or "fixed").lower(),
        "canceled": [], "errors": [],
    }
    if not parent_order_id:
        out["errors"].append("parent_order_id required")
        return out
    if not alpaca.client:
        out["errors"].append("alpaca not connected")
        return out

    # Look up existing children + symbol from oco_watch
    existing_sl_id = existing_tp_id = None
    symbol = None
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            row = conn.execute(
                "SELECT sl_order_id, tp_order_id, symbol "
                "  FROM autopilot_oco_watch WHERE parent_order_id=?",
                (parent_order_id,),
            ).fetchone()
            if not row:
                out["errors"].append(
                    f"no oco_watch row for parent {parent_order_id}"
                )
                return out
            existing_sl_id, existing_tp_id, symbol = row[0], row[1], row[2]
        finally:
            conn.close()
    except Exception as e:
        out["errors"].append(
            f"oco_watch lookup: {type(e).__name__}: {e!r}"
        )
        return out

    # Fetch parent order details from Alpaca to get side + fill price + qty.
    try:
        parent = alpaca.client.get_order_by_id(parent_order_id)
    except Exception as e:
        out["errors"].append(
            f"alpaca get_order: {type(e).__name__}: {e!r}"
        )
        return out

    try:
        side = (parent.side.value if hasattr(parent.side, "value")
                else str(parent.side)).upper()
        filled_qty = float(parent.filled_qty or 0)
        fill_price = float(parent.filled_avg_price or 0)
    except Exception as e:
        out["errors"].append(
            f"parent order parse: {type(e).__name__}: {e!r}"
        )
        return out

    if filled_qty <= 0 or fill_price <= 0:
        out["errors"].append(
            f"parent not filled (qty={filled_qty}, fill=${fill_price})"
        )
        return out

    # Cancel existing children first.
    for child_id, leg in ((existing_sl_id, "sl"), (existing_tp_id, "tp")):
        if not child_id:
            continue
        try:
            alpaca.cancel(child_id)
            out["canceled"].append({"leg": leg, "order_id": child_id})
        except Exception as e:
            out["errors"].append(
                f"{leg} cancel: {type(e).__name__}: {e!r}"
            )

    # Submit new children with the requested pcts (0 disables leg).
    effective_sl_pct = float(sl_pct or 0) if sl_enabled else 0.0
    effective_tp_pct = float(tp_pct or 0) if tp_enabled else 0.0
    try:
        result = alpaca.submit_protective_orders(
            symbol=symbol, entry_side=side,
            qty=filled_qty, fill_price=fill_price,
            sl_pct=effective_sl_pct, tp_pct=effective_tp_pct,
            sl_kind=out["sl_kind"],
        )
        out["sl_order_id"] = result.get("stop_order_id")
        out["tp_order_id"] = result.get("target_order_id")
        out["stop_price"] = result.get("stop_price")
        out["target_price"] = result.get("target_price")
        if result.get("errors"):
            out["errors"].extend(result["errors"])
    except Exception as e:
        out["errors"].append(
            f"resubmit crash: {type(e).__name__}: {e!r}"
        )
        return out

    # Update oco_watch row with new child IDs and reset status to active
    # (covers the case where the row was previously resolved).
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            conn.execute(
                "UPDATE autopilot_oco_watch "
                "   SET sl_order_id=?, tp_order_id=?, "
                "       status='active', resolved_at=NULL, "
                "       resolution=NULL, "
                "       attached_at=datetime('now') "
                " WHERE parent_order_id=?",
                (
                    out["sl_order_id"], out["tp_order_id"],
                    parent_order_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        out["errors"].append(
            f"oco_watch update: {type(e).__name__}: {e!r}"
        )

    return out


def list_active_oco_pairs() -> list[dict]:
    """HM-TRADE-DESK-AUTOPILOT-PHASE3 — return active OCO pairs for the
    UI's protective-orders panel. Each dict: {parent_order_id, symbol,
    sl_order_id, tp_order_id, agent_id, attached_at}.
    """
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT parent_order_id, sl_order_id, tp_order_id, "
                "       symbol, agent_id, attached_at "
                "  FROM autopilot_oco_watch "
                " WHERE status='active' "
                " ORDER BY attached_at DESC"
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
    except Exception:
        return []


# ---------------------------------------------------------------------------
# OCO daemon
# ---------------------------------------------------------------------------


def _ntfy_admin(title: str, message: str, priority: str = "default") -> None:
    """Best-effort NTFY to ollietrades-admin. Never raises."""
    try:
        from engine.alert_channels import _send_ntfy
        _send_ntfy(
            title=title, message=message,
            priority=priority,
            tags="ollietrades,trade-desk,autopilot",
            topic="ollietrades-admin",
        )
    except Exception:
        # Fallback to direct requests if alert_channels unavailable.
        try:
            import requests
            requests.post(
                "https://ntfy.sh/ollietrades-admin",
                data=message.encode("utf-8"),
                headers={
                    "Title": title, "Priority": priority,
                    "Tags": "ollietrades,trade-desk,autopilot",
                },
                timeout=8,
            )
        except Exception as e:
            console.log(f"[yellow]trade_desk_autopilot: ntfy fallback POST also failed (title={title!r}): {e}")


def _is_terminal_status(status: str | None) -> bool:
    """Alpaca terminal statuses for an order."""
    if not status:
        return False
    s = status.lower()
    return s in (
        "filled", "canceled", "cancelled", "rejected", "expired", "done_for_day"
    )


def _resolve_oco_pair(parent_id: str, sl_id: str | None,
                     tp_id: str | None) -> None:
    """Check status of SL + TP. If one is terminal-filled, cancel the
    sibling. NTFY admin on OCO trigger. Idempotent — once resolved,
    leaves the row alone.
    """
    from engine.alpaca_bridge import alpaca
    if not (sl_id or tp_id):
        return
    sl_status = tp_status = None
    if sl_id:
        try:
            o = alpaca.client.get_order_by_id(sl_id) if alpaca.client else None
            sl_status = (o.status.value if hasattr(o.status, "value")
                         else str(o.status)) if o else None
        except Exception:
            sl_status = None
    if tp_id:
        try:
            o = alpaca.client.get_order_by_id(tp_id) if alpaca.client else None
            tp_status = (o.status.value if hasattr(o.status, "value")
                         else str(o.status)) if o else None
        except Exception:
            tp_status = None

    sl_terminal = _is_terminal_status(sl_status)
    tp_terminal = _is_terminal_status(tp_status)

    if not (sl_terminal or tp_terminal):
        return  # both still active — nothing to do this tick

    # Determine resolution + cancel sibling
    resolution = None
    sibling_id = None
    sibling_label = None
    if sl_terminal and sl_status == "filled" and tp_id and not tp_terminal:
        resolution = "sl_filled"
        sibling_id = tp_id
        sibling_label = "tp"
    elif tp_terminal and tp_status == "filled" and sl_id and not sl_terminal:
        resolution = "tp_filled"
        sibling_id = sl_id
        sibling_label = "sl"
    elif sl_terminal and tp_terminal:
        # Both terminal already — no sibling to cancel, just mark resolved
        if sl_status == "filled":
            resolution = "sl_filled"
        elif tp_status == "filled":
            resolution = "tp_filled"
        else:
            resolution = f"both_terminal_no_fill ({sl_status}/{tp_status})"
    else:
        # One terminal but NOT filled (canceled / rejected). Don't kill
        # the live sibling — let the surviving leg work alone.
        resolution = None

    if resolution and sibling_id:
        try:
            cancel_result = alpaca.cancel(sibling_id)
            console.log(
                f"[cyan][TDA-PHASE2-OCO] parent={parent_id[:8]} "
                f"{resolution} — cancelled sibling {sibling_label}="
                f"{sibling_id[:8]}: {cancel_result.get('status')}"
            )
            _ntfy_admin(
                title="🛡 Autopilot OCO triggered",
                message=(
                    f"parent={parent_id[:8]} {resolution} — "
                    f"sibling {sibling_label}={sibling_id[:8]} cancelled"
                ),
            )
        except Exception as e:
            console.log(
                f"[red][TDA-PHASE2-OCO] sibling cancel failed: "
                f"{type(e).__name__}: {e!r}"
            )

    # Mark row resolved
    if resolution:
        try:
            conn = sqlite3.connect(str(_DB_PATH), timeout=10)
            try:
                conn.execute(
                    "UPDATE autopilot_oco_watch "
                    "   SET status='resolved', resolution=?, "
                    "       resolved_at=datetime('now') "
                    " WHERE parent_order_id=?",
                    (resolution, parent_id),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            console.log(
                f"[red][TDA-PHASE2-OCO] resolve write failed: "
                f"{type(e).__name__}: {e!r}"
            )


def _daemon_loop() -> None:
    """Outer poll loop for the OCO daemon. 30s cadence."""
    console.log(
        f"[green][TDA-PHASE2-DAEMON] started — poll interval "
        f"{_OCO_POLL_INTERVAL_S}s"
    )
    while True:
        try:
            init_schema()  # idempotent — handles cold start
            active = []
            try:
                conn = sqlite3.connect(str(_DB_PATH), timeout=10)
                try:
                    rows = conn.execute(
                        "SELECT parent_order_id, sl_order_id, tp_order_id "
                        "  FROM autopilot_oco_watch WHERE status='active'"
                    ).fetchall()
                    active = [(r[0], r[1], r[2]) for r in rows]
                finally:
                    conn.close()
            except Exception as e:
                console.log(
                    f"[red][TDA-PHASE2-DAEMON] active-load failed: "
                    f"{type(e).__name__}: {e!r}"
                )
            for parent_id, sl_id, tp_id in active:
                try:
                    _resolve_oco_pair(parent_id, sl_id, tp_id)
                except Exception as e:
                    console.log(
                        f"[red][TDA-PHASE2-DAEMON] resolve crash for "
                        f"parent={parent_id}: {type(e).__name__}: {e!r}"
                    )
        except Exception as outer:
            # Outer crash — NTFY admin + log, then sleep and continue
            console.log(
                f"[red][TDA-PHASE2-CRASH] daemon outer loop crash: "
                f"{type(outer).__name__}: {outer!r}"
            )
            _ntfy_admin(
                title="⚠ Autopilot OCO daemon crash",
                message=(
                    f"OCO daemon outer loop hit "
                    f"{type(outer).__name__}: {outer!r} — daemon will "
                    f"sleep {_OCO_POLL_INTERVAL_S}s and continue."
                ),
                priority="high",
            )
        time.sleep(_OCO_POLL_INTERVAL_S)


def start_trade_desk_autopilot_daemon() -> None:
    """Module-level daemon spawn — called from main.py at boot per
    CLAUDE.md HM-EQ daemon lifecycle rule (never lazy)."""
    global _DAEMON_THREAD
    with _DAEMON_LOCK:
        if _DAEMON_THREAD is not None and _DAEMON_THREAD.is_alive():
            console.log(
                "[yellow][TDA-PHASE2-DAEMON] start_*_daemon called but "
                "already alive — skipping duplicate spawn"
            )
            return
        init_schema()
        _DAEMON_THREAD = threading.Thread(
            target=_daemon_loop, daemon=True,
            name="trade_desk_autopilot_oco",
        )
        _DAEMON_THREAD.start()
        console.log(
            "[green][TDA-PHASE2-DAEMON] spawned — "
            f"thread={_DAEMON_THREAD.name}"
        )


def daemon_status() -> dict:
    """Diagnostic snapshot of daemon + watch counts."""
    out = {
        "alive": _DAEMON_THREAD.is_alive() if _DAEMON_THREAD else False,
        "thread_name": (_DAEMON_THREAD.name if _DAEMON_THREAD else None),
        "poll_interval_s": _OCO_POLL_INTERVAL_S,
        "active_watches": 0, "resolved_watches": 0,
    }
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            for status_val, key in (("active", "active_watches"),
                                    ("resolved", "resolved_watches")):
                row = conn.execute(
                    "SELECT COUNT(*) FROM autopilot_oco_watch WHERE status=?",
                    (status_val,),
                ).fetchone()
                out[key] = int(row[0]) if row else 0
        finally:
            conn.close()
    except Exception:
        pass
    return out


if __name__ == "__main__":
    init_schema()
    import json as _json
    print(_json.dumps(daemon_status(), indent=2))
