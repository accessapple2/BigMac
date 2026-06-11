"""
SWINGDESK-W4 — spread exit manager.

Monitors OPEN, FILLED multi-leg positions in options_trades each cycle and
evaluates four exit conditions for debit verticals. Behaviour by origin:
  * strategy='swingdesk_manual'  → ALERT ONLY (NTFY ollietrades-admin). A manual
    position is NEVER auto-closed.
  * strategy LIKE 'auto_spread_%' → auto-close path is built but INERT until the
    W3 master gate (config.AUTO_SPREADS_ENABLED) is ON — same gating as W3.

Marks come from live Alpaca option mids; if a quote is None (e.g. no data) the
position is skipped for that cycle (logged) — never a false alert. Closing routes
through the W2 chokepoint (spread_executor.submit_spread, action='close') so it
inherits the W2.1 idempotency guard; the close client_order_id is distinct from
the open key. Evaluations persist to spread_exit_log (additive; options_trades is
not mutated). The cycle is bound at module level in main.py (lifecycle doctrine).
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import config

_REPO = Path(__file__).resolve().parent.parent
_DB = _REPO / "data" / "trader.db"
_W2_URL = os.getenv("SWINGDESK_W2_URL", "http://127.0.0.1:8889/api/swingdesk/spread/submit")
sys.path.insert(0, str(_REPO / "swingdesk"))   # spread_executor (marks/status/close)

# regime → which direction is "with trend" (same vocabulary as W3 auto_spread)
_BULL_REGIMES = frozenset({"BULL_CROSS", "CAUTIOUS_BULL"})
_BEAR_REGIMES = frozenset({"BEAR_CROSS", "CAUTIOUS_BEAR"})

# exit thresholds (debit verticals)
_PROFIT_TAKE_FRAC = 0.50   # take at 50% of max remaining profit
_DTE_CLOSE = 21            # time exit at/under 21 DTE
_MAX_LOSS_FRAC = 0.20      # cut when mark <= 20% of entry debit (80% lost)

try:
    from rich.console import Console
    _console = Console()
    def _log(m): _console.log(m)
except Exception:
    def _log(m): print(m)


# ── helpers ──────────────────────────────────────────────────────────────────
def _ensure_exit_log(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS spread_exit_log (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            evaluated_at     TEXT NOT NULL,
            options_trade_id INTEGER,
            symbol           TEXT,
            strategy_id      TEXT,
            origin           TEXT,
            condition        TEXT,
            signal           TEXT,
            mark             REAL,
            entry_debit      REAL,
            width            REAL,
            dte              INTEGER,
            regime           TEXT,
            action_taken     TEXT,
            detail           TEXT
        )
    """)


def _leg_mid_dicts(legs_json: str) -> list[dict]:
    return [{"occ": l["symbol"], "side": l["side"]} for l in json.loads(legs_json or "[]")]


def _width(legs_json: str):
    ks = [float(l["strike"]) for l in json.loads(legs_json or "[]") if l.get("strike") is not None]
    return abs(max(ks) - min(ks)) if len(ks) >= 2 else None


def _direction(structure: str, legs_json: str) -> str:
    s = (structure or "").lower()
    if "put" in s:
        return "put"
    if "call" in s:
        return "call"
    types = {l.get("type") for l in json.loads(legs_json or "[]")}
    return "put" if "put" in types else ("call" if "call" in types else "unknown")


def _origin(strategy_id: str) -> str:
    if strategy_id == "swingdesk_manual":
        return "manual"
    if (strategy_id or "").startswith("auto_spread_"):
        return "auto"
    return "other"


def evaluate_exit(debit: float, width, mark, dte, direction: str, regime: str) -> list:
    """Pure: return [(condition, signal, detail)] for the conditions that fired."""
    sigs = []
    if width is not None and mark is not None and debit:
        take_at = debit + _PROFIT_TAKE_FRAC * (width - debit)
        if mark >= take_at:
            sigs.append(("PROFIT_TAKE", "close",
                         f"mark {mark} >= take {round(take_at, 2)} "
                         f"(debit {debit} + 50% of width-debit {round(width - debit, 2)})"))
        if mark <= _MAX_LOSS_FRAC * debit:
            sigs.append(("MAX_LOSS", "cut",
                         f"mark {mark} <= {round(_MAX_LOSS_FRAC * debit, 2)} (20% of debit {debit})"))
    if dte is not None and dte <= _DTE_CLOSE:
        sigs.append(("TIME", "close_or_roll", f"DTE {dte} <= {_DTE_CLOSE}"))
    if direction == "put" and regime in _BULL_REGIMES:
        sigs.append(("REGIME_FLIP", "review", f"bearish put spread in {regime}"))
    if direction == "call" and regime in _BEAR_REGIMES:
        sigs.append(("REGIME_FLIP", "review", f"bullish call spread in {regime}"))
    return sigs


def _ntfy_manual(row, cond, signal, mark, detail) -> None:
    try:
        from engine.alert_channels import send_alert, AlertLevel
        send_alert(
            message=(f"SPREAD EXIT [{cond}] {row['symbol']} {row['structure']} "
                     f"(trade {row['id']}): mark={mark} entry_debit={row['entry_credit_debit']} "
                     f"→ SUGGEST {signal}. {detail}"),
            level=AlertLevel.WARNING, alert_type=f"spread-exit-{cond}-{row['id']}",
            audience="admin", rate_limit_secs=3600)
    except Exception as e:
        _log(f"[yellow][SPREAD-EXIT] NTFY failed: {type(e).__name__}: {e}")


def _auto_close(row, mark) -> str:
    """Auto-close via the W2 chokepoint (action=close). GATED by the W3 master
    gate — inert (logs only) until config.AUTO_SPREADS_ENABLED is True."""
    if not getattr(config, "AUTO_SPREADS_ENABLED", False):
        _log(f"[dim][SPREAD-EXIT] auto-close GATED OFF — would close trade {row['id']} "
             f"({row['strategy_id']})")
        return "gated_off"
    import requests
    legs = [{"underlying": row["symbol"], "occ": l["symbol"], "option_type": l["type"],
             "strike": l["strike"], "side": l["side"]}
            for l in json.loads(row["legs_json"])]
    try:
        r = requests.post(_W2_URL, json={
            "legs": legs, "net_debit_limit": mark, "structure": row["structure"],
            "strategy": row["strategy_id"], "action": "close", "dry_run": False,
        }, timeout=30)
        return f"close_submitted:{r.status_code}"
    except Exception as e:
        return f"close_error:{type(e).__name__}"


def _refresh_fills(rows) -> None:
    """No standalone fill-poller exists; refresh exec_status on candidate open rows
    each cycle so the 'filled' scan is current."""
    try:
        import spread_executor as se
    except Exception as e:
        _log(f"[yellow][SPREAD-EXIT] spread_executor import failed: {e}")
        return
    for r in rows:
        if r["broker_order_id"]:
            try:
                se.poll_fill(r["broker_order_id"])
            except Exception:
                pass


# ── module-level daemon (bound in main.py; 5 min, market hours) ──────────────
def run_spread_exit_cycle() -> dict:
    try:
        import spread_executor as se
    except Exception as e:
        _log(f"[yellow][SPREAD-EXIT] cannot import spread_executor: {e}")
        return {"error": "spread_executor unavailable"}

    conn = sqlite3.connect(str(_DB), timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        _ensure_exit_log(conn)
        conn.commit()
        base_q = ("SELECT * FROM options_trades WHERE legs_json IS NOT NULL "
                  "AND status='open'")
        _refresh_fills(conn.execute(base_q).fetchall())
        rows = conn.execute(base_q).fetchall()

        try:
            from engine.regime_ma import detect_ma_cross_regime
            regime = (detect_ma_cross_regime() or {}).get("regime") or "UNKNOWN"
        except Exception:
            regime = "UNKNOWN"

        evaluated = skipped = signaled = 0
        for row in rows:
            if se._norm_status(row["exec_status"]) != "filled":
                continue  # only OPEN + FILLED spreads are managed
            mark = se.mid_net_debit(_leg_mid_dicts(row["legs_json"]))
            if mark is None:
                skipped += 1
                _log(f"[dim][SPREAD-EXIT] trade {row['id']} {row['symbol']}: quote None "
                     f"→ skip (no false alert)")
                continue
            evaluated += 1
            width = _width(row["legs_json"])
            try:
                dte = (date.fromisoformat(row["expiration"]) - date.today()).days
            except Exception:
                dte = None
            direction = _direction(row["structure"], row["legs_json"])
            origin = _origin(row["strategy_id"])
            for cond, signal, detail in evaluate_exit(
                    row["entry_credit_debit"], width, mark, dte, direction, regime):
                signaled += 1
                if origin == "manual":
                    action_taken = "alert"
                    _ntfy_manual(row, cond, signal, mark, detail)
                elif origin == "auto":
                    action_taken = _auto_close(row, mark)
                else:
                    action_taken = "noop_other"
                conn.execute(
                    "INSERT INTO spread_exit_log (evaluated_at, options_trade_id, symbol, "
                    "strategy_id, origin, condition, signal, mark, entry_debit, width, dte, "
                    "regime, action_taken, detail) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (datetime.now(timezone.utc).isoformat(timespec="seconds"), row["id"],
                     row["symbol"], row["strategy_id"], origin, cond, signal, mark,
                     row["entry_credit_debit"], width, dte, regime, action_taken, detail))
            conn.commit()
        _log(f"[SPREAD-EXIT] cycle: {evaluated} evaluated, {skipped} quote-skipped, "
             f"{signaled} signals, regime={regime}")
        return {"evaluated": evaluated, "skipped": skipped, "signaled": signaled, "regime": regime}
    finally:
        conn.close()
