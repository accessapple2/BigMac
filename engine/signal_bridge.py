"""engine/signal_bridge.py — HM-BRINGBACK: deep_scan_results → trade_signals bridge.

Revives the dark scored feed (trade_signals) from the generator that's ALREADY
running: engine/deep_scan.py writes 300+ high-conf setups/day to deep_scan_results
(trader.db) for the Game Plan display. This bridge re-emits the W0-PROVEN-EDGE
subset into the signal-center scored substrate (trade_signals → signal_outcomes →
W0 forward scoring).

⚠️ OBSERVATION / SHADOW ONLY — HARD BOUNDARY:
  - Emitted with agent='shadow-bridge:<setup>' so it is excluded from execution:
    (1) the only trade_signals→buy consumer (neo-matrix _hm_an2_consume) is
        halt_mode='exit_only' (paper_trader.buy returns None), AND
    (2) a defensive guard skips agent_name.startswith('shadow') in that consumer.
  - These rows exist for W0 forward scoring ONLY. No order path is touched.
  - Rides the trader's in-process scheduler (main.py) → inherits @reboot
    survivability. NOT a launchd agent (the exact 2026-05-23 reboot gap).

Validation: each setup-type accrues forward and must clear DSR>=0.95 AND PBO<=0.3
(strategies/validation.py) before any execution is even PROPOSED. Execution stays
OFF until the Admiral's explicit go. bridge_readiness() reports the gate status.

Additive only: new table signal_bridge_emitted (dedupe); no source-row mutation.
"""
from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime, timezone

import requests

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

TRADER_DB = os.path.join(_ROOT, "data", "trader.db")
FLOW_GEX_DB = os.path.join(_ROOT, "data", "flow_gex.db")
SIGNAL_CENTER_URL = "http://127.0.0.1:9000/api/signal"
SHADOW_AGENT_PREFIX = "shadow-bridge"

# deep_scan strategy_name -> W0-PROVEN setup tag. ONLY proven edges (W0):
#   relative_strength +0.52R@5d (n=444 backbone), bull_flag +0.56R@5d (n=38).
# etf_regime (+1.0R@10d, n=29 undeflated) and premarket_gap are NOT deep_scan
# strategies — they need their own revival, not fabrication from deep_scan.
SETUP_MAP = {
    "relative_strength_high": "relative_strength",
    "bull_momentum_breakout": "bull_flag",
    "breakout_volume":        "bull_flag",
    # unproven — accruing from zero, graduation-gated (no W0 evidence yet)
    "rsi_oversold_bounce":    "rsi_bounce",
    "rsi_divergence":         "rsi_divergence",
}
MIN_CONF = 0.70          # deep_scan confidence is 0..1
MAX_PER_RUN = 40


def _ensure_dedupe_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_bridge_emitted (
            symbol TEXT NOT NULL, setup_tag TEXT NOT NULL, scan_date TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'deep_scan',
            emitted_at TEXT DEFAULT CURRENT_TIMESTAMP, http_status INTEGER,
            PRIMARY KEY (symbol, setup_tag, scan_date, source)
        )""")


def _already_emitted(conn: sqlite3.Connection, symbol: str, setup: str, day: str, source: str) -> bool:
    r = conn.execute("SELECT 1 FROM signal_bridge_emitted WHERE symbol=? AND setup_tag=? "
                     "AND scan_date=? AND source=?", (symbol, setup, day, source)).fetchone()
    return r is not None


def _mark_emitted(conn: sqlite3.Connection, symbol: str, setup: str, day: str, source: str, http: int) -> None:
    conn.execute("INSERT OR REPLACE INTO signal_bridge_emitted "
                 "(symbol,setup_tag,scan_date,source,emitted_at,http_status) VALUES (?,?,?,?,?,?)",
                 (symbol, setup, day, source, datetime.now(timezone.utc).isoformat(), http))


def _post_signal(symbol: str, setup: str, conf01: float, entry, stop, target, reasoning: str) -> int:
    """POST a SHADOW signal to signal-center. Returns HTTP status (0 on error).
    agent='shadow-bridge:<setup>' => excluded from execution by construction."""
    payload = {
        "symbol": symbol, "action": "BUY", "type": "SWING",
        "confidence": int(round(conf01 * 100)),
        "agent": f"{SHADOW_AGENT_PREFIX}:{setup}",
        "model": "deep_scan_bridge",
        "reasoning": f"[SHADOW · observation-only · W0 forward-scoring] {reasoning}",
        "price": float(entry or 0), "stop_loss": float(stop or 0),
        "take_profit": float(target or 0), "timeframe": "SWING",
        "context_summary": f"SHADOW bridge: {setup} | conf {conf01:.2f} | NOT executed (W0 scoring only)",
        "sources": [SHADOW_AGENT_PREFIX, setup, "deep_scan"],
    }
    try:
        r = requests.post(SIGNAL_CENTER_URL, json=payload, timeout=6)
        return r.status_code
    except Exception:
        return 0


def run_signal_bridge() -> dict:
    """Primary bridge: emit W0-edge deep_scan_results rows as SHADOW signals.
    Observation-only. Idempotent per (symbol, setup, scan_date)."""
    emitted, skipped_dupe, failed = 0, 0, 0
    by_setup: dict = {}
    conn = sqlite3.connect(TRADER_DB, timeout=20)
    conn.row_factory = sqlite3.Row
    try:
        _ensure_dedupe_table(conn)
        rows = conn.execute(
            "SELECT symbol, strategy_name, confidence, entry_price, stop_price, target_price, scan_date "
            "FROM deep_scan_results WHERE scan_date >= date('now','-1 day') AND confidence >= ? "
            "ORDER BY confidence DESC", (MIN_CONF,)).fetchall()
        for r in rows:
            setup = SETUP_MAP.get(r["strategy_name"])
            if not setup:
                continue   # not a W0-proven edge
            if not r["entry_price"] or not r["stop_price"]:
                continue   # need levels for R-scoring
            if _already_emitted(conn, r["symbol"], setup, r["scan_date"], "deep_scan"):
                skipped_dupe += 1
                continue
            if emitted >= MAX_PER_RUN:
                break
            http = _post_signal(r["symbol"], setup, float(r["confidence"]),
                                r["entry_price"], r["stop_price"], r["target_price"],
                                f"{r['strategy_name']} {r['symbol']} @ {r['entry_price']}")
            if http in (200, 201):
                _mark_emitted(conn, r["symbol"], setup, r["scan_date"], "deep_scan", http)
                emitted += 1
                by_setup[setup] = by_setup.get(setup, 0) + 1
            else:
                failed += 1
        conn.commit()
    finally:
        conn.close()
    return {"emitted": emitted, "skipped_dupe": skipped_dupe, "failed": failed,
            "by_setup": by_setup, "ts": datetime.now(timezone.utc).isoformat()}


def run_flow_bridge() -> dict:
    """Secondary: emit unusual-OI setups from the aggregate flow we already compute
    (flow_gex.db flow_aggregates). Print-level flow stays Polygon-tier-blocked (403)
    — a tier-upgrade DECISION for the Admiral, not a build here. Observation-only."""
    emitted, skipped_dupe, failed = 0, 0, 0
    if not os.path.exists(FLOW_GEX_DB):
        return {"emitted": 0, "note": "no flow_gex.db yet"}
    day = datetime.now().strftime("%Y-%m-%d")
    fconn = sqlite3.connect(FLOW_GEX_DB, timeout=20)
    fconn.row_factory = sqlite3.Row
    tconn = sqlite3.connect(TRADER_DB, timeout=20)
    try:
        _ensure_dedupe_table(tconn)
        import json as _json
        rows = fconn.execute("SELECT underlying, lean, unusual_json FROM flow_aggregates "
                             "ORDER BY id DESC LIMIT 4").fetchall()
        seen = set()
        for fr in rows:
            u = fr["underlying"]
            if u in seen:
                continue
            seen.add(u)
            try:
                unusual = _json.loads(fr["unusual_json"] or "[]")
            except Exception:
                unusual = []
            # one aggregate 'unusual_oi' shadow signal per underlying (directional lean)
            if not unusual:
                continue
            if _already_emitted(tconn, u, "unusual_oi", day, "flow_gex"):
                skipped_dupe += 1
                continue
            top = unusual[0]
            http = _post_signal(u, "unusual_oi", 0.70, top.get("strike"), None, None,
                                f"unusual OI {fr['lean']} ({len(unusual)} contracts, top {top.get('type')} {top.get('strike')})")
            if http in (200, 201):
                _mark_emitted(tconn, u, "unusual_oi", day, "flow_gex", http)
                emitted += 1
            else:
                failed += 1
        tconn.commit()
    finally:
        fconn.close()
        tconn.close()
    return {"emitted": emitted, "skipped_dupe": skipped_dupe, "failed": failed,
            "note": "aggregate flow only; print-level (sweep/block/aggressor) needs Polygon tier upgrade"}


def bridge_readiness() -> dict:
    """Report each shadow setup-type's W0 readiness vs the graduation gate
    (DSR>=0.95 AND PBO<=0.3). Observation-only — populates as signals accrue.
    Execution stays OFF until a setup clears AND the Admiral gives the go."""
    try:
        from strategies import validation as V
    except Exception as e:
        return {"error": f"validation import: {e}"}
    return {"gate": {"dsr": V.DSR_GRADUATE, "pbo": V.PBO_GRADUATE},
            "note": "per-setup DSR/PBO accrues from shadow signals via W0 (scored_predictions). "
                    "Run the W0 expectancy/PBO over agent='shadow-bridge:*' once enough closed. "
                    "Nothing graduates without clearing the gate + explicit Admiral go."}


if __name__ == "__main__":
    import json
    print(json.dumps({"primary": run_signal_bridge(), "secondary": run_flow_bridge(),
                      "readiness": bridge_readiness()}, indent=2, default=str))
