#!/usr/bin/env python3
"""HM-GUARDIAN-ADOPTION (2026-06-12) — adopt orphan Alpaca positions under an
exit-only guardian player so they get stop coverage.

Creates the exit-only player `guardian-of-forever` (may NEVER buy — exit_only +
absent from every scanner/AI_SIGNAL_PLAYERS) and adopts the 22 orphan Alpaca
(paper) positions from docs/orphan_positions_2026-06-12.md, sourcing exact qty +
avg_entry_price from the LIVE Alpaca positions API and cross-checking the doc.

IDEMPOTENT — keyed on (player_id, symbol, asset_type='stock'). Re-running never
duplicates a player row or a position row. Source mirror rows (`alpaca-mirror`)
are NOT touched — they remain the broker sync record.

Paper account only. No DELETE/DROP/TRUNCATE. RULE #1 (Schwab) untouched.
"""
from __future__ import annotations
import os
import sqlite3
import sys

# Run from the project root so `engine.*` imports resolve regardless of CWD.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
os.chdir(_ROOT)

DB = "data/trader.db"
GUARDIAN = "guardian-of-forever"

# The 22 orphans from docs/orphan_positions_2026-06-12.md (alpaca-mirror symbols
# with no active-player owner). Sourced live from Alpaca; cross-checked vs doc.
ORPHANS = {
    "AVB", "AVGO", "BAP", "F", "GM", "INTU", "IWP", "KMI", "LII", "LLY", "LNTH",
    "MDGL", "MSFT", "NUGT", "RBC", "SPGI", "SYM", "TKR", "WFRD", "WMB", "WMG", "ZM",
}


def ensure_guardian(conn: sqlite3.Connection) -> str:
    row = conn.execute("SELECT id FROM ai_players WHERE id=?", (GUARDIAN,)).fetchone()
    if row:
        return "exists"
    conn.execute(
        """INSERT INTO ai_players
           (id, display_name, provider, model_id, cash, is_active, is_paused,
            is_human, can_trade_live, options_enabled, short_enabled,
            crew_role, role, halt_mode, halt_reason)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (GUARDIAN, "Guardian of Forever", "system", "rule-based-stop",
         10000.0, 1, 0, 0, 0, 0, 0,
         "guardian", "production", "exit_only",
         "HM-GUARDIAN-ADOPTION 2026-06-12: exit-only stop guardian for orphan "
         "Alpaca positions; may NEVER buy. Flat 12% stop, routes exits to Alpaca."),
    )
    return "created"


def adopt(conn: sqlite3.Connection, live: dict) -> tuple[int, int, list]:
    adopted, skipped, rows = 0, 0, []
    for sym in sorted(ORPHANS):
        p = live.get(sym)
        if not p:
            rows.append((sym, "MISSING-FROM-ALPACA", None, None))
            continue
        qty, entry = float(p["qty"]), float(p["avg_entry"])
        exists = conn.execute(
            "SELECT qty FROM positions WHERE player_id=? AND symbol=? AND asset_type='stock'",
            (GUARDIAN, sym),
        ).fetchone()
        if exists:
            skipped += 1
            rows.append((sym, "skip(exists)", qty, entry))
            continue
        conn.execute(
            """INSERT INTO positions
               (player_id, symbol, qty, avg_price, asset_type, high_watermark, opened_at)
               VALUES (?,?,?,?,'stock',?,CURRENT_TIMESTAMP)""",
            (GUARDIAN, sym, qty, entry, entry),
        )
        adopted += 1
        rows.append((sym, "ADOPTED", qty, entry))
    return adopted, skipped, rows


def main() -> int:
    from engine.alpaca_bridge import AlpacaBridge
    live = {p["symbol"]: p for p in AlpacaBridge().positions()
            if isinstance(p, dict) and "symbol" in p}
    missing = sorted(s for s in ORPHANS if s not in live)
    if missing:
        print(f"[WARN] orphans not in live Alpaca API (skipping): {missing}", file=sys.stderr)

    conn = sqlite3.connect(DB, timeout=30)
    try:
        state = ensure_guardian(conn)
        adopted, skipped, rows = adopt(conn, live)
        conn.commit()
    finally:
        conn.close()

    print(f"guardian-of-forever: {state}")
    print(f"adopted={adopted}  skipped(existing)={skipped}  target={len(ORPHANS)}")
    for sym, status, qty, entry in rows:
        print(f"  {sym:6s} {status:14s} qty={qty} entry={entry}")
    total = adopted + skipped
    print(f"guardian now owns {total}/{len(ORPHANS)} orphans")
    return 0 if total == len(ORPHANS) else 1


if __name__ == "__main__":
    raise SystemExit(main())
