"""O-Tasty SHADOW autopilot — WAVE 8 (HM-O-TASTY-AUTOPILOT).

SHADOW ONLY. Every loop in this module is read/compute/persist. There is NO
order-submission path here — nothing imports the Alpaca trading client, nothing
calls buy/sell/submit_order. The isolated paper account PA3YVDTUH5CB is never
touched by this module. Verification is enforced loop-by-loop (zero-order audit).

Loops (built incrementally, hard checkpoint between each):
  A. IVR scan        → swingdesk_ivr        [THIS COMMIT]
  B. structure+entry → swingdesk_shadow_trades (shadow)   [pending]
  C. position mgr    → updates shadow_trades             [pending]
  D. kill-switch                                          [pending]
  E. nightly auditor                                      [pending]

DB: the swingdesk-local swingdesk.db (repo root) — never the fleet trader.db.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from pathlib import Path

# Load the swingdesk-local .env (isolated O-Tasty creds) so POLYGON_API_KEY
# resolves — mirrors swingdesk/scanner.py. Reads only the .env next to this file.
def _load_local_env() -> None:
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip())


_load_local_env()

from options_engine import calc_ivr, get_spot, MIN_IVR  # noqa: E402  (read-only IVR + spot)

DB_PATH = str(Path(__file__).resolve().parent.parent / "swingdesk.db")

# Dual IVR gate per HM-O-TASTY-DOCTRINE: IVR >= 50 AND IV >= 35%.
# MIN_IVR (=50) comes from options_engine; the IV floor is new here.
MIN_IV_GATE = 35.0

# 15-ETF universe with sector tags (for the later 3-per-sector cap in Loop C).
# Liquid, optionable, premium-selling-friendly ETFs across sectors.
ETF_UNIVERSE: list[tuple[str, str]] = [
    ("SPY", "index"),   ("QQQ", "index"),   ("IWM", "index"),
    ("XLE", "energy"),  ("USO", "energy"),
    ("XLF", "financials"),
    ("XLK", "technology"), ("SMH", "semiconductors"),
    ("XLV", "healthcare"),
    ("GLD", "metals"),  ("SLV", "metals"),
    ("TLT", "bonds"),   ("HYG", "bonds"),
    ("EEM", "international"), ("EFA", "international"),
]


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS swingdesk_ivr (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date   TEXT NOT NULL,
            symbol      TEXT NOT NULL,
            sector      TEXT,
            ivr         REAL,
            iv_current  REAL,
            iv_high     REAL,
            iv_low      REAL,
            ivp         REAL,
            spot        REAL,
            gate_pass   INTEGER NOT NULL DEFAULT 0,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_swingdesk_ivr_date ON swingdesk_ivr(scan_date, symbol)"
    )
    conn.commit()


def run_loop_a(universe: list[tuple[str, str]] | None = None) -> dict:
    """LOOP A — IVR scan. Read-only: computes IVR per ETF and persists rows to
    swingdesk_ivr with the dual-gate (IVR>=MIN_IVR AND IV>=MIN_IV_GATE) flag.

    NO order submission anywhere in this path. Returns a summary dict.
    """
    universe = universe or ETF_UNIVERSE
    scan_date = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    _ensure_schema(conn)

    written, passed, errored = 0, 0, []
    for symbol, sector in universe:
        try:
            ivr_data = calc_ivr(symbol)
            if ivr_data.get("error") or ivr_data.get("ivr") is None:
                errored.append(symbol)
                continue
            spot = get_spot(symbol) or 0.0
            ivr = ivr_data["ivr"]
            iv_current = ivr_data["iv_current"]
            gate_pass = int(ivr >= MIN_IVR and (iv_current or 0) >= MIN_IV_GATE)
            conn.execute(
                "INSERT INTO swingdesk_ivr "
                "(scan_date, symbol, sector, ivr, iv_current, iv_high, iv_low, ivp, spot, gate_pass) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (scan_date, symbol, sector, ivr, iv_current,
                 ivr_data.get("iv_high"), ivr_data.get("iv_low"),
                 ivr_data.get("ivp"), spot, gate_pass),
            )
            written += 1
            passed += gate_pass
        except Exception as e:  # never raise out of the shadow loop
            errored.append(f"{symbol}:{type(e).__name__}")

    conn.commit()
    conn.close()
    return {
        "loop": "A",
        "scan_date": scan_date,
        "universe": len(universe),
        "written": written,
        "gate_pass": passed,
        "errored": errored,
        "gate": f"IVR>={MIN_IVR} AND IV>={MIN_IV_GATE}",
        "orders_submitted": 0,  # invariant: Loop A never submits
    }


if __name__ == "__main__":
    print(run_loop_a())
