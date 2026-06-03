"""Ollie Machine — Step 7 P2a: SIM player + brackets + entry (tracking-mode, log-only).

Run BY HAND. Builds on P1 (`engine/ollie_machine.py`, which writes `ollie_machine_picks`):
  1. Register the `ollie-machine` player in `ai_players` — rule-based provider,
     can_trade_live=0, and its own tracking-mode portfolio. (NOTE 2026-06-02:
     once the P3 SIM accrual loop was enabled, the row runs is_paused=0 /
     halt_mode='active' so the loop can write the ledger — is_paused is NOT a
     safety guard here; see the corrected guard list below.)
  2. Generate brackets for the picks (NULL in the broad-universe screen) via the
     EXISTING `/api/trade-levels` endpoint (signal-center :9000). Write them back
     onto `ollie_machine_picks`.
  3. SIM-enter the top-3 into a new `ollie_machine_ledger` — sized at 2% notional,
     respecting a 5-concurrent cap + a -2% daily breaker. NO executor call.

SIM-SAFE BY CONSTRUCTION — the binding guards keep this inert on the live box.
(Corrected 2026-06-02: is_paused is NOT one of them — SIM accrual requires
is_paused=0/active, so the real guards are the four below.)
  • can_trade_live=0 — the player can never route a live order.
  • ROSTER-ABSENCE — ollie-machine is NOT in any scan/trade roster list
    (_SCAN_TIER{1,2,3}, SNIPER_AGENTS, RULES_SCANNERS, ADVISORY_CREW). Per the
    auto-discovery audit 2026-06-01, an ai_players row is inert unless it is a
    MEMBER of one of those hardcoded Python lists; a row absent from all of them
    is invisible to every scan/trade loop REGARDLESS of is_paused/halt_mode.
  • TRACKING-PORTFOLIO — portfolio.execution_mode='tracking' (log-only, like
    dalio-metals) AND ollie-machine is NOT in `_EXECUTION_PORTFOLIO_BY_PLAYER`
    (engine/paper_trader.py) → no broker forward path exists for it.
  • LEDGER-DIRECT / NEVER-buy() — the SIM entry path (`sim_enter`) is a direct
    INSERT into `ollie_machine_ledger`; it never calls paper_trader.buy() / the
    executor chokepoint, so the dedup + re-entry guards there are moot here.

NO scheduling, NO exit-monitor, NO restart. The `ollie_machine_ledger` table is
SIM-private — the running trader has NO reader for it and needs none.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone

import requests

DB_PATH = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))
SIGNAL_CENTER = os.environ.get("OLLIE_MACHINE_SC", "http://127.0.0.1:9000")

PLAYER_ID = "ollie-machine"
PORTFOLIO_NAME = "Ollie Machine SIM"
GENESIS_CAPITAL = 10_000.0     # match ollie-auto's S6 genesis (portfolio_history opens ~$10,076 / $9,238 cash)
POSITION_PCT = 0.02            # 2% notional / trade (matches fleet base alloc — paper_trader.py:1339-1363)
MAX_CONCURRENT = 5             # 5-concurrent SIM cap
DAILY_BREAKER_PCT = -0.02      # halt new entries if today's SIM realized P&L <= -2% of equity
TOP_ENTER = 3                  # SIM-enter the top-3 picks


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c


# ─────────────────────────── schema / registration ───────────────────────────
def ensure_ledger_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ollie_machine_ledger (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id         TEXT NOT NULL DEFAULT 'ollie-machine',
            portfolio_id      INTEGER,
            symbol            TEXT NOT NULL,
            side              TEXT NOT NULL DEFAULT 'long',
            entry_price       REAL NOT NULL,
            qty               REAL NOT NULL,
            notional          REAL NOT NULL,
            stop              REAL,
            tp1               REAL,
            tp2               REAL,
            tp3               REAL,
            risk_per_share    REAL,
            risk_amount       REAL,
            rr                REAL,
            sl_pct            REAL,
            convergence_count INTEGER,
            pick_rank         INTEGER,
            status            TEXT NOT NULL DEFAULT 'open',
            realized_pnl      REAL,
            source            TEXT NOT NULL DEFAULT 'p2a-sim',
            opened_at         TEXT NOT NULL,
            closed_at         TEXT,
            created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()


def register_player(conn: sqlite3.Connection) -> dict:
    """Register the dormant SIM player + its tracking-mode portfolio (idempotent)."""
    existed_player = conn.execute("SELECT 1 FROM ai_players WHERE id=?", (PLAYER_ID,)).fetchone() is not None
    if not existed_player:
        conn.execute(
            """
            INSERT INTO ai_players
              (id, display_name, provider, model_id, cash, is_active, can_trade_live,
               is_paused, season, halt_mode, halt_reason, role, crew_role, timeframe)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (PLAYER_ID, "Ollie Machine", "rule-based", "convergence-2of4", GENESIS_CAPITAL,
             1, 0,                       # is_active=1, can_trade_live=0
             1,                          # is_paused=1 → dormant (belt-and-braces; also not in any roster list)
             6, "active",
             "[2026-06-01] HM-OLLIE-MACHINE P2a — SIM convergence player. tracking-mode, "
             "can_trade_live=0, dormant (is_paused=1) until P3 scheduling. Not in any "
             "scan/exec roster list → inert on live trader.",
             "production", "sim", "swing"),
        )

    existed_pf = conn.execute("SELECT id FROM portfolios WHERE name=?", (PORTFOLIO_NAME,)).fetchone()
    if existed_pf is None:
        cur = conn.execute(
            """
            INSERT INTO portfolios
              (name, broker, account_type, initial_balance, current_balance,
               is_active, notes, execution_mode, type)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (PORTFOLIO_NAME, "sim", "paper", GENESIS_CAPITAL, GENESIS_CAPITAL, 1,
             "HM-OLLIE-MACHINE P2a SIM book — tracking/log-only head-to-head vs ollie-auto.",
             "tracking", "paper"),
        )
        portfolio_id = cur.lastrowid
    else:
        portfolio_id = existed_pf["id"]

    conn.commit()
    return {"player_existed": existed_player, "portfolio_id": portfolio_id,
            "portfolio_existed": existed_pf is not None}


# ─────────────────────────── brackets via /api/trade-levels ───────────────────
def fetch_levels(symbol: str) -> dict | None:
    """Pull entry/stop/tp from the EXISTING signal-center /api/trade-levels endpoint."""
    try:
        r = requests.get(f"{SIGNAL_CENTER}/api/trade-levels/{symbol.upper()}", timeout=6)
        if r.status_code != 200:
            return None
        d = r.json()
        lng = d.get("long") or {}
        entry = lng.get("entry_hi") or lng.get("entry_lo") or d.get("price")
        stop = lng.get("stop_loss")
        if not entry or not stop:
            return None
        return {
            "entry": float(entry), "stop": float(stop),
            "tp1": lng.get("tp1"), "tp2": lng.get("tp2"), "tp3": lng.get("tp3"),
            "rr": lng.get("rr"), "sl_pct": lng.get("sl_pct"),
            "atr": d.get("atr"), "atr_pct": d.get("atr_pct"),
            "price": d.get("price"), "regime": d.get("regime"),
            "recommendation": d.get("recommendation"),
        }
    except Exception as e:  # noqa: BLE001 — manual run; surface and skip
        print(f"  [trade-levels] {symbol}: {type(e).__name__}: {e!r}")
        return None


def generate_brackets(conn: sqlite3.Connection) -> list[dict]:
    """For each logged pick (top conviction first), fetch levels and write them back."""
    picks = conn.execute(
        "SELECT id, symbol, convergence_count, conviction_rank, signals_fired, rs_rank, convergence_type "
        "FROM ollie_machine_picks ORDER BY conviction_rank"
    ).fetchall()
    out = []
    for p in picks:
        lv = fetch_levels(p["symbol"])
        if lv:
            conn.execute(
                "UPDATE ollie_machine_picks SET entry_price=?, stop=?, tp1=?, tp2=?, tp3=? WHERE id=?",
                (lv["entry"], lv["stop"], lv["tp1"], lv["tp2"], lv["tp3"], p["id"]),
            )
        out.append({**dict(p), "levels": lv})
    conn.commit()
    return out


# ─────────────────────────── SIM entry (no executor) ──────────────────────────
def _open_count(conn: sqlite3.Connection) -> int:
    return conn.execute(
        "SELECT COUNT(*) FROM ollie_machine_ledger WHERE player_id=? AND status='open'", (PLAYER_ID,)
    ).fetchone()[0]


def _today_realized(conn: sqlite3.Connection) -> float:
    """Today's SIM realized P&L (drives the -2% daily breaker). 0 until an exit-monitor exists (P3)."""
    row = conn.execute(
        "SELECT COALESCE(SUM(realized_pnl),0) FROM ollie_machine_ledger "
        "WHERE player_id=? AND status='closed' AND date(closed_at)=date('now')", (PLAYER_ID,)
    ).fetchone()
    return float(row[0] or 0.0)


def sim_enter(conn: sqlite3.Connection, bracketed: list[dict], portfolio_id: int,
              source: str = "p2a-sim") -> dict:
    """SIM-enter the top picks. 2% notional sizing, 5-concurrent cap, -2% daily breaker.
    Flat-then-enter: clears ALL of the player's prior OPEN rows (any source) first."""
    ts = datetime.now(timezone.utc).isoformat()
    notional_per = round(POSITION_PCT * GENESIS_CAPITAL, 2)   # $200 on $10k

    # idempotent re-enter: clear this player's prior open SIM rows (any phase)
    conn.execute("DELETE FROM ollie_machine_ledger WHERE player_id=? AND status='open'", (PLAYER_ID,))
    conn.commit()

    realized = _today_realized(conn)
    breaker_tripped = (realized / GENESIS_CAPITAL) <= DAILY_BREAKER_PCT
    opened, skipped = [], []

    for p in bracketed[:TOP_ENTER]:
        lv = p.get("levels")
        if breaker_tripped:
            skipped.append({**p, "reason": "daily -2% breaker tripped"}); continue
        if _open_count(conn) >= MAX_CONCURRENT:
            skipped.append({**p, "reason": "5-concurrent cap"}); continue
        if not lv:
            skipped.append({**p, "reason": "no trade-levels (bracket unavailable)"}); continue
        entry, stop = lv["entry"], lv["stop"]
        qty = int(notional_per // entry)
        if qty < 1:
            skipped.append({**p, "reason": f"share price {entry} > notional/trade {notional_per}"}); continue
        risk_ps = round(entry - stop, 4)
        rec = {
            "symbol": p["symbol"], "entry": entry, "qty": qty,
            "notional": round(qty * entry, 2), "stop": stop,
            "tp1": lv["tp1"], "tp2": lv["tp2"], "tp3": lv["tp3"],
            "risk_per_share": risk_ps, "risk_amount": round(qty * risk_ps, 2),
            "rr": lv["rr"], "sl_pct": lv["sl_pct"],
            "convergence_count": p["convergence_count"], "pick_rank": p["conviction_rank"],
        }
        conn.execute(
            """
            INSERT INTO ollie_machine_ledger
              (player_id, portfolio_id, symbol, side, entry_price, qty, notional, stop,
               tp1, tp2, tp3, risk_per_share, risk_amount, rr, sl_pct,
               convergence_count, pick_rank, status, source, opened_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (PLAYER_ID, portfolio_id, rec["symbol"], "long", entry, qty, rec["notional"], stop,
             rec["tp1"], rec["tp2"], rec["tp3"], risk_ps, rec["risk_amount"], rec["rr"], rec["sl_pct"],
             rec["convergence_count"], rec["pick_rank"], "open", source, ts),
        )
        opened.append(rec)
    conn.commit()
    return {"notional_per": notional_per, "breaker_tripped": breaker_tripped,
            "today_realized": realized, "opened": opened, "skipped": skipped}


def run() -> dict:
    conn = _conn()
    try:
        ensure_ledger_table(conn)
        reg = register_player(conn)
        bracketed = generate_brackets(conn)
        entry = sim_enter(conn, bracketed, reg["portfolio_id"])
        return {"reg": reg, "bracketed": bracketed, "entry": entry}
    finally:
        conn.close()


if __name__ == "__main__":
    r = run()
    reg, bracketed, entry = r["reg"], r["bracketed"], r["entry"]

    print("\n=== Ollie Machine P2a — SIM player + brackets + entry (tracking/log-only) ===")
    print("\n--- player + portfolio ---")
    print(f"  player   : {PLAYER_ID} ({'already existed' if reg['player_existed'] else 'CREATED'}) "
          f"— provider=rule-based, can_trade_live=0, is_paused=1 (dormant)")
    print(f"  portfolio: #{reg['portfolio_id']} '{PORTFOLIO_NAME}' "
          f"({'already existed' if reg['portfolio_existed'] else 'CREATED'}) "
          f"— execution_mode=tracking, genesis ${GENESIS_CAPITAL:,.0f}")

    print(f"\n--- brackets generated (via {SIGNAL_CENTER}/api/trade-levels) ---")
    print(f"  {'#':>2} {'SYM':6} {'entry':>8} {'stop':>8} {'tp1':>8} {'tp2':>8} {'tp3':>8} {'rr':>5} {'sl%':>6}")
    for p in bracketed:
        lv = p.get("levels")
        if not lv:
            print(f"  {p['conviction_rank']:>2} {p['symbol']:6}  (no levels — endpoint unavailable)"); continue
        rr = f"{lv['rr']:.2f}" if lv.get("rr") is not None else "  —"
        slp = f"{lv['sl_pct']:.2f}" if lv.get("sl_pct") is not None else "   —"
        print(f"  {p['conviction_rank']:>2} {p['symbol']:6} {lv['entry']:>8.2f} {lv['stop']:>8.2f} "
              f"{(lv['tp1'] or 0):>8.2f} {(lv['tp2'] or 0):>8.2f} {(lv['tp3'] or 0):>8.2f} {rr:>5} {slp:>6}")

    e = entry
    print(f"\n--- SIM entry (2% notional = ${e['notional_per']:.0f}/trade, "
          f"5-cap, -2% breaker={'TRIPPED' if e['breaker_tripped'] else 'ok'} "
          f"[today realized ${e['today_realized']:.2f}]) ---")
    if not e["opened"]:
        print("  (no positions opened)")
    for o in e["opened"]:
        print(f"  #{o['pick_rank']} {o['symbol']:6} qty={o['qty']:>4} @ {o['entry']:.2f}  "
              f"notional=${o['notional']:.2f}  stop={o['stop']:.2f}  "
              f"tp1={o['tp1']} tp2={o['tp2']} tp3={o['tp3']}  "
              f"risk=${o['risk_amount']:.2f} ({o['risk_per_share']:.2f}/sh, rr={o['rr']})")
    for s in e["skipped"]:
        print(f"  SKIP {s['symbol']:6} — {s['reason']}")

    print("\n--- SIM-safety ---")
    print("  can_trade_live=0 + execution_mode=tracking + not in _EXECUTION_PORTFOLIO_BY_PLAYER")
    print("  + not in any scan roster + is_paused=1  →  physically cannot route to Alpaca.")
    print("  NEW TABLE ollie_machine_ledger is SIM-private; the running trader has no reader and needs none.")
