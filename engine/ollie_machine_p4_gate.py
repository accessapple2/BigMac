"""Ollie Machine — P4 promotion gate (standalone, READ-ONLY over the SIM ledger).

Computes performance metrics over CLOSED `ollie_machine_ledger` trades and assigns a
promotion tier (Observe / Eligible / Promote). Persists a single status row to
`ollie_machine_p4_status`. PURE OBSERVABILITY — four safety invariants by construction:

  1. ZERO executor calls. This module imports NOTHING from paper_trader / alpaca /
     alpaca_options / executor / any broker path. Only sqlite3 + stdlib.
  2. Reads `ollie_machine_ledger`; WRITES ONLY its own `ollie_machine_p4_status` row
     (single row, id=1, upsert). It never mutates the ledger or any other table.
  3. NEVER touches `ai_players` — `can_trade_live` stays 0. The gate cannot flip a
     player live. It only reports.
  4. Eligible / Promote RAISE A FLAG (`flag_raised=1`); they NEVER auto-advance. Crossing
     a tier is an Admiral signal, not an action.

Metrics over closed trades (genesis = $10k, matches ollie_machine_p2a.GENESIS_CAPITAL):
  • count            — closed trades
  • win_rate         — wins / count   (win = realized_pnl > 0)
  • expectancy_r     — mean(realized_pnl / risk) per trade, risk = (entry − stop) × qty
  • profit_factor    — Σ winning P&L / |Σ losing P&L|
  • max_dd_pct       — max peak-to-trough of the realized-equity curve, as % of $10k genesis
  • breaker health   — reconstructed: any day whose summed realized P&L ≤ −2% of genesis
                       is a breaker-fire day. (The −2% daily breaker in p2a.sim_enter is
                       computed transiently and NOT persisted, so it is reconstructed here.)

Tiers (locked):
  • Observe  = <30 trades OR any floor unmet
  • Eligible = ≥30 trades AND all six floors met
  • Promote  = ≥50 trades AND all six floors met AND prior eval was Eligible (or Promote)

Six floors: trades≥30 · WR≥48% · expectancy≥+0.30R · PF≥1.5 · maxDD≤18% · breaker clean.

Cadence: evaluate every 10 NEW closed trades OR weekly (see `should_eval`). Bound to the
process lifecycle via `_bg_ollie_machine_p4_gate` in main.py (daemon-lifecycle rule) — the
due-ness state lives in the persisted status row, NOT in lazy module state.

Run by hand:  python -m engine.ollie_machine_p4_gate          (eval if due)
              python -m engine.ollie_machine_p4_gate --force  (force eval now)
"""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone

DB_PATH = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))

PLAYER_ID = "ollie-machine"
GENESIS_CAPITAL = 10_000.0     # matches ollie_machine_p2a.GENESIS_CAPITAL (hardcoded — keep standalone)

# ── locked floors ─────────────────────────────────────────────────────────────
FLOOR_MIN_TRADES = 30
FLOOR_WIN_RATE = 0.48
FLOOR_EXPECTANCY_R = 0.30
FLOOR_PROFIT_FACTOR = 1.5
FLOOR_MAX_DD_PCT = 18.0
PROMOTE_MIN_TRADES = 50
BREAKER_DAILY_PCT = -0.02      # a day's summed realized ≤ −2% of genesis = breaker fire

# ── cadence ───────────────────────────────────────────────────────────────────
EVAL_EVERY_N_CLOSED = 10
EVAL_EVERY_DAYS = 7


def _conn(db_path: str | None = None) -> sqlite3.Connection:
    c = sqlite3.connect(db_path or DB_PATH, timeout=30)
    c.execute("PRAGMA busy_timeout=30000")   # match the trader's _patched_connect (WAL writer contention)
    c.row_factory = sqlite3.Row
    return c


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


# ─────────────────────────── status table (own row only) ─────────────────────
def ensure_status_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ollie_machine_p4_status (
            id                   INTEGER PRIMARY KEY CHECK (id = 1),
            evaluated_at         TEXT    NOT NULL,
            tier                 TEXT    NOT NULL,
            prev_tier            TEXT,
            trade_count          INTEGER NOT NULL,
            win_rate             REAL,
            expectancy_r         REAL,
            profit_factor        REAL,
            max_dd_pct           REAL,
            breaker_fire_count   INTEGER,
            breaker_clean        INTEGER,
            failed_floors        TEXT,
            flag_raised          INTEGER NOT NULL DEFAULT 0,
            can_trade_live_guard INTEGER NOT NULL DEFAULT 0,
            snapshot_json        TEXT,
            updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()


def _read_status(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM ollie_machine_p4_status WHERE id = 1").fetchone()


# ─────────────────────────── metrics ─────────────────────────────────────────
def _closed_trades(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT symbol, entry_price, stop, qty, realized_pnl, risk_amount, "
        "       opened_at, closed_at "
        "FROM ollie_machine_ledger "
        "WHERE player_id = ? AND status = 'closed' AND realized_pnl IS NOT NULL "
        "ORDER BY COALESCE(closed_at, opened_at) ASC, id ASC",
        (PLAYER_ID,),
    ).fetchall()


def _trade_risk(r: sqlite3.Row) -> float | None:
    """Initial $ risk = (entry − stop) × qty. Falls back to the stored risk_amount."""
    entry, stop, qty = r["entry_price"], r["stop"], r["qty"]
    if entry is not None and stop is not None and qty:
        risk = (float(entry) - float(stop)) * float(qty)
        if risk > 0:
            return risk
    ra = r["risk_amount"]
    if ra is not None and float(ra) > 0:
        return float(ra)
    return None


def compute_metrics(conn: sqlite3.Connection) -> dict:
    rows = _closed_trades(conn)
    count = len(rows)

    if count == 0:
        return {
            "count": 0, "wins": 0, "losses": 0,
            "win_rate": None, "expectancy_r": None, "profit_factor": None,
            "max_dd_pct": None, "breaker_fire_count": 0, "breaker_clean": True,
            "breaker_days": [], "r_sample": 0, "gross_pnl": 0.0,
        }

    wins = sum(1 for r in rows if (r["realized_pnl"] or 0) > 0)
    losses = sum(1 for r in rows if (r["realized_pnl"] or 0) < 0)
    win_rate = wins / count

    gross_win = sum(float(r["realized_pnl"]) for r in rows if (r["realized_pnl"] or 0) > 0)
    gross_loss = sum(float(r["realized_pnl"]) for r in rows if (r["realized_pnl"] or 0) < 0)
    profit_factor = (gross_win / abs(gross_loss)) if gross_loss < 0 else None  # None = no losses

    # expectancy in R: mean(P&L / initial risk) over trades with a defined risk
    r_multiples = []
    for r in rows:
        risk = _trade_risk(r)
        if risk:
            r_multiples.append(float(r["realized_pnl"]) / risk)
    expectancy_r = (sum(r_multiples) / len(r_multiples)) if r_multiples else None

    # max drawdown over the realized-equity curve, as % of genesis
    equity = GENESIS_CAPITAL
    peak = GENESIS_CAPITAL
    max_dd = 0.0
    for r in rows:
        equity += float(r["realized_pnl"] or 0)
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    max_dd_pct = (max_dd / GENESIS_CAPITAL) * 100.0

    # breaker health — reconstruct daily realized sums, fire if ≤ −2% of genesis
    daily: dict[str, float] = {}
    for r in rows:
        day = (r["closed_at"] or "")[:10]
        daily[day] = daily.get(day, 0.0) + float(r["realized_pnl"] or 0)
    fire_threshold = BREAKER_DAILY_PCT * GENESIS_CAPITAL  # −$200
    breaker_days = sorted(d for d, pnl in daily.items() if pnl <= fire_threshold)

    return {
        "count": count, "wins": wins, "losses": losses,
        "win_rate": win_rate,
        "expectancy_r": expectancy_r,
        "profit_factor": profit_factor,
        "max_dd_pct": max_dd_pct,
        "breaker_fire_count": len(breaker_days),
        "breaker_clean": len(breaker_days) == 0,
        "breaker_days": breaker_days,
        "r_sample": len(r_multiples),
        "gross_pnl": round(gross_win + gross_loss, 2),
    }


# ─────────────────────────── floors + tier ───────────────────────────────────
def evaluate_floors(m: dict) -> dict:
    """Return {floor_key: bool}. profit_factor None (no losses) passes the PF floor."""
    return {
        "min_trades":    m["count"] >= FLOOR_MIN_TRADES,
        "win_rate":      m["win_rate"] is not None and m["win_rate"] >= FLOOR_WIN_RATE,
        "expectancy_r":  m["expectancy_r"] is not None and m["expectancy_r"] >= FLOOR_EXPECTANCY_R,
        "profit_factor": (m["profit_factor"] is None and m["count"] > 0)
                         or (m["profit_factor"] is not None and m["profit_factor"] >= FLOOR_PROFIT_FACTOR),
        "max_dd":        m["max_dd_pct"] is not None and m["max_dd_pct"] <= FLOOR_MAX_DD_PCT,
        "breaker_clean": bool(m["breaker_clean"]),
    }


def assign_tier(m: dict, floors: dict, prev_tier: str | None) -> str:
    all_met = all(floors.values())
    if not all_met:
        return "Observe"
    if m["count"] >= PROMOTE_MIN_TRADES and (prev_tier in ("Eligible", "Promote")):
        return "Promote"
    return "Eligible"


def evaluate(conn: sqlite3.Connection) -> dict:
    """Compute the full snapshot (metrics + floors + tier). Does NOT write."""
    prev = _read_status(conn)
    prev_tier = prev["tier"] if prev else None

    m = compute_metrics(conn)
    floors = evaluate_floors(m)
    tier = assign_tier(m, floors, prev_tier)
    failed = sorted(k for k, ok in floors.items() if not ok)

    return {
        "evaluated_at": _utc_now().isoformat(),
        "tier": tier,
        "prev_tier": prev_tier,
        "metrics": m,
        "floors": floors,
        "failed_floors": failed,
        "flag_raised": tier in ("Eligible", "Promote"),
        "can_trade_live_guard": 0,   # asserted: the gate never advances the player
    }


def persist(conn: sqlite3.Connection, snap: dict) -> None:
    """Upsert the single status row (id=1). Writes ONLY this table."""
    m = snap["metrics"]
    conn.execute(
        """
        INSERT INTO ollie_machine_p4_status
            (id, evaluated_at, tier, prev_tier, trade_count, win_rate, expectancy_r,
             profit_factor, max_dd_pct, breaker_fire_count, breaker_clean, failed_floors,
             flag_raised, can_trade_live_guard, snapshot_json, updated_at)
        VALUES (1, ?,?,?,?,?,?,?,?,?,?,?,?,?,?, CURRENT_TIMESTAMP)
        ON CONFLICT(id) DO UPDATE SET
            evaluated_at=excluded.evaluated_at, tier=excluded.tier, prev_tier=excluded.prev_tier,
            trade_count=excluded.trade_count, win_rate=excluded.win_rate,
            expectancy_r=excluded.expectancy_r, profit_factor=excluded.profit_factor,
            max_dd_pct=excluded.max_dd_pct, breaker_fire_count=excluded.breaker_fire_count,
            breaker_clean=excluded.breaker_clean, failed_floors=excluded.failed_floors,
            flag_raised=excluded.flag_raised, can_trade_live_guard=excluded.can_trade_live_guard,
            snapshot_json=excluded.snapshot_json, updated_at=CURRENT_TIMESTAMP
        """,
        (
            snap["evaluated_at"], snap["tier"], snap["prev_tier"], m["count"],
            m["win_rate"], m["expectancy_r"], m["profit_factor"], m["max_dd_pct"],
            m["breaker_fire_count"], 1 if m["breaker_clean"] else 0,
            json.dumps(snap["failed_floors"]), 1 if snap["flag_raised"] else 0,
            0, json.dumps(snap),
        ),
    )
    conn.commit()


# ─────────────────────────── cadence ─────────────────────────────────────────
def should_eval(conn: sqlite3.Connection) -> tuple[bool, str]:
    """Due if no prior eval, OR ≥10 new closed trades since last eval, OR ≥7 days elapsed."""
    prev = _read_status(conn)
    if prev is None:
        return True, "first-eval"
    closed_now = conn.execute(
        "SELECT COUNT(*) FROM ollie_machine_ledger "
        "WHERE player_id = ? AND status = 'closed' AND realized_pnl IS NOT NULL",
        (PLAYER_ID,),
    ).fetchone()[0]
    if closed_now - (prev["trade_count"] or 0) >= EVAL_EVERY_N_CLOSED:
        return True, f"+{closed_now - (prev['trade_count'] or 0)} closed since last eval"
    last = _parse_ts(prev["evaluated_at"])
    if last is None or (_utc_now() - last) >= timedelta(days=EVAL_EVERY_DAYS):
        return True, "weekly"
    return False, "not-due"


def run_eval(db_path: str | None = None, force: bool = False) -> dict:
    """Eval if due (or forced), persist, return the snapshot. Self-contained connection."""
    conn = _conn(db_path)
    try:
        ensure_status_table(conn)
        due, why = should_eval(conn)
        if not (due or force):
            cur = _read_status(conn)
            return {"evaluated": False, "reason": why,
                    "current_tier": cur["tier"] if cur else None,
                    "trade_count": cur["trade_count"] if cur else 0}
        snap = evaluate(conn)
        persist(conn, snap)
        snap["evaluated"] = True
        snap["reason"] = "forced" if (force and not due) else why
        return snap
    finally:
        conn.close()


def _fmt(v, pct=False, plus=False):
    if v is None:
        return "—"
    s = f"{v:+.2f}" if plus else f"{v:.2f}"
    return s + "%" if pct else s


if __name__ == "__main__":
    import sys
    force = "--force" in sys.argv
    snap = run_eval(force=force)

    print("\n=== Ollie Machine P4 promotion gate (SIM, read-only) ===")
    if not snap.get("evaluated"):
        print(f"  not evaluated ({snap['reason']}) — current tier "
              f"{snap.get('current_tier')} @ {snap.get('trade_count')} trades")
        sys.exit(0)

    m = snap["metrics"]
    n = m["count"]
    if n < FLOOR_MIN_TRADES:
        print(f"  TIER: {snap['tier']} — N<{FLOOR_MIN_TRADES} (have {n}; building sample)")
    else:
        print(f"  TIER: {snap['tier']}  (prev: {snap['prev_tier']})")

    print(f"\n  closed trades : {n}  (W {m['wins']} / L {m['losses']})")
    print(f"  win_rate      : {_fmt((m['win_rate'] or 0) * 100, pct=True) if m['win_rate'] is not None else '—':>8}"
          f"   floor ≥ {FLOOR_WIN_RATE * 100:.0f}%")
    print(f"  expectancy    : {_fmt(m['expectancy_r'], plus=True):>8} R  floor ≥ +{FLOOR_EXPECTANCY_R:.2f}R"
          f"  (n={m['r_sample']})")
    pf = m["profit_factor"]
    print(f"  profit_factor : {('∞' if pf is None and n else _fmt(pf)):>8}   floor ≥ {FLOOR_PROFIT_FACTOR}")
    print(f"  max_drawdown  : {_fmt(m['max_dd_pct'], pct=True):>8}   floor ≤ {FLOOR_MAX_DD_PCT:.0f}%  (of ${GENESIS_CAPITAL:,.0f})")
    print(f"  breaker       : {'CLEAN' if m['breaker_clean'] else f'{m['breaker_fire_count']} FIRE(S) {m['breaker_days']}'}")
    print(f"  gross_pnl     : ${m['gross_pnl']:,.2f}")

    if snap["failed_floors"]:
        print(f"\n  failed floors : {', '.join(snap['failed_floors'])}")
    else:
        print("\n  failed floors : none — all six floors met")

    print(f"\n  flag_raised   : {snap['flag_raised']}  "
          f"(Eligible/Promote raise a FLAG for the Admiral — NEVER auto-advance)")
    print(f"  can_trade_live: 0 (gate never advances the player) | persisted → ollie_machine_p4_status")
