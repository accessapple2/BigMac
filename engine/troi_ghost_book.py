"""HM-TROI-GHOST-BOOK-2026-07-04 — run-off counterfactual for the trimmed Troi book.

The 48 CSP legs closed in HM-TROI-GUARDRAILS-TRIM-2026-07-04 (36 SOXL/UPRO +
12 QQQ/SPY, exit_reason='manual_troi_guardrails_trim_2026-07-04') are tracked
here as if they had never been closed, run to their ORIGINAL expiry using the
same assignment/expiry logic the live wheel used. Purpose: answer "did the
trim help or hurt vs holding to expiry?" per leg, once each leg's original
expiry actually passes.

Read-mostly: seeding reads options_trades (never writes to it). All writes
go to the new `ghost_csp_book` table only. Append-only in spirit — rows are
UPDATEd in place to advance their mark/status (mirrors how options_trades
itself tracks status open->closed), never deleted.

Assignment modeling (documented simplification): a real CSP assignment
converts to a stock position and the wheel continues with covered calls
(engine.wheel_assignment_ledger). Simulating that full multi-leg continuation
for a ghost book is a much larger, more speculative undertaking than "run
this exact leg to its original expiry." Ghost assignment is instead marked
to market at the ORIGINAL expiry date: ghost_pnl = entry_credit -
(strike - close_price) * 100 * qty, i.e. the same intrinsic-value approach
check_wheel_assignments() uses for the OTM branch, applied symmetrically to
the ITM case. This is consistent with the -20%/-60% worst-case method used
elsewhere in this deep-dive, not a new methodology.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime
from pathlib import Path

# HM-TROI-GHOST-BOOK-2026-07-04: absolute path, not "data/trader.db" --
# this module is called from scripts/daily_report.py under cron, whose
# working directory is not guaranteed to be the project root (verified: a
# relative path here raised "unable to open database file" when invoked
# from a different cwd, exactly the cron scenario).
DB_PATH = str(Path(__file__).resolve().parent.parent / "data" / "trader.db")
SOURCE_EXIT_REASON = "manual_troi_guardrails_trim_2026-07-04"
LEVERAGE = {"SOXL": 3, "UPRO": 3, "TQQQ": 3, "TNA": 3, "QQQ": 1, "SPY": 1}


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, timeout=10)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ghost_csp_book (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            source_trade_id         INTEGER NOT NULL UNIQUE,
            symbol                  TEXT NOT NULL,
            strike                  REAL NOT NULL,
            qty                     INTEGER NOT NULL,
            expiration              TEXT NOT NULL,
            open_date               TEXT NOT NULL,
            entry_premium           REAL NOT NULL,
            entry_credit            REAL NOT NULL,
            trim_close_date         TEXT NOT NULL,
            trim_close_price        REAL NOT NULL,
            trim_pnl                REAL NOT NULL,
            status                  TEXT NOT NULL DEFAULT 'open',
            last_mark_date          TEXT,
            last_underlying_close   REAL,
            last_distance_to_strike_pct REAL,
            ghost_assigned_date     TEXT,
            ghost_assigned_price    REAL,
            ghost_pnl               REAL,
            created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()


def seed_ghost_book() -> dict:
    """One-time (idempotent) seed from the 48 trimmed legs. Read-only against
    options_trades. Returns {seeded, skipped_existing}."""
    conn = _conn()
    ensure_table(conn)
    try:
        rows = conn.execute(
            "SELECT id, symbol, legs_json, entry_credit_debit, expiration, "
            "entry_date, exit_date, exit_credit_debit, pnl "
            "FROM options_trades WHERE agent_id='options-sosnoff' AND exit_reason=?",
            (SOURCE_EXIT_REASON,),
        ).fetchall()

        existing = {r[0] for r in conn.execute(
            "SELECT source_trade_id FROM ghost_csp_book"
        ).fetchall()}

        seeded = 0
        for r in rows:
            if r["id"] in existing:
                continue
            legs = json.loads(r["legs_json"])
            leg = legs[0]
            strike = float(leg["strike"])
            qty = int(leg.get("qty", 1))
            entry_premium = float(leg["entry_price"])
            # exit_credit_debit is stored negative (a debit) per close_options_trade's
            # convention; the actual price paid to buy back is its absolute value.
            trim_close_price = abs(float(r["exit_credit_debit"])) / (100 * qty)
            conn.execute(
                "INSERT INTO ghost_csp_book "
                "(source_trade_id, symbol, strike, qty, expiration, open_date, "
                " entry_premium, entry_credit, trim_close_date, trim_close_price, trim_pnl, status) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')",
                (r["id"], r["symbol"], strike, qty, r["expiration"][:10],
                 r["entry_date"][:10], entry_premium, float(r["entry_credit_debit"]),
                 r["exit_date"][:10], trim_close_price, float(r["pnl"])),
            )
            seeded += 1
        conn.commit()
        return {"seeded": seeded, "skipped_existing": len(rows) - seeded, "total_source_legs": len(rows)}
    finally:
        conn.close()


def run_ghost_mark(as_of: date | None = None) -> dict:
    """Daily mark: update still-open ghost legs with underlying close +
    distance-to-strike; resolve legs at/past their original expiry to
    'assigned' or 'expired_otm'. Never raises -- a marking failure must not
    break the caller's cron job (Error Handling Posture)."""
    from engine.market_data import get_stock_price

    today = as_of or date.today()
    conn = _conn()
    ensure_table(conn)
    summary = {"marked": 0, "assigned": 0, "expired_otm": 0, "errors": 0}
    try:
        rows = conn.execute(
            "SELECT id, symbol, strike, qty, expiration, entry_credit "
            "FROM ghost_csp_book WHERE status = 'open'"
        ).fetchall()

        price_cache: dict[str, float | None] = {}
        for r in rows:
            sym = r["symbol"]
            if sym not in price_cache:
                try:
                    price_cache[sym] = get_stock_price(sym).get("price")
                except Exception:
                    price_cache[sym] = None
            close_price = price_cache[sym]
            if not close_price or close_price <= 0:
                summary["errors"] += 1
                continue

            strike = float(r["strike"])
            distance_pct = round((close_price - strike) / strike * 100, 2)
            exp_date = datetime.strptime(r["expiration"][:10], "%Y-%m-%d").date()

            if today < exp_date:
                # Still pre-expiry -- just update the mark.
                conn.execute(
                    "UPDATE ghost_csp_book SET last_mark_date=?, last_underlying_close=?, "
                    "last_distance_to_strike_pct=? WHERE id=?",
                    (today.isoformat(), close_price, distance_pct, r["id"]),
                )
                summary["marked"] += 1
                continue

            # At/past original expiry -- resolve.
            qty = int(r["qty"])
            entry_credit = float(r["entry_credit"])
            if close_price <= strike:
                # Ghost assignment -- mark-to-market intrinsic loss at expiry
                # (documented simplification: no simulated covered-call continuation).
                ghost_pnl = entry_credit - (strike - close_price) * 100 * qty
                conn.execute(
                    "UPDATE ghost_csp_book SET status='assigned', last_mark_date=?, "
                    "last_underlying_close=?, last_distance_to_strike_pct=?, "
                    "ghost_assigned_date=?, ghost_assigned_price=?, ghost_pnl=? WHERE id=?",
                    (today.isoformat(), close_price, distance_pct, today.isoformat(),
                     close_price, ghost_pnl, r["id"]),
                )
                summary["assigned"] += 1
            else:
                # Expired OTM -- full premium kept, same as the live book's own
                # expired_otm path.
                ghost_pnl = entry_credit
                conn.execute(
                    "UPDATE ghost_csp_book SET status='expired_otm', last_mark_date=?, "
                    "last_underlying_close=?, last_distance_to_strike_pct=?, ghost_pnl=? WHERE id=?",
                    (today.isoformat(), close_price, distance_pct, ghost_pnl, r["id"]),
                )
                summary["expired_otm"] += 1
        conn.commit()
    except Exception as e:
        summary["errors"] += 1
        try:
            from rich.console import Console
            Console().log(f"[yellow][ghost-book] run_ghost_mark error: {type(e).__name__}: {e!r}")
        except Exception:
            pass
    finally:
        conn.close()
    return summary


def ghost_worst_case() -> dict:
    """-20% sector shock / -60% on 3x-leveraged legs, applied to ghost legs
    still open (status='open') -- same method as the original deep-dive
    report, so the two numbers are directly comparable over time."""
    conn = _conn()
    ensure_table(conn)
    try:
        rows = conn.execute(
            "SELECT symbol, strike, qty, entry_credit, last_underlying_close "
            "FROM ghost_csp_book WHERE status = 'open'"
        ).fetchall()
    finally:
        conn.close()

    by_symbol: dict[str, float] = {}
    total = 0.0
    for r in rows:
        spot = r["last_underlying_close"]
        if not spot:
            continue
        lev = LEVERAGE.get(r["symbol"], 1)
        shock = min(0.95, 0.20 * lev)
        stressed = spot * (1 - shock)
        strike = float(r["strike"])
        if stressed < strike:
            qty = int(r["qty"])
            gross = (strike - stressed) * 100 * qty
            net = gross - float(r["entry_credit"])
            by_symbol[r["symbol"]] = by_symbol.get(r["symbol"], 0.0) + net
            total += net
    return {"per_symbol": by_symbol, "total": total, "n_open": len(rows)}


def ghost_vs_trim_summary() -> dict:
    """Cumulative ghost-vs-trim P&L delta. Resolved legs (assigned/expired_otm)
    contribute their final ghost_pnl vs trim_pnl. Still-open legs contribute
    an "unrealized" delta using the latest mark (ghost_pnl not yet final --
    approximated as entry_credit if OTM-so-far, else intrinsic-based, purely
    for a running "how's it looking" view, not a final number)."""
    conn = _conn()
    ensure_table(conn)
    try:
        resolved = conn.execute(
            "SELECT symbol, status, trim_pnl, ghost_pnl FROM ghost_csp_book "
            "WHERE status IN ('assigned', 'expired_otm')"
        ).fetchall()
        still_open = conn.execute(
            "SELECT symbol, strike, qty, entry_credit, last_underlying_close, trim_pnl "
            "FROM ghost_csp_book WHERE status = 'open'"
        ).fetchall()
    finally:
        conn.close()

    resolved_delta = sum((r["ghost_pnl"] or 0) - r["trim_pnl"] for r in resolved)
    resolved_trim_total = sum(r["trim_pnl"] for r in resolved)
    resolved_ghost_total = sum((r["ghost_pnl"] or 0) for r in resolved)

    unrealized_delta = 0.0
    for r in still_open:
        spot = r["last_underlying_close"]
        if not spot:
            continue
        intrinsic = max(0.0, float(r["strike"]) - spot)
        approx_ghost_pnl = float(r["entry_credit"]) - intrinsic * 100 * int(r["qty"])
        unrealized_delta += approx_ghost_pnl - r["trim_pnl"]

    return {
        "resolved_legs": len(resolved),
        "resolved_trim_total": round(resolved_trim_total, 2),
        "resolved_ghost_total": round(resolved_ghost_total, 2),
        "resolved_delta": round(resolved_delta, 2),
        "still_open_legs": len(still_open),
        "still_open_unrealized_delta_approx": round(unrealized_delta, 2),
    }
