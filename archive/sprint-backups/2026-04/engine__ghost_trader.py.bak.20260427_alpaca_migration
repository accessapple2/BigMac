"""
Ghost Trader — Prediction Scoring System

Tracks all signals from signal-center and scores them against actual price
movement.  Leverages signal_outcomes (already maintained by signal-center) for
signals that have been resolved, and fetches live prices for still-open ones.

DB: data/ghost_trades.db
"""
from __future__ import annotations

import re
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [GHOST] %(message)s")
log = logging.getLogger("ghost_trader")

ROOT       = Path(__file__).resolve().parent.parent
DB_PATH    = ROOT / "data" / "ghost_trades.db"
SIGNAL_DB  = ROOT / "signal-center" / "signals.db"


# ── helpers ───────────────────────────────────────────────────────────────────

def _sc():
    """Open signal-center DB (read-only snapshot)."""
    c = sqlite3.connect(str(SIGNAL_DB), timeout=10)
    c.row_factory = sqlite3.Row
    return c


def _ghost():
    """Open ghost trades DB."""
    c = sqlite3.connect(str(DB_PATH), timeout=15)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=15000")
    return c


def _extract_pattern(reasoning: str | None) -> str:
    """Pull pattern tag like [BREAKOUT] from reasoning text."""
    if not reasoning:
        return "SIGNAL"
    m = re.match(r"\[([A-Z0-9_\s]+)\]", reasoning.strip())
    return m.group(1).strip() if m else "SIGNAL"


# ── schema ────────────────────────────────────────────────────────────────────

def init_db():
    conn = _ghost()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS ghost_trades (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id    INTEGER UNIQUE,
            symbol       TEXT    NOT NULL,
            agent        TEXT    NOT NULL,
            action       TEXT    NOT NULL,
            entry_price  REAL    NOT NULL,
            stop_price   REAL,
            target_price REAL,
            confidence   REAL,
            pattern      TEXT,
            reasoning    TEXT,
            signal_time  TEXT    NOT NULL,

            -- Outcome
            status       TEXT    DEFAULT 'OPEN',
            exit_price   REAL,
            exit_time    TEXT,
            pnl_pct      REAL,
            hit_target   INTEGER DEFAULT 0,
            hit_stop     INTEGER DEFAULT 0,

            -- Price snapshots
            price_1h     REAL,
            price_4h     REAL,
            price_1d     REAL,
            price_3d     REAL,
            max_gain_pct REAL,
            max_loss_pct REAL,

            created_at   TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_ghost_status ON ghost_trades(status);
        CREATE INDEX IF NOT EXISTS idx_ghost_agent  ON ghost_trades(agent);
        CREATE INDEX IF NOT EXISTS idx_ghost_symbol ON ghost_trades(symbol);
    """)
    conn.commit()
    conn.close()
    log.info("Ghost DB ready at %s", DB_PATH)


# ── capture ───────────────────────────────────────────────────────────────────

def capture_new_signals() -> int:
    """Pull new BUY signals from signal-center into ghost_trades."""
    if not SIGNAL_DB.exists():
        log.warning("Signal DB not found: %s", SIGNAL_DB)
        return 0

    ghost = _ghost()
    sc    = _sc()

    existing: set[int] = {
        r[0] for r in ghost.execute(
            "SELECT signal_id FROM ghost_trades WHERE signal_id IS NOT NULL"
        ).fetchall()
    }

    cutoff = (datetime.now() - timedelta(days=7)).isoformat()
    rows = sc.execute(
        """SELECT id, symbol, agent_name, action, entry_price, stop_loss,
                  take_profit, confidence, reasoning, created_at
             FROM trade_signals
            WHERE action = 'BUY'
              AND created_at > ?
              AND confidence >= 70
            ORDER BY created_at DESC""",
        (cutoff,),
    ).fetchall()
    sc.close()

    captured = 0
    now = datetime.now()
    for r in rows:
        sig_id = r["id"]
        if sig_id in existing:
            continue

        entry = r["entry_price"] or 0
        if entry <= 0:
            continue

        conf      = float(r["confidence"] or 75)
        stop_pct  = 0.03 if conf >= 90 else 0.05 if conf >= 85 else 0.07
        stop      = r["stop_loss"]   or round(entry * (1 - stop_pct), 4)
        target    = r["take_profit"] or round(entry * (1 + stop_pct * 2), 4)
        pattern   = _extract_pattern(r["reasoning"])

        try:
            ghost.execute(
                """INSERT INTO ghost_trades
                   (signal_id, symbol, agent, action, entry_price, stop_price,
                    target_price, confidence, pattern, reasoning, signal_time)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (sig_id, r["symbol"], r["agent_name"], r["action"],
                 entry, stop, target, conf, pattern, r["reasoning"], r["created_at"]),
            )
            captured += 1
        except sqlite3.IntegrityError:
            pass

    ghost.commit()
    ghost.close()
    if captured:
        log.info("Captured %d new signals", captured)
    return captured


# ── outcome scoring ───────────────────────────────────────────────────────────

def check_outcomes() -> int:
    """Score open ghost trades.

    Priority: use signal_outcomes from signal-center (already computed).
    Fallback: fetch live prices via yfinance for signals not yet in outcomes.
    """
    if not SIGNAL_DB.exists():
        return 0

    ghost = _ghost()
    sc    = _sc()

    open_rows = ghost.execute(
        """SELECT id, signal_id, symbol, entry_price, stop_price,
                  target_price, signal_time, confidence
             FROM ghost_trades WHERE status = 'OPEN'"""
    ).fetchall()

    if not open_rows:
        ghost.close()
        sc.close()
        return 0

    # Pull existing outcomes from signal-center
    sig_ids = [r["signal_id"] for r in open_rows if r["signal_id"]]
    if sig_ids:
        placeholders = ",".join("?" * len(sig_ids))
        outcomes: dict[int, sqlite3.Row] = {
            r["signal_id"]: r
            for r in sc.execute(
                f"""SELECT signal_id, would_hit_tp, would_hit_sl,
                           theoretical_pnl, tracked_high, tracked_low,
                           tracked_current
                      FROM signal_outcomes
                     WHERE signal_id IN ({placeholders})""",
                sig_ids,
            ).fetchall()
        }
    else:
        outcomes = {}
    sc.close()

    now = datetime.now()
    scored = 0

    for row in open_rows:
        trade_id   = row["id"]
        signal_id  = row["signal_id"]
        symbol     = row["symbol"]
        entry      = row["entry_price"]
        stop       = row["stop_price"]
        target     = row["target_price"]
        conf       = row["confidence"]

        try:
            sig_dt = datetime.fromisoformat(
                row["signal_time"].replace("Z", "").split("+")[0]
            )
        except Exception:
            sig_dt = now - timedelta(hours=1)

        age_hours = (now - sig_dt).total_seconds() / 3600

        # ── Use signal_outcomes if available ──────────────────────────────
        if signal_id in outcomes:
            o = outcomes[signal_id]
            high_seen = o["tracked_high"] or entry
            low_seen  = o["tracked_low"]  or entry
            current   = o["tracked_current"] or entry

            max_gain = ((high_seen - entry) / entry) * 100
            max_loss = ((low_seen  - entry) / entry) * 100

            if o["would_hit_tp"]:
                status     = "WIN"
                exit_price = target
                hit_target = 1
                hit_stop   = 0
                pnl_pct    = ((target - entry) / entry) * 100
            elif o["would_hit_sl"]:
                status     = "LOSS"
                exit_price = stop
                hit_target = 0
                hit_stop   = 1
                pnl_pct    = ((stop - entry) / entry) * 100
            elif age_hours >= 72:
                status     = "EXPIRED"
                exit_price = current
                hit_target = 0
                hit_stop   = 0
                pnl_pct    = o["theoretical_pnl"] or ((current - entry) / entry) * 100
            else:
                continue  # still open

        # ── Fallback: live price fetch ────────────────────────────────────
        else:
            if age_hours < 1:
                continue  # too new to score
            try:
                import yfinance as yf
                hist = yf.Ticker(symbol).history(period="5d")
                if hist.empty:
                    continue
                current  = float(hist["Close"].iloc[-1])
                high_5d  = float(hist["High"].max())
                low_5d   = float(hist["Low"].min())
            except Exception as e:
                log.warning("Price fetch failed for %s: %s", symbol, e)
                continue

            max_gain = ((high_5d - entry) / entry) * 100
            max_loss = ((low_5d  - entry) / entry) * 100

            if high_5d >= target:
                status = "WIN";    exit_price = target; hit_target = 1; hit_stop = 0
                pnl_pct = ((target - entry) / entry) * 100
            elif low_5d <= stop:
                status = "LOSS";   exit_price = stop;   hit_target = 0; hit_stop = 1
                pnl_pct = ((stop - entry) / entry) * 100
            elif age_hours >= 72:
                status = "EXPIRED"; exit_price = current; hit_target = 0; hit_stop = 0
                pnl_pct = ((current - entry) / entry) * 100
            else:
                continue

        ghost.execute(
            """UPDATE ghost_trades
                  SET status=?, exit_price=?, pnl_pct=?,
                      hit_target=?, hit_stop=?,
                      max_gain_pct=?, max_loss_pct=?,
                      exit_time=?
                WHERE id=?""",
            (status, exit_price, pnl_pct, hit_target, hit_stop,
             max_gain, max_loss, now.isoformat(), trade_id),
        )
        scored += 1
        log.info("👻 %-6s → %-8s %+.1f%%", symbol, status, pnl_pct)

    ghost.commit()
    ghost.close()
    if scored:
        log.info("Scored %d trades", scored)
    return scored


# ── scorecard ─────────────────────────────────────────────────────────────────

def get_scorecard(days: int = 30) -> list[dict]:
    conn   = _ghost()
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()

    rows = conn.execute(
        """SELECT agent,
                  COUNT(*) as total,
                  SUM(CASE WHEN status='WIN'     THEN 1 ELSE 0 END) as wins,
                  SUM(CASE WHEN status='LOSS'    THEN 1 ELSE 0 END) as losses,
                  SUM(CASE WHEN status='EXPIRED' THEN 1 ELSE 0 END) as expired,
                  SUM(CASE WHEN status='OPEN'    THEN 1 ELSE 0 END) as open_count,
                  AVG(CASE WHEN status='WIN'  THEN pnl_pct END) as avg_win,
                  AVG(CASE WHEN status='LOSS' THEN pnl_pct END) as avg_loss,
                  AVG(CASE WHEN status IN ('WIN','LOSS','EXPIRED') THEN pnl_pct END) as avg_pnl,
                  AVG(confidence) as avg_conf
             FROM ghost_trades
            WHERE signal_time > ?
            GROUP BY agent
            ORDER BY wins DESC""",
        (cutoff,),
    ).fetchall()
    conn.close()

    scorecard = []
    for r in rows:
        wins   = r["wins"]   or 0
        losses = r["losses"] or 0
        closed = wins + losses + (r["expired"] or 0)
        wr     = (wins / closed * 100) if closed > 0 else 0

        avg_win  = r["avg_win"]  or 0
        avg_loss = r["avg_loss"] or 0
        if losses > 0 and avg_loss != 0 and avg_win != 0:
            pf = min(abs((wins * avg_win) / (losses * avg_loss)), 99.9)
        else:
            pf = 99.9 if wins > 0 else 0.0

        scorecard.append({
            "agent":         r["agent"],
            "total":         r["total"],
            "open":          r["open_count"] or 0,
            "closed":        closed,
            "wins":          wins,
            "losses":        losses,
            "expired":       r["expired"] or 0,
            "win_rate":      round(wr, 1),
            "avg_win":       round(avg_win, 2),
            "avg_loss":      round(avg_loss, 2),
            "avg_pnl":       round(r["avg_pnl"] or 0, 2),
            "profit_factor": round(pf, 2),
            "avg_conf":      round(r["avg_conf"] or 0, 1),
        })

    return sorted(scorecard, key=lambda x: x["win_rate"], reverse=True)


def get_recent_trades(
    limit: int = 20,
    agent: str | None = None,
    status: str | None = None,
) -> list[dict]:
    conn   = _ghost()
    query  = "SELECT * FROM ghost_trades WHERE 1=1"
    params: list = []
    if agent:
        query  += " AND agent = ?"
        params.append(agent)
    if status:
        query  += " AND status = ?"
        params.append(status)
    query  += " ORDER BY signal_time DESC LIMIT ?"
    params.append(limit)

    cur  = conn.execute(query, params)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    conn.close()
    return [dict(zip(cols, row)) for row in rows]


# ── display ───────────────────────────────────────────────────────────────────

def print_scorecard():
    cards = get_scorecard()
    if not cards:
        print("\n👻 Ghost Trader: no closed trades yet — run again after signals resolve.")
        return
    print("\n" + "=" * 72)
    print("👻 GHOST TRADER SCORECARD — Last 30 Days")
    print("=" * 72)
    print(f"{'Agent':<22} {'W/L/E':>9} {'Win%':>7} {'AvgWin':>9} {'AvgLoss':>9} {'PF':>6} {'Conf':>6}")
    print("-" * 72)
    for s in cards:
        wle = f"{s['wins']}/{s['losses']}/{s['expired']}"
        print(
            f"{s['agent']:<22} {wle:>9} {s['win_rate']:>6.1f}%"
            f" {s['avg_win']:>+8.1f}% {s['avg_loss']:>+8.1f}%"
            f" {s['profit_factor']:>5.1f}x {s['avg_conf']:>5.0f}%"
        )
    print("=" * 72)


# ── daemon ────────────────────────────────────────────────────────────────────

def run_daemon(interval_minutes: int = 30):
    log.info("👻 Ghost Trader daemon starting (interval=%dm)", interval_minutes)
    init_db()
    while True:
        try:
            capture_new_signals()
            check_outcomes()
            print_scorecard()
        except Exception as e:
            log.error("Daemon error: %s", e, exc_info=True)
        log.info("Sleeping %d minutes…", interval_minutes)
        time.sleep(interval_minutes * 60)


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    if cmd == "daemon":
        run_daemon(interval_minutes=30)
    else:
        init_db()
        n = capture_new_signals()
        s = check_outcomes()
        print(f"Captured {n} new signals, scored {s} outcomes.")
        print_scorecard()
