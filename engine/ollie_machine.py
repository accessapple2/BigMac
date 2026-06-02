"""Ollie Machine — Step 7 P1 (signal-native universe rerun): convergence evaluator + pick log.

SIM-only, standalone. Proves the convergence doctrine in ISOLATION. NO executor,
NO player, NO positions, NO scheduling — this module ONLY writes the
`ollie_machine_picks` table and is run by hand. Wiring to paper_trader / a SIM
player comes in a later phase.

Doctrine: a candidate qualifies on >=2 of four signals, evaluated against the LIVE tables:
  1. squeeze pre-breakout      — squeeze_watch(kind='bbkc') pre_breakout_watch=1, recent
  2. squeeze-release + volume  — squeeze_watch.released_at recent AND release_volume_ratio
                                 >= _RELEASE_VOL_GATE (the BBKC scanner's own 2.0 gate)
  3. Minervini 8/8             — minervini_trend strict template_pass=1, latest row
  4. RS-rank >= 80             — rs_rank.rs_rank >= 80, latest row

CANDIDATE UNIVERSE (P1-rerun change): the UNION of recent symbols across the
signal tables themselves —
    squeeze_watch(kind='bbkc') ∪ minervini_trend ∪ rs_rank, within LOOKBACK_DAYS.
We screen on the names the signals actually cover, NOT on ollie-auto's
foreign-universe picks (the prior `ollie_super_trades` pool). This removes the
cross-universe coupling: a name can only converge if the signals themselves
saw it.

Consequence: there are no per-symbol entry/stop/tp brackets in this universe
(those were carried from ollie_super_trades). Bracket columns are written NULL;
the eventual SIM entry will source levels at entry time. `signal_score` is
substituted with the latest BBKC composite_score (the only per-symbol signal
score native to this universe) to preserve the count→rs→score rank tiebreak.

Of the >=2 qualifiers, rank by conviction (convergence_count desc, tiebreak
rs_rank then signal_score) and take the top N; log them to ollie_machine_picks.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta, timezone

DB_PATH = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))

# ── Doctrine thresholds (sourced from engine/bbkc_squeeze_scanner.py constants) ──
LOOKBACK_DAYS = 7             # universe recency window: signal rows within / scans this fresh
PREBREAKOUT_RECENT_DAYS = 2   # pre_breakout_watch must be from a recent bbkc scan
RELEASE_RECENT_DAYS = 3       # released_at must be recent
RELEASE_VOL_GATE = 2.0        # _RELEASE_VOL_GATE — min vol_ratio for a "with-volume" release
RS_FLOOR = 80                 # _COMPOSITE_RS_FLOOR — RS-rank threshold
MIN_CONVERGENCE = 2           # doctrine: need >= 2 of 4
TOP_N = 10                    # picks logged (and shown) per run

SIGNALS = ("pre_breakout", "release_vol", "minervini", "rs_rank")
SQUEEZE_SIGNALS = ("pre_breakout", "release_vol")
STRUCTURE_SIGNALS = ("minervini", "rs_rank")

# convergence-type buckets (mutually exclusive, exhaustive for count >= 2)
TYPE_SQUEEZE_STRUCTURE = "squeeze+structure"        # >=1 squeeze AND >=1 structure (cross-family)
TYPE_DOUBLE_STRUCTURE = "minervini+rs_double"       # both structure, no squeeze
TYPE_OTHER = "other_combos"                          # both squeeze, no structure (double-squeeze)


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def ensure_picks_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ollie_machine_picks (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            ts                TEXT NOT NULL,           -- evaluation run timestamp (UTC)
            symbol            TEXT NOT NULL,
            signals_fired     TEXT NOT NULL,           -- comma list of fired signal keys
            convergence_count INTEGER NOT NULL,
            conviction_rank   INTEGER NOT NULL,        -- 1..TOP_N
            entry_price       REAL,
            stop              REAL,
            tp1               REAL,
            tp2               REAL,
            tp3               REAL,
            rs_rank           INTEGER,
            signal_score      REAL,
            regime            TEXT,
            created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    # P1-rerun added a logical column: convergence type bucket. Add if absent.
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(ollie_machine_picks)").fetchall()}
    if "convergence_type" not in cols:
        conn.execute("ALTER TABLE ollie_machine_picks ADD COLUMN convergence_type TEXT")
    conn.commit()


def get_universe(conn: sqlite3.Connection) -> list[str]:
    """Candidate universe = UNION of recent symbols across the three signal tables.

    squeeze_watch(kind='bbkc') by scan_ts, minervini_trend + rs_rank by computed_at,
    each within LOOKBACK_DAYS. These are the names the signals actually cover.
    """
    win = f"-{LOOKBACK_DAYS} days"
    rows = conn.execute(
        """
        SELECT DISTINCT symbol FROM squeeze_watch
            WHERE kind='bbkc' AND scan_ts >= datetime('now', ?)
        UNION
        SELECT symbol FROM minervini_trend
            WHERE computed_at >= datetime('now', ?)
        UNION
        SELECT symbol FROM rs_rank
            WHERE computed_at >= datetime('now', ?)
        """,
        (win, win, win),
    ).fetchall()
    return sorted(r["symbol"] for r in rows)


def _latest_bbkc(conn: sqlite3.Connection) -> dict:
    """Latest bbkc squeeze_watch state per symbol (pre-breakout + release + composite)."""
    out = {}
    rows = conn.execute(
        "SELECT symbol, pre_breakout_watch, scan_ts, released_at, release_volume_ratio, composite_score "
        "FROM squeeze_watch WHERE kind='bbkc' AND id IN ("
        "  SELECT MAX(id) FROM squeeze_watch WHERE kind='bbkc' GROUP BY symbol)"
    ).fetchall()
    for r in rows:
        out[r["symbol"]] = dict(r)
    return out


def _latest_minervini(conn: sqlite3.Connection) -> dict:
    out = {}
    for r in conn.execute(
        "SELECT symbol, template_pass FROM minervini_trend WHERE rowid IN ("
        "  SELECT MAX(rowid) FROM minervini_trend GROUP BY symbol)"
    ).fetchall():
        out[r["symbol"]] = int(r["template_pass"] or 0)
    return out


def _latest_rs(conn: sqlite3.Connection) -> dict:
    out = {}
    for r in conn.execute("SELECT symbol, rs_rank FROM rs_rank").fetchall():  # PK symbol = 1/symbol
        out[r["symbol"]] = int(r["rs_rank"] or 0)
    return out


def _convergence_type(fired: dict) -> str:
    """Classify a qualifier (count>=2) into one of the three exhaustive buckets."""
    s = sum(1 for k in SQUEEZE_SIGNALS if fired[k])
    t = sum(1 for k in STRUCTURE_SIGNALS if fired[k])
    if s >= 1 and t >= 1:
        return TYPE_SQUEEZE_STRUCTURE
    if s == 0 and t == 2:
        return TYPE_DOUBLE_STRUCTURE
    return TYPE_OTHER  # s == 2 and t == 0 (double-squeeze)


def evaluate(conn: sqlite3.Connection, apply_filter: bool = True) -> dict:
    """Run the full evaluation. With apply_filter (P2b default), the signal-native
    universe is screened to a TRADEABLE universe BEFORE the >=2-of-4 vote — frozen
    deal-grinders, leveraged/inverse ETFs, penny + illiquid names are removed so
    convergence can only fire on real momentum candidates. Returns a summary dict
    (no write). (bad-mark sanity is applied later in run() — it needs per-symbol calls.)"""
    universe = get_universe(conn)
    universe_pre = len(universe)

    dropped: dict = {}
    window: dict = {}
    if apply_filter:
        from engine import ollie_machine_universe as omu  # lazy — pulls requests/polygon
        window = omu.fetch_market_window(set(universe))
        meta = omu.load_meta(conn)
        universe, dropped = omu.classify_universe(universe, window, meta)

    bbkc = _latest_bbkc(conn)
    minv = _latest_minervini(conn)
    rs = _latest_rs(conn)

    now = datetime.now(timezone.utc)

    def _recent(ts: str | None, days: int) -> bool:
        if not ts:
            return False
        norm = str(ts).replace("T", " ")[:19]                      # 'YYYY-MM-DD HH:MM:SS'
        cutoff = (now - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        return norm >= cutoff                                       # ISO8601 sorts lexically

    scored = []
    for sym in universe:
        b = bbkc.get(sym, {})
        fired = {}
        fired["pre_breakout"] = bool(b.get("pre_breakout_watch") == 1 and _recent(b.get("scan_ts"), PREBREAKOUT_RECENT_DAYS))
        fired["release_vol"] = bool(b.get("released_at") and _recent(b.get("released_at"), RELEASE_RECENT_DAYS)
                                    and float(b.get("release_volume_ratio") or 0) >= RELEASE_VOL_GATE)
        fired["minervini"] = bool(minv.get(sym, 0) == 1)
        rsv = rs.get(sym, 0)
        fired["rs_rank"] = bool(rsv >= RS_FLOOR)
        count = sum(1 for k in SIGNALS if fired[k])
        score = b.get("composite_score")                           # native per-symbol signal score (tiebreak)
        scored.append({
            "symbol": sym, "fired": fired, "convergence_count": count,
            "rs_rank_val": rsv, "signal_score": score,
            "ctype": _convergence_type(fired) if count >= MIN_CONVERGENCE else None,
        })

    qualifiers = [s for s in scored if s["convergence_count"] >= MIN_CONVERGENCE]
    qualifiers.sort(key=lambda s: (s["convergence_count"], s["rs_rank_val"], s.get("signal_score") or 0), reverse=True)

    type_split = {TYPE_SQUEEZE_STRUCTURE: 0, TYPE_DOUBLE_STRUCTURE: 0, TYPE_OTHER: 0}
    for q in qualifiers:
        type_split[q["ctype"]] += 1

    return {
        "universe_pre": universe_pre,
        "universe_post": len(universe),
        "dropped": dropped,
        "window": window,
        "candidates": scored,
        "qualifiers": qualifiers,
        "type_split": type_split,
    }


MARK_SANITY_CHECK_N = 25   # bounded: bad-mark check walks at most this many ranked qualifiers


def _apply_mark_sanity(res: dict) -> tuple[list, list]:
    """Bounded bad-mark filter: walk ranked qualifiers (top MARK_SANITY_CHECK_N), drop any whose
    trade-levels mark deviates > MARK_DEV_PCT from last close. Returns (clean_ranked, bad_marks)."""
    from engine import ollie_machine_universe as omu
    window = res.get("window") or {}
    clean, bad = [], []
    for q in res["qualifiers"][:MARK_SANITY_CHECK_N]:
        last_close = (window.get(q["symbol"]) or {}).get("last_close")
        is_bad, mark, dev = omu.mark_deviation(q["symbol"], last_close)
        if is_bad:
            bad.append({**q, "mark": mark, "last_close": last_close, "dev_pct": dev})
        else:
            clean.append(q)
    # qualifiers beyond the bounded check window are assumed clean (kept after the checked block)
    clean.extend(res["qualifiers"][MARK_SANITY_CHECK_N:])
    return clean, bad


def run(write: bool = True, apply_filter: bool = True) -> dict:
    conn = _conn()
    try:
        ensure_picks_table(conn)
        res = evaluate(conn, apply_filter=apply_filter)
        clean, bad = _apply_mark_sanity(res) if apply_filter else (res["qualifiers"], [])
        top = clean[:TOP_N]
        res["bad_marks"], res["clean_qualifiers"], res["top"] = bad, clean, top
        ts = datetime.now(timezone.utc).isoformat()
        if write:
            conn.execute("DELETE FROM ollie_machine_picks")  # clear prior run (idempotent rerun)
            for rank, s in enumerate(top, start=1):
                fired_keys = ",".join(k for k in SIGNALS if s["fired"][k])
                conn.execute(
                    "INSERT INTO ollie_machine_picks "
                    "(ts, symbol, signals_fired, convergence_count, conviction_rank, "
                    " entry_price, stop, tp1, tp2, tp3, rs_rank, signal_score, regime, convergence_type) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (ts, s["symbol"], fired_keys, s["convergence_count"], rank,
                     None, None, None, None, None,
                     s["rs_rank_val"], s.get("signal_score"), None, s["ctype"]),
                )
            conn.commit()
        res["ts"] = ts
        return res
    finally:
        conn.close()


if __name__ == "__main__":
    r = run(write=True, apply_filter=True)
    quals, top, split = r["clean_qualifiers"], r["top"], r["type_split"]
    dropped, bad = r["dropped"], r.get("bad_marks", [])

    print(f"\n=== Ollie Machine P2b — tradeable-universe filter (pre-vote) run {r['ts']} ===")
    print(f"candidate universe: {r['universe_pre']} → {r['universe_post']} after filter "
          f"(squeeze_watch ∪ minervini_trend ∪ rs_rank, last {LOOKBACK_DAYS}d)")

    print(f"\n--- dropped per filter reason ({r['universe_pre'] - r['universe_post']} pre-vote) ---")
    labels = {"frozen": f"frozen (30d range% < {3.0})", "leveraged": "leveraged/inverse ETF",
              "penny": "penny (close < $5)", "illiquid": "illiquid (med $-vol < $2M)",
              "no_data": "no recent market data"}
    for reason in ("frozen", "leveraged", "penny", "illiquid", "no_data"):
        print(f"  {labels[reason]:32}: {len(dropped.get(reason, []))}")
    print(f"  bad_mark (trade-levels vs close, top {MARK_SANITY_CHECK_N} quals): {len(bad)}")
    if bad:
        for b in bad:
            print(f"      drop {b['symbol']}: mark={b['mark']} vs close={b['last_close']} (dev {b['dev_pct']:.0f}%)")

    print(f"\n--- pass >= {MIN_CONVERGENCE}-of-4 on clean universe: {len(quals)} ---")
    print(f"  squeeze+structure (squeeze ∧ structure)     : {split[TYPE_SQUEEZE_STRUCTURE]}")
    print(f"  Minervini+RS double-structure (struct only) : {split[TYPE_DOUBLE_STRUCTURE]}")
    print(f"  other combos (double-squeeze, no structure) : {split[TYPE_OTHER]}")

    print(f"\n--- TOP {TOP_N} (logged to ollie_machine_picks) ---")
    print(f"  {'#':>2} {'SYM':6} [PB  REL MIN RS ] cnt  rs  score  type")
    for rank, s in enumerate(top, start=1):
        cells = "".join(("✓" if s["fired"][k] else "·").ljust(4 if i < 3 else 3) for i, k in enumerate(SIGNALS))
        sc = s.get("signal_score")
        sc_s = f"{sc:5.1f}" if sc is not None else "  —  "
        print(f"  {rank:>2} {s['symbol']:6} [{cells}] {s['convergence_count']:>3} {s['rs_rank_val']:>3} {sc_s}  {s['ctype']}")

    print("\n--- target-name exclusion confirmation ---")
    drop_index = {sym: reason for reason, syms in dropped.items() for sym in syms}
    bad_syms = {b["symbol"] for b in bad}
    top_syms = {s["symbol"] for s in top}
    for sym in ("CNTA", "SILA", "MULL", "MUU", "AMDL"):
        if sym in drop_index:
            verdict = f"GONE (dropped: {drop_index[sym]})"
        elif sym in bad_syms:
            verdict = "GONE (dropped: bad_mark)"
        elif sym in top_syms:
            verdict = "*** STILL IN TOP — FAIL ***"
        else:
            verdict = "absent from picks (not a qualifier / not in universe)"
        print(f"  {sym:6}: {verdict}")
