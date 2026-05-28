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
import threading
import time
import urllib.request
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

from options_engine import (  # noqa: E402  — all read-only compute/build; NONE submit orders
    calc_ivr, get_spot, MIN_IVR, find_target_expiration, _directional_lean,
    build_bull_put_spread, build_bear_call_spread, build_iron_condor, build_csp,
    bs_price, get_dte, EXIT_DTE, LOSS_LIMIT_MULT,
)

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


# ── Kill switch (Loop D) — O-Tasty's OWN switch, separate from the fleet kill ─
# switch so the Captain can halt O-Tasty independently. Captain-only control;
# NO auto-flipping in WAVE 8. Append-only audit log; current state = latest row.
# Gates internal loop behavior ONLY — never touches the broker.
_KS_STATES = ("ARMED", "TRIPPED", "HARD_HALT")


def _ensure_killswitch_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS swingdesk_killswitch (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state TEXT NOT NULL,
            reason TEXT,
            changed_by TEXT,
            changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()


def get_killswitch_state(conn: sqlite3.Connection | None = None) -> str:
    """Current O-Tasty kill-switch state (latest row). Seeds ARMED on first read."""
    own = conn is None
    if own:
        conn = sqlite3.connect(DB_PATH)
    _ensure_killswitch_schema(conn)
    row = conn.execute("SELECT state FROM swingdesk_killswitch ORDER BY id DESC LIMIT 1").fetchone()
    if row is None:
        conn.execute("INSERT INTO swingdesk_killswitch (state, reason, changed_by) "
                     "VALUES ('ARMED','default seed','system')")
        conn.commit()
        state = "ARMED"
    else:
        state = row[0]
    if own:
        conn.close()
    return state


def set_killswitch(state: str, reason: str = "", changed_by: str = "captain") -> dict:
    """Captain-only kill-switch control. Appends a state-change row (audit trail).
    Gates internal loop behavior only — NO broker calls."""
    state = state.upper()
    if state not in _KS_STATES:
        raise ValueError(f"invalid state {state!r}; must be one of {_KS_STATES}")
    conn = sqlite3.connect(DB_PATH)
    _ensure_killswitch_schema(conn)
    conn.execute("INSERT INTO swingdesk_killswitch (state, reason, changed_by) VALUES (?,?,?)",
                 (state, reason, changed_by))
    conn.commit()
    conn.close()
    return {"state": state, "reason": reason, "changed_by": changed_by}


def killswitch_log(limit: int = 20) -> list:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    _ensure_killswitch_schema(conn)
    rows = [dict(r) for r in conn.execute(
        "SELECT state, reason, changed_by, changed_at FROM swingdesk_killswitch "
        "ORDER BY id DESC LIMIT ?", (limit,))]
    conn.close()
    return rows


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

    state = get_killswitch_state(conn)
    if state == "HARD_HALT":   # halt everything, including the read-only scan
        conn.close()
        return {"loop": "A", "killswitch": state, "halted": True, "written": 0, "orders_submitted": 0}
    # ARMED + TRIPPED both scan (data collection is safe under TRIPPED)

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


# ── Loop B params (HM-O-TASTY-DOCTRINE) ──────────────────────────────────────
SHADOW_PORTFOLIO = 52340.0   # shadow-book notional (matches options_engine default)
BPR_PER_TRADE    = 0.03      # 3% buying-power reduction per trade
SOFT_CAP         = 0.35      # refuse NEW entries when total book BPR > 35%
HARD_CAP         = 0.50      # refuse entirely when total book BPR > 50%
MAX_POSITIONS    = 20        # max concurrent shadow positions
MAX_PER_SECTOR   = 3         # max concurrent per sector

# Directional auto-map (WAVE 3 doctrine). CSP is NOT auto-selected here — it's the
# discretionary "willing-to-own" trade and was excluded from the directional default
# in WAVE 3 (no wrong-direction CSP). The CSP special-case MATH (cash-secured BPR,
# defined_risk=0, audit tag) IS implemented in _size_at_3pct + the builder import,
# so CSP is handled correctly the moment a willing-to-own signal selects it.
_LEAN_STRUCT = {"bullish": "bull_put_spread", "bearish": "bear_call_spread", "neutral": "iron_condor"}
_BUILDER = {
    "bull_put_spread":  build_bull_put_spread,
    "bear_call_spread": build_bear_call_spread,
    "iron_condor":      build_iron_condor,
    "csp":              build_csp,
}


def _ensure_shadow_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS swingdesk_shadow_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            would_have_submitted_at TIMESTAMP,
            scan_date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            sector TEXT,
            structure TEXT,
            directional_lean TEXT,
            defined_risk INTEGER,
            legs TEXT,
            credit REAL,
            max_loss REAL,
            breakevens TEXT,
            contracts INTEGER,
            bpr REAL,
            bpr_pct REAL,
            pop REAL,
            dte INTEGER,
            expiration TEXT,
            spot REAL,
            ivr REAL,
            iv REAL,
            status TEXT NOT NULL DEFAULT 'shadow_open',
            audit_tag TEXT,
            refused_reason TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    # idempotent column adds (refused_reason + Loop C exit fields)
    for _col, _typ in [("refused_reason", "TEXT"), ("exit_reason", "TEXT"),
                       ("would_have_exit_at", "TIMESTAMP"), ("exit_value", "REAL"),
                       ("exit_pnl", "REAL")]:
        try:
            conn.execute(f"ALTER TABLE swingdesk_shadow_trades ADD COLUMN {_col} {_typ}")
        except sqlite3.OperationalError:
            pass
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_shadow_trades ON swingdesk_shadow_trades(scan_date, symbol, status)"
    )
    conn.commit()


def _size_at_3pct(structure: str, build: dict):
    """Contracts at 3% BPR. Defined-risk spreads: BPR = max_loss×100. CSP
    (special-case, non-defined-risk): BPR = cash_required (cash-secured).
    Returns (contracts, bpr_total, bpr_per_contract, defined_risk)."""
    if structure == "csp":
        per = float(build.get("cash_required") or 0.0)
        defined = 0
    else:
        per = float(build.get("max_loss") or 0.0) * 100.0
        defined = 1
    if per <= 0:
        return 0, 0.0, per, defined
    contracts = int((SHADOW_PORTFOLIO * BPR_PER_TRADE) / per)
    return contracts, round(per * contracts, 2), per, defined


def run_loop_b() -> dict:
    """LOOP B — structure + entry (SHADOW ONLY). For each Loop-A gate-passer:
    pick directional structure, size at 3% BPR, enforce 35%/50% caps + 20-max /
    3-per-sector limits, and write a would-have-entered row to
    swingdesk_shadow_trades. NO order submission anywhere in this path.
    """
    import json
    from collections import Counter

    scan_date = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    _ensure_shadow_schema(conn)
    conn.row_factory = sqlite3.Row

    state = get_killswitch_state(conn)
    if state == "HARD_HALT":   # halt everything
        conn.close()
        return {"loop": "B", "killswitch": state, "halted": True, "entered": 0, "orders_submitted": 0}
    # TRIPPED → still evaluate candidates but refuse every new entry (below)

    passers = conn.execute(
        "SELECT symbol, sector, ivr, iv_current, spot FROM swingdesk_ivr "
        "WHERE scan_date=? AND gate_pass=1 ORDER BY ivr DESC", (scan_date,)).fetchall()

    openrows = conn.execute(
        "SELECT symbol, sector, bpr FROM swingdesk_shadow_trades WHERE status='shadow_open'").fetchall()
    open_count = len(openrows)
    open_bpr = sum((r["bpr"] or 0) for r in openrows)
    sector_ct = Counter(r["sector"] for r in openrows)
    open_syms = {r["symbol"] for r in openrows}

    entered, refused = [], []
    for p in passers:
        sym, sector, ivr, iv = p["symbol"], p["sector"], p["ivr"], p["iv_current"]
        # evaluate (single persist point at the end — every candidate gets a row)
        lean = structure = exp = None
        build, bes = {}, []
        contracts, bpr, defined, audit_tag = 0, 0.0, None, None
        status, reason = "refused", None

        if state == "TRIPPED":
            reason = "killswitch_tripped"   # kill switch halts NEW entries (exits still run in Loop C)
        elif sym in open_syms:
            reason = "already_open"
        else:
            lean = _directional_lean(sym)
            structure = _LEAN_STRUCT.get(lean)
            if not structure:
                reason = f"no_structure:{lean}"
            else:
                exp = find_target_expiration(sym)
                if not exp:
                    reason = "no_expiration"
                else:
                    spot = get_spot(sym) or p["spot"]
                    build = _BUILDER[structure](sym, spot, iv, exp) or {}
                    bes = ([build.get("breakeven_low"), build.get("breakeven_high")]
                           if structure == "iron_condor" else [build.get("breakeven")])
                    audit_tag = "csp_cash_secured" if structure == "csp" else "defined_risk_spread"
                    if not build.get("viable") or (build.get("max_loss") or 0) <= 0:
                        reason = f"{structure}_not_viable"
                    else:
                        contracts, bpr, per, defined = _size_at_3pct(structure, build)
                        proj = (open_bpr + bpr) / SHADOW_PORTFOLIO
                        if contracts < 1:
                            reason = f"{structure}_per_contract_bpr>{int(BPR_PER_TRADE*100)}pct"
                        elif proj > HARD_CAP:
                            reason = f"hard_cap_50pct({proj:.0%})"
                        elif proj > SOFT_CAP:
                            reason = f"soft_cap_35pct({proj:.0%})"
                        elif open_count >= MAX_POSITIONS:
                            reason = "max_positions_20"
                        elif sector_ct[sector] >= MAX_PER_SECTOR:
                            reason = f"sector_limit_3:{sector}"
                        else:
                            status, reason = "shadow_open", None

        conn.execute(
            "INSERT INTO swingdesk_shadow_trades "
            "(would_have_submitted_at, scan_date, symbol, sector, structure, directional_lean, "
            " defined_risk, legs, credit, max_loss, breakevens, contracts, bpr, bpr_pct, pop, dte, "
            " expiration, spot, ivr, iv, status, audit_tag, refused_reason) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (datetime.now().isoformat(timespec="seconds") if status == "shadow_open" else None,
             scan_date, sym, sector, structure, lean, defined,
             json.dumps(build.get("legs", {})), build.get("credit"), build.get("max_loss"),
             json.dumps(bes) if build else None, contracts, bpr,
             round(bpr / SHADOW_PORTFOLIO, 4), build.get("pop"), build.get("dte"),
             exp, build.get("spot") or p["spot"], ivr, iv, status, audit_tag, reason),
        )
        if status == "shadow_open":
            open_count += 1; open_bpr += bpr; sector_ct[sector] += 1
            entered.append((sym, structure, contracts, bpr))
        else:
            refused.append((sym, reason))

    conn.commit()
    conn.close()
    return {
        "loop": "B",
        "scan_date": scan_date,
        "gate_passers": len(passers),
        "entered": len(entered),
        "entered_detail": entered,
        "refused": refused,
        "book_bpr_pct": round(open_bpr / SHADOW_PORTFOLIO, 4),
        "open_positions": open_count,
        "orders_submitted": 0,  # invariant: Loop B never submits
    }


# ── Loop C: position manager (SHADOW) ────────────────────────────────────────
_R = 0.045


def _current_spread_value(structure: str, legs: dict, spot: float, dte: int, sigma: float) -> float:
    """Re-price the structure's current debit-to-close at current spot + remaining
    DTE (entry IV as sigma proxy). Read-only Black-Scholes; no orders."""
    T = max(dte, 0) / 365.0

    def p(strike, typ):
        return bs_price(spot, float(strike), T, _R, sigma, typ)

    if structure == "bull_put_spread":
        return round(p(legs["short_put"], "put") - p(legs["long_put"], "put"), 2)
    if structure == "bear_call_spread":
        return round(p(legs["short_call"], "call") - p(legs["long_call"], "call"), 2)
    if structure == "iron_condor":
        return round((p(legs["short_put"], "put") - p(legs["long_put"], "put"))
                     + (p(legs["short_call"], "call") - p(legs["long_call"], "call")), 2)
    if structure == "csp":
        return round(p(legs["short_put"], "put"), 2)
    return 0.0


def _earnings_within_7d(symbol: str) -> bool:
    """Earnings-within-7-days check. The O-Tasty universe is ETFs (no earnings),
    so this returns False for them; a stock universe would wire an earnings
    calendar here. Read-only; no orders."""
    return False


def _short_strike_breached(structure: str, legs: dict, spot: float) -> bool:
    if structure in ("bull_put_spread", "csp"):
        return spot < float(legs["short_put"])
    if structure == "bear_call_spread":
        return spot > float(legs["short_call"])
    if structure == "iron_condor":
        return spot < float(legs["short_put"]) or spot > float(legs["short_call"])
    return False


def _exit_decision(structure: str, legs: dict, credit: float, current_value: float,
                   spot: float, dte, symbol: str):
    """Exit triggers in priority order (HM-O-TASTY-DOCTRINE). Returns exit_reason
    str or None. Pure decision — no I/O, no orders."""
    if credit and credit > 0 and (credit - current_value) / credit * 100.0 >= 50.0:
        return "profit_50pct"                                   # 1
    if dte is not None and dte <= EXIT_DTE:
        return f"time_{EXIT_DTE}dte"                            # 2
    if credit and current_value >= credit * LOSS_LIMIT_MULT:
        return "loss_2x_credit"                                 # 3
    if _earnings_within_7d(symbol):
        return "earnings_7d"                                    # 4
    if _short_strike_breached(structure, legs, spot):
        return "short_strike_breach"                            # 5
    return None


def run_loop_c() -> dict:
    """LOOP C — position manager (SHADOW). Walk shadow_open rows, re-price current
    value, check exit triggers in priority order, mark would-have-closed rows
    (status='would_have_closed' + exit_reason + would_have_exit_at + exit_value +
    exit_pnl). NO order submission anywhere in this path."""
    import json
    conn = sqlite3.connect(DB_PATH)
    _ensure_shadow_schema(conn)
    conn.row_factory = sqlite3.Row

    state = get_killswitch_state(conn)
    if state == "HARD_HALT":   # halt everything, including management exits
        conn.close()
        return {"loop": "C", "killswitch": state, "halted": True, "walked": 0, "orders_submitted": 0}
    # ARMED + TRIPPED both manage — closing positions is allowed/safe under TRIPPED

    rows = conn.execute(
        "SELECT * FROM swingdesk_shadow_trades WHERE status='shadow_open'").fetchall()

    closed, held = [], []
    for r in rows:
        try:
            legs = json.loads(r["legs"] or "{}")
        except Exception:
            legs = {}
        structure = r["structure"]
        credit = r["credit"] or 0.0
        contracts = r["contracts"] or 1
        sigma = (r["iv"] or 30.0) / 100.0
        spot = get_spot(r["symbol"]) or r["spot"]
        dte = get_dte(r["expiration"]) if r["expiration"] else None
        cur_val = _current_spread_value(structure, legs, spot, dte if dte is not None else 0, sigma)
        reason = _exit_decision(structure, legs, credit, cur_val, spot, dte, r["symbol"])
        if reason:
            exit_pnl = round((credit - cur_val) * 100.0 * contracts, 2)
            conn.execute(
                "UPDATE swingdesk_shadow_trades SET status='would_have_closed', "
                "exit_reason=?, would_have_exit_at=?, exit_value=?, exit_pnl=? WHERE id=?",
                (reason, datetime.now().isoformat(timespec="seconds"), cur_val, exit_pnl, r["id"]),
            )
            closed.append((r["symbol"], structure, reason, exit_pnl))
        else:
            held.append((r["symbol"], structure, cur_val, dte))
    conn.commit()
    conn.close()
    return {
        "loop": "C",
        "walked": len(rows),
        "would_have_closed": closed,
        "held": held,
        "orders_submitted": 0,  # invariant: Loop C never submits
    }


# ── Loop E: nightly doctrine compliance auditor (SHADOW) ─────────────────────
# Exempt from the kill switch by design — it's read-only + the trust layer, so it
# audits even during a halt. (Loop D's kill switch gates A/B/C, not E.)
VALID_EXIT_REASONS = {
    "profit_50pct", "time_21dte", "loss_2x_credit", "short_strike_breach",
    "earnings_7d", "killswitch_tripped", "killswitch_hard_halt",
}


def _ensure_audit_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS swingdesk_audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            audit_date TEXT NOT NULL,
            audit_type TEXT NOT NULL,
            shadow_trade_id INTEGER,
            exit_reason TEXT,
            compliant INTEGER,
            violation_detail TEXT,
            note TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()


def _ntfy_admin(title: str, message: str, priority: str = "high") -> bool:
    """Fire an NTFY alert to the O-Tasty admin topic. Self-contained (ntfy.sh
    public topic via urllib) — NO broker, no engine dependency. Returns True on 2xx."""
    topic = os.getenv("OTASTY_NTFY_TOPIC", "ollietrades-admin")
    try:
        req = urllib.request.Request(
            f"https://ntfy.sh/{topic}",
            data=message.encode("utf-8"),
            headers={"Title": title, "Priority": priority, "Tags": "rotating_light"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=8) as r:
            return 200 <= r.status < 300
    except Exception as e:
        print(f"[NTFY] admin alert failed: {type(e).__name__}: {e}")
        return False


def run_loop_e() -> dict:
    """LOOP E — nightly doctrine compliance auditor (SHADOW). Walks today's closed
    shadow trades, verifies each exit_reason is a documented rule, logs findings to
    swingdesk_audit_log, logs today's kill-switch transitions, and NTFYs the admin
    topic on ANY violation (P0 trust layer). Pure read + audit-write — NO orders."""
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    _ensure_shadow_schema(conn)
    _ensure_audit_schema(conn)
    _ensure_killswitch_schema(conn)

    closed = conn.execute(
        "SELECT id, symbol, structure, exit_reason FROM swingdesk_shadow_trades "
        "WHERE status='would_have_closed' AND would_have_exit_at LIKE ?", (today + "%",)).fetchall()
    compliant_n, violations = 0, []
    for t in closed:
        er = t["exit_reason"]
        ok = er in VALID_EXIT_REASONS
        detail = None if ok else f"undocumented exit_reason '{er}' — not in rule set"
        conn.execute(
            "INSERT INTO swingdesk_audit_log "
            "(audit_date, audit_type, shadow_trade_id, exit_reason, compliant, violation_detail) "
            "VALUES (?,?,?,?,?,?)",
            (today, "exit_compliance", t["id"], er, int(ok), detail))
        if ok:
            compliant_n += 1
        else:
            violations.append({"shadow_trade_id": t["id"], "symbol": t["symbol"],
                               "exit_reason": er, "detail": detail})

    ks = conn.execute(
        "SELECT state, reason, changed_by, changed_at FROM swingdesk_killswitch "
        "WHERE changed_at LIKE ?", (today + "%",)).fetchall()
    for k in ks:
        conn.execute(
            "INSERT INTO swingdesk_audit_log (audit_date, audit_type, exit_reason, compliant, note) "
            "VALUES (?,?,?,?,?)",
            (today, "killswitch_transition", k["state"], 1,
             f"{k['state']} by {k['changed_by']}: {k['reason']} @ {k['changed_at']}"))
    conn.commit()
    conn.close()

    total = len(closed)
    pct = round(100.0 * compliant_n / total, 1) if total else 100.0
    alerted = False
    if violations:
        msg = (f"O-TASTY COMPLIANCE VIOLATION x{len(violations)}: "
               + "; ".join(f"#{v['shadow_trade_id']} {v['symbol']} exit='{v['exit_reason']}'"
                           for v in violations))
        alerted = _ntfy_admin("O-Tasty Compliance Violation", msg, priority="urgent")

    return {
        "loop": "E",
        "audit_date": today,
        "closed_today": total,
        "compliant": compliant_n,
        "violations": len(violations),
        "pct_compliant": pct,
        "violation_detail": violations,
        "killswitch_transitions_logged": len(ks),
        "ntfy_alerted": alerted,
        "orders_submitted": 0,  # invariant: Loop E never submits
    }


# ── Scheduler cadence (still SHADOW, still zero-order) ───────────────────────
# Runs in the isolated O-Tasty backend process — NOT the fleet's 145-job
# single-thread scheduler (HM-AS-β). The scheduler only INVOKES the shadow loops
# (all zero-order); it submits nothing itself.
_E_LAST_RUN_DATE: str | None = None
_scheduler_thread: threading.Thread | None = None


def _et_now() -> datetime:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        return datetime.now()


def is_rth() -> bool:
    """US equity regular trading hours: 9:30–16:00 ET, Mon–Fri (DST-aware)."""
    now = _et_now()
    if now.weekday() >= 5:
        return False
    mins = now.hour * 60 + now.minute
    return (9 * 60 + 30) <= mins < (16 * 60)


def _shadow_cycle() -> dict:
    """A/B/C every 5 min during RTH. Shadow; zero-order. No-op outside RTH."""
    if not is_rth():
        return {"cycle": "skipped_outside_rth"}
    a = run_loop_a()
    b = run_loop_b()
    c = run_loop_c()
    return {"cycle": "ran", "killswitch": a.get("killswitch"),
            "A_written": a.get("written"), "B_entered": b.get("entered"),
            "C_walked": c.get("walked")}


def _nightly_e_guard() -> dict:
    """Fire Loop E once per day after 18:00 ET (DST-safe via live ET clock)."""
    global _E_LAST_RUN_DATE
    now = _et_now()
    today = now.strftime("%Y-%m-%d")
    if now.hour >= 18 and _E_LAST_RUN_DATE != today:
        _E_LAST_RUN_DATE = today
        return run_loop_e()
    return {"e": "not_due"}


def register_shadow_schedule(scheduler=None):
    """Register the O-Tasty shadow cadence: A/B/C every 5 min (RTH-gated) + Loop E
    nightly after 6 PM ET. Registration only — does not start the run loop.
    Returns the registered jobs (for inspection)."""
    import schedule as _sch
    s = scheduler or _sch
    s.every(5).minutes.do(_shadow_cycle).tag("otasty", "abc-5min-rth")
    s.every(15).minutes.do(_nightly_e_guard).tag("otasty", "e-nightly-6pm-et")
    return s.get_jobs("otasty")


def start_shadow_scheduler() -> bool:
    """Start the O-Tasty shadow scheduler in a daemon thread (HM-EQ lifecycle:
    startup-bound, not lazy). Idempotent. Shadow; zero-order. Called from
    backend.py startup()."""
    global _scheduler_thread
    if _scheduler_thread is not None and _scheduler_thread.is_alive():
        return False
    import schedule as _sch
    register_shadow_schedule(_sch)

    def _loop():
        while True:
            try:
                _sch.run_pending()
            except Exception as e:
                print(f"[otasty-shadow-scheduler] {type(e).__name__}: {e}")
            time.sleep(30)

    _scheduler_thread = threading.Thread(target=_loop, daemon=True, name="otasty-shadow-scheduler")
    _scheduler_thread.start()
    return True


if __name__ == "__main__":
    import sys
    loop = sys.argv[1].upper() if len(sys.argv) > 1 else "A"
    if loop == "D":   # kill switch: "D" → status+log; "D <STATE> [reason]" → set
        if len(sys.argv) > 2:
            print(set_killswitch(sys.argv[2], " ".join(sys.argv[3:]) or "cli"))
        else:
            print({"state": get_killswitch_state(), "log": killswitch_log(10)})
    elif loop == "SCHED":   # show the registered cadence (no run loop)
        import schedule as _sch
        for j in register_shadow_schedule(_sch):
            print(j)
    else:
        fn = {"A": run_loop_a, "B": run_loop_b, "C": run_loop_c, "E": run_loop_e}.get(loop, run_loop_a)
        print(fn())
