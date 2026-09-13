#!/usr/bin/env python3
"""scripts/flashalpha_gex_validation.py — HM-FLASHALPHA-GEX-VALIDATION-2026-09-12.

FlashAlpha as a VALIDATION source, never a decision input. One independent,
free-tier, second-opinion GEX read per day, on NVDA, cross-checked against
our own live Alpaca-derived GEX for STRUCTURAL agreement only:
  - same sign on net GEX (both long-gamma or both short-gamma)
  - call wall above spot, put wall below spot -- on BOTH sources
  - gamma flip within a sane band of spot (+/-15%, this repo's own existing
    convention -- options_flow_gex.py's zero-cross scan window and
    gex_calculator.py's own `relevant` strike filter both already use it)

Explicitly NOT validated: raw numeric agreement. FlashAlpha's free tier is
last-day settled OI, a single expiry, 15-min cache; ours is the live chain
across expirations. They will not match numerically and are not expected
to -- an alert here means a STRUCTURAL contradiction (a wall on the wrong
side of spot, or opposite overall sign), which is the exact class of bug
HM-GEX-WALL-LABEL-FIX-2026-09-12 fixed earlier today, not ordinary
cross-source noise.

HARD BUDGET CONSTRAINT: FlashAlpha's free tier is 5 requests/day TOTAL,
shared with the Admiral's own manual use in chat -- this script has no way
to see how many of those the Admiral has already used, so its only lever
is to guarantee it NEVER spends more than ONE of the 5 per day, and to
FAIL CLOSED (skip, not spend) whenever it can't be certain of that. The
budget gate is the row this script's own previous run wrote, not anything
FlashAlpha's API reports about itself.

Never imported by any trading/decision-path code -- this is a standalone
script, invoked by cron only, writing to its own log table.
"""
from __future__ import annotations

import os
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

DB = str(REPO / "data" / "trader.db")
SYMBOL = "NVDA"  # free tier: individual equities only -- SPY/QQQ/SPX require Basic
FLASHALPHA_BASE = "https://lab.flashalpha.com/v1"
GAMMA_FLIP_BAND_PCT = 0.15  # +/-15% of spot -- this repo's existing "sane band" convention


def _init_db() -> None:
    conn = sqlite3.connect(DB, timeout=30)
    try:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gex_validation_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                expiration TEXT,
                ours_spot REAL, ours_net_gex REAL, ours_call_wall REAL,
                ours_put_wall REAL, ours_gamma_flip REAL,
                theirs_spot REAL, theirs_net_gex REAL, theirs_call_wall REAL,
                theirs_put_wall REAL, theirs_gamma_flip REAL,
                delta_net_gex_pct REAL, delta_spot_pct REAL, delta_flip_pct REAL,
                structural_match INTEGER,
                contradiction_type TEXT,
                fetched_at TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_gex_validation_date ON gex_validation_log(date)")
        conn.commit()
    finally:
        conn.close()


def _already_ran_today() -> bool:
    """The budget gate. A row for today's date means the fleet's one call is
    spent -- refuse to run again, unconditionally, regardless of anything
    else (including --force, which does not bypass this)."""
    conn = sqlite3.connect(DB, timeout=30)
    try:
        row = conn.execute(
            "SELECT 1 FROM gex_validation_log WHERE date=? AND symbol=? LIMIT 1",
            (date.today().isoformat(), SYMBOL),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def _pick_expiration() -> str | None:
    """One real listed NVDA expiration strictly after today, from our own
    already-fetched Alpaca options-contracts data -- never invented. Costs
    nothing against FlashAlpha's budget; this is our own broker's chain."""
    try:
        from gex_calculator import _get_calculator
        calc = _get_calculator()
        if calc is None:
            return None
        today = date.today()
        contracts = calc._fetch_contracts(
            SYMBOL, today.isoformat(), (today + timedelta(days=60)).isoformat()
        )
        expirations = sorted({
            c.expiration_date.isoformat() if hasattr(c.expiration_date, "isoformat")
            else str(c.expiration_date)
            for c in contracts
        })
        future = [e for e in expirations if e > today.isoformat()]
        return future[0] if future else None
    except Exception as e:
        print(f"[flashalpha-gex] expiration lookup failed: {type(e).__name__}: {e!r}", file=sys.stderr)
        return None


def _our_gex(expiration: str) -> dict | None:
    """Our own GEX, filtered to the SAME single expiration FlashAlpha's free
    tier is restricted to -- an apples-to-apples scope match (methodology
    and freshness still differ, by design, per the module docstring)."""
    import asyncio
    try:
        from gex_calculator import _get_calculator
        calc = _get_calculator()
        if calc is None:
            return None
        profile = asyncio.run(calc.compute(
            SYMBOL, expiration_date_gte=expiration, expiration_date_lte=expiration
        ))
        return {
            "spot": profile.spot_price, "net_gex": profile.total_gex,
            "call_wall": profile.call_wall, "put_wall": profile.put_wall,
            "gamma_flip": profile.gamma_flip,
        }
    except Exception as e:
        print(f"[flashalpha-gex] our own GEX compute failed: {type(e).__name__}: {e!r}", file=sys.stderr)
        return None


def _their_gex(expiration: str) -> dict | None:
    """FlashAlpha's free-tier GEX call. THE ONLY NETWORK CALL THAT SPENDS
    BUDGET IN THIS SCRIPT -- everything else is local or hits our own broker."""
    api_key = os.getenv("FLASHALPHA_API_KEY", "")
    if not api_key:
        print("[flashalpha-gex] FLASHALPHA_API_KEY not set -- skipping", file=sys.stderr)
        return None
    try:
        import requests
        resp = requests.get(
            f"{FLASHALPHA_BASE}/exposure/gex/{SYMBOL}",
            params={"expiration": expiration},
            headers={"X-Api-Key": api_key},
            timeout=15,
        )
        if resp.status_code != 200:
            print(f"[flashalpha-gex] HTTP {resp.status_code}: {resp.text[:300]}", file=sys.stderr)
            return None
        data = resp.json()
        spot = float(data["underlying_price"])
        strikes = data.get("strikes", [])
        # HM-GEX-WALL-LABEL-FIX-2026-09-12's own fix, applied here too:
        # derive call/put wall by POSITION relative to spot, not by trusting
        # FlashAlpha's own sign/labeling convention (unverified, and this is
        # exactly the class of bug this whole validation exists to catch --
        # it would be circular to trust the other side's labels uncritically).
        above = [s for s in strikes if s["strike"] >= spot]
        below = [s for s in strikes if s["strike"] < spot]
        call_wall = max(above, key=lambda s: s["net_gex"])["strike"] if above else None
        put_wall = min(below, key=lambda s: s["net_gex"])["strike"] if below else None
        return {
            "spot": spot, "net_gex": float(data["net_gex"]),
            "call_wall": call_wall, "put_wall": put_wall,
            "gamma_flip": float(data["gamma_flip"]),
        }
    except Exception as e:
        print(f"[flashalpha-gex] FlashAlpha call failed: {type(e).__name__}: {e!r}", file=sys.stderr)
        return None


def _structural_check(ours: dict, theirs: dict) -> tuple[bool, str | None]:
    """Returns (match, contradiction_type). Structure only -- never compares
    magnitudes. A None wall (e.g. FlashAlpha had no strikes on one side) is
    reported as a contradiction of its own, not silently skipped."""
    if ours["call_wall"] is None or ours["put_wall"] is None:
        return False, "our_own_wall_missing"
    if theirs["call_wall"] is None or theirs["put_wall"] is None:
        return False, "their_wall_missing"
    if ours["call_wall"] < ours["spot"] or ours["put_wall"] > ours["spot"]:
        return False, "our_own_wall_wrong_side"  # would mean today's fix regressed
    if theirs["call_wall"] < theirs["spot"] or theirs["put_wall"] > theirs["spot"]:
        return False, "their_wall_wrong_side"
    our_sign = ours["net_gex"] >= 0
    their_sign = theirs["net_gex"] >= 0
    if our_sign != their_sign:
        return False, "opposite_net_gex_sign"
    for label, d in (("our", ours), ("their", theirs)):
        lo, hi = d["spot"] * (1 - GAMMA_FLIP_BAND_PCT), d["spot"] * (1 + GAMMA_FLIP_BAND_PCT)
        if not (lo <= d["gamma_flip"] <= hi):
            return False, f"{label}_flip_out_of_band"
    return True, None


def main() -> int:
    # HM-FLASHALPHA-GEX-VALIDATION-2026-09-12: deliberately no override flag
    # of any kind for the budget gate below. "Refuse to run twice" means
    # exactly that -- a bypass flag is a foot-gun waiting for a fat-fingered
    # cron edit or manual invocation to burn shared budget. Testing this
    # script's logic uses a temp DB fixture (see tests/), never a real
    # override switch on the real one.
    _init_db()

    if _already_ran_today():
        print(f"[flashalpha-gex] already ran today ({date.today().isoformat()}) -- "
              "budget gate: skipping, not spending.")
        return 0

    expiration = _pick_expiration()
    if expiration is None:
        print("[flashalpha-gex] could not determine a real listed expiration -- "
              "skipping without spending FlashAlpha budget.", file=sys.stderr)
        return 1

    ours = _our_gex(expiration)
    if ours is None:
        print("[flashalpha-gex] our own GEX compute failed -- skipping without "
              "spending FlashAlpha budget (no point paying for a comparison with "
              "nothing to compare against).", file=sys.stderr)
        return 1

    theirs = _their_gex(expiration)  # the one call that spends budget
    if theirs is None:
        print("[flashalpha-gex] FlashAlpha call failed or was skipped -- nothing "
              "to log (a failed/skipped call is not a spent budget slot, so this "
              "does NOT count as today's run; a retry later today is fine).")
        return 1

    match, contradiction = _structural_check(ours, theirs)

    delta_spot_pct = (theirs["spot"] - ours["spot"]) / ours["spot"] * 100 if ours["spot"] else None
    delta_gex_pct = None
    if ours["net_gex"]:
        delta_gex_pct = (theirs["net_gex"] - ours["net_gex"]) / abs(ours["net_gex"]) * 100
    delta_flip_pct = (theirs["gamma_flip"] - ours["gamma_flip"]) / ours["gamma_flip"] * 100 if ours["gamma_flip"] else None

    conn = sqlite3.connect(DB, timeout=30)
    try:
        conn.execute(
            """INSERT INTO gex_validation_log
               (date, symbol, expiration, ours_spot, ours_net_gex, ours_call_wall,
                ours_put_wall, ours_gamma_flip, theirs_spot, theirs_net_gex,
                theirs_call_wall, theirs_put_wall, theirs_gamma_flip,
                delta_net_gex_pct, delta_spot_pct, delta_flip_pct,
                structural_match, contradiction_type, fetched_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (date.today().isoformat(), SYMBOL, expiration,
             ours["spot"], ours["net_gex"], ours["call_wall"], ours["put_wall"], ours["gamma_flip"],
             theirs["spot"], theirs["net_gex"], theirs["call_wall"], theirs["put_wall"], theirs["gamma_flip"],
             delta_gex_pct, delta_spot_pct, delta_flip_pct,
             1 if match else 0, contradiction,
             datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
    finally:
        conn.close()

    print(f"[flashalpha-gex] {SYMBOL} exp={expiration} structural_match={match} "
          f"contradiction={contradiction} (ours spot={ours['spot']} theirs spot={theirs['spot']}, "
          f"delta_spot={delta_spot_pct:.2f}% if ours nonzero)")

    if not match:
        try:
            from engine.alert_channels import send_alert, AlertLevel
            send_alert(
                message=(
                    f"FlashAlpha GEX validation: structural contradiction on {SYMBOL} "
                    f"({contradiction}). Ours: spot={ours['spot']} call_wall={ours['call_wall']} "
                    f"put_wall={ours['put_wall']} flip={ours['gamma_flip']} net_gex={ours['net_gex']:+,.0f}. "
                    f"FlashAlpha: spot={theirs['spot']} call_wall={theirs['call_wall']} "
                    f"put_wall={theirs['put_wall']} flip={theirs['gamma_flip']} net_gex={theirs['net_gex']:+,.0f}. "
                    f"This flags a possible bug in OUR pipeline -- validation only, "
                    f"not a trade signal, never read by any decision path."
                ),
                level=AlertLevel.WARNING,
                alert_type="flashalpha_gex_structural_contradiction",
                title="⚠️ GEX validation: structural contradiction",
                rate_limit_secs=3600,
            )
        except Exception as e:
            print(f"[flashalpha-gex] alert send failed: {type(e).__name__}: {e!r}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
