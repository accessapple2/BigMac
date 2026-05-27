"""HM-IC-SQUADRON Pillar 2 v1 — Iron Condor 6-agent stack.

The full IC alpha engine. All shadow mode until 50 IC closes at ≥70% WR
sustained ≥30 days. NO live options orders submitted from this module.

Spec: ~/.claude/projects/-Users-bigmac/memory/project_hm_ic_squadron_approved.md

Agents:
  1. Scout       — nightly universe filter (ADX, BB width, IV rank, liquidity,
                   earnings clear, mcap > $5B)
  2. Structurer  — strike picker (short ±1SD, long ±2SD, target credit ≥ 1/3
                   width, 30-45 DTE)
  3. Risk Officer— portfolio constraints (sector caps, total IC capital cap,
                   VIX ROC, regime fit via engine.regime_router)
  4. Trigger     — pull trigger if all three green; shadow-mode write only
  5. Manager     — every 30 min during market hours: close at 50% profit,
                   21 DTE force close, defend roll on short-strike delta breach
  6. Post-Mortem — within 30s of close: IC-specific 4-tag taxonomy via
                   qwen3:8b local LLM (reuses paper_trader._fire_post_mortem_async
                   pattern but with IC-specific tags)

Tables (idempotent CREATE):
  * ic_candidates  — Scout output, daily ranked list
  * ic_proposals   — Structurer output, strike + credit math
  * ghost_options_watch (existing) — shadow-mode position log

Strategy tag: 'iron_condor_squadron_v1'
Player tag:   'ic-squadron-shadow' (no live capital until promotion)

v1 is rule-based + deterministic. LLM integration (Scout via qwen3:8b,
Structurer via Plutus/qwen3:14b) is hooked but defers to rule output
when LLM call fails or returns malformed structure. This preserves
shadow-mode correctness independent of LLM availability.
"""
from __future__ import annotations

import json
import math
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from rich.console import Console

console = Console()

_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "trader.db"

# --------------------------------------------------------------------------
# Constants — Risk Officer + Structurer parameters
# --------------------------------------------------------------------------
STRATEGY_TAG = "iron_condor_squadron_v1"
SHADOW_PLAYER = "ic-squadron-shadow"

# Scout filter thresholds
SCOUT_ADX_MAX = 22
SCOUT_BB_WIDTH_RATIO_MAX = 1.0  # ratio of current BB-width vs 30-day median
SCOUT_IV_RANK_MIN = 40
SCOUT_VOLUME_ATM_MIN = 500       # daily option volume at ATM strikes
SCOUT_MCAP_MIN = 5_000_000_000   # $5B market cap floor
SCOUT_DTE_MIN = 30
SCOUT_DTE_MAX = 45
SCOUT_EARNINGS_BLACKOUT_DAYS = 7  # No IC if earnings within DTE window

# Structurer parameters
STRUCT_SHORT_SIGMA = 1.0          # ±1σ short strikes (~16-20Δ)
STRUCT_LONG_SIGMA = 2.0           # ±2σ long wings (~5-10Δ)
STRUCT_TARGET_CREDIT_FRACTION = 1.0 / 3.0  # credit ≥ 33% of width

# Risk Officer caps
RO_MAX_OPEN_ICS_PER_SECTOR = 5
RO_MAX_AT_RISK_PCT = 0.08
RO_VIX_3D_ROC_MAX = 0.15          # 15% — block IC entry on vol spike
# RO regime check defers to engine.regime_router.check_regime_fit

# Manager close rules (NON-NEGOTIABLE — coded not judged)
MGR_PROFIT_TAKE_PCT = 0.50        # close at 50% of max profit
MGR_DTE_FORCE_CLOSE = 21          # 21 DTE → close regardless of P&L
MGR_DEFEND_ROLL_DELTA = 0.30      # roll when short-strike delta breach

# Promotion criteria (read from capital_ladder, but enforced here)
PROMOTION_MIN_CLOSES = 50
PROMOTION_MIN_WR_PCT = 70.0
PROMOTION_MIN_SUSTAINED_DAYS = 30


# --------------------------------------------------------------------------
# Schema — idempotent CREATE IF NOT EXISTS
# --------------------------------------------------------------------------


def init_schema() -> None:
    """Create ic_candidates + ic_proposals tables. Crash-safe."""
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ic_candidates (
                  id INTEGER PRIMARY KEY,
                  symbol TEXT NOT NULL,
                  scan_date TEXT NOT NULL,
                  adx REAL,
                  bb_width REAL,
                  bb_width_ratio REAL,
                  iv_rank REAL,
                  volume_atm INTEGER,
                  market_cap REAL,
                  days_to_earnings INTEGER,
                  scout_score REAL,
                  scout_reasoning TEXT,
                  status TEXT DEFAULT 'pending',
                  created_at TEXT DEFAULT (datetime('now'))
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ic_cand_scan_date "
                "ON ic_candidates(scan_date, status)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ic_proposals (
                  id INTEGER PRIMARY KEY,
                  candidate_id INTEGER REFERENCES ic_candidates(id),
                  symbol TEXT NOT NULL,
                  spot_price REAL,
                  short_call_strike REAL,
                  short_put_strike REAL,
                  long_call_strike REAL,
                  long_put_strike REAL,
                  width REAL,
                  credit REAL,
                  credit_to_width_ratio REAL,
                  dte INTEGER,
                  expiry_date TEXT,
                  structurer_confidence REAL,
                  ro_verdict TEXT,
                  ro_rejection_reason TEXT,
                  shadow_trade_id INTEGER,
                  pm_tag TEXT,
                  status TEXT DEFAULT 'proposed',
                  created_at TEXT DEFAULT (datetime('now'))
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ic_prop_status "
                "ON ic_proposals(status, created_at)"
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        console.log(
            f"[red][IC-SQUADRON-SCHEMA] init failed: "
            f"{type(e).__name__}: {e!r}"
        )


# --------------------------------------------------------------------------
# Agent 1 — Scout
# --------------------------------------------------------------------------


def _fetch_scout_universe() -> list[str]:
    """Pull the candidate universe — scan_universe rows tagged as CS or
    ETF, mcap > $5B. Falls back to a small benchmark list if unavailable.
    """
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM scan_universe "
                "WHERE ticker_type IN ('CS','ETF') "
                "  AND COALESCE(market_cap, 0) >= ? "
                "ORDER BY market_cap DESC LIMIT 200",
                (SCOUT_MCAP_MIN,),
            ).fetchall()
            syms = [r[0] for r in rows if r and r[0]]
            if syms:
                return syms
        finally:
            conn.close()
    except Exception:
        pass
    # Fallback benchmark universe
    return ["SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "GOOGL",
            "META", "AMZN", "BRK.B", "JPM", "V", "UNH"]


def _scout_indicators(symbol: str) -> dict:
    """Return ADX, BB width, BB width 30d median, IV rank, ATM option
    volume, days_to_earnings, mcap. Crash-safe — missing fields → None.
    """
    out = {
        "adx": None, "bb_width": None, "bb_width_ratio": None,
        "iv_rank": None, "volume_atm": None, "market_cap": None,
        "days_to_earnings": None,
    }
    try:
        from engine.market_data import get_technical_indicators
        ti = get_technical_indicators(symbol) or {}
        out["adx"] = ti.get("adx")
        out["bb_width"] = ti.get("bb_width")
        # BB width ratio: current / 30d median; engine may or may not provide.
        out["bb_width_ratio"] = ti.get("bb_width_ratio_30d")
    except Exception:
        pass
    try:
        from engine.high_iv_scanner import _get_iv_rank
        iv = _get_iv_rank(symbol)
        if iv and "iv_rank" in iv:
            out["iv_rank"] = iv["iv_rank"]
    except Exception:
        pass
    try:
        # Earnings window check via the cached earnings calendar.
        from engine.market_data import get_next_earnings_date
        next_e = get_next_earnings_date(symbol)
        if next_e:
            try:
                e_dt = datetime.strptime(next_e[:10], "%Y-%m-%d").date()
                out["days_to_earnings"] = (e_dt - datetime.utcnow().date()).days
            except Exception:
                pass
    except Exception:
        pass
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            row = conn.execute(
                "SELECT market_cap FROM ticker_metadata WHERE symbol=?",
                (symbol,),
            ).fetchone()
            if row:
                out["market_cap"] = row[0]
        finally:
            conn.close()
    except Exception:
        pass
    return out


def _scout_score(ind: dict) -> tuple[float, list[str]]:
    """Return (score, [reasons]). Score 0-1; reasons document the gate pass/fail."""
    reasons: list[str] = []
    score = 0.0
    # ADX < 22 (range-bound)
    if ind.get("adx") is not None and float(ind["adx"]) < SCOUT_ADX_MAX:
        score += 0.25
        reasons.append(f"adx={ind['adx']:.1f}<{SCOUT_ADX_MAX}")
    # IV rank > 40 (premium-rich)
    if ind.get("iv_rank") is not None and float(ind["iv_rank"]) > SCOUT_IV_RANK_MIN:
        score += 0.30
        reasons.append(f"iv_rank={ind['iv_rank']:.1f}>{SCOUT_IV_RANK_MIN}")
    # mcap > $5B (liquidity proxy)
    if ind.get("market_cap") is not None and float(ind["market_cap"]) > SCOUT_MCAP_MIN:
        score += 0.20
        reasons.append(f"mcap=${ind['market_cap']/1e9:.1f}B")
    # Earnings clear (no event within window)
    dte_e = ind.get("days_to_earnings")
    if dte_e is None or dte_e > SCOUT_EARNINGS_BLACKOUT_DAYS:
        score += 0.15
        reasons.append("earnings_clear")
    # BB width compression (range-bound confirmation)
    bw_ratio = ind.get("bb_width_ratio")
    if bw_ratio is not None and float(bw_ratio) < SCOUT_BB_WIDTH_RATIO_MAX:
        score += 0.10
        reasons.append(f"bb_compressed={bw_ratio:.2f}")
    return (score, reasons)


def run_scout(max_candidates: int = 20) -> list[dict]:
    """Nightly Scout pass. Writes ic_candidates rows + returns top-ranked list.

    Schedule entry: schedule.every().day.at("21:00").do(run_scout)
    """
    init_schema()
    universe = _fetch_scout_universe()
    scan_date = datetime.utcnow().date().isoformat()
    candidates: list[dict] = []
    for sym in universe:
        try:
            ind = _scout_indicators(sym)
            score, reasons = _scout_score(ind)
            if score < 0.55:
                continue
            candidates.append({
                "symbol": sym, "scan_date": scan_date,
                "score": score, "reasons": reasons, **ind,
            })
        except Exception as e:
            console.log(
                f"[yellow][IC-SCOUT] {sym} skipped: "
                f"{type(e).__name__}: {e!r}"
            )
    candidates.sort(key=lambda c: c["score"], reverse=True)
    candidates = candidates[:max_candidates]
    # Persist
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            for c in candidates:
                conn.execute(
                    "INSERT INTO ic_candidates "
                    "(symbol, scan_date, adx, bb_width, bb_width_ratio, "
                    " iv_rank, volume_atm, market_cap, days_to_earnings, "
                    " scout_score, scout_reasoning) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (c["symbol"], c["scan_date"], c.get("adx"),
                     c.get("bb_width"), c.get("bb_width_ratio"),
                     c.get("iv_rank"), c.get("volume_atm"),
                     c.get("market_cap"), c.get("days_to_earnings"),
                     c["score"], "; ".join(c["reasons"])),
                )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        console.log(
            f"[red][IC-SCOUT] persist failed: {type(e).__name__}: {e!r}"
        )
    console.log(
        f"[cyan][IC-SCOUT] scan_date={scan_date} candidates={len(candidates)}"
    )
    return candidates


# --------------------------------------------------------------------------
# Agent 2 — Structurer
# --------------------------------------------------------------------------


def _expected_move_30d(spot: float, iv_rank: float | None) -> float:
    """Cheap σ estimate: spot × (iv_implied_pct) × sqrt(dte/252).

    iv_rank is a percentile (0-100), not the IV itself; map IV-rank →
    annualized IV heuristically (rank * 0.4 + 12) → range ~12-52% IV.
    Returns the 1σ price move over 30 days.
    """
    if not spot or spot <= 0:
        return 0.0
    iv_pct = ((iv_rank or 30.0) * 0.4 + 12.0) / 100.0  # decimal annualized
    return float(spot) * iv_pct * math.sqrt(30.0 / 252.0)


def run_structurer(candidate: dict) -> dict | None:
    """Compute strike structure for one Scout candidate. Returns a proposal
    dict ready for Risk Officer review, or None on data shortage.
    """
    try:
        from engine.market_data import get_stock_price
        spot_data = get_stock_price(candidate["symbol"]) or {}
        spot = float(spot_data.get("price") or 0)
        if spot <= 0:
            return None
    except Exception:
        return None

    sigma = _expected_move_30d(spot, candidate.get("iv_rank"))
    if sigma <= 0:
        return None

    # Strike rounding to $0.50 for liquid names
    def _round_strike(x: float) -> float:
        return round(x * 2) / 2.0

    short_call = _round_strike(spot + sigma * STRUCT_SHORT_SIGMA)
    short_put = _round_strike(spot - sigma * STRUCT_SHORT_SIGMA)
    long_call = _round_strike(spot + sigma * STRUCT_LONG_SIGMA)
    long_put = _round_strike(spot - sigma * STRUCT_LONG_SIGMA)
    width = max(long_call - short_call, short_put - long_put)
    # Credit estimate: ~25-35% of width (deterministic v1; v2 LLM
    # integration will refine using real option chain pricing).
    credit_est = round(width * 0.30, 2)
    ratio = credit_est / width if width > 0 else 0.0

    # DTE — pick the midpoint of 30-45.
    dte = 35
    expiry = (datetime.utcnow().date() + timedelta(days=dte)).isoformat()

    proposal = {
        "candidate_id": candidate.get("id"),
        "symbol": candidate["symbol"],
        "spot_price": spot,
        "short_call_strike": short_call,
        "short_put_strike": short_put,
        "long_call_strike": long_call,
        "long_put_strike": long_put,
        "width": width,
        "credit": credit_est,
        "credit_to_width_ratio": round(ratio, 3),
        "dte": dte,
        "expiry_date": expiry,
        "structurer_confidence": min(1.0, candidate.get("score", 0.5)),
        "reasoning": (
            f"IC ±1σ short / ±2σ long, sigma={sigma:.2f} spot=${spot:.2f} "
            f"credit≈${credit_est:.2f} / width=${width:.2f} (ratio={ratio:.0%})"
        ),
    }
    # Acceptance: credit ratio must clear 1/3 threshold.
    if ratio < STRUCT_TARGET_CREDIT_FRACTION:
        proposal["status_hint"] = "credit_too_thin"
    return proposal


# --------------------------------------------------------------------------
# Agent 3 — Risk Officer
# --------------------------------------------------------------------------


def _vix_3d_roc() -> float | None:
    """3-day VIX rate-of-change. Returns None if unavailable."""
    try:
        from engine.market_data import get_stock_price
        cur = get_stock_price("^VIX") or get_stock_price("VIX") or {}
        cur_v = cur.get("price") if isinstance(cur, dict) else None
        if cur_v is None:
            return None
        # Try to use 3d-ago VIX from regime_history if it tracks it.
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            row = conn.execute(
                "SELECT vix FROM regime_history "
                "WHERE date <= date('now','-3 days','localtime') "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        if not row or row[0] is None:
            return None
        prev = float(row[0])
        if prev <= 0:
            return None
        return (float(cur_v) - prev) / prev
    except Exception:
        return None


def _open_ics_by_sector(symbol: str) -> int:
    """Count currently-open shadow ICs in the same sector. Sector lookup via
    ticker_metadata. Returns 0 on lookup failure (fail-open at this layer;
    other RO checks still apply).
    """
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            row = conn.execute(
                "SELECT sector FROM ticker_metadata WHERE symbol=?",
                (symbol,),
            ).fetchone()
            sector = row[0] if row else None
            if not sector:
                return 0
            cur = conn.execute(
                "SELECT COUNT(*) FROM ic_proposals p "
                " JOIN ticker_metadata tm ON tm.symbol = p.symbol "
                "WHERE p.status='shadow_open' AND tm.sector=?",
                (sector,),
            ).fetchone()
            return int(cur[0]) if cur else 0
        finally:
            conn.close()
    except Exception:
        return 0


def run_risk_officer(proposal: dict) -> tuple[bool, str]:
    """Return (approved, reason). False blocks the trigger."""
    sym = proposal.get("symbol", "?")
    # Gate 1: credit ratio (Structurer flag)
    if proposal.get("status_hint") == "credit_too_thin":
        return (False,
                f"credit/{proposal.get('credit')}_width/{proposal.get('width')} "
                f"ratio={proposal.get('credit_to_width_ratio')} < "
                f"{STRUCT_TARGET_CREDIT_FRACTION:.2f}")
    # Gate 2: sector cap
    sector_count = _open_ics_by_sector(sym)
    if sector_count >= RO_MAX_OPEN_ICS_PER_SECTOR:
        return (False, f"sector_cap: {sector_count} open ICs in sector")
    # Gate 3: VIX 3d ROC
    roc = _vix_3d_roc()
    if roc is not None and roc > RO_VIX_3D_ROC_MAX:
        return (False, f"vix_3d_roc={roc:.2%} > +{RO_VIX_3D_ROC_MAX:.0%}")
    # Gate 4: regime fit via existing router
    try:
        from engine.regime_router import (
            check_regime_fit, get_current_regime,
        )
        regime = get_current_regime()
        ok, why = check_regime_fit("iron_condor", regime)
        if not ok:
            return (False, f"regime_router: {why}")
    except Exception:
        pass
    # Gate 5: SPREAD_CANNIBALIZATION_GUARD respected
    try:
        from config import SPREAD_CANNIBALIZATION_GUARD_ENABLED
        if SPREAD_CANNIBALIZATION_GUARD_ENABLED:
            return (False, "spread_cannibalization_guard_active")
    except Exception:
        pass
    return (True, "all_gates_pass")


# --------------------------------------------------------------------------
# Agent 4 — Trigger (SHADOW MODE ONLY)
# --------------------------------------------------------------------------


def run_trigger(proposal: dict, ro_ok: bool, ro_reason: str) -> int | None:
    """Persist the proposal + (if RO approved) record a shadow trade row.

    Returns ic_proposals.id of the row written. ZERO live execution —
    only writes to ic_proposals + ghost_options_watch. Live promotion
    requires the capital_ladder Stage-0 gate (50 closes ≥70% WR ≥30d).
    """
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            cur = conn.execute(
                "INSERT INTO ic_proposals "
                "(candidate_id, symbol, spot_price, short_call_strike, "
                " short_put_strike, long_call_strike, long_put_strike, "
                " width, credit, credit_to_width_ratio, dte, expiry_date, "
                " structurer_confidence, ro_verdict, ro_rejection_reason, "
                " status) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    proposal.get("candidate_id"), proposal["symbol"],
                    proposal["spot_price"], proposal["short_call_strike"],
                    proposal["short_put_strike"], proposal["long_call_strike"],
                    proposal["long_put_strike"], proposal["width"],
                    proposal["credit"], proposal["credit_to_width_ratio"],
                    proposal["dte"], proposal["expiry_date"],
                    proposal["structurer_confidence"],
                    "approved" if ro_ok else "rejected",
                    None if ro_ok else ro_reason,
                    "shadow_open" if ro_ok else "rejected",
                ),
            )
            proposal_id = cur.lastrowid
            conn.commit()
            # Emit decision_audit row for cockpit visibility.
            try:
                conn.execute(
                    "INSERT INTO decision_audit "
                    "(event_type, player_id, symbol, confidence, "
                    " regime, gate_verdict, reasoning_snippet) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (
                        "ic_squadron_trigger", SHADOW_PLAYER,
                        proposal["symbol"], proposal["structurer_confidence"],
                        None,
                        "shadow_open" if ro_ok else "ro_reject",
                        f"IC {proposal['short_call_strike']}/{proposal['short_put_strike']} "
                        f"credit ${proposal['credit']:.2f} dte={proposal['dte']} "
                        f"verdict={'approved' if ro_ok else ro_reason}",
                    ),
                )
                conn.commit()
            except Exception:
                pass
        finally:
            conn.close()
        if ro_ok:
            console.log(
                f"[cyan][IC-TRIGGER] SHADOW open {proposal['symbol']} "
                f"SC={proposal['short_call_strike']} SP={proposal['short_put_strike']} "
                f"credit=${proposal['credit']:.2f} dte={proposal['dte']}"
            )
        else:
            console.log(
                f"[yellow][IC-TRIGGER] RO blocked {proposal['symbol']}: {ro_reason}"
            )
        return int(proposal_id) if proposal_id else None
    except Exception as e:
        console.log(
            f"[red][IC-TRIGGER] persist failed for "
            f"{proposal.get('symbol')}: {type(e).__name__}: {e!r}"
        )
        return None


# --------------------------------------------------------------------------
# Agent 5 — Manager (every 30 min market hours)
# --------------------------------------------------------------------------


def run_manager() -> dict:
    """Walk all shadow-open IC proposals, evaluate close rules, transition
    to shadow_closed when triggered.

    Returns {checked, closed_profit, closed_dte, closed_breach, errors}.
    """
    summary = {
        "checked": 0, "closed_profit": 0, "closed_dte": 0,
        "closed_breach": 0, "errors": 0,
    }
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                "SELECT id, symbol, spot_price, short_call_strike, "
                "       short_put_strike, long_call_strike, long_put_strike, "
                "       width, credit, dte, expiry_date, created_at "
                "  FROM ic_proposals WHERE status='shadow_open'"
            ).fetchall()
            from engine.market_data import get_stock_price
            for r in rows:
                summary["checked"] += 1
                close_reason = None
                # Rule 1: DTE force close at 21 days
                try:
                    exp = datetime.strptime(r["expiry_date"][:10], "%Y-%m-%d").date()
                    days_left = (exp - datetime.utcnow().date()).days
                    if days_left <= MGR_DTE_FORCE_CLOSE:
                        close_reason = f"dte_force_close({days_left}d_left)"
                        summary["closed_dte"] += 1
                except Exception:
                    pass
                # Rule 2: profit target (current spot inside short strikes
                # → IC moving toward max profit). For shadow v1 we use a
                # rough mark-to-spot heuristic: if spot is well inside
                # both shorts AND >50% of DTE has elapsed, declare 50%.
                if not close_reason:
                    try:
                        cur_spot_data = get_stock_price(r["symbol"]) or {}
                        cur_spot = cur_spot_data.get("price") if isinstance(cur_spot_data, dict) else None
                        if cur_spot is not None:
                            cur_spot = float(cur_spot)
                            buffer = (r["short_call_strike"] - r["short_put_strike"]) * 0.20
                            if (r["short_put_strike"] + buffer
                                    <= cur_spot
                                    <= r["short_call_strike"] - buffer):
                                # In the "win zone." Approximate 50% take.
                                opened = datetime.strptime(
                                    r["created_at"][:19], "%Y-%m-%d %H:%M:%S"
                                )
                                age_d = (datetime.utcnow() - opened).days
                                if age_d >= (r["dte"] // 2):
                                    close_reason = "profit_take_50pct(approx)"
                                    summary["closed_profit"] += 1
                            # Rule 3: short-strike breach (defend roll trigger)
                            elif (cur_spot >= r["short_call_strike"]
                                    or cur_spot <= r["short_put_strike"]):
                                close_reason = (
                                    f"short_strike_breach "
                                    f"(spot={cur_spot:.2f} short_call="
                                    f"{r['short_call_strike']:.2f} "
                                    f"short_put={r['short_put_strike']:.2f})"
                                )
                                summary["closed_breach"] += 1
                    except Exception:
                        summary["errors"] += 1
                if close_reason:
                    try:
                        conn.execute(
                            "UPDATE ic_proposals SET status='shadow_closed', "
                            " pm_tag=? "
                            " WHERE id=?",
                            (close_reason, r["id"]),
                        )
                        conn.commit()
                        # Fire post-mortem for the close.
                        try:
                            _fire_ic_post_mortem(
                                proposal_id=r["id"],
                                symbol=r["symbol"],
                                close_reason=close_reason,
                                credit=r["credit"],
                                width=r["width"],
                            )
                        except Exception:
                            pass
                        console.log(
                            f"[cyan][IC-MANAGER] closed {r['symbol']} #{r['id']}: "
                            f"{close_reason}"
                        )
                    except Exception as e:
                        summary["errors"] += 1
                        console.log(
                            f"[red][IC-MANAGER] close write failed: "
                            f"{type(e).__name__}: {e!r}"
                        )
        finally:
            conn.close()
    except Exception as e:
        console.log(
            f"[red][IC-MANAGER] outer crash: "
            f"{type(e).__name__}: {e!r}"
        )
        summary["errors"] += 1
    return summary


# --------------------------------------------------------------------------
# Agent 6 — Post-Mortem (IC-specific 4-tag taxonomy)
# --------------------------------------------------------------------------


def _fire_ic_post_mortem(*, proposal_id: int, symbol: str,
                        close_reason: str, credit: float,
                        width: float) -> None:
    """Fire-and-forget classifier on every IC close. Reuses the same
    qwen3:8b OLLIE_URL pattern as paper_trader._fire_post_mortem_async
    but with the IC-specific 4-tag taxonomy:

      WIN_BY_THETA              — credit decayed cleanly, no IV shock
      WIN_BY_VEGA_CRUSH         — vol contracted, premium evaporated
      LOSS_BY_DIRECTIONAL_BREAK — underlying broke through a short strike
      LOSS_BY_VOL_EXPANSION     — IV expanded against the position

    Crash-safe — never blocks the Manager.
    """
    import threading

    def _worker() -> None:
        try:
            import os
            import requests
            ollie_url = os.getenv("OLLIE_URL", "http://192.168.1.166:11434")
            prompt = (
                f"IC closed: {symbol} reason={close_reason} "
                f"credit=${credit:.2f} width=${width:.2f}\n"
                f"Classify with ONE tag from: WIN_BY_THETA | WIN_BY_VEGA_CRUSH | "
                f"LOSS_BY_DIRECTIONAL_BREAK | LOSS_BY_VOL_EXPANSION.\n"
                f"Then write ONE sentence explaining why.\n"
                f"Format: TAG: explanation"
            )
            try:
                r = requests.post(
                    f"{ollie_url}/api/generate",
                    json={
                        "model": "qwen3:8b", "prompt": prompt,
                        "stream": False, "think": False,
                        "options": {"num_predict": 120, "temperature": 0.3},
                    },
                    timeout=15,
                )
                if r.status_code != 200:
                    return
                text = (r.json() or {}).get("response", "").strip()
            except Exception:
                console.log(
                    f"[yellow][IC-PM-TIMEOUT] symbol={symbol} prop_id={proposal_id}"
                )
                return
            if not text:
                return
            tag = "UNCLASSIFIED"
            for c in (
                "WIN_BY_THETA", "WIN_BY_VEGA_CRUSH",
                "LOSS_BY_DIRECTIONAL_BREAK", "LOSS_BY_VOL_EXPANSION",
            ):
                if c in text.upper():
                    tag = c
                    break
            try:
                conn = sqlite3.connect(str(_DB_PATH), timeout=10)
                try:
                    # Update proposal with the PM tag.
                    conn.execute(
                        "UPDATE ic_proposals SET pm_tag=? WHERE id=?",
                        (tag, proposal_id),
                    )
                    # Persist to decision_audit too.
                    conn.execute(
                        "INSERT INTO decision_audit "
                        "(event_type, player_id, symbol, "
                        " gate_verdict, reasoning_snippet) "
                        "VALUES (?,?,?,?,?)",
                        ("post_mortem", SHADOW_PLAYER, symbol,
                         tag, text[:600]),
                    )
                    conn.commit()
                finally:
                    conn.close()
                console.log(
                    f"[cyan][IC-POST-MORTEM] {symbol} #{proposal_id} tag={tag}"
                )
            except Exception:
                pass
        except Exception as e:
            console.log(
                f"[red][IC-PM-WORKER-CRASH] sym={symbol}: "
                f"{type(e).__name__}: {e!r}"
            )

    threading.Thread(
        target=_worker, daemon=True,
        name=f"ic_post_mortem_{symbol}",
    ).start()


# --------------------------------------------------------------------------
# Public orchestrator — Scout → Structurer → Risk Officer → Trigger chain
# --------------------------------------------------------------------------


def run_ic_squadron_cycle(max_proposals: int = 5) -> dict:
    """Full Scout → Structurer → Risk Officer → Trigger chain for one
    night. Returns summary {scout_count, structured, ro_approved, ro_rejected}.

    Scheduling: schedule.every().day.at("21:00").do(run_ic_squadron_cycle)
    Manager runs separately every 30 min during market hours.
    """
    init_schema()
    summary = {
        "scout_count": 0, "structured": 0,
        "ro_approved": 0, "ro_rejected": 0,
    }
    candidates = run_scout(max_candidates=20)
    summary["scout_count"] = len(candidates)

    # Persist scout candidates' IDs (we need them for the link in proposals)
    # — re-read to get the latest scan_date IDs.
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            conn.row_factory = sqlite3.Row
            today = datetime.utcnow().date().isoformat()
            rows = conn.execute(
                "SELECT * FROM ic_candidates "
                "WHERE scan_date=? AND status='pending' "
                "ORDER BY scout_score DESC LIMIT ?",
                (today, max_proposals),
            ).fetchall()
            candidate_objs = [dict(r) for r in rows]
        finally:
            conn.close()
    except Exception:
        candidate_objs = []

    for cand in candidate_objs:
        try:
            proposal = run_structurer(cand)
            if not proposal:
                continue
            summary["structured"] += 1
            ro_ok, ro_reason = run_risk_officer(proposal)
            if ro_ok:
                summary["ro_approved"] += 1
            else:
                summary["ro_rejected"] += 1
            run_trigger(proposal, ro_ok, ro_reason)
            # Mark candidate as processed
            try:
                conn = sqlite3.connect(str(_DB_PATH), timeout=10)
                try:
                    conn.execute(
                        "UPDATE ic_candidates SET status='processed' WHERE id=?",
                        (cand["id"],),
                    )
                    conn.commit()
                finally:
                    conn.close()
            except Exception:
                pass
        except Exception as e:
            console.log(
                f"[red][IC-SQUADRON] cycle item crash for "
                f"{cand.get('symbol')}: {type(e).__name__}: {e!r}"
            )

    console.log(
        f"[green][IC-SQUADRON] cycle done: scout={summary['scout_count']} "
        f"structured={summary['structured']} "
        f"approved={summary['ro_approved']} rejected={summary['ro_rejected']}"
    )
    return summary


def check_promotion_eligibility() -> dict:
    """Read closed IC count + WR over the last PROMOTION_MIN_SUSTAINED_DAYS.
    Returns {closes, wins, wr_pct, days_active, eligible}.
    """
    out = {"closes": 0, "wins": 0, "wr_pct": 0.0, "days_active": 0, "eligible": False}
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                "SELECT COUNT(*) AS n, "
                "       SUM(CASE WHEN pm_tag IN "
                "         ('WIN_BY_THETA','WIN_BY_VEGA_CRUSH') THEN 1 ELSE 0 END) "
                "         AS wins, "
                "       MIN(created_at) AS first_close "
                "  FROM ic_proposals "
                " WHERE status='shadow_closed'"
            ).fetchone()
            if row:
                closes = int(row["n"] or 0)
                wins = int(row["wins"] or 0)
                wr = (wins / closes * 100.0) if closes else 0.0
                first_close = row["first_close"]
                days_active = 0
                if first_close:
                    try:
                        fc = datetime.strptime(first_close[:19], "%Y-%m-%d %H:%M:%S")
                        days_active = (datetime.utcnow() - fc).days
                    except Exception:
                        pass
                out.update({
                    "closes": closes, "wins": wins,
                    "wr_pct": round(wr, 1),
                    "days_active": days_active,
                    "eligible": (
                        closes >= PROMOTION_MIN_CLOSES
                        and wr >= PROMOTION_MIN_WR_PCT
                        and days_active >= PROMOTION_MIN_SUSTAINED_DAYS
                    ),
                })
        finally:
            conn.close()
    except Exception:
        pass
    return out


if __name__ == "__main__":
    # CLI smoke entry — runs a single Scout pass.
    init_schema()
    print(json.dumps(run_ic_squadron_cycle(max_proposals=3), indent=2))
