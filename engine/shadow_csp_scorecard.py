"""engine/shadow_csp_scorecard.py — HM-SHADOW-CSP scoring + graduation (2026-06-07).

The options-income bake-off's scoring substrate. The LIVE scorecard
(engine/agent_scorecard.py) reads the `trades` table and is BLIND to
`options_trades` — so the shadow CSP seats (which live only in options_trades,
book_tag='ghost') need their own read-only scorecard. This is it.

Metric: return-on-collateral per CLOSED CSP = pnl / (strike × 100 × contracts).
CSP rows carry max_loss=NULL (underlying-dependent), so collateral is derived
from the short-put leg, not max_loss.

Compared seats:
  • shadow-plutus-csp  (ghost)  — candidate A (replicate edge on plutus-v1)
  • shadow-qwen35-csp  (ghost)  — candidate B (fresh model)
  • options-sosnoff    (fleet)  — BASELINE: Troi's proven deterministic wheel

Validation reuses strategies/validation.py exactly (same DSR/PBO the live
scorecard uses). Graduation is REPORT-ONLY: DSR ≥ 0.95 AND PBO ≤ 0.30 AND
N_closes ≥ GRADUATE_N. The Admiral flips ghost→fleet manually; nothing here
auto-graduates or executes.

CAVEAT (load-bearing, surfaced in the design review): PBO needs a non-degenerate
config universe. Two or three agent columns is the degenerate "coin-flip" case
our own doctrine warns about — a real PBO needs a strike/delta/DTE config grid,
not just the seats. This module computes PBO when it can and LABELS it degenerate
when the column count is too small; do not graduate on a degenerate PBO.

Read-only: no writes, no execution.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
TRADER_DB = _ROOT / "data" / "trader.db"

GRADUATE_N = 30   # CSP closes required before a graduation verdict is meaningful

SHADOW_SEATS = {
    "shadow-plutus-csp": "ghost",
    "shadow-qwen35-csp": "ghost",
}
BASELINE_AGENT = "options-sosnoff"   # Troi's wheel (fleet) — the proven CSP seat
BASELINE_BOOK = "fleet"

_PBO_MIN_COLUMNS_NONDEGENERATE = 4   # below this, PBO is a coin-flip artifact


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(str(TRADER_DB), timeout=15)
    c.row_factory = sqlite3.Row
    return c


def _collateral(legs_json: str) -> float | None:
    """Cash-secured collateral from the short-put leg: strike × 100 × qty."""
    try:
        legs = json.loads(legs_json)
    except (json.JSONDecodeError, TypeError):
        return None
    for leg in legs:
        if leg.get("side") == "short" and leg.get("type") == "put":
            strike = float(leg.get("strike") or 0)
            qty = int(leg.get("qty") or 0)
            if strike > 0 and qty > 0:
                return strike * 100 * qty
    return None


def _roc(row: sqlite3.Row) -> float | None:
    """Return-on-collateral for one closed CSP. Clipped to ±5 (basis-error guard)."""
    if row["pnl"] is None:
        return None
    coll = _collateral(row["legs_json"])
    if not coll:
        return None
    return max(-5.0, min(5.0, float(row["pnl"]) / coll))


def _closed_csps(agent_id: str, book_tag: str) -> list[sqlite3.Row]:
    # P0-A item 5, 2026-07-07 (HM-OPTIONS-FILL-INTEGRITY): both the baseline
    # (Troi/options-sosnoff) and both ghost seats built their candidate
    # premiums from the SAME synthetic VIX-formula (confirmed byte-identical
    # between engine/wheel_strategy.py and engine/shadow_csp.py before this
    # fix) -- the bakeoff was comparing synthetic vs synthetic, not a real
    # edge test. Era-fenced to real-quote trades only; will correctly read
    # zero closes for a while post-ship rather than reproduce the same
    # tainted 95%-WR-style verdict with extra steps. See
    # docs/XO_BACKLOG.md "P0-A: OPTIONS FILL INTEGRITY".
    from engine.paper_trader import TROI_REAL_QUOTES_ERA_START
    c = _conn()
    try:
        return c.execute(
            "SELECT id, agent_id, symbol, pnl, legs_json, entry_date, exit_date "
            "FROM options_trades "
            "WHERE agent_id=? AND book_tag=? AND structure='csp' AND status='closed' "
            "AND exit_date >= ? "
            "ORDER BY exit_date ASC",
            (agent_id, book_tag, TROI_REAL_QUOTES_ERA_START),
        ).fetchall()
    finally:
        c.close()


def _per_agent(agent_id: str, book_tag: str) -> dict:
    rows = _closed_csps(agent_id, book_tag)
    rets, daily = [], {}
    for r in rows:
        rf = _roc(r)
        if rf is None:
            continue
        rets.append(rf)
        day = str(r["exit_date"] or r["entry_date"])[:10]
        daily[day] = daily.get(day, 0.0) + rf
    return {"agent": agent_id, "book": book_tag, "rets": rets,
            "daily": daily, "n_closed": len(rets)}


def _pbo(per_agent: list[dict], V) -> dict:
    """CSCV PBO over agents with closes, on aligned daily ROC. Labels degeneracy."""
    import numpy as np
    elig = [a for a in per_agent if a["daily"]]
    cols = len(elig)
    if cols < 2:
        return {"pbo": None, "error": "<2 agents with closes", "degenerate": True}
    all_days = sorted({d for a in elig for d in a["daily"]})
    if len(all_days) < 4:
        return {"pbo": None, "error": "<4 distinct days", "degenerate": True}
    M = np.array([[a["daily"].get(d, 0.0) for a in elig] for d in all_days], dtype=float)
    try:
        res = V.cscv_pbo(M, n_blocks=min(16, max(2, (len(all_days) // 2) * 2)))
    except Exception as e:
        return {"pbo": None, "error": f"{type(e).__name__}: {e}", "degenerate": True}
    res["columns"] = [a["agent"] for a in elig]
    res["degenerate"] = cols < _PBO_MIN_COLUMNS_NONDEGENERATE
    if res["degenerate"]:
        res["note"] = (f"PBO over {cols} columns is a degenerate coin-flip artifact — "
                       f"needs a strike/delta/DTE config grid (>= "
                       f"{_PBO_MIN_COLUMNS_NONDEGENERATE} columns). Do not graduate on this PBO.")
    return res


def compute() -> dict:
    """Full bake-off scorecard: shadow seats vs Troi baseline, DSR + PBO + verdicts."""
    from strategies import validation as V

    agents = [_per_agent(a, b) for a, b in SHADOW_SEATS.items()]
    baseline = _per_agent(BASELINE_AGENT, BASELINE_BOOK)
    everyone = agents + [baseline]

    # Per-agent metrics + DSR trials (deflate across the whole compared set)
    trials = []
    for a in everyone:
        m = V.trade_metrics(a["rets"]) if a["rets"] else {"n": 0}
        a["metrics"] = m
        if len(a["rets"]) >= 2:
            trials.append({"name": a["agent"], "sharpe": m.get("sharpe_per_trade", 0.0),
                           "T": len(a["rets"]), "skew": m.get("skew", 0.0),
                           "kurt": m.get("kurtosis", 3.0)})
    rank = V.deflate_ranking(trials, n_trials=len(trials)) if trials else {"ranking": [], "sr0": 0}
    dsr_by = {r["name"]: r["dsr"] for r in rank.get("ranking", [])}

    pbo = _pbo(everyone, V)
    pbo_val = pbo.get("pbo")
    pbo_for_gate = None if pbo.get("degenerate") else pbo_val

    seats_out = []
    for a in agents:  # graduation verdicts only for the shadow seats
        dsr = dsr_by.get(a["agent"])
        n_ok = a["n_closed"] >= GRADUATE_N
        verdict = V.graduation_verdict(dsr, pbo_for_gate, name=a["agent"])
        # Layer the N-closes gate on top of DSR/PBO (still report-only).
        if not n_ok:
            verdict["verdict"] = "HOLD"
            verdict["reasons"] = ([f"N_closes {a['n_closed']} < {GRADUATE_N}"]
                                  + verdict.get("reasons", []))
        verdict["n_closed"] = a["n_closed"]
        verdict["n_required"] = GRADUATE_N
        seats_out.append({
            "agent": a["agent"], "book": a["book"], "n_closed": a["n_closed"],
            "win_rate_pct": round(a["metrics"].get("win_rate_pct", 0.0), 1),
            "avg_roc_pct": round(a["metrics"].get("avg_trade_pct", 0.0), 3),
            "total_roc_pct": round(a["metrics"].get("total_return_pct", 0.0), 2),
            "sharpe_per_trade": round(a["metrics"].get("sharpe_per_trade", 0.0), 3),
            "dsr": dsr, "graduation": verdict,
        })

    bm = baseline["metrics"]
    baseline_out = {
        "agent": baseline["agent"], "book": baseline["book"], "n_closed": baseline["n_closed"],
        "win_rate_pct": round(bm.get("win_rate_pct", 0.0), 1),
        "avg_roc_pct": round(bm.get("avg_trade_pct", 0.0), 3),
        "total_roc_pct": round(bm.get("total_return_pct", 0.0), 2),
        "sharpe_per_trade": round(bm.get("sharpe_per_trade", 0.0), 3),
        "dsr": dsr_by.get(baseline["agent"]),
    }

    return {
        "metric": "return-on-collateral (pnl / strike*100*contracts), clipped ±5",
        "graduate_n": GRADUATE_N, "sr0_null": rank.get("sr0"),
        "baseline": baseline_out, "seats": seats_out, "pbo": pbo,
        "note": "report-only — graduation is the Admiral's call; PBO must be non-degenerate.",
    }


def render(sc: dict) -> str:
    L = ["# Shadow CSP Bake-off — return-on-collateral",
         f"_Metric: {sc['metric']} · graduate N={sc['graduate_n']} · SR0(null)={sc.get('sr0_null')}_",
         "",
         "| Seat | Book | Closed | WR% | avgROC% | totROC% | Sharpe | DSR | Verdict |",
         "|------|------|------:|----:|-------:|-------:|------:|----:|---------|"]
    b = sc["baseline"]
    L.append(f"| {b['agent']} (baseline) | {b['book']} | {b['n_closed']} | {b['win_rate_pct']} | "
             f"{b['avg_roc_pct']} | {b['total_roc_pct']} | {b['sharpe_per_trade']} | "
             f"{b['dsr'] if b['dsr'] is not None else '—'} | — |")
    for s in sc["seats"]:
        dsr = f"{s['dsr']:.3f}" if s.get("dsr") is not None else "—"
        L.append(f"| {s['agent']} | {s['book']} | {s['n_closed']} | {s['win_rate_pct']} | "
                 f"{s['avg_roc_pct']} | {s['total_roc_pct']} | {s['sharpe_per_trade']} | "
                 f"{dsr} | {s['graduation']['verdict']} |")
    pbo = sc["pbo"]
    if pbo.get("pbo") is not None:
        tag = "DEGENERATE — do not graduate" if pbo.get("degenerate") else ("FRAGILE >0.30" if pbo.get("fragile") else "OK")
        L += ["", f"**PBO: {pbo['pbo']}** ({tag}; columns={pbo.get('columns')})"]
    else:
        L += ["", f"**PBO: n/a** ({pbo.get('error')})"]
    L += ["", "## Graduation reasons (report-only)"]
    for s in sc["seats"]:
        g = s["graduation"]
        L.append(f"- **{s['agent']}** → {g['verdict']} "
                 f"(N {g['n_closed']}/{g['n_required']}): {'; '.join(g.get('reasons', [])) or 'all gates pass'}")
    L.append("")
    L.append(f"_{sc['note']}_")
    return "\n".join(L)


if __name__ == "__main__":
    print(render(compute()))
