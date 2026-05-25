"""HM-RISK-MANAGER-CONVICTION-STOP-WIRE Phase 5 — 30-day A/B backtest.

For each player in RiskManager.AI_SIGNAL_PLAYERS, runs two guarded
backtests over the same 30-day window using the same signal stream,
differing only in stop logic:

  SCALED:  engine.stops.get_stop_loss_pct(conviction)   (0.08/0.12/0.15/0.18)
  FLAT:    0.12                                          (production default)

Per-player metrics: net P&L, profit factor, max drawdown %, win rate,
total trades, avg winner, avg loser. Fleet aggregate at the bottom.

Acceptance gates (per mission brief):
  G1: Net fleet P&L improves OR stays within +/-5% of flat baseline
  G2: No agent's max DD grows >5 percentage points vs flat
  G3: Profit factor improves on >=6 of 9 active agents
  G4: ZERO agent shows >50% drop in trade count vs flat

Exits 0 if all gates pass, 1 otherwise.

Local-only — backtester uses Yahoo historical prices via HTTP. No Alpaca
calls. No DB writes.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.backtester import backtest_player  # noqa: E402
from engine.risk_manager import RiskManager  # noqa: E402

DAYS = 30
FLAT_STOP = 0.12


def _max_drawdown_pct(equity_curve: list) -> float:
    """Max drawdown as percent of peak. equity_curve is [{day, value}, ...]."""
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]["value"]
    max_dd = 0.0
    for pt in equity_curve:
        v = pt["value"]
        if v > peak:
            peak = v
        if peak > 0:
            dd = (peak - v) / peak * 100.0
            if dd > max_dd:
                max_dd = dd
    return max_dd


def _profit_factor(trades: list) -> float | None:
    """sum(wins) / abs(sum(losses)). None if no losses (degenerate)."""
    wins_sum = sum(t["pnl"] for t in trades if t["pnl"] > 0)
    loss_sum = sum(t["pnl"] for t in trades if t["pnl"] <= 0)
    if loss_sum >= 0:
        return None
    return wins_sum / abs(loss_sum)


def _agg(result: dict) -> dict:
    """Extract comparison metrics from a backtest_player result."""
    trades = result.get("trades", [])
    stats = result.get("stats", {})
    eq = result.get("equity_curve", [])
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    return {
        "trades": stats.get("total_trades", 0),
        "pnl": stats.get("total_pnl", 0.0),
        "win_rate": stats.get("win_rate", 0.0),
        "max_dd_pct": _max_drawdown_pct(eq),
        "profit_factor": _profit_factor(trades),
        "avg_winner": (sum(t["pnl"] for t in wins) / len(wins)) if wins else 0.0,
        "avg_loser": (sum(t["pnl"] for t in losses) / len(losses)) if losses else 0.0,
    }


def _fmt_pf(pf: float | None) -> str:
    if pf is None:
        return "  inf"
    return f"{pf:5.2f}"


def main() -> int:
    players = sorted(RiskManager.AI_SIGNAL_PLAYERS)
    print(f"Phase 5 A/B backtest: {len(players)} AI-signal players, {DAYS}-day window")
    print(f"  SCALED uses engine.stops.get_stop_loss_pct (0.08/0.12/0.15/0.18 by tier)")
    print(f"  FLAT   uses {FLAT_STOP:.2f} (production pre-wire default)")
    print()

    rows = []
    for i, pid in enumerate(players, 1):
        print(f"[{i:2}/{len(players)}] {pid:<22} ", end="", flush=True)
        try:
            scaled = backtest_player(pid, days=DAYS, apply_guardrails=True)
            flat = backtest_player(pid, days=DAYS, apply_guardrails=True,
                                   flat_stop_pct=FLAT_STOP)
        except Exception as e:
            print(f"ERROR: {type(e).__name__}: {e}")
            continue

        if scaled.get("error") or flat.get("error"):
            print(f"player error: {scaled.get('error') or flat.get('error')}")
            continue

        s = _agg(scaled)
        f = _agg(flat)
        rows.append({"player_id": pid, "scaled": s, "flat": f})
        print(f"scaled trades={s['trades']:>3} pnl={s['pnl']:>8.0f} | "
              f"flat trades={f['trades']:>3} pnl={f['pnl']:>8.0f}")

    print()
    print("=" * 130)
    print(f"{'player':<22} | {'SCALED trades':>13} {'pnl':>9} {'wr%':>6} "
          f"{'dd%':>6} {'pf':>6} | {'FLAT trades':>11} {'pnl':>9} {'wr%':>6} "
          f"{'dd%':>6} {'pf':>6}")
    print("-" * 130)
    for r in rows:
        s, f = r["scaled"], r["flat"]
        print(f"{r['player_id']:<22} | "
              f"{s['trades']:>13} {s['pnl']:>9.0f} {s['win_rate']:>6.1f} "
              f"{s['max_dd_pct']:>6.1f} {_fmt_pf(s['profit_factor']):>6} | "
              f"{f['trades']:>11} {f['pnl']:>9.0f} {f['win_rate']:>6.1f} "
              f"{f['max_dd_pct']:>6.1f} {_fmt_pf(f['profit_factor']):>6}")

    if not rows:
        print("\nNo players produced backtest results — cannot evaluate gates.")
        return 1

    fleet_scaled_pnl = sum(r["scaled"]["pnl"] for r in rows)
    fleet_flat_pnl = sum(r["flat"]["pnl"] for r in rows)
    fleet_pnl_delta_pct = ((fleet_scaled_pnl - fleet_flat_pnl) / abs(fleet_flat_pnl) * 100
                           if fleet_flat_pnl != 0 else 0.0)

    print("-" * 130)
    print(f"{'FLEET TOTAL':<22} | "
          f"{'':>13} {fleet_scaled_pnl:>9.0f}{'':>27} | "
          f"{'':>11} {fleet_flat_pnl:>9.0f}{'':>20}")
    print(f"{'fleet pnl delta':<22}   "
          f"scaled - flat = {fleet_scaled_pnl - fleet_flat_pnl:+.0f}  "
          f"({fleet_pnl_delta_pct:+.1f}%)")

    active_rows = [r for r in rows if r["scaled"]["trades"] > 0 or r["flat"]["trades"] > 0]

    gates = {}

    # G1: net fleet P&L improves OR within +/-5%
    gates["G1_fleet_pnl"] = {
        "pass": (fleet_scaled_pnl >= fleet_flat_pnl) or abs(fleet_pnl_delta_pct) <= 5.0,
        "detail": f"fleet pnl delta {fleet_pnl_delta_pct:+.1f}%  "
                  f"(scaled {fleet_scaled_pnl:+.0f} vs flat {fleet_flat_pnl:+.0f})",
    }

    # G2: no agent's max DD grows >5pp
    g2_violators = []
    for r in active_rows:
        dd_delta = r["scaled"]["max_dd_pct"] - r["flat"]["max_dd_pct"]
        if dd_delta > 5.0:
            g2_violators.append((r["player_id"], dd_delta, r["scaled"]["max_dd_pct"], r["flat"]["max_dd_pct"]))
    gates["G2_max_dd"] = {
        "pass": len(g2_violators) == 0,
        "detail": f"{len(g2_violators)} agent(s) with >5pp DD growth: " +
                  ", ".join(f"{p}(+{d:.1f}pp:{s:.1f}->{f:.1f})" for p, d, s, f in g2_violators)
                  if g2_violators else "no DD violators",
    }

    # G3: profit factor improves on >=6 of 9 active agents
    pf_improvers = 0
    pf_active = 0
    pf_detail = []
    for r in active_rows:
        s_pf = r["scaled"]["profit_factor"]
        f_pf = r["flat"]["profit_factor"]
        if s_pf is None and f_pf is None:
            continue
        pf_active += 1
        s_val = s_pf if s_pf is not None else 999.0
        f_val = f_pf if f_pf is not None else 999.0
        if s_val > f_val:
            pf_improvers += 1
            pf_detail.append(f"{r['player_id']}:{_fmt_pf(f_pf).strip()}->{_fmt_pf(s_pf).strip()}+")
        else:
            pf_detail.append(f"{r['player_id']}:{_fmt_pf(f_pf).strip()}->{_fmt_pf(s_pf).strip()}-")
    gates["G3_profit_factor"] = {
        "pass": pf_improvers >= min(6, pf_active),
        "detail": f"{pf_improvers}/{pf_active} agents improved profit factor "
                  f"(brief threshold >=6 of 9; using >=min(6, active))",
    }

    # G4: zero agent shows >50% drop in trade count vs flat
    g4_violators = []
    for r in active_rows:
        if r["flat"]["trades"] > 0:
            drop = (r["flat"]["trades"] - r["scaled"]["trades"]) / r["flat"]["trades"]
            if drop > 0.5:
                g4_violators.append((r["player_id"], drop, r["scaled"]["trades"], r["flat"]["trades"]))
    gates["G4_trade_count"] = {
        "pass": len(g4_violators) == 0,
        "detail": f"{len(g4_violators)} agent(s) with >50% trade-count drop: " +
                  ", ".join(f"{p}(-{d*100:.0f}%:{s}<-{f})" for p, d, s, f in g4_violators)
                  if g4_violators else "no trade-count violators",
    }

    print()
    print("ACCEPTANCE GATES:")
    all_pass = True
    for gname, g in gates.items():
        status = "PASS" if g["pass"] else "FAIL"
        print(f"  [{status}] {gname}: {g['detail']}")
        if not g["pass"]:
            all_pass = False

    print()
    if all_pass:
        print("GREENLIGHT — all 4 gates pass. Phase 6 can ship feature flag.")
        return 0
    print("HOLD — one or more gates failed. Phase 6 should NOT enable flag.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
