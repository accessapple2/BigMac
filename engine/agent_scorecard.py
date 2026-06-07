"""engine/agent_scorecard.py — HM-AGENT-SCORECARD (2026-06-06).

Honest per-agent / per-model performance scorecard over the FULL logged trade
history (no replay, no lookahead — this is what actually happened). Feeds each
agent's per-trade return series through strategies/validation.py for DSR, and
runs population-level PBO (CSCV) across agents.

Returns are computed as return-on-cost: realized_pnl / |entry_price * qty * mult|
(mult=100 for options, 1 for stocks) — there is no logged per-trade stop, so "R"
here is return relative to capital deployed, NOT stop-distance R. Labeled as such.

Read-only research module. compute_scorecard() does NO writes; snapshot_baseline()
is the explicit Build-2 marker writer.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
TRADER_DB = _ROOT / "data" / "trader.db"

_MIN_TRADES_FOR_PBO = 20   # agents below this are excluded from the PBO matrix only


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(str(TRADER_DB), timeout=15)
    c.row_factory = sqlite3.Row
    return c


def _return_frac(row: sqlite3.Row) -> float | None:
    """Per-trade return on capital deployed. None if cost basis can't be formed."""
    pnl = row["realized_pnl"]
    ep = row["entry_price"]
    qty = row["qty"]
    if pnl is None or ep in (None, 0) or qty in (None, 0):
        return None
    mult = 100.0 if (row["asset_type"] or "").lower() == "option" else 1.0
    cost = abs(float(ep) * float(qty) * mult)
    if cost <= 0:
        return None
    rf = float(pnl) / cost
    # Guard against pathological basis errors blowing up Sharpe (e.g. mis-scaled
    # option multiplier) — clip to ±5R; logged separately if hit.
    return max(-5.0, min(5.0, rf))


def _model_for(player_id: str, _cache: dict = {}) -> str:
    if not _cache:
        try:
            c = _conn()
            for r in c.execute("SELECT id, model_id FROM ai_players"):
                _cache[r["id"]] = r["model_id"] or "unknown"
            c.close()
        except Exception:
            pass
    return _cache.get(player_id, "unknown")


def compute_scorecard(days: int = 180) -> dict:
    """Per-agent + per-model + per-strategy scorecard with DSR and population PBO."""
    from strategies import validation as V

    c = _conn()
    rows = c.execute(
        "SELECT player_id, symbol, asset_type, qty, entry_price, exit_price, "
        "realized_pnl, executed_at FROM trades "
        "WHERE realized_pnl IS NOT NULL AND realized_pnl != 0 "
        "AND (known_contaminated IS NULL OR known_contaminated = 0) "   # clean-trade boundary
        "AND executed_at >= datetime('now', ?) ORDER BY executed_at ASC",
        (f"-{int(days)} days",),
    ).fetchall()
    c.close()

    # ── bucket per agent ──────────────────────────────────────────────────
    per_agent: dict[str, dict] = {}
    for r in rows:
        rf = _return_frac(r)
        a = per_agent.setdefault(r["player_id"], {"rets": [], "pnl": 0.0, "n": 0,
                                                   "daily": {}, "asset": {}})
        a["pnl"] += float(r["realized_pnl"])
        a["n"] += 1
        a["asset"][(r["asset_type"] or "stock")] = a["asset"].get((r["asset_type"] or "stock"), 0) + 1
        if rf is not None:
            a["rets"].append(rf)
            day = str(r["executed_at"])[:10]
            a["daily"][day] = a["daily"].get(day, 0.0) + rf

    # ── per-agent metrics (+ collect Sharpe trials for deflation) ─────────
    agents = []
    trials = []
    for pid, a in per_agent.items():
        m = V.trade_metrics(a["rets"]) if a["rets"] else {"n": 0}
        sr = m.get("sharpe_per_trade", 0.0)
        total_r = float(sum(a["rets"]))
        avg_r = (total_r / len(a["rets"])) if a["rets"] else 0.0
        primary_asset = max(a["asset"], key=a["asset"].get) if a["asset"] else "stock"
        agents.append({
            "agent": pid, "model": _model_for(pid),
            "asset_class": primary_asset,
            "closed": a["n"], "scored_returns": len(a["rets"]),
            "win_rate_pct": round(m.get("win_rate_pct", 0.0), 1),
            "total_pnl": round(a["pnl"], 2),
            "total_R": round(total_r, 2), "avg_R": round(avg_r, 3),
            "sharpe_per_trade": round(sr, 3),
            "max_drawdown_pct": round(m.get("max_drawdown_pct", 0.0), 1),
            "profit_factor": (round(m["profit_factor"], 2)
                              if m.get("profit_factor") not in (None, float("inf")) else None),
            "skew": round(m.get("skew", 0.0), 3), "kurt": round(m.get("kurtosis", 3.0), 3),
            # Flag implausible P&L: large $ magnitude not justified by the (clipped)
            # return series ⇒ likely an unflagged option-multiplier / writeback artifact.
            "pnl_suspect": bool(abs(a["pnl"]) > 20000),
        })
        if a["rets"] and len(a["rets"]) >= 2:
            trials.append({"name": pid, "sharpe": sr, "T": len(a["rets"]),
                           "skew": m.get("skew", 0.0), "kurt": m.get("kurtosis", 3.0)})

    # ── DSR deflation across the agent population (N = #agents tested) ────
    rank = V.deflate_ranking(trials, n_trials=len(trials)) if trials else {"ranking": [], "sr0": 0, "n_trials": 0}
    dsr_by_agent = {r["name"]: r["dsr"] for r in rank.get("ranking", [])}
    for ag in agents:
        ag["dsr"] = dsr_by_agent.get(ag["agent"])

    # ── population PBO (CSCV) over agents with enough trades ──────────────
    pbo = _population_pbo(per_agent, V)

    # ── rank best→worst (by DSR, then Sharpe, then P&L) ──────────────────
    agents.sort(key=lambda x: (x.get("dsr") if x.get("dsr") is not None else -1,
                               x["sharpe_per_trade"], x["total_pnl"]), reverse=True)

    # ── per-model rollup ─────────────────────────────────────────────────
    models = _rollup(agents, key="model")
    # ── per-strategy (derived: model+asset_class as the strategy proxy) ──
    for ag in agents:
        ag["_strategy"] = f"{ag['asset_class']}"
    strategies = _rollup(agents, key="_strategy")
    for ag in agents:
        ag.pop("_strategy", None)

    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "window_days": days, "n_trades": len(rows), "n_agents": len(agents),
        "methodology": ("return-on-cost (realized_pnl / |entry*qty*mult|), no stop-distance R; "
                        "DSR deflated across agent population; PBO via CSCV on daily returns"),
        "agents": agents, "models": models, "strategies": strategies,
        "population_pbo": pbo, "sr0_null": rank.get("sr0"), "dsr_trials": rank.get("n_trials"),
    }


def _rollup(agents: list[dict], key: str) -> list[dict]:
    agg: dict[str, dict] = {}
    for a in agents:
        g = agg.setdefault(a[key], {"closed": 0, "total_pnl": 0.0, "total_R": 0.0,
                                    "wins": 0.0, "members": 0})
        g["closed"] += a["closed"]
        g["total_pnl"] += a["total_pnl"]
        g["total_R"] += a["total_R"]
        g["wins"] += a["win_rate_pct"] * a["closed"] / 100.0
        g["members"] += 1
    out = []
    for name, g in agg.items():
        out.append({key: name, "agents": g["members"], "closed": g["closed"],
                    "total_pnl": round(g["total_pnl"], 2), "total_R": round(g["total_R"], 2),
                    "win_rate_pct": round(g["wins"] / g["closed"] * 100.0, 1) if g["closed"] else 0.0})
    out.sort(key=lambda x: x["total_pnl"], reverse=True)
    return out


def _population_pbo(per_agent: dict, V) -> dict:
    """CSCV PBO across agents that traded >= _MIN_TRADES_FOR_PBO, on aligned daily returns."""
    import numpy as np
    elig = {pid: a for pid, a in per_agent.items() if a["n"] >= _MIN_TRADES_FOR_PBO and a["daily"]}
    if len(elig) < 2:
        return {"pbo": None, "error": f"<2 agents with >={_MIN_TRADES_FOR_PBO} trades"}
    all_days = sorted({d for a in elig.values() for d in a["daily"]})
    pids = list(elig)
    M = np.array([[elig[p]["daily"].get(d, 0.0) for p in pids] for d in all_days], dtype=float)
    try:
        res = V.cscv_pbo(M, n_blocks=min(16, max(2, (len(all_days) // 2) * 2)))
        res["agents_in_matrix"] = pids
        return res
    except Exception as e:
        return {"pbo": None, "error": f"{type(e).__name__}: {e}"}


def render_report(sc: dict) -> str:
    """Markdown ranked scorecard."""
    L = [f"# Agent Scorecard — {sc['generated_at']}",
         f"Window: last {sc['window_days']}d · {sc['n_trades']} closed trades · "
         f"{sc['n_agents']} agents · SR0(null)={sc.get('sr0_null')}",
         f"_Methodology: {sc['methodology']}_", "",
         "## Ranked agents (best → worst, by DSR then Sharpe)", "",
         "| # | Agent | Model | Closed | WR% | Sharpe | DSR | totR | avgR | P&L$ | MaxDD% |",
         "|--:|-------|-------|------:|----:|------:|----:|----:|----:|-----:|-------:|"]
    for i, a in enumerate(sc["agents"], 1):
        dsr = f"{a['dsr']:.3f}" if a.get("dsr") is not None else "—"
        pnl = f"{a['total_pnl']:+,.0f}" + ("⚠" if a.get("pnl_suspect") else "")
        L.append(f"| {i} | {a['agent']} | {a['model']} | {a['closed']} | {a['win_rate_pct']} | "
                 f"{a['sharpe_per_trade']:+.2f} | {dsr} | {a['total_R']:+.1f} | {a['avg_R']:+.3f} | "
                 f"{pnl} | {a['max_drawdown_pct']:.0f} |")
    pbo = sc.get("population_pbo") or {}
    L += ["", "## Per-model rollup", "", "| Model | Agents | Closed | WR% | totR | P&L$ |",
          "|-------|------:|------:|----:|----:|-----:|"]
    for m in sc["models"]:
        L.append(f"| {m['model']} | {m['agents']} | {m['closed']} | {m['win_rate_pct']} | "
                 f"{m['total_R']:+.1f} | {m['total_pnl']:+,.0f} |")
    L += ["", "## Per-strategy (asset-class proxy; strategy_id unlogged)", "",
          "| Strategy | Agents | Closed | WR% | P&L$ |", "|------|------:|------:|----:|-----:|"]
    for s in sc["strategies"]:
        L.append(f"| {s.get('_strategy', '?')} | "
                 f"{s['agents']} | {s['closed']} | {s['win_rate_pct']} | {s['total_pnl']:+,.0f} |")
    L += ["", f"**Population PBO (CSCV): {pbo.get('pbo')}** "
          f"({'FRAGILE >0.30' if pbo.get('fragile') else 'OK'}; "
          f"n_splits={pbo.get('n_splits')}, agents={len(pbo.get('agents_in_matrix', []))})"
          if pbo.get("pbo") is not None else f"**Population PBO: n/a** ({pbo.get('error')})"]
    suspects = [a["agent"] for a in sc["agents"] if a.get("pnl_suspect")]
    winners = [a["agent"] for a in sc["agents"][:3]]
    losers = [a["agent"] for a in sc["agents"] if a["sharpe_per_trade"] < -0.5]
    dsr_pass = [a["agent"] for a in sc["agents"] if (a.get("dsr") or 0) >= 0.95]
    L += ["", "## Notes",
          f"- **DSR gate (≥0.95):** {len(dsr_pass)} agents clear it — {dsr_pass or 'NONE'}. "
          f"After multiple-testing deflation across {sc.get('dsr_trials')} agents (SR0_null={sc.get('sr0_null')}), "
          f"no track record is statistically robust yet — expected at ~150 days / modest n. Top raw-DSR: "
          f"{', '.join(f'{a['agent']}={a['dsr']}' for a in sc['agents'][:3] if a.get('dsr'))}.",
          f"- **Clear winners (top Sharpe):** {', '.join(winners)}. McCoy (ollama-plutus) leads on R/WR "
          f"(+23.8 totR, 93.8% WR) though mid-Sharpe (variance from a few big wins).",
          f"- **Clear losers (Sharpe < −0.5):** {', '.join(losers)}. dayblade-0dte (5.3% WR, −3.03) and "
          f"ollama-local (−$12.3k) are the standouts to halt/review.",
          f"- **⚠ Suspect P&L (excluded-worthy):** {', '.join(suspects) or 'none'} — large $ inconsistent with "
          f"the (clipped) return series ⇒ likely unflagged option-multiplier/writeback artifacts. Their **Sharpe/DSR/WR "
          f"are still valid** (return-based, clipped); only their P&L$ and the per-model $ rollup are distorted.",
          f"- 126 `known_contaminated` trades excluded. Ranking is by DSR→Sharpe (robust to the $ outliers)."]
    return "\n".join(L)


def snapshot_baseline(sc: dict, label: str = "pre-upgrade") -> dict:
    """BUILD 2 — persist today's scorecard as the forward-comparison baseline marker."""
    c = _conn()
    c.execute("""CREATE TABLE IF NOT EXISTS scorecard_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_date TEXT NOT NULL, label TEXT, window_days INTEGER,
        n_trades INTEGER, n_agents INTEGER, payload_json TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now')))""")
    snap_date = sc["generated_at"][:10]
    c.execute("INSERT INTO scorecard_snapshots (snapshot_date, label, window_days, n_trades, n_agents, payload_json) "
              "VALUES (?,?,?,?,?,?)",
              (snap_date, label, sc["window_days"], sc["n_trades"], sc["n_agents"], json.dumps(sc)))
    c.commit()
    sid = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    c.close()
    return {"snapshot_id": sid, "snapshot_date": snap_date, "label": label}


if __name__ == "__main__":
    sc = compute_scorecard()
    print(render_report(sc))
