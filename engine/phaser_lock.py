"""engine/phaser_lock.py — HM-PHASER-LOCK Highest-Conviction Setup Generator (Phase 1).

Funnels the universe to OT's single sharpest daily setup (Trade of the Day) plus up
to 2 ranked backups. FAIL-CLOSED: if nothing clears the bar, it says "no qualifying
setup" — never forces a pick. Pairs with HM-REVEILLE (broad brief) as the sharp spear.

Funnel: universe (liquid only) -> setup detection (entry/stop/target + R:R from the
strategy-convergence plane) -> conviction score -> risk-definability gate -> top-N -> narrative.

CONVICTION (encodes the Admiral's spec, 2026-06-08):
  conviction = (W_PAT*pattern_strength + W_FLEET*fleet_agreement_indep) * cross_source_mult
  - pattern_strength       = best strategy-setup confidence on the ticker (0-1).
  - fleet_agreement_indep  = distinct strategy FAMILIES converging, normalized — INDEPENDENCE-
                             weighted (families, not raw strategy count) so correlated/duplicate
                             strategies don't linearly inflate. Kills the Chekov flat-count problem.
  - cross_source_mult      = 1.0, x1.3 when a CLEAN (non-SUSPECT) external_picks intel independently
                             names the ticker (rewards BBY-style cross-validation). SUSPECT never counts.
  - RS leg  = computed + DISPLAYED only, never gates live conviction (PBO 0.63 FAIL — observation-only).

GATES (both required; fail-closed): R:R >= RR_FLOOR AND conviction >= CONVICTION_FLOOR.

Discipline: READ-ONLY on sacred DBs (trader.db/arena.db/tractor.db never written; only writes
data/phaser_lock_pick.json). Research/paper SETUPS, NOT live orders. RULE #1 untouched.

v1.1 hooks (documented, not wired): replay_pattern_matcher boost on pattern_strength — deferred
because the live agent-signal plane (trade_signals) is currently shadow-dominated/sparse, so
replay would have little to match. pattern_strength rides strategy-convergence for now.
"""
from __future__ import annotations

import os
import json
import sqlite3
import logging
import statistics
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRADER_DB   = os.path.join(_ROOT, "data", "trader.db")
PICK_JSON   = os.path.join(_ROOT, "data", "phaser_lock_pick.json")
PICK_TOPIC  = os.environ.get("NTFY_PICK_TOPIC", "ollie-pick")

# Tunable bar (Admiral-set 2026-06-08). PHASE 1: floor 1.5 — the strategy substrate bakes
# 1.5:1 targets (0/6220 clear 2:1), so PHASER-LOCK surfaces the sharpest *convergent* setups
# at ~1.5:1 today. PHASE 2 raises this to the true 2:1 once an independent ATR/level target
# model lets it compute its OWN asymmetric target instead of inheriting the strategy's 1.5:1.
RR_FLOOR          = float(os.getenv("PHASER_RR_FLOOR", "1.5"))
CONVICTION_FLOOR  = float(os.getenv("PHASER_CONVICTION_FLOOR", "0.75"))
MAX_PICKS         = int(os.getenv("PHASER_MAX_PICKS", "3"))
W_PAT             = 0.5
W_FLEET           = 0.5
CROSS_SOURCE_MULT = 1.3
FLEET_NORM        = 4          # distinct families that saturate fleet_agreement_indep at 1.0

try:
    from config import OLLIE_URL as _OLLIE_URL
except Exception:  # pragma: no cover
    _OLLIE_URL = "http://192.168.1.168:11434"
OLLAMA_URL    = os.getenv("ADVISORY_OLLAMA_URL", os.getenv("OLLAMA_BASE_URL", _OLLIE_URL))
PHASER_MODEL  = os.getenv("PHASER_LOCK_MODEL", "plutus-v1")


# ── strategy → family (independence grouping) ────────────────────────────────
# Correlated strategies collapse to one family so N of them don't inflate agreement.
_FAMILY_KEYWORDS = [
    ("momentum",      ("momentum", "mom_", "trend", "minervini", "breakout", "52w", "highs")),
    ("mean_reversion",("mean", "revert", "rsi", "oversold", "bounce", "bollinger", "bbkc")),
    ("macd_ma",       ("macd", "sma", "ema", "ma_", "golden", "cross")),
    ("volume_flow",   ("volume", "vol_", "squeeze", "obv", "accumulation", "whale")),
    ("options_gex",   ("gex", "gamma", "oi", "options", "premium", "max_pain")),
    ("relative_strength", ("relative_strength", "rs_", "rs ")),  # observation tier — see note in compute
]


def _family(strategy_name: str) -> str:
    s = (strategy_name or "").lower()
    for fam, kws in _FAMILY_KEYWORDS:
        if any(k in s for k in kws):
            return fam
    return s or "unknown"


# ── inputs (READ-ONLY) ───────────────────────────────────────────────────────

def _today() -> str:
    from engine.market_calendar import az_now
    return az_now().strftime("%Y-%m-%d")


def get_setup_candidates(today: str) -> dict[str, list[dict]]:
    """Group today's strategy_signals by ticker (long/bullish setups with a usable triple)."""
    out: dict[str, list[dict]] = {}
    try:
        conn = sqlite3.connect(TRADER_DB, timeout=15)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT ticker, strategy_name, signal_type, confidence,
                      entry_price, stop_price, target_price
                 FROM strategy_signals
                WHERE scan_date = ?
                  AND entry_price > 0 AND stop_price > 0 AND target_price > 0""",
            (today,),
        ).fetchall()
        conn.close()
        for r in rows:
            st = (r["signal_type"] or "").lower()
            if st and st not in ("buy", "long", "bullish", "bull"):
                continue  # Phase 1 = long setups only
            out.setdefault(r["ticker"].upper(), []).append(dict(r))
    except Exception as e:
        logger.warning("phaser get_setup_candidates failed: %s: %r", type(e).__name__, e)
    return out


def get_rs_map(tickers: set[str]) -> dict[str, float]:
    """rs_rank_blended per ticker — DISPLAY ONLY (PBO fail, never gates)."""
    if not tickers:
        return {}
    try:
        conn = sqlite3.connect(TRADER_DB, timeout=15)
        qs = ",".join("?" * len(tickers))
        rows = conn.execute(
            f"SELECT symbol, rs_rank_blended FROM rs_rank WHERE symbol IN ({qs})",
            tuple(tickers),
        ).fetchall()
        conn.close()
        return {s.upper(): v for s, v in rows if v is not None}
    except Exception as e:
        logger.warning("phaser get_rs_map failed: %s: %r", type(e).__name__, e)
        return {}


def get_cross_source(today: str) -> set[str]:
    """Tickers independently named by CLEAN (non-SUSPECT) external intel — reuses the
    REVEILLE validator so the BBY-style mangled entries can't grant a multiplier."""
    try:
        from engine.reveille import get_recent_picks
        clean, _suspect = get_recent_picks()
        return {(p.get("ticker") or "").upper() for p in clean if p.get("ticker")}
    except Exception as e:
        logger.warning("phaser get_cross_source failed: %s: %r", type(e).__name__, e)
        return set()


# ── scoring ──────────────────────────────────────────────────────────────────

def _aggregate_setup(rows: list[dict]) -> dict | None:
    """Median entry/stop/target across the ticker's converging strategies + R:R.
    Returns None if risk is not cleanly definable (fail-closed at the source)."""
    entries = [r["entry_price"] for r in rows if r.get("entry_price")]
    stops   = [r["stop_price"]  for r in rows if r.get("stop_price")]
    targets = [r["target_price"] for r in rows if r.get("target_price")]
    if not (entries and stops and targets):
        return None
    entry, stop, target = statistics.median(entries), statistics.median(stops), statistics.median(targets)
    if not (entry > stop and target > entry):   # undefinable / inverted risk
        return None
    risk, reward = entry - stop, target - entry
    if risk <= 0:
        return None
    return {"entry": round(entry, 2), "stop": round(stop, 2), "target": round(target, 2),
            "rr": round(reward / risk, 2)}


def score_ticker(ticker: str, rows: list[dict], rs_map: dict, cross: set[str]) -> dict | None:
    setup = _aggregate_setup(rows)
    if setup is None:
        return None
    # pattern strength = best converging-setup confidence (strategy_signals.confidence is 0-1)
    pattern_strength = max((float(r.get("confidence") or 0) for r in rows), default=0.0)
    pattern_strength = max(0.0, min(1.0, pattern_strength))
    # independence-weighted agreement = DISTINCT strategy families (not raw count)
    fams = {_family(r.get("strategy_name")) for r in rows}
    fams.discard("relative_strength")          # RS is observation-only; never gates
    fleet_agreement_indep = min(1.0, len(fams) / FLEET_NORM)
    cross_hit = ticker in cross
    cross_mult = CROSS_SOURCE_MULT if cross_hit else 1.0
    conviction = (W_PAT * pattern_strength + W_FLEET * fleet_agreement_indep) * cross_mult
    return {
        "ticker": ticker, **setup,
        "conviction": round(conviction, 4),
        "pattern_strength": round(pattern_strength, 4),
        "fleet_agreement_indep": round(fleet_agreement_indep, 4),
        "families": sorted(fams),
        "cross_source": cross_hit,
        "rs_rank_blended": rs_map.get(ticker),   # display only
        "n_strategies": len(rows),
    }


def rank_setups() -> dict:
    """Full funnel → fail-closed top-N. Always returns a dict (qualifying may be empty)."""
    today = _today()
    candidates = get_setup_candidates(today)
    rs_map = get_rs_map(set(candidates))
    cross = get_cross_source(today)

    scored = [s for t, rows in candidates.items() if (s := score_ticker(t, rows, rs_map, cross))]
    qualifying = [s for s in scored if s["rr"] >= RR_FLOOR and s["conviction"] >= CONVICTION_FLOOR]
    qualifying.sort(key=lambda s: s["conviction"], reverse=True)
    top = qualifying[:MAX_PICKS]
    return {
        "date": today,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "model": PHASER_MODEL,
        "bar": {"rr_floor": RR_FLOOR, "conviction_floor": CONVICTION_FLOOR},
        "scanned": len(candidates),
        "qualified": len(qualifying),
        "picks": top,
        "no_qualifying_setup": len(top) == 0,
    }


# ── narrative + delivery ───────────────────────────────────────────────────────

_SYSTEM = (
    "You are the XO of the OllieTrades research fleet writing the daily 'Trade of the Day' "
    "setup note. Paper-research SETUPS, never live orders. Be concise and tactical. Ground "
    "every claim in the provided numbers; never invent a figure — write '[gap]' if missing."
)


def _narrative(result: dict) -> str:
    if result["no_qualifying_setup"]:
        return ("NO QUALIFYING SETUP TODAY.\n"
                f"Scanned {result['scanned']} tickers; none cleared the bar "
                f"(R:R >= {RR_FLOOR}, conviction >= {CONVICTION_FLOOR}). No pick forced.")
    lines = ["Write a ToW-style note. #1 is the Trade of the Day; the rest are ranked backups.",
             "Each pick (entry/stop/target/R:R/conviction/why-it-converges):"]
    for i, p in enumerate(result["picks"], 1):
        lines.append(
            f"{i}. {p['ticker']}: entry {p['entry']} stop {p['stop']} target {p['target']} "
            f"R:R {p['rr']} | conviction {p['conviction']} (pattern {p['pattern_strength']}, "
            f"fleet-indep {p['fleet_agreement_indep']} over families {p['families']}, "
            f"cross-source={'yes' if p['cross_source'] else 'no'}, RS={p.get('rs_rank_blended','[gap]')})"
        )
    lines.append("For each: 1-2 lines on the setup, the trigger, the stop logic, and the target. "
                 "Note RS is context only. Plain prose, no preamble.")
    out = _ollama("\n".join(lines))
    return out or "[narrative unavailable — synthesis returned empty; numbers above stand]"


def _ollama(prompt: str, timeout: int = 120) -> str:
    import requests
    try:
        r = requests.post(f"{OLLAMA_URL}/api/generate",
                          json={"model": PHASER_MODEL, "prompt": prompt, "system": _SYSTEM, "stream": False},
                          timeout=timeout)
        if r.ok:
            return (r.json().get("response") or "").strip()
    except Exception as e:
        logger.warning("phaser _ollama failed: %s: %r", type(e).__name__, e)
    return ""


def _headline(result: dict) -> str:
    if result["no_qualifying_setup"]:
        return f"🎯 Trade of the Day {result['date']}: no qualifying setup ({result['scanned']} scanned)"
    p = result["picks"][0]
    return (f"🎯 Trade of the Day {result['date']}: {p['ticker']} "
            f"@ {p['entry']} (stop {p['stop']}, tgt {p['target']}, R:R {p['rr']}, conv {p['conviction']})")


def build_html(result: dict, narrative: str) -> str:
    if result["no_qualifying_setup"]:
        rows_html = "<p><b>No qualifying setup today.</b> No pick forced (fail-closed).</p>"
    else:
        rows = "".join(
            f"<tr><td>{i}</td><td><b>{p['ticker']}</b></td><td>{p['entry']}</td><td>{p['stop']}</td>"
            f"<td>{p['target']}</td><td>{p['rr']}</td><td>{p['conviction']}</td>"
            f"<td>{'✓' if p['cross_source'] else ''}</td><td>{p.get('rs_rank_blended','—')}</td></tr>"
            for i, p in enumerate(result["picks"], 1)
        )
        rows_html = (
            "<table border='1' cellpadding='6' style='border-collapse:collapse'>"
            "<tr><th>#</th><th>Ticker</th><th>Entry</th><th>Stop</th><th>Target</th>"
            "<th>R:R</th><th>Conv</th><th>X-src</th><th>RS*</th></tr>" + rows + "</table>"
            "<p style='color:#888;font-size:12px'>*RS = observation only (PBO fail), does not gate.</p>"
        )
    return (
        f"<div style='font-family:system-ui,Arial,sans-serif;max-width:720px'>"
        f"<h2>🎯 PHASER-LOCK — Trade of the Day</h2>"
        f"<p style='color:#888'>{result['date']} · {result['model']} · bar R:R≥{RR_FLOOR} conv≥{CONVICTION_FLOOR} · "
        f"research/paper SETUP, not a live order — RULE #1 untouched</p>"
        f"{rows_html}<div style='white-space:pre-wrap;line-height:1.5'>{narrative}</div></div>"
    )


def build_pick(deliver_it: bool = False) -> dict:
    result = rank_setups()
    narrative = _narrative(result)
    result["narrative"] = narrative
    result["headline"] = _headline(result)
    result["html"] = build_html(result, narrative)
    try:
        with open(PICK_JSON, "w") as fh:
            json.dump(result, fh, indent=2, default=str)
    except Exception as e:
        logger.warning("phaser pick save failed: %s: %r", type(e).__name__, e)
    if deliver_it:
        from engine import alert_channels as ac
        result["_delivery"] = {
            "ntfy": ac.push_ntfy(PICK_TOPIC, result["headline"], narrative[:1500],
                                 priority="default", tags="dart,ollietrades"),
            "email": ac.send_email(f"Trade of the Day {result['date']}", result["html"]),
        }
    return result


def run_phaser_lock(dry_run: bool = False) -> dict:
    return build_pick(deliver_it=not dry_run)


if __name__ == "__main__":
    import sys
    b = run_phaser_lock(dry_run="--dry-run" in sys.argv)
    print("=== HEADLINE ===\n" + b["headline"])
    print(f"\nscanned={b['scanned']} qualified={b['qualified']} picks={len(b['picks'])} "
          f"no_qualifying={b['no_qualifying_setup']} delivery={b.get('_delivery')}")
    print("\n=== NARRATIVE ===\n" + b["narrative"])
