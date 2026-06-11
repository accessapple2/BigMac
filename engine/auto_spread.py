"""
SWINGDESK-W3 — agent auto-spread gate chain + single submit chokepoint.

BUILT GATED-OFF. No agent can submit a spread until config.AUTO_SPREADS_ENABLED
is flipped True (config edit + canonical restart — there is NO API flip) AND the
proposing agent clears the full gate chain. Every rejection is logged with reason.

THE CHOKEPOINT is submit_if_allowed() — the master gate is enforced there, in ONE
place, before anything else. Proposals reach it via propose() from an agent's
decision flow. Submission routes through the W2 endpoint (Alpaca PAPER only;
RULE #1 Schwab hands-off is never touched).

Gate chain (per proposal):
  1. Master gate ON                          (submit_if_allowed — the chokepoint)
  2. Eligibility: not relative_strength (HARDCODED), whitelisted, active, and the
     strategy is SUPER_MAX-attested
  3. Regime ↔ direction match               (no spreads in UNKNOWN)
  4. Per-trade: chain-resolve + mid computes + width×0.5 ceiling (W2) + ≤$500 debit
  5. Portfolio caps: ≤5 open, ≤3 new/day, ≤$2,500 total open debit (auto_spread_*)
  6. Conviction ≥ 8 from the proposal
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import config

_REPO = Path(__file__).resolve().parent.parent
_DB = _REPO / "data" / "trader.db"
_W2_ENDPOINT = os.getenv("SWINGDESK_W2_URL",
                         "http://127.0.0.1:8889/api/swingdesk/spread/submit")

# HARDCODED permanent exclusion — relative_strength can NEVER be eligible,
# regardless of config.AUTO_SPREAD_WHITELIST / AUTO_SPREAD_VALIDATED_STRATEGIES.
_PERMANENTLY_EXCLUDED = frozenset({"relative_strength"})

# regime → allowed debit-spread direction (engine.regime_ma vocabulary)
_CALL_OK = frozenset({"BULL_CROSS", "CAUTIOUS_BULL"})   # bullish debit call spreads
_PUT_OK = frozenset({"BEAR_CROSS", "CAUTIOUS_BEAR"})    # bearish debit put spreads

_STRATEGY_PREFIX = "auto_spread_"

try:
    from rich.console import Console
    _console = Console()
    def _log(m): _console.log(m)
except Exception:
    def _log(m): print(m)

_flip_notified = False


# ── helpers ──────────────────────────────────────────────────────────────────
def _reject(gate: int, reason: str, proposal: dict) -> dict:
    _log(f"[yellow][AUTO-SPREAD-REJECT] gate={gate} agent={proposal.get('agent_id')} "
         f"strategy={proposal.get('strategy_id')} reason={reason}")
    return {"allowed": False, "submitted": False, "gate": gate, "reason": reason}


def _current_regime() -> str:
    try:
        from engine.regime_ma import detect_ma_cross_regime
        return (detect_ma_cross_regime() or {}).get("regime") or "UNKNOWN"
    except Exception as e:
        _log(f"[yellow][AUTO-SPREAD] regime read failed: {type(e).__name__}: {e}")
        return "UNKNOWN"


def _is_active(agent_id: str) -> bool:
    try:
        conn = sqlite3.connect(str(_DB), timeout=10)
        row = conn.execute(
            "SELECT COALESCE(halt_mode,'active') FROM ai_players WHERE id=?",
            (agent_id,)).fetchone()
        conn.close()
        return bool(row) and row[0] == "active"
    except Exception:
        return False


def _open_auto_spread_stats() -> tuple[int, float, int]:
    """(open_count, total_open_debit_$, new_today) over auto_spread_* rows."""
    conn = sqlite3.connect(str(_DB), timeout=10)
    try:
        oc, td = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(entry_credit_debit),0) FROM options_trades "
            "WHERE strategy_id LIKE ? AND status='open'", (_STRATEGY_PREFIX + "%",)
        ).fetchone()
        nt = conn.execute(
            "SELECT COUNT(*) FROM options_trades WHERE strategy_id LIKE ? "
            "AND date(entry_date)=date('now','localtime')", (_STRATEGY_PREFIX + "%",)
        ).fetchone()[0]
        # entry_credit_debit is per-share net; ×100 → per-contract $ (qty=1 in W3).
        return int(oc or 0), float(td or 0) * 100.0, int(nt or 0)
    finally:
        conn.close()


def _w2_call(legs, strategy: str, structure: str, net_debit_limit, dry_run: bool) -> dict:
    import requests
    try:
        r = requests.post(_W2_ENDPOINT, json={
            "legs": legs, "net_debit_limit": net_debit_limit, "qty": 1,
            "structure": structure, "strategy": strategy, "dry_run": dry_run,
        }, timeout=30)
        if r.status_code != 200:
            return {"ok": False, "error": f"HTTP {r.status_code}: {r.text[:200]}"}
        return r.json()
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def _ntfy(message: str, level=None, alert_type: str = "auto-spread") -> None:
    try:
        from engine.alert_channels import send_alert, AlertLevel
        send_alert(message=message, level=level or AlertLevel.WARNING,
                   alert_type=alert_type, audience="admin",
                   bypass_rate_limit=True, rate_limit_secs=0)
    except Exception as e:
        _log(f"[yellow][AUTO-SPREAD] NTFY failed: {type(e).__name__}: {e}")


# ── gate chain (gates 2,3,5,6 — pure, no submission) ─────────────────────────
def evaluate_proposal(proposal: dict) -> dict:
    """Gates 2/3/5/6 (master + per-trade W2 gate 4 live in submit_if_allowed).
    Returns {allowed: True, ...} or {allowed: False, gate, reason}."""
    agent = proposal.get("agent_id", "")
    strat = proposal.get("strategy_id", "")
    direction = str(proposal.get("direction", "")).lower()
    conviction = proposal.get("conviction")

    # Gate 2 — eligibility
    if strat in _PERMANENTLY_EXCLUDED or agent in _PERMANENTLY_EXCLUDED:
        return _reject(2, "relative_strength is permanently excluded (hardcoded)", proposal)
    if agent not in (getattr(config, "AUTO_SPREAD_WHITELIST", []) or []):
        return _reject(2, f"agent {agent!r} not whitelisted", proposal)
    if not _is_active(agent):
        return _reject(2, f"agent {agent!r} not active (halted/exit_only/missing)", proposal)
    if strat not in (getattr(config, "AUTO_SPREAD_VALIDATED_STRATEGIES", []) or []):
        return _reject(2, f"strategy {strat!r} not SUPER_MAX-attested", proposal)

    # Gate 3 — regime ↔ direction
    if direction not in ("call", "put"):
        return _reject(3, f"direction must be call/put (got {direction!r})", proposal)
    regime = _current_regime()
    if regime == "UNKNOWN":
        return _reject(3, "regime UNKNOWN — no spreads", proposal)
    if direction == "call" and regime not in _CALL_OK:
        return _reject(3, f"debit call spread not allowed in regime {regime}", proposal)
    if direction == "put" and regime not in _PUT_OK:
        return _reject(3, f"debit put spread not allowed in regime {regime}", proposal)

    # Gate 6 — conviction
    try:
        if float(conviction) < config.AUTO_SPREAD_MIN_CONVICTION:
            return _reject(6, f"conviction {conviction} < {config.AUTO_SPREAD_MIN_CONVICTION}", proposal)
    except (TypeError, ValueError):
        return _reject(6, f"conviction missing/non-numeric ({conviction!r})", proposal)

    # Gate 5 — count-based portfolio caps (debit caps need the resolved quote → gate 4)
    oc, td, nt = _open_auto_spread_stats()
    if oc >= config.AUTO_SPREAD_MAX_OPEN:
        return _reject(5, f"open auto-spreads {oc} >= cap {config.AUTO_SPREAD_MAX_OPEN}", proposal)
    if nt >= config.AUTO_SPREAD_MAX_NEW_PER_DAY:
        return _reject(5, f"new-today {nt} >= cap {config.AUTO_SPREAD_MAX_NEW_PER_DAY}", proposal)

    return {"allowed": True, "regime": regime, "open": oc, "total_debit": td, "new_today": nt}


# ── THE chokepoint ───────────────────────────────────────────────────────────
def submit_if_allowed(proposal: dict) -> dict:
    """The single submit chokepoint. Master gate is enforced HERE, first."""
    # ── GATE 1 — MASTER. The one place OFF blocks everything. ──
    if not getattr(config, "AUTO_SPREADS_ENABLED", False):
        _log(f"[dim][AUTO-SPREAD-BLOCK] master gate OFF — dropped proposal from "
             f"{proposal.get('agent_id')}")
        return {"submitted": False, "blocked": "master_gate_off", "gate": 1}

    ev = evaluate_proposal(proposal)
    if not ev.get("allowed"):
        return {"submitted": False, "blocked": ev["reason"], "gate": ev["gate"]}

    agent = proposal["agent_id"]
    legs = proposal.get("legs")
    structure = proposal.get("structure", "vertical_spread")
    strategy_tag = f"{_STRATEGY_PREFIX}{agent}"

    # ── GATE 4 — per-trade, via a W2 dry-run (chain resolve + mid + width ceiling) ──
    dry = _w2_call(legs, strategy_tag, structure, net_debit_limit=None, dry_run=True)
    if not dry.get("ok"):
        return _reject(4, f"W2 dry-run failed: {dry.get('reason') or dry.get('error')}", proposal)
    debit = dry.get("net_debit")
    if debit is None:                          # quote mid must compute
        return _reject(4, "quote mid did not compute (None) — reject", proposal)
    per_trade_cost = float(debit) * 100.0      # qty=1
    if per_trade_cost > config.AUTO_SPREAD_MAX_DEBIT_PER_TRADE:
        return _reject(4, f"per-trade debit ${per_trade_cost:.0f} > "
                          f"${config.AUTO_SPREAD_MAX_DEBIT_PER_TRADE:.0f}", proposal)

    # ── GATE 5 (cont.) — total open debit cap, needs the resolved quote ──
    oc, td, nt = _open_auto_spread_stats()
    if td + per_trade_cost > config.AUTO_SPREAD_MAX_TOTAL_DEBIT:
        return _reject(5, f"total open debit ${td:.0f}+${per_trade_cost:.0f} > "
                          f"${config.AUTO_SPREAD_MAX_TOTAL_DEBIT:.0f}", proposal)

    # ── ALL GATES PASS → live submit through W2 (Alpaca PAPER) ──
    live = _w2_call(legs, strategy_tag, structure, net_debit_limit=None, dry_run=False)
    if live.get("submitted"):
        _ntfy(f"AUTO-SPREAD SUBMITTED: {agent} {structure} {proposal.get('direction')} "
              f"debit=${per_trade_cost:.0f} order={live.get('broker_order_id')}",
              alert_type="auto-spread-submit")
    else:
        _log(f"[yellow][AUTO-SPREAD] W2 live submit returned: {live}")
    return live


def propose(proposal: dict) -> dict:
    """Entry point an agent's decision flow calls to propose an auto-spread.
    Routes straight to the chokepoint (which enforces the master gate first)."""
    return submit_if_allowed(proposal)


# ── module-level daemon (lifecycle doctrine: bound in main.py, not lazy) ──────
def run_auto_spread_cycle() -> dict:
    """One scheduler tick. Heartbeat + master-gate-flip detection. When the gate
    is ON it drains proposals from whitelisted agents (none until whitelisted —
    agents call propose() directly; W3 ships no autonomous scan)."""
    global _flip_notified
    on = getattr(config, "AUTO_SPREADS_ENABLED", False)
    if on and not _flip_notified:
        _flip_notified = True
        _ntfy("⚠️ AUTO_SPREADS master gate observed ON (flip detected) — agent "
              "auto-spreads can now reach Alpaca paper. If unintended, revert "
              "config.AUTO_SPREADS_ENABLED=False + restart.",
              alert_type="auto-spread-gate-flip")
    if not on:
        _log("[dim][AUTO-SPREAD] heartbeat — master gate OFF (0 proposals processed)")
        return {"gate": "off"}
    proposals = _gather_proposals()
    submitted = sum(1 for p in proposals if submit_if_allowed(p).get("submitted"))
    _log(f"[AUTO-SPREAD] cycle ON — {len(proposals)} proposals, {submitted} submitted")
    return {"gate": "on", "processed": len(proposals), "submitted": submitted}


def _gather_proposals() -> list:
    """Integration seam for when the Admiral whitelists agents. Empty whitelist
    yields nothing; agents propose() directly from their decision flow in W3."""
    return []
