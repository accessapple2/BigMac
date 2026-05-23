"""HM-IC-SQUADRON Pillar 1 — regime-strategy fit matrix.

Encodes which strategies are approved / avoided per market regime. Wired into
paper_trader.buy() upstream of the Grade B fleet gate so regime mismatches are
rejected at the coarse strategy-fit layer before the fine-grained quality
band fires.

Spec: ~/.claude/projects/-Users-bigmac/memory/project_hm_ic_squadron_approved.md

Behavior:
  * check_regime_fit(strategy, regime) → (bool allowed, str reason)
  * Rejections persisted to decision_audit with gate_verdict='regime_mismatch'
  * Fail-safe: unknown regime → allow trade + emit [REGIME-ROUTER-UNKNOWN]
    log line (HM-Z/HM-AA error posture — fail open, log loudly)

The matrix is intentionally hardcoded for the v1 ship. Pillar 5 (nightly
strategy lab sweep) emits `regime_fit_matrix_update.json` which a future
loader will merge into this dict; for now the matrix is the source of truth.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from rich.console import Console

console = Console()

_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "trader.db"


# ---------------------------------------------------------------------------
# Strategy-regime fit matrix
# ---------------------------------------------------------------------------
# Captain spec (Admiral-approved 2026-05-22). Extended with extrapolated
# defaults for the legacy regime taxonomy already present in regime_history
# (BULL_CROSS, CAUTIOUS_BULL, CAUTIOUS_BEAR, BEAR_CROSS, BULL, BEAR).
#
# Schema per row:
#   primary               list[str]  — strategies this regime favors
#   avoid                 list[str]  — strategies that should be rejected
#   ic_enabled            bool       — iron condor squadron allowed?
#   long_equity_max_pct   float      — portfolio cap on long stock exposure
#   hedge_sleeve_pct      float?     — recommended long-put hedge sleeve

REGIME_STRATEGY_MATRIX: dict[str, dict] = {
    "BEAR_CHOPPY": {
        "primary": ["iron_condor", "bull_put_spread", "csp"],
        "avoid": ["long_call", "momentum", "mean_reversion", "bull_call_spread"],
        "ic_enabled": True,
        "long_equity_max_pct": 0.20,
    },
    "BULL_LOW_VOL": {
        "primary": ["long_equity", "bull_put_spread"],
        # IC on broad-index ETFs gets compressed by trending tape;
        # range-bound large caps only.
        "avoid": ["iron_condor_broad_index"],
        "ic_enabled": True,
        "long_equity_max_pct": 0.60,
    },
    "EUPHORIC": {
        # F&G > 80 + VIX < 18 — sell premium + hedge, do not add longs
        "primary": ["reduce_exposure", "sell_premium", "long_puts_hedge"],
        "avoid": ["adding_longs", "momentum"],
        "ic_enabled": True,
        "long_equity_max_pct": 0.40,
        "hedge_sleeve_pct": 0.10,
    },
    # ---- legacy regime taxonomy (regime_history values, extrapolated) ------
    "BULL_CROSS": {
        "primary": ["long_equity", "bull_call_spread", "momentum"],
        "avoid": ["iron_condor_broad_index", "bear_call_spread"],
        "ic_enabled": False,
        "long_equity_max_pct": 0.65,
    },
    "CAUTIOUS_BULL": {
        "primary": ["long_equity", "bull_put_spread", "csp"],
        "avoid": ["aggressive_momentum"],
        "ic_enabled": True,
        "long_equity_max_pct": 0.55,
    },
    "CAUTIOUS_BEAR": {
        "primary": ["bear_call_spread", "csp", "iron_condor"],
        "avoid": ["long_call", "momentum"],
        "ic_enabled": True,
        "long_equity_max_pct": 0.35,
    },
    "BEAR_CROSS": {
        "primary": ["bear_call_spread", "long_puts_hedge"],
        "avoid": ["long_call", "momentum", "long_equity"],
        "ic_enabled": False,
        "long_equity_max_pct": 0.20,
    },
    # Short-form aliases (some legacy paths emit BULL/BEAR without _CROSS)
    "BULL": {
        "primary": ["long_equity", "momentum", "bull_call_spread"],
        "avoid": [],
        "ic_enabled": False,
        "long_equity_max_pct": 0.65,
    },
    "BEAR": {
        "primary": ["bear_call_spread", "long_puts_hedge", "iron_condor"],
        "avoid": ["long_call", "momentum", "long_equity"],
        "ic_enabled": True,
        "long_equity_max_pct": 0.15,
    },
}


# ---------------------------------------------------------------------------
# Strategy label normalization
# ---------------------------------------------------------------------------
# Callers pass varied labels (asset_type='stock', option_type='call',
# strategy_id='iron_condor_squadron_v1', etc.). Map them to matrix keys.

_STRATEGY_ALIASES: dict[str, str] = {
    "stock": "long_equity",
    "equity": "long_equity",
    "long": "long_equity",
    "longequity": "long_equity",
    "call": "long_call",
    "longcall": "long_call",
    "put": "long_put",
    "longput": "long_put",
    "ic": "iron_condor",
    "condor": "iron_condor",
    "iron_condor_v1": "iron_condor",
    "iron_condor_squadron_v1": "iron_condor",
    "bull_call_spread_v1": "bull_call_spread",
    "bear_put_spread_v1": "bear_put_spread",
    "bull_put_spread_v1": "bull_put_spread",
    "bear_call_spread_v1": "bear_call_spread",
    "covered_call": "covered_call",
    "cash_secured_put": "csp",
    "csp": "csp",
}


def _normalize_strategy(strategy: str | None) -> str:
    s = (strategy or "").strip().lower().replace("-", "_").replace(" ", "_")
    return _STRATEGY_ALIASES.get(s, s)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


# HM-EUPHORIC-CLASSIFIER 2026-05-22 — in-memory cache for the F&G + VIX + SPY-200MA
# override probes so get_current_regime() doesn't go to the network every call.
_EUPHORIC_CACHE: dict = {"ts": 0.0, "value": None}
_EUPHORIC_TTL_S = 300  # 5 minutes


def _is_euphoric_now() -> bool:
    """Probe F&G > 80, VIX < 18, SPY > 200-day MA. ALL three must hold.

    Cached 5 minutes to avoid repeating the network calls on every gate
    evaluation. Fail-safe: any probe that errors or returns missing data
    counts as a NO (we never falsely promote to EUPHORIC).
    """
    import time as _time
    now = _time.time()
    if (
        _EUPHORIC_CACHE["value"] is not None
        and now - _EUPHORIC_CACHE["ts"] < _EUPHORIC_TTL_S
    ):
        return bool(_EUPHORIC_CACHE["value"])

    fg_ok = False
    vix_ok = False
    spy_ok = False
    try:
        # F&G — engine.fear_greed wraps the CNN F&G index.
        from engine.fear_greed import compute_score
        fg = compute_score() or {}
        fg_val = fg.get("score") if isinstance(fg, dict) else None
        if fg_val is not None and float(fg_val) > 80.0:
            fg_ok = True
    except Exception:
        fg_ok = False
    try:
        from engine.market_data import get_stock_price
        vix_data = get_stock_price("^VIX") or get_stock_price("VIX") or {}
        vix_val = vix_data.get("price") if isinstance(vix_data, dict) else None
        if vix_val is not None and float(vix_val) < 18.0:
            vix_ok = True
    except Exception:
        vix_ok = False
    try:
        # SPY > 200MA — compare current price to the 200-day SMA. Avoid
        # pulling a full historical series here; instead use a thin helper
        # if available, else skip the criterion (fail-safe: NO override).
        from engine.market_data import get_stock_price
        spy = get_stock_price("SPY") or {}
        spy_price = spy.get("price") if isinstance(spy, dict) else None
        # Try a cheap 200MA via regime_ma cache if it exists.
        try:
            from engine.regime_ma import get_200ma_for as _g200
            spy_200 = _g200("SPY") if callable(_g200) else None
        except Exception:
            spy_200 = None
        if spy_price is not None and spy_200 is not None and float(spy_price) > float(spy_200):
            spy_ok = True
    except Exception:
        spy_ok = False

    result = fg_ok and vix_ok and spy_ok
    _EUPHORIC_CACHE["value"] = result
    _EUPHORIC_CACHE["ts"] = now
    if result:
        console.log(
            f"[cyan][EUPHORIC-CLASSIFIER] criteria MET: F&G>80, VIX<18, SPY>200MA"
        )
    return result


def get_current_regime() -> str | None:
    """Read the latest regime from regime_history. None if missing.

    Read-only on its own connection (no thread-safety risk). Fail-safe:
    any DB error returns None and the router treats this as unknown_regime.

    HM-EUPHORIC-CLASSIFIER 2026-05-22: when the base regime is BULL or
    BULL_CROSS AND F&G>80 + VIX<18 + SPY>200MA all hold, override to
    EUPHORIC. This is the more-specific classification per the approved
    matrix. Override only triggers on bullish base regimes — never
    upgrades BEAR/CAUTIOUS_BEAR to EUPHORIC, which would be nonsensical.
    """
    base: str | None = None
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            row = conn.execute(
                "SELECT regime FROM regime_history "
                "WHERE date = date('now','localtime') "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()
            base = row[0] if row else None
        finally:
            conn.close()
    except Exception:
        return None

    # EUPHORIC override layer (HM-EUPHORIC-CLASSIFIER 2026-05-22).
    if base in ("BULL", "BULL_CROSS", "BULL_LOW_VOL"):
        try:
            if _is_euphoric_now():
                return "EUPHORIC"
        except Exception:
            # Fail-safe: any probe error → return base regime, do not promote.
            pass
    return base


def check_regime_fit(
    strategy: str | None, current_regime: str | None
) -> tuple[bool, str]:
    """Return (allowed, reason).

    allowed=True with reason describing approval; allowed=False with reason
    describing the avoid-list match. Fail-safe: unknown regime emits
    [REGIME-ROUTER-UNKNOWN] and returns allowed=True so unrecognized regime
    labels don't block the entire fleet.
    """
    norm = _normalize_strategy(strategy)
    if not current_regime or current_regime not in REGIME_STRATEGY_MATRIX:
        console.log(
            f"[yellow][REGIME-ROUTER-UNKNOWN] strategy={norm} "
            f"regime={current_regime!r} — fail-open (allowed)"
        )
        return (True, "unknown_regime_fail_open")
    profile = REGIME_STRATEGY_MATRIX[current_regime]
    avoid = profile.get("avoid", [])
    if norm in avoid:
        return (
            False,
            f"{norm} not approved in {current_regime} (avoid-list)",
        )
    # HM-MASTER-PLAN W5-B 2026-05-23: enforce the `ic_enabled` profile flag.
    # Previously descriptive metadata only; W5-B surfaced the gap. Now blocks
    # IC strategy submissions when the active regime sets ic_enabled=False
    # (BULL_CROSS, BEAR_CROSS today). Applies to "iron_condor" and
    # "iron_condor_broad_index"; the latter also stays avoid-list gated
    # above for back-compat with the explicit avoid entry in some profiles.
    if (norm in ("iron_condor", "iron_condor_broad_index")
            and not profile.get("ic_enabled", True)):
        return (
            False,
            f"{norm} blocked in {current_regime} (ic_enabled=False)",
        )
    return (True, f"{norm} approved in {current_regime}")


def log_regime_reject(
    *,
    player_id: str | None,
    symbol: str | None,
    strategy: str | None,
    regime: str | None,
    reason: str,
    confidence: float | None = None,
) -> None:
    """Persist a regime_mismatch rejection to decision_audit.

    Never raises — audit failures must not block the calling code path
    (HM-Z/HM-AA error-handling doctrine).
    """
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            conn.execute(
                "INSERT INTO decision_audit "
                "(event_type, player_id, symbol, regime, gate_verdict, "
                " reasoning_snippet, confidence) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    "gate_reject",
                    player_id,
                    symbol,
                    regime,
                    "regime_mismatch",
                    f"[regime_router] {reason} (strategy={strategy})",
                    confidence,
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        console.log(
            f"[red][REGIME-ROUTER-AUDIT] persist failed: "
            f"{type(e).__name__}: {e!r}"
        )


def get_regime_allocation(regime: str | None) -> dict | None:
    """Look up the regime_allocations row for a given regime. None if missing.

    Convenience for Pillar 2 Risk Officer (caps on long_equity_max_pct, etc.).
    """
    if not regime:
        return None
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT regime, long_equity_pct, ic_pct, bear_call_spread_pct, "
                "       hedge_pct, cash_pct, long_equity_max_pct, updated_at "
                "  FROM regime_allocations WHERE regime = ?",
                (regime,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
    except Exception:
        return None
