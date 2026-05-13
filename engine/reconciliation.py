"""HM-I-β Item 5 + Item 6 — Daily reconciliation report (stocks + options).

Computes internal-book vs Alpaca-paper deltas at market close. Writes JSON
to data/reconciliation/YYYY-MM-DD.json. NTFYs on threshold breach via
engine.alert_channels.send_alert.

Replaces the ε canary that HM-I-Option-ε silenced when partial-SELL gate
fix shipped (commit d06c33c).

Established 2026-05-05 as part of HM-I-β implementation per CLAUDE.md
two-book policy. Scheduled daily at 13:30 MST (4:30 PM ET, ~30 min post
NYSE close) so the broker book is final-state-of-day.

Observability only — no auto-fix. Drift surfaces via NTFY (rate-limited
per HM-U posture: first occurrence per drift-class per day).

HM-I-β-Item6 (2026-05-05): extended with options pass alongside stocks.
Internal source = options_trades (status='open' AND exec_status='open').
Alpaca source = /v2/positions filtered by asset_class.value == 'us_option'.
Mirrors Item 5's routed/unrouted split: routed options = agent_id with
'strategy:' prefix (routes via engine.alpaca_options.execute_options_signal,
the third forward path bypassing the player-keyed routing table).
"""

# HM-I-β-Item5: new module — daily reconciliation canary
# HM-I-β-Item6: options pass appended to stocks pass.
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path

from rich.console import Console

console = Console()

# Routed players: their internal positions should mirror Alpaca via the forward
# paths (paper_trader._forward_to_alpaca for super-agent/ollie-auto/neo-matrix,
# alpaca_portfolio_sync.run_full_alpaca_sync for alpaca-mirror, tracking-only for
# dalio-metals). Drift on these = real divergence worth alerting on.
# Source of truth: paper_trader._EXECUTION_PORTFOLIO_BY_PLAYER (4 entries) plus
# alpaca-mirror (HM-I-β-Item3 broker-mirror destination, commit 5186408).
ROUTED_PLAYERS = frozenset({
    "super-agent",
    "ollie-auto",
    "neo-matrix",
    "dalio-metals",
    "alpaca-mirror",
}

# === HM-BM === Players whose internal book IS the Alpaca book by design.
# Excluding them from routed-drift detection prevents circular comparison
# inflating false "drift". See L186 comment: alpaca-mirror is 1:1 with Alpaca.
_BY_DESIGN_MIRROR_PLAYERS = {"alpaca-mirror"}
# === /HM-BM ===
)

# HM-I-β-Item5-thread-A (2026-05-05): tracking-mode routed players hold
# positions by design that never forward to Alpaca. Including them in
# routed-drift detection produces false-positive findings.
#
# Concrete case from 2026-05-05 reconciliation: dalio-metals holds ONDS short
# (-76 shares) per its tracking-only metals-thesis paper book. Its route
# resolves to portfolio='Enterprise Computer' route_mode='tracking'
# (paper_trader._resolve_execution_portfolio); positions log-only via
# _log_signal_only and never hit Alpaca. Including dalio-metals in
# routed-drift surfaced ONDS as a false drift finding (see
# docs/RECONCILIATION_DRIFT_INVESTIGATION_2026-05-05.md Thread A).
#
# Maintenance note: when new tracking-mode players are added (e.g., a future
# Schwab tracker), append their player_id here. Pattern mirrors webull
# exclusion in compute_unrouted_drift.
TRACKING_ONLY_ROUTED_PLAYERS = frozenset({
    "dalio-metals",
})

# HM-I-β-Item6 (2026-05-05): agent_id prefixes that route options trades
# through engine.alpaca_options.execute_options_signal (the third forward
# path, separate from paper_trader._forward_to_alpaca's player-keyed table).
# Today, only `strategy:bull_spread_v1` lives here. Future strategies that
# fire options orders should follow the same naming convention.
ROUTED_OPTIONS_AGENT_PREFIXES = ("strategy:",)

OUTPUT_DIR = Path("data/reconciliation")
DB_PATH = Path("data/trader.db")


# HM-I-β-Item6: build OCC symbol from internal legs_json fields for matching
# against Alpaca-side symbols. Format: <underlying><YYMMDD>{C|P}<strike*1000:08d>.
# Example: SPY + 2026-05-15 + put + 718.0 → 'SPY260515P00718000'.
def _build_occ_symbol(
    underlying: str, expiration: str, option_type: str, strike: float
) -> str | None:
    try:
        yymmdd = expiration.replace("-", "")[2:]
        if len(yymmdd) != 6 or not yymmdd.isdigit():
            return None
        cp = "C" if str(option_type).lower().startswith("c") else "P"
        strike_int = int(round(float(strike) * 1000))
        if strike_int <= 0:
            return None
        return f"{str(underlying).upper()}{yymmdd}{cp}{strike_int:08d}"
    except (ValueError, TypeError, AttributeError):
        return None


def _is_routed_options_agent(agent_id: str) -> bool:
    """Return True if this agent_id routes options through Alpaca."""
    if not agent_id:
        return False
    return any(agent_id.startswith(prefix) for prefix in ROUTED_OPTIONS_AGENT_PREFIXES)


def get_alpaca_positions() -> dict:
    """Return Alpaca paper positions keyed by symbol.

    Returns: {symbol: {"qty": float, "market_value": float, "avg_entry": float}}

    HM-I-β-Item5: read-only Alpaca query via engine.alpaca_bridge wrapper
    (the canonical established read pattern; same one alpaca_portfolio_sync uses).
    Returns empty dict on bridge unavailability — reconciliation continues
    against an empty broker side, which itself becomes the drift signal.
    """
    try:
        from engine.alpaca_bridge import alpaca
    except Exception as e:
        console.log(f"[yellow]reconciliation: alpaca_bridge import failed: {type(e).__name__}: {e!r}")
        return {}

    raw = alpaca.positions() or []
    out: dict = {}
    for p in raw:
        # alpaca_bridge.positions() returns dicts on success, single
        # {'error': ...} dict on failure. Filter the error case.
        if not isinstance(p, dict) or "error" in p or "symbol" not in p:
            continue
        sym = str(p["symbol"]).upper()
        out[sym] = {
            "qty":           float(p.get("qty", 0) or 0),
            "market_value":  float(p.get("market_value", 0) or 0),
            "avg_entry":     float(p.get("avg_entry", 0) or 0),
        }
    return out


def get_internal_positions() -> dict:
    """Return internal positions grouped by player_id.

    Returns: {player_id: [{"symbol": str, "qty": float, "asset_type": str}, ...]}

    HM-I-β-Item5: read-only DB query against `positions` table. Filters out
    rows with qty<=0 (closed positions waiting cleanup don't count as drift).
    """
    out: dict = {}
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    try:
        rows = conn.execute(
            "SELECT player_id, symbol, qty, asset_type "
            "FROM positions WHERE qty IS NOT NULL AND qty != 0"
        ).fetchall()
    finally:
        conn.close()

    for player_id, symbol, qty, asset_type in rows:
        out.setdefault(player_id, []).append({
            "symbol":     str(symbol).upper(),
            "qty":        float(qty),
            "asset_type": asset_type or "stock",
        })
    return out


def compute_routed_drift(internal: dict, alpaca: dict) -> dict:
    """For routed players: symbol-set difference internal vs Alpaca.

    Returns dict with four lists:
      - in_internal_not_alpaca: routed positions present internally but missing
        on Alpaca (forward path failed or never ran)
      - in_alpaca_not_internal: positions on Alpaca but missing from any
        routed player's internal book (sync didn't write back, or position
        was created externally)
      - in_both_qty_match: present on both sides with matching qty
      - in_both_qty_mismatch: present on both sides with diverging qty

    HM-I-β-Item5: routed players should be in sync with Alpaca via the
    forward paths. Drift = real divergence worth alerting on.

    Stocks-only — options reconciliation is a separate book (options_trades
    table, not positions). Future Item 6 if needed.
    """
    # Build the routed-internal symbol → qty map (stocks only, summed across
    # routed players — alpaca-mirror is by-design 1:1 with Alpaca, but the
    # other 4 routed players each maintain their own subset).
    #
    # HM-I-β-Item5-thread-A (2026-05-05): exclude tracking-mode players from
    # the routed-internal aggregation. Their positions are by-design log-only
    # and never forward to Alpaca; including them produces false-positive
    # drift findings (see TRACKING_ONLY_ROUTED_PLAYERS docstring above and
    # docs/RECONCILIATION_DRIFT_INVESTIGATION_2026-05-05.md Thread A).
    routed_internal: dict[str, float] = {}
    for player_id, positions in internal.items():
        # === HM-BM === skip by-design mirrors (circular self-comparison)
        if player_id in _BY_DESIGN_MIRROR_PLAYERS:
            continue
        # === /HM-BM ===
        if player_id not in ROUTED_PLAYERS:
            continue
        if player_id in TRACKING_ONLY_ROUTED_PLAYERS:
            continue  # HM-I-β-Item5-thread-A: tracking-mode = log-only by design
        for p in positions:
            if p.get("asset_type") != "stock":
                continue
            sym = p["symbol"]
            routed_internal[sym] = routed_internal.get(sym, 0.0) + p["qty"]

    alpaca_syms = set(alpaca.keys())
    internal_syms = set(routed_internal.keys())

    in_internal_not_alpaca = sorted(internal_syms - alpaca_syms)
    in_alpaca_not_internal = sorted(alpaca_syms - internal_syms)

    in_both_qty_match: list = []
    in_both_qty_mismatch: list = []
    for sym in sorted(internal_syms & alpaca_syms):
        i_qty = routed_internal[sym]
        a_qty = alpaca[sym]["qty"]
        if abs(i_qty - a_qty) < 0.01:
            in_both_qty_match.append({"symbol": sym, "qty": round(i_qty, 4)})
        else:
            in_both_qty_mismatch.append({
                "symbol":      sym,
                "internal_qty": round(i_qty, 4),
                "alpaca_qty":   round(a_qty, 4),
                "delta":        round(i_qty - a_qty, 4),
            })

    return {
        "in_internal_not_alpaca": in_internal_not_alpaca,
        "in_alpaca_not_internal": in_alpaca_not_internal,
        "in_both_qty_match":      in_both_qty_match,
        "in_both_qty_mismatch":   in_both_qty_mismatch,
    }


def compute_unrouted_drift(internal: dict, alpaca: dict) -> dict:
    """For non-routed players: count positions that overlap with Alpaca symbols.

    Returns: {player_id: overlap_count}
      Only non-zero entries included. Empty dict in normal operation.

    HM-I-β-Item5: non-routed players should have ZERO Alpaca overlap
    per the two-book policy (CLAUDE.md). Any overlap = drift —
    either a non-routed player got onto Alpaca (forward path leak)
    or alpaca_portfolio_sync wrote to the wrong player_id.
    """
    alpaca_syms = set(alpaca.keys())
    out: dict = {}
    for player_id, positions in internal.items():
        if player_id in ROUTED_PLAYERS:
            continue
        # Skip the human (webull) — its 127 imported Webull trades are
        # historical Steve data, not drift. Per HM-I-β-Item3, webull retains
        # its human-Webull-import role; positions there are intentional.
        if player_id == "webull":
            continue
        overlap = sum(
            1 for p in positions
            if p.get("asset_type") == "stock" and p["symbol"] in alpaca_syms
        )
        if overlap > 0:
            out[player_id] = overlap
    return out


# HM-I-β-Item6: Alpaca options-positions reader. Mirrors get_alpaca_positions
# (stocks) but filters to asset_class=us_option. AssetClass enum is matched
# via .value (str() returns 'AssetClass.US_OPTION' which would silently fail).
def get_alpaca_options_positions() -> dict:
    """Return Alpaca paper options positions keyed by OCC symbol.

    Returns: {occ_symbol: {"qty": float, "side": str, "market_value": float,
                           "avg_entry": float}}
    `qty` is signed: positive for long, negative for short.

    HM-I-β-Item6: read-only Alpaca query via the same TradingClient used by
    engine.alpaca_bridge. Returns empty dict on bridge unavailability —
    reconciliation continues against an empty broker side, which itself
    becomes the drift signal.
    """
    try:
        from engine.alpaca_bridge import alpaca
    except Exception as e:
        console.log(
            f"[yellow]reconciliation options: alpaca_bridge import failed: "
            f"{type(e).__name__}: {e!r}"
        )
        return {}

    client = getattr(alpaca, "client", None)
    if client is None:
        console.log("[yellow]reconciliation options: alpaca client unavailable")
        return {}

    try:
        raw = client.get_all_positions() or []
    except Exception as e:
        console.log(
            f"[yellow]reconciliation options: get_all_positions failed: "
            f"{type(e).__name__}: {e!r}"
        )
        return {}

    out: dict = {}
    for p in raw:
        ac = getattr(p, "asset_class", None)
        ac_value = getattr(ac, "value", None) or str(ac).lower()
        if ac_value not in ("us_option", "option"):
            continue
        sym = str(getattr(p, "symbol", "")).upper()
        if not sym:
            continue
        try:
            qty = float(p.qty)
        except (ValueError, TypeError, AttributeError):
            continue
        side = str(getattr(p, "side", "")).lower()
        if "short" in side and qty > 0:
            qty = -qty
        out[sym] = {
            "qty":          round(qty, 4),
            "side":         side or ("long" if qty >= 0 else "short"),
            "market_value": float(getattr(p, "market_value", 0) or 0),
            "avg_entry":    float(getattr(p, "avg_entry_price", 0) or 0),
        }
    return out


# HM-I-β-Item6: internal options snapshot from options_trades. Excludes
# test_cleanup and failed_pre_fix rows; both flag non-real positions.
def get_internal_options_positions() -> dict:
    """Return internal options positions grouped by agent_id, with OCC symbols.

    Returns: {agent_id: [{"occ_symbol": str, "qty": int, "side": str,
                          "structure": str, "row_id": int}, ...]}
      Each list element represents one LEG of an open options_trades row.
      A 2-leg vertical spread row produces 2 list elements.

    Filter: status='open' AND exec_status='open'. Excludes failed_pre_fix
    (HM-Z-era ghost rows) and test_cleanup (cleanup-flagged, not real).

    HM-I-β-Item6: read-only DB query. legs_json is parsed per row;
    OCC symbols are constructed via _build_occ_symbol so they are directly
    comparable against Alpaca's symbol field.
    """
    out: dict = {}
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    try:
        rows = conn.execute(
            "SELECT id, agent_id, structure, symbol, expiration, legs_json, contracts "
            "FROM options_trades "
            "WHERE status='open' AND exec_status='open'"
        ).fetchall()
    finally:
        conn.close()

    for row_id, agent_id, structure, underlying, expiration, legs_json, contracts in rows:
        try:
            legs = json.loads(legs_json) if legs_json else []
        except (TypeError, ValueError):
            legs = []
        if not legs:
            continue
        qty_per_leg = int(contracts or 1)
        for leg in legs:
            occ = _build_occ_symbol(
                underlying=underlying,
                expiration=leg.get("expiration") or expiration,
                option_type=leg.get("option_type") or "",
                strike=leg.get("strike") or 0,
            )
            if not occ:
                continue
            action = str(leg.get("action") or "").lower()
            side = "long" if action.startswith("b") else "short"
            signed_qty = qty_per_leg if side == "long" else -qty_per_leg
            out.setdefault(agent_id, []).append({
                "occ_symbol": occ,
                "qty":        signed_qty,
                "side":       side,
                "structure":  structure,
                "row_id":     row_id,
            })
    return out


# HM-I-β-Item6: routed-options drift detection. Mirrors compute_routed_drift
# (stocks) but operates on aggregated OCC-symbol → signed-qty maps. Aggregation
# is required because a single OCC symbol may appear in multiple internal rows
# (e.g., 13 spreads sharing the same long leg) while Alpaca reports one
# net-quantity row per OCC symbol.
def compute_options_routed_drift(internal: dict, alpaca: dict) -> dict:
    """For routed-options agents: OCC-symbol-set diff internal vs Alpaca.

    Aggregates internal across all agent_ids matching ROUTED_OPTIONS_AGENT_PREFIXES,
    then symbol-set diffs and qty-compares against Alpaca's options view.

    Returns dict with four lists (same shape as compute_routed_drift):
      - in_internal_not_alpaca: OCC symbols present internally but missing on Alpaca
      - in_alpaca_not_internal: OCC symbols on Alpaca but missing internally
      - in_both_qty_match: present on both sides with matching signed qty
      - in_both_qty_mismatch: present on both sides with diverging signed qty

    HM-I-β-Item6: routed-options agents (today: strategy:bull_spread_v1) flow
    through alpaca_options.execute_options_signal. Their internal book should
    match Alpaca's options positions — drift is real divergence worth alerting.
    """
    routed_internal: dict[str, int] = {}
    for agent_id, legs in internal.items():
        if not _is_routed_options_agent(agent_id):
            continue
        for leg in legs:
            occ = leg["occ_symbol"]
            routed_internal[occ] = routed_internal.get(occ, 0) + leg["qty"]

    alpaca_syms = set(alpaca.keys())
    internal_syms = set(routed_internal.keys())

    in_internal_not_alpaca = sorted(internal_syms - alpaca_syms)
    in_alpaca_not_internal = sorted(alpaca_syms - internal_syms)

    in_both_qty_match: list = []
    in_both_qty_mismatch: list = []
    for sym in sorted(internal_syms & alpaca_syms):
        i_qty = routed_internal[sym]
        a_qty = alpaca[sym]["qty"]
        if abs(i_qty - a_qty) < 0.01:
            in_both_qty_match.append({"occ_symbol": sym, "qty": round(i_qty, 4)})
        else:
            in_both_qty_mismatch.append({
                "occ_symbol":   sym,
                "internal_qty": round(i_qty, 4),
                "alpaca_qty":   round(a_qty, 4),
                "delta":        round(i_qty - a_qty, 4),
            })

    return {
        "in_internal_not_alpaca": in_internal_not_alpaca,
        "in_alpaca_not_internal": in_alpaca_not_internal,
        "in_both_qty_match":      in_both_qty_match,
        "in_both_qty_mismatch":   in_both_qty_mismatch,
    }


# HM-I-β-Item6: unrouted-options drift detection. Catches the case where a
# non-strategy agent has options_trades rows whose OCC symbols overlap with
# Alpaca's options book — implies a forward-path leak or a misclassified agent.
def compute_options_unrouted_drift(internal: dict, alpaca: dict) -> dict:
    """For non-routed options agents: count OCC overlap with Alpaca options.

    Returns: {agent_id: overlap_count}
      Only non-zero entries included. Empty dict in normal operation.

    HM-I-β-Item6: today no non-strategy agent emits options trades, so this
    should be {} on every run. A non-empty result indicates either a new
    agent that needs routing review or a misclassified existing agent.
    """
    alpaca_syms = set(alpaca.keys())
    out: dict = {}
    for agent_id, legs in internal.items():
        if _is_routed_options_agent(agent_id):
            continue
        overlap = sum(1 for leg in legs if leg["occ_symbol"] in alpaca_syms)
        if overlap > 0:
            out[agent_id] = overlap
    return out


def write_report(report: dict, date_str: str) -> Path:
    """Write JSON report to data/reconciliation/YYYY-MM-DD.json.

    Returns the file path written.

    HM-I-β-Item5: pure file output, no side effects beyond write.
    Creates OUTPUT_DIR if missing.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"{date_str}.json"
    path.write_text(json.dumps(report, indent=2, default=str))
    return path


def has_drift(report: dict) -> bool:
    """Determine if the report shows drift worth NTFY-ing.

    Threshold: any non-empty mismatch list in routed_drift OR options_routed_drift,
    OR any non-zero overlap in unrouted_drift OR options_unrouted_drift,
    triggers NTFY.

    HM-I-β-Item5: deliberately strict — first occurrence of any drift NTFYs.
    HM-I-β-Item6: same threshold semantics extended to options drift.
    Suppression handled by HM-U rate_limit_secs=86400 per alert_type.
    """
    routed = report.get("routed_drift", {}) or {}
    unrouted = report.get("unrouted_drift", {}) or {}

    if any(routed.get(k) for k in (
        "in_internal_not_alpaca", "in_alpaca_not_internal", "in_both_qty_mismatch",
    )):
        return True
    if any(count > 0 for count in unrouted.values()):
        return True

    # HM-I-β-Item6: options drift folded into the same threshold
    options_routed = report.get("options_routed_drift", {}) or {}
    options_unrouted = report.get("options_unrouted_drift", {}) or {}
    if any(options_routed.get(k) for k in (
        "in_internal_not_alpaca", "in_alpaca_not_internal", "in_both_qty_mismatch",
    )):
        return True
    if any(count > 0 for count in options_unrouted.values()):
        return True

    return False


def _format_drift_summary(report: dict) -> str:
    """Compact human-readable summary of drift findings.

    HM-I-β-Item5: keeps NTFY message under reasonable length (<400 chars).
    HM-I-β-Item6: extended with options drift counts (stocks vs options
    distinction kept in the message so on-call can triage).
    """
    parts: list = []
    routed = report.get("routed_drift", {}) or {}
    if routed.get("in_internal_not_alpaca"):
        parts.append(
            f"stocks: {len(routed['in_internal_not_alpaca'])} routed in internal not on Alpaca"
        )
    if routed.get("in_alpaca_not_internal"):
        parts.append(
            f"stocks: {len(routed['in_alpaca_not_internal'])} on Alpaca not in routed internal"
        )
    if routed.get("in_both_qty_mismatch"):
        parts.append(
            f"stocks: {len(routed['in_both_qty_mismatch'])} qty mismatches"
        )

    unrouted = report.get("unrouted_drift", {}) or {}
    drift_players = [pid for pid, count in unrouted.items() if count > 0]
    if drift_players:
        sample = drift_players[:5]
        suffix = "" if len(drift_players) <= 5 else f" (+{len(drift_players) - 5} more)"
        parts.append(
            f"stocks: {len(drift_players)} non-routed overlap: {sample}{suffix}"
        )

    # HM-I-β-Item6: options counts
    opt_routed = report.get("options_routed_drift", {}) or {}
    if opt_routed.get("in_internal_not_alpaca"):
        parts.append(
            f"options: {len(opt_routed['in_internal_not_alpaca'])} OCC in internal not on Alpaca"
        )
    if opt_routed.get("in_alpaca_not_internal"):
        parts.append(
            f"options: {len(opt_routed['in_alpaca_not_internal'])} OCC on Alpaca not in internal"
        )
    if opt_routed.get("in_both_qty_mismatch"):
        parts.append(
            f"options: {len(opt_routed['in_both_qty_mismatch'])} qty mismatches"
        )

    opt_unrouted = report.get("options_unrouted_drift", {}) or {}
    opt_drift_agents = [aid for aid, count in opt_unrouted.items() if count > 0]
    if opt_drift_agents:
        sample = opt_drift_agents[:5]
        suffix = "" if len(opt_drift_agents) <= 5 else f" (+{len(opt_drift_agents) - 5} more)"
        parts.append(
            f"options: {len(opt_drift_agents)} non-routed agents with Alpaca overlap: {sample}{suffix}"
        )

    return "; ".join(parts) if parts else "drift detected (see report)"


def run_reconciliation() -> dict:
    """Top-level entry point. Run reconciliation, write report, NTFY on drift.

    Returns the full report dict.

    HM-I-β-Item5: scheduled daily at 13:30 MST per main.py registration
    (~30 min post NYSE close). Idempotent — safe to call manually.
    """
    date_str = datetime.now().strftime("%Y-%m-%d")
    timestamp = datetime.now().isoformat()

    try:
        internal = get_internal_positions()
        alpaca = get_alpaca_positions()

        routed_drift = compute_routed_drift(internal, alpaca)
        unrouted_drift = compute_unrouted_drift(internal, alpaca)

        # HM-I-β-Item6: options pass — same cadence and JSON output, separate
        # source tables (options_trades + Alpaca options-class positions).
        internal_options = get_internal_options_positions()
        alpaca_options = get_alpaca_options_positions()
        options_routed_drift = compute_options_routed_drift(
            internal_options, alpaca_options
        )
        options_unrouted_drift = compute_options_unrouted_drift(
            internal_options, alpaca_options
        )

        report = {
            "date":           date_str,
            "timestamp":      timestamp,
            "routed_drift":   routed_drift,
            "unrouted_drift": unrouted_drift,
            "options_routed_drift":   options_routed_drift,
            "options_unrouted_drift": options_unrouted_drift,
            "summary": {
                "internal_player_count":          len(internal),
                "internal_position_count":        sum(len(v) for v in internal.values()),
                "alpaca_position_count":          len(alpaca),
                "routed_players":                 sorted(ROUTED_PLAYERS),
                # HM-I-β-Item6: options counts
                "internal_options_position_count": sum(len(v) for v in internal_options.values()),
                "alpaca_options_position_count":   len(alpaca_options),
                "routed_options_agent_prefixes":   list(ROUTED_OPTIONS_AGENT_PREFIXES),
            },
        }

        path = write_report(report, date_str)
        report["report_path"] = str(path)
        console.log(
            f"[cyan]reconciliation: {date_str} — "
            f"stocks internal={report['summary']['internal_position_count']} "
            f"alpaca={len(alpaca)} "
            f"options internal={report['summary']['internal_options_position_count']} "
            f"alpaca={len(alpaca_options)} "
            f"drift={'YES' if has_drift(report) else 'no'} "
            f"→ {path}"
        )

        # NTFY on drift (HM-U wiring — first occurrence per day per alert_type).
        # Wrap in try/except so dispatch failure never propagates into the
        # reconciliation result. Same pattern as HM-U wiring sites.
        if has_drift(report):
            try:
                from engine.alert_channels import send_alert, AlertLevel
                drift_summary = _format_drift_summary(report)
                send_alert(
                    message=f"Daily reconciliation drift detected ({date_str}): {drift_summary}\nReport: {path}",
                    level=AlertLevel.WARNING,
                    alert_type=f"hm-i-b-item5-drift-{date_str}",
                    rate_limit_secs=86400,
                )
            except Exception as alert_e:
                console.log(
                    f"[yellow]reconciliation NTFY dispatch error: "
                    f"{type(alert_e).__name__}: {alert_e!r}"
                )

        return report

    except Exception as e:
        # HM-U: NTFY first occurrence per error class per day for this
        # architecture-class observability path. Same pattern as commit bb33660.
        console.log(
            f"[red]reconciliation error: {type(e).__name__}: {e!r}"
        )
        try:
            from engine.alert_channels import send_alert, AlertLevel
            send_alert(
                message=f"Daily reconciliation FAILED ({date_str}): {type(e).__name__}: {e!r}",
                level=AlertLevel.WARNING,
                alert_type=f"hm-u-reconciliation-{type(e).__name__}",
                rate_limit_secs=86400,
            )
        except Exception:
            pass
        raise


if __name__ == "__main__":
    # Manual invocation: python -m engine.reconciliation
    report = run_reconciliation()
    print(json.dumps(report, indent=2, default=str))
