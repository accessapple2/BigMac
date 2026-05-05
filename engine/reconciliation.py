"""HM-I-β Item 5 — Daily reconciliation report.

Computes internal-book vs Alpaca-paper deltas at market close.
Writes JSON to data/reconciliation/YYYY-MM-DD.json.
NTFYs on threshold breach via engine.alert_channels.send_alert.

Replaces the ε canary that HM-I-Option-ε silenced when partial-SELL
gate fix shipped (commit d06c33c).

Established 2026-05-05 as part of HM-I-β implementation per CLAUDE.md
two-book policy. Scheduled daily at 13:30 MST (4:30 PM ET, ~30 min
post NYSE close) so the broker book is final-state-of-day.

Observability only — no auto-fix. Drift surfaces via NTFY (rate-limited
per HM-U posture: first occurrence per drift-class per day).
"""

# HM-I-β-Item5: new module — daily reconciliation canary
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
})

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

OUTPUT_DIR = Path("data/reconciliation")
DB_PATH = Path("data/trader.db")


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

    Threshold: any non-empty mismatch list in routed_drift, OR any
    non-zero overlap in unrouted_drift, triggers NTFY.

    HM-I-β-Item5: deliberately strict — first occurrence of any drift NTFYs.
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
    return False


def _format_drift_summary(report: dict) -> str:
    """Compact human-readable summary of drift findings.

    HM-I-β-Item5: keeps NTFY message under reasonable length (<400 chars).
    """
    parts: list = []
    routed = report.get("routed_drift", {}) or {}
    if routed.get("in_internal_not_alpaca"):
        parts.append(
            f"{len(routed['in_internal_not_alpaca'])} routed positions in internal book not on Alpaca"
        )
    if routed.get("in_alpaca_not_internal"):
        parts.append(
            f"{len(routed['in_alpaca_not_internal'])} positions on Alpaca not in routed players' internal book"
        )
    if routed.get("in_both_qty_mismatch"):
        parts.append(
            f"{len(routed['in_both_qty_mismatch'])} routed positions with qty mismatch"
        )

    unrouted = report.get("unrouted_drift", {}) or {}
    drift_players = [pid for pid, count in unrouted.items() if count > 0]
    if drift_players:
        sample = drift_players[:5]
        suffix = "" if len(drift_players) <= 5 else f" (+{len(drift_players) - 5} more)"
        parts.append(
            f"{len(drift_players)} non-routed players with Alpaca overlap: {sample}{suffix}"
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

        report = {
            "date":           date_str,
            "timestamp":      timestamp,
            "routed_drift":   routed_drift,
            "unrouted_drift": unrouted_drift,
            "summary": {
                "internal_player_count":  len(internal),
                "internal_position_count": sum(len(v) for v in internal.values()),
                "alpaca_position_count":   len(alpaca),
                "routed_players":          sorted(ROUTED_PLAYERS),
            },
        }

        path = write_report(report, date_str)
        report["report_path"] = str(path)
        console.log(
            f"[cyan]reconciliation: {date_str} — "
            f"internal={report['summary']['internal_position_count']} "
            f"alpaca={len(alpaca)} "
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
