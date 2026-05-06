"""
shared/alpaca_portfolio_sync.py — Tiered full Alpaca portfolio sync

Syncs the live Alpaca paper account into TradeMinds DB on a tiered schedule:
  Market hours  (9:30–16:00 ET)  → every 2 minutes
  Pre/post mkt  (7:00–9:30,      → every 10 minutes
                 16:00–18:30 ET)
  After hours                    → every 60 minutes
  Weekends                       → every 6 hours

Each sync writes:
  - ai_players.cash              (live cash balance)
  - positions table              (re-synced from Alpaca for webull)
  - portfolios.current_balance   (total portfolio value)
  - settings table               (last_alpaca_full_sync, *_value, *_cash)

Returns:
  {"ok": True, "cash": ..., "portfolio_value": ..., "positions": N,
   "synced_at": "ISO string", "skipped": False}
"""
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime, timezone

logger = logging.getLogger("alpaca_portfolio_sync")

DB_PATH = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)

# ── Sync interval thresholds (seconds) ────────────────────────────────────────
_INTERVAL = {
    "market":   2 * 60,        # 2 min during regular hours
    "pre":      10 * 60,       # 10 min pre-market
    "post":     10 * 60,       # 10 min post-market
    "after":    60 * 60,       # 60 min after hours
    "weekend":  6 * 60 * 60,   # 6 hours on weekends
}

# Module-level timestamp so the scheduler doesn't hit Alpaca every minute
_last_sync_epoch: float = 0.0
_last_sync_result: dict = {}


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _market_session() -> str:
    """Return the current Alpaca market session string."""
    try:
        from engine.risk_manager import RiskManager
        result = RiskManager.is_market_hours()
        if result == "market":
            return "market"
        if result == "pre":
            return "pre"
        if result == "post":
            return "post"
        # Distinguish weekends from plain after-hours
        import pytz
        az = pytz.timezone("US/Arizona")
        now = datetime.now(az)
        if now.weekday() >= 5:
            return "weekend"
        return "after"
    except Exception:
        return "after"


def _required_interval() -> int:
    """Return the required sync interval in seconds based on market session."""
    session = _market_session()
    return _INTERVAL.get(session, _INTERVAL["after"])


# alpaca-sync-asset-type-fix (2026-05-05): map Alpaca SDK's AssetClass enum
# to the codebase's asset_type vocabulary. Pre-fix this module hardcoded
# 'stock' for every position, which mislabeled options legs (e.g., the
# SPY 719 PUT during today's bull_put_spread fills) and forced downstream
# consumers (Kirk realign, Item 6 reconciliation, dashboard filters) to
# either work around the mislabel or read alternate sources.
def _alpaca_asset_class_to_type(asset_class) -> str:
    """Map Alpaca SDK's AssetClass enum to the codebase's asset_type vocabulary.

    Alpaca AssetClass values seen in the wild:
      - us_equity  → 'stock'
      - us_option  → 'option'
      - crypto / us_crypto → 'crypto'

    Codebase vocabulary (per `positions` table inventory 2026-05-05):
      stock, option, metal, spread, bond, crypto. Of these, only the first
      three are produced by Alpaca's get_all_positions; metal/spread/bond
      are set elsewhere (dashboard manual annotation, strategy writes).

    Falls back to 'stock' for None or unknown asset classes (preserves
    legacy behavior). Unknown values get a HM-AA-style enriched log
    so a future surprise surfaces in trader_error.log instead of silently
    propagating mislabels.
    """
    if asset_class is None:
        return 'stock'
    # AssetClass enum has .value ('us_equity'); plain str passes through unchanged.
    raw = getattr(asset_class, 'value', None) or str(asset_class)
    value = raw.lower()
    if 'option' in value:
        return 'option'
    if 'crypto' in value:
        return 'crypto'
    if 'equity' in value or 'stock' in value:
        return 'stock'
    logger.warning(
        "alpaca-sync: unknown asset_class %s: %r — defaulting to 'stock'",
        type(asset_class).__name__, asset_class,
    )
    return 'stock'


def run_full_alpaca_sync(force: bool = False) -> dict:
    """
    Pull Alpaca account + positions and persist to DB.

    Args:
        force: Skip the interval check and sync immediately.

    Returns:
        dict with ok, cash, portfolio_value, positions, synced_at, skipped
    """
    import time
    global _last_sync_epoch, _last_sync_result

    now_epoch = time.time()
    required = _required_interval()

    if not force and (now_epoch - _last_sync_epoch) < required:
        return {**_last_sync_result, "skipped": True}

    try:
        from engine.alpaca_bridge import AlpacaBridge
        bridge = AlpacaBridge()
        if not bridge.client:
            return {"ok": False, "error": "Alpaca client unavailable", "skipped": False}

        # ── Pull account data ───────────────────────────────────────────────
        acct   = bridge.client.get_account()
        cash   = round(float(acct.cash), 2)
        pv     = round(float(acct.portfolio_value), 2)
        bp     = round(float(acct.buying_power), 2)

        # ── Pull live positions ─────────────────────────────────────────────
        raw_positions = bridge.client.get_all_positions()
        positions = [
            {
                "symbol":        p.symbol.upper(),
                "qty":           float(p.qty),
                "avg_price":     round(float(p.avg_entry_price or 0), 4),
                "current_price": round(float(p.current_price or 0), 4),
                "market_value":  round(float(p.market_value or 0), 2),
                "unrealized_pl": round(float(p.unrealized_pl or 0), 2),
                # alpaca-sync-asset-type-fix: read actual asset_class instead of
                # hardcoded 'stock' (was mislabeling option legs as stocks).
                "asset_type":    _alpaca_asset_class_to_type(p.asset_class),
            }
            for p in raw_positions
        ]

        synced_at = datetime.now(timezone.utc).isoformat()
        az_time   = _az_time_label()

        conn = _db()
        try:
            # HM-I-β-Item3 (2026-05-05): writes to 'alpaca-mirror' (broker-mirror
            # player), no longer 'webull' (which retains its role as Steve's
            # human-Webull-benchmark). Split per CLAUDE.md two-book policy.
            # ── 1. Update ai_players cash (alpaca-mirror) ────────────────────
            conn.execute(
                "UPDATE ai_players SET cash=? WHERE id='alpaca-mirror'",
                (cash,),
            )

            # ── 2. Re-sync positions table for alpaca-mirror ─────────────────
            # Clear current open positions, preserve history (closed trades stay
            # in the trades table so W/L stats are unaffected).
            conn.execute(
                "DELETE FROM positions WHERE player_id='alpaca-mirror'",
            )
            for p in positions:
                conn.execute(
                    """INSERT INTO positions
                         (player_id, symbol, qty, avg_price, asset_type, opened_at)
                       VALUES ('alpaca-mirror', ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                    (p["symbol"], p["qty"], p["avg_price"], p["asset_type"]),
                )

            # ── 3. Update portfolios.current_balance ────────────────────────
            conn.execute(
                "UPDATE portfolios SET current_balance=?, updated_at=CURRENT_TIMESTAMP WHERE id=1",
                (pv,),
            )

            # ── 4. Update portfolio_positions (current_price + unrealized) ──
            ticker_map = {p["symbol"]: p for p in positions}
            open_pp = conn.execute(
                "SELECT id, ticker FROM portfolio_positions "
                "WHERE status='open' AND portfolio_id=1"
            ).fetchall()
            for row in open_pp:
                ticker = row["ticker"].upper()
                if ticker in ticker_map:
                    pd = ticker_map[ticker]
                    conn.execute(
                        """UPDATE portfolio_positions
                           SET current_price=?, unrealized_pnl=?,
                               updated_at=CURRENT_TIMESTAMP
                           WHERE id=?""",
                        (pd["current_price"], pd["unrealized_pl"], row["id"]),
                    )

            # ── 5. Persist sync metadata to settings ────────────────────────
            for key, val in [
                ("last_alpaca_full_sync",     synced_at),
                ("last_alpaca_portfolio_value", str(pv)),
                ("last_alpaca_cash",           str(cash)),
                ("last_alpaca_buying_power",   str(bp)),
                ("last_alpaca_position_count", str(len(positions))),
                ("last_alpaca_sync_label",     az_time),
            ]:
                conn.execute(
                    "INSERT INTO settings (key, value) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (key, val),
                )

            conn.commit()
        finally:
            conn.close()

        result = {
            "ok":              True,
            "cash":            cash,
            "portfolio_value": pv,
            "buying_power":    bp,
            "positions":       len(positions),
            "tickers":         [p["symbol"] for p in positions],
            "synced_at":       synced_at,
            "synced_label":    az_time,
            "skipped":         False,
        }

        _last_sync_epoch  = now_epoch
        _last_sync_result = result

        logger.info(
            "[SYNC] Portfolio: $%s | Cash: $%s | %d positions | %s",
            f"{pv:,.2f}", f"{cash:,.2f}", len(positions), az_time,
        )
        return result

    except Exception as exc:
        # HM-U: NTFY first occurrence per error class per day (architecture-class
        # broker-mirror sync failure — Item 3 alpaca-mirror is the broker truth source).
        logger.warning("Alpaca full sync error: %s: %r", type(exc).__name__, exc)
        try:
            from engine.alert_channels import send_alert, AlertLevel
            send_alert(
                message=f"alpaca_portfolio_sync {type(exc).__name__}: {exc!r}",
                level=AlertLevel.WARNING,
                alert_type=f"hm-u-alpaca_sync-{type(exc).__name__}",
                rate_limit_secs=86400,
            )
        except Exception:
            pass
        return {"ok": False, "error": str(exc), "skipped": False,
                "error_type": type(exc).__name__, "error_repr": repr(exc)}


def get_last_sync_status() -> dict:
    """Return the last sync metadata stored in the settings table."""
    try:
        conn = _db()
        rows = conn.execute(
            "SELECT key, value FROM settings WHERE key LIKE 'last_alpaca_%'"
        ).fetchall()
        conn.close()
        data = {r["key"]: r["value"] for r in rows}
        return {
            "synced_at":       data.get("last_alpaca_full_sync"),
            "synced_label":    data.get("last_alpaca_sync_label", "—"),
            "portfolio_value": float(data["last_alpaca_portfolio_value"])
                               if "last_alpaca_portfolio_value" in data else None,
            "cash":            float(data["last_alpaca_cash"])
                               if "last_alpaca_cash" in data else None,
            "buying_power":    float(data["last_alpaca_buying_power"])
                               if "last_alpaca_buying_power" in data else None,
            "position_count":  int(data["last_alpaca_position_count"])
                               if "last_alpaca_position_count" in data else None,
        }
    except Exception as exc:
        return {"error": str(exc)}


def _az_time_label() -> str:
    """Return current time as 'H:MM AM/PM AZ' in Arizona timezone."""
    try:
        import pytz
        az = pytz.timezone("US/Arizona")
        return datetime.now(az).strftime("%-I:%M %p AZ")
    except Exception:
        return datetime.utcnow().strftime("%H:%M UTC")
