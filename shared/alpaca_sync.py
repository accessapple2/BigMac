"""Lightweight Alpaca and portfolio sync helpers.

This module is intentionally import-safe for the Arena Python 3.9 runtime.
It avoids CrewAI imports so scheduled sync jobs can run without pulling in the
full Crew stack.
"""

import os
import sqlite3


DB_PATH = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def sync_positions_from_alpaca() -> dict:
    """Update portfolio_positions from live Alpaca and metals spot prices."""
    results = {"alpaca_synced": 0, "metals_synced": 0}

    try:
        from engine.alpaca_bridge import AlpacaBridge

        bridge = AlpacaBridge()
        if bridge.client:
            alpaca_positions = bridge.client.get_all_positions()
            live = {
                p.symbol.upper(): {
                    "current_price": float(p.current_price or 0),
                    "unrealized_pnl": float(p.unrealized_pl or 0),
                }
                for p in alpaca_positions
            }

            conn = _db()
            try:
                rows = conn.execute(
                    "SELECT id, ticker FROM portfolio_positions "
                    "WHERE status='open' AND portfolio_id=1"
                ).fetchall()
                for row in rows:
                    ticker = row["ticker"].upper()
                    if ticker in live:
                        conn.execute(
                            """UPDATE portfolio_positions
                               SET current_price=?, unrealized_pnl=?,
                                   updated_at=CURRENT_TIMESTAMP
                               WHERE id=?""",
                            (
                                live[ticker]["current_price"],
                                live[ticker]["unrealized_pnl"],
                                row["id"],
                            ),
                        )
                        results["alpaca_synced"] += 1
                conn.commit()
            finally:
                conn.close()
            results["alpaca_tickers"] = list(live.keys())

            try:
                acct = bridge.status()
                if acct.get("connected") and acct.get("portfolio_value"):
                    equity = round(float(acct["portfolio_value"]), 2)
                    results["alpaca_equity"] = equity
                    conn2 = _db()
                    try:
                        conn2.execute(
                            "UPDATE portfolios SET updated_at=CURRENT_TIMESTAMP WHERE id=1",
                        )
                        conn2.commit()
                    finally:
                        conn2.close()
            except Exception as eq_err:
                results["alpaca_equity_error"] = str(eq_err)
    except Exception as e:
        results["alpaca_error"] = str(e)

    metals_ticker_map = {"XAUUSD": "GOLD", "XAGUSD": "SILVER"}
    try:
        from engine.metals_tracker import get_spot_prices

        spot = get_spot_prices()

        conn = _db()
        try:
            rows = conn.execute(
                "SELECT id, ticker, metal_oz, entry_price "
                "FROM portfolio_positions "
                "WHERE status='open' AND asset_class='metal'"
            ).fetchall()

            metals_market_value = 0.0
            for row in rows:
                metal_key = metals_ticker_map.get(row["ticker"].upper())
                if not metal_key or metal_key not in spot:
                    continue
                price = float(spot[metal_key]["price"])
                oz = float(row["metal_oz"] or 0)
                entry = float(row["entry_price"] or 0)
                unrealized = round((price - entry) * oz, 2) if entry > 0 else 0.0
                metals_market_value += price * oz
                conn.execute(
                    """UPDATE portfolio_positions
                       SET current_price=?, unrealized_pnl=?,
                           updated_at=CURRENT_TIMESTAMP
                       WHERE id=?""",
                    (round(price, 4), unrealized, row["id"]),
                )
                results["metals_synced"] += 1

            if metals_market_value > 0:
                conn.execute(
                    "UPDATE portfolios SET current_balance=?, updated_at=CURRENT_TIMESTAMP "
                    "WHERE id=5",
                    (round(metals_market_value, 2),),
                )
            conn.commit()
        finally:
            conn.close()
        results["metals_spot"] = {
            k: round(spot[k]["price"], 2) for k in ("GOLD", "SILVER") if k in spot
        }
    except Exception as e:
        results["metals_error"] = str(e)

    try:
        conn3 = _db()
        try:
            row = conn3.execute(
                "SELECT COALESCE(SUM(current_balance),0) as total "
                "FROM portfolios WHERE is_human=0 AND is_active=1 AND id != 6"
            ).fetchone()
            super_balance = round(float(row["total"]), 2)
            conn3.execute(
                "UPDATE portfolios SET current_balance=?, updated_at=CURRENT_TIMESTAMP WHERE id=6",
                (super_balance,),
            )
            conn3.commit()
            results["super_agent_balance"] = super_balance
        finally:
            conn3.close()
    except Exception as e:
        results["super_agent_error"] = str(e)

    results["synced"] = results["alpaca_synced"] + results["metals_synced"]
    return results
