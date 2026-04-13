"""Kirk Advisory — actionable recommendations for Captain Kirk's Webull positions.
Cross-references positions with GEX, F&G, VIX, fleet intelligence, trade history, and backtest results.
Generates TRIM/HOLD/ADD signals with reasoning.
"""
import logging
import os
import sqlite3 as _sq
from datetime import datetime
import pytz
from engine.market_data import get_stock_price
from engine.fear_greed import get_fear_greed_index
from rich.console import Console

console = Console()
logger = logging.getLogger("kirk_advisory")

PLAYER_ID = "steve-webull"
STOP_LOSS_PCT = -8.0      # Hard stop at -8%
TRIM_WARNING_PCT = -6.0   # Warn when approaching stop
WINNER_HOLD_PCT = 5.0     # Don't sell winners above this


def _get_db():
    """Open trader.db with row_factory."""
    db = _sq.connect("data/trader.db", timeout=10)
    db.row_factory = _sq.Row
    return db


def _get_trade_history_summary():
    """Return win rate and avg hold days from steve-webull's 127 imported trades."""
    try:
        db = _get_db()
        rows = db.execute(
            "SELECT t.symbol, t.price AS sell_price, t.qty, t.executed_at, "
            "(SELECT b.price FROM trades b WHERE b.player_id='steve-webull' "
            "AND b.action='BUY' AND b.symbol=t.symbol AND b.executed_at<=t.executed_at "
            "ORDER BY b.executed_at DESC LIMIT 1) AS buy_price "
            "FROM trades t WHERE t.player_id='steve-webull' AND t.action='SELL'"
        ).fetchall()
        db.close()
        total = len(rows)
        if not total:
            return {}
        wins = sum(1 for r in rows if r["buy_price"] and r["sell_price"] > r["buy_price"])
        win_rate = round(wins / total * 100, 1)
        # Count most traded symbols
        from collections import Counter
        sym_counts = Counter(r["symbol"] for r in rows)
        top_symbols = [s for s, _ in sym_counts.most_common(3)]
        return {
            "total_closed": total,
            "wins": wins,
            "win_rate": win_rate,
            "top_symbols": top_symbols,
        }
    except Exception as e:
        logger.debug(f"Trade history error: {e}")
        return {}


def _get_backtest_context(symbols: list) -> dict:
    """Return best backtest metrics for the given symbols."""
    if not symbols:
        return {}
    try:
        db = _get_db()
        placeholders = ",".join("?" * len(symbols))
        rows = db.execute(
            f"SELECT player_id as symbol, AVG(win_rate) as avg_wr, AVG(sharpe_ratio) as avg_sharpe, "
            f"AVG(total_return_pct) as avg_return, COUNT(*) as runs "
            f"FROM backtest_results WHERE player_id IN ({placeholders}) "
            f"GROUP BY player_id",
            symbols
        ).fetchall()
        db.close()
        return {r["symbol"]: {
            "avg_win_rate": round(r["avg_wr"] or 0, 1),
            "avg_sharpe": round(r["avg_sharpe"] or 0, 2),
            "avg_return": round(r["avg_return"] or 0, 1),
            "runs": r["runs"],
        } for r in rows}
    except Exception as e:
        logger.debug(f"Backtest context error: {e}")
        return {}


def _get_rebalance_recs(symbols: list) -> dict:
    """Return any TRIM recommendations from the rebalance engine for given symbols."""
    try:
        db = _get_db()
        rows = db.execute(
            "SELECT symbol, action, rationale FROM rebalance_recs "
            "WHERE action IN ('TRIM','SELL') ORDER BY created_at DESC LIMIT 20"
        ).fetchall()
        db.close()
        result = {}
        for r in rows:
            if r["symbol"] in symbols and r["symbol"] not in result:
                result[r["symbol"]] = {"action": r["action"], "rationale": r["rationale"] or ""}
        return result
    except Exception:
        return {}


def _get_live_cash(fallback: float = 0.0) -> float:
    """Return Kirk's account value. Priority:
    1. KIRK_PORTFOLIO_CASH env var (manual override)
    2. settings.webull_synced_value (auto-updated by Webull sync)
    3. ai_players.cash (DB fallback)
    """
    env_cash = os.environ.get("KIRK_PORTFOLIO_CASH", "").strip()
    if env_cash and env_cash != "0":
        try:
            return float(env_cash)
        except ValueError:
            pass
    try:
        db = _get_db()
        # Prefer the synced Webull total value (most accurate, updated on sync)
        synced_row = db.execute(
            "SELECT value FROM settings WHERE key='webull_synced_value'"
        ).fetchone()
        if synced_row and synced_row["value"]:
            val = float(synced_row["value"])
            if val > 0:
                db.close()
                return val
        # Fall back to ai_players.cash
        row = db.execute(
            "SELECT cash FROM ai_players WHERE id=?", (PLAYER_ID,)
        ).fetchone()
        db.close()
        if row and row["cash"] is not None and 0 < float(row["cash"]) < 500_000:
            return float(row["cash"])
    except Exception as e:
        logger.warning("Webull cash lookup failed, using fallback: %s", e)
    return fallback


def _get_gex_context():
    """Return (gex_regime, put_wall) — safe, never raises."""
    try:
        from gex_calculator import get_latest_snapshot
        gex = get_latest_snapshot("SPY")
        if not gex:
            return "unknown", 0
        total_gex = gex.get("total_gex", 0) or 0
        regime = "pinned" if total_gex > 0 else "volatile"
        put_wall = gex.get("put_wall", 0) or 0
        return regime, put_wall
    except Exception:
        return "unknown", 0


def _get_live_webull_portfolio():
    """Try to get live Webull positions from the synced cache in DB."""
    try:
        import json
        db = _get_db()
        row = db.execute("SELECT value FROM settings WHERE key='webull_positions_cache'").fetchone()
        db.close()
        if row and row[0]:
            return json.loads(row[0])
    except Exception:
        pass
    return None


def generate_kirk_advisory():
    """Generate actionable recommendations for Kirk's Webull positions."""
    try:
        from engine.paper_trader import get_portfolio
        # Try live Webull cache first; fall back to paper_trader DB
        live = _get_live_webull_portfolio()
        if live and live.get("positions"):
            portfolio = live
        else:
            portfolio = get_portfolio(PLAYER_ID)
        positions = portfolio.get("positions", [])
        cash = _get_live_cash(fallback=portfolio.get("cash", 0))

        # Portfolio value from env var (manually updated from real Webull account)
        env_value = os.environ.get("KIRK_PORTFOLIO_VALUE", "").strip()
        portfolio_value = None
        portfolio_value_label = "Manual update needed"
        if env_value and env_value != "0":
            try:
                portfolio_value = float(env_value)
                portfolio_value_label = "Webull (manually tracked)"
            except ValueError:
                pass

        fg = get_fear_greed_index()
        fg_score = fg.get("score", 50) if fg else 50
        vix = 20.0
        try:
            from engine.vix_monitor import get_vix_term_structure
            vix_data = get_vix_term_structure()
            raw_vix = vix_data.get("vix")
            if raw_vix is not None and 5.0 <= float(raw_vix) <= 90.0:
                vix = float(raw_vix)
            else:
                logger.warning("VIX sanity check failed (got %s) — using default 20.0", raw_vix)
                vix = 20.0
        except Exception as vix_err:
            logger.warning("vix_monitor unavailable (%s) — using default 20.0", vix_err)

        gex_regime, put_wall = _get_gex_context()

        # Pull trade history + backtest context
        held_symbols = [p.get("symbol", "") for p in positions if p.get("symbol")]
        trade_history = _get_trade_history_summary()
        backtest_ctx = _get_backtest_context(held_symbols)
        rebalance_trims = _get_rebalance_recs(held_symbols)

        recommendations = []

        for pos in positions:
            symbol = pos.get("symbol", "")
            entry = pos.get("avg_price", 0) or 0
            current = pos.get("current_price", 0) or 0
            qty = pos.get("qty", 0) or 0

            if entry <= 0 or not symbol:
                continue

            # Refresh current price if stale/missing
            if current <= 0:
                try:
                    price_data = get_stock_price(symbol)
                    current = price_data.get("price", 0) if price_data else 0
                except Exception:
                    pass
            if current <= 0:
                current = entry  # fallback: treat as flat

            pnl_pct = (current - entry) / entry * 100
            value = qty * current

            rec = {
                "symbol": symbol,
                "qty": qty,
                "entry": round(entry, 2),
                "current": round(current, 2),
                "pnl_pct": round(pnl_pct, 1),
                "value": round(value, 2),
                "action": "HOLD",
                "reasoning": "",
                "urgency": "low",
            }

            if pnl_pct <= STOP_LOSS_PCT:
                rec["action"] = "SELL"
                rec["reasoning"] = f"Hit -{abs(STOP_LOSS_PCT):.0f}% stop loss. Cut the loss."
                rec["urgency"] = "critical"

            elif pnl_pct <= TRIM_WARNING_PCT:
                if gex_regime == "volatile":
                    rec["action"] = "TRIM"
                    rec["reasoning"] = (
                        f"Down {pnl_pct:.1f}%, GEX negative gamma = more downside likely. "
                        f"Trim before stop hits."
                    )
                    rec["urgency"] = "high"
                else:
                    rec["action"] = "HOLD"
                    rec["reasoning"] = (
                        f"Down {pnl_pct:.1f}% but GEX positive = support likely. Hold with stop."
                    )
                    rec["urgency"] = "medium"

            elif pnl_pct <= -3:
                if vix > 30 and fg_score < 35:
                    rec["action"] = "TRIM"
                    rec["reasoning"] = (
                        f"Down {pnl_pct:.1f}%, VIX {vix:.0f} + F&G {fg_score} = fear rising. "
                        f"Consider trimming."
                    )
                    rec["urgency"] = "medium"
                else:
                    rec["action"] = "HOLD"
                    rec["reasoning"] = f"Down {pnl_pct:.1f}% but manageable. Stop protects downside."
                    rec["urgency"] = "low"

            elif pnl_pct >= WINNER_HOLD_PCT:
                rec["action"] = "HOLD"
                rec["reasoning"] = f"Winner at +{pnl_pct:.1f}%. Let it run. Consider trailing stop."
                rec["urgency"] = "low"

            else:
                rec["action"] = "HOLD"
                rec["reasoning"] = f"Flat at {pnl_pct:+.1f}%. No action needed."
                rec["urgency"] = "low"

            # Append backtest context if available
            bt = backtest_ctx.get(symbol)
            if bt:
                rec["reasoning"] += (
                    f" Backtest avg: {bt['avg_return']:+.1f}% return, "
                    f"{bt['avg_win_rate']:.0f}% win rate over {bt['runs']} runs."
                )

            # Append rebalance conflict note if rebalance says TRIM but Kirk says HOLD
            rb = rebalance_trims.get(symbol)
            if rb and rec["action"] == "HOLD":
                rec["reasoning"] += (
                    f" Note: Rebalance engine suggests {rb['action']} "
                    f"(concentration target) — Kirk's regime analysis says HOLD."
                )
            elif rb and rec["action"] in ("HOLD", "TRIM"):
                rec["reasoning"] += f" Rebalance also suggests trimming ({rb.get('rationale','')[:60]})."

            # Append captain's trade history win rate (once, on first position only)
            if trade_history and not recommendations:
                wr = trade_history.get("win_rate", 0)
                total = trade_history.get("total_closed", 0)
                rec["reasoning"] += f" Your 127-trade history: {wr}% win rate ({total} closed trades)."

            recommendations.append(rec)

            # Log non-HOLD recommendations (dedup: skip if same ticker+action logged in last 30 min)
            if rec["action"] != "HOLD":
                try:
                    _db = _get_db()
                    recent = _db.execute(
                        "SELECT id FROM kirk_advisory_log WHERE ticker=? AND action=? "
                        "AND created_at >= datetime('now','-30 minutes') LIMIT 1",
                        (symbol, rec["action"])
                    ).fetchone()
                    if not recent:
                        _db.execute(
                            "INSERT INTO kirk_advisory_log (ticker, action, message, alert_type, "
                            "fear_greed_score, vix_level, created_at) VALUES (?,?,?,?,?,?,datetime('now'))",
                            (symbol, rec["action"], rec["reasoning"],
                             rec["urgency"], fg_score, round(vix, 1))
                        )
                        _db.commit()
                    _db.close()
                except Exception:
                    pass

        # Cash deployment recommendation
        if fg_score < 35 and vix > 28:
            cash_action = "DEPLOY"
            cash_reasoning = (
                f"F&G {fg_score} + VIX {vix:.0f} = extreme fear. "
                f"Consider buying SPY near put wall ${put_wall:.0f}."
            )
        elif fg_score < 45:
            cash_action = "WAIT"
            cash_reasoning = f"F&G {fg_score} = mild fear. Wait for F&G < 35 to deploy."
        else:
            cash_action = "WAIT"
            cash_reasoning = f"F&G {fg_score} = neutral/greed. No rush to deploy cash."

        return {
            "positions": recommendations,
            "cash": round(cash, 2),
            "cash_source": "KIRK_PORTFOLIO_CASH env" if os.environ.get("KIRK_PORTFOLIO_CASH", "").strip() not in ("", "0") else "Manual update needed",
            "portfolio_value": portfolio_value,
            "portfolio_value_label": portfolio_value_label,
            "cash_recommendation": {"action": cash_action, "reasoning": cash_reasoning},
            "market_context": {
                "fg_score": fg_score,
                "vix": round(vix, 1),
                "gex_regime": gex_regime,
                "put_wall": put_wall,
            },
            "trade_history": trade_history,
            "rebalance_conflicts": list(rebalance_trims.keys()),
            "generated_at": datetime.now(pytz.timezone("US/Arizona")).isoformat(),
        }

    except Exception as e:
        logger.error(f"Kirk advisory error: {e}")
        return {"error": str(e)}
