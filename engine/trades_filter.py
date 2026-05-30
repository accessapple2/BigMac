"""
engine/trades_filter.py — canonical clean-trades boundary for realized-PnL / win-rate rollups.

HM-TRACKING-AGGREGATOR (2026-05-30). The single source of truth for "which trades count" in
fleet/agent realized-PnL and win-rate rollups. Replaces ~30 inline copies of
`action='SELL' AND realized_pnl IS NOT NULL` that silently included two pollution sources:

  1. PRE-S5 MISPRICING GARBAGE (pre-2026-05-14): impossible prices (MU $533, TSLA $4;
     gemini-2.5-pro alone carries +$225K) from before the price-writeback fix. → date floor.
  2. TRACKING-ROUTE POLLUTION (dalio-metals et al.): log-only / manual-SQL rows that book
     realized PnL against tracking-only players. → player exclusion.

It is NOT `alpaca_order_id IS NOT NULL`. That conflates "not real Alpaca" with "dirty", but
72% of clean post-boundary fleet performance is legitimate SIM (paper-evaluation agents);
the aoid filter would erase 11 of 13 agents. See docs/TRACKING-AGGREGATOR-SITE-MAP-2026-05-30.md.

Usage — inject the WHERE fragment into existing rollup SQL:
    from engine.trades_filter import CLEAN_TRADES_WHERE
    cur.execute(f"SELECT ... FROM trades WHERE action='SELL' AND realized_pnl IS NOT NULL "
                f"AND {CLEAN_TRADES_WHERE} GROUP BY player_id")
Or use the helper for a standard per-player rollup:
    from engine.trades_filter import fleet_realized_pnl
    rows = fleet_realized_pnl(conn)                 # fleet, clean
    one  = fleet_realized_pnl(conn, player="ollama-plutus")
    sim  = fleet_realized_pnl(conn, sim_eval=True)  # proving_ground: date-floor only
"""

# First real Alpaca fill = the trustworthiness boundary. Pre = pre-S5 mispricing garbage.
GARBAGE_FLOOR = "2026-05-14 07:37:44"

# Tracking-route players: their portfolio has execution_mode='tracking' OR type='physical'
# (mirror of engine/paper_trader.py _EXECUTION_PORTFOLIO_BY_PLAYER → Enterprise Computer / Schwab).
# dalio-metals is the only one with `trades` rows today; the other two are 0-row future-proofing.
# If a NEW tracking player is added to _EXECUTION_PORTFOLIO_BY_PLAYER (tracking portfolio), add it here.
TRACKING_PLAYERS = ("dalio-metals", "enterprise-computer", "schwab")

_TRACK_SQL = ", ".join("'%s'" % p for p in TRACKING_PLAYERS)

# Fleet / dashboard / scorecard rollups → drop garbage AND tracking pollution.
CLEAN_TRADES_WHERE = "executed_at >= '%s' AND player_id NOT IN (%s)" % (GARBAGE_FLOOR, _TRACK_SQL)

# Sim-evaluation gates (proving_ground) → drop garbage ONLY; keep clean sim (the eval data).
SIM_EVAL_WHERE = "executed_at >= '%s'" % GARBAGE_FLOOR


def fleet_realized_pnl(conn, player=None, season=None, since_days=None, sim_eval=False):
    """Clean realized-PnL + win-rate rollup over `trades`.

    Returns a single dict when `player` is given, else a list of per-player dicts.
    Each dict: player_id, trade_count, wins, losses, total_pnl, win_rate (0-100 or None).
    sim_eval=True → date-floor-only boundary (proving_ground keeps its clean sim).
    """
    where = SIM_EVAL_WHERE if sim_eval else CLEAN_TRADES_WHERE
    clauses = ["action IN ('SELL', 'COVER')", "realized_pnl IS NOT NULL", where]
    params = []
    if player is not None:
        clauses.append("player_id = ?")
        params.append(player)
    if season is not None:
        clauses.append("season = ?")
        params.append(season)
    if since_days is not None:
        clauses.append("executed_at >= datetime('now', ?)")
        params.append("-%d days" % int(since_days))
    sql = (
        "SELECT player_id, COUNT(*) AS trade_count, "
        "SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) AS wins, "
        "SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END) AS losses, "
        "ROUND(SUM(realized_pnl), 2) AS total_pnl, "
        "ROUND(100.0 * SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS win_rate "
        "FROM trades WHERE " + " AND ".join(clauses) + " GROUP BY player_id"
    )
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    if player is not None:
        return rows[0] if rows else {
            "player_id": player, "trade_count": 0, "wins": 0,
            "losses": 0, "total_pnl": 0.0, "win_rate": None,
        }
    return rows
