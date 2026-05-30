# RETIRED 2026-05-30 — HM-EQUITY-CURVE-ORPHAN (YELLOW retire)
# Source: dashboard/app.py:9449-9479 (removed in this commit).
# Reason: /api/equity-curve had ZERO fetch consumers (grep dashboard/static/ = no
#   matches) and get_equity_curve() had ZERO internal callers. Cosmetic dead path.
#   It parsed "PnL: $X" out of trades.reasoning via regex — a fragile legacy
#   pattern superseded by trades.realized_pnl + the arena /api/arena/equity-curve
#   endpoint (a DIFFERENT function, still live, consumed across index.html).
# Rehab: restore this block under @app.get("/api/equity-curve") in dashboard/app.py
#   if a real consumer is ever built. Prefer reading realized_pnl over regex.

@app.get("/api/equity-curve")
def get_equity_curve(starting_capital: float = 10000, season: int = None):
    """Get equity curve from trade history, filtered by season."""
    conn = _conn()
    if season is None:
        s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        season = int(s_row["value"]) if s_row else 2
    if season == -1:
        sells = conn.execute(
            "SELECT executed_at, reasoning FROM trades WHERE action='SELL' ORDER BY executed_at"
        ).fetchall()
    else:
        sells = conn.execute(
            "SELECT executed_at, reasoning FROM trades WHERE action='SELL' AND season=? ORDER BY executed_at",
            (season,)
        ).fetchall()
    conn.close()
    import re
    curve = [{"date": "start", "equity": starting_capital, "trade": None}]
    equity = starting_capital
    for s in sells:
        m = re.search(r'PnL: \$([+-]?[\d.]+)', s["reasoning"] or "")
        if m:
            pnl = float(m.group(1))
            equity += pnl
            curve.append({
                "date": (s["executed_at"] or "")[:10],
                "equity": round(equity, 2),
                "pnl": round(pnl, 2)
            })
    return curve
