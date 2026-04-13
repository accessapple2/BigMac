"""
Cost Tracker — token-level API cost tracking for all AI providers.

Estimates tokens by: input = len(prompt) / 4, output = len(response) / 4
Stores per-call data in api_costs table for granular analytics.
"""
from __future__ import annotations
import sqlite3
import threading
from datetime import datetime

DB = "data/trader.db"
_lock = threading.Lock()

# In-memory set of player IDs currently in fallback mode (free local inference).
# Maintained by engine/fallback.py — avoids DB query on every log_cost() call.
_fallback_active: set[str] = set()
_fallback_set_lock = threading.Lock()


def mark_player_fallback(player_id: str, active: bool) -> None:
    """Called by engine/fallback.py when a player enters or exits fallback mode."""
    with _fallback_set_lock:
        if active:
            _fallback_active.add(player_id)
        else:
            _fallback_active.discard(player_id)

# Rates per 1M tokens (input, output).
# All cloud-named players now route to local Ollama — rates zeroed out.
# Only dalio-metals uses a real cloud API (Google Gemini Flash free tier).
TOKEN_RATES = {
    # ── Ollama local models — always free ──────────────────────────────────
    "ollama-local":      (0.00, 0.00),
    "ollama-gemma27b":   (0.00, 0.00),
    "ollama-deepseek":   (0.00, 0.00),
    "ollama-qwen3":      (0.00, 0.00),
    "ollama-llama":      (0.00, 0.00),
    "ollama-glm4":       (0.00, 0.00),
    "ollama-kimi":       (0.00, 0.00),
    "ollama-plutus":     (0.00, 0.00),
    "energy-arnold":     (0.00, 0.00),
    "dayblade-0dte":     (0.00, 0.00),
    "navigator":         (0.00, 0.00),
    # ── Formerly paid — now routed to Ollama locally ───────────────────────
    "claude-haiku":      (0.00, 0.00),  # Lt. Malcolm Reed → ollama/qwen2.5-coder:7b
    "claude-sonnet":     (0.00, 0.00),  # Captain Sisko    → ollama/qwen3.5:9b
    "gemini-2.5-flash":  (0.00, 0.00),  # Lt. Cmdr. Worf   → ollama/qwen3.5:9b
    "gemini-2.5-pro":    (0.00, 0.00),  # Seven of Nine    → ollama/qwen3:14b
    "options-sosnoff":   (0.00, 0.00),  # Counselor Troi   → ollama/qwen3.5:9b
    "gpt-4o":            (0.00, 0.00),  # Captain Janeway  → ollama/qwen3.5:9b
    "gpt-o3":            (0.00, 0.00),  # Lt. Tuvok        → ollama/deepseek-r1:7b
    "grok-3":            (0.00, 0.00),  # Ensign Hoshi     → ollama/qwen3.5:9b
    "grok-4":            (0.00, 0.00),  # Lt. Cmdr. Spock  → ollama/deepseek-r1:7b
    "cto-grok42":        (0.00, 0.00),  # CTO Grok 4.2     → ollama/qwen2.5-coder:7b
    "kirk-grok-advisor": (3.00, 15.00), # Kirk Grok Swing Advisor — real xAI API calls
    "first-officer":     (0.00, 0.00),
    "q-entity":          (0.00, 0.00),
    # ── Google free tier (dalio-metals uses Gemini Flash — $0 under quota) ─
    "dalio-metals":      (0.00, 0.00),
}


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 chars per token."""
    return max(1, len(text) // 4)


def _is_local_provider(player_id: str) -> bool:
    """Check if player uses a free local provider (e.g. Ollama) via DB lookup."""
    try:
        conn = _conn()
        row = conn.execute(
            "SELECT provider FROM ai_players WHERE id = ?", (player_id,)
        ).fetchone()
        conn.close()
        return row is not None and row["provider"] in ("ollama", "dayblade")
    except Exception:
        return False


def compute_cost(player_id: str, input_tokens: int, output_tokens: int) -> float:
    """Compute USD cost from token counts using per-model rates."""
    # Fallback mode: always $0
    if player_id in _fallback_active:
        return 0.0
    # DB provider check comes FIRST — if the player is on Ollama/dayblade, it's free
    # regardless of what TOKEN_RATES says (prevents stale rates from billing local calls)
    if player_id.startswith("ollama-") or _is_local_provider(player_id):
        return 0.0
    # Then consult TOKEN_RATES for known cloud players
    if player_id in TOKEN_RATES:
        rates = TOKEN_RATES[player_id]
    else:
        rates = (3.00, 15.00)  # conservative default for unknown cloud players
    input_cost = (input_tokens / 1_000_000) * rates[0]
    output_cost = (output_tokens / 1_000_000) * rates[1]
    return input_cost + output_cost


def log_cost(player_id: str, call_type: str, prompt: str, response: str) -> float:
    """Log an API call with estimated tokens and cost. Returns cost_usd."""
    # Mark fallback calls so they're distinguishable in api_costs table
    if player_id in _fallback_active:
        call_type = f"fallback:{call_type}"
    input_tokens = estimate_tokens(prompt)
    output_tokens = estimate_tokens(response)
    cost_usd = compute_cost(player_id, input_tokens, output_tokens)
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")

    with _lock:
        try:
            conn = _conn()
            # Insert into granular api_costs table
            conn.execute("""
                INSERT INTO api_costs (player_id, call_type, input_tokens, output_tokens, cost_usd, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (player_id, call_type, input_tokens, output_tokens, cost_usd, now.isoformat()))

            # Also update legacy model_stats for backward compat
            conn.execute("""
                INSERT INTO model_stats (player_id, api_calls, total_cost, date)
                VALUES (?, 1, ?, ?)
                ON CONFLICT(player_id, date) DO UPDATE SET
                    api_calls = api_calls + 1, total_cost = total_cost + ?
            """, (player_id, cost_usd, today, cost_usd))

            conn.commit()
            conn.close()
        except Exception:
            pass

    return cost_usd


def get_daily_costs(date: str = None) -> dict:
    """Get per-model costs for a given date (default today)."""
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")
    conn = _conn()
    rows = conn.execute("""
        SELECT player_id,
               SUM(input_tokens) as total_input,
               SUM(output_tokens) as total_output,
               SUM(cost_usd) as total_cost,
               COUNT(*) as num_calls
        FROM api_costs WHERE date(timestamp) = ?
        GROUP BY player_id
    """, (date,)).fetchall()
    conn.close()
    return {r["player_id"]: dict(r) for r in rows}


def get_total_daily_cost(date: str = None) -> float:
    """Get total cost across all models for a date."""
    costs = get_daily_costs(date)
    return sum(c["total_cost"] for c in costs.values())


def get_free_call_tracking(date: str = None, free_calls_limit: int = None) -> dict:
    """Get free-call usage for a date from zero-cost api_costs rows."""
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")

    if free_calls_limit is None:
        try:
            from config import FREE_CALLS_DAILY_LIMIT
            free_calls_limit = int(FREE_CALLS_DAILY_LIMIT)
        except Exception:
            free_calls_limit = 1000

    conn = _conn()
    try:
        row = conn.execute(
            """
            SELECT COUNT(*) AS free_calls_used
            FROM api_costs
            WHERE date(timestamp) = ?
              AND COALESCE(cost_usd, 0) = 0
            """,
            (date,),
        ).fetchone()
    finally:
        conn.close()

    used = int((row["free_calls_used"] if row else 0) or 0)
    limit = max(0, int(free_calls_limit or 0))
    remaining = max(0, limit - used)
    return {
        "free_calls_used": used,
        "free_calls_remaining": remaining,
        "free_calls_limit": limit,
    }


def get_cumulative_costs() -> dict:
    """Get all-time cumulative costs per model."""
    conn = _conn()
    rows = conn.execute("""
        SELECT player_id,
               SUM(input_tokens) as total_input,
               SUM(output_tokens) as total_output,
               SUM(cost_usd) as total_cost,
               COUNT(*) as num_calls,
               MIN(timestamp) as first_call,
               MAX(timestamp) as last_call
        FROM api_costs
        GROUP BY player_id
    """).fetchall()
    conn.close()
    return {r["player_id"]: dict(r) for r in rows}


def get_cost_per_trade() -> dict:
    """Cost per trade = total API cost / number of trades executed."""
    conn = _conn()
    # Total costs per model
    costs = conn.execute("""
        SELECT player_id, SUM(cost_usd) as total_cost, COUNT(*) as num_calls
        FROM api_costs GROUP BY player_id
    """).fetchall()
    # Total trades per model
    trades = conn.execute("""
        SELECT player_id, COUNT(*) as num_trades FROM trades
        WHERE action != 'HOLD' GROUP BY player_id
    """).fetchall()
    conn.close()

    cost_map = {r["player_id"]: r["total_cost"] for r in costs}
    trade_map = {r["player_id"]: r["num_trades"] for r in trades}

    result = {}
    for pid in set(list(cost_map.keys()) + list(trade_map.keys())):
        tc = cost_map.get(pid, 0)
        nt = trade_map.get(pid, 0)
        result[pid] = {
            "total_cost": tc,
            "num_trades": nt,
            "cost_per_trade": tc / nt if nt > 0 else 0,
        }
    return result


def get_projected_monthly_cost() -> dict:
    """Project monthly cost based on last 7 days of usage."""
    conn = _conn()
    rows = conn.execute("""
        SELECT player_id, SUM(cost_usd) as week_cost, COUNT(DISTINCT date(timestamp)) as days_active
        FROM api_costs
        WHERE timestamp >= datetime('now', '-7 days')
        GROUP BY player_id
    """).fetchall()
    conn.close()

    result = {}
    total = 0.0
    for r in rows:
        days = max(r["days_active"], 1)
        daily_avg = r["week_cost"] / days
        monthly = daily_avg * 22  # ~22 trading days
        result[r["player_id"]] = {"daily_avg": daily_avg, "projected_monthly": monthly}
        total += monthly
    return {"by_model": result, "total_monthly": total}


def get_token_efficiency() -> dict:
    """Tokens per trade decision — measures model verbosity."""
    conn = _conn()
    rows = conn.execute("""
        SELECT player_id,
               AVG(input_tokens) as avg_input,
               AVG(output_tokens) as avg_output,
               AVG(input_tokens + output_tokens) as avg_total,
               COUNT(*) as num_calls
        FROM api_costs WHERE call_type = 'scan'
        GROUP BY player_id
    """).fetchall()
    conn.close()
    return {r["player_id"]: dict(r) for r in rows}


def get_free_vs_paid_pnl() -> dict:
    """Compare cumulative P&L: free local models vs paid cloud models."""
    free_ids = {pid for pid, rates in TOKEN_RATES.items() if rates == (0.0, 0.0)}
    conn = _conn()
    rows = conn.execute("""
        SELECT p.id, p.display_name, p.cash,
               COALESCE(SUM(CASE WHEN t.action='SELL' AND t.realized_pnl IS NOT NULL THEN t.realized_pnl ELSE 0 END), 0) as realized_pnl
        FROM ai_players p
        LEFT JOIN trades t ON p.id = t.player_id
        WHERE p.is_active = 1
        GROUP BY p.id
    """).fetchall()
    conn.close()

    free_pnl, paid_pnl = 0.0, 0.0
    free_models, paid_models = [], []
    for r in rows:
        pid = r["id"]
        starting = 3500.0 if pid == "dayblade-0dte" else (7021.81 if pid == "steve-webull" else 7000.0)
        pnl = r["cash"] - starting + r["realized_pnl"]
        entry = {"player_id": pid, "name": r["display_name"], "pnl": pnl}
        if pid in free_ids:
            free_pnl += pnl
            free_models.append(entry)
        else:
            paid_pnl += pnl
            paid_models.append(entry)

    return {
        "free": {"total_pnl": free_pnl, "models": free_models},
        "paid": {"total_pnl": paid_pnl, "models": paid_models},
    }


def get_model_roi_ranking() -> list:
    """Rank models by profit per dollar spent on API calls."""
    conn = _conn()
    costs = conn.execute("SELECT player_id, SUM(cost_usd) as total_cost FROM api_costs GROUP BY player_id").fetchall()
    cost_map = {r["player_id"]: r["total_cost"] for r in costs}

    players = conn.execute("""
        SELECT p.id, p.display_name, p.cash, p.provider,
               COALESCE(SUM(CASE WHEN t.action='SELL' AND t.realized_pnl IS NOT NULL THEN t.realized_pnl ELSE 0 END), 0) as realized_pnl
        FROM ai_players p
        LEFT JOIN trades t ON p.id = t.player_id
        WHERE p.is_active = 1
        GROUP BY p.id
    """).fetchall()
    conn.close()

    result = []
    for p in players:
        starting = 3500.0 if p["id"] == "dayblade-0dte" else (7021.81 if p["id"] == "steve-webull" else 7000.0)
        pnl = p["cash"] - starting + p["realized_pnl"]
        cost = cost_map.get(p["id"], 0)
        is_free = TOKEN_RATES.get(p["id"], (1, 1))[0] == 0.0
        roi = pnl / cost if cost > 0 else (float("inf") if pnl > 0 else 0)
        result.append({
            "player_id": p["id"],
            "name": p["display_name"],
            "provider": p["provider"],
            "pnl": round(pnl, 2),
            "api_cost": round(cost, 4),
            "net_pnl": round(pnl - cost, 2),
            "roi": round(roi, 1) if roi != float("inf") else 999999,
            "is_free": is_free,
        })
    result.sort(key=lambda x: x["roi"], reverse=True)
    return result


def get_model_efficiency_grades() -> list:
    """Grade each model A-F based on win rate, avg P&L, cost per trade, token efficiency."""
    conn = _conn()
    # Win rate and avg P&L per trade
    trade_stats = conn.execute("""
        SELECT player_id,
               COUNT(*) as total_sells,
               SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
               AVG(realized_pnl) as avg_pnl
        FROM trades WHERE action='SELL' AND realized_pnl IS NOT NULL
        GROUP BY player_id
    """).fetchall()
    trade_map = {r["player_id"]: dict(r) for r in trade_stats}

    # Costs
    cost_rows = conn.execute("SELECT player_id, SUM(cost_usd) as total_cost, COUNT(*) as calls FROM api_costs GROUP BY player_id").fetchall()
    cost_map = {r["player_id"]: {"total_cost": r["total_cost"], "calls": r["calls"]} for r in cost_rows}

    # Token efficiency
    eff_rows = conn.execute("SELECT player_id, AVG(output_tokens) as avg_output FROM api_costs WHERE call_type='scan' GROUP BY player_id").fetchall()
    eff_map = {r["player_id"]: r["avg_output"] for r in eff_rows}

    # Trade counts
    total_trades = conn.execute("SELECT player_id, COUNT(*) as cnt FROM trades GROUP BY player_id").fetchall()
    trades_map = {r["player_id"]: r["cnt"] for r in total_trades}

    players = conn.execute("SELECT id, display_name, provider FROM ai_players WHERE is_active=1").fetchall()
    conn.close()

    result = []
    for p in players:
        pid = p["id"]
        ts = trade_map.get(pid, {"total_sells": 0, "wins": 0, "avg_pnl": 0})
        cm = cost_map.get(pid, {"total_cost": 0, "calls": 0})
        nt = trades_map.get(pid, 0)

        win_rate = (ts["wins"] / ts["total_sells"] * 100) if ts["total_sells"] > 0 else 0
        avg_pnl = ts["avg_pnl"] or 0
        cost_per_trade = cm["total_cost"] / nt if nt > 0 else 0
        avg_output = eff_map.get(pid, 0) or 0

        # Score components (0-100 each)
        wr_score = min(100, win_rate * 1.5)  # 67% win rate = 100
        pnl_score = min(100, max(0, (avg_pnl + 50) * 1.0))  # +$50 avg = 100
        cost_score = 100 if cost_per_trade == 0 else max(0, 100 - cost_per_trade * 10000)
        eff_score = 100 if avg_output == 0 else max(0, 100 - avg_output / 5)

        total_score = wr_score * 0.35 + pnl_score * 0.30 + cost_score * 0.20 + eff_score * 0.15

        if total_score >= 85:
            grade = "A"
        elif total_score >= 70:
            grade = "B"
        elif total_score >= 55:
            grade = "C"
        elif total_score >= 40:
            grade = "D"
        else:
            grade = "F"

        result.append({
            "player_id": pid,
            "name": p["display_name"],
            "provider": p["provider"],
            "grade": grade,
            "score": round(total_score, 1),
            "win_rate": round(win_rate, 1),
            "avg_pnl": round(avg_pnl, 2),
            "cost_per_trade": round(cost_per_trade, 4),
            "avg_output_tokens": round(avg_output, 0),
            "total_trades": nt,
        })
    result.sort(key=lambda x: x["score"], reverse=True)
    return result


def get_dead_models(hours: int = 48) -> list:
    """Find active models that haven't traded in N hours."""
    conn = _conn()
    rows = conn.execute(f"""
        SELECT p.id, p.display_name, p.provider,
               MAX(t.executed_at) as last_trade,
               ROUND((julianday('now') - julianday(COALESCE(MAX(t.executed_at), '2000-01-01'))) * 24, 1) as hours_since
        FROM ai_players p
        LEFT JOIN trades t ON p.id = t.player_id
        WHERE p.is_active = 1 AND COALESCE(p.is_paused, 0) = 0
        GROUP BY p.id
        HAVING hours_since > ?
    """, (hours,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_model_diversity() -> dict:
    """Track position overlap across models. Flag concentration risk."""
    conn = _conn()
    positions = conn.execute("""
        SELECT player_id, symbol FROM positions
    """).fetchall()
    conn.close()

    # Count how many models hold each symbol
    symbol_holders = {}
    model_holdings = {}
    for p in positions:
        sym = p["symbol"]
        pid = p["player_id"]
        if sym not in symbol_holders:
            symbol_holders[sym] = set()
        symbol_holders[sym].add(pid)
        if pid not in model_holdings:
            model_holdings[pid] = set()
        model_holdings[pid].add(sym)

    # Concentration alerts: 5+ models holding same stock
    concentration_alerts = []
    for sym, holders in symbol_holders.items():
        if len(holders) >= 5:
            concentration_alerts.append({"symbol": sym, "holders": len(holders), "models": list(holders)})
    concentration_alerts.sort(key=lambda x: x["holders"], reverse=True)

    # Pairwise overlap (Jaccard similarity)
    models = list(model_holdings.keys())
    total_overlap = 0
    pairs = 0
    for i in range(len(models)):
        for j in range(i + 1, len(models)):
            s1 = model_holdings[models[i]]
            s2 = model_holdings[models[j]]
            if s1 or s2:
                jaccard = len(s1 & s2) / len(s1 | s2) if (s1 | s2) else 0
                total_overlap += jaccard
                pairs += 1

    avg_overlap = total_overlap / pairs if pairs > 0 else 0
    diversity = "healthy" if avg_overlap < 0.3 else "moderate" if avg_overlap < 0.6 else "high_overlap"

    return {
        "avg_overlap_pct": round(avg_overlap * 100, 1),
        "diversity_rating": diversity,
        "concentration_alerts": concentration_alerts,
        "symbols_held": {sym: len(holders) for sym, holders in symbol_holders.items()},
        "total_models_with_positions": len(model_holdings),
    }


def check_budget_alert(daily_limit: float = 5.0) -> dict | None:
    """Check if daily cost exceeds budget. Returns alert dict or None."""
    total = get_total_daily_cost()
    if total > daily_limit:
        costs = get_daily_costs()
        top_spender = max(costs.items(), key=lambda x: x[1]["total_cost"]) if costs else ("unknown", {"total_cost": 0})
        return {
            "total_today": round(total, 4),
            "limit": daily_limit,
            "top_spender": top_spender[0],
            "top_cost": round(top_spender[1]["total_cost"], 4),
        }
    return None


def check_auto_pause_losers() -> list:
    """Auto-pause paid models with 3+ consecutive losing days."""
    conn = _conn()
    # Get paid model IDs
    paid_ids = [pid for pid, rates in TOKEN_RATES.items() if rates != (0.0, 0.0)]

    paused = []
    for pid in paid_ids:
        # Get last 3 days of portfolio snapshots
        rows = conn.execute("""
            SELECT date(recorded_at) as d,
                   MAX(total_value) as end_val,
                   MIN(total_value) as start_val
            FROM portfolio_history
            WHERE player_id = ? AND recorded_at >= datetime('now', '-4 days')
            GROUP BY date(recorded_at)
            ORDER BY d DESC
            LIMIT 3
        """, (pid,)).fetchall()

        if len(rows) >= 3:
            # Check if all 3 days were losses (end < start)
            all_losing = all(r["end_val"] < r["start_val"] for r in rows)
            if all_losing:
                # Check current status
                player = conn.execute("SELECT COALESCE(is_paused, 0) as is_paused FROM ai_players WHERE id=?", (pid,)).fetchone()
                if player and not player["is_paused"]:
                    # Get total cost wasted
                    cost = conn.execute(
                        "SELECT COALESCE(SUM(cost_usd), 0) as c FROM api_costs WHERE player_id=? AND timestamp >= datetime('now', '-3 days')",
                        (pid,)
                    ).fetchone()
                    conn.execute("UPDATE ai_players SET is_paused=1 WHERE id=?", (pid,))
                    paused.append({
                        "player_id": pid,
                        "cost_wasted": round(cost["c"], 4) if cost else 0,
                        "days_losing": 3,
                    })

    if paused:
        conn.commit()
    conn.close()
    return paused
