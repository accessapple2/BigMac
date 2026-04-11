"""CrewAI-style Strategy-Writing Crew for USS TradeMinds.

4 agents collaborate sequentially: Researcher → Writer → Backtester → Risk Reviewer.
Uses direct Ollama/Gemini API calls (no crewai dependency — Python 3.9 compatible).
All backtest results saved to strategy_backtests table.
"""
from __future__ import annotations
import json
import os
import sqlite3
import requests
from datetime import datetime
from rich.console import Console

console = Console()
DB = "data/trader.db"
OLLAMA_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("CREWAI_MODEL", "qwen3.5:9b")


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def _ollama(prompt, system="", model=None):
    """Call Ollama and return text."""
    payload = {"model": model or OLLAMA_MODEL, "prompt": prompt, "stream": False}
    if system:
        payload["system"] = system
    try:
        r = requests.post(f"{OLLAMA_URL}/api/generate", json=payload, timeout=120)
        if r.ok:
            return r.json().get("response", "").strip()
    except Exception as e:
        console.log(f"[red]Ollama error: {e}")
    return ""


def _gemini(prompt, system=""):
    """Call Gemini free tier (gemini-3.1-flash-lite, 400/day cap) with Ollama fallback."""
    from engine.gemini_free_tier import call_gemini
    return call_gemini(prompt, system)


def _get_backtest_history():
    """Get recent backtest results."""
    conn = _conn()
    rows = conn.execute(
        "SELECT strategy_type, ticker, total_return, sharpe_ratio, "
        "max_drawdown, win_rate, profit_factor, num_trades, created_at "
        "FROM strategy_backtests ORDER BY created_at DESC LIMIT 20"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _get_regime():
    """Get current market regime."""
    try:
        r = requests.get("http://127.0.0.1:8080/api/regime", timeout=10)
        return r.json() if r.ok else {}
    except Exception:
        return {}


def _get_strategies():
    """Get Strategy Lab strategies."""
    try:
        r = requests.get("http://127.0.0.1:8080/api/strategy-lab/strategies", timeout=10)
        return r.json() if r.ok else {}
    except Exception:
        return {}


def _run_backtest(params):
    """Run VectorBT backtest and save results."""
    import vectorbt as vbt
    import yfinance as yf

    symbol = params.get("symbol", "SPY")
    period = params.get("period", "2y")
    rsi_window = params.get("rsi_window", 14)
    rsi_buy = params.get("rsi_buy_threshold", 30)
    rsi_sell = params.get("rsi_sell_threshold", 70)
    stop_loss = params.get("stop_loss_pct", 0.05)
    take_profit = params.get("take_profit_pct", 0.10)

    data = yf.download(symbol, period=period, interval="1d", progress=False)
    close = data["Close"].squeeze()
    rsi = vbt.RSI.run(close, window=rsi_window)
    entries = rsi.rsi_below(rsi_buy)
    exits = rsi.rsi_above(rsi_sell)
    pf = vbt.Portfolio.from_signals(
        close, entries, exits,
        sl_stop=stop_loss, tp_stop=take_profit,
        init_cash=10000, fees=0.001, freq='1D'
    )

    n_trades = len(pf.trades.records_readable)
    import numpy as np
    sharpe = pf.sharpe_ratio()
    results = {
        "strategy_name": params.get("name", "unnamed"),
        "symbol": symbol,
        "total_return": round(pf.total_return() * 100, 2),
        "sharpe_ratio": round(sharpe if not np.isnan(sharpe) else 0, 3),
        "max_drawdown": round(pf.max_drawdown() * 100, 2),
        "win_rate": round(pf.trades.win_rate() * 100, 2) if n_trades > 0 else 0,
        "profit_factor": round(pf.trades.profit_factor(), 3) if n_trades > 0 else 0,
        "total_trades": n_trades,
    }

    # Save to strategy_backtests
    conn = _conn()
    conn.execute("""
        INSERT INTO strategy_backtests
        (source, ticker, strategy_type, parameters, total_return,
         sharpe_ratio, max_drawdown, win_rate, profit_factor, num_trades, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "crewai", symbol, results["strategy_name"],
        json.dumps(params), results["total_return"],
        results["sharpe_ratio"], results["max_drawdown"],
        results["win_rate"], results["profit_factor"],
        results["total_trades"], "CrewAI Strategy Crew backtest"
    ))
    conn.commit()
    conn.close()
    return results


def run_strategy_crew():
    """Run the 4-agent strategy crew: Research → Write → Backtest → Review."""
    console.log("[bold cyan]Strategy Crew: Assembling...")
    discussion = []

    # ── Agent 1: Strategy Researcher ──
    console.log("[cyan]  Agent 1: Strategy Research Officer analyzing...")
    history = _get_backtest_history()
    regime = _get_regime()
    strategies = _get_strategies()

    history_text = json.dumps(history[:10], indent=2, default=str) if history else "No backtest history."
    regime_text = json.dumps(regime, indent=2) if regime else "Regime unavailable."
    strats_text = json.dumps(strategies, indent=2, default=str) if strategies else "No strategies."

    research_output = _ollama(
        f"Analyze the market environment and suggest 2-3 strategy concepts:\n\n"
        f"RECENT BACKTEST RESULTS:\n{history_text}\n\n"
        f"CURRENT REGIME:\n{regime_text}\n\n"
        f"DEPLOYED STRATEGIES:\n{strats_text}\n\n"
        f"Based on this data, identify which strategies work in current conditions. "
        f"Output a research brief with 2-3 specific strategy ideas.",
        system=(
            "You are a Starfleet Strategy Research Officer aboard USS TradeMinds. "
            "You analyze backtest history, market regime, and existing strategies to "
            "identify opportunities. Be concise and data-driven. Max 400 words."
        )
    )
    discussion.append(f"RESEARCH OFFICER:\n{research_output}")
    console.log(f"[green]  Research complete ({len(research_output)} chars)")

    if not research_output:
        return {"status": "failed", "error": "Research agent failed", "discussion": discussion}

    # ── Agent 2: Strategy Writer ──
    console.log("[cyan]  Agent 2: Quantitative Strategy Engineer writing...")
    strategy_json_text = _ollama(
        f"Based on this research brief, write ONE trading strategy as JSON:\n\n"
        f"{research_output}\n\n"
        f"Output ONLY a valid JSON object with these fields:\n"
        f'{{"name": "strategy_name", "thesis": "why this works", '
        f'"symbol": "SPY", "rsi_window": 14, "rsi_buy_threshold": 30, '
        f'"rsi_sell_threshold": 70, "stop_loss_pct": 0.05, '
        f'"take_profit_pct": 0.10, "period": "2y"}}',
        system=(
            "You are a Quantitative Strategy Engineer. Output ONLY valid JSON. "
            "No markdown, no explanations, just the JSON object."
        )
    )
    discussion.append(f"STRATEGY ENGINEER:\n{strategy_json_text}")

    # Parse JSON from response
    try:
        # Try to extract JSON from the response
        import re
        json_match = re.search(r'\{[^{}]*\}', strategy_json_text, re.DOTALL)
        if json_match:
            strategy_params = json.loads(json_match.group())
        else:
            strategy_params = json.loads(strategy_json_text)
    except (json.JSONDecodeError, Exception) as e:
        console.log(f"[red]  Strategy writer output not valid JSON: {e}")
        return {"status": "failed", "error": f"Strategy writer failed: {e}", "discussion": discussion}

    console.log(f"[green]  Strategy written: {strategy_params.get('name', 'unnamed')}")

    # ── Agent 3: Backtester ──
    console.log("[cyan]  Agent 3: Holodeck Simulation Officer backtesting...")
    try:
        backtest_results = _run_backtest(strategy_params)
        discussion.append(f"HOLODECK OFFICER:\n{json.dumps(backtest_results, indent=2)}")
        console.log(f"[green]  Backtest complete: {backtest_results['total_return']}% return, "
                     f"Sharpe {backtest_results['sharpe_ratio']}")
    except Exception as e:
        console.log(f"[red]  Backtest error: {e}")
        return {"status": "failed", "error": f"Backtest failed: {e}", "discussion": discussion}

    # ── Agent 4: Risk Reviewer (Gemini Flash) ──
    console.log("[cyan]  Agent 4: Chief Risk Officer reviewing...")
    review_output = _gemini(
        f"Review this strategy backtest and decide APPROVE or REJECT:\n\n"
        f"STRATEGY: {json.dumps(strategy_params, indent=2)}\n\n"
        f"RESULTS: {json.dumps(backtest_results, indent=2)}\n\n"
        f"CRITERIA (all must pass for APPROVE):\n"
        f"- Sharpe ratio >= 0.5\n"
        f"- Max drawdown <= 25%\n"
        f"- Win rate >= 35%\n"
        f"- Total trades >= 15\n"
        f"- Profit factor >= 1.2\n\n"
        f"Output: APPROVED or REJECTED with brief reasoning.",
        system=(
            "You are Commander Spock, Chief Risk Officer. Logic above emotion. "
            "Evaluate strictly against the criteria. Be concise."
        )
    )
    discussion.append(f"RISK OFFICER (SPOCK):\n{review_output}")

    approved = "APPROV" in review_output.upper()
    if approved:
        # Save approved strategy
        conn = _conn()
        conn.execute("""
            INSERT INTO strategy_backtests
            (source, ticker, strategy_type, parameters, total_return,
             sharpe_ratio, max_drawdown, win_rate, profit_factor,
             num_trades, recommendation, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "crewai-approved",
            backtest_results["symbol"],
            strategy_params.get("name", "unnamed"),
            json.dumps(strategy_params),
            backtest_results["total_return"],
            backtest_results["sharpe_ratio"],
            backtest_results["max_drawdown"],
            backtest_results["win_rate"],
            backtest_results["profit_factor"],
            backtest_results["total_trades"],
            "APPROVED by CrewAI Risk Reviewer",
            strategy_params.get("thesis", "")
        ))
        conn.commit()
        conn.close()
        console.log("[bold green]  Strategy APPROVED and saved!")
    else:
        console.log("[yellow]  Strategy REJECTED by Risk Officer")

    result = {
        "status": "complete",
        "approved": approved,
        "strategy": strategy_params,
        "backtest": backtest_results,
        "review": review_output,
        "discussion": discussion,
    }

    console.log(f"[bold green]Strategy Crew complete — {'APPROVED' if approved else 'REJECTED'}")
    return result


if __name__ == "__main__":
    print("Launching Strategy Crew...")
    result = run_strategy_crew()
    print("\n" + "=" * 60)
    print(f"RESULT: {result['status']} — {'APPROVED' if result.get('approved') else 'REJECTED'}")
    print("=" * 60)
    for d in result.get("discussion", []):
        print(d)
        print("-" * 40)
