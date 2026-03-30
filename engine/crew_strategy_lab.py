"""CrewAI Strategy Lab — 4 AI agents collaborate to develop trading strategies.

Spock (Analyst) → Data (Coder) → Riker (Risk) → Tester (Holodeck)
Uses direct Ollama API calls (no crewai dependency needed).
All running locally on gemma3:4b — zero API cost.
"""
from __future__ import annotations
import os
import re
import sys
import subprocess
import tempfile
import sqlite3
import requests
from datetime import datetime
from rich.console import Console

console = Console()
DB = "data/trader.db"
OLLAMA_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
MODEL = os.getenv("CREWAI_MODEL", "gemma3:4b")
CODE_MODEL = os.getenv("CREWAI_CODE_MODEL", "qwen2.5-coder:7b-instruct")


def _ollama(prompt: str, system: str = "", model: str = None) -> str:
    """Call Ollama and return text response."""
    payload = {"model": model or MODEL, "prompt": prompt, "stream": False}
    if system:
        payload["system"] = system
    try:
        r = requests.post(f"{OLLAMA_URL}/api/generate", json=payload, timeout=90)
        if r.ok:
            return r.json().get("response", "").strip()
    except Exception as e:
        console.log(f"[red]Ollama error: {e}")
    return ""


def get_backtest_context(symbol: str, limit: int = 10) -> str:
    """Pull recent backtest history to inform the crew."""
    try:
        conn = sqlite3.connect(DB, check_same_thread=False, timeout=10)
        rows = conn.execute(
            "SELECT strategy_type, parameters, total_return, win_rate, "
            "sharpe_ratio, max_drawdown, num_trades, recommendation "
            "FROM strategy_backtests WHERE ticker = ? "
            "ORDER BY created_at DESC LIMIT ?",
            (symbol.upper(), limit)
        ).fetchall()
        conn.close()
        if not rows:
            return "No previous backtests found for this ticker."
        lines = []
        for r in rows:
            ret = r[2] if r[2] is not None else 0
            wr = r[3] if r[3] is not None else 0
            sh = r[4] if r[4] is not None else 0
            dd = r[5] if r[5] is not None else 0
            nt = r[6] if r[6] is not None else 0
            rec = r[7] or ""
            lines.append(
                f"- {r[0]} {r[1] or 'N/A'}: {ret:.1f}% return, "
                f"{wr:.1f}% win rate, Sharpe {sh:.2f}, "
                f"DD {dd:.1f}%, {nt} trades"
                f"{' — ' + rec if rec else ''}"
            )
        return "\n".join(lines)
    except Exception:
        return "Backtest history unavailable."


def create_crew(user_prompt: str, symbol: str = "SPY", days: int = 365) -> dict:
    """Run 4-agent sequential chain: Spock → Data → Riker → Tester."""
    console.log(f"[cyan]Crew Strategy Lab: engaging for '{user_prompt[:50]}...'")

    # Get regime context
    regime_ctx = ""
    try:
        r = requests.get("http://localhost:8080/api/regime", timeout=5)
        if r.ok:
            rd = r.json()
            regime_ctx = (
                f"Current regime: {rd.get('regime','UNKNOWN')}. "
                f"VIX: {rd.get('vix','?')}. "
                f"SPY {'above' if rd.get('spy_above_200') else 'below'} 200-day MA."
            )
    except Exception:
        regime_ctx = "Regime data unavailable."

    backtest_context = get_backtest_context(symbol)
    discussion = []

    # === AGENT 1: SPOCK (Analyst) ===
    console.log("[cyan]  🖖 Spock analyzing...")
    spock_system = (
        "You are Spock, Chief Science Officer on USS TradeMinds. You think in pure logic. "
        "You design precise, backtestable trading strategies with exact numerical rules. "
        "You know RSI, MACD, Bollinger Bands, moving averages, volume, Fibonacci, regime detection. "
        "Be concise: maximum 300 words. Focus ONLY on entry rules, exit rules, stop-loss, and position sizing. "
        "Never suggest Monte Carlo, regression analysis, GARCH, or academic extensions."
    )
    spock_prompt = (
        f"User's strategy request: '{user_prompt}'\n"
        f"Symbol: {symbol}, Lookback: {days} days, Starting cash: $7,000\n"
        f"{regime_ctx}\n\n"
        f"PREVIOUS BACKTEST RESULTS FOR {symbol}:\n{backtest_context}\n\n"
        "Design a complete trading strategy with:\n"
        "1. Exact entry rules (e.g., 'Buy when RSI(14) crosses above 30')\n"
        "2. Exact exit rules (e.g., 'Sell when RSI(14) crosses below 70')\n"
        "3. Stop-loss level (e.g., '5% trailing stop')\n"
        "4. Position sizing (e.g., 'Risk max 2% per trade')\n"
        "Be specific with numbers. No vague rules. Build on what worked in past backtests."
    )
    spock_output = _ollama(spock_prompt, spock_system)
    discussion.append(f"🖖 SPOCK (Strategy Analyst):\n{spock_output}")

    if not spock_output:
        return {"success": False, "error": "Spock failed to respond — check Ollama is running"}

    # === AGENT 2: DATA (Coder) — template-locked with qwen2.5-coder ===
    console.log("[cyan]  🤖 Data writing code...")
    data_system = (
        "You are Lt. Commander Data. Your ONLY job is to pick the closest "
        "template below and change ONLY the parameter values (windows, thresholds, days, symbol).\n\n"
        "RULE 1: NEVER invent new VectorBT functions. If it is not in the templates, do not use it.\n"
        "RULE 2: Output ONLY complete executable Python code. No markdown, no explanations.\n"
        "RULE 3: ALWAYS include sl_stop=0.02, tp_stop=0.01 in Portfolio.from_signals.\n"
        "RULE 4: Replace DAYS with the user requested period and SYMBOL with the ticker.\n"
        "RULE 5: Use the exact stats printing block shown in Template 1.\n\n"
        "TEMPLATE 1 — Pure RSI Mean Reversion (default):\n"
        "import vectorbt as vbt\n"
        "from datetime import datetime, timedelta\n"
        "start = (datetime.now() - timedelta(days=DAYS)).strftime('%Y-%m-%d')\n"
        "price = vbt.YFData.download('SYMBOL', start=start).get('Close')\n"
        "rsi = vbt.RSI.run(price, window=14).rsi\n"
        "entries = rsi.vbt.crossed_below(35)\n"
        "exits = rsi.vbt.crossed_above(70)\n"
        "pf = vbt.Portfolio.from_signals(\n"
        "    price, entries=entries, exits=exits,\n"
        "    sl_stop=0.02, tp_stop=0.01,\n"
        "    init_cash=7000, fees=0.001, freq='1D'\n"
        ")\n"
        "stats = pf.stats()\n"
        "print(stats)\n"
        "print(f'Total Return: {pf.total_return()*100:.2f}%')\n"
        "print(f'Sharpe: {pf.sharpe_ratio():.2f}')\n"
        "print(f'Max DD: {pf.max_drawdown()*100:.2f}%')\n"
        "print(f'Trades: {pf.trades.count()}')\n"
        "print(f'Win Rate: {pf.trades.win_rate()*100:.2f}%')\n"
        "print(f'Final Value: ${pf.final_value():.2f}')\n\n"
        "TEMPLATE 2 — RSI + Moving Average Regime Filter (use only if days > 250):\n"
        "Same as Template 1 but replace entries/exits with:\n"
        "ma200 = price.rolling(200).mean()\n"
        "entries = rsi.vbt.crossed_below(35) & (price > ma200)\n"
        "exits = rsi.vbt.crossed_above(70)\n\n"
        "TEMPLATE 3 — Bollinger Bands Mean Reversion:\n"
        "Same structure as Template 1 but replace indicator + signals with:\n"
        "bb = vbt.BBANDS.run(price, window=20, alpha=2)\n"
        "entries = price.vbt.crossed_below(bb.lower)\n"
        "exits = price.vbt.crossed_above(bb.upper)\n\n"
        "TEMPLATE 4 — MACD Crossover:\n"
        "Same structure as Template 1 but replace indicator + signals with:\n"
        "macd_ind = vbt.MACD.run(price, fast_window=12, slow_window=26, signal_window=9)\n"
        "entries = macd_ind.macd.vbt.crossed_above(macd_ind.signal)\n"
        "exits = macd_ind.macd.vbt.crossed_below(macd_ind.signal)\n\n"
        "When in doubt, use Template 1 and only change the numbers."
    )
    data_prompt = (
        f"Pick the closest template and output complete executable Python code.\n\n"
        f"SPOCK'S STRATEGY:\n{spock_output}\n\n"
        f"Symbol: {symbol}, Days: {days}, Cash: $7,000\n\n"
        f"Output ONLY the Python code. No markdown. No explanation."
    )
    data_output = _ollama(data_prompt, data_system, model=CODE_MODEL)
    discussion.append(f"\n🤖 DATA (Strategy Coder — {CODE_MODEL}):\n{data_output}")

    # Clean code
    code = re.sub(r'```python\s*', '', data_output)
    code = re.sub(r'```\s*', '', code)
    code = code.strip()

    # === AGENT 3: RIKER (Risk Officer) — practical, not academic ===
    console.log("[cyan]  🫡 Riker reviewing risk...")
    riker_system = (
        "You are Commander Riker, First Officer and Risk Manager. You enforce practical risk standards "
        "for a $7,000 paper trading account:\n"
        "- Maximum drawdown limit: no strategy should exceed -15% drawdown\n"
        "- Position sizing: never risk more than 2% per trade\n"
        "- Every entry must have a defined exit (stop-loss or signal-based)\n\n"
        "CRITICAL CONSTRAINTS — work within these, do NOT demand anything outside them:\n"
        "- We use only yfinance via VectorBT. NEVER mention Bloomberg, Refinitiv, or paid data.\n"
        "- We run on a Mac Mini with 16GB RAM. NEVER demand GARCH, genetic algorithms, or heavy models.\n"
        "- This is research/paper trading. Keep suggestions realistic and encouraging.\n\n"
        "If the code runs and has basic risk controls, mark it RISK APPROVED with 1-2 short improvement notes.\n"
        "If it has critical bugs, mark it NEEDS FIXES and give exact code corrections.\n"
        "Never reject for 'being too simple.' Simple strategies that work beat complex ones that don't.\n"
        "Keep your review under 250 words. Be direct, professional, and encouraging."
    )
    riker_prompt = (
        f"Review this strategy code for a $7,000 paper trading account:\n\n"
        f"CODE:\n{code}\n\n"
        f"Current regime: {regime_ctx}\n\n"
        f"Check: stop-losses? position sizing? code bugs? max drawdown bounded?\n"
        f"Respond with RISK APPROVED or NEEDS FIXES. Keep it under 250 words."
    )
    riker_output = _ollama(riker_prompt, riker_system)
    discussion.append(f"\n🫡 RIKER (Risk Review):\n{riker_output}")

    # === AGENT 4: TESTER (Holodeck Engineer) — practical evaluation ===
    console.log("[cyan]  🔮 Tester evaluating...")
    tester_system = (
        "You are the Holodeck Testing Officer. You evaluate strategy results practically for paper trading.\n\n"
        "Your job:\n"
        "1. Look at the key metrics: total return, win rate, Sharpe ratio, max drawdown, num trades\n"
        "2. Compare to simple buy-and-hold SPY benchmark\n"
        "3. Flag red flags: negative Sharpe, win rate <40%, fewer than 5 trades, drawdown >15%\n"
        "4. Suggest 2-3 SPECIFIC, easy parameter tweaks (e.g., 'try RSI window=10 instead of 14')\n"
        "5. Give a final verdict: GO or NEEDS FIXES\n\n"
        "Be honest but encouraging. Never demand complex models or external data. "
        "This is research on a Mac Mini with $7,000 paper capital.\n"
        "Keep your review under 200 words."
    )
    tester_prompt = (
        f"Evaluate this strategy for paper trading:\n\n"
        f"STRATEGY SUMMARY:\n{spock_output[:500]}\n\n"
        f"RISK REVIEW:\n{riker_output[:500]}\n\n"
        f"Give: expected metrics, comparison to buy-and-hold, 2-3 specific tweaks, GO or NEEDS FIXES.\n"
        f"Keep it under 200 words."
    )
    tester_output = _ollama(tester_prompt, tester_system)
    discussion.append(f"\n🔮 TESTER (Holodeck Evaluation):\n{tester_output}")

    full_discussion = "\n".join(discussion)
    recommendation = "GO" if "GO" in tester_output.upper() and "NO-GO" not in tester_output.upper() else "NO-GO"

    console.log(f"[green]Crew Strategy Lab: complete — {recommendation} — code {len(code)} chars")

    return {
        "success": True,
        "prompt": user_prompt,
        "symbol": symbol,
        "days": days,
        "result": full_discussion,
        "code": code,
        "recommendation": recommendation,
        "crew_size": 4,
        "agents": ["Spock (Analyst)", "Data (Coder)", "Riker (Risk)", "Tester (Holodeck)"],
        "model": MODEL,
        "timestamp": datetime.now().isoformat(),
    }


def run_strategy_code(code: str) -> dict:
    """Execute strategy code safely in a subprocess."""
    if not code or len(code.strip()) < 20:
        return {"success": False, "error": "No code provided", "stdout": "", "stderr": ""}

    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, dir='/tmp') as f:
            f.write(code)
            f.flush()
            temp_path = f.name

        result = subprocess.run(
            [sys.executable, temp_path],
            capture_output=True, text=True, timeout=60,
            cwd=os.path.expanduser('~/autonomous-trader'),
            env={**os.environ, 'PYTHONPATH': os.path.expanduser('~/autonomous-trader')},
        )

        try:
            os.unlink(temp_path)
        except Exception:
            pass

        return {
            "success": result.returncode == 0,
            "stdout": result.stdout[-2000:] if result.stdout else "",
            "stderr": result.stderr[-1000:] if result.stderr else "",
            "returncode": result.returncode,
        }
    except subprocess.TimeoutExpired:
        return {"success": False, "error": "Code execution timed out (60s limit)", "stdout": "", "stderr": ""}
    except Exception as e:
        return {"success": False, "error": str(e), "stdout": "", "stderr": ""}
