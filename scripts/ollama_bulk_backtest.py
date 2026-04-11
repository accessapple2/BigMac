#!/usr/bin/env python3
"""
Holodeck Bulk Backtest — Ollama Model Comparison
-------------------------------------------------
Tests 4 local Ollama models + Debate Pipeline against 12 months of price data
on Steve's core watchlist: SPY, NVDA, META, TSLA, AMD, QQQ.

Each model generates monthly BUY/SELL/HOLD signals from technical indicators.
VectorBT simulates the resulting trades. All results saved to trader.db.
"""

import os
import sys
import json
import sqlite3
import re
import warnings
from datetime import datetime, timedelta
from pathlib import Path

import time
import numpy as np
import pandas as pd
import vectorbt as vbt
import yfinance as yf
import requests

warnings.filterwarnings("ignore")

# ── Config ────────────────────────────────────────────────────────────────────

TICKERS    = ["SPY", "NVDA", "META", "TSLA", "AMD", "QQQ"]
MODELS     = [
    # Fast models first — get early results; slow think-models last
    {"id": "qwen3:14b",          "name": "Ensign Gemma (Baseline)"},
    {"id": "qwen3-coder:7b",   "name": "Lt. Cmdr. Data (Coder)"},
    {"id": "deepseek-r1:14b",     "name": "Spock (DeepSeek R1)"},
    {"id": "qwen3:8b",           "name": "Lt. Cmdr. Data (Qwen3)"},
]
DEBATE_ID  = "debate-pipeline"
DEBATE_NAME = "Riker+Worf+Picard (Debate Pipeline)"

INIT_CASH  = 10_000.0
FEES       = 0.001     # 0.1% per trade
DB_PATH    = Path(__file__).parent.parent / "data" / "trader.db"
RUN_DATE   = datetime.now().strftime("%Y-%m-%d")

END_DATE   = datetime.today()
START_DATE = END_DATE - timedelta(days=365)


# ── Database ──────────────────────────────────────────────────────────────────

def conn():
    c = sqlite3.connect(str(DB_PATH), timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def log(msg, color=""):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


# ── Data Download ─────────────────────────────────────────────────────────────

def download_all_prices():
    """Download 12 months OHLCV for all tickers. Returns dict of DataFrames."""
    log("📡 Downloading 12-month price data for all tickers...")
    prices = {}
    for ticker in TICKERS:
        try:
            df = yf.download(
                ticker,
                start=START_DATE.strftime("%Y-%m-%d"),
                end=END_DATE.strftime("%Y-%m-%d"),
                progress=False,
                auto_adjust=True,
            )
            # Flatten MultiIndex columns if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [col[0] for col in df.columns]
            df.index = pd.to_datetime(df.index)
            df = df.sort_index()
            prices[ticker] = df
            log(f"  ✅ {ticker}: {len(df)} trading days ({df.index[0].date()} → {df.index[-1].date()})")
        except Exception as e:
            log(f"  ❌ {ticker}: {e}")
    return prices


# ── Technical Indicators ──────────────────────────────────────────────────────

def compute_rsi(close, period=14):
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return (100 - (100 / (1 + rs))).fillna(50)


def build_snapshot(df, as_of_idx):
    """Build indicator snapshot at a given index position (no lookahead)."""
    sub = df.iloc[: as_of_idx + 1]
    close = sub["Close"]

    current  = float(close.iloc[-1])
    sma50    = float(close.rolling(50).mean().iloc[-1]) if len(close) >= 50 else current
    sma200   = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else current
    rsi      = float(compute_rsi(close).iloc[-1])
    vol_avg  = float(sub["Volume"].rolling(20).mean().iloc[-1]) if "Volume" in sub.columns else 0
    vol_now  = float(sub["Volume"].iloc[-1]) if "Volume" in sub.columns else 0

    ret_1m  = float(close.pct_change(20).iloc[-1] * 100) if len(close) >= 21 else 0.0
    ret_3m  = float(close.pct_change(60).iloc[-1] * 100) if len(close) >= 61 else 0.0
    ret_ytd = float((close.iloc[-1] / close.iloc[0] - 1) * 100) if len(close) > 1 else 0.0

    vs50  = (current - sma50)  / sma50  * 100 if sma50  else 0
    vs200 = (current - sma200) / sma200 * 100 if sma200 else 0
    vol_ratio = vol_now / vol_avg if vol_avg else 1.0

    trend = "BULLISH" if current > sma200 else "BEARISH"

    return {
        "date": str(df.index[as_of_idx].date()),
        "price": round(current, 2),
        "sma50": round(sma50, 2),
        "sma200": round(sma200, 2),
        "rsi": round(rsi, 1),
        "vs50_pct": round(vs50, 2),
        "vs200_pct": round(vs200, 2),
        "ret_1m_pct": round(ret_1m, 2),
        "ret_3m_pct": round(ret_3m, 2),
        "ret_ytd_pct": round(ret_ytd, 2),
        "vol_ratio": round(vol_ratio, 2),
        "trend": trend,
    }


def get_monthly_decision_points(df):
    """Return index positions of bi-monthly (every 2 months) trading days.
    Gives 6 decision points per year — faster run, still captures regime changes.
    """
    monthly = df.resample("ME").last()
    # Take every other month-end (bi-monthly = 6 points per 12 months)
    bi_monthly = monthly.iloc[::2]
    idxs = []
    for dt in bi_monthly.index:
        pos = df.index.searchsorted(dt, side="right") - 1
        if 0 <= pos < len(df):
            idxs.append(pos)
    return idxs


# ── LLM Signal Generation ─────────────────────────────────────────────────────

SIGNAL_PROMPT = """\
/no_think
You are a quantitative trader. Output ONE line only, no markdown, no explanation.

Ticker: {ticker} | Date: {date} | Price: ${price}
RSI: {rsi} | vs 50d MA: {vs50_pct:+.1f}% | vs 200d MA: {vs200_pct:+.1f}%
1m return: {ret_1m_pct:+.1f}% | 3m return: {ret_3m_pct:+.1f}% | Vol: {vol_ratio:.1f}x | Trend: {trend}

Output format (copy exactly, replace values):
SIGNAL: BUY | CONFIDENCE: 7 | REASON: price above both MAs RSI neutral

SIGNAL must be BUY, SELL, or HOLD. Output that one line only."""


RIKER_PROMPT = """\
/no_think
You are the BULL analyst. Output ONE line only, no markdown.

Ticker: {ticker} | Date: {date} | Price: ${price}
RSI: {rsi} | vs 50d MA: {vs50_pct:+.1f}% | vs 200d MA: {vs200_pct:+.1f}%
1m return: {ret_1m_pct:+.1f}% | 3m return: {ret_3m_pct:+.1f}%

Output (copy format, replace values):
BULL_CASE: price momentum supports upside | CONFIDENCE: 7"""


WORF_PROMPT = """\
/no_think
You are the BEAR analyst. Output ONE line only, no markdown.

Ticker: {ticker} | Date: {date} | Price: ${price}
RSI: {rsi} | vs 50d MA: {vs50_pct:+.1f}% | vs 200d MA: {vs200_pct:+.1f}%
1m return: {ret_1m_pct:+.1f}% | 3m return: {ret_3m_pct:+.1f}%

Output (copy format, replace values):
BEAR_CASE: overbought with resistance overhead | CONFIDENCE: 6"""


PICARD_PROMPT = """\
/no_think
You are the judge. Weigh the bull and bear cases. Output ONE line only, no markdown.

Ticker: {ticker} | Date: {date}
BULL: {bull_case} [conf {bull_conf}/10]
BEAR: {bear_case} [conf {bear_conf}/10]
Price vs 200d MA: {vs200_pct:+.1f}% | RSI: {rsi}

Output (copy format, replace values):
VERDICT: BUY | CONFIDENCE: 7 | WINNER: Riker | REASON: bull case stronger

VERDICT must be BUY, SELL, or HOLD."""


OLLAMA_GENERATE = "http://localhost:11434/api/generate"
OLLAMA_CHAT     = "http://localhost:11434/api/chat"

# Models that use chain-of-thought — need think suppressed or large token budget
THINK_MODELS = {"qwen3:8b", "deepseek-r1:14b", "deepseek-r1:14b", "qwen3:4b", "qwen3:30b"}


def _strip_think(text):
    """Remove <think>...</think> blocks; if block is unclosed, strip everything after <think>."""
    # Complete think blocks
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL | re.IGNORECASE)
    # Unclosed think block (model ran out of tokens mid-think)
    text = re.sub(r"<think>.*", "", text, flags=re.DOTALL | re.IGNORECASE)
    return text.strip()


def call_model(model_id, prompt, max_retries=3, per_call_timeout=300):
    """
    Call Ollama with the right strategy per model type:
    - Think models (qwen3, deepseek-r1): use /api/chat with think:false to suppress CoT
    - Other models: use /api/generate with small token budget
    """
    is_think_model = model_id in THINK_MODELS

    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                wait = 30 * attempt
                log(f"    ⏳ Retry {attempt}/{max_retries} after {wait}s (Ollama busy — arena contention)...")
                time.sleep(wait)

            if is_think_model:
                # Use chat API with think:false — suppresses chain-of-thought entirely
                resp = requests.post(
                    OLLAMA_CHAT,
                    json={
                        "model": model_id,
                        "messages": [{"role": "user", "content": prompt}],
                        "stream": False,
                        "think": False,
                        "options": {
                            "temperature": 0.1,
                            "num_predict": 150,
                            "num_ctx": 1024,
                        },
                    },
                    timeout=per_call_timeout,
                )
                resp.raise_for_status()
                data = resp.json()
                raw = data.get("message", {}).get("content", "") or ""
            else:
                resp = requests.post(
                    OLLAMA_GENERATE,
                    json={
                        "model": model_id,
                        "prompt": prompt,
                        "stream": False,
                        "options": {
                            "temperature": 0.1,
                            "num_predict": 150,
                            "num_ctx": 1024,
                        },
                    },
                    timeout=per_call_timeout,
                )
                resp.raise_for_status()
                raw = resp.json().get("response", "") or ""

            cleaned = _strip_think(raw).strip()
            return cleaned

        except requests.exceptions.Timeout:
            log(f"    ⏱  {model_id} timed out after {per_call_timeout}s (attempt {attempt+1})")
        except Exception as e:
            log(f"    ⚠️  {model_id} error: {type(e).__name__}: {str(e)[:80]}")

    log(f"    ❌ {model_id} failed after {max_retries+1} attempts — defaulting HOLD")
    return ""


def _extract(pattern, text, default=None):
    """Case-insensitive search, strip markdown bold markers."""
    clean = re.sub(r"\*+", "", text)  # strip ** markdown
    m = re.search(pattern, clean, re.IGNORECASE)
    return m if m else None


def parse_signal(text):
    """Parse SIGNAL: BUY/SELL/HOLD — handles full format AND bare-word responses."""
    # Try full format: SIGNAL: BUY
    m = _extract(r"SIGNAL:\s*(BUY|SELL|HOLD)", text)
    if m:
        signal = m.group(1).upper()
    else:
        # Fallback: model returned bare BUY/SELL/HOLD
        for word in re.sub(r"[^\w\s]", " ", text.upper()).split()[:8]:
            if word in ("BUY", "SELL", "HOLD"):
                signal = word
                break
        else:
            signal = "HOLD"

    m_conf = _extract(r"CONFIDENCE:\s*(\d+)", text)
    conf = int(m_conf.group(1)) if m_conf else 5

    m_reason = _extract(r"REASON:\s*(.+?)(?:\||[\r\n]|$)", text)
    reason = m_reason.group(1).strip()[:100] if m_reason else text[:60].strip()

    return signal, min(max(conf, 1), 10), reason


def parse_debate_verdict(picard_text):
    """Parse VERDICT: BUY/SELL/HOLD — handles full format AND bare-word responses."""
    m = _extract(r"VERDICT:\s*(BUY|SELL|HOLD)", picard_text)
    if m:
        signal = m.group(1).upper()
    else:
        # Fallback: bare word
        for word in re.sub(r"[^\w\s]", " ", picard_text.upper()).split()[:8]:
            if word in ("BUY", "SELL", "HOLD"):
                signal = word
                break
        else:
            signal = "HOLD"

    m_conf = _extract(r"CONFIDENCE:\s*(\d+)", picard_text)
    conf = int(m_conf.group(1)) if m_conf else 5

    m_reason = _extract(r"REASON:\s*(.+?)(?:\||[\r\n]|$)", picard_text)
    reason = m_reason.group(1).strip()[:100] if m_reason else picard_text[:60].strip()

    return signal, min(max(conf, 1), 10), reason


def get_solo_signals(model_id, ticker, snapshots):
    """Get bi-monthly signals from a single model for one ticker."""
    results = []
    for snap in snapshots:
        prompt = SIGNAL_PROMPT.format(ticker=ticker, **snap)
        raw = call_model(model_id, prompt)
        signal, conf, reason = parse_signal(raw)
        results.append({
            "date": snap["date"],
            "signal": signal,
            "confidence": conf,
            "reason": reason,
            "raw": raw[:200],
        })
        log(f"    {snap['date']}: {signal} ({conf}/10) — {reason[:50]}")
        time.sleep(2)   # brief pause — don't hammer Ollama
    return results


def get_debate_signals(ticker, snapshots):
    """Run Riker→Worf→Picard debate for each monthly snapshot."""
    results = []
    for snap in snapshots:
        # Step 1: Riker bull case
        riker_raw = call_model("qwen3:8b", RIKER_PROMPT.format(ticker=ticker, **snap))
        bull_m = re.search(r"BULL_CASE:\s*(.+?)\s*\|\s*CONFIDENCE:\s*(\d+)", riker_raw, re.IGNORECASE)
        bull_case = bull_m.group(1).strip() if bull_m else "bullish momentum"
        bull_conf = int(bull_m.group(2)) if bull_m else 5

        # Step 2: Worf bear case
        worf_raw = call_model("qwen3-coder:7b", WORF_PROMPT.format(ticker=ticker, **snap))
        bear_m = re.search(r"BEAR_CASE:\s*(.+?)\s*\|\s*CONFIDENCE:\s*(\d+)", worf_raw, re.IGNORECASE)
        bear_case = bear_m.group(1).strip() if bear_m else "bearish headwinds"
        bear_conf = int(bear_m.group(2)) if bear_m else 5

        # Step 3: Picard verdict (uses deepseek for reasoning)
        picard_raw = call_model(
            "deepseek-r1:14b",
            PICARD_PROMPT.format(
                ticker=ticker,
                bull_case=bull_case,
                bull_conf=bull_conf,
                bear_case=bear_case,
                bear_conf=bear_conf,
                **snap,
            ),
        )
        signal, conf, reason = parse_debate_verdict(picard_raw)

        results.append({
            "date": snap["date"],
            "signal": signal,
            "confidence": conf,
            "reason": f"Riker({bull_conf})vs Worf({bear_conf}): {reason[:60]}",
            "raw": f"R:{riker_raw[:80]} | W:{worf_raw[:80]} | P:{picard_raw[:80]}",
        })
        log(f"    {snap['date']}: {signal} ({conf}/10) Riker={bull_conf} Worf={bear_conf} — {reason[:40]}")
        time.sleep(3)   # brief pause between debate rounds
    return results


# ── VectorBT Simulation ───────────────────────────────────────────────────────

def _safe(v):
    if v is None:
        return 0.0
    f = float(v)
    return 0.0 if (np.isnan(f) or np.isinf(f)) else f


def run_vectorbt(ticker, price_df, signal_list, init_cash=INIT_CASH):
    """
    Convert monthly signal list into daily entry/exit arrays.
    Run VectorBT backtest. Returns metrics dict.
    """
    close = price_df["Close"].astype(float)
    dates = close.index

    # Build signal series: carry forward each monthly signal
    sig_series = pd.Series("HOLD", index=dates)
    for sig_info in signal_list:
        dt = pd.Timestamp(sig_info["date"])
        # Apply signal starting from next trading day after decision
        future = dates[dates > dt]
        if len(future):
            apply_from = future[0]
            sig_series.loc[apply_from:] = sig_info["signal"]

    entries = (sig_series == "BUY").values
    exits   = (sig_series == "SELL").values

    # Ensure we don't enter/exit on same bar
    # VectorBT handles this automatically

    try:
        pf = vbt.Portfolio.from_signals(
            close,
            entries,
            exits,
            freq="1D",
            fees=FEES,
            init_cash=init_cash,
            sl_stop=None,
        )
        stats = pf.stats()

        total_return  = _safe(stats.get("Total Return [%]", 0))
        sharpe        = _safe(stats.get("Sharpe Ratio", 0))
        max_dd        = _safe(stats.get("Max Drawdown [%]", 0))
        win_rate      = _safe(stats.get("Win Rate [%]", 0))
        num_trades    = int(_safe(stats.get("Total Trades", 0)))
        final_value   = _safe(pf.final_value())
        best_trade    = _safe(stats.get("Best Trade [%]", 0))
        worst_trade   = _safe(stats.get("Worst Trade [%]", 0))

        # Equity curve (sampled weekly to keep JSON small)
        equity = pf.value()
        equity_weekly = equity.resample("W").last()
        equity_json = json.dumps({
            str(d.date()): round(float(v), 2)
            for d, v in equity_weekly.items()
            if not np.isnan(v)
        })

        # Trade log
        try:
            trades_df = pf.trades.records_readable
            trades_json = trades_df[["Entry Index", "Exit Index", "PnL", "Return [%]"]].head(50).to_json(orient="records")
        except Exception:
            trades_json = "[]"

        return {
            "final_value": round(final_value, 2),
            "total_return": round(total_return, 2),
            "sharpe": round(sharpe, 3),
            "max_drawdown": round(max_dd, 2),
            "win_rate": round(win_rate, 2),
            "num_trades": num_trades,
            "best_trade_pct": round(best_trade, 2),
            "worst_trade_pct": round(worst_trade, 2),
            "equity_json": equity_json,
            "trades_json": trades_json,
        }
    except Exception as e:
        log(f"    ❌ VectorBT error for {ticker}: {e}")
        return {
            "final_value": init_cash,
            "total_return": 0.0,
            "sharpe": 0.0,
            "max_drawdown": 0.0,
            "win_rate": 0.0,
            "num_trades": 0,
            "best_trade_pct": 0.0,
            "worst_trade_pct": 0.0,
            "equity_json": "{}",
            "trades_json": "[]",
        }


# ── SPY Benchmark ─────────────────────────────────────────────────────────────

def get_spy_return(prices):
    """Compute SPY buy-and-hold return over the period."""
    spy_df = prices.get("SPY")
    if spy_df is None or len(spy_df) < 2:
        return 0.0
    ret = (float(spy_df["Close"].iloc[-1]) / float(spy_df["Close"].iloc[0]) - 1) * 100
    return round(ret, 2)


# ── Database Logging ──────────────────────────────────────────────────────────

def save_run(c, model_ids_str, start, end):
    c.execute(
        "INSERT INTO backtest_runs (run_type, start_date, end_date, model_ids, status, created_at) "
        "VALUES (?, ?, ?, ?, 'running', datetime('now'))",
        ("ollama_bulk", start, end, model_ids_str),
    )
    c.commit()
    return c.execute("SELECT last_insert_rowid()").fetchone()[0]


def update_run_complete(c, run_id):
    c.execute(
        "UPDATE backtest_runs SET status='completed', completed_at=datetime('now') WHERE id=?",
        (run_id,),
    )
    c.commit()


def save_result(c, run_id, player_id, display_name, metrics, ticker, test_date):
    c.execute(
        """INSERT INTO backtest_results
           (run_id, player_id, display_name, test_date, final_value,
            total_return_pct, win_rate, sharpe_ratio, max_drawdown,
            num_trades, best_trade_pct, worst_trade_pct, trades_json, equity_json)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            run_id, player_id, f"{display_name} [{ticker}]", test_date,
            metrics["final_value"], metrics["total_return"],
            metrics["win_rate"], metrics["sharpe"],
            metrics["max_drawdown"], metrics["num_trades"],
            metrics["best_trade_pct"], metrics["worst_trade_pct"],
            metrics["trades_json"], metrics["equity_json"],
        ),
    )
    c.commit()


def save_history(c, player_id, player_name, spy_return, per_ticker_results):
    """Roll up per-ticker results into one backtest_history row per model."""
    returns     = [r["total_return"] for r in per_ticker_results.values()]
    final_vals  = [r["final_value"]  for r in per_ticker_results.values()]
    win_rates   = [r["win_rate"]     for r in per_ticker_results.values()]
    sharpes     = [r["sharpe"]       for r in per_ticker_results.values()]
    trades      = [r["num_trades"]   for r in per_ticker_results.values()]

    avg_return   = round(float(np.mean(returns)), 2)
    avg_wr       = round(float(np.mean(win_rates)), 2)
    avg_sharpe   = round(float(np.mean(sharpes)), 2)
    total_trades = sum(trades)
    final_port   = round(sum(final_vals), 2)

    best_ticker  = max(per_ticker_results, key=lambda t: per_ticker_results[t]["total_return"])
    worst_ticker = min(per_ticker_results, key=lambda t: per_ticker_results[t]["total_return"])

    notes = json.dumps({
        "per_ticker": {t: {"return": v["total_return"], "sharpe": v["sharpe"]}
                       for t, v in per_ticker_results.items()},
        "spy_benchmark": spy_return,
        "run_date": RUN_DATE,
    })

    c.execute(
        """INSERT INTO backtest_history
           (player_id, player_name, run_date, period_days,
            start_date, end_date, starting_value, final_value,
            return_pct, total_trades, win_rate, spy_return_pct,
            best_trade_symbol, worst_trade_symbol, notes, signals_tested)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            player_id, player_name, RUN_DATE, 365,
            START_DATE.strftime("%Y-%m-%d"), END_DATE.strftime("%Y-%m-%d"),
            INIT_CASH * len(TICKERS), final_port,
            avg_return, total_trades, avg_wr, spy_return,
            best_ticker, worst_ticker, notes, total_trades,
        ),
    )
    c.commit()
    return avg_return, avg_wr, avg_sharpe, best_ticker, worst_ticker


# ── Leaderboard ───────────────────────────────────────────────────────────────

def print_leaderboard(results_table, spy_return):
    """Print formatted comparison table."""
    # Sort by avg cumulative return
    sorted_rows = sorted(results_table, key=lambda r: r["avg_return"], reverse=True)

    header = f"\n{'═'*90}"
    print(header)
    print("  HOLODECK BULK BACKTEST — OLLAMA MODEL LEADERBOARD")
    print(f"  Period: {START_DATE.strftime('%Y-%m-%d')} → {END_DATE.strftime('%Y-%m-%d')}  |  Tickers: {', '.join(TICKERS)}")
    print(f"  SPY Buy-and-Hold Benchmark: {spy_return:+.1f}%")
    print(f"{'═'*90}")
    print(f"  {'Model':<32} {'Cum Ret':>8} {'Sharpe':>7} {'Max DD':>8} {'Win Rate':>9} {'Best':>8} {'Worst':>8}")
    print(f"  {'─'*32} {'─'*8} {'─'*7} {'─'*8} {'─'*9} {'─'*8} {'─'*8}")

    for r in sorted_rows:
        flag = " ⭐" if r["rank"] == 1 else ""
        print(
            f"  {r['model']:<32} {r['avg_return']:>+7.1f}% {r['avg_sharpe']:>7.2f} "
            f"{r['avg_maxdd']:>7.1f}% {r['avg_winrate']:>8.1f}% "
            f"{r['best_ticker']:>8} {r['worst_ticker']:>8}{flag}"
        )

    print(f"{'═'*90}")
    print("\nPer-Ticker Breakdown:")
    print(f"  {'Model':<32} " + "  ".join(f"{t:>8}" for t in TICKERS))
    print(f"  {'─'*32} " + "  ".join("─"*8 for _ in TICKERS))
    for r in sorted_rows:
        row = f"  {r['model']:<32} "
        for t in TICKERS:
            ret = r["per_ticker"].get(t, {}).get("return", 0)
            row += f"{ret:>+8.1f}%"[:-1] + "  "
        print(row)
    print(f"{'═'*90}\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log("🚀 USS TradeMinds — Holodeck Bulk Backtest Starting")
    log(f"   Period : {START_DATE.strftime('%Y-%m-%d')} → {END_DATE.strftime('%Y-%m-%d')}")
    log(f"   Tickers: {', '.join(TICKERS)}")
    log(f"   Models : {len(MODELS)} solo + 1 debate pipeline")

    # Download price data once
    prices = download_all_prices()

    spy_return = get_spy_return(prices)
    log(f"\n📊 SPY Buy-and-Hold Benchmark: {spy_return:+.1f}%\n")

    # Precompute monthly decision points per ticker
    decision_points = {}
    snapshots = {}
    for ticker, df in prices.items():
        pts = get_monthly_decision_points(df)
        decision_points[ticker] = pts
        snapshots[ticker] = [build_snapshot(df, idx) for idx in pts]
        log(f"  {ticker}: {len(pts)} monthly decision points")

    # Open DB, create run record
    c = conn()
    all_model_ids = [m["id"] for m in MODELS] + [DEBATE_ID]
    run_id = save_run(
        c,
        ",".join(all_model_ids),
        START_DATE.strftime("%Y-%m-%d"),
        END_DATE.strftime("%Y-%m-%d"),
    )
    log(f"\n📋 Backtest run #{run_id} created in DB")

    # ── Run each solo model ───────────────────────────────────────────────────
    results_table = []

    all_to_test = MODELS + [{"id": DEBATE_ID, "name": DEBATE_NAME}]

    for model_info in all_to_test:
        model_id   = model_info["id"]
        model_name = model_info["name"]
        is_debate  = (model_id == DEBATE_ID)

        log(f"\n{'━'*60}")
        log(f"  MODEL: {model_name}")
        log(f"{'━'*60}")

        per_ticker = {}

        for ticker in TICKERS:
            if ticker not in prices:
                log(f"  ⏭  {ticker}: no price data, skipping")
                continue

            log(f"\n  📈 {ticker}:")
            snaps = snapshots[ticker]

            # Generate signals
            if is_debate:
                sigs = get_debate_signals(ticker, snaps)
            else:
                sigs = get_solo_signals(model_id, ticker, snaps)

            # VectorBT simulation
            metrics = run_vectorbt(ticker, prices[ticker], sigs)
            per_ticker[ticker] = metrics

            # Save per-ticker result immediately (checkpoint — sacred data)
            save_result(c, run_id, model_id, model_name, metrics, ticker, RUN_DATE)

            log(
                f"    → Return: {metrics['total_return']:+.1f}% | "
                f"Sharpe: {metrics['sharpe']:.2f} | "
                f"MaxDD: {metrics['max_drawdown']:.1f}% | "
                f"WinRate: {metrics['win_rate']:.0f}% | "
                f"Trades: {metrics['num_trades']}"
            )
            sys.stdout.flush()

        # Roll up to backtest_history
        if per_ticker:
            avg_ret, avg_wr, avg_sharpe, best_t, worst_t = save_history(
                c, model_id, model_name, spy_return, per_ticker
            )
            results_table.append({
                "model":        model_name[:32],
                "avg_return":   avg_ret,
                "avg_sharpe":   avg_sharpe,
                "avg_maxdd":    round(float(np.mean([v["max_drawdown"] for v in per_ticker.values()])), 2),
                "avg_winrate":  avg_wr,
                "best_ticker":  best_t,
                "worst_ticker": worst_t,
                "per_ticker":   {t: {"return": v["total_return"]} for t, v in per_ticker.items()},
                "rank":         0,
            })

    # Assign ranks
    results_table.sort(key=lambda r: r["avg_return"], reverse=True)
    for i, r in enumerate(results_table):
        r["rank"] = i + 1

    # Mark run complete
    update_run_complete(c, run_id)
    c.close()

    # Print leaderboard
    print_leaderboard(results_table, spy_return)

    # Auto-resume arena (was paused to give Ollama exclusive access)
    try:
        db = conn()
        db.execute("UPDATE settings SET value='0' WHERE key='pause_all'")
        db.commit()
        db.close()
        log("▶️  Arena auto-resumed (pause_all → 0)")
    except Exception as e:
        log(f"⚠️  Could not auto-resume arena: {e} — run: UPDATE settings SET value='0' WHERE key='pause_all'")

    log(f"✅ All results saved to backtest_history and backtest_results (run_id #{run_id})")
    log(f"   SPY benchmark: {spy_return:+.1f}%")
    log(f"   Winner: {results_table[0]['model']} ({results_table[0]['avg_return']:+.1f}% avg return)\n")

    return results_table


if __name__ == "__main__":
    os.chdir(Path(__file__).parent.parent)  # ensure relative DB path works
    main()
