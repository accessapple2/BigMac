#!/usr/bin/env python3
"""
USS TradeMinds — Model Quality Sweep 2026-04-20
================================================
Runs the same 180-day backtest (2024-10-01 → 2025-04-01) with 3 model
variants per agent to find the optimal LLM for each war_room role.

All inference routed to Ollie GPU (192.168.1.166:11434).
Results saved to trader.db: backtest_runs, backtest_results, backtest_history.
Progress logged to /tmp/model_sweep.log with timestamps.

Usage:
    cd ~/autonomous-trader
    venv/bin/python3 scripts/model_sweep_2026_04_20.py

Safety:
    touch KILL_SWITCH  →  sweep pauses each run boundary, retries every 60s
    Any run >30 min     →  sweep halts with reason logged
    Ollie unreachable   →  sweep halts
    Disk <10% free      →  sweep halts

DO NOT EDIT config.py or main.py. DO NOT restart the trader.
This script is read-only on the live DB except for backtest_* tables.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sqlite3
import sys
import time
import warnings
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import vectorbt as vbt
import yfinance as yf

warnings.filterwarnings("ignore")

# ── Paths ─────────────────────────────────────────────────────────────────────

REPO_ROOT  = Path(__file__).parent.parent
DB_PATH    = REPO_ROOT / "data" / "trader.db"
KILL_FILE  = REPO_ROOT / "KILL_SWITCH"
LOG_FILE   = Path("/tmp/model_sweep.log")

# ── Inference endpoint (Ollie GPU only) ───────────────────────────────────────

OLLIE_BASE      = "http://192.168.1.166:11434"
OLLIE_GENERATE  = OLLIE_BASE + "/api/generate"
OLLIE_CHAT      = OLLIE_BASE + "/api/chat"
OLLIE_TAGS      = OLLIE_BASE + "/api/tags"

# ── Backtest window ───────────────────────────────────────────────────────────

START_DATE = date(2024, 10, 1)
END_DATE   = date(2025, 4, 1)
PERIOD_DAYS = (END_DATE - START_DATE).days   # 182

TICKERS    = ["SPY", "NVDA", "META", "TSLA", "AMD", "QQQ"]
INIT_CASH  = 10_000.0
FEES       = 0.001

# ── Sweep matrix ──────────────────────────────────────────────────────────────
# Format: (agent_id, agent_name, variant, model_id, notes)
# chekov and capitol-trades are BASE-only per CLAUDE.md.

SWEEP_MATRIX = [
    # ── ollie-auto (Ollie quality gate) ─────────────────────────────────────
    ("ollie-auto",      "Ollie (Quality Gate)",   "BASE",   "qwen3:8b",           "current"),
    ("ollie-auto",      "Ollie (Quality Gate)",   "LIGHT",  "phi3:mini",          "lighter/faster"),
    ("ollie-auto",      "Ollie (Quality Gate)",   "HEAVY",  "qwen3:14b",          "deeper reasoning"),
    # ── navigator (convergence aggregator) ──────────────────────────────────
    ("navigator",       "Navigator",              "BASE",   "qwen3:8b",           "current Ollie"),
    ("navigator",       "Navigator",              "LIGHT",  "phi3:mini",          "lighter"),
    ("navigator",       "Navigator",              "HEAVY",  "qwen3:14b",          "heavier"),
    # ── chekov (momentum agent, muted) — BASE only ──────────────────────────
    ("chekov",          "Chekov (muted)",         "BASE",   "phi3:mini",          "muted — 1 run only"),
    # ── ollama-llama (Uhura, llama family) ──────────────────────────────────
    ("ollama-llama",    "Uhura (llama3.1)",       "BASE",   "llama3.1:latest",    "current"),
    ("ollama-llama",    "Uhura (llama3.1)",       "LIGHT",  "llama3.2:3b",        "smaller llama"),
    ("ollama-llama",    "Uhura (llama3.1)",       "HEAVY",  "qwen3:14b",          "cross-family heavy"),
    # ── ollama-plutus (McCoy, finance-tuned) ────────────────────────────────
    ("ollama-plutus",   "McCoy (Plutus-3B)",      "BASE",   "0xroyce/plutus:latest", "current, finance-tuned"),
    ("ollama-plutus",   "McCoy (Plutus-3B)",      "LIGHT",  "qwen3:8b",           "general 8b"),
    ("ollama-plutus",   "McCoy (Plutus-3B)",      "HEAVY",  "deepseek-r1:14b",    "deep reasoning"),
    # ── ollama-qwen3 (Dax, CSP low-VIX) ────────────────────────────────────
    ("ollama-qwen3",    "Dax (qwen3:8b)",         "BASE",   "qwen3:8b",           "current"),
    ("ollama-qwen3",    "Dax (qwen3:8b)",         "LIGHT",  "phi3:mini",          "lighter"),
    ("ollama-qwen3",    "Dax (qwen3:8b)",         "HEAVY",  "qwen3:14b",          "heavier"),
    # ── ollama-coder (Data/code agent) ──────────────────────────────────────
    ("ollama-coder",    "Data (qwen2.5-coder)",   "BASE",   "qwen2.5-coder:7b",   "current"),
    ("ollama-coder",    "Data (qwen2.5-coder)",   "LIGHT",  "phi3:mini",          "lighter"),
    ("ollama-coder",    "Data (qwen2.5-coder)",   "HEAVY",  "qwen3-coder:30b",    "30b coder"),
    # ── neo-matrix (Neo, bridge voter) ──────────────────────────────────────
    ("neo-matrix",      "Neo (phi3:mini)",        "BASE",   "phi3:mini",          "current"),
    ("neo-matrix",      "Neo (phi3:mini)",        "HEAVY",  "qwen3:8b",           "upgrade candidate"),
    # ── capitol-trades (data-feed driven) — BASE only ───────────────────────
    ("capitol-trades",  "Capitol (data-feed)",    "BASE",   "phi3:mini",          "data-driven — 1 run only"),
]

TOTAL_RUNS = len(SWEEP_MATRIX)

# ── Logging ───────────────────────────────────────────────────────────────────

def log(msg: str, also_print: bool = True) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} | {msg}"
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")
    if also_print:
        print(line)
    sys.stdout.flush()


# ── Safety checks ─────────────────────────────────────────────────────────────

def check_kill_switch(block: bool = False) -> bool:
    """Return True if KILL_SWITCH is active. If block=True, poll until cleared."""
    if not KILL_FILE.exists():
        return False
    if not block:
        return True
    log("SWEEP PAUSED — kill switch detected — polling every 60s until cleared")
    while KILL_FILE.exists():
        time.sleep(60)
    log("SWEEP RESUMED — kill switch cleared")
    return False


def check_disk() -> None:
    usage = shutil.disk_usage(REPO_ROOT)
    pct_free = usage.free / usage.total * 100
    if pct_free < 10:
        log(f"SWEEP HALTED — disk only {pct_free:.1f}% free ({usage.free // 1073741824}GB)")
        sys.exit(1)


def check_ollie() -> None:
    try:
        r = requests.get(OLLIE_TAGS, timeout=5)
        r.raise_for_status()
    except Exception as e:
        log(f"SWEEP HALTED — Ollie unreachable: {e}")
        sys.exit(1)


def verify_model_on_ollie(model_id: str) -> bool:
    """Return True if model is available on Ollie."""
    try:
        r = requests.get(OLLIE_TAGS, timeout=5)
        models = [m["name"] for m in r.json().get("models", [])]
        return any(model_id in name for name in models)
    except Exception:
        return False


# ── Technical indicators ──────────────────────────────────────────────────────

def compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss.replace(0, np.nan)
    return (100 - (100 / (1 + rs))).fillna(50)


def build_snapshot(df: pd.DataFrame, as_of_idx: int) -> dict:
    sub   = df.iloc[: as_of_idx + 1]
    close = sub["Close"]
    current  = float(close.iloc[-1])
    sma50    = float(close.rolling(50).mean().iloc[-1]) if len(close) >= 50 else current
    sma200   = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else current
    rsi      = float(compute_rsi(close).iloc[-1])
    vol_avg  = float(sub["Volume"].rolling(20).mean().iloc[-1]) if "Volume" in sub.columns else 0
    vol_now  = float(sub["Volume"].iloc[-1]) if "Volume" in sub.columns else 0
    ret_1m   = float(close.pct_change(20).iloc[-1] * 100) if len(close) >= 21 else 0.0
    ret_3m   = float(close.pct_change(60).iloc[-1] * 100) if len(close) >= 61 else 0.0
    ret_ytd  = float((close.iloc[-1] / close.iloc[0] - 1) * 100) if len(close) > 1 else 0.0
    vs50     = (current - sma50)  / sma50  * 100 if sma50  else 0
    vs200    = (current - sma200) / sma200 * 100 if sma200 else 0
    vol_ratio = vol_now / vol_avg if vol_avg else 1.0
    trend    = "BULLISH" if current > sma200 else "BEARISH"
    return {
        "date":        str(df.index[as_of_idx].date()),
        "price":       round(current, 2),
        "sma50":       round(sma50, 2),
        "sma200":      round(sma200, 2),
        "rsi":         round(rsi, 1),
        "vs50_pct":    round(vs50, 2),
        "vs200_pct":   round(vs200, 2),
        "ret_1m_pct":  round(ret_1m, 2),
        "ret_3m_pct":  round(ret_3m, 2),
        "ret_ytd_pct": round(ret_ytd, 2),
        "vol_ratio":   round(vol_ratio, 2),
        "trend":       trend,
    }


def get_decision_points(df: pd.DataFrame) -> list[int]:
    """Bi-monthly decision points — ~3 for 180-day window."""
    monthly = df.resample("ME").last()
    bi_monthly = monthly.iloc[::2]
    idxs = []
    for dt in bi_monthly.index:
        pos = df.index.searchsorted(dt, side="right") - 1
        if 0 <= pos < len(df):
            idxs.append(pos)
    return idxs


# ── LLM signal generation ─────────────────────────────────────────────────────

SIGNAL_PROMPT = """\
/no_think
You are a quantitative trader. Output ONE line only, no markdown, no explanation.

Ticker: {ticker} | Date: {date} | Price: ${price}
RSI: {rsi} | vs 50d MA: {vs50_pct:+.1f}% | vs 200d MA: {vs200_pct:+.1f}%
1m return: {ret_1m_pct:+.1f}% | 3m return: {ret_3m_pct:+.1f}% | Vol: {vol_ratio:.1f}x | Trend: {trend}

Output format (copy exactly, replace values):
SIGNAL: BUY | CONFIDENCE: 7 | REASON: price above both MAs RSI neutral

SIGNAL must be BUY, SELL, or HOLD. Output that one line only."""

THINK_MODELS = {"qwen3:8b", "qwen3:14b", "deepseek-r1:14b", "deepseek-r1:7b", "qwen3-coder:30b"}

_consecutive_errors = 0


def _strip_think(text: str) -> str:
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<think>.*", "", text, flags=re.DOTALL | re.IGNORECASE)
    return text.strip()


def call_model(model_id: str, prompt: str, max_retries: int = 3,
               timeout: int = 120, run_start: float = 0.0) -> str:
    """Call Ollie GPU with retry logic and safety guards."""
    global _consecutive_errors

    # 30-minute per-run hard stop
    if run_start and (time.time() - run_start) > 1800:
        log(f"SWEEP HALTED — run exceeded 30 min ceiling (model {model_id})")
        sys.exit(1)

    is_think = model_id in THINK_MODELS

    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                wait = 30 * attempt
                log(f"  ⏳ retry {attempt}/{max_retries} in {wait}s")
                time.sleep(wait)

            if is_think:
                resp = requests.post(
                    OLLIE_CHAT,
                    json={
                        "model": model_id,
                        "messages": [{"role": "user", "content": prompt}],
                        "stream": False,
                        "think": False,
                        "options": {"temperature": 0.1, "num_predict": 150, "num_ctx": 1024},
                    },
                    timeout=timeout,
                )
                resp.raise_for_status()
                raw = resp.json().get("message", {}).get("content", "") or ""
            else:
                resp = requests.post(
                    OLLIE_GENERATE,
                    json={
                        "model": model_id,
                        "prompt": prompt,
                        "stream": False,
                        "options": {"temperature": 0.1, "num_predict": 150, "num_ctx": 1024},
                    },
                    timeout=timeout,
                )
                resp.raise_for_status()
                raw = resp.json().get("response", "") or ""

            _consecutive_errors = 0
            return _strip_think(raw).strip()

        except requests.HTTPError as e:
            _consecutive_errors += 1
            log(f"  ⚠️  HTTP {e.response.status_code} from Ollie ({model_id})")
            if _consecutive_errors > 3:
                log(f"SWEEP HALTED — >3 consecutive 500s from Ollie")
                sys.exit(1)
        except requests.exceptions.ConnectionError:
            log(f"SWEEP HALTED — Ollie unreachable during run ({model_id})")
            sys.exit(1)
        except Exception as e:
            _consecutive_errors += 1
            log(f"  ⚠️  {model_id}: {type(e).__name__}: {str(e)[:80]}")

    log(f"  ❌ {model_id} failed after {max_retries+1} attempts — defaulting HOLD")
    return ""


def _extract(pattern: str, text: str):
    clean = re.sub(r"\*+", "", text)
    return re.search(pattern, clean, re.IGNORECASE)


def parse_signal(text: str) -> tuple[str, int, str]:
    m = _extract(r"SIGNAL:\s*(BUY|SELL|HOLD)", text)
    if m:
        signal = m.group(1).upper()
    else:
        signal = "HOLD"
        for word in re.sub(r"[^\w\s]", " ", text.upper()).split()[:8]:
            if word in ("BUY", "SELL", "HOLD"):
                signal = word
                break
    m_conf   = _extract(r"CONFIDENCE:\s*(\d+)", text)
    m_reason = _extract(r"REASON:\s*(.+?)(?:\||[\r\n]|$)", text)
    conf   = int(m_conf.group(1)) if m_conf else 5
    reason = m_reason.group(1).strip()[:100] if m_reason else text[:60].strip()
    return signal, min(max(conf, 1), 10), reason


def get_signals(model_id: str, ticker: str, snapshots: list[dict],
                run_start: float = 0.0) -> list[dict]:
    results = []
    t0_call = time.time()
    for snap in snapshots:
        prompt = SIGNAL_PROMPT.format(ticker=ticker, **snap)
        t_call = time.time()
        raw    = call_model(model_id, prompt, run_start=run_start)
        latency_ms = int((time.time() - t_call) * 1000)
        signal, conf, reason = parse_signal(raw)
        results.append({
            "date":       snap["date"],
            "signal":     signal,
            "confidence": conf,
            "reason":     reason,
            "latency_ms": latency_ms,
            "raw":        raw[:200],
        })
        time.sleep(2)
    return results


# ── VectorBT simulation ───────────────────────────────────────────────────────

def _safe(v) -> float:
    if v is None:
        return 0.0
    f = float(v)
    return 0.0 if (np.isnan(f) or np.isinf(f)) else f


def run_vectorbt(ticker: str, price_df: pd.DataFrame, signal_list: list[dict],
                 init_cash: float = INIT_CASH) -> dict:
    close  = price_df["Close"].astype(float)
    dates  = close.index
    sig_series = pd.Series("HOLD", index=dates)
    for sig_info in signal_list:
        dt     = pd.Timestamp(sig_info["date"])
        future = dates[dates > dt]
        if len(future):
            sig_series.loc[future[0]:] = sig_info["signal"]
    entries = (sig_series == "BUY").values
    exits   = (sig_series == "SELL").values
    try:
        pf    = vbt.Portfolio.from_signals(close, entries, exits, freq="1D",
                                           fees=FEES, init_cash=init_cash)
        stats = pf.stats()
        total_return  = _safe(stats.get("Total Return [%]", 0))
        sharpe        = _safe(stats.get("Sharpe Ratio", 0))
        max_dd        = _safe(stats.get("Max Drawdown [%]", 0))
        win_rate      = _safe(stats.get("Win Rate [%]", 0))
        num_trades    = int(_safe(stats.get("Total Trades", 0)))
        final_value   = _safe(pf.final_value())
        best_trade    = _safe(stats.get("Best Trade [%]", 0))
        worst_trade   = _safe(stats.get("Worst Trade [%]", 0))
        equity        = pf.value().resample("W").last()
        equity_json   = json.dumps({str(d.date()): round(float(v), 2)
                                    for d, v in equity.items() if not np.isnan(v)})
        try:
            trades_df   = pf.trades.records_readable
            trades_json = trades_df[["Entry Index","Exit Index","PnL","Return [%]"]
                                    ].head(50).to_json(orient="records")
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
        log(f"  ❌ VectorBT error for {ticker}: {e}")
        return {k: v for k, v in [
            ("final_value", init_cash), ("total_return", 0.0), ("sharpe", 0.0),
            ("max_drawdown", 0.0), ("win_rate", 0.0), ("num_trades", 0),
            ("best_trade_pct", 0.0), ("worst_trade_pct", 0.0),
            ("equity_json", "{}"), ("trades_json", "[]"),
        ]}


# ── Database helpers ──────────────────────────────────────────────────────────

def db_conn() -> sqlite3.Connection:
    c = sqlite3.connect(str(DB_PATH), timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def save_sweep_run(c, agent_id: str, model_id: str, variant: str, notes: str) -> int:
    run_name = f"model_sweep_{agent_id}_{model_id.replace(':', '_').replace('/', '_')}_{variant}"
    c.execute(
        """INSERT INTO backtest_runs
           (run_type, start_date, end_date, model_ids, status, created_at, run_name, version_tag, days)
           VALUES (?,?,?,?,'running',datetime('now'),?,?,?)""",
        (
            "model_sweep",
            START_DATE.isoformat(),
            END_DATE.isoformat(),
            model_id,
            run_name,
            f"sweep_2026-04-20_{variant}",
            PERIOD_DAYS,
        ),
    )
    c.commit()
    return c.execute("SELECT last_insert_rowid()").fetchone()[0]


def save_ticker_result(c, run_id: int, agent_id: str, display_name: str,
                       model_id: str, variant: str, metrics: dict,
                       ticker: str, run_date: str) -> None:
    c.execute(
        """INSERT INTO backtest_results
           (run_id, player_id, display_name, test_date,
            final_value, total_return_pct, win_rate, sharpe_ratio,
            max_drawdown, num_trades, best_trade_pct, worst_trade_pct,
            trades_json, equity_json)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            run_id,
            f"{agent_id}_{variant}",
            f"{display_name} [{ticker}] ({model_id})",
            run_date,
            metrics["final_value"], metrics["total_return"],
            metrics["win_rate"],    metrics["sharpe"],
            metrics["max_drawdown"], metrics["num_trades"],
            metrics["best_trade_pct"], metrics["worst_trade_pct"],
            metrics["trades_json"],    metrics["equity_json"],
        ),
    )
    c.commit()


def save_history_row(c, agent_id: str, display_name: str, model_id: str,
                     variant: str, spy_return: float, per_ticker: dict,
                     run_id: int, avg_latency_ms: float) -> dict:
    returns   = [v["total_return"]  for v in per_ticker.values()]
    win_rates = [v["win_rate"]      for v in per_ticker.values()]
    sharpes   = [v["sharpe"]        for v in per_ticker.values()]
    final_vals= [v["final_value"]   for v in per_ticker.values()]
    max_dds   = [v["max_drawdown"]  for v in per_ticker.values()]
    trades    = [v["num_trades"]    for v in per_ticker.values()]

    avg_return = round(float(np.mean(returns)), 3)
    avg_wr     = round(float(np.mean(win_rates)), 2)
    avg_sharpe = round(float(np.mean(sharpes)), 3)
    avg_dd     = round(float(np.mean(max_dds)), 2)
    total_tr   = sum(trades)
    final_port = round(sum(final_vals), 2)
    best_t     = max(per_ticker, key=lambda t: per_ticker[t]["total_return"])
    worst_t    = min(per_ticker, key=lambda t: per_ticker[t]["total_return"])

    notes = json.dumps({
        "sweep_variant":   variant,
        "model_id":        model_id,
        "agent_id":        agent_id,
        "per_ticker": {t: {"return": v["total_return"], "sharpe": v["sharpe"]}
                       for t, v in per_ticker.items()},
        "spy_benchmark":   spy_return,
        "avg_latency_ms":  round(avg_latency_ms, 1),
        "sweep_run_id":    run_id,
        "sweep_date":      "2026-04-20",
    })

    c.execute(
        """INSERT INTO backtest_history
           (player_id, player_name, run_date, period_days,
            start_date, end_date, starting_value, final_value,
            return_pct, total_trades, win_rate, spy_return_pct,
            best_trade_symbol, worst_trade_symbol, notes,
            signals_tested, sharpe, run_id, max_dd)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            f"{agent_id}__{model_id.replace(':','_').replace('/','_')}__{variant}",
            f"{display_name} | {model_id} | {variant}",
            datetime.now().strftime("%Y-%m-%d"),
            PERIOD_DAYS,
            START_DATE.isoformat(),
            END_DATE.isoformat(),
            INIT_CASH * len(TICKERS),
            final_port,
            avg_return,
            total_tr,
            avg_wr,
            spy_return,
            best_t,
            worst_t,
            notes,
            total_tr,
            avg_sharpe,
            str(run_id),
            avg_dd,
        ),
    )
    c.commit()

    return {
        "avg_return": avg_return,
        "avg_sharpe": avg_sharpe,
        "avg_dd":     avg_dd,
        "avg_wr":     avg_wr,
        "total_trades": total_tr,
        "best_ticker":  best_t,
        "worst_ticker": worst_t,
    }


def mark_run_complete(c, run_id: int, spy_return: float) -> None:
    c.execute(
        "UPDATE backtest_runs SET status='completed', completed_at=datetime('now'), spy_return=? WHERE id=?",
        (spy_return, run_id),
    )
    c.commit()


# ── Price data ────────────────────────────────────────────────────────────────

def download_prices() -> dict[str, pd.DataFrame]:
    log(f"📡 Downloading price data {START_DATE} → {END_DATE} for {TICKERS}")
    # Add 200-day warmup for SMA200
    warmup = START_DATE - timedelta(days=280)
    prices = {}
    for ticker in TICKERS:
        try:
            df = yf.download(
                ticker,
                start=warmup.strftime("%Y-%m-%d"),
                end=(END_DATE + timedelta(days=1)).strftime("%Y-%m-%d"),
                progress=False,
                auto_adjust=True,
            )
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [col[0] for col in df.columns]
            df.index = pd.to_datetime(df.index)
            df = df.sort_index()
            prices[ticker] = df
            # We'll slice to the analysis window later
            log(f"  ✓ {ticker}: {len(df)} days (incl. warmup)")
        except Exception as e:
            log(f"  ✗ {ticker}: {e}")
    return prices


def slice_to_window(df: pd.DataFrame) -> pd.DataFrame:
    """Return rows within [START_DATE, END_DATE] — warmup rows excluded from signals."""
    start_ts = pd.Timestamp(START_DATE)
    end_ts   = pd.Timestamp(END_DATE)
    return df[(df.index >= start_ts) & (df.index <= end_ts)]


def get_spy_return(prices: dict) -> float:
    spy = slice_to_window(prices.get("SPY", pd.DataFrame()))
    if len(spy) < 2:
        return 0.0
    return round((float(spy["Close"].iloc[-1]) / float(spy["Close"].iloc[0]) - 1) * 100, 2)


# ── Main sweep ────────────────────────────────────────────────────────────────

def run_single(
    agent_id: str,
    agent_name: str,
    variant: str,
    model_id: str,
    notes: str,
    prices: dict,
    spy_return: float,
    run_number: int,
) -> dict:
    """Execute one sweep row. Returns summary dict."""
    log(f"\n{'━'*70}")
    log(f"  RUN {run_number}/{TOTAL_RUNS} | {agent_id} | {variant} | {model_id}")
    log(f"  Note: {notes}")
    log(f"{'━'*70}")

    run_start = time.time()

    # Kill switch (blocking — waits until cleared)
    check_kill_switch(block=True)

    # Model availability check
    if not verify_model_on_ollie(model_id):
        log(f"  ⚠️  SKIP — {model_id} not found on Ollie (pull it first)")
        return {
            "agent_id": agent_id, "variant": variant, "model_id": model_id,
            "skipped": True, "reason": "model_not_on_ollie",
            "avg_sharpe": 0, "avg_return": 0, "avg_wr": 0, "total_trades": 0,
        }

    # Disk check
    check_disk()

    c = db_conn()
    run_id = save_sweep_run(c, agent_id, model_id, variant, notes)
    run_date = datetime.now().strftime("%Y-%m-%d")

    per_ticker: dict[str, dict] = {}
    all_latencies: list[float] = []

    for ticker in TICKERS:
        if ticker not in prices:
            log(f"  ⏭  {ticker}: no price data")
            continue

        full_df  = prices[ticker]
        window   = slice_to_window(full_df)

        if len(window) < 10:
            log(f"  ⏭  {ticker}: too few rows in window")
            continue

        # Decision points inside the analysis window
        pts       = get_decision_points(window)
        snapshots = [build_snapshot(full_df, full_df.index.get_loc(window.index[i]))
                     for i in pts]

        log(f"  📈 {ticker}: {len(pts)} decision points → {[s['date'] for s in snapshots]}")

        # Generate signals
        sigs = get_signals(model_id, ticker, snapshots, run_start=run_start)

        # Collect latencies
        all_latencies.extend(s["latency_ms"] for s in sigs)

        # VectorBT
        metrics = run_vectorbt(ticker, window, sigs)
        per_ticker[ticker] = metrics

        log(
            f"    → Return: {metrics['total_return']:+.1f}%  "
            f"Sharpe: {metrics['sharpe']:.3f}  "
            f"MaxDD: {metrics['max_drawdown']:.1f}%  "
            f"WR: {metrics['win_rate']:.0f}%  "
            f"N={metrics['num_trades']}"
        )

        save_ticker_result(c, run_id, agent_id, agent_name, model_id,
                           variant, metrics, ticker, run_date)

    avg_latency = float(np.mean(all_latencies)) if all_latencies else 0.0

    # Roll up
    if per_ticker:
        summary = save_history_row(
            c, agent_id, agent_name, model_id, variant,
            spy_return, per_ticker, run_id, avg_latency,
        )
    else:
        summary = {"avg_sharpe": 0, "avg_return": 0, "avg_wr": 0, "total_trades": 0,
                   "avg_dd": 0, "best_ticker": "?", "worst_ticker": "?"}

    mark_run_complete(c, run_id, spy_return)
    c.close()

    elapsed = int(time.time() - run_start)
    summary.update({
        "agent_id":    agent_id,
        "agent_name":  agent_name,
        "variant":     variant,
        "model_id":    model_id,
        "elapsed_s":   elapsed,
        "avg_latency_ms": round(avg_latency, 1),
        "run_id":      run_id,
        "skipped":     False,
    })

    log(
        f"{datetime.now().strftime('%Y-%m-%d %H:%M')} | "
        f"{agent_id} | {model_id} | "
        f"Sharpe {summary['avg_sharpe']:.3f} | "
        f"WR {summary['avg_wr']:.0f}% | "
        f"DD {summary['avg_dd']:.1f}% | "
        f"N={summary['total_trades']} | "
        f"{elapsed}s"
    )

    return summary


def produce_analysis(all_results: list[dict], spy_return: float) -> None:
    """Write recommendation doc to docs/MODEL_SWEEP_RESULTS_2026-04-20.md."""
    docs_dir = REPO_ROOT / "docs"
    docs_dir.mkdir(exist_ok=True)
    out_path = docs_dir / "MODEL_SWEEP_RESULTS_2026-04-20.md"

    # Group by agent
    by_agent: dict[str, list[dict]] = defaultdict(list)
    for r in all_results:
        if not r.get("skipped"):
            by_agent[r["agent_id"]].append(r)

    lines = [
        "# Model Quality Sweep — USS TradeMinds 2026-04-20",
        "",
        f"**Period**: {START_DATE} → {END_DATE} ({PERIOD_DAYS} days)",
        f"**Tickers**: {', '.join(TICKERS)}",
        f"**SPY benchmark**: {spy_return:+.1f}%",
        f"**Total runs**: {TOTAL_RUNS}  |  Completed: {sum(1 for r in all_results if not r.get('skipped'))}",
        "",
        "> DO NOT AUTO-DEPLOY — Steve reviews recommendations before any config.py edits.",
        "",
        "---",
        "",
        "## Full Matrix Results",
        "",
        "| # | Agent | Variant | Model | Sharpe | WR% | MaxDD% | N | Latency (ms) |",
        "|---|-------|---------|-------|--------|-----|--------|---|-------------|",
    ]

    for i, r in enumerate(all_results, 1):
        skip = "⚠️ SKIP" if r.get("skipped") else ""
        lines.append(
            f"| {i} | {r['agent_id']} | {r['variant']} | `{r['model_id']}` | "
            f"{r.get('avg_sharpe',0):.3f} | {r.get('avg_wr',0):.0f} | "
            f"{r.get('avg_dd',0):.1f} | {r.get('total_trades',0)} | "
            f"{r.get('avg_latency_ms',0):.0f} {skip} |"
        )

    lines += ["", "---", "", "## Per-Agent Winners & Recommendations", ""]

    all_recs = []
    for agent_id, results in sorted(by_agent.items()):
        # Pick winner by Sharpe
        winner = max(results, key=lambda x: x["avg_sharpe"])
        baseline = next((r for r in results if r["variant"] == "BASE"), None)
        sharpe_delta = winner["avg_sharpe"] - (baseline["avg_sharpe"] if baseline else 0)

        # Overfit flag: if winner 2x+ outperforms others
        if len(results) > 1:
            non_winner_sharpes = [r["avg_sharpe"] for r in results if r != winner]
            avg_others = np.mean(non_winner_sharpes) if non_winner_sharpes else 0
            overfit_flag = (winner["avg_sharpe"] > 2 * abs(avg_others) + 0.5
                            and avg_others < 0.5)
        else:
            overfit_flag = False

        # Confidence
        if overfit_flag:
            confidence = "Low (overfit risk)"
        elif len(results) == 1:
            confidence = "Low (single run, no comparison)"
        elif sharpe_delta > 0.3:
            confidence = "High"
        elif sharpe_delta > 0.1:
            confidence = "Med"
        else:
            confidence = "Low (small delta)"

        lines += [
            f"### {agent_id}",
            "",
            "| Variant | Model | Sharpe | WR% | MaxDD% |",
            "|---------|-------|--------|-----|--------|",
        ]
        for r in sorted(results, key=lambda x: ("BASE","LIGHT","HEAVY").index(x["variant"])
                        if x["variant"] in ("BASE","LIGHT","HEAVY") else 99):
            star = " ⭐" if r == winner else ""
            lines.append(
                f"| {r['variant']}{star} | `{r['model_id']}` | "
                f"{r['avg_sharpe']:.3f} | {r['avg_wr']:.0f} | {r['avg_dd']:.1f} |"
            )
        lines += [
            "",
            f"**Recommended**: `{winner['model_id']}` (variant: {winner['variant']})",
            f"**Sharpe Δ vs BASE**: {sharpe_delta:+.3f}",
            f"**Confidence**: {confidence}",
        ]
        if overfit_flag:
            lines.append("**⚠️ OVERFIT RISK**: Winner outperforms others by >2x — validate on separate OOS window before deploying.")
        lines.append("")

        all_recs.append({
            "agent_id":    agent_id,
            "current":     baseline["model_id"] if baseline else "?",
            "recommended": winner["model_id"],
            "variant":     winner["variant"],
            "sharpe_delta": round(sharpe_delta, 3),
            "confidence":  confidence,
        })

    # Summary table
    lines += [
        "---",
        "",
        "## Recommendation Summary",
        "",
        "| Agent | Current Model | Recommended | Sharpe Δ | Confidence |",
        "|-------|--------------|-------------|----------|------------|",
    ]
    for r in all_recs:
        change = "→ " if r["current"] != r["recommended"] else "= "
        lines.append(
            f"| {r['agent_id']} | `{r['current']}` | "
            f"{change}`{r['recommended']}` | {r['sharpe_delta']:+.3f} | {r['confidence']} |"
        )

    # Proposed config.py edits
    lines += [
        "",
        "---",
        "",
        "## Proposed config.py / main.py Edits (DO NOT AUTO-APPLY)",
        "",
        "```python",
        "# Paste these changes ONLY after Steve reviews the above table.",
        "# Each line is: agent_id → model change required.",
        "",
    ]
    for r in all_recs:
        if r["current"] != r["recommended"]:
            lines.append(f"# {r['agent_id']}: {r['current']} → {r['recommended']}  ({r['confidence']})")
    lines += ["```", "", "---", "", f"_Generated by model_sweep_2026_04_20.py at {datetime.now().isoformat()}_"]

    out_path.write_text("\n".join(lines))
    log(f"\n📄 Recommendation doc → {out_path}")


def print_matrix() -> None:
    """Print the full matrix to log before starting."""
    log("=" * 70)
    log("SWEEP MATRIX — 22 runs planned:")
    log("=" * 70)
    for i, (agent_id, agent_name, variant, model_id, notes) in enumerate(SWEEP_MATRIX, 1):
        log(f"  {i:2d}. {agent_id:<20} {variant:<6} {model_id:<30} {notes}")
    log("=" * 70)
    log(f"  Date window : {START_DATE} → {END_DATE} ({PERIOD_DAYS} days)")
    log(f"  Tickers     : {', '.join(TICKERS)}")
    log(f"  Ollie GPU   : {OLLIE_BASE}")
    log("=" * 70)


def main() -> None:
    os.chdir(REPO_ROOT)

    log("🚀 USS TradeMinds — Model Quality Sweep 2026-04-20")
    log(f"   Total runs : {TOTAL_RUNS}")
    log(f"   Window     : {START_DATE} → {END_DATE}")

    # Phase 1 preflight
    log("\n── PHASE 1: PREFLIGHT ──")
    check_ollie()
    check_disk()
    check_kill_switch()
    log("  Ollie: OK  |  Disk: OK  |  Kill switch: clear")

    # Verify all models present on Ollie
    log("\n── Verifying models on Ollie ──")
    all_needed = sorted(set(model_id for _, _, _, model_id, _ in SWEEP_MATRIX))
    missing = []
    for m in all_needed:
        ok = verify_model_on_ollie(m)
        log(f"  {'✓' if ok else '✗'} {m}")
        if not ok:
            missing.append(m)
    if missing:
        log(f"\n  ⚠️  Missing models (will skip those runs): {missing}")
        log("  Pull missing models: ssh bigmac@192.168.1.166 'ollama pull <model>'")

    # Phase 2 — print matrix
    log("\n── PHASE 2: SWEEP MATRIX ──")
    print_matrix()

    # Download price data once (reused across all runs)
    log("\n── PHASE 3: DOWNLOADING PRICE DATA ──")
    prices = download_prices()
    if len(prices) < len(TICKERS) - 1:
        log(f"SWEEP HALTED — only {len(prices)}/{len(TICKERS)} tickers downloaded")
        sys.exit(1)
    spy_return = get_spy_return(prices)
    log(f"  SPY benchmark ({START_DATE}→{END_DATE}): {spy_return:+.1f}%\n")

    # Phase 3 — execute matrix
    log("── PHASE 3: EXECUTING MATRIX ──")
    all_results  = []
    sweep_start  = time.time()
    last_hourly  = time.time()

    for run_number, (agent_id, agent_name, variant, model_id, notes) in enumerate(SWEEP_MATRIX, 1):
        # Hourly status check-in
        if time.time() - last_hourly >= 3600:
            done   = run_number - 1
            remain = TOTAL_RUNS - done
            eta_s  = (time.time() - sweep_start) / max(done, 1) * remain
            eta_ts = datetime.fromtimestamp(time.time() + eta_s).strftime("%H:%M")
            log(f"STATUS | {done}/{TOTAL_RUNS} runs done | ETA ~{eta_ts} | "
                f"elapsed {int((time.time()-sweep_start)/60)}min")
            last_hourly = time.time()

        result = run_single(
            agent_id, agent_name, variant, model_id, notes,
            prices, spy_return, run_number,
        )
        all_results.append(result)

    # Phase 4 — analysis
    log("\n── PHASE 4: ANALYSIS ──")
    produce_analysis(all_results, spy_return)

    # Update SESSION doc
    session_doc = REPO_ROOT / "SESSION_2026-04-20.md"
    if session_doc.exists():
        old = session_doc.read_text()
        ts = datetime.now().strftime("%Y-%m-%d %H:%M AZ")
        tag = f"\n\n---\n\n## Model Sweep\n\nModel sweep completed {ts}, results in `docs/MODEL_SWEEP_RESULTS_2026-04-20.md`\n"
        session_doc.write_text(old + tag)
        log("  SESSION_2026-04-20.md updated")

    elapsed_total = int(time.time() - sweep_start)
    completed = sum(1 for r in all_results if not r.get("skipped"))
    skipped   = sum(1 for r in all_results if r.get("skipped"))

    log(f"\n{'═'*70}")
    log(f"  SWEEP COMPLETE — {completed} runs done, {skipped} skipped")
    log(f"  Total elapsed: {elapsed_total//3600}h {(elapsed_total%3600)//60}m")
    log(f"  Results → data/trader.db (backtest_runs, backtest_results, backtest_history)")
    log(f"  Report  → docs/MODEL_SWEEP_RESULTS_2026-04-20.md")
    log(f"{'═'*70}")


if __name__ == "__main__":
    main()
