#!/usr/bin/env python3
"""
USS TradeMinds — Model Quality Sweep v2
========================================
Overnight backtest matrix. Runs the same 365-day window with weekly decision
points and VectorBT 5% SL / 10% TP for each of 22 model variants across
9 S6 agents. Results saved to trader.db incrementally.

Usage (nohup-wrapped, survives terminal close):
    cd ~/autonomous-trader
    nohup venv/bin/python3 scripts/model_sweep_v2.py \
      > /tmp/model_sweep_v2.log 2>&1 &

Kill switches:
    touch ~/autonomous-trader/SWEEP_KILL_SWITCH   # pause sweep, live trader runs
    touch ~/autonomous-trader/KILL_SWITCH         # pause everything (market-closed only)

Time guard:
    Auto-pauses at 6:25 AM AZ (MST) by creating SWEEP_KILL_SWITCH.
    Resume after market close: rm ~/autonomous-trader/SWEEP_KILL_SWITCH

v1 postmortem: bi-monthly decision points → 4 calls/ticker → N≤1 trades →
identical results across models. v2 fixes this with weekly (52/ticker) +
SL/TP to generate 130-150 closed round-trips per model variant.
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
from datetime import date, datetime, timedelta, time as dtime
from pathlib import Path
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import vectorbt as vbt
import yfinance as yf

warnings.filterwarnings("ignore")

# ── Paths ─────────────────────────────────────────────────────────────────────

REPO_ROOT       = Path(__file__).parent.parent
DB_PATH         = REPO_ROOT / "data" / "trader.db"
KILL_FILE       = REPO_ROOT / "KILL_SWITCH"
SWEEP_KILL_FILE = REPO_ROOT / "SWEEP_KILL_SWITCH"
LOG_FILE        = Path("/tmp/model_sweep_v2.log")

# ── Inference ─────────────────────────────────────────────────────────────────

OLLIE_BASE     = "http://192.168.1.166:11434"
OLLIE_GENERATE = OLLIE_BASE + "/api/generate"
OLLIE_CHAT     = OLLIE_BASE + "/api/chat"
OLLIE_TAGS     = OLLIE_BASE + "/api/tags"

# ── Backtest window ───────────────────────────────────────────────────────────

START_DATE  = date(2024, 4, 1)
END_DATE    = date(2025, 4, 1)
PERIOD_DAYS = (END_DATE - START_DATE).days   # 366
SMA_WARMUP  = START_DATE - timedelta(days=280)   # for SMA-200 calculation

TICKERS = [
    "SPY",  "QQQ",   # broad indices
    "IWM",  "DIA",   # small-cap / DJIA
    "NVDA", "AMD",   # semis — volatile, LLM-sensitive
    "TSLA", "META",  # high-beta
    "MSFT", "AAPL",  # large-cap anchors
    "GOOGL","PLTR",  # mega-cap + speculative
]

INIT_CASH = 10_000.0
FEES      = 0.001    # 0.1% per trade
SL_STOP   = 0.05     # 5% stop-loss
TP_STOP   = 0.10     # 10% take-profit

# ── Time guard ────────────────────────────────────────────────────────────────

AZ_TZ              = ZoneInfo("America/Phoenix")   # MST year-round, no DST
MARKET_GUARD_START = dtime(6, 25)                  # AZ pre-market → pause window begins
MARKET_GUARD_END   = dtime(13, 30)                 # AZ 30 min past close → pause window ends
MARKET_GUARD_TIME  = MARKET_GUARD_START            # legacy alias (used in log line below)

# ── Notifications ─────────────────────────────────────────────────────────────

NTFY_TOPIC = os.environ.get("NTFY_ADMIN_TOPIC", "Ollie-Alert-35")

# ── Sweep matrix (22 variants) ────────────────────────────────────────────────

SWEEP_MATRIX = [
    # (agent_id, agent_name, variant, model_id, notes)
    ("ollie-auto",    "Ollie (Quality Gate)",  "BASE",  "qwen3:8b",              "current"),
    ("ollie-auto",    "Ollie (Quality Gate)",  "LIGHT", "phi3:mini",             "lighter/faster"),
    ("ollie-auto",    "Ollie (Quality Gate)",  "HEAVY", "qwen3:14b",             "deeper reasoning"),
    ("navigator",     "Navigator",             "BASE",  "qwen3:8b",              "current Ollie"),
    ("navigator",     "Navigator",             "LIGHT", "phi3:mini",             "lighter"),
    ("navigator",     "Navigator",             "HEAVY", "qwen3:14b",             "heavier"),
    ("chekov",        "Chekov (muted)",        "BASE",  "phi3:mini",             "muted — 1 run only"),
    ("ollama-llama",  "Uhura (llama3.1)",      "BASE",  "llama3.1:latest",       "current"),
    ("ollama-llama",  "Uhura (llama3.1)",      "LIGHT", "llama3.2:3b",           "smaller llama"),
    ("ollama-llama",  "Uhura (llama3.1)",      "HEAVY", "qwen3:14b",             "cross-family heavy"),
    ("ollama-plutus", "McCoy (Plutus-3B)",     "BASE",  "0xroyce/plutus:latest", "current, finance-tuned"),
    ("ollama-plutus", "McCoy (Plutus-3B)",     "LIGHT", "qwen3:8b",              "general 8b"),
    ("ollama-plutus", "McCoy (Plutus-3B)",     "HEAVY", "deepseek-r1:14b",       "deep reasoning"),
    ("ollama-qwen3",  "Dax (qwen3:8b)",        "BASE",  "qwen3:8b",              "current"),
    ("ollama-qwen3",  "Dax (qwen3:8b)",        "LIGHT", "phi3:mini",             "lighter"),
    ("ollama-qwen3",  "Dax (qwen3:8b)",        "HEAVY", "qwen3:14b",             "heavier"),
    ("ollama-coder",  "Data (qwen2.5-coder)",  "BASE",  "qwen2.5-coder:7b",      "current"),
    ("ollama-coder",  "Data (qwen2.5-coder)",  "LIGHT", "phi3:mini",             "lighter"),
    ("ollama-coder",  "Data (qwen2.5-coder)",  "HEAVY", "qwen3-coder:30b",       "30b coder"),
    ("neo-matrix",    "Neo (phi3:mini)",       "BASE",  "phi3:mini",             "current"),
    ("neo-matrix",    "Neo (phi3:mini)",       "HEAVY", "qwen3:8b",              "upgrade candidate"),
    ("capitol-trades","Capitol (data-feed)",   "BASE",  "phi3:mini",             "data-driven — 1 run"),
]

TOTAL_RUNS = len(SWEEP_MATRIX)

# ── Logging ───────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} | {msg}"
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")
    print(line, flush=True)


# ── Kill switch + time guard ──────────────────────────────────────────────────

def check_kills_and_guard() -> bool:
    """
    Check all stop conditions at run boundaries.
    Returns True if sweep should abort entirely (KILL_SWITCH only).
    Blocks (polling) if SWEEP_KILL_SWITCH or time guard fires.
    Caller should break outer loop when this returns True.
    """
    # 1. Hard fleet halt — abort sweep entirely
    if KILL_FILE.exists():
        log("SWEEP HALTED — KILL_SWITCH detected (fleet halt). Abandoning.")
        return True

    # 2. Time guard — inside market window (06:25-13:30 AZ); evening/overnight runs proceed
    now_az = datetime.now(AZ_TZ)
    now_t = now_az.time()
    if MARKET_GUARD_START <= now_t <= MARKET_GUARD_END:
        log(
            f"SWEEP INCOMPLETE — time guard fired at {now_az.strftime('%H:%M %Z')}. "
            "Creating SWEEP_KILL_SWITCH to pause. "
            "Resume after close: rm ~/autonomous-trader/SWEEP_KILL_SWITCH"
        )
        SWEEP_KILL_FILE.touch()
        # Fall through to sweep-pause block below

    # 3. Sweep-only pause — poll until cleared
    if SWEEP_KILL_FILE.exists():
        log("SWEEP PAUSED — SWEEP_KILL_SWITCH detected. Polling every 60s.")
        while SWEEP_KILL_FILE.exists():
            time.sleep(60)
            if KILL_FILE.exists():
                log("SWEEP HALTED — KILL_SWITCH detected while paused.")
                return True
        log("SWEEP RESUMED — SWEEP_KILL_SWITCH cleared.")

    return False


# ── Safety checks ─────────────────────────────────────────────────────────────

def check_disk() -> None:
    usage    = shutil.disk_usage(REPO_ROOT)
    pct_free = usage.free / usage.total * 100
    gb_free  = usage.free / 1_073_741_824
    if gb_free < 5:
        log(f"SWEEP HALTED — only {gb_free:.1f} GB free ({pct_free:.0f}%)")
        sys.exit(1)


def check_ollie() -> None:
    try:
        r = requests.get(OLLIE_TAGS, timeout=5)
        r.raise_for_status()
    except Exception as e:
        log(f"SWEEP HALTED — Ollie unreachable: {e}")
        sys.exit(1)


def verify_model(model_id: str) -> bool:
    try:
        models = [m["name"] for m in
                  requests.get(OLLIE_TAGS, timeout=5).json().get("models", [])]
        return any(model_id in n or n in model_id for n in models)
    except Exception:
        return False


# ── ntfy ──────────────────────────────────────────────────────────────────────

def push_ntfy(title: str, body: str, priority: str = "default",
              tags: str = "white_check_mark,sweep") -> None:
    try:
        ascii_title = title.encode("ascii", errors="ignore").decode("ascii").strip()
        req = Request(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=body.encode("utf-8"),
            headers={
                "Title":        ascii_title,
                "Priority":     priority,
                "Tags":         tags,
                "Content-Type": "text/plain; charset=utf-8",
            },
            method="POST",
        )
        urlopen(req, timeout=6)
    except Exception:
        pass


# ── Technical indicators ──────────────────────────────────────────────────────

def compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss.replace(0, np.nan)
    return (100 - (100 / (1 + rs))).fillna(50)


def build_snapshot(full_df: pd.DataFrame, as_of_idx: int) -> dict:
    """Build indicator snapshot using all warmup data up to as_of_idx."""
    sub   = full_df.iloc[: as_of_idx + 1]
    close = sub["Close"]
    cur   = float(close.iloc[-1])
    sma50  = float(close.rolling(50).mean().iloc[-1])  if len(close) >= 50  else cur
    sma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else cur
    rsi    = float(compute_rsi(close).iloc[-1])
    vol_avg = float(sub["Volume"].rolling(20).mean().iloc[-1]) if "Volume" in sub.columns else 0
    vol_now = float(sub["Volume"].iloc[-1])                    if "Volume" in sub.columns else 0
    ret_1m  = float(close.pct_change(20).iloc[-1] * 100) if len(close) >= 21 else 0.0
    ret_3m  = float(close.pct_change(60).iloc[-1] * 100) if len(close) >= 61 else 0.0
    vs50    = (cur - sma50)  / sma50  * 100 if sma50  else 0.0
    vs200   = (cur - sma200) / sma200 * 100 if sma200 else 0.0
    return {
        "date":        str(full_df.index[as_of_idx].date()),
        "price":       round(cur, 2),
        "sma50":       round(sma50, 2),
        "sma200":      round(sma200, 2),
        "rsi":         round(rsi, 1),
        "vs50_pct":    round(vs50, 2),
        "vs200_pct":   round(vs200, 2),
        "ret_1m_pct":  round(ret_1m, 2),
        "ret_3m_pct":  round(ret_3m, 2),
        "ret_ytd_pct": 0.0,
        "vol_ratio":   round(vol_now / vol_avg, 2) if vol_avg else 1.0,
        "trend":       "BULLISH" if cur > sma200 else "BEARISH",
    }


def get_weekly_decision_points(full_df: pd.DataFrame,
                               window_start: date, window_end: date) -> list[int]:
    """
    Weekly Friday close decision points within [window_start, window_end].
    Returns integer positions in full_df (which includes warmup rows).
    """
    start_ts = pd.Timestamp(window_start)
    end_ts   = pd.Timestamp(window_end)
    window   = full_df[(full_df.index >= start_ts) & (full_df.index <= end_ts)]
    weekly   = window.resample("W-FRI").last()
    idxs = []
    for dt in weekly.index:
        pos = full_df.index.searchsorted(dt, side="right") - 1
        if 0 <= pos < len(full_df):
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

THINK_MODELS = {
    "qwen3:8b", "qwen3:14b", "qwen3-coder:30b",
    "deepseek-r1:14b", "deepseek-r1:7b",
}

_consec_errors = 0


def _strip_think(text: str) -> str:
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL | re.IGNORECASE)
    return re.sub(r"<think>.*", "", text, flags=re.DOTALL | re.IGNORECASE).strip()


def call_model(model_id: str, prompt: str, run_start: float,
               max_retries: int = 3, timeout: int = 120) -> str:
    global _consec_errors

    # 30-min per-run hard ceiling
    if time.time() - run_start > 1800:
        log("SWEEP HALTED — single run exceeded 30-min ceiling")
        sys.exit(1)

    is_think = model_id in THINK_MODELS
    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                wait = 30 * attempt
                log(f"  retry {attempt}/{max_retries} in {wait}s")
                time.sleep(wait)

            if is_think:
                resp = requests.post(
                    OLLIE_CHAT,
                    json={
                        "model":   model_id,
                        "messages": [{"role": "user", "content": prompt}],
                        "stream":  False,
                        "think":   False,
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
                        "model":   model_id,
                        "prompt":  prompt,
                        "stream":  False,
                        "options": {"temperature": 0.1, "num_predict": 150, "num_ctx": 1024},
                    },
                    timeout=timeout,
                )
                resp.raise_for_status()
                raw = resp.json().get("response", "") or ""

            _consec_errors = 0
            return _strip_think(raw).strip()

        except requests.HTTPError as e:
            _consec_errors += 1
            log(f"  HTTP {e.response.status_code} from Ollie ({model_id})")
            if _consec_errors > 3:
                log("SWEEP HALTED — >3 consecutive Ollie 500s")
                sys.exit(1)
        except requests.exceptions.ConnectionError:
            log(f"SWEEP HALTED — Ollie unreachable during run ({model_id})")
            sys.exit(1)
        except Exception as e:
            _consec_errors += 1
            log(f"  {model_id}: {type(e).__name__}: {str(e)[:80]}")

    return ""   # default HOLD on parse


def parse_signal(text: str) -> tuple[str, int, str]:
    clean = re.sub(r"\*+", "", text)
    m = re.search(r"SIGNAL:\s*(BUY|SELL|HOLD)", clean, re.IGNORECASE)
    signal = m.group(1).upper() if m else "HOLD"
    if not m:
        for word in re.sub(r"[^\w\s]", " ", text.upper()).split()[:8]:
            if word in ("BUY", "SELL", "HOLD"):
                signal = word
                break
    m_conf   = re.search(r"CONFIDENCE:\s*(\d+)", clean, re.IGNORECASE)
    m_reason = re.search(r"REASON:\s*(.+?)(?:\||[\r\n]|$)", clean, re.IGNORECASE)
    conf   = min(max(int(m_conf.group(1)), 1), 10) if m_conf else 5
    reason = m_reason.group(1).strip()[:100] if m_reason else text[:60].strip()
    return signal, conf, reason


def get_signals(model_id: str, ticker: str, full_df: pd.DataFrame,
                decision_idxs: list[int], run_start: float) -> list[dict]:
    results = []
    for idx in decision_idxs:
        snap    = build_snapshot(full_df, idx)
        prompt  = SIGNAL_PROMPT.format(ticker=ticker, **snap)
        t0      = time.time()
        raw     = call_model(model_id, prompt, run_start)
        latency = int((time.time() - t0) * 1000)
        signal, conf, reason = parse_signal(raw)
        results.append({
            "date": snap["date"], "signal": signal,
            "confidence": conf, "reason": reason,
            "latency_ms": latency, "raw": raw[:200],
        })
        time.sleep(1)   # 1s between calls — lighter than v1's 2s
    return results


# ── VectorBT ──────────────────────────────────────────────────────────────────

def _safe(v) -> float:
    if v is None: return 0.0
    f = float(v)
    return 0.0 if (np.isnan(f) or np.isinf(f)) else f


def run_vectorbt(ticker: str, window_df: pd.DataFrame,
                 signal_list: list[dict]) -> dict:
    close  = window_df["Close"].astype(float)
    dates  = close.index
    sig_s  = pd.Series("HOLD", index=dates)
    for s in signal_list:
        dt  = pd.Timestamp(s["date"])
        fut = dates[dates > dt]
        if len(fut):
            sig_s.loc[fut[0]:] = s["signal"]
    entries = (sig_s == "BUY").values
    exits   = (sig_s == "SELL").values
    try:
        pf = vbt.Portfolio.from_signals(
            close, entries, exits,
            freq="1D",
            fees=FEES,
            init_cash=INIT_CASH,
            sl_stop=SL_STOP,
            tp_stop=TP_STOP,
        )
        st = pf.stats()
        equity = pf.value().resample("W").last()
        equity_json = json.dumps({
            str(d.date()): round(float(v), 2)
            for d, v in equity.items() if not np.isnan(v)
        })
        try:
            tr = pf.trades.records_readable
            trades_json = tr[["Entry Index", "Exit Index", "PnL", "Return [%]"]
                             ].head(100).to_json(orient="records")
        except Exception:
            trades_json = "[]"
        return {
            "final_value":    round(_safe(pf.final_value()), 2),
            "total_return":   round(_safe(st.get("Total Return [%]", 0)), 2),
            "sharpe":         round(_safe(st.get("Sharpe Ratio", 0)), 4),
            "max_drawdown":   round(_safe(st.get("Max Drawdown [%]", 0)), 2),
            "win_rate":       round(_safe(st.get("Win Rate [%]", 0)), 2),
            "num_trades":     int(_safe(st.get("Total Trades", 0))),
            "best_trade_pct": round(_safe(st.get("Best Trade [%]", 0)), 2),
            "worst_trade_pct":round(_safe(st.get("Worst Trade [%]", 0)), 2),
            "equity_json":    equity_json,
            "trades_json":    trades_json,
        }
    except Exception as e:
        log(f"  VectorBT error ({ticker}): {e}")
        return {k: v for k, v in [
            ("final_value", INIT_CASH), ("total_return", 0.0), ("sharpe", 0.0),
            ("max_drawdown", 0.0), ("win_rate", 0.0), ("num_trades", 0),
            ("best_trade_pct", 0.0), ("worst_trade_pct", 0.0),
            ("equity_json", "{}"), ("trades_json", "[]"),
        ]}


# ── Database ──────────────────────────────────────────────────────────────────

def db_conn() -> sqlite3.Connection:
    c = sqlite3.connect(str(DB_PATH), timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def db_create_run(agent_id: str, model_id: str, variant: str) -> int:
    run_name = (
        f"sweep_v2_{agent_id}_{model_id.replace(':','_').replace('/','_')}_{variant}"
    )
    c = db_conn()
    c.execute(
        """INSERT INTO backtest_runs
           (run_type, start_date, end_date, model_ids, status,
            created_at, run_name, version_tag, days, notes)
           VALUES (?,?,?,?,'running',datetime('now'),?,?,?,?)""",
        (
            "model_sweep_v2",
            START_DATE.isoformat(),
            END_DATE.isoformat(),
            json.dumps({"agent": agent_id, "model": model_id, "variant": variant}),
            run_name,
            "sweep_v2_2026-04-20",
            PERIOD_DAYS,
            "v2 weekly decisions, 12 tickers, 5% SL + 10% TP",
        ),
    )
    c.commit()
    run_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    c.close()
    return run_id


def db_save_ticker(run_id: int, agent_id: str, agent_name: str, model_id: str,
                   variant: str, ticker: str, metrics: dict) -> None:
    c = db_conn()
    c.execute(
        """INSERT INTO backtest_results
           (run_id, player_id, display_name, test_date,
            final_value, total_return_pct, win_rate, sharpe_ratio,
            max_drawdown, num_trades, best_trade_pct, worst_trade_pct,
            trades_json, equity_json)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            run_id,
            f"{agent_id}__{variant}",
            f"{agent_name} [{ticker}] | {model_id}",
            datetime.now().strftime("%Y-%m-%d"),
            metrics["final_value"], metrics["total_return"],
            metrics["win_rate"],    metrics["sharpe"],
            metrics["max_drawdown"],metrics["num_trades"],
            metrics["best_trade_pct"], metrics["worst_trade_pct"],
            metrics["trades_json"],    metrics["equity_json"],
        ),
    )
    c.commit()
    c.close()


def db_complete_run(run_id: int, spy_return: float, rollup: dict) -> None:
    c = db_conn()
    c.execute(
        """UPDATE backtest_runs
           SET status='completed', completed_at=datetime('now'), spy_return=?
           WHERE id=?""",
        (spy_return, run_id),
    )
    # Also write to backtest_history for dashboard visibility
    c.execute(
        """INSERT INTO backtest_history
           (player_id, player_name, run_date, period_days,
            start_date, end_date, starting_value, final_value,
            return_pct, total_trades, win_rate, spy_return_pct,
            best_trade_symbol, worst_trade_symbol, notes,
            signals_tested, sharpe, run_id, max_dd)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            rollup["player_id"],
            rollup["player_name"],
            datetime.now().strftime("%Y-%m-%d"),
            PERIOD_DAYS,
            START_DATE.isoformat(),
            END_DATE.isoformat(),
            INIT_CASH * len(TICKERS),
            rollup["final_port"],
            rollup["avg_return"],
            rollup["total_trades"],
            rollup["avg_wr"],
            spy_return,
            rollup["best_ticker"],
            rollup["worst_ticker"],
            json.dumps({
                "sweep_variant":  rollup["variant"],
                "model_id":       rollup["model_id"],
                "agent_id":       rollup["agent_id"],
                "per_ticker":     rollup["per_ticker_summary"],
                "avg_latency_ms": rollup["avg_latency_ms"],
                "sweep_run_id":   run_id,
                "sweep_version":  "v2",
            }),
            rollup["total_trades"],
            rollup["avg_sharpe"],
            str(run_id),
            rollup["avg_dd"],
        ),
    )
    c.commit()
    c.close()


def db_cancel_run(run_id: int, reason: str) -> None:
    c = db_conn()
    c.execute(
        "UPDATE backtest_runs SET status='cancelled', notes=notes||? WHERE id=?",
        (f" — cancelled: {reason}", run_id),
    )
    c.commit()
    c.close()


# ── Price data ────────────────────────────────────────────────────────────────

def download_prices() -> dict[str, pd.DataFrame]:
    """Download all tickers with warmup window. Returns {ticker: full_df}."""
    log(f"Downloading {len(TICKERS)} tickers ({SMA_WARMUP} → {END_DATE}, incl. SMA warmup)...")
    prices: dict[str, pd.DataFrame] = {}
    dl_end = (END_DATE + timedelta(days=1)).strftime("%Y-%m-%d")
    for ticker in TICKERS:
        try:
            df = yf.download(
                ticker,
                start=SMA_WARMUP.strftime("%Y-%m-%d"),
                end=dl_end,
                progress=False,
                auto_adjust=True,
            )
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0] for c in df.columns]
            df.index = pd.to_datetime(df.index)
            df = df.sort_index()
            if len(df) > 20:
                prices[ticker] = df
                log(f"  {ticker}: {len(df)} rows ({df.index[0].date()} → {df.index[-1].date()})")
            else:
                log(f"  {ticker}: TOO FEW ROWS — skipping")
        except Exception as e:
            log(f"  {ticker}: DOWNLOAD FAILED — {e}")
    return prices


def get_spy_return(prices: dict) -> float:
    spy   = prices.get("SPY", pd.DataFrame())
    start = pd.Timestamp(START_DATE)
    end   = pd.Timestamp(END_DATE)
    win   = spy[(spy.index >= start) & (spy.index <= end)]
    if len(win) < 2:
        return 0.0
    return round((float(win["Close"].iloc[-1]) / float(win["Close"].iloc[0]) - 1) * 100, 2)


# ── Single variant run ────────────────────────────────────────────────────────

def run_single(
    run_number: int,
    agent_id: str, agent_name: str, variant: str, model_id: str, notes: str,
    prices: dict[str, pd.DataFrame],
    spy_return: float,
) -> dict | None:
    """
    Execute one sweep variant. Returns summary dict or None if skipped/halted.
    Saves to DB incrementally — every ticker result written immediately.
    """
    log(f"\n{'━'*72}")
    log(f"  RUN {run_number}/{TOTAL_RUNS} | {agent_id} | {variant} | {model_id}")
    log(f"  Notes: {notes}")
    log(f"{'━'*72}")

    run_start = time.time()

    if not verify_model(model_id):
        log(f"  SKIP — {model_id} not found on Ollie")
        return {"skipped": True, "reason": "model_not_on_ollie",
                "agent_id": agent_id, "variant": variant, "model_id": model_id,
                "avg_sharpe": 0.0, "avg_return": 0.0, "avg_wr": 0.0,
                "total_trades": 0, "avg_dd": 0.0}

    check_disk()
    run_id = db_create_run(agent_id, model_id, variant)

    per_ticker: dict[str, dict] = {}
    all_latencies: list[float]  = []

    for ticker in TICKERS:
        if ticker not in prices:
            log(f"  {ticker}: no price data, skipping")
            continue

        full_df  = prices[ticker]
        idxs     = get_weekly_decision_points(full_df, START_DATE, END_DATE)
        n_points = len(idxs)

        log(f"  {ticker}: {n_points} weekly decision points")
        if n_points == 0:
            log(f"  {ticker}: 0 decision points — skipping")
            continue

        sigs = get_signals(model_id, ticker, full_df, idxs, run_start)
        all_latencies.extend(s["latency_ms"] for s in sigs)

        # VectorBT on analysis window only (not warmup)
        start_ts = pd.Timestamp(START_DATE)
        end_ts   = pd.Timestamp(END_DATE)
        window   = full_df[(full_df.index >= start_ts) & (full_df.index <= end_ts)]
        metrics  = run_vectorbt(ticker, window, sigs)
        per_ticker[ticker] = metrics

        log(
            f"    {ticker} → Return {metrics['total_return']:+.1f}%  "
            f"Sharpe {metrics['sharpe']:.3f}  "
            f"MaxDD {metrics['max_drawdown']:.1f}%  "
            f"WR {metrics['win_rate']:.0f}%  "
            f"N={metrics['num_trades']}"
        )

        # Incremental save — written immediately, never lost
        db_save_ticker(run_id, agent_id, agent_name, model_id, variant,
                       ticker, metrics)

    if not per_ticker:
        log(f"  No ticker results — cancelling run {run_id}")
        db_cancel_run(run_id, "no ticker results")
        return None

    avg_latency = float(np.mean(all_latencies)) if all_latencies else 0.0

    # Roll-up metrics
    returns    = [v["total_return"]  for v in per_ticker.values()]
    sharpes    = [v["sharpe"]        for v in per_ticker.values()]
    win_rates  = [v["win_rate"]      for v in per_ticker.values()]
    max_dds    = [v["max_drawdown"]  for v in per_ticker.values()]
    final_vals = [v["final_value"]   for v in per_ticker.values()]
    n_trades   = [v["num_trades"]    for v in per_ticker.values()]

    avg_return  = round(float(np.mean(returns)), 4)
    avg_sharpe  = round(float(np.mean(sharpes)), 4)
    avg_wr      = round(float(np.mean(win_rates)), 2)
    avg_dd      = round(float(np.mean(max_dds)), 2)
    total_trades = sum(n_trades)
    final_port   = round(sum(final_vals), 2)
    best_ticker  = max(per_ticker, key=lambda t: per_ticker[t]["total_return"])
    worst_ticker = min(per_ticker, key=lambda t: per_ticker[t]["total_return"])

    rollup = {
        "player_id":   f"{agent_id}__{model_id.replace(':','_').replace('/','_')}__{variant}",
        "player_name": f"{agent_name} | {model_id} | {variant}",
        "agent_id":    agent_id,
        "model_id":    model_id,
        "variant":     variant,
        "avg_return":  avg_return,
        "avg_sharpe":  avg_sharpe,
        "avg_wr":      avg_wr,
        "avg_dd":      avg_dd,
        "total_trades": total_trades,
        "final_port":  final_port,
        "best_ticker": best_ticker,
        "worst_ticker":worst_ticker,
        "avg_latency_ms": round(avg_latency, 1),
        "per_ticker_summary": {
            t: {"return": v["total_return"], "sharpe": v["sharpe"], "n": v["num_trades"]}
            for t, v in per_ticker.items()
        },
    }

    db_complete_run(run_id, spy_return, rollup)

    elapsed = int(time.time() - run_start)
    log(
        f"{datetime.now().strftime('%Y-%m-%d %H:%M')} | "
        f"Run {run_number}/{TOTAL_RUNS} | {agent_id} | {model_id} | "
        f"Sharpe {avg_sharpe:.3f} | WR {avg_wr:.0f}% | N={total_trades} | "
        f"MaxDD {avg_dd:.1f}% | {elapsed}s"
    )

    return {**rollup, "run_id": run_id, "elapsed_s": elapsed, "skipped": False}


# ── Final analysis ────────────────────────────────────────────────────────────

def produce_results(all_results: list[dict], spy_return: float) -> None:
    docs_dir  = REPO_ROOT / "docs"
    docs_dir.mkdir(exist_ok=True)
    out_path  = docs_dir / "MODEL_SWEEP_V2_RESULTS_2026-04-20.md"
    done      = [r for r in all_results if not r.get("skipped")]

    by_agent: dict[str, list[dict]] = defaultdict(list)
    for r in done:
        by_agent[r["agent_id"]].append(r)

    lines = [
        "# Model Quality Sweep v2 — Results 2026-04-20",
        "",
        f"**Window**: {START_DATE} → {END_DATE} ({PERIOD_DAYS} days)",
        f"**Decision points**: Weekly (52/ticker) via W-FRI resample",
        f"**Tickers**: {', '.join(TICKERS)}",
        f"**Exit logic**: 5% stop-loss + 10% take-profit (VectorBT)",
        f"**SPY benchmark**: {spy_return:+.1f}%",
        f"**Runs completed**: {len(done)}/{TOTAL_RUNS}",
        "",
        "> **DO NOT AUTO-DEPLOY** — Steve reviews all recommendations.",
        "",
        "---", "",
        "## Full Matrix Results", "",
        "| # | Agent | Variant | Model | Sharpe | WR% | MaxDD% | N | Lat(ms) |",
        "|---|-------|---------|-------|--------|-----|--------|---|---------|",
    ]
    for i, r in enumerate(all_results, 1):
        if r.get("skipped"):
            lines.append(f"| {i} | {r['agent_id']} | {r['variant']} | `{r['model_id']}` | — | — | — | — | SKIP |")
        else:
            lines.append(
                f"| {i} | {r['agent_id']} | {r['variant']} | `{r['model_id']}` | "
                f"{r['avg_sharpe']:.3f} | {r['avg_wr']:.0f} | "
                f"{r['avg_dd']:.1f} | {r['total_trades']} | {r['avg_latency_ms']:.0f} |"
            )

    lines += ["", "---", "", "## Per-Agent Recommendations", ""]
    recs = []

    for agent_id, results in sorted(by_agent.items()):
        winner   = max(results, key=lambda x: x["avg_sharpe"])
        baseline = next((r for r in results if r["variant"] == "BASE"), None)
        sharpe_delta = winner["avg_sharpe"] - (baseline["avg_sharpe"] if baseline else 0)

        # Overfit flag: winner >2x Sharpe AND others mostly negative
        non_winner_sharpes = [r["avg_sharpe"] for r in results if r is not winner]
        avg_others = float(np.mean(non_winner_sharpes)) if non_winner_sharpes else 0.0
        overfit = (len(results) > 1
                   and winner["avg_sharpe"] > 2 * max(abs(avg_others), 0.1) + 0.3
                   and avg_others < 0.3)

        # Statistical significance flag: <30 total trades
        insig = winner["total_trades"] < 30

        if overfit:          confidence = "Low (overfit risk — winner 2x+ others)"
        elif insig:          confidence = "Low (<30 trades — insufficient sample)"
        elif len(results)==1: confidence = "Low (single run, no comparison)"
        elif sharpe_delta > 0.4: confidence = "High"
        elif sharpe_delta > 0.15: confidence = "Med"
        else:                confidence = "Low (delta <0.15)"

        lines += [
            f"### {agent_id}",
            "",
            "| Variant | Model | Sharpe | WR% | MaxDD% | N |",
            "|---------|-------|--------|-----|--------|---|",
        ]
        order = {"BASE": 0, "LIGHT": 1, "HEAVY": 2}
        for r in sorted(results, key=lambda x: order.get(x["variant"], 9)):
            star = " ⭐" if r is winner else ""
            lines.append(
                f"| {r['variant']}{star} | `{r['model_id']}` | "
                f"{r['avg_sharpe']:.3f} | {r['avg_wr']:.0f} | "
                f"{r['avg_dd']:.1f} | {r['total_trades']} |"
            )

        lines += [
            "",
            f"**Recommended**: `{winner['model_id']}` ({winner['variant']})",
            f"**Sharpe Δ vs BASE**: {sharpe_delta:+.3f}",
            f"**Confidence**: {confidence}",
        ]
        if overfit:
            lines.append("**⚠️ OVERFIT RISK** — validate on separate OOS window before deploying.")
        if insig:
            lines.append("**⚠️ INSUFFICIENT SAMPLE** — fewer than 30 trades; ranking may be noise.")
        lines.append("")

        recs.append({
            "agent_id":    agent_id,
            "current":     baseline["model_id"] if baseline else "?",
            "recommended": winner["model_id"],
            "variant":     winner["variant"],
            "sharpe_delta": round(sharpe_delta, 3),
            "confidence":  confidence,
        })

    lines += [
        "---", "",
        "## Recommendation Summary", "",
        "| Agent | Current | Recommended | Sharpe Δ | Confidence |",
        "|-------|---------|-------------|----------|------------|",
    ]
    for r in recs:
        arrow = "→ " if r["current"] != r["recommended"] else "= "
        lines.append(
            f"| {r['agent_id']} | `{r['current']}` | "
            f"{arrow}`{r['recommended']}` | {r['sharpe_delta']:+.3f} | {r['confidence']} |"
        )

    lines += [
        "", "---", "",
        "## Proposed config.py Edits (DO NOT AUTO-APPLY)", "",
        "```python",
        "# Review each change. Only apply after Steve's go-ahead.",
    ]
    for r in recs:
        if r["current"] != r["recommended"] and "High" in r["confidence"]:
            lines.append(
                f"# {r['agent_id']}: {r['current']} → {r['recommended']}  "
                f"(Δ{r['sharpe_delta']:+.3f} Sharpe, {r['confidence']})"
            )
    lines += ["```", "", f"_Generated {datetime.now().isoformat()}_"]

    out_path.write_text("\n".join(lines))
    log(f"Results → {out_path}")

    # Append to SESSION doc
    session_doc = REPO_ROOT / "SESSION_2026-04-20.md"
    if session_doc.exists():
        ts = datetime.now(AZ_TZ).strftime("%Y-%m-%d %H:%M AZ")
        session_doc.write_text(
            session_doc.read_text()
            + f"\n\n---\n\n## Model Sweep v2\n\n"
              f"Sweep v2 completed {ts}. "
              f"{len(done)}/{TOTAL_RUNS} runs done. "
              f"Results in `docs/MODEL_SWEEP_V2_RESULTS_2026-04-20.md`\n"
        )
        log("SESSION_2026-04-20.md updated")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    os.chdir(REPO_ROOT)

    log("=" * 72)
    log("  USS TradeMinds — Model Quality Sweep v2")
    log(f"  Window  : {START_DATE} → {END_DATE} ({PERIOD_DAYS}d)")
    log(f"  Tickers : {', '.join(TICKERS)}")
    log(f"  Runs    : {TOTAL_RUNS}")
    log(f"  Ollie   : {OLLIE_BASE}")
    log(f"  SL/TP   : {SL_STOP*100:.0f}% / {TP_STOP*100:.0f}%")
    log(f"  Guard   : auto-pause at {MARKET_GUARD_TIME} AZ")
    log("=" * 72)

    # ── Preflight ─────────────────────────────────────────────────────────────
    log("\n── PREFLIGHT ──")
    check_ollie()
    check_disk()
    if KILL_FILE.exists():
        log("ABORT — KILL_SWITCH already set before sweep started")
        sys.exit(1)
    if SWEEP_KILL_FILE.exists():
        log("NOTE — SWEEP_KILL_SWITCH already set; will pause before first run")
    log("  Ollie OK  |  Disk OK  |  Kill switches clear")

    # Verify all models
    log("\n── Model verification ──")
    all_models = sorted(set(m for _, _, _, m, _ in SWEEP_MATRIX))
    for m in all_models:
        ok = verify_model(m)
        log(f"  {'✓' if ok else '✗ MISSING'} {m}")

    # ── Price download (once, reused for all runs) ─────────────────────────
    log("\n── Downloading price data ──")
    prices    = download_prices()
    spy_return = get_spy_return(prices)
    log(f"  SPY benchmark ({START_DATE}→{END_DATE}): {spy_return:+.1f}%")

    if len(prices) < len(TICKERS) - 2:
        log(f"SWEEP HALTED — only {len(prices)}/{len(TICKERS)} tickers downloaded")
        sys.exit(1)

    # Pre-compute decision point counts for logging
    sample_idxs = get_weekly_decision_points(
        prices.get("SPY", pd.DataFrame()), START_DATE, END_DATE
    )
    log(f"  Decision points per ticker: {len(sample_idxs)} (weekly)")
    log(f"  LLM calls per run: {len(sample_idxs) * len(prices)}")
    log(f"  Total LLM calls: {len(sample_idxs) * len(prices) * TOTAL_RUNS:,}")

    # ── Sweep loop ────────────────────────────────────────────────────────────
    log("\n── EXECUTING SWEEP MATRIX ──")
    all_results: list[dict] = []
    sweep_start   = time.time()
    last_hourly   = time.time()
    completed     = 0
    halted        = False

    for run_number, (agent_id, agent_name, variant, model_id, notes) in \
            enumerate(SWEEP_MATRIX, 1):

        # Kill switch + time guard at every boundary
        if check_kills_and_guard():
            halted = True
            break

        # Hourly status anchor
        if time.time() - last_hourly >= 3600:
            remaining  = TOTAL_RUNS - completed
            elapsed_s  = time.time() - sweep_start
            rate       = elapsed_s / max(completed, 1)
            eta_s      = rate * remaining
            eta_az     = datetime.now(AZ_TZ) + timedelta(seconds=eta_s)
            try:
                cd_status = json.loads(
                    urlopen("http://localhost:8080/api/status", timeout=3).read()
                ).get("status", "?")
            except Exception:
                cd_status = "?"
            log(
                f"STATUS | Sweep running | {completed}/{TOTAL_RUNS} complete | "
                f"ETA {eta_az.strftime('%H:%M %Z')} | "
                f"Live trader: {cd_status}"
            )
            last_hourly = time.time()

        result = run_single(
            run_number, agent_id, agent_name, variant, model_id, notes,
            prices, spy_return,
        )
        if result:
            all_results.append(result)
            if not result.get("skipped"):
                completed += 1

    # ── Final analysis ────────────────────────────────────────────────────────
    elapsed_total = int(time.time() - sweep_start)
    skipped_count = sum(1 for r in all_results if r.get("skipped"))

    if halted:
        log(f"\nSWEEP HALTED at run {len(all_results)}/{TOTAL_RUNS} — "
            f"{completed} complete, {skipped_count} skipped. "
            f"Partial results saved to backtest_runs.")
        push_ntfy(
            "Model sweep v2 HALTED",
            f"Halted at {completed}/{TOTAL_RUNS} runs. "
            f"Check /tmp/model_sweep_v2.log for reason.",
            priority="high", tags="warning,sweep",
        )
    else:
        log(f"\n{'═'*72}")
        log(f"  SWEEP COMPLETE — {completed}/{TOTAL_RUNS} runs done, {skipped_count} skipped")
        log(f"  Total elapsed: {elapsed_total//3600}h {(elapsed_total%3600)//60}m")
        log(f"{'═'*72}")

        produce_results(all_results, spy_return)

        push_ntfy(
            "Model sweep v2 complete",
            f"{completed}/{TOTAL_RUNS} runs done in "
            f"{elapsed_total//3600}h{(elapsed_total%3600)//60}m.\n"
            f"See docs/MODEL_SWEEP_V2_RESULTS_2026-04-20.md",
            priority="default", tags="white_check_mark,sweep",
        )
        log(f"ntfy notification sent to {NTFY_TOPIC}")


if __name__ == "__main__":
    main()
