#!/usr/bin/env python3
# ============================================================
# OllieTrades Backtest Baseline Standard
# ALL backtests import this. Never run equity-only again.
# Sacred: never delete trader.db or arena.db
# ============================================================

import json
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

try:
    import requests as _requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

CACHE_DIR = Path(__file__).parent.parent / "data" / "backtest_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OLLAMA_URL = "http://localhost:11434/api/chat"

AGENT_MODELS = {
    "navigator":     "qwen3.5:9b",
    "ollama-plutus": "0xroyce/plutus",
    "ollama-qwen3":  "qwen3.5:9b",
    "ollama-coder":  "qwen3.5:9b",
    "neo-matrix":    "qwen3.5:9b",
    "ollama-llama":  "llama3.1",
    "ollie-auto":    "qwen3.5:9b",
}

AGENT_STRATEGIES = {
    "navigator":     ["momentum", "rsi_bounce", "swing_trade"],
    "ollama-plutus": ["defensive_rotation", "long_put", "inverse_etf"],
    "ollama-qwen3":  ["swing_trade", "ema_pullback"],
    "ollama-coder":  ["momentum", "long_equity", "mean_reversion"],
    "neo-matrix":    ["high_conviction_only"],
    "ollama-llama":  ["earnings_play", "event_driven"],
    "ollie-auto":    ["all"],
}

AGENT_INSTRUMENTS = {
    "ollama-plutus": ["GLD", "TLT", "XLU", "SH", "PSQ", "GDX"],
    "navigator":     None,   # full universe
    "ollama-qwen3":  None,
    "ollama-coder":  None,
    "neo-matrix":    None,
    "ollama-llama":  None,
}

VIX_THRESHOLDS = {
    "BULL_CALM": (0,  15),
    "NEUTRAL":   (15, 20),
    "CAUTIOUS":  (20, 25),
    "BEAR":      (25, 30),
    "CRISIS":    (30, 999),
}

MCCOY_ACTIVATES_ABOVE_VIX = 22
MCCOY_ONLY_ABOVE_VIX      = 35

REGIME_SIZE_MULTIPLIER = {
    "BULL_CALM": 1.25,
    "NEUTRAL":   1.00,
    "CAUTIOUS":  0.75,
    "BEAR":      0.50,
    "CRISIS":    0.25,
}

FG_REGIME = {
    "EXTREME_FEAR":  (0,  25),
    "FEAR":          (25, 45),
    "NEUTRAL":       (45, 55),
    "GREED":         (55, 75),
    "EXTREME_GREED": (75, 100),
}

_AGENT_PERSONAS = {
    "navigator":     "You are Chekov, a signal scanner. You trade convergence: only buy when 3+ indicators agree.",
    "ollama-plutus": "You are McCoy, a crisis doctor and finance expert trained on 394 books. You buy GLD/TLT/defensive assets when VIX is high.",
    "ollama-qwen3":  "You are Dax, a patient swing trader. You hold 3-7 days for full moves. Don't cut early.",
    "ollama-coder":  "You are Data, a rules-based scoring machine. You follow 11-signal composite logic precisely.",
    "neo-matrix":    "You are Neo. You only take high-conviction trades. Alpha must be >= 0.6. Sit out if unsure.",
    "ollama-llama":  "You are Uhura. You specialize in earnings plays and event-driven momentum.",
}

_AGENT_CONSTRAINTS = {
    "navigator":     "CONSTRAINT: Only respond BUY if at least 3 of the signals above agree.",
    "ollama-plutus": "CONSTRAINT: Only respond BUY if VIX > 22. Otherwise HOLD.",
    "ollama-qwen3":  "CONSTRAINT: Only respond BUY for swing setups (3-7 day holds), not day trades.",
    "ollama-coder":  "CONSTRAINT: Score all 11 signals mentally. Only BUY if composite score > 0.6.",
    "neo-matrix":    "CONSTRAINT: Only respond BUY if confidence >= 80. Otherwise HOLD.",
}


# ---------------------------------------------------------------------------
# Data fetch helpers (all cached to data/backtest_cache/)
# ---------------------------------------------------------------------------

def fetch_ohlcv(ticker: str, start: str, end: str) -> pd.DataFrame:
    """Standard OHLCV fetch with file caching."""
    cache_file = CACHE_DIR / f"{ticker}_{start}_{end}.parquet"
    if cache_file.exists():
        try:
            return pd.read_parquet(cache_file)
        except Exception:
            pass
    df = yf.download(ticker, start=start, end=end, auto_adjust=True, progress=False)
    if len(df) > 0:
        try:
            df.to_parquet(cache_file)
        except Exception:
            pass
    return df


def fetch_vix_history(start: str, end: str) -> dict[str, float]:
    """VIX as GEX proxy. Returns {YYYY-MM-DD: vix_close}. Cached."""
    cache_file = CACHE_DIR / f"VIX_{start}_{end}.json"
    if cache_file.exists():
        try:
            with open(cache_file) as f:
                return json.load(f)
        except Exception:
            pass
    try:
        df = yf.download("^VIX", start=start, end=end, auto_adjust=True, progress=False)
        result: dict[str, float] = {}
        # Flatten MultiIndex if present
        close = df["Close"] if "Close" in df.columns else df.iloc[:, 0]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        for idx, val in close.items():
            try:
                result[pd.Timestamp(idx).strftime("%Y-%m-%d")] = round(float(val), 2)
            except Exception:
                pass
        with open(cache_file, "w") as f:
            json.dump(result, f)
        return result
    except Exception as e:
        print(f"  [VIX] fetch failed: {e} — using neutral 20")
        return {}


def fetch_fear_greed_history(start: str, end: str) -> dict[str, float]:
    """CNN Fear & Greed historical scores. Returns {YYYY-MM-DD: score}. Cached."""
    cache_file = CACHE_DIR / f"FG_{start}_{end}.json"
    if cache_file.exists():
        try:
            with open(cache_file) as f:
                return json.load(f)
        except Exception:
            pass
    if not _HAS_REQUESTS:
        print("  [F&G] requests not available — using neutral 50")
        return {}
    try:
        url = f"https://production.dataviz.cnn.io/index/fearandgreed/graphdata/{start}"
        resp = _requests.get(url, timeout=15,
                             headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        data = resp.json()
        scores: dict[str, float] = {}
        for point in data.get("fear_and_greed_historical", {}).get("data", []):
            try:
                date_str = datetime.fromtimestamp(point["x"] / 1000).strftime("%Y-%m-%d")
                scores[date_str] = round(float(point["y"]), 1)
            except Exception:
                pass
        if scores:
            with open(cache_file, "w") as f:
                json.dump(scores, f)
            print(f"  [F&G] fetched {len(scores)} days from CNN ({min(scores)} → {max(scores)})")
        else:
            print("  [F&G] empty response — using neutral 50")
        return scores
    except Exception as e:
        print(f"  [F&G] fetch failed: {e} — using neutral 50")
        return {}


def fetch_spy_vs_200ma(start: str, end: str) -> dict[str, bool]:
    """Is SPY above its 200-day MA each day? Returns {YYYY-MM-DD: bool}. Cached."""
    cache_file = CACHE_DIR / f"SPY200_{start}_{end}.json"
    if cache_file.exists():
        try:
            with open(cache_file) as f:
                raw = json.load(f)
                return {k: bool(v) for k, v in raw.items()}
        except Exception:
            pass
    try:
        fetch_start = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=250)).strftime("%Y-%m-%d")
        df = yf.download("SPY", start=fetch_start, end=end,
                         auto_adjust=True, progress=False)
        close = df["Close"] if "Close" in df.columns else df.iloc[:, 0]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        ma200 = close.rolling(200).mean()
        result: dict[str, bool] = {}
        for idx in close.index:
            try:
                ds = pd.Timestamp(idx).strftime("%Y-%m-%d")
                if pd.notna(ma200.loc[idx]):
                    result[ds] = bool(float(close.loc[idx]) > float(ma200.loc[idx]))
            except Exception:
                pass
        with open(cache_file, "w") as f:
            json.dump({k: int(v) for k, v in result.items()}, f)
        return result
    except Exception as e:
        print(f"  [SPY200] fetch failed: {e} — assuming above 200MA")
        return {}


def _closest_prior(data_dict: dict, date_str: str, default: float = 20.0) -> float:
    """Look up by date; fall back to closest prior date if exact date missing."""
    if date_str in data_dict:
        return float(data_dict[date_str])
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        for i in range(1, 14):
            prev = (d - timedelta(days=i)).strftime("%Y-%m-%d")
            if prev in data_dict:
                return float(data_dict[prev])
    except Exception:
        pass
    return default


# ---------------------------------------------------------------------------
# Regime + sizing
# ---------------------------------------------------------------------------

def get_daily_regime(date_str: str, vix_data: dict, fg_data: dict,
                     spy_200_data: dict) -> str:
    """Combine VIX + F&G + SPY/200MA into a single regime label."""
    vix = _closest_prior(vix_data, date_str, default=20.0)
    fg  = float(fg_data.get(date_str, 50))
    above_200 = bool(spy_200_data.get(date_str, True))

    if vix > 30 or fg < 20:
        return "CRISIS"
    if vix > 25 or fg < 35:
        return "BEAR"
    if vix > 20 or fg < 45:
        return "CAUTIOUS"
    if above_200 and vix < 15:
        return "BULL_CALM"
    return "NEUTRAL"


def get_position_size_multiplier(regime: str, agent_id: str) -> float:
    """Regime-aware sizing. McCoy gets special crisis rules."""
    base = REGIME_SIZE_MULTIPLIER.get(regime, 1.0)
    if agent_id == "ollama-plutus":
        if regime in ("BULL_CALM", "NEUTRAL"):
            return 0.0   # McCoy sits out in calm markets
        return min(base * 1.5, 1.5)  # extra weight in crisis
    return base


def should_agent_trade_today(agent_id: str, regime: str, vix: float) -> bool:
    """Per-agent regime gates."""
    if agent_id == "ollama-plutus":
        return vix >= MCCOY_ACTIVATES_ABOVE_VIX
    if regime == "CRISIS" and agent_id != "ollama-plutus":
        return False   # only McCoy in crisis
    return True


# ---------------------------------------------------------------------------
# Ollama integration
# ---------------------------------------------------------------------------

def build_agent_prompt(agent_id: str, ticker: str, date_str: str,
                       ohlcv_context: dict, regime: str, vix: float,
                       fg_score: float, rsi: float,
                       sma20: float, sma50: float) -> str:
    """Standard prompt template: persona + regime + technicals."""
    strategies = AGENT_STRATEGIES.get(agent_id, ["momentum"])
    personality = _AGENT_PERSONAS.get(agent_id, "You are an AI trading agent.")
    price = ohlcv_context.get("close", ohlcv_context.get("price", 0))
    vs20 = f"{'ABOVE' if price > sma20 else 'BELOW'}" if sma20 else "UNKNOWN"
    vs50 = f"{'ABOVE' if price > sma50 else 'BELOW'}" if sma50 else "UNKNOWN"

    constraint = _AGENT_CONSTRAINTS.get(agent_id, "")
    constraint_line = f"\n{constraint}" if constraint else ""

    return (
        f"/no_think\n{personality}\n\n"
        f"TODAY: {date_str} | TICKER: {ticker} | PRICE: ${price:.2f}\n"
        f"REGIME: {regime} | VIX: {vix:.1f} | FEAR & GREED: {fg_score:.0f}/100\n"
        f"RSI(14): {rsi:.1f} | vs SMA20: {vs20} | vs SMA50: {vs50}\n"
        f"Your strategies: {', '.join(strategies)}{constraint_line}\n\n"
        f"Respond with ONE word on line 1: BUY, SELL, or HOLD\n"
        f"Then: CONFIDENCE: [1-10]\n"
        f"Then: REASON: [one sentence max]"
    )


def query_ollama(agent_id: str, context_prompt: str,
                 timeout: int = 90) -> tuple[str, int, str]:
    """Real Ollama query. Returns (signal, confidence, reason).
    Falls back to ('HOLD', 5, reason) on timeout/error."""
    if not _HAS_REQUESTS:
        return "HOLD", 5, "requests unavailable"
    model = AGENT_MODELS.get(agent_id, "qwen3.5:9b")
    try:
        resp = _requests.post(OLLAMA_URL, json={
            "model": model,
            "messages": [{"role": "user", "content": context_prompt}],
            "stream": False,
            "think": False,
            "options": {"temperature": 0.1, "num_predict": 120, "num_ctx": 512},
        }, timeout=timeout)
        resp.raise_for_status()
        text = resp.json().get("message", {}).get("content", "") or ""
        # strip <think> blocks
        text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<think>.*", "", text, flags=re.DOTALL | re.IGNORECASE)
        text = text.strip()
        upper = text.upper()

        # parse signal
        m = re.search(r"SIGNAL:\s*(BUY|SELL|HOLD)", upper)
        if m:
            signal = m.group(1)
        else:
            signal = "HOLD"
            for word in re.sub(r"[^\w\s]", " ", upper).split()[:5]:
                if word in ("BUY", "SELL", "HOLD"):
                    signal = word
                    break

        # parse confidence
        mc = re.search(r"CONFIDENCE:\s*(\d+)", upper)
        conf = min(max(int(mc.group(1)), 1), 10) if mc else 5

        # parse reason
        mr = re.search(r"REASON:\s*(.+?)(?:\n|$)", text, re.IGNORECASE)
        reason = mr.group(1).strip()[:100] if mr else text[:80]

        return signal, conf, reason
    except Exception as e:
        return "HOLD", 0, f"error: {e!s:.60}"


# ---------------------------------------------------------------------------
# Summary output
# ---------------------------------------------------------------------------

def build_results_summary(agent_results: list[dict], spy_return: float,
                           period_days: int, regime_counts: dict,
                           fg_avg: float, vix_avg: float) -> None:
    """Standard summary block printed at end of every backtest."""
    print("\n" + "=" * 70)
    print(f"BASELINE STANDARD RESULTS  |  {period_days}d  |  SPY: {spy_return:+.2f}%")
    print(f"Regime: {regime_counts}  |  Avg VIX: {vix_avg:.1f}  |  Avg F&G: {fg_avg:.0f}")
    print("=" * 70)
    print(f"{'Agent':<20} {'Return':>8} {'Sharpe':>8} "
          f"{'WR':>6} {'Trades':>7} {'vs SPY':>8} {'Regime':>10}")
    print("-" * 70)
    for r in sorted(agent_results, key=lambda x: x.get("sharpe", 0), reverse=True):
        ret = r.get("return", 0)
        print(f"{r.get('agent','?'):<20} {ret:>+7.2f}% {r.get('sharpe',0):>8.3f} "
              f"{r.get('win_rate',0):>5.1f}% {r.get('trades',0):>7} "
              f"{ret - spy_return:>+7.2f}% {r.get('best_regime','?'):>10}")
    if agent_results:
        fleet_avg = sum(r.get("return", 0) for r in agent_results) / len(agent_results)
        print("-" * 70)
        print(f"{'FLEET AVG':<20} {fleet_avg:>+7.2f}%  {'vs SPY':>16} "
              f"{fleet_avg - spy_return:>+7.2f}%")
    print("=" * 70)
