"""Chart Analyzer — AI-powered technical chart analysis with multi-model support."""
from __future__ import annotations
import json
import re
import requests
import time
from datetime import datetime
from pathlib import Path
from rich.console import Console

import config
from engine.openai_text import DEFAULT_CODEX_MODEL, generate_text

console = Console()
DATA_FILE = Path("data/chart_analyses.json")


def analyze_chart(symbol: str, model: str) -> dict:
    """Fetch 60 days of OHLCV data, compute indicators, and send to an AI model for chart analysis.

    Args:
        symbol: Stock ticker symbol.
        model: One of "codex", "gemini", "grok", "ollama".

    Returns:
        Dict with support, resistance, patterns, trend, volume_assessment,
        recommendation, confidence, and metadata.
    """
    try:
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="60d", interval="1d")
        if hist.empty or len(hist) < 30:
            return {"error": f"Insufficient data for {symbol} (got {len(hist)} bars)"}

        close = hist["Close"].values
        high = hist["High"].values
        low = hist["Low"].values
        volume = hist["Volume"].values
        opens = hist["Open"].values

        # --- RSI(14) ---
        rsi = _calc_rsi(close, 14)

        # --- MACD(12,26,9) ---
        ema12 = _ema(close, 12)
        ema26 = _ema(close, 26)
        macd_line = ema12 - ema26
        signal_line = _ema(macd_line, 9)

        # --- SMA(20) ---
        sma20 = sum(close[-20:]) / 20 if len(close) >= 20 else sum(close) / len(close)

        # Build data table from last 30 candles
        rows = []
        start_idx = max(0, len(close) - 30)
        dates = hist.index[start_idx:]
        for i, dt in enumerate(dates):
            idx = start_idx + i
            rows.append(
                f"{dt.strftime('%Y-%m-%d')}  "
                f"O:{opens[idx]:.2f}  H:{high[idx]:.2f}  L:{low[idx]:.2f}  "
                f"C:{close[idx]:.2f}  V:{int(volume[idx])}"
            )
        data_table = "\n".join(rows)

        latest_rsi = round(rsi, 2)
        latest_macd = round(float(macd_line[-1]), 4)
        latest_signal = round(float(signal_line[-1]), 4)
        latest_sma20 = round(sma20, 2)

        prompt = (
            f"Analyze this chart data for {symbol}. Here is the recent price data:\n\n"
            f"{data_table}\n\n"
            f"Technical indicators (latest values): RSI(14): {latest_rsi}, "
            f"MACD: {latest_macd}, Signal: {latest_signal}, SMA(20): {latest_sma20}\n\n"
            f"Identify:\n"
            f"1. Key support levels (up to 3 prices)\n"
            f"2. Key resistance levels (up to 3 prices)\n"
            f"3. Active chart patterns (head & shoulders, double bottom, wedge, flag, triangle, etc)\n"
            f"4. Trend direction (bullish, bearish, or neutral)\n"
            f"5. Volume profile assessment\n"
            f"6. One-sentence trade recommendation\n\n"
            f'Respond ONLY as JSON: {{"support": [price1, price2], "resistance": [price1, price2], '
            f'"patterns": ["pattern name"], "trend": "bullish|bearish|neutral", '
            f'"volume_assessment": "string", "recommendation": "string", "confidence": 1-100}}'
        )

        # Send to selected model
        raw_response = _call_model(model, prompt)
        if not raw_response:
            return {"error": f"No response from {model}"}

        # Parse JSON from response
        parsed = _parse_json_response(raw_response)
        if not parsed:
            return {"error": "Failed to parse AI response as JSON", "raw": raw_response[:500]}

        result = {
            "symbol": symbol,
            "model": model,
            "support": parsed.get("support", []),
            "resistance": parsed.get("resistance", []),
            "patterns": parsed.get("patterns", []),
            "trend": parsed.get("trend", "neutral"),
            "volume_assessment": parsed.get("volume_assessment", ""),
            "recommendation": parsed.get("recommendation", ""),
            "confidence": parsed.get("confidence", 50),
            "indicators": {
                "rsi": latest_rsi,
                "macd": latest_macd,
                "signal": latest_signal,
                "sma20": latest_sma20,
            },
            "current_price": round(float(close[-1]), 2),
            "analyzed_at": datetime.now().isoformat(),
        }

        # Auto-save
        save_analysis(result)
        return result

    except Exception as e:
        console.log(f"[red]Chart analysis error for {symbol}: {e}")
        return {"error": str(e)}


def _call_model(model: str, prompt: str) -> str:
    """Send prompt to the specified AI model and return raw text."""
    try:
        if model in ("codex", "claude"):
            return generate_text(
                prompt,
                model=DEFAULT_CODEX_MODEL,
                api_key=config.OPENAI_API_KEY,
                max_output_tokens=800,
                reasoning_effort="medium",
            )
        elif model == "gemini":
            resp = requests.post(
                f"{config.OLLAMA_URL}/api/generate",
                json={"model": "qwen3:14b", "prompt": prompt, "stream": False},
                timeout=90,
            )
            resp.raise_for_status()
            return resp.json().get("response", "")

        elif model == "grok":
            # Routed to local deepseek-r1:14b — eliminates xAI API cost
            resp = requests.post(
                config.OLLAMA_URL + "/api/generate",
                json={"model": "deepseek-r1:14b", "prompt": prompt, "stream": False},
                timeout=90,
            )
            resp.raise_for_status()
            return resp.json().get("response", "")

        elif model == "ollama":
            resp = requests.post(
                config.OLLAMA_URL + "/api/generate",
                json={
                    "model": config.OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False,
                },
                timeout=120,
            )
            resp.raise_for_status()
            return resp.json().get("response", "")

        else:
            return ""

    except Exception as e:
        console.log(f"[red]Model call error ({model}): {e}")
        return ""


def _parse_json_response(text: str) -> dict | None:
    """Extract JSON object from AI response using regex."""
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    return None


def _calc_rsi(closes, period: int = 14) -> float:
    """Calculate RSI manually."""
    if len(closes) < period + 1:
        return 50.0
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    recent = deltas[-(period):]
    gains = [d for d in recent if d > 0]
    losses = [-d for d in recent if d < 0]
    avg_gain = sum(gains) / period if gains else 0
    avg_loss = sum(losses) / period if losses else 0.001
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _ema(data, period: int):
    """Calculate EMA, returning a numpy array."""
    import numpy as np
    if len(data) < period:
        return np.array(data, dtype=float)
    alpha = 2 / (period + 1)
    result = np.zeros(len(data))
    result[period - 1] = sum(data[:period]) / period
    for i in range(period, len(data)):
        result[i] = alpha * float(data[i]) + (1 - alpha) * result[i - 1]
    return result


# ── Persistence ──────────────────────────────────────────────────────

def load_analyses() -> list:
    """Load all chart analyses from disk."""
    try:
        if DATA_FILE.exists():
            return json.loads(DATA_FILE.read_text())
    except Exception as e:
        console.log(f"[red]Error loading chart analyses: {e}")
    return []


def save_analysis(entry: dict):
    """Append a chart analysis entry to the JSON file."""
    try:
        DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        analyses = load_analyses()
        analyses.append(entry)
        # Keep last 500 entries
        if len(analyses) > 500:
            analyses = analyses[-500:]
        DATA_FILE.write_text(json.dumps(analyses, indent=2))
    except Exception as e:
        console.log(f"[red]Error saving chart analysis: {e}")


def get_analyses_for_symbol(symbol: str) -> list:
    """Get all chart analyses for a specific symbol, newest first."""
    analyses = load_analyses()
    return sorted(
        [a for a in analyses if a.get("symbol", "").upper() == symbol.upper()],
        key=lambda x: x.get("analyzed_at", ""),
        reverse=True,
    )


def get_comparison(symbol: str) -> dict:
    """Compare the latest analysis from each model for a symbol.

    Returns dict with model names as keys and their latest analysis as values,
    plus a consensus section.
    """
    analyses = get_analyses_for_symbol(symbol)
    if not analyses:
        return {"symbol": symbol, "models": {}, "consensus": None}

    # Get latest from each model
    latest_by_model = {}
    for a in analyses:
        model = a.get("model", "unknown")
        if model not in latest_by_model:
            latest_by_model[model] = a

    # Build consensus
    trends = [a.get("trend", "neutral") for a in latest_by_model.values()]
    confidences = [a.get("confidence", 50) for a in latest_by_model.values()]
    all_support = []
    all_resistance = []
    for a in latest_by_model.values():
        all_support.extend(a.get("support", []))
        all_resistance.extend(a.get("resistance", []))

    trend_counts = {}
    for t in trends:
        trend_counts[t] = trend_counts.get(t, 0) + 1
    consensus_trend = max(trend_counts, key=trend_counts.get) if trend_counts else "neutral"

    consensus = {
        "trend": consensus_trend,
        "avg_confidence": round(sum(confidences) / len(confidences), 1) if confidences else 0,
        "model_count": len(latest_by_model),
        "agreement": trend_counts.get(consensus_trend, 0) == len(latest_by_model),
        "support_levels": sorted(set(s for s in all_support if isinstance(s, (int, float)))),
        "resistance_levels": sorted(set(r for r in all_resistance if isinstance(r, (int, float)))),
    }

    return {
        "symbol": symbol,
        "models": latest_by_model,
        "consensus": consensus,
    }
