"""Bull/Bear Analysis — ask AI for strongest bull and bear case for each position."""
from __future__ import annotations
import requests
import json
import sqlite3
import time as _time
from datetime import datetime
from pathlib import Path
from rich.console import Console

import config
from engine.openai_text import DEFAULT_CODEX_MINI_MODEL, generate_text

console = Console()
DB = "data/trader.db"
CACHE_FILE = Path("data/bull_bear_cache.json")
_cache = {}
_CACHE_TTL = 3600  # 1 hour


def _load_cache():
    global _cache
    if CACHE_FILE.exists():
        try:
            _cache = json.loads(CACHE_FILE.read_text())
        except Exception:
            pass


def _save_cache():
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    CACHE_FILE.write_text(json.dumps(_cache, indent=2))


def _call_ai(prompt: str, model: str = "codex") -> str:
    """Call an AI model with a prompt."""
    try:
        if model in ("codex", "claude") and config.OPENAI_API_KEY:
            return generate_text(
                prompt,
                model=DEFAULT_CODEX_MINI_MODEL,
                api_key=config.OPENAI_API_KEY,
                max_output_tokens=300,
                reasoning_effort="medium",
            )
        elif model == "gemini":
            r = requests.post(
                f"{config.OLLAMA_URL}/api/generate",
                json={"model": "qwen3.5:9b", "prompt": prompt, "stream": False},
                timeout=30,
            )
            return r.json().get("response", "")
        elif model == "ollama":
            r = requests.post(
                f"{config.OLLAMA_URL}/api/generate",
                json={"model": config.OLLAMA_MODEL, "prompt": prompt, "stream": False},
                timeout=30,
            )
            return r.json().get("response", "")
    except Exception as e:
        console.log(f"[red]Bull/Bear AI error ({model}): {e}")
    return ""


def analyze_bull_bear(symbol: str, model: str = "codex") -> dict:
    """Get bull and bear case analysis for a symbol."""
    _load_cache()

    cache_key = f"{symbol}:{model}"
    if cache_key in _cache:
        entry = _cache[cache_key]
        if _time.time() - entry.get("timestamp", 0) < _CACHE_TTL:
            return entry

    # Get current price context
    from engine.market_data import get_stock_price
    price_data = get_stock_price(symbol)
    price_str = f"${price_data['price']}" if "error" not in price_data else ""

    prompt = f"""Give the strongest bull case and strongest bear case for {symbol} {price_str} in exactly this format:
BULL: [2 sentences max]
BEAR: [2 sentences max]
No other text."""

    response = _call_ai(prompt, model)

    # Parse bull/bear from response
    bull = ""
    bear = ""
    lines = response.strip().split("\n")
    for line in lines:
        line = line.strip()
        if line.upper().startswith("BULL:"):
            bull = line[5:].strip()
        elif line.upper().startswith("BEAR:"):
            bear = line[5:].strip()

    if not bull and not bear:
        # Fallback: split response in half
        mid = len(response) // 2
        bull = response[:mid].strip()
        bear = response[mid:].strip()

    result = {
        "symbol": symbol,
        "model": model,
        "bull_case": bull,
        "bear_case": bear,
        "timestamp": _time.time(),
        "price": price_data.get("price") if "error" not in price_data else None,
    }

    _cache[cache_key] = result
    _save_cache()
    return result


def analyze_all_positions(model: str = "codex") -> list:
    """Get bull/bear analysis for all held positions."""
    try:
        conn = sqlite3.connect(DB, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT DISTINCT symbol FROM positions WHERE qty > 0").fetchall()
        conn.close()
        symbols = [r["symbol"] for r in rows]
    except Exception:
        symbols = []

    if not symbols:
        # Fallback to watchlist subset
        symbols = config.WATCH_STOCKS[:5]

    results = []
    for sym in symbols:
        analysis = analyze_bull_bear(sym, model)
        results.append(analysis)
    return results
