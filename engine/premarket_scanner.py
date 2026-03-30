"""Pre-Market Scanner — detect gaps, analyze with AI, and identify DayBlade candidates."""
from __future__ import annotations
import json
import re
import time
import requests
from datetime import datetime
from pathlib import Path
from rich.console import Console

import config
from engine.openai_text import DEFAULT_CODEX_MODEL, generate_text

console = Console()
DATA_FILE = Path("data/premarket_gaps.json")
DAYBLADE_TICKERS = ["SPY", "QQQ", "NVDA", "TSLA", "AAPL"]


def scan_premarket_gaps() -> list:
    """Scan WATCH_STOCKS for pre-market gaps vs previous close.

    Uses yfinance to get the previous close and current pre/post market price.
    Filters for |gap| > 2%.

    Returns list of dicts with symbol, prev_close, premarket_price, gap_pct, direction.
    """
    try:
        import yfinance as yf
    except ImportError:
        return [{"error": "yfinance not installed"}]

    gaps = []
    for symbol in config.WATCH_STOCKS:
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info or {}

            prev_close = info.get("previousClose") or info.get("regularMarketPreviousClose")
            # Try pre-market price first, then post-market, then current
            premarket_price = (
                info.get("preMarketPrice")
                or info.get("postMarketPrice")
                or info.get("regularMarketPrice")
            )

            if not prev_close or not premarket_price or prev_close <= 0:
                continue

            gap_pct = round(((premarket_price - prev_close) / prev_close) * 100, 2)

            if abs(gap_pct) < 2.0:
                continue

            direction = "gap_up" if gap_pct > 0 else "gap_down"
            gaps.append({
                "symbol": symbol,
                "prev_close": round(prev_close, 2),
                "premarket_price": round(premarket_price, 2),
                "gap_pct": gap_pct,
                "direction": direction,
                "scanned_at": datetime.now().isoformat(),
            })

        except Exception as e:
            console.log(f"[red]Premarket scan error for {symbol}: {e}")

    # Sort by absolute gap size
    gaps.sort(key=lambda x: abs(x["gap_pct"]), reverse=True)

    # Save to disk
    _save_gaps(gaps)
    return gaps


def analyze_gaps_with_ai() -> list:
    """Load latest gaps and send to ALL 4 AI models for analysis.

    Each model provides: catalyst, setup type (gap-and-go vs gap-and-fade),
    key levels, recommendation, and 0DTE candidacy assessment.

    Returns list of gap entries enriched with AI analyses from all models.
    """
    gaps = _load_gaps()
    if not gaps:
        return []

    # Build the prompt
    gap_summary = "\n".join(
        f"  {g['symbol']}: {g['gap_pct']:+.2f}% gap ({g['direction']}), "
        f"prev close ${g['prev_close']}, premarket ${g['premarket_price']}"
        for g in gaps
    )

    prompt = (
        f"You are a pre-market analyst. Analyze these gap stocks:\n\n"
        f"{gap_summary}\n\n"
        f"For EACH stock, provide:\n"
        f"1. Likely catalyst (earnings, news, sector rotation, etc)\n"
        f"2. Setup type: 'gap-and-go' (momentum continues) or 'gap-and-fade' (reversal expected)\n"
        f"3. Key levels to watch (support and resistance)\n"
        f"4. Trade recommendation (entry, stop, target)\n"
        f"5. Is this a good 0DTE options candidate? (yes/no with reason)\n\n"
        f"Respond ONLY as JSON array: ["
        f'{{"symbol": "XXX", "catalyst": "string", "setup": "gap-and-go|gap-and-fade", '
        f'"key_levels": {{"support": price, "resistance": price}}, '
        f'"recommendation": "string", "dte_0_candidate": true|false, "dte_0_reason": "string"}}]'
    )

    models = ["codex", "gemini", "grok", "ollama"]
    all_results = []

    for model in models:
        try:
            raw = _call_model(model, prompt)
            if not raw:
                continue
            parsed = _parse_json_response(raw)
            if parsed and isinstance(parsed, list):
                for entry in parsed:
                    entry["model"] = model
                    entry["analyzed_at"] = datetime.now().isoformat()
                all_results.extend(parsed)
            elif parsed and isinstance(parsed, dict):
                parsed["model"] = model
                parsed["analyzed_at"] = datetime.now().isoformat()
                all_results.append(parsed)
        except Exception as e:
            console.log(f"[red]Gap AI analysis error ({model}): {e}")

    return all_results


def get_dayblade_gap_candidates() -> list:
    """Filter gaps for DayBlade tickers (SPY, QQQ, NVDA, TSLA, AAPL) with |gap| > 3%.

    Returns list of gap entries suitable for DayBlade 0DTE strategies.
    """
    gaps = _load_gaps()
    candidates = [
        g for g in gaps
        if g["symbol"] in DAYBLADE_TICKERS and abs(g["gap_pct"]) >= 3.0
    ]
    # Enrich with DayBlade-specific fields
    for c in candidates:
        c["dayblade_eligible"] = True
        c["suggested_strategy"] = (
            "momentum_calls" if c["gap_pct"] > 0 else "momentum_puts"
        )
        c["urgency"] = "high" if abs(c["gap_pct"]) >= 5.0 else "medium"
    return candidates


# ── AI Model Caller ──────────────────────────────────────────────────

def _call_model(model: str, prompt: str) -> str:
    """Send prompt to the specified AI model and return raw text."""
    try:
        if model in ("codex", "claude"):
            return generate_text(
                prompt,
                model=DEFAULT_CODEX_MODEL,
                api_key=config.OPENAI_API_KEY,
                max_output_tokens=1000,
                reasoning_effort="medium",
            )
        elif model == "gemini":
            resp = requests.post(
                f"{config.OLLAMA_URL}/api/generate",
                json={"model": "gemma3:4b", "prompt": prompt, "stream": False},
                timeout=90,
            )
            resp.raise_for_status()
            return resp.json().get("response", "")

        elif model == "grok":
            resp = requests.post(
                "https://api.x.ai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {config.GROK_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "grok-3",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 1000,
                },
                timeout=90,
            )
            resp.raise_for_status()
            data = resp.json()
            choices = data.get("choices", [])
            return choices[0].get("message", {}).get("content", "") if choices else ""

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

        return ""

    except Exception as e:
        console.log(f"[red]Model call error ({model}): {e}")
        return ""


def _parse_json_response(text: str):
    """Extract JSON object or array from AI response."""
    # Try array first
    match = re.search(r"\[.*\]", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    # Try object
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    return None


# ── Persistence ──────────────────────────────────────────────────────

def _load_gaps() -> list:
    """Load latest gaps from disk."""
    try:
        if DATA_FILE.exists():
            return json.loads(DATA_FILE.read_text())
    except Exception as e:
        console.log(f"[red]Error loading premarket gaps: {e}")
    return []


def _save_gaps(gaps: list):
    """Save gaps to disk."""
    try:
        DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        DATA_FILE.write_text(json.dumps(gaps, indent=2))
    except Exception as e:
        console.log(f"[red]Error saving premarket gaps: {e}")


SECTOR_ETFS = {
    "Technology": {"etf": "XLK", "holdings": ["AAPL", "MSFT", "NVDA", "AVGO", "CRM"]},
    "Financials": {"etf": "XLF", "holdings": ["BRK-B", "JPM", "V", "MA", "BAC"]},
    "Healthcare": {"etf": "XLV", "holdings": ["LLY", "UNH", "JNJ", "ABBV", "MRK"]},
    "Energy": {"etf": "XLE", "holdings": ["XOM", "CVX", "COP", "SLB", "EOG"]},
    "Consumer Disc": {"etf": "XLY", "holdings": ["AMZN", "TSLA", "HD", "MCD", "NKE"]},
    "Consumer Staples": {"etf": "XLP", "holdings": ["PG", "COST", "KO", "PEP", "WMT"]},
    "Industrials": {"etf": "XLI", "holdings": ["GE", "CAT", "UNP", "HON", "BA"]},
    "Materials": {"etf": "XLB", "holdings": ["LIN", "APD", "SHW", "FCX", "NEM"]},
    "Real Estate": {"etf": "XLRE", "holdings": ["PLD", "AMT", "EQIX", "SPG", "O"]},
    "Utilities": {"etf": "XLU", "holdings": ["NEE", "SO", "DUK", "CEG", "SRE"]},
    "Communication": {"etf": "XLC", "holdings": ["META", "GOOGL", "NFLX", "DIS", "CMCSA"]},
}

# Defense/Aero is NOT in SECTOR_ETFS because Finviz doesn't cover it.
# It is always fetched separately from Yahoo and appended as the 12th sector.
_DEFENSE_ETF = "ITA"
_DEFENSE_HOLDINGS = ["LMT", "RTX", "NOC", "GD", "LHX", "BA", "HII", "LDOS", "BAH"]


_SECTOR_CACHE_FILE = "data/sector_cache.json"
_sector_disk_cache: dict = {}  # {sectors: [...], ts: float}

# All 12 expected sector names — used to guarantee we always return all 12
_ALL_SECTOR_NAMES = list(SECTOR_ETFS.keys()) + ["Defense/Aero"]


def _load_sector_disk_cache():
    """Load cached sector data from disk (survives restarts)."""
    global _sector_disk_cache
    try:
        import json as _json
        with open(_SECTOR_CACHE_FILE, "r") as f:
            content = f.read()
        if content.strip():
            _sector_disk_cache = _json.loads(content)
    except Exception:
        pass


def _save_sector_disk_cache(data: list):
    """Persist sector data to disk using atomic write (temp file → rename).

    Atomic write prevents 0-byte corruption if the process dies during write.
    """
    import json as _json
    import os
    import tempfile
    try:
        payload = {"sectors": data, "ts": time.time()}
        # Serialize first to catch any non-serializable values before touching the file
        serialized = _json.dumps(payload)
        # Write to temp file in same directory, then rename (atomic on POSIX)
        cache_dir = os.path.dirname(os.path.abspath(_SECTOR_CACHE_FILE))
        fd, tmp_path = tempfile.mkstemp(dir=cache_dir, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                f.write(serialized)
            os.replace(tmp_path, _SECTOR_CACHE_FILE)  # atomic rename
        except Exception:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
            raise
        _sector_disk_cache["sectors"] = data
        _sector_disk_cache["ts"] = payload["ts"]
    except Exception:
        pass


def _stale_sector(name: str) -> dict:
    """Return stale sector data from disk cache, or a zero-value placeholder."""
    for s in (_sector_disk_cache.get("sectors") or []):
        if s.get("sector") == name:
            return s
    return None


def _stale_holding(sector_name: str, symbol: str) -> dict:
    """Return stale holding from disk cache for a specific sector+symbol."""
    stale_sector = _stale_sector(sector_name)
    if stale_sector:
        for h in stale_sector.get("holdings", []):
            if h.get("symbol") == symbol:
                return h
    return None


# Load disk cache at import time so first request is instant
_load_sector_disk_cache()


def get_sector_heatmap() -> list:
    """Get S&P 500 sector ETF heat map with all holdings — ALWAYS returns all 12 sectors.

    Performance source priority per sector:
      1. Finviz (11 standard sectors, single fast request)
      2. Yahoo Finance bulk batch quote (all symbols in ONE request)
      3. Disk cache (stale but non-zero values from last successful fetch)
      4. Zero placeholder (last resort — sector still appears, just shows 0.00%)

    Defense/Aero is always the 12th sector from Yahoo ITA data (Finviz doesn't cover it).
    The full 12-sector result is persisted atomically to disk after every call.
    """
    from engine.market_data import get_bulk_prices

    # --- Step 1: Try Finviz for broad sector % (fast, single request) ---
    finviz_perf: dict = {}
    try:
        from engine.finviz_sectors import get_finviz_sector_performance
        finviz_perf = get_finviz_sector_performance()
    except Exception:
        pass

    # --- Step 2: Bulk-fetch ALL symbols (SPY + 11 sector ETFs + all holdings + Defense/Aero) ---
    all_symbols = ["SPY"]  # SPY first so we can compute "beating SPY" in the frontend
    for info in SECTOR_ETFS.values():
        if info["etf"] not in all_symbols:
            all_symbols.append(info["etf"])
        for sym in info["holdings"]:
            if sym not in all_symbols:
                all_symbols.append(sym)
    for sym in [_DEFENSE_ETF] + _DEFENSE_HOLDINGS:
        if sym not in all_symbols:
            all_symbols.append(sym)

    prices = get_bulk_prices(all_symbols, timeout=5)
    has_fresh_prices = bool(prices)

    # SPY change_pct for "beating the market" context
    spy_change_pct = round(prices["SPY"]["change_pct"], 2) if "SPY" in prices else None

    # --- Step 3: Build results for the 11 Finviz-covered sectors ---
    # Always iterates all 11 — stale disk cache fills any gaps
    results = []
    for sector, info in SECTOR_ETFS.items():
        stale = _stale_sector(sector)

        # Sector-level %: Finviz → Yahoo ETF → stale disk → 0
        if sector in finviz_perf:
            change_pct = finviz_perf[sector]
            source = "finviz"
        elif has_fresh_prices and info["etf"] in prices:
            change_pct = round(prices[info["etf"]].get("change_pct", 0), 2)
            source = "yahoo"
        elif stale:
            change_pct = stale.get("change_pct", 0)
            source = "stale"
        else:
            change_pct = 0.0
            source = "stale"

        # Holdings: Yahoo price → stale disk → zero placeholder
        holdings = []
        for sym in info["holdings"]:
            if has_fresh_prices and sym in prices:
                hdata = prices[sym]
                holdings.append({
                    "symbol": sym,
                    "price": round(float(hdata.get("price", 0)), 2),
                    "change_pct": round(float(hdata.get("change_pct", 0)), 2),
                    "volume": int(hdata.get("volume", 0)),
                })
            else:
                stale_h = _stale_holding(sector, sym)
                holdings.append(stale_h or {"symbol": sym, "price": 0.0, "change_pct": 0.0, "volume": 0})
        holdings.sort(key=lambda x: abs(x["change_pct"]), reverse=True)

        results.append({
            "sector": sector,
            "etf": info["etf"],
            "change_pct": change_pct,
            "source": source,
            "holdings": holdings,
        })

    results.sort(key=lambda x: abs(x["change_pct"]), reverse=True)

    # --- Step 4: Always append Defense/Aero (Yahoo only, never Finviz) ---
    stale_defense = _stale_sector("Defense/Aero")

    ita_data = prices.get(_DEFENSE_ETF) if has_fresh_prices else None
    if ita_data:
        ita_change = round(float(ita_data.get("change_pct", 0)), 2)
        defense_source = "yahoo"
    elif stale_defense:
        ita_change = stale_defense.get("change_pct", 0)
        defense_source = "stale"
    else:
        ita_change = 0.0
        defense_source = "stale"

    defense_holdings = []
    for sym in _DEFENSE_HOLDINGS:
        if has_fresh_prices and sym in prices:
            hdata = prices[sym]
            defense_holdings.append({
                "symbol": sym,
                "price": round(float(hdata.get("price", 0)), 2),
                "change_pct": round(float(hdata.get("change_pct", 0)), 2),
                "volume": int(hdata.get("volume", 0)),
            })
        else:
            stale_h = _stale_holding("Defense/Aero", sym)
            defense_holdings.append(stale_h or {"symbol": sym, "price": 0.0, "change_pct": 0.0, "volume": 0})
    defense_holdings.sort(key=lambda x: abs(x["change_pct"]), reverse=True)

    results.append({
        "sector": "Defense/Aero",
        "etf": _DEFENSE_ETF,
        "change_pct": ita_change,
        "source": defense_source,
        "holdings": defense_holdings,
    })

    # Include SPY change_pct in a metadata entry so frontend can compute "beating SPY"
    if spy_change_pct is not None:
        for r in results:
            r["spy_change_pct"] = spy_change_pct

    # Persist atomically — never leaves a 0-byte file
    _save_sector_disk_cache(results)
    return results
