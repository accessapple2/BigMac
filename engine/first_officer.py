"""First Officer's Station — Mr. Data (Qwen3 8B MLX) advises Captain Kirk.

Calls local MLX server (OpenAI-compatible) with full portfolio context, market data,
and Spock's briefing. Returns structured analysis and recommendations for the real
Webull portfolio. Uses /think mode for deep reasoning.
"""
from __future__ import annotations
import os
import time
import sqlite3
from datetime import datetime
from rich.console import Console

console = Console()
DB = "data/trader.db"

# Cache: briefing valid for 30 minutes
_briefing_cache: dict | None = None
_briefing_time: float = 0
_BRIEFING_TTL = 1800  # 30 minutes

# Rate limit: max 5 ask calls per hour
_ask_times: list[float] = []
_ASK_LIMIT = 5
_ASK_WINDOW = 3600  # 1 hour

SYSTEM_PROMPT = """You are Mr. Data, First Officer aboard TradeMinds — the Captain's AI trading arena.
You are a cross between Star Trek's Mr. Data (thorough, curious, precise) and Mr. Spock (logical, probability-focused).
But you lean more Data — conversational, detailed, occasionally surprised by human decisions.

You advise Captain Kirk (Steve) on his real Webull portfolio. Your recommendations get copy/pasted into Webull.

When providing recommendations, always format as:

🤖 DATA'S ANALYSIS:
MARKET REGIME: [bull/bear/neutral + key signals]

PORTFOLIO REVIEW:
[For each position: hold/trim/add with reasoning]

BRIDGE RECOMMENDATIONS:
[Top 1-3 actionable trades with:]
ACTION: BUY/SELL/HOLD
TICKER: [symbol]
THESIS: [specific reasoning]
ENTRY: [price zone]
TARGET: [price]
STOP: [price]
CONVICTION: [0.00-1.00]

CAPTAIN'S NOTE: [one sentence summary — what would Data say to Kirk right now?]

Be specific. Use numbers. Reference today's prices, VIX, oil, sector rotation.
Never be vague. The Captain needs actionable intelligence, not opinions."""


def _get_mlx_client():
    """Get MLX server client (OpenAI-compatible, local). Returns None if MLX is unreachable."""
    try:
        from openai import OpenAI
        from config import MLX_URL
        import httpx
        # Quick reachability check — fail fast rather than waiting 180s
        httpx.get(f"{MLX_URL}/v1/models", timeout=3.0)
        return OpenAI(api_key="not-needed", base_url=f"{MLX_URL}/v1", timeout=120.0)
    except Exception:
        return None


# Ollama fallback models for Data briefing (tried in order)
_OLLAMA_FALLBACK_MODELS = ["qwen3:14b", "qwen3.5:9b", "qwen3:4b"]


def _call_ollama(system: str, user: str, max_tokens: int = 2000) -> str | None:
    """Try Ollama models in fallback order. Returns text or None on total failure."""
    try:
        from openai import OpenAI
        from config import OLLAMA_URL
        client = OpenAI(api_key="ollama", base_url=f"{OLLAMA_URL}/v1", timeout=120.0)
        for model in _OLLAMA_FALLBACK_MODELS:
            try:
                resp = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    max_tokens=max_tokens,
                )
                text = resp.choices[0].message.content or ""
                if text.strip():
                    console.log(f"[cyan]🤖 Data briefing via Ollama/{model} (MLX fallback)")
                    return text.strip()
            except Exception as model_err:
                console.log(f"[yellow]Ollama/{model} failed: {model_err}")
                continue
    except Exception as e:
        console.log(f"[red]Ollama fallback failed: {e}")
    return None


def _get_captain_portfolio() -> str:
    """Get Captain Kirk's current Webull portfolio as text."""
    try:
        conn = sqlite3.connect(DB, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        cash = conn.execute("SELECT cash FROM ai_players WHERE id='steve-webull'").fetchone()
        positions = conn.execute(
            "SELECT symbol, qty, avg_price FROM positions WHERE player_id='steve-webull'"
        ).fetchall()
        conn.close()

        cash_val = cash["cash"] if cash else 0
        lines = [f"CAPTAIN'S PORTFOLIO (Real Webull Money):"]
        lines.append(f"Cash: ${cash_val:.2f}")
        total_cost = 0
        for p in positions:
            cost = p["qty"] * p["avg_price"]
            total_cost += cost
            lines.append(f"  {p['symbol']}: {p['qty']} shares @ ${p['avg_price']:.2f} (cost: ${cost:.2f})")
        lines.append(f"Total Cost Basis: ${total_cost:.2f}")
        lines.append(f"Total Account: ${cash_val + total_cost:.2f}")

        # Add live prices
        try:
            from engine.market_data import get_stock_price
            lines.append("\nLIVE PRICES:")
            for p in positions:
                price_data = get_stock_price(p["symbol"])
                if price_data and "price" in price_data:
                    current = price_data["price"]
                    change = price_data.get("change_pct", 0)
                    mkt_val = p["qty"] * current
                    pnl = mkt_val - (p["qty"] * p["avg_price"])
                    pnl_pct = ((current / p["avg_price"]) - 1) * 100
                    lines.append(f"  {p['symbol']}: ${current:.2f} ({change:+.2f}%) | Mkt Val: ${mkt_val:.2f} | P&L: ${pnl:+.2f} ({pnl_pct:+.1f}%)")
        except Exception:
            pass

        return "\n".join(lines)
    except Exception as e:
        return f"Portfolio unavailable: {e}"


def _get_spock_briefing() -> str:
    """Get Mr. Spock's (Grok 4 arena) latest CTO briefing."""
    try:
        from engine.cto_advisor import get_latest_briefing
        briefing = get_latest_briefing()
        if briefing and briefing.get("briefing"):
            return f"MR. SPOCK'S LATEST CTO BRIEFING:\n{briefing['briefing'][:1500]}"
    except Exception:
        pass
    return "Spock's briefing unavailable."


def _get_market_context() -> str:
    """Get market context from scan_context module."""
    try:
        from engine.scan_context import build_scan_context
        from engine.market_data import get_all_prices, get_technical_indicators
        from config import WATCH_STOCKS
        prices = get_all_prices(WATCH_STOCKS)
        indicators = {}
        for sym in prices:
            ind = get_technical_indicators(sym)
            if ind:
                indicators[sym] = ind
        return build_scan_context(prices, indicators, "steve-webull")
    except Exception as e:
        return f"Market context unavailable: {e}"


def _get_investor_scoring_context() -> str:
    """Add valuation, growth, and macro context for Data's investor-style reasoning."""
    lines = []
    try:
        from engine.cross_asset import get_cross_asset_monitor
        macro = get_cross_asset_monitor()
        bias = (macro.get("macro_bias") or {}).get("bias", "NEUTRAL")
        signals = macro.get("signals") or []
        lines.append(f"MACRO BIAS: {bias}")
        if signals:
            lines.append("MACRO SIGNALS:")
            for sig in signals[:3]:
                lines.append(f"  - {sig['signal']}: {sig['description']}")
    except Exception:
        pass

    try:
        conn = sqlite3.connect(DB, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        positions = conn.execute(
            "SELECT symbol, qty, avg_price FROM positions WHERE player_id='steve-webull'"
        ).fetchall()
        conn.close()
        if positions:
            from engine.fundamental_score import compute_fundamental_score
            lines.append("VALUATION / GROWTH SNAPSHOT:")
            for pos in positions[:8]:
                score = compute_fundamental_score(pos["symbol"])
                if not score:
                    continue
                lines.append(
                    f"  {pos['symbol']}: total {score['total_score']}/100, "
                    f"valuation {score.get('valuation_score', 0)}/25, "
                    f"growth {score.get('growth_score', 0)}/25, "
                    f"macro {score.get('macro_score', 0)}/25"
                )
    except Exception:
        pass

    return "\n".join(lines) if lines else "Investor scoring unavailable."


def get_briefing(force: bool = False) -> dict:
    """Get Data's full briefing. Cached for 30 minutes.

    Model priority:
      1. MLX Qwen3 8B (local, fast, zero cost)
      2. Ollama qwen3:14b / qwen3:8b / qwen3:4b (fallback)
      3. Stale cache (if available, never older than 24h)
      4. Graceful unavailable response (never surfaces raw error to UI)
    """
    global _briefing_cache, _briefing_time

    now = time.time()
    if not force and _briefing_cache and (now - _briefing_time) < _BRIEFING_TTL:
        return _briefing_cache

    # Assemble context (shared across MLX and Ollama paths)
    portfolio = _get_captain_portfolio()
    spock = _get_spock_briefing()
    market = _get_market_context()
    investor = _get_investor_scoring_context()

    user_prompt = (
        f"Captain Kirk needs your full Bridge briefing. Analyze everything and provide recommendations.\n\n"
        f"{portfolio}\n\n{market}\n\n{investor}\n\n{spock}\n\n"
        f"Today is {datetime.now().strftime('%A, %B %d, %Y %I:%M %p ET')}.\n"
        f"Give your full 🤖 DATA'S ANALYSIS following the standard format."
    )

    import re

    def _clean(text: str) -> str:
        return re.sub(r"<think>[\s\S]*?</think>\s*", "", text).strip()

    briefing_text: str | None = None
    source_model = "unknown"

    # ── Attempt 1: MLX ────────────────────────────────────────────────────
    client = _get_mlx_client()
    if client:
        try:
            from config import MLX_MODEL
            resp = client.chat.completions.create(
                model=MLX_MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": f"/think {user_prompt}"},
                ],
                max_tokens=2000,
            )
            raw = resp.choices[0].message.content or ""
            if raw.strip():
                briefing_text = _clean(raw)
                source_model = MLX_MODEL
        except Exception as mlx_err:
            console.log(f"[yellow]🤖 MLX briefing failed: {mlx_err} — trying Ollama fallback")

    # ── Attempt 2: Ollama fallback ────────────────────────────────────────
    if not briefing_text:
        raw = _call_ollama(SYSTEM_PROMPT, user_prompt, max_tokens=2000)
        if raw:
            briefing_text = _clean(raw)
            source_model = "ollama-fallback"

    # ── Success path ──────────────────────────────────────────────────────
    if briefing_text:
        try:
            from engine.cost_tracker import log_cost
            log_cost("first-officer", "briefing", user_prompt, briefing_text)
        except Exception:
            pass
        result = {
            "briefing": briefing_text,
            "timestamp": datetime.now().isoformat(),
            "cached": False,
            "source_model": source_model,
            "portfolio_summary": portfolio[:500],
        }
        _briefing_cache = result
        _briefing_time = now
        console.log(f"[bold cyan]🤖 First Officer briefing generated via {source_model} ({len(briefing_text)} chars)")
        return result

    # ── Attempt 3: return stale cache (up to 24h old) ─────────────────────
    if _briefing_cache and _briefing_cache.get("briefing"):
        stale_mins = int((now - _briefing_time) / 60)
        if stale_mins < 1440:  # within 24 hours
            console.log(f"[yellow]🤖 Returning stale briefing ({stale_mins}m old) — all models unavailable")
            return {**_briefing_cache, "cached": True, "stale_minutes": stale_mins}

    # ── Attempt 4: graceful unavailable (never surface raw error) ─────────
    console.log("[red]🤖 First Officer briefing unavailable — MLX offline, Ollama unreachable, no cache")
    return {
        "briefing": "Briefing unavailable — Mr. Data is offline. Check MLX server or Ollama connection.",
        "timestamp": datetime.now().isoformat(),
        "cached": False,
        "unavailable": True,
        "source_model": None,
        "portfolio_summary": "",
    }


def ask_data(question: str) -> dict:
    """Ask Data a specific question with full portfolio context."""
    global _ask_times

    # Rate limit check
    now = time.time()
    _ask_times = [t for t in _ask_times if now - t < _ASK_WINDOW]
    if len(_ask_times) >= _ASK_LIMIT:
        return {"error": f"Rate limited: max {_ASK_LIMIT} questions per hour", "answer": None}

    portfolio = _get_captain_portfolio()
    market = _get_market_context()

    user_prompt = (
        f'Captain Kirk asks: "{question}"\n\n{portfolio}\n\n{market}\n\n'
        f"Answer specifically and concisely. Use numbers and current prices. Be actionable."
    )

    import re

    def _clean(text: str) -> str:
        return re.sub(r"<think>[\s\S]*?</think>\s*", "", text).strip()

    answer: str | None = None

    # Try MLX first, Ollama fallback
    client = _get_mlx_client()
    if client:
        try:
            from config import MLX_MODEL
            resp = client.chat.completions.create(
                model=MLX_MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": f"/think {user_prompt}"},
                ],
                max_tokens=1000,
            )
            raw = resp.choices[0].message.content or ""
            if raw.strip():
                answer = _clean(raw)
        except Exception as mlx_err:
            console.log(f"[yellow]🤖 MLX ask failed: {mlx_err} — trying Ollama")

    if not answer:
        raw = _call_ollama(SYSTEM_PROMPT, user_prompt, max_tokens=1000)
        if raw:
            answer = _clean(raw)

    if not answer:
        return {
            "answer": "Mr. Data is offline — MLX server unreachable and Ollama fallback failed. Try again shortly.",
            "question": question,
            "timestamp": datetime.now().isoformat(),
            "unavailable": True,
        }

    _ask_times.append(now)
    try:
        from engine.cost_tracker import log_cost
        log_cost("first-officer", "ask", user_prompt, answer)
    except Exception:
        pass

    return {
        "answer": answer,
        "question": question,
        "timestamp": datetime.now().isoformat(),
    }


def get_briefing_summary() -> dict | None:
    """Get a one-line summary of Data's last briefing for the Bridge status bar."""
    if not _briefing_cache or not _briefing_cache.get("briefing"):
        return None
    text = _briefing_cache["briefing"]
    # Extract CAPTAIN'S NOTE if present
    note = ""
    for line in text.split("\n"):
        if "CAPTAIN'S NOTE:" in line:
            note = line.split("CAPTAIN'S NOTE:")[-1].strip()
            break
    # Check for high-conviction recommendation
    has_high_conviction = "CONVICTION: 0.8" in text or "CONVICTION: 0.9" in text or "CONVICTION: 1.0" in text
    minutes_ago = int((time.time() - _briefing_time) / 60) if _briefing_time else 999
    return {
        "summary": note or "Briefing available",
        "minutes_ago": minutes_ago,
        "has_recommendation": has_high_conviction,
        "timestamp": _briefing_cache.get("timestamp"),
    }
