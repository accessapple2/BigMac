"""
TradeMinds Season 5 — Scaled Debate Engine
============================================
6 Bull agents + 6 Bear agents running in parallel via local Ollama.
Picard synthesizes. Risk Triad reviews.
Results saved to debate_history in trader.db.

Usage:
    from engine.debate_engine import run_full_debate
    result = asyncio.run(run_full_debate("AAPL", stock_data))
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from datetime import datetime
from typing import Any

import aiohttp

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

OLLAMA_BASE = "http://localhost:11434"
TRADER_DB = "data/trader.db"

# Model assignments — tune these to what runs best on your M4
MODELS = {
    "primary": "phi4:14b",            # Picard judge + Risk Triad — deep reasoning
    "general": "qwen3.5:9b",           # bull/bear agents — balanced analysis
    "light": "qwen3.5:9b",             # fast, lightweight
    "scanner": "qwen3.5:9b",           # fastest, simple tasks
}

# How many Ollama requests can run at once (tune for your RAM)
MAX_CONCURRENT = 4

# Ollama generation settings
OLLAMA_OPTIONS = {
    "temperature": 0.7,
    "num_predict": 1024,      # enough tokens for full JSON responses
    "think": False,           # disable chain-of-thought — we want fast JSON
}

logger = logging.getLogger("debate_engine")


# ---------------------------------------------------------------------------
# Live Market Data Fetch
# ---------------------------------------------------------------------------

def fetch_live_market_data(ticker: str) -> dict:
    """
    Fetch current price, technicals, and RSI for ticker.
    Falls back to universe_scan cache if live fetch fails.
    Returns a dict suitable for injecting into agent prompts.
    """
    data: dict[str, Any] = {"ticker": ticker}

    # --- Live price ---
    try:
        from engine.market_data import get_stock_price
        price_data = get_stock_price(ticker)
        if price_data and price_data.get("price"):
            data["price"] = price_data["price"]
            data["change_pct"] = price_data.get("change_pct", 0)
            data["volume"] = price_data.get("volume", 0)
    except Exception as e:
        logger.warning(f"Price fetch failed for {ticker}: {e}")

    # --- Technical indicators (RSI, SMA50/200, MACD, vol_ratio) ---
    try:
        from engine.market_data import get_technical_indicators
        tech = get_technical_indicators(ticker)
        if tech:
            data["rsi"] = tech.get("rsi")
            data["sma_50"] = tech.get("sma_50")
            data["sma_200"] = tech.get("sma_200")
            data["above_sma50"] = tech.get("above_sma50")
            data["above_sma200"] = tech.get("above_sma200")
            data["macd"] = tech.get("macd")
            data["macd_signal"] = tech.get("macd_signal")
            data["macd_histogram"] = tech.get("macd_histogram")
            data["volume_ratio"] = tech.get("volume_ratio")
            data["avg_volume_20d"] = tech.get("avg_volume_20d")
    except Exception as e:
        logger.warning(f"Technical indicators failed for {ticker}: {e}")

    # --- RSI fallback from universe_scan if tech fetch failed ---
    if not data.get("rsi"):
        try:
            conn = sqlite3.connect(TRADER_DB)
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT rsi, close, volume, volume_ratio FROM universe_scan "
                "WHERE ticker=? ORDER BY id DESC LIMIT 1",
                (ticker,),
            ).fetchone()
            conn.close()
            if row:
                if not data.get("rsi"):
                    data["rsi"] = row["rsi"]
                if not data.get("price"):
                    data["price"] = row["close"]
                if not data.get("volume") and row["volume"]:
                    data["volume"] = row["volume"]
                if not data.get("volume_ratio"):
                    data["volume_ratio"] = row["volume_ratio"]
                data["rsi_source"] = "universe_scan_cache"
        except Exception as e:
            logger.warning(f"universe_scan RSI fallback failed for {ticker}: {e}")

    return data


def _format_market_data_block(ticker: str, md: dict) -> str:
    """Format market data dict into a readable block for agent prompts."""
    price = md.get("price")
    if not price:
        return f"\nNOTE: No live market data available for {ticker}. Reason from your training knowledge.\n"

    chg = md.get("change_pct", 0)
    vol = md.get("volume", 0)
    avg_vol = md.get("avg_volume_20d", 0)
    vol_ratio = md.get("volume_ratio")
    rsi = md.get("rsi")
    sma50 = md.get("sma_50")
    sma200 = md.get("sma_200")
    macd = md.get("macd")
    macd_sig = md.get("macd_signal")

    lines = [
        f"\n=== LIVE MARKET DATA: {ticker} ===",
        f"Price:       ${price:.2f}  ({chg:+.2f}% today)",
    ]
    if vol:
        vol_note = ""
        if vol_ratio:
            vol_note = f"  [{vol_ratio:.1f}x avg]"
        lines.append(f"Volume:      {vol:,}{vol_note}")
    if rsi is not None:
        rsi_note = " (oversold)" if rsi < 30 else " (overbought)" if rsi > 70 else ""
        lines.append(f"RSI(14):     {rsi:.1f}{rsi_note}")
    if sma50:
        pos50 = "above" if md.get("above_sma50") else "below"
        lines.append(f"SMA-50:      ${sma50:.2f}  (price is {pos50})")
    if sma200:
        pos200 = "above" if md.get("above_sma200") else "below"
        lines.append(f"SMA-200:     ${sma200:.2f}  (price is {pos200})")
    if macd is not None and macd_sig is not None:
        macd_trend = "bullish crossover" if macd > macd_sig else "bearish crossover"
        lines.append(f"MACD:        {macd:.3f}  Signal: {macd_sig:.3f}  ({macd_trend})")
    lines.append("=== USE THESE NUMBERS IN YOUR ANALYSIS ===\n")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Agent Definitions
# ---------------------------------------------------------------------------

BULL_AGENTS = [
    {
        "name": "Riker-Prime",
        "lens": "overall",
        "model": MODELS["primary"],
        "prompt": (
            "You are Commander Riker, the LEAD BULL analyst. "
            "Build the strongest overall investment case for {ticker}. "
            "Cover why this stock should be BOUGHT now. "
            "Be specific — cite numbers, trends, and catalysts."
        ),
    },
    {
        "name": "Bull-Momentum",
        "lens": "momentum",
        "model": MODELS["general"],
        "prompt": (
            "You are a technical momentum analyst arguing the BULL case for {ticker}. "
            "Focus ONLY on: price action, trend direction, RSI, MACD, moving averages, "
            "volume patterns, and any bullish chart setups. "
            "Why does the technical picture say BUY?"
        ),
    },
    {
        "name": "Bull-Fundamental",
        "lens": "fundamental",
        "model": MODELS["primary"],
        "prompt": (
            "You are a fundamental analyst arguing the BULL case for {ticker}. "
            "Focus ONLY on: revenue growth, earnings trajectory, profit margins, "
            "competitive moat, and balance sheet strength. "
            "Why do the fundamentals say BUY?"
        ),
    },
    {
        "name": "Bull-Sentiment",
        "lens": "sentiment",
        "model": MODELS["light"],
        "prompt": (
            "You are a sentiment analyst arguing the BULL case for {ticker}. "
            "Focus ONLY on: recent news headlines, analyst upgrades, "
            "institutional buying, insider purchases, and social sentiment. "
            "Why does the sentiment picture say BUY?"
        ),
    },
    {
        "name": "Bull-Sector",
        "lens": "sector",
        "model": MODELS["scanner"],
        "prompt": (
            "You are a sector analyst arguing the BULL case for {ticker}. "
            "Focus ONLY on: industry tailwinds, sector rotation trends, "
            "peer comparison, and macro factors favoring this sector. "
            "Why does the sector outlook say BUY?"
        ),
    },
    {
        "name": "Bull-Catalyst",
        "lens": "catalyst",
        "model": MODELS["light"],
        "prompt": (
            "You are an event-driven analyst arguing the BULL case for {ticker}. "
            "Focus ONLY on: upcoming earnings, product launches, partnerships, "
            "regulatory approvals, policy changes, or any near-term catalyst. "
            "What specific event could drive this stock UP in the next 1-3 months?"
        ),
    },
]

BEAR_AGENTS = [
    {
        "name": "Worf-Prime",
        "lens": "overall",
        "model": MODELS["primary"],
        "prompt": (
            "You are Lt. Commander Worf, the LEAD BEAR analyst. "
            "Build the strongest case AGAINST investing in {ticker}. "
            "Cover why this stock should be AVOIDED or SOLD now. "
            "Be specific — cite numbers, risks, and red flags."
        ),
    },
    {
        "name": "Bear-Momentum",
        "lens": "momentum",
        "model": MODELS["general"],
        "prompt": (
            "You are a technical analyst arguing the BEAR case for {ticker}. "
            "Focus ONLY on: bearish chart patterns, breakdown signals, "
            "declining volume, support levels at risk, overbought signals. "
            "Why does the technical picture say SELL or AVOID?"
        ),
    },
    {
        "name": "Bear-Fundamental",
        "lens": "fundamental",
        "model": MODELS["primary"],
        "prompt": (
            "You are a fundamental analyst arguing the BEAR case for {ticker}. "
            "Focus ONLY on: slowing growth, margin compression, rising debt, "
            "overvaluation (high P/E, P/S), and competitive threats. "
            "Why do the fundamentals say SELL or AVOID?"
        ),
    },
    {
        "name": "Bear-Sentiment",
        "lens": "sentiment",
        "model": MODELS["light"],
        "prompt": (
            "You are a sentiment analyst arguing the BEAR case for {ticker}. "
            "Focus ONLY on: analyst downgrades, insider selling, negative news, "
            "short interest increases, and deteriorating social sentiment. "
            "Why does the sentiment picture say SELL or AVOID?"
        ),
    },
    {
        "name": "Bear-Sector",
        "lens": "sector",
        "model": MODELS["scanner"],
        "prompt": (
            "You are a sector analyst arguing the BEAR case for {ticker}. "
            "Focus ONLY on: sector headwinds, regulatory risks, rising competition, "
            "disruption threats, and macro factors hurting this sector. "
            "Why does the sector outlook say SELL or AVOID?"
        ),
    },
    {
        "name": "Bear-Catalyst",
        "lens": "catalyst",
        "model": MODELS["light"],
        "prompt": (
            "You are a risk analyst arguing the BEAR case for {ticker}. "
            "Focus ONLY on: earnings miss risk, guidance risk, litigation, "
            "macro shocks, geopolitical exposure, or any near-term threat. "
            "What specific event could drive this stock DOWN in the next 1-3 months?"
        ),
    },
]

PICARD_PROMPT = """You are Captain Picard, impartial judge of this stock debate for {ticker}.

BULL (6 analysts):
{bull_summary}

BEAR (6 analysts):
{bear_summary}
{expert_witness}
Weigh all arguments. Score conviction 1-10:
1-3=Strong Sell, 4=Lean Sell, 5=Hold, 6=Lean Buy, 7-10=Strong Buy

Respond with ONLY this JSON (no extra text):
{{"conviction":<1-10>,"decision":"STRONG_BUY|BUY|LEAN_BUY|HOLD|LEAN_SELL|SELL|STRONG_SELL","synthesis":"<1 sentence>"}}"""

PLUTUS_PROMPT = """You are a financial markets expert trained on hundreds of books covering technical analysis, options trading, risk management, and behavioral finance. Given the bull and bear cases below, plus the market data, provide your expert assessment. Focus on: 1) What historical pattern does this setup most resemble? 2) What does options positioning (GEX/gamma) suggest about near-term direction? 3) What risk factors are the bull and bear cases each ignoring? 4) Your verdict: BULLISH, BEARISH, or NEUTRAL with one sentence of reasoning. Be concise — 4-5 sentences max.

TICKER: {ticker}
{market_data}
BULL CASE:
{bull_summary}

BEAR CASE:
{bear_summary}"""

RISK_TRIAD_PROMPT = """You are the Risk Triad — Spock (logic), Dr. Crusher (human impact), and Scotty (execution risk).

Captain Picard has made this decision about {ticker}:
Conviction: {conviction}/10
Decision: {decision}
Synthesis: {synthesis}

Review this decision through three lenses:

SPOCK (Logic): Are there logical flaws in the reasoning? Does the data support the conviction level?
CRUSHER (Impact): What's the worst-case scenario for the portfolio? How bad could losses get?
SCOTTY (Execution): Can this trade actually be executed well? Liquidity, slippage, timing risks?

Respond in this exact JSON format:
{{
  "risk_rating": "LOW|MEDIUM|HIGH|CRITICAL",
  "spock_assessment": "<1-2 sentences>",
  "crusher_assessment": "<1-2 sentences>",
  "scotty_assessment": "<1-2 sentences>",
  "override_recommendation": "PROCEED|REDUCE_SIZE|DELAY|ABORT",
  "adjusted_conviction": <1-10 or same as original if no change>
}}
"""

# ---------------------------------------------------------------------------
# Ollama Communication
# ---------------------------------------------------------------------------

async def call_ollama(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    model: str,
    prompt: str,
    system: str = "",
) -> str:
    """Call Ollama's generate endpoint with concurrency control."""
    async with semaphore:
        options = {k: v for k, v in OLLAMA_OPTIONS.items() if k != "think"}
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "think": OLLAMA_OPTIONS.get("think", False),
            "options": options,
        }
        if system:
            payload["system"] = system

        try:
            async with session.post(
                f"{OLLAMA_BASE}/api/generate",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=600),
            ) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error(f"Ollama error ({resp.status}): {error_text}")
                    return ""
                data = await resp.json()
                return data.get("response", "")
        except asyncio.TimeoutError:
            logger.error(f"Ollama timeout for model {model}")
            return ""
        except Exception as e:
            logger.error(f"Ollama call failed: {e}")
            return ""


def parse_json_response(text: str) -> dict:
    """Extract JSON from a model response, handling markdown fences and <think> blocks."""
    import re
    cleaned = text.strip()

    # Strip <think>...</think> blocks (qwen3 reasoning prefix)
    cleaned = re.sub(r"<think>.*?</think>", "", cleaned, flags=re.DOTALL).strip()

    # Strip markdown code fences
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        cleaned = "\n".join(lines)

    # Try to find JSON object in the text
    start = cleaned.find("{")
    end = cleaned.rfind("}") + 1
    if start != -1 and end > start:
        try:
            return json.loads(cleaned[start:end])
        except json.JSONDecodeError:
            pass

    logger.warning(f"Could not parse JSON from response: {text[:200]}...")
    return {}


# ---------------------------------------------------------------------------
# Agent Runner
# ---------------------------------------------------------------------------

async def run_agent(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    agent: dict,
    ticker: str,
    stock_data: dict,
) -> dict:
    """Run a single debate agent and return structured result."""
    prompt = agent["prompt"].format(ticker=ticker)

    # Inject crew track record on this ticker (before market data)
    try:
        from engine.trade_memory import get_memory_block_for_debate
        memory_block = get_memory_block_for_debate(ticker)
        if memory_block:
            prompt += memory_block
    except Exception:
        pass

    # Inject live market data block (structured, readable — not raw JSON)
    if stock_data:
        prompt += _format_market_data_block(ticker, stock_data)

    prompt += (
        "\n\nRespond in this exact JSON format:\n"
        "{\n"
        '  "conviction": <1-10>,\n'
        '  "thesis": "<your 2-3 sentence argument>",\n'
        '  "key_data_point": "<most important specific number or fact>"\n'
        "}"
    )

    response = await call_ollama(session, semaphore, agent["model"], prompt)
    parsed = parse_json_response(response)

    return {
        "agent_name": agent["name"],
        "side": "bull" if agent in BULL_AGENTS else "bear",
        "lens": agent["lens"],
        "model": agent["model"],
        "conviction": parsed.get("conviction", 5),
        "thesis": parsed.get("thesis", response[:300] if response else "No response"),
        "key_data_point": parsed.get("key_data_point", "N/A"),
        "raw_response": response[:500],
    }


async def run_squad(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    agents: list,
    ticker: str,
    stock_data: dict,
) -> list:
    """Run a full squad (bull or bear) in parallel."""
    tasks = [
        run_agent(session, semaphore, agent, ticker, stock_data)
        for agent in agents
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Filter out exceptions
    valid = []
    for r in results:
        if isinstance(r, Exception):
            logger.error(f"Agent failed: {r}")
        else:
            valid.append(r)
    return valid


# ---------------------------------------------------------------------------
# Picard & Risk Triad
# ---------------------------------------------------------------------------

async def run_picard(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    ticker: str,
    bull_results: list,
    bear_results: list,
    plutus_analysis: str | None = None,
) -> dict:
    """Captain Picard synthesizes all debate arguments."""
    # Inject crew trade history for this ticker
    crew_history = ""
    try:
        from engine.trade_memory import get_memory_block_for_debate
        crew_history = get_memory_block_for_debate(ticker) or ""
    except Exception:
        pass

    bull_summary = "\n".join(
        f"  [{r['lens']}] {r['conviction']}/10 — {r['thesis'][:80]}"
        for r in bull_results
    )
    bear_summary = "\n".join(
        f"  [{r['lens']}] {r['conviction']}/10 — {r['thesis'][:80]}"
        for r in bear_results
    )

    if plutus_analysis:
        expert_witness = (
            "\nEXPERT WITNESS — Plutus (Financial Markets Specialist):\n"
            f"{plutus_analysis[:600]}\n"
        )
    else:
        expert_witness = ""

    prompt = crew_history + PICARD_PROMPT.format(
        ticker=ticker,
        bull_summary=bull_summary,
        bear_summary=bear_summary,
        expert_witness=expert_witness,
    )

    response = await call_ollama(
        session, semaphore, MODELS["primary"], prompt
    )
    parsed = parse_json_response(response)

    return {
        "conviction": parsed.get("conviction", 5),
        "decision": parsed.get("decision", "HOLD"),
        "synthesis": parsed.get("synthesis", ""),
        "raw_response": response[:500],
    }


async def run_plutus_witness(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    ticker: str,
    bull_results: list,
    bear_results: list,
    stock_data: dict,
) -> str | None:
    """Expert Witness step — Plutus reviews the bull/bear cases before Picard judges.

    Returns the raw text assessment, or None if Plutus is unavailable.
    """
    bull_summary = "\n".join(
        f"  [{r['lens']}] {r['conviction']}/10 — {r['thesis'][:100]}"
        for r in bull_results
    )
    bear_summary = "\n".join(
        f"  [{r['lens']}] {r['conviction']}/10 — {r['thesis'][:100]}"
        for r in bear_results
    )
    market_data = _format_market_data_block(ticker, stock_data)

    prompt = PLUTUS_PROMPT.format(
        ticker=ticker,
        market_data=market_data,
        bull_summary=bull_summary,
        bear_summary=bear_summary,
    )

    response = await call_ollama(session, semaphore, "0xroyce/plutus", prompt)
    if not response or not response.strip():
        return None

    # Strip <think> blocks if present
    import re
    response = re.sub(r"<think>.*?</think>", "", response, flags=re.DOTALL).strip()
    return response if response else None


async def run_risk_triad(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    ticker: str,
    picard_result: dict,
) -> dict:
    """Risk Triad (Spock/Crusher/Scotty) reviews Picard's decision."""
    prompt = RISK_TRIAD_PROMPT.format(
        ticker=ticker,
        conviction=picard_result["conviction"],
        decision=picard_result["decision"],
        synthesis=picard_result["synthesis"],
    )

    response = await call_ollama(
        session, semaphore, MODELS["general"], prompt
    )
    parsed = parse_json_response(response)

    return {
        "risk_rating": parsed.get("risk_rating", "MEDIUM"),
        "spock": parsed.get("spock_assessment", "N/A"),
        "crusher": parsed.get("crusher_assessment", "N/A"),
        "scotty": parsed.get("scotty_assessment", "N/A"),
        "override": parsed.get("override_recommendation", "PROCEED"),
        "adjusted_conviction": parsed.get(
            "adjusted_conviction", picard_result["conviction"]
        ),
        "raw_response": response[:500],
    }


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db():
    """Create the scaled debate tables if they don't exist."""
    conn = sqlite3.connect(TRADER_DB)
    c = conn.cursor()

    # Main debate record
    c.execute("""
        CREATE TABLE IF NOT EXISTS debate_history_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            picard_conviction INTEGER,
            picard_decision TEXT,
            picard_synthesis TEXT,
            picard_strongest_bull TEXT,
            picard_strongest_bear TEXT,
            risk_rating TEXT,
            risk_override TEXT,
            adjusted_conviction INTEGER,
            spock_assessment TEXT,
            crusher_assessment TEXT,
            scotty_assessment TEXT,
            bull_avg_conviction REAL,
            bear_avg_conviction REAL,
            agent_count INTEGER,
            stock_data_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Individual agent verdicts (linked to debate)
    c.execute("""
        CREATE TABLE IF NOT EXISTS debate_agent_verdicts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            debate_id INTEGER NOT NULL,
            agent_name TEXT NOT NULL,
            side TEXT NOT NULL,
            lens TEXT NOT NULL,
            model TEXT,
            conviction INTEGER,
            thesis TEXT,
            key_data_point TEXT,
            raw_response TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (debate_id) REFERENCES debate_history_v2(id)
        )
    """)

    # Migrate: add plutus_analysis column if not present (never drops existing data)
    try:
        c.execute("ALTER TABLE debate_history_v2 ADD COLUMN plutus_analysis TEXT")
    except Exception:
        pass  # Column already exists

    conn.commit()
    conn.close()
    logger.info("Database tables initialized")


def save_debate(
    ticker: str,
    bull_results: list,
    bear_results: list,
    picard_result: dict,
    risk_result: dict,
    stock_data: dict,
    plutus_analysis: str | None = None,
) -> int:
    """Save full debate results to trader.db. Returns the debate ID."""
    conn = sqlite3.connect(TRADER_DB)
    c = conn.cursor()

    bull_avg = (
        sum(r["conviction"] for r in bull_results) / len(bull_results)
        if bull_results
        else 0
    )
    bear_avg = (
        sum(r["conviction"] for r in bear_results) / len(bear_results)
        if bear_results
        else 0
    )

    # Insert main debate record
    c.execute(
        """
        INSERT INTO debate_history_v2
        (ticker, picard_conviction, picard_decision, picard_synthesis,
         picard_strongest_bull, picard_strongest_bear,
         risk_rating, risk_override, adjusted_conviction,
         spock_assessment, crusher_assessment, scotty_assessment,
         bull_avg_conviction, bear_avg_conviction, agent_count,
         stock_data_json, plutus_analysis)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ticker,
            picard_result["conviction"],
            picard_result["decision"],
            picard_result["synthesis"],
            "",  # strongest_bull (removed)
            "",  # strongest_bear (removed)
            risk_result["risk_rating"],
            risk_result["override"],
            risk_result["adjusted_conviction"],
            risk_result["spock"],
            risk_result["crusher"],
            risk_result["scotty"],
            round(bull_avg, 2),
            round(bear_avg, 2),
            len(bull_results) + len(bear_results),
            json.dumps(stock_data, default=str),
            plutus_analysis,
        ),
    )
    debate_id = c.lastrowid

    # Insert individual agent verdicts
    for result in bull_results + bear_results:
        c.execute(
            """
            INSERT INTO debate_agent_verdicts
            (debate_id, agent_name, side, lens, model,
             conviction, thesis, key_data_point, raw_response)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                debate_id,
                result["agent_name"],
                result["side"],
                result["lens"],
                result["model"],
                result["conviction"],
                result["thesis"],
                result["key_data_point"],
                result["raw_response"],
            ),
        )

    conn.commit()
    conn.close()
    logger.info(f"Debate #{debate_id} saved for {ticker}")
    return debate_id


# ---------------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------------

async def run_full_debate(
    ticker: str,
    stock_data: dict | None = None,
) -> dict:
    """
    Run the full 12-agent debate for a single ticker.

    Args:
        ticker: Stock symbol (e.g., "AAPL")
        stock_data: Optional dict with price, financials, news, etc.

    Returns:
        dict with debate_id, picard_result, risk_result, and all verdicts.
    """
    if stock_data is None:
        stock_data = {}

    init_db()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    # Fetch live market data and merge into stock_data
    logger.info(f"Fetching live market data for {ticker}...")
    live_md = fetch_live_market_data(ticker)
    if live_md.get("price"):
        logger.info(
            f"Live: ${live_md['price']:.2f} "
            f"({live_md.get('change_pct', 0):+.2f}%) "
            f"RSI={live_md.get('rsi', 'N/A')}"
        )
    else:
        logger.warning(f"No live price for {ticker} — agents will reason from training data")
    # Caller-supplied stock_data takes precedence over live fetch
    merged = {**live_md, **stock_data}
    stock_data = merged

    logger.info(f"Starting full debate for {ticker} with 12 agents...")
    start_time = datetime.now()

    async with aiohttp.ClientSession() as session:
        # Step 1: Run bull and bear squads in parallel
        logger.info("Launching Bull Squad (6 agents)...")
        logger.info("Launching Bear Squad (6 agents)...")
        bull_results, bear_results = await asyncio.gather(
            run_squad(session, semaphore, BULL_AGENTS, ticker, stock_data),
            run_squad(session, semaphore, BEAR_AGENTS, ticker, stock_data),
        )

        logger.info(
            f"Squads complete: {len(bull_results)} bull, "
            f"{len(bear_results)} bear verdicts"
        )

        # Step 2: Expert Witness — Plutus weighs in before Picard
        logger.info("Expert Witness — Plutus is reviewing the cases...")
        plutus_analysis = await run_plutus_witness(
            session, semaphore, ticker, bull_results, bear_results, stock_data
        )
        if plutus_analysis:
            logger.info(f"Plutus verdict received ({len(plutus_analysis)} chars)")
        else:
            logger.warning("Plutus unavailable — Picard will proceed without expert witness")

        # Step 3: Picard synthesizes (with Plutus context if available)
        logger.info("Captain Picard is deliberating...")
        picard_result = await run_picard(
            session, semaphore, ticker, bull_results, bear_results,
            plutus_analysis=plutus_analysis,
        )
        logger.info(
            f"Picard verdict: {picard_result['decision']} "
            f"(conviction: {picard_result['conviction']}/10)"
        )

        # Step 4: Risk Triad reviews
        logger.info("Risk Triad reviewing...")
        risk_result = await run_risk_triad(
            session, semaphore, ticker, picard_result
        )
        logger.info(
            f"Risk Triad: {risk_result['risk_rating']} — "
            f"{risk_result['override']}"
        )

    # Step 5: Save everything
    debate_id = save_debate(
        ticker, bull_results, bear_results,
        picard_result, risk_result, stock_data,
        plutus_analysis=plutus_analysis,
    )

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"Debate #{debate_id} complete in {elapsed:.1f}s")

    return {
        "debate_id": debate_id,
        "ticker": ticker,
        "picard": picard_result,
        "risk_triad": risk_result,
        "bull_verdicts": bull_results,
        "bear_verdicts": bear_results,
        "plutus_analysis": plutus_analysis,
        "elapsed_seconds": round(elapsed, 1),
    }


async def run_batch_debate(
    tickers: list[str],
    stock_data_map: dict[str, dict] | None = None,
) -> list[dict]:
    """
    Run debates for multiple tickers sequentially.
    (Sequential per ticker to avoid overloading Ollama.)
    """
    if stock_data_map is None:
        stock_data_map = {}

    results = []
    for i, ticker in enumerate(tickers, 1):
        logger.info(f"=== Debate {i}/{len(tickers)}: {ticker} ===")
        result = await run_full_debate(
            ticker, stock_data_map.get(ticker, {})
        )
        results.append(result)

    return results


# ---------------------------------------------------------------------------
# TradingAgents Integration — optional enhancement (lib/TradingAgents)
# Uses Ollama only. Returns None gracefully on any failure.
# ---------------------------------------------------------------------------

import threading as _ta_threading
import time as _ta_time

_TA_PATH = __import__("os").path.expanduser("~/autonomous-trader/lib/TradingAgents")
_TA_AVAILABLE: bool = False
_ta_latest_debate: dict | None = None
_ta_debate_lock = _ta_threading.Lock()
_TA_STALE_SEC = 28_800  # 8 hours


def _ta_check_availability() -> bool:
    global _TA_AVAILABLE
    import os
    if not os.path.isdir(os.path.join(_TA_PATH, "tradingagents")):
        return False
    try:
        import langgraph  # noqa: F401
        _TA_AVAILABLE = True
        logger.info("[TA-DEBATE] TradingAgents available")
        return True
    except ImportError:
        logger.debug("[TA-DEBATE] langgraph not installed — disabled")
        return False


_ta_check_availability()


def _ta_run_inner(symbol: str, date_str: str) -> dict | None:
    """Execute TradingAgents propagate() — runs inside a timeout thread."""
    import sys
    if _TA_PATH not in sys.path:
        sys.path.insert(0, _TA_PATH)
    try:
        from tradingagents.graph.trading_graph import TradingAgentsGraph
        from tradingagents.default_config import DEFAULT_CONFIG
    except ImportError as exc:
        logger.warning("[TA-DEBATE] import error: %s", exc)
        return None

    config = {
        **DEFAULT_CONFIG,
        "llm_provider":            "ollama",
        "deep_think_llm":          "qwen3.5:9b",
        "quick_think_llm":         "mistral:7b",
        "backend_url":             "http://localhost:11434/v1",
        "max_debate_rounds":       1,
        "max_risk_discuss_rounds": 1,
        "max_recur_limit":         50,
        "data_vendors": {
            "core_stock_apis":      "yfinance",
            "technical_indicators": "yfinance",
            "fundamental_data":     "yfinance",
            "news_data":            "yfinance",
        },
    }

    try:
        graph = TradingAgentsGraph(
            selected_analysts=["market", "news"],
            debug=False,
            config=config,
        )
        final_state, signal = graph.propagate(symbol, date_str)
    except Exception as exc:
        logger.warning("[TA-DEBATE] propagate() error: %s", exc)
        return None

    inv        = final_state.get("investment_debate_state", {})
    bull_case  = str(inv.get("bull_history",  ""))[:500]
    bear_case  = str(inv.get("bear_history",  ""))[:500]
    reasoning  = str(final_state.get("final_trade_decision", ""))[:600]

    sig = str(signal).strip().upper()
    if "BUY" in sig or "OVERWEIGHT" in sig:
        consensus = "BUY"
    elif "SELL" in sig or "UNDERWEIGHT" in sig:
        consensus = "SELL"
    else:
        consensus = "HOLD"

    return {
        "symbol":    symbol,
        "date":      date_str,
        "consensus": consensus,
        "reasoning": reasoning,
        "bull_case": bull_case,
        "bear_case": bear_case,
        "ts":        _ta_time.time(),
    }


def run_tradingagents_debate(symbol: str = "SPY") -> dict | None:
    """
    Run a TradingAgents debate with 120-second hard timeout.
    Stores result so brain_context can read it all day via get_latest_ta_debate().
    Returns None on any failure — does not affect existing run_full_debate().
    """
    global _ta_latest_debate

    if not _TA_AVAILABLE:
        return None

    from datetime import date as _date
    date_str = _date.today().strftime("%Y-%m-%d")

    result_box: list = []

    def _worker():
        try:
            result_box.append(_ta_run_inner(symbol, date_str))
        except Exception as exc:
            logger.warning("[TA-DEBATE] worker: %s", exc)
            result_box.append(None)

    t = _ta_threading.Thread(target=_worker, daemon=True, name="ta-debate")
    t.start()
    t.join(timeout=120)

    if not result_box:
        logger.warning("[TA-DEBATE] %s: timed out (120s)", symbol)
        return None

    result = result_box[0]
    if result:
        with _ta_debate_lock:
            _ta_latest_debate = result
        logger.info(
            "[TA-DEBATE] %s → %s | bull: %.80s | bear: %.80s",
            symbol, result["consensus"],
            result["bull_case"], result["bear_case"],
        )
    return result


def get_latest_ta_debate() -> dict | None:
    """Return the cached TradingAgents debate result (for brain_context)."""
    with _ta_debate_lock:
        d = _ta_latest_debate
    if d and _ta_time.time() - d.get("ts", 0) > _TA_STALE_SEC:
        return None
    return d


# ---------------------------------------------------------------------------
# CLI — quick test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    ticker = sys.argv[1] if len(sys.argv) > 1 else "SPY"

    # run_full_debate now auto-fetches live market data before launching agents
    result = asyncio.run(run_full_debate(ticker))

    print("\n" + "=" * 60)
    print(f"DEBATE RESULT: {ticker}")
    print("=" * 60)
    print(f"Debate ID:  #{result['debate_id']}")
    print(f"Decision:   {result['picard']['decision']}")
    print(f"Conviction: {result['picard']['conviction']}/10")
    print(f"Risk:       {result['risk_triad']['risk_rating']}")
    print(f"Override:   {result['risk_triad']['override']}")
    print(f"Adjusted:   {result['risk_triad']['adjusted_conviction']}/10")
    print(f"Time:       {result['elapsed_seconds']}s")
    print(f"\nSynthesis: {result['picard']['synthesis']}")
    print(f"\nSpock:   {result['risk_triad']['spock']}")
    print(f"Crusher: {result['risk_triad']['crusher']}")
    print(f"Scotty:  {result['risk_triad']['scotty']}")
    print("\n--- Bull Verdicts ---")
    for v in result["bull_verdicts"]:
        print(f"  {v['agent_name']:20s} [{v['lens']:12s}] "
              f"Conv: {v['conviction']}/10 — {v['thesis'][:80]}...")
    print("\n--- Bear Verdicts ---")
    for v in result["bear_verdicts"]:
        print(f"  {v['agent_name']:20s} [{v['lens']:12s}] "
              f"Conv: {v['conviction']}/10 — {v['thesis'][:80]}...")
