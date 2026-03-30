"""Weekly Picks — Sunday 6 PM ET analysis of top 5 stocks via Telegram."""
from __future__ import annotations
import sqlite3
from datetime import datetime
from rich.console import Console

console = Console()
DB = "data/trader.db"


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def generate_weekly_picks(provider, symbols: list, prices: dict, indicators: dict) -> list:
    """Have the AI analyze all watchlist stocks and rank the top 5.

    Returns list of picks with: symbol, conviction, entry_zone, stop, target, thesis
    """
    # Build a summary prompt with all stocks' data
    stock_summaries = []
    for sym in symbols:
        p = prices.get(sym)
        ind = indicators.get(sym, {})
        if not p:
            continue

        rsi = ind.get("rsi", "?")
        macd_h = ind.get("macd_histogram", "?")
        vol_r = ind.get("volume_ratio", "?")
        sma50 = ind.get("sma_50", "?")
        sma200 = ind.get("sma_200", "?")

        stock_summaries.append(
            f"{sym}: ${p['price']:.2f} ({p['change_pct']:+.2f}%) | "
            f"RSI={rsi} MACD_H={macd_h} Vol={vol_r}x | "
            f"SMA50=${sma50} SMA200=${sma200}"
        )

    if not stock_summaries:
        return []

    stocks_block = "\n".join(stock_summaries)

    prompt = f"""You are a senior swing trader preparing your weekly watchlist for the coming week.

Analyze ALL of the following stocks and pick the TOP 5 with highest conviction for the week ahead.
Consider: technical setup (RSI, MACD, MA), momentum, volume, and recent price action.

Stocks to analyze:
{stocks_block}

For EACH of your top 5 picks, provide:
1. Symbol
2. Conviction score (0.0-1.0)
3. Entry zone (price range to buy)
4. Stop loss level
5. Target price
6. One-sentence thesis

Respond with EXACTLY this format for each pick (5 picks total):

PICK 1:
Symbol: [TICKER]
Conviction: [0.0-1.0]
Entry: [low]-[high]
Stop: [price]
Target: [price]
Thesis: [one sentence]

PICK 2:
... (repeat for all 5)"""

    try:
        response = provider.call_model(prompt)
        picks = _parse_picks(response, prices)
        return picks
    except Exception as e:
        console.log(f"[red]Weekly picks error: {e}")
        return []


def _parse_picks(text: str, prices: dict) -> list:
    """Parse the AI's response into structured picks."""
    import re
    picks = []
    current = {}

    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue

        if line.upper().startswith("PICK"):
            if current and "symbol" in current:
                picks.append(current)
            current = {}
            continue

        if line.lower().startswith("symbol:"):
            sym = line.split(":", 1)[1].strip().upper()
            # Clean up any extra text
            sym = re.sub(r'[^A-Z]', '', sym[:6])
            current["symbol"] = sym
            p = prices.get(sym, {})
            current["current_price"] = p.get("price", 0) if p else 0

        elif line.lower().startswith("conviction:"):
            nums = re.findall(r'[\d.]+', line)
            if nums:
                val = float(nums[0])
                current["conviction"] = val if val <= 1 else val / 100

        elif line.lower().startswith("entry:"):
            val = line.split(":", 1)[1].strip()
            current["entry_zone"] = val

        elif line.lower().startswith("stop:"):
            nums = re.findall(r'[\d.]+', line)
            if nums:
                current["stop"] = float(nums[0])

        elif line.lower().startswith("target:"):
            nums = re.findall(r'[\d.]+', line)
            if nums:
                current["target"] = float(nums[0])

        elif line.lower().startswith("thesis:"):
            current["thesis"] = line.split(":", 1)[1].strip()

    # Don't forget last pick
    if current and "symbol" in current:
        picks.append(current)

    return picks[:5]


def format_weekly_telegram(picks: list) -> str:
    """Format picks as a Telegram HTML message."""
    if not picks:
        return ""

    today = datetime.now().strftime("%B %d, %Y")
    lines = [
        f"<b>TradeMinds Weekly 5</b>",
        f"<i>{today}</i>",
        "",
    ]

    for i, pick in enumerate(picks, 1):
        sym = pick.get("symbol", "?")
        conv = pick.get("conviction", 0)
        entry = pick.get("entry_zone", "?")
        stop = pick.get("stop", 0)
        target = pick.get("target", 0)
        thesis = pick.get("thesis", "")
        price = pick.get("current_price", 0)

        stars = int(conv * 5)
        star_str = "*" * stars + "-" * (5 - stars)

        lines.append(f"<b>#{i} {sym}</b> ${price:.2f}")
        lines.append(f"  Conviction: {conv:.0%} [{star_str}]")
        lines.append(f"  Entry: {entry}")
        lines.append(f"  Stop: ${stop:.2f}")
        lines.append(f"  Target: ${target:.2f}")
        lines.append(f"  {thesis}")
        lines.append("")

    lines.append("<i>Powered by TradeMinds AI Arena</i>")
    return "\n".join(lines)


def save_weekly_picks(picks: list):
    """Save picks to DB for dashboard display."""
    conn = _conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weekly_picks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, conviction REAL, entry_zone TEXT,
            stop_price REAL, target_price REAL, thesis TEXT,
            current_price REAL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    for pick in picks:
        conn.execute(
            "INSERT INTO weekly_picks (symbol, conviction, entry_zone, stop_price, target_price, thesis, current_price) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (pick.get("symbol"), pick.get("conviction", 0), pick.get("entry_zone", ""),
             pick.get("stop", 0), pick.get("target", 0), pick.get("thesis", ""),
             pick.get("current_price", 0))
        )
    conn.commit()
    conn.close()


def get_weekly_picks(limit: int = 5) -> list:
    """Get the most recent weekly picks."""
    conn = _conn()
    try:
        conn.execute("SELECT 1 FROM weekly_picks LIMIT 1")
    except Exception:
        conn.close()
        return []
    picks = conn.execute(
        "SELECT * FROM weekly_picks ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(p) for p in picks]


def run_weekly_picks(provider, symbols: list):
    """Full weekly picks pipeline: analyze, save, send Telegram."""
    from engine.market_data import get_stock_price, get_technical_indicators

    prices = {}
    indicators = {}
    for sym in symbols:
        data = get_stock_price(sym)
        if "error" not in data:
            prices[sym] = data
        ind = get_technical_indicators(sym)
        if ind:
            indicators[sym] = ind

    picks = generate_weekly_picks(provider, symbols, prices, indicators)
    if not picks:
        console.log("[yellow]Weekly picks: no picks generated")
        return

    save_weekly_picks(picks)

    # Send Telegram
    try:
        from engine.telegram_alerts import send_alert
        msg = format_weekly_telegram(picks)
        if msg:
            send_alert(msg)
            console.log(f"[green]Weekly picks sent: {', '.join(p['symbol'] for p in picks)}")
    except Exception as e:
        console.log(f"[red]Weekly picks Telegram error: {e}")
