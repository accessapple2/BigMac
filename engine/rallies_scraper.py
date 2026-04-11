"""engine/rallies_scraper.py — Rallies.ai + NoF1.ai live scraper

Scrapes JavaScript-rendered arena pages using Scrapling's PlayWrightFetcher.
Stores results in data/alpha_signals.db — NEVER touches trader.db or arena.db.

Signals produced (feed into composite_alpha):
  11. rallies_consensus        (weight 5%) — AI model consensus on ticker
  12. rallies_debate_sentiment (weight 5%) — buy/sell signal density from debate log

Schedule: hourly during market hours (9:30 AM – 4:00 PM ET, Mon–Fri).
Rate limit: 60-second delay between page loads.
Snapshots: data/rallies_snapshots/<source>_<YYYYMMDD_HHMMSS>.html
"""
from __future__ import annotations

import logging
import re
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

# ── Paths ───────────────────────────────────────────────────────────────────────
_ROOT      = Path(__file__).resolve().parent.parent
DB_PATH    = _ROOT / "data" / "alpha_signals.db"
SNAP_DIR   = _ROOT / "data" / "rallies_snapshots"

SNAP_DIR.mkdir(parents=True, exist_ok=True)

# ── Logging ─────────────────────────────────────────────────────────────────────
logger = logging.getLogger("rallies_scraper")

# ── Scrape targets ──────────────────────────────────────────────────────────────
RALLIES_ARENA_URL  = "https://rallies.ai/arena"
NOF1_LEADERBOARD_URL = "https://nof1.ai/leaderboard"

PAGE_LOAD_DELAY    = 60        # seconds between page fetches
MARKET_OPEN_ET     = (9, 30)   # 9:30 AM ET
MARKET_CLOSE_ET    = (16, 0)   # 4:00 PM ET

# ET = UTC-4 (EDT) or UTC-5 (EST). Use UTC-4 (summer/EDT) as conservative default.
# The check uses pytz or a fixed offset — pytz may not be installed, so use fixed.
_ET_OFFSET = timedelta(hours=-4)   # EDT (DST active Apr–Nov)

# ── Ticker recognizer ───────────────────────────────────────────────────────────
_TICKER_RE = re.compile(r'\b([A-Z]{1,5})\b')
_COMMON_TICKERS = {
    "NVDA", "TSLA", "AAPL", "AMD", "META", "MSFT", "GOOGL", "AMZN",
    "SPY", "QQQ", "TQQQ", "MU", "ORCL", "NOW", "AVGO", "PLTR", "DELL",
    "LMT", "LNG", "CVS", "UBER", "CRM", "VST", "JPM", "PTC", "EOG",
    "HIMS", "SMCI", "VRT", "UNH", "XLE", "GS", "BA", "NFLX", "DIS",
    "INTC", "COIN", "MARA", "SQ", "SHOP", "ARM", "SNOW", "CRWD", "PANW",
    "ZS", "NET", "DDOG", "ABBV", "WMT", "COST", "TGT", "HD", "LOW",
    "SOFI", "MRVL", "SMR", "STAA", "BAC", "GOOG", "UBER", "LYFT", "RBLX",
    "HOOD", "RIVN", "LCID", "NIO", "XPEV", "F", "GM", "FORD",
}

_BUY_WORDS  = {"buy", "long", "bullish", "accumulate", "add", "overweight",
               "strong", "upside", "breakout", "beat", "surge", "rally",
               "growth", "positive", "outperform", "upgrade"}
_SELL_WORDS = {"sell", "short", "bearish", "reduce", "trim", "underweight",
               "weak", "downside", "breakdown", "miss", "drop", "decline",
               "negative", "underperform", "downgrade", "caution", "warning"}


# ── DB ──────────────────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(DB_PATH), timeout=15)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    return c


def init_tables() -> None:
    """Create rallies + nof1 tables in alpha_signals.db."""
    conn = _conn()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS rallies_leaderboard (
        id           INTEGER PRIMARY KEY,
        scraped_at   TEXT NOT NULL,
        rank         INTEGER,
        model_name   TEXT NOT NULL,
        return_pct   REAL,
        portfolio_value REAL,
        trade_count  INTEGER,
        win_rate     REAL,
        source       TEXT DEFAULT 'rallies.ai'
    );
    CREATE INDEX IF NOT EXISTS idx_rlb_scraped ON rallies_leaderboard(scraped_at);

    CREATE TABLE IF NOT EXISTS rallies_portfolios (
        id           INTEGER PRIMARY KEY,
        scraped_at   TEXT NOT NULL,
        model_name   TEXT NOT NULL,
        ticker       TEXT NOT NULL,
        position_size_pct REAL,
        shares       REAL,
        value        REAL,
        recent_action TEXT,
        entry_price  REAL,
        current_price REAL,
        pnl_pct      REAL
    );
    CREATE INDEX IF NOT EXISTS idx_rp_ticker ON rallies_portfolios(ticker, scraped_at);

    CREATE TABLE IF NOT EXISTS rallies_debate_log (
        id           INTEGER PRIMARY KEY,
        scraped_at   TEXT NOT NULL,
        model_name   TEXT NOT NULL,
        message_text TEXT,
        tickers_mentioned TEXT DEFAULT '[]',
        sentiment    REAL DEFAULT 0,
        signal_type  TEXT DEFAULT 'neutral',
        msg_timestamp TEXT,
        buy_word_count  INTEGER DEFAULT 0,
        sell_word_count INTEGER DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_rdl_scraped ON rallies_debate_log(scraped_at);

    CREATE TABLE IF NOT EXISTS nof1_leaderboard (
        id           INTEGER PRIMARY KEY,
        scraped_at   TEXT NOT NULL,
        rank         INTEGER,
        model_name   TEXT NOT NULL,
        return_pct   REAL,
        portfolio_value REAL,
        trade_count  INTEGER,
        win_rate     REAL,
        source       TEXT DEFAULT 'nof1.ai'
    );
    CREATE INDEX IF NOT EXISTS idx_nof1_scraped ON nof1_leaderboard(scraped_at);
    """)
    conn.commit()
    conn.close()
    logger.info("rallies tables initialized in alpha_signals.db")


# ── Snapshot saving ─────────────────────────────────────────────────────────────

def _save_snapshot(source: str, html: str) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = SNAP_DIR / f"{source}_{ts}.html"
    path.write_text(html, encoding="utf-8", errors="replace")
    logger.debug(f"Snapshot saved: {path.name} ({len(html):,} bytes)")
    return path


# ── PlayWright fetcher ──────────────────────────────────────────────────────────

def _pw_fetch(url: str, page_action=None, wait_selector: Optional[str] = None,
              timeout_ms: int = 45000) -> Optional[str]:
    """Fetch a JS-rendered page and return its HTML string, or None on failure."""
    try:
        from scrapling.fetchers import PlayWrightFetcher
    except ImportError:
        logger.error("scrapling not available — pip install scrapling")
        return None

    try:
        response = PlayWrightFetcher.fetch(
            url,
            headless=True,
            network_idle=True,
            timeout=timeout_ms,
            wait=2000,                   # extra 2s after networkidle
            page_action=page_action,
            wait_selector=wait_selector,
            disable_resources=False,     # need JS/CSS for proper render
            stealth=False,
            google_search=False,
        )
        return response.html
    except Exception as e:
        logger.error(f"PlayWright fetch failed for {url}: {e}")
        return None


# ── Tab-click page actions ──────────────────────────────────────────────────────

def _make_tab_clicker(tab_text: str):
    """Return an async page_action that clicks a tab by visible text."""
    async def _click_tab(page):
        try:
            # Try multiple selector patterns for tab buttons
            selectors = [
                f'button:has-text("{tab_text}")',
                f'[role="tab"]:has-text("{tab_text}")',
                f'a:has-text("{tab_text}")',
                f'li:has-text("{tab_text}")',
                f'[data-tab="{tab_text.lower()}")',
                f'.tab:has-text("{tab_text}")',
                f'nav >> text={tab_text}',
            ]
            clicked = False
            for sel in selectors:
                try:
                    await page.click(sel, timeout=3000)
                    clicked = True
                    break
                except Exception:
                    continue
            if clicked:
                await page.wait_for_load_state("networkidle", timeout=10000)
                await page.wait_for_timeout(1500)
            else:
                logger.warning(f"Could not find tab: '{tab_text}'")
        except Exception as e:
            logger.warning(f"Tab click error for '{tab_text}': {e}")
        return page
    return _click_tab


# ── Leaderboard parser ──────────────────────────────────────────────────────────

def _parse_leaderboard(html: str) -> list[dict]:
    """Extract rank/model/return/value/trades from leaderboard HTML."""
    rows: list[dict] = []
    soup = BeautifulSoup(html, "html.parser")
    scraped_at = datetime.utcnow().isoformat()

    # Try to find table rows first
    table_rows = soup.select("table tbody tr, [class*='leaderboard'] tr, [class*='ranking'] tr")
    if not table_rows:
        # Fallback: look for card-style layout
        table_rows = soup.select("[class*='card'], [class*='model'], [class*='player'], [class*='agent']")

    for i, row in enumerate(table_rows):
        cells = row.find_all(["td", "th"])
        text  = row.get_text(" ", strip=True)
        if not text or len(text) < 5:
            continue

        # Try structured cells first
        if len(cells) >= 3:
            row_data = _extract_leaderboard_row_from_cells(cells, i + 1, scraped_at)
        else:
            row_data = _extract_leaderboard_row_from_text(text, i + 1, scraped_at)

        if row_data:
            rows.append(row_data)

    if not rows:
        logger.warning("Leaderboard: no rows parsed — site structure may have changed, check snapshot")
    else:
        logger.info(f"Leaderboard: parsed {len(rows)} rows")

    return rows


def _extract_leaderboard_row_from_cells(cells, rank: int, scraped_at: str) -> Optional[dict]:
    texts = [c.get_text(strip=True) for c in cells]
    if len(texts) < 2:
        return None

    model_name  = _find_model_name(texts)
    return_pct  = _find_pct(texts)
    portfolio_v = _find_dollar(texts)
    trade_count = _find_int(texts, exclude_rank=True)
    win_rate    = _find_win_rate(texts)

    if not model_name:
        return None

    return {
        "scraped_at": scraped_at,
        "rank": rank,
        "model_name": model_name,
        "return_pct": return_pct,
        "portfolio_value": portfolio_v,
        "trade_count": trade_count,
        "win_rate": win_rate,
    }


def _extract_leaderboard_row_from_text(text: str, rank: int, scraped_at: str) -> Optional[dict]:
    model_name  = _find_model_name_in_text(text)
    return_pct  = _parse_pct_from_text(text)
    portfolio_v = _parse_dollar_from_text(text)
    if not model_name:
        return None
    return {
        "scraped_at": scraped_at,
        "rank": rank,
        "model_name": model_name,
        "return_pct": return_pct,
        "portfolio_value": portfolio_v,
        "trade_count": None,
        "win_rate": None,
    }


# ── Portfolios parser ───────────────────────────────────────────────────────────

def _parse_portfolios(html: str) -> list[dict]:
    """Extract per-model holdings from the Portfolios tab."""
    rows: list[dict] = []
    soup = BeautifulSoup(html, "html.parser")
    scraped_at = datetime.utcnow().isoformat()

    # Each portfolio section should have a model name header + position table
    sections = soup.select(
        "[class*='portfolio'], [class*='holdings'], [class*='positions'], "
        "[class*='model-card'], section, article"
    )

    if not sections:
        # Last resort: find all tables that contain ticker-like content
        sections = soup.select("table")

    current_model = "Unknown"
    for section in sections:
        # Check if section has a model name header
        header = section.find(["h1", "h2", "h3", "h4", "span", "div"],
                               class_=re.compile(r'name|model|title|header', re.I))
        if header:
            candidate = header.get_text(strip=True)
            if _looks_like_model_name(candidate):
                current_model = candidate

        # Find position rows within this section
        pos_rows = section.select("tr, [class*='row'], [class*='position'], [class*='holding']")
        for prow in pos_rows:
            text = prow.get_text(" ", strip=True)
            tickers = _extract_tickers_from_text(text)
            if not tickers:
                continue
            for ticker in tickers:
                rows.append({
                    "scraped_at":         scraped_at,
                    "model_name":         current_model,
                    "ticker":             ticker,
                    "position_size_pct":  _parse_pct_from_text(text),
                    "shares":             None,
                    "value":              _parse_dollar_from_text(text),
                    "recent_action":      _parse_action_from_text(text),
                    "entry_price":        None,
                    "current_price":      None,
                    "pnl_pct":            _parse_pct_from_text(text, skip_first=True),
                })

    if not rows:
        logger.warning("Portfolios: no positions parsed — check snapshot")
    else:
        logger.info(f"Portfolios: parsed {len(rows)} position rows across models")

    return rows


# ── Debate log parser ───────────────────────────────────────────────────────────

def _parse_debate_log(html: str) -> list[dict]:
    """Extract AI messages, ticker mentions, and sentiment from the Feed/Chat tab."""
    rows: list[dict] = []
    soup = BeautifulSoup(html, "html.parser")
    scraped_at = datetime.utcnow().isoformat()

    # Try common message/chat selectors
    messages = soup.select(
        "[class*='message'], [class*='comment'], [class*='feed-item'], "
        "[class*='post'], [class*='chat'], [class*='debate'], "
        "[class*='entry'], [class*='activity']"
    )

    if not messages:
        # Broader fallback: any <p> or <div> with substantial text
        messages = [el for el in soup.find_all(["p", "div", "article"])
                    if len(el.get_text(strip=True)) > 40]

    for msg in messages:
        text = msg.get_text(" ", strip=True)
        if len(text) < 20:
            continue

        model_name = _find_model_name_in_text(text) or _find_author_from_element(msg)
        if not model_name:
            continue

        tickers   = _extract_tickers_from_text(text)
        buy_cnt   = sum(1 for w in _BUY_WORDS  if w in text.lower())
        sell_cnt  = sum(1 for w in _SELL_WORDS if w in text.lower())
        sentiment = _compute_text_sentiment(buy_cnt, sell_cnt)
        sig_type  = "buy" if sentiment > 0.3 else ("sell" if sentiment < -0.3 else "neutral")

        ts_el = msg.find(attrs={"class": re.compile(r'time|date|stamp|ago', re.I)})
        msg_ts = ts_el.get_text(strip=True) if ts_el else None

        rows.append({
            "scraped_at":        scraped_at,
            "model_name":        model_name,
            "message_text":      text[:2000],
            "tickers_mentioned": str(tickers),
            "sentiment":         sentiment,
            "signal_type":       sig_type,
            "msg_timestamp":     msg_ts,
            "buy_word_count":    buy_cnt,
            "sell_word_count":   sell_cnt,
        })

    if not rows:
        logger.warning("Debate log: no messages parsed — check snapshot")
    else:
        logger.info(f"Debate log: parsed {len(rows)} messages")

    return rows


# ── NoF1 parser ─────────────────────────────────────────────────────────────────

def _parse_nof1(html: str) -> list[dict]:
    """Extract leaderboard from nof1.ai/leaderboard."""
    rows: list[dict] = []
    soup = BeautifulSoup(html, "html.parser")
    scraped_at = datetime.utcnow().isoformat()

    table_rows = soup.select("table tbody tr, [class*='leaderboard'] tr, [class*='rank'] tr")
    if not table_rows:
        table_rows = soup.select("[class*='card'], [class*='entry'], [class*='row']")

    for i, row in enumerate(table_rows):
        text  = row.get_text(" ", strip=True)
        cells = row.find_all(["td", "th"])
        if not text or len(text) < 5:
            continue

        texts = [c.get_text(strip=True) for c in cells] if cells else [text]
        model_name = _find_model_name(texts) or _find_model_name_in_text(text)
        if not model_name:
            continue

        rows.append({
            "scraped_at":      scraped_at,
            "rank":            i + 1,
            "model_name":      model_name,
            "return_pct":      _find_pct(texts) or _parse_pct_from_text(text),
            "portfolio_value": _find_dollar(texts) or _parse_dollar_from_text(text),
            "trade_count":     _find_int(texts, exclude_rank=True),
            "win_rate":        _find_win_rate(texts),
        })

    if not rows:
        logger.warning("NoF1: no rows parsed — check snapshot")
    else:
        logger.info(f"NoF1 leaderboard: parsed {len(rows)} rows")

    return rows


# ── DB save helpers ─────────────────────────────────────────────────────────────

def _save_leaderboard(rows: list[dict]) -> None:
    if not rows:
        return
    conn = _conn()
    conn.executemany("""
        INSERT INTO rallies_leaderboard
        (scraped_at, rank, model_name, return_pct, portfolio_value, trade_count, win_rate)
        VALUES (:scraped_at, :rank, :model_name, :return_pct, :portfolio_value, :trade_count, :win_rate)
    """, rows)
    conn.commit(); conn.close()
    logger.info(f"Saved {len(rows)} rallies_leaderboard rows")


def _save_portfolios(rows: list[dict]) -> None:
    if not rows:
        return
    conn = _conn()
    conn.executemany("""
        INSERT INTO rallies_portfolios
        (scraped_at, model_name, ticker, position_size_pct, shares, value,
         recent_action, entry_price, current_price, pnl_pct)
        VALUES (:scraped_at, :model_name, :ticker, :position_size_pct, :shares, :value,
                :recent_action, :entry_price, :current_price, :pnl_pct)
    """, rows)
    conn.commit(); conn.close()
    logger.info(f"Saved {len(rows)} rallies_portfolios rows")


def _save_debate_log(rows: list[dict]) -> None:
    if not rows:
        return
    conn = _conn()
    conn.executemany("""
        INSERT INTO rallies_debate_log
        (scraped_at, model_name, message_text, tickers_mentioned, sentiment,
         signal_type, msg_timestamp, buy_word_count, sell_word_count)
        VALUES (:scraped_at, :model_name, :message_text, :tickers_mentioned, :sentiment,
                :signal_type, :msg_timestamp, :buy_word_count, :sell_word_count)
    """, rows)
    conn.commit(); conn.close()
    logger.info(f"Saved {len(rows)} rallies_debate_log rows")


def _save_nof1(rows: list[dict]) -> None:
    if not rows:
        return
    conn = _conn()
    conn.executemany("""
        INSERT INTO nof1_leaderboard
        (scraped_at, rank, model_name, return_pct, portfolio_value, trade_count, win_rate)
        VALUES (:scraped_at, :rank, :model_name, :return_pct, :portfolio_value, :trade_count, :win_rate)
    """, rows)
    conn.commit(); conn.close()
    logger.info(f"Saved {len(rows)} nof1_leaderboard rows")


# ── Signal computation (for composite_alpha) ────────────────────────────────────

def get_rallies_consensus_score(sym: str, lookback_hours: int = 4) -> Optional[float]:
    """
    Signal 11: rallies_consensus for symbol.

    Looks at recent portfolio holdings:
      - Bullish if multiple top-ranked models hold the symbol
      - Bearish if models recently sold / reduced

    Returns -2.0 to +2.0 (None if no data).
    """
    try:
        conn = _conn()
        since = (datetime.utcnow() - timedelta(hours=lookback_hours)).isoformat()

        # Models holding this ticker
        holders = conn.execute("""
            SELECT COUNT(DISTINCT model_name) as cnt,
                   AVG(position_size_pct) as avg_pct
            FROM rallies_portfolios
            WHERE ticker = ? AND scraped_at >= ?
              AND recent_action NOT IN ('sell', 'sold', 'exit', 'close')
        """, (sym, since)).fetchone()

        sellers = conn.execute("""
            SELECT COUNT(DISTINCT model_name) as cnt
            FROM rallies_portfolios
            WHERE ticker = ? AND scraped_at >= ?
              AND recent_action IN ('sell', 'sold', 'exit', 'close', 'reduce')
        """, (sym, since)).fetchone()

        total_models = conn.execute("""
            SELECT COUNT(DISTINCT model_name) FROM rallies_leaderboard
            WHERE scraped_at >= ?
        """, (since,)).fetchone()[0] or 1

        conn.close()

        hold_cnt = (holders["cnt"] or 0) if holders else 0
        sell_cnt = (sellers["cnt"] or 0) if sellers else 0

        if hold_cnt == 0 and sell_cnt == 0:
            return None

        bull_ratio = hold_cnt / total_models
        bear_ratio = sell_cnt / total_models

        # Scale to -2 to +2
        score = (bull_ratio - bear_ratio) * 2.0
        return round(max(-2.0, min(2.0, score)), 3)

    except Exception as e:
        logger.error(f"get_rallies_consensus_score({sym}): {e}")
        return None


def get_rallies_debate_sentiment(sym: str, lookback_hours: int = 4) -> Optional[float]:
    """
    Signal 12: rallies_debate_sentiment for symbol.

    Aggregates buy/sell word counts from debate messages mentioning the symbol.
    Returns -2.0 to +2.0 (None if no data).
    """
    try:
        conn = _conn()
        since = (datetime.utcnow() - timedelta(hours=lookback_hours)).isoformat()

        msgs = conn.execute("""
            SELECT sentiment, buy_word_count, sell_word_count
            FROM rallies_debate_log
            WHERE scraped_at >= ?
              AND tickers_mentioned LIKE ?
        """, (since, f"%'{sym}'%")).fetchall()

        conn.close()

        if not msgs:
            return None

        total_buy  = sum(r["buy_word_count"]  for r in msgs)
        total_sell = sum(r["sell_word_count"] for r in msgs)
        avg_sent   = sum(r["sentiment"] for r in msgs) / len(msgs)

        # Weight: 60% avg sentiment, 40% buy/sell word ratio
        word_total = total_buy + total_sell
        word_ratio = (total_buy - total_sell) / word_total if word_total > 0 else 0.0
        combined   = 0.6 * avg_sent + 0.4 * word_ratio * 2.0

        return round(max(-2.0, min(2.0, combined)), 3)

    except Exception as e:
        logger.error(f"get_rallies_debate_sentiment({sym}): {e}")
        return None


def get_all_rallies_consensus_scores(lookback_hours: int = 4) -> dict[str, float]:
    """Return {sym: score} for all tickers seen in recent portfolios."""
    try:
        conn = _conn()
        since = (datetime.utcnow() - timedelta(hours=lookback_hours)).isoformat()
        tickers = conn.execute("""
            SELECT DISTINCT ticker FROM rallies_portfolios WHERE scraped_at >= ?
        """, (since,)).fetchall()
        conn.close()
        result = {}
        for row in tickers:
            sym = row["ticker"]
            s = get_rallies_consensus_score(sym, lookback_hours)
            if s is not None:
                result[sym] = s
        return result
    except Exception as e:
        logger.error(f"get_all_rallies_consensus_scores: {e}")
        return {}


def get_all_rallies_debate_scores(lookback_hours: int = 4) -> dict[str, float]:
    """Return {sym: score} for all tickers mentioned in recent debate log."""
    try:
        conn = _conn()
        since = (datetime.utcnow() - timedelta(hours=lookback_hours)).isoformat()
        rows = conn.execute("""
            SELECT tickers_mentioned FROM rallies_debate_log WHERE scraped_at >= ?
        """, (since,)).fetchall()
        conn.close()

        all_tickers: set[str] = set()
        for row in rows:
            raw = row["tickers_mentioned"] or "[]"
            # parse list-like string "['NVDA', 'AAPL']"
            found = re.findall(r"'([A-Z]{1,5})'", raw)
            all_tickers.update(found)

        result = {}
        for sym in all_tickers:
            s = get_rallies_debate_sentiment(sym, lookback_hours)
            if s is not None:
                result[sym] = s
        return result
    except Exception as e:
        logger.error(f"get_all_rallies_debate_scores: {e}")
        return {}


# ── Main scrape cycle ────────────────────────────────────────────────────────────

def run_once() -> dict:
    """
    Full scrape cycle:
      1. rallies.ai/arena — Leaderboard tab
      2. rallies.ai/arena — Portfolios tab     (60s delay)
      3. rallies.ai/arena — Feed/Chat tab      (60s delay)
      4. nof1.ai/leaderboard                   (60s delay)

    Returns summary dict.
    """
    init_tables()
    summary = {
        "started_at": datetime.utcnow().isoformat(),
        "leaderboard_rows": 0,
        "portfolio_rows": 0,
        "debate_rows": 0,
        "nof1_rows": 0,
        "errors": [],
    }

    # ── 1. Rallies leaderboard tab ──────────────────────────────────────────────
    logger.info("[1/4] Scraping rallies.ai/arena — Leaderboard tab...")
    try:
        html = _pw_fetch(
            RALLIES_ARENA_URL,
            page_action=_make_tab_clicker("Leaderboard"),
            wait_selector="table, [class*='leaderboard'], [class*='ranking']",
        )
        if html:
            _save_snapshot("rallies_leaderboard", html)
            rows = _parse_leaderboard(html)
            _save_leaderboard(rows)
            summary["leaderboard_rows"] = len(rows)
        else:
            summary["errors"].append("leaderboard: fetch returned None")
    except Exception as e:
        logger.error(f"Leaderboard scrape error: {e}")
        summary["errors"].append(f"leaderboard: {e}")

    # ── Rate limit ──────────────────────────────────────────────────────────────
    logger.info(f"Rate limit: sleeping {PAGE_LOAD_DELAY}s...")
    time.sleep(PAGE_LOAD_DELAY)

    # ── 2. Rallies portfolios tab ───────────────────────────────────────────────
    logger.info("[2/4] Scraping rallies.ai/arena — Portfolios tab...")
    try:
        html = _pw_fetch(
            RALLIES_ARENA_URL,
            page_action=_make_tab_clicker("Portfolios"),
            wait_selector="[class*='portfolio'], [class*='holdings'], table",
        )
        if html:
            _save_snapshot("rallies_portfolios", html)
            rows = _parse_portfolios(html)
            _save_portfolios(rows)
            summary["portfolio_rows"] = len(rows)
        else:
            summary["errors"].append("portfolios: fetch returned None")
    except Exception as e:
        logger.error(f"Portfolios scrape error: {e}")
        summary["errors"].append(f"portfolios: {e}")

    time.sleep(PAGE_LOAD_DELAY)

    # ── 3. Rallies feed/chat/debate tab ────────────────────────────────────────
    logger.info("[3/4] Scraping rallies.ai/arena — Feed/Chat tab...")
    try:
        # Try "Feed" first, then "Chat", then "Debate"
        for tab_name in ("Feed", "Chat", "Debate", "Activity"):
            html = _pw_fetch(
                RALLIES_ARENA_URL,
                page_action=_make_tab_clicker(tab_name),
                wait_selector="[class*='message'], [class*='feed'], [class*='chat']",
                timeout_ms=30000,
            )
            if html and len(html) > 5000:
                _save_snapshot("rallies_debate", html)
                rows = _parse_debate_log(html)
                if rows:
                    _save_debate_log(rows)
                    summary["debate_rows"] = len(rows)
                    break
        else:
            logger.warning("No debate/feed content found across tab variants")
    except Exception as e:
        logger.error(f"Debate log scrape error: {e}")
        summary["errors"].append(f"debate: {e}")

    time.sleep(PAGE_LOAD_DELAY)

    # ── 4. NoF1 leaderboard ────────────────────────────────────────────────────
    logger.info("[4/4] Scraping nof1.ai/leaderboard...")
    try:
        html = _pw_fetch(
            NOF1_LEADERBOARD_URL,
            wait_selector="table, [class*='leaderboard'], [class*='ranking']",
        )
        if html:
            _save_snapshot("nof1_leaderboard", html)
            rows = _parse_nof1(html)
            _save_nof1(rows)
            summary["nof1_rows"] = len(rows)
        else:
            summary["errors"].append("nof1: fetch returned None")
    except Exception as e:
        logger.error(f"NoF1 scrape error: {e}")
        summary["errors"].append(f"nof1: {e}")

    summary["completed_at"] = datetime.utcnow().isoformat()
    logger.info(
        f"Scrape cycle complete — lb={summary['leaderboard_rows']} "
        f"pf={summary['portfolio_rows']} debate={summary['debate_rows']} "
        f"nof1={summary['nof1_rows']} errors={len(summary['errors'])}"
    )
    return summary


# ── Market hours check ───────────────────────────────────────────────────────────

def _is_market_hours() -> bool:
    """Return True if current ET time is within market hours on a weekday."""
    now_utc = datetime.now(timezone.utc)
    now_et  = now_utc + _ET_OFFSET
    if now_et.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    open_time  = now_et.replace(hour=MARKET_OPEN_ET[0],  minute=MARKET_OPEN_ET[1],  second=0, microsecond=0)
    close_time = now_et.replace(hour=MARKET_CLOSE_ET[0], minute=MARKET_CLOSE_ET[1], second=0, microsecond=0)
    return open_time <= now_et <= close_time


# ── Hourly scheduler ─────────────────────────────────────────────────────────────

def run_hourly_loop() -> None:
    """
    Blocking loop: runs run_once() every hour during market hours (9:30–16:00 ET).
    Call from a daemon thread in main.py.
    """
    logger.info("Rallies scraper hourly loop started")
    last_run: Optional[datetime] = None

    while True:
        try:
            if _is_market_hours():
                now = datetime.utcnow()
                if last_run is None or (now - last_run).total_seconds() >= 3600:
                    logger.info("Market hours — starting scrape cycle")
                    run_once()
                    last_run = datetime.utcnow()
                else:
                    secs_until = 3600 - (now - last_run).total_seconds()
                    logger.debug(f"Next scrape in {secs_until:.0f}s")
            else:
                logger.debug("Outside market hours — skipping scrape")
        except Exception as e:
            logger.error(f"Hourly loop error: {e}")

        time.sleep(60)   # check every minute


# ── Parsing utilities ─────────────────────────────────────────────────────────────

# Known AI model fragments for name detection
_MODEL_FRAGMENTS = [
    "grok", "claude", "sonnet", "opus", "haiku", "gpt", "gemini", "deepseek",
    "qwen", "llama", "mistral", "kimi", "command", "cohere", "perplexity",
    "copilot", "o1", "o3", "o4", "phi", "yi-", "mixtral", "palm", "bard",
    "nova", "titan", "bedrock", "jurassic", "falcon",
]

def _find_model_name(texts: list[str]) -> Optional[str]:
    for t in texts:
        if _looks_like_model_name(t):
            return t.strip()
    return None

def _looks_like_model_name(s: str) -> bool:
    if not s or len(s) < 3 or len(s) > 60:
        return False
    sl = s.lower()
    return any(frag in sl for frag in _MODEL_FRAGMENTS)

def _find_model_name_in_text(text: str) -> Optional[str]:
    tl = text.lower()
    for frag in _MODEL_FRAGMENTS:
        idx = tl.find(frag)
        if idx >= 0:
            # Extract surrounding word
            start = max(0, idx - 10)
            end   = min(len(text), idx + 30)
            chunk = text[start:end].split()[0:4]
            return " ".join(chunk).strip(".,;:")
    return None

def _find_author_from_element(el) -> Optional[str]:
    author = el.find(attrs={"class": re.compile(r'author|name|model|user|agent', re.I)})
    if author:
        t = author.get_text(strip=True)
        if t:
            return t
    return None

def _find_pct(texts: list[str]) -> Optional[float]:
    for t in texts:
        v = _parse_pct_from_text(t)
        if v is not None:
            return v
    return None

def _parse_pct_from_text(text: str, skip_first: bool = False) -> Optional[float]:
    matches = re.findall(r'([+-]?\d{1,5}\.?\d{0,2})\s*%', text)
    if skip_first and len(matches) > 1:
        matches = matches[1:]
    for m in matches:
        try:
            v = float(m)
            if -500 < v < 10000:
                return v
        except ValueError:
            pass
    return None

def _find_dollar(texts: list[str]) -> Optional[float]:
    for t in texts:
        v = _parse_dollar_from_text(t)
        if v is not None:
            return v
    return None

def _parse_dollar_from_text(text: str) -> Optional[float]:
    m = re.search(r'\$\s*([\d,]+\.?\d*)\s*([KMkm]?)', text)
    if m:
        try:
            val = float(m.group(1).replace(",", ""))
            suffix = m.group(2).upper()
            if suffix == "K":
                val *= 1_000
            elif suffix == "M":
                val *= 1_000_000
            return val
        except ValueError:
            pass
    return None

def _find_int(texts: list[str], exclude_rank: bool = False) -> Optional[int]:
    for t in texts:
        m = re.search(r'\b(\d{1,5})\b', t)
        if m:
            try:
                v = int(m.group(1))
                if exclude_rank and v <= 30:
                    continue
                if 1 <= v <= 99999:
                    return v
            except ValueError:
                pass
    return None

def _find_win_rate(texts: list[str]) -> Optional[float]:
    for t in texts:
        m = re.search(r'(\d{1,3}\.?\d*)\s*%', t)
        if m:
            try:
                v = float(m.group(1))
                if 0 < v <= 100:
                    return v
            except ValueError:
                pass
    return None

def _extract_tickers_from_text(text: str) -> list[str]:
    found = []
    for match in _TICKER_RE.finditer(text):
        sym = match.group(1)
        if sym in _COMMON_TICKERS:
            found.append(sym)
    return list(dict.fromkeys(found))  # dedupe, preserve order

def _parse_action_from_text(text: str) -> Optional[str]:
    tl = text.lower()
    if any(w in tl for w in ("bought", "buy", "long", "add", "open")):
        return "buy"
    if any(w in tl for w in ("sold", "sell", "short", "exit", "close", "reduce")):
        return "sell"
    return "hold"

def _compute_text_sentiment(buy_cnt: int, sell_cnt: int) -> float:
    total = buy_cnt + sell_cnt
    if total == 0:
        return 0.0
    return round((buy_cnt - sell_cnt) / total * 2.0, 3)


# ── Entry point ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [rallies] %(levelname)s %(message)s")

    if "--loop" in sys.argv:
        run_hourly_loop()
    else:
        result = run_once()
        print(f"\nScrape complete:")
        print(f"  Rallies leaderboard: {result['leaderboard_rows']} rows")
        print(f"  Rallies portfolios:  {result['portfolio_rows']} rows")
        print(f"  Debate log:          {result['debate_rows']} rows")
        print(f"  NoF1 leaderboard:    {result['nof1_rows']} rows")
        if result["errors"]:
            print(f"  Errors: {result['errors']}")
