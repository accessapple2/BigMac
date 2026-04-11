"""Aladdin — BlackRock Intelligence Module for TradeMinds.

Four unified data sources:
  1. iShares ETF Holdings CSV  (IVV, AGG, IAU)
  2. BlackRock Investment Institute commentary (BII)
  3. iShares Fund Flow delta  (shares_outstanding change)
  4. Congress trades filtered for BlackRock / iShares tickers

Output: get_aladdin_brief() → unified macro signal dict.

Sacred rule: never DROP aladdin_signals or aladdin_holdings tables.
"""
from __future__ import annotations

import csv
import io
import json
import logging
import re
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Any

import requests
from rich.console import Console

console = Console()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
_log_handler = logging.FileHandler("logs/aladdin.log")
_log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger = logging.getLogger("aladdin")
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.addHandler(_log_handler)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
_DB = "data/trader.db"
_CACHE: dict[str, Any] = {"brief": None, "ts": 0}
_CACHE_TTL = 14400  # 4 hours

# iShares products (IVV, AGG) + SPDR gold proxy via IAU (iShares gold ETF)
_ISHARES_ETF_URLS = {
    "IVV": (
        "https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/"
        "1467271812596.ajax?fileType=csv&fileName=IVV_holdings&dataType=fund"
    ),
    "AGG": (
        "https://www.ishares.com/us/products/239458/"
        "ishares-core-total-us-bond-market-etf/"
        "1467271812596.ajax?fileType=csv&fileName=AGG_holdings&dataType=fund"
    ),
    # IBIT is BlackRock's Bitcoin ETF — IAU (gold) returns empty CSV (physical bars, no holdings)
    "IBIT": (
        "https://www.ishares.com/us/products/333011/ishares-bitcoin-trust/"
        "1467271812596.ajax?fileType=csv&fileName=IBIT_holdings&dataType=fund"
    ),
}

_BII_URL = "https://www.blackrock.com/us/individual/insights/blackrock-investment-institute"

# Congress tickers / keywords to flag as BlackRock / iShares related
_BLK_TICKERS = {"BLK", "IVV", "AGG", "GLD", "IBIT", "IWM", "EFA", "IAU"}
_BLK_KEYWORDS = ["blackrock", "ishares"]

# Sentiment keyword weights (case-insensitive)
_SENTIMENT_WEIGHTS: dict[str, int] = {
    "overweight": +3,
    "upgrade": +2,
    "risk-on": +2,
    "risk on": +2,
    "bullish": +2,
    "positive": +1,
    "constructive": +1,
    "neutral": 0,
    "underweight": -3,
    "downgrade": -2,
    "risk-off": -2,
    "risk off": -2,
    "bearish": -2,
    "negative": -1,
    "cautious": -1,
    "defensive": -1,
}

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def _ensure_tables() -> None:
    """Create tables if they don't exist. NEVER drops existing tables."""
    try:
        con = sqlite3.connect(_DB, check_same_thread=False)
        con.execute("""
            CREATE TABLE IF NOT EXISTS aladdin_signals (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   TEXT NOT NULL,
                source      TEXT NOT NULL,
                signal      TEXT NOT NULL,
                confidence  INTEGER NOT NULL DEFAULT 50,
                raw_data    TEXT,
                notes       TEXT
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS aladdin_holdings (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                etf_ticker    TEXT NOT NULL,
                holding_ticker TEXT NOT NULL,
                weight        REAL,
                shares        REAL,
                market_value  REAL,
                date          TEXT NOT NULL
            )
        """)
        con.execute(
            "CREATE INDEX IF NOT EXISTS ix_aladdin_holdings_etf_date "
            "ON aladdin_holdings(etf_ticker, date)"
        )
        con.commit()
        con.close()
    except Exception as exc:
        logger.error("_ensure_tables error: %s", exc)


_ensure_tables()


def _db() -> sqlite3.Connection:
    return sqlite3.connect(_DB, check_same_thread=False, timeout=30)


def _db_aladdin() -> sqlite3.Connection:
    """WAL-mode connection for Aladdin writes — avoids contention with main scheduler."""
    con = sqlite3.connect(_DB, check_same_thread=False, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=10000")
    return con


def _save_signal(source: str, signal: str, confidence: int,
                 raw_data: dict | None = None, notes: str = "") -> None:
    try:
        con = _db()
        con.execute(
            "INSERT INTO aladdin_signals (timestamp, source, signal, confidence, raw_data, notes) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                datetime.now().isoformat(),
                source,
                signal,
                confidence,
                json.dumps(raw_data) if raw_data else None,
                notes,
            ),
        )
        con.commit()
        con.close()
    except Exception as exc:
        logger.error("_save_signal error: %s", exc)


# ---------------------------------------------------------------------------
# Source 1 — iShares Holdings CSV
# ---------------------------------------------------------------------------

def _parse_ishares_csv(raw_text: str, etf_ticker: str) -> list[dict]:
    """Parse iShares holdings CSV. Skips metadata header rows."""
    holdings = []
    lines = raw_text.splitlines()

    # iShares CSVs have metadata at the top; data starts after a row that
    # contains 'Ticker' as the first non-empty field.
    data_start = -1
    for i, line in enumerate(lines):
        if line.startswith("Ticker,") or ",Ticker," in line or line.strip().startswith('"Ticker"'):
            data_start = i
            break
        # Also accept header row variations
        parts = [p.strip().strip('"') for p in line.split(",")]
        if parts and parts[0].lower() == "ticker":
            data_start = i
            break

    if data_start == -1:
        # Fallback: find any row with 'Name' or 'Market Value' as first field
        for i, line in enumerate(lines):
            parts = [p.strip().strip('"').lower() for p in line.split(",")]
            if parts and parts[0] in ("name", "security name"):
                data_start = i
                break

    if data_start == -1:
        logger.warning("%s CSV: could not find header row", etf_ticker)
        return holdings

    csv_block = "\n".join(lines[data_start:])
    reader = csv.DictReader(io.StringIO(csv_block))

    for row in reader:
        # Normalise key names (strip whitespace, lower for lookup)
        norm = {
            k.strip().strip('"').lower(): (v or "").strip().strip('"')
            for k, v in row.items()
            if k is not None
        }

        # Bond/gold funds have no Ticker column — use Name as identifier
        ticker = norm.get("ticker", "").upper()
        if not ticker or ticker in ("-", "N/A", ""):
            name = norm.get("name", "").strip()
            if not name or name in ("-", "N/A", ""):
                continue
            # Abbreviate long bond names to first 20 chars for storage
            ticker = name[:20].upper()
        if not ticker:
            continue

        # Weight: try 'weight (%)' or 'weight'
        weight_raw = norm.get("weight (%)", norm.get("weight", "0")) or "0"
        try:
            weight = float(weight_raw.replace(",", "").replace("%", ""))
        except ValueError:
            weight = 0.0

        # Shares
        shares_raw = norm.get("shares", norm.get("quantity", "0")) or "0"
        try:
            shares = float(shares_raw.replace(",", ""))
        except ValueError:
            shares = 0.0

        # Market value
        mv_raw = norm.get("market value", norm.get("market_value", "0")) or "0"
        try:
            market_value = float(mv_raw.replace(",", "").replace("$", ""))
        except ValueError:
            market_value = 0.0

        holdings.append({
            "etf_ticker": etf_ticker,
            "holding_ticker": ticker,
            "weight": weight,
            "shares": shares,
            "market_value": market_value,
        })

        if len(holdings) >= 20:
            break

    return holdings


def fetch_ishares_holdings(etf_ticker: str, url: str) -> list[dict]:
    """Download and store top-20 holdings for one iShares ETF."""
    logger.info("Fetching iShares holdings: %s", etf_ticker)
    try:
        resp = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/124.0.0.0 Safari/537.36",
                "Referer": "https://www.ishares.com/",
            },
            timeout=30,
        )
        resp.raise_for_status()
        raw = resp.text
    except Exception as exc:
        logger.error("iShares CSV fetch error (%s): %s", etf_ticker, exc)
        return []

    holdings = _parse_ishares_csv(raw, etf_ticker)
    if not holdings:
        logger.warning("iShares CSV parse returned 0 holdings for %s", etf_ticker)
        return []

    today = datetime.now().strftime("%Y-%m-%d")
    rows = [
        (h["etf_ticker"], h["holding_ticker"], h["weight"],
         h["shares"], h["market_value"], today)
        for h in holdings
    ]
    for attempt in range(3):
        try:
            con = _db_aladdin()
            con.execute(
                "DELETE FROM aladdin_holdings WHERE etf_ticker=? AND date=?",
                (etf_ticker, today),
            )
            con.executemany(
                "INSERT INTO aladdin_holdings "
                "(etf_ticker, holding_ticker, weight, shares, market_value, date) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                rows,
            )
            con.commit()
            con.close()
            logger.info("Stored %d holdings for %s (%s)", len(holdings), etf_ticker, today)
            break
        except Exception as exc:
            if attempt < 2:
                logger.warning("iShares DB write retry %d (%s): %s", attempt + 1, etf_ticker, exc)
                time.sleep(2 ** attempt)
            else:
                logger.error("iShares DB write error (%s): %s", etf_ticker, exc)

    return holdings


def get_ishares_holdings_all() -> dict[str, list[dict]]:
    """Fetch all configured iShares ETFs and return by ticker."""
    result = {}
    for ticker, url in _ISHARES_ETF_URLS.items():
        holdings = fetch_ishares_holdings(ticker, url)
        if holdings:
            result[ticker] = holdings
        time.sleep(1)  # polite delay between requests
    return result


# ---------------------------------------------------------------------------
# Source 2 — BlackRock Investment Institute commentary
# ---------------------------------------------------------------------------

def _score_text(text: str) -> int:
    """Return net sentiment score for a block of text."""
    text_lower = text.lower()
    score = 0
    for keyword, weight in _SENTIMENT_WEIGHTS.items():
        count = text_lower.count(keyword)
        score += count * weight
    return score


_BII_OUTLOOK_URL = "https://www.blackrock.com/us/individual/insights/blackrock-investment-institute/outlook"
_BII_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}


def get_bii_signals() -> dict:
    """Fetch BII commentary from the BII Outlook page (plain HTML, ~1-2s).

    BlackRock's RSS feeds were removed. We now scrape the Outlook page directly —
    it contains rich plain-text sentiment ("pro-risk", "overweight U.S. stocks", etc.)
    and returns 200 without requiring JS or headless browser.
    Falls back to the BII insights index page if the outlook page fails.
    """
    net_score = 0
    headline = ""

    # --- Primary: BII Outlook page (plain HTML, consistently returns 200) ---
    try:
        logger.info("Fetching BII Outlook: %s", _BII_OUTLOOK_URL)
        resp = requests.get(_BII_OUTLOOK_URL, headers=_BII_HEADERS, timeout=15)
        resp.raise_for_status()
        text = resp.text

        # Strip HTML tags for clean scoring
        clean = re.sub(r"<[^>]+>", " ", text)
        clean = re.sub(r"\s+", " ", clean)
        net_score = _score_text(clean)

        # Extract first meaningful <title> or <h1> as headline
        m = re.search(r"<h1[^>]*>(.*?)</h1>", text, re.I | re.DOTALL)
        if m:
            headline = re.sub(r"<[^>]+>", "", m.group(1)).strip()
        if not headline:
            m = re.search(r"<title>(.*?)</title>", text, re.I)
            if m:
                headline = m.group(1).split("|")[0].strip()

    except Exception as exc:
        logger.warning("BII Outlook error (%s); trying BII insights index fallback", exc)

        # --- Fallback: BII insights index page ---
        try:
            resp = requests.get(_BII_URL, headers=_BII_HEADERS, timeout=15)
            resp.raise_for_status()
            text = resp.text
            clean = re.sub(r"<[^>]+>", " ", text)
            net_score = _score_text(clean)
            m = re.search(r"<title>(.*?)</title>", text, re.I)
            if m:
                headline = m.group(1).split("|")[0].strip()
        except Exception as exc2:
            logger.error("BII fallback error: %s", exc2)
            return _bii_neutral(f"BII unavailable: {exc2}")

    # Translate score → signal + confidence
    if net_score >= 5:
        signal = "BULLISH"
        confidence = min(95, 50 + net_score * 3)
    elif net_score <= -5:
        signal = "BEARISH"
        confidence = min(95, 50 + abs(net_score) * 3)
    else:
        signal = "NEUTRAL"
        confidence = max(20, 50 - abs(net_score) * 5)

    confidence = int(confidence)
    logger.info("BII signal: %s (score=%d, confidence=%d)", signal, net_score, confidence)

    _save_signal(
        source="bii_commentary",
        signal=signal,
        confidence=confidence,
        raw_data={"net_score": net_score, "headline": headline},
        notes="Scraped BII Outlook page",
    )

    return {
        "signal": signal,
        "confidence": confidence,
        "headline": headline,
        "articles": [],
        "net_score": net_score,
    }


def _bii_neutral(reason: str) -> dict:
    return {
        "signal": "NEUTRAL",
        "confidence": 30,
        "headline": reason,
        "articles": [],
        "net_score": 0,
    }


# ---------------------------------------------------------------------------
# Source 3 — iShares Fund Flows
# ---------------------------------------------------------------------------

def get_fund_flows() -> list[dict]:
    """Compare today's vs yesterday's shares_outstanding per ETF.

    Rising shares → inflow (BULLISH), falling → outflow (BEARISH).
    """
    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    flows = []

    try:
        con = _db()

        for etf in _ISHARES_ETF_URLS:
            # Sum shares for today
            row_today = con.execute(
                "SELECT SUM(shares) FROM aladdin_holdings WHERE etf_ticker=? AND date=?",
                (etf, today),
            ).fetchone()

            row_prev = con.execute(
                "SELECT SUM(shares), date FROM aladdin_holdings "
                "WHERE etf_ticker=? AND date < ? "
                "ORDER BY date DESC LIMIT 1",
                (etf, today),
            ).fetchone()

            shares_today = (row_today[0] or 0) if row_today else 0
            shares_prev = (row_prev[0] or 0) if row_prev else 0
            prev_date = row_prev[1] if row_prev else yesterday

            if shares_today == 0 or shares_prev == 0:
                flow_signal = "UNKNOWN"
                delta_pct = 0.0
            else:
                delta_pct = (shares_today - shares_prev) / shares_prev * 100
                if delta_pct > 0.1:
                    flow_signal = "INFLOW"
                elif delta_pct < -0.1:
                    flow_signal = "OUTFLOW"
                else:
                    flow_signal = "FLAT"

            flows.append({
                "etf": etf,
                "flow_signal": flow_signal,
                "delta_pct": round(delta_pct, 4),
                "shares_today": shares_today,
                "shares_prev": shares_prev,
                "prev_date": prev_date,
            })

        con.close()
    except Exception as exc:
        logger.error("Fund flow calculation error: %s", exc)

    # Persist flow signals
    for f in flows:
        if f["flow_signal"] != "UNKNOWN":
            sig = "BULLISH" if f["flow_signal"] == "INFLOW" else (
                "BEARISH" if f["flow_signal"] == "OUTFLOW" else "NEUTRAL"
            )
            _save_signal(
                source=f"ishares_flows_{f['etf']}",
                signal=sig,
                confidence=60,
                raw_data=f,
                notes=f"ETF flow delta: {f['delta_pct']:+.2f}%",
            )

    logger.info("Fund flows calculated for %d ETFs", len(flows))
    return flows


# ---------------------------------------------------------------------------
# Source 4 — Congress trades filtered for BlackRock / iShares
# ---------------------------------------------------------------------------

def get_congress_blackrock(days_back: int = 30) -> list[dict]:
    """Return recent congress trades involving BlackRock / iShares tickers."""
    cutoff = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    flagged = []

    try:
        # Pull from in-memory scraper (live data, no stale DB dependency)
        from engine.congress_scraper import get_all_congress_trades
        all_trades = get_all_congress_trades()

        for trade in all_trades:
            ticker = (trade.get("ticker") or "").upper().strip()
            company = (trade.get("company") or "").lower()

            is_blk_ticker = ticker in _BLK_TICKERS
            is_blk_company = any(kw in company for kw in _BLK_KEYWORDS)

            if not (is_blk_ticker or is_blk_company):
                continue

            # Date filter (filed_date or trade_date)
            filed = trade.get("filed_date", "") or trade.get("trade_date", "")
            # Normalize date formats for comparison
            try:
                import re as _re
                filed_norm = _re.sub(r"(\w{3})\s+(\d{4})", r"\2", filed)
                if filed and filed < cutoff and "2025" not in filed and "2026" not in filed:
                    continue
            except Exception:
                pass

            flagged.append({
                "senator": trade.get("politician", ""),
                "party": trade.get("party", ""),
                "ticker": ticker,
                "company": trade.get("company", ""),
                "direction": trade.get("type", ""),
                "size": trade.get("size", ""),
                "trade_date": trade.get("trade_date", ""),
                "filed_date": trade.get("filed_date", ""),
                "source": trade.get("source", ""),
            })

    except Exception as exc:
        logger.error("Congress BlackRock filter error: %s", exc)

    logger.info("Congress BlackRock filter: %d flagged trades", len(flagged))

    if flagged:
        _save_signal(
            source="congress_blackrock",
            signal="INFO",
            confidence=50,
            raw_data={"count": len(flagged), "trades": flagged[:5]},
            notes=f"{len(flagged)} congress trades in BlackRock/iShares tickers",
        )

    return flagged


# ---------------------------------------------------------------------------
# Unified Output
# ---------------------------------------------------------------------------

def _aggregate_signal(bii: dict, flows: list[dict]) -> tuple[str, int]:
    """Combine BII + flow signals into a single macro signal."""
    scores = []

    # BII carries the most weight
    bii_sig = bii.get("signal", "NEUTRAL")
    bii_conf = bii.get("confidence", 50) / 100
    if bii_sig == "BULLISH":
        scores.append(+2 * bii_conf)
    elif bii_sig == "BEARISH":
        scores.append(-2 * bii_conf)

    # Fund flows
    for f in flows:
        fs = f.get("flow_signal", "UNKNOWN")
        if fs == "INFLOW":
            scores.append(+1.0)
        elif fs == "OUTFLOW":
            scores.append(-1.0)

    if not scores:
        return "NEUTRAL", 40

    net = sum(scores) / len(scores)
    if net > 0.3:
        macro_signal = "BULLISH"
    elif net < -0.3:
        macro_signal = "BEARISH"
    else:
        macro_signal = "NEUTRAL"

    confidence = min(95, int(50 + abs(net) * 40))
    return macro_signal, confidence


def get_aladdin_brief(force: bool = False) -> dict:
    """Return unified Aladdin intelligence brief.

    Cached for 4 hours unless force=True.
    """
    now = time.time()
    if not force and _CACHE["brief"] and (now - _CACHE["ts"]) < _CACHE_TTL:
        return _CACHE["brief"]

    logger.info("Building Aladdin brief...")
    ts = datetime.now().isoformat()

    # --- Source 1: Holdings ---
    try:
        holdings_by_etf = get_ishares_holdings_all()
        top_holdings = {
            etf: [
                {"ticker": h["holding_ticker"], "weight": h["weight"]}
                for h in sorted(holdings, key=lambda x: x["weight"], reverse=True)[:5]
            ]
            for etf, holdings in holdings_by_etf.items()
        }
    except Exception as exc:
        logger.error("Holdings fetch error: %s", exc)
        top_holdings = {}

    # --- Source 2: BII ---
    try:
        bii = get_bii_signals()
    except Exception as exc:
        logger.error("BII error: %s", exc)
        bii = _bii_neutral(f"BII error: {exc}")

    # --- Source 3: Fund flows ---
    try:
        flows = get_fund_flows()
    except Exception as exc:
        logger.error("Fund flows error: %s", exc)
        flows = []

    # --- Source 4: Congress ---
    try:
        congress_flags = get_congress_blackrock()
    except Exception as exc:
        logger.error("Congress filter error: %s", exc)
        congress_flags = []

    # --- Aggregate ---
    macro_signal, confidence = _aggregate_signal(bii, flows)

    brief: dict = {
        "macro_signal": macro_signal,
        "confidence": confidence,
        "top_etf_flows": flows,
        "top_holdings": top_holdings,
        "congress_flags": congress_flags[:10],
        "bii_headline": bii.get("headline", ""),
        "bii_signal": bii.get("signal", "NEUTRAL"),
        "bii_confidence": bii.get("confidence", 50),
        "timestamp": ts,
    }

    _CACHE["brief"] = brief
    _CACHE["ts"] = now

    logger.info(
        "Aladdin brief complete — %s (confidence=%d, congress_flags=%d)",
        macro_signal, confidence, len(congress_flags),
    )
    return brief
