"""
engine/alpha_signals.py — Dilithium Crystal Alpha Signal Layer v1.0

10 free data sources → daily composite alpha score per symbol →
ntfy alerts + Signal Center (port 9000) integration.

Run daily at 7:00 AM AZ (UTC-7) via main.py scheduler.
All data stored in data/alpha_signals.db — NEVER touches trader.db or arena.db.

Signal scores:  -2 (strong bearish)  to  +2 (strong bullish)
Composite alert: ntfy fires when abs(composite_score) >= 1.5
"""
from __future__ import annotations

import io
import json
import logging
import re
import sqlite3
import time
import threading
import urllib.request
import zipfile
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional
from xml.etree import ElementTree as ET

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup

# ── Universe & paths ───────────────────────────────────────────────────────────
DB_PATH  = Path("data/alpha_signals.db")
LOG_PATH = Path("alpha_signals.log")

ALPHA_UNIVERSE: list[str] = [
    "SPY", "QQQ", "TQQQ", "NVDA", "TSLA", "AAPL", "AMD", "META", "MSFT",
    "GOOGL", "AMZN", "MU", "AVGO", "PLTR", "COIN", "BAC", "MARA", "SOFI",
    "NFLX", "MRVL", "SMR", "XLE", "INTC", "STAA",
]

# Composite weights (normalized at runtime by weight_used — need not sum exactly to 1.0)
WEIGHTS: dict[str, float] = {
    "dark_pool":                0.20,
    "insider":                  0.20,
    "ftd":                      0.15,
    "put_call":                 0.10,
    "vix_structure":            0.10,
    "sentiment":                0.10,
    "yield_curve":              0.03,   # reduced from 0.05
    "opex":                     0.05,
    "earnings":                 0.03,
    "rebalancing":              0.00,   # reduced from 0.02 (calendar signal, low alpha)
    "rallies_consensus":        0.05,   # new: AI arena model consensus
    "rallies_debate_sentiment": 0.05,   # new: AI debate buy/sell sentiment
}

ALERT_THRESHOLD = 1.5  # ntfy fires when abs(composite_score) >= this

_EDGAR_HEADERS = {
    "User-Agent": "TradeMinds research@trademinds.local",
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json, text/plain, */*",
}
_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "TradeMinds research@trademinds.local"})

# ── Logging ────────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)

# ── DB helpers ─────────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _init_db() -> None:
    """Create all 11 tables (10 signals + composite_alpha)."""
    conn = _conn()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS dark_pool_signals (
        id INTEGER PRIMARY KEY,
        as_of_date TEXT NOT NULL,
        symbol TEXT NOT NULL,
        short_volume INTEGER,
        total_volume INTEGER,
        short_ratio REAL,
        signal_score REAL,
        flag TEXT,
        fetched_at TEXT,
        UNIQUE(as_of_date, symbol)
    );
    CREATE TABLE IF NOT EXISTS ftd_signals (
        id INTEGER PRIMARY KEY,
        settlement_date TEXT NOT NULL,
        symbol TEXT NOT NULL,
        ftd_quantity INTEGER,
        price REAL,
        signal_score REAL,
        flag TEXT,
        fetched_at TEXT,
        UNIQUE(settlement_date, symbol)
    );
    CREATE TABLE IF NOT EXISTS insider_signals (
        id INTEGER PRIMARY KEY,
        as_of_date TEXT NOT NULL,
        symbol TEXT NOT NULL,
        buy_count INTEGER DEFAULT 0,
        sell_count INTEGER DEFAULT 0,
        net_buys INTEGER DEFAULT 0,
        signal_score REAL,
        flag TEXT,
        fetched_at TEXT,
        UNIQUE(as_of_date, symbol)
    );
    CREATE TABLE IF NOT EXISTS put_call_signals (
        id INTEGER PRIMARY KEY,
        trade_date TEXT NOT NULL UNIQUE,
        equity_pc_ratio REAL,
        total_pc_ratio REAL,
        pc_5d_ma REAL,
        pc_21d_ma REAL,
        signal_score REAL,
        signal TEXT,
        fetched_at TEXT
    );
    CREATE TABLE IF NOT EXISTS yield_curve_signals (
        id INTEGER PRIMARY KEY,
        trade_date TEXT NOT NULL UNIQUE,
        rate_2y REAL,
        rate_10y REAL,
        spread_bps REAL,
        spread_change_bps REAL,
        curve_state TEXT,
        signal_score REAL,
        sector_bias TEXT,
        fetched_at TEXT
    );
    CREATE TABLE IF NOT EXISTS sentiment_signals (
        id INTEGER PRIMARY KEY,
        as_of_date TEXT NOT NULL,
        symbol TEXT NOT NULL,
        mention_count INTEGER DEFAULT 0,
        mention_7d_avg REAL,
        sentiment_score_raw REAL,
        signal_score REAL,
        flag TEXT,
        fetched_at TEXT,
        UNIQUE(as_of_date, symbol)
    );
    CREATE TABLE IF NOT EXISTS earnings_signals (
        id INTEGER PRIMARY KEY,
        as_of_date TEXT NOT NULL,
        symbol TEXT NOT NULL,
        next_earnings_date TEXT,
        days_to_earnings INTEGER,
        eps_beat_streak INTEGER DEFAULT 0,
        beat_pct_avg REAL,
        signal_score REAL,
        flag TEXT,
        fetched_at TEXT,
        UNIQUE(as_of_date, symbol)
    );
    CREATE TABLE IF NOT EXISTS opex_signals (
        id INTEGER PRIMARY KEY,
        trade_date TEXT NOT NULL UNIQUE,
        is_opex_week INTEGER DEFAULT 0,
        opex_date TEXT,
        days_to_opex INTEGER,
        signal_score REAL,
        notes TEXT,
        fetched_at TEXT
    );
    CREATE TABLE IF NOT EXISTS rebalancing_signals (
        id INTEGER PRIMARY KEY,
        trade_date TEXT NOT NULL UNIQUE,
        is_quarter_end_window INTEGER DEFAULT 0,
        quarter_end_date TEXT,
        days_to_quarter_end INTEGER,
        signal_score REAL,
        notes TEXT,
        fetched_at TEXT
    );
    CREATE TABLE IF NOT EXISTS vix_structure_signals (
        id INTEGER PRIMARY KEY,
        trade_date TEXT NOT NULL UNIQUE,
        vix_spot REAL,
        vix_3m REAL,
        contango_ratio REAL,
        structure_state TEXT,
        signal_score REAL,
        fetched_at TEXT
    );
    CREATE TABLE IF NOT EXISTS composite_alpha (
        id INTEGER PRIMARY KEY,
        as_of_date TEXT NOT NULL,
        symbol TEXT NOT NULL,
        composite_score REAL,
        dark_pool_score REAL,
        ftd_score REAL,
        insider_score REAL,
        put_call_score REAL,
        vix_structure_score REAL,
        sentiment_score REAL,
        yield_curve_score REAL,
        opex_score REAL,
        earnings_score REAL,
        rebalancing_score REAL,
        rallies_consensus_score REAL,
        rallies_debate_score REAL,
        signal_count INTEGER DEFAULT 0,
        alert_fired INTEGER DEFAULT 0,
        created_at TEXT,
        UNIQUE(as_of_date, symbol)
    );
    """)
    conn.commit()
    # Migrate existing DB: add rallies columns if not present
    for col, typedef in [
        ("rallies_consensus_score", "REAL"),
        ("rallies_debate_score",    "REAL"),
    ]:
        try:
            conn.execute(f"ALTER TABLE composite_alpha ADD COLUMN {col} {typedef}")
            conn.commit()
        except Exception:
            pass  # column already exists
    conn.close()
    logger.info("alpha_signals.db initialized")


# ── Calendar utilities ──────────────────────────────────────────────────────────

def _today_str() -> str:
    return date.today().isoformat()


def _prev_trading_day(d: date | None = None, lookback: int = 1) -> date:
    """Return the most recent weekday going back `lookback` days from d."""
    d = d or date.today()
    for _ in range(lookback * 3 + 5):
        d -= timedelta(days=1)
        if d.weekday() < 5:  # Mon-Fri
            lookback -= 1
            if lookback == 0:
                return d
    return d


def _opex_date(year: int, month: int) -> date:
    """Third Friday of the given month."""
    d = date(year, month, 1)
    fridays = 0
    while True:
        if d.weekday() == 4:  # Friday
            fridays += 1
            if fridays == 3:
                return d
        d += timedelta(days=1)


def _quarter_end_date(ref: date | None = None) -> date:
    """Last calendar day of current quarter (Mar, Jun, Sep, Dec)."""
    d = ref or date.today()
    quarter_end_months = {1: 3, 2: 3, 3: 3, 4: 6, 5: 6, 6: 6,
                          7: 9, 8: 9, 9: 9, 10: 12, 11: 12, 12: 12}
    end_month = quarter_end_months[d.month]
    last_day = {3: 31, 6: 30, 9: 30, 12: 31}[end_month]
    return date(d.year, end_month, last_day)


def _last_n_trading_days(n: int, ref: date | None = None) -> list[date]:
    days = []
    d = ref or date.today()
    while len(days) < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            days.append(d)
    return days


def _clamp(score: float, lo: float = -2.0, hi: float = 2.0) -> float:
    return max(lo, min(hi, score))


# ═══════════════════════════════════════════════════════════════════════════════
# Signal 1 — DARK POOL (FINRA daily short volume)
# ═══════════════════════════════════════════════════════════════════════════════

def run_dark_pool(as_of: date | None = None) -> dict[str, float]:
    """
    Download FINRA REGSHO short volume data.
    URL: https://cdn.finra.org/equity/regsho/daily/CNMSshvol{YYYYMMDD}.txt
    Flag symbols with short ratio > 45% as bearish institutional pressure.
    Returns symbol → score mapping.
    """
    today = as_of or _prev_trading_day()
    scores: dict[str, float] = {}

    # Try up to 3 prior trading days in case today's file isn't published yet
    for lookback in range(3):
        d = today if lookback == 0 else _prev_trading_day(today, lookback)
        url = f"https://cdn.finra.org/equity/regsho/daily/CNMSshvol{d.strftime('%Y%m%d')}.txt"
        try:
            resp = _SESSION.get(url, timeout=20)
            if resp.status_code == 200 and len(resp.content) > 500:
                raw = resp.text
                break
        except Exception as e:
            logger.debug(f"FINRA dark pool fetch attempt {lookback}: {e}")
    else:
        logger.warning("Dark pool: no FINRA data available for last 3 trading days")
        return scores

    fetched_at = datetime.utcnow().isoformat()
    date_str = d.isoformat()
    rows_saved = 0
    conn = _conn()

    for line in raw.splitlines():
        parts = line.split("|")
        if len(parts) < 5 or parts[1] == "Symbol":
            continue
        sym = parts[1].strip().upper()
        if sym not in ALPHA_UNIVERSE:
            continue
        try:
            short_vol = int(float(parts[2]))
            total_vol = int(float(parts[4]))
            if total_vol == 0:
                continue
            ratio = short_vol / total_vol

            # Score: bearish pressure when institutions short heavily
            if ratio >= 0.60:
                score = -2.0
                flag = "HEAVY_SHORT_PRESSURE"
            elif ratio >= 0.50:
                score = -1.5
                flag = "HIGH_SHORT_RATIO"
            elif ratio >= 0.45:
                score = -1.0
                flag = "ELEVATED_SHORT_RATIO"
            elif ratio <= 0.20:
                score = 2.0
                flag = "LOW_SHORT_BULLISH"
            elif ratio <= 0.30:
                score = 1.0
                flag = "BELOW_AVG_SHORT"
            else:
                score = 0.0
                flag = "NEUTRAL"

            scores[sym] = score
            conn.execute("""
                INSERT OR REPLACE INTO dark_pool_signals
                (as_of_date, symbol, short_volume, total_volume, short_ratio,
                 signal_score, flag, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (date_str, sym, short_vol, total_vol, round(ratio, 4),
                  score, flag, fetched_at))
            rows_saved += 1
        except (ValueError, IndexError):
            continue

    conn.commit()
    conn.close()
    logger.info(f"Dark pool: {rows_saved} symbols saved for {date_str}")
    return scores


# ═══════════════════════════════════════════════════════════════════════════════
# Signal 2 — FTD (SEC Failure to Deliver)
# ═══════════════════════════════════════════════════════════════════════════════

def run_ftd(as_of: date | None = None) -> dict[str, float]:
    """
    Download SEC FOIADOCS FTD data (pipe-delimited, zipped).
    URL: https://www.sec.gov/data/foiadocsfailsdata/cnsfails{YYYYMM}a.zip (first half)
         https://www.sec.gov/data/foiadocsfailsdata/cnsfails{YYYYMM}b.zip (second half)
    High FTD = potential short squeeze candidate → bullish signal.
    Returns symbol → score mapping.
    """
    today = as_of or date.today()
    scores: dict[str, float] = {}
    fetched_at = datetime.utcnow().isoformat()

    # FTD data has a ~2-week lag; try current month then previous
    candidates: list[tuple[int, int, str]] = []
    for delta_months in range(3):
        y, m = today.year, today.month - delta_months
        if m <= 0:
            m += 12
            y -= 1
        suffix = "a" if today.day <= 15 else "b"
        candidates.append((y, m, suffix))
        # Also try the other half
        candidates.append((y, m, "b" if suffix == "a" else "a"))

    raw_lines: list[str] = []
    used_period = ""

    for y, m, sfx in candidates:
        url = (f"https://www.sec.gov/data/foiadocsfailsdata/"
               f"cnsfails{y:04d}{m:02d}{sfx}.zip")
        try:
            resp = _SESSION.get(url, timeout=30)
            if resp.status_code == 200 and len(resp.content) > 1000:
                with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                    fname = zf.namelist()[0]
                    raw_lines = zf.read(fname).decode("latin-1").splitlines()
                used_period = f"{y:04d}-{m:02d}{sfx}"
                logger.info(f"FTD: loaded {len(raw_lines)} rows from {used_period}")
                break
        except Exception as e:
            logger.debug(f"FTD fetch {url}: {e}")
        time.sleep(0.5)

    if not raw_lines:
        logger.warning("FTD: no SEC data available")
        return scores

    # Aggregate FTD quantity per symbol (same symbol may appear on multiple dates in the file)
    ftd_agg: dict[str, dict] = {}
    for line in raw_lines:
        if not line.strip() or line.startswith("SETTLEMENT"):
            continue
        parts = line.split("|")
        if len(parts) < 5:
            continue
        sym = parts[2].strip().upper()
        if sym not in ALPHA_UNIVERSE:
            continue
        try:
            qty = int(float(parts[3]))
            price = float(parts[4]) if parts[4].strip() else 0.0
            settle_date = parts[0].strip()
            if sym not in ftd_agg or qty > ftd_agg[sym]["qty"]:
                ftd_agg[sym] = {"qty": qty, "price": price, "settle_date": settle_date}
        except (ValueError, IndexError):
            continue

    conn = _conn()
    for sym, data in ftd_agg.items():
        qty = data["qty"]
        # High FTD = short sellers haven't covered → potential squeeze
        if qty >= 2_000_000:
            score = 2.0
            flag = "EXTREME_FTD_SQUEEZE_CANDIDATE"
        elif qty >= 1_000_000:
            score = 1.5
            flag = "VERY_HIGH_FTD"
        elif qty >= 500_000:
            score = 1.0
            flag = "HIGH_FTD_SQUEEZE_WATCH"
        elif qty >= 100_000:
            score = 0.5
            flag = "ELEVATED_FTD"
        else:
            score = 0.0
            flag = "NORMAL_FTD"

        scores[sym] = score
        conn.execute("""
            INSERT OR REPLACE INTO ftd_signals
            (settlement_date, symbol, ftd_quantity, price, signal_score, flag, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (data["settle_date"], sym, qty, data["price"], score, flag, fetched_at))

    conn.commit()
    conn.close()
    logger.info(f"FTD: {len(ftd_agg)} symbols saved from period {used_period}")
    return scores


# ═══════════════════════════════════════════════════════════════════════════════
# Signal 3 — INSIDER CLUSTER BUYS (Form 4 via EDGAR)
# ═══════════════════════════════════════════════════════════════════════════════

_cik_cache: dict[str, str] = {}
_CIK_FILE = Path("data/sec_cik_cache.json")


def _get_cik(symbol: str) -> str | None:
    """CIK lookup — shares cache with sec_edgar.py."""
    global _cik_cache
    if not _cik_cache and _CIK_FILE.exists():
        try:
            _cik_cache = json.loads(_CIK_FILE.read_text())
        except Exception:
            pass

    sym = symbol.upper()
    if sym in _cik_cache:
        return _cik_cache[sym]

    try:
        r = _SESSION.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers=_EDGAR_HEADERS, timeout=10,
        )
        if r.status_code == 200:
            for entry in r.json().values():
                if entry.get("ticker", "").upper() == sym:
                    cik = str(entry["cik_str"]).zfill(10)
                    _cik_cache[sym] = cik
                    _CIK_FILE.parent.mkdir(parents=True, exist_ok=True)
                    _CIK_FILE.write_text(json.dumps(_cik_cache, indent=2))
                    return cik
    except Exception as e:
        logger.debug(f"CIK lookup {sym}: {e}")
    return None


def _parse_form4_transactions(xml_text: str) -> tuple[int, int]:
    """Parse Form 4 XML → (buy_count, sell_count) for this filing."""
    buys = sells = 0
    try:
        root = ET.fromstring(xml_text)
        # Look in both derivative and non-derivative tables
        for elem in root.iter():
            tag = elem.tag.split("}")[-1].lower()  # strip namespace
            if tag == "transactioncode":
                code = (elem.text or "").strip().upper()
                if code in ("P",):       # Purchase
                    buys += 1
                elif code in ("S", "I"): # Sale, Disposition
                    sells += 1
                # A = Award/Grant, G = Gift, etc. → ignore for buy/sell
    except ET.ParseError:
        pass
    return buys, sells


def run_insider_cluster(as_of: date | None = None) -> dict[str, float]:
    """
    Query EDGAR for Form 4 filings in the last 14 days per symbol.
    Flag when 3+ insiders buy within 14 days (cluster buy) → strong bullish.
    Returns symbol → score mapping.
    """
    today = as_of or date.today()
    since = (today - timedelta(days=14)).isoformat()
    today_str = today.isoformat()
    fetched_at = datetime.utcnow().isoformat()
    scores: dict[str, float] = {}
    conn = _conn()

    for sym in ALPHA_UNIVERSE:
        cik = _get_cik(sym)
        if not cik:
            logger.debug(f"Insider: no CIK for {sym}")
            continue

        try:
            url = f"https://data.sec.gov/submissions/CIK{cik}.json"
            r = _SESSION.get(url, headers=_EDGAR_HEADERS, timeout=12)
            if r.status_code != 200:
                continue

            data = r.json()
            recent = data.get("filings", {}).get("recent", {})
            forms = recent.get("form", [])
            dates = recent.get("filingDate", [])
            accessions = recent.get("accessionNumber", [])
            primary_docs = recent.get("primaryDocument", [])

            # Find Form 4 filings within the last 14 days
            form4_filings: list[dict] = []
            for i in range(min(len(forms), 60)):
                if forms[i] == "4" and dates[i] >= since:
                    form4_filings.append({
                        "date": dates[i],
                        "accession": accessions[i],
                        "doc": primary_docs[i] if i < len(primary_docs) else "",
                    })

            if not form4_filings:
                scores[sym] = 0.0
                conn.execute("""
                    INSERT OR REPLACE INTO insider_signals
                    (as_of_date, symbol, buy_count, sell_count, net_buys,
                     signal_score, flag, fetched_at)
                    VALUES (?, ?, 0, 0, 0, 0.0, 'NO_ACTIVITY', ?)
                """, (today_str, sym, fetched_at))
                continue

            # Parse each Form 4 XML (cap at 8 per symbol)
            total_buys = total_sells = 0
            for filing in form4_filings[:8]:
                acc_raw = filing["accession"]
                cik_int = int(cik)
                acc_nodashes = acc_raw.replace("-", "")
                doc_name = filing["doc"]
                if not doc_name.endswith(".xml"):
                    # Try constructing doc name from accession
                    doc_name = acc_raw + ".xml"

                xml_url = (f"https://www.sec.gov/Archives/edgar/data/"
                           f"{cik_int}/{acc_nodashes}/{doc_name}")
                try:
                    xr = _SESSION.get(xml_url, headers=_SEC_HEADERS, timeout=8)
                    if xr.status_code == 200:
                        b, s = _parse_form4_transactions(xr.text)
                        total_buys += b
                        total_sells += s
                except Exception:
                    pass
                time.sleep(0.25)

            net_buys = total_buys - total_sells
            filer_count = len(form4_filings)

            # Score based on cluster buys
            if filer_count >= 3 and net_buys >= 3:
                score = 2.0
                flag = "CLUSTER_BUY_STRONG"
            elif filer_count >= 3 and net_buys >= 1:
                score = 1.5
                flag = "CLUSTER_BUY"
            elif filer_count >= 2 and net_buys >= 2:
                score = 1.0
                flag = "DOUBLE_INSIDER_BUY"
            elif net_buys >= 1:
                score = 0.5
                flag = "INSIDER_BUY"
            elif net_buys <= -3:
                score = -2.0
                flag = "CLUSTER_SELL"
            elif net_buys <= -1:
                score = -1.0
                flag = "INSIDER_SELL"
            else:
                score = 0.0
                flag = "NEUTRAL_ACTIVITY"

            scores[sym] = score
            conn.execute("""
                INSERT OR REPLACE INTO insider_signals
                (as_of_date, symbol, buy_count, sell_count, net_buys,
                 signal_score, flag, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (today_str, sym, total_buys, total_sells, net_buys,
                  score, flag, fetched_at))
            logger.debug(f"Insider {sym}: {filer_count} Form4s, {total_buys}B/{total_sells}S → {score}")

        except Exception as e:
            logger.warning(f"Insider signal {sym}: {e}")

        time.sleep(0.4)  # EDGAR rate limit

    conn.commit()
    conn.close()
    logger.info(f"Insider cluster: {len(scores)} symbols processed")
    return scores


# ═══════════════════════════════════════════════════════════════════════════════
# Signal 4 — PUT/CALL RATIO (CBOE)
# ═══════════════════════════════════════════════════════════════════════════════

def run_put_call(as_of: date | None = None) -> float:
    """
    Fetch CBOE equity put/call ratio via yfinance (^PCCE).
    Contrarian signal: high P/C = excessive fear = bullish; low P/C = greed = bearish.
    Returns market-wide score (-2 to +2).
    """
    today_str = (as_of or date.today()).isoformat()
    fetched_at = datetime.utcnow().isoformat()
    score = 0.0

    equity_pc = None
    total_pc   = None

    def _yf_series(ticker: str, period: str = "30d") -> pd.Series | None:
        """Download single ticker close series."""
        try:
            df = yf.download(ticker, period=period, interval="1d",
                             progress=False, auto_adjust=True)
            if df.empty:
                return None
            if isinstance(df.columns, pd.MultiIndex):
                lvl0 = df.columns.get_level_values(0)
                col = "Close" if "Close" in lvl0 else "Adj Close"
                series = df.xs(col, axis=1, level=0).iloc[:, 0].dropna()
            else:
                series = (df["Close"] if "Close" in df.columns else df["Adj Close"]).dropna()
            return series if not series.empty else None
        except Exception as e:
            logger.debug(f"yf_series {ticker}: {e}")
            return None

    # Primary: yfinance CBOE P/C tickers
    for sym_pc, attr in [("^PCCE", "equity_pc"), ("^PCCR", "total_pc")]:
        s = _yf_series(sym_pc, period="5d")
        if s is not None:
            if attr == "equity_pc":
                equity_pc = float(s.iloc[-1])
            else:
                total_pc = float(s.iloc[-1])

    # Fallback: scrape CBOE options statistics page
    if equity_pc is None:
        try:
            resp = _SESSION.get(
                "https://www.cboe.com/us/options/market_statistics/daily/",
                timeout=15,
            )
            soup = BeautifulSoup(resp.text, "lxml")
            for row in soup.find_all("tr"):
                cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
                if len(cells) >= 2 and "Equity" in cells[0]:
                    try:
                        equity_pc = float(cells[1])
                        break
                    except ValueError:
                        pass
        except Exception as e:
            logger.debug(f"P/C CBOE scrape: {e}")

    if equity_pc is None and total_pc is None:
        logger.warning("P/C ratio: no data available")
        return 0.0

    pc = equity_pc or total_pc

    # Build 5d and 21d MA from historical yfinance data
    pc_5d_ma = pc_21d_ma = pc
    hist_series = _yf_series("^PCCE", period="30d")
    if hist_series is not None:
        if len(hist_series) >= 5:
            pc_5d_ma  = float(hist_series.tail(5).mean())
        if len(hist_series) >= 21:
            pc_21d_ma = float(hist_series.tail(21).mean())

    # Contrarian scoring
    if pc >= 1.5:
        score = 2.0
        signal = "EXTREME_FEAR_STRONG_BUY"
    elif pc >= 1.2:
        score = 1.0
        signal = "ELEVATED_FEAR_CONTRARIAN_BUY"
    elif pc <= 0.4:
        score = -2.0
        signal = "EXTREME_GREED_STRONG_SELL"
    elif pc <= 0.6:
        score = -1.0
        signal = "LOW_PC_CONTRARIAN_SELL"
    else:
        score = 0.0
        signal = "NEUTRAL_PC"

    conn = _conn()
    conn.execute("""
        INSERT OR REPLACE INTO put_call_signals
        (trade_date, equity_pc_ratio, total_pc_ratio, pc_5d_ma, pc_21d_ma,
         signal_score, signal, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (today_str, equity_pc, total_pc, round(pc_5d_ma, 3),
          round(pc_21d_ma, 3), score, signal, fetched_at))
    conn.commit()
    conn.close()
    logger.info(f"P/C ratio: equity={equity_pc:.3f} → {signal} (score={score})")
    return score


# ═══════════════════════════════════════════════════════════════════════════════
# Signal 5 — YIELD CURVE (2Y/10Y via FRED)
# ═══════════════════════════════════════════════════════════════════════════════

def run_yield_curve(as_of: date | None = None) -> float:
    """
    Fetch 2Y and 10Y Treasury rates from FRED (no API key needed for CSV downloads).
    Steepening spread → risk-on (tech/growth). Inversion → risk-off (staples/utilities).
    Returns market-wide score (-2 to +2).
    """
    today_str = (as_of or date.today()).isoformat()
    fetched_at = datetime.utcnow().isoformat()

    rate_2y = rate_10y = None

    # FRED free CSV endpoint — no API key required
    for series, attr in [("DGS2", "rate_2y"), ("DGS10", "rate_10y")]:
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}"
        try:
            resp = _SESSION.get(url, timeout=15)
            if resp.status_code == 200:
                df = pd.read_csv(io.StringIO(resp.text))
                # Column names vary: "observation_date"/"DATE" and series-id/"VALUE"
                val_col = [c for c in df.columns if c not in ("observation_date", "DATE")][0]
                df = df[df[val_col] != "."].copy()
                df[val_col] = pd.to_numeric(df[val_col], errors="coerce")
                df.dropna(subset=[val_col], inplace=True)
                if not df.empty:
                    val = float(df[val_col].iloc[-1])
                    if attr == "rate_2y":
                        rate_2y = val
                    else:
                        rate_10y = val
        except Exception as e:
            logger.debug(f"FRED {series}: {e}")
        time.sleep(0.3)

    # Fallback: yfinance TNX/FVX proxies
    if rate_10y is None:
        try:
            t = yf.Ticker("^TNX")
            rate_10y = float(t.fast_info.get("last_price", 0)) / 10
        except Exception:
            pass

    if rate_2y is None or rate_10y is None:
        logger.warning("Yield curve: insufficient rate data")
        return 0.0

    spread_bps = (rate_10y - rate_2y) * 100  # basis points

    # Previous spread for change calculation
    spread_change = 0.0
    try:
        conn = _conn()
        prev = conn.execute(
            "SELECT spread_bps FROM yield_curve_signals ORDER BY trade_date DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if prev:
            spread_change = spread_bps - prev["spread_bps"]
    except Exception:
        pass

    # Determine curve state
    if spread_bps > 100 and spread_change > 0:
        state = "STEEP_STEEPENING"
        score = 2.0
        bias = "TECH_GROWTH_CYCLICALS"
    elif spread_bps > 50:
        state = "NORMAL"
        score = 1.0
        bias = "BALANCED"
    elif spread_bps > 0:
        state = "FLAT"
        score = 0.0
        bias = "NEUTRAL"
    elif spread_bps > -50:
        state = "SLIGHT_INVERSION"
        score = -1.0
        bias = "UTILITIES_STAPLES"
    else:
        state = "DEEP_INVERSION"
        score = -2.0
        bias = "DEFENSIVES_CASH"

    conn = _conn()
    conn.execute("""
        INSERT OR REPLACE INTO yield_curve_signals
        (trade_date, rate_2y, rate_10y, spread_bps, spread_change_bps,
         curve_state, signal_score, sector_bias, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (today_str, round(rate_2y, 3), round(rate_10y, 3),
          round(spread_bps, 1), round(spread_change, 1),
          state, score, bias, fetched_at))
    conn.commit()
    conn.close()
    logger.info(f"Yield curve: 2Y={rate_2y:.2f}% 10Y={rate_10y:.2f}% "
                f"spread={spread_bps:.0f}bps → {state} (score={score})")
    return score


# ═══════════════════════════════════════════════════════════════════════════════
# Signal 6 — REDDIT RETAIL SENTIMENT (contrarian)
# ═══════════════════════════════════════════════════════════════════════════════

# Pre-compiled ticker pattern — only match known universe symbols
_TICKER_RE = re.compile(
    r"\b(" + "|".join(re.escape(s) for s in sorted(ALPHA_UNIVERSE, key=len, reverse=True)) + r")\b"
)

_REDDIT_HEADERS = {
    "User-Agent": "TradeMinds/1.0 alpha-signals bot; +https://trademinds.local"
}


def run_reddit_sentiment(as_of: date | None = None) -> dict[str, float]:
    """
    Scrape r/wallstreetbets and r/stocks hot posts for ticker mentions.
    Contrarian signal: extreme bullish mentions = caution; extreme bearish = opportunity.
    Returns symbol → score mapping.
    """
    today_str = (as_of or date.today()).isoformat()
    fetched_at = datetime.utcnow().isoformat()
    mention_counts: dict[str, int] = defaultdict(int)
    scores: dict[str, float] = {}

    subreddits = ["wallstreetbets", "stocks", "investing"]
    for sub in subreddits:
        for endpoint in ["hot", "new"]:
            url = f"https://www.reddit.com/r/{sub}/{endpoint}.json?limit=100&t=day"
            try:
                resp = _SESSION.get(url, headers=_REDDIT_HEADERS, timeout=12)
                if resp.status_code != 200:
                    continue
                posts = resp.json().get("data", {}).get("children", [])
                for post in posts:
                    d = post.get("data", {})
                    text = f"{d.get('title', '')} {d.get('selftext', '')}"
                    for m in _TICKER_RE.findall(text.upper()):
                        mention_counts[m] += 1
            except Exception as e:
                logger.debug(f"Reddit {sub}/{endpoint}: {e}")
            time.sleep(0.5)

    if not mention_counts:
        logger.warning("Reddit sentiment: no data fetched")
        return scores

    # Load 7-day history to compute moving average
    try:
        conn = _conn()
        week_ago = (date.today() - timedelta(days=7)).isoformat()
        rows = conn.execute("""
            SELECT symbol, AVG(mention_count) as avg_mc
            FROM sentiment_signals WHERE as_of_date >= ?
            GROUP BY symbol
        """, (week_ago,)).fetchall()
        conn.close()
        hist_avgs = {r["symbol"]: r["avg_mc"] for r in rows}
    except Exception:
        hist_avgs = {}

    total_mentions = sum(mention_counts.values()) or 1
    conn = _conn()

    for sym in ALPHA_UNIVERSE:
        count = mention_counts.get(sym, 0)
        mention_pct = count / total_mentions * 100
        avg_7d = hist_avgs.get(sym, count * 0.5)

        # Contrarian: abnormally high mentions = cautious signal
        if avg_7d > 0:
            buzz_ratio = count / avg_7d
        else:
            buzz_ratio = 1.0 if count == 0 else 3.0

        if mention_pct > 5.0 or buzz_ratio > 4.0:
            score = -1.5  # Extreme retail attention = contrarian sell
            flag = "EXTREME_RETAIL_BUZZ"
        elif mention_pct > 2.5 or buzz_ratio > 2.5:
            score = -0.5
            flag = "HIGH_RETAIL_BUZZ"
        elif count == 0 and avg_7d > 2:
            score = 0.5   # Unusually quiet after active period = contrarian buy
            flag = "SENTIMENT_QUIET"
        else:
            score = 0.0
            flag = "NORMAL_SENTIMENT"

        scores[sym] = score
        conn.execute("""
            INSERT OR REPLACE INTO sentiment_signals
            (as_of_date, symbol, mention_count, mention_7d_avg,
             sentiment_score_raw, signal_score, flag, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (today_str, sym, count, round(avg_7d, 1),
              round(buzz_ratio, 2), score, flag, fetched_at))

    conn.commit()
    conn.close()
    logger.info(f"Reddit sentiment: {len(mention_counts)} tickers mentioned, "
                f"top: {sorted(mention_counts.items(), key=lambda x: -x[1])[:5]}")
    return scores


# ═══════════════════════════════════════════════════════════════════════════════
# Signal 7 — EARNINGS WHISPER (Yahoo Finance via yfinance)
# ═══════════════════════════════════════════════════════════════════════════════

def run_earnings_signals(as_of: date | None = None) -> dict[str, float]:
    """
    Use yfinance to fetch upcoming earnings dates and EPS surprise history.
    Flag symbols with 3+ consecutive beats as positive; upcoming earnings as vol warning.
    Returns symbol → score mapping.
    """
    today = as_of or date.today()
    today_str = today.isoformat()
    fetched_at = datetime.utcnow().isoformat()
    scores: dict[str, float] = {}
    conn = _conn()

    for sym in ALPHA_UNIVERSE:
        try:
            tk = yf.Ticker(sym)

            # Next earnings date
            next_earnings = None
            days_to_earnings = 999
            try:
                cal = tk.calendar
                if cal is not None and not (isinstance(cal, dict) and not cal):
                    if isinstance(cal, dict):
                        erd = cal.get("Earnings Date")
                        if erd is not None:
                            if hasattr(erd, "__iter__") and not isinstance(erd, str):
                                erd = list(erd)[0]
                            if hasattr(erd, "date"):
                                erd = erd.date()
                            if isinstance(erd, date):
                                next_earnings = erd.isoformat()
                                days_to_earnings = (erd - today).days
            except Exception:
                pass

            # EPS beat streak — use earnings_history or quarterly_earnings
            beat_streak = 0
            beat_pcts: list[float] = []
            try:
                hist = tk.earnings_history
                if hist is not None and not hist.empty:
                    # Columns: epsEstimate, epsActual, epsDifference, surprisePercent
                    hist = hist.dropna(subset=["epsEstimate", "epsActual"])
                    for _, row in hist.tail(6).iterrows():
                        est = row.get("epsEstimate", 0)
                        act = row.get("epsActual", 0)
                        if est and act:
                            beat_pcts.append((act - est) / abs(est) * 100 if est != 0 else 0)
                    # Count consecutive beats from most recent
                    for bp in reversed(beat_pcts):
                        if bp > 0:
                            beat_streak += 1
                        else:
                            break
            except Exception:
                pass

            beat_avg = float(np.mean(beat_pcts)) if beat_pcts else 0.0

            # Score
            if days_to_earnings <= 2:
                # Imminent earnings: volatility warning (neutral signal — manage risk)
                score = 0.0
                flag = "EARNINGS_IMMINENT"
            elif beat_streak >= 4 and beat_avg > 10:
                score = 2.0
                flag = "CONSISTENT_BEAT_STRONG"
            elif beat_streak >= 3:
                score = 1.0
                flag = "CONSISTENT_BEAT"
            elif beat_streak >= 1 and beat_avg > 5:
                score = 0.5
                flag = "RECENT_BEAT"
            elif beat_streak == 0 and beat_pcts and beat_pcts[-1] < -10:
                score = -1.0
                flag = "RECENT_MISS"
            else:
                score = 0.0
                flag = "NEUTRAL"

            scores[sym] = score
            conn.execute("""
                INSERT OR REPLACE INTO earnings_signals
                (as_of_date, symbol, next_earnings_date, days_to_earnings,
                 eps_beat_streak, beat_pct_avg, signal_score, flag, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (today_str, sym, next_earnings, days_to_earnings,
                  beat_streak, round(beat_avg, 1), score, flag, fetched_at))

        except Exception as e:
            logger.debug(f"Earnings signal {sym}: {e}")
            scores[sym] = 0.0

        time.sleep(0.2)

    conn.commit()
    conn.close()
    logger.info(f"Earnings signals: {len(scores)} symbols processed")
    return scores


# ═══════════════════════════════════════════════════════════════════════════════
# Signal 8 — OPEX GAMMA UNWIND (calendar-based)
# ═══════════════════════════════════════════════════════════════════════════════

def run_opex_signal(as_of: date | None = None) -> float:
    """
    Identify monthly options expiration (3rd Friday) and the surrounding week.
    During OPEX week, mean-reversion strategies historically outperform.
    Returns market-wide score (-2 to +2), positive if we're in OPEX week.
    """
    today = as_of or date.today()
    today_str = today.isoformat()
    fetched_at = datetime.utcnow().isoformat()

    opex = _opex_date(today.year, today.month)
    days_to_opex = (opex - today).days

    # OPEX week = Mon-Fri of the week containing 3rd Friday
    opex_monday = opex - timedelta(days=opex.weekday())
    in_opex_week = opex_monday <= today <= opex

    if in_opex_week:
        if today == opex:
            score = 2.0
            notes = "OPEX_DAY: Max gamma unwind, expect volatility spike + mean-reversion"
        elif days_to_opex <= 2:
            score = 1.5
            notes = "OPEX_IMMINENT: Pin risk rising, boost mean-reversion size"
        else:
            score = 1.0
            notes = "OPEX_WEEK: Gamma unwind environment, favor mean-reversion"
    elif 0 < days_to_opex <= 7:
        score = 0.5
        notes = f"PRE_OPEX: {days_to_opex}d to OPEX on {opex}"
    else:
        score = 0.0
        notes = f"STANDARD: OPEX in {days_to_opex}d on {opex}"

    conn = _conn()
    conn.execute("""
        INSERT OR REPLACE INTO opex_signals
        (trade_date, is_opex_week, opex_date, days_to_opex,
         signal_score, notes, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (today_str, int(in_opex_week), opex.isoformat(),
          days_to_opex, score, notes, fetched_at))
    conn.commit()
    conn.close()
    logger.info(f"OPEX signal: {notes} (score={score})")
    return score


# ═══════════════════════════════════════════════════════════════════════════════
# Signal 9 — QUARTER-END REBALANCING
# ═══════════════════════════════════════════════════════════════════════════════

def run_rebalancing_signal(as_of: date | None = None) -> float:
    """
    Flag the last 3 trading days of each quarter.
    Historically: fund rebalancing creates artificial buy/sell pressure.
    Strategy: fade extreme intraday moves in the final 2 days.
    Returns market-wide score (-2 to +2).
    """
    today = as_of or date.today()
    today_str = today.isoformat()
    fetched_at = datetime.utcnow().isoformat()

    qend = _quarter_end_date(today)
    days_to_qend = (qend - today).days

    # Find last 3 trading days of quarter
    last_trade_days = []
    d = qend
    while len(last_trade_days) < 3:
        if d.weekday() < 5:
            last_trade_days.append(d)
        d -= timedelta(days=1)

    in_window = today in last_trade_days
    is_final_2 = today in last_trade_days[:2]  # last 2 trading days

    if today == last_trade_days[0]:
        score = 1.5
        notes = "QUARTER_END_FINAL_DAY: Peak rebalancing flows, fade extremes"
    elif is_final_2:
        score = 1.0
        notes = f"QUARTER_END_WINDOW: {days_to_qend}d left, watch for window dressing"
    elif in_window:
        score = 0.5
        notes = "QUARTER_END_APPROACHING: Pre-rebalancing flows"
    elif 0 < days_to_qend <= 5:
        score = 0.3
        notes = f"PRE_QUARTER_END: {days_to_qend}d to quarter end"
    else:
        score = 0.0
        notes = f"NORMAL: Quarter end {days_to_qend}d away on {qend}"

    conn = _conn()
    conn.execute("""
        INSERT OR REPLACE INTO rebalancing_signals
        (trade_date, is_quarter_end_window, quarter_end_date, days_to_quarter_end,
         signal_score, notes, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (today_str, int(in_window), qend.isoformat(),
          days_to_qend, score, notes, fetched_at))
    conn.commit()
    conn.close()
    logger.info(f"Rebalancing signal: {notes} (score={score})")
    return score


# ═══════════════════════════════════════════════════════════════════════════════
# Signal 10 — VIX TERM STRUCTURE (contango / backwardation)
# ═══════════════════════════════════════════════════════════════════════════════

def run_vix_structure(as_of: date | None = None) -> float:
    """
    Fetch VIX spot (^VIX) and 3-month VIX (^VIX3M) from yfinance.
    Contango (VIX3M > VIX): complacency → stay long.
    Backwardation (VIX spot > VIX3M): fear → reduce exposure.
    Returns market-wide score (-2 to +2).
    """
    today_str = (as_of or date.today()).isoformat()
    fetched_at = datetime.utcnow().isoformat()

    vix_spot = vix_3m = None

    def _yf_last(ticker: str) -> float | None:
        """Download single ticker, return latest close."""
        try:
            df = yf.download(ticker, period="5d", interval="1d",
                             progress=False, auto_adjust=True)
            if df.empty:
                return None
            # Handle multi-level columns (yfinance ≥0.2.x)
            if isinstance(df.columns, pd.MultiIndex):
                close_col = df.xs("Close", axis=1, level=0) if "Close" in df.columns.get_level_values(0) else df.xs("Adj Close", axis=1, level=0)
                series = close_col.iloc[:, 0].dropna()
            else:
                series = (df["Close"] if "Close" in df.columns else df["Adj Close"]).dropna()
            return float(series.iloc[-1]) if not series.empty else None
        except Exception as e:
            logger.debug(f"yf_last {ticker}: {e}")
            return None

    vix_spot = _yf_last("^VIX")
    vix_3m   = _yf_last("^VIX3M")

    if not vix_spot or not vix_3m:
        logger.warning("VIX structure: insufficient data")
        return 0.0

    ratio = vix_3m / vix_spot  # > 1 = contango (bullish), < 1 = backwardation (bearish)

    if ratio >= 1.20:
        state = "STRONG_CONTANGO"
        score = 2.0
    elif ratio >= 1.08:
        state = "CONTANGO"
        score = 1.0
    elif ratio >= 0.95:
        state = "FLAT"
        score = 0.0
    elif ratio >= 0.85:
        state = "BACKWARDATION"
        score = -1.0
    else:
        state = "STRONG_BACKWARDATION"
        score = -2.0

    conn = _conn()
    conn.execute("""
        INSERT OR REPLACE INTO vix_structure_signals
        (trade_date, vix_spot, vix_3m, contango_ratio, structure_state,
         signal_score, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (today_str, round(vix_spot, 2), round(vix_3m, 2),
          round(ratio, 4), state, score, fetched_at))
    conn.commit()
    conn.close()
    logger.info(f"VIX structure: spot={vix_spot:.1f} 3M={vix_3m:.1f} "
                f"ratio={ratio:.3f} → {state} (score={score})")
    return score


# ═══════════════════════════════════════════════════════════════════════════════
# Composite Alpha Score
# ═══════════════════════════════════════════════════════════════════════════════

def compute_composite(
    as_of: date | None,
    dark_pool_scores:            dict[str, float],
    ftd_scores:                  dict[str, float],
    insider_scores:              dict[str, float],
    put_call_score:              float,
    yield_curve_score:           float,
    sentiment_scores:            dict[str, float],
    opex_score:                  float,
    rebalancing_score:           float,
    vix_structure_score:         float,
    earnings_scores:             dict[str, float],
    rallies_consensus_scores:    dict[str, float] | None = None,
    rallies_debate_scores:       dict[str, float] | None = None,
) -> list[dict]:
    """
    Compute weighted composite alpha score per symbol.
    Market-wide signals (P/C, yield curve, OPEX, rebalancing, VIX) apply to all symbols.
    Per-symbol signals: dark_pool, ftd, insider, sentiment, earnings, rallies_consensus,
    rallies_debate_sentiment.
    Returns list of composite rows for DB insertion.
    """
    today_str  = (as_of or date.today()).isoformat()
    fetched_at = datetime.utcnow().isoformat()
    rows: list[dict] = []

    r_consensus = rallies_consensus_scores or {}
    r_debate    = rallies_debate_scores    or {}

    for sym in ALPHA_UNIVERSE:
        signal_map = {
            "dark_pool":                dark_pool_scores.get(sym, None),
            "ftd":                      ftd_scores.get(sym, None),
            "insider":                  insider_scores.get(sym, None),
            "put_call":                 put_call_score if put_call_score != 0.0 else None,
            "yield_curve":              yield_curve_score if yield_curve_score != 0.0 else None,
            "sentiment":                sentiment_scores.get(sym, None),
            "opex":                     opex_score if opex_score != 0.0 else None,
            "rebalancing":              rebalancing_score if rebalancing_score != 0.0 else None,
            "vix_structure":            vix_structure_score if vix_structure_score != 0.0 else None,
            "earnings":                 earnings_scores.get(sym, None),
            "rallies_consensus":        r_consensus.get(sym, None),
            "rallies_debate_sentiment": r_debate.get(sym, None),
        }

        weighted_sum = 0.0
        weight_used  = 0.0
        for key, val in signal_map.items():
            if val is not None:
                w = WEIGHTS.get(key, 0.0)
                if w > 0:
                    weighted_sum += _clamp(val) * w
                    weight_used  += w

        composite    = round(weighted_sum / weight_used, 4) if weight_used > 0 else 0.0
        signal_count = sum(1 for v in signal_map.values() if v is not None)

        rows.append({
            "as_of_date":               today_str,
            "symbol":                   sym,
            "composite_score":          composite,
            "dark_pool_score":          signal_map["dark_pool"],
            "ftd_score":                signal_map["ftd"],
            "insider_score":            signal_map["insider"],
            "put_call_score":           put_call_score,
            "vix_structure_score":      vix_structure_score,
            "sentiment_score":          signal_map["sentiment"],
            "yield_curve_score":        yield_curve_score,
            "opex_score":               opex_score,
            "earnings_score":           signal_map["earnings"],
            "rebalancing_score":        rebalancing_score,
            "rallies_consensus_score":  signal_map["rallies_consensus"],
            "rallies_debate_score":     signal_map["rallies_debate_sentiment"],
            "signal_count":             signal_count,
            "created_at":               fetched_at,
        })

    return rows


def _save_composite(rows: list[dict]) -> None:
    conn = _conn()
    for r in rows:
        conn.execute("""
            INSERT OR REPLACE INTO composite_alpha
            (as_of_date, symbol, composite_score,
             dark_pool_score, ftd_score, insider_score,
             put_call_score, vix_structure_score, sentiment_score,
             yield_curve_score, opex_score, earnings_score, rebalancing_score,
             rallies_consensus_score, rallies_debate_score,
             signal_count, alert_fired, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
        """, (
            r["as_of_date"], r["symbol"], r["composite_score"],
            r.get("dark_pool_score"), r.get("ftd_score"), r.get("insider_score"),
            r.get("put_call_score"), r.get("vix_structure_score"), r.get("sentiment_score"),
            r.get("yield_curve_score"), r.get("opex_score"), r.get("earnings_score"),
            r.get("rebalancing_score"),
            r.get("rallies_consensus_score"), r.get("rallies_debate_score"),
            r["signal_count"], r["created_at"],
        ))
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# Alerts — ntfy + Signal Center
# ═══════════════════════════════════════════════════════════════════════════════

_NTFY_URL = "https://ntfy.sh"


def _ntfy_send(title: str, body: str, priority: int = 3, tags: list[str] | None = None) -> None:
    """Fire-and-forget ntfy push in a daemon thread."""
    def _send():
        try:
            data = json.dumps({
                "topic": "ollietrades",
                "title": title,
                "message": body,
                "priority": priority,
                "tags": tags or [],
            }).encode()
            req = urllib.request.Request(
                _NTFY_URL, data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=6)
        except Exception:
            pass

    threading.Thread(target=_send, daemon=True).start()


def _post_to_signal_center(sym: str, score: float, details: dict) -> None:
    """Post composite alpha score to Signal Center (port 9000) if it's running."""
    try:
        from engine.signal_poster import post_to_9000
        direction = "BUY" if score >= 1.0 else ("SELL" if score <= -1.0 else "HOLD")
        post_to_9000("ALPHA_SIGNAL", {
            "symbol": sym,
            "composite_score": score,
            "direction": direction,
            "signal_count": details.get("signal_count", 0),
            "dark_pool": details.get("dark_pool_score"),
            "ftd": details.get("ftd_score"),
            "insider": details.get("insider_score"),
            "sentiment": details.get("sentiment_score"),
            "earnings": details.get("earnings_score"),
            "put_call": details.get("put_call_score"),
            "vix_structure": details.get("vix_structure_score"),
            "yield_curve": details.get("yield_curve_score"),
            "source": "dilithium_crystal",
        })
    except Exception:
        pass


def _fire_alpha_alerts(rows: list[dict]) -> None:
    """Send ntfy alerts and Signal Center posts for all symbols above threshold."""
    alerts = [r for r in rows if abs(r["composite_score"]) >= ALERT_THRESHOLD]
    if not alerts:
        return

    # Sort by absolute score descending
    alerts.sort(key=lambda r: -abs(r["composite_score"]))

    # Aggregate alert message
    bull = [r for r in alerts if r["composite_score"] >= ALERT_THRESHOLD]
    bear = [r for r in alerts if r["composite_score"] <= -ALERT_THRESHOLD]

    if bull:
        bull_syms = ", ".join(f"{r['symbol']}({r['composite_score']:+.2f})" for r in bull[:5])
        _ntfy_send(
            title=f"Dilithium Crystal — ALPHA BUY ({len(bull)} signals)",
            body=f"Strong alpha: {bull_syms}\n"
                 f"P/C={rows[0]['put_call_score']:.2f} "
                 f"VIX={rows[0]['vix_structure_score']:.1f}",
            priority=4,
            tags=["white_check_mark", "crystal_ball"],
        )

    if bear:
        bear_syms = ", ".join(f"{r['symbol']}({r['composite_score']:+.2f})" for r in bear[:5])
        _ntfy_send(
            title=f"Dilithium Crystal — ALPHA SELL ({len(bear)} signals)",
            body=f"Bearish alpha: {bear_syms}\n"
                 f"P/C={rows[0]['put_call_score']:.2f} "
                 f"VIX={rows[0]['vix_structure_score']:.1f}",
            priority=4,
            tags=["red_circle", "crystal_ball"],
        )

    # Mark alert_fired in DB
    conn = _conn()
    today_str = rows[0]["as_of_date"] if rows else _today_str()
    for r in alerts:
        conn.execute(
            "UPDATE composite_alpha SET alert_fired=1 WHERE as_of_date=? AND symbol=?",
            (today_str, r["symbol"]),
        )
        _post_to_signal_center(r["symbol"], r["composite_score"], r)

    conn.commit()
    conn.close()
    logger.info(f"Alerts: {len(bull)} bullish, {len(bear)} bearish signals fired to ntfy + port 9000")


# ═══════════════════════════════════════════════════════════════════════════════
# Main orchestrator
# ═══════════════════════════════════════════════════════════════════════════════

def run_all_signals(as_of: date | None = None) -> dict:
    """
    Run all 10 alpha signal sources, compute composite scores, fire alerts.
    Designed to run in ~3-5 minutes at 7:00 AM AZ before market open.

    Returns summary dict with composite scores and timing.
    """
    start_ts = time.time()
    today = as_of or date.today()
    today_str = today.isoformat()

    logger.info(f"━━━ Dilithium Crystal alpha signals run: {today_str} ━━━")
    _init_db()

    # ── Run each signal source ──
    logger.info("[1/10] Dark Pool (FINRA short volume)...")
    dark_pool_scores = {}
    try:
        dark_pool_scores = run_dark_pool(today)
    except Exception as e:
        logger.error(f"Dark pool failed: {e}")

    logger.info("[2/10] FTD (SEC failure to deliver)...")
    ftd_scores = {}
    try:
        ftd_scores = run_ftd(today)
    except Exception as e:
        logger.error(f"FTD failed: {e}")

    logger.info("[3/10] Insider cluster buys (EDGAR Form 4)...")
    insider_scores = {}
    try:
        insider_scores = run_insider_cluster(today)
    except Exception as e:
        logger.error(f"Insider cluster failed: {e}")

    logger.info("[4/10] Put/Call ratio (CBOE)...")
    put_call_score = 0.0
    try:
        put_call_score = run_put_call(today)
    except Exception as e:
        logger.error(f"P/C ratio failed: {e}")

    logger.info("[5/10] Yield curve (FRED 2Y/10Y)...")
    yield_curve_score = 0.0
    try:
        yield_curve_score = run_yield_curve(today)
    except Exception as e:
        logger.error(f"Yield curve failed: {e}")

    logger.info("[6/10] Reddit retail sentiment...")
    sentiment_scores = {}
    try:
        sentiment_scores = run_reddit_sentiment(today)
    except Exception as e:
        logger.error(f"Reddit sentiment failed: {e}")

    # Blend FinGPT news sentiment into sentiment_scores
    try:
        from engine.fingpt_sentiment import get_sentiment
        for sym in ALPHA_UNIVERSE:
            news = get_sentiment(sym)
            if news and news.get("sentiment"):
                s = news["sentiment"]
                strength = news.get("avg_strength", 5) / 10.0  # normalize 0-1
                if "BULLISH" in s:
                    news_score = strength
                elif "BEARISH" in s:
                    news_score = -strength
                else:
                    news_score = 0.0
                # Blend: average with Reddit if available, else use news alone
                if sym in sentiment_scores:
                    sentiment_scores[sym] = round((sentiment_scores[sym] + news_score) / 2, 4)
                else:
                    sentiment_scores[sym] = round(news_score, 4)
        logger.info(f"[6b/10] FinGPT news sentiment blended for {len(sentiment_scores)} symbols")
    except Exception as e:
        logger.warning(f"FinGPT news sentiment blend failed: {e}")

    logger.info("[7/10] Earnings signals (yfinance)...")
    earnings_scores = {}
    try:
        earnings_scores = run_earnings_signals(today)
    except Exception as e:
        logger.error(f"Earnings signals failed: {e}")

    logger.info("[8/10] OPEX gamma unwind (calendar)...")
    opex_score = 0.0
    try:
        opex_score = run_opex_signal(today)
    except Exception as e:
        logger.error(f"OPEX signal failed: {e}")

    logger.info("[9/10] Quarter-end rebalancing (calendar)...")
    rebalancing_score = 0.0
    try:
        rebalancing_score = run_rebalancing_signal(today)
    except Exception as e:
        logger.error(f"Rebalancing signal failed: {e}")

    logger.info("[10/12] VIX term structure (yfinance)...")
    vix_structure_score = 0.0
    try:
        vix_structure_score = run_vix_structure(today)
    except Exception as e:
        logger.error(f"VIX structure failed: {e}")

    logger.info("[11/12] Rallies.ai consensus (arena portfolios)...")
    rallies_consensus_scores: dict[str, float] = {}
    try:
        from engine.rallies_scraper import get_all_rallies_consensus_scores
        rallies_consensus_scores = get_all_rallies_consensus_scores()
        logger.info(f"Rallies consensus: {len(rallies_consensus_scores)} symbols scored")
    except Exception as e:
        logger.warning(f"Rallies consensus unavailable: {e}")

    logger.info("[12/12] Rallies.ai debate sentiment (arena feed)...")
    rallies_debate_scores: dict[str, float] = {}
    try:
        from engine.rallies_scraper import get_all_rallies_debate_scores
        rallies_debate_scores = get_all_rallies_debate_scores()
        logger.info(f"Rallies debate: {len(rallies_debate_scores)} symbols scored")
    except Exception as e:
        logger.warning(f"Rallies debate sentiment unavailable: {e}")

    # ── Compute composite ──
    logger.info("Computing composite alpha scores (12 signals)...")
    composite_rows = compute_composite(
        as_of=today,
        dark_pool_scores=dark_pool_scores,
        ftd_scores=ftd_scores,
        insider_scores=insider_scores,
        put_call_score=put_call_score,
        yield_curve_score=yield_curve_score,
        sentiment_scores=sentiment_scores,
        opex_score=opex_score,
        rebalancing_score=rebalancing_score,
        vix_structure_score=vix_structure_score,
        earnings_scores=earnings_scores,
        rallies_consensus_scores=rallies_consensus_scores,
        rallies_debate_scores=rallies_debate_scores,
    )
    _save_composite(composite_rows)

    # ── Fire alerts ──
    _fire_alpha_alerts(composite_rows)

    elapsed = round(time.time() - start_ts, 1)
    top_bull = sorted([r for r in composite_rows if r["composite_score"] > 0],
                      key=lambda r: -r["composite_score"])[:5]
    top_bear = sorted([r for r in composite_rows if r["composite_score"] < 0],
                      key=lambda r: r["composite_score"])[:5]

    summary = {
        "status":            "ok",
        "run_date":          today_str,
        "elapsed_seconds":   elapsed,
        "symbols_scored":    len(composite_rows),
        "signals_fired":     sum(1 for r in composite_rows if abs(r["composite_score"]) >= ALERT_THRESHOLD),
        "market_signals": {
            "put_call_score":    put_call_score,
            "yield_curve_score": yield_curve_score,
            "vix_structure_score": vix_structure_score,
            "opex_score":        opex_score,
            "rebalancing_score": rebalancing_score,
        },
        "top_bullish": [{"symbol": r["symbol"], "score": r["composite_score"]} for r in top_bull],
        "top_bearish": [{"symbol": r["symbol"], "score": r["composite_score"]} for r in top_bear],
    }

    logger.info(
        f"━━━ Alpha run complete in {elapsed}s — "
        f"{summary['signals_fired']} alerts fired ━━━"
    )
    logger.info(f"Top bullish: {[r['symbol'] for r in top_bull]}")
    logger.info(f"Top bearish: {[r['symbol'] for r in top_bear]}")
    return summary


def get_latest_composite(symbols: list[str] | None = None) -> list[dict]:
    """Read the most recent composite alpha scores from DB."""
    conn = _conn()
    syms = symbols or ALPHA_UNIVERSE
    placeholders = ",".join("?" * len(syms))
    rows = conn.execute(f"""
        SELECT c.*
        FROM composite_alpha c
        INNER JOIN (
            SELECT symbol, MAX(as_of_date) as max_date
            FROM composite_alpha GROUP BY symbol
        ) latest ON c.symbol = latest.symbol AND c.as_of_date = latest.max_date
        WHERE c.symbol IN ({placeholders})
        ORDER BY c.composite_score DESC
    """, syms).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════════════════════════════
# Scheduler hook (called from main.py)
# ═══════════════════════════════════════════════════════════════════════════════

_last_run_date: str = ""


def run_alpha_signals_job() -> None:
    """
    Scheduler entry point — check if today's run has already completed.
    main.py should call: schedule.every().day.at("07:00").do(run_alpha_signals_job)
    """
    global _last_run_date
    today_str = _today_str()

    # Skip weekends
    if date.today().weekday() >= 5:
        return

    # Skip if already ran today
    if _last_run_date == today_str:
        return

    logger.info("Alpha signals job triggered by scheduler")
    try:
        summary = run_all_signals()
        _last_run_date = today_str
        logger.info(f"Alpha signals job done: {summary.get('signals_fired', 0)} alerts")
    except Exception as e:
        logger.error(f"Alpha signals job error: {e}", exc_info=True)


# ═══════════════════════════════════════════════════════════════════════════════
# Standalone runner
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [alpha] %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH),
            logging.StreamHandler(),
        ],
    )

    parser = argparse.ArgumentParser(description="Dilithium Crystal Alpha Signal Layer")
    parser.add_argument("--date", help="Override run date YYYY-MM-DD", default=None)
    parser.add_argument("--signal", help="Run single signal: dark_pool|ftd|insider|"
                        "put_call|yield_curve|sentiment|earnings|opex|rebalancing|vix",
                        default=None)
    parser.add_argument("--show", action="store_true",
                        help="Show latest composite scores from DB (no fetch)")
    args = parser.parse_args()

    run_date = date.fromisoformat(args.date) if args.date else date.today()

    if args.show:
        _init_db()
        rows = get_latest_composite()
        print(f"\n{'Symbol':<8} {'Score':>7} {'DarkPool':>9} {'FTD':>6} "
              f"{'Insider':>8} {'P/C':>5} {'VIX':>5} {'Senti':>6}")
        print("─" * 70)
        for r in rows:
            print(f"{r['symbol']:<8} {r['composite_score']:>+7.3f} "
                  f"{(r['dark_pool_score'] or 0):>+9.2f} "
                  f"{(r['ftd_score'] or 0):>+6.2f} "
                  f"{(r['insider_score'] or 0):>+8.2f} "
                  f"{(r['put_call_score'] or 0):>+5.2f} "
                  f"{(r['vix_structure_score'] or 0):>+5.2f} "
                  f"{(r['sentiment_score'] or 0):>+6.2f}")
        raise SystemExit(0)

    if args.signal:
        _init_db()
        sig = args.signal.lower()
        fn_map = {
            "dark_pool":   lambda: run_dark_pool(run_date),
            "ftd":         lambda: run_ftd(run_date),
            "insider":     lambda: run_insider_cluster(run_date),
            "put_call":    lambda: run_put_call(run_date),
            "yield_curve": lambda: run_yield_curve(run_date),
            "sentiment":   lambda: run_reddit_sentiment(run_date),
            "earnings":    lambda: run_earnings_signals(run_date),
            "opex":        lambda: run_opex_signal(run_date),
            "rebalancing": lambda: run_rebalancing_signal(run_date),
            "vix":         lambda: run_vix_structure(run_date),
        }
        if sig not in fn_map:
            print(f"Unknown signal: {sig}. Options: {list(fn_map)}")
            raise SystemExit(1)
        result = fn_map[sig]()
        print(f"\nResult: {result}")
        raise SystemExit(0)

    # Full run
    summary = run_all_signals(run_date)
    print(f"\n{'━'*60}")
    print(f"  Dilithium Crystal Alpha Signal Summary — {summary['run_date']}")
    print(f"{'━'*60}")
    print(f"  Elapsed:       {summary['elapsed_seconds']}s")
    print(f"  Symbols:       {summary['symbols_scored']}")
    print(f"  Alerts fired:  {summary['signals_fired']}")
    print(f"\n  Market Signals:")
    for k, v in summary["market_signals"].items():
        print(f"    {k:<22}: {v:+.2f}")
    print(f"\n  Top Bullish: {[r['symbol'] for r in summary['top_bullish']]}")
    print(f"  Top Bearish: {[r['symbol'] for r in summary['top_bearish']]}")
    print(f"{'━'*60}\n")
