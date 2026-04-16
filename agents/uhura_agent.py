#!/usr/bin/env python3
"""
Lt. Uhura — Institutional Intelligence Officer
Intercepts SEC communications: 13F hedge fund holdings + Form 4 insider trades.
Uses free edgartools library — no API key needed.

Tables used:
  institutional_holdings  (created here if missing)
  institutional_signals   (created here if missing)
  insider_trades          (pre-existing schema respected)

Run: cd ~/autonomous-trader && python3 agents/uhura_agent.py
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path

# edgartools requires an identity string (EDGAR fair-use policy)
try:
    from edgar import Company, Entity, get_filings, set_identity
    set_identity("OllieTrades scanner ollietrades@example.com")
    EDGAR_AVAILABLE = True
except ImportError:
    EDGAR_AVAILABLE = False
    print("⚠️  edgartools not installed. Run: pip install edgartools --break-system-packages")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [UHURA] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "data" / "trader.db"

# Major hedge funds tracked — CIK numbers
HEDGE_FUNDS = {
    "Berkshire Hathaway":    "0001067983",
    "Citadel Advisors":      "0001423053",
    "Renaissance Technologies": "0001037389",
    "Pershing Square":       "0001336528",
    "Soros Fund Management": "0001029160",
    "Appaloosa Management":  "0001115066",
    "Third Point":           "0001040273",
}


# ── DB setup ──────────────────────────────────────────────────────────────────

def init_tables():
    """Create Uhura's tables if missing. Never touches existing tables."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS institutional_holdings (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_name       TEXT NOT NULL,
            fund_cik        TEXT,
            ticker          TEXT,
            cusip           TEXT,
            shares          INTEGER,
            value_usd       INTEGER,
            period_of_report TEXT,
            filed_at        TEXT,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS institutional_signals (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker      TEXT NOT NULL,
            signal      TEXT NOT NULL,
            reasoning   TEXT,
            scan_date   TEXT NOT NULL,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    log.info("Tables verified")


# ── 13F hedge fund holdings ───────────────────────────────────────────────────

def scan_13f_holdings() -> list[dict]:
    """Pull the most recent 13F-HR for each tracked fund via Entity(cik)."""
    if not EDGAR_AVAILABLE:
        return []

    conn = sqlite3.connect(DB_PATH)
    results: list[dict] = []

    for fund_name, cik in HEDGE_FUNDS.items():
        try:
            entity  = Entity(cik)
            filings = entity.get_filings(form="13F-HR")
            if not filings or len(filings) == 0:
                log.warning(f"{fund_name}: no 13F filings found")
                continue

            latest = filings[0]
            period = str(getattr(latest, "period_of_report", ""))
            filed  = str(getattr(latest, "filing_date", ""))

            filing_obj = latest.obj()
            if filing_obj is None:
                log.warning(f"{fund_name}: could not parse 13F object")
                continue

            holdings = getattr(filing_obj, "infotable", None)
            if holdings is None or len(holdings) == 0:
                log.info(f"{fund_name}: 0 holdings in latest 13F")
                continue

            # Actual columns: Issuer, Class, Cusip, Value, SharesPrnAmount, Type, PutCall, ..., Ticker
            count = 0
            for _, row in holdings.iterrows():
                cusip  = str(row.get("Cusip", "") or "")
                ticker = str(row.get("Ticker", "") or cusip[:6])
                shares = int(row.get("SharesPrnAmount", 0) or 0)
                value  = int(row.get("Value", 0) or 0)

                conn.execute("""
                    INSERT INTO institutional_holdings
                    (fund_name, fund_cik, ticker, cusip, shares, value_usd, period_of_report, filed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (fund_name, cik, ticker, cusip, shares, value, period, filed))

                results.append({"fund": fund_name, "ticker": ticker,
                                 "shares": shares, "value": value})
                count += 1

            conn.commit()
            log.info(f"{fund_name}: {count} holdings imported (period {period})")

        except Exception as e:
            log.warning(f"{fund_name}: {e}")

    conn.close()
    return results


# ── Form 4 insider trades ─────────────────────────────────────────────────────

def scan_insider_trades(tickers: list[str], max_tickers: int = 20) -> list[dict]:
    """
    Pull recent Form 4 insider trades for the given tickers.
    Inserts into the pre-existing insider_trades table.
    """
    if not EDGAR_AVAILABLE:
        return []

    conn = sqlite3.connect(DB_PATH)
    results: list[dict] = []

    for ticker in tickers[:max_tickers]:
        try:
            company = Company(ticker)
            form4s  = company.get_filings(form="4")
            if not form4s or len(form4s) == 0:
                continue

            for filing in list(form4s)[:5]:   # last 5 filings per ticker
                try:
                    trade_obj = filing.obj()
                    if trade_obj is None:
                        continue

                    insider_name = str(getattr(trade_obj, "insider_name", "") or "")
                    filed_date   = str(getattr(filing, "filing_date", ""))

                    # market_trades is a DataFrame with columns:
                    # Security, Date, Shares, Price, TransactionType, ...
                    market_trades = getattr(trade_obj, "market_trades", None)
                    if market_trades is None or len(market_trades) == 0:
                        continue

                    for _, tx in market_trades.iterrows():
                        tx_type = str(tx.get("TransactionType", "") or "")
                        shares  = int(tx.get("Shares", 0) or 0)
                        price   = float(tx.get("Price", 0) or 0)
                        tx_date = str(tx.get("Date", "") or "")
                        value   = round(shares * price, 2)

                        conn.execute("""
                            INSERT OR IGNORE INTO insider_trades
                            (symbol, insider_name, title, transaction_type,
                             shares, price_per_share, total_value,
                             filing_date, transaction_date)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (ticker, insider_name, "", tx_type,
                              shares, price, value, filed_date, tx_date))

                        results.append({"ticker": ticker, "insider": insider_name,
                                        "type": tx_type, "shares": shares})
                except Exception:
                    pass   # malformed filing — skip silently

            conn.commit()

        except Exception as e:
            log.warning(f"{ticker}: {e}")

    conn.close()
    return results


# ── Signal generation ─────────────────────────────────────────────────────────

def generate_signals() -> int:
    """
    Scan institutional_holdings + insider_trades for recent activity,
    write STRONG_BUY / BUY / SELL / STRONG_SELL signals.
    Returns number of signals written.
    """
    conn  = sqlite3.connect(DB_PATH)
    today = datetime.now().strftime("%Y-%m-%d")

    tickers = [
        r[0] for r in conn.execute("""
            SELECT DISTINCT ticker FROM institutional_holdings
            WHERE created_at > datetime('now', '-7 days')
              AND ticker != ''
            UNION
            SELECT DISTINCT symbol FROM insider_trades
            WHERE created_at > datetime('now', '-7 days')
        """).fetchall()
    ]

    count = 0
    for ticker in tickers:
        adds = conn.execute("""
            SELECT COUNT(DISTINCT fund_cik) FROM institutional_holdings
            WHERE ticker = ? AND created_at > datetime('now', '-90 days')
        """, (ticker,)).fetchone()[0]

        insider_buys = conn.execute("""
            SELECT COUNT(*) FROM insider_trades
            WHERE symbol = ? AND transaction_type LIKE '%urchase%'
              AND created_at > datetime('now', '-30 days')
        """, (ticker,)).fetchone()[0]

        insider_sells = conn.execute("""
            SELECT COUNT(*) FROM insider_trades
            WHERE symbol = ? AND transaction_type LIKE '%ale%'
              AND created_at > datetime('now', '-30 days')
        """, (ticker,)).fetchone()[0]

        # Filter out junk tickers (nan, empty, bare CUSIPs)
        if not ticker or ticker.lower() in ("nan", "none", "") or len(ticker) > 6:
            continue

        # STRONG_SELL must be checked BEFORE SELL (was unreachable)
        if insider_sells >= 3:
            signal, reasoning = "STRONG_SELL", f"{insider_sells} insider sells in 30 days"
        elif adds >= 4 and insider_buys >= 1:
            signal, reasoning = "STRONG_BUY", f"{adds} funds + {insider_buys} insider buys"
        elif adds >= 3 and insider_buys >= 1:
            signal, reasoning = "BUY", f"{adds} funds + {insider_buys} insider buys"
        elif insider_buys >= 2:
            signal, reasoning = "BUY", f"{insider_buys} insider buys in 30 days"
        elif insider_sells >= 2:
            signal, reasoning = "SELL", f"{insider_sells} insider sells in 30 days"
        else:
            continue   # NEUTRAL — don't clutter the table

        conn.execute("""
            INSERT INTO institutional_signals (ticker, signal, reasoning, scan_date)
            VALUES (?, ?, ?, ?)
        """, (ticker, signal, reasoning, today))
        count += 1
        log.info(f"  Signal: {ticker} → {signal} ({reasoning})")

    conn.commit()
    conn.close()
    return count


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    log.info("=== Lt. Uhura — Institutional Intelligence scan ===")
    init_tables()

    # 13F holdings scan
    holdings = scan_13f_holdings()
    log.info(f"13F import: {len(holdings)} holdings across {len(HEDGE_FUNDS)} funds")

    # Insider trades — use recent signal universe as ticker list
    conn = sqlite3.connect(DB_PATH)
    try:
        tickers = [r[0] for r in conn.execute("""
            SELECT DISTINCT symbol FROM signals
            WHERE created_at > datetime('now', '-7 days')
            LIMIT 30
        """).fetchall()]
    except Exception:
        tickers = ["AAPL", "NVDA", "MSFT", "GOOGL", "AMZN", "META", "TSLA"]
    conn.close()

    if not tickers:
        tickers = ["AAPL", "NVDA", "MSFT", "GOOGL", "AMZN", "META", "TSLA"]

    trades = scan_insider_trades(tickers)
    log.info(f"Form 4 import: {len(trades)} insider transactions across {len(tickers)} tickers")

    # Generate signals
    n = generate_signals()
    log.info(f"Signals generated: {n}")
    log.info("=== Uhura scan complete ===")


if __name__ == "__main__":
    run()
