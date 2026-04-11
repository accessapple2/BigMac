"""Universe Scanner — Ensign Chekov's nightly sweep of 500+ stocks.

Scans S&P 500 + popular growth/momentum names using FREE Yahoo Finance data.
Runs nightly at 11 PM MST. Finds tomorrow's top 50 candidates by technical score.
"""
from __future__ import annotations
import sqlite3
import json
import time
import threading
from datetime import datetime
from rich.console import Console

console = Console()
DB = "data/trader.db"

_cache = {"data": [], "ts": 0, "total_scanned": 0}
_cache_lock = threading.Lock()
_CACHE_TTL = 3600 * 8  # 8 hours — nightly scan, valid until next day

# Popular non-S&P tickers to include
EXTRA_TICKERS = [
    "CRWD", "MELI", "CRDO", "SMCI", "MRVL", "ANET", "VEEV", "DDOG", "NET",
    "ZS", "PANW", "SNOW", "COIN", "RBLX", "RIVN", "LCID", "SOFI", "HOOD",
    "IONQ", "RGTI", "ARM", "HIMS", "MSTR", "CELH", "DUOL", "SOUN", "JOBY",
    "OKLO", "RKLB", "LUNR", "ASTS", "ACHR", "VST", "CEG", "FTNT", "WDAY",
]


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def ensure_universe_tables():
    conn = _conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS universe_scan (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date DATE NOT NULL,
            ticker TEXT NOT NULL,
            close REAL,
            volume INTEGER,
            volume_ratio REAL,
            rsi REAL,
            score INTEGER,
            signals TEXT,
            gap_pct REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(scan_date, ticker)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS strategy_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date DATE NOT NULL,
            ticker TEXT NOT NULL,
            strategy_name TEXT NOT NULL,
            signal_type TEXT,
            confidence REAL,
            entry_price REAL,
            stop_price REAL,
            target_price REAL,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS strategy_optimization (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            opt_date DATE NOT NULL,
            strategy_name TEXT NOT NULL,
            win_rate REAL,
            profit_factor REAL,
            risk_reward REAL,
            total_trades INTEGER,
            deployed BOOLEAN,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def _get_sp500_tickers() -> list:
    """Get S&P 500 tickers from Wikipedia."""
    try:
        import pandas as pd
        import requests
        # Wikipedia blocks default pandas user-agent, use requests first
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) TradeMinds/1.0"}
        resp = requests.get(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            headers=headers, timeout=15,
        )
        resp.raise_for_status()
        tables = pd.read_html(resp.text, attrs={"id": "constituents"})
        if tables:
            tickers = tables[0]["Symbol"].tolist()
            # Fix tickers with dots (BRK.B → BRK-B for yfinance)
            return [t.replace(".", "-") for t in tickers]
    except Exception as e:
        console.log(f"[yellow]S&P 500 Wikipedia fetch failed: {e}")

    # Fallback: hardcoded top ~100 S&P 500 by market cap
    return [
        "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA", "BRK-B", "AVGO", "JPM",
        "LLY", "V", "UNH", "MA", "XOM", "COST", "HD", "PG", "JNJ", "ABBV",
        "WMT", "NFLX", "BAC", "CRM", "AMD", "CVX", "KO", "MRK", "PEP", "TMO",
        "ACN", "LIN", "MCD", "CSCO", "ADBE", "ABT", "WFC", "DHR", "TXN", "PM",
        "MS", "NEE", "QCOM", "ISRG", "INTU", "GE", "AMGN", "AMAT", "NOW", "IBM",
        "GS", "CAT", "PFE", "RTX", "BLK", "BKNG", "T", "LOW", "UBER", "UNP",
        "SPGI", "SYK", "VRTX", "ADP", "SCHW", "BSX", "GILD", "MMC", "LRCX", "MDT",
        "CB", "TMUS", "DE", "PLD", "ADI", "FI", "MO", "PANW", "SO", "ICE",
        "CI", "DUK", "CL", "EQIX", "PYPL", "CME", "SNPS", "CDNS", "MU", "MCK",
        "SHW", "ZTS", "HCA", "NOC", "CMG", "ORLY", "WM", "APH", "USB", "PNC",
        "DELL", "ORCL", "PLTR", "INTC", "F", "GM", "RIVN", "LCID", "SHOP", "SQ",
    ]


def _calculate_rsi(closes, period=14):
    """Calculate RSI from a list/series of close prices."""
    import numpy as np
    closes = np.array(closes, dtype=float)
    if len(closes) < period + 1:
        return 50.0
    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)


def _calculate_macd(closes):
    """Calculate MACD line and signal line."""
    import numpy as np
    closes = np.array(closes, dtype=float)
    if len(closes) < 26:
        return 0, 0

    def ema(data, span):
        alpha = 2 / (span + 1)
        result = [data[0]]
        for i in range(1, len(data)):
            result.append(alpha * data[i] + (1 - alpha) * result[-1])
        return np.array(result)

    ema12 = ema(closes, 12)
    ema26 = ema(closes, 26)
    macd_line = ema12 - ema26
    signal = ema(macd_line[-9:], 9) if len(macd_line) >= 9 else [0]
    return float(macd_line[-1]), float(signal[-1])


def get_core_watchlist() -> list[str]:
    """Return the core ~528 stock watchlist (S&P 500 + popular growth names).

    This is the stable, proven list that the nightly universe scan runs against.
    Volume scanner hot stocks are ADDED to this, not replacing it.
    """
    tickers = _get_sp500_tickers()
    tickers = list(set(tickers + EXTRA_TICKERS))
    return tickers


def scan_universe(max_tickers: int = 600) -> list:
    """Nightly scan of 500+ stocks to find tomorrow's top 50 candidates.

    Uses ONLY free Yahoo Finance data. Returns sorted list of scored candidates.
    """
    import yfinance as yf
    import numpy as np

    console.log("[cyan]🧭 Ensign Chekov: Starting universe scan...")

    # Build ticker list
    tickers = _get_sp500_tickers()
    tickers.extend(EXTRA_TICKERS)
    tickers = list(set(tickers))[:max_tickers]
    console.log(f"[cyan]🧭 Scanning {len(tickers)} tickers...")

    # Batch download 3 months of data
    try:
        data = yf.download(
            tickers, period="3mo", group_by="ticker",
            threads=True, progress=False, auto_adjust=True
        )
    except Exception as e:
        console.log(f"[red]Universe scan download failed: {e}")
        return []

    results = []
    for ticker in tickers:
        try:
            # Extract ticker data from multi-level DataFrame
            try:
                if len(tickers) == 1:
                    df = data
                else:
                    df = data[ticker].dropna()
            except (KeyError, TypeError):
                continue

            if df is None or len(df) < 20:
                continue

            close_arr = df["Close"].values
            vol_arr = df["Volume"].values
            high_arr = df["High"].values
            low_arr = df["Low"].values

            close = float(close_arr[-1])
            volume = float(vol_arr[-1])
            if close <= 0 or volume <= 0:
                continue

            avg_volume = float(np.mean(vol_arr[-20:])) if len(vol_arr) >= 20 else volume

            # Technicals
            rsi = _calculate_rsi(close_arr)
            sma20 = float(np.mean(close_arr[-20:])) if len(close_arr) >= 20 else close
            sma50 = float(np.mean(close_arr[-50:])) if len(close_arr) >= 50 else close

            # Bollinger Bands
            if len(close_arr) >= 20:
                bb_std = float(np.std(close_arr[-20:]))
                bb_upper = sma20 + 2 * bb_std
                bb_lower = sma20 - 2 * bb_std
            else:
                bb_upper = bb_lower = close

            # Score (0-100)
            score = 0
            signals = []

            # 1. Volume surge (2x+ avg)
            vol_ratio = volume / avg_volume if avg_volume > 0 else 0
            if vol_ratio >= 2.0:
                score += 20
                signals.append("VOLUME_SURGE")

            # 2. RSI oversold
            if rsi < 30:
                score += 20
                signals.append("RSI_OVERSOLD")
            elif rsi > 70:
                score += 5
                signals.append("RSI_OVERBOUGHT")

            # 3. Uptrend (above 20 & 50 SMA)
            if close > sma20 and close > sma50:
                score += 15
                signals.append("UPTREND")

            # 4. Golden cross (20 SMA > 50 SMA, just crossed)
            if len(close_arr) >= 51 and sma20 > sma50:
                prev_sma20 = float(np.mean(close_arr[-21:-1]))
                prev_sma50 = float(np.mean(close_arr[-51:-1]))
                if prev_sma20 <= prev_sma50:
                    score += 25
                    signals.append("GOLDEN_CROSS")

            # 5. Bollinger squeeze
            bb_width = (bb_upper - bb_lower) / close if close > 0 else 0
            if 0 < bb_width < 0.05:
                score += 15
                signals.append("BB_SQUEEZE")

            # 6. Near 52-week high (momentum)
            high_52w = float(np.max(high_arr)) if len(high_arr) >= 50 else float(np.max(high_arr))
            if close > high_52w * 0.95:
                score += 10
                signals.append("NEAR_52W_HIGH")

            # 7. Near 52-week low (value)
            low_52w = float(np.min(low_arr)) if len(low_arr) >= 50 else float(np.min(low_arr))
            if close < low_52w * 1.10:
                score += 10
                signals.append("NEAR_52W_LOW")

            # 8. MACD crossover
            if len(close_arr) >= 26:
                macd, signal_line = _calculate_macd(close_arr)
                if macd > signal_line:
                    # Check if just crossed
                    macd_prev, sig_prev = _calculate_macd(close_arr[:-1])
                    if macd_prev <= sig_prev:
                        score += 20
                        signals.append("MACD_CROSS_UP")

            # 9. Gap (>2%)
            if len(close_arr) >= 2:
                prev_close = float(close_arr[-2])
                gap_pct = (close - prev_close) / prev_close * 100 if prev_close > 0 else 0
            else:
                gap_pct = 0
            if abs(gap_pct) > 2:
                score += 10
                signals.append(f"GAP_{'UP' if gap_pct > 0 else 'DOWN'}_{abs(gap_pct):.1f}%")

            # 10. Unusual volume (3x+)
            if vol_ratio >= 3.0:
                score += 10
                signals.append("UNUSUAL_VOLUME_3X")

            if score >= 10:  # Minimum score filter
                results.append({
                    "ticker": ticker,
                    "close": round(close, 2),
                    "volume": int(volume),
                    "avg_volume": int(avg_volume),
                    "volume_ratio": round(vol_ratio, 2),
                    "rsi": round(rsi, 1),
                    "sma20": round(sma20, 2),
                    "sma50": round(sma50, 2),
                    "score": score,
                    "signals": signals,
                    "gap_pct": round(gap_pct, 2),
                })
        except Exception:
            continue

    results.sort(key=lambda x: x["score"], reverse=True)
    top50 = results[:50]

    console.log(f"[green]🧭 Ensign Chekov: Scanned {len(tickers)} stocks, "
                f"found {len(results)} with signals, top 50 selected "
                f"(best: {top50[0]['ticker']} score={top50[0]['score']} {top50[0]['signals']})"
                if top50 else f"[yellow]No signals found")

    # Save to DB
    _save_results(top50, len(tickers))

    # Update cache
    with _cache_lock:
        _cache["data"] = top50
        _cache["ts"] = time.time()
        _cache["total_scanned"] = len(tickers)

    return top50


def _save_results(results: list, total_scanned: int):
    """Save universe scan results to DB."""
    ensure_universe_tables()
    today = datetime.now().strftime("%Y-%m-%d")
    conn = _conn()
    for r in results:
        try:
            conn.execute(
                "INSERT OR REPLACE INTO universe_scan "
                "(scan_date, ticker, close, volume, volume_ratio, rsi, score, signals, gap_pct) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (today, r["ticker"], r["close"], r["volume"], r["volume_ratio"],
                 r["rsi"], r["score"], json.dumps(r["signals"]), r["gap_pct"]),
            )
        except Exception:
            pass
    conn.commit()
    conn.close()


def get_latest_universe_scan() -> dict:
    """Get the most recent universe scan results (from cache or DB)."""
    with _cache_lock:
        if _cache["data"] and time.time() - _cache["ts"] < _CACHE_TTL:
            return {
                "results": _cache["data"],
                "total_scanned": _cache["total_scanned"],
                "scan_time": datetime.fromtimestamp(_cache["ts"]).isoformat(),
            }

    # Fall back to DB
    try:
        conn = _conn()
        rows = conn.execute(
            "SELECT * FROM universe_scan WHERE scan_date = "
            "(SELECT MAX(scan_date) FROM universe_scan) ORDER BY score DESC LIMIT 50"
        ).fetchall()
        conn.close()

        if rows:
            results = []
            for r in rows:
                results.append({
                    "ticker": r["ticker"],
                    "close": r["close"],
                    "volume": r["volume"],
                    "volume_ratio": r["volume_ratio"],
                    "rsi": r["rsi"],
                    "score": r["score"],
                    "signals": json.loads(r["signals"]) if r["signals"] else [],
                    "gap_pct": r["gap_pct"],
                })
            with _cache_lock:
                _cache["data"] = results
                _cache["ts"] = time.time()
                _cache["total_scanned"] = len(results)
            return {
                "results": results,
                "total_scanned": len(results),
                "scan_time": str(rows[0]["created_at"]) if rows else None,
            }
    except Exception:
        pass

    return {"results": [], "total_scanned": 0, "scan_time": None}


def build_universe_prompt_section() -> str:
    """Build prompt section with universe scan results for AI models."""
    scan = get_latest_universe_scan()
    if not scan["results"]:
        return ""

    lines = [
        f"\n=== UNIVERSE SCANNER (Chekov's overnight sweep) ===",
        f"Scanned {scan['total_scanned']} stocks. Top candidates by technical score:",
    ]

    for s in scan["results"][:10]:
        sigs = ", ".join(s["signals"][:3])
        lines.append(
            f"  {s['ticker']}: Score {s['score']}/100 | "
            f"${s['close']:.2f} | RSI {s['rsi']:.0f} | "
            f"Vol {s['volume_ratio']:.1f}x | {sigs}"
        )

    top_tickers = ", ".join(s["ticker"] for s in scan["results"][:20])
    lines.append(f"\nTop 20 candidates: {top_tickers}")
    lines.append("These stocks showed the strongest technical setups. Consider them for your watchlist.")

    return "\n".join(lines)
