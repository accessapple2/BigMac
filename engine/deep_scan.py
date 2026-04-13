from __future__ import annotations
"""Chekov's Deep Space Scan — Expands trading universe from ~528 to 2000+ stocks.

Pulls every active US equity from Alpaca, filters for liquidity / price, then
runs the full strategy suite across the expanded universe in batches.

Key functions
-------------
    build_universe(force)       — populate scan_universe table from Alpaca
    get_universe()              — return symbol list (with S&P 500 fallback)
    run_deep_scan(max, force)   — execute strategies across universe; store results
    get_deep_scan_results()     — read latest results from DB
    get_universe_stats()        — exchange / size summary

DB tables created on import:
    scan_universe       — filtered tradeable symbols with liquidity metrics
    deep_scan_results   — per-scan strategy signals
"""

import gc
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv
from rich.console import Console

load_dotenv()

console = Console()
logger = logging.getLogger("deep_scan")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [deep_scan] %(levelname)s: %(message)s",
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_PATH = os.environ.get("TRADEMINDS_DB", "data/trader.db")
ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "")

_VALID_EXCHANGES = {"NYSE", "NASDAQ", "AMEX", "ARCA"}
_UNIVERSE_STALE_DAYS = 6       # rebuild if older than this
_CHUNK_SIZE = 200              # symbols per Alpaca bars request
_MIN_AVG_VOLUME = 500_000.0
_MIN_AVG_PRICE = 5.0
_MAX_AVG_PRICE = 500.0
_BARS_LOOKBACK = 20            # trading days for avg_volume / avg_price

# ---------------------------------------------------------------------------
# Hardcoded S&P 500 + extras fallback (~528 symbols)
# ---------------------------------------------------------------------------

_SP500_FALLBACK: list[str] = [
    "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "GOOG", "META", "TSLA", "BRK-B", "AVGO",
    "JPM", "LLY", "V", "UNH", "MA", "XOM", "COST", "HD", "PG", "JNJ", "ABBV",
    "WMT", "NFLX", "BAC", "CRM", "AMD", "CVX", "KO", "MRK", "PEP", "TMO",
    "ACN", "LIN", "MCD", "CSCO", "ADBE", "ABT", "WFC", "DHR", "TXN", "PM",
    "MS", "NEE", "QCOM", "ISRG", "INTU", "GE", "AMGN", "AMAT", "NOW", "IBM",
    "GS", "CAT", "PFE", "RTX", "BLK", "BKNG", "T", "LOW", "UBER", "UNP",
    "SPGI", "SYK", "VRTX", "ADP", "SCHW", "BSX", "GILD", "MMC", "LRCX", "MDT",
    "CB", "TMUS", "DE", "PLD", "ADI", "FI", "MO", "PANW", "SO", "ICE",
    "CI", "DUK", "CL", "EQIX", "PYPL", "CME", "SNPS", "CDNS", "MU", "MCK",
    "SHW", "ZTS", "HCA", "NOC", "CMG", "ORLY", "WM", "APH", "USB", "PNC",
    "DELL", "ORCL", "PLTR", "INTC", "F", "GM", "RIVN", "LCID", "SHOP", "SQ",
    "CRWD", "MELI", "CRDO", "SMCI", "MRVL", "ANET", "VEEV", "DDOG", "NET",
    "ZS", "SNOW", "COIN", "RBLX", "SOFI", "HOOD", "IONQ", "RGTI", "ARM",
    "HIMS", "MSTR", "CELH", "DUOL", "SOUN", "JOBY", "OKLO", "RKLB", "LUNR",
    "ASTS", "ACHR", "VST", "CEG", "FTNT", "WDAY", "SPOT", "DASH", "ABNB",
    "ROKU", "PINS", "SNAP", "LYFT", "U", "RDFN", "OPEN", "MTTR", "BIRD",
    "MMM", "AOS", "ABT", "ABBV", "ACN", "ADBE", "ADI", "ADP", "ADSK", "AEP",
    "AES", "AFL", "AIG", "AIZ", "AJG", "AKAM", "ALB", "ALGN", "ALK", "ALL",
    "ALLE", "AMAT", "AME", "AMGN", "AMP", "AMT", "AMZN", "ANET", "ANSS", "AON",
    "APA", "APD", "APH", "APTV", "ARE", "ATO", "AVB", "AVGO", "AVY", "AWK",
    "AXP", "AZO", "BA", "BAC", "BALL", "BAX", "BBWI", "BBY", "BDX", "BEN",
    "BF-B", "BIIB", "BIO", "BK", "BKNG", "BKR", "BLK", "BMY", "BR", "BRO",
    "BSX", "BWA", "BXP", "C", "CAG", "CAH", "CARR", "CAT", "CB", "CBOE",
    "CBRE", "CCI", "CCL", "CDNS", "CDW", "CE", "CEG", "CF", "CFG", "CHD",
    "CHRW", "CHTR", "CI", "CINF", "CL", "CLX", "CMA", "CMCSA", "CME", "CMG",
    "CMI", "CMS", "CNC", "CNP", "COF", "COO", "COP", "COST", "CPB", "CPRT",
    "CRL", "CRM", "CSCO", "CSGP", "CSX", "CTAS", "CTLT", "CTRA", "CTSH",
    "CTVA", "CVS", "CVX", "CZR", "D", "DAL", "DAY", "DD", "DE", "DECK",
    "DFS", "DG", "DGX", "DHI", "DHR", "DIS", "DISH", "DLR", "DLTR", "DOV",
    "DOW", "DPZ", "DRI", "DTE", "DUK", "DVA", "DVN", "DXC", "DXCM", "EA",
    "EBAY", "ECL", "ED", "EFX", "EG", "EIX", "EL", "ELV", "EMN", "EMR",
    "ENPH", "EOG", "EPAM", "EQIX", "EQR", "EQT", "ES", "ESS", "ETN", "ETR",
    "EVRG", "EW", "EXC", "EXPD", "EXPE", "EXR", "F", "FANG", "FAST", "FCX",
    "FDS", "FDX", "FE", "FFIV", "FI", "FICO", "FIS", "FITB", "FLT", "FMC",
    "FOX", "FOXA", "FRC", "FRT", "FTNT", "FTV", "GD", "GE", "GEHC", "GEN",
    "GILD", "GIS", "GL", "GLW", "GM", "GNRC", "GOOGL", "GPC", "GPN", "GRMN",
    "GS", "GWW", "HAL", "HAS", "HBAN", "HCA", "HD", "HES", "HIG", "HII",
    "HLT", "HOLX", "HON", "HPE", "HPQ", "HRL", "HSIC", "HST", "HSY", "HUM",
    "HWM", "IBM", "ICE", "IDXX", "IEX", "IFF", "ILMN", "INCY", "INTC", "INTU",
    "INVH", "IP", "IPG", "IQV", "IR", "IRM", "ISRG", "IT", "ITW", "IVZ",
    "J", "JBHT", "JCI", "JKHY", "JNJ", "JNPR", "JPM", "K", "KEY", "KEYS",
    "KHC", "KIM", "KLAC", "KMB", "KMI", "KMX", "KO", "KR", "L", "LDOS",
    "LEN", "LH", "LHX", "LIN", "LKQ", "LLY", "LMT", "LNC", "LNT", "LOW",
    "LRCX", "LUMN", "LUV", "LVS", "LW", "LYB", "LYV", "MA", "MAA", "MAR",
    "MAS", "MCD", "MCHP", "MCK", "MCO", "MDLZ", "MDT", "MET", "MGM", "MHK",
    "MKC", "MKTX", "MLM", "MMC", "MMM", "MNST", "MO", "MOH", "MOS", "MPC",
    "MPWR", "MRK", "MRNA", "MRO", "MS", "MSCI", "MSFT", "MSI", "MTB", "MTCH",
    "MTD", "MU", "NCLH", "NDAQ", "NEE", "NEM", "NFLX", "NI", "NKE", "NOC",
    "NOW", "NRG", "NSC", "NTAP", "NTRS", "NUE", "NVDA", "NVR", "NWL", "NWS",
    "NWSA", "NXPI", "O", "ODFL", "OGN", "OKE", "OMC", "ON", "ORCL", "ORLY",
    "OTIS", "OXY", "PAYC", "PAYX", "PCAR", "PCG", "PEAK", "PEG", "PEP", "PFE",
    "PFG", "PG", "PGR", "PH", "PHM", "PKG", "PLD", "PM", "PNC", "PNR",
    "PNW", "PODD", "POOL", "PPG", "PPL", "PRU", "PSA", "PSX", "PTC", "PWR",
    "PYPL", "QCOM", "QRVO", "RCL", "REG", "REGN", "RF", "RJF", "RL", "RMD",
    "ROK", "ROL", "ROP", "ROST", "RSG", "RTX", "SBAC", "SBUX", "SEDG", "SEE",
    "SHW", "SJM", "SLB", "SNPS", "SO", "SPG", "SPGI", "SRE", "STE", "STLD",
    "STT", "STX", "STZ", "SWK", "SWKS", "SYF", "SYK", "SYY", "T", "TAP",
    "TDG", "TDY", "TECH", "TEL", "TER", "TFC", "TFX", "TGT", "TJX", "TMO",
    "TMUS", "TPR", "TRMB", "TROW", "TRV", "TSCO", "TSLA", "TSN", "TT", "TTWO",
    "TXN", "TYL", "UAL", "UDR", "UHS", "ULTA", "UNH", "UNP", "UPS", "URI",
    "USB", "V", "VFC", "VICI", "VLO", "VMC", "VRSK", "VRSN", "VRTX", "VTR",
    "VTRS", "VZ", "WAB", "WAT", "WBA", "WBD", "WDC", "WEC", "WELL", "WFC",
    "WHR", "WM", "WMB", "WMT", "WRB", "WRK", "WST", "WTW", "WY", "WYNN",
    "XEL", "XOM", "XRAY", "XYL", "YUM", "ZBH", "ZBRA", "ZION", "ZTS",
]

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def _init_db() -> None:
    """Create required tables and indexes on first import. Never DROP/DELETE/TRUNCATE."""
    with _conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS scan_universe (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL UNIQUE,
                name TEXT,
                exchange TEXT,
                sector TEXT,
                avg_volume REAL,
                avg_price REAL,
                last_updated TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS deep_scan_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_date TEXT NOT NULL,
                scan_time TEXT NOT NULL,
                symbol TEXT NOT NULL,
                strategy_name TEXT NOT NULL,
                signal_strength REAL DEFAULT 0.0,
                confidence REAL DEFAULT 0.0,
                entry_price REAL,
                stop_price REAL,
                target_price REAL,
                risk_reward REAL,
                volume REAL,
                avg_volume REAL,
                rel_volume REAL,
                sector TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        # Indexes
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_scan_universe_symbol "
            "ON scan_universe(symbol)"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_deep_scan_date "
            "ON deep_scan_results(scan_date)"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_deep_scan_symbol "
            "ON deep_scan_results(symbol)"
        )
        c.commit()


# ---------------------------------------------------------------------------
# Internal utilities
# ---------------------------------------------------------------------------


def _chunks(lst: list, n: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def _alpaca_configured() -> bool:
    return bool(ALPACA_API_KEY and ALPACA_SECRET_KEY)


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------


def build_universe(force: bool = False) -> dict:
    """Fetch all US equity assets from Alpaca and filter for liquidity / price.

    Checks if the universe was updated in the last 6 days; skips refresh if
    fresh and `force` is False.

    Filtering criteria:
        - asset.tradable == True
        - exchange in {NYSE, NASDAQ, AMEX, ARCA}
        - avg_volume > 500,000 over 20-day bars
        - 5.0 <= avg_price <= 500.0

    Data is fetched from Alpaca in chunks of 200 symbols.  The rate limiter is
    called before each bars request to stay within the 150 req/min budget.

    Returns:
        dict with keys: total_assets, filtered_count, universe_size,
                        duration_seconds, error (on failure)
    """
    t0 = time.time()

    if not _alpaca_configured():
        return {"error": "Alpaca keys not configured (ALPACA_API_KEY / ALPACA_SECRET_KEY)"}

    # Freshness check
    if not force:
        try:
            with _conn() as c:
                row = c.execute(
                    "SELECT last_updated FROM scan_universe ORDER BY last_updated DESC LIMIT 1"
                ).fetchone()
            if row:
                last_ts = datetime.fromisoformat(row["last_updated"])
                age_days = (datetime.now() - last_ts).days
                if age_days < _UNIVERSE_STALE_DAYS:
                    count = _conn().execute(
                        "SELECT COUNT(*) as n FROM scan_universe"
                    ).fetchone()["n"]
                    console.log(
                        f"[cyan][DeepScan] Universe fresh ({age_days}d old, "
                        f"{count} symbols) — skipping rebuild. Use force=True to override."
                    )
                    return {
                        "total_assets": count,
                        "filtered_count": count,
                        "universe_size": count,
                        "duration_seconds": round(time.time() - t0, 2),
                        "skipped": True,
                    }
        except Exception as e:
            logger.warning("Freshness check failed: %s", e)

    try:
        from alpaca.trading.client import TradingClient
        from alpaca.trading.requests import GetAssetsRequest
        from alpaca.trading.enums import AssetClass, AssetStatus
        from alpaca.data import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame
        from engine.rate_limiter import limiter
    except ImportError as e:
        return {"error": f"Import failed: {e}"}

    trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)
    data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

    # ----------------------------------------------------------------
    # Step 1 — fetch all active tradeable US equity assets
    # ----------------------------------------------------------------
    console.log("[cyan][DeepScan] Fetching asset list from Alpaca…")
    try:
        request_params = GetAssetsRequest(
            asset_class=AssetClass.US_EQUITY,
            status=AssetStatus.ACTIVE,
        )
        all_assets = trading_client.get_all_assets(request_params)
    except Exception as e:
        return {"error": f"Alpaca get_all_assets failed: {e}"}

    # Filter: tradable + valid exchange + alphabetic symbol (no warrants/preferred)
    candidates: list[dict] = []
    for asset in all_assets:
        try:
            if not asset.tradable:
                continue
            if asset.exchange not in _VALID_EXCHANGES:
                continue
            sym = asset.symbol or ""
            if not sym or not sym.isalpha() or len(sym) > 5:
                continue
            candidates.append({
                "symbol": sym,
                "name": getattr(asset, "name", "") or "",
                "exchange": asset.exchange,
            })
        except Exception:
            continue

    total_assets = len(candidates)
    console.log(f"[cyan][DeepScan] {total_assets} candidate symbols after exchange/tradable filter.")

    # ----------------------------------------------------------------
    # Step 2 — liquidity filter in chunks of 200
    # ----------------------------------------------------------------
    qualifying: list[dict] = []
    now_str = datetime.now().isoformat()
    end_date = datetime.now()
    start_date = end_date - timedelta(days=35)  # covers 20 trading days with weekends

    for chunk_idx, chunk in enumerate(_chunks(candidates, _CHUNK_SIZE)):
        chunk_symbols = [c["symbol"] for c in chunk]
        symbol_meta = {c["symbol"]: c for c in chunk}

        try:
            limiter.acquire()
            req = StockBarsRequest(
                symbol_or_symbols=chunk_symbols,
                timeframe=TimeFrame.Day,
                start=start_date,
                end=end_date,
                limit=_BARS_LOOKBACK * len(chunk_symbols),
            )
            bars_response = data_client.get_stock_bars(req)
            bars_data = bars_response.data  # dict[symbol -> list[Bar]]
        except Exception as e:
            logger.warning("Chunk %d bars fetch failed: %s", chunk_idx, e)
            gc.collect()
            time.sleep(1)
            continue

        for sym, bars in bars_data.items():
            try:
                if not bars or len(bars) < 5:
                    continue
                closes = [b.close for b in bars]
                volumes = [b.volume for b in bars]
                avg_price = sum(closes) / len(closes)
                avg_volume = sum(volumes) / len(volumes)

                if avg_volume < _MIN_AVG_VOLUME:
                    continue
                if not (_MIN_AVG_PRICE <= avg_price <= _MAX_AVG_PRICE):
                    continue

                meta = symbol_meta.get(sym, {})
                qualifying.append({
                    "symbol": sym,
                    "name": meta.get("name", ""),
                    "exchange": meta.get("exchange", ""),
                    "avg_volume": round(avg_volume, 0),
                    "avg_price": round(avg_price, 4),
                    "last_updated": now_str,
                })
            except Exception:
                continue

        gc.collect()
        time.sleep(1)

        if chunk_idx % 5 == 0:
            console.log(
                f"[dim][DeepScan] Chunk {chunk_idx + 1} done — "
                f"{len(qualifying)} qualifying so far…"
            )

    # ----------------------------------------------------------------
    # Step 3 — upsert into scan_universe
    # ----------------------------------------------------------------
    if qualifying:
        with _conn() as c:
            c.executemany(
                """
                INSERT OR REPLACE INTO scan_universe
                    (symbol, name, exchange, sector, avg_volume, avg_price, last_updated)
                VALUES
                    (:symbol, :name, :exchange, NULL, :avg_volume, :avg_price, :last_updated)
                """,
                qualifying,
            )
            c.commit()

    universe_size = len(qualifying)
    duration = round(time.time() - t0, 2)
    console.log(
        f"[green][DeepScan] Universe built: {universe_size} qualifying symbols "
        f"from {total_assets} candidates in {duration}s"
    )

    return {
        "total_assets": total_assets,
        "filtered_count": universe_size,
        "universe_size": universe_size,
        "duration_seconds": duration,
    }


def get_universe() -> list[str]:
    """Return list of symbols from scan_universe table.

    Falls back to the hardcoded S&P 500 + extras list (~528 symbols) if the
    table is empty or unavailable.
    """
    try:
        from config import DELISTED_BLACKLIST
    except Exception:
        DELISTED_BLACKLIST = set()

    try:
        with _conn() as c:
            rows = c.execute(
                "SELECT symbol FROM scan_universe ORDER BY avg_volume DESC"
            ).fetchall()
        symbols = [r["symbol"] for r in rows if r["symbol"] not in DELISTED_BLACKLIST]
        if symbols:
            return symbols
    except Exception as e:
        logger.warning("get_universe DB read failed: %s", e)

    # Fallback: try to extend with universe_scanner S&P 500 list
    try:
        from engine.universe_scanner import _get_sp500_tickers, EXTRA_TICKERS  # type: ignore
        sp500 = _get_sp500_tickers()
        combined = list(dict.fromkeys(sp500 + EXTRA_TICKERS))  # preserve order, dedupe
        logger.info("get_universe fallback: %d symbols from S&P 500 + extras", len(combined))
        return combined
    except Exception:
        pass

    # Last-resort static fallback
    logger.info("get_universe last-resort fallback: %d hardcoded symbols", len(_SP500_FALLBACK))
    return list(_SP500_FALLBACK)


def run_deep_scan(max_symbols: int = 2000, force: bool = False) -> dict:
    """Execute all strategies across the expanded universe and store results.

    Processes symbols in batches of 200, calling the rate limiter between
    batches.  Results are saved to deep_scan_results.

    Args:
        max_symbols: cap on how many symbols to scan (default 2000).
        force:       if True, rebuild universe first even if fresh.

    Returns:
        dict with: scan_date, symbols_scanned, signals_found,
                   duration_seconds, top_5_symbols, error (on failure)
    """
    t0 = time.time()
    scan_date = datetime.now().strftime("%Y-%m-%d")
    scan_time = datetime.now().strftime("%H:%M:%S")

    try:
        from engine.strategies import scan_strategies  # type: ignore
        from engine.rate_limiter import limiter
    except ImportError as e:
        msg = f"strategies import failed: {e}"
        logger.error(msg)
        return {
            "scan_date": scan_date,
            "symbols_scanned": 0,
            "signals_found": 0,
            "duration_seconds": 0,
            "top_5_symbols": [],
            "message": msg,
        }

    universe = get_universe()
    if not universe:
        return {
            "scan_date": scan_date,
            "symbols_scanned": 0,
            "signals_found": 0,
            "duration_seconds": round(time.time() - t0, 2),
            "top_5_symbols": [],
            "message": "Empty universe",
        }

    universe = universe[:max_symbols]
    console.log(
        f"[cyan][DeepScan] Starting deep scan across {len(universe)} symbols…"
    )

    all_signals: list[dict] = []
    symbols_scanned = 0

    for batch_idx, batch in enumerate(_chunks(universe, _CHUNK_SIZE)):
        try:
            results = scan_strategies(tickers=list(batch), save=False)
            if results:
                all_signals.extend(results)
            symbols_scanned += len(batch)
        except Exception as e:
            logger.warning("Batch %d scan_strategies error: %s", batch_idx, e)

        limiter.acquire()
        gc.collect()

        if batch_idx % 3 == 0:
            console.log(
                f"[dim][DeepScan] Batch {batch_idx + 1} done — "
                f"{symbols_scanned} scanned, {len(all_signals)} signals so far…"
            )

    # ----------------------------------------------------------------
    # Save results to deep_scan_results
    # ----------------------------------------------------------------
    if all_signals:
        rows_to_insert: list[dict] = []
        for sig in all_signals:
            try:
                # scan_strategies returns convergence dicts with "ticker" key
                symbol = sig.get("ticker") or sig.get("symbol", "")
                if not symbol:
                    continue

                strategy_names = sig.get("strategy_names", [])
                strategies_triggered = float(sig.get("strategies_triggered", 0))
                confidence = float(sig.get("confidence", 0.0))
                entry = sig.get("entry") or sig.get("entry_price")
                stop = sig.get("stop") or sig.get("stop_price")
                target = sig.get("target") or sig.get("target_price")
                rr = float(sig.get("risk_reward", 0.0))

                # One row per strategy name; if list is empty, store aggregate row
                names_to_store = strategy_names if strategy_names else ["convergence"]
                for strat_name in names_to_store:
                    rows_to_insert.append({
                        "scan_date": scan_date,
                        "scan_time": scan_time,
                        "symbol": symbol,
                        "strategy_name": strat_name,
                        "signal_strength": strategies_triggered,
                        "confidence": confidence,
                        "entry_price": float(entry) if entry is not None else None,
                        "stop_price": float(stop) if stop is not None else None,
                        "target_price": float(target) if target is not None else None,
                        "risk_reward": rr,
                        "volume": None,
                        "avg_volume": None,
                        "rel_volume": None,
                        "sector": None,
                    })
            except Exception as e:
                logger.debug("Signal row prep error: %s", e)
                continue

        if rows_to_insert:
            try:
                with _conn() as c:
                    c.executemany(
                        """
                        INSERT INTO deep_scan_results
                            (scan_date, scan_time, symbol, strategy_name,
                             signal_strength, confidence, entry_price, stop_price,
                             target_price, risk_reward, volume, avg_volume,
                             rel_volume, sector)
                        VALUES
                            (:scan_date, :scan_time, :symbol, :strategy_name,
                             :signal_strength, :confidence, :entry_price, :stop_price,
                             :target_price, :risk_reward, :volume, :avg_volume,
                             :rel_volume, :sector)
                        """,
                        rows_to_insert,
                    )
                    c.commit()
                logger.info("Saved %d signal rows to deep_scan_results", len(rows_to_insert))
            except Exception as e:
                logger.error("DB insert deep_scan_results failed: %s", e)

    # Top 5 by confidence
    top_5 = sorted(all_signals, key=lambda x: float(x.get("confidence", 0)), reverse=True)[:5]
    top_5_symbols = [s.get("ticker") or s.get("symbol", "") for s in top_5]

    duration = round(time.time() - t0, 2)
    console.log(
        f"[green][DeepScan] Scan complete: {symbols_scanned} symbols, "
        f"{len(all_signals)} signals in {duration}s"
    )

    return {
        "scan_date": scan_date,
        "symbols_scanned": symbols_scanned,
        "signals_found": len(all_signals),
        "duration_seconds": duration,
        "top_5_symbols": top_5_symbols,
    }


def get_deep_scan_results(limit: int = 50, min_strength: float = 0.0) -> dict:
    """Read the latest deep_scan_results from DB.

    Reads today's results first; falls back to the most recent scan date if
    today has no data.

    Args:
        limit:        max rows to return (default 50).
        min_strength: filter by signal_strength >= this value.

    Returns:
        dict with: scan_date, scan_time, results (list), total_count,
                   universe_size, error (on failure)
    """
    try:
        with _conn() as c:
            today = datetime.now().strftime("%Y-%m-%d")

            # Try today first, fall back to most recent date
            row = c.execute(
                "SELECT scan_date FROM deep_scan_results WHERE scan_date = ? LIMIT 1",
                (today,),
            ).fetchone()

            if row:
                use_date = today
            else:
                row = c.execute(
                    "SELECT scan_date FROM deep_scan_results ORDER BY scan_date DESC LIMIT 1"
                ).fetchone()
                use_date = row["scan_date"] if row else today

            # Fetch latest scan_time for that date
            time_row = c.execute(
                "SELECT scan_time FROM deep_scan_results WHERE scan_date = ? "
                "ORDER BY scan_time DESC LIMIT 1",
                (use_date,),
            ).fetchone()
            use_time = time_row["scan_time"] if time_row else "—"

            results = c.execute(
                """
                SELECT symbol, strategy_name, signal_strength, confidence,
                       entry_price, stop_price, target_price, risk_reward,
                       volume, avg_volume, rel_volume, sector
                FROM deep_scan_results
                WHERE scan_date = ? AND signal_strength >= ?
                ORDER BY confidence DESC, signal_strength DESC
                LIMIT ?
                """,
                (use_date, min_strength, limit),
            ).fetchall()

            total_count = c.execute(
                "SELECT COUNT(*) as n FROM deep_scan_results WHERE scan_date = ?",
                (use_date,),
            ).fetchone()["n"]

            universe_size = c.execute(
                "SELECT COUNT(*) as n FROM scan_universe"
            ).fetchone()["n"]

        return {
            "scan_date": use_date,
            "scan_time": use_time,
            "results": [dict(r) for r in results],
            "total_count": total_count,
            "universe_size": universe_size,
        }

    except Exception as e:
        logger.error("get_deep_scan_results failed: %s", e)
        return {
            "scan_date": "",
            "scan_time": "",
            "results": [],
            "total_count": 0,
            "universe_size": 0,
            "error": str(e),
        }


def get_universe_stats() -> dict:
    """Return summary statistics about the scan_universe table.

    Returns:
        dict with: total_symbols, last_updated, exchange_breakdown,
                   error (on failure)
    """
    try:
        with _conn() as c:
            total_row = c.execute(
                "SELECT COUNT(*) as n FROM scan_universe"
            ).fetchone()
            total = total_row["n"] if total_row else 0

            last_row = c.execute(
                "SELECT last_updated FROM scan_universe ORDER BY last_updated DESC LIMIT 1"
            ).fetchone()
            last_updated = last_row["last_updated"] if last_row else None

            exchange_rows = c.execute(
                "SELECT exchange, COUNT(*) as cnt FROM scan_universe "
                "GROUP BY exchange ORDER BY cnt DESC"
            ).fetchall()
            exchange_breakdown = {r["exchange"]: r["cnt"] for r in exchange_rows}

        return {
            "total_symbols": total,
            "last_updated": last_updated,
            "exchange_breakdown": exchange_breakdown,
        }

    except Exception as e:
        logger.error("get_universe_stats failed: %s", e)
        return {
            "total_symbols": 0,
            "last_updated": None,
            "exchange_breakdown": {},
            "error": str(e),
        }


# ---------------------------------------------------------------------------
# Module init — create tables on import
# ---------------------------------------------------------------------------

try:
    _init_db()
except Exception as _init_err:
    logger.warning("deep_scan _init_db failed: %s", _init_err)
