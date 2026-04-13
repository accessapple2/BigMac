"""AI SaaS Disruption Scanner — monitors the SaaS/cloud sector for disruption signals.

Watchlist: IGV (sector ETF) + 13 individual names across security, infra, and platform SaaS.

Triggers:
  1. IGV volume > 1.5x 20-day avg          → SECTOR_ACTIVE   (sector disruption is on)
  2. Any ticker drops >3% intraday on vol >1.5x avg → SHORT signal, theme=saas_disruption
  3. Any ticker RSI<35 AND price<20-day SMA  → LONG  signal, theme=saas_disruption
  4. PANW or CRWD up >1% while IGV down >1% → LONG  signal, theme=saas_resilience

Posts all signals to Signal Center (port 9000) via post_to_9000.
Cached 5 min per run. Safe to call from scheduler; never touches trader.db or arena.db.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime

from rich.console import Console

console = Console()
logger = logging.getLogger("ai_saas_disruption")

# ---------------------------------------------------------------------------
# Watchlist
# ---------------------------------------------------------------------------

WATCHLIST: list[str] = [
    "IGV",   # iShares Expanded Tech-Software ETF — sector proxy
    "MSFT", "CRM", "PLTR", "ORCL", "SNOW",
    "OKTA", "DDOG", "ADBE", "NOW", "PANW",
    "APP", "CRWD", "INTU",
]

SECTOR_ETF = "IGV"
RESILIENCE_TICKERS = {"PANW", "CRWD"}

# Thresholds
VOL_RATIO_TRIGGER   = 1.5   # 1.5x 20-day avg
DROP_PCT_TRIGGER    = 3.0   # >3% intraday drop
RSI_OVERSOLD        = 35.0  # RSI < 35 → oversold
RESILIENCE_UP_PCT   = 1.0   # PANW/CRWD up >1%
RESILIENCE_DOWN_PCT = 1.0   # IGV down >1% (absolute)

# Cache
_scan_lock  = threading.Lock()
_last_result: dict | None  = None
_last_scan_ts: float = 0.0
_CACHE_TTL: int = 300  # 5 min


# ---------------------------------------------------------------------------
# Market data helpers
# ---------------------------------------------------------------------------

def _fetch_ticker_data(symbol: str) -> dict | None:
    """Fetch OHLCV + RSI-14 + 20-day SMA + 20-day avg volume via yfinance.

    Returns:
        {
            symbol, price, open_price, day_change_pct,
            today_volume, avg_volume_20d, vol_ratio,
            rsi, sma_20, price_vs_sma_pct
        }
    or None on error / insufficient history.
    """
    try:
        import yfinance as yf

        hist = yf.download(
            symbol, period="60d", interval="1d",
            progress=False, auto_adjust=True,
        )
        if hist is None or len(hist) < 22:
            return None

        close  = hist["Close"].dropna()
        volume = hist["Volume"].dropna()
        open_  = hist["Open"].dropna()

        if len(close) < 22:
            return None

        # Current values
        price      = float(close.iloc[-1])
        open_price = float(open_.iloc[-1])
        today_vol  = float(volume.iloc[-1])

        # 20-day avg volume (exclude today)
        avg_vol_20d = float(volume.iloc[-21:-1].mean()) if len(volume) >= 21 else float(volume.iloc[:-1].mean())
        vol_ratio   = round(today_vol / avg_vol_20d, 2) if avg_vol_20d > 0 else 0.0

        # Intraday % change
        day_change_pct = round((price - open_price) / open_price * 100, 2) if open_price else 0.0

        # 20-day SMA (exclude today)
        sma_20 = round(float(close.iloc[-21:-1].mean()), 4) if len(close) >= 21 else round(float(close.mean()), 4)
        price_vs_sma_pct = round((price - sma_20) / sma_20 * 100, 2) if sma_20 else 0.0

        # RSI-14
        delta    = close.diff().dropna()
        gain     = delta.clip(lower=0)
        loss     = (-delta).clip(lower=0)
        avg_gain = gain.rolling(14).mean().iloc[-1]
        avg_loss = loss.rolling(14).mean().iloc[-1]
        if avg_loss == 0:
            rsi = 100.0
        else:
            rs  = avg_gain / avg_loss
            rsi = round(100 - (100 / (1 + rs)), 1)

        return {
            "symbol":          symbol,
            "price":           round(price, 4),
            "open_price":      round(open_price, 4),
            "day_change_pct":  day_change_pct,
            "today_volume":    int(today_vol),
            "avg_volume_20d":  round(avg_vol_20d, 0),
            "vol_ratio":       vol_ratio,
            "rsi":             rsi,
            "sma_20":          sma_20,
            "price_vs_sma_pct": price_vs_sma_pct,
        }
    except Exception as e:
        logger.debug("ai_saas_disruption: yfinance error for %s: %s", symbol, e)
        return None


# ---------------------------------------------------------------------------
# Signal evaluation
# ---------------------------------------------------------------------------

def _evaluate_signals(data: dict[str, dict]) -> list[dict]:
    """Apply the four trigger rules and return a list of signal dicts."""
    signals: list[dict] = []
    ts = datetime.now().isoformat()

    igv = data.get(SECTOR_ETF)

    # ── Trigger 1: IGV volume > 1.5x 20-day avg ─────────────────────────────
    if igv and igv["vol_ratio"] >= VOL_RATIO_TRIGGER:
        signals.append({
            "trigger":    "igv_volume_spike",
            "type":       "SECTOR_ACTIVE",
            "direction":  "neutral",
            "theme":      "saas_disruption",
            "symbol":     SECTOR_ETF,
            "price":      igv["price"],
            "vol_ratio":  igv["vol_ratio"],
            "day_change": igv["day_change_pct"],
            "note":       f"IGV volume {igv['vol_ratio']}x 20d avg — SaaS sector disruption active",
            "scanned_at": ts,
        })
        console.log(
            f"[bold yellow]AI SaaS: IGV volume spike {igv['vol_ratio']}x "
            f"(price {igv['price']}, {igv['day_change_pct']:+.1f}%)"
        )

    # ── Trigger 2: any ticker drops >3% intraday on vol >1.5x avg ───────────
    for sym, d in data.items():
        if sym == SECTOR_ETF:
            continue
        if d["day_change_pct"] < -DROP_PCT_TRIGGER and d["vol_ratio"] >= VOL_RATIO_TRIGGER:
            signals.append({
                "trigger":    "intraday_drop_high_vol",
                "type":       "SIGNAL",
                "direction":  "short",
                "theme":      "saas_disruption",
                "symbol":     sym,
                "price":      d["price"],
                "day_change": d["day_change_pct"],
                "vol_ratio":  d["vol_ratio"],
                "rsi":        d["rsi"],
                "note":       (
                    f"{sym} down {d['day_change_pct']:.1f}% on {d['vol_ratio']}x vol — "
                    f"SaaS disruption pressure"
                ),
                "scanned_at": ts,
            })
            console.log(
                f"[red]AI SaaS SHORT: {sym} {d['day_change_pct']:+.1f}% "
                f"vol={d['vol_ratio']}x RSI={d['rsi']}"
            )

    # ── Trigger 3: RSI<35 AND price < 20-day SMA ────────────────────────────
    for sym, d in data.items():
        if sym == SECTOR_ETF:
            continue
        if d["rsi"] < RSI_OVERSOLD and d["price_vs_sma_pct"] < 0:
            signals.append({
                "trigger":    "rsi_oversold_below_sma",
                "type":       "SIGNAL",
                "direction":  "long",
                "theme":      "saas_disruption",
                "symbol":     sym,
                "price":      d["price"],
                "sma_20":     d["sma_20"],
                "price_vs_sma_pct": d["price_vs_sma_pct"],
                "rsi":        d["rsi"],
                "note":       (
                    f"{sym} RSI {d['rsi']} + {d['price_vs_sma_pct']:.1f}% below 20SMA — "
                    f"oversold bounce setup"
                ),
                "scanned_at": ts,
            })
            console.log(
                f"[green]AI SaaS LONG: {sym} RSI={d['rsi']} "
                f"vs SMA20={d['price_vs_sma_pct']:+.1f}%"
            )

    # ── Trigger 4: PANW or CRWD up >1% while IGV down >1% ───────────────────
    if igv and igv["day_change_pct"] < -RESILIENCE_DOWN_PCT:
        for sym in RESILIENCE_TICKERS:
            d = data.get(sym)
            if d and d["day_change_pct"] > RESILIENCE_UP_PCT:
                signals.append({
                    "trigger":    "cybersecurity_resilience",
                    "type":       "SIGNAL",
                    "direction":  "long",
                    "theme":      "saas_resilience",
                    "symbol":     sym,
                    "price":      d["price"],
                    "day_change": d["day_change_pct"],
                    "igv_change": igv["day_change_pct"],
                    "note":       (
                        f"{sym} +{d['day_change_pct']:.1f}% while IGV "
                        f"{igv['day_change_pct']:.1f}% — cyber resilience divergence"
                    ),
                    "scanned_at": ts,
                })
                console.log(
                    f"[bold green]AI SaaS RESILIENCE: {sym} "
                    f"+{d['day_change_pct']:.1f}% vs IGV {igv['day_change_pct']:.1f}%"
                )

    return signals


# ---------------------------------------------------------------------------
# Signal Center posting
# ---------------------------------------------------------------------------

def _post_signals(signals: list[dict]) -> None:
    """Fire-and-forget each signal to the Signal Center (port 9000)."""
    if not signals:
        return
    try:
        from engine.signal_poster import post_to_9000
        for sig in signals:
            post_to_9000("AI_SAAS_DISRUPTION", sig)
    except Exception as e:
        logger.warning("ai_saas_disruption: post error: %s", e)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_scan(force: bool = False) -> dict:
    """Scan the AI SaaS watchlist. Cached 5 min unless force=True.

    Returns:
        {
            signals: list[dict],      # triggered signals this run
            watchlist_data: dict,     # per-ticker data snapshot
            scanned_at: str,          # ISO timestamp
            tickers_scanned: int,
            tickers_ok: int,
        }
    """
    global _last_result, _last_scan_ts

    if not force and _last_result is not None:
        if time.time() - _last_scan_ts < _CACHE_TTL:
            return _last_result

    with _scan_lock:
        # Double-check inside lock
        if not force and _last_result is not None:
            if time.time() - _last_scan_ts < _CACHE_TTL:
                return _last_result

        console.log(f"[cyan]AI SaaS Disruption Scanner: fetching {len(WATCHLIST)} tickers…")

        data: dict[str, dict] = {}
        for sym in WATCHLIST:
            d = _fetch_ticker_data(sym)
            if d:
                data[sym] = d

        console.log(
            f"[cyan]AI SaaS: {len(data)}/{len(WATCHLIST)} tickers fetched — "
            f"evaluating triggers…"
        )

        signals = _evaluate_signals(data)
        _post_signals(signals)

        result = {
            "signals":        signals,
            "watchlist_data": data,
            "scanned_at":     datetime.now().isoformat(),
            "tickers_scanned": len(WATCHLIST),
            "tickers_ok":      len(data),
        }

        _last_result  = result
        _last_scan_ts = time.time()

        if signals:
            console.log(
                f"[bold cyan]AI SaaS Disruption: {len(signals)} signal(s) → "
                + ", ".join(
                    f"{s['symbol']}({s['direction']}:{s['theme']})"
                    for s in signals
                )
            )
        else:
            console.log("[dim]AI SaaS Disruption: no signals this cycle")

        return result
