"""Short Squeeze Scanner — finds high short-interest, low-float tickers.

Squeeze Score (1–10) based on:
  - Short Interest % of Float  (>20% required, higher = more points)
  - Float size                 (<20M required, smaller = more points)
  - Volume Ratio today vs avg  (>2x required, higher = more points)
  - Price vs 10-day high       (breaking out = more points)
  - RSI                        (<70 required — not already overbought)

Auto-posts War Room alert from Chekov (mlx-qwen3) when score > 8.
"""
from __future__ import annotations
import time
import threading
from datetime import datetime
from rich.console import Console

console = Console()

_scan_lock = threading.Lock()
_last_result: dict | None = None
_last_scan_ts: float = 0.0
_CACHE_TTL: int = 300  # 5 min cache


def _fetch_finviz_candidates() -> list[dict]:
    """Use Finviz Ownership screener to pull stocks with high short interest + small float."""
    try:
        from finvizfinance.screener.ownership import Ownership
        screener = Ownership()
        # Filter: Float Short > 20% (high squeeze potential)
        screener.set_filter(filters_dict={"Float Short": "Over 20%"})
        df = screener.screener_view()
        if df is None or df.empty:
            return []
        return df.to_dict("records")
    except Exception as e:
        console.log(f"[yellow]Squeeze: Finviz fetch error: {e}")
        return []


def _get_yfinance_data(ticker: str) -> dict | None:
    """Fetch RSI, volume ratio, 10d high via yfinance."""
    try:
        import yfinance as yf
        hist = yf.download(ticker, period="30d", interval="1d",
                           progress=False, auto_adjust=True)
        if hist is None or len(hist) < 11:
            return None
        close = hist["Close"].dropna()
        volume = hist["Volume"].dropna()
        if len(close) < 11 or len(volume) < 11:
            return None

        current_price = float(close.iloc[-1])
        today_vol = float(volume.iloc[-1])
        avg_vol = float(volume.iloc[-21:-1].mean()) if len(volume) >= 21 else float(volume.iloc[:-1].mean())
        vol_ratio = round(today_vol / avg_vol, 2) if avg_vol > 0 else 1.0
        high_10d = float(close.iloc[-11:-1].max())
        above_10d_high = current_price > high_10d

        # RSI-14
        delta = close.diff().dropna()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_gain = gain.rolling(14).mean().iloc[-1]
        avg_loss = loss.rolling(14).mean().iloc[-1]
        if avg_loss == 0:
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = round(100 - (100 / (1 + rs)), 1)

        return {
            "price": round(current_price, 2),
            "vol_ratio": vol_ratio,
            "above_10d_high": above_10d_high,
            "high_10d": round(high_10d, 2),
            "rsi": rsi,
        }
    except Exception as e:
        console.log(f"[dim]Squeeze: yfinance error for {ticker}: {e}")
        return None


def _parse_float_val(val) -> float:
    """Parse finviz values:
    - '21.68%' → 21.68 (percentage, strip %)
    - 232440000.0 → 232.44 (raw share count, convert to millions)
    - '5.23M' → 5.23 (with suffix, keep as millions)
    """
    if val is None:
        return 0.0
    s = str(val).strip().replace(",", "")
    # Handle percentage strings like '21.68%'
    if s.endswith("%"):
        try:
            return float(s[:-1])
        except ValueError:
            return 0.0
    # Handle suffixes
    multiplier = 1.0
    if s.endswith("B"):
        multiplier = 1_000.0
        s = s[:-1]
    elif s.endswith("M"):
        multiplier = 1.0
        s = s[:-1]
    elif s.endswith("K"):
        multiplier = 0.001
        s = s[:-1]
    try:
        raw = float(s) * multiplier
        # If raw value is very large (raw share count > 1M), convert to millions
        if raw > 1_000_000:
            return raw / 1_000_000
        return raw
    except ValueError:
        return 0.0


def _score_candidate(row: dict, yf_data: dict) -> int:
    """Compute squeeze score 1–10."""
    score = 0

    # Short interest component (0–3 pts) — 'Short Float' col has value like '21.68%'
    short_pct = _parse_float_val(row.get("Short Float", 0))
    if short_pct >= 40:
        score += 3
    elif short_pct >= 30:
        score += 2
    elif short_pct >= 20:
        score += 1

    # Float component (0–2 pts) — 'Float' col has raw share count (e.g. 232440000.0)
    float_m = _parse_float_val(row.get("Float", 0))
    if float_m <= 5:
        score += 2
    elif float_m <= 20:
        score += 1

    # Volume ratio (0–2 pts)
    vol_ratio = yf_data.get("vol_ratio", 1.0)
    if vol_ratio >= 5:
        score += 2
    elif vol_ratio >= 2:
        score += 1

    # Price above 10-day high (0–2 pts)
    if yf_data.get("above_10d_high"):
        score += 2

    # RSI not overbought (1 pt bonus if RSI < 60 — room to run)
    rsi = yf_data.get("rsi", 50)
    if rsi < 60:
        score += 1

    return max(1, min(10, score))


def run_scan(force: bool = False) -> dict:
    """Run the squeeze scan, return results. Cached 5 min unless force=True."""
    global _last_result, _last_scan_ts

    if not force and _last_result is not None:
        if time.time() - _last_scan_ts < _CACHE_TTL:
            return _last_result

    with _scan_lock:
        # Double-check inside lock
        if not force and _last_result is not None:
            if time.time() - _last_scan_ts < _CACHE_TTL:
                return _last_result

        console.log("[cyan]Squeeze Scanner: fetching Finviz candidates...")
        candidates = _fetch_finviz_candidates()
        console.log(f"[cyan]Squeeze Scanner: {len(candidates)} candidates from Finviz")

        results = []
        for row in candidates[:40]:  # cap at 40 to avoid rate limits
            ticker = str(row.get("Ticker", "")).strip()
            if not ticker:
                continue

            short_pct = _parse_float_val(row.get("Short Float", 0))
            float_m = _parse_float_val(row.get("Float", 0))

            # Must meet base criteria
            if short_pct < 20:
                continue

            yf_data = _get_yfinance_data(ticker)
            if yf_data is None:
                continue

            vol_ratio = yf_data.get("vol_ratio", 1.0)
            rsi = yf_data.get("rsi", 50.0)

            if vol_ratio < 2.0 or rsi >= 70:
                continue

            score = _score_candidate(row, yf_data)

            change_pct = 0.0
            try:
                raw_change = row.get("Change", 0)
                cv = float(str(raw_change).replace("%", ""))
                # Finviz Ownership returns Change as decimal fraction (e.g. -0.0118 = -1.18%)
                change_pct = cv * 100 if abs(cv) < 1.0 else cv
            except Exception:
                pass

            results.append({
                "ticker": ticker,
                "short_interest_pct": round(short_pct, 1),
                "float_m": round(float_m, 2),
                "vol_ratio": vol_ratio,
                "price": yf_data["price"],
                "day_change_pct": round(change_pct, 2),
                "rsi": rsi,
                "above_10d_high": yf_data["above_10d_high"],
                "score": score,
            })

        # Sort by score desc
        results.sort(key=lambda x: x["score"], reverse=True)

        # Auto-post War Room for score > 8
        _post_war_room_alerts(results)

        _last_result = {
            "results": results,
            "scanned_at": datetime.now().isoformat(),
            "candidate_count": len(candidates),
        }
        _last_scan_ts = time.time()

        console.log(f"[green]Squeeze Scanner: {len(results)} squeeze candidates found")
        return _last_result


def _post_war_room_alerts(results: list[dict]) -> None:
    """Post War Room hot takes from Chekov for any score > 8 ticker."""
    try:
        from engine.war_room import save_hot_take
        for r in results:
            if r["score"] > 8:
                ticker = r["ticker"]
                short_pct = r["short_interest_pct"]
                float_m = r["float_m"]
                vol_ratio = r["vol_ratio"]
                take = (
                    f"SQUEEZE ALERT: {ticker} — short interest {short_pct}%, "
                    f"float {float_m}M, volume {vol_ratio}x. Shields up."
                )
                saved = save_hot_take("mlx-qwen3", ticker, take)
                if saved:
                    console.log(f"[bold magenta]Chekov → War Room: {ticker} squeeze alert posted")
    except Exception as e:
        console.log(f"[yellow]Squeeze War Room post error: {e}")
