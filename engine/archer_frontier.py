"""Admiral Archer — Pioneer Corps, Frontier Scanner.

Scans small caps, recent IPOs, Russell 2000 movers, and stocks outside S&P 500.
Runs weekly on Sunday at 10:30 PM MST alongside Picard's strategy briefing.
Uses Yahoo Finance (free) to scan ~200 small/mid cap tickers.
"""
from __future__ import annotations
import time
import threading
import requests
from datetime import datetime
from rich.console import Console

console = Console()

_cache = {"picks": [], "report": None, "ts": 0}
_lock = threading.Lock()
_TTL = 604800  # 1 week

# Frontier universe — outside S&P 500 mainstream
FRONTIER_TICKERS = [
    # Russell 2000 / Small Cap leaders
    "SMCI", "IONQ", "RKLB", "JOBY", "LUNR", "RDW", "ASTS", "DNA",
    "AFRM", "HOOD", "SOFI", "UPST", "OPEN", "LMND", "ROOT",
    "RIVN", "LCID", "FFIE", "GOEV", "PSNY",
    "HIMS", "CLOV", "WISH", "BBAI", "SOUN", "GSAT",
    "RXRX", "CRSP", "NTLA", "BEAM", "EDIT",
    "MARA", "RIOT", "COIN", "MSTR", "CLSK", "HUT",
    "GRAB", "SE", "NU", "BABA", "JD", "PDD", "MELI",
    "ACHR", "EVTL", "BLDE", "UAM",
    "AI", "PATH", "BRZE", "CFLT", "MDB", "SNOW", "NET",
    "CELH", "MNST", "OLPX", "ELF",
    "IREN", "BTDR", "CORZ", "WULF",
    # Recent IPO / SPAC graduates
    "ARM", "CART", "BIRK", "VRT", "DUOL", "IBKR",
    "RDDT", "ASML",
]


def scan_frontier() -> list:
    """Scan frontier tickers for unusual momentum/volume and contrarian setups."""
    import yfinance as yf

    picks = []
    for ticker in FRONTIER_TICKERS:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="5d")
            if hist.empty or len(hist) < 2:
                continue

            current = float(hist["Close"].iloc[-1])
            prev = float(hist["Close"].iloc[-2])
            volume = float(hist["Volume"].iloc[-1])
            avg_vol = float(hist["Volume"].mean())

            change_pct = ((current / prev) - 1) * 100
            vol_ratio = volume / avg_vol if avg_vol > 0 else 1.0

            # Score: momentum + volume surge
            score = 0
            signals = []
            if abs(change_pct) > 3:
                score += 20
                signals.append(f"{'▲' if change_pct > 0 else '▼'} {change_pct:+.1f}% move")
            if vol_ratio > 2.0:
                score += 20
                signals.append(f"Volume {vol_ratio:.1f}x avg")
            if change_pct > 5:
                score += 15
                signals.append("Strong momentum")
            if vol_ratio > 3.0:
                score += 15
                signals.append("Volume spike")

            # Get 30-day trend
            hist30 = stock.history(period="1mo")
            if len(hist30) >= 10:
                month_ret = ((float(hist30["Close"].iloc[-1]) / float(hist30["Close"].iloc[0])) - 1) * 100
                if month_ret > 10:
                    score += 15
                    signals.append(f"30d: {month_ret:+.1f}%")
                elif month_ret < -12 and change_pct > 1:
                    score += 18
                    signals.append(f"Contrarian rebound after 30d {month_ret:+.1f}% washout")

                lows = hist30["Low"].tail(10)
                closes = hist30["Close"].tail(10)
                if not lows.empty and not closes.empty:
                    ten_day_low = float(lows.min())
                    if ten_day_low > 0 and current <= ten_day_low * 1.05 and change_pct > 0:
                        score += 10
                        signals.append("Near 10d low with bounce attempt")

            # Simple contrarian RSI proxy from the recent 5d path
            if len(hist) >= 5:
                closes5 = hist["Close"].tolist()
                losses = []
                gains = []
                for idx in range(1, len(closes5)):
                    diff = float(closes5[idx]) - float(closes5[idx - 1])
                    if diff >= 0:
                        gains.append(diff)
                    else:
                        losses.append(abs(diff))
                avg_gain = sum(gains) / len(gains) if gains else 0
                avg_loss = sum(losses) / len(losses) if losses else 0
                if avg_loss > 0:
                    rs = avg_gain / avg_loss if avg_loss else 0
                    rsi = 100 - (100 / (1 + rs))
                    if rsi < 40:
                        score += 8
                        signals.append(f"Contrarian RSI {rsi:.0f}")

            if score >= 20:
                picks.append({
                    "ticker": ticker,
                    "price": round(current, 2),
                    "change_pct": round(change_pct, 2),
                    "volume_ratio": round(vol_ratio, 1),
                    "score": score,
                    "signals": signals,
                })

        except Exception:
            continue

    picks.sort(key=lambda x: x["score"], reverse=True)
    return picks[:15]


def generate_archer_report() -> str | None:
    """Generate Archer's frontier report using Ollama."""
    from config import OLLAMA_URL, OLLAMA_MODEL

    picks = scan_frontier()
    if not picks:
        return None

    picks_text = "\n".join(
        f"  {p['ticker']}: ${p['price']} ({p['change_pct']:+.1f}%), "
        f"Vol {p['volume_ratio']}x, Score {p['score']} — {', '.join(p['signals'])}"
        for p in picks
    )

    prompt = f"""You are Admiral Jonathan Archer, Pioneer Corps commander aboard USS TradeMinds.
You speak with enthusiasm, pioneering spirit, and folksy charm. You love exploration and finding opportunities nobody else sees.
You explore stocks OUTSIDE the mainstream S&P 500 — small caps, IPOs, emerging sectors.

Voice examples:
- "I've been scanning the frontier and found something interesting out past the S&P 500 border."
- "We're going where no one has gone before... and I don't need a Vulcan to tell me that's exciting."
- "Porthos would be wagging his tail at this setup." (Porthos is your beagle)
- "Back when I captained the NX-01, we took risks. This is one worth taking."

Here are this week's frontier scanner results:
{picks_text}

Write a short (200-300 word) frontier report highlighting the top 3-5 most interesting opportunities.
Format as:

🚀 ADMIRAL ARCHER — FRONTIER REPORT
[Date]

FRONTIER PICKS:
[For each pick: ticker, why it's interesting, risk level]

ARCHER'S TAKE:
[Your overall assessment of the frontier this week — is it worth exploring or should the crew stick to blue chips?]"""

    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
            timeout=180,
        )
        resp.raise_for_status()
        report = resp.json().get("response", "").strip()
        if not report:
            return None

        with _lock:
            _cache["picks"] = picks
            _cache["report"] = report
            _cache["ts"] = time.time()

        # Post to War Room
        try:
            from engine.war_room import save_hot_take
            preview = report[:400] + ("..." if len(report) > 400 else "")
            save_hot_take("archer", "FRONTIER", f"🚀 ADMIRAL ARCHER: {preview}")
        except Exception:
            pass

        # Post top picks to Signal Center (trade_signals table)
        try:
            from engine.signal_poster import post_signal_to_9000
            for p in picks[:5]:
                post_signal_to_9000({
                    "symbol": p["ticker"],
                    "action": "BUY",
                    "type": "FRONTIER",
                    "timeframe": "SWING",
                    "price": p["price"],
                    "confidence": min(p["score"], 100),
                    "agent": "archer",
                    "model": "frontier-scanner",
                    "reasoning": f"Frontier scan: {', '.join(p['signals'])}. "
                                 f"Change {p['change_pct']:+.1f}%, vol {p['volume_ratio']}x avg.",
                })
        except Exception:
            pass

        console.log(f"[bold green]Admiral Archer: Frontier report generated ({len(picks)} picks)")
        return report

    except Exception as e:
        console.log(f"[red]Admiral Archer frontier error: {e}")
        return None


def get_latest_report() -> dict:
    """Get Archer's latest frontier report."""
    with _lock:
        return {
            "picks": _cache.get("picks", []),
            "report": _cache.get("report"),
            "generated_at": (
                datetime.fromtimestamp(_cache["ts"]).isoformat()
                if _cache.get("ts") else None
            ),
        }
