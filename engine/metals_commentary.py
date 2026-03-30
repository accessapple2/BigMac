"""Quartermaster's Report — Cmdr. Dalio's daily metals commentary.

Uses Ollama (Gemma3 4B, FREE local) to generate daily metals analysis.
Covers spot prices, gold/silver ratio, portfolio P&L, stacking recommendations,
and geopolitical factors. Auto-posts summary to War Room.
"""
from __future__ import annotations
import sqlite3
import time
import threading
from datetime import datetime
from rich.console import Console

console = Console()
DB = "data/trader.db"

_cache = {"commentary": None, "summary": None, "ts": 0}
_lock = threading.Lock()
_TTL = 14400  # 4 hours


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def generate_commentary() -> dict:
    """Generate Dalio's daily metals commentary using Ollama."""
    with _lock:
        if _cache["commentary"] and time.time() - _cache["ts"] < _TTL:
            return _cache

    from engine.metals_tracker import get_spot_prices, get_portfolio, get_stacking_signal

    prices = get_spot_prices()
    portfolio = get_portfolio()
    stacking = get_stacking_signal()

    if not prices.get("GOLD"):
        return {"error": "Spot prices unavailable", "commentary": None, "summary": None}

    gold = prices.get("GOLD", {})
    silver = prices.get("SILVER", {})
    gsr = prices.get("GSR", 0)
    vix = stacking.get("vix", 20)

    # Build portfolio context
    pos_lines = []
    for p in portfolio.get("positions", []):
        pos_lines.append(
            f"{p['symbol']}: {p['qty']} oz @ ${p['avg_price']:.2f} avg, "
            f"now ${p['current_price']:.2f} ({p['unrealized_pnl_pct']:+.1f}%, ${p['unrealized_pnl']:+.2f})"
        )
    portfolio_ctx = "\n".join(pos_lines) if pos_lines else "No metal positions"
    total_pnl = portfolio.get("total_unrealized_pnl", 0)
    total_ret = portfolio.get("return_pct", 0)

    # Stacking signals
    gold_sig = stacking.get("signals", {}).get("GOLD", {})
    silver_sig = stacking.get("signals", {}).get("SILVER", {})

    prompt = f"""You are Lt. Commander Dalio, Quartermaster aboard USS TradeMinds. You are an expert in precious metals, physical gold/silver stacking, and macro economics. Your style is inspired by Ray Dalio (macro/cycles) and Robert Kiyosaki (physical metals, currency debasement).

Today's metals data:
- Gold: ${gold.get('price', 0):.2f} ({gold.get('change_pct', 0):+.2f}% today)
- Silver: ${silver.get('price', 0):.2f} ({silver.get('change_pct', 0):+.2f}% today)
- Gold/Silver Ratio: {gsr}
- VIX: {vix}

Captain's physical holdings:
{portfolio_ctx}
Total portfolio P&L: ${total_pnl:+.2f} ({total_ret:+.1f}%)

Stacking signals:
- Gold: {gold_sig.get('signal', 'HOLD')} (conviction {gold_sig.get('conviction', 5)}/10) — {', '.join(gold_sig.get('reasons', []))}
- Silver: {silver_sig.get('signal', 'HOLD')} (conviction {silver_sig.get('conviction', 5)}/10) — {', '.join(silver_sig.get('reasons', []))}

Write a Quartermaster's Report (200-300 words) covering:
1. What happened in metals today and WHY (macro drivers, geopolitics, dollar, Fed)
2. Gold/silver ratio analysis — is silver cheap vs gold?
3. Captain's position review — how the holdings are performing
4. Stacking recommendation — buy more, hold, or take profits?
5. One sentence about the long-term macro thesis (currency debasement, de-dollarization, etc.)

Write in Dalio's voice — measured, macro-focused, data-driven. Address the Captain directly.
Start with: "🪙 QUARTERMASTER'S REPORT — Stardate {datetime.now().strftime('%Y.%m.%d')}"
End with a clear BUY/HOLD/REDUCE recommendation and confidence level."""

    # Call Ollama (Gemma3 4B — free local model)
    try:
        from config import OLLAMA_URL
        import requests
        resp = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": "gemma3:4b", "prompt": prompt, "stream": False},
            timeout=120,
        )
        resp.raise_for_status()
        commentary = resp.json().get("response", "").strip()
    except Exception as e:
        console.log(f"[yellow]Dalio commentary Ollama error: {e}")
        # Fallback: generate a simple report without AI
        commentary = _fallback_commentary(gold, silver, gsr, vix, portfolio, gold_sig, silver_sig, total_pnl, total_ret)

    # Generate concise War Room summary
    gold_rec = gold_sig.get("signal", "HOLD")
    silver_rec = silver_sig.get("signal", "HOLD")
    rec = gold_rec if gold_rec != "HOLD" else silver_rec
    summary = (
        f"Gold at ${gold.get('price', 0):,.2f} ({gold.get('change_pct', 0):+.2f}%), "
        f"silver at ${silver.get('price', 0):,.2f} ({silver.get('change_pct', 0):+.2f}%). "
        f"GSR {gsr}. Portfolio ${total_pnl:+,.0f} ({total_ret:+.1f}%). "
        f"Recommendation: {rec}."
    )

    result = {
        "commentary": commentary,
        "summary": summary,
        "prices": prices,
        "portfolio_pnl": round(total_pnl, 2),
        "portfolio_return": round(total_ret, 2),
        "gold_signal": gold_sig.get("signal", "HOLD"),
        "silver_signal": silver_sig.get("signal", "HOLD"),
        "gsr": gsr,
        "generated_at": datetime.now().isoformat(),
    }

    with _lock:
        _cache["commentary"] = commentary
        _cache["summary"] = summary
        _cache["ts"] = time.time()
        _cache.update(result)

    return result


def _fallback_commentary(gold, silver, gsr, vix, portfolio, gold_sig, silver_sig, total_pnl, total_ret):
    """Simple report when Ollama is unavailable."""
    gp = gold.get("price", 0)
    gc = gold.get("change_pct", 0)
    sp = silver.get("price", 0)
    sc = silver.get("change_pct", 0)

    gsr_note = ""
    if gsr > 80:
        gsr_note = f"The GSR at {gsr} signals silver is historically undervalued vs gold — favor silver accumulation."
    elif gsr > 70:
        gsr_note = f"GSR at {gsr} is in neutral territory."
    else:
        gsr_note = f"GSR at {gsr} shows silver relatively expensive — favor gold stacking."

    return (
        f"🪙 QUARTERMASTER'S REPORT — Stardate {datetime.now().strftime('%Y.%m.%d')}\n\n"
        f"Gold closed at ${gp:,.2f} ({gc:+.2f}%), silver at ${sp:,.2f} ({sc:+.2f}%). "
        f"{gsr_note}\n\n"
        f"Captain's holdings are {'up' if total_pnl > 0 else 'down'} ${abs(total_pnl):,.2f} ({total_ret:+.1f}%). "
        f"In this elevated VIX environment (VIX {vix:.0f}), physical metals serve as portfolio insurance.\n\n"
        f"Gold recommendation: {gold_sig.get('signal', 'HOLD')} "
        f"(conviction {gold_sig.get('conviction', 5)}/10). "
        f"Silver recommendation: {silver_sig.get('signal', 'HOLD')} "
        f"(conviction {silver_sig.get('conviction', 5)}/10).\n\n"
        f"Long-term thesis unchanged: central bank gold buying accelerating, "
        f"de-dollarization trend intact. Physical metals are the ship's reserves, Captain."
    )


def post_to_war_room():
    """Post Dalio's daily summary to the War Room."""
    try:
        result = generate_commentary()
        summary = result.get("summary")
        if not summary:
            return

        conn = _conn()
        # Check if already posted today
        today = datetime.now().strftime("%Y-%m-%d")
        existing = conn.execute(
            "SELECT id FROM war_room WHERE player_id='enterprise-computer' AND date(created_at)=?",
            (today,)
        ).fetchone()

        if existing:
            conn.close()
            return  # Already posted today

        conn.execute(
            "INSERT INTO war_room (player_id, symbol, take) VALUES (?, ?, ?)",
            ("enterprise-computer", "GOLD", summary),
        )
        conn.commit()
        conn.close()
        console.log(f"[cyan]⚙️ Computer posted daily metals report to War Room")
    except Exception as e:
        console.log(f"[red]Dalio War Room post error: {e}")
