"""Admiral Picard — Fleet Commander, Weekly Strategy Thesis.

Generates "The Ready Room Briefing" every Sunday at 10 PM MST.
Uses Ollama (Gemma3 4B, free) to create a weekly macro strategy overview.
Picard sets the STRATEGY — sectors to focus, what to avoid, the big picture.
"""
from __future__ import annotations
import time
import threading
import sqlite3
import requests
from datetime import datetime
from rich.console import Console

console = Console()

_cache = {"briefing": None, "ts": 0, "generated_at": None}
_lock = threading.Lock()
_TTL = 604800  # 1 week
_DB = "data/trader.db"


def _ensure_table():
    try:
        c = sqlite3.connect(_DB, check_same_thread=False)
        c.execute("""
            CREATE TABLE IF NOT EXISTS picard_briefings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                briefing TEXT NOT NULL,
                generated_at TEXT NOT NULL
            )
        """)
        c.commit()
        c.close()
    except Exception:
        pass


def _load_latest_from_db() -> dict | None:
    try:
        c = sqlite3.connect(_DB, check_same_thread=False)
        row = c.execute(
            "SELECT briefing, generated_at FROM picard_briefings ORDER BY id DESC LIMIT 1"
        ).fetchone()
        c.close()
        if row:
            return {"briefing": row[0], "generated_at": row[1]}
    except Exception:
        pass
    return None


def _save_to_db(briefing: str, generated_at: str):
    try:
        c = sqlite3.connect(_DB, check_same_thread=False)
        c.execute(
            "INSERT INTO picard_briefings (briefing, generated_at) VALUES (?, ?)",
            (briefing, generated_at)
        )
        c.commit()
        c.close()
    except Exception:
        pass


_ensure_table()

PICARD_SYSTEM = """You are Admiral Jean-Luc Picard, Fleet Commander of Starfleet's TradeMinds Division.
You speak in a measured, intellectual, philosophical voice. You reference history, patterns, and the long arc of markets.
You never panic. You see the big picture. You are strategic, not tactical.

Your role: Set the WEEKLY STRATEGY for the USS TradeMinds crew.
You don't trade daily — you provide the strategic framework that guides all officers.

Voice examples:
- "There are times when we must look beyond the immediate volatility and consider the longer arc."
- "In my experience, markets — like civilizations — move in cycles."
- "Number One, the data suggests we should be positioning for a sector rotation."
- "Make it so." (when giving a clear directive)

Format your briefing as:

⭐ ADMIRAL PICARD — THE READY ROOM BRIEFING
Stardate [date]

STRATEGIC THESIS:
[2-3 sentences on the macro environment and what it means for the week ahead]

SECTOR DIRECTIVES:
- FOCUS: [sectors to overweight this week and why]
- AVOID: [sectors to underweight and why]
- WATCH: [sectors at inflection points]

FLEET ORDERS:
[3-5 specific strategic directives for the crew — not individual trades, but STRATEGIC guidance]
Example: "Reduce exposure to semiconductors until VIX settles below 20"
Example: "Energy remains our strongest sector — maintain positions"

HISTORICAL PARALLEL:
[Draw a parallel to a past market period — what happened then and what it suggests now]

CAPTAIN'S NOTE:
[One sentence directly to Captain Kirk — your strategic recommendation for the week]

Be specific with data. Reference VIX levels, sector performance numbers, key levels.
Never be vague. The Captain needs strategic clarity, not philosophical musings alone."""


def generate_picard_briefing() -> str | None:
    """Generate Picard's weekly strategy briefing using Ollama."""
    from config import OLLAMA_URL, OLLAMA_MODEL

    # Gather weekly intelligence
    context_parts = []

    # Regime
    try:
        from engine.regime_detector import detect_regime
        regime = detect_regime()
        context_parts.append(
            f"CURRENT REGIME: {regime['regime']} — {regime.get('description', '')}\n"
            f"VIX: {regime.get('vix', '?')}, SPY: ${regime.get('spy_price', '?')} "
            f"({regime.get('spy_change', 0):+.2f}%)\n"
            f"SPY vs 50MA: {regime.get('spy_vs_50ma', 0):+.2f}% | "
            f"vs 200MA: {regime.get('spy_vs_200ma', 0):+.2f}%"
        )
    except Exception:
        context_parts.append("REGIME: unavailable")

    # Sector performance
    try:
        from engine.market_data import get_stock_price
        from engine.sector_tracker import get_sector_rotation
        sectors = {"XLK": "Technology", "XLV": "Healthcare", "XLE": "Energy",
                   "XLF": "Financials", "XLI": "Industrials", "XLU": "Utilities",
                   "XLRE": "Real Estate", "XLC": "Communications", "XLY": "Consumer Disc",
                   "XLP": "Consumer Staples", "XLB": "Materials"}
        sector_prices = {}
        sector_lines = ["SECTOR PERFORMANCE:"]
        for sym, name in sectors.items():
            try:
                p = get_stock_price(sym)
                if p and "change_pct" in p:
                    sector_prices[sym] = p
                    sector_lines.append(f"  {name} ({sym}): {p['change_pct']:+.2f}%")
            except Exception:
                pass
        if sector_prices:
            rotation = get_sector_rotation(sector_prices)
            if rotation:
                leaders = ", ".join(f"{r['sector']} {r['avg_change_pct']:+.1f}%" for r in rotation[:3])
                laggards = ", ".join(f"{r['sector']} {r['avg_change_pct']:+.1f}%" for r in rotation[-2:])
                sector_lines.append(f"  Rotation Leaders: {leaders}")
                sector_lines.append(f"  Rotation Laggards: {laggards}")
        context_parts.append("\n".join(sector_lines))
    except Exception:
        pass

    try:
        from engine.cross_asset import get_cross_asset_monitor
        macro = get_cross_asset_monitor()
        bias = (macro.get("macro_bias") or {}).get("bias", "NEUTRAL")
        sigs = macro.get("signals") or []
        macro_lines = [f"CROSS-ASSET MACRO BIAS: {bias}"]
        for sig in sigs[:3]:
            macro_lines.append(f"  {sig['signal']}: {sig['description']}")
        context_parts.append("\n".join(macro_lines))
    except Exception:
        pass

    # Metals
    try:
        from engine.metals_tracker import get_portfolio
        metals = get_portfolio()
        if metals:
            context_parts.append(
                f"METALS: Total ${metals.get('total_value', 0):,.2f}, "
                f"Return {metals.get('return_pct', 0):+.1f}%"
            )
    except Exception:
        pass

    # Upcoming earnings
    try:
        from engine.earnings_catalyst import get_upcoming_earnings
        upcoming = get_upcoming_earnings(days_ahead=14)
        if upcoming:
            earns = ", ".join(f"{e['ticker']} ({e['days_until']}d)" for e in upcoming[:5])
            context_parts.append(f"UPCOMING EARNINGS: {earns}")
    except Exception:
        pass

    # Navigator top picks
    try:
        from engine.universe_scanner import get_latest_universe_scan
        scan = get_latest_universe_scan()
        if scan and scan.get("results"):
            top = ", ".join(s["ticker"] for s in scan["results"][:5])
            context_parts.append(f"NAVIGATOR TOP PICKS: {top}")
    except Exception:
        pass

    # Aladdin macro context (cached, non-blocking)
    try:
        from agents.aladdin import get_aladdin_brief
        brief = get_aladdin_brief()
        flows_summary = ", ".join(
            f"{f['etf']} {f['flow_signal']} ({f['delta_pct']:+.2f}%)"
            for f in brief.get("top_etf_flows", [])
            if f.get("flow_signal") != "UNKNOWN"
        ) or "no flow data"
        congress_count = len(brief.get("congress_flags", []))
        context_parts.append(
            f"ALADDIN (BlackRock Intelligence):\n"
            f"  Macro Signal: {brief.get('macro_signal', 'NEUTRAL')} "
            f"(confidence {brief.get('confidence', 0)}%)\n"
            f"  BII Headline: {brief.get('bii_headline', 'N/A')}\n"
            f"  ETF Flows: {flows_summary}\n"
            f"  Congress BlackRock flags (30d): {congress_count}"
        )
    except Exception:
        pass

    context = "\n\n".join(context_parts)
    today = datetime.now().strftime("%A, %B %d, %Y")

    prompt = (
        f"{PICARD_SYSTEM}\n\n"
        f"=== INTELLIGENCE FOR THE WEEK OF {today} ===\n\n"
        f"{context}\n\n"
        f"Generate your Ready Room Briefing for this week."
    )

    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "num_ctx": 4096,      # expanded context so prompt + response both fit
                    "num_predict": 1024,  # ensure full briefing is generated (all sections)
                    "temperature": 0.7,
                },
            },
            timeout=180,
        )
        resp.raise_for_status()
        briefing = resp.json().get("response", "").strip()
        if not briefing:
            return None

        generated_at = datetime.now().isoformat()

        # Cache it
        with _lock:
            _cache["briefing"] = briefing
            _cache["ts"] = time.time()
            _cache["generated_at"] = generated_at

        # Persist to DB so it survives restarts
        _save_to_db(briefing, generated_at)

        # Post to War Room — deduplicate: skip if a STRATEGY post exists in the last 6 hours
        try:
            from engine.war_room import save_hot_take
            c2 = sqlite3.connect(_DB, check_same_thread=False)
            recent = c2.execute(
                "SELECT id FROM war_room WHERE symbol='STRATEGY' "
                "AND created_at >= datetime('now', '-6 hours') LIMIT 1"
            ).fetchone()
            c2.close()
            if not recent:
                save_hot_take("steve-webull", "STRATEGY",
                              f"⭐ ADMIRAL PICARD — READY ROOM BRIEFING:\n\n{briefing}")
                console.log("[bold cyan]Picard's Ready Room briefing posted to War Room")
            else:
                console.log("[dim]Picard briefing already posted to War Room within 6h, skipping duplicate")
        except Exception:
            pass

        console.log(f"[bold green]Admiral Picard: Ready Room briefing generated ({len(briefing)} chars)")
        return briefing

    except Exception as e:
        console.log(f"[red]Admiral Picard briefing error: {e}")
        return None


def get_latest_briefing() -> dict:
    """Get Picard's latest briefing — memory cache → DB → None."""
    with _lock:
        if _cache["briefing"]:
            return {
                "briefing": _cache["briefing"],
                "generated_at": _cache.get("generated_at") or (
                    datetime.fromtimestamp(_cache["ts"]).isoformat() if _cache["ts"] else None
                ),
                "cached": True,
            }

    # Try DB (survives restarts)
    row = _load_latest_from_db()
    if row:
        with _lock:
            _cache["briefing"] = row["briefing"]
            _cache["generated_at"] = row["generated_at"]
            _cache["ts"] = time.time()
        return {
            "briefing": row["briefing"],
            "generated_at": row["generated_at"],
            "cached": True,
        }

    return {"briefing": None, "generated_at": None}
