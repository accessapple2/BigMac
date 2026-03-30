"""Officer Consensus Engine — compares Spock, Data, and full crew stances.

Parses briefing text and War Room posts to extract BUY/HOLD/SELL/TRIM per ticker,
then builds a visual consensus report showing agreement/disagreement across all officers.
"""
from __future__ import annotations
import re
import sqlite3
from datetime import datetime
from rich.console import Console

console = Console()
DB = "data/trader.db"

# Canonical actions in priority order (for matching)
ACTIONS = {"BUY", "SELL", "HOLD", "TRIM", "ADD", "CLOSE", "SKIP"}

# Map variations to canonical actions
ACTION_ALIASES = {
    "BUY": "BUY", "LONG": "BUY", "ADD": "ADD", "ACCUMULATE": "ADD",
    "SELL": "SELL", "EXIT": "SELL", "CLOSE": "CLOSE",
    "HOLD": "HOLD", "MAINTAIN": "HOLD", "KEEP": "HOLD",
    "TRIM": "TRIM", "REDUCE": "TRIM", "LIGHTEN": "TRIM",
    "SKIP": "SKIP", "AVOID": "SKIP", "PASS": "SKIP",
}

# Outlook keywords
BULLISH_WORDS = {"bullish", "bull", "upside", "rally", "breakout", "growth", "recovery"}
BEARISH_WORDS = {"bearish", "bear", "downside", "correction", "decline", "risk-off", "selloff", "sell-off"}
NEUTRAL_WORDS = {"neutral", "sideways", "range-bound", "mixed", "choppy", "uncertain"}

# Crew mapping: player_id → (emoji, display_name)
CREW_INFO = {
    "grok-4": ("🖖", "Lt. Cmdr. Spock"),
    "first-officer": ("🤖", "Lt. Cmdr. Data"),
    "mlx-qwen3": ("🧭", "Ensign Chekov"),
    "riker": ("🫡", "Cmdr. Riker"),
    "ollama-local": ("🔧", "Geordi"),
    "gemini-2.5-flash": ("⚔️", "Worf"),
    "ollama-qwen3": ("⚙️", "Scotty"),
    "ollama-plutus": ("💉", "Bones"),
    "energy-arnold": ("⚡", "Trip"),
    "options-sosnoff": ("💜", "Troi"),
    "q-entity": ("✨", "Q"),
    "dalio-metals": ("📊", "Mr. Dalio"),
    "enterprise-computer": ("⚙️", "Computer"),
    "ollama-llama": ("U", "Uhura"),
    "neo-matrix": ("🕶️", "Neo"),
}


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def _normalize_action(raw: str) -> str | None:
    """Normalize a raw action string to a canonical action."""
    raw = raw.strip().upper()
    return ACTION_ALIASES.get(raw)


def parse_outlook(text: str) -> str:
    """Extract market outlook (BULLISH/BEARISH/NEUTRAL) from briefing text."""
    text_lower = text.lower()

    # Look for explicit regime/outlook declarations first
    regime_patterns = [
        r'market\s+(?:regime|outlook)[:\s]+(\w+)',
        r'regime[:\s]+(\w+)',
        r'outlook[:\s]+(\w+)',
    ]
    for pat in regime_patterns:
        m = re.search(pat, text_lower)
        if m:
            word = m.group(1)
            if word in BULLISH_WORDS:
                return "BULLISH"
            if word in BEARISH_WORDS:
                return "BEARISH"
            if word in NEUTRAL_WORDS:
                return "NEUTRAL"

    # Count sentiment words
    bull_count = sum(1 for w in BULLISH_WORDS if w in text_lower)
    bear_count = sum(1 for w in BEARISH_WORDS if w in text_lower)

    if bull_count > bear_count + 1:
        return "BULLISH"
    if bear_count > bull_count + 1:
        return "BEARISH"
    return "NEUTRAL"


def parse_officer_stance(text: str, tickers: list[str]) -> dict:
    """Extract BUY/HOLD/SELL/TRIM per ticker from briefing/take text.

    Returns: {ticker: {"action": str, "conviction": float}}
    """
    results = {}
    text_upper = text.upper()

    actions_alt = "|".join(ACTION_ALIASES.keys())

    for ticker in tickers:
        # Search for patterns like "NVDA: HOLD", "NVDA — BUY", "HOLD NVDA", "BUY NVDA"
        # Also handles Data's format: "AMD (2sh @ $201.33, P&L -0.2%): TRIM 1 share"
        patterns = [
            # "TICKER (anything): ACTION" — Data's SuperGrok format with parenthetical details
            rf'\b{ticker}\b\s*\([^)]*\)\s*[:—\-]+\s*({actions_alt})\b',
            # "TICKER: ACTION" or "TICKER — ACTION"
            rf'\b{ticker}\b[\s:—\-]+({actions_alt})\b',
            # "ACTION TICKER"
            rf'\b({actions_alt})\s+{ticker}\b',
            # "ACTION: ... TICKER: SYMBOL" (structured format)
            rf'ACTION:\s*({actions_alt})\s*\n\s*TICKER:\s*{ticker}\b',
        ]

        action = None
        for pat in patterns:
            m = re.search(pat, text_upper)
            if m:
                raw = m.group(1)
                action = _normalize_action(raw)
                if action:
                    break

        # Extract conviction if present near the ticker
        conviction = 0.5  # default
        conv_patterns = [
            rf'{ticker}[^\n]*?(?:CONVICTION|CONFIDENCE)[:\s]+([0-9]+\.?[0-9]*)',
            rf'{ticker}[^\n]*?\(([0-9]+\.?[0-9]*)(?:\s*%|\s*CONVICTION)?\)',
        ]
        for cp in conv_patterns:
            m = re.search(cp, text_upper)
            if m:
                val = float(m.group(1))
                conviction = val if val <= 1.0 else val / 100.0
                break

        if action:
            results[ticker] = {"action": action, "conviction": round(conviction, 2)}

    return results


def _get_spock_stance(tickers: list[str]) -> tuple[str, dict]:
    """Get Spock's stance from latest CTO briefing."""
    try:
        from engine.cto_advisor import get_latest_briefing
        briefing = get_latest_briefing()
        if not briefing or not briefing.get("briefing"):
            return "NEUTRAL", {}
        text = briefing["briefing"]
        outlook = parse_outlook(text)
        stances = parse_officer_stance(text, tickers)
        return outlook, stances
    except Exception as e:
        console.log(f"[dim]Consensus: Spock briefing unavailable: {e}")
        return "NEUTRAL", {}


def _get_data_stance(tickers: list[str]) -> tuple[str, dict]:
    """Get Data's stance from latest First Officer briefing (MLX-powered).

    Falls back to mlx-qwen3 signals if briefing cache is empty.
    """
    try:
        from engine.first_officer import _briefing_cache
        if _briefing_cache and _briefing_cache.get("briefing"):
            text = _briefing_cache["briefing"]
            outlook = parse_outlook(text)
            stances = parse_officer_stance(text, tickers)
            return outlook, stances
    except Exception as e:
        console.log(f"[dim]Consensus: Data briefing unavailable: {e}")

    # Fallback: pull mlx-qwen3 signals from DB
    try:
        conn = _conn()
        rows = conn.execute(
            "SELECT symbol, signal, confidence, reasoning FROM signals "
            "WHERE player_id='mlx-qwen3' "
            "AND created_at >= datetime('now', '-24 hours') "
            "ORDER BY created_at DESC"
        ).fetchall()
        conn.close()
        if rows:
            stances = {}
            all_text = ""
            seen = set()
            for r in rows:
                sym = r["symbol"]
                if sym in seen or sym not in tickers:
                    continue
                seen.add(sym)
                action = _normalize_action(r["signal"] or "HOLD")
                if action:
                    stances[sym] = {"action": action, "conviction": round(r["confidence"] or 0.5, 2)}
                all_text += (r["reasoning"] or "") + " "
            outlook = parse_outlook(all_text) if all_text.strip() else "NEUTRAL"
            return outlook, stances
    except Exception as e:
        console.log(f"[dim]Consensus: mlx-qwen3 fallback failed: {e}")

    return "NEUTRAL", {}


def _get_uhura_stance(tickers: list[str]) -> tuple[str, dict]:
    """Get Uhura's stance from latest scan signals (sentiment/news angle).

    Pulls ollama-llama signals from DB and War Room posts.
    """
    try:
        conn = _conn()
        # Check War Room posts first
        rows = conn.execute(
            "SELECT symbol, take FROM war_room "
            "WHERE player_id='ollama-llama' "
            "AND created_at >= datetime('now', '-24 hours') "
            "ORDER BY created_at DESC"
        ).fetchall()
        if rows:
            stances = {}
            all_text = ""
            seen = set()
            for r in rows:
                sym = r["symbol"]
                if sym in seen or sym not in tickers:
                    continue
                seen.add(sym)
                parsed = parse_officer_stance(r["take"], [sym])
                stances.update(parsed)
                all_text += (r["take"] or "") + " "
            if stances:
                outlook = parse_outlook(all_text) if all_text.strip() else "NEUTRAL"
                conn.close()
                return outlook, stances

        # Fallback: pull signals from DB
        sig_rows = conn.execute(
            "SELECT symbol, signal, confidence, reasoning FROM signals "
            "WHERE player_id='ollama-llama' "
            "AND created_at >= datetime('now', '-24 hours') "
            "ORDER BY created_at DESC"
        ).fetchall()
        conn.close()
        if sig_rows:
            stances = {}
            all_text = ""
            seen = set()
            for r in sig_rows:
                sym = r["symbol"]
                if sym in seen or sym not in tickers:
                    continue
                seen.add(sym)
                action = _normalize_action(r["signal"] or "HOLD")
                if action:
                    stances[sym] = {"action": action, "conviction": round(r["confidence"] or 0.5, 2)}
                all_text += (r["reasoning"] or "") + " "
            outlook = parse_outlook(all_text) if all_text.strip() else "NEUTRAL"
            return outlook, stances
    except Exception as e:
        console.log(f"[dim]Consensus: Uhura signals unavailable: {e}")

    return "NEUTRAL", {}


def _get_crew_stances(tickers: list[str]) -> dict:
    """Get all crew stances from latest War Room posts and signals.

    Returns: {player_id: {ticker: {"action": str, "conviction": float}}}
    """
    crew = {}
    conn = _conn()

    # Get latest War Room take per player (last 24h)
    rows = conn.execute(
        "SELECT player_id, symbol, take FROM war_room "
        "WHERE created_at >= datetime('now', '-24 hours') "
        "ORDER BY created_at DESC"
    ).fetchall()

    # Group by player_id, keep only latest per player per symbol
    seen = set()
    for r in rows:
        pid = r["player_id"]
        sym = r["symbol"]
        key = f"{pid}:{sym}"
        if key in seen:
            continue
        seen.add(key)

        if pid not in crew:
            crew[pid] = {}

        stances = parse_officer_stance(r["take"], [sym] if sym in tickers else [])
        for t, stance in stances.items():
            crew[pid][t] = stance

    # Also pull from latest signals (last 24h) for models that may not have War Room posts
    signal_rows = conn.execute(
        "SELECT player_id, symbol, signal, confidence FROM signals "
        "WHERE created_at >= datetime('now', '-24 hours') "
        "ORDER BY created_at DESC"
    ).fetchall()

    seen_signals = set()
    for r in signal_rows:
        pid = r["player_id"]
        sym = r["symbol"]
        key = f"{pid}:{sym}"
        if key in seen_signals:
            continue
        seen_signals.add(key)

        if sym not in tickers:
            continue

        # Only add if this player doesn't already have a War Room stance for this ticker
        if pid in crew and sym in crew[pid]:
            continue

        signal = (r["signal"] or "").upper().strip()
        action = _normalize_action(signal)
        if action:
            if pid not in crew:
                crew[pid] = {}
            conviction = r["confidence"] or 0.5
            crew[pid][sym] = {"action": action, "conviction": round(conviction, 2)}

    conn.close()
    return crew


def _compare_actions(a1: str, a2: str) -> str:
    """Compare two actions and return agreement level.

    Returns: 'agree', 'partial', 'disagree', 'opposite'
    """
    if a1 == a2:
        return "agree"

    # Opposite: BUY vs SELL/CLOSE
    opposites = {
        frozenset({"BUY", "SELL"}), frozenset({"BUY", "CLOSE"}),
        frozenset({"ADD", "SELL"}), frozenset({"ADD", "CLOSE"}),
    }
    if frozenset({a1, a2}) in opposites:
        return "opposite"

    # Partial: similar direction but different intensity
    partials = {
        frozenset({"HOLD", "TRIM"}), frozenset({"TRIM", "SELL"}),
        frozenset({"BUY", "ADD"}), frozenset({"HOLD", "ADD"}),
        frozenset({"SELL", "CLOSE"}),
    }
    if frozenset({a1, a2}) in partials:
        return "partial"

    return "disagree"


def build_consensus(tickers: list[str] | None = None) -> dict:
    """Build full consensus report across all officers.

    Returns structured data for the dashboard API.
    """
    # Get Steve's portfolio tickers if not provided
    if not tickers:
        try:
            conn = _conn()
            positions = conn.execute(
                "SELECT DISTINCT symbol FROM positions WHERE player_id='steve-webull'"
            ).fetchall()
            conn.close()
            tickers = [r["symbol"] for r in positions]
        except Exception:
            pass

    if not tickers:
        try:
            from config import WATCH_STOCKS
            tickers = WATCH_STOCKS[:10]
        except Exception:
            tickers = []

    # Get officer stances
    spock_outlook, spock_stances = _get_spock_stance(tickers)
    data_outlook, data_stances = _get_data_stance(tickers)
    uhura_outlook, uhura_stances = _get_uhura_stance(tickers)
    crew_stances = _get_crew_stances(tickers)

    # Market outlook comparison (3-way)
    outlooks = [spock_outlook, data_outlook, uhura_outlook]
    outlook_agree = len(set(outlooks)) == 1
    market_outlook = {
        "spock": spock_outlook,
        "data": data_outlook,
        "uhura": uhura_outlook,
        "agree": outlook_agree,
    }

    # Per-ticker comparison
    ticker_results = {}
    agree_count = 0
    total_compared = 0

    for ticker in tickers:
        spock = spock_stances.get(ticker)
        data = data_stances.get(ticker)
        uhura = uhura_stances.get(ticker)

        # Officer comparison (3-way: count how many of the available officers agree)
        officers = [o for o in [spock, data, uhura] if o]
        if len(officers) >= 2:
            actions = [o["action"] for o in officers]
            # All same = agree, all different = opposite, else partial/disagree
            unique = set(actions)
            if len(unique) == 1:
                comparison = "agree"
                agree_count += 1
            elif len(officers) == 3 and len(unique) == 3:
                comparison = "opposite"
            else:
                # 2 agree, 1 differs — check if the differing one is opposite
                from collections import Counter
                counts = Counter(actions)
                majority = counts.most_common(1)[0][0]
                minority = [a for a in actions if a != majority][0]
                comparison = _compare_actions(majority, minority)
            total_compared += 1
        elif len(officers) == 1:
            comparison = "skip"
            total_compared += 1
        else:
            comparison = "no_data"

        # Crew poll
        crew_poll = {}
        action_counts = {}
        outlier = None
        crew_entries = []

        for pid, stances in crew_stances.items():
            if ticker in stances:
                stance = stances[ticker]
                emoji, name = CREW_INFO.get(pid, ("👤", pid))
                crew_entries.append({
                    "player_id": pid,
                    "emoji": emoji,
                    "name": name,
                    "action": stance["action"],
                    "conviction": stance["conviction"],
                })
                act = stance["action"]
                action_counts[act] = action_counts.get(act, 0) + 1

        # Add Spock, Data, and Uhura to crew entries if not already present
        for pid, stance, label_emoji, label_name in [
            ("grok-4", spock, "S", "Spock"),
            ("first-officer", data, "D", "Data"),
            ("ollama-llama", uhura, "U", "Uhura"),
        ]:
            if stance and not any(e["player_id"] == pid for e in crew_entries):
                crew_entries.append({
                    "player_id": pid,
                    "emoji": label_emoji,
                    "name": label_name,
                    "action": stance["action"],
                    "conviction": stance["conviction"],
                })
                act = stance["action"]
                action_counts[act] = action_counts.get(act, 0) + 1

        # Find consensus and outlier
        total_votes = sum(action_counts.values())
        consensus_action = max(action_counts, key=action_counts.get) if action_counts else None
        consensus_pct = round(action_counts.get(consensus_action, 0) / total_votes * 100) if total_votes > 0 and consensus_action else 0

        # Find outlier (minority opinion holder)
        if consensus_action and total_votes >= 3:
            for entry in crew_entries:
                if entry["action"] != consensus_action:
                    outlier = {"name": entry["name"], "action": entry["action"], "emoji": entry["emoji"]}
                    break

        ticker_results[ticker] = {
            "spock": spock,
            "data": data,
            "uhura": uhura,
            "comparison": comparison,
            "crew_poll": {
                "entries": crew_entries,
                "action_counts": action_counts,
                "consensus_action": consensus_action,
                "consensus_pct": consensus_pct,
                "total_votes": total_votes,
                "outlier": outlier,
            },
        }

    # Overall agreement
    overall_agreement = round(agree_count / total_compared * 100) if total_compared > 0 else 0

    # High conviction calls: tickers where officers agree AND conviction > 0.7
    high_conviction = []
    for ticker, result in ticker_results.items():
        if result["comparison"] == "agree":
            s_conv = result["spock"]["conviction"] if result["spock"] else 0
            d_conv = result["data"]["conviction"] if result["data"] else 0
            u_conv = result["uhura"]["conviction"] if result["uhura"] else 0
            if s_conv >= 0.7 or d_conv >= 0.7 or u_conv >= 0.7:
                high_conviction.append(ticker)

    return {
        "market_outlook": market_outlook,
        "tickers": ticker_results,
        "overall_agreement": overall_agreement,
        "total_compared": total_compared,
        "agree_count": agree_count,
        "high_conviction_calls": high_conviction,
        "timestamp": datetime.now().isoformat(),
    }


def post_consensus_alert(ticker: str, consensus_pct: int, action: str):
    """Post a Riker consensus alert to War Room when agreement > 80%."""
    if consensus_pct < 80:
        return

    from engine.war_room import save_hot_take
    msg = (
        f"🫡 CMDR RIKER: Bridge Consensus Alert — {consensus_pct}% of crew agrees: "
        f"{action} {ticker}. When the crew speaks with one voice, the Captain should listen."
    )
    try:
        save_hot_take("steve-webull", ticker, msg)
        console.log(f"[bold cyan]Consensus alert posted: {ticker} {action} ({consensus_pct}%)")
    except Exception as e:
        console.log(f"[red]Consensus alert failed: {e}")
