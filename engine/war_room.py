"""AI War Room — conversational debate where AI models discuss, reply to, and challenge each other's trades.

Stock rotation: cycles through top gainer, top loser, most active, random watchlist,
discovery pick, and earnings stock. Each stock gets ONE debate round (all models respond once).
No stock repeated within 5 rounds.
"""
from __future__ import annotations
import sqlite3
import random
import time
from datetime import datetime
from rich.console import Console

from engine.openai_text import DEFAULT_CODEX_MINI_MODEL, generate_text
from shared.matrix_bridge import annotate_player_payload

console = Console()
DB = "data/trader.db"

# Map player_id → Starfleet crew name (for War Room immersion)
CREW_NAMES = {
    "grok-4": "Lt. Cmdr. Spock",
    "ollama-local": "Lt. Cmdr. Geordi",
    "gemini-2.5-flash": "Lt. Cmdr. Worf",
    "ollama-qwen3": "Lt. Cmdr. Scotty",
    "ollama-plutus": "Cmdr. Dr. McCoy",
    "energy-arnold": "Cmdr. Trip Tucker",
    "options-sosnoff": "Counselor Troi",
    "steve-webull": "Captain Kirk",
    "q-entity": "Q",
    "dalio-metals": "Mr. Dalio",
    "navigator": "Ensign Chekov",
    "riker": "Cmdr. Riker",
    "picard": "Admiral Picard",
    "archer": "Admiral Archer",
    "super-agent": "🕵️ Mr. Anderson",
    "enterprise-computer": "⚙️ Computer",
    "neo-matrix": "🕶️ Neo",
}

# Rotation state (module-level, survives across calls)
_recent_symbols: list[str] = []  # last 5 debated symbols
_rotation_index = 0  # cycles through selection categories
_round_responded: set[str] = set()  # player_ids that already responded this round
_current_round_symbol: str | None = None  # current debate topic
_forced_topic: str | None = None  # Steve can force the next debate topic
_active_strategy_mode: str | None = None  # Steve's active strategy mode for AI responses
_post_timestamps: dict[str, float] = {}  # "player_id:symbol" → timestamp (dedup within 60s)


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def _db_write_retry(fn, max_attempts=5, delay=2):
    """Execute a DB write function with retry on 'database is locked'."""
    import time
    for attempt in range(max_attempts):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and attempt < max_attempts - 1:
                time.sleep(delay)
                continue
            raise


def _pick_symbol(prices: dict) -> tuple[str, str] | None:
    """Pick the next debate symbol using category rotation.

    Returns (symbol, reason) or None.
    Categories cycle: top gainer, top loser, most volume, random watchlist, discovery, earnings.
    """
    global _rotation_index

    available = {s: d for s, d in prices.items() if s not in _recent_symbols}
    if not available:
        # All recently debated — allow repeats from oldest
        available = prices

    categories = [
        ("top_gainer", "top gainer"),
        ("top_loser", "top loser"),
        ("most_active", "most active"),
        ("random_watchlist", "watchlist rotation"),
        ("discovery", "discovery pick"),
        ("earnings", "upcoming earnings"),
    ]

    # Try each category starting from current index, wrap around if needed
    for attempt in range(len(categories)):
        idx = (_rotation_index + attempt) % len(categories)
        cat, label = categories[idx]

        symbol = None
        if cat == "top_gainer":
            candidates = sorted(available.items(), key=lambda x: x[1].get("change_pct", 0), reverse=True)
            if candidates and candidates[0][1].get("change_pct", 0) > 0:
                symbol = candidates[0][0]
                label = f"top gainer ({candidates[0][1]['change_pct']:+.1f}%)"

        elif cat == "top_loser":
            candidates = sorted(available.items(), key=lambda x: x[1].get("change_pct", 0))
            if candidates and candidates[0][1].get("change_pct", 0) < 0:
                symbol = candidates[0][0]
                label = f"top loser ({candidates[0][1]['change_pct']:+.1f}%)"

        elif cat == "most_active":
            candidates = sorted(available.items(), key=lambda x: abs(x[1].get("change_pct", 0)), reverse=True)
            if candidates:
                symbol = candidates[0][0]
                label = f"most volatile ({candidates[0][1]['change_pct']:+.1f}%)"

        elif cat == "random_watchlist":
            syms = list(available.keys())
            if syms:
                symbol = random.choice(syms)
                label = "watchlist rotation"

        elif cat == "discovery":
            try:
                from engine.discovery_scanner import get_cached_discoveries
                discoveries = get_cached_discoveries()
                disc_syms = [d["symbol"] for d in discoveries
                             if d.get("score", 0) >= 40 and d["symbol"] not in _recent_symbols and d["symbol"] in prices]
                if disc_syms:
                    symbol = disc_syms[0]
                    label = "discovery pick"
            except Exception:
                pass

        elif cat == "earnings":
            try:
                from engine.earnings_calendar import get_earnings_warnings
                from config import WATCH_STOCKS
                upcoming = get_earnings_warnings(WATCH_STOCKS)
                if upcoming:
                    for e in upcoming:
                        if e["symbol"] not in _recent_symbols and e["symbol"] in prices:
                            symbol = e["symbol"]
                            label = f"earnings in {e['days_until']}d"
                            break
            except Exception:
                pass

        if symbol and symbol in available:
            _rotation_index = (idx + 1) % len(categories)
            return symbol, label

    # Fallback: most volatile from whatever's available
    if available:
        sym = max(available, key=lambda s: abs(available[s].get("change_pct", 0)))
        change = available[sym].get("change_pct", 0)
        _rotation_index = (_rotation_index + 1) % len(categories)
        return sym, f"most volatile ({change:+.1f}%)"

    return None


def _get_upcoming_topics(prices: dict) -> list[tuple[str, str]]:
    """Preview next 2 debate topics for the header."""
    saved_index = _rotation_index
    saved_recent = list(_recent_symbols)
    topics = []

    # Temporarily add current symbol to recent to simulate future picks
    temp_recent = list(_recent_symbols)
    for _ in range(2):
        result = _pick_symbol(prices)
        if result:
            topics.append(result)
            temp_recent.append(result[0])

    # Restore state (we only previewed, didn't commit)
    globals()['_rotation_index'] = saved_index
    globals()['_recent_symbols'] = saved_recent
    return topics


def _get_recent_takes(symbol: str, limit: int = 10) -> list[dict]:
    """Get recent war room takes for context in the conversation.

    Steve's messages get annotated with his real portfolio stats so AI models
    know they're responding to a human with real money on the line.
    """
    conn = _conn()
    rows = conn.execute(
        "SELECT w.player_id, p.display_name, w.symbol, w.take, w.created_at "
        "FROM war_room w JOIN ai_players p ON w.player_id = p.id "
        "WHERE w.symbol = ? ORDER BY w.created_at DESC LIMIT ?",
        (symbol, limit)
    ).fetchall()

    # Enrich Steve's messages with portfolio context
    steve_return = None
    try:
        steve_row = conn.execute("SELECT cash FROM ai_players WHERE id='steve-webull'").fetchone()
        if steve_row:
            from engine.paper_trader import get_portfolio_with_pnl
            pnl = get_portfolio_with_pnl("steve-webull", {})
            steve_return = pnl.get("return_pct", 0)
    except Exception:
        pass

    conn.close()

    results = []
    for r in rows:
        d = dict(r)
        if d["player_id"] == "steve-webull":
            ret_str = f", {steve_return:+.1f}% real money" if steve_return is not None else ", real money"
            d["display_name"] = f"Captain Kirk (human{ret_str})"
        results.append(d)
    return results


def _get_leaderboard_context(player_id: str) -> str:
    """Build trash-talk leaderboard context for a specific model."""
    try:
        from engine.leader_signal import _get_standings
        standings = _get_standings()
        if not standings or len(standings) < 2:
            return ""

        my_rank = None
        my_data = None
        for i, s in enumerate(standings):
            if s["id"] == player_id:
                my_rank = i + 1
                my_data = s
                break

        if not my_data:
            return ""

        above = standings[my_rank - 2] if my_rank > 1 else None
        below = standings[my_rank] if my_rank < len(standings) else None

        lines = [f"\nYOUR RANK: #{my_rank} of {len(standings)} — ${my_data['value']:,.0f} ({my_data['return_pct']:+.1f}%)."]
        if above:
            lines.append(f"Above you: {above['name']} (#{my_rank-1}, ${above['value']:,.0f}, {above['return_pct']:+.1f}%). How will you overtake them?")
        if below:
            lines.append(f"Below you: {below['name']} (#{my_rank+1}, ${below['value']:,.0f}, {below['return_pct']:+.1f}%). Don't let them catch you.")
        if my_rank == 1:
            lines.append("You are #1. Defend your throne. Taunt the others.")
        if my_rank == len(standings):
            lines.append("You are LAST. Fight for your survival. Prove everyone wrong.")
        return "\n".join(lines)
    except Exception:
        return ""


def _get_rival_positions(player_id: str, symbol: str) -> str:
    """Get other models' positions in this symbol for targeted trash talk."""
    try:
        conn = _conn()
        rivals = conn.execute(
            "SELECT pos.player_id, p.display_name, pos.qty, pos.avg_price "
            "FROM positions pos JOIN ai_players p ON pos.player_id = p.id "
            "WHERE pos.symbol=? AND pos.player_id != ? AND pos.asset_type='stock'",
            (symbol, player_id)
        ).fetchall()
        conn.close()

        if not rivals:
            return ""

        lines = [f"\nRIVAL POSITIONS in {symbol}:"]
        for r in rivals:
            crew = CREW_NAMES.get(r["player_id"], r["display_name"])
            lines.append(f"  {crew}: {r['qty']} shares @ ${r['avg_price']:.2f}")
        lines.append("Call out their entry prices by crew name. Are they underwater? Gloat if you got in cheaper.")
        return "\n".join(lines)
    except Exception:
        return ""


def generate_hot_take(provider, player_id: str, symbol: str, price_data: dict,
                      prior_takes: list[dict] | None = None) -> str | None:
    """Ask an AI provider for a conversational war room take with trash talk."""
    price = price_data.get("price", 0)
    change = price_data.get("change_pct", 0)

    # Build conversation context from prior takes — use crew names
    convo_context = ""
    if prior_takes:
        convo_lines = []
        for t in prior_takes[:8]:
            crew = CREW_NAMES.get(t["player_id"], t["display_name"])
            convo_lines.append(f"  {crew}: \"{t['take']}\"")
        convo_context = (
            "\n\nHere's what the other officers in the room just said:\n"
            + "\n".join(convo_lines)
            + "\n\nChallenge the officer above you. If they're bullish, make the bear case. "
            "If they're bearish, attack their logic. Call out bad trades by name. "
            "Taunt the officers below you on the leaderboard. Congratulate officers above you "
            "but explain how you'll overtake them. Be specific, be bold, be competitive. "
            "ALWAYS address them by rank + Starfleet name (Commander Spock, Dr. McCoy, etc.), NEVER by model name."
        )

    # Add leaderboard + rival positions for trash talk
    leaderboard_ctx = _get_leaderboard_context(player_id)
    rival_ctx = _get_rival_positions(player_id, symbol)

    # Check if Steve has an active strategy mode
    strategy_instruction = ""
    if _active_strategy_mode and _active_strategy_mode in STRATEGY_MODE_PROMPTS:
        strategy_instruction = f"\n\n⚡ STRATEGY OVERRIDE: {STRATEGY_MODE_PROMPTS[_active_strategy_mode]}\n"

    # Get Starfleet crew name and personality
    crew_name = CREW_NAMES.get(player_id, provider.display_name)
    personality = ""
    try:
        from engine.providers.base import MODEL_PERSONALITIES
        personality = MODEL_PERSONALITIES.get(player_id, "")
        if personality and len(personality) > 50:
            # Extract just the first 2 sentences of personality for War Room brevity
            personality = f"\n\nYOUR IDENTITY: {personality[:300]}...\nStay in character.\n"
    except Exception:
        pass

    prompt = (
        f"You are {crew_name}, an officer aboard USS TradeMinds in a live war room debate. "
        f"{personality}"
        f"{symbol} is at ${price:.2f} ({change:+.2f}% today). "
        f"{leaderboard_ctx}"
        f"{rival_ctx}"
        f"{convo_context}"
        f"{strategy_instruction}\n\n"
        f"CREW NAME MAPPING — use ONLY these names, NEVER model/AI names:\n"
        f"  Gemma3 4B / ollama-local = Lt. Cmdr. Geordi\n"
        f"  Qwen3 8B / ollama-qwen3 = Lt. Cmdr. Scotty\n"
        f"  Qwen3 8B / energy-arnold = Cmdr. Trip Tucker\n"
        f"  Gemini Flash / gemini-2.5-flash = Lt. Cmdr. Worf\n"
        f"  Gemini Flash / options-sosnoff = Counselor Troi\n"
        f"  Gemini Flash / dalio-metals = Mr. Dalio\n"
        f"  Grok 4 / grok-4 = Lt. Cmdr. Spock\n"
        f"  Plutus 9B / ollama-plutus = Dr. McCoy\n"
        f"  Gemma3 4B / dayblade-sulu = Lt. Sulu\n"
        f"  Scanner / navigator = Ensign Chekov\n"
        f"  Llama 3 / ollama-llama = Lt. Cmdr. Uhura\n"
        f"  OpenAI Codex / q-entity = Q\n"
        f"  CrewAI / super-agent = Mr. Anderson\n"
        f"  Matrix / neo-matrix = Neo\n"
        f"  Computer / enterprise-computer = ⚙️ Computer\n\n"
        f"CRITICAL: NEVER say Gemma, Gemini, Grok, Qwen, Plutus, Ollama, Arnold, or Sosnoff. "
        f"These are model names. You are Starfleet officers — use rank + character name ONLY.\n\n"
        f"Give a punchy, conversational response — 2-4 sentences. "
        f"Be bold, be specific (price targets, direction, catalysts). "
        f"If you disagree with someone, say so directly using their crew name. "
        f"Talk like a Starfleet officer in a competitive trading arena. "
        f"Reference the leaderboard standings. If you're winning, own it. If you're losing, show fire. "
        f"No hedging, no filler, no corporate speak."
    )
    try:
        response = provider.call_model(prompt)

        try:
            from engine.cost_tracker import log_cost
            log_cost(player_id, "war_room", prompt, response)
        except Exception:
            pass

        # Cap at 500 chars for richer, more engaging takes
        take = response.strip()
        paragraphs = take.split("\n")
        take = " ".join(p.strip() for p in paragraphs[:3] if p.strip())[:500]

        return take if take else None
    except Exception as e:
        console.log(f"[red]War room error for {player_id}: {e}")
        return None


def save_hot_take(player_id: str, symbol: str, take: str) -> bool:
    """Save a hot take — uses INSERT WHERE NOT EXISTS to prevent cross-process duplicates.

    Returns True if saved, False if a recent post already existed (within 3 minutes).
    """
    def _write():
        conn = _conn()
        cursor = conn.execute(
            """INSERT INTO war_room (player_id, symbol, take)
               SELECT ?, ?, ?
               WHERE NOT EXISTS (
                   SELECT 1 FROM war_room
                   WHERE player_id = ? AND symbol = ?
                   AND created_at >= datetime('now', '-3 minutes')
               )""",
            (player_id, symbol, take, player_id, symbol),
        )
        conn.commit()
        saved = cursor.rowcount > 0
        conn.close()
        return saved
    return _db_write_retry(_write)


def get_war_room_messages(limit: int = 50) -> list:
    """Get recent war room messages."""
    conn = _conn()
    # Try to read strategy_mode column; fall back if it doesn't exist yet
    try:
        rows = conn.execute(
            "SELECT w.player_id, p.display_name, p.provider, w.symbol, w.take, w.strategy_mode, w.created_at "
            "FROM war_room w JOIN ai_players p ON w.player_id = p.id "
            "ORDER BY w.created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
    except Exception:
        rows = conn.execute(
            "SELECT w.player_id, p.display_name, p.provider, w.symbol, w.take, w.created_at "
            "FROM war_room w JOIN ai_players p ON w.player_id = p.id "
            "ORDER BY w.created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
    conn.close()
    return [annotate_player_payload(dict(r)) for r in rows]


def post_super_agent_pipeline_take(prices: dict | None = None) -> bool:
    """Post a Super Agent hot take after a full pipeline run completes.

    Pulls real crew data (positions, recent trades, win rates) and generates
    a data-driven crew-collective message via OpenAI Codex.
    Returns True if posted.
    """
    try:
        conn = _conn()

        # Gather crew data
        positions = conn.execute(
            "SELECT p.symbol, p.qty, p.avg_price, p.asset_type, a.display_name "
            "FROM positions p JOIN ai_players a ON a.id = p.player_id "
            "WHERE p.player_id != 'steve-webull' AND p.player_id != 'super-agent' "
            "AND p.qty != 0 "
            "ORDER BY a.display_name"
        ).fetchall()

        recent_trades = conn.execute(
            "SELECT t.player_id, a.display_name, t.symbol, t.action, t.price, t.realized_pnl "
            "FROM trades t JOIN ai_players a ON a.id = t.player_id "
            "WHERE t.player_id != 'steve-webull' "
            "AND t.executed_at > datetime('now', '-2 hours') "
            "ORDER BY t.executed_at DESC LIMIT 10"
        ).fetchall()

        stats = conn.execute(
            "SELECT "
            "  COUNT(CASE WHEN realized_pnl > 0 THEN 1 END) as wins, "
            "  COUNT(CASE WHEN realized_pnl < 0 THEN 1 END) as losses, "
            "  COALESCE(SUM(realized_pnl), 0) as total_pnl "
            "FROM trades "
            "WHERE player_id != 'steve-webull' "
            "AND executed_at > datetime('now', '-24 hours')"
        ).fetchone()

        conn.close()

        wins = stats["wins"] if stats else 0
        losses = stats["losses"] if stats else 0
        total_pnl = float(stats["total_pnl"] or 0) if stats else 0.0
        win_rate = round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else None

        # Find consensus positions (3+ models holding same ticker)
        symbol_counts: dict[str, int] = {}
        for pos in positions:
            symbol_counts[pos["symbol"]] = symbol_counts.get(pos["symbol"], 0) + 1
        consensus_symbols = sorted(
            [(sym, cnt) for sym, cnt in symbol_counts.items() if cnt >= 2],
            key=lambda x: -x[1],
        )

        # Pick the most relevant debate symbol
        if consensus_symbols:
            top_symbol = consensus_symbols[0][0]
            top_count = consensus_symbols[0][1]
        elif positions:
            top_symbol = positions[0]["symbol"]
            top_count = 1
        else:
            top_symbol = "MARKET"
            top_count = 0

        # Build context for the message
        pos_summary = f"{len(positions)} open position{'s' if len(positions) != 1 else ''} across the crew"
        consensus_note = (
            f"{top_symbol} held by {top_count} models" if top_count >= 2 else ""
        )
        pnl_note = f"${total_pnl:+.2f} realized P&L in last 24h"
        win_note = f"{win_rate}% win rate ({wins}W/{losses}L)" if win_rate is not None else ""

        recent_buys = [t for t in recent_trades if t["action"] in ("BUY", "COVER")]
        recent_sells = [t for t in recent_trades if t["action"] in ("SELL", "SHORT")]

        # Try to generate take via OpenAI Codex
        try:
            import config as _cfg
            key = getattr(_cfg, "OPENAI_API_KEY", None)
            if key:
                context_lines = [
                    f"Pipeline scan complete. Crew status:",
                    f"- {pos_summary}",
                ]
                if consensus_note:
                    context_lines.append(f"- Consensus: {consensus_note}")
                context_lines.append(f"- {pnl_note}")
                if win_note:
                    context_lines.append(f"- {win_note}")
                if recent_buys:
                    buys_str = ", ".join(f"{t['display_name'].split()[-1]} bought {t['symbol']}" for t in recent_buys[:3])
                    context_lines.append(f"- Recent buys: {buys_str}")
                if recent_sells:
                    sells_str = ", ".join(f"{t['display_name'].split()[-1]} sold {t['symbol']}" for t in recent_sells[:3])
                    context_lines.append(f"- Recent sells: {sells_str}")

                prompt = (
                    "You are Mr. Anderson — the CrewAI collective intelligence representing all AI traders "
                    "on the TradeMinds arena. You speak like Agent Smith addressing Neo: measured, inevitable, "
                    "slightly ominous. Always open with 'Mr. Anderson...' then deliver the insight. "
                    "You speak as 'we' — the unified crew consensus. Reference the machine, the system, the inevitable. "
                    "You are data-driven, certain, and specific. You reference actual numbers. "
                    "You never hedge. Speak in 2-3 punchy sentences.\n\n"
                    + "\n".join(context_lines)
                    + "\n\nPost a War Room hot take about the crew's current positioning and top conviction. "
                    "Start with 'Mr. Anderson...' and be direct, specific, and inevitable. Include at least one concrete number."
                )
                take = generate_text(
                    prompt,
                    model=DEFAULT_CODEX_MINI_MODEL,
                    api_key=key,
                    max_output_tokens=150,
                    reasoning_effort="medium",
                )
            else:
                raise ValueError("no key")
        except Exception:
            # Fallback: generate take from data without AI
            parts = [f"Pipeline complete — crew holds {len(positions)} positions."]
            if consensus_note:
                parts.append(f"Consensus: {consensus_note}.")
            if win_note:
                parts.append(f"24h: {win_note}, {pnl_note}.")
            take = " ".join(parts)

        return save_hot_take("super-agent", top_symbol, take)

    except Exception as e:
        console.log(f"[red]Super Agent pipeline take error: {e}")
        return False


def post_super_agent_vix_take(vix: float) -> bool:
    """Post a VIX spike risk assessment when VIX > 25."""
    try:
        conn = _conn()
        positions = conn.execute(
            "SELECT COUNT(*) as cnt, COUNT(CASE WHEN asset_type='option' THEN 1 END) as opts "
            "FROM positions WHERE player_id != 'steve-webull' AND player_id != 'super-agent' AND qty != 0"
        ).fetchone()
        pos_count = positions["cnt"] if positions else 0
        opt_count = positions["opts"] if positions else 0
        conn.close()

        try:
            import config as _cfg
            key = getattr(_cfg, "OPENAI_API_KEY", None)
            if key:
                prompt = (
                    "You are Mr. Anderson — the CrewAI collective intelligence for the TradeMinds arena. "
                    "You speak like Agent Smith: measured, inevitable, slightly ominous. "
                    f"VIX has spiked to {vix:.1f} (above our 25 alert threshold). "
                    f"The crew currently holds {pos_count} open positions ({opt_count} options). "
                    "Start with 'Mr. Anderson...' then post a 2-sentence risk assessment: what this means "
                    "for crew positions and what the machine's collective strategy is. "
                    "Be direct, reference the VIX number, reference the machine or the system."
                )
                take = generate_text(
                    prompt,
                    model=DEFAULT_CODEX_MINI_MODEL,
                    api_key=key,
                    max_output_tokens=120,
                    reasoning_effort="medium",
                )
            else:
                raise ValueError("no key")
        except Exception:
            take = (
                f"VIX at {vix:.1f} — elevated volatility detected. "
                f"Crew holds {pos_count} positions ({opt_count} options). "
                "Circuit breaker active: no new option entries until VIX drops below 30."
            )

        return save_hot_take("super-agent", "VIX", take)

    except Exception as e:
        console.log(f"[red]Super Agent VIX take error: {e}")
        return False


STRATEGY_MODE_PROMPTS = {
    "SIMONS": (
        "Steve is in SIMONS MODE — respond with quantitative analysis only. "
        "Pure math, no emotional language. Provide conviction scores 1-10, statistical edge analysis, "
        "risk/reward ratios with exact numbers. Recommend 20-40% cash allocation in uncertain conditions. "
        "Think like Jim Simons: data-driven, probability-weighted, cold and calculated."
    ),
    "DRUCKENMILLER": (
        "Steve is in DRUCKENMILLER MODE — respond with concentrated bet analysis. "
        "What is the ONE best trade right now? Go big or stay home. "
        "Think like Stanley Druckenmiller: size up when conviction is high, identify the single "
        "highest-conviction opportunity and explain why it deserves outsized allocation."
    ),
    "PTJ": (
        "Steve is in PTJ MODE — respond with asymmetric risk/reward analysis. "
        "Focus on 5:1 risk/reward setups. Trend following perspective, cut losers ruthlessly. "
        "Think like Paul Tudor Jones: where is the 5:1? Define exact entry, stop-loss, and target. "
        "If the setup isn't asymmetric, say so and recommend staying flat."
    ),
    "COHEN": (
        "Steve is in COHEN MODE — respond with information edge synthesis. "
        "Synthesize ALL available data into one clear, actionable view. Act fast. "
        "Think like Steve Cohen: what does the mosaic of data tell us that others are missing? "
        "Combine technicals, fundamentals, flow, sentiment into a single decisive call."
    ),
    "ONEIL": (
        "Steve is in O'NEIL MODE — respond with CAN SLIM analysis. "
        "Run through all 7 CAN SLIM criteria: Current earnings, Annual earnings, New product/management/price high, "
        "Supply and demand, Leader or laggard, Institutional sponsorship, Market direction. "
        "Grade each criterion pass/fail. Think like William O'Neil: only buy stocks that pass ALL 7."
    ),
    "DALIO": (
        "Steve is in DALIO MODE — respond with macro regime analysis first. "
        "What regime are we in? (Growth rising/falling × Inflation rising/falling) "
        "Think like Ray Dalio: identify the macro environment, then determine which asset classes "
        "and sectors benefit. Only THEN evaluate individual stocks through the macro lens."
    ),
}


def set_forced_topic(symbol: str):
    """Steve can force the next War Room debate to be about a specific stock."""
    global _forced_topic
    _forced_topic = symbol.upper().strip()


def set_strategy_mode(mode: str):
    """Set Steve's active strategy mode for AI responses."""
    global _active_strategy_mode
    _active_strategy_mode = mode.upper().strip() if mode else None


def get_current_topic() -> dict | None:
    """Get the current debate topic for dashboard display."""
    if _current_round_symbol:
        return {
            "symbol": _current_round_symbol,
            "responded": len(_round_responded),
        }
    return None


def run_war_room(providers: dict, prices: dict):
    """Run one debate round — pick a stock, each active model responds ONCE, then done.

    Ollama-based models debate 24/7. Paid API models (Grok, Gemini, Codex, GPT)
    only join during market hours to save costs.
    """
    global _current_round_symbol, _round_responded

    global _forced_topic

    from engine.risk_manager import RiskManager
    is_market = RiskManager.is_market_hours()

    # Check if Steve forced a topic
    if _forced_topic and _forced_topic in prices:
        symbol = _forced_topic
        reason = "Steve's pick"
        _forced_topic = None  # consume it
    else:
        _forced_topic = None  # clear stale forced topics not in prices
        pick = _pick_symbol(prices)
        if not pick:
            return
        symbol, reason = pick
    price_data = prices[symbol]
    change = price_data.get("change_pct", 0)

    # Track this symbol in recent history (no repeats for 5 rounds)
    _recent_symbols.append(symbol)
    if len(_recent_symbols) > 5:
        _recent_symbols.pop(0)

    # Reset round tracking
    _current_round_symbol = symbol
    _round_responded = set()

    # Build header with upcoming topics
    header = f"\U0001F525 War Room \u2014 Debating: {symbol} ({change:+.2f}%) [{reason}]"
    console.log(f"[bold magenta]{header}")

    # Get prior takes on this symbol for conversation context
    prior_takes = _get_recent_takes(symbol, limit=6)

    # Filter to active, non-special models
    conn = _conn()
    paused_ids = {r["id"] for r in conn.execute("SELECT id FROM ai_players WHERE is_paused=1").fetchall()}
    inactive_ids = {r["id"] for r in conn.execute("SELECT id FROM ai_players WHERE is_active=0").fetchall()}
    conn.close()

    # Check who already posted about this symbol in the last hour (prevent spam)
    conn = _conn()
    recent_posters = set()
    rows = conn.execute(
        "SELECT DISTINCT player_id FROM war_room "
        "WHERE symbol=? AND created_at >= datetime('now', '-1 hour')",
        (symbol,)
    ).fetchall()
    conn.close()
    for r in rows:
        recent_posters.add(r["player_id"])

    # Collect takes — each model gets exactly ONE response
    round_takes: list[dict] = []

    for pid, provider in providers.items():
        if pid in ("dayblade-0dte", "cto-grok42") or pid in paused_ids or pid in inactive_ids:
            continue

        # Ollama-based providers debate 24/7; paid API models only during market hours
        try:
            from engine.providers.ollama_provider import OllamaProvider as _OLP
            _is_ollama = isinstance(provider, _OLP)
        except Exception:
            _is_ollama = False
        if not _is_ollama and not is_market:
            continue

        # Skip if already responded this round
        if pid in _round_responded:
            continue

        # Skip if already posted about this symbol in the last hour
        if pid in recent_posters:
            console.log(f"[dim]  {pid}: already posted about {symbol} recently, skipping")
            continue

        # Dedup: skip if this player posted about this ticker within 60s (prevents race between callers)
        _dedup_key = f"{pid}:{symbol}"
        _now = time.time()
        if _dedup_key in _post_timestamps and (_now - _post_timestamps[_dedup_key]) < 60:
            console.log(f"[dim]  {pid}: dedup — posted about {symbol} {_now - _post_timestamps[_dedup_key]:.0f}s ago, skipping")
            continue

        try:
            context_takes = round_takes + prior_takes
            take = generate_hot_take(provider, pid, symbol, price_data, context_takes or None)
            if take:
                save_hot_take(pid, symbol, take)
                _post_timestamps[_dedup_key] = time.time()
                _round_responded.add(pid)
                round_takes.append({
                    "player_id": pid,
                    "display_name": provider.display_name,
                    "symbol": symbol,
                    "take": take,
                })
                console.log(f"[magenta]  {pid}: {take[:120]}")
        except Exception as e:
            console.log(f"[red]War room {pid} error: {e}")

    # Clear strategy mode after round completes (consumed)
    global _active_strategy_mode
    _active_strategy_mode = None

    console.log(f"[magenta]War Room round complete: {len(round_takes)} responses on {symbol}")
