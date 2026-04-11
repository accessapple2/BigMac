"""
USS TradeMinds — Bridge Vote System
=====================================
Every morning at 9:00 AM ET, the 8 Tier-3 Bridge Voters receive the
Ready Room briefing and each cast a vote: BUY | SELL | HOLD.

Tables (in data/trader.db — SACRED DATA RULE: never drop/truncate):
  bridge_votes     — one row per voter per session
  bridge_consensus — one row per session with tally and consensus

API:
  GET /api/bridge/votes      — latest votes for today (or most recent)
  GET /api/bridge/consensus  — latest consensus record
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")

# ── Bridge voters (Tier 3) — active fleet ───────────────────────────────────
BRIDGE_VOTERS: list[dict] = [
    {"player_id": "neo-matrix",     "name": "Neo",               "model": "qwen3.5:9b"},
    {"player_id": "grok-4",         "name": "Spock",             "model": "qwen3.5:9b"},
    {"player_id": "ollama-glm4",    "name": "Q",                 "model": "qwen3.5:9b"},
    {"player_id": "ollama-qwen3",   "name": "Dax",               "model": "qwen3.5:9b"},
    {"player_id": "super-agent",    "name": "Mr. Anderson",      "model": "qwen3.5:9b"},
    {"player_id": "navigator",      "name": "Ensign Chekov",     "model": "qwen3.5:9b"},
    {"player_id": "capitol-trades", "name": "Capitol Trades",    "model": "qwen3.5:9b"},
    {"player_id": "ollama-plutus",  "name": "Dr. McCoy",         "model": "qwen3.5:9b"},
]

# Module-level lock: prevents concurrent vote sessions (e.g. two scheduler ticks)
_VOTE_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _init_db() -> None:
    """Create bridge_votes and bridge_consensus tables if they don't exist."""
    with _conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS bridge_votes (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                session_date TEXT    NOT NULL,
                session_time TEXT    NOT NULL,
                player_id    TEXT    NOT NULL,
                player_name  TEXT    NOT NULL,
                vote         TEXT    NOT NULL,   -- BUY | SELL | HOLD
                confidence   INTEGER NOT NULL,   -- 0-100
                reason       TEXT,
                model_used   TEXT,
                created_at   TEXT    NOT NULL DEFAULT (datetime('now'))
            )
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_bv_date
            ON bridge_votes (session_date, player_id)
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS bridge_consensus (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                session_date    TEXT    NOT NULL,
                session_time    TEXT    NOT NULL,
                buy_votes       INTEGER NOT NULL DEFAULT 0,
                sell_votes      INTEGER NOT NULL DEFAULT 0,
                hold_votes      INTEGER NOT NULL DEFAULT 0,
                total_voters    INTEGER NOT NULL DEFAULT 0,
                conviction      TEXT    NOT NULL,  -- HIGH | MODERATE | HOLD
                consensus_vote  TEXT    NOT NULL,  -- BUY | SELL | HOLD
                avg_confidence  INTEGER NOT NULL DEFAULT 0,
                briefing_summary TEXT,
                created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
            )
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_bc_date
            ON bridge_consensus (session_date)
        """)
        c.commit()


# ---------------------------------------------------------------------------
# Ollama voter
# ---------------------------------------------------------------------------

def _ask_voter(voter: dict, briefing_text: str, session_type: str) -> dict:
    """
    Ask a single voter to cast their vote given the Ready Room briefing.
    Returns {"vote": "BUY"|"SELL"|"HOLD", "confidence": 0-100, "reason": str, "model": str}
    """
    model = voter["model"]
    name = voter["name"]

    system_prompt = (
        f"You are {name}, a trading analyst on the USS TradeMinds Bridge Crew. "
        "Analyze the market data below and cast ONE vote for SPY today.\n\n"
        "Respond in EXACTLY this format — three lines, nothing else:\n\n"
        "VOTE: BUY\n"
        "CONFIDENCE: 78\n"
        "REASON: SPY holding above gamma flip at $560 with VIX declining to 18, put/call ratio neutral at 0.9\n\n"
        "Rules:\n"
        "- VOTE must be BUY, SELL, or HOLD\n"
        "- CONFIDENCE is an integer 0-100\n"
        "- REASON must be YOUR OWN specific analysis citing actual numbers from the briefing (price, VIX, P/C ratio, GEX levels, etc.)\n"
        "- Do NOT use placeholder text — write a real 1-2 sentence opinion with data\n"
        "- Do NOT include any text outside of the three lines above\n"
        "- Do NOT show your thinking process"
    )

    user_msg = (
        f"BRIDGE VOTE REQUEST — {datetime.now().strftime('%Y-%m-%d %H:%M')} ET\n"
        f"Session Type: {session_type}\n\n"
        f"Ready Room Briefing:\n{briefing_text}\n\n"
        f"Cast your vote as {name}. Use the VOTE:/CONFIDENCE:/REASON: format only."
    )

    # Try the model, fallback to mistral:7b (different model so qwen busy → mistral steps in)
    fallback = "mistral:7b" if model != "mistral:7b" else "qwen3.5:9b"
    for attempt_model in (model, fallback):
        try:
            resp = requests.post(
                f"{OLLAMA_URL}/api/chat",
                json={
                    "model": attempt_model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_msg},
                    ],
                    "stream": False,
                    "think": False,  # disable thinking mode for qwen3/deepseek reasoning models
                    "options": {"temperature": 0.3, "num_predict": 300},
                },
                timeout=60,
            )
            resp.raise_for_status()
            msg = resp.json().get("message", {})
            raw = msg.get("content", "")
            # Fallback: thinking models may still put output in thinking field
            if not raw.strip():
                raw = msg.get("thinking", "")
            # Strip <think>...</think> blocks from reasoning models
            if "<think>" in raw:
                raw = raw[raw.rfind("</think>") + 8:].strip() if "</think>" in raw else raw

            import re as _re

            def _parse_structured(text: str) -> tuple[str, int, str]:
                """Parse VOTE:/CONFIDENCE:/REASON: lines from text. Returns (vote, confidence, reason)."""
                _vote = ""
                _conf = -1
                _reason = ""
                for _line in text.splitlines():
                    _l = _line.strip()
                    if _l.upper().startswith("VOTE:"):
                        _v = _l.split(":", 1)[1].strip().upper()
                        if _v in ("BUY", "SELL", "HOLD"):
                            _vote = _v
                    elif _l.upper().startswith("CONFIDENCE:"):
                        try:
                            _conf = max(0, min(100, int(_re.sub(r"[^\d]", "", _l.split(":", 1)[1]))))
                        except Exception:
                            pass
                    elif _l.upper().startswith("REASON:"):
                        _reason = _l.split(":", 1)[1].strip()[:500]
                return _vote, _conf, _reason

            vote = "HOLD"
            confidence = 50
            reason = ""

            # Pass 1: strip everything before the first "VOTE:" occurrence
            raw_upper = raw.upper()
            vote_idx = raw_upper.find("VOTE:")
            if vote_idx != -1:
                candidate = raw[vote_idx:]
                _v, _c, _r = _parse_structured(candidate)
                if _v:
                    vote, confidence, reason = _v, (_c if _c >= 0 else 50), _r

            # Pass 2: if still no hit, try the last 10 lines (model often ends with the answer)
            if not reason:
                tail = "\n".join(raw.splitlines()[-10:])
                _v, _c, _r = _parse_structured(tail)
                if _v:
                    vote, confidence, reason = _v, (_c if _c >= 0 else 50), _r

            # Pass 3: bare keyword scan — at least get the direction right
            if not reason:
                for _kw in ("BUY", "SELL", "HOLD"):
                    if _kw in raw_upper:
                        vote = _kw
                        reason = raw.strip()[:300]
                        break

            return {"vote": vote, "confidence": confidence, "reason": reason or raw.strip()[:200], "model": attempt_model}
        except Exception as exc:
            logger.warning("bridge_vote: %s model %s failed: %s", name, attempt_model, exc)
            if attempt_model == fallback:
                break
            time.sleep(1)


    return {"vote": "HOLD", "confidence": 0, "reason": "Model unavailable", "model": "none"}


# ---------------------------------------------------------------------------
# Main vote runner
# ---------------------------------------------------------------------------

def run_morning_vote(force: bool = False) -> dict:
    """
    Run the morning Bridge Vote.

    - Fetches latest Ready Room briefing
    - Polls all 8 voters via Ollama
    - Tallies votes and stores consensus
    - Returns consensus dict

    Args:
        force: if True, run even if already voted today
    """
    # Prevent concurrent runs (e.g. two scheduler ticks overlapping)
    if not _VOTE_LOCK.acquire(blocking=False):
        logger.info("bridge_vote: another vote session is already running, skipping")
        return get_latest_consensus()

    try:
        return _run_morning_vote_inner(force=force)
    finally:
        _VOTE_LOCK.release()


def _run_morning_vote_inner(force: bool = False) -> dict:
    """Inner implementation — called only when _VOTE_LOCK is held."""
    _init_db()

    today = datetime.now().strftime("%Y-%m-%d")
    # 5-minute slot bucket (e.g. "09:05") for dedup
    slot_minute = (datetime.now().minute // 5) * 5
    session_slot = datetime.now().strftime(f"%H:{slot_minute:02d}")

    # Check if already voted today (full set)
    if not force:
        with _conn() as c:
            existing = c.execute(
                "SELECT COUNT(*) as n FROM bridge_votes WHERE session_date=?",
                (today,),
            ).fetchone()
            if existing and existing["n"] >= len(BRIDGE_VOTERS):
                logger.info("bridge_vote: already voted today (%s), skipping", today)
                return get_latest_consensus()

        # Also guard: skip if votes already exist for this 5-minute slot
        with _conn() as c:
            slot_existing = c.execute(
                "SELECT COUNT(*) as n FROM bridge_votes WHERE session_date=? AND session_time=?",
                (today, session_slot),
            ).fetchone()
            if slot_existing and slot_existing["n"] > 0:
                logger.info(
                    "bridge_vote: votes already exist for slot %s %s, skipping",
                    today, session_slot,
                )
                return get_latest_consensus()

    # ── 1. Get Ready Room briefing ─────────────────────────────────────────
    briefing_text = ""
    session_type = "UNKNOWN"
    try:
        from engine.ready_room import get_latest_briefing
        briefing = get_latest_briefing() or {}
        session_type = briefing.get("session_type", "UNKNOWN")
        vix = briefing.get("vix", "?")
        pc = briefing.get("pc_ratio", "?")
        spot = briefing.get("spot_price", "?")
        gameplan = briefing.get("gameplan", "")
        signals_raw = briefing.get("signals", [])
        signals_text = "\n".join(signals_raw[:8]) if signals_raw else "No signals."
        briefing_text = (
            f"SPY Spot: {spot}  |  VIX: {vix}  |  P/C Ratio: {pc}\n"
            f"Session: {session_type}\n"
            f"Signals:\n{signals_text}\n"
            f"Gameplan:\n{gameplan[:600]}"
        )
    except Exception as exc:
        logger.warning("bridge_vote: could not fetch briefing: %s", exc)
        briefing_text = f"Ready Room briefing unavailable. Vote based on available data. Date: {today}"

    logger.info("bridge_vote: starting morning vote (%s), session=%s", today, session_type)

    # ── 2. Collect votes ───────────────────────────────────────────────────
    # Short transaction 1: clear stale data (fast, no long lock held)
    # Retry with backoff — DB may be briefly locked by concurrent writers.
    if force:
        for _attempt in range(20):  # up to ~3 minutes total
            try:
                with _conn() as c:
                    c.execute("DELETE FROM bridge_votes WHERE session_date=?", (today,))
                    c.execute("DELETE FROM bridge_consensus WHERE session_date=?", (today,))
                    c.commit()
                break
            except Exception as _exc:
                if "locked" in str(_exc).lower() and _attempt < 19:
                    time.sleep(5 + _attempt * 2)
                else:
                    raise

    # Poll all voters outside any DB transaction (Ollama calls can take minutes)
    raw_results: list[tuple] = []
    votes: list[dict] = []
    for voter in BRIDGE_VOTERS:
        logger.info("bridge_vote: asking %s (%s)...", voter["name"], voter["model"])
        result = _ask_voter(voter, briefing_text, session_type)
        raw_results.append((
            today, session_slot,
            voter["player_id"], voter["name"],
            result["vote"], result["confidence"],
            result["reason"], result["model"],
        ))
        votes.append({
            "player_id": voter["player_id"],
            "name": voter["name"],
            "vote": result["vote"],
            "confidence": result["confidence"],
            "reason": result["reason"],
            "model": result["model"],
        })
        time.sleep(5)  # stagger Ollama calls — prevents qwen3.5:9b timeout under load

    # Short transaction 2: batch-insert all results at once (fast, minimal lock)
    for _attempt in range(20):
        try:
            with _conn() as c:
                c.executemany(
                    """
                    INSERT INTO bridge_votes
                      (session_date, session_time, player_id, player_name,
                       vote, confidence, reason, model_used)
                    VALUES (?,?,?,?,?,?,?,?)
                    """,
                    raw_results,
                )
                c.commit()
            break
        except Exception as _exc:
            if "locked" in str(_exc).lower() and _attempt < 19:
                time.sleep(5 + _attempt * 2)
            else:
                raise

    # ── 2.5. TradingAgents Debate (optional enhancement) ───────────────────
    # Runs before tally — stores result in debate_engine cache for brain_context.
    # If it fails or times out, Bridge Vote is completely unaffected.
    _debate_summary = ""
    try:
        from engine.debate_engine import run_tradingagents_debate as _ta_debate
        _d = _ta_debate("SPY")
        if _d:
            _debate_summary = (
                f"\nDEBATE INTEL (TradingAgents): Consensus {_d['consensus']} | "
                f"Bull: {_d['bull_case'][:200]} | Bear: {_d['bear_case'][:200]}"
            )
            logger.info(
                "bridge_vote: TradingAgents debate → %s", _d["consensus"]
            )
            # Post to war room
            try:
                import requests as _req
                _req.post(
                    "http://localhost:8080/api/war-room/post",
                    json={
                        "player_id": "debate-engine",
                        "symbol": "SPY",
                        "take": (
                            f"🤖 TradingAgents Debate: {_d['consensus']} | "
                            f"{_d['reasoning'][:180]}"
                        ),
                        "strategy_mode": "DEBATE",
                    },
                    timeout=3,
                )
            except Exception:
                pass
    except Exception as _de_err:
        logger.debug("bridge_vote: debate_engine skip: %s", _de_err)

    # ── 3. Tally ───────────────────────────────────────────────────────────
    # Only count real votes — skip "Model unavailable" / zero-confidence failures
    live_votes = [
        v for v in votes
        if not (v["reason"] == "Model unavailable" or
                (v["confidence"] == 0 and v["model"] == "none"))
    ]
    buy_votes  = sum(1 for v in live_votes if v["vote"] == "BUY")
    sell_votes = sum(1 for v in live_votes if v["vote"] == "SELL")
    hold_votes = sum(1 for v in live_votes if v["vote"] == "HOLD")
    total = len(live_votes)
    avg_conf = int(sum(v["confidence"] for v in live_votes) / total) if total else 0

    if total == 0:
        # All models failed — no quorum
        conviction = "NO QUORUM"
        consensus_vote = "HOLD"
        logger.warning("bridge_vote: NO QUORUM — all voters failed")
    else:
        # Consensus logic (based on live votes only)
        majority_vote = max(
            [("BUY", buy_votes), ("SELL", sell_votes), ("HOLD", hold_votes)],
            key=lambda x: x[1],
        )[0]
        majority_count = {"BUY": buy_votes, "SELL": sell_votes, "HOLD": hold_votes}[majority_vote]

        if majority_count >= 6:
            conviction = "HIGH"
            consensus_vote = majority_vote
        elif majority_count >= 5:
            conviction = "MODERATE"
            consensus_vote = majority_vote
        else:
            conviction = "HOLD"
            consensus_vote = "HOLD"

    # ── 4. Store consensus ─────────────────────────────────────────────────
    with _conn() as c:
        c.execute(
            """
            INSERT INTO bridge_consensus
              (session_date, session_time, buy_votes, sell_votes, hold_votes,
               total_voters, conviction, consensus_vote, avg_confidence, briefing_summary)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                today, session_slot,
                buy_votes, sell_votes, hold_votes, total,
                conviction, consensus_vote, avg_conf,
                (briefing_text + _debate_summary)[:1000],
            ),
        )
        c.commit()

    consensus = {
        "session_date":   today,
        "session_time":   session_slot,
        "session_type":   session_type,
        "buy_votes":      buy_votes,
        "sell_votes":     sell_votes,
        "hold_votes":     hold_votes,
        "total_voters":   total,
        "conviction":     conviction,
        "consensus_vote": consensus_vote,
        "avg_confidence": avg_conf,
        "votes":          votes,
    }

    logger.info(
        "bridge_vote: consensus %s (%s) — BUY:%d SELL:%d HOLD:%d avg_conf:%d%%",
        consensus_vote, conviction, buy_votes, sell_votes, hold_votes, avg_conf,
    )

    # ── Post to Signal Center (fire-and-forget) ─────────────────────────────
    try:
        from engine.signal_poster import post_to_9000
        post_to_9000("BRIDGE_VOTE", {
            "consensus": consensus_vote,
            "conviction": conviction,
            "confidence": avg_conf,
            "buy": buy_votes,
            "sell": sell_votes,
            "hold": hold_votes,
            "summary": (briefing_text + _debate_summary)[:400],
        })
    except Exception:
        pass

    return consensus


# ---------------------------------------------------------------------------
# Read helpers (for API endpoints)
# ---------------------------------------------------------------------------

def get_latest_votes(limit: int = 50) -> dict:
    """Return today's individual votes (fallback to most recent session)."""
    _init_db()
    try:
        with _conn() as c:
            today = datetime.now().strftime("%Y-%m-%d")
            rows = c.execute(
                "SELECT * FROM bridge_votes WHERE session_date=? ORDER BY id DESC LIMIT ?",
                (today, limit),
            ).fetchall()
            if not rows:
                # Fall back to most recent date
                row = c.execute(
                    "SELECT session_date FROM bridge_votes ORDER BY id DESC LIMIT 1"
                ).fetchone()
                if row:
                    rows = c.execute(
                        "SELECT * FROM bridge_votes WHERE session_date=? ORDER BY id DESC LIMIT ?",
                        (row["session_date"], limit),
                    ).fetchall()

            votes = [dict(r) for r in rows]
            buy   = sum(1 for v in votes if v["vote"] == "BUY")
            sell  = sum(1 for v in votes if v["vote"] == "SELL")
            hold  = sum(1 for v in votes if v["vote"] == "HOLD")
            return {
                "session_date": votes[0]["session_date"] if votes else today,
                "votes": votes,
                "tally": {"buy": buy, "sell": sell, "hold": hold, "total": len(votes)},
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
    except Exception as exc:
        logger.error("bridge_vote.get_latest_votes: %s", exc)
        return {"error": str(exc), "votes": [], "tally": {}}


def get_latest_consensus() -> dict:
    """Return the most recent bridge consensus record."""
    _init_db()
    try:
        with _conn() as c:
            row = c.execute(
                "SELECT * FROM bridge_consensus ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                d = dict(row)
                # Attach individual votes for this session
                votes = c.execute(
                    "SELECT * FROM bridge_votes WHERE session_date=? ORDER BY id",
                    (d["session_date"],),
                ).fetchall()
                d["votes"] = [dict(v) for v in votes]
                return d
            return {
                "consensus_vote": "HOLD",
                "conviction": "HOLD",
                "buy_votes": 0,
                "sell_votes": 0,
                "hold_votes": 0,
                "total_voters": 0,
                "avg_confidence": 0,
                "votes": [],
                "note": "No votes recorded yet.",
            }
    except Exception as exc:
        logger.error("bridge_vote.get_latest_consensus: %s", exc)
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Scheduler entry point (called from main.py)
# ---------------------------------------------------------------------------

def run_bridge_vote_job() -> None:
    """
    Scheduled job wrapper: runs morning vote at 9:00 AM ET on weekdays.
    main.py calls this every 5 minutes; gate fires once per day.
    """
    import pytz
    tz_et = pytz.timezone("America/New_York")
    now_et = datetime.now(tz_et)

    # Only on weekdays
    if now_et.weekday() >= 5:
        return

    # Fire window: 9:00–9:10 AM ET
    if not (9 == now_et.hour and 0 <= now_et.minute < 10):
        return

    # Guard: already voted today
    today = now_et.strftime("%Y-%m-%d")
    try:
        _init_db()
        with _conn() as c:
            n = c.execute(
                "SELECT COUNT(*) as n FROM bridge_votes WHERE session_date=?",
                (today,),
            ).fetchone()["n"]
        if n >= len(BRIDGE_VOTERS):
            return
    except Exception:
        pass

    logger.info("bridge_vote: 9 AM ET gate — firing morning vote")
    try:
        result = run_morning_vote()
        logger.info(
            "bridge_vote: complete — %s (%s)",
            result.get("consensus_vote"), result.get("conviction"),
        )
    except Exception as exc:
        logger.error("bridge_vote: run_morning_vote failed: %s", exc)


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    print("=== Bridge Vote Self-Test ===")
    print(f"Voters ({len(BRIDGE_VOTERS)}): {[v['name'] for v in BRIDGE_VOTERS]}")
    print("\nInitializing DB tables...")
    _init_db()
    print("Tables: bridge_votes, bridge_consensus — OK")

    print("\nLatest consensus:")
    c = get_latest_consensus()
    if c.get("note"):
        print(f"  {c['note']}")
    else:
        print(f"  {c.get('consensus_vote')} ({c.get('conviction')}) "
              f"— BUY:{c.get('buy_votes')} SELL:{c.get('sell_votes')} "
              f"HOLD:{c.get('hold_votes')} — {c.get('session_date')}")

    if "--run" in sys.argv:
        print("\nRunning morning vote (force=True)...")
        result = run_morning_vote(force=True)
        print(f"\n=== RESULT ===")
        print(f"Consensus: {result['consensus_vote']} ({result['conviction']})")
        print(f"Tally: BUY={result['buy_votes']} SELL={result['sell_votes']} HOLD={result['hold_votes']}")
        print(f"Avg Confidence: {result['avg_confidence']}%")
        print("\nIndividual votes:")
        for v in result.get("votes", []):
            print(f"  {v['name']:22s}  {v['vote']:4s}  {v['confidence']:3d}%  {v['reason'][:60]}")
