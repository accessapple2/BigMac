"""
engine/agent_builder.py — Natural Language Agent Builder
=========================================================
Parse plain-English trading strategies into structured agent specs,
store them in trader.db, and evaluate conditions each scan cycle.

Supported condition types:
  rsi_below / rsi_above
  price_below / price_above
  price_drop_pct / price_rise_pct  (value = percent, e.g. 5 for 5%)
  bridge_vote_consensus_above      (value = percent, e.g. 80)
  vix_above / vix_below
  gex_flip_negative / gex_flip_positive
  volume_spike                     (value = multiplier, e.g. 2 for 2x avg)
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import time
from typing import Optional

import requests

from config import OLLIE_URL as _OLLIE_URL

logger = logging.getLogger("agent_builder")

DB_PATH = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)
OLLAMA_URL = os.environ.get("OLLAMA_URL", _OLLIE_URL)  # Ollie Box GPU (was localhost)
MAX_USER_AGENTS = 10
DEFAULT_SIZE_PCT = 0.01   # 1% of portfolio value
PLAYER_ID = "user-agent"  # paper player for all user-created agents

VALID_CONDITIONS = {
    "rsi_below", "rsi_above",
    "price_below", "price_above",
    "price_drop_pct", "price_rise_pct",
    "bridge_vote_consensus_above",
    "vix_above", "vix_below",
    "gex_flip_negative", "gex_flip_positive",
    "volume_spike",
}


# ── DB helpers ────────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _init_table() -> None:
    conn = _conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_agents (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT    NOT NULL,
            ticker          TEXT    NOT NULL,
            condition_type  TEXT    NOT NULL,
            condition_value REAL,
            action          TEXT    NOT NULL DEFAULT 'buy',
            quantity        REAL,
            stop_loss_pct   REAL,
            take_profit_pct REAL,
            status          TEXT    NOT NULL DEFAULT 'WATCHING',
            nl_prompt       TEXT,
            player_id       TEXT    NOT NULL DEFAULT 'user-agent',
            created_at      TEXT    DEFAULT (datetime('now')),
            triggered_at    TEXT
        )
    """)
    conn.commit()
    conn.close()


def _ensure_user_player(conn: sqlite3.Connection) -> None:
    """Ensure the paper player 'user-agent' exists in ai_players."""
    if not conn.execute("SELECT 1 FROM ai_players WHERE id=?", (PLAYER_ID,)).fetchone():
        conn.execute("""
            INSERT OR IGNORE INTO ai_players
            (id, display_name, provider, model_id, cash, is_active, is_paused, season)
            VALUES (?,?,?,?,?,1,0,5)
        """, (PLAYER_ID, "Captain's Custom Agents", "user", "rules-based", 10000.0))
        conn.commit()


# ── Ollama parser ─────────────────────────────────────────────────────────────

_PARSE_SYSTEM = """You are a trading strategy parser for TradeMinds.
Extract a structured JSON object from the user's natural language trading strategy.

Output ONLY valid JSON with exactly these fields:
{
  "name": "short_snake_case_name",
  "ticker": "XXXX",
  "condition_type": "rsi_below",
  "condition_value": 30,
  "action": "buy",
  "quantity": null,
  "stop_loss_pct": null,
  "take_profit_pct": null
}

Supported condition_type values:
  rsi_below, rsi_above
  price_below, price_above
  price_drop_pct, price_rise_pct   (value = percent, e.g. 5 for 5%)
  bridge_vote_consensus_above      (value = percent, e.g. 80)
  vix_above, vix_below
  gex_flip_negative, gex_flip_positive
  volume_spike                     (value = multiplier, e.g. 2 for 2x avg)

Rules:
- ticker: 1-5 uppercase letters, no $ prefix
- action: "buy" or "sell" only
- quantity: number of shares, or null to use default 1% portfolio sizing
- stop_loss_pct: % below entry for stop-loss, or null
- take_profit_pct: % above entry for take-profit, or null
- name: short snake_case label, max 30 chars
- Output ONLY the JSON, no other text, no markdown fences
"""


def parse_nl_agent(prompt: str) -> Optional[dict]:
    """
    Call Ollama to parse a natural-language strategy into a structured spec.
    Returns dict on success, None on failure.
    """
    # Use the already-loaded model; skip thinking models
    model = "llama3.1:latest"
    try:
        ps = requests.get(f"{OLLAMA_URL}/api/ps", timeout=3).json()
        loaded = (ps.get("models") or [{}])[0].get("name", "")
        if loaded and not any(t in loaded for t in ("qwen3", "deepseek-r1")):
            model = loaded
    except Exception:
        pass

    messages = [
        {"role": "system", "content": _PARSE_SYSTEM},
        {"role": "user",   "content": prompt},
    ]
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model":   model,
                "messages": messages,
                "stream":  False,
                "options": {"temperature": 0.05, "num_predict": 300},
            },
            timeout=45,
        )
        raw = r.json().get("message", {}).get("content", "").strip()
        # Strip markdown fences if present
        raw = re.sub(r"```[a-z]*\n?", "", raw).strip().rstrip("`").strip()
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            return json.loads(m.group())
    except Exception as e:
        logger.error(f"[AgentBuilder] parse_nl_agent error: {e}")
    return None


# ── Agent CRUD ────────────────────────────────────────────────────────────────

def create_agent(spec: dict) -> dict:
    """Validate and persist a new user agent. Returns {ok, agent} or {error}."""
    _init_table()
    conn = _conn()

    active_count = conn.execute(
        "SELECT COUNT(*) FROM user_agents WHERE status != 'DELETED'"
    ).fetchone()[0]
    if active_count >= MAX_USER_AGENTS:
        conn.close()
        return {"error": f"Maximum {MAX_USER_AGENTS} user agents reached — delete one first."}

    for f in ("ticker", "condition_type", "action"):
        if not spec.get(f):
            conn.close()
            return {"error": f"Missing required field: {f}"}

    ticker    = str(spec["ticker"]).upper().strip()
    cond_type = str(spec["condition_type"]).lower().strip()
    action    = str(spec["action"]).lower().strip()

    if cond_type not in VALID_CONDITIONS:
        conn.close()
        return {"error": f"Unknown condition_type '{cond_type}'. "
                         f"Valid: {', '.join(sorted(VALID_CONDITIONS))}"}
    if action not in ("buy", "sell"):
        conn.close()
        return {"error": "action must be 'buy' or 'sell'"}

    name = (spec.get("name") or
            f"ua_{ticker.lower()}_{cond_type[:8]}_{int(time.time()) % 10000}")[:40]

    _ensure_user_player(conn)
    conn.execute("""
        INSERT INTO user_agents
            (name, ticker, condition_type, condition_value, action,
             quantity, stop_loss_pct, take_profit_pct, status, nl_prompt, player_id)
        VALUES (?,?,?,?,?,?,?,?,'WATCHING',?,?)
    """, (
        name, ticker, cond_type,
        spec.get("condition_value"),
        action,
        spec.get("quantity"),
        spec.get("stop_loss_pct"),
        spec.get("take_profit_pct"),
        spec.get("nl_prompt", ""),
        PLAYER_ID,
    ))
    conn.commit()
    row_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    agent  = dict(conn.execute("SELECT * FROM user_agents WHERE id=?", (row_id,)).fetchone())
    conn.close()
    logger.info(f"[AgentBuilder] Created #{row_id}: {name} | {ticker} {cond_type} → {action}")
    return {"ok": True, "agent": agent}


def list_agents() -> list:
    _init_table()
    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM user_agents WHERE status != 'DELETED' ORDER BY created_at DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def pause_agent(agent_id: int) -> dict:
    _init_table()
    conn = _conn()
    conn.execute(
        "UPDATE user_agents SET status='PAUSED' WHERE id=? AND status='WATCHING'",
        (agent_id,)
    )
    conn.commit()
    conn.close()
    return {"ok": True}


def resume_agent(agent_id: int) -> dict:
    _init_table()
    conn = _conn()
    conn.execute(
        "UPDATE user_agents SET status='WATCHING' WHERE id=? AND status='PAUSED'",
        (agent_id,)
    )
    conn.commit()
    conn.close()
    return {"ok": True}


def delete_agent(agent_id: int) -> dict:
    _init_table()
    conn = _conn()
    conn.execute("UPDATE user_agents SET status='DELETED' WHERE id=?", (agent_id,))
    conn.commit()
    conn.close()
    return {"ok": True}


# ── Condition evaluation ──────────────────────────────────────────────────────

def _get_ticker_snapshot(ticker: str) -> dict:
    """Fetch price, RSI-14, volume ratio for one ticker via yfinance."""
    try:
        import yfinance as yf
        hist = yf.Ticker(ticker).history(period="30d", interval="1d")
        if hist.empty:
            return {}
        closes = hist["Close"]
        price  = float(closes.iloc[-1])
        volume = float(hist["Volume"].iloc[-1])

        rsi = 50.0
        if len(closes) >= 15:
            delta = closes.diff()
            gain  = delta.clip(lower=0).rolling(14).mean()
            loss  = (-delta.clip(upper=0)).rolling(14).mean()
            denom = loss.replace(0, float("nan"))
            rsi_s = 100 - (100 / (1 + gain / denom))
            rsi   = float(rsi_s.iloc[-1])

        vol_ratio = 1.0
        try:
            c = _conn()
            row = c.execute(
                "SELECT avg_volume_20d FROM volume_baselines WHERE symbol=?", (ticker,)
            ).fetchone()
            c.close()
            if row and row["avg_volume_20d"]:
                vol_ratio = volume / row["avg_volume_20d"]
        except Exception:
            pass

        return {"price": price, "rsi": rsi, "vol_ratio": vol_ratio, "closes": closes}
    except Exception as e:
        logger.warning(f"[AgentBuilder] snapshot({ticker}): {e}")
        return {}


def _check_condition(agent: dict, market_ctx: dict) -> tuple:
    """
    Evaluate one agent condition against live data.
    Returns (triggered: bool, reason: str).
    """
    ctype  = agent["condition_type"]
    cval   = agent["condition_value"]
    ticker = agent["ticker"]

    # ── VIX (no yfinance needed) ──
    if ctype in ("vix_above", "vix_below"):
        vix = float(market_ctx.get("vix") or 0)
        if cval is None:
            return False, "condition_value required for VIX check"
        if ctype == "vix_above" and vix > cval:
            return True, f"VIX {vix:.1f} > {cval}"
        if ctype == "vix_below" and vix < cval:
            return True, f"VIX {vix:.1f} < {cval}"
        return False, f"VIX {vix:.1f} ({ctype} {cval} not met)"

    # ── GEX regime ──
    if ctype in ("gex_flip_negative", "gex_flip_positive"):
        regime = str(market_ctx.get("gex_regime") or "").upper()
        if ctype == "gex_flip_negative" and "NEG" in regime:
            return True, f"GEX flipped negative ({regime})"
        if ctype == "gex_flip_positive" and "POS" in regime:
            return True, f"GEX flipped positive ({regime})"
        return False, f"GEX regime '{regime}' not matched"

    # ── Bridge vote consensus ──
    if ctype == "bridge_vote_consensus_above":
        pct = float(market_ctx.get("bridge_consensus_pct") or 0)
        if cval is not None and pct > cval:
            return True, f"Bridge consensus {pct:.0f}% > {cval}%"
        return False, f"Bridge consensus {pct:.0f}% ≤ {cval}%"

    # ── Price / RSI / Volume (need yfinance) ──
    snap = _get_ticker_snapshot(ticker)
    if not snap:
        return False, f"No price data for {ticker}"

    price     = snap["price"]
    rsi       = snap["rsi"]
    vol_ratio = snap["vol_ratio"]
    closes    = snap.get("closes")

    if ctype == "rsi_below":
        if cval is not None and rsi < cval:
            return True, f"{ticker} RSI {rsi:.1f} < {cval}"
        return False, f"{ticker} RSI {rsi:.1f} (need < {cval})"

    if ctype == "rsi_above":
        if cval is not None and rsi > cval:
            return True, f"{ticker} RSI {rsi:.1f} > {cval}"
        return False, f"{ticker} RSI {rsi:.1f} (need > {cval})"

    if ctype == "price_below":
        if cval is not None and price < cval:
            return True, f"{ticker} ${price:.2f} < ${cval}"
        return False, f"{ticker} ${price:.2f} (need < ${cval})"

    if ctype == "price_above":
        if cval is not None and price > cval:
            return True, f"{ticker} ${price:.2f} > ${cval}"
        return False, f"{ticker} ${price:.2f} (need > ${cval})"

    if ctype == "price_drop_pct":
        if closes is not None and len(closes) >= 2:
            prev = float(closes.iloc[-2])
            drop = (prev - price) / prev * 100 if prev > 0 else 0.0
            if cval is not None and drop >= cval:
                return True, f"{ticker} dropped {drop:.1f}% ≥ {cval}%"
            return False, f"{ticker} dropped {drop:.1f}% (need ≥ {cval}%)"
        return False, "Insufficient price history"

    if ctype == "price_rise_pct":
        if closes is not None and len(closes) >= 2:
            prev = float(closes.iloc[-2])
            rise = (price - prev) / prev * 100 if prev > 0 else 0.0
            if cval is not None and rise >= cval:
                return True, f"{ticker} rose {rise:.1f}% ≥ {cval}%"
            return False, f"{ticker} rose {rise:.1f}% (need ≥ {cval}%)"
        return False, "Insufficient price history"

    if ctype == "volume_spike":
        threshold = cval if cval is not None else 2.0
        if vol_ratio >= threshold:
            return True, f"{ticker} volume {vol_ratio:.1f}x avg (≥ {threshold}x)"
        return False, f"{ticker} volume {vol_ratio:.1f}x avg (need ≥ {threshold}x)"

    return False, f"Unhandled condition_type: {ctype}"


# ── Scan entry point ──────────────────────────────────────────────────────────

def check_user_agents(market_ctx: dict) -> int:
    """
    Called from crew_scanner each cycle.
    Evaluates all WATCHING agents; fires trade + marks TRIGGERED when met.
    Returns number of agents triggered this cycle.
    """
    _init_table()

    # Hydrate bridge consensus pct if missing
    if "bridge_consensus_pct" not in market_ctx:
        try:
            c = _conn()
            bc = c.execute(
                "SELECT avg_confidence FROM bridge_consensus ORDER BY id DESC LIMIT 1"
            ).fetchone()
            c.close()
            market_ctx["bridge_consensus_pct"] = float(bc["avg_confidence"]) if bc else 0.0
        except Exception:
            market_ctx["bridge_consensus_pct"] = 0.0

    try:
        c = _conn()
        agents = [dict(r) for r in c.execute(
            "SELECT * FROM user_agents WHERE status='WATCHING'"
        ).fetchall()]
        c.close()
    except Exception as e:
        logger.error(f"[AgentBuilder] DB read error: {e}")
        return 0

    triggered = 0
    for agent in agents:
        try:
            fired, reason = _check_condition(agent, market_ctx)
            if not fired:
                continue
            _execute_agent_trade(agent, reason)
            triggered += 1
            c = _conn()
            c.execute(
                "UPDATE user_agents SET status='TRIGGERED', triggered_at=datetime('now') WHERE id=?",
                (agent["id"],)
            )
            c.commit()
            c.close()
            logger.info(f"[AgentBuilder] FIRED #{agent['id']} {agent['name']}: {reason}")
        except Exception as e:
            logger.error(f"[AgentBuilder] agent #{agent.get('id')} check error: {e}")

    return triggered


def _execute_agent_trade(agent: dict, reason: str) -> None:
    """Submit buy/sell via paper_trader for a triggered user agent."""
    from engine.paper_trader import buy, sell, get_portfolio

    ticker    = agent["ticker"]
    action    = agent["action"].upper()
    player_id = agent["player_id"]

    snap = _get_ticker_snapshot(ticker)
    price = snap.get("price")
    if not price:
        logger.warning(f"[AgentBuilder] No price for {ticker} — trade aborted")
        return

    qty = agent.get("quantity")
    if not qty:
        try:
            pf    = get_portfolio(player_id)
            cash  = float(pf.get("cash") or 0)
            pos_v = sum(
                float(p.get("qty", 0)) * float(p.get("avg_price", 0))
                for p in (pf.get("positions") or [])
            )
            qty = max(1, int((cash + pos_v) * DEFAULT_SIZE_PCT / price))
        except Exception:
            qty = 1

    # ── Sub-portfolio budget check ────────────────────────────────────────────
    if action == "BUY":
        try:
            from engine.sub_portfolio import check_budget
            trade_value = price * qty
            allowed, budget_reason = check_budget("User Agents", trade_value)
            if not allowed:
                logger.warning(f"[AgentBuilder] Trade BLOCKED — {budget_reason}")
                return
        except Exception as _be:
            logger.debug(f"[AgentBuilder] Budget check skipped: {_be}")

    reasoning = f"[UserAgent:{agent['name']}] {reason}"
    if action == "BUY":
        buy(player_id=player_id, symbol=ticker, price=price,
            qty=float(qty), reasoning=reasoning, confidence=75.0, timeframe="SWING")
    elif action == "SELL":
        sell(player_id=player_id, symbol=ticker, price=price,
             reasoning=reasoning, confidence=75.0)
    logger.info(f"[AgentBuilder] {action} {ticker} x{qty} @ ${price:.2f} for {player_id}")
