"""HM-ARCHER-REBUILD — Intel source layer.

One reader per live surface. Each returns normalized data or empty.
All failures are LOGGED (no silent `except: pass` — the old Archer's sin).

Shapes verified live 2026-06-05 (Phase 2):
  /api/regime        -> regime, vix, spy_price, spy_above_200, allocation
  /api/gex-snapshot  -> {data:{SPY:{...},QQQ:{...}}, observation_only}  (INDEX ONLY)
  /api/uhura/signal  -> flow_bias, conviction, tickers_flagged, suggested_ticker, ...
  /api/cockpit/snapshot -> regime, fleet_summary, last_decisions, bus, ...
  war_room(trader.db)         -> id, player_id, symbol, take, created_at, strategy_mode
  deep_scan_results(trader.db)-> scan_date, symbol, strategy_name, confidence, ...
  trade_signals(signals.db)   -> agent_name, symbol, w3_strategy_tag, w2_bracket_tier, ...
  congress -> congress_tracker.get_congressional_trades() (in-memory scraper, NO table)
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import requests

from engine import ticker_names as _names  # FIX-4: verified ticker→company name

logger = logging.getLogger(__name__)

# Resolve DB paths relative to repo root (engine/archer/intel_sources.py -> repo)
_ROOT = Path(__file__).resolve().parent.parent.parent
TRADER_DB = _ROOT / "data" / "trader.db"
SIGNALS_DB = _ROOT / "signal-center" / "signals.db"
BRIDGE = "http://127.0.0.1:8080"


def _ro(db: Path) -> sqlite3.Connection:
    """Read-only sqlite connection (Archer never writes via intel layer)."""
    return sqlite3.connect(f"file:{db}?mode=ro", uri=True)


# --------------------------------------------------------------------------
# HTTP surfaces
# --------------------------------------------------------------------------
def get_regime() -> dict:
    """Market regime: {regime, vix, spy_price, spy_above_200, allocation}."""
    try:
        r = requests.get(f"{BRIDGE}/api/regime", timeout=5)
        return r.json() if r.ok else {}
    except Exception as e:
        logger.warning("[Archer/intel] regime failed: %s: %r", type(e).__name__, e)
        return {}


def get_gex() -> dict:
    """SPY/QQQ gamma snapshot. INDEX-LEVEL ONLY (no per-symbol list).

    Returns {SPY:{...}, QQQ:{...}, observation_only:bool}. Per-index dict keeps
    the fields useful for convergence/briefing. NOTE: gamma state is read from
    the `regime` LABEL string, never the sign of total_gex (they can disagree).
    """
    try:
        r = requests.get(f"{BRIDGE}/api/gex-snapshot", timeout=6)
        if not r.ok:
            return {}
        raw = r.json()
        data = raw.get("data", {}) or {}
        out: dict = {"observation_only": raw.get("observation_only", True)}
        for idx in ("SPY", "QQQ"):
            d = data.get(idx) or {}
            if not d:
                continue
            out[idx] = {
                "spot": d.get("spot"),
                "regime": d.get("regime", ""),          # e.g. "LONG GAMMA · stable (...)"
                "total_gex": d.get("total_gex"),
                "gamma_flip": d.get("gamma_flip"),
                "call_wall": d.get("call_wall"),
                "put_wall": d.get("put_wall"),
                "king_node": d.get("king_node"),
            }
        return out
    except Exception as e:
        logger.warning("[Archer/intel] gex failed: %s: %r", type(e).__name__, e)
        return {}


def get_uhura() -> dict:
    """Uhura v2 options-flow / GEX confluence (the per-symbol options surface).

    Returns the live signal dict incl. tickers_flagged, suggested_ticker,
    flow_bias, conviction, gamma_regime, signal_votes, reasoning.
    """
    try:
        r = requests.get(f"{BRIDGE}/api/uhura/signal", timeout=6)
        if not r.ok:
            return {}
        d = r.json()
        return {
            "flow_bias": d.get("flow_bias"),
            "conviction": d.get("conviction"),
            "recommended_trade": d.get("recommended_trade"),
            "gamma_regime": d.get("gamma_regime"),
            "tickers_flagged": d.get("tickers_flagged") or [],
            "suggested_ticker": d.get("suggested_ticker"),
            "suggested_direction": d.get("suggested_direction"),
            "reasoning": d.get("reasoning"),
            "regime_context": d.get("regime_context"),
            "signal_votes": d.get("signal_votes") or [],
            "aligned_signals": d.get("aligned_signals"),
            "total_signals": d.get("total_signals"),
        }
    except Exception as e:
        logger.warning("[Archer/intel] uhura failed: %s: %r", type(e).__name__, e)
        return {}


def get_cockpit() -> dict:
    """Cockpit snapshot — fleet/regime/bus context for the morning briefing."""
    try:
        r = requests.get(f"{BRIDGE}/api/cockpit/snapshot", timeout=6)
        if not r.ok:
            return {}
        d = r.json()
        return {
            "regime": d.get("regime", {}),
            "fleet_summary": d.get("fleet_summary", {}),
            "wr_heartbeat": d.get("wr_heartbeat", {}),
            "kill_switch": d.get("kill_switch", {}),
            "trade_desk_today": d.get("trade_desk_today", {}),
            "bus": d.get("bus", {}),
            "last_decisions": d.get("last_decisions", [])[:10],
        }
    except Exception as e:
        logger.warning("[Archer/intel] cockpit failed: %s: %r", type(e).__name__, e)
        return {}


# --------------------------------------------------------------------------
# DB surfaces
# --------------------------------------------------------------------------
def get_crew_consensus() -> list[dict]:
    """Recent War Room takes (last 24h), most-recent first."""
    try:
        conn = _ro(TRADER_DB)
        rows = conn.execute(
            """
            SELECT symbol, take, created_at, player_id
            FROM war_room
            WHERE created_at >= datetime('now', '-1 day')
              AND symbol IS NOT NULL AND symbol != ''
            ORDER BY created_at DESC LIMIT 50
            """
        ).fetchall()
        conn.close()
        return _names.annotate(
            [{"symbol": r[0], "take": r[1], "ts": r[2], "player": r[3]} for r in rows]
        )
    except Exception as e:
        logger.warning("[Archer/intel] crew failed: %s: %r", type(e).__name__, e)
        return []


def get_ollie_scanner() -> list[dict]:
    """Ollie AI scanner convergence — symbols flagged by N strategies (last 2d)."""
    try:
        conn = _ro(TRADER_DB)
        rows = conn.execute(
            """
            SELECT symbol, COUNT(DISTINCT strategy_name) AS strat_count,
                   MAX(confidence) AS max_conf
            FROM deep_scan_results
            WHERE scan_date >= date('now', '-2 day')
              AND symbol IS NOT NULL AND symbol != ''
            GROUP BY symbol
            ORDER BY strat_count DESC, max_conf DESC LIMIT 25
            """
        ).fetchall()
        conn.close()
        return _names.annotate(
            [{"symbol": r[0], "signals": r[1], "max_conf": r[2]} for r in rows]
        )
    except Exception as e:
        logger.warning("[Archer/intel] ollie scanner failed: %s: %r", type(e).__name__, e)
        return []


def get_supermax_edges() -> list[dict]:
    """SUPER_MAX forward-scored shadow edges (last 24h)."""
    try:
        conn = _ro(SIGNALS_DB)
        rows = conn.execute(
            """
            SELECT symbol, agent_name, w3_strategy_tag, w2_bracket_tier, created_at
            FROM trade_signals
            WHERE agent_name LIKE 'shadow-bridge%'
              AND created_at >= datetime('now', '-1 day')
            ORDER BY created_at DESC LIMIT 30
            """
        ).fetchall()
        conn.close()
        return _names.annotate([
            {"symbol": r[0], "agent": r[1], "tag": r[2], "tier": r[3], "ts": r[4]}
            for r in rows
        ])
    except Exception as e:
        logger.warning("[Archer/intel] supermax failed: %s: %r", type(e).__name__, e)
        return []


def get_short_signals() -> list[dict]:
    """Sell-the-news shadow shorts (RED-tier trigger source).

    Dormant until earnings season by design (shadow engine flipped on
    2026-06-05; emits 0 until sell-the-news fires). Returns [] gracefully.
    """
    try:
        conn = _ro(SIGNALS_DB)
        rows = conn.execute(
            """
            SELECT symbol, entry_price, stop_loss, take_profit, reasoning, created_at
            FROM trade_signals
            WHERE agent_name = 'shadow-bridge:sell_the_news'
              AND created_at >= datetime('now', '-1 day')
            ORDER BY created_at DESC LIMIT 10
            """
        ).fetchall()
        conn.close()
        return _names.annotate([
            {"symbol": r[0], "entry": r[1], "stop": r[2], "target": r[3],
             "why": r[4], "ts": r[5]}
            for r in rows
        ])
    except Exception as e:
        logger.warning("[Archer/intel] shorts failed: %s: %r", type(e).__name__, e)
        return []


def get_congress() -> list[dict]:
    """Congressional trades via the canonical in-memory scraper (NO DB table).

    Degrades gracefully to [] — currently empty because the scrapling
    'Adaptor' import is broken on Py3.14 + QUIVER_ENABLED=False
    (tracked: HM-CONGRESS-SCRAPER-REPAIR). Auto-contributes once the feed
    flows again — no code change needed here.
    """
    try:
        from engine.congress_tracker import get_congressional_trades
        d = get_congressional_trades() or {}
        trades = d.get("trades", []) or []
        return _names.annotate([
            {
                "symbol": t.get("ticker", ""),
                "who": t.get("politician", "Unknown"),
                "action": t.get("transaction", ""),
                "amt": t.get("amount_range", ""),
                "date": t.get("transaction_date", ""),
            }
            for t in trades
            if t.get("ticker")
        ])
    except Exception as e:
        logger.warning("[Archer/intel] congress failed: %s: %r", type(e).__name__, e)
        return []


def get_bridge_consensus() -> dict:
    """Latest daily Bridge Vote — market-direction consensus on SPY. NO per-symbol.
    Stored in bridge_consensus (trader.db) — DISTINCT from war_room crew takes and
    from the shadow-bridge signal emitter."""
    try:
        conn = _ro(TRADER_DB)
        row = conn.execute(
            """SELECT session_date, consensus_vote, buy_votes, sell_votes, hold_votes,
                      total_voters, conviction, avg_confidence, briefing_summary
               FROM bridge_consensus ORDER BY id DESC LIMIT 1"""
        ).fetchone()
        conn.close()
        if not row:
            return {}
        return {
            "session_date": row[0], "consensus_vote": row[1], "buy": row[2],
            "sell": row[3], "hold": row[4], "total_voters": row[5],
            "conviction": row[6], "avg_confidence": row[7], "briefing": row[8],
        }
    except Exception as e:
        logger.warning("[Archer/intel] bridge consensus failed: %s: %r", type(e).__name__, e)
        return {}


def get_crew_dissent() -> dict:
    """HM-CREW-DISSENT: compact contrarian track record for Archer's prompt.

    Returns {top_contrarian: {dissenter, accuracy, count} | None, pending,
    recent: [{symbol, dissenter, call, correct}]}. Capped — top contrarian only
    (30d, min 5 resolved dissents) + a couple of recent dissents. Empty until
    dissents accrue + resolve."""
    out = {"top_contrarian": None, "pending": 0, "recent": []}
    try:
        conn = _ro(TRADER_DB)
        try:
            has = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='crew_dissent_log'"
            ).fetchone()
            if not has:
                return out
            out["pending"] = conn.execute(
                "SELECT COUNT(*) FROM crew_dissent_log WHERE dissenter_correct IS NULL"
            ).fetchone()[0]
            top = conn.execute(
                "SELECT dissenter, accuracy, dissent_count FROM crew_dissent_stats "
                "WHERE window_days=30 AND dissent_count>=5 AND accuracy IS NOT NULL "
                "ORDER BY accuracy DESC LIMIT 1"
            ).fetchone()
            if top:
                out["top_contrarian"] = {"dissenter": top[0], "accuracy": top[1], "count": top[2]}
            out["recent"] = [
                {"symbol": r[0], "dissenter": r[1], "call": r[2], "correct": r[3]}
                for r in conn.execute(
                    "SELECT symbol, dissenter, dissenter_call, dissenter_correct "
                    "FROM crew_dissent_log ORDER BY id DESC LIMIT 3"
                ).fetchall()
            ]
        finally:
            conn.close()
    except Exception as e:
        logger.warning("[Archer/intel] crew dissent failed: %s: %r", type(e).__name__, e)
    return out
