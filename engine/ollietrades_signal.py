"""engine/ollietrades_signal.py — OllieTrades Signal: unanimous-consensus alert pipeline.

Phase 1 (ghost book, no execution — OLLIETRADES_SIGNAL_PUSH_ENABLED gates the
only place a real push could happen). Full design: docs/OLLIETRADES_SIGNAL.md.

Pipeline: get_winning_models() -> find_consensus_candidates() -> match_playbook()
-> [confidence + market-hours filter] -> rank_and_cap() -> log_to_ledger() for
every candidate (regardless of whether it was pushed) -> resolve_outcomes()
later fills in win/loss for every logged row.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

from rich.console import Console

console = Console()
DB = "data/trader.db"

# ─── Playbook registry — extensible, never hardcode logic per named strategy ──
# A candidate's (direction, asset_type/option_type, scanner tier) shape maps to
# a playbook key here. Unmatched-but-otherwise-qualifying candidates still log
# to the ledger (status SHOWN-ONLY) with strategy="unmatched" rather than being
# silently dropped -- add a new entry here to start recognizing a new setup.
PLAYBOOK_REGISTRY = {
    "bull_put_spread": {
        "direction": "long",
        "option_types": {"BUY_PUT", "SHORT_PUT", "CSP"},
        "label": "Bull Put Spread",
    },
    "leveraged_put": {
        "direction": "short",
        "option_types": {"BUY_PUT"},
        "label": "Leveraged Put",
    },
    "bear_play": {
        "direction": "short",
        "option_types": {None, "BUY_PUT"},
        "label": "Bear Play",
    },
    "ollie_live_swing": {
        "direction": "long",
        "option_types": {None},
        "requires_scanner_tier": True,
        "label": "Ollie Live Swing",
    },
}


def _conn():
    from engine.db_conn import get_conn
    c = get_conn(DB, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def ensure_ledger_table():
    conn = _conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_ledger (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at            TEXT NOT NULL DEFAULT (datetime('now')),
            symbol                TEXT NOT NULL,
            direction             TEXT NOT NULL,
            strategy              TEXT NOT NULL,
            entry_price           REAL,
            stop_price            REAL,
            target_price          REAL,
            composite_conviction  REAL NOT NULL,
            approving_models_json TEXT NOT NULL,
            dissents_json         TEXT,
            context_json          TEXT,
            gate_config_json      TEXT NOT NULL,
            status                TEXT NOT NULL DEFAULT 'SHOWN-ONLY',
            pushed_at             TEXT,
            trade_id              INTEGER,
            outcome               TEXT,
            outcome_r_multiple    REAL,
            outcome_resolved_at   TEXT,
            outcome_detail_json   TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_ledger_created ON signal_ledger(created_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_ledger_status ON signal_ledger(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_ledger_symbol ON signal_ledger(symbol)")
    conn.commit()
    conn.close()


# ─── Step 1: winning models (dynamic — never hardcoded names) ────────────────

def _fetch_leaderboard_rows() -> list[dict]:
    """Isolated so tests can mock this one function instead of the network.
    Same-process localhost call -- consistent with the existing pattern in
    main.py::run_signal_center_refresh (HM-SIGNAL-CENTER-REFRESH), and
    guarantees this reads the exact same return_pct/total_pnl the dashboard
    displays rather than re-deriving a second copy of that computation."""
    try:
        import requests
        r = requests.get("http://localhost:8080/api/arena/leaderboard", timeout=15)
        if not r.ok:
            return []
        data = r.json()
        return data if isinstance(data, list) else data.get("leaderboard", [])
    except Exception as e:
        console.log(f"[yellow]ollietrades_signal: leaderboard fetch failed: {e}")
        return []


_RATING_ORDER = {"A": 5, "B": 4, "C": 3, "D": 2, "E": 1}


def get_winning_models(min_rating: str = "B", min_trades: int = 20,
                        min_return_pct: float = 0.0) -> list[dict]:
    """Dynamic winning-model roster -- recomputed every gate cycle, never a
    hardcoded name list. A player qualifies iff: halt_mode == 'active' AND
    rating is at least min_rating AND total_trades >= min_trades AND
    return_pct > min_return_pct. Returns [{player_id, display_name, rating,
    rating_score, total_trades, return_pct}, ...]."""
    from engine.agent_ratings import fleet_report_card

    min_rank = _RATING_ORDER.get(min_rating.upper(), 4)
    ratings_by_id = {r["player_id"]: r for r in fleet_report_card()}
    leaderboard = {r.get("player_id"): r for r in _fetch_leaderboard_rows() if r.get("player_id")}

    winners = []
    for pid, rating_row in ratings_by_id.items():
        lb_row = leaderboard.get(pid)
        if lb_row is None:
            continue
        if lb_row.get("halt_mode") != "active":
            continue
        rank = _RATING_ORDER.get(str(rating_row.get("rating", "E")).upper(), 0)
        if rank < min_rank:
            continue
        if (rating_row.get("total_trades") or 0) < min_trades:
            continue
        return_pct = lb_row.get("return_pct")
        if return_pct is None or return_pct <= min_return_pct:
            continue
        winners.append({
            "player_id": pid,
            "display_name": rating_row.get("display_name") or lb_row.get("name") or pid,
            "rating": rating_row.get("rating"),
            "rating_score": rating_row.get("rating_score"),
            "total_trades": rating_row.get("total_trades"),
            "return_pct": return_pct,
        })
    return winners


# ─── Step 2: directional agreement ────────────────────────────────────────────

_LONG_SIGNALS = {"BUY", "BUY_CALL"}
_SHORT_SIGNALS = {"SHORT", "BUY_PUT"}


def _direction_bucket(signal: Optional[str]) -> Optional[str]:
    """Normalize a raw signals.signal value to 'long' / 'short' / None.
    HOLD (and anything unrecognized) is None -- it doesn't break unanimity,
    it just doesn't count as an opinion either way."""
    if not signal:
        return None
    s = signal.upper()
    if s in _LONG_SIGNALS:
        return "long"
    if s in _SHORT_SIGNALS:
        return "short"
    return None


def _recent_signals_by_winner(winning_ids: set[str], lookback_minutes: int) -> dict[str, list[dict]]:
    """{symbol: [{player_id, signal, confidence, option_type, created_at}, ...]}
    for signals from winning models only, within the lookback window."""
    if not winning_ids:
        return {}
    conn = _conn()
    cutoff = (datetime.utcnow() - timedelta(minutes=lookback_minutes)).strftime("%Y-%m-%d %H:%M:%S")
    placeholders = ",".join("?" * len(winning_ids))
    rows = conn.execute(
        f"SELECT player_id, symbol, signal, confidence, option_type, created_at "
        f"FROM signals WHERE player_id IN ({placeholders}) AND created_at >= ? "
        f"ORDER BY created_at DESC",
        [*winning_ids, cutoff],
    ).fetchall()
    conn.close()
    by_symbol: dict[str, list[dict]] = {}
    for r in rows:
        by_symbol.setdefault(r["symbol"], []).append(dict(r))
    return by_symbol


def find_consensus_candidates(min_agreeing_models: int = 2,
                               lookback_minutes: int = 60,
                               winning_models: Optional[list[dict]] = None) -> list[dict]:
    """Every symbol where >= min_agreeing_models currently-winning models have
    a fresh signal AND every one of them agrees on direction (HOLD/unrecognized
    excluded from the count, doesn't break unanimity). Returns candidate dicts
    with the winning-model roster info attached (frozen for the ledger)."""
    if winning_models is None:
        winning_models = get_winning_models()
    winners_by_id = {w["player_id"]: w for w in winning_models}
    signals_by_symbol = _recent_signals_by_winner(set(winners_by_id), lookback_minutes)

    candidates = []
    for symbol, sigs in signals_by_symbol.items():
        directional = [s for s in sigs if _direction_bucket(s["signal"]) is not None]
        if len(directional) < min_agreeing_models:
            continue
        directions = {_direction_bucket(s["signal"]) for s in directional}
        if len(directions) != 1:
            continue  # not unanimous
        direction = directions.pop()

        # One signal per player_id (most recent -- sigs is created_at DESC already)
        seen_players = set()
        approving = []
        for s in directional:
            if s["player_id"] in seen_players:
                continue
            seen_players.add(s["player_id"])
            w = winners_by_id[s["player_id"]]
            approving.append({
                "player_id": s["player_id"],
                "display_name": w["display_name"],
                "action": s["signal"],
                "confidence": s["confidence"],
                "option_type": s["option_type"],
                "rating": w["rating"],
                "rating_score": w["rating_score"],
            })

        total_weight = sum((a["rating_score"] or 0) for a in approving) or 1.0
        composite_conviction = sum(
            (a["confidence"] or 0) * (a["rating_score"] or 0) for a in approving
        ) / total_weight

        candidates.append({
            "symbol": symbol,
            "direction": direction,
            "approving_models": approving,
            "composite_conviction": composite_conviction,
        })
    return candidates


# ─── Step 3: playbook match ───────────────────────────────────────────────────

def match_playbook(candidate: dict, scanner_tiers: Optional[dict] = None) -> Optional[str]:
    """Returns a PLAYBOOK_REGISTRY key, or None if the candidate's shape
    doesn't match any registered playbook (still gets logged, just as
    'unmatched' -- see evaluate_gate)."""
    direction = candidate["direction"]
    option_types = {a.get("option_type") for a in candidate["approving_models"]}
    scanner_tiers = scanner_tiers or {}

    for key, spec in PLAYBOOK_REGISTRY.items():
        if spec["direction"] != direction:
            continue
        if not (option_types & spec["option_types"]):
            continue
        if spec.get("requires_scanner_tier") and candidate["symbol"] not in scanner_tiers:
            continue
        return key
    return None


# ─── Step 4 + 5: confidence/hours filter, ranking, daily cap ─────────────────

def rank_and_cap(candidates: list[dict], max_per_day: int = 3) -> tuple[list[dict], list[dict]]:
    """Ranks by composite_conviction descending; top max_per_day -> (to_push,
    shown_only). If candidates is empty, returns ([], []) -- silence is the
    expected common case, not an error."""
    ranked = sorted(candidates, key=lambda c: c["composite_conviction"], reverse=True)
    return ranked[:max_per_day], ranked[max_per_day:]


# ─── Ledger I/O ────────────────────────────────────────────────────────────────

def log_to_ledger(candidate: dict, status: str, strategy: str, gate_config: dict,
                   entry_price: Optional[float] = None, stop_price: Optional[float] = None,
                   target_price: Optional[float] = None, context: Optional[dict] = None,
                   dissents: Optional[list] = None) -> int:
    """INSERT-only. Never call this to update an existing row -- the frozen
    columns (everything except status/pushed_at/trade_id/outcome*) must stay
    byte-identical for the life of the row (no-repaint rule, docs/
    OLLIETRADES_SIGNAL.md #4)."""
    ensure_ledger_table()
    conn = _conn()
    cur = conn.execute(
        "INSERT INTO signal_ledger "
        "(symbol, direction, strategy, entry_price, stop_price, target_price, "
        " composite_conviction, approving_models_json, dissents_json, context_json, "
        " gate_config_json, status) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            candidate["symbol"], candidate["direction"], strategy,
            entry_price, stop_price, target_price,
            candidate["composite_conviction"],
            json.dumps(candidate["approving_models"]),
            json.dumps(dissents or []),
            json.dumps(context or {}),
            json.dumps(gate_config),
            status,
        ),
    )
    conn.commit()
    row_id = cur.lastrowid
    conn.close()
    return row_id


def get_ledger_row(ledger_id: int) -> Optional[dict]:
    conn = _conn()
    row = conn.execute("SELECT * FROM signal_ledger WHERE id = ?", (ledger_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


# ─── Orchestrator ──────────────────────────────────────────────────────────────

def evaluate_gate(min_rating: str = "B", min_trades: int = 20, min_return_pct: float = 0.0,
                   min_agreeing_models: int = 2, lookback_minutes: int = 60,
                   min_conviction: float = 0.75, max_per_day: int = 3,
                   now: Optional[datetime] = None) -> dict:
    """Full pipeline for one gate cycle. Returns {pushed: [...], shown_only: [...],
    gate_config: {...}} -- caller (run_ollietrades_signal_cycle) is responsible
    for logging every candidate to the ledger and, if OLLIETRADES_SIGNAL_
    PUSH_ENABLED, actually pushing the ones in `pushed`."""
    from engine.market_calendar import is_within_alert_hours

    gate_config = {
        "min_rating": min_rating, "min_trades": min_trades, "min_return_pct": min_return_pct,
        "min_agreeing_models": min_agreeing_models, "lookback_minutes": lookback_minutes,
        "min_conviction": min_conviction, "max_per_day": max_per_day,
    }

    if not is_within_alert_hours(now):
        return {"pushed": [], "shown_only": [], "gate_config": gate_config, "gated_out": "market_hours"}

    winners = get_winning_models(min_rating, min_trades, min_return_pct)
    candidates = find_consensus_candidates(min_agreeing_models, lookback_minutes, winners)
    candidates = [c for c in candidates if c["composite_conviction"] >= min_conviction]

    for c in candidates:
        c["strategy"] = match_playbook(c) or "unmatched"

    to_push, shown_only = rank_and_cap(candidates, max_per_day)
    return {"pushed": to_push, "shown_only": shown_only, "gate_config": gate_config, "gated_out": None}


def run_ollietrades_signal_cycle() -> dict:
    """Scheduled entry point (main.py). Logs every candidate to the ledger;
    only actually pushes (ntfy) if config.OLLIETRADES_SIGNAL_PUSH_ENABLED --
    Phase 1 ships with that False, so every would-be push logs as SHOWN-ONLY
    instead. Never raises -- errors are logged, cycle just produces nothing."""
    try:
        from config import (
            OLLIETRADES_SIGNAL_PUSH_ENABLED, OLLIETRADES_SIGNAL_MIN_RATING,
            OLLIETRADES_SIGNAL_MIN_TRADES, OLLIETRADES_SIGNAL_MIN_RETURN_PCT,
            OLLIETRADES_SIGNAL_MIN_AGREEING_MODELS, OLLIETRADES_SIGNAL_MIN_CONVICTION,
            OLLIETRADES_SIGNAL_MAX_PUSHES_PER_DAY, OLLIETRADES_SIGNAL_LOOKBACK_MINUTES,
        )
    except Exception:
        console.log("[red]ollietrades_signal: config import failed, skipping cycle")
        return {"pushed": 0, "shown_only": 0}

    try:
        result = evaluate_gate(
            min_rating=OLLIETRADES_SIGNAL_MIN_RATING,
            min_trades=OLLIETRADES_SIGNAL_MIN_TRADES,
            min_return_pct=OLLIETRADES_SIGNAL_MIN_RETURN_PCT,
            min_agreeing_models=OLLIETRADES_SIGNAL_MIN_AGREEING_MODELS,
            lookback_minutes=OLLIETRADES_SIGNAL_LOOKBACK_MINUTES,
            min_conviction=OLLIETRADES_SIGNAL_MIN_CONVICTION,
            max_per_day=OLLIETRADES_SIGNAL_MAX_PUSHES_PER_DAY,
        )
    except Exception as e:
        console.log(f"[red]ollietrades_signal: gate evaluation failed: {e}")
        return {"pushed": 0, "shown_only": 0}

    pushed_count = 0
    for c in result["pushed"]:
        status = "SHOWN-ONLY"
        if OLLIETRADES_SIGNAL_PUSH_ENABLED:
            try:
                from engine.alert_channels import send_alert, AlertLevel
                send_alert(
                    f"{c['symbol']} {c['direction'].upper()} — {len(c['approving_models'])} models agree "
                    f"(conviction {c['composite_conviction']:.2f})",
                    level=AlertLevel.RED_ALERT,
                    alert_type="ollietrades_signal",
                    title=f"OllieTrades Signal: {c['symbol']}",
                    source="ollietrades_signal",
                )
                status = "PUSHED"
                pushed_count += 1
            except Exception as e:
                console.log(f"[red]ollietrades_signal: push failed for {c['symbol']}: {e}")
        log_to_ledger(c, status, c["strategy"], result["gate_config"])

    for c in result["shown_only"]:
        log_to_ledger(c, "SHOWN-ONLY", c["strategy"], result["gate_config"])

    if result["pushed"] or result["shown_only"]:
        console.log(
            f"[cyan]ollietrades_signal: {pushed_count} pushed, "
            f"{len(result['shown_only']) + (len(result['pushed']) - pushed_count)} shown-only this cycle"
        )
    return {"pushed": pushed_count, "shown_only": len(result["shown_only"])}
