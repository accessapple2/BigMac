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
    expected common case, not an error.

    NOTE: `max_per_day` here is really "max this call" -- it has no memory of
    earlier calls. That's fine for a one-shot evaluate_gate() call, but once
    task 36 wires this to a repeating scheduler (every 10min during RTH,
    ~39 calls/day), treating it as a per-call cap would let a config named
    MAX_PUSHES_PER_DAY silently allow far more than that many pushes across
    a real day. evaluate_gate() is responsible for narrowing max_per_day to
    the REMAINING daily budget (via _pushed_count_today) before calling this
    -- this function itself stays a pure per-call top-N ranker."""
    ranked = sorted(candidates, key=lambda c: c["composite_conviction"], reverse=True)
    return ranked[:max_per_day], ranked[max_per_day:]


def _pushed_count_today(now: Optional[datetime] = None) -> int:
    """Count of signal_ledger rows already marked PUSHED today (UTC calendar
    day, matching created_at's datetime('now') storage). Used to turn the
    per-call `max_per_day` in rank_and_cap into an actual daily budget across
    repeated scheduler cycles."""
    ensure_ledger_table()
    today = (now or datetime.utcnow()).strftime("%Y-%m-%d")
    conn = _conn()
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM signal_ledger WHERE status = 'PUSHED' AND substr(created_at, 1, 10) = ?",
        (today,),
    ).fetchone()
    conn.close()
    return row["n"] if row else 0


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
    ensure_ledger_table()
    conn = _conn()
    row = conn.execute("SELECT * FROM signal_ledger WHERE id = ?", (ledger_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_ledger_row_decoded(ledger_id: int) -> Optional[dict]:
    """Single-row equivalent of query_ledger's decoding, plus risk_reward
    (task 39, docs/OLLIETRADES_SIGNAL.md §7 -- "/signal/<id> scorecard page").
    Renders directly from the frozen row, no live lookups -- the no-repaint
    rule applies to the render layer too, not just storage."""
    row = get_ledger_row(ledger_id)
    if row is None:
        return None
    d = _decode_ledger_row(row)
    entry, stop, target = d.get("entry_price"), d.get("stop_price"), d.get("target_price")
    if entry is not None and stop is not None and target is not None:
        risk = abs(entry - stop)
        d["risk_reward"] = round(abs(target - entry) / risk, 2) if risk > 0 else None
    else:
        d["risk_reward"] = None
    return d


# ─── Entry/stop/target (task 37 — required for resolve_outcomes to have
# anything to resolve; the design doc left this computation unspecified) ──────

def compute_entry_stop_target(symbol: str, direction: str, stop_pct: float,
                               target_r_multiple: float) -> Optional[tuple[float, float, float]]:
    """Entry = latest 5min bar close (same get_intraday_candles cascade as the
    session-VWAP fix, so this never invents a second price source). Stop/target
    are symmetric % moves off entry -- stop_pct is the fleet's own canonical
    STOP_LOSS_PCT by default (config.py), target is stop_pct * target_r_multiple
    away (a fixed reward:risk, not a data-driven level -- Phase 1 ghost book
    only needs a consistent, direction-aware yardstick for WIN/LOSS, not a
    "real" technical stop). Returns None (never raises) on any data failure --
    the candidate still logs to the ledger with entry_price=None; resolve_
    outcomes() simply has nothing to resolve for that row, which is correct:
    silence/no-signal is not corruption."""
    try:
        from engine.market_data import get_intraday_candles
        candles = get_intraday_candles(symbol, interval="5m", range_="1d")
        if not candles:
            return None
        entry = float(candles[-1]["close"])
        if entry <= 0:
            return None
    except Exception as e:
        console.log(f"[yellow]ollietrades_signal: entry price fetch failed for {symbol}: {e}")
        return None

    stop_distance = entry * stop_pct
    if direction == "long":
        stop = entry - stop_distance
        target = entry + stop_distance * target_r_multiple
    else:  # short
        stop = entry + stop_distance
        target = entry - stop_distance * target_r_multiple
    return round(entry, 4), round(stop, 4), round(target, 4)


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

    # max_per_day is a DAILY budget, not a per-call one -- task 36 wires this
    # to a repeating scheduler, so narrow it by what's already pushed today
    # before ranking, else a 10min cadence across RTH could push far more
    # than max_per_day in a real day (see rank_and_cap's docstring).
    remaining_budget = max(max_per_day - _pushed_count_today(now), 0)
    to_push, shown_only = rank_and_cap(candidates, remaining_budget)
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
            OLLIETRADES_SIGNAL_STOP_PCT, OLLIETRADES_SIGNAL_TARGET_R_MULTIPLE,
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
        entry_stop_target = compute_entry_stop_target(
            c["symbol"], c["direction"], OLLIETRADES_SIGNAL_STOP_PCT, OLLIETRADES_SIGNAL_TARGET_R_MULTIPLE
        )
        entry, stop, target = entry_stop_target if entry_stop_target else (None, None, None)
        log_to_ledger(c, status, c["strategy"], result["gate_config"],
                      entry_price=entry, stop_price=stop, target_price=target)

    for c in result["shown_only"]:
        entry_stop_target = compute_entry_stop_target(
            c["symbol"], c["direction"], OLLIETRADES_SIGNAL_STOP_PCT, OLLIETRADES_SIGNAL_TARGET_R_MULTIPLE
        )
        entry, stop, target = entry_stop_target if entry_stop_target else (None, None, None)
        log_to_ledger(c, "SHOWN-ONLY", c["strategy"], result["gate_config"],
                      entry_price=entry, stop_price=stop, target_price=target)

    if result["pushed"] or result["shown_only"]:
        console.log(
            f"[cyan]ollietrades_signal: {pushed_count} pushed, "
            f"{len(result['shown_only']) + (len(result['pushed']) - pushed_count)} shown-only this cycle"
        )
    return {"pushed": pushed_count, "shown_only": len(result["shown_only"])}


# ─── Outcome Resolution Engine (task 37, docs/OLLIETRADES_SIGNAL.md §5) ───────
# "Every signal that cleared the gate gets a verdict" -- runs identically
# regardless of status (PUSHED/SHOWN-ONLY/TRADED), which is the entire point
# of the regret-meter comparison in §6/§8. Only ever writes outcome*/status/
# pushed_at/trade_id -- never the frozen call-time columns (enforced by
# test_ledger_has_no_update_path_for_frozen_columns).

def _trading_days_elapsed(start: datetime, end: datetime) -> int:
    """Count NYSE trading days strictly between start.date() and end.date()
    (exclusive of start day, inclusive of end day) -- sessions, not wall-clock
    (established doctrine: count market sessions, not calendar days, same
    reasoning as engine.market_calendar.is_trading_day's other callers)."""
    from engine.market_calendar import is_trading_day
    if end <= start:
        return 0
    d = start.date()
    end_date = end.date()
    count = 0
    while d < end_date:
        d = d + timedelta(days=1)
        if is_trading_day(d):
            count += 1
    return count


def _parse_candle_time(t: str) -> datetime:
    return datetime.fromisoformat(t.rstrip("Z"))


def _resolve_one_row(row: dict, now: datetime, resolution_window_days: int) -> tuple:
    """Returns (outcome, r_multiple, detail_dict) or (None, None, None) if
    still pending (not yet resolvable -- leave outcome NULL, try again next
    cycle). Walk-forward tie-break within a single candle: if both stop AND
    target fall inside that candle's [low, high] range, STOP is assumed hit
    first -- the conservative assumption every backtest simulator without
    tick data has to make, documented rather than silently optimistic."""
    from engine.market_data import get_intraday_candles

    symbol = row["symbol"]
    direction = row["direction"]
    entry = row["entry_price"]
    stop = row["stop_price"]
    target = row["target_price"]
    created_at = datetime.strptime(row["created_at"][:19], "%Y-%m-%d %H:%M:%S")

    stop_distance = abs(entry - stop)
    if stop_distance <= 0:
        return "EXPIRED_UNRESOLVED", None, {"reason": "zero_stop_distance"}

    range_ = "1mo" if resolution_window_days <= 15 else "3mo"
    try:
        candles = get_intraday_candles(symbol, interval="5m", range_=range_)
    except Exception as e:
        console.log(f"[yellow]ollietrades_signal: resolve candle fetch failed for {symbol}: {e}")
        return None, None, None
    if not candles:
        return None, None, None

    forward = sorted(
        (c for c in candles if _parse_candle_time(c["time"]) >= created_at),
        key=lambda c: c["time"],
    )

    for c in forward:
        lo, hi = float(c["low"]), float(c["high"])
        if direction == "long":
            if lo <= stop:
                move = stop - entry
                return "LOSS", round(move / stop_distance, 3), {
                    "hit": "stop", "hit_price": stop, "hit_time": c["time"],
                }
            if hi >= target:
                move = target - entry
                return "WIN", round(move / stop_distance, 3), {
                    "hit": "target", "hit_price": target, "hit_time": c["time"],
                }
        else:  # short
            if hi >= stop:
                move = entry - stop
                return "LOSS", round(move / stop_distance, 3), {
                    "hit": "stop", "hit_price": stop, "hit_time": c["time"],
                }
            if lo <= target:
                move = entry - target
                return "WIN", round(move / stop_distance, 3), {
                    "hit": "target", "hit_price": target, "hit_time": c["time"],
                }

    if _trading_days_elapsed(created_at, now) >= resolution_window_days:
        last_close = float(forward[-1]["close"]) if forward else entry
        move = (last_close - entry) if direction == "long" else (entry - last_close)
        return "EXPIRED_UNRESOLVED", round(move / stop_distance, 3), {
            "reason": "resolution_window_elapsed", "last_close": last_close,
        }

    return None, None, None  # still pending, neither hit nor expired yet


def _write_outcome(ledger_id: int, outcome: str, r_multiple: Optional[float], detail: dict) -> None:
    """The ONLY function in this module that UPDATEs signal_ledger -- and only
    ever touches outcome/outcome_r_multiple/outcome_resolved_at/outcome_detail_json.
    Never the frozen columns (no-repaint rule, docs/OLLIETRADES_SIGNAL.md #4)."""
    conn = _conn()
    conn.execute(
        "UPDATE signal_ledger SET outcome = ?, outcome_r_multiple = ?, "
        "outcome_resolved_at = datetime('now'), outcome_detail_json = ? WHERE id = ?",
        (outcome, r_multiple, json.dumps(detail), ledger_id),
    )
    conn.commit()
    conn.close()


def resolve_outcomes(now: Optional[datetime] = None, resolution_window_days: Optional[int] = None) -> dict:
    """Scheduled entry point (main.py, every 15min market hours per design
    doc). For every signal_ledger row with outcome IS NULL and entry_price
    set, resolve WIN/LOSS/EXPIRED_UNRESOLVED. Never raises -- a single row's
    resolution failure is logged and skipped, not fatal to the batch."""
    if resolution_window_days is None:
        try:
            from config import OLLIETRADES_SIGNAL_RESOLUTION_WINDOW_DAYS
            resolution_window_days = OLLIETRADES_SIGNAL_RESOLUTION_WINDOW_DAYS
        except Exception:
            resolution_window_days = 5
    now = now or datetime.utcnow()

    ensure_ledger_table()
    conn = _conn()
    rows = conn.execute(
        "SELECT id, symbol, direction, created_at, entry_price, stop_price, target_price "
        "FROM signal_ledger WHERE outcome IS NULL AND entry_price IS NOT NULL"
    ).fetchall()
    conn.close()

    tally = {"WIN": 0, "LOSS": 0, "EXPIRED_UNRESOLVED": 0, "pending": 0, "errors": 0}
    for row in rows:
        try:
            outcome, r_multiple, detail = _resolve_one_row(dict(row), now, resolution_window_days)
        except Exception as e:
            console.log(f"[yellow]ollietrades_signal: resolve failed for ledger id {row['id']}: {e}")
            tally["errors"] += 1
            continue
        if outcome is None:
            tally["pending"] += 1
            continue
        _write_outcome(row["id"], outcome, r_multiple, detail)
        tally[outcome] += 1

    if tally["WIN"] or tally["LOSS"] or tally["EXPIRED_UNRESOLVED"]:
        console.log(
            f"[cyan]ollietrades_signal: resolved {tally['WIN']} WIN, {tally['LOSS']} LOSS, "
            f"{tally['EXPIRED_UNRESOLVED']} expired ({tally['pending']} still pending)"
        )
    return tally


# ─── /signals/history — Ledger Page (task 38, docs/OLLIETRADES_SIGNAL.md §6) ──

_JSON_FIELDS = ("approving_models_json", "dissents_json", "context_json",
                "gate_config_json", "outcome_detail_json")


def _decode_ledger_row(row: dict) -> dict:
    """SELECT * -> plain dict with the frozen/outcome JSON blob columns
    decoded into their `_json` suffix's stripped name, and the raw *_json
    column kept alongside (callers that just want to render don't need to
    know the storage format; callers doing a byte-identical check still can)."""
    d = dict(row)
    d["approving_models"] = json.loads(d.get("approving_models_json") or "[]")
    d["dissents"] = json.loads(d.get("dissents_json") or "[]")
    d["context"] = json.loads(d.get("context_json") or "{}")
    d["gate_config"] = json.loads(d.get("gate_config_json") or "{}")
    d["outcome_detail"] = json.loads(d["outcome_detail_json"]) if d.get("outcome_detail_json") else None
    return d


def query_ledger(from_date: Optional[str] = None, to_date: Optional[str] = None,
                  strategy: Optional[str] = None, status: Optional[str] = None,
                  model: Optional[str] = None, limit: int = 200) -> list[dict]:
    """Every signal_ledger row, newest first, JSON columns decoded. `model`
    filters on approving_models_json membership -- not a SQL column, so it's
    applied in Python after fetch (fine at ghost-phase volume, same reasoning
    as get_notifications' stream filter). from_date/to_date compare directly
    against created_at's 'YYYY-MM-DD HH:MM:SS' text format (lexicographic
    compare is correct for this fixed-width format -- same convention as
    _pushed_count_today's substr match)."""
    ensure_ledger_table()
    conn = _conn()
    where = []
    params: list = []
    if from_date:
        where.append("created_at >= ?")
        params.append(from_date)
    if to_date:
        where.append("created_at <= ?")
        params.append(to_date)
    if strategy:
        where.append("strategy = ?")
        params.append(strategy)
    if status:
        where.append("status = ?")
        params.append(status)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    # model filtering happens post-fetch, so fetch a wider window when it's
    # active -- same starvation fix as get_notifications' stream filter.
    sql_limit = limit * 10 if model else limit
    rows = conn.execute(
        f"SELECT * FROM signal_ledger {clause} ORDER BY created_at DESC LIMIT ?",
        (*params, sql_limit),
    ).fetchall()
    conn.close()

    result = []
    for r in rows:
        d = _decode_ledger_row(dict(r))
        if model and not any(a.get("player_id") == model for a in d["approving_models"]):
            continue
        result.append(d)
        if len(result) >= limit:
            break
    return result


_RESOLVED_OUTCOMES = ("WIN", "LOSS")


def _win_rate(rows: list[dict]) -> tuple[Optional[float], int]:
    """(win_rate, n_resolved). EXPIRED_UNRESOLVED and pending (outcome NULL)
    rows are excluded from both numerator and denominator -- they're
    inconclusive, not losses; counting them as losses would understate a
    genuinely-undecided call. n_resolved==0 -> (None, 0), not division by
    zero or a misleading 0.0."""
    resolved = [r for r in rows if r.get("outcome") in _RESOLVED_OUTCOMES]
    if not resolved:
        return None, 0
    wins = sum(1 for r in resolved if r["outcome"] == "WIN")
    return wins / len(resolved), len(resolved)


def compute_rollup(rows: Optional[list[dict]] = None, from_date: Optional[str] = None,
                    to_date: Optional[str] = None, strategy: Optional[str] = None,
                    status: Optional[str] = None, model: Optional[str] = None) -> dict:
    """Server-side rollups for /signals/history (design doc §6): overall WR,
    WR by strategy, WR by approving-model combination, avg R multiple, the
    "regret meter" (WR(SHOWN-ONLY|SKIPPED-BY-OWNER) - WR(TRADED), signed so
    positive means winners are being left on the table), pushes/day, current
    streak. Pass `rows` directly (e.g. already-fetched from query_ledger) to
    avoid a second DB round-trip; otherwise this fetches with the given
    filters itself (unbounded limit -- rollups need the full filtered set,
    not a page of it)."""
    if rows is None:
        rows = query_ledger(from_date=from_date, to_date=to_date, strategy=strategy,
                             status=status, model=model, limit=1_000_000)

    overall_wr, overall_n = _win_rate(rows)

    by_strategy: dict = {}
    for r in rows:
        by_strategy.setdefault(r["strategy"], []).append(r)
    wr_by_strategy = {}
    for key, group in by_strategy.items():
        wr, n = _win_rate(group)
        wr_by_strategy[key] = {"wr": wr, "n": n, "total": len(group)}

    by_model_combo: dict = {}
    for r in rows:
        combo = tuple(sorted(a.get("player_id") for a in r.get("approving_models", []) if a.get("player_id")))
        by_model_combo.setdefault(combo, []).append(r)
    wr_by_model_combo = {}
    for combo, group in by_model_combo.items():
        wr, n = _win_rate(group)
        wr_by_model_combo["+".join(combo) or "(none)"] = {"wr": wr, "n": n, "total": len(group)}

    r_multiples = [r["outcome_r_multiple"] for r in rows
                   if r.get("outcome") is not None and r.get("outcome_r_multiple") is not None]
    avg_r_multiple = sum(r_multiples) / len(r_multiples) if r_multiples else None

    traded = [r for r in rows if r["status"] == "TRADED"]
    skipped = [r for r in rows if r["status"] in ("SHOWN-ONLY", "SKIPPED-BY-OWNER")]
    wr_traded, n_traded = _win_rate(traded)
    wr_skipped, n_skipped = _win_rate(skipped)
    regret_meter = (wr_skipped - wr_traded) if (wr_traded is not None and wr_skipped is not None) else None

    pushed_n = sum(1 for r in rows if r["status"] == "PUSHED")
    distinct_days = {r["created_at"][:10] for r in rows if r.get("created_at")}
    pushes_per_day = (pushed_n / len(distinct_days)) if distinct_days else None

    # Current streak: rows are newest-first (query_ledger orders DESC).
    # Pending/expired rows are skipped, not streak-breaking -- an
    # undecided call shouldn't reset a real win/loss streak.
    streak_type, streak_count = None, 0
    for r in rows:
        if r.get("outcome") not in _RESOLVED_OUTCOMES:
            continue
        if streak_type is None:
            streak_type, streak_count = r["outcome"], 1
        elif r["outcome"] == streak_type:
            streak_count += 1
        else:
            break

    return {
        "overall_wr": overall_wr, "overall_n": overall_n,
        "wr_by_strategy": wr_by_strategy,
        "wr_by_model_combo": wr_by_model_combo,
        "avg_r_multiple": avg_r_multiple,
        "wr_traded": wr_traded, "n_traded": n_traded,
        "wr_skipped": wr_skipped, "n_skipped": n_skipped,
        "regret_meter": regret_meter,
        "pushes_per_day": pushes_per_day,
        "current_streak": {"type": streak_type, "count": streak_count} if streak_type else None,
        "total_signals": len(rows),
    }
