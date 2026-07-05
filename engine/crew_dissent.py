"""HM-CREW-DISSENT — crew-dissent eval pipeline.

When the bridge officers (Spock / Data / Uhura) disagree on a ticker, log the
dissenter(s), then later resolve whether the dissenter was RIGHT against the
realized 5-day outcome (signals.db scored_predictions). Builds a per-officer
contrarian track record.

ADVISORY ONLY — this is a measurement/reporting pipeline. No order path, never
executes a trade. Tables live in data/trader.db; outcomes read from
signal-center/signals.db (read-only ATTACH).

Source: build_consensus() (engine/consensus.py) per-ticker spock/data/uhura
stances. Wired in engine/riker_xo.py right after build_consensus() is computed.
No bare except:pass — every failure is logged with type+repr.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Optional

logger = logging.getLogger("crew_dissent")

_ROOT = Path(__file__).resolve().parent.parent
TRADER_DB = _ROOT / "data" / "trader.db"
SIGNALS_DB = _ROOT / "signal-center" / "signals.db"

HORIZON_DAYS = 5  # resolution horizon (matches the W0 forward-scoring window)

# Officer raw action (from build_consensus stances) -> directional call.
_ACTION_TO_CALL = {
    "BUY": "BULL", "ADD": "BULL",
    "SELL": "BEAR", "CLOSE": "BEAR", "TRIM": "BEAR",
    "HOLD": "HOLD",
}

# Officer display names, in the order build_consensus exposes them per ticker.
_OFFICERS = ("Spock", "Data", "Uhura", "Q")  # HM-Q-WARROOM: Q (Grok) is a tracked dissenter


def _today_str() -> str:
    """Current AZ date 'YYYY-MM-DD' (matches DATE(entry_date) in scored_predictions)."""
    try:
        from engine.market_calendar import az_now
        return az_now().strftime("%Y-%m-%d")
    except Exception as e:
        logger.warning("[crew_dissent] az_now unavailable (%s: %r) — falling back to UTC date",
                       type(e).__name__, e)
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _ensure_tables() -> None:
    conn = sqlite3.connect(str(TRADER_DB), timeout=15)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS crew_dissent_log (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol            TEXT    NOT NULL,
                dissent_date      TEXT    NOT NULL,
                dissenter         TEXT    NOT NULL,
                dissenter_call    TEXT    NOT NULL,          -- BULL/BEAR/HOLD
                consensus_call    TEXT,
                consensus_size    INTEGER,
                total_voters      INTEGER,
                dissent_magnitude TEXT,                      -- MINOR/SPLIT/OUTLIER
                outcome_r         REAL,
                dissenter_correct INTEGER,                   -- 1 / 0 / NULL (unresolved)
                resolved_at       TEXT,
                created_at        TEXT DEFAULT (datetime('now')),
                UNIQUE(symbol, dissent_date, dissenter)
            )
            """
        )
        # HM-DISSENT-PRICE-RESOLVER-2026-07-05: outcome_basis distinguishes
        # rows resolved via the original scored_predictions r_multiple path
        # ('r_multiple') from rows resolved via the new price-return fallback
        # ('price_pct', see resolve_dissent_outcomes). These are DIFFERENT
        # scales (risk-normalized R vs raw % return) — never silently reuse
        # outcome_r's existing semantics for the new path, or every future
        # consumer of this column breaks invisibly. NULL = legacy row
        # resolved before this column existed with unknown basis (none
        # exist yet in production; all 22 pending rows as of 2026-07-05
        # were never resolved under the old path).
        try:
            conn.execute("ALTER TABLE crew_dissent_log ADD COLUMN outcome_basis TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS crew_dissent_stats (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                dissenter          TEXT    NOT NULL,
                window_days        INTEGER NOT NULL,         -- 30 / 90
                dissent_count      INTEGER,
                correct_count      INTEGER,
                accuracy           REAL,
                avg_r_when_correct REAL,
                avg_r_when_wrong   REAL,
                last_computed      TEXT,
                UNIQUE(dissenter, window_days)
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def _magnitude(consensus_size: int, total_voters: int) -> str:
    """MINOR = exactly 1 dissenter; SPLIT = even split (no true majority);
    OUTLIER = consensus is a lone plurality (everyone scattered, size==1)."""
    n_dissent = total_voters - consensus_size
    if total_voters >= 2 and consensus_size * 2 == total_voters:
        return "SPLIT"
    if n_dissent == 1:
        return "MINOR"
    if consensus_size == 1:
        return "OUTLIER"
    return "SPLIT"


def _save_notification(title: str, body: str, severity: str, notif_type: str, icon: str) -> None:
    """Write a dashboard notification (trader.db) so Archer announces it on the
    frontend. 5-min title+body dedup mirrors the canonical helper. Never raises."""
    try:
        conn = sqlite3.connect(str(TRADER_DB), timeout=10)
        try:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS notifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                    type TEXT, severity TEXT, title TEXT, body TEXT,
                    icon TEXT, agent_id TEXT, acknowledged INTEGER DEFAULT 0)"""
            )
            exists = conn.execute(
                "SELECT id FROM notifications WHERE title=? AND body=? "
                "AND timestamp >= datetime('now','-5 minutes')",
                (title, body),
            ).fetchone()
            if not exists:
                conn.execute(
                    "INSERT INTO notifications (type, severity, title, body, icon) VALUES (?,?,?,?,?)",
                    (notif_type, severity, title, body, icon),
                )
                conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("[crew_dissent] notify failed: %s: %r", type(e).__name__, e)


def _dissenter_accuracy_30d(dissenter: str) -> Optional[float]:
    """Latest 30d dissent accuracy (0..1) for an officer, or None if not computed yet."""
    try:
        conn = sqlite3.connect(str(TRADER_DB), timeout=10)
        try:
            row = conn.execute(
                "SELECT accuracy FROM crew_dissent_stats WHERE dissenter=? AND window_days=30",
                (dissenter,),
            ).fetchone()
        finally:
            conn.close()
        return float(row[0]) if row and row[0] is not None else None
    except Exception:
        return None


def log_dissents(consensus_result: dict) -> dict:
    """For each ticker, map the 3 officers' stances to BULL/BEAR/HOLD, find the
    majority, and INSERT OR IGNORE a dissent row for each officer that disagrees.

    Returns {tickers_examined, dissents_logged, skipped_unanimous, skipped_thin}.
    """
    stats = {"tickers_examined": 0, "dissents_logged": 0,
             "skipped_unanimous": 0, "skipped_thin": 0}
    if not isinstance(consensus_result, dict):
        logger.warning("[crew_dissent] log_dissents got non-dict (%s) — skip", type(consensus_result).__name__)
        return stats

    tickers = consensus_result.get("tickers") or {}
    if not tickers:
        return stats

    try:
        _ensure_tables()
    except Exception as e:
        logger.warning("[crew_dissent] ensure_tables failed: %s: %r", type(e).__name__, e)
        return stats

    dissent_date = _today_str()
    # Notable NEW dissents to announce via Archer (collected here, emitted AFTER the
    # write txn closes so the notification INSERT doesn't contend on the lock).
    _to_announce: list[tuple] = []
    conn = sqlite3.connect(str(TRADER_DB), timeout=15)
    try:
        for symbol, data in tickers.items():
            if not isinstance(data, dict):
                continue
            stats["tickers_examined"] += 1

            # Map each present officer's action to a directional call.
            calls: dict[str, str] = {}
            for officer, key in zip(_OFFICERS, ("spock", "data", "uhura", "grok")):
                stance = data.get(key)
                if not stance:
                    continue
                act = (stance.get("action") or "").upper()
                call = _ACTION_TO_CALL.get(act)
                if call:
                    calls[officer] = call

            total_voters = len(calls)
            if total_voters < 2:
                stats["skipped_thin"] += 1
                continue

            # Majority call.
            counts: dict[str, int] = {}
            for c in calls.values():
                counts[c] = counts.get(c, 0) + 1
            consensus_call = max(counts, key=counts.get)
            consensus_size = counts[consensus_call]

            if consensus_size == total_voters:
                stats["skipped_unanimous"] += 1
                continue

            magnitude = _magnitude(consensus_size, total_voters)
            sym = (symbol or "").strip().upper()
            if not sym:
                continue

            for officer, call in calls.items():
                if call == consensus_call:
                    continue
                try:
                    cur = conn.execute(
                        """INSERT OR IGNORE INTO crew_dissent_log
                           (symbol, dissent_date, dissenter, dissenter_call,
                            consensus_call, consensus_size, total_voters, dissent_magnitude)
                           VALUES (?,?,?,?,?,?,?,?)""",
                        (sym, dissent_date, officer, call,
                         consensus_call, consensus_size, total_voters, magnitude),
                    )
                    if cur.rowcount:
                        stats["dissents_logged"] += 1
                        # Announce only NOTABLE new splits (OUTLIER/SPLIT) — routine
                        # MINOR 2-1 dissents log silently to avoid TTS storms.
                        if magnitude in ("OUTLIER", "SPLIT"):
                            _to_announce.append((sym, officer, call))
                except Exception as e:
                    logger.warning("[crew_dissent] insert failed %s/%s/%s: %s: %r",
                                   sym, dissent_date, officer, type(e).__name__, e)
        conn.commit()
    finally:
        conn.close()

    # Emit one dashboard notification per notable new dissent (after the txn closed).
    # type='dissent' → frontend routes to archerAnnounce → speaks + captions + logs.
    for sym, officer, call in _to_announce:
        acc = _dissenter_accuracy_30d(officer)
        acc_txt = f", {round(acc * 100)}% dissent accuracy" if acc is not None else ""
        body = f"Crew split on {sym} — {officer} dissenting {call}{acc_txt}."
        _save_notification(
            title=f"⚖️ Crew Dissent — {sym}", body=body,
            severity="info", notif_type="dissent", icon="⚖️",
        )

    return stats


def _prefetch_bars_by_symbol(rows: list) -> dict:
    """HM-DISSENT-PRICE-RESOLVER-2026-07-05: one Polygon call per SYMBOL,
    not per row and not per (symbol, dissent_date) — several pending rows
    share a symbol at different dissent_dates (e.g. AVGO appears 7x across
    a 2-week span), so the fetch window per symbol spans
    [earliest dissent_date for that symbol, today], wide enough to cover
    every one of that symbol's rows in a single call. Respects
    DAILY_API_BUDGET by construction (N symbols, not N rows, worth of calls).

    Returns {symbol: bars_list_or_None}. bars is Polygon's day-timespan
    list (see PolygonData.get_bars), sorted ascending by "time".

    CAUGHT IN REVIEW (2026-07-05): an earlier version of this cached bars
    per-symbol keyed only by symbol, fetched with whatever the FIRST-seen
    row's window happened to be — a later row for the same symbol with a
    later dissent_date silently got a too-narrow window and read back as
    false "still pending" even though the real data existed. Fixed by
    computing the per-symbol window up front from ALL of that symbol's rows
    before any fetch happens, instead of lazily caching on first access.
    """
    from datetime import datetime as _dt
    earliest_by_symbol: dict = {}
    for _row_id, symbol, dissent_date, _call in rows:
        if symbol not in earliest_by_symbol or dissent_date < earliest_by_symbol[symbol]:
            earliest_by_symbol[symbol] = dissent_date

    bars_by_symbol: dict = {}
    try:
        from engine.providers.polygon_provider import PolygonData
        pd = PolygonData()
        pd_active = pd.is_active()
    except Exception as e:
        logger.warning("[crew_dissent] PolygonData init failed: %s: %r", type(e).__name__, e)
        pd_active = False

    if not pd_active:
        return {symbol: None for symbol in earliest_by_symbol}

    to_date = _dt.now().strftime("%Y-%m-%d")
    for symbol, from_date in earliest_by_symbol.items():
        try:
            bars = pd.get_bars(symbol, timespan="day", from_date=from_date,
                               to_date=to_date, limit=120)
            bars_by_symbol[symbol] = sorted(bars, key=lambda b: b["time"]) if bars else None
        except Exception as e:
            logger.warning("[crew_dissent] Polygon fetch failed symbol=%s: %s: %r",
                           symbol, type(e).__name__, e)
            bars_by_symbol[symbol] = None
    return bars_by_symbol


def _forward_price_return_pct(symbol: str, dissent_date: str, horizon_days: int,
                               bars_by_symbol: dict) -> Optional[float]:
    """Raw %-return fallback for when no scored_predictions row exists (the
    original design's assumption that one would always be there for any
    dissented ticker+date does not hold in practice — verified 2026-07-05,
    0 matches at ANY horizon for all 22 pending rows, and the local price
    history tables (price_ticks, empty; backtest_market_data, stale since
    2026-04-02; market_snapshots, covers only a fixed small watchlist that
    doesn't include any dissent symbol) have no usable data for these dates
    either).

    entry = first close on/after dissent_date, exit = close of the
    `horizon_days`-th trading day strictly after entry (per Polygon's own
    market calendar — no local weekend/holiday handling needed). Returns
    None (still-pending, not an error) if there isn't enough forward data
    yet (forward data hasn't happened) or the symbol's bars are unavailable.
    `bars_by_symbol` comes from _prefetch_bars_by_symbol, called once per
    resolve_dissent_outcomes() run.
    """
    bars = bars_by_symbol.get(symbol)
    if not bars:
        return None
    trading_days = [b for b in bars if b["time"][:10] >= dissent_date]
    if len(trading_days) <= horizon_days:
        return None  # not enough forward data yet — genuinely still pending
    entry_price = trading_days[0]["close"]
    exit_price = trading_days[horizon_days]["close"]
    if not entry_price:
        return None
    return (exit_price - entry_price) / entry_price


def resolve_dissent_outcomes() -> dict:
    """Resolve unresolved dissents against realized 5-day outcomes.

    Two-tier resolution, in order:
      1. scored_predictions match in signal-center/signals.db (symbol + same
         date + horizon=5, CLOSED, non-null r_multiple) — original design.
         outcome_basis='r_multiple' (risk-normalized R).
      2. HM-DISSENT-PRICE-RESOLVER-2026-07-05 fallback: raw %-return from
         Polygon daily bars (see _forward_price_return_pct). outcome_basis=
         'price_pct'. Added because tier 1 was found 2026-07-05 to
         structurally never match for any of the 22 then-pending rows — the
         two pipelines' (symbol, date) keys don't coincide (crew_dissent_log
         is a daily-consensus artifact; scored_predictions is keyed to
         individual per-agent signal-generation events).

    dissenter_correct = 1 when (BULL & outcome>0) or (BEAR & outcome<0),
    else 0, under EITHER basis — same sign rule, different scale. HOLD calls
    are undefined under the directional rule and marked not-correct, as before.

    Returns {checked, resolved, resolved_r_multiple, resolved_price_pct, still_pending}.
    """
    out = {"checked": 0, "resolved": 0, "resolved_r_multiple": 0,
           "resolved_price_pct": 0, "still_pending": 0}
    try:
        _ensure_tables()
    except Exception as e:
        logger.warning("[crew_dissent] ensure_tables failed: %s: %r", type(e).__name__, e)
        return out

    conn = sqlite3.connect(str(TRADER_DB), timeout=20)
    _sig_attached = False
    try:
        if SIGNALS_DB.exists():
            try:
                conn.execute("ATTACH DATABASE ? AS sig", (str(SIGNALS_DB),))
                _sig_attached = True
            except Exception as e:
                logger.warning("[crew_dissent] ATTACH signals.db failed: %s: %r", type(e).__name__, e)
        else:
            logger.warning("[crew_dissent] signals.db not found at %s — tier 1 (r_multiple) "
                           "skipped this run, tier 2 (price_pct) still attempted", SIGNALS_DB)

        unresolved = conn.execute(
            "SELECT id, symbol, dissent_date, dissenter_call FROM crew_dissent_log "
            "WHERE dissenter_correct IS NULL"
        ).fetchall()
        out["checked"] = len(unresolved)
        # Pre-fetch once per symbol (not per row) — see _prefetch_bars_by_symbol.
        bars_by_symbol = _prefetch_bars_by_symbol(unresolved)

        for row_id, symbol, dissent_date, call in unresolved:
            outcome_val = None
            outcome_basis = None

            if _sig_attached:
                try:
                    match = conn.execute(
                        """SELECT r_multiple FROM sig.scored_predictions
                           WHERE symbol = ? AND DATE(entry_date) = ?
                             AND horizon_days = ? AND closed = 1 AND r_multiple IS NOT NULL
                           ORDER BY scored_at DESC LIMIT 1""",
                        (symbol, dissent_date, HORIZON_DAYS),
                    ).fetchone()
                    if match and match[0] is not None:
                        outcome_val = float(match[0])
                        outcome_basis = "r_multiple"
                except Exception as e:
                    logger.warning("[crew_dissent] tier-1 resolve query failed id=%s: %s: %r",
                                   row_id, type(e).__name__, e)

            if outcome_val is None:
                pct = _forward_price_return_pct(symbol, dissent_date, HORIZON_DAYS, bars_by_symbol)
                if pct is not None:
                    outcome_val = pct
                    outcome_basis = "price_pct"

            if outcome_val is None:
                out["still_pending"] += 1
                continue

            if call == "BULL":
                correct = 1 if outcome_val > 0 else 0
            elif call == "BEAR":
                correct = 1 if outcome_val < 0 else 0
            else:  # HOLD — undefined under the directional rule; mark not-correct
                correct = 0
            try:
                conn.execute(
                    "UPDATE crew_dissent_log SET outcome_r=?, outcome_basis=?, "
                    "dissenter_correct=?, resolved_at=datetime('now') WHERE id=?",
                    (round(outcome_val, 4), outcome_basis, correct, row_id),
                )
                out["resolved"] += 1
                out[f"resolved_{outcome_basis}"] += 1
            except Exception as e:
                logger.warning("[crew_dissent] resolve update failed id=%s: %s: %r",
                               row_id, type(e).__name__, e)
        conn.commit()
    finally:
        if _sig_attached:
            try:
                conn.execute("DETACH DATABASE sig")
            except Exception:
                pass
        conn.close()
    return out


def recompute_dissent_stats() -> dict:
    """Recompute per-dissenter accuracy for the 30d and 90d windows; UPSERT into
    crew_dissent_stats. Returns {rows_upserted}."""
    res = {"rows_upserted": 0}
    try:
        _ensure_tables()
    except Exception as e:
        logger.warning("[crew_dissent] ensure_tables failed: %s: %r", type(e).__name__, e)
        return res

    conn = sqlite3.connect(str(TRADER_DB), timeout=15)
    try:
        dissenters = [r[0] for r in conn.execute(
            "SELECT DISTINCT dissenter FROM crew_dissent_log"
        ).fetchall()]

        for dissenter in dissenters:
            for window in (30, 90):
                rows = conn.execute(
                    """SELECT dissenter_correct, outcome_r FROM crew_dissent_log
                       WHERE dissenter = ? AND dissenter_correct IS NOT NULL
                         AND dissent_date >= date('now', ?)""",
                    (dissenter, f"-{window} days"),
                ).fetchall()
                dissent_count = len(rows)
                correct_count = sum(1 for c, _ in rows if c == 1)
                accuracy = round(correct_count / dissent_count, 4) if dissent_count else None
                correct_rs = [r for c, r in rows if c == 1 and r is not None]
                wrong_rs = [r for c, r in rows if c == 0 and r is not None]
                avg_correct = round(sum(correct_rs) / len(correct_rs), 4) if correct_rs else None
                avg_wrong = round(sum(wrong_rs) / len(wrong_rs), 4) if wrong_rs else None
                try:
                    conn.execute(
                        """INSERT INTO crew_dissent_stats
                           (dissenter, window_days, dissent_count, correct_count, accuracy,
                            avg_r_when_correct, avg_r_when_wrong, last_computed)
                           VALUES (?,?,?,?,?,?,?,datetime('now'))
                           ON CONFLICT(dissenter, window_days) DO UPDATE SET
                             dissent_count=excluded.dissent_count,
                             correct_count=excluded.correct_count,
                             accuracy=excluded.accuracy,
                             avg_r_when_correct=excluded.avg_r_when_correct,
                             avg_r_when_wrong=excluded.avg_r_when_wrong,
                             last_computed=excluded.last_computed""",
                        (dissenter, window, dissent_count, correct_count, accuracy,
                         avg_correct, avg_wrong),
                    )
                    res["rows_upserted"] += 1
                except Exception as e:
                    logger.warning("[crew_dissent] stats upsert failed %s/%dd: %s: %r",
                                   dissenter, window, type(e).__name__, e)
        conn.commit()
    finally:
        conn.close()
    return res
