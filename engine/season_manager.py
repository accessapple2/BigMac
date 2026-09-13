"""Season Manager — handles season rotation, history, and resets.

Seasons are started deliberately (the unattended Sunday job is gated off by
SEASON_AUTOROTATE_ENABLED — engine/season_autorotate.py).
All historical data is preserved forever — never delete trades/signals.
Steve's Webull portfolio (is_human=1) is never reset.
"""
from __future__ import annotations
import re
import sqlite3
from datetime import datetime
from rich.console import Console
from shared.matrix_bridge import NEO_PLAYER_ID
from engine.trades_filter import CLEAN_TRADES_WHERE

console = Console()
DB = "data/trader.db"

DEFAULT_CASH = 7000.0
DAYBLADE_CASH = 3500.0

# HM-SEASON-ROTATION-BLANKET-REACTIVATE (2026-07-18): the halt_mode reset
# below must never touch an agent with an explicit halt_reason on file
# (retired, HM-item halts, roster-cap exclusions, bakeoff/audit-trail-only
# clones, exit_only, etc.) — only agents with halt_reason IS NULL are
# eligible, i.e. agents that were never deliberately halted with a reason.
# Verified 2026-07-18 against live data: 100% clean partition (every
# currently-active agent has halt_reason IS NULL; every currently-halted
# agent has halt_reason set) — this predicate currently matches exactly
# the already-active set, making the reset a no-op under normal operation.
# Belt+suspenders: if the dry-run count of eligible rows ever exceeds the
# current active count by more than this margin, rotation aborts before
# any write — a season rotation should never multiply the active fleet.
ROTATION_REACTIVATION_MARGIN = 10

# === HM-ROTATION-POSITIONS-ARCHIVE-2026-09-13 ================================
# rotate_season()/start_season() used to `DELETE FROM positions` for every AI player
# except a hardcoded webull/alpaca-mirror/neo-matrix list — no halt-state or broker
# check. A deleted row that was backed by an open broker position left that broker
# position with no owning row and no working stop (KMI: the 2026-07-12 rotation deleted
# guardian-of-forever's row). Now rotation:
#   * never clears rows of humans, passive broker mirrors, independent players
#     (neo-matrix — its rows are owned/rewritten by shared/matrix_bridge; the old
#     hardcoded exclusion is preserved explicitly), or holders not in ai_players;
#   * ABORTS before any write if any other row is backed by an open broker position —
#     keyed on actual broker presence (live Alpaca, else a fresh alpaca-mirror snapshot),
#     and FAILS CLOSED if neither can be read;
#   * archives every other row and only then clears it — the house archive-then-clear
#     pattern from scripts/signal_center_archive_rotate.py::_rotate_table: INSERT into the
#     archive, read back, verify row-for-row (count AND every field), then DELETE that
#     batch; any failure rolls back the whole rotation. Same trg_rule1_no_delete_* trigger
#     pattern, a one-time snapshot before the first-ever archiving run (archive_metadata
#     key), and dry-run by default (rotate_season/start_season need apply=True).
POSITIONS_ARCHIVE_TABLE = "positions_season_archive"
ARCHIVE_METADATA_TABLE = "archive_metadata"
FIRST_RUN_BACKUP_KEY = "positions_season_archive.backup_taken_at"
LAST_RUN_KEY = "positions_season_archive.last_run"
BROKER_SNAPSHOT_MAX_AGE_HOURS = 6.0
# Every positions column verbatim (like signals_archive.db mirrors its live tables), plus
# the rotation's own archive_* columns. Append-only.
POSITIONS_ARCHIVE_DDL = (
    """CREATE TABLE IF NOT EXISTS positions_season_archive (
        archive_id          INTEGER PRIMARY KEY AUTOINCREMENT,
        archive_season      INTEGER NOT NULL,
        archive_season_name TEXT    NOT NULL,
        archived_at         TEXT    NOT NULL,
        archive_reason      TEXT    NOT NULL,
        source_rowid        INTEGER NOT NULL,
        id                  INTEGER,
        player_id           TEXT,
        symbol              TEXT,
        qty                 REAL,
        avg_price           REAL,
        asset_type          TEXT,
        option_type         TEXT,
        strike_price        REAL,
        expiry_date         TEXT,
        opened_at           TEXT,
        high_watermark      REAL,
        conviction          REAL,
        conviction_source   TEXT
    )""",
    "CREATE TABLE IF NOT EXISTS archive_metadata (key TEXT PRIMARY KEY, value TEXT)",
    "CREATE TRIGGER IF NOT EXISTS trg_rule1_no_delete_positions_season_archive "
    "BEFORE DELETE ON positions_season_archive BEGIN "
    "SELECT RAISE(ABORT, 'RULE #1: positions_season_archive rows are never deleted -- this is the cold copy'); END",
    "CREATE TRIGGER IF NOT EXISTS trg_rule1_no_update_positions_season_archive "
    "BEFORE UPDATE ON positions_season_archive BEGIN "
    "SELECT RAISE(ABORT, 'RULE #1: positions_season_archive rows are never updated -- append-only'); END",
)
POSITIONS_ARCHIVE_TRIGGERS = frozenset({
    "trg_rule1_no_delete_positions_season_archive",
    "trg_rule1_no_update_positions_season_archive",
})


class BrokerCheckUnavailable(RuntimeError):
    """Neither the live broker nor a fresh mirror snapshot could be read."""


class RotationPositionGuardError(RuntimeError):
    """Archive/clear verification failed inside the rotation transaction."""


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def ensure_tables():
    """Create season_history table if it doesn't exist."""
    conn = _conn()
    conn.execute("""CREATE TABLE IF NOT EXISTS season_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        season INTEGER NOT NULL,
        player_id TEXT NOT NULL,
        display_name TEXT,
        final_value REAL,
        total_return_pct REAL,
        total_trades INTEGER DEFAULT 0,
        win_rate REAL DEFAULT 0,
        ended_at TEXT
    )""")
    conn.commit()
    conn.close()


def get_current_season() -> int:
    """Get current season number from settings."""
    conn = _conn()
    row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
    conn.close()
    return int(row[0]) if row else 1


def save_season_summary(season: int):
    """Save final leaderboard standings for a completed season."""
    ensure_tables()
    conn = _conn()

    # Check if already saved
    existing = conn.execute(
        "SELECT 1 FROM season_history WHERE season=?", (season,)
    ).fetchone()
    if existing:
        conn.close()
        return

    players = conn.execute(
        "SELECT id, display_name, cash FROM ai_players WHERE is_active=1"
    ).fetchall()

    for p in players:
        pid = p["id"]
        cash = p["cash"]

        # Calculate total value from positions
        positions = conn.execute(
            "SELECT symbol, qty, avg_price FROM positions WHERE player_id=?",
            (pid,)
        ).fetchall()
        positions_value = sum(r["qty"] * r["avg_price"] for r in positions)
        total_value = cash + positions_value

        # Get trade stats for this season
        stats = conn.execute(
            "SELECT COUNT(*) as total, "
            "SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins "
            "FROM trades WHERE player_id=? AND season=? "
            # HM-TRACKING-AGGREGATOR: query had NO realized_pnl gate (counted every row
            # incl BUY). Added realized_pnl IS NOT NULL (BUY-row exclusion) separately
            # from the clean-trades boundary.
            "AND realized_pnl IS NOT NULL "
            f"AND {CLEAN_TRADES_WHERE}",
            (pid, season)
        ).fetchone()
        total_trades = stats["total"] or 0
        wins = stats["wins"] or 0
        win_rate = round(wins / total_trades * 100, 1) if total_trades > 0 else 0

        # Determine starting cash for return calculation
        if pid == "dayblade-0dte":
            starting = DAYBLADE_CASH
        elif pid == "webull":
            starting = 7021.81
        else:
            starting = DEFAULT_CASH
        return_pct = round((total_value - starting) / starting * 100, 2) if starting > 0 else 0

        conn.execute(
            "INSERT INTO season_history "
            "(season, player_id, display_name, final_value, total_return_pct, "
            "total_trades, win_rate, ended_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (season, pid, p["display_name"], round(total_value, 2),
             return_pct, total_trades, win_rate, datetime.now().isoformat())
        )

    conn.commit()
    conn.close()
    console.log(f"[bold green]Season {season} summary saved ({len(players)} players)")


# Rows eligible for the season-reset unhalt — never touches a row with an
# explicit halt_reason on file. See ROTATION_REACTIVATION_MARGIN docstring.
_UNHALT_ELIGIBLE_WHERE = (
    "id NOT IN ('webull','alpaca-mirror') AND id != ? AND halt_reason IS NULL"
)


def _dry_run_unhalt_scope(conn) -> dict:
    """Read-only: count agents currently active vs. agents the season-reset
    UPDATE would touch under _UNHALT_ELIGIBLE_WHERE. No writes.

    Returns {"active_before": int, "would_affect": int, "safe": bool}.
    "safe" is False if would_affect exceeds active_before by more than
    ROTATION_REACTIVATION_MARGIN — the caller must abort before writing.
    """
    active_before = conn.execute(
        "SELECT COUNT(*) FROM ai_players "
        "WHERE id NOT IN ('webull','alpaca-mirror') AND id != ? AND halt_mode='active'",
        (NEO_PLAYER_ID,)
    ).fetchone()[0]
    would_affect = conn.execute(
        f"SELECT COUNT(*) FROM ai_players WHERE {_UNHALT_ELIGIBLE_WHERE}",
        (NEO_PLAYER_ID,)
    ).fetchone()[0]
    return {
        "active_before": active_before,
        "would_affect": would_affect,
        "safe": would_affect <= active_before + ROTATION_REACTIVATION_MARGIN,
    }


def _alert_rotation_aborted(scope: dict, season: int, caller: str) -> None:
    # HM-FALSE-RED-ALERT-FORENSICS-2026-09-09: this exact alert fired 8
    # times (2026-07-18 -> 2026-09-09), byte-identical payload every time,
    # and NEVER once recorded who or what invoked rotate_season() --
    # exhaustive audits of every locally-stored Claude Code session
    # transcript and the live trader.log's ENDPOINT-DUR request log found
    # zero trace of a real invocation anywhere. caller (now a required
    # rotate_season() argument), sys.argv, and a 3-frame stack summary are
    # stamped into the alert so the next occurrence names itself instead
    # of recurring as another unsolved mystery.
    import sys, traceback
    stack_summary = "".join(traceback.format_stack(limit=4)[:-1])  # drop this frame itself
    try:
        from engine.alert_channels import send_alert, AlertLevel
        send_alert(
            message=(
                f"Season rotation ABORTED before Season {season}: the halt_mode "
                f"reset would have touched {scope['would_affect']} agents against "
                f"{scope['active_before']} currently active "
                f"(margin={ROTATION_REACTIVATION_MARGIN}). No DB writes were made. "
                f"A season rotation should never multiply the active fleet — "
                f"investigate engine/season_manager.py before the next attempt. "
                f"caller={caller!r} argv={sys.argv!r}\n"
                f"stack (nearest-caller-first, 3 frames):\n{stack_summary}"
            ),
            level=AlertLevel.RED_ALERT,
            alert_type="hm-season-rotation-aborted",
            rate_limit_secs=3600,
        )
    except Exception as e:
        console.log(f"[red]Season rotation abort-NTFY failed: {e}")


def _write_season_config(conn, season: int, start_iso: str) -> None:
    """HM-SEASON-CONFIG-AUTOWRITE-2026-09-13: rotation never wrote season_config or
    settings.season_N_name (S7's row was hand-written 2026-09-01; S8 had none, so
    /api/season returned config:{} and the Season panel read "Active: 1 agents").
    New rows only — INSERT OR IGNORE never rewrites an existing season's row or name.
    active_agents = halt_mode='active' ids after the unhalt step (same rule as S7's row)."""
    name_key = f"season_{season}_name"
    row = conn.execute("SELECT value FROM settings WHERE key=?", (name_key,)).fetchone()
    name = row[0] if row and row[0] else f"Season {season}"
    conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (name_key, name))
    active = [r[0] for r in conn.execute("SELECT id FROM ai_players WHERE halt_mode='active' ORDER BY id")]
    conn.execute(
        "INSERT OR IGNORE INTO season_config (season, name, start_date, active_agents) VALUES (?, ?, ?, ?)",
        (season, name, str(start_iso)[:10], ",".join(active)),
    )


def ensure_season_config(season: int | None = None) -> dict:
    """Backfill the season_config row + season_N_name for `season` (default: current) from
    settings.season_N_start. Idempotent; refuses to invent a start date. Returns the row."""
    season = season or get_current_season()
    conn = _conn()
    try:
        start = conn.execute("SELECT value FROM settings WHERE key=?", (f"season_{season}_start",)).fetchone()
        if not start:
            raise ValueError(f"settings.season_{season}_start missing — refusing to invent a start date")
        _write_season_config(conn, season, start[0])
        conn.commit()
        row = conn.execute("SELECT * FROM season_config WHERE season=?", (season,)).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def _underlying(symbol) -> str:
    """Stock symbol or an option's root (OCC 'SPY260918P00719000' -> 'SPY')."""
    s = str(symbol or "").upper()
    m = re.match(r"[A-Z][A-Z.]*", s)
    return m.group(0) if m else s


def _broker_open_symbols(conn) -> tuple[set[str], str]:
    """(open broker symbols, source). Live Alpaca first; else the alpaca-mirror rows if
    the last full sync is <= BROKER_SNAPSHOT_MAX_AGE_HOURS old. Raises
    BrokerCheckUnavailable if neither can be read — callers must FAIL CLOSED."""
    errors = []
    try:
        from engine.alpaca_bridge import alpaca
        if getattr(alpaca, "client", None) is None:
            raise RuntimeError("Alpaca client not initialised")
        held = alpaca.client.get_all_positions()
        return {_underlying(p.symbol) for p in held if float(p.qty or 0) != 0}, "alpaca-live"
    except Exception as e:
        errors.append(f"live: {type(e).__name__}: {e}")
    try:
        from engine.bridge_staleness import age_hours
        synced = conn.execute("SELECT value FROM settings WHERE key='last_alpaca_full_sync'").fetchone()
        if not synced:
            raise RuntimeError("settings.last_alpaca_full_sync missing")
        age = age_hours(synced[0])
        if age is None or age > BROKER_SNAPSHOT_MAX_AGE_HOURS:
            raise RuntimeError(f"alpaca-mirror snapshot unreadable or older than "
                               f"{BROKER_SNAPSHOT_MAX_AGE_HOURS}h (age_h={age})")
        rows = conn.execute("SELECT symbol FROM positions WHERE player_id='alpaca-mirror' AND qty != 0").fetchall()
        return {_underlying(r[0]) for r in rows}, f"alpaca-mirror snapshot ({age:.1f}h old)"
    except Exception as e:
        errors.append(f"snapshot: {type(e).__name__}: {e}")
    raise BrokerCheckUnavailable("; ".join(errors))


def _position_keep_reason(player_id, holder) -> str | None:
    """Why a holder's rows are never cleared by rotation, or None if clearable."""
    from engine.halt_gate import _PASSIVE_MIRROR_PLAYER_IDS
    from shared.matrix_bridge import is_independent_player
    if player_id in _PASSIVE_MIRROR_PLAYER_IDS:
        return "passive broker mirror"
    if is_independent_player(player_id):
        return "independent player (rows owned by shared/matrix_bridge)"
    if holder is None:
        return "holder not in ai_players"
    if holder.get("is_human"):
        return "human account"
    return None


def _position_clear_plan(conn, broker_symbols: set[str]) -> dict:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(ai_players)")}
    human = "is_human" if "is_human" in cols else "0 AS is_human"
    holders = {r["id"]: dict(r) for r in conn.execute(f"SELECT id, {human} FROM ai_players")}
    clear, backed, kept = [], [], {}
    for r in conn.execute("SELECT rowid AS _rowid, * FROM positions"):
        row = dict(r)
        why = _position_keep_reason(row.get("player_id"), holders.get(row.get("player_id")))
        if why:
            kept[why] = kept.get(why, 0) + 1
        elif _underlying(row.get("symbol")) in broker_symbols:
            backed.append(row)
        else:
            clear.append(row)
    return {"clear": clear, "backed": backed, "kept": kept}


def _position_preflight() -> dict:
    """Read-only, before ANY rotation write. ok=False means abort."""
    conn = _conn()
    try:
        try:
            broker_symbols, source = _broker_open_symbols(conn)
        except BrokerCheckUnavailable as e:
            return {"ok": False, "reason": f"broker position check unavailable, failing closed ({e})"}
        plan = _position_clear_plan(conn, broker_symbols)
        out = {"broker_symbols": broker_symbols, "broker_source": source, **plan}
        if plan["backed"]:
            names = ", ".join(f"{r['player_id']}:{r['symbol']}" for r in plan["backed"])
            return {**out, "ok": False,
                    "reason": f"{len(plan['backed'])} broker-backed position row(s) would be cleared "
                              f"({names}); close or reassign them first"}
        return {**out, "ok": True, "reason": ""}
    finally:
        conn.close()


def _ensure_positions_archive(conn) -> None:
    """Idempotent, at apply time (like signal_center_archive_rotate._ensure_archive_triggers):
    the archive table, archive_metadata, and the trg_rule1_* triggers. Fails loud if a
    trigger didn't install."""
    for stmt in POSITIONS_ARCHIVE_DDL:
        conn.execute(stmt)
    conn.commit()
    installed = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name=?", (POSITIONS_ARCHIVE_TABLE,))}
    if not POSITIONS_ARCHIVE_TRIGGERS <= installed:
        raise RotationPositionGuardError(
            f"archive triggers missing after install: {sorted(POSITIONS_ARCHIVE_TRIGGERS - installed)}")


def _backup_live_db_if_first_run(conn) -> str:
    """One-time consistent snapshot before the first-ever archiving run, recorded under
    archive_metadata FIRST_RUN_BACKUP_KEY so it can neither repeat nor be skipped (the key is
    written only after the snapshot passes integrity_check; any failure raises and the
    rotation aborts). Written as data/backups/trader_pre-positions-archive-first-run_<ts>.db
    so scripts/offhost_backup.sh's ad-hoc sweep carries it to the X9."""
    row = conn.execute(f"SELECT value FROM {ARCHIVE_METADATA_TABLE} WHERE key=?", (FIRST_RUN_BACKUP_KEY,)).fetchone()
    if row:
        return f"already taken ({row[0]})"
    from pathlib import Path
    out_dir = Path(DB).resolve().parent / "backups"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"trader_pre-positions-archive-first-run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    dst = sqlite3.connect(path)
    try:
        with dst:
            conn.backup(dst)
        integrity = dst.execute("PRAGMA integrity_check").fetchone()[0]
    finally:
        dst.close()
    if integrity != "ok":
        raise RuntimeError(f"first-run snapshot integrity_check={integrity!r} ({path})")
    conn.execute(f"INSERT INTO {ARCHIVE_METADATA_TABLE} (key, value) VALUES (?, ?)",
                 (FIRST_RUN_BACKUP_KEY, f"{datetime.now().isoformat()} {path}"))
    conn.commit()
    return f"taken: {path}"


def _prepare_positions_archive() -> str:
    """Apply path, before any rotation write. Raises on failure — the caller aborts."""
    conn = _conn()
    try:
        _ensure_positions_archive(conn)
        return _backup_live_db_if_first_run(conn)
    finally:
        conn.close()


def _archive_rows(conn, rows, cols, season, season_name, archived_at, reason) -> int:
    col_list = ", ".join(cols)
    placeholders = ", ".join("?" for _ in cols)
    conn.executemany(
        f"INSERT INTO {POSITIONS_ARCHIVE_TABLE} (archive_season, archive_season_name, archived_at, "
        f"archive_reason, source_rowid, {col_list}) VALUES (?, ?, ?, ?, ?, {placeholders})",
        [(season, season_name, archived_at, reason, r["_rowid"], *[r[c] for c in cols]) for r in rows],
    )
    return len(rows)


def _archive_then_clear_positions(conn, broker_symbols, ending_season: int, new_season: int, caller: str) -> int:
    """House archive-then-clear (scripts/signal_center_archive_rotate.py::_rotate_table), run
    inside the caller's open rotation transaction: re-plan (rows may have changed since the
    preflight), INSERT into the archive, read back and verify row-for-row — count AND every
    field — and only then DELETE that batch. Raises RotationPositionGuardError on any
    mismatch; the caller rolls back the whole rotation. Never delete-then-check."""
    plan = _position_clear_plan(conn, broker_symbols)
    if plan["backed"]:
        names = ", ".join(f"{r['player_id']}:{r['symbol']}" for r in plan["backed"])
        raise RotationPositionGuardError(f"broker-backed rows appeared since preflight ({names})")
    rows = sorted(plan["clear"], key=lambda r: r["_rowid"])
    if not rows:
        return 0
    cols = [r[1] for r in conn.execute("PRAGMA table_info(positions)")]
    archive_cols = {r[1] for r in conn.execute(f"PRAGMA table_info({POSITIONS_ARCHIVE_TABLE})")}
    missing = [c for c in cols if c not in archive_cols]
    if missing:
        raise RotationPositionGuardError(
            f"{POSITIONS_ARCHIVE_TABLE} lacks positions column(s) {missing}; refusing to archive a partial row")
    name_row = conn.execute("SELECT value FROM settings WHERE key=?", (f"season_{ending_season}_name",)).fetchone()
    season_name = name_row[0] if name_row and name_row[0] else f"Season {ending_season}"
    archived_at = datetime.now().isoformat()
    reason = f"season rotation {ending_season}->{new_season} (caller={caller})"

    _archive_rows(conn, rows, cols, ending_season, season_name, archived_at, reason)
    col_list = ", ".join(cols)
    archived = conn.execute(
        f"SELECT source_rowid, {col_list} FROM {POSITIONS_ARCHIVE_TABLE} "
        f"WHERE archived_at=? AND archive_reason=? ORDER BY source_rowid",
        (archived_at, reason)).fetchall()
    if len(archived) != len(rows):
        raise RotationPositionGuardError(
            f"verify FAILED — selected {len(rows)} positions rows, found {len(archived)} readable in archive")
    for s, a in zip(rows, archived):
        if a["source_rowid"] != s["_rowid"] or tuple(s[c] for c in cols) != tuple(a[c] for c in cols):
            raise RotationPositionGuardError(f"verify FAILED — content mismatch on positions rowid={s['_rowid']}")

    rowids = [r["_rowid"] for r in rows]
    cur = conn.execute(f"DELETE FROM positions WHERE rowid IN ({','.join('?' * len(rowids))})", rowids)
    if cur.rowcount != len(rows):
        raise RotationPositionGuardError(f"clear count mismatch: verified={len(rows)} removed={cur.rowcount}")
    conn.execute(
        f"INSERT INTO {ARCHIVE_METADATA_TABLE} (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (LAST_RUN_KEY, f"{archived_at} season {ending_season}->{new_season} rows={len(rows)} caller={caller}"))
    return len(rows)


def _fill_ending_season_end_date(conn, season: int, end_iso: str) -> None:
    """Config row only; fills a NULL end_date, never overwrites."""
    try:
        conn.execute("UPDATE season_config SET end_date=? WHERE season=? AND end_date IS NULL",
                     (str(end_iso)[:10], season))
    except sqlite3.OperationalError as e:
        console.log(f"[bold red]Season rotation: season_config end_date for Season {season} NOT written: {e}")


def backfill_season_end_dates() -> list[tuple[int, str]]:
    """Fill NULL season_config.end_date for PAST seasons from MAX(season_history.ended_at).
    Only NULLs, never overwrites, skips the current season. Returns what it filled."""
    current = get_current_season()
    conn = _conn()
    try:
        filled = []
        for r in conn.execute("SELECT season FROM season_config WHERE end_date IS NULL AND season < ? "
                              "ORDER BY season", (current,)).fetchall():
            season = r[0]
            ended = conn.execute("SELECT MAX(ended_at) FROM season_history WHERE season=?", (season,)).fetchone()[0]
            if not ended:
                continue
            cur = conn.execute("UPDATE season_config SET end_date=? WHERE season=? AND end_date IS NULL",
                               (str(ended)[:10], season))
            if cur.rowcount:
                filled.append((season, str(ended)[:10]))
        conn.commit()
        return filled
    finally:
        conn.close()


def _alert_rotation_position_guard(reason: str, season: int, caller: str) -> None:
    try:
        from engine.alert_channels import send_alert, AlertLevel
        send_alert(
            message=(f"Season rotation ABORTED before Season {season}: {reason}. No positions were "
                     f"cleared and nothing was written (or it was rolled back). caller={caller!r}"),
            level=AlertLevel.RED_ALERT,
            alert_type="hm-season-rotation-position-guard",
            rate_limit_secs=3600,
        )
    except Exception as e:
        console.log(f"[red]Season rotation position-guard alert failed: {e}")


def rotate_season(caller: str, *, apply: bool = False) -> int | dict | None:
    """Rotate to a new season.

    Dry-run by default (house archive-then-clear pattern): runs every safety check and
    returns a plan dict — {"dry_run": True, "blocked": reason-or-None, ...} — with zero
    writes. Pass apply=True to rotate; then returns the new season number, or None if a
    safety check aborted (no writes, or everything rolled back — safe to retry once
    investigated).

    HM-FALSE-RED-ALERT-FORENSICS-2026-09-09: caller is now a REQUIRED,
    explicit argument (e.g. "cron-sunday", "s8-manual") -- no default, no
    silent fallback. This function has fired a false abort alert 8 times
    with zero record of who invoked it; refusing to run anonymously, and
    stamping the caller (plus argv and a stack summary) into the abort
    alert, means the next occurrence identifies itself instead of adding
    a 9th unsolved entry to docs/XO_BACKLOG.md's HM-FALSE-RED-ALERT.
    """
    if not caller or not isinstance(caller, str):
        raise TypeError(
            "rotate_season() requires an explicit caller=... argument "
            "(e.g. 'cron-sunday', 's8-manual') -- see "
            "HM-FALSE-RED-ALERT-FORENSICS-2026-09-09"
        )
    ensure_tables()
    current = get_current_season()
    new_season = current + 1

    # HM-SEASON-ROTATION-BLANKET-REACTIVATE: dry-run the unhalt scope BEFORE
    # any write (including save_season_summary) so an abort leaves the DB
    # completely untouched — no rollback needed, trivially safe to retry.
    check_conn = _conn()
    scope = _dry_run_unhalt_scope(check_conn)
    check_conn.close()
    if not scope["safe"]:
        if not apply:
            return {"dry_run": True, "new_season": new_season,
                    "blocked": "reactivation-scope check failed", "scope": scope}
        console.log(
            f"[bold red]SEASON ROTATION ABORTED — reactivation scope check failed: "
            f"would_affect={scope['would_affect']} active_before={scope['active_before']} "
            f"margin={ROTATION_REACTIVATION_MARGIN}[/bold red]"
        )
        _alert_rotation_aborted(scope, new_season, caller)
        return None

    # HM-ROTATION-POSITIONS-ARCHIVE-2026-09-13: position guard, also before ANY write.
    preflight = _position_preflight()
    if not preflight["ok"]:
        if not apply:
            return {"dry_run": True, "new_season": new_season, "blocked": preflight["reason"]}
        console.log(f"[bold red]SEASON ROTATION ABORTED — {preflight['reason']}[/bold red]")
        _alert_rotation_position_guard(preflight["reason"], new_season, caller)
        return None

    if not apply:
        return {"dry_run": True, "new_season": new_season, "blocked": None,
                "would_archive": len(preflight["clear"]), "kept": preflight["kept"],
                "broker_source": preflight["broker_source"]}

    try:
        snapshot = _prepare_positions_archive()
    except Exception as e:
        reason = f"positions archive not ready ({type(e).__name__}: {e})"
        console.log(f"[bold red]SEASON ROTATION ABORTED — {reason}[/bold red]")
        _alert_rotation_position_guard(reason, new_season, caller)
        return None
    console.log(f"[cyan]Season rotation: positions archive ready; first-run snapshot {snapshot}")

    # Save summary of ending season
    save_season_summary(current)

    conn = _conn()

    # Update season number
    start_iso = datetime.now().isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('current_season', ?)",
        (str(new_season),)
    )
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
        (f"season_{new_season}_start", start_iso)
    )

    # Reset AI player cash — NOT human players, NOT Steve, NOT broker mirror
    # HM-I-β-Item3 (2026-05-05): exclude alpaca-mirror — its cash is sync'd
    # from Alpaca every 5 min by alpaca_portfolio_sync, not season-managed.
    conn.execute(
        "UPDATE ai_players SET cash=?, season=? WHERE id NOT IN ('webull','alpaca-mirror','dayblade-0dte') AND id != ?",
        (DEFAULT_CASH, new_season, NEO_PLAYER_ID)
    )
    conn.execute(
        "UPDATE ai_players SET cash=?, season=? WHERE id='dayblade-0dte'",
        (DAYBLADE_CASH, new_season)
    )
    # Steve and the broker mirror keep their portfolios but get season tag updated
    conn.execute(
        "UPDATE ai_players SET season=? WHERE id IN ('webull','alpaca-mirror')",
        (new_season,)
    )

    # Unhalt AI players for the new season — HM-SEASON-ROTATION-BLANKET-
    # REACTIVATE (2026-07-18): scoped to halt_reason IS NULL only. Any agent
    # with an explicit halt_reason (retired, HM-item halts, roster-cap
    # exclusions, bakeoff clones, exit_only, etc.) is never touched here.
    # HM-B-pre: migrated is_halted=0 → halt_mode='active' (drop halt_reason + halted_at on season reset)
    cur = conn.execute(
        f"UPDATE ai_players SET halt_mode='active', halt_reason=NULL, halted_at=NULL WHERE {_UNHALT_ELIGIBLE_WHERE}",
        (NEO_PLAYER_ID,)
    )
    console.log(f"[cyan]Season rotation unhalt: {cur.rowcount} rows touched (scope check: {scope})")
    try:
        _write_season_config(conn, new_season, start_iso)
    except sqlite3.OperationalError as e:
        console.log(f"[bold red]Season rotation: season_config/season_{new_season}_name NOT written: {e}")

    _fill_ending_season_end_date(conn, current, start_iso)

    # HM-ROTATION-POSITIONS-ARCHIVE-2026-09-13: archive-then-clear (was a blind DELETE).
    try:
        cleared = _archive_then_clear_positions(conn, preflight["broker_symbols"], current, new_season, caller)
    except (RotationPositionGuardError, sqlite3.Error) as e:
        conn.rollback()
        conn.close()
        console.log(f"[bold red]SEASON ROTATION ABORTED — positions archive/clear failed, "
                    f"whole rotation rolled back: {e}[/bold red]")
        _alert_rotation_position_guard(f"positions archive/clear failed ({e})", new_season, caller)
        return None
    console.log(f"[cyan]Season rotation positions: {cleared} archived to {POSITIONS_ARCHIVE_TABLE} then "
                f"cleared; kept {preflight['kept']} (broker source: {preflight['broker_source']})")

    conn.commit()
    conn.close()

    # Post to War Room
    try:
        from engine.war_room import save_hot_take
        save_hot_take(
            "webull", "SEASON",
            f"⭐ ADMIRAL PICARD: Season {new_season} has begun. "
            f"Final standings for Season {current} are locked. "
            f"All crew reset to starting positions. "
            f"Captain Kirk's portfolio carries forward as the human benchmark. "
            f"Engage."
        )
    except Exception as e:
        console.log(f"[red]Season rotation War Room post failed: {e}")

    console.log(f"[bold green]SEASON ROTATION: Season {current} → Season {new_season}")
    return new_season


def get_season_history() -> list:
    """Get all season summaries with winners."""
    ensure_tables()
    conn = _conn()
    current = get_current_season()

    seasons = []
    # Get all unique seasons from history
    season_nums = conn.execute(
        "SELECT DISTINCT season FROM season_history ORDER BY season DESC"
    ).fetchall()

    for row in season_nums:
        s = row["season"]
        # Get all players for this season, ordered by return
        players = conn.execute(
            "SELECT * FROM season_history WHERE season=? ORDER BY total_return_pct DESC",
            (s,)
        ).fetchall()
        players_list = [dict(p) for p in players]
        winner = players_list[0] if players_list else None
        seasons.append({
            "season": s,
            "winner": winner,
            "players": players_list,
            "ended_at": winner["ended_at"] if winner else None,
        })

    # Add current season as "LIVE"
    seasons.insert(0, {
        "season": current,
        "winner": None,
        "players": [],
        "ended_at": None,
        "live": True,
    })

    conn.close()
    return seasons


def start_season(season_num: int, *, apply: bool = False):
    """Directly start a specific season number (for manual season launches).
    Dry-run by default — returns a plan with zero writes; pass apply=True to start."""
    current = get_current_season()
    if season_num <= current:
        return {"error": f"Season {season_num} is not greater than current season {current}"}

    # HM-SEASON-ROTATION-BLANKET-REACTIVATE: same dry-run safety check as
    # rotate_season() — abort before any write if the unhalt scope looks
    # like it would multiply the active fleet instead of a no-op refresh.
    check_conn = _conn()
    scope = _dry_run_unhalt_scope(check_conn)
    check_conn.close()
    if not scope["safe"]:
        if not apply:
            return {"dry_run": True, "season": season_num,
                    "blocked": "reactivation-scope check failed", "scope": scope}
        console.log(
            f"[bold red]SEASON START ABORTED — reactivation scope check failed: "
            f"would_affect={scope['would_affect']} active_before={scope['active_before']} "
            f"margin={ROTATION_REACTIVATION_MARGIN}[/bold red]"
        )
        _alert_rotation_aborted(scope, season_num, "dashboard:/api/seasons/start")
        return {"error": "aborted by reactivation-scope safety check", "scope": scope}

    # HM-ROTATION-POSITIONS-ARCHIVE-2026-09-13: position guard, before ANY write.
    preflight = _position_preflight()
    if not preflight["ok"]:
        if not apply:
            return {"dry_run": True, "season": season_num, "blocked": preflight["reason"]}
        console.log(f"[bold red]SEASON START ABORTED — {preflight['reason']}[/bold red]")
        _alert_rotation_position_guard(preflight["reason"], season_num, "dashboard:/api/seasons/start")
        return {"error": "aborted by position guard", "position_guard": preflight["reason"]}

    if not apply:
        return {"dry_run": True, "season": season_num, "blocked": None,
                "would_archive": len(preflight["clear"]), "kept": preflight["kept"],
                "broker_source": preflight["broker_source"]}

    try:
        snapshot = _prepare_positions_archive()
    except Exception as e:
        reason = f"positions archive not ready ({type(e).__name__}: {e})"
        console.log(f"[bold red]SEASON START ABORTED — {reason}[/bold red]")
        _alert_rotation_position_guard(reason, season_num, "dashboard:/api/seasons/start")
        return {"error": "aborted by position guard", "position_guard": reason}
    console.log(f"[cyan]Season start: positions archive ready; first-run snapshot {snapshot}")

    # Save current season summary
    save_season_summary(current)

    conn = _conn()

    # Set new season
    start_iso = datetime.now().isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('current_season', ?)",
        (str(season_num),)
    )
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
        (f"season_{season_num}_start", start_iso)
    )

    # Reset AI player cash
    # HM-I-β-Item3 (2026-05-05): exclude alpaca-mirror (broker-sync target).
    conn.execute(
        "UPDATE ai_players SET cash=?, season=? WHERE id NOT IN ('webull','alpaca-mirror','dayblade-0dte') AND id != ?",
        (DEFAULT_CASH, season_num, NEO_PLAYER_ID)
    )
    conn.execute(
        "UPDATE ai_players SET cash=?, season=? WHERE id='dayblade-0dte'",
        (DAYBLADE_CASH, season_num)
    )
    conn.execute("UPDATE ai_players SET season=? WHERE id IN ('webull','alpaca-mirror')", (season_num,))
    # Unhalt AI players — HM-SEASON-ROTATION-BLANKET-REACTIVATE (2026-07-18):
    # scoped to halt_reason IS NULL only, same as rotate_season(). Never
    # touches an agent with an explicit halt_reason on file.
    # HM-B-pre: migrated is_halted=0 → halt_mode='active' (drop halt_reason + halted_at on season reset)
    cur = conn.execute(
        f"UPDATE ai_players SET halt_mode='active', halt_reason=NULL, halted_at=NULL WHERE {_UNHALT_ELIGIBLE_WHERE}",
        (NEO_PLAYER_ID,),
    )
    console.log(f"[cyan]Season start unhalt: {cur.rowcount} rows touched (scope check: {scope})")
    try:
        _write_season_config(conn, season_num, start_iso)
    except sqlite3.OperationalError as e:
        console.log(f"[bold red]Season start: season_config/season_{season_num}_name NOT written: {e}")
    _fill_ending_season_end_date(conn, current, start_iso)
    try:
        cleared = _archive_then_clear_positions(conn, preflight["broker_symbols"], current, season_num,
                                                "dashboard:/api/seasons/start")
    except (RotationPositionGuardError, sqlite3.Error) as e:
        conn.rollback()
        conn.close()
        console.log(f"[bold red]SEASON START ABORTED — positions archive/clear failed, rolled back: {e}[/bold red]")
        _alert_rotation_position_guard(f"positions archive/clear failed ({e})", season_num,
                                       "dashboard:/api/seasons/start")
        return {"error": "aborted by position guard", "position_guard": str(e)}
    console.log(f"[cyan]Season start positions: {cleared} archived then cleared; kept {preflight['kept']}")

    conn.commit()
    conn.close()

    # Post announcement
    try:
        from engine.war_room import save_hot_take
        save_hot_take(
            "webull", "SEASON",
            f"⭐ ADMIRAL PICARD: Season {season_num} has begun. "
            f"All crew reset to starting positions. "
            f"Captain Kirk's portfolio carries forward as the human benchmark. "
            f"Make it so."
        )
    except Exception:
        pass

    console.log(f"[bold green]Season {season_num} started manually")
    return {"ok": True, "season": season_num}
