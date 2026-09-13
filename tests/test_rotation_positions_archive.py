"""tests/test_rotation_positions_archive.py — HM-ROTATION-POSITIONS-ARCHIVE-2026-09-13.

rotate_season()/start_season() no longer blind-DELETE position rows. They follow the house
archive-then-clear pattern (scripts/signal_center_archive_rotate.py): dry-run by default;
broker-backed rows block the rotation (keyed on actual broker presence, fail-closed); every
other row is INSERTed into positions_season_archive, read back and verified row-for-row
(count AND every field), and only then removed — any failure rolls back the whole rotation;
a one-time snapshot precedes the first-ever archive run; the ending season's end_date is
filled. Fixture DB only; the broker is a stubbed engine.alpaca_bridge; alerts stay behind
_under_pytest().
"""
from __future__ import annotations

import smtplib
import sqlite3
import sys
import types
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import engine.season_manager as sm  # noqa: E402

SCHEMA = """
CREATE TABLE ai_players (id TEXT PRIMARY KEY, display_name TEXT, cash REAL DEFAULT 7000.0,
    is_active INTEGER DEFAULT 1, is_human INTEGER DEFAULT 0, halt_reason TEXT, halted_at TIMESTAMP,
    halt_mode TEXT DEFAULT 'active', season INTEGER DEFAULT 1);
CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE season_history (id INTEGER PRIMARY KEY AUTOINCREMENT, season INTEGER NOT NULL,
    player_id TEXT NOT NULL, display_name TEXT, final_value REAL, total_return_pct REAL,
    total_trades INTEGER DEFAULT 0, win_rate REAL DEFAULT 0, ended_at TEXT);
CREATE TABLE positions (id INTEGER PRIMARY KEY, player_id TEXT NOT NULL, symbol TEXT NOT NULL, qty REAL,
    avg_price REAL, asset_type TEXT DEFAULT 'stock', option_type TEXT, strike_price REAL, expiry_date TEXT,
    opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, high_watermark REAL, conviction REAL, conviction_source TEXT);
CREATE TABLE trades (id INTEGER PRIMARY KEY AUTOINCREMENT, player_id TEXT, symbol TEXT,
    executed_at TEXT, realized_pnl REAL, season INTEGER);
CREATE TABLE season_config (season INTEGER PRIMARY KEY, name TEXT NOT NULL, start_date TEXT NOT NULL,
    end_date TEXT, active_agents TEXT, strategies TEXT, alpha_gate REAL DEFAULT 0.3,
    triple_filter TEXT, proving_ground INTEGER DEFAULT 0, created_at TEXT DEFAULT (datetime('now')));
"""

POSITIONS = [
    ("agent-a", "AGG", 8.5508, 96.19, 101.2, 0.8, "live_buy"),     # clearable, simulated
    ("agent-b", "KMI", 18.0, 33.825, None, None, None),            # clearable unless the broker holds KMI
    ("neo-matrix", "AG", 0.0478, 16.57, None, 0.9, "live_buy"),    # independent player — always kept
    ("alpaca-mirror", "KMI", 18.0, 33.825, None, None, None),      # passive broker mirror — always kept
    ("trade-desk", "F", 3.0, 11.0, None, None, None),              # human account — always kept
]
POS_COLS = ["id", "player_id", "symbol", "qty", "avg_price", "asset_type", "option_type", "strike_price",
            "expiry_date", "opened_at", "high_watermark", "conviction", "conviction_source"]


class _Pos:
    def __init__(self, symbol, qty="1"):
        self.symbol, self.qty = symbol, qty


def _broker(monkeypatch, symbols=None, raises=None, client=True):
    mod = types.ModuleType("engine.alpaca_bridge")
    if not client:
        mod.alpaca = types.SimpleNamespace(client=None)
    else:
        c = MagicMock()
        if raises is not None:
            c.get_all_positions.side_effect = raises
        else:
            c.get_all_positions.return_value = [_Pos(s) for s in (symbols or [])]
        mod.alpaca = types.SimpleNamespace(client=c)
    monkeypatch.setitem(sys.modules, "engine.alpaca_bridge", mod)
    return mod


@pytest.fixture
def db(tmp_path, monkeypatch):
    p = tmp_path / "trader.db"
    c = sqlite3.connect(p)
    c.executescript(SCHEMA)
    c.executemany("INSERT INTO ai_players (id, is_human, halt_mode, halt_reason) VALUES (?,?,?,?)", [
        ("agent-a", 0, "active", None), ("agent-b", 0, "active", None),
        ("neo-matrix", 0, "full", "independent operator"), ("alpaca-mirror", 0, "full", "broker mirror"),
        ("trade-desk", 1, "active", None),
    ])
    c.executemany("INSERT INTO positions (player_id, symbol, qty, avg_price, high_watermark, conviction, "
                  "conviction_source, opened_at) VALUES (?,?,?,?,?,?,?,'2026-09-11 15:34:53')", POSITIONS)
    c.executemany("INSERT INTO settings VALUES (?, ?)", [
        ("current_season", "7"), ("season_7_start", "2026-07-12T23:59:02"), ("season_7_name", "Season 7")])
    c.execute("INSERT INTO season_config (season, name, start_date) VALUES (7, 'Season 7', '2026-07-12')")
    c.commit()
    c.close()
    monkeypatch.setattr(sm, "DB", str(p))
    return p


def _q(p, sql, args=()):
    c = sqlite3.connect(p)
    c.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in c.execute(sql, args)]
    finally:
        c.close()


def _has_table(p, name):
    return bool(_q(p, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)))


def _state(p):
    return {
        "positions": sorted((r["player_id"], r["symbol"], r["qty"]) for r in _q(p, "SELECT * FROM positions")),
        "archive": _q(p, "SELECT COUNT(*) n FROM positions_season_archive")[0]["n"]
        if _has_table(p, "positions_season_archive") else 0,
        "season": _q(p, "SELECT value FROM settings WHERE key='current_season'")[0]["value"],
        "players": sorted(tuple(r.values()) for r in _q(p, "SELECT id, cash, season, halt_mode FROM ai_players")),
        "config": sorted((r["season"], r["end_date"]) for r in _q(p, "SELECT season, end_date FROM season_config")),
    }


def _snapshots(p):
    d = Path(p).parent / "backups"
    return sorted(d.glob("trader_pre-positions-archive-first-run_*.db")) if d.exists() else []


def _rotate(**kw):
    with patch("engine.war_room.save_hot_take", return_value=True):
        return sm.rotate_season(caller="test", **kw)


def test_dry_run_is_the_default_and_writes_nothing(db, monkeypatch):
    _broker(monkeypatch, symbols=[])
    before = _state(db)
    plan = _rotate()
    assert plan["dry_run"] is True and plan["blocked"] is None
    assert plan["new_season"] == 8 and plan["would_archive"] == 2
    assert plan["kept"] == {"independent player (rows owned by shared/matrix_bridge)": 1,
                            "passive broker mirror": 1, "human account": 1}
    assert _state(db) == before
    assert not _has_table(db, "positions_season_archive") and not _snapshots(db)
    assert _q(db, "SELECT COUNT(*) n FROM season_history")[0]["n"] == 0


def test_broker_backed_row_aborts_and_deletes_nothing(db, monkeypatch):
    _broker(monkeypatch, symbols=["KMI"])  # agent-b's KMI row is backed by the broker
    before = _state(db)
    assert _rotate()["blocked"].startswith("1 broker-backed")  # dry-run reports it
    assert _rotate(apply=True) is None
    assert _state(db) == before
    assert not _has_table(db, "positions_season_archive") and not _snapshots(db)


def test_clean_rows_archived_verbatim_then_cleared(db, monkeypatch):
    _broker(monkeypatch, symbols=["WMB"])
    source = {r["id"]: r for r in _q(db, "SELECT * FROM positions")}
    assert _rotate(apply=True) == 8
    after = _state(db)
    assert after["positions"] == sorted([("alpaca-mirror", "KMI", 18.0), ("neo-matrix", "AG", 0.0478),
                                         ("trade-desk", "F", 3.0)])
    arch = _q(db, "SELECT * FROM positions_season_archive ORDER BY source_rowid")
    assert [(a["player_id"], a["symbol"]) for a in arch] == [("agent-a", "AGG"), ("agent-b", "KMI")]
    assert after["archive"] == 2 == len(POSITIONS) - len(after["positions"])
    for a in arch:  # every positions field survives verbatim
        assert {c: a[c] for c in POS_COLS} == source[a["source_rowid"]]
        assert a["archive_season"] == 7 and a["archive_season_name"] == "Season 7"
        assert "7->8" in a["archive_reason"] and "caller=test" in a["archive_reason"]
    assert _q(db, "SELECT value FROM archive_metadata WHERE key=?", (sm.LAST_RUN_KEY,))[0]["value"]


def test_first_run_snapshot_is_taken_exactly_once(db, monkeypatch):
    _broker(monkeypatch, symbols=[])
    assert _rotate(apply=True) == 8
    snaps = _snapshots(db)
    assert len(snaps) == 1
    c = sqlite3.connect(snaps[0])
    assert c.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert c.execute("SELECT COUNT(*) FROM positions").fetchone()[0] == len(POSITIONS)  # taken BEFORE clearing
    c.close()
    key = _q(db, "SELECT value FROM archive_metadata WHERE key=?", (sm.FIRST_RUN_BACKUP_KEY,))
    assert len(key) == 1 and str(snaps[0]) in key[0]["value"]
    assert _rotate(apply=True) == 9
    assert _snapshots(db) == snaps


def test_first_run_snapshot_failure_aborts_before_any_rotation_write(db, monkeypatch):
    _broker(monkeypatch, symbols=[])
    monkeypatch.setattr(sm, "_backup_live_db_if_first_run", MagicMock(side_effect=OSError("disk full")))
    before = _state(db)
    assert _rotate(apply=True) is None
    assert _state(db) == before
    assert _q(db, "SELECT COUNT(*) n FROM season_history")[0]["n"] == 0
    assert not _q(db, "SELECT 1 FROM archive_metadata WHERE key=?", (sm.FIRST_RUN_BACKUP_KEY,))


def test_rotation_fills_ending_season_end_date(db, monkeypatch):
    _broker(monkeypatch, symbols=[])
    assert _rotate(apply=True) == 8
    assert dict(_state(db)["config"])[7] == datetime.now().date().isoformat()


def test_backfill_season_end_dates_only_fills_past_nulls(db):
    c = sqlite3.connect(db)
    c.execute("INSERT OR REPLACE INTO settings VALUES ('current_season', '8')")
    c.executemany("INSERT INTO season_config (season, name, start_date, end_date) VALUES (?,?,?,?)",
                  [(6, "Sniper Mode", "2026-04-10", "2026-07-10"), (8, "Season 8", "2026-09-11", None)])
    c.executemany("INSERT INTO season_history (season, player_id, ended_at) VALUES (?,?,?)",
                  [(7, "agent-a", "2026-09-11T06:13:15.684162"), (6, "agent-a", "2026-07-12T23:59:02")])
    c.commit()
    c.close()
    assert sm.backfill_season_end_dates() == [(7, "2026-09-11")]
    assert dict(_state(db)["config"]) == {6: "2026-07-10", 7: "2026-09-11", 8: None}
    assert sm.backfill_season_end_dates() == []


@pytest.mark.parametrize("case", ["live-raises-no-snapshot", "no-client-stale-snapshot"])
def test_broker_check_unavailable_fails_closed(db, monkeypatch, case):
    if case == "live-raises-no-snapshot":
        _broker(monkeypatch, raises=ConnectionError("alpaca down"))
    else:
        _broker(monkeypatch, client=False)
        stale = (datetime.now(timezone.utc) - timedelta(hours=30)).isoformat()
        c = sqlite3.connect(db)
        c.execute("INSERT INTO settings VALUES ('last_alpaca_full_sync', ?)", (stale,))
        c.commit()
        c.close()
    before = _state(db)
    assert _rotate(apply=True) is None
    assert _state(db) == before and not _snapshots(db)


def test_fresh_mirror_snapshot_used_when_live_broker_down(db, monkeypatch):
    _broker(monkeypatch, client=False)
    c = sqlite3.connect(db)
    c.execute("INSERT INTO settings VALUES ('last_alpaca_full_sync', ?)", (datetime.now(timezone.utc).isoformat(),))
    c.commit()
    c.close()
    before = _state(db)
    assert _rotate(apply=True) is None, "mirror snapshot holds KMI, so agent-b's KMI row is broker-backed"
    assert _state(db) == before


def test_verify_count_mismatch_rolls_back_the_whole_rotation(db, monkeypatch):
    _broker(monkeypatch, symbols=[])
    monkeypatch.setattr(sm, "_archive_rows", lambda conn, rows, *a, **k: len(rows))  # claims success, writes nothing
    before = _state(db)
    assert _rotate(apply=True) is None
    assert _state(db) == before


def test_verify_field_mismatch_rolls_back_the_whole_rotation(db, monkeypatch):
    _broker(monkeypatch, symbols=[])
    real = sm._archive_rows

    def corrupting(conn, rows, cols, *a):
        return real(conn, [dict(r, qty=(r["qty"] or 0) + 1) for r in rows], cols, *a)

    monkeypatch.setattr(sm, "_archive_rows", corrupting)
    before = _state(db)
    assert _rotate(apply=True) is None
    assert _state(db) == before


def test_archive_missing_a_positions_column_refuses(db, monkeypatch):
    _broker(monkeypatch, symbols=[])
    c = sqlite3.connect(db)
    c.execute("ALTER TABLE positions ADD COLUMN future_col TEXT")
    c.commit()
    c.close()
    before = _state(db)
    assert _rotate(apply=True) is None
    assert _state(db) == before


def test_margin_guard_still_aborts_first(db, monkeypatch):
    mod = _broker(monkeypatch, symbols=[])
    c = sqlite3.connect(db)
    c.executemany("INSERT INTO ai_players (id, halt_mode, halt_reason) VALUES (?,?,?)",
                  [(f"zombie-{i}", "full", None) for i in range(30)])
    c.commit()
    c.close()
    before = _state(db)
    assert _rotate(apply=True) is None
    assert _state(db) == before
    mod.alpaca.client.get_all_positions.assert_not_called()


def test_start_season_is_dry_run_by_default_and_uses_the_same_guard(db, monkeypatch):
    _broker(monkeypatch, symbols=["KMI"])
    before = _state(db)
    with patch("engine.war_room.save_hot_take", return_value=True):
        assert sm.start_season(9)["dry_run"] is True
        r = sm.start_season(9, apply=True)
    assert r.get("error") and "broker-backed" in r["position_guard"]
    assert _state(db) == before


def test_archive_is_append_only(tmp_path):
    c = sqlite3.connect(tmp_path / "a.db")
    sm._ensure_positions_archive(c)
    c.execute("INSERT INTO positions_season_archive (archive_season, archive_season_name, archived_at, "
              "archive_reason, source_rowid, player_id, symbol) VALUES (7, 'Season 7', 't', 'r', 1, 'x', 'Y')")
    c.commit()
    with pytest.raises(sqlite3.IntegrityError, match="never deleted"):
        c.execute("DELETE FROM positions_season_archive")
    with pytest.raises(sqlite3.IntegrityError, match="never updated"):
        c.execute("UPDATE positions_season_archive SET qty=1")
    c.close()


def test_live_db_archive_if_present_is_protected():
    live = ROOT / "data" / "trader.db"
    if not live.exists():
        return
    c = sqlite3.connect(f"file:{live}?mode=ro", uri=True)
    try:
        has_table = c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                              (sm.POSITIONS_ARCHIVE_TABLE,)).fetchone()
        triggers = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name=?",
                                            (sm.POSITIONS_ARCHIVE_TABLE,))}
    finally:
        c.close()
    if has_table:
        assert sm.POSITIONS_ARCHIVE_TRIGGERS <= triggers


def test_no_alert_can_push_from_a_test_run(db, monkeypatch):
    from engine.alert_channels import _under_pytest
    assert _under_pytest()
    import requests
    sends = {
        "requests.post": MagicMock(side_effect=AssertionError("network send")),
        "requests.get": MagicMock(side_effect=AssertionError("network send")),
        "Session.post": MagicMock(side_effect=AssertionError("network send")),
        "urlopen": MagicMock(side_effect=AssertionError("network send")),
        "SMTP": MagicMock(side_effect=AssertionError("smtp send")),
        "SMTP_SSL": MagicMock(side_effect=AssertionError("smtp send")),
    }
    monkeypatch.setattr(requests, "post", sends["requests.post"])
    monkeypatch.setattr(requests, "get", sends["requests.get"])
    monkeypatch.setattr(requests.Session, "post", sends["Session.post"])
    monkeypatch.setattr(urllib.request, "urlopen", sends["urlopen"])
    monkeypatch.setattr(smtplib, "SMTP", sends["SMTP"])
    monkeypatch.setattr(smtplib, "SMTP_SSL", sends["SMTP_SSL"])
    _broker(monkeypatch, symbols=["KMI"])
    assert _rotate(apply=True) is None  # the abort path raises the RED_ALERT
    for name, m in sends.items():
        assert not m.called, f"{name} was called from a test run"
