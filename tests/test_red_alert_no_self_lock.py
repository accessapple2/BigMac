"""HM-RED-ALERT-SELF-LOCK 2026-09-14 — red_alert_check must not hold a write
transaction open while it posts to the War Room.

Live incident: red_alert_check opened one connection for its whole symbol loop
and committed only after it. The first red-alert INSERT started a write
transaction; every later alert's _post_to_war_room -> war_room.save_hot_take
wrote through its own connection, waited out the busy timeout on that lock
and failed ("War Room post failed: database is locked", ~169s per symbol).
That held schedule.run_pending() from 06:41 MST for over half an hour, with
168 shared-queue jobs dark, and no red_alert row ever committed.

The fake War Room post here writes through a SEPARATE connection with a short
busy timeout, the same shape as save_hot_take, so a held write lock shows up
as a failed post instead of a 30 s hang."""
from __future__ import annotations

import sqlite3
import sys
import types
from datetime import date

import pytest

import engine.volume_scanner as vs

TODAY = date.today().isoformat()


@pytest.fixture
def harness(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()
    db = tmp_path / "data" / "trader.db"
    vs._init_tables()
    with sqlite3.connect(db) as c:
        c.execute("CREATE TABLE war_room_posts (symbol TEXT, msg TEXT)")
        for sym in ("AAA", "BBB", "CCC", "DDD"):
            c.execute(
                "INSERT INTO volume_alerts (symbol, alert_type, price, relative_volume, gap_pct, dollar_volume, detected_at) "
                "VALUES (?, 'volume_explosion', 10, 20, 1, 1e6, ?)",
                (sym, f"{TODAY} 12:00:00"),
            )

    rel_vol = {"AAA": 150.0, "BBB": 60.0, "CCC": 5.0, "DDD": 75.0}
    state = {"posts_ok": [], "posts_failed": []}

    def fake_post(symbol, message):
        conn = sqlite3.connect(db, timeout=0.5)
        try:
            conn.execute("INSERT INTO war_room_posts VALUES (?, ?)", (symbol, message))
            conn.commit()
            state["posts_ok"].append(symbol)
        except sqlite3.OperationalError as exc:
            state["posts_failed"].append((symbol, str(exc)))
        finally:
            conn.close()

    monkeypatch.setattr(vs, "_alpaca_headers", lambda: {})
    monkeypatch.setattr(vs, "_fetch_snapshots", lambda symbols, headers: {s: {"sym": s} for s in rel_vol})
    monkeypatch.setattr(vs, "_session_fraction_elapsed", lambda: 1.0)
    monkeypatch.setattr(
        vs, "_parse_snapshot",
        lambda sym, snap, baselines, frac: {"relative_volume": rel_vol[sym], "price": 10.0, "gap_pct": 2.0, "dollar_volume": 5e6},
    )
    monkeypatch.setattr(vs, "_post_to_war_room", fake_post)
    baselines = types.ModuleType("engine.volume_baselines")
    baselines.get_baselines = lambda symbols: {}
    monkeypatch.setitem(sys.modules, "engine.volume_baselines", baselines)
    state["db"] = db
    state["rel_vol"] = rel_vol
    return state


def _red_rows(db):
    with sqlite3.connect(db) as c:
        return sorted(r[0] for r in c.execute("SELECT symbol FROM volume_alerts WHERE alert_type='red_alert'"))


def test_war_room_posts_never_contend_with_a_held_write_lock(harness):
    vs.red_alert_check()
    assert harness["posts_failed"] == []
    assert sorted(harness["posts_ok"]) == ["AAA", "BBB", "DDD"]
    assert _red_rows(harness["db"]) == ["AAA", "BBB", "DDD"]


def test_already_flagged_symbols_are_not_reinserted_but_critical_still_posts(harness):
    vs.red_alert_check()
    harness["posts_ok"].clear()
    vs.red_alert_check()
    assert harness["posts_failed"] == []
    assert harness["posts_ok"] == ["AAA"]  # CRITICAL re-posts every run; RED only on first flag
    assert _red_rows(harness["db"]) == ["AAA", "BBB", "DDD"]


def test_no_write_lock_is_held_during_any_post(harness, monkeypatch):
    lock_free_during_post = []

    def probing_post(symbol, message):
        conn = sqlite3.connect(harness["db"], timeout=0)
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.rollback()
            lock_free_during_post.append(True)
        except sqlite3.OperationalError:
            lock_free_during_post.append(False)
        finally:
            conn.close()

    monkeypatch.setattr(vs, "_post_to_war_room", probing_post)
    vs.red_alert_check()
    assert lock_free_during_post == [True, True, True]
