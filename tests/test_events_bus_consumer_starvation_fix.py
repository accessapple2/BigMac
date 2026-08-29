"""tests/test_events_bus_consumer_starvation_fix.py — HM-SIGNALS-V2-STARVATION-RECURRENCE.

engine.events_bus_consumer.consume_pending_signals() used to be pure
newest-first (created_at DESC LIMIT max_batch) -- a batch of old pending
rows could be permanently starved if fresh rows kept arriving every tick,
even with plenty of aggregate daily drain capacity. This is the exact
mechanism that produced three real incidents (2026-07-06, -09, -29). The
fix reserves a couple of every tick's slots for the single oldest pending
row(s) regardless of source recency, guaranteeing forward progress on the
tail of the queue.

These tests exercise the SELECTION behavior specifically (which rows a
tick would touch) against a temp DB, with buy()/price lookups mocked out
so nothing hits a real API or the live trader.db.
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import engine.events_bus_consumer as ebc  # noqa: E402


def _make_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE signals_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, source TEXT,
            signal_type TEXT, symbol TEXT, direction TEXT, confidence REAL,
            regime_fit TEXT, timeframe TEXT, strategy_tag TEXT, event_id TEXT,
            agent_debate_id TEXT, prompt_version TEXT, metadata TEXT,
            status TEXT, stale_after TEXT, trade_id INTEGER, created_at TEXT
        )
    """)
    conn.commit()
    conn.close()


def _insert(path, source, symbol, created_at, status="pending"):
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO signals_v2 (source, symbol, status, created_at) VALUES (?,?,?,?)",
        (source, symbol, status, created_at),
    )
    conn.commit()
    conn.close()


def _run_with_mocks(db_path, max_batch=10):
    """Run consume_pending_signals against the temp DB with buy()/price
    mocked to always fail softly (no price) -- keeps rows 'pending' so we
    can inspect exactly which ones the SELECT touched via scanned count
    and by checking which symbols got a [no price] skip logged, without
    needing a full paper_trader.buy() mock."""
    processed_symbols = []

    def _fake_get_current_price(symbol):
        processed_symbols.append(symbol)
        return None  # soft-skip: row stays pending, doesn't touch buy()

    with patch.object(ebc, "_DB_PATH", Path(db_path)), \
         patch("engine.chekov_autotrade._get_current_price", side_effect=_fake_get_current_price):
        stats = ebc.consume_pending_signals(max_batch=max_batch)
    return stats, processed_symbols


def test_oldest_row_reached_despite_heavy_newer_volume():
    """The exact starvation scenario: one ancient row, then far more than
    max_batch newer rows arriving after it. Pure newest-first would never
    touch the old row; the hybrid fix must always include it."""
    with tempfile.TemporaryDirectory() as d:
        path = str(Path(d) / "t.db")
        _make_db(path)
        _insert(path, "ollama-plutus", "OLD", "2026-07-01 00:00:00")
        for i in range(20):
            _insert(path, "ollama-qwen3", f"NEW{i}", f"2026-08-29 12:{i:02d}:00")

        _, processed = _run_with_mocks(path, max_batch=10)
        assert "OLD" in processed, f"oldest row was starved; batch touched: {processed}"


def test_reserve_does_not_exceed_batch_when_queue_is_small():
    """With fewer pending rows than max_batch, nothing should crash or
    double-count -- every row gets processed exactly once."""
    with tempfile.TemporaryDirectory() as d:
        path = str(Path(d) / "t.db")
        _make_db(path)
        _insert(path, "ollama-plutus", "A", "2026-08-29 12:00:00")
        _insert(path, "ollama-plutus", "B", "2026-08-29 12:01:00")

        stats, processed = _run_with_mocks(path, max_batch=10)
        assert stats["scanned"] == 2
        assert sorted(processed) == ["A", "B"]


def test_newest_rows_still_prioritized_for_remaining_slots():
    """The bulk of each tick's capacity should still go to the newest
    rows -- only a small reserve goes to the oldest, not a full flip to
    FIFO (which would reintroduce the original 2026-07-06 problem)."""
    with tempfile.TemporaryDirectory() as d:
        path = str(Path(d) / "t.db")
        _make_db(path)
        _insert(path, "ollama-plutus", "OLDEST", "2026-07-01 00:00:00")
        for i in range(15):
            _insert(path, "ollama-qwen3", f"NEW{i}", f"2026-08-29 12:{i:02d}:00")

        stats, processed = _run_with_mocks(path, max_batch=10)
        assert stats["scanned"] == 10
        assert "OLDEST" in processed
        # the newest of the 15 (NEW14, NEW13, ...) should dominate the rest
        assert "NEW14" in processed
        assert "NEW13" in processed


def test_empty_queue_no_crash():
    with tempfile.TemporaryDirectory() as d:
        path = str(Path(d) / "t.db")
        _make_db(path)
        stats, processed = _run_with_mocks(path, max_batch=10)
        assert stats["scanned"] == 0
        assert processed == []
