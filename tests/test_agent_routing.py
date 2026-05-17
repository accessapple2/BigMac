"""Tests for engine/agent_routing.py — HM-CN Phase 1.

Run standalone:
    venv/bin/python3 -m pytest tests/test_agent_routing.py -v

Tests use a disposable in-memory DB seeded with synthetic ai_players rows so they
do NOT touch production data/trader.db. Verifies the core resolver semantics:

  - Returns DB model_id when present
  - Falls back to AI_PLAYERS entry's model field if DB read returns empty/null
  - Honors halt_mode='full' by returning None from build_provider
  - Returns OllamaProvider instances for provider='ollama' entries
  - Skips silent on non-ollama provider types
  - compare_against_main_py_inventory finds disagreements
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from engine import agent_routing  # noqa: E402


def _seed_db(tmp_path, rows):
    """Create a disposable trader.db at tmp_path with synthetic ai_players rows."""
    db = tmp_path / "trader.db"
    conn = sqlite3.connect(db)
    conn.execute("""CREATE TABLE ai_players (
        id TEXT PRIMARY KEY,
        model_id TEXT,
        halt_mode TEXT DEFAULT 'active'
    )""")
    for r in rows:
        conn.execute(
            "INSERT INTO ai_players (id, model_id, halt_mode) VALUES (?,?,?)",
            (r["id"], r["model_id"], r.get("halt_mode", "active")),
        )
    conn.commit()
    conn.close()
    return db


def test_resolve_model_id_returns_db_value(tmp_path):
    db = _seed_db(tmp_path, [{"id": "alpha", "model_id": "ministral-3:3b"}])
    got = agent_routing._resolve_model_id("alpha", "fallback-model", db_path=db)
    assert got == "ministral-3:3b"


def test_resolve_model_id_fallback_when_row_missing(tmp_path):
    db = _seed_db(tmp_path, [])
    got = agent_routing._resolve_model_id("missing-agent", "qwen3:8b", db_path=db)
    assert got == "qwen3:8b"


def test_resolve_model_id_fallback_when_db_empty_string(tmp_path):
    db = _seed_db(tmp_path, [{"id": "alpha", "model_id": ""}])
    got = agent_routing._resolve_model_id("alpha", "fallback-model", db_path=db)
    assert got == "fallback-model"


def test_get_halt_mode_returns_value(tmp_path):
    db = _seed_db(tmp_path, [{"id": "alpha", "model_id": "m", "halt_mode": "exit_only"}])
    assert agent_routing._get_halt_mode("alpha", db_path=db) == "exit_only"


def test_get_halt_mode_returns_empty_for_missing(tmp_path):
    db = _seed_db(tmp_path, [])
    assert agent_routing._get_halt_mode("ghost", db_path=db) == ""


def test_build_provider_skips_halt_full(tmp_path):
    db = _seed_db(tmp_path, [
        {"id": "halted", "model_id": "ministral-3:3b", "halt_mode": "full"},
    ])
    entry = {"id": "halted", "provider": "ollama", "model": "ministral-3:3b"}
    got = agent_routing.build_provider(entry, db_path=db, default_url="http://test")
    assert got is None


def test_build_provider_returns_ollama_instance_for_active(tmp_path):
    db = _seed_db(tmp_path, [
        {"id": "live", "model_id": "ministral-3:3b", "halt_mode": "active"},
    ])
    entry = {"id": "live", "provider": "ollama", "model": "fallback-model",
             "url": "http://192.168.1.166:11434"}
    got = agent_routing.build_provider(entry, db_path=db, default_url="http://test")
    # We don't import OllamaProvider eagerly (lazy import inside the function);
    # verify by attribute access on the returned object.
    assert got is not None
    assert got.player_id == "live"
    assert got.model_id == "ministral-3:3b"  # DB value, not fallback
    assert got.url.startswith("http://192.168.1.166:11434")


def test_build_provider_uses_entry_url_then_default(tmp_path):
    db = _seed_db(tmp_path, [
        {"id": "live2", "model_id": "ministral-3:3b", "halt_mode": "active"},
    ])
    entry_no_url = {"id": "live2", "provider": "ollama", "model": "m"}
    got = agent_routing.build_provider(entry_no_url, db_path=db,
                                       default_url="http://default-host:11434")
    assert got is not None
    assert got.url.startswith("http://default-host:11434")


def test_build_provider_skips_unknown_provider_type(tmp_path):
    db = _seed_db(tmp_path, [
        {"id": "weird", "model_id": "m", "halt_mode": "active"},
    ])
    entry = {"id": "weird", "provider": "openai", "model": "m"}
    got = agent_routing.build_provider(entry, db_path=db, default_url="http://test")
    assert got is None


def test_build_provider_skips_when_no_model(tmp_path):
    db = _seed_db(tmp_path, [
        {"id": "no_model", "model_id": "", "halt_mode": "active"},
    ])
    entry = {"id": "no_model", "provider": "ollama", "model": ""}
    got = agent_routing.build_provider(entry, db_path=db, default_url="http://test")
    assert got is None


def test_build_all_providers_filters_halted_and_unknown(tmp_path):
    db = _seed_db(tmp_path, [
        {"id": "live", "model_id": "ministral-3:3b", "halt_mode": "active"},
        {"id": "halted", "model_id": "ministral-3:3b", "halt_mode": "full"},
        {"id": "exitonly", "model_id": "ministral-3:3b", "halt_mode": "exit_only"},
        {"id": "weird", "model_id": "ministral-3:3b", "halt_mode": "active"},
    ])
    entries = [
        {"id": "live", "provider": "ollama", "model": "m"},
        {"id": "halted", "provider": "ollama", "model": "m"},
        {"id": "exitonly", "provider": "ollama", "model": "m"},
        {"id": "weird", "provider": "openai", "model": "m"},
    ]
    got = agent_routing.build_all_providers(entries, db_path=db,
                                            default_url="http://t")
    pids = {p.player_id for p in got}
    assert pids == {"live", "exitonly"}, f"unexpected: {pids}"


def test_compare_against_main_py_inventory_finds_disagreements(tmp_path):
    db = _seed_db(tmp_path, [
        {"id": "agent_a", "model_id": "ministral-3:3b", "halt_mode": "active"},
        {"id": "agent_b", "model_id": "ministral-3:3b", "halt_mode": "active"},
        {"id": "agent_c", "model_id": "phi4:14b", "halt_mode": "active"},
    ])
    entries = [
        {"id": "agent_a", "provider": "ollama", "model": "stale-config-model"},
        {"id": "agent_b", "provider": "ollama", "model": "stale-config-model"},
        {"id": "agent_c", "provider": "ollama", "model": "stale-config-model"},
    ]
    # main.py inventory: agent_a agrees with DB; agent_b disagrees; agent_c also disagrees
    main_py_inv = {
        "agent_a": "ministral-3:3b",
        "agent_b": "qwen3:8b",           # disagrees: DB has ministral-3:3b
        "agent_c": "qwen2.5-coder:7b",   # disagrees: DB has phi4:14b
    }
    disagreements = agent_routing.compare_against_main_py_inventory(
        entries, main_py_inv, db_path=db
    )
    pids = sorted(d["agent_id"] for d in disagreements)
    assert pids == ["agent_b", "agent_c"], f"unexpected disagreements: {pids}"
    # Verify shape of disagreement dict
    d_b = next(d for d in disagreements if d["agent_id"] == "agent_b")
    assert d_b["main_py_model"] == "qwen3:8b"
    assert d_b["ai_players_model_id"] == "ministral-3:3b"
    assert d_b["halt_mode"] == "active"
    assert d_b["config_AI_PLAYERS_model"] == "stale-config-model"
