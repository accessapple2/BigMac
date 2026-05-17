"""Tests for engine/agent_routing.py — HM-CN Phase 2 (Option B).

Run standalone:
    venv/bin/python3 -m pytest tests/test_agent_routing.py -v

Tests use a disposable in-memory DB seeded with synthetic ai_players rows so
they do NOT touch production data/trader.db. Covers:

Phase 1 baseline (12 tests):
  - DB model_id read, fallback semantics, halt mode lookup
  - build_provider construction + skips
  - build_all_providers list filtering
  - compare_against_main_py_inventory disagreement detection

Phase 2 (Option B) additions:
  - DB-iterated build_all_providers() (no ai_players_list arg)
  - halt_mode='full' rows are skipped at DB iteration step
  - local_redirect=True overrides non-ollama provider check
  - Missing config.AI_PLAYERS entry falls back to defaults
  - DB authoritative for model_id even when config has stale fallback
  - 23-agent fleet smoke (production-shape)
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
    """Create a disposable trader.db at tmp_path with synthetic ai_players rows.

    Schema includes `provider` column (Phase 2 — DB.provider is read during
    iteration to filter non-LLM agents).
    """
    db = tmp_path / "trader.db"
    conn = sqlite3.connect(db)
    conn.execute("""CREATE TABLE ai_players (
        id TEXT PRIMARY KEY,
        model_id TEXT,
        halt_mode TEXT DEFAULT 'active',
        provider TEXT DEFAULT 'ollama'
    )""")
    for r in rows:
        conn.execute(
            "INSERT INTO ai_players (id, model_id, halt_mode, provider) VALUES (?,?,?,?)",
            (r["id"], r["model_id"], r.get("halt_mode", "active"), r.get("provider", "ollama")),
        )
    conn.commit()
    conn.close()
    return db


# ── Phase 1 baseline tests ─────────────────────────────────────────────────


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


def test_build_provider_skips_unknown_provider_type_no_redirect(tmp_path):
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


def test_build_all_providers_filters_halted_and_unknown_with_list(tmp_path):
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
    main_py_inv = {
        "agent_a": "ministral-3:3b",
        "agent_b": "qwen3:8b",
        "agent_c": "qwen2.5-coder:7b",
    }
    disagreements = agent_routing.compare_against_main_py_inventory(
        entries, main_py_inv, db_path=db
    )
    pids = sorted(d["agent_id"] for d in disagreements)
    assert pids == ["agent_b", "agent_c"]


# ── Phase 2 (Option B) additions ───────────────────────────────────────────


def test_db_iter_returns_all_rows(tmp_path):
    db = _seed_db(tmp_path, [
        {"id": "a", "model_id": "ma", "halt_mode": "active"},
        {"id": "b", "model_id": "mb", "halt_mode": "exit_only"},
        {"id": "c", "model_id": "mc", "halt_mode": "full"},
    ])
    rows = agent_routing._iter_ai_players_db(db_path=db)
    assert len(rows) == 3
    # Phase 2: now returns (id, model_id, halt_mode, provider)
    assert rows[0] == ("a", "ma", "active", "ollama")


def test_db_iter_skips_non_ollama_provider_at_default_iteration(tmp_path):
    """DB rows with provider!='ollama' (e.g., 'system', 'matrix', 'sync')
    should NOT construct providers in DB-iterated mode unless config has a
    local_redirect override. This catches the data-feed-agent bug where
    capitol-trades / neo-matrix / alpaca-mirror were getting bogus Ollama
    constructions."""
    db = _seed_db(tmp_path, [
        {"id": "ollama-agent", "model_id": "ministral-3:3b", "halt_mode": "active", "provider": "ollama"},
        {"id": "data-feed",    "model_id": "congress-copycat", "halt_mode": "active", "provider": "system"},
        {"id": "deterministic", "model_id": "8000 / Independent", "halt_mode": "active", "provider": "matrix"},
    ])
    got = agent_routing.build_all_providers(
        db_path=db, default_url="http://t", config_overrides={}
    )
    pids = sorted(p.player_id for p in got)
    assert pids == ["ollama-agent"], f"unexpected: {pids}"


def test_build_all_providers_db_iterated_no_arg(tmp_path):
    """When ai_players_list is None, builder iterates DB directly."""
    db = _seed_db(tmp_path, [
        {"id": "alpha", "model_id": "ministral-3:3b", "halt_mode": "active"},
        {"id": "halted", "model_id": "ministral-3:3b", "halt_mode": "full"},
        {"id": "beta", "model_id": "qwen3:8b", "halt_mode": "exit_only"},
    ])
    got = agent_routing.build_all_providers(
        db_path=db, default_url="http://t", config_overrides={}
    )
    pids = sorted(p.player_id for p in got)
    assert pids == ["alpha", "beta"]  # halted skipped


def test_local_redirect_overrides_openai_provider(tmp_path):
    """A config entry with provider=openai + local_redirect=True still
    constructs an OllamaProvider (Free-Models-First)."""
    db = _seed_db(tmp_path, [
        {"id": "codex", "model_id": "ministral-3:3b", "halt_mode": "active"},
    ])
    config_overrides = {
        "codex": {"id": "codex", "provider": "openai",
                  "model": "gpt-5.2-codex",
                  "local_redirect": True,
                  "url": "http://192.168.1.166:11434"},
    }
    got = agent_routing.build_all_providers(
        db_path=db, default_url="http://t",
        config_overrides=config_overrides
    )
    assert len(got) == 1
    assert got[0].player_id == "codex"
    # DB model_id (ministral-3:3b), not config.AI_PLAYERS' "gpt-5.2-codex":
    assert got[0].model_id == "ministral-3:3b"


def test_db_authoritative_for_model_id(tmp_path):
    """Even when config.AI_PLAYERS has a stale model, DB wins."""
    db = _seed_db(tmp_path, [
        {"id": "agent", "model_id": "ministral-3:3b", "halt_mode": "active"},
    ])
    config_overrides = {
        "agent": {"id": "agent", "provider": "ollama",
                  "model": "old-stale-model-from-config"},
    }
    got = agent_routing.build_all_providers(
        db_path=db, default_url="http://t",
        config_overrides=config_overrides
    )
    assert len(got) == 1
    assert got[0].model_id == "ministral-3:3b"


def test_missing_config_entry_defaults_to_ollama(tmp_path):
    """DB has the agent but no config.AI_PLAYERS entry → defaults to ollama
    provider and uses default_url."""
    db = _seed_db(tmp_path, [
        {"id": "orphan", "model_id": "ministral-3:3b", "halt_mode": "active"},
    ])
    got = agent_routing.build_all_providers(
        db_path=db, default_url="http://default-fallback:11434",
        config_overrides={}
    )
    assert len(got) == 1
    assert got[0].player_id == "orphan"
    assert got[0].url.startswith("http://default-fallback:11434")


def test_unknown_provider_without_redirect_is_skipped(tmp_path):
    """DB.provider=anthropic without local_redirect → no provider built.

    Phase 2 (Option B): DB.provider is authoritative for routing decisions.
    Config can override with local_redirect=True to force Ollama construction
    (Free-Models-First case).
    """
    db = _seed_db(tmp_path, [
        {"id": "antho", "model_id": "ministral-3:3b", "halt_mode": "active",
         "provider": "anthropic"},
    ])
    config_overrides = {
        "antho": {"id": "antho", "provider": "anthropic", "model": "claude"},
    }
    got = agent_routing.build_all_providers(
        db_path=db, default_url="http://t",
        config_overrides=config_overrides
    )
    assert got == []


def test_local_redirect_overrides_non_ollama_db_provider(tmp_path):
    """DB.provider=openai + config local_redirect=True → forces Ollama
    construction (Free-Models-First doctrine, even when DB says non-ollama)."""
    db = _seed_db(tmp_path, [
        {"id": "codex", "model_id": "ministral-3:3b", "halt_mode": "active",
         "provider": "openai"},
    ])
    config_overrides = {
        "codex": {"id": "codex", "provider": "openai",
                  "local_redirect": True,
                  "url": "http://192.168.1.166:11434"},
    }
    got = agent_routing.build_all_providers(
        db_path=db, default_url="http://t",
        config_overrides=config_overrides
    )
    assert len(got) == 1
    assert got[0].player_id == "codex"
    assert got[0].model_id == "ministral-3:3b"


def test_db_iterated_full_fleet_shape(tmp_path):
    """Synthetic 23-active-agent shape proves DB-iterated path handles
    the production fleet correctly."""
    rows = [
        # 5 previously-BREAK active agents (Phase 2 coverage gap fix targets)
        {"id": "cto-grok42",      "model_id": "devstral-small-2", "halt_mode": "active"},
        {"id": "dalio-metals",    "model_id": "ministral-3:3b",   "halt_mode": "active"},
        {"id": "energy-arnold",   "model_id": "ministral-3:3b",   "halt_mode": "active"},
        {"id": "options-sosnoff", "model_id": "gemma4:31b",       "halt_mode": "active"},
        {"id": "qwen3-8b-sonnet", "model_id": "ministral-3:3b",   "halt_mode": "active"},
        # 5 already-covered active agents
        {"id": "ollama-qwen3",    "model_id": "ministral-3:3b",   "halt_mode": "active"},
        {"id": "ollama-coder",    "model_id": "devstral-small-2", "halt_mode": "active"},
        {"id": "ollama-plutus",   "model_id": "ministral-3:3b",   "halt_mode": "active"},
        {"id": "mlx-qwen3",       "model_id": "ministral-3:3b",   "halt_mode": "active"},
        {"id": "navigator",       "model_id": "gemma4:26b",       "halt_mode": "active"},
        # Zombies (skipped)
        {"id": "ollama-gemma27b", "model_id": "ministral-3:3b",   "halt_mode": "full"},
        {"id": "qwen3-14b-grok3", "model_id": "qwen3:8b",         "halt_mode": "full"},
    ]
    db = _seed_db(tmp_path, rows)
    config_overrides = {
        # Free-Models-First flag for qwen3-8b-sonnet
        "qwen3-8b-sonnet": {"id": "qwen3-8b-sonnet", "provider": "openai",
                             "local_redirect": True},
        # Most others default-route to ollama
    }
    got = agent_routing.build_all_providers(
        db_path=db, default_url="http://t",
        config_overrides=config_overrides
    )
    pids = sorted(p.player_id for p in got)
    expected = sorted([
        "cto-grok42", "dalio-metals", "energy-arnold", "options-sosnoff",
        "qwen3-8b-sonnet",
        "ollama-qwen3", "ollama-coder", "ollama-plutus", "mlx-qwen3",
        "navigator",
    ])
    assert pids == expected, f"missing: {set(expected) - set(pids)}, extra: {set(pids) - set(expected)}"
