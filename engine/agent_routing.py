"""HM-CN 2026-05-17 — canonical agent routing.

Single source of truth = ai_players.model_id. Reads config.AI_PLAYERS for static
identity (id, provider, optional url, optional timeout). Returns provider
instances ready to register with the Arena.

Halt semantics: halt_mode='full' agents are NOT instantiated (they can't fire).
halt_mode='exit_only' AND 'active' agents ARE instantiated.

Status: Phase 1 build-only — module exists on disk but is NOT yet imported by
main.py. Integration is Phase 2 pending Admiral approval of side-by-side
findings.

Design: reports/HM-CN_agent_routing_design.md
"""
from __future__ import annotations
import logging
import sqlite3
from pathlib import Path
from typing import Iterable, List, Optional

logger = logging.getLogger("agent_routing")
logger.setLevel(logging.INFO)

_DB_DEFAULT = Path(__file__).resolve().parent.parent / "data" / "trader.db"


def _resolve_model_id(player_id: str, fallback: str,
                     db_path: Path = _DB_DEFAULT) -> str:
    """Read ai_players.model_id for player_id. Fall back to AI_PLAYERS-entry
    model field if DB read fails or returns empty.

    Same pattern as scripts/backtest_baseline.py::_resolve_model (commit 071fa75).
    """
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        row = conn.execute(
            "SELECT model_id FROM ai_players WHERE id=?", (player_id,)
        ).fetchone()
        conn.close()
        if row and row[0]:
            return row[0]
    except Exception as e:
        logger.warning(f"DB model_id read failed for {player_id}: {e}")
    return fallback


def _get_halt_mode(player_id: str, db_path: Path = _DB_DEFAULT) -> str:
    """Return 'active', 'exit_only', 'full', or '' if row missing."""
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        row = conn.execute(
            "SELECT halt_mode FROM ai_players WHERE id=?", (player_id,)
        ).fetchone()
        conn.close()
        return row[0] if row else ""
    except Exception:
        return ""


def build_provider(player_entry: dict,
                   db_path: Path = _DB_DEFAULT,
                   default_url: Optional[str] = None,
                   default_timeout: int = 180):
    """Construct an OllamaProvider (or other provider) from an AI_PLAYERS
    entry. Returns None for halt_mode='full' (skip — can't fire).

    Provider class import is lazy to avoid circular imports.
    """
    pid = player_entry["id"]
    halt = _get_halt_mode(player_id=pid, db_path=db_path)
    if halt == "full":
        logger.info(f"skipping halted provider for {pid} (halt_mode='full')")
        return None

    provider = player_entry.get("provider", "ollama")
    model = _resolve_model_id(pid, player_entry.get("model", ""), db_path=db_path)
    if not model:
        logger.warning(f"no model resolvable for {pid}; skipping provider")
        return None

    if provider == "ollama":
        from engine.providers.ollama_provider import OllamaProvider
        url = player_entry.get("url") or default_url
        timeout = player_entry.get("timeout", default_timeout)
        return OllamaProvider(player_id=pid, model=model, url=url, timeout=timeout)

    if provider == "mlx":
        try:
            from engine.providers.mlx_provider import MLXProvider
            return MLXProvider(player_id=pid, model=model)
        except ImportError:
            logger.error(f"mlx provider requested for {pid} but mlx_provider import failed")
            return None

    logger.debug(f"unhandled provider type {provider!r} for {pid}; skipping in HM-CN routing")
    return None


def build_all_providers(ai_players_list: Iterable[dict],
                        db_path: Path = _DB_DEFAULT,
                        default_url: Optional[str] = None,
                        default_timeout: int = 180) -> List:
    """Build providers for all entries in AI_PLAYERS. Skips halted + non-Ollama.
    Returns list of provider instances ready to pass into Arena().
    """
    providers = []
    for entry in ai_players_list:
        p = build_provider(entry, db_path=db_path,
                           default_url=default_url,
                           default_timeout=default_timeout)
        if p is not None:
            providers.append(p)
    return providers


def compare_against_main_py_inventory(ai_players_list: Iterable[dict],
                                       main_py_inventory: dict,
                                       db_path: Path = _DB_DEFAULT) -> List[dict]:
    """Read-only diagnostic: for each agent in AI_PLAYERS, compare main.py's
    hardcoded model (passed via `main_py_inventory[pid] = model`) against
    what agent_routing.py would return.

    Returns a list of disagreement dicts with full context for the
    side-by-side report.
    """
    disagreements = []
    for entry in ai_players_list:
        pid = entry["id"]
        if pid not in main_py_inventory:
            continue
        main_model = main_py_inventory[pid]
        routing_model = _resolve_model_id(pid, entry.get("model", ""),
                                          db_path=db_path)
        if main_model != routing_model:
            disagreements.append({
                "agent_id": pid,
                "main_py_model": main_model,
                "ai_players_model_id": routing_model,
                "halt_mode": _get_halt_mode(pid, db_path=db_path),
                "config_AI_PLAYERS_model": entry.get("model", ""),
            })
    return disagreements
