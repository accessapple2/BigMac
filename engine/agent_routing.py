"""HM-CN 2026-05-17 — canonical agent routing (Phase 2 Option B).

Single source of truth = `ai_players` DB. Iterates DB rows directly as the
authoritative agent set. `config.AI_PLAYERS` supplies optional static metadata
(provider, url, timeout, local_redirect) per-agent.

Halt semantics: halt_mode='full' agents are skipped. halt_mode in {'active',
'exit_only'} agents ARE constructed.

Provider routing:
- DB row's config.AI_PLAYERS entry provider="ollama"  → OllamaProvider
- DB row has no config entry                         → defaults to OllamaProvider
- config entry has "local_redirect": True             → OllamaProvider regardless
  of declared provider (Free-Models-First doctrine — provider="openai"/etc
  with local_redirect=True routes to local Ollama using DB model_id)
- config entry provider in {"openai","anthropic","xai","sync","system","matrix"}
  WITHOUT local_redirect → skipped (handled by other code paths if needed)

Phase 2-B status: Module integrated into main.py via single
`build_all_providers()` call. The `ai_players_list` arg is retained for
test compatibility (pass a synthetic list) but main.py invocation uses
the no-arg DB-iterated form.
"""
from __future__ import annotations
import logging
import sqlite3
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

logger = logging.getLogger("agent_routing")
logger.setLevel(logging.INFO)

_DB_DEFAULT = Path(__file__).resolve().parent.parent / "data" / "trader.db"

# Providers that should always route to OllamaProvider when local_redirect is
# absent. Any provider value not in this set will be skipped unless flagged
# with local_redirect=True.
_OLLAMA_PROVIDER_VALUES = {"ollama"}


def _iter_ai_players_db(db_path: Path = _DB_DEFAULT) -> List[Tuple[str, str, str, str]]:
    """Return (id, model_id, halt_mode, provider) for every row in ai_players.

    DB.provider is needed to filter out non-LLM agents (e.g., capitol-trades
    has provider='system' / model_id='congress-copycat' — must NOT construct
    an Ollama provider for it).
    """
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        rows = conn.execute(
            "SELECT id, model_id, halt_mode, provider FROM ai_players ORDER BY id"
        ).fetchall()
        conn.close()
        return rows
    except Exception as e:
        logger.warning(f"DB iteration failed: {e}")
        return []


def _resolve_model_id(player_id: str, fallback: str,
                     db_path: Path = _DB_DEFAULT) -> str:
    """Read ai_players.model_id for player_id. Fall back to caller-supplied
    value if DB read fails or returns empty.
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


def _build_config_overrides(config_overrides: Optional[dict] = None) -> dict:
    """Build {agent_id: config_entry} lookup from config.AI_PLAYERS (or caller-
    supplied dict for tests).
    """
    if config_overrides is not None:
        return config_overrides
    try:
        from config import AI_PLAYERS as _AIP
        return {p["id"]: p for p in _AIP}
    except Exception as e:
        logger.warning(f"config.AI_PLAYERS import failed: {e}")
        return {}


def build_provider(player_entry: dict,
                   db_path: Path = _DB_DEFAULT,
                   default_url: Optional[str] = None,
                   default_timeout: int = 85):  # HM-WR-CANCEL-ON-TIMEOUT: under WR's 90s
    """Construct an OllamaProvider from a synthetic player entry.

    Entry shape (built either from a config.AI_PLAYERS row or a DB row):
      id            (required)
      provider      (optional, default 'ollama')
      model         (optional fallback model — DB value preferred)
      url           (optional, default = default_url)
      timeout       (optional, default = default_timeout)
      local_redirect (optional, default False)

    Returns the constructed provider, or None to skip.
    """
    pid = player_entry["id"]
    halt = _get_halt_mode(player_id=pid, db_path=db_path)
    if halt == "full":
        logger.info(f"skipping halted provider for {pid} (halt_mode='full')")
        return None

    provider = player_entry.get("provider", "ollama")
    local_redirect = bool(player_entry.get("local_redirect", False))

    # Resolve model: DB authoritative, with caller's fallback
    model = _resolve_model_id(pid, player_entry.get("model", ""), db_path=db_path)
    if not model:
        logger.warning(f"no model resolvable for {pid}; skipping provider")
        return None

    # HM-Q-WARROOM 2026-06-06: Q (q-witness) — the Admiral-approved paid xAI
    # debate voice. This is the explicit routing path that gets the real-xAI
    # GrokProvider into the War Room providers dict (otherwise the non-ollama
    # filter below would skip it). Q_ENABLED is the kill switch; the provider
    # itself degrades to local Ollama on xAI failure or when its daily cost cap
    # (Q_DAILY_COST_CAP) is hit, so the debate never blocks.
    if pid == "q-witness":
        import os as _os
        if _os.getenv("Q_ENABLED", "1").lower() not in ("1", "true", "yes", "on"):
            logger.info("Q (q-witness) War Room voice disabled (Q_ENABLED off) — skipping")
            return None
        from engine.providers.grok_provider import GrokProvider
        return GrokProvider(player_id=pid, model=model, display_name="Q", use_xai=True)

    # Route to OllamaProvider if explicitly ollama OR local_redirect=True
    if provider in _OLLAMA_PROVIDER_VALUES or local_redirect:
        from engine.providers.ollama_provider import OllamaProvider
        url = player_entry.get("url") or default_url
        timeout = player_entry.get("timeout", default_timeout)
        return OllamaProvider(player_id=pid, model=model, url=url, timeout=timeout)

    # Unhandled provider type (openai/anthropic/xai/system/etc.) without
    # local_redirect → skip; let other code paths handle (e.g., GroqProvider
    # for ollama-llama in main.py).
    logger.debug(f"skipping {pid}: provider={provider!r} no local_redirect")
    return None


def build_all_providers(ai_players_list: Optional[Iterable[dict]] = None,
                        db_path: Path = _DB_DEFAULT,
                        default_url: Optional[str] = None,
                        default_timeout: int = 85,  # HM-WR-CANCEL-ON-TIMEOUT: under WR's 90s
                        config_overrides: Optional[dict] = None,
                        skip_ids: Optional[set] = None) -> List:
    """Build providers for all active agents.

    Default (production): iterate ai_players DB; layer config.AI_PLAYERS on
    top as metadata overlay.

    Test mode: pass `ai_players_list` directly. `config_overrides` can also
    be supplied to override the global AI_PLAYERS lookup.

    Halt='full' agents are skipped at the DB iteration step (and again at
    build_provider as belt-and-suspenders).
    """
    overrides = _build_config_overrides(config_overrides)
    skip = skip_ids or set()

    if ai_players_list is None:
        # DB-iterated path
        synthetic_entries: List[dict] = []
        for pid, model_id, halt_mode, db_provider in _iter_ai_players_db(db_path=db_path):
            if halt_mode == "full" or pid in skip:
                continue
            cfg = overrides.get(pid, {})
            # Provider precedence: DB.provider authoritative for routing. Config
            # supplies the local_redirect flag (Free-Models-First override) for
            # cases where DB.provider != 'ollama' but we want Ollama routing
            # anyway. Without local_redirect, non-ollama DB providers are
            # correctly skipped (data-feed agents like alpaca-mirror, neo-matrix
            # / matrix-provider deterministic agents, capitol-trades / system,
            # etc. — they must not be constructed as Ollama).
            provider = db_provider or cfg.get("provider", "ollama")
            synthetic_entries.append({
                "id": pid,
                "provider": provider,
                "model": model_id,  # DB authoritative
                "url": cfg.get("url"),
                "timeout": cfg.get("timeout", default_timeout),
                "local_redirect": cfg.get("local_redirect", False),
            })
        ai_players_list = synthetic_entries

    providers: List = []
    for entry in ai_players_list:
        if entry.get("id") in skip:
            continue
        p = build_provider(entry, db_path=db_path,
                           default_url=default_url,
                           default_timeout=default_timeout)
        if p is not None:
            providers.append(p)
    return providers


def compare_against_main_py_inventory(ai_players_list: Iterable[dict],
                                       main_py_inventory: dict,
                                       db_path: Path = _DB_DEFAULT) -> List[dict]:
    """Read-only diagnostic for the side-by-side script. Compares main.py's
    hardcoded model (passed via `main_py_inventory[pid] = model`) against
    what agent_routing.py would return (= DB model_id).
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
