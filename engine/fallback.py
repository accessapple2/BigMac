"""
Fallback model routing — when a paid model is paused, route scans to a free
local Ollama model instead of stopping completely.

The player keeps its identity, personality, and trading history.
Only the inference engine is swapped temporarily.

When the model is unpaused, the paid provider automatically resumes.
"""
from __future__ import annotations
import sqlite3
import threading
from config import OLLAMA_URL as _OLLAMA_URL, OLLIE_URL as _OLLIE_URL

DB = "data/trader.db"
_lock = threading.Lock()

# Fallback mapping: player_id → free Ollama model (Ollie GPU primary, bigmac localhost only
# for models that live on bigmac: qwen2.5-coder:7b).
# 2026-04-20: qwen3:8b replaced — was loading 8GB on bigmac localhost and causing swap storms.
FALLBACK_MODEL_MAP: dict[str, str] = {
    # xAI Grok players → Ollie GPU
    "qwen3-14b-grok3":           "qwen3:14b",         # heavy — was qwen3:8b@localhost
    "deepseek-7b-grok4":           "deepseek-r1:7b",    # Lt. Cmdr. Spock — reasoning model
    "cto-grok42":       "qwen2.5-coder:7b",  # stays bigmac (coder model lives there)
    # OpenAI players → Ollie GPU
    "qwen3-8b-4o":           "qwen3:8b",          # was qwen3:8b@localhost
    "qwen3-8b-o3":           "deepseek-r1:7b",    # keep deepseek → Ollie
    "qwen3-8b-sonnet":    "qwen3:8b",          # Captain Sisko — was qwen3:8b@localhost
    "qwen-coder-haiku":     "qwen2.5-coder:7b",  # stays bigmac (coder model lives there)
    # Gemini players → Ollie GPU
    "qwen3-8b-flash": "qwen3:8b",          # Lt. Cmdr. Worf — was qwen3:8b@localhost
    "qwen3-14b-pro":   "qwen3:14b",         # heavy — unchanged model, URL → Ollie
    "options-sosnoff":  "qwen3:8b",          # Counselor Troi — was qwen3:8b@localhost
    # Dalio → Ollie GPU
    "dalio-metals":     "qwen3:8b",          # Cmdr. Dalio — was qwen3:8b@localhost
    # CrewAI / Mr. Anderson → Ollie GPU
    "super-agent":      "qwen3:8b",          # Mr. Anderson — was deepseek-r1:7b@localhost
    # Groq (rate-limited) → Ollie GPU
    "ollama-llama":     "deepseek-r1:7b",    # Lt. Cmdr. Uhura → Ollie
}

# Agents whose fallback model lives on bigmac localhost (qwen2.5-coder only)
_BIGMAC_FALLBACK_IDS: frozenset[str] = frozenset({"cto-grok42", "qwen-coder-haiku"})

# URL map derived from the above — keeps URL logic co-located with model logic
FALLBACK_URL_MAP: dict[str, str] = {
    pid: (_OLLAMA_URL if pid in _BIGMAC_FALLBACK_IDS else _OLLIE_URL)
    for pid in FALLBACK_MODEL_MAP
}


def get_fallback_url(player_id: str) -> str:
    """Return the Ollama server URL to use for a player's fallback provider.

    Ollie GPU (192.168.1.166:11434) is primary for all non-coder fallbacks.
    Bigmac localhost is reserved for qwen2.5-coder:7b agents.
    """
    return FALLBACK_URL_MAP.get(player_id, _OLLAMA_URL)


def get_fallback_model(player_id: str) -> str | None:
    """Return the fallback Ollama model for a player.

    Checks DB override first (allows per-player customization at runtime),
    then falls back to the static FALLBACK_MODEL_MAP.
    """
    try:
        conn = sqlite3.connect(DB, check_same_thread=False, timeout=10)
        row = conn.execute(
            "SELECT fallback_model FROM ai_players WHERE id=?", (player_id,)
        ).fetchone()
        conn.close()
        if row and row[0]:
            return row[0]
    except Exception:
        pass
    return FALLBACK_MODEL_MAP.get(player_id)


def is_fallbacks_enabled() -> bool:
    """Check global fallbacks toggle. Default: ON."""
    try:
        conn = sqlite3.connect(DB, check_same_thread=False, timeout=10)
        row = conn.execute(
            "SELECT value FROM settings WHERE key='fallbacks_enabled'"
        ).fetchone()
        conn.close()
        return not (row and row[0] == "0")
    except Exception:
        pass
    return True


def set_player_fallback_state(player_id: str, active: bool) -> None:
    """Set is_fallback flag for a player. Also updates cost_tracker in-memory set."""
    # Update cost tracker first (in-memory, no DB round-trip on hot path)
    try:
        from engine.cost_tracker import mark_player_fallback
        mark_player_fallback(player_id, active)
    except Exception:
        pass
    # Persist to DB
    with _lock:
        try:
            conn = sqlite3.connect(DB, check_same_thread=False, timeout=10)
            conn.execute(
                "UPDATE ai_players SET is_fallback=? WHERE id=?",
                (1 if active else 0, player_id)
            )
            conn.commit()
            conn.close()
        except Exception:
            pass


def init_fallback_columns() -> None:
    """Migrate DB: add fallback_model and is_fallback columns if missing, seed values."""
    conn = sqlite3.connect(DB, check_same_thread=False, timeout=10)
    for stmt in [
        "ALTER TABLE ai_players ADD COLUMN fallback_model TEXT",
        "ALTER TABLE ai_players ADD COLUMN is_fallback INTEGER DEFAULT 0",
    ]:
        try:
            conn.execute(stmt)
        except Exception:
            pass  # Column already exists
    # Seed fallback_model values (don't overwrite existing custom values)
    for pid, model in FALLBACK_MODEL_MAP.items():
        try:
            conn.execute(
                "UPDATE ai_players SET fallback_model=? WHERE id=? "
                "AND (fallback_model IS NULL OR fallback_model='')",
                (model, pid)
            )
        except Exception:
            pass
    # Seed fallbacks_enabled default setting
    try:
        conn.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES ('fallbacks_enabled', '1')"
        )
    except Exception:
        pass
    conn.commit()
    conn.close()
