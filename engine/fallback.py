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

DB = "data/trader.db"
_lock = threading.Lock()

# Fallback mapping: player_id → free local Ollama model
FALLBACK_MODEL_MAP: dict[str, str] = {
    # xAI Grok players → free local Ollama
    "grok-3":           "qwen3.5:9b",
    "grok-4":           "deepseek-r1:7b",    # Lt. Cmdr. Spock — reasoning model
    "cto-grok42":       "qwen2.5-coder:7b",
    # OpenAI players → free local Ollama
    "gpt-4o":           "qwen3.5:9b",
    "gpt-o3":           "deepseek-r1:7b",
    "claude-sonnet":    "qwen3.5:9b",        # Captain Sisko
    "claude-haiku":     "qwen2.5-coder:7b",  # Lt. Malcolm Reed
    # Gemini players → qwen3.5:9b
    "gemini-2.5-flash": "qwen3.5:9b",        # Lt. Cmdr. Worf
    "gemini-2.5-pro":   "qwen3:14b",
    "options-sosnoff":  "qwen3.5:9b",        # Counselor Troi
    # Dalio → qwen3.5:9b
    "dalio-metals":     "qwen3.5:9b",        # Cmdr. Dalio
    # CrewAI / Mr. Anderson → deepseek-r1:7b
    "super-agent":      "deepseek-r1:7b",    # Mr. Anderson (The One)
    # Groq (rate-limited) → deepseek-r1:7b
    "ollama-llama":     "deepseek-r1:7b",    # Lt. Cmdr. Uhura
}


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
