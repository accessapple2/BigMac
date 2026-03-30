"""Shared utilities for USS TradeMinds."""

try:
    from shared.ollama_lock import OllamaLock
    __all__ = ["OllamaLock"]
except Exception:
    # ollama_lock uses str | None union syntax (Python 3.10+);
    # arena server runs Python 3.9 — skip this import there.
    pass
