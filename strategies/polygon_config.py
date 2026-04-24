"""
Polygon.io API key management.

Key is read from the POLYGON_API_KEY environment variable.
No key = client returns None on every call (falls back to mock_data).
"""
from __future__ import annotations
import os
from pathlib import Path
from typing import Optional


def _load_dotenv() -> None:
    """Load .env from project root if present. No-op if already in environment."""
    env_path = Path(__file__).parent.parent / ".env"
    if not env_path.exists():
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


# Load on import so callers don't have to think about it
_load_dotenv()


def _get_api_key() -> Optional[str]:
    """Return the Polygon API key, or None if not configured."""
    return os.environ.get("POLYGON_API_KEY") or None


def is_polygon_configured() -> bool:
    """True when a non-empty API key is available."""
    return bool(_get_api_key())


def api_key_preview() -> str:
    """
    Return a masked preview of the key for safe logging.
    Format: first 4 chars + '...' + last 4 chars, e.g. 'abcd...wxyz'.
    Returns 'NOT SET' if unconfigured.
    """
    key = _get_api_key()
    if not key:
        return "NOT SET"
    if len(key) <= 8:
        return "*" * len(key)
    return f"{key[:4]}...{key[-4:]}"
