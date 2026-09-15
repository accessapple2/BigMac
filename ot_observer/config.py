"""Fixed configuration for the ot-mcp observer: paths, limits and the named-log enum."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

HOST = "127.0.0.1"
PORT = 8765
PUBLIC_HOSTNAME = "ot-mcp.ollietrades.com"

# Doctrine 2026-09-14: the observer must never be the thing holding a lock. Short busy
# timeout, a hard per-query time budget, one read-only handle per request.
BUSY_TIMEOUT_MS = 200
QUERY_BUDGET_MS = 2000

DEFAULT_LIMIT = 20
MAX_LIMIT = 100
PROMPT_TEXT_LIMIT = 1  # prompt_text is the largest field; one decision's prompt at a time

LOG_LINES_DEFAULT = 100
LOG_LINES_MAX = 500
LOG_BYTE_CAP = 8 * 1024 * 1024

SCHED_LOOKBACK_DEFAULT_MIN = 60
SCHED_LOOKBACK_MAX_MIN = 1440
SCHED_BYTE_CAP = 64 * 1024 * 1024
IN_FLIGHT_WARN_SECONDS = 300

FILTER_MAX_LEN = 64
CONTAINS_MAX_LEN = 100

# name -> (file under logs/, line format). Only these files are readable.
LOGS: dict[str, tuple[str, str]] = {
    "trader": ("trader.log", "rich"),
    "trader_error": ("trader_error.log", "time_only"),
    "ops_sentinel": ("hm_ops_sentinel_cron.log", "python_logging"),
    "origin_healthcheck": ("origin_healthcheck_cron.log", "iso_offset"),
    "signal_center": ("signal-center.log", "werkzeug"),
    "crusher": ("crusher.log", "date_cmd"),
    "watchdog_cron": ("watchdog_cron.log", "none"),
}


@dataclass(frozen=True)
class ObserverPaths:
    root: Path
    trader_db: Path
    signals_db: Path
    log_dir: Path
    kill_switch: Path
    audit_log: Path

    @classmethod
    def under(cls, root: Path) -> "ObserverPaths":
        root = Path(root)
        return cls(
            root=root,
            trader_db=root / "data" / "trader.db",
            signals_db=root / "signal-center" / "signals.db",
            log_dir=root / "logs",
            kill_switch=root / "data" / "ot_observer.disabled",
            audit_log=root / "logs" / "ot_observer.log",
        )

    @classmethod
    def default(cls) -> "ObserverPaths":
        return cls.under(REPO_ROOT)
