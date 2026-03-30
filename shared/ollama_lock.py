"""
File-based Ollama lock for 16GB Mac Mini.

Prevents the arena scanner and CrewAI pipeline from calling Ollama
simultaneously. Uses fcntl.flock — macOS native, zero dependencies.

Usage:
    with OllamaLock("crew_pipeline"):
        crew.kickoff()

    with OllamaLock("arena_scanner"):
        arena.run_scan(tickers)
"""

import fcntl
import json
import os
import time

LOCK_PATH = "/tmp/trademinds-ollama.lock"


class OllamaLock:
    """Exclusive file lock for Ollama access."""

    def __init__(self, caller_name: str, timeout: int = 300):
        self.caller_name = caller_name
        self.timeout = timeout
        self._fd = None

    def acquire(self):
        """Acquire the lock. Blocks until available or timeout."""
        # Open without truncating so we can read holder info on timeout
        if not os.path.exists(LOCK_PATH):
            open(LOCK_PATH, "a").close()
        self._fd = open(LOCK_PATH, "r+")
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                # Got the lock — write owner info for debugging
                self._fd.seek(0)
                self._fd.truncate()
                self._fd.write(json.dumps({
                    "caller": self.caller_name,
                    "pid": os.getpid(),
                    "acquired_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                }))
                self._fd.flush()
                return
            except (OSError, BlockingIOError):
                if time.monotonic() >= deadline:
                    holder = _read_holder()
                    self._fd.close()
                    self._fd = None
                    raise TimeoutError(
                        f"Ollama lock timeout after {self.timeout}s. "
                        f"Held by: {holder}"
                    )
                time.sleep(0.5)

    def release(self):
        """Release the lock."""
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
                self._fd.close()
            except OSError:
                pass
            self._fd = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc):
        self.release()
        return False

    @classmethod
    def force_release(cls):
        """Manual override — remove the lock file if a process crashed."""
        try:
            os.remove(LOCK_PATH)
        except FileNotFoundError:
            pass


def _read_holder() -> str:
    """Read who currently holds the lock, for error messages."""
    try:
        with open(LOCK_PATH) as f:
            data = json.loads(f.read())
        return f"{data['caller']} (PID {data['pid']}, since {data['acquired_at']})"
    except Exception:
        return "unknown"


def cleanup_stale_lock() -> str | None:
    """
    Called at server startup. If the lock file exists and the PID recorded in
    it is no longer running, remove the file so the next acquire() doesn't
    wait on a ghost process.

    Returns a log message if the lock was cleared, None if nothing to do.
    """
    if not os.path.exists(LOCK_PATH):
        return None
    try:
        with open(LOCK_PATH) as f:
            raw = f.read().strip()
        if not raw:
            os.remove(LOCK_PATH)
            return "Startup: removed empty Ollama lock file."
        data = json.loads(raw)
        pid = data.get("pid")
        caller = data.get("caller", "unknown")
        acquired_at = data.get("acquired_at", "?")
        if pid is None:
            os.remove(LOCK_PATH)
            return f"Startup: removed Ollama lock with no PID (caller={caller})."
        # os.kill(pid, 0) raises ProcessLookupError if the process is dead
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            os.remove(LOCK_PATH)
            return (
                f"Startup: cleared stale Ollama lock — PID {pid} ({caller}) "
                f"acquired at {acquired_at} is no longer running."
            )
        except PermissionError:
            # Process exists but we can't signal it — leave the lock alone
            return None
    except Exception:
        pass
    return None


def get_lock_status() -> dict:
    """Return current lock state — caller, PID, elapsed seconds, and whether it's held."""
    if not os.path.exists(LOCK_PATH):
        return {"locked": False, "caller": None, "pid": None, "acquired_at": None, "elapsed_seconds": 0}
    try:
        with open(LOCK_PATH) as f:
            raw = f.read().strip()
        if not raw:
            return {"locked": False, "caller": None, "pid": None, "acquired_at": None, "elapsed_seconds": 0}
        data = json.loads(raw)
        # Try a non-blocking lock attempt — if it succeeds, nobody holds it
        fd = open(LOCK_PATH, "r+")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fd, fcntl.LOCK_UN)
            fd.close()
            return {"locked": False, "caller": data.get("caller"), "pid": data.get("pid"),
                    "acquired_at": data.get("acquired_at"), "elapsed_seconds": 0}
        except (OSError, BlockingIOError):
            fd.close()
        # Lock IS held — compute elapsed
        acquired_at = data.get("acquired_at", "")
        elapsed = 0
        try:
            import datetime
            t = datetime.datetime.strptime(acquired_at, "%Y-%m-%d %H:%M:%S")
            elapsed = int((datetime.datetime.now() - t).total_seconds())
        except Exception:
            pass
        return {"locked": True, "caller": data.get("caller"), "pid": data.get("pid"),
                "acquired_at": acquired_at, "elapsed_seconds": elapsed}
    except Exception:
        return {"locked": False, "caller": None, "pid": None, "acquired_at": None, "elapsed_seconds": 0}
