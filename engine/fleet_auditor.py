"""Fleet Auditor v1 — autonomous health manifest generator.

Runs every 15 minutes (via launchd). Checks:
  - 10 scheduled job freshness (DB table recency)
  - 10 dashboard API endpoints (HTTP 200 + valid content)
  - 5 data freshness timestamps
  - Ollie GPU reachability
  - bigmac Ollama health

Writes: data/health_manifest.json
Fires: ntfy push to ollietrades-admin on UP↔DOWN state transitions
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import URLError

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH          = os.path.join(os.path.dirname(__file__), "..", "data", "trader.db")
MANIFEST_PATH    = os.path.join(os.path.dirname(__file__), "..", "data", "health_manifest.json")
DASHBOARD_URL    = "http://localhost:8080"
BIGMAC_OLLAMA    = "http://localhost:11434"
OLLIE_OLLAMA     = "http://192.168.1.166:11434"
NTFY_TOPIC       = os.environ.get("NTFY_ADMIN_TOPIC", "Ollie-Alert-35")
HTTP_TIMEOUT     = 8   # seconds for API checks
OLLAMA_TIMEOUT   = 5   # seconds for Ollama ping

# ── Database helper ────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def _age_minutes(ts_str: str | None) -> float | None:
    """Return minutes since an ISO timestamp, or None if unparseable."""
    if not ts_str:
        return None
    try:
        # Handle both 'YYYY-MM-DD HH:MM:SS' and ISO 8601 formats
        ts_str = ts_str.replace("T", " ").split(".")[0]
        dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        return (now - dt).total_seconds() / 60
    except Exception:
        return None


def _last_row_age(table: str, ts_col: str = "created_at") -> float | None:
    """Return minutes since the newest row in a table, or None if empty/error."""
    try:
        conn = _conn()
        row = conn.execute(
            f"SELECT MAX({ts_col}) as ts FROM {table}"
        ).fetchone()
        conn.close()
        return _age_minutes(row["ts"] if row else None)
    except Exception:
        return None


# ── Scheduled Job Freshness ────────────────────────────────────────────────────

def _check_scheduled_jobs() -> list[dict]:
    """Check 10 key scheduled jobs by DB table recency."""
    now_utc = datetime.now(timezone.utc)
    market_hour = 9 <= now_utc.hour <= 21  # loose UTC window for ET market hours

    jobs = [
        # (label, table, ts_col, stale_minutes, market_only)
        ("signals (scanner)",      "signals",           "created_at", 15,  True),
        ("war_room",               "war_room",          "created_at", 25,  True),
        ("portfolio_positions",    "portfolio_positions","updated_at", 20,  False),
        ("rikers_log",             "rikers_log",        "created_at", 25,  True),
        ("battle_station_log",     "battle_station_log","timestamp",  10,  True),
        ("gex_snapshots",          "gex_snapshots",     "created_at", 30,  False),
        ("picard_briefings",       "picard_briefings",  "generated_at",7*24*60,False),
        ("premarket_scan",         "premarket_scan",    "scanned_at", 30,  False),
        ("trades",                 "trades",            "executed_at",8*60,False),
        ("rikers_log (synthesis)", "rikers_log",        "created_at", 25,  True),
    ]

    results = []
    for label, table, ts_col, stale_mins, market_only in jobs:
        age = _last_row_age(table, ts_col)
        if age is None:
            status = "UNKNOWN"
            detail = f"{table} unavailable"
        elif market_only and not market_hour:
            status = "SKIP"
            detail = "market closed"
        elif age > stale_mins:
            status = "STALE"
            detail = f"last activity {age:.0f}m ago (threshold {stale_mins}m)"
        else:
            status = "OK"
            detail = f"last activity {age:.0f}m ago"

        results.append({
            "name": label,
            "table": table,
            "status": status,
            "age_minutes": round(age, 1) if age is not None else None,
            "stale_threshold_minutes": stale_mins,
            "detail": detail,
        })

    return results


# ── API Endpoint Health ────────────────────────────────────────────────────────

def _http_get(url: str, timeout: int = HTTP_TIMEOUT) -> tuple[int, dict | None]:
    """GET url, return (status_code, json_body_or_None)."""
    try:
        req = Request(url, headers={"Accept": "application/json"})
        with urlopen(req, timeout=timeout) as resp:
            code = resp.status
            body = json.loads(resp.read().decode("utf-8", errors="replace"))
            return code, body
    except URLError as e:
        reason = getattr(e, "reason", str(e))
        return 0, {"error": str(reason)}
    except Exception as e:
        return 0, {"error": str(e)}


def _check_api_endpoints() -> list[dict]:
    """Check 10 dashboard API endpoints."""
    endpoints = [
        ("/api/status",           lambda d: d.get("status") in ("running", "ok")),
        ("/api/arena/leaderboard",lambda d: (isinstance(d, list) and len(d) > 0) or (isinstance(d, dict) and len(d.get("leaderboard", [])) > 0)),
        ("/api/market/prices",    lambda d: not d.get("error")),
        ("/api/signals/recent",   lambda d: d is not None),
        ("/api/market/gex/SPY",   lambda d: "spot" in d or d.get("error") == ""),
        ("/api/trades",           lambda d: d is not None),
        ("/api/market/vix",       lambda d: not d.get("error")),
        ("/api/fear-greed",       lambda d: not d.get("error")),
        ("/api/regime",           lambda d: not d.get("error")),
        ("/api/operations",       lambda d: not d.get("error")),
    ]

    results = []
    for path, validator in endpoints:
        url = DASHBOARD_URL + path
        code, body = _http_get(url)
        if code == 0:
            status = "DOWN"
            detail = body.get("error", "connection refused") if body else "connection refused"
        elif code != 200:
            status = "ERROR"
            detail = f"HTTP {code}"
        else:
            try:
                valid = validator(body or {})
                status = "OK" if valid else "WARN"
                detail = "OK" if valid else "unexpected response shape"
            except Exception as e:
                status = "WARN"
                detail = str(e)

        results.append({
            "endpoint": path,
            "status": status,
            "http_code": code,
            "detail": detail,
        })

    return results


# ── Data Freshness ─────────────────────────────────────────────────────────────

def _check_data_freshness() -> list[dict]:
    """Check 5 key data freshness timestamps."""
    checks = [
        ("last_signal",          "signals",             "created_at",  15),
        ("last_trade",           "trades",              "created_at",  8*60),
        ("last_war_room_post",   "war_room",            "created_at",  30),
        ("last_portfolio_sync",  "portfolio_positions", "updated_at",  20),
        ("last_riker_alert",     "rikers_log",          "created_at",  30),
    ]

    results = []
    for name, table, col, warn_after in checks:
        age = _last_row_age(table, col)
        results.append({
            "name": name,
            "table": table,
            "age_minutes": round(age, 1) if age is not None else None,
            "warn_after_minutes": warn_after,
            "status": (
                "UNKNOWN" if age is None
                else "WARN" if age > warn_after
                else "OK"
            ),
        })

    return results


# ── Ollama Health ──────────────────────────────────────────────────────────────

def _check_ollama(base_url: str, name: str) -> dict:
    """Ping Ollama /api/tags to check reachability and list loaded models."""
    t0 = time.monotonic()
    code, body = _http_get(f"{base_url}/api/tags", timeout=OLLAMA_TIMEOUT)
    elapsed = round((time.monotonic() - t0) * 1000)

    if code == 0:
        status = "DOWN"
        models = []
        detail = body.get("error", "unreachable") if body else "unreachable"
    elif code != 200:
        status = "ERROR"
        models = []
        detail = f"HTTP {code}"
    else:
        models = [m.get("name", "") for m in (body or {}).get("models", [])]
        status = "OK"
        detail = f"{len(models)} model(s) listed"

    return {
        "name": name,
        "url": base_url,
        "status": status,
        "http_code": code,
        "latency_ms": elapsed,
        "models_loaded": models,
        "detail": detail,
    }


# ── ntfy Alerts ───────────────────────────────────────────────────────────────

def _push_ntfy(title: str, body: str, priority: str = "default") -> None:
    """Fire-and-forget ntfy push. Never raises."""
    try:
        ascii_title = title.encode("ascii", errors="ignore").decode("ascii").strip()
        req = Request(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=body.encode("utf-8"),
            headers={
                "Title":        ascii_title or "Fleet Auditor",
                "Priority":     priority,
                "Tags":         "warning,fleet,auditor",
                "Content-Type": "text/plain; charset=utf-8",
            },
            method="POST",
        )
        urlopen(req, timeout=6)
    except Exception:
        pass


def _detect_transitions(prev: dict, curr: dict) -> list[str]:
    """Compare previous vs current manifest for state transitions."""
    alerts = []

    def _prev_statuses(section_key: str, id_key: str) -> dict[str, str]:
        return {
            item.get(id_key, "?"): item.get("status", "?")
            for item in prev.get(section_key, [])
        }

    # Jobs
    prev_jobs = _prev_statuses("scheduled_jobs", "name")
    for job in curr.get("scheduled_jobs", []):
        name = job["name"]
        old = prev_jobs.get(name, "UNKNOWN")
        new = job["status"]
        if old in ("OK", "SKIP") and new == "STALE":
            alerts.append(f"⚠️ Job STALE: {name} ({job['detail']})")
        elif old == "STALE" and new in ("OK", "SKIP"):
            alerts.append(f"✅ Job recovered: {name}")

    # API endpoints
    prev_api = _prev_statuses("api_health", "endpoint")
    for ep in curr.get("api_health", []):
        path = ep["endpoint"]
        old = prev_api.get(path, "UNKNOWN")
        new = ep["status"]
        if old == "OK" and new in ("DOWN", "ERROR"):
            alerts.append(f"🔴 API DOWN: {path} ({ep['detail']})")
        elif old in ("DOWN", "ERROR") and new == "OK":
            alerts.append(f"🟢 API restored: {path}")

    # Ollama
    prev_ollama = _prev_statuses("ollama_health", "name")
    for node in curr.get("ollama_health", []):
        name = node["name"]
        old = prev_ollama.get(name, "UNKNOWN")
        new = node["status"]
        if old == "OK" and new in ("DOWN", "ERROR"):
            alerts.append(f"🔴 Ollama DOWN: {name} ({node['url']})")
        elif old in ("DOWN", "ERROR") and new == "OK":
            alerts.append(f"🟢 Ollama recovered: {name}")

    return alerts


# ── Main Audit ─────────────────────────────────────────────────────────────────

def run_audit(send_alerts: bool = True) -> dict:
    """Run the full audit, write manifest, fire alerts on transitions."""
    ts = datetime.now(timezone.utc).isoformat()

    # Load previous manifest for transition detection
    prev_manifest: dict = {}
    try:
        with open(MANIFEST_PATH, "r") as f:
            prev_manifest = json.load(f)
    except Exception:
        pass

    manifest = {
        "generated_at": ts,
        "scheduled_jobs": _check_scheduled_jobs(),
        "api_health": _check_api_endpoints(),
        "data_freshness": _check_data_freshness(),
        "ollama_health": [
            _check_ollama(BIGMAC_OLLAMA, "bigmac-ollama"),
            _check_ollama(OLLIE_OLLAMA,  "ollie-gpu"),
        ],
    }

    # Summary counts
    def _count(section: str, status: str) -> int:
        return sum(1 for x in manifest.get(section, []) if x.get("status") == status)

    manifest["summary"] = {
        "jobs_ok":    _count("scheduled_jobs", "OK") + _count("scheduled_jobs", "SKIP"),
        "jobs_stale": _count("scheduled_jobs", "STALE"),
        "api_ok":     _count("api_health", "OK"),
        "api_down":   _count("api_health", "DOWN") + _count("api_health", "ERROR"),
        "ollama_ok":  _count("ollama_health", "OK"),
        "ollama_down":_count("ollama_health", "DOWN") + _count("ollama_health", "ERROR"),
    }

    # Write manifest
    try:
        with open(MANIFEST_PATH, "w") as f:
            json.dump(manifest, f, indent=2)
    except Exception as e:
        manifest["write_error"] = str(e)

    # Send ntfy on transitions
    if send_alerts and prev_manifest:
        transitions = _detect_transitions(prev_manifest, manifest)
        if transitions:
            title = f"Fleet Alert — {len(transitions)} state change(s)"
            body = "\n".join(transitions)
            priority = "high" if any("DOWN" in t or "STALE" in t for t in transitions) else "default"
            _push_ntfy(title, body, priority)

    return manifest


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    quiet = "--quiet" in sys.argv
    manifest = run_audit(send_alerts=True)
    s = manifest["summary"]
    if not quiet:
        print(f"Fleet Auditor — {manifest['generated_at']}")
        print(f"  Jobs:   {s['jobs_ok']} OK / {s['jobs_stale']} stale")
        print(f"  APIs:   {s['api_ok']} OK / {s['api_down']} down")
        print(f"  Ollama: {s['ollama_ok']} OK / {s['ollama_down']} down")

        issues = []
        for j in manifest["scheduled_jobs"]:
            if j["status"] == "STALE":
                issues.append(f"  STALE JOB: {j['name']} — {j['detail']}")
        for ep in manifest["api_health"]:
            if ep["status"] in ("DOWN", "ERROR"):
                issues.append(f"  API {ep['status']}: {ep['endpoint']} — {ep['detail']}")
        for node in manifest["ollama_health"]:
            if node["status"] != "OK":
                issues.append(f"  OLLAMA {node['status']}: {node['name']} — {node['detail']}")

        if issues:
            print("\nISSUES:")
            for i in issues:
                print(i)
        else:
            print("  All systems nominal.")
