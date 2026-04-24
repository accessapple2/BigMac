#!/usr/bin/env python3
"""
scripts/uhura_watch.py

Uhura-Watch — Fleet Health Monitor
Runs every 15 minutes during market hours via launchd.
Silent unless anomaly — ntfy push only on actual issues.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import socket
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

REPO_ROOT = Path(os.environ.get("BIGMAC_REPO", "/Users/bigmac/autonomous-trader"))
DB_PATH = REPO_ROOT / "data" / "trader.db"
LOG_DIR = REPO_ROOT / "logs" / "uhura_watch"
NTFY_TOPIC = os.environ.get("UHURA_NTFY_TOPIC", "ollietrades-watch")

DASHBOARD_BASE = os.environ.get("DASHBOARD_URL", "http://127.0.0.1:8080")
ENDPOINTS_TO_CHECK = ["/api/status"]

ENDPOINT_TIMEOUT_SEC  = 10.0
DB_QUERY_TIMEOUT_SEC  = 5.0
MIN_SCAN_CYCLES_PER_30MIN = 1
MAX_BIGMAC_RAM_GB     = 15.5  # Ollama baseline is ~6GB; 15.5 leaves headroom
MAX_SWAP_PRESSURE_MB  = 2500

MARKET_OPEN_HOUR_ET  = 9
MARKET_OPEN_MIN_ET   = 30
MARKET_CLOSE_HOUR_ET = 16
MARKET_CLOSE_MIN_ET  = 0


# Suppress repeat ntfy pushes for the same anomaly within this window
NTFY_DEDUP_MINUTES = 60
DEDUP_STATE_FILE = LOG_DIR / "ntfy_dedup.json"


class CheckResult:
    def __init__(self, name, ok, detail="", anomaly=None):
        self.name, self.ok, self.detail, self.anomaly = name, ok, detail, anomaly
    def to_dict(self):
        return {"name": self.name, "ok": self.ok, "detail": self.detail, "anomaly": self.anomaly}


def check_endpoints():
    results = []
    for path in ENDPOINTS_TO_CHECK:
        url = f"{DASHBOARD_BASE}{path}"
        start = time.monotonic()
        try:
            req = Request(url, headers={"User-Agent": "uhura-watch/1.0"})
            with urlopen(req, timeout=ENDPOINT_TIMEOUT_SEC) as resp:
                elapsed = time.monotonic() - start
                body = resp.read(256)
                if resp.status == 200:
                    results.append(CheckResult(f"endpoint:{path}", True,
                        f"200 in {elapsed:.2f}s, {len(body)}B"))
                else:
                    results.append(CheckResult(f"endpoint:{path}", False,
                        f"status={resp.status} elapsed={elapsed:.2f}s",
                        anomaly=f"{path} returned {resp.status}"))
        except (URLError, HTTPError, socket.timeout, ConnectionError, OSError) as e:
            elapsed = time.monotonic() - start
            results.append(CheckResult(f"endpoint:{path}", False,
                f"error={type(e).__name__}: {e} elapsed={elapsed:.2f}s",
                anomaly=f"{path} unreachable: {type(e).__name__}"))
    return results


def _safe_count(cur, sql, params):
    try:
        cur.execute(sql, params)
        row = cur.fetchone()
        return int(row[0]) if row else None
    except sqlite3.OperationalError:
        return None


def check_db():
    results = []
    if not DB_PATH.exists():
        return [CheckResult("db:exists", False, f"DB missing: {DB_PATH}",
                            anomaly=f"DB file not found at {DB_PATH}")]
    start = time.monotonic()
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=DB_QUERY_TIMEOUT_SEC)
        cur = conn.cursor()
        cutoff_15 = (datetime.now(timezone.utc) - timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M:%S")
        cutoff_30 = (datetime.now(timezone.utc) - timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
        cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        sig_15 = _safe_count(cur, "SELECT COUNT(*) FROM signals WHERE created_at >= ?", (cutoff_15,))
        sig_30 = _safe_count(cur, "SELECT COUNT(*) FROM signals WHERE created_at >= ?", (cutoff_30,))
        trades_15 = _safe_count(cur, "SELECT COUNT(*) FROM trades WHERE created_at >= ?", (cutoff_15,))
        scotty_count = _safe_count(cur, "SELECT COUNT(*) FROM scotty_watchlist WHERE created_at >= ?", (cutoff_24h,))
        scans_30 = _safe_count(cur, "SELECT COUNT(*) FROM fast_scan_results WHERE created_at >= ?", (cutoff_30,))
        elapsed = time.monotonic() - start
        conn.close()
        detail = f"signals_15m={sig_15} signals_30m={sig_30} trades_15m={trades_15} scotty_24h={scotty_count} scan_cycles_30m={scans_30} db_elapsed={elapsed:.2f}s"
        results.append(CheckResult("db:query", True, detail))
        if _is_market_hours() and scans_30 is not None and scans_30 < MIN_SCAN_CYCLES_PER_30MIN:
            results.append(CheckResult("db:fleet_activity", False,
                f"only {scans_30} scan cycles in last 30 min during market hours",
                anomaly=f"Fleet stalled? {scans_30} scan cycles/30m during market hours"))
        if elapsed > DB_QUERY_TIMEOUT_SEC * 0.8:
            results.append(CheckResult("db:latency", False,
                f"DB queries took {elapsed:.2f}s",
                anomaly=f"DB slow: {elapsed:.2f}s for basic queries"))
    except sqlite3.OperationalError as e:
        results.append(CheckResult("db:query", False,
            f"OperationalError: {e}", anomaly=f"DB error: {e}"))
    except Exception as e:
        results.append(CheckResult("db:query", False,
            f"unexpected: {type(e).__name__}: {e}",
            anomaly=f"DB unexpected error: {type(e).__name__}"))
    return results


def _to_gb(num, unit):
    n = float(num)
    return {"K": n / (1024*1024), "M": n / 1024, "G": n, "T": n * 1024}[unit]


def check_memory():
    results = []
    try:
        out = subprocess.run(["top", "-l", "1", "-n", "0"],
            capture_output=True, text=True, timeout=5)
        if out.returncode != 0:
            return [CheckResult("memory", False, f"top exit {out.returncode}")]
        text = out.stdout
        physmem_match = re.search(r"PhysMem:\s+(\d+)([KMGT])\s+used.*?(\d+)([KMGT])\s+unused", text)
        used_gb = unused_gb = None
        if physmem_match:
            used_gb = _to_gb(physmem_match.group(1), physmem_match.group(2))
            unused_gb = _to_gb(physmem_match.group(3), physmem_match.group(4))
        comp_mb = None
        comp_match = re.search(r"(\d+)M\s+compressor", text)
        if comp_match:
            comp_mb = int(comp_match.group(1))
        detail = f"used={used_gb}GB unused={unused_gb}GB compressor={comp_mb}MB"
        anomaly = None
        if used_gb is not None and used_gb > MAX_BIGMAC_RAM_GB:
            anomaly = f"bigmac RAM {used_gb:.1f}GB / 16GB"
        elif comp_mb is not None and comp_mb > MAX_SWAP_PRESSURE_MB:
            anomaly = f"bigmac swap pressure: compressor={comp_mb}MB"
        results.append(CheckResult("memory:physmem", anomaly is None, detail, anomaly=anomaly))
        ps_out = subprocess.run(["ps", "-Ao", "rss,comm", "-m"],
            capture_output=True, text=True, timeout=5)
        if ps_out.returncode == 0:
            lines = ps_out.stdout.strip().split("\n")[1:4]
            hogs = []
            for line in lines:
                parts = line.strip().split(None, 1)
                if len(parts) == 2:
                    try:
                        rss_mb = int(parts[0]) // 1024
                        hogs.append(f"{Path(parts[1]).name}={rss_mb}MB")
                    except ValueError:
                        pass
            results.append(CheckResult("memory:top3", True, " ".join(hogs)))
    except subprocess.TimeoutExpired:
        results.append(CheckResult("memory", False, "top command timed out",
            anomaly="System responsiveness: top timed out"))
    except Exception as e:
        results.append(CheckResult("memory", False, f"{type(e).__name__}: {e}"))
    return results


def check_scotty():
    results = []
    try:
        out = subprocess.run(["launchctl", "list"],
            capture_output=True, text=True, timeout=5)
        if out.returncode != 0:
            return [CheckResult("scotty:launchctl", False, f"launchctl exit {out.returncode}")]
        scotty_line = None
        for line in out.stdout.splitlines():
            if "com.ollietrades.scotty" in line:
                scotty_line = line
                break
        if not scotty_line:
            return [CheckResult("scotty:loaded", False, "not loaded in launchctl",
                anomaly="Scotty plist not loaded")]
        parts = scotty_line.split()
        if len(parts) >= 3:
            pid, exit_status, label = parts[0], parts[1], parts[2]
            detail = f"pid={pid} last_exit={exit_status}"
            if exit_status not in ("0", "-"):
                try:
                    code = int(exit_status)
                    if code != 0:
                        results.append(CheckResult("scotty:status", False, detail,
                            anomaly=f"Scotty last exit={code}"))
                        return results
                except ValueError:
                    pass
            results.append(CheckResult("scotty:status", True, detail))
        else:
            results.append(CheckResult("scotty:status", True, f"line={scotty_line!r}"))
    except Exception as e:
        results.append(CheckResult("scotty", False, f"{type(e).__name__}: {e}"))
    return results


def _is_market_hours():
    now_utc = datetime.now(timezone.utc)
    et_offset = -4 if 3 <= now_utc.month <= 11 else -5
    now_et = now_utc + timedelta(hours=et_offset)
    if now_et.weekday() >= 5:
        return False
    minute_of_day = now_et.hour * 60 + now_et.minute
    open_min  = MARKET_OPEN_HOUR_ET * 60 + MARKET_OPEN_MIN_ET
    close_min = MARKET_CLOSE_HOUR_ET * 60 + MARKET_CLOSE_MIN_ET
    return open_min <= minute_of_day <= close_min


def _load_dedup_state():
    try:
        if DEDUP_STATE_FILE.exists():
            with DEDUP_STATE_FILE.open() as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_dedup_state(state):
    try:
        DEDUP_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with DEDUP_STATE_FILE.open("w") as f:
            json.dump(state, f)
    except Exception as e:
        print(f"[uhura-watch] dedup save failed: {e}", file=sys.stderr)


def send_ntfy(anomalies, dry_run=False):
    if not anomalies:
        return
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=NTFY_DEDUP_MINUTES)
    state = _load_dedup_state()

    # Prune old entries
    state = {k: v for k, v in state.items()
             if datetime.fromisoformat(v) > cutoff}

    # Filter out anomalies we alerted on recently
    fresh = [a for a in anomalies if a not in state]

    if not fresh:
        print(f"[uhura-watch] {len(anomalies)} anomalies all deduped (within {NTFY_DEDUP_MINUTES}min window)")
        return

    body = " | ".join(fresh[:5])
    if dry_run:
        print(f"[DRY-RUN] would ntfy: {body}")
        return

    try:
        import urllib.request
        req = urllib.request.Request(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=body.encode("utf-8"),
            headers={"Title": "Uhura-Watch: fleet anomaly",
                     "Priority": "high", "Tags": "warning"},
            method="POST")
        urllib.request.urlopen(req, timeout=5).read()
        # Record what we alerted on
        for a in fresh:
            state[a] = now.isoformat()
        _save_dedup_state(state)
    except Exception as e:
        print(f"[uhura-watch] ntfy push failed: {type(e).__name__}: {e}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--test-alert", action="store_true")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{datetime.now().strftime('%Y-%m-%d')}.log"

    if args.test_alert:
        send_ntfy(["TEST ALERT from uhura-watch"], dry_run=args.dry_run)
        print(f"Test alert sent to topic={NTFY_TOPIC} (dry_run={args.dry_run})")
        return 0

    all_results = []
    all_results += check_endpoints()
    all_results += check_db()
    all_results += check_memory()
    all_results += check_scotty()

    anomalies = [r.anomaly for r in all_results if r.anomaly]
    ok_count = sum(1 for r in all_results if r.ok)
    timestamp = datetime.now().isoformat(timespec="seconds")
    status = "OK" if not anomalies else f"ANOMALY({len(anomalies)})"
    summary = f"{timestamp} {status} checks={ok_count}/{len(all_results)}"

    log_line = {"ts": timestamp, "status": status, "anomalies": anomalies,
                "checks": [r.to_dict() for r in all_results]}
    with log_path.open("a") as f:
        f.write(json.dumps(log_line) + "\n")

    print(summary)
    for r in all_results:
        marker = "✓" if r.ok else "✗"
        print(f"  {marker} {r.name}: {r.detail}" + (f"  [!! {r.anomaly}]" if r.anomaly else ""))

    if anomalies:
        send_ntfy(anomalies, dry_run=args.dry_run)

    return 0 if not anomalies else 2


if __name__ == "__main__":
    sys.exit(main())
