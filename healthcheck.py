#!/usr/bin/env python3
"""USS TradeMinds — Health Check & Auto-Restart (Dr. Crusher Edition)
Runs at 6AM pre-market, then hourly 7AM-1PM MST.
Uses stdlib only so it works with any Python (no venv dependency).
"""
from __future__ import annotations
import os
import subprocess
import sys
import time
import sqlite3
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.error import URLError
import json

# --- Config ---
BASE_DIR      = os.path.expanduser("~/autonomous-trader")
PLIST         = os.path.expanduser("~/Library/LaunchAgents/com.trademinds.trader.plist")
TUNNEL_PLIST  = os.path.expanduser("~/Library/LaunchAgents/com.trademinds.tunnel.plist")
DASHBOARD_URL = "http://127.0.0.1:8080"
NTFY_ADMIN_TOPIC = os.environ.get("NTFY_ADMIN_TOPIC", "Ollie-Alert-35")  # iPhone push topic
OLLAMA_URL    = "http://127.0.0.1:11434"
OLLIE_URL     = "http://192.168.1.166:11434"   # 2026-04-20: Ollie GPU (RTX 5060, primary inference)
TUNNEL_URL    = "https://bridge.accessapple.com"
DB_PATH       = os.path.join(BASE_DIR, "data", "trader.db")
AUTO_DB_PATH  = os.path.join(BASE_DIR, "autonomous_trader.db")
BACKUP_DIR    = os.path.join(BASE_DIR, "backups")
SCANNER_LOG   = os.path.join(BASE_DIR, "logs", "scanner.err")
HEALTH_LOG    = os.path.join(BASE_DIR, "logs", "healthcheck.log")
LOG_STALE_MIN = 15   # logs/trader.err freshness threshold (minutes)
RESTART_WAIT  = 12   # seconds to wait after launchctl load before verifying
DB_BACKUP_KEEP = 7   # number of daily backups to retain
_START_TIME   = time.time()
_db_backup_done_today: str = ""  # date string — prevents double-backup per day
_archer_killed_for_trading: bool = False  # True if we killed Archer during market hours


# --- Logging ---
def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(HEALTH_LOG, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


# --- Checks ---
def check_process() -> bool:
    result = subprocess.run(["pgrep", "-f", r"main\.py"], capture_output=True)
    return result.returncode == 0


def check_dashboard() -> bool:
    try:
        req = Request(DASHBOARD_URL + "/", headers={"User-Agent": "TradeMinds-Healthcheck/1.0"})
        with urlopen(req, timeout=6) as r:
            return r.status == 200
    except Exception:
        return False


def check_ollama() -> tuple[bool, str]:
    """Check if Ollama is reachable and return list of loaded models."""
    try:
        req = Request(OLLAMA_URL + "/api/tags", headers={"User-Agent": "TradeMinds-Healthcheck/1.0"})
        with urlopen(req, timeout=5) as r:
            if r.status != 200:
                return False, "HTTP " + str(r.status)
            data = json.loads(r.read())
            models = [m.get("name", "") for m in data.get("models", [])]
            return True, f"{len(models)} models"
    except Exception as e:
        return False, str(e)[:60]



def check_ollie() -> tuple[bool, str]:
    """Check Ollie GPU: TCP reachability, HTTP 200 on /api/tags, qwen3:8b present.
    2026-04-20: added — Ollie is primary inference box; bigmac degrades badly if unreachable."""
    import socket
    # Step 1: TCP connect (1s timeout — fast ping substitute)
    try:
        sock = socket.create_connection(("192.168.1.166", 11434), timeout=1)
        sock.close()
    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        return False, f"unreachable (TCP): {str(e)[:50]}"
    # Step 2: /api/tags (3s timeout, expect HTTP 200)
    try:
        req = Request(OLLIE_URL + "/api/tags", headers={"User-Agent": "TradeMinds-Healthcheck/1.0"})
        with urlopen(req, timeout=3) as r:
            if r.status != 200:
                return False, f"HTTP {r.status} from /api/tags"
            data = json.loads(r.read())
            models = [m.get("name", "") for m in data.get("models", [])]
    except Exception as e:
        return False, f"api/tags failed: {str(e)[:60]}"
    # Step 3: verify qwen3:8b available
    if not any("qwen3:8b" in m for m in models):
        return False, f"qwen3:8b missing from Ollie (have: {', '.join(models[:5])})"
    return True, f"ok — {len(models)} models, qwen3:8b present"


def check_db() -> tuple[bool, str]:
    try:
        con = sqlite3.connect(DB_PATH, timeout=5)
        row = con.execute("PRAGMA integrity_check").fetchone()
        con.close()
        ok = row and row[0] == "ok"
        return ok, row[0] if row else "no result"
    except Exception as e:
        return False, str(e)


def check_log_freshness() -> tuple[bool, float]:
    try:
        age_min = (time.time() - os.path.getmtime(SCANNER_LOG)) / 60
        return age_min < LOG_STALE_MIN, round(age_min, 1)
    except Exception:
        return False, -1


def get_uptime() -> str:
    """Return healthcheck process uptime as human-readable string."""
    secs = int(time.time() - _START_TIME)
    if secs < 60:
        return f"{secs}s"
    elif secs < 3600:
        return f"{secs // 60}m {secs % 60}s"
    else:
        h = secs // 3600
        m = (secs % 3600) // 60
        return f"{h}h {m}m"


def get_db_stats() -> str:
    """Return quick DB stats: trade count and signal count."""
    try:
        con = sqlite3.connect(DB_PATH, timeout=5)
        trades = con.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        signals = con.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
        con.close()
        return f"{trades} trades, {signals} signals"
    except Exception:
        return "unavailable"


# --- Actions ---
def push_ntfy(title: str, body: str, priority: str = "default",
              tags: str = "warning,bigmac,drcrusher") -> None:
    """Fire-and-forget ntfy.sh push to the admin topic. Stdlib only; silent on failure.
    Mirrors watchdog.py push_alert() pattern for consistency."""
    try:
        ascii_title = title.encode("ascii", errors="ignore").decode("ascii").strip() or "Dr. Crusher"
        req = Request(
            f"https://ntfy.sh/{NTFY_ADMIN_TOPIC}",
            data=body.encode("utf-8"),
            headers={
                "Title":        ascii_title,
                "Priority":     priority,
                "Tags":         tags,
                "Content-Type": "text/plain; charset=utf-8",
            },
            method="POST",
        )
        urlopen(req, timeout=6)
    except Exception:
        pass  # ntfy failures must never crash the healthcheck


def notify(title: str, message: str, priority: str = "default") -> None:
    """macOS desktop popup (osascript) + iPhone ntfy push. Dual-channel."""
    script = f'display notification "{message}" with title "{title}" sound name "Sosumi"'
    subprocess.run(["osascript", "-e", script], capture_output=True)
    push_ntfy(title, message, priority=priority)


def notify_with_show(title: str, message: str, log_path: str) -> None:
    """macOS dialog with Show/Dismiss buttons — non-blocking via Popen."""
    safe_title   = title.replace('"', "'")
    safe_msg     = message.replace('"', "'")
    safe_path    = log_path.replace('"', "'")
    script = (
        f'display dialog "{safe_msg}" with title "{safe_title}" '
        f'buttons {{"Dismiss", "Show Log"}} default button "Show Log" '
        f'with icon caution\n'
        f'if button returned of result is "Show Log" then\n'
        f'  do shell script "open -a Terminal \\"{safe_path}\\""\n'
        f'end if'
    )
    try:
        subprocess.Popen(["osascript", "-e", script],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass


def post_war_room(message: str) -> None:
    """Post a message to the dashboard war room feed."""
    try:
        body = json.dumps({"message": message, "source": "healthcheck"}).encode()
        req = Request(
            DASHBOARD_URL + "/api/war-room",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(req, timeout=4):
            pass
    except Exception:
        pass  # dashboard may be down — that's fine


SCANNER_LOG_PATH = os.path.join(BASE_DIR, "scanner.log")


def _is_market_hours() -> bool:
    """Return True during 9:30–16:00 ET on weekdays."""
    try:
        import pytz
        from datetime import datetime as _dt
        et = pytz.timezone("US/Eastern")
        now = _dt.now(et)
        if now.weekday() >= 5:
            return False
        h = now.hour + now.minute / 60.0
        return 9.5 <= h < 16.0
    except Exception:
        return False  # pytz not available — default safe


def rotate_scanner_log() -> bool:
    """Rotate scanner.log → scanner.log.1 if it exceeds 100K lines. Returns True if rotated."""
    try:
        wc = subprocess.check_output(["wc", "-l", SCANNER_LOG_PATH],
                                     text=True, stderr=subprocess.DEVNULL)
        count = int(wc.strip().split()[0])
        if count > 100_000:
            backup = SCANNER_LOG_PATH + ".1"
            os.rename(SCANNER_LOG_PATH, backup)
            open(SCANNER_LOG_PATH, "w").close()  # touch new empty file
            log(f"LOG ROTATION: scanner.log was {count} lines — rotated to scanner.log.1")
            return True
    except Exception:
        pass
    return False


def restart_server() -> None:
    log("Restarting USS TradeMinds via launchctl...")
    subprocess.run(["launchctl", "unload", PLIST], capture_output=True)
    subprocess.run(["pkill", "-9", "-f", r"main\.py"], capture_output=True)
    time.sleep(3)
    # Clear port 8080 in case it's still bound
    try:
        port_pids = subprocess.check_output(["lsof", "-ti", ":8080"], text=True).split()
        for pid in port_pids:
            subprocess.run(["kill", "-9", pid], capture_output=True)
    except Exception:
        pass
    time.sleep(1)
    subprocess.run(["launchctl", "load", PLIST], capture_output=True)
    log(f"Waiting {RESTART_WAIT}s for startup...")
    time.sleep(RESTART_WAIT)


def restart_ollama() -> None:
    """Kill and restart the Ollama server process."""
    log("Restarting Ollama server...")
    subprocess.run(["pkill", "ollama"], capture_output=True)
    time.sleep(3)
    subprocess.Popen(
        ["/usr/local/bin/ollama", "serve"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(5)
    log("Ollama restart launched (background)")


def auto_restart(reason: str, also_restart_ollama: bool = False) -> None:
    """Rotate logs, restart main.py (and optionally Ollama), notify, post to war room."""
    log(f"AUTO-RESTART triggered: {reason}")
    rotate_scanner_log()
    if also_restart_ollama:
        restart_ollama()
    notify("🚨 Dr. Crusher AUTO-RESTART", reason, priority="urgent")
    restart_server()
    # Verify + report
    if check_dashboard():
        ok_msg = f"Auto-restart SUCCESS ({reason})"
        log(f"SUCCESS: {ok_msg}")
        notify("USS TradeMinds ✓", ok_msg)
        post_war_room(f"🔧 Dr. Crusher AUTO-RESTART: {reason} — now nominal")
    else:
        fail_msg = f"Auto-restart FAILED after: {reason} — manual intervention required"
        log(f"CRITICAL: {fail_msg}")
        notify("🚨 USS TradeMinds CRITICAL", fail_msg, priority="urgent")
        post_war_room(f"🚨 {fail_msg}")


# ---------------------------------------------------------------------------
# Auto-restart checks (return True = trigger fires → caller calls auto_restart)
# ---------------------------------------------------------------------------

def check_ollama_stalled() -> tuple[bool, str]:
    """Return (stalled, info). Stalled = >30 min no Ollama success during market hours."""
    if not _is_market_hours():
        return False, "outside market hours"
    try:
        req = Request(
            DASHBOARD_URL + "/api/ollama-queue-status",
            headers={"User-Agent": "DrCrusher/3.0"},
        )
        with urlopen(req, timeout=5) as r:
            data = json.loads(r.read())
        age_min = data.get("last_success_age_min")
        stale = data.get("stale", False)
        if age_min is None:
            return False, "no successful analysis yet (process just started?)"
        if stale:
            return True, f"last Ollama success {age_min}m ago (>30m)"
        return False, f"ok ({age_min}m ago)"
    except Exception as e:
        return False, f"queue endpoint unavailable: {str(e)[:50]}"


def check_websocket_dead() -> tuple[bool, str]:
    """Return (dead, info). Dead = 'polling mode'/'WebSocket failed 3x' in last 15 min
    with no reconnect in that window."""
    try:
        tail = subprocess.check_output(
            ["tail", "-300", SCANNER_LOG_PATH],
            text=True, stderr=subprocess.DEVNULL,
        )
        lines = tail.splitlines()
        has_bad = any(
            "polling mode" in l or "WebSocket failed 3x" in l
            for l in lines
        )
        has_reconnect = any(
            "WebSocket connected" in l or "WebSocket reconnect" in l
            for l in lines[-50:]  # last 50 lines = most recent activity
        )
        if has_bad and not has_reconnect:
            return True, "polling mode / WebSocket failed 3x, no reconnect seen"
        return False, "websocket ok"
    except Exception:
        return False, "log unreadable"


def check_scheduler_spam() -> tuple[bool, int]:
    """Return (spam, count). Spam = >50 'Scheduler job error' in scanner.log."""
    try:
        result = subprocess.run(
            ["grep", "-c", "Scheduler job error", SCANNER_LOG_PATH],
            capture_output=True, text=True,
        )
        count = int(result.stdout.strip() or "0")
        return count > 50, count
    except Exception:
        return False, 0


def check_dayblade_dark() -> tuple[bool, str]:
    """Return (dark, info). Dark = no DayBlade log entry in last 2000 lines during market hours."""
    if not _is_market_hours():
        return False, "outside market hours"
    try:
        tail = subprocess.check_output(
            ["tail", "-2000", SCANNER_LOG_PATH],
            text=True, stderr=subprocess.DEVNULL,
        )
        has_dayblade = any(
            "dayblade" in l.lower() or "DayBlade" in l
            for l in tail.splitlines()
        )
        if not has_dayblade:
            return True, "no DayBlade activity in last 2000 log lines during market hours"
        return False, "ok"
    except Exception:
        return False, "log unreadable"


def check_port_zombie() -> tuple[bool, str]:
    """Return (zombie, info). Zombie = port 8080 bound but dashboard returns non-200."""
    try:
        # Is anything bound to 8080?
        bound = subprocess.check_output(["lsof", "-ti", ":8080"],
                                        text=True, stderr=subprocess.DEVNULL).strip()
        if not bound:
            return False, "port 8080 not bound (process down — handled elsewhere)"
        # Something is bound — does the dashboard respond 200?
        if check_dashboard():
            return False, "ok"
        # Port bound but non-200 → zombie
        pids = bound.split()
        return True, f"port 8080 bound (pid {', '.join(pids)}) but dashboard non-200"
    except subprocess.CalledProcessError:
        return False, "port 8080 not bound"
    except Exception:
        return False, "check error"


# ---------------------------------------------------------------------------
# DB backup (Fix 2)
# ---------------------------------------------------------------------------

def backup_trader_db() -> tuple[bool, str]:
    """Copy trader.db to backups/trader_YYYY-MM-DD.db. Keep last 7 days.
    Returns (did_backup, message). NEVER deletes source DB."""
    global _db_backup_done_today
    today = datetime.now().strftime("%Y-%m-%d")
    if _db_backup_done_today == today:
        return False, "already backed up today"
    try:
        os.makedirs(BACKUP_DIR, exist_ok=True)
        dest = os.path.join(BACKUP_DIR, f"trader_{today}.db")
        if os.path.exists(dest):
            _db_backup_done_today = today
            return False, f"backup already exists: {os.path.basename(dest)}"
        # Use sqlite3 backup API — safe even while DB is being written
        src_con = sqlite3.connect(DB_PATH, timeout=10)
        dst_con = sqlite3.connect(dest, timeout=10)
        src_con.backup(dst_con)
        dst_con.close()
        src_con.close()
        size_kb = os.path.getsize(dest) // 1024
        _db_backup_done_today = today
        # Prune backups older than DB_BACKUP_KEEP days
        pruned = 0
        all_backups = sorted([
            f for f in os.listdir(BACKUP_DIR)
            if f.startswith("trader_") and f.endswith(".db")
        ])
        while len(all_backups) > DB_BACKUP_KEEP:
            old = all_backups.pop(0)
            old_path = os.path.join(BACKUP_DIR, old)
            os.remove(old_path)   # safe: these are backups, not the source
            pruned += 1
        msg = f"backed up → {os.path.basename(dest)} ({size_kb}KB)"
        if pruned:
            msg += f", pruned {pruned} old backup(s)"
        return True, msg
    except Exception as e:
        return False, f"backup failed: {str(e)[:80]}"


# ---------------------------------------------------------------------------
# Tunnel health (Fix 4)
# ---------------------------------------------------------------------------

def check_tunnel_health() -> tuple[bool, str]:
    """Return (stale, info). Stale = localhost:8080 up but bridge.accessapple.com down."""
    try:
        req = Request(
            TUNNEL_URL + "/api/status",
            headers={"User-Agent": "DrCrusher/3.0"},
        )
        with urlopen(req, timeout=8) as r:
            if r.status == 200:
                return False, "tunnel ok"
            return True, f"tunnel returned HTTP {r.status}"
    except Exception as e:
        # Could be network error or tunnel truly down — only flag as stale
        # if local dashboard is healthy (confirms it's a tunnel issue not a local one)
        return True, f"tunnel unreachable: {str(e)[:60]}"


def restart_tunnel() -> None:
    """Unload and reload the cloudflared tunnel plist."""
    log("Restarting Cloudflare tunnel via launchctl...")
    subprocess.run(["launchctl", "unload", TUNNEL_PLIST], capture_output=True)
    time.sleep(2)
    subprocess.run(["launchctl", "load", TUNNEL_PLIST], capture_output=True)
    log("Tunnel plist reloaded")


# ---------------------------------------------------------------------------
# Memory monitor (Fix 5)
# ---------------------------------------------------------------------------

def check_memory() -> tuple[bool, str]:
    """Return (low, info). Low = available RAM < 320MB (98% of 16GB used).
    If low, kill VTuber on port 12393 first (Archer is lowest priority).
    """
    try:
        # Use vm_stat (stdlib-friendly, no psutil needed)
        vm = subprocess.check_output(["vm_stat"], text=True, stderr=subprocess.DEVNULL)
        page_size = 16384  # Apple Silicon default page size (16KB)
        free_pages = 0
        inactive_pages = 0
        for line in vm.splitlines():
            if "Pages free" in line:
                free_pages = int(line.split(":")[1].strip().rstrip("."))
            elif "Pages inactive" in line:
                inactive_pages = int(line.split(":")[1].strip().rstrip("."))
        available_mb = (free_pages + inactive_pages) * page_size // (1024 * 1024)
        if available_mb < 320:
            # Kill VTuber (Archer) on port 12393 to free memory
            try:
                archer_pids = subprocess.check_output(
                    ["lsof", "-ti", ":12393"], text=True, stderr=subprocess.DEVNULL
                ).split()
                if archer_pids:
                    for pid in archer_pids:
                        subprocess.run(["kill", "-9", pid], capture_output=True)
                    log(f"MEMORY: killed Archer (port 12393, pid {', '.join(archer_pids)}) — {available_mb}MB available")
                    return True, f"{available_mb}MB available (<320MB) — Archer killed to free memory"
            except Exception:
                pass
            return True, f"{available_mb}MB available (<320MB threshold)"
        return False, f"{available_mb}MB available"
    except Exception as e:
        return False, f"memory check unavailable: {str(e)[:50]}"


# ---------------------------------------------------------------------------
# Archer trading priority (Fix 6)
# ---------------------------------------------------------------------------

def check_archer_trading_priority() -> tuple[bool, str]:
    """During market hours, if Ollama queue depth > 0, kill Archer (port 12393).
    Trading brain always wins. Returns (killed, reason)."""
    global _archer_killed_for_trading
    if not _is_market_hours():
        return False, "outside market hours"
    # Check queue depth via dashboard API (stdlib HTTP, no venv)
    try:
        req = Request(
            DASHBOARD_URL + "/api/ollama-queue-status",
            headers={"User-Agent": "DrCrusher/2.0"},
        )
        with urlopen(req, timeout=4) as r:
            data = json.loads(r.read())
            queue_depth = data.get("queue_depth", 0)
    except Exception:
        return False, "queue status unavailable"
    if queue_depth <= 0:
        return False, f"Ollama queue empty (depth={queue_depth})"
    # Check if Archer is actually running on port 12393
    try:
        archer_pids = subprocess.check_output(
            ["lsof", "-ti", ":12393"], text=True, stderr=subprocess.DEVNULL
        ).split()
    except Exception:
        archer_pids = []
    if not archer_pids:
        return False, "Archer not running on port 12393"
    for pid in archer_pids:
        subprocess.run(["kill", "-9", pid], capture_output=True)
    _archer_killed_for_trading = True
    return True, f"killed Archer pid(s) {', '.join(archer_pids)} — Ollama queue depth {queue_depth}"


def check_archer_restart_due() -> tuple[bool, str]:
    """After 4:05 PM ET, restart Archer if we killed it during trading hours."""
    global _archer_killed_for_trading
    if not _archer_killed_for_trading:
        return False, "Archer not killed by trading priority"
    try:
        import pytz
        from datetime import datetime as _dt
        et = pytz.timezone("US/Eastern")
        now = _dt.now(et)
        h = now.hour + now.minute / 60.0
        if h < 16.083:  # before 4:05 PM ET
            return False, f"market not closed yet ({now.strftime('%H:%M')} ET)"
        if now.weekday() >= 5:  # weekend — skip restart
            _archer_killed_for_trading = False
            return False, "weekend — not restarting Archer"
    except Exception:
        return False, "timezone check unavailable"
    # Find and run Archer restart script
    archer_script = os.path.join(BASE_DIR, "restart-archer.sh")
    if not os.path.exists(archer_script):
        return False, f"restart script not found: {archer_script}"
    subprocess.Popen(
        ["/bin/bash", archer_script],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    _archer_killed_for_trading = False
    return True, "Archer restarted after market close"


# --- Main ---
def check_ready_room() -> dict:
    """
    Dr. Crusher's Extended Scan — Ready Room + Red Alert health.
    Uses stdlib only (no venv) so falls back gracefully if deps missing.
    """
    today    = datetime.now().strftime("%Y-%m-%d")
    results  = {}

    # 1. Briefing table populated today
    try:
        db_path = os.path.join(BASE_DIR, "data", "trader.db")
        con = sqlite3.connect(db_path, timeout=5)
        count = con.execute(
            "SELECT COUNT(*) FROM ready_room_briefings WHERE date(created_at) = ?",
            (today,)
        ).fetchone()[0]
        con.close()
        results["briefing_today"] = count > 0
        results["briefing_count"] = count
    except Exception as e:
        results["briefing_today"] = False
        results["briefing_count"] = 0

    # 2. Red Alert snapshot in last 10 min
    try:
        alert_db = os.path.join(BASE_DIR, "autonomous_trader.db")
        con = sqlite3.connect(alert_db, timeout=5)
        ts = con.execute(
            "SELECT MAX(created_at) FROM intraday_snapshots WHERE snap_date = ?",
            (today,)
        ).fetchone()[0]
        con.close()
        if ts:
            # simple string comparison: if it's within 10 min of now
            from datetime import datetime as _dt
            try:
                snap_dt = _dt.fromisoformat(ts)
                age_min = (time.time() - snap_dt.timestamp()) / 60
            except Exception:
                age_min = 999
            results["red_alert_ok"] = age_min < 10
            results["red_alert_age_min"] = round(age_min, 1)
        else:
            results["red_alert_ok"] = False
            results["red_alert_age_min"] = None
    except Exception:
        results["red_alert_ok"] = False
        results["red_alert_age_min"] = None

    # 3. Condition endpoint
    try:
        req = Request(
            DASHBOARD_URL + "/api/ready-room/condition",
            headers={"User-Agent": "DrCrusher/2.0"},
        )
        with urlopen(req, timeout=5) as r:
            results["condition_endpoint"] = r.status == 200
    except Exception:
        results["condition_endpoint"] = False

    return results


def main() -> None:
    log("=" * 60)
    log("USS TradeMinds Health Check — Dr. Crusher reporting")
    log("=" * 60)

    proc_ok                = check_process()
    dash_ok                = check_dashboard()
    ollama_ok, ollama_info = check_ollama()
    ollie_ok,  ollie_info  = check_ollie()   # 2026-04-20: Ollie GPU healthcheck
    db_ok, db_msg          = check_db()
    log_ok, log_age        = check_log_freshness()
    db_stats               = get_db_stats()
    uptime                 = get_uptime()

    log(f"  Process (8080)    : {'✓' if proc_ok   else '✗ DOWN'}")
    log(f"  Dashboard (8080)  : {'✓' if dash_ok   else '✗ NOT RESPONDING'}")
    log(f"  Ollama (11434)    : {'✓ ' + ollama_info if ollama_ok else '✗ ' + ollama_info}")
    log(f"  Ollie GPU         : {'✓ ' + ollie_info if ollie_ok else '⚠️ UNREACHABLE — ' + ollie_info}")
    if not ollie_ok:
        push_ntfy(
            "Ollie unreachable — war_room degrading",
            f"Ollie GPU ({OLLIE_URL}) failed: {ollie_info}\nInference routing to bigmac localhost (swap risk!)",
            priority="high",
            tags="warning,ollie,gpu",
        )
    log(f"  trader.db         : {'✓' if db_ok     else f'✗ {db_msg}'}")
    log(f"  DB stats          : {db_stats}")
    log(f"  log age           : {log_age}m {'✓' if log_ok else f'✗ STALE (>{LOG_STALE_MIN}m)'}")
    log(f"  Healthcheck uptime: {uptime}")

    # Dr. Crusher Extended Scan — Ready Room
    rr = check_ready_room()
    log(f"  Ready Room briefing : {'✓ ' + str(rr.get('briefing_count',0)) + ' today' if rr.get('briefing_today') else '✗ No briefing yet'}")
    rr_age = rr.get('red_alert_age_min')
    log(f"  Red Alert polling   : {'✓ ' + str(rr_age) + 'm ago' if rr.get('red_alert_ok') else '✗ No recent poll' + (f' ({rr_age}m ago)' if rr_age else '')}")
    log(f"  /condition endpoint : {'✓' if rr.get('condition_endpoint') else '✗ NOT RESPONDING'}")

    # -----------------------------------------------------------------------
    # PRIORITY 1: Process / dashboard down → restart immediately
    # -----------------------------------------------------------------------
    needs_restart = not proc_ok or not dash_ok

    if needs_restart:
        reason = "process down" if not proc_ok else "dashboard not responding"
        log(f"ACTION: {reason} — triggering auto-restart")
        auto_restart(reason)
        log("Health check complete (post-restart)")
        log("=" * 60)
        return

    # -----------------------------------------------------------------------
    # PRIORITY 2: Port zombie — 8080 bound but non-200
    # -----------------------------------------------------------------------
    zombie, zombie_info = check_port_zombie()
    log(f"  Port zombie check : {'✗ ZOMBIE — ' + zombie_info if zombie else '✓ ok'}")
    if zombie:
        log(f"ACTION: killing zombie holder and restarting — {zombie_info}")
        try:
            port_pids = subprocess.check_output(["lsof", "-ti", ":8080"],
                                                text=True, stderr=subprocess.DEVNULL).split()
            for pid in port_pids:
                subprocess.run(["kill", "-9", pid], capture_output=True)
        except Exception:
            pass
        auto_restart(f"port zombie: {zombie_info}")
        log("Health check complete (post-zombie-restart)")
        log("=" * 60)
        return

    # -----------------------------------------------------------------------
    # Server is up — run secondary auto-restart triggers
    # -----------------------------------------------------------------------
    log("Server is up — running secondary checks...")

    # 3. OLLAMA STALLED
    ollama_stalled, stall_info = check_ollama_stalled()
    log(f"  Ollama pipeline   : {'✗ STALLED — ' + stall_info if ollama_stalled else '✓ ' + stall_info}")
    if ollama_stalled:
        auto_restart(f"Ollama stalled: {stall_info}", also_restart_ollama=True)
        log("Health check complete (post-ollama-stall-restart)")
        log("=" * 60)
        return

    # 4. WEBSOCKET DEAD
    ws_dead, ws_info = check_websocket_dead()
    log(f"  WebSocket         : {'✗ DEAD — ' + ws_info if ws_dead else '✓ ' + ws_info}")
    if ws_dead:
        auto_restart(f"WebSocket dead: {ws_info}")
        log("Health check complete (post-websocket-restart)")
        log("=" * 60)
        return

    # 5. SCHEDULER SPAM
    sched_spam, sched_count = check_scheduler_spam()
    log(f"  Scheduler errors  : {sched_count} {'✗ SPAM — restarting' if sched_spam else ('⚠️ elevated' if sched_count > 10 else '✓')}")
    if sched_spam:
        auto_restart(f"scheduler spam: {sched_count} 'Scheduler job error' entries")
        log("Health check complete (post-scheduler-restart)")
        log("=" * 60)
        return

    # 6. DAYBLADE DARK
    db_dark, db_dark_info = check_dayblade_dark()
    log(f"  DayBlade activity : {'✗ DARK — ' + db_dark_info if db_dark else '✓ active'}")
    if db_dark:
        log(f"WARNING: DayBlade dark — {db_dark_info}")
        notify("⚠️ Dr. Crusher", f"DayBlade dark: {db_dark_info}")
        auto_restart(f"DayBlade dark: {db_dark_info}")
        log("Health check complete (post-dayblade-restart)")
        log("=" * 60)
        return

    # -----------------------------------------------------------------------
    # Fix 2: DB backup (once per day, non-fatal)
    # -----------------------------------------------------------------------
    did_backup, backup_msg = backup_trader_db()
    log(f"  DB backup         : {'✓ ' + backup_msg if did_backup else '– ' + backup_msg}")

    # -----------------------------------------------------------------------
    # Fix 4: Tunnel health — restart if stale (local up but bridge down)
    # -----------------------------------------------------------------------
    tunnel_stale, tunnel_info = check_tunnel_health()
    log(f"  Tunnel health     : {'✗ STALE — ' + tunnel_info if tunnel_stale else '✓ ' + tunnel_info}")
    if tunnel_stale and dash_ok:  # only restart tunnel when local is healthy
        log("ACTION: restarting stale Cloudflare tunnel")
        restart_tunnel()
        notify("⚠️ Dr. Crusher", f"Tunnel restarted: {tunnel_info}")
        post_war_room(f"⚠️ Tunnel stale — restarted: {tunnel_info}")

    # -----------------------------------------------------------------------
    # Fix 5: Memory monitor — kill Archer if RAM < 1GB, warn always
    # -----------------------------------------------------------------------
    mem_low, mem_info = check_memory()
    log(f"  Memory            : {'⚠️ LOW — ' + mem_info if mem_low else '✓ ' + mem_info}")
    if mem_low:
        notify("⚠️ Dr. Crusher", f"Low memory: {mem_info}")
        post_war_room(f"⚠️ Low memory: {mem_info}")

    # -----------------------------------------------------------------------
    # Fix 6: Archer trading priority — kill during market hours if Ollama busy
    # -----------------------------------------------------------------------
    archer_killed, archer_info = check_archer_trading_priority()
    if archer_killed:
        log(f"  Archer priority   : ⚡ KILLED — {archer_info}")
        notify("⚡ Dr. Crusher", f"Archer killed for trading: {archer_info}")
        post_war_room(f"⚡ Archer killed — trading brain priority: {archer_info}")
    else:
        log(f"  Archer priority   : – {archer_info}")

    archer_restarted, restart_info = check_archer_restart_due()
    if archer_restarted:
        log(f"  Archer restart    : ✓ {restart_info}")
        notify("✓ Dr. Crusher", f"Archer restarted post-market: {restart_info}")
        post_war_room(f"✓ Archer restarted after market close: {restart_info}")
    elif _archer_killed_for_trading:
        log(f"  Archer restart    : – {restart_info}")

    # -----------------------------------------------------------------------
    # No restart needed — collect non-fatal warnings
    # -----------------------------------------------------------------------
    log("All auto-restart checks passed")
    warnings = []

    if not db_ok:
        warnings.append(f"trader.db integrity: {db_msg}")
    if not log_ok:
        warnings.append(f"logs/scanner.err stale ({log_age}m)")
        notify_with_show(
            "⚠️ Dr. Crusher",
            f"trader_error.log is stale ({log_age}m) — server may be stuck",
            os.path.join(BASE_DIR, "logs", "trader_error.log"),
        )
    if not ollama_ok:
        warnings.append(f"Ollama unreachable: {ollama_info}")
    if sched_count > 10:
        warnings.append(f"Scheduler job errors: {sched_count} occurrences (below restart threshold of 50)")
    # Finnhub WebSocket errors (non-fatal count)
    try:
        tail_out = subprocess.check_output(
            ["tail", "-200", SCANNER_LOG_PATH],
            text=True, stderr=subprocess.DEVNULL,
        )
        ws_errors = sum(1 for line in tail_out.splitlines()
                        if "WebSocket error" in line or "WebSocket closed" in line)
        if ws_errors > 2:
            warnings.append(f"Finnhub WebSocket: {ws_errors} errors/closes in last 200 log lines")
    except Exception:
        pass
    # Cloudflare tunnel
    try:
        cf_result = subprocess.run(["pgrep", "cloudflared"], capture_output=True)
        if cf_result.returncode != 0:
            warnings.append("Cloudflare tunnel: cloudflared not running")
            notify("⚠️ USS TradeMinds", "Cloudflare tunnel is DOWN — cloudflared not running")
    except Exception:
        pass
    # scanner.log size (warn at 75K, rotate threshold is 100K)
    try:
        wc_out = subprocess.check_output(
            ["wc", "-l", SCANNER_LOG_PATH],
            text=True, stderr=subprocess.DEVNULL,
        )
        line_count = int(wc_out.strip().split()[0])
        if line_count > 75_000:
            warnings.append(f"scanner.log is large: {line_count} lines (rotation triggers at 100K)")
    except Exception:
        pass
    # Alpaca sync stale
    try:
        sync_result = subprocess.run(
            ["grep", "-m", "1", "SYNC.*Portfolio", SCANNER_LOG_PATH],
            capture_output=True,
        )
        if sync_result.returncode != 0:
            warnings.append("Alpaca sync: no 'SYNC.*Portfolio' match found in scanner.log")
    except Exception:
        pass
    if not rr.get("briefing_today"):
        warnings.append("Ready Room: no briefing today (expected by 9:30 AM ET)")
    if not rr.get("condition_endpoint"):
        warnings.append("Ready Room: /condition endpoint not responding")
        notify("⚠️ Dr. Crusher", "Ready Room offline — /condition endpoint not responding")

    for w in warnings:
        log(f"WARNING: {w}")
    if warnings:
        # Only send one bulk notification to avoid spam
        summary_body = "\n".join([
            f"{len(warnings)} warning(s) on bigmac:",
            "",
            *[f"• {w}" for w in warnings],
            "",
            f"Time: {datetime.now().strftime('%H:%M:%S %Z')}",
            "",
            "Diagnose:",
            "  ssh bigmac 'tail -80 ~/autonomous-trader/logs/healthcheck.log'",
        ])
        notify("⚠️ USS TradeMinds", summary_body, priority="high")
        post_war_room("⚠️ Health check warnings: " + "; ".join(warnings))
    else:
        log("All systems nominal — no warnings")

    log("Health check complete")
    log("=" * 60)


if __name__ == "__main__":
    main()
