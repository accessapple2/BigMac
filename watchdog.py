#!/usr/bin/env python3
"""
TradeMinds Watchdog — monitors all services every 60 seconds.

Services watched:
  Bridge         http://127.0.0.1:8080   (launchd: com.trademinds.trader)
  Signal Center  http://127.0.0.1:9000   (launchd: com.trademinds.signal-center)
  Ollama         http://127.0.0.1:11434  (brew services: ollama)
  Cloudflare     process: cloudflared    (launchd: com.trademinds.tunnel)

Alerts:  macOS notification + ntfy.sh push (iPhone)
Restart: launchctl kickstart (Bridge, Signal Center, Tunnel) / brew (Ollama)
"""
import subprocess
import time
import urllib.request
import urllib.error
import os
import logging
try:
    import psutil
    _PSUTIL = True
except ImportError:
    _PSUTIL = False
from datetime import datetime

LOG_PATH = os.path.expanduser("~/autonomous-trader/logs/watchdog.log")
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("watchdog")

# ── config ───────────────────────────────────────────────────────────────────
CHECK_INTERVAL  = 60    # seconds between full sweeps
NOTIFY_COOLDOWN = 300   # seconds before re-alerting the same service

CPU_WARN_PCT    = 90    # alert threshold
MEM_WARN_PCT    = 85    # alert threshold
MEM_CRIT_PCT    = 95    # kill non-essential processes to free RAM

BRIDGE_URL        = "http://127.0.0.1:8080/api/status"
SIGNAL_CENTER_URL = "http://127.0.0.1:9000/"
OLLAMA_URL        = "http://127.0.0.1:11434/api/tags"
NTFY_TOPIC        = "ollietrades-admin"   # subscribe in ntfy app on iPhone

DAILY_SNAPSHOT_HOUR_ET = 16   # 4 PM ET
DAILY_SNAPSHOT_MIN_ET  =  5   # 4:05 PM ET

# State
_last_notify: dict = {}
_last_snapshot_date: str = ""


# ── alert helpers ─────────────────────────────────────────────────────────────
def _cooldown_ok(key: str) -> bool:
    now = time.time()
    if now - _last_notify.get(key, 0) < NOTIFY_COOLDOWN:
        return False
    _last_notify[key] = now
    return True


def mac_notify(title: str, body: str, key: str = "") -> None:
    """macOS notification via osascript."""
    if not _cooldown_ok(key or title):
        return
    script = f'display notification "{body}" with title "{title}" sound name "Funk"'
    try:
        subprocess.run(["osascript", "-e", script], capture_output=True, timeout=5)
    except Exception as e:
        log.warning(f"osascript failed: {e}")


def push_alert(title: str, body: str, key: str = "", priority: str = "high") -> None:
    """iPhone push via ntfy.sh — free, no account needed.
    Install 'ntfy' from App Store → subscribe to: ollietrades-admin
    """
    if not _cooldown_ok((key or title) + "_ntfy"):
        return
    # HTTP headers must be ASCII — strip non-ASCII characters from title
    ascii_title = title.encode("ascii", errors="ignore").decode("ascii").strip()
    if not ascii_title:
        ascii_title = "TradeMinds Alert"
    try:
        req = urllib.request.Request(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=f"{title}\n{body}".encode("utf-8"),
            headers={
                "Title":        ascii_title,
                "Priority":     priority,
                "Tags":         "warning,trademinds",
                "Content-Type": "text/plain; charset=utf-8",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=8) as r:
            log.info(f"Push sent [{r.status}]: {ascii_title}")
    except Exception as e:
        log.warning(f"ntfy push failed: {e}")


def alert(title: str, body: str, key: str = "", priority: str = "high") -> None:
    """Fire both macOS + iPhone alert."""
    log.warning(f"ALERT: {title} — {body}")
    mac_notify(title, body, key)
    push_alert(title, body, key, priority)


# ── http / process helpers ────────────────────────────────────────────────────
def http_ok(url: str, timeout: int = 5) -> bool:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "trademinds-watchdog/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status < 400
    except Exception:
        return False


def process_running(name: str) -> bool:
    try:
        out = subprocess.check_output(["pgrep", "-f", name], text=True)
        return bool(out.strip())
    except subprocess.CalledProcessError:
        return False


def launchctl_kickstart(label: str) -> bool:
    uid = os.getuid()
    try:
        r = subprocess.run(
            ["launchctl", "kickstart", "-k", f"gui/{uid}/{label}"],
            capture_output=True, text=True, timeout=15,
        )
        if r.returncode == 0:
            return True
    except Exception:
        pass
    # Fallback: unload + load
    plist = os.path.expanduser(f"~/Library/LaunchAgents/{label}.plist")
    try:
        subprocess.run(["launchctl", "unload", plist], capture_output=True, timeout=10)
        time.sleep(1)
        r2 = subprocess.run(["launchctl", "load", plist], capture_output=True, timeout=10)
        return r2.returncode == 0
    except Exception as e:
        log.error(f"launchctl {label} failed: {e}")
        return False


def restart_ollama() -> bool:
    try:
        r = subprocess.run(
            ["/opt/homebrew/bin/brew", "services", "restart", "ollama"],
            capture_output=True, text=True, timeout=30,
        )
        return r.returncode == 0
    except Exception as e:
        log.error(f"brew restart ollama failed: {e}")
        return False


def trigger_snapshot() -> None:
    try:
        req = urllib.request.Request(
            "http://127.0.0.1:9000/api/predictions/auto-snapshot",
            data=b"",
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode()
            log.info(f"Daily snapshot triggered: {body[:120]}")
    except Exception as e:
        log.warning(f"Snapshot trigger failed: {e}")


# ── service checks ────────────────────────────────────────────────────────────
def check_bridge() -> None:
    if http_ok(BRIDGE_URL):
        return
    # ── diagnose WHY it's down ──
    diag = []
    import subprocess, shutil
    # 1. Is the process running?
    try:
        result = subprocess.run(["lsof", "-i", ":8080"], capture_output=True, text=True, timeout=5)
        if "LISTEN" in result.stdout:
            diag.append("Process listening but not responding")
        else:
            diag.append("No process on port 8080")
    except Exception:
        diag.append("Could not check port")
    # 2. Is Ollama running?
    ollama = shutil.which("ollama")
    if not ollama:
        try:
            result = subprocess.run(["pgrep", "-x", "ollama"], capture_output=True, text=True, timeout=3)
            if not result.stdout.strip():
                diag.append("Ollama not running")
        except Exception:
            pass
    # 3. Is tunnel alive?
    try:
        result = subprocess.run(["pgrep", "-f", "cloudflared"], capture_output=True, text=True, timeout=3)
        if not result.stdout.strip():
            diag.append("Cloudflare tunnel dead")
    except Exception:
        pass
    # 4. Disk space?
    try:
        result = subprocess.run(["df", "-h", "/"], capture_output=True, text=True, timeout=3)
        for line in result.stdout.splitlines()[1:]:
            parts = line.split()
            if len(parts) >= 5 and int(parts[4].replace("%","")) > 90:
                diag.append(f"Disk {parts[4]} full")
    except Exception:
        pass
    diagnosis = " | ".join(diag) if diag else "Unknown cause"
    alert("🚨 Bridge Down", f"Port 8080 unresponsive — {diagnosis}\nRestarting via launchd", "bridge")
    launchctl_kickstart("com.trademinds.trader")
    time.sleep(6)
    if http_ok(BRIDGE_URL):
        log.info("Bridge RECOVERED")
        push_alert("✅ Bridge Recovered", "Port 8080 is back online", "bridge_ok", "low")
    else:
        alert("🔴 Bridge Still Down", f"Manual fix needed — {diagnosis}", "bridge_fail")


def check_signal_center() -> None:
    if http_ok(SIGNAL_CENTER_URL):
        return
    alert("🚨 Signal Center Down", "Port 9000 unresponsive — restarting via launchd", "sc")
    launchctl_kickstart("com.trademinds.signal-center")
    time.sleep(6)
    if http_ok(SIGNAL_CENTER_URL):
        log.info("Signal Center RECOVERED")
        push_alert("✅ Signal Center Recovered", "Port 9000 is back online", "sc_ok", "low")
    else:
        alert("🔴 Signal Center Still Down", "Manual fix needed on port 9000", "sc_fail")


def check_ollama() -> None:
    if http_ok(OLLAMA_URL):
        return
    alert("🚨 Ollama Down", "Port 11434 unresponsive — restarting", "ollama")
    restart_ollama()
    time.sleep(10)
    if http_ok(OLLAMA_URL):
        log.info("Ollama RECOVERED")
        push_alert("✅ Ollama Recovered", "Port 11434 is back online", "ollama_ok", "low")
    else:
        alert("🔴 Ollama Still Down", "Manual fix needed for Ollama", "ollama_fail")


def check_cloudflare() -> None:
    if os.environ.get("WATCHDOG_SKIP_CLOUDFLARED") == "1":
        return
    if process_running("cloudflared"):
        return
    alert("🚨 Cloudflare Tunnel Down", "cloudflared not running — restarting via launchd", "cf")
    launchctl_kickstart("com.trademinds.tunnel")
    time.sleep(5)
    if process_running("cloudflared"):
        log.info("Cloudflare RECOVERED")
        push_alert("✅ Cloudflare Recovered", "Tunnel is back online", "cf_ok", "low")
    else:
        alert("🔴 Cloudflare Still Down", "Manual fix needed for tunnel", "cf_fail")


def check_resources() -> None:
    """Monitor CPU and memory. Alert and shed load if critical."""
    if not _PSUTIL:
        return
    try:
        cpu = psutil.cpu_percent(interval=1)
        mem = psutil.virtual_memory()
        swap = psutil.swap_memory()
        mem_pct   = mem.percent
        mem_avail = round(mem.available / 1e9, 1)

        # Verbose log every cycle so we have a history trail
        try:
            ollama_ps = subprocess.run(
                ["ollama", "ps"], capture_output=True, text=True, timeout=3
            ).stdout.strip()
            ollama_loaded = ollama_ps.splitlines()[1] if len(ollama_ps.splitlines()) > 1 else "none"
        except Exception:
            ollama_loaded = "?"

        log.info(
            f"CPU {cpu:.0f}%  RAM {mem_pct:.0f}% ({mem_avail}GB free)  "
            f"Swap {swap.percent:.0f}%  Ollama: {ollama_loaded}"
        )

        if cpu > CPU_WARN_PCT:
            log.warning(f"HIGH CPU: {cpu:.0f}% — Ollama inference likely running")

        if mem_pct >= MEM_CRIT_PCT:
            alert(
                "Critical Memory",
                f"RAM {mem_pct:.0f}% — only {mem_avail}GB free. Shedding load.",
                "mem_crit",
            )
            # Kill VTuber (heaviest non-essential process) to free RAM
            killed = subprocess.run(
                ["pkill", "-f", "run_server.py"], capture_output=True
            ).returncode == 0
            if killed:
                log.warning("Killed VTuber (run_server.py) to free memory")
            # Also unload largest Ollama model from VRAM
            try:
                subprocess.run(
                    ["ollama", "stop"],
                    capture_output=True, timeout=10
                )
                log.warning("Unloaded Ollama model from VRAM")
            except Exception:
                pass

        elif mem_pct >= MEM_WARN_PCT:
            alert(
                "Memory Warning",
                f"RAM {mem_pct:.0f}% used — {mem_avail}GB free",
                "mem_warn",
            )

    except Exception as e:
        log.warning(f"Resource check error: {e}")


def check_daily_snapshot() -> None:
    """Fire predictions snapshot once at 4:05 PM ET each trading day."""
    global _last_snapshot_date
    now_utc = datetime.utcnow()
    et_hour = (now_utc.hour - 4) % 24   # EDT (UTC-4, Apr–Nov)
    today   = now_utc.strftime("%Y-%m-%d")
    if (et_hour == DAILY_SNAPSHOT_HOUR_ET and
            now_utc.minute >= DAILY_SNAPSHOT_MIN_ET and
            today != _last_snapshot_date):
        _last_snapshot_date = today
        log.info(f"Triggering daily predictions snapshot for {today}")
        trigger_snapshot()


# ── entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    log.info("=== TradeMinds Watchdog started ===")
    push_alert("🛡 Watchdog Online",
               "Monitoring Bridge/Signal Center/Ollama/Cloudflare every 60s",
               "start", "low")

    cycle = 0
    while True:
        try:
            check_bridge()
            check_signal_center()
            check_cloudflare()
            check_resources()       # CPU/RAM every cycle
            check_daily_snapshot()

            # Stagger Ollama health check — every 3rd cycle (3 min)
            # so we don't hammer it while a model is mid-inference
            if cycle % 3 == 0:
                check_ollama()

            cycle += 1
        except Exception as e:
            log.error(f"Unexpected error in watchdog loop: {e}")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
