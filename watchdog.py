#!/usr/bin/env python3
"""
TradeMinds Watchdog — monitors all services every 60 seconds.

Services watched:
  Bridge         http://127.0.0.1:8080   (launchd: com.trademinds.trader)
  Signal Center  http://127.0.0.1:9000   (launchd: com.trademinds.signal-center)
  Ollama         http://192.168.1.168:11434  (olliemax — systemd: ollama)
  Cloudflare     process: cloudflared    (launchd: com.trademinds.tunnel)

Alerts:  macOS notification + ntfy.sh push (iPhone)
Restart: launchctl kickstart (Bridge, Signal Center, Tunnel). Ollama is
         alert-only — it lives on the olliemax box (192.168.1.168), restarted
         there by its own systemd unit (no passwordless sudo from bigmac), so
         the watchdog can't remotely respawn it.
"""
from __future__ import annotations  # HM-BH hotfix: defer annotation evaluation (venv is Py3.9, no PEP 604 unions)
import subprocess
import socket
import threading
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
MEM_PRESSURE_FREE_CRIT = 10  # HM-BH: macOS memory_pressure free% — true thrash signal (replaces HM-BF SWAP_CRIT_PCT)

BRIDGE_URL        = "http://127.0.0.1:8080/healthz"
SIGNAL_CENTER_URL = "http://127.0.0.1:9000/"
# HM-OLLAMA-WATCH-RETARGET (2026-06-14): was 127.0.0.1:11434 (an unused
# bigmac-local instance — config.py routes ALL live inference to OLLIE_URL),
# so the watchdog spammed "Ollama Down" for a host nothing actually calls.
# Point it at the real inference host (olliemax, config.py OLLIE_URL).
OLLAMA_URL        = "http://192.168.1.168:11434/api/tags"
NTFY_TOPIC        = os.environ.get("NTFY_ADMIN_TOPIC", "ollietrades-admin")  # subscribe in ntfy app on iPhone

DAILY_SNAPSHOT_HOUR_ET = 16   # 4 PM ET
DAILY_SNAPSHOT_MIN_ET  =  5   # 4:05 PM ET

# Bridge restart controls — added 2026-04-17 after cascading-restart incident.
# Dashboard warmup (Ollama model preload + price cache + market history backfill)
# takes ~25-30s; prior 5s HTTP timeout + 6s post-kickstart wait + no cooldown
# meant a single slow startup triggered a kickstart loop every 60s. Fix:
# require 3 consecutive strikes (180s grace), cool down 5 min between
# kickstarts, and wait 30s after kickstart for warmup to complete.
BRIDGE_HTTP_TIMEOUT      = 8     # was implicit 5s; slow cycles were false-flagging
BRIDGE_STRIKES_NEEDED    = 3     # 3 × 60s check interval = 180s grace
BRIDGE_RESTART_COOLDOWN  = 300   # max one kickstart per 5 min
BRIDGE_POST_RESTART_WAIT = 30    # was 6s — matches actual dashboard warmup

# State
_last_notify: dict = {}
_last_snapshot_date: str = ""
_bridge_down_count: int = 0
_bridge_last_restart_ts: float = 0.0


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


# HM-NTFY-IPV6-NOROUTE-WATCHDOG-FIX 2026-07-10: this box has no working
# IPv6 route to ntfy.sh (HM-NTFY-IPV6-NOROUTE, engine/alert_channels.py,
# 2026-07-07) -- confirmed watchdog.py's own push_alert() was hit by this
# too, via real evidence in its own log (12 "ntfy push failed: <urlopen
# error [Errno 65] No route to host>" occurrences, 2026-07-09). Standalone
# fix (not importing engine.alert_channels) because watchdog.py is
# deliberately dependency-free from the engine/ package it monitors --
# it needs to keep working even if that package has an import-time
# problem. Same lock-and-monkeypatch technique alert_channels.py already
# uses, self-contained here.
_ntfy_ipv4_lock = threading.Lock()
_orig_getaddrinfo = socket.getaddrinfo


def _ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    return _orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)


def push_alert(title: str, body: str, key: str = "", priority: str = "high") -> None:
    """iPhone push via ntfy.sh — free, no account needed.
    Install 'ntfy' from App Store → subscribe to: Ollie-Alert-35
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
        with _ntfy_ipv4_lock:
            socket.getaddrinfo = _ipv4_only_getaddrinfo
            try:
                with urllib.request.urlopen(req, timeout=8) as r:
                    log.info(f"Push sent [{r.status}]: {ascii_title}")
            finally:
                socket.getaddrinfo = _orig_getaddrinfo
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


def restart_trader() -> bool:
    """Restart the trader via the orphan-proof scripts/trader_restart.sh.

    HM-WATCHDOG-RESTART-REPOINT (2026-05-30). The old path,
    launchctl_kickstart("com.trademinds.trader"), targeted a gui/$UID LaunchAgent
    that does NOT bootstrap on this headless box — and the trader now runs via
    cron @reboot, not that launchd job — so it was a stale no-op: watchdog alarmed
    but could not actually heal the trader. trader_restart.sh kills ALL trader.log
    WRITE-holders (orphans included) via SIGTERM→SIGKILL, relaunches detached, and
    GATES on exactly one writer. It detects instances by lsof on the log file, not
    by interpreter name (immune to the venv→python3.9 argv reality). It also holds
    an mkdir-atomic flock mutex (HM-TRADER-RESTART-FLOCK): if healthcheck is
    restarting concurrently, this invocation ABORTS exit-4 and defers — two restart
    actors are serialized, no double-spawn. Returns True iff exit 0 (clean restart).
    """
    script = os.path.expanduser("~/autonomous-trader/scripts/trader_restart.sh")
    try:
        r = subprocess.run(["/bin/zsh", script], capture_output=True, text=True, timeout=180)
        for line in (r.stdout or "").splitlines():
            log.info(f"trader_restart: {line}")
        if r.returncode == 0:
            return True
        if r.returncode == 4:
            log.warning("trader_restart: another restart already in progress (flock) — deferring to it")
            return False
        log.error(f"trader_restart exited {r.returncode}: {(r.stderr or '').strip()}")
        return False
    except subprocess.TimeoutExpired:
        log.error("trader_restart timed out (>180s) — manual intervention may be needed")
        return False
    except Exception as e:
        log.error(f"trader_restart invocation failed: {type(e).__name__}: {e}")
        return False


# NOTE: no restart_ollama() — Ollama runs on olliemax (192.168.1.168) under its
# own systemd unit, and bigmac has no passwordless sudo there (verified
# 2026-06-14: `ssh bigmac@192.168.1.168 sudo -n systemctl restart ollama` →
# "interactive authentication is required"). The prior `brew services restart
# ollama` was a no-op anyway (ollama is not a brew service on this box), which
# produced the false "restarting → still down → manual fix needed" escalation.
# The down-path is therefore alert-only; recovery is a hands-on / power action
# on olliemax.


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
    global _bridge_down_count, _bridge_last_restart_ts
    if http_ok(BRIDGE_URL, timeout=BRIDGE_HTTP_TIMEOUT):
        _bridge_down_count = 0   # reset strikes on any success
        return
    # ── strike system: require N consecutive failures before acting ──
    # Dashboard warmup takes ~25-30s after kickstart; a single slow response
    # during that window used to trigger another kickstart, creating a loop.
    _bridge_down_count += 1
    if _bridge_down_count < BRIDGE_STRIKES_NEEDED:
        log.info(
            f"Bridge unresponsive ({_bridge_down_count}/{BRIDGE_STRIKES_NEEDED} strikes) — "
            f"waiting before kickstart (dashboard warmup can take 30s)"
        )
        return
    # ── cooldown: no more than one kickstart per BRIDGE_RESTART_COOLDOWN sec ──
    now = time.time()
    if now - _bridge_last_restart_ts < BRIDGE_RESTART_COOLDOWN:
        remaining = int(BRIDGE_RESTART_COOLDOWN - (now - _bridge_last_restart_ts))
        log.warning(f"Bridge still down but cooldown active ({remaining}s remaining)")
        return
    _bridge_last_restart_ts = now
    _bridge_down_count = 0
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
    alert("🚨 Bridge Down", f"Port 8080 unresponsive — {diagnosis}\nRestarting via trader_restart.sh", "bridge")
    restart_trader()
    time.sleep(BRIDGE_POST_RESTART_WAIT)
    if http_ok(BRIDGE_URL, timeout=BRIDGE_HTTP_TIMEOUT):
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
    # Alert-only: olliemax owns the ollama process; the watchdog can't restart
    # it remotely (see restart-path note above). One alert per NOTIFY_COOLDOWN.
    if http_ok(OLLAMA_URL):
        return
    alert(
        "🔴 Ollama Down",
        "olliemax 192.168.1.168:11434 unreachable — check power/network on the box",
        "ollama",
    )


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


# === HM-BH: macOS memory_pressure helper ===
# psutil.swap_memory().percent stays high on healthy macOS (compressed-memory
# is a feature). memory_pressure's "System-wide memory free percentage" is
# the OS-native thrash signal — same metric scripts/vitals.sh:29 already uses.
def _macos_memory_free_pct() -> int | None:
    """Return macOS 'System-wide memory free percentage' as int 0-100, or None on failure."""
    try:
        out = subprocess.run(
            ["memory_pressure"], capture_output=True, text=True, timeout=3
        ).stdout
        for line in out.splitlines():
            if "System-wide" in line:
                tok = line.rstrip().rstrip("%").rsplit(" ", 1)[-1]
                return int(tok)
    except Exception:
        return None
    return None
# === end HM-BH ===


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
        free_pct  = _macos_memory_free_pct()  # HM-BH: macOS-native pressure metric, int or None
        free_disp = f"{free_pct}%" if free_pct is not None else "?"

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
            f"Swap {swap.percent:.0f}%  Free {free_disp}  Ollama: {ollama_loaded}"
        )

        if cpu > CPU_WARN_PCT:
            log.warning(f"HIGH CPU: {cpu:.0f}% — Ollama inference likely running")

        # === HM-BH: pressure-aware critical trigger (replaces HM-BF swap trigger) ===
        # macOS uses swap aggressively as compressed memory — swap.percent stays
        # high on healthy systems (89% baseline observed). memory_pressure's
        # free% is the OS-native thrash signal: fire critical if free% ≤
        # MEM_PRESSURE_FREE_CRIT or psutil mem.percent ≥ MEM_CRIT_PCT.
        pressure_crit = free_pct is not None and free_pct <= MEM_PRESSURE_FREE_CRIT
        if mem_pct >= MEM_CRIT_PCT or pressure_crit:
            alert(
                "Critical Memory",
                f"RAM {mem_pct:.0f}% / Free {free_disp} / Swap {swap.percent:.0f}% — {mem_avail}GB avail. Shedding load.",
                "mem_crit",
            )
            # Kill VTuber (heaviest non-essential process) to free RAM
            killed = subprocess.run(
                # === HM-BI: narrow pkill to Python invocations of run_server.py
                # (was "run_server.py" — matched any shell/editor with the string in argv)
                ["pkill", "-f", r"python.*run_server\.py"], capture_output=True
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
