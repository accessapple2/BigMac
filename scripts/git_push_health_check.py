"""HM-PUSH-HEALTH-MONITOR (2026-05-29, fixed 2026-06-28).

Independent daily watchdog for git push health. Runs `git fetch` then counts
how many local commits are ahead of the CURRENT BRANCH'S OWN upstream (@{u});
NTFYs ollietrades-admin WARNING if ahead > THRESHOLD, OR if `git fetch` itself
fails (can't reach origin — also a push-pipeline health problem worth alerting
on).

Using @{u} instead of origin/main prevents false alarms when developing on a
feature branch (e.g. exec-pipeline) that is fully pushed but diverges from
main by design.  If no upstream is set the monitor exits 0 silently.

DOCTRINE — an alarm must run on a DIFFERENT mechanism than the thing it watches.
The 87-commit silent push gap (HM-PUSH-UNBLOCK, 2026-05-28) went undetected
because nothing monitored push health independently of the push pipeline. This
cron is that independent watchdog (second instance of the shared-fate disease in
one day, after the Schwab launchd watcher/alarm). See CLAUDE.md "Alarms must not
share a failure mode with what they watch".

Fires daily via crontab (NOT launchd — launchd doesn't survive reboot on this
box; see CLAUDE.md "LaunchAgent Reboot Lifecycle").

Exit codes:
  0 - healthy (within threshold, fetch ok) OR no upstream set (skip)
  2 - alert fired (ahead > threshold, or fetch failed)
  1 - error
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

REPO = str(Path(__file__).resolve().parent.parent)
THRESHOLD = int(os.environ.get("PUSH_HEALTH_AHEAD_THRESHOLD", "5"))
GIT = os.environ.get("GIT_BIN", "/usr/bin/git")


def _git(*args, timeout: int = 60):
    return subprocess.run(
        [GIT, "-C", REPO, *args],
        capture_output=True, text=True, timeout=timeout,
    )


def main() -> int:
    # Resolve current branch upstream (e.g. "origin/exec-pipeline").
    # If no upstream is configured, skip silently — nothing to check.
    r = _git("rev-parse", "--abbrev-ref", "@{u}")
    if r.returncode != 0:
        r2 = _git("rev-parse", "--abbrev-ref", "HEAD")
        branch = r2.stdout.strip() if r2.returncode == 0 else "unknown"
        print(f"[push-health] no upstream set for branch '{branch}' — skipping")
        return 0
    upstream_ref = r.stdout.strip()  # e.g. "origin/exec-pipeline"
    remote = upstream_ref.split("/")[0]  # e.g. "origin"

    fetch_ok = True
    fetch_err = ""
    try:
        r = _git("fetch", remote)
        if r.returncode != 0:
            fetch_ok = False
            fetch_err = (r.stderr or r.stdout).strip()[:200]
    except Exception as e:
        fetch_ok = False
        fetch_err = f"{type(e).__name__}: {e}"

    # Count local commits ahead of the branch's own upstream (@{u}).
    # If fetch failed this is vs the last-known (stale) ref — still informative.
    try:
        r = _git("rev-list", "--count", "@{u}..HEAD")
        if r.returncode != 0:
            print(f"[push-health] rev-list failed: {r.stderr.strip()}", file=sys.stderr)
            return 1
        ahead = int((r.stdout or "0").strip() or "0")
    except Exception as e:
        print(f"[push-health] error: {type(e).__name__}: {e!r}", file=sys.stderr)
        return 1

    if not fetch_ok:
        msg = (
            f"git push-health: `git fetch {remote}` FAILED — cannot reach "
            f"origin (push pipeline may be down). Local is {ahead} ahead of "
            f"last-known {upstream_ref}. err: {fetch_err}"
        )
        print(f"[push-health] FETCH-FAIL — {msg}")
        _alert(msg, "warning")
        return 2

    if ahead > THRESHOLD:
        msg = (
            f"git push-health: local is {ahead} commits ahead of {upstream_ref} "
            f"(threshold {THRESHOLD}). Unpushed work — run `git push`. "
            f"Backstop for the HM-PUSH-UNBLOCK 87-commit silent gap."
        )
        print(f"[push-health] AHEAD — {msg}")
        _alert(msg, "warning")
        return 2

    print(f"[push-health] OK — {ahead} ahead of {upstream_ref} "
          f"(threshold {THRESHOLD}), fetch ok")
    return 0


def _alert(message: str, level_kw: str = "warning") -> None:
    """Send NTFY to ollietrades-admin. engine.alert_channels when available;
    plain HTTP POST fallback otherwise (matches alert_channels topic convention)."""
    try:
        from engine.alert_channels import send_alert, AlertLevel
        level_map = {"warning": AlertLevel.WARNING, "info": AlertLevel.INFO}
        send_alert(
            message=message,
            level=level_map.get(level_kw, AlertLevel.WARNING),
            alert_type=f"hm-push-health-{level_kw}",
            rate_limit_secs=86400,  # 24h per process; fine for daily cron
        )
        print("[push-health] NTFY dispatched via engine.alert_channels")
        return
    except Exception as e:
        print(f"[push-health] engine.alert_channels unavailable: {e}", file=sys.stderr)

    try:
        import urllib.request
        topic = os.environ.get("NTFY_TOPIC", "ollietrades-admin")
        req = urllib.request.Request(
            f"https://ntfy.sh/{topic}",
            data=message.encode("utf-8"),
            headers={
                "Title": "TradeMinds — git push health",
                "Priority": "high",
                "Tags": "warning,git",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            print(f"[push-health] fallback ntfy POST HTTP {r.status}")
    except Exception as e:
        print(f"[push-health] fallback ntfy failed: {e}", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
