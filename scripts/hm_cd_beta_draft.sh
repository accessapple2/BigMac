#!/bin/bash
# HM-CD-β: batch plist hygiene fixes. Adds WorkingDirectory + SoftResourceLimits
# to plists that are missing them, per the HM-CD-α audit.
#
# Usage:
#   bash scripts/hm_cd_beta_draft.sh                       # dry-run (default)
#   bash scripts/hm_cd_beta_draft.sh --dry-run             # explicit dry-run
#   bash scripts/hm_cd_beta_draft.sh --apply               # apply LOW + MED only
#                                                          # (HIGH plists SKIPPED by --apply
#                                                          #  per HM-CD-β-2026-05-15 gate)
#   bash scripts/hm_cd_beta_draft.sh --apply-high LABEL    # apply ONE HIGH plist by label
#                                                          # (per-plist Captain re-approval gate)
#   bash scripts/hm_cd_beta_draft.sh --revert              # restore from /tmp/plist_backup_*
#
# HIGH-risk plists (mcp, scanner, watchdog, tunnel) are gated individually.
# Each one needs its own --apply-high invocation. This is the inline
# Captain re-approval gate added 2026-05-15 — bulk --apply will not touch them.

set -uo pipefail

MODE="${1:-dry-run}"
HIGH_LABEL=""
case "$MODE" in
  --apply)    MODE_VAR="apply" ;;
  --apply-high)
              MODE_VAR="apply-high"
              HIGH_LABEL="${2:-}"
              if [ -z "$HIGH_LABEL" ]; then
                echo "Error: --apply-high requires a plist label."
                echo "  Allowed: com.trademinds.mcp | com.trademinds.scanner |"
                echo "           com.trademinds.watchdog | com.trademinds.tunnel"
                exit 1
              fi
              ;;
  --dry-run|dry-run) MODE_VAR="dry-run" ;;
  --revert)   MODE_VAR="revert" ;;
  *) echo "Usage: $0 [--dry-run|--apply|--apply-high LABEL|--revert]" ; exit 1 ;;
esac

MODE="$MODE_VAR" HIGH_LABEL="$HIGH_LABEL" venv/bin/python3 - <<'PY'
import os
import plistlib
import shutil
from pathlib import Path
from datetime import datetime
from subprocess import run, PIPE

MODE = os.environ["MODE"]
HIGH_LABEL = os.environ.get("HIGH_LABEL", "")
LA = Path("/Users/bigmac/Library/LaunchAgents")
WORKDIR = "/Users/bigmac/autonomous-trader"
SOFT_FD = 16384
HARD_FD = 32768
TODAY = datetime.now().strftime("%Y-%m-%d")

# (label, risk, want_workdir, want_fdlimits, want_stderr_path, note)
PLISTS = [
    # ── LOW risk batch (apply first; minimal blast radius) ─────────────
    ("com.trademinds.caffeinate",                 "LOW",  True,  False, False, "trivial caffeinate; WD harmless"),
    ("com.ollietrades.crusher",                   "LOW",  True,  False, False, "interval=360s; not in critical path"),
    ("com.ollietrades.etfregime",                 "LOW",  True,  False, False, "calendar-based; not blocking"),
    ("com.ollietrades.morning-an2-observation",   "LOW",  True,  False, False, "shell script handles cd internally; hygiene only"),
    ("com.ollietrades.morning-cd-instr",          "LOW",  True,  False, False, "shell script handles cd internally; hygiene only"),
    ("com.ollietrades.stale-trim-obs",            "LOW",  True,  False, False, "calendar-based"),
    # ── MED risk batch (apply after LOW confirmed clean) ────────────────
    ("com.ollietrades.optionsflow",               "MED",  True,  False, False, "calendar-based, data scraper"),
    ("com.ollietrades.schwab-watcher",            "MED",  True,  False, False, "60s interval, mirrors HM-AT-β"),
    ("com.trademinds.webull-sync",               "MED",  True,  False, False, "calendar-based"),
    ("com.ollietrades.ollama-keepalive",          "MED",  True,  False, True,  "also needs StandardErrorPath (currently undefined)"),
    ("com.ollietrades.danelfin-update",           "MED",  True,  False, False, "10-day ModuleNotFoundError since 2026-05-04 (same class as morningbriefing)"),
    ("com.ollietrades.ti-email-poller",           "MED",  True,  False, False, "hygiene only — script uses Path(__file__) for resolution"),
    ("com.ollietrades.ti-picks-watcher",          "MED",  True,  False, False, "hygiene only — script handles cd internally"),
    # ── HIGH risk batch (apply LAST; critical services) ─────────────────
    ("com.trademinds.mcp",                        "HIGH", False, True,  False, "long-running daemon; FD headroom needed"),
    ("com.trademinds.scanner",                    "HIGH", False, True,  False, "long-running daemon; FD headroom needed"),
    ("com.trademinds.watchdog",                   "HIGH", False, True,  False, "CRITICAL — if watchdog FDs out, no recovery for trader/SC"),
    ("com.trademinds.tunnel",                     "HIGH", True,  True,  False, "26 fresh stderr errors today; root-cause needed BEFORE config patch"),
]

def show_change(label, diff_lines, risk):
    print(f"  [{risk:<4}] {label}")
    for line in diff_lines:
        print(f"           {line}")

def edit_plist(path: Path, want_workdir, want_fdlimits, want_stderr):
    """Returns (changed: bool, changes: list[str])."""
    with open(path, "rb") as f:
        pl = plistlib.load(f)
    changes = []
    if want_workdir and "WorkingDirectory" not in pl:
        pl["WorkingDirectory"] = WORKDIR
        changes.append(f"+ WorkingDirectory = {WORKDIR}")
    elif want_workdir:
        changes.append(f"= WorkingDirectory already present ({pl.get('WorkingDirectory')!r})")
    if want_fdlimits:
        if "SoftResourceLimits" not in pl:
            pl["SoftResourceLimits"] = {"NumberOfFiles": SOFT_FD}
            changes.append(f"+ SoftResourceLimits.NumberOfFiles = {SOFT_FD}")
        if "HardResourceLimits" not in pl:
            pl["HardResourceLimits"] = {"NumberOfFiles": HARD_FD}
            changes.append(f"+ HardResourceLimits.NumberOfFiles = {HARD_FD}")
    if want_stderr and "StandardErrorPath" not in pl:
        # Best-effort default — captain can override
        label = pl.get("Label", path.stem).replace(".", "_")
        default_err = f"{WORKDIR}/logs/{label}_stderr.log"
        pl["StandardErrorPath"] = default_err
        changes.append(f"+ StandardErrorPath = {default_err}")
    return pl, changes

# Per HM-CD-β spec: backup to /tmp/plist_backup_$ts/ before modification.
# Computed once at run start; same dir for all plists in this run.
TS = datetime.now().strftime("%Y%m%d_%H%M%S")
BACKUP_DIR = Path(f"/tmp/plist_backup_{TS}")
if MODE in ("apply", "apply-high", "revert"):
    BACKUP_DIR.mkdir(exist_ok=True)

def _bak_path(path: Path) -> Path:
    """For revert mode we look for the most recent /tmp/plist_backup_*/<name>."""
    return BACKUP_DIR / path.name

def apply_plist(path: Path, new_pl) -> bool:
    """Backup then write. Returns True on lint success."""
    bak = _bak_path(path)
    shutil.copy2(path, bak)
    with open(path, "wb") as f:
        plistlib.dump(new_pl, f)
    r = run(["plutil", "-lint", str(path)], capture_output=True, text=True)
    if r.returncode != 0:
        shutil.copy2(bak, path)
        print(f"           ❌ plutil -lint FAILED — reverted from {bak}")
        print(f"           {r.stderr.strip()}")
        return False
    return True

def revert_plist(path: Path):
    """Restore from the MOST RECENT /tmp/plist_backup_*/<name>."""
    candidates = sorted(Path("/tmp").glob(f"plist_backup_*/{path.name}"))
    if not candidates:
        return False
    src_bak = candidates[-1]  # most recent
    shutil.copy2(src_bak, path)
    return True

# ── Main ─────────────────────────────────────────────────────────────────
print(f"HM-CD-β mode: {MODE}")
print(f"Total plists in batch: {len(PLISTS)}")
print()

# Group by risk for ordered output
order = ["LOW", "MED", "HIGH"]
by_risk = {r: [p for p in PLISTS if p[1] == r] for r in order}

total_changed = 0
total_skipped = 0
needs_reload = []

for risk in order:
    # apply-high mode: skip non-HIGH tiers entirely (they get a separate --apply pass)
    if MODE == "apply-high" and risk != "HIGH":
        continue
    print(f"── {risk} RISK BATCH ({len(by_risk[risk])} plists) ──")
    for label, _, want_wd, want_fd, want_err, note in by_risk[risk]:
        path = LA / f"{label}.plist"
        if not path.exists():
            print(f"  [{risk}] {label}: ⚠️ plist not found at {path}")
            continue
        if MODE == "revert":
            ok = revert_plist(path)
            print(f"  [{risk}] {label}: {'✓ reverted' if ok else '(no backup to revert)'}")
            continue
        # ── HIGH-risk gate (HM-CD-β-2026-05-15) ──────────────────────────
        # --apply skips HIGH entirely — Captain must invoke --apply-high LABEL
        # per individual plist. Each HIGH plist is its own re-approval boundary.
        if MODE == "apply" and risk == "HIGH":
            print(
                f"  [{risk}] {label}: SKIPPED — HIGH-risk gate. To apply, run:"
            )
            print(
                f"           bash scripts/hm_cd_beta_draft.sh --apply-high {label}"
            )
            total_skipped += 1
            continue
        if MODE == "apply-high" and label != HIGH_LABEL:
            # apply-high targets one specific plist — skip the others silently
            continue
        # ── end gate ─────────────────────────────────────────────────────
        new_pl, changes = edit_plist(path, want_wd, want_fd, want_err)
        if not [c for c in changes if c.startswith("+")]:
            total_skipped += 1
            print(f"  [{risk}] {label}: no changes needed ({note})")
            continue
        show_change(label, changes, risk)
        print(f"           ── note: {note}")
        if MODE in ("apply", "apply-high"):
            ok = apply_plist(path, new_pl)
            if ok:
                print(f"           ✓ written, plutil -lint passed")
                total_changed += 1
                needs_reload.append(label)
            else:
                pass  # already reported
        else:
            total_changed += 1  # would-be
    print()

print(f"── Summary ──")
print(f"  Changed:  {total_changed}")
print(f"  Skipped:  {total_skipped} (already present / no missing keys)")
print(f"  Mode:     {MODE}")
if MODE in ("apply", "apply-high") and needs_reload:
    print()
    print("── Services needing launchctl reload (Captain action — not run by script) ──")
    print()
    risk_for = {label: risk for label, risk, *_ in PLISTS}
    low_med = [l for l in needs_reload if risk_for.get(l) in ("LOW", "MED")]
    high = [l for l in needs_reload if risk_for.get(l) == "HIGH"]
    if low_med:
        print("  # LOW + MED reload (any order; services independent):")
        for label in low_med:
            print(f"  launchctl unload ~/Library/LaunchAgents/{label}.plist && launchctl load ~/Library/LaunchAgents/{label}.plist")
    if high:
        print()
        print("  # HIGH-risk reload — ONE AT A TIME, watchdog absolute last:")
        # mcp, scanner, tunnel first; watchdog last
        order_high = ["com.trademinds.mcp", "com.trademinds.scanner", "com.trademinds.tunnel", "com.trademinds.watchdog"]
        for label in order_high:
            if label in high:
                print(f"  launchctl unload ~/Library/LaunchAgents/{label}.plist && launchctl load ~/Library/LaunchAgents/{label}.plist")
                print(f"  sleep 5 && launchctl list | grep {label}  # verify alive before next")
PY
