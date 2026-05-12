# HM-BH + HM-BI Discovery — Watchdog Calibration & pkill Fix

**Phase BHBI.0 — read-only profiling. No code edits, no DB writes, no restart.**
**Date:** 2026-05-11
**Engineer:** Scotty (Opus 4.7)

---

## Pre-flight summary
- HM-BE (`3e90dbc`) + HM-BF (`58ecfbc`) confirmed in `origin/main`.
- Watchdog daemon alive on HM-BF code: PIDs 85795 (parent) + transient subprocess children.
- Current firing pattern (last 10 lines): **`Critical Memory` alert on every cycle** (~60s) — `RAM 57–59% / Swap 89%`. NTFY suppressed by 300s cooldown but log fires every cycle. `Unloaded Ollama model from VRAM` every cycle. Working tree has no tracked diffs.

---

# HM-BI — pkill self-match

## The actual pkill line

`watchdog.py:344-350` (line numbers shifted +5 since HM-BF added the anchored block):

```python
# Kill VTuber (heaviest non-essential process) to free RAM
killed = subprocess.run(
    ["pkill", "-f", "run_server.py"], capture_output=True
).returncode == 0
if killed:
    log.warning("Killed VTuber (run_server.py) to free memory")
```

## What the actual risk is (correction to my BEBF.D post-fire note)

I previously claimed the watchdog process's *argv* contains `run_server.py`. **That was wrong.** Verified now:

```
Watchdog argv:  Python /Users/bigmac/autonomous-trader/watchdog.py
```

The string `run_server.py` only exists as a **source-code literal at lines 347, 350** of watchdog.py — it is NOT in the Python interpreter's argv. macOS `pkill -f` scans argv, not process source, and per the man page `pkill` excludes its own PID from matches.

So why did the first cycle at 17:30:24 log `Killed VTuber (run_server.py)` (which requires `pkill returncode == 0` — i.e. at least one match)?

The empirical match-pattern from BEBF post-fire snapshots: `pgrep -af run_server.py` matched **transient PIDs** (85822, 86762, etc.) momentarily and dropped fast. These were almost certainly shell/subprocess wrappers running OUR own bash commands — recall that my interactive Bash commands contained the literal string `run_server.py` as part of the discovery script, so zsh subshells executing those commands had `run_server.py` in their argv. The watchdog's `subprocess.run(["pkill", "-f", "run_server.py"])` happened to fire at the same moment as one of those, hence returncode 0.

**Right now** with no shell side-traffic from me containing the string, `pgrep -af run_server.py` returns nothing. The risk is **NOT** self-match, but **bystander-match against shell processes that contain the string in their argv** — including future operators running diagnostic commands.

## Real risk scenarios

| Scenario | Match? | Outcome |
|---|---|---|
| A real VTuber `run_server.py` Python script running | Yes (intended) | Kill it. ✓ |
| Shell running a command that has `run_server.py` in argv (e.g. `pgrep run_server.py` diagnostics) | Yes (bystander) | Kill the shell, possibly the user's terminal session. ✗ |
| Editor or grep over a file containing the string | Maybe | Kill the editor / grep. ✗ |
| Subprocess like `ollama ps` | No | Safe |
| Watchdog itself | No (verified above) | Safe |

The actual harm is small today (VTuber isn't running, no editors auto-restart) but the pattern is overly permissive.

## Proposed fix (recommended)

**Narrow the match to "a Python interpreter explicitly running a `run_server.py` script":**

```python
killed = subprocess.run(
    ["pkill", "-f", "python.*run_server\\.py"], capture_output=True
).returncode == 0
```

This matches:
- `Python ... /path/to/run_server.py` ✓ (intended VTuber)
- `python3 run_server.py` ✓ (intended)

Does NOT match:
- `pgrep -af run_server.py` (no `python` prefix)
- `vim run_server.py` (no `python` prefix)
- A shell command line containing the literal string (no `python` prefix)

**Alternative considered:** pgrep-first guard (only call pkill if the targets look like Python). More code, same effect; the regex approach is one-line and self-documenting. Picking the regex.

**Captain question for HM-BI:** approve the regex tightening as drafted?

---

# HM-BH — metric switch

## How vitals.sh reads memory_pressure (the prior art)

`scripts/vitals.sh:29`:
```bash
FREE_PCT=$(memory_pressure 2>/dev/null | grep "System-wide" | grep -oE "[0-9]+%")
```

The macOS native `memory_pressure` command emits exactly one line of the form:
```
System-wide memory free percentage: 83%
```
…right at the bottom of the output. The `grep "System-wide" | grep -oE "[0-9]+%"` pipeline extracts e.g. `83%`. Same pattern reused in `scripts/fleet_status.sh:8`.

## Current HM-BF swap trigger (in scope to replace)

`watchdog.py`:
- Line 44: `SWAP_CRIT_PCT = 40` — the constant
- Line 339: `if mem_pct >= MEM_CRIT_PCT or swap.percent >= SWAP_CRIT_PCT:` — the trigger

`MEM_CRIT_PCT` is unchanged at 95 and remains a useful trigger for genuine RAM-exhaustion. `SWAP_CRIT_PCT=40` is the broken half firing on macOS baseline.

## Live memory_pressure output on this box (right now)

```
The system has 17179869184 (1048576 pages with a page size of 16384).
Stats:
Pages free: 87336
...
System-wide memory free percentage: 83%
```

Exit code 0. With the fleet running (trader bridge live, watchdog alive, ollama empty), the system reports **83% free** — a comfortable headroom. At `MEM_PRESSURE_FREE_CRIT=10`, this state would NOT fire critical. The metric matches the operator's intuition: macOS is healthy when `memory_pressure` says it is, regardless of swap usage.

For comparison the SAME state in `psutil.swap_memory().percent` is 89% — the steady-state high-swap-but-healthy paradox HM-BH exists to fix.

## Proposed code change

1. **Add helper at module top** (near the other helpers):
```python
def _macos_memory_free_pct() -> int | None:
    """Return macOS 'System-wide memory free percentage' as int 0-100, or None on failure."""
    try:
        out = subprocess.run(
            ["memory_pressure"], capture_output=True, text=True, timeout=3
        ).stdout
        for line in out.splitlines():
            if "System-wide" in line:
                # e.g. "System-wide memory free percentage: 83%"
                tok = line.rstrip("%").rsplit(" ", 1)[-1]
                return int(tok)
    except Exception:
        return None
    return None
```

2. **Add new constant** (replacing `SWAP_CRIT_PCT`):
```python
MEM_PRESSURE_FREE_CRIT = 10   # HM-BH: macOS memory_pressure free% — true thrash signal
```

3. **Replace trigger** at line 339:
```python
free_pct = _macos_memory_free_pct()  # int or None
pressure_crit = free_pct is not None and free_pct <= MEM_PRESSURE_FREE_CRIT
if mem_pct >= MEM_CRIT_PCT or pressure_crit:
    alert(
        "Critical Memory",
        f"RAM {mem_pct:.0f}% / Free {free_pct if free_pct is not None else '?'}% "
        f"/ Swap {swap.percent:.0f}% — {mem_avail}GB avail. Shedding load.",
        "mem_crit",
    )
```

4. **Optionally also add `Free {free_pct}%` to the cycle log line at 328** so the operator sees this metric every minute.

## Captain question Q1 — what about `SWAP_CRIT_PCT`?

The directive offers two options:
- **α — keep SWAP_CRIT_PCT as informational logging only** (don't trigger on it, but keep the constant + log).
- **β — remove SWAP_CRIT_PCT entirely**.

Once the metric switch lands, `SWAP_CRIT_PCT` is no longer used as a threshold. The swap percentage **value** is already in the log line (`Swap {swap.percent:.0f}%`) at line 329 — that's independent of the constant.

**Scotty's recommendation: β (remove the constant).** Reasoning:
- The constant is dead code after the trigger removes it.
- Per CLAUDE.md style: "Don't add error handling, fallbacks, or validation for scenarios that can't happen." A constant that no code reads is dead weight.
- The HM-BF git history is intact — anyone curious about the prior threshold can `git log -p` to find it.
- The swap **value** stays in the log line; only the unused threshold constant goes.

α adds no functional benefit but increases code surface; preference β.

---

# Summary table — what changes

| File | What | LOC |
|---|---|---|
| `watchdog.py` (HM-BI commit) | Tighten pkill regex to `python.*run_server\.py` | 1 line |
| `watchdog.py` (HM-BH commit) | Add `_macos_memory_free_pct()` helper, add `MEM_PRESSURE_FREE_CRIT` constant, remove `SWAP_CRIT_PCT` constant, replace trigger to OR on free% instead of swap%, optionally extend log line | ~20–25 lines |

Both changes are local to `watchdog.py`. No DB writes. No restart-of-bridge needed (only watchdog kickstart).

## Captain decisions blocking BHBI.1 & BHBI.2

**Q-BI:** Approve narrow pkill regex `python.*run_server\.py`?
*Scotty's recommendation: yes.*

**Q1 (HM-BH):** SWAP_CRIT_PCT — α (keep as info-only constant) or β (remove entirely)?
*Scotty's recommendation: β (remove).*

**Q-BH-extra:** Extend the per-cycle INFO log line to surface `Free {free_pct}%` alongside RAM/Swap?
*Scotty's recommendation: yes (one-line change, free signal-to-operator improvement).*

---

**HALT — awaiting Captain decision on Q-BI, Q1, Q-BH-extra before BHBI.1.**

---

# Captain Decisions (received 2026-05-11)

- **Q-BI:** YES — narrow pkill to `python.*run_server\.py`.
- **Q1:** β — remove `SWAP_CRIT_PCT` constant entirely.
- **Q-BH-extra:** YES — add `Free {free_pct}%` to per-cycle INFO log line.

Inline workflow approved: ship both commits, push, kickstart watchdog, verify against three success criteria post-restart.

---

## HM-BHBI Closure

### HM-BI outcome (commit 07bb46d)
Single-line change at `watchdog.py:349`:
```python
["pkill", "-f", r"python.*run_server\.py"], capture_output=True
```
Old pattern matched any process whose argv contained `run_server.py` (shells, editors, my own discovery scripts). New pattern requires `python` somewhere in argv before `run_server.py`, so:
- ✓ `Python3 .../run_server.py` (intended VTuber)
- ✗ `vim run_server.py`, `pgrep run_server.py`, shell scripts containing the string

Diff: +3 / -1.

### HM-BH outcome (commit c1befbc)
`watchdog.py` +32 / -8:
1. Removed `SWAP_CRIT_PCT = 40` constant.
2. Added `MEM_PRESSURE_FREE_CRIT = 10` constant.
3. Added module-level helper `_macos_memory_free_pct()` (subprocess on `memory_pressure`, parse "System-wide memory free percentage", return int or None).
4. Captured `free_pct = _macos_memory_free_pct()` and `free_disp` in `check_resources()`.
5. Extended per-cycle INFO log line: `CPU x%  RAM y% (Z GB free)  Swap s%  Free f%  Ollama: ...`.
6. Replaced critical trigger from `swap.percent >= SWAP_CRIT_PCT` to `free_pct <= MEM_PRESSURE_FREE_CRIT`.
7. Updated alert body to show RAM% / Free% / Swap% / GB-avail for full context.

Threshold rationale: live `memory_pressure` returned 83% free on this box during discovery, 79% later — both well above 10. Critical fires only at <=10% free OR `mem.percent >= 95`, matching the operator's actual thrash intuition.

### Commits shipped
```
c1befbc fix(watchdog): HM-BH — switch critical trigger from swap% to memory_pressure free%
07bb46d fix(watchdog): HM-BI — scope shed-load pkill to Python invocations only
```

Both will be pushed and the watchdog restarted inline by Scotty per the directive's new workflow.

### Success criteria (post-restart)
1. No `Critical Memory` log lines on the first or subsequent cycles (current state: ~79–83% free, way above the 10% threshold).
2. No `Killed VTuber (run_server.py)` log lines (because no shed-load fires, AND the narrower pkill pattern wouldn't match anything anyway).
3. The per-cycle INFO line includes `Free X%` token alongside `RAM/Swap/Ollama`.

### Restart needed
**Watchdog daemon only.** Trader bridge stays as-is (PID 85371, healthy).

### What's NOT changed
- `MEM_CRIT_PCT=95` still gates on `psutil.virtual_memory().percent` — same as pre-HM-BF, untouched.
- `MEM_WARN_PCT=85` warning tier unchanged.
- Alert cooldowns unchanged (300s for ntfy/macOS notify).
- Shed-load actions (pkill VTuber narrowed match, ollama stop) unchanged except for the HM-BI pattern tightening.
- Dashboard / trader bridge / signal center — all out of scope.
