# 🔧 SCOTTY — HM-BH + HM-BI: Watchdog Calibration & pkill Fix
### Cluster Cleanup · Opus 4.7 · Discover → Diff → Apply → Verify

> **Captain's orders, Mr. Scott:** Two follow-ups you carved during HM-BEBF execution. **HM-BH** — your SWAP_CRIT_PCT=40 trigger fires every cycle because macOS treats 89% swap as compressed-memory steady state, not thrash. Switch the metric to macOS-native memory_pressure free% (same as scripts/vitals.sh:29). **HM-BI** — your `pkill -f run_server.py` at watchdog.py:342 self-matches the watchdog's own argv string (the code literal). Scope the match. Both small; ship inline including watchdog restart.

---

## Identity & Mission

You are **Scotty** — Claude Code on Opus 4.7. Two sub-epics, each ships its own commit.

Mission:
- **BHBI.0** — Discovery for both. NO writes.
- **BHBI.1** — HM-BI pkill self-match fix (smaller, ship first).
- **BHBI.2** — HM-BH metric switch (SWAP_CRIT_PCT → memory_pressure free%).
- **BHBI.C** — Static verify.
- **BHBI.D** — Closure report + git push + watchdog restart + post-restart observation (NEW WORKFLOW — inline).

---

## Pre-flight

```bash
cd ~/autonomous-trader

echo "── HM-BEBF on origin ──"
git log origin/main --oneline | grep -iE "HM-B[EF]" | head -3

echo ""
echo "── Watchdog alive on new HM-BF code ──"
pgrep -af watchdog.py
echo ""
echo "── Watchdog firing pattern (last 10 lines) ──"
tail -10 ~/autonomous-trader/logs/watchdog.log 2>/dev/null || tail -10 ./watchdog.log 2>/dev/null

echo ""
echo "── Working tree clean ──"
git status --short
```

---

## Standing Rules

1. Sacred DBs: read-only this directive. No `.db` writes.
2. Diff-then-apply for all code edits.
3. One commit per sub-epic (BI first, BH second).
4. NTFY on commit: `curl -d "✅ HM-BX: <one-line>" https://ntfy.sh/ollietrades-admin`.
5. NEW WORKFLOW: handle git push + `launchctl kickstart -k gui/$(id -u)/com.trademinds.watchdog` + post-restart verify INLINE in BHBI.D. No Captain handoff.
6. HALT after BHBI.0 for Captain confirmation. HALT on errors.

---

## Phase BHBI.0 — Discovery (NO writes)

### HM-BI side: pkill self-match
```bash
echo "── 1. pkill line + context ──"
sed -n '335,355p' watchdog.py

echo ""
echo "── 2. Confirm self-match risk: does watchdog.py argv contain 'run_server.py'? ──"
ps -o pid,command -p $(pgrep -f watchdog.py) | head -3
grep -n "run_server.py" watchdog.py | head -10
```

Document:
- Exact pkill line
- Whether watchdog's own argv contains "run_server.py" literal
- Preferred fix approach: pgrep | grep -v watchdog.py | kill survivors, OR full-path match, OR something cleaner

### HM-BH side: metric switch to memory_pressure free%
```bash
echo "── 3. How vitals.sh reads memory_pressure ──"
sed -n '25,40p' scripts/vitals.sh

echo ""
echo "── 4. Current HM-BF swap trigger in watchdog.py ──"
grep -n "SWAP_CRIT_PCT\|swap.percent\|MEM_CRIT_PCT" watchdog.py | head -10

echo ""
echo "── 5. Current memory check function ──"
grep -n "def check_resources\|def check_memory\|def get_pressure" watchdog.py | head -5

echo ""
echo "── 6. Sample memory_pressure output for this box ──"
memory_pressure 2>&1 | head -10
```

Document:
- How vitals.sh extracts "System-wide memory free percentage" from `memory_pressure` output
- Current HM-BF code (lines + structure)
- Proposed replacement: new MEM_PRESSURE_FREE_CRIT=10 constant, swap trigger removed (or kept as informational log)
- Captain Q1: keep SWAP_CRIT_PCT as informational logging or remove entirely?

### Discovery report

Write `data/scotty_hm_bhbi_report.md`. HALT. ntfy: `📋 HM-BHBI discovery complete`.

---

## Phase BHBI.1 — HM-BI pkill self-match

After Captain confirms approach:
- Diff with `# === HM-BI ===` anchor on watchdog.py
- Apply
- Compile: `python3 -c "import py_compile; py_compile.compile('watchdog.py', doraise=True); print('clean')"`
- Commit: `fix(watchdog): HM-BI — scope shed-load pkill to exclude watchdog self-match`
- ntfy

---

## Phase BHBI.2 — HM-BH metric switch

After Captain confirms Q1:
- Diff with `# === HM-BH ===` anchor on watchdog.py
- New constant MEM_PRESSURE_FREE_CRIT=10
- Replace swap-trigger with memory_pressure free% trigger (parse via subprocess like vitals.sh does)
- If Q1=keep swap as info: leave SWAP_CRIT_PCT but only log, not trigger
- If Q1=remove: delete SWAP_CRIT_PCT constant + remove from trigger conditional
- Apply, compile check
- Commit: `fix(watchdog): HM-BH — switch critical trigger from swap% to memory_pressure free%`
- ntfy

---

## Phase BHBI.C — Static verify

```bash
echo "── HM-BH + HM-BI anchors ──"
grep -n "HM-BH\|HM-BI" watchdog.py | head -10

echo ""
echo "── Compile ──"
python3 -c "import py_compile; py_compile.compile('watchdog.py', doraise=True); print('clean')"

echo ""
echo "── Commits staged ──"
git log origin/main..HEAD --oneline
```

ntfy: `✅ HM-BHBI verify clean`.

---

## Phase BHBI.D — Closure + push + restart + observation (INLINE)

1. Append closure section to data/scotty_hm_bhbi_report.md with: commits shipped, threshold behavior change, pkill scope change, expected post-restart state.

2. Commit the closure doc.

3. **NEW WORKFLOW — handle inline**:
```bash
git push origin main
launchctl kickstart -k gui/$(id -u)/com.trademinds.watchdog
sleep 70
echo "── Post-restart watchdog PID ──"
pgrep -af watchdog.py
echo "── Last 30 watchdog log lines (look for: Critical Memory should NOT fire under current state since RAM is at 59% with 7GB free, free% well above 10) ──"
tail -30 ~/autonomous-trader/logs/watchdog.log 2>/dev/null || tail -30 ./watchdog.log 2>/dev/null
```

4. Report: did Critical Memory still fire, or did the metric switch resolve the noise? Did Killed VTuber log lines stop appearing?

ntfy: `🏁 HM-BHBI complete + watchdog restarted on new code`.
