# 🔧 SCOTTY — HM-BC.E: init_db Fail-Loud Hardening
### Single-File Hardening · Opus 4.7 · Discover → Diff → Apply

> **Captain's orders, Mr. Scott:** During HM-BC you discovered that engine/ghost_scoring.py's init_db() silently auto-revived a renamed DB by creating an empty file on first call. That silent revival is what hid the ghost-mess from us for weeks. Fix it: make init_db FAIL LOUDLY when the canonical DB doesn't exist, log a clear actionable error, and refuse to create an empty stub. Defensive hygiene, prevents the next silent-data-loss scenario.

## Pre-flight

```bash
cd ~/autonomous-trader
git log origin/main --oneline | head -3
git status --short
pgrep -af main.py | head -1
```

## Phase BCE.0 — Discovery (NO writes)

```bash
echo "── Find every init_db() in the codebase ──"
grep -rn "def init_db\|init_db()" engine/ scripts/ main.py dashboard/app.py 2>/dev/null | head -20

echo ""
echo "── ghost_scoring.py init_db body (per HM-BC context) ──"
grep -n "def init_db\|sqlite3.connect" engine/ghost_scoring.py | head -10
sed -n '1,80p' engine/ghost_scoring.py

echo ""
echo "── Which init_db sites still silently auto-create when DB missing? ──"
grep -B 2 -A 8 "sqlite3.connect" engine/ghost_scoring.py engine/ghost_trades.py 2>/dev/null | head -40

echo ""
echo "── Logger pattern in target file ──"
grep -n "logger\|logging\|console" engine/ghost_scoring.py | head -5
```

Document for Captain:
- Every init_db site found
- Which ones silently auto-create when DB missing (= bug class)
- Proposed fix pattern: explicit os.path.exists() check → raise/log → refuse to connect if missing
- Q1: scope — just ghost_scoring.py (the known offender) OR all init_db sites we find?
- Q2: failure mode — raise an exception (loud crash on startup; launchd respawn loops) OR log error + skip gracefully (degraded mode)?

Write `data/scotty_hm_bce_report.md`. **HALT.** ntfy.

## Phase BCE.1 — Apply (after Captain confirms Q1+Q2)

- Diff with `# === HM-BC.E ===` anchor
- Compile check (use venv/bin/python3 per BHBI lesson)
- Commit: `fix(observability): HM-BC.E — init_db fail-loud when canonical DB missing`
- Push inline (no service restart needed — defensive code only fires on missing-DB scenario)
- ntfy

## Phase BCE.C — Verify

```bash
grep -n "HM-BC.E" engine/ghost_scoring.py | head -3
git log origin/main --oneline | head -3
```

ntfy: `🏁 HM-BC.E complete — silent DB revival defused`.
