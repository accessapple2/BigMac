# 🔧 SCOTTY — HM-BD.F: Loud-Fail Observability at ai_brain Wrappers
### Observability Hardening · Opus 4.7 · Discover → Diff → Apply

> **Captain's orders, Mr. Scott:** Today's biggest lesson — your HM-BD discovery uncovered that two `bare except` wrappers in `engine/ai_brain.py` silently swallowed HTTP 000 timeouts for *months*, hiding a 6-minute cold-path bug. HM-BD.F replaces those swallows with typed exception catches that log loudly. Same bug class, different file, prevented forever.

---

## Identity & Mission

You are **Scotty** — Claude Code on Opus 4.7. Single-file Python change, one commit.

Mission:
- **BDF.0** — Discovery: locate the bare-except wrappers (you flagged ~L911 + ~L968 during HM-BD.0).
- **BDF.1** — Replace with typed catches + WARNING-level logs. Preserve fallback return values.
- **BDF.C** — Static verify + grep for any other bare-except patterns in the same file as bonus.
- **BDF.D** — Closure + git push + restart + verify (inline, new workflow).

---

## Pre-flight

```bash
cd ~/autonomous-trader

echo "── Prerequisites ──"
git log origin/main --oneline | grep -iE "HM-BD" | head -5

echo ""
echo "── Service alive ──"
pgrep -af main.py | head -1
lsof -ti :8080 | head -1

echo ""
echo "── Working tree clean ──"
git status --short
```

---

## Standing Rules

1. Diff-then-apply.
2. One commit total.
3. NTFY each phase.
4. NEW WORKFLOW: handle git push + `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader` + post-restart verify INLINE in BDF.D.
5. HALT after BDF.0 for Captain confirmation on approach.

---

## Phase BDF.0 — Discovery

```bash
echo "── 1. The bare-except wrappers you flagged (~L911 + ~L968) ──"
sed -n '900,940p' engine/ai_brain.py
echo "── ── ── ──"
sed -n '955,985p' engine/ai_brain.py

echo ""
echo "── 2. Other bare-except in same file (bonus find) ──"
grep -n "except:\|except Exception:" engine/ai_brain.py | head -20

echo ""
echo "── 3. Logger import + setup pattern in ai_brain.py ──"
grep -n "logger\|logging\." engine/ai_brain.py | head -10

echo ""
echo "── 4. CLAUDE.md error-handling posture (if documented) ──"
grep -iA 3 "error.handling\|loud.fail\|logging posture" CLAUDE.md 2>/dev/null | head -20
```

Document for Captain:
- Exact lines of each bare-except + what they're wrapping
- Proposed replacement pattern (e.g. catch `(requests.RequestException, TimeoutError, ConnectionError)` → log WARNING → return same fallback)
- Bonus finds: any other bare-except in ai_brain.py that should also get loud-fail treatment
- Captain Q1: scope of fix — just the 2 known wrappers, OR include all bare-except in same file?

Write `data/scotty_hm_bdf_report.md`. HALT. ntfy.

---

## Phase BDF.1 — Apply loud-fail wrappers

After Captain approves scope:
- Diff with `# === HM-BD.F ===` anchor
- Pattern per wrapper:
```python
  # === HM-BD.F ===
  try:
      ...existing call...
  except (requests.RequestException, TimeoutError, ConnectionError) as e:
      logger.warning(f"premarket_gaps fetch failed [{type(e).__name__}]: {e}")
      return <existing fallback>
```
- Preserve the original return values exactly — purely additive log lines
- Compile check (use venv/bin/python3 per BHBI lesson if any annotations touched)
- Commit: `fix(observability): HM-BD.F — replace bare-except with typed catches + WARNING logs in ai_brain.py`
- ntfy

---

## Phase BDF.C — Static verify

```bash
echo "── Anchor present ──"
grep -n "HM-BD.F" engine/ai_brain.py | head -5

echo ""
echo "── No remaining bare-except in touched scope ──"
grep -n "except:\|except Exception:" engine/ai_brain.py | head -10

echo ""
echo "── Compile against venv/bin/python3 (launchd interpreter) ──"
venv/bin/python3 -c "import py_compile; py_compile.compile('engine/ai_brain.py', doraise=True); print('clean')" 2>&1 || python3 -c "import py_compile; py_compile.compile('engine/ai_brain.py', doraise=True); print('clean (fallback interp)')"

echo ""
echo "── Commits staged ──"
git log origin/main..HEAD --oneline
```

ntfy: `✅ HM-BD.F verify clean`.

---

## Phase BDF.D — Closure + push + restart + verify (INLINE)

```bash
git push origin main
launchctl kickstart -k gui/$(id -u)/com.trademinds.trader
sleep 8

echo "── Post-restart trader PID + port ──"
pgrep -af main.py | head -1
lsof -ti :8080 | head -1

echo ""
echo "── Endpoint smoke ──"
curl -s -o /dev/null -w "  /api/premarket-gaps   HTTP %{http_code}  %{time_total}s\n" --max-time 5 http://localhost:8080/api/premarket-gaps
curl -s -o /dev/null -w "  /api/momentum/premarket  HTTP %{http_code}  %{time_total}s\n" --max-time 5 http://localhost:8080/api/momentum/premarket

echo ""
echo "── Log tail for any new WARNING lines from HM-BD.F (premature is fine — proves the helper is in scope) ──"
tail -50 ~/autonomous-trader/logs/main.log 2>/dev/null | grep -iE "premarket|WARNING" | head -10
```

Closure report appended. ntfy: `🏁 HM-BD.F complete — silent swallows replaced with loud logs`.
