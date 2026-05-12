# 🔧 SCOTTY — HM-BE + HM-BF: Config Drift & Watchdog Tune
### Cluster Cleanup · Opus 4.7 · Discover → Diff → Apply → Verify

> **Captain's orders, Mr. Scott:** Two small loose ends from the original HM-AZ sweep that were deferred for manual handling. Now we bundle them. **HM-BE** is a name/model_id config mismatch on the qwen3 agent — could be DB row or code constant. **HM-BF** is a watchdog metric using raw swap% where it should use memory free%. Both small; both worth shipping before the architectural HM-BD.E and HM-AM epics.

---

## Identity & Mission

You are **Scotty** — Claude Code on Opus 4.7. Two sub-epics in one directive; each ships its own commit.

Mission:
- **BEBF.0** — Discovery for both. NO writes. Captain decides on BE remediation path.
- **BEBF.1** — **HM-BF** watchdog fix (Python code change — easier; ship first).
- **BEBF.2** — **HM-BE** qwen3 config fix (could be DB UPDATE or Python constant; depends on discovery).
- **BEBF.C** — Static verify both.
- **BEBF.D** — Closure report.

---

## Pre-flight

```bash
cd ~/autonomous-trader

echo "── Prerequisites ──"
git log origin/main --oneline | grep -iE "HM-BD" | head -3

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

1. **Sacred DBs**: if HM-BE turns out to need a `trader.db` UPDATE, Scotty drafts the SQL and HALTs — Captain executes it manually.
2. Diff-then-apply for all code edits.
3. One commit per sub-epic (BF, BE).
4. NTFY on commit: `curl -d "✅ HM-BX.Y: <one-line>" https://ntfy.sh/ollietrades-admin`.
5. Push gate: do NOT push. Stage commits locally.
6. NO service restart. Captain handles it.
7. HALT after BEBF.0 for Captain decisions. HALT after each commit.

---

## Phase BEBF.0 — Discovery (NO writes)

### HM-BE side: qwen3 config drift

Symptom: dashboard shows `name=qwen3-14b-pro` but `model_id=qwen3:8b` for the same agent.

```bash
cd ~/autonomous-trader

echo "── 1. Where do agent name + model_id live? ──"
grep -rn "qwen3-14b-pro\|qwen3:14b\|qwen3:8b" engine/ scripts/ main.py dashboard/app.py 2>/dev/null | head -20

echo ""
echo "── 2. trader.db tables that might hold agent config ──"
sqlite3 ~/autonomous-trader/data/trader.db ".tables" | tr ' ' '\n' | grep -iE "agent|player|model"

echo ""
echo "── 3. For each matching table, dump rows mentioning qwen3 ──"
for T in $(sqlite3 ~/autonomous-trader/data/trader.db ".tables" | tr ' ' '\n' | grep -iE "agent|player|model"); do
  echo "── $T ──"
  sqlite3 ~/autonomous-trader/data/trader.db "SELECT * FROM $T;" 2>/dev/null | grep -i qwen | head -5
done

echo ""
echo "── 4. JSON/YAML config files ──"
grep -rn "qwen3" config/ 2>/dev/null | head -10
ls -la config/*.json config/*.yaml 2>/dev/null | head -5
```

Document for HM-BE:
- **Location**: DB table + row id, OR Python file + line, OR JSON config
- **Current state**: name=X, model_id=Y
- **Captain question Q1**: which is the intent?
  - **α**: rename to match model (UI cosmetic only — preserves what's currently running)
  - **β**: change model_id to qwen3:14b (real fix — runs the heavier model)
  - **γ**: something else

### HM-BF side: watchdog memory metric

Symptom: watchdog uses raw swap% where memory free% would be more accurate signal.

```bash
echo "── 5. Locate watchdog memory check ──"
grep -rn "memory_pressure\|swap\|free.percent\|vm_stat\|memory_percent" engine/watchdog.py engine/crusher.py 2>/dev/null | head -20

echo ""
echo "── 6. Watchdog file size + structure ──"
wc -l engine/watchdog.py engine/crusher.py 2>/dev/null
```

Document for HM-BF:
- **File + line** of the memory metric
- **Current code** (excerpt of the relevant ~5 lines)
- **Intended fix**: switch from raw swap% to `psutil.virtual_memory().percent` or `.available / .total`
- **Captain question Q2**: any thresholds need re-tuning after metric switch?
  - **α**: keep current threshold value
  - **β**: re-tune based on what free% means
  - **γ**: surface for Captain to decide post-restart from observation

### Discovery report

Write `data/scotty_hm_bebf_report.md` with:
- HM-BE location, current state, recommended path with rationale
- HM-BF file+line, current excerpt, proposed fix, threshold call

**HALT.** ntfy: `📋 HM-BEBF discovery complete`.

---

## Phase BEBF.1 — HM-BF watchdog fix (Python)

After Captain approves Q2:
- Show diff with `# === HM-BF ===` anchor
- Apply
- Compile check: `python3 -c "import py_compile; py_compile.compile('engine/watchdog.py', doraise=True); print('clean')"`

Commit: `fix(watchdog): HM-BF — memory pressure use free% not raw swap%`. ntfy.

---

## Phase BEBF.2 — HM-BE config fix

After Captain approves Q1, path depends on discovery:

**If Python file**: diff with `# === HM-BE ===` anchor → apply → compile → commit `fix(agents): HM-BE — qwen3 config alignment` → ntfy.

**If trader.db**: do NOT execute UPDATE. Draft `BEGIN; <update>; COMMIT;` block in closure report for Captain to copy-paste. Commit `docs(agents): HM-BE — qwen3 config SQL handoff (Captain runs)`. ntfy.

**If JSON/YAML**: diff → apply → commit `fix(config): HM-BE — qwen3 agent definition aligned`. ntfy.

---

## Phase BEBF.C — Static verify

```bash
cd ~/autonomous-trader

echo "── HM-BE + HM-BF anchors ──"
grep -rn "HM-BE\|HM-BF" engine/ config/ main.py 2>/dev/null | grep -v "HM-BB\|HM-BC\|HM-BD" | head -10

echo ""
echo "── Files compile ──"
python3 -c "
import py_compile, os
for f in ['engine/watchdog.py']:
    if os.path.exists(f):
        py_compile.compile(f, doraise=True)
        print(f'  {f}: clean')
"

echo ""
echo "── Commits staged ──"
git log origin/main..HEAD --oneline
```

ntfy: `✅ HM-BEBF verify clean`.

---

## Phase BEBF.D — Closure report

Append HM-BE outcome (shipped via code path or SQL handoff with runnable block), HM-BF outcome (commit hash, threshold kept/re-tuned), restart needed, follow-ups. Push and restart handled by Scotty inline once Captain green-lights.

ntfy: `🏁 HM-BEBF complete`.
