# 🔧 SCOTTY — HM-BA: Dashboard & State Cleanup
### Loose Ends Sweep · Opus 4.7 · Discover → Patch → Verify

> **Captain's orders, Mr. Scott:** With HM-AZ + HM-BB + HM-BC landed, sweep the cosmetic and small functional debt that's been piling up since HM-AW. Five micro-phases, each its own commit, all diff-then-apply. NO DB writes — all code-only.

---

## Identity & Mission

You are **Scotty** — Claude Code on Opus 4.7. Five micro-phases, each ≤1 file change.

Mission:
- **BA.0** — Discovery: verify each item is still applicable (some may have self-resolved or been fixed in HM-AZ/BB/BC).
- **BA.1** — Race tile timestamp stale (was "Last update HH:MM:SS" frozen). Investigate frontend polling or backend cache. Fix wherever the bug lives.
- **BA.2** — `/api/momentum/scanner` returns 404. Wire the missing endpoint (Phase 3 from earlier momentum remodel left it dangling).
- **BA.3** — `/api/premarket-gaps` legacy endpoint broken. Decide: alias to `/api/momentum/premarket` OR remove the legacy route entirely.
- **BA.4** — Housekeeping: commit any working-tree stragglers (caches, ops logs, model watch notes). `main.py` only if the change is heartbeat-related and nothing else.
- **BA.5** — Remove HM-AW.4 heartbeat log line. Function is proven firing; the log was a debug aid, now noise.

DB-touch items deferred to their own tickets (NOT in scope here):
- HM-BD: dalio-metals GOOGL ghost position
- HM-BE: qwen3-14b-pro config drift (`name`=14b vs `model_id`=qwen3:8b)

---

## Pre-flight check

```bash
cd ~/autonomous-trader

echo "── Confirm prerequisites shipped ──"
git log origin/main --oneline | grep -iE "HM-AZ|HM-BB|HM-BC" | head -10

echo ""
echo "── Working tree state ──"
git status --short

echo ""
echo "── Service alive ──"
pgrep -af main.py | head -1
lsof -ti :8080 | head -1
```

If service isn't running or prerequisites missing: **HALT**.

---

## Standing Rules

1. Sacred DBs: **read-only this directive**. NO writes to `trader.db`, `arena.db`, or any `.db` file.
2. Sacred directories: no `rm -rf` on `~/ollietrades` or `~/autonomous-trader`.
3. Diff-then-apply: unified diff before every code edit.
4. One commit per sub-phase. Each commit independently revertable.
5. NTFY on commit: `curl -d "✅ HM-BA.X: <one-line>" https://ntfy.sh/ollietrades-admin`.
6. Push gate: do NOT push. Stage commits locally — Captain pushes after verify.
7. NO service restart. Captain handles it.
8. HALT after each phase and report — Captain confirms before next phase.

---

## Phase BA.0 — Discovery (NO writes)

For each of the five items, confirm whether it's still a real problem in current `main` (some may have self-resolved):

```bash
cd ~/autonomous-trader

echo "── 1. Race tile timestamp: hit the endpoint, check freshness ──"
curl -s http://localhost:8080/api/race/state 2>/dev/null | python3 -m json.tool 2>/dev/null | head -20 || echo "endpoint missing/error"

echo ""
echo "── 2. /api/momentum/scanner — does it 404? ──"
curl -s -o /dev/null -w "HTTP %{http_code}\n" http://localhost:8080/api/momentum/scanner

echo ""
echo "── 3. /api/premarket-gaps — does it 404 or error? ──"
curl -s -o /dev/null -w "HTTP %{http_code}\n" http://localhost:8080/api/premarket-gaps

echo ""
echo "── 4. Working-tree stragglers ──"
git status --short

echo ""
echo "── 5. HM-AW.4 heartbeat log line still present? ──"
grep -n "HM-AW.4\|heartbeat" main.py | head -10
```

Write `data/scotty_hm_ba_report.md` summarizing for each item:
- **Status:** STILL APPLICABLE / SELF-RESOLVED / NEEDS DECISION
- **Evidence:** curl output, file state, etc.
- **Plan:** what BA.X will do (or "skip — already clean")
- **Decision needed from Captain (Y/N):** for BA.3 in particular (alias vs remove)

**HALT.** ntfy: `📋 HM-BA discovery complete`.

---

## Phase BA.1 — Race tile timestamp

If still stale:
1. Find the Race tile rendering code (`dashboard/frontend/src/` or `dashboard/app.py` template).
2. Identify whether timestamp comes from backend (cache key) or frontend (polling interval).
3. Apply minimal fix:
   - Backend cache: invalidate or shorten TTL on `/api/race/state` (or whatever endpoint)
   - Frontend polling: ensure setInterval is firing and component re-rendering
4. If frontend, also confirm `dashboard/frontend/dist/` is rebuilt (`npm run build` if needed).

Show diff. Anchor with `# === HM-BA.1 ===`.

Commit: `fix(dashboard): HM-BA.1 — race tile timestamp refresh`.

ntfy.

---

## Phase BA.2 — /api/momentum/scanner

If 404:
1. Search for `momentum/scanner` route registrations — confirm none exists.
2. Look for the partial implementation from earlier remodel: was there a `def momentum_scanner(...)` left orphaned?
3. Wire the route in `dashboard/app.py` (or wherever momentum endpoints live).
4. Match the response shape of sibling endpoints (e.g. `/api/momentum/premarket`).

Show diff. Anchor with `# === HM-BA.2 ===`.

Smoke:
```bash
curl -s -o /dev/null -w "HTTP %{http_code}\n" http://localhost:8080/api/momentum/scanner
```

NOTE: smoke won't pass until Captain restarts. Don't restart; just confirm code compiles and route is registered:
```bash
python3 -c "from dashboard.app import app; print([r.path for r in app.routes if 'momentum' in r.path])"
```

Commit: `feat(dashboard): HM-BA.2 — wire /api/momentum/scanner endpoint`.

ntfy.

---

## Phase BA.3 — /api/premarket-gaps legacy

**Captain decision required** (collected in BA.0): alias or remove.

If alias:
- Add 301 redirect (or function alias) to `/api/momentum/premarket`.
- Commit: `fix(dashboard): HM-BA.3 — alias legacy /api/premarket-gaps → /api/momentum/premarket`.

If remove:
- Delete the legacy route. Confirm via grep that nothing in `dashboard/frontend/` still references it.
- If frontend references found, list them in the closure report — those are follow-ups.
- Commit: `refactor(dashboard): HM-BA.3 — remove dead /api/premarket-gaps endpoint`.

Show diff. Anchor with `# === HM-BA.3 ===`.

ntfy.

---

## Phase BA.4 — Housekeeping

For each file in `git status --short`, decide:
- **Cache files** (`data/bull_bear_cache.json`, etc.): commit as `chore: refresh cache state`
- **Docs** (`docs/OPS_LOG.md`, `docs/model_watch/*.md`): commit as `docs: update ops log + model watch`
- **`main.py`**: ONLY if the diff is HM-AW.4-heartbeat-only. If `main.py` has unrelated changes, HALT and report — those are separate concerns.

One commit (or two: chore + docs) covers BA.4.

ntfy.

---

## Phase BA.5 — Remove HM-AW.4 heartbeat log

If grep in BA.0 showed the heartbeat log line still present:
1. Show diff removing only that line (and any surrounding comment block specific to it).
2. Anchor removed line is fine to leave (it documents what was there).

Compile check:
```bash
python3 -c "import py_compile; py_compile.compile('main.py', doraise=True); print('clean')"
```

Commit: `refactor: HM-BA.5 — remove HM-AW.4 heartbeat debug log (no longer needed)`.

ntfy.

---

## Phase BA.C — Static verify

```bash
cd ~/autonomous-trader

echo "── HM-BA anchors present ──"
grep -rn "HM-BA" dashboard/ main.py 2>/dev/null | head -10

echo ""
echo "── main.py compiles ──"
python3 -c "import py_compile; py_compile.compile('main.py', doraise=True); print('clean')"

echo ""
echo "── dashboard module imports ──"
python3 -c "from dashboard.app import app; print('app loaded with', len(app.routes), 'routes')"

echo ""
echo "── Commits staged ──"
git log origin/main..HEAD --oneline

echo ""
echo "── Working tree clean ──"
git status --short
```

ntfy: `✅ HM-BA verify clean`.

---

## Phase BA.D — Closure report

Append to `data/scotty_hm_ba_report.md`:

```markdown
## HM-BA Closure

### Commits staged (not pushed)
<output of: git log origin/main..HEAD --oneline>

### Per-phase outcomes
- BA.1 (Race tile): <SHIPPED | SKIPPED — already clean | DEFERRED — why>
- BA.2 (scanner endpoint): <same>
- BA.3 (premarket-gaps): <SHIPPED via alias | SHIPPED via remove | DEFERRED>
- BA.4 (housekeeping): <SHIPPED N files | nothing to commit>
- BA.5 (heartbeat log): <SHIPPED | SKIPPED — already removed>

### Restart needed
Yes if BA.1, BA.2, BA.3, or BA.5 shipped — Captain runs `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader`.
No if only BA.4 housekeeping shipped.

### Out-of-scope follow-ups
<any frontend references to removed routes, etc.>
```

ntfy: `🏁 HM-BA complete — ready for Captain push & restart`.
