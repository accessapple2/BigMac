# SCOTTY 3.4 — Phase 7 Status (HM-AO-β-2 Squeeze Panel)

> Single-file ship of the dashboard panel for HM-AO-β. Edits scoped to
> `dashboard/static/index.html` per the doctrine resolution (commit
> `ae425fb`, 2026-05-08). No service restart required — static HTML is
> read fresh per request via `StaticFiles(directory=_static_dir)`.

**Date:** 2026-05-08
**Branch:** `main`
**Commits added this sprint:** 2 (panel + this status doc)

---

## 1. Tasks shipped

| # | Task | Status | Commit | Evidence |
|---|---|---|---|---|
| 1 | Pattern discovery | **SHIPPED** | (analysis only) | Ghost Scorecard chosen as the mirror; section-fetch-map + lazy-init infra adopted |
| 2 | Single-file panel build | **SHIPPED** | `143a94a` | 215 lines added to `dashboard/static/index.html`; 4 surgical edits (sidebar, section, fetchMap, JS) |
| 3 | Status doc + push | next | — | — |

---

## 2. Pattern discovery summary

**Mirror chosen: Ghost Scorecard** (sidebar entry around line 2179, section
at line 9591, JS block ending at line 21792). It already exhibits the
exact pattern HM-AO-β-2 needs:

- Sidebar item with `onclick="showSection('X');fetchX()"` invocation
- Section `<div id="section-X" style="display:none;">` containing a
  `card`-class header with controls (day-range select, refresh button)
  + a content area
- JS block defining `fetchX()` + render functions + entry in
  `_sectionFetchMap` so `_switchSection` / `_tabHidden` infra can
  auto-pause polling

**Key infra discovered (and adopted):**

- `_sectionFetchMap` at line 14042 — symbol → fetch-fn name lookup
- `registerSectionInit(sectionName, initFn, intervalFns)` at line 14121 —
  registers a fetch fn + interval list. Runtime auto-starts intervals
  when section becomes active and clears them when leaving (`_startSectionIntervals`,
  line 14137). **No manual `setInterval` / `visibilitychange` plumbing
  needed** — the panel passes `[{fn, ms: 60000}]` to `registerSectionInit`
  and the runtime handles 60s cadence + visibility pause + section
  pause for free.
- `_sectionInitQueue` (defined earlier in the bootstrap) — graceful
  fallback if our script block runs before `registerSectionInit` is
  defined.

**CSS conventions in use** (no new vars added):

- `var(--red)` (#ea580c — also re-aliased to var(--red) below)
- `var(--accent3)` — amber/warning
- `var(--accent)` — cyan/info
- `var(--green)`, `var(--text)`, `var(--muted)`, `var(--border)`, `var(--surface)`

For tier coloring the panel uses these CSS vars only — no inline hex.

---

## 3. Files changed

**Single source file:** `dashboard/static/index.html` (+215 lines, 4 hunks)

1. **Sidebar item** (line 2182, in TIER 1 always-visible group between
   Ghost Scorecard and Leaderboard):
   ```html
   <div class="sidebar-item" onclick="showSection('squeeze');fetchSqueezeRecent()">
     <span class="icon">🎯</span> Squeeze
   </div>
   ```

2. **`_sectionFetchMap` entry** (line 14142, alphabetically near
   `'ghost-scorecard'`):
   ```js
   'squeeze': 'fetchSqueezeRecent',
   ```

3. **Section markup** (line 9645, between ghost-scorecard end and
   time-machine start) — header card with day-range + tier-filter
   selects, summary line, and an auto-fitting card grid
   (`grid-template-columns: repeat(auto-fill, minmax(320px, 1fr))`).

4. **JS script block** (after ghost-trades script ends at 21792, before
   time-machine script at 21794) — `fetchSqueezeRecent`,
   `renderSqueezeSummary`, `renderSqueezeCards`, `_squeezeDismissPrompt /
   Cancel / Confirm`, plus the `registerSectionInit('squeeze', ...)`
   call.

**Plus this status doc.**

**Out of scope, not modified:** `dashboard/app.py` (routes were already
shipped under `857b318`). The other files showing in `git diff --stat`
(`data/bull_bear_cache.json`, `docs/OPS_LOG.md`,
`docs/model_watch/MODEL_WATCH_2026-05-08.md`) are runtime artifacts the
trader writes autonomously — they're not part of this sprint's commit.

---

## 4. Smoke test results

### Static-file delivery
The trader serves `/static/index.html` directly from disk, so the panel
is live the moment the file is saved — no restart required.

```
$ curl -s http://127.0.0.1:8080/static/index.html | grep -cE "showSection\('squeeze|section-squeeze|fetchSqueezeRecent"
10   # 4 expected hooks × multiple references each
```

### Backend routes (live)
```
$ curl -s "http://127.0.0.1:8080/api/squeeze/summary?days=7"
{"ok":true,"days":7,"PRIORITY":0,"ALERT":0,"WATCH":0,"total":0}

$ curl -s "http://127.0.0.1:8080/api/squeeze/recent?days=7&tier=all"
{"ok":true,"days":7,"tier":"all","count":0,"items":[]}
```

Both `ok:true`. `squeeze_watch` table currently empty — first scheduled
scan after this morning's 06:56 trader restart fires at ~07:26 (Phase 6
will verify).

### Syntax checks
- **HTML parser** — `python3 html.parser` parses the full file without
  errors
- **JS** — `node --check` exit 0 on the new script block
- **Brace/paren/bracket balance** — `{}` 38/38 · `()` 173/173 · `[]` 11/11

### Admiral browser checklist (smoke I can't run from session)

1. Open `http://localhost:8080/` in a browser → log in → Bridge loads
2. Click 🎯 **Squeeze** in the sidebar (between Ghost Scorecard and
   Leaderboard) → section reveals
3. Summary line populates (currently `TOTAL 0 · PRIORITY 0 · ALERT 0 ·
   WATCH 0`) and "No active squeeze candidates" empty state appears
4. After the watcher's first scan, refresh — cards should appear sorted
   PRIORITY → ALERT → WATCH then by composite-score desc
5. Click "Dismiss" on any card → reason input appears inline → type a
   reason → click "Confirm" → POST fires, card fades out, summary
   counts decrement
6. Switch to a different sidebar section, wait > 60 s, switch back —
   confirm Console shows `[LazyInit] Started 1 intervals for "squeeze"`
   on entry and the auto-refresh isn't firing in the background
7. Hide the browser tab for > 60 s, return — `_tabHidden` should pause
   the cadence and resume on visibility-restore

---

## 5. Wall-clock + commit count

| | |
|---|---|
| Commits added | **2** (panel + this status doc) |
| Source files mutated | **1** (`dashboard/static/index.html`) |
| Lines added | 215 (panel) + ~150 (this status doc) ≈ 365 |
| Tests added | 0 — UI code; visual smoke is the right gate |
| Service restarts | **0** — static HTML is hot |
| Schema migrations | **0** |
| `paper_trader.py` / `main.py` / gate / strategy edits | **0** |
| `dashboard/app.py` edits | **0** (routes pre-shipped under `857b318`) |
| `dashboard/frontend/` edits | **0** (legacy per doctrine A) |
| Force-pushes | **0** |
| Secrets generated | **0** |

---

## 6. Outstanding for Admiral go

### Immediate visual smoke
- Run the 7-step browser checklist above (≤ 5 min)
- Tweak tier coloring if PRIORITY / ALERT / WATCH borders / fills don't
  read clearly in the live theme — vars are `--red` / `--accent3` /
  `--accent` and can be remapped in CSS without touching the panel JS

### Activation dependency
- Panel is live but empty until the squeeze watcher generates rows.
  Watcher activation: `SQUEEZE_WATCHER_ENABLED=True` in `.env` (set
  earlier this sprint at 06:56 + trader restart). First scheduled fire
  is ~30 min post-restart; **Phase 6 wakeup at 07:29 will verify**.

### Auth Phase 1 wiring (TIER B)
- `POST /api/squeeze/dismiss` is currently auth-stubbed. Panel calls it
  without an auth header (works because the route's `Depends(verify_admin_token)`
  is commented behind `# TODO Phase 1: enable after Admiral secret-gen`).
  When Admiral runs `docs/AUTH_SETUP.md` to generate the 3 secrets:
  - Uncomment the `Depends` line in `dashboard/app.py` near
    `dismiss_squeeze_candidate`
  - Add a bearer-token header to the panel's `_squeezeDismissConfirm`
    fetch — there's a `// TODO: add admin token header after Phase 1
    secret-gen` marker right before that fetch call

### Phase 8 follow-ups (not in scope here)
- Per-symbol click-through to a small detail panel (history of scan_ts
  + composite_score over time for that ticker) — useful once the
  watcher accumulates a few weeks of evidence
- Forward-return tracker on dismissed-vs-not candidates — feeds the
  "promote to voter" decision after 30+ days

---

## 7. Findings worth flagging

### Ghost Scorecard reference panel — clean
Reference pattern is in good shape. No broken-pattern flags. The only
quirk: Ghost Scorecard mounts its day-range filter `<select>` with id
`ghostDays` directly in the section header rather than in a separate
controls bar, and the panel followed that same shape (`squeezeDays` /
`squeezeTier`).

### `dashboard/frontend/` confirmed dead
While locating patterns, no live JS reference to `dashboard/frontend/`
or its `dist/` was found anywhere. Doctrine verdict A holds. Panel
ships in static HTML alone.

### Section-fetch infra is well-designed
The `_sectionFetchMap` + `registerSectionInit` + `_startSectionIntervals`
chain is the cleanest piece of frontend code in this 34k-line file.
Any future sprint that needs to add a panel should adopt this pattern
directly — it solves the visibility-pause + section-pause + lazy-init
problems for free.

---

## 8. Push readiness

2 commits ahead of `origin/main` (this status doc + the panel). Single
source-file edit per the brief. No untracked production code, no
modified gate / strategy / `paper_trader.py` / `main.py` / `dashboard/app.py`
files, no force-push, no rebase. Push authorized in the Captain's
Phase 7 brief — proceeding with end-of-Task-3 push.
