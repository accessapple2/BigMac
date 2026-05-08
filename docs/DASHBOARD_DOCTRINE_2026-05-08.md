# Dashboard Doctrine — 2026-05-08

**Author:** Scotty 3.3 Phase 6 (Claude Code Opus 4.7)
**Question:** which file is the canonical dashboard surface — the
hand-rolled `dashboard/static/index.html`, or the Vite/React build at
`dashboard/frontend/dist/index.html`? CLAUDE.md and Phase 4 audit
agreed on `static/`, but the React tree at `dashboard/frontend/src/`
created enough doubt that the HM-AO-β-2 frontend panel was deferred
pending this resolution.

---

## 1. Verdict — **A. `dashboard/static/index.html` is canonical**

The Vite/React frontend at `dashboard/frontend/` is **unwired
experimental code**. Its build artifact is not mounted, not served,
and not referenced by the live FastAPI app.

CLAUDE.md is correct. No update to the *content* of the Dashboard Rules
section is required, but a one-line empirical footnote is added so
future audits don't re-litigate this.

---

## 2. Evidence

### 2.A FastAPI mount inventory (`dashboard/app.py`)

| Line | Code | Effect |
|---:|---|---|
| 9405 | `_static_dir = os.path.join(os.path.dirname(__file__), "static")` | resolves to `dashboard/static/` |
| 9406 | `app.mount("/static", StaticFiles(directory=_static_dir), ...)` | mounts only `dashboard/static/` at `/static` |
| 9464 | `@app.get("/")` | root route handler |
| 9467 | `FileResponse(os.path.join(_static_dir, "index.html"))` | **serves `dashboard/static/index.html` for `/`** |

The Vite `dist/` directory is referenced **nowhere** in `dashboard/app.py`
or `main.py`:

```bash
$ grep -nE "frontend.dist|frontend/dist" dashboard/app.py main.py
$    # empty
```

### 2.B File modtimes + sizes

| Path | Size | Last modified | Last commit |
|---|---:|---|---|
| `dashboard/static/index.html` | **1,897,650 B** (1.9 MB) | 2026-04-30 08:27 | `d9ebe8c` 2026-05-02 |
| `dashboard/frontend/dist/index.html` | 597 B | 2026-04-25 14:38 | (stale) |

### 2.C Git activity (last 60 days)

```bash
$ git log --since="60 days ago" -- dashboard/static/index.html | wc -l   # 9 commits
$ git log --since="60 days ago" -- dashboard/frontend/        | wc -l   # 4 commits, all pre-Apr-13
```

`dashboard/static/index.html` is actively maintained. `dashboard/frontend/`
last saw real activity at commit `08cc0eb` 2026-04-12 — a month ago.

### 2.D Live runtime confirmation

```bash
$ curl -sL http://localhost:8080/ | head -3
<!DOCTYPE html>
<html lang="en">
<head>
$ curl -sL http://localhost:8080/ | grep -c "id=\"root\""    # Vite SPA marker
0
$ curl -sL http://localhost:8080/ | grep -c "<form method=\"POST\" action=\"/login\">"
1
```

The served root is hand-rolled login HTML, not a Vite-bundled SPA. There
is no `<div id="root">` mount, no hashed `<script type="module">`, no
ESM import map.

### 2.E Vite frontend posture

The Vite tree exists with React 19 + recharts:

```json
// dashboard/frontend/package.json
{ "name": "trademinds-dashboard", "version": "1.0.0", "type": "module",
  "scripts": { "dev": "vite", "build": "vite build", "preview": "vite preview" },
  "dependencies": { "react": "^19.0.0", "react-dom": "^19.0.0", "recharts": "^2.15.0" } }
```

It has source components (`ModelControl.jsx`, `BacktestLab.jsx`,
arena/`Leaderboard.jsx`, etc.) and an api client (`api/client.js`).
**None of them are reached by the live server** — the FastAPI process
never asks for any path under `dashboard/frontend/`.

The fact that `frontend/api/client.js:57-59` calls
`/api/model-control/...` endpoints (which DO exist at `dashboard/app.py`)
shows the React UI was built against the same backend, but the React UI
itself is not deployed.

---

## 3. Updated CLAUDE.md section

### Diff applied

```
 ## Dashboard Rules
-- Dashboard is served from `dashboard/static/index.html` on port 8080
+- Dashboard is served from `dashboard/static/index.html` on port 8080.
+  Verified empirically 2026-05-08 (`docs/DASHBOARD_DOCTRINE_2026-05-08.md`):
+  FastAPI `/` route at `dashboard/app.py:9464-9467` returns
+  `FileResponse(_static_dir + "/index.html")` and the only `StaticFiles`
+  mount is `dashboard/static/`. The Vite tree at `dashboard/frontend/`
+  is unwired experimental code — its `dist/` is never mounted.
 - ALL dashboard edits target that single file — do not create new HTML files unless explicitly asked
 - `main.py` is the entry point; it imports `from dashboard.app import app` and runs uvicorn on 8080
```

The original two rules stand — they are correct. The new line is
empirical evidence so the next audit doesn't re-investigate.

---

## 4. Future-Scotty rules (HM-AO-β-2 unblocker)

When the squeeze panel ships:

1. **Edit `dashboard/static/index.html`** — append a panel section into the
   existing dashboard structure. Find the Ghost Trader / risk panels for
   layout reference; mirror their pattern.
2. The static HTML talks to backend via `fetch('/api/squeeze/recent')`,
   `fetch('/api/squeeze/summary')`, `fetch('/api/squeeze/dismiss', {method:'POST',...})`.
   All three routes are already shipped in `dashboard/app.py` (commit `857b318`).
3. **Do NOT touch `dashboard/frontend/`.** The React components there
   (including the `ModelControl.jsx` toggle UI) are not reached by the
   live server. Any "fix" there would be invisible.
4. Auth header injection for `/api/squeeze/dismiss` lands as part of
   the broader Phase 1 auth wiring — not in HM-AO-β-2.

---

## 5. Cleanup recommendations (NOT done in this paste)

These are notes for future cleanup tickets — none are in scope for
Scotty 3.3:

- **Archive `dashboard/frontend/`** to `archive/2026-05-XX-vite-frontend/`
  once it's clear no live deploy depends on it. **Risk:** if any
  contractor or future Admiral plans to revive the React UI, the source
  + dependency lock are still useful. Recommend leaving in place but
  marked DEPRECATED.
- **Add `dashboard/frontend/DEPRECATED.md`** with a one-paragraph
  "this is not the dashboard you're looking for" pointer — separate
  cleanup ticket.
- **Or, alternatively, the inverse**: if Admiral wants to revive the
  React frontend (clean modular components, recharts visualizations,
  React 19), that's a distinct multi-week project that requires
  rewriting all the Jinja-style state injection currently in
  static/index.html. Out of scope.

---

## 6. Doctrine summary

| Question | Answer |
|---|---|
| Which file does `/` serve? | `dashboard/static/index.html` |
| Which file should I edit for dashboard changes? | `dashboard/static/index.html` |
| Where do CSS / JS / icons live? | `dashboard/static/` (mounted at `/static`) |
| Is the Vite/React frontend deployed? | No |
| Where does it live? | `dashboard/frontend/` (unwired) |
| Where does the React build output go? | `dashboard/frontend/dist/` (not mounted) |
| Does the static HTML use the React components? | No — entirely independent |
| Should HM-AO-β-2 panel land in static or frontend? | **static** |

**Verdict: A.** Update CLAUDE.md with empirical footnote.
