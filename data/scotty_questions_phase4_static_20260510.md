# Scotty — Phase 4-static blockers / open questions

Generated 2026-05-10 20:14 MST during Phase 0 discovery.

## Q1 — Scanner endpoint missing (HARD BLOCKER for the Scanner tile)

**Finding:** `/api/momentum/scanner` returns **HTTP 404**. Only four `/api/momentum/*` routes are registered in `dashboard/app.py`:

```
@app.get("/api/momentum/heartbeat")           # line 17908
@app.get("/api/momentum/recent_signals")      # line 17923
@app.get("/api/momentum/race")                # line 17945
@app.get("/api/momentum/detail/{ticker}")     # line 17967
```

Backend comment at `dashboard/app.py:17904` reads `"Race + Scanner tiles will build on this"` — Scanner was planned but never landed. Phase 3 in the Dashboard Remodel v1 series was supposed to ship it; git log shows no such commit.

**Phase 4-static is locked to "no backend changes."** Two options:

- **Option A — defer Scanner.** Ship Race nav entry only. Scanner becomes a separate Phase 3 ticket.
- **Option B — extend scope.** Allow Phase 4-static to add `engine/momentum/scanner.py` + register the endpoint. Violates the lock but unblocks the tile.

**Recommend Option A.** Closure report will note the gap and the future ticket.

## Q2 — Race nav placement on desktop sidebar

**Finding:** Mobile nav entry for Race **already exists** at line 12796 (`<button class="mobile-nav-item" onclick="mobileNav('race',this);if(typeof raceStart==='function')raceStart()">`). Desktop sidebar has no equivalent entry.

**Recommend insertion at line ~2173** (between "Sniff Scan" and "Backtest" in the main visible sidebar block), using the existing pattern:

```html
<div class="sidebar-item" onclick="showSection('race');if(typeof raceStart==='function')raceStart()"><span class="icon">🏁</span> Race</div>
```

Confirm placement OR specify an alternative slot.

## Q3 — Phase 5 closure stuck at 5.0b

**Finding:** Phase 5 plan (Live restyle — Kirk Advisory / Captain's Portfolio / Bridge Votes / War Room) only reached **5.0b**, which produced `data/phase5_panel_map.md`. Sub-phases 5.1 (shared CSS + JS helpers) through 5.7 (closure) never executed.

The Phase 4-static plan references "Phase 5 helpers" (`momentum-restyle`, `deltaFlash`, `updateHeartbeat`, `.heartbeat-dot`, `.panel-header`) as prerequisites that should already be in the file. **They are not.** This isn't a blocker for the Race nav entry (which doesn't need them), but it leaves the Dashboard Remodel v1 timeline state ambiguous.

Decision needed:
- **Option A — resume Phase 5 after Phase 4-static.** Finish 5.1–5.7 as originally planned.
- **Option B — abandon Phase 5 restyle.** Mark 5.0b as the final state (the map is still valuable as documentation), and consider the panels visually frozen.
- **Option C — merge Phase 5 work into Phase 4-static as a follow-on.** Add the helpers when something else needs them.

No recommendation — depends on whether the panel restyle is still wanted.

## Q4 — `_momentumDetailEnrich` vs. the plan's new detail modal

**Finding:** Phase 4 enrichment is already shipped as `_momentumDetailEnrich()` (line ~34568) which appends a `<div id="momentum-detail-extra">` block into the existing `#posDetailModal` body. The plan's proposed new modal (`#detail-backdrop` + `#detail-panel` aside) would duplicate and conflict with this.

**Recommend no action.** The existing `_momentumDetailEnrich` is wired throughout the dashboard (18+ existing call sites use `onclick="openTickerDetail('X')"`). Building a parallel modal would create a UX inconsistency where Race rows open a different modal than every other ticker click in the app.

If the Admiral specifically wants the slide-in `aside` modal styling, that's a UX redesign, not a retrofit — separate ticket.

## Q5 — Service restart behavior

**Finding:** `dashboard/app.py:9466` returns `FileResponse(_static_dir + "/index.html")` — no in-process caching, file is re-read per request. Empirically confirmed: editing the file and curling `/` shows the new bytes immediately, without restart.

**No service restart required for any HTML/CSS/JS edit in `dashboard/static/index.html`.** Hard browser refresh (Cmd+Shift+R) is enough.

The plan's 4-static.6 verify step lists "Surface restart question to Admiral" — answer for the closure: **no restart needed.**
