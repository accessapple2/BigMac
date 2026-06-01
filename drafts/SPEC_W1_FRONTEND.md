# SPEC — W1 Frontend: Health Grid + As-Of Stamps + NTFY Auto-Quarantine (DESIGN ONLY)
Status: design draft 2026-06-01. No build (frontend → needs Admiral browser smoke per HM-BJ.E4).
Backend (W1 source_gate + /api/sources/health) already SHIPPED; this is the UI + alerting layer.

## 1. Health grid (replaces "13/13 loaded")
Render `/api/sources/health` as a grid: RED-first sort, per-source {display_name, state badge,
age_human, criticality, as_of}. Header badge becomes e.g. "11 GREEN · 0 AMBER · 6 RED · 2 RETIRED"
linking to the grid. RETIRED rendered distinctly (grey, not red-alarm). Replaces the meaningless
"13/13 loaded" count. (Note: build the GEX-style card pattern already used for the GEX tile.)

## 2. Per-tile as-of stamps (the trust layer)
Every data tile renders its source's `as_of` from /api/sources/health:
- GREEN → quiet timestamp ("as of 09:42").
- AMBER → amber + "(aging)".
- RED → red "STALE — {age}" badge + tile greyed/de-emphasized so it can't read as live.
Tiles to stamp first: portfolio, Schwab snapshot, metals, consensus, Riker, movers, CTO briefing,
daily snapshot, GEX. (A position shown without snapshot age is a latent trust bug — Schwab is a
3-day snapshot.)

## 3. NTFY auto-quarantine + alerting
- Fire NTFY (ollietrades-admin) when: a live_decision source flips RED; any source auto-quarantines;
  health RED-count > 0 at market open. Throttle: one alert per source per state-change.
- Auto-quarantine: a live_decision source RED for > 3 consecutive cadence periods → auto enabled=0
  + one NTFY. Re-enable is manual. (source_gate.set_enabled already exists; needs the state-history
  + the >3-period tracker + the NTFY hook.)

## Constraints
Frontend ships ONLY after Admiral browser hover/click smoke (HM-BJ.E4). The single POST
(/api/sources/quarantine) requires admin auth. No order path. The grid is the W6 /api/health source
of truth (don't build two health systems).
