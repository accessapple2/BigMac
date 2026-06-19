# THE BRIDGE — REDESIGN SPEC  ·  LOCKED 2026-06-19
Direction A · Refined LCARS · Fighter (actionable, paper-only)
Build target: `dashboard/static/bridge-v2.html` (shuttle — runs parallel to live `index.html`)

---

## DOCTRINE — non-negotiable

- **Shuttle-first.** All work lands in `dashboard/static/bridge-v2.html`. NEVER touch `index.html`
  until an explicit "promote" order. On promote: archive `index.html` → `index.html.<YYYYMMDD>.bak`
  first (never `rm`). Append-only doctrine applies (memory rule #16).
- **Paper-only.** Actions hit `/api/paper/*` and `/api/scan/*` ONLY. No Schwab, no live-broker path.
  RULE #1 sacred — Schwab read-only display, no order route exists or gets built.
- **Confirm before fire.** Any order-submitting / position-closing button must `window.confirm()`
  with an explicit summary before the fetch. Nothing trades on a single click.
- **Semantic color — one meaning per hue.** green=bullish/nominal · red=bearish/alert ·
  amber=caution/Captain accent · blue=info. Colorblind-safe per existing --green=blue/--red=orange.
- **Type stack.** Antonio (display) · Archivo (UI/reading) · IBM Plex Mono (ALL numbers, tabular).
- **One emoji max** per live signal. Kill decorative emoji headers.
- **No "Loading…".** Empty states = skeletons + last-known values so the bridge never reads as broken.
- **Single source of truth per metric.** A value (e.g. gamma flip) is read once; no two panels may disagree.
- **Frontend ship rule (#25).** Promote requires a manual browser hover/click smoke test, not just static checks.
- Static file: served via FileResponse at GET `/bridge-v2.html` from `dashboard/static/`. No restart needed for edits.

---

## INFORMATION ARCHITECTURE — 5 tabs

### ◢ BRIDGE — command, at-a-glance
Fleet equity + day P&L hero · regime line (regime/VIX/SPY/F&G) · Kirk Advisory · Number One (Riker) rec ·
Captain Archer intel · Counselor Troi read · Bridge Vote · Positions At Risk · Ship Systems status strip ·
Tomorrow's Game Plan · Reveille pre-market brief · Earnings alert.

### ◢ BATTLE — execution
0DTE Battle Station (tactical + rules engine + capital ceilings + fleet cash) · Options Engine
(Production Book, Wheel CSPs, IC Squadron) · Trade of the Day (Phaser-Lock) · Trade Desk · Live Trading ·
Worf's inverse-ETF arsenal.

### ◢ RESEARCH — signals & shadow
Live Scanner (T1/T2/T3) · Live Event Tape · Batch Scan Alerts · Volume Radar (full market) · Smart Money ·
Live Chart · Ghost Research · shadow bakeoffs (CSP / qwen3.5 / plutus) · Backtest · Ask Q · Captain's Guide ·
Generated Indexes · Crew Dissent.

### ◢ FLEET — the crew
Fleet Leaderboard (all agents) · Fleet Report Card (grades) · Season standings · Consensus per-ticker votes ·
War Room · Crew Activity · Fleet Activity (trades) · Comms (trade history) · Models · Costs.

### ◢ MACRO — market structure & economy
GEX gamma map + overlay · Sector Heatmap + sectors · Fear & Greed · VIX regime map · FRED macro ·
Metals exposure · Midterm Recovery Protocol · Real Portfolio · Portfolio Value chart · Sector Allocation ·
Congress trades · Earnings Calendar · Risk Vitals · Tax Alpha.

---

## BUILD ORDER
1. **BRIDGE** tab — wired to real paper data first (current pass).
2. BATTLE  3. RESEARCH  4. FLEET  5. MACRO.
Each tab = a grid of equal LCARS-fighter cards. Actions per the doctrine above.

## KNOWN DATA BUGS TO FIX DURING BUILD (observed on live page 2026-06-19)
- **Gamma contradiction.** Main-Bridge blurb says "ABOVE gamma flip — stable" while spot 748.46 < flip 752
  (GEX panel correctly reads VOLATILE / negative gamma). Wire gamma from ONE source.
- **Dalio / tracking players.** Best agent +2.5% shows Win Rate 0% / Trades —. `win_rate=0` should be `null`
  when trades=0 → render "—", not "0%". (Third cosmetic fix that was pending.)
