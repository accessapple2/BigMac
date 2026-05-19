# Banked Items — 2026-05-18

No urgency. Audit-first when appetite returns.

## High-value (one ship fixes multiple panels)

### HM-SIGNAL-CENTER-PROXY-NULL-CACHE
- **Severity:** MEDIUM-HIGH
- **Source:** Diagnosed during F1 stop (commit 6443fe6) on
  HM-MORPHEUS-MATRIX-UI-FIXES-FOLLOWUP
- **Surface:** signal-center/server.py:830 _fetch_all_signals
- **Bug:** On per-endpoint exception (line 841 except: results[key]
  = None), the proxy poisons its own SWR cache with None, then
  serves that to the frontend until next successful fetch.
- **Affects 5 panels showing UNKNOWN/missing data:**
  - Regime (Header reads UNKNOWN)
  - ema_pullback
  - metals
  - red_alert_score
  - risk_radar
- **Fix options:**
  - (a) Treat per-endpoint exceptions as cache-misses not
    cache-poisoning Nones
  - (b) Preserve last-good value across blips (SWR semantic)
  - (c) Repoint header Regime reader to /api/quant-signals.regime
    as more reliable single source (narrowest fix, doesn't
    address other 4 panels)
- **Recommendation:** Audit first, ship Option (b) — proper SWR
  with last-known-good fallback. Closes 5 panels in one commit.

## Medium

### HM-QUANT-VIX-CATEGORIZER-LIVE-READ
- **Source:** Bug #3 from Matrix UI fix bundle (STOPPED — backend)
- **Surface:** /api/quant-signals → signals.momentum.why[]
- **Bug:** Emits literal string "VIX 0.0 low (risk-on)" — VIX
  categorizer defaulting to 0 when live feed not read. Quant tab
  Trend Momentum bullet shows VIX 0.0 while real VIX is 18.6.
- **Effort:** ~30 min Scotty (find categorizer, point at live VIX
  feed, restart)

## Low-priority (data sources idle/wire pending)

### HM-INSIDER-FEED-INIT-STUCK
- Insider Activity panel stuck on "Insider feed initializing…"
- Grade C, "?" score
- Likely upstream producer not emitting OR proxy null-cache
  victim (may close itself when HM-SIGNAL-CENTER-PROXY-NULL-CACHE
  ships)

### HM-METALS-GSR-DATA-WIRE
- Metals/GSR panel showing "--", "No metals data"
- metals_ledger probably not piped to the renderer
- Same potential proxy null-cache victim

### HM-BULL-BEAR-CONSENSUS-WIRE
- 0/28 models producing analyses
- History tab consistently shows "No data / No consensus / No
  analyses" for: volume_radar, smart_money, bull_bear, earnings,
  convergence
- Producers idle OR proxy null-cache victims

### HM-PREDICTIONS-API-DEDUPE
- /api/predictions/accuracy emits two real duplicate
  2026-04-05 / 0% rows in the response
- UI-side dedupe shipped in commit d9637b1 (covers the symptom)
- Backend should dedupe at source

## Active soaks (no action, baking until Saturday May 24)

13+ soaks converging for big roll-up review. See
reports/SESSION_HANDOFF_2026-05-17.md for full inventory.
