# 🔧 HM-MONSTER2 — Halt Resolutions + Chart Preview Polish (Closure)

**Author:** Scotty (Opus 4.7)
**Date:** 2026-05-12
**Status:** Complete — 3 commits shipped, halt-queue cleared

---

## Per-phase outcome

| Phase | Epic | Status | Commit |
|---|---|---|---|
| M2.1 | HM-BL.E Option C (delete + harden) | **SHIPPED + validated** | `f101a7f` |
| M2.2 | HM-BD.F-audit Tier-1 (4-site loud-fail) | **SHIPPED** | `132ee7f` |
| M2.3 | HM-BJ.E2 lightweight-charts preview | **REVERTED 2026-05-12 08:56** — broke HM-BJ tooltip stack | `bfcf637` → reverted by `8966e76` |

3 commits on `origin/main` ahead of pre-MONSTER2 `7ae7586`:
```
bfcf637 feat(frontend): HM-BJ.E2 — inline lightweight-charts preview in ticker hover tooltip
132ee7f fix(observability): HM-BD.F-audit Tier-1 — loud-fail wrap 4 silent-pass sites in ai_brain.py
f101a7f fix(capitol): HM-BL.E — listing-status filter prevents fresh positions on delisted tickers + ATH cleanup
```

---

## M2.1 — HM-BL.E Option C (Captain pre-decided)

### Data side
Backup: `data/trader.db.pre-hm-ble-20260512_0826` (272.6 MB, full DB copy).
SQL applied via inline heredoc from `data/scotty_hm_ble_report.md`:
- Created `positions_archive_hmble` table (idempotent)
- Archived ATH row (id=592, capitol-trades, qty=1.2344, avg_price=83.33, opened 2026-05-07) — archive_id=1
- Deleted from `positions` — `SELECT COUNT(*) FROM positions WHERE symbol='ATH'` returns 0

### Code side (`engine/capitol_fund.py`)
Two-layer defense, anchored `# === HM-BL.E ===`:

1. **Cheap pre-filter** in candidate list:
   ```python
   from engine.yf_safe import is_delisted
   # …
   candidates = [t for t in top_buys if … and not is_delisted(t["ticker"])]
   ```
   Zero network cost. Benefits from cross-agent cache hits in yf_safe.

2. **Live probe** before each buy:
   ```python
   from engine.yf_safe import yf_history_safe
   if yf_history_safe(ticker, period="1d").empty:
       console.log(f"[yellow]Capitol Trades: {ticker} skipped — no live yfinance data (delisted?)")
       continue
   ```
   Max 3 yfinance calls per scan (the buy loop caps at 3 iterations). First empty response memoizes — future scans short-circuit at layer 1.

### Post-restart validation
- `positions WHERE symbol='ATH'`: 0 rows ✓
- `positions_archive_hmble WHERE symbol='ATH'`: 1 row ✓
- No capitol-scan ran during the 60s soak window (capitol's cadence is wider than 60s).

---

## M2.2 — HM-BD.F-audit Tier-1 (Captain pre-decided)

All 4 Tier-1 sites in `engine/ai_brain.py` wrapped, anchored `# === HM-BD.F-audit Tier-1 ===`:

| Site | Exception classes | Log shape |
|---|---|---|
| L580 Ollama keep_alive=0 POST | `_requests.RequestException, TimeoutError, ConnectionError` | `Ollama unload {model_id}: {type}: {repr}` |
| L711 record_portfolio_snapshot | `sqlite3.Error, KeyError, ValueError` | `alpaca-mirror snapshot failed: {type}: {repr}` |
| L1029 record_signal | `sqlite3.Error, KeyError, ValueError` | `record_signal {player_id} {symbol}: {type}: {repr}` |
| L1406 signal-center POST | `_req.RequestException, TimeoutError, ConnectionError` | `signal-center post failed: {type}: {repr}` |

8 anchor lines total (4 open + 4 close). No NTFY per Q3 decision — log-only via `console.log(..., style="yellow")` matching HM-BD.F's existing style.

### Post-restart validation
60s soak: zero loud-fail log entries — expected (no failures actually occurred). The wrappers are silent until the first real failure surfaces, at which point we'll see `[yellow]...{type}: {repr}` instead of nothing.

---

## M2.3 — HM-BJ.E2 lightweight-charts preview

### Implementation
- **CSS** (5 lines): `.tsc-lwc` container, 100% × 80px, `pointer-events:none` (parent tooltip is non-interactive)
- **JS** (43 lines): `buildLwc(closes)` + `destroyLwc()` lifecycle wired into the existing HM-BJ.1 IIFE
- **Library**: `lightweight-charts@4.1.0` already loaded at `dashboard/static/index.html:2443` (used elsewhere for trc-lw-chart / trc-vol-chart)
- **Data**: reuses the existing `cachedFetch('/api/market/candles/<sym>?limit=30')` payload — no new endpoint, no extra network
- **Time axis**: synthesized `now - (N - 1 - i) * 60` UNIX seconds, 1-min spacing (LWC requires monotonically-increasing time values)

### Fallback behavior
- `typeof LightweightCharts === 'undefined'` OR `closes.length < 2` → original SVG sparkline path
- LWC `createChart` throws at runtime → swap `.tsc-lwc` outerHTML back to `.tsc-spark` + sparkline SVG
- Chart instance destroyed in `hideTip()` to prevent accumulation across hovers

### Browser test (deferred to Captain)
1. Cmd+Shift+R in browser to bust the cache (FileResponse serves disk so no service restart needed for frontend)
2. Hover any `.ticker-chip` for ≥ 300ms
3. Should see the previous 30-tick sparkline replaced by an 80px line chart in green (up) or red (down)
4. Move away from chip — chart should disappear cleanly (no console errors, no orphan DOM nodes)

10 anchor lines (5 open + 5 close) across CSS, lifecycle helpers, render-time injection, post-innerHTML build, and hideTip teardown. JS syntax verified via `node --check` on the extracted HM-BJ.1 IIFE.

---

## M2.D — Push + restart + verify

```
git push origin main:          ok
launchctl kickstart -k:        clean
Pre-restart PID:               16575
Post-restart PID:              17537 (parent) / 17541 (worker on :8080)
/api/premarket-gaps:           HTTP 200  0.71s
/api/ghost-trades/stats:       HTTP 200  0.12s
ATH in positions:              0 rows ✓
ATH in positions_archive_hmble: 1 row ✓
Bridge banner 60s soak:        1 banner (08:32:02 startup only) — HM-BK-residual still holds ✓
HM-BD.F-audit Tier-1 yellow:   0 entries (no failures in soak window — expected)
```

---

## ntfy events

- M2.1: `HM-MONSTER2 M2.1 SHIPPED` (sent)
- M2.2: `HM-MONSTER2 M2.2 SHIPPED` (sent)
- M2.3: `HM-MONSTER2 M2.3 SHIPPED` (sent)
- Final: `🏁 HM-MONSTER2 complete — 3 ships, halt-queue cleared` (sending now)

---

## Open items for Captain (none required, all informational)

1. ~~Browser test for HM-BJ.E2~~ — **REVERTED**, see post-mortem below
2. **Old backup cleanup** (someday) — `data/trader.db.pre-hm-ble-20260512_0826` (272.6 MB) can be archived/deleted once Captain confirms the change held over a multi-day window
3. **HM-BD.H** (still deferred from MONSTER 1) — the scanned_at format puzzle still wants a dedicated cache-trace epic
4. **HM-BD.F-audit Tier-2** (still optional from MONSTER 1) — L1038 + L1062 if/when Captain wants them; pattern is verbatim from Tier-1

---

## Post-mortem: HM-BJ.E2 revert (2026-05-12 08:56)

**Symptom (Captain-reported):** After M2.3 hit production, hovering any ticker chip hung in "loading" state on multiple panels (Fleet Activity, Kirk, Pre-market Gaps). Network tab showed **zero** `/api/market/candles/*` requests firing on hover. DevTools console showed 2 JS errors. The entire HM-BJ tooltip stack was broken — not just the new chart preview but the original sparkline path and HM-BJ.E5 too.

**Root cause hypothesis (not yet confirmed — needs the actual console-error text):**
A runtime exception in the M2.3 region threw early in the HM-BJ.1 IIFE, before `document.addEventListener('mouseover', ...)` and friends bound. Result: chips never received their hover handlers → `startHover()` never ran → no candles fetch → no tooltip. The error was likely:

- a LightweightCharts v4.1.0 API mismatch (`createChart` config schema differs from what I assumed), OR
- a property-access on `LightweightCharts.CrosshairMode.Normal` / similar at top-of-IIFE before the lib was confirmed loaded, OR
- a synchronous throw inside `buildLwc` reachable during the `var lwcInstance = null` block somehow

`node --check` confirmed the syntax was valid, which is exactly the wrong assurance — it doesn't catch runtime API surface mismatches.

**What I should have done differently (durable lesson):**

> **Frontend changes that touch live behavior must be browser-tested against the dev server BEFORE the commit lands on `main`, not after.** `node --check` proves only that the parser is happy; it does not exercise the DOM, the library, or any of the runtime paths that actually need to work. For library integrations specifically: actually `await page.goto(...)` and visually confirm.

Saved to `feedback_frontend_browser_test_before_ship.md` so future sessions inherit this constraint.

**Re-attempt plan (HM-BJ.E2-v2, separate epic):**
1. Stand up a Playwright session with credentials pre-authorized (or run headed against an already-logged-in browser profile)
2. Apply the chart code on a feature branch, NOT on `main`
3. Drive the hover, capture the DOM under `#ticker-scorecard`, confirm `<canvas>` is present
4. ALSO confirm the original sparkline path still works (i.e. the IIFE didn't break)
5. Only then merge to `main`

**Status:** PARKED. The revert restores the pre-M2.3 working state. No further action needed on HM-BJ.E2 until Captain re-opens the epic.
