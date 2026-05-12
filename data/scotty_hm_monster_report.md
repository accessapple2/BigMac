# 🔧 HM-MONSTER — Multi-Epic Clear-the-Deck Closure

**Author:** Scotty (Opus 4.7)
**Date:** 2026-05-12
**Status:** Complete — 4 commits shipped, 1 deferred, 2 audits parked for Captain

---

## Per-phase outcome

| Phase | Epic | Status | Commit |
|---|---|---|---|
| M.1 | HM-BD.H scanned_at format fix | **DEFERRED** — deeper than 1-file | (none) |
| M.2 | HM-BK-residual second bridge banner | **SHIPPED** | `8eabdd2` |
| M.3 | HM-BJ.E1 right-click context menu | **SHIPPED** | `d0f1e63` |
| M.4 | HM-BJ.E3 arrow-key chip navigation | **SHIPPED** | `2790c70` |
| M.5 | HM-BL.E stale 0-qty positions discovery | **HALT-FOR-CAPTAIN** — SQL handoff ready | `99dcf25` (report) |
| M.6 | HM-BD.F-audit silent-pass inventory | **HALT-FOR-CAPTAIN** — scope proposal ready | `ed6c4d2` (report) |

5 commits on `origin/main` ahead of pre-MONSTER `774d822`:
```
ed6c4d2 docs(scotty): HM-BD.F-audit — silent-pass inventory + HM-BD.F2 scope proposal
99dcf25 docs(scotty): HM-BL.E — stale 0-qty positions discovery + SQL handoff
2790c70 feat(frontend): HM-BJ.E3 — arrow-key navigation between ticker chips
d0f1e63 feat(frontend): HM-BJ.E1 — right-click context menu on ticker chips
8eabdd2 fix(kirk): HM-BK-residual — second bridge banner deduped at 6 remaining sites
```

---

## M.1 — HM-BD.H DEFERRED rationale

`scan_premarket_gaps()` correctly produces ISO format (`2026-05-12T08:10:37.759533`) on direct invocation (verified: 232 rows, ISO format). The endpoint returns space format (`2026-05-12 01:21:14`) from a timestamp that **predates the process restart** at 07:46:56.

The `_endpoint_cache` is a plain in-memory `dict` (dashboard/app.py:377) that should clear on restart. Yet the cached response somehow has pre-restart data. Mechanism unclear — likely a second SQLite cache layer or a startup task warming the cache from a different code path I didn't trace.

Per directive: *"If discovery reveals it's deeper than 1-file: HALT and report — defer to its own epic."* DEFERRED. Should be its own future epic with full cache-mechanism trace.

Post-MONSTER restart at 08:20:55 still shows space format (`2026-05-12 01:21:14`, 257 rows) — consistent with deferral.

---

## M.2 — HM-BK-residual SHIPPED + validated

6 stragglers swapped to singleton (`main_crew.py:253`, `crew/agents.py:599`, `crew/agents.py:764`, `crew/learning.py:270`, `shared/alpaca_sync.py:32`, `shared/alpaca_portfolio_sync.py:147`). Anchor `# === HM-BK-residual ===`.

**Validation post-restart at 08:20:55:**
- Pre-restart banner count: 7+ over the last 90 minutes (07:10, 07:12, 07:15, 07:23 doublet, 07:47 doublet)
- Post-restart banner count over 60s soak: **1 banner** (08:20:55 only)
- Source: `engine/alpaca_bridge.py:185` module-level singleton — the expected single init

Banner count dropped from many-per-hour to one-per-process-lifetime. ✓

---

## M.3 — HM-BJ.E1 SHIPPED

Right-click context menu on `.ticker-chip` with 5 quick-jump items: TradingView, Yahoo Finance, Webull, Schwab, X/Twitter cashtag. Dismiss on outside-click, Esc, or scroll. Event delegation matches the rest of HM-BJ. Anchors `// === HM-BJ.E1 ===` (CSS + JS).

JS syntax verified via `node --check` (2885 chars block). Frontend changes don't need a restart — `FileResponse` serves disk on each request.

UI test deferred to Captain via browser (the page already serves the new menu; verify by right-clicking any `.ticker-chip`).

---

## M.4 — HM-BJ.E3 SHIPPED

Arrow-key navigation: Left/Up = prev chip, Right/Down = next chip, wraps at boundaries. Preserves existing Tab + Enter/Space from HM-BJ.1. Anchor `// === HM-BJ.E3 ===`.

JS syntax verified via `node --check` (1097 chars block). UI test deferred to Captain.

---

## M.5 — HM-BL.E HALT-FOR-CAPTAIN

**Original premise reframed.** The BKBL closure stated stale 0-qty rows existed; the audit found **zero 0-qty rows** (`SELECT COUNT(*) FROM positions WHERE qty = 0 OR qty IS NULL` → 0).

Real issue: `capitol-trades` agent opened a NEW position on delisted ATH on 2026-05-07 (qty=1.2344, avg_price=$83.33). Root cause: `engine/capitol_fund.py` candidate filter has no `is_delisted()` check.

Full report at `data/scotty_hm_ble_report.md`. Includes:
- 4 options (A delete-only, B harden capitol_fund, **C both — recommended**, D defer)
- SQL handoff block with archive+delete pattern (idempotent)
- 8 informational oddities surfaced during audit (energy-arnold shorts, dalio-metals non-metals positions, etc.)
- 3 Captain questions

**Captain action needed:** answer Q1 (approach), Q2 (archive convention), Q3 (related oddities).

---

## M.6 — HM-BD.F-audit HALT-FOR-CAPTAIN

23 silent-pass `except Exception:` blocks in `engine/ai_brain.py`. Auto-classified + manually verified.

Full report at `data/scotty_hm_bdf_audit_report.md`. Recommendation:

- **Tier 1 (4 sites)** — recommended HM-BD.F2 scope:
  - L580 Ollama unload POST (true network)
  - L1406 signal-center POST (true network)
  - L711 record_portfolio_snapshot (observability DB write)
  - L1029 record_signal (observability DB write)
- Tier 2 (2 sites): L1038, L1062 — same shape, optional cherry-pick
- Tier 3 (17 sites): keep silent per CLAUDE.md error-handling posture (internal-state fallbacks)

Estimated diff for Tier 1: ~16 lines in one file.

**Captain action needed:** answer Q1 (scope: A=Tier1, B=Tier1+2, C=cherry-pick), Q2 (exception classes per site OR uniform HM-BD.F pattern), Q3 (NTFY policy — recommended log-only).

---

## M.D — Post-restart verification

```
Pre-restart PID:  13811 (BLB.D)  →  Post-MONSTER PID: 16575
Port 8080:        bound at 16575
Endpoint smoke:
  /api/premarket-gaps        HTTP 200  0.72s  (count 257)
  /api/ghost-trades/stats    HTTP 200  0.12s
scanned_at format:   space-separator (HM-BD.H deferred, expected)
Bridge banner 60s soak: 1 (HM-BK-residual validated)
```

---

## ntfy events

- M.1: `HM-MONSTER M.1 DEFERRED` (sent)
- M.2: `HM-MONSTER M.2 SHIPPED` (sent)
- M.3: `HM-MONSTER M.3 SHIPPED` (sent)
- M.4: `HM-MONSTER M.4 SHIPPED` (sent)
- M.5: `HM-MONSTER M.5 HALT-FOR-CAPTAIN` (sent)
- M.6: `HM-MONSTER M.6 HALT-FOR-CAPTAIN` (sent)
- Final: `HM-MONSTER complete — 5 commits, 3 ships, 1 deferred, 2 audits parked` (sending now)

---

## Open items for Captain

1. **HM-BD.H** (M.1 deferred): trace the cache-mechanism mystery — `_endpoint_cache` shows data older than process start
2. **HM-BL.E** (M.5): pick approach from A/B/C/D in `data/scotty_hm_ble_report.md`
3. **HM-BD.F2** (M.6): pick Tier-1 vs Tier-1+2 vs cherry-pick from `data/scotty_hm_bdf_audit_report.md`
