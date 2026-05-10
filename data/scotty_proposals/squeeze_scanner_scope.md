# Investigation 5 — Short-Squeeze Scanner Scope (RETROSPECTIVE)

**Filed:** 2026-05-10 by Scotty (loose-ends sweep)
**Status:** Retrospective — the directive presumed a new build; the build has shipped.
**Linked:** HM-AO-α/β/β-2 + HM-AS-β.2 commits 2026-05-08 → 2026-05-10.

## What the directive asked for

> Produce a scope doc for a short-squeeze scanner module: data sources, location (Signal Center :9000 vs new agent), module skeleton, thresholds, effort estimate.

## Why this is now retrospective

The squeeze stack **shipped** before this session. Confirmed by code + git log:

| Component | Path | Commit | Status |
|---|---|---|---|
| Schema | `data/trader.db::squeeze_watch` | `f43acac feat(schema): add squeeze_watch table` | LIVE |
| Writer | `engine/squeeze_scanner.py` | `6dd1475 feat(squeeze-watcher): writer hook` | LIVE |
| Scheduler | `main.py::run_squeeze_watcher` | `57f5043 feat(squeeze-watcher): scheduler wiring` | 30-min cadence |
| NTFY surfacer + tests | `engine/alert_channels.py` + tests | `8f25182 feat(squeeze-watcher): ntfy surfacer tests` | Throttled (5/rollup) |
| Dashboard panel | `dashboard/static/index.html` | `143a94a feat(dashboard): squeeze candidates panel` | Read-only grid + dismiss |
| Scheduler-fire perf | `_bg()` wrapper on `run_squeeze_watcher` | `a18487e feat(scheduler): HM-AS-β.2 Option A pilot` | Pilot live |
| Runbook | `docs/squeeze_watcher_activation.md` | `bc1be5f docs(squeeze-watcher)` | Doctrine filed |

## Where it lives (vs. directive's question)

- **Lives in:** the main `com.trademinds.trader` service via `engine/squeeze_scanner.py`, scheduled in `main.py` on 30-min cadence (default-OFF via env flag, current state TBD by Admiral).
- **NOT** in Signal Center :9000 (that's the producer port, separate concern).
- **NOT** a separate agent — folded into the existing scanner module pattern.

## Filters & thresholds (per `engine/squeeze_scanner.py`)

- Float Short > **20%**
- Days-to-Cover (Short Ratio) tracked but not gated
- Composite score 1–10 (raw 1–100 scaled); persists rows with `score >= 5`
- Tier classification per row in `squeeze_watch.threshold_tier`
- NTFY rules: 5 individual alerts max per cycle, then rollup; quiet-hours gated; dismiss-aware

## Data sources used (Polygon + free)

The shipped scanner uses local data + Polygon-friendly path (per code inspection). No new paid integrations.

## Residual work (not blocking)

1. **Activation** — scheduler-fire is **pilot** (`a18487e`). Default-off via env flag. Admiral decides when to flip to default-on. Recent doc commit `a70c10b` ("squeeze activation — armed + hand-fire proof; scheduler-fire pending HM-AS-β") confirms manual fire works; auto-fire is the next milestone.
2. **`b0260e6 docs(hm-as-β)` diagnostic** — 129 `schedule.every()` jobs on single-thread schedule lib is the root scheduler bottleneck. `_bg()` wrapper on squeeze watcher is one targeted relief; broader scheduler refactor is its own ticket.
3. **Frontend polish** — squeeze candidates panel is read-only grid + dismiss; no charting yet (deferred per `b4084ad` Phase 7 status doc).

## Effort estimate (residual only, not original build)

- Auto-fire activation: **S (1–2h)** — flip env flag, observe 1–2 cycles, validate NTFY throttle.
- Frontend chart enhancement: **M (4–8h)** — optional.
- Scheduler refactor for the 129-job bottleneck: **L (multi-session, separate epic)** — not on this loose-ends sweep.

## Dependency on HM-AM

**None.** The squeeze scanner reads market data; it doesn't need unified portfolio. Independent of HM-AM.

## Recommendation

Close Investigation 5 as **retrospective + activation-pending**. No new design work.
