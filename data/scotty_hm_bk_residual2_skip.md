# HM-BK-residual2 — Documented as accepted behavior (no commit)

**Date:** 2026-05-12
**Status:** SKIP (per HM-CLOSE-GAP Q2 decision tree)
**Auditor:** Scotty (Opus 4.7)

## Finding

Banner duplication was **not actually duplicating per cycle** — HM-BK already fixed it. Today's `logs/trader.log` shows 2913 total banner emissions, but only **one per launchd restart**. The recent restart timestamps (today) show exactly one banner per kickstart:

```
[11:21:51] Alpaca Paper Trading bridge initialized
[11:34:08] Alpaca Paper Trading bridge initialized
[11:38:43] Alpaca Paper Trading bridge initialized
[11:48:38] Alpaca Paper Trading bridge initialized
[14:25:08] Alpaca Paper Trading bridge initialized
[16:00:56] Alpaca Paper Trading bridge initialized  ← HM-BR restart
[16:15:11] Alpaca Paper Trading bridge initialized  ← HM-DASH.4 restart
[16:43:35] Alpaca Paper Trading bridge initialized  ← HM-DASH restart 2
[17:01:08] Alpaca Paper Trading bridge initialized  ← HM-AN2.C restart 1
[17:57:07] Alpaca Paper Trading bridge initialized  ← HM-AN2.C restart 2
```

10 trader restarts today (heavy dev cycle), 10 banners. Normal/expected.

The 2913 historical figure is the count accumulated across many weeks of restarts — `logs/trader.log` is not daily-rotated, so the running tally includes every restart since the log was last truncated.

## Root cause was already fixed by HM-BK

`engine/total_portfolio.py:194-198` comment documents the prior bug:

```python
# === HM-BK ===
# Reuse the module-level singleton from engine.alpaca_bridge (constructed
# once at import time). The previous code imported the class and ran
# AlpacaBridge() per call, which re-emitted "Alpaca Paper Trading bridge
# initialized" every Kirk advisory cycle (~2 min). HM-BKBL.0 discovery.
from engine.alpaca_bridge import alpaca as bridge
# === /HM-BK ===
```

## Verification

`grep -rn "AlpacaBridge()" engine/ main.py dashboard/ scripts/` returns only:
- `engine/alpaca_bridge.py:185 alpaca = AlpacaBridge()` (module-level singleton)
- `engine/total_portfolio.py:196` (comment quoting the OLD bug)

No live re-instantiation. No `multiprocessing` / `fork` / `Process(` in `main.py` or `engine/ai_brain.py`.

## Decision

Per HM-CLOSE-GAP Q2 decision tree: cause is not multiprocessing-fork, and there is no clean fix because **there is no bug**. The single banner per process spawn is the correct, intended behavior post-HM-BK.

**No commit.** This note exists as audit-trail evidence the question was investigated and resolved.

## Recommended follow-up (out of scope here)

Daily log rotation would make banner-count metrics more useful. Currently `logs/trader.log` accumulates across all restarts indefinitely, so `grep -c "banner"` measures process-lifetime restarts, not per-day activity. A `newsyslog` or `logrotate`-style daily cut would let the daily-watch summary (HM-CLOSE-GAP W3) cleanly count today's restarts as a process-stability metric.
