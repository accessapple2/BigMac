# Kirk Swing Desk — Retired 2026-05-04

## What this is

Two Python modules archived from the active codebase:

- `agents__kirk.py` — was `agents/kirk.py`. Defined `propose_swing(ticker, context)` and `get_kirk_brief()`. Designed to write to `kirk_signals` and `kirk_swing_trades` tables in `data/trader.db`.
- `agents__pike.py` — was `agents/pike.py`. Defined `second_opinion(ticker, kirk_proposal)`. Pike was Kirk's "veto" partner — only callable from `agents/kirk.py:173`.

## Why retired

Per `docs/AUDIT_6_INVESTIGATION_2026-05-04.md` Problem B:

- Modules were scaffolded but **never wired** to the scheduler. `propose_swing()` had zero callers in the production codebase. `kirk_signals`, `kirk_swing_trades`, `pike_votes` were all 0 rows.
- `CLAUDE.md` claimed Swing Desk was active in the fleet — drift between docs and code.
- The "active Kirk" (`engine/kirk_advisory.py` + `engine/kirk_grok_advisor.py`) is a different concept: a daily Webull-style advisor that writes to `kirk_advisory_log` (272 rows, daily writes since 2026-03-31). That Kirk is **preserved** and untouched by this retirement.
- Manual swing-trading workflow that motivated the Swing Desk concept no longer applies — OllieTrades fully shifted to autonomous Alpaca-paper-only trading.

Admiral chose **RETIRE** over BUILD on 2026-05-04 (build estimate was 6-8 hours; retirement was 30 minutes including this README).

## What stays in the live system

- `engine/kirk_advisory.py` — daily Webull-style advisor (active)
- `engine/kirk_grok_advisor.py` — Grok-based variant of the same (active, scheduled at 9:30 AM and 1:30 PM ET via `main.py:1718`)
- `kirk_advisory_log` table — 272 rows, written daily by the modules above
- `kirk_signals`, `kirk_swing_trades`, `pike_votes` tables — preserved as empty schemas per SACRED-DATA discipline; can be dropped in a future cleanup if/when the Admiral approves a schema-drop migration

## How to restore (if ever needed)

```bash
cd ~/autonomous-trader
git mv archive/retired/2026-05-04-kirk-swing-desk/agents__kirk.py agents/kirk.py
git mv archive/retired/2026-05-04-kirk-swing-desk/agents__pike.py agents/pike.py
# Then add scheduler wiring in main.py — the missing piece that prevented
# this from being live in the first place. See AUDIT_6_INVESTIGATION_2026-05-04.md
# Problem B "BUILD" recommendation for the original effort estimate.
```

## Provenance

- Audit: `docs/AUDIT_6_INVESTIGATION_2026-05-04.md` (commit `40361a6`, pushed 2026-05-04 07:30 MST)
- Retirement commit: this commit
- Module mtimes at archive time: 2026-04-23 18:43 (both kirk.py and pike.py)
- Module sizes at archive time: 9,143 bytes (kirk.py), 5,829 bytes (pike.py)
