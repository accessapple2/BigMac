# Retired: Post-Earnings Drift (PED) Strategy

**Archived:** 2026-05-04
**Final commit before archive:** 7357a96
**Investigation:** `docs/HM-T_PED_OPERATIONAL_PROBE_2026-05-04.md`

## Why retired

PED was silently inert in production. HM-T investigation confirmed:

- **Wiring was correct.** Module imported at `main.py:3486`, scheduled every 15 min at `main.py:3541`. The `if __name__ == "__main__":` block reached the registration on every trader startup.
- **Trigger never fired.** PED reads `data/watchlist.txt` (at `main.py:3496`) to define its universe; the file never existed in production. Fallback was 9 hardcoded mega-caps (SPY, QQQ, NVDA, AAPL, MSFT, GOOGL, META, AMZN, TSLA) that virtually never matched the 1-48hr post-earnings window. None had earnings within 16 days of the audit.
- **Zero lifetime activity.** Zero rows in `signals`, zero rows in `trades`, zero log lines across all of `logs/*.log`. Sitrep history (794 lines, 2026-05-01 onward) reported `PED signals: 0` every cycle.
- **Gate-promotion path structurally unreachable.** The documented "30 trades + positive expectancy" criterion required trades that were never possible under the fallback universe.

## Architectural debt closed by this retirement

- **HM-S-code:** PED contained the only production reference to the phantom `agent_state` table (a dead `is_halted()` method wrapped in `try/except → False`). Archiving the module removes the phantom reference from active code paths. CLAUDE.md documentation was already corrected in HM-S-docs (commit `9ab18ec`).

## What was archived

- `post_earnings_drift.py` — the strategy module (`PostEarningsDriftAgent` class + `_AGENT_NAME = "post_earnings_drift"` + helper functions for VWAP/window math).

## What was removed from active code

- `main.py` block at lines 3476-3541 (comment block, `_ped_state` dict, `def run_post_earnings_drift`, schedule registration). Replaced with a single `# HM-T-retire:` marker for greppability.

## What was NOT touched

- `engine/earnings_catalyst.py::get_post_earnings_drift()` — different function (snake-case helper, used by dashboard panel). Unrelated to the retired strategy.
- `dashboard/app.py:12120-12123` — imports the dashboard helper above, not the retired strategy.
- `scripts/situation_report.py:79` — greps logs for PED activity. Harmless after retirement; will continue to find 0 just like today.

## Restore procedure

If post-earnings-drift is wanted as a strategy in the future:

1. **Don't restore this code.** It was abandoned scaffolding; the gate-promotion path was aspirational and never validated. The module also referenced the phantom `agent_state` table, which would need cleanup before any restoration.
2. **Write fresh.** Decide on a real universe (mid-cap+, with earnings calendar integration), validate against historical post-earnings drift data, OOS-test against ≥50 trades before promoting to paper, then promote to real per the same gate-flip discipline used for `bull_call_spread_v1` / `bear_put_spread_v1`.
3. **Consult `docs/HM-T_PED_OPERATIONAL_PROBE_2026-05-04.md`** for the full forensic record of why PED-as-it-was didn't work.

## Soak impact

The running trader process (PID 13734) loaded the old in-memory scheduler at startup. PED will continue to silently no-op in that process until the next service restart. Since PED has produced nothing for its entire lifetime, the in-memory residue is harmless. Retirement is fully effective on next restart.
