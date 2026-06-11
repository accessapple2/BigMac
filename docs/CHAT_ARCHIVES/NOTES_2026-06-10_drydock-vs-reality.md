# Notes 2026-06-10 — April Drydock checklist vs. current reality

Two corrections surfaced while verifying HM-FORGE Phase 3 against the April
Saturday Drydock work. Recorded so future readers of the April docs aren't misled.

## 1. `docs/S7_THESIS_2026-04-26.md` — claimed shipped, never committed
- Referenced by `MONDAY_CHECKLIST_2026-04-27.md`, but the file was **never tracked
  by git** (no history on any branch, not present on disk, not under another name).
- The April Drydock XO claimed it shipped — it did not. No commit ever added or
  deleted it; it simply never landed.
- **Action if S7 thesis matters for current planning:** re-draft from scratch, OR
  pull it from the April chat transcript **before that chat is deleted**. Do not
  assume it exists in the repo.

## 2. `_EXECUTION_ENABLED` — April checklist says False; it's been True since May 5
- `MONDAY_CHECKLIST_2026-04-27.md` notes the gate was held **False all day on
  April 25**. That was accurate *then*.
- It was deliberately flipped **True on 2026-05-05** via commit **`df7320c`**
  ("gate-flip: _EXECUTION_ENABLED False -> True at 3 sites (atomic)", `HM-AB-unhalted`),
  enabling **Alpaca paper** live execution. Hardcoded `True` in
  `strategies/executor.py:26` (+ `bull_call_spread_v1.py`, `bull_spread_v1.py`).
- **RULE #1 (Schwab read-only) is unchanged** — execution routes to Alpaca paper
  only, never Schwab.
- **Why this note exists:** anyone reading the April checklist's "gate False" line
  in the future may think the live `True` is a regression. It is not — it is the
  intended state since May 5. Verify live code, not the April doc.
