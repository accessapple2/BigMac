# Fleet lifecycle drift (ollama-qwen3, qwen3-4b-audition) + two verifications

**Diagnose and report only — no fixes.** Nothing written to any table,
no code changed, no restart.

---

## 1. Lifecycle drift — RECONCILED, and the sentinel's "clear" verdict is correct, not a detector bug

**Short answer: possibility 1 (reconciled), verified three independent
ways — the DB state, the ledger's own record, and the detector's actual
code.**

### Do `ai_players` and `fleet_lifecycle_ledger` agree now?

Yes.

| | `ollama-qwen3` | `qwen3-4b-audition` |
|---|---|---|
| `ai_players.halt_mode` | `full` | `full` |
| `ai_players.halted_at` | `2026-08-31T11:26:12` | `2026-08-31T11:26:16` |
| Ledger's latest row (`MAX(created_at)`) | `halt`, `2026-08-31 18:26:12` | `halt`, `2026-08-31 18:26:16` |
| Ledger's `created_by` | `fleet_lifecycle.py` | `fleet_lifecycle.py` |
| Ledger's `backfilled` flag | `0` | `0` |

Both agree on the substance (halted, `full`, same reason family, same
date). The two timestamp columns are ~7h apart (11:26 vs. 18:26) — that's
the same UTC-vs-fixed-local-MST offset already documented elsewhere
today, not a data problem; `ai_players.halted_at` is written by a
SQLite-trigger `CURRENT_TIMESTAMP` (genuine UTC), the ledger row's
`created_at` default is also `datetime('now')` (UTC) — both should read
close together if written by the same action, and the gap here is
consistent with the ledger row being written by a *second*, later step,
not the same instant as the `ai_players` UPDATE (see below).

### What actually reconciled it — read directly from the order docs already on disk

`docs/orders/ORDER_2026-08-31_halt_agent_ollama-qwen3.md` and the
`qwen3-4b-audition` sibling **self-disclose the bypass**, not hide it:

> State was applied by direct SQLite UPDATE at 09:35 and this order
> backfills the ledger row that bypass skipped.

Sequence, reconstructed from the disclosed text plus the DB's own
timestamps:
1. **09:35 (disclosed, undated column to cross-check against
   independently)** — someone applied the halt via a raw `UPDATE
   ai_players SET halt_mode='full', ...` directly, bypassing
   `scripts/fleet_lifecycle.py` entirely. This is the original "someone
   likely bypassed fleet_lifecycle.py" finding from Monday's earlier
   check — confirmed true, and confirmed *why* (HM-SEAT-CONSOLIDATION,
   an urgent same-day VRAM-thrash mitigation, documented reason, not a
   mystery edit).
2. **Later that day** — `scripts/fleet_lifecycle.py halt <agent>
   --reason "..." --review-by 2026-09-30` was run for real, producing the
   order doc and the ledger rows (`created_by='fleet_lifecycle.py'`,
   confirmed via the ledger's own column — not a hand-typed row).

### Was it recorded properly, or just edited into place?

**Recorded properly, through the actual tool — with one honest process
wrinkle.** `created_by='fleet_lifecycle.py'` on both ledger rows rules out
a raw manual `INSERT` (a hand-edit wouldn't populate that column via the
tool's own code path). But there's a real dedicated companion script for
this exact situation — `scripts/fleet_lifecycle_backfill.py` — which sets
the ledger's own `backfilled=1` flag; `cmd_status` in
`fleet_lifecycle.py` even prints `"(backfilled)"` next to entries marked
that way. **These two rows show `backfilled=0`** — meaning whoever
reconciled this ran the tool's *normal* `halt` action after the fact
(idempotent on `ai_players`, which was already halted) rather than the
purpose-built backfill script. The order doc's own prose calls it a
"backfill" but the ledger's own flag doesn't reflect that. Not a
correctness problem — the record is honest, complete, and tool-written —
but a minor process inconsistency: the dedicated tool for exactly this
situation exists and wasn't used.

### Is the sentinel's "all clear" a real verdict, or a detector that stopped looking?

**Real verdict — verified from `scripts/hm_ops_sentinel.py`'s actual
code, not inferred from the output alone:**

- `_ledger_latest_by_target("agent")` picks each target's row via
  `MAX(created_at)` per `target_name` — correctly selects the 08-31
  `halt` row over the older 08-29 `active`/backfilled row for both
  agents (verified: the SQL `INNER JOIN ... ON created_at = MAX(...)`
  has no ordering bug that would let a stale row win).
- `check_fleet_lifecycle_drift()` maps `"halt" → expected_halt_mode
  "full"`, reads live `ai_players.halt_mode`, and only flags drift `if
  expected and live_mode != expected`. Live is `full` for both, expected
  is `full` for both — condition is false, correctly not flagged.
- The `overdue` check separately confirmed clean: `review_by=2026-09-30`
  is still in the future relative to today (2026-09-01) — no overdue
  finding is the correct result, not a miss, per the order doc's own
  stated rule ("a sentinel finding against this target before its
  review-by date is a false alarm").

No detector bug. The drift genuinely doesn't exist anymore, and the
sentinel is correctly reporting that.

### Does this connect to the BENCH 30-day fail-open residual risk? — plainly, partially

**`ollama-qwen3`: yes, directly.** It's D-rated (`42.4/100`, frozen at
the same `2026-07-13` snapshot as the rest of that cohort — checked live)
and is one of the six halt-blocked D/E agents named in this morning's
residual-risk note (`super-agent`, `ollama-llama`, `deepseek-7b-grok4`,
`guardian-of-forever`, `navigator`, `ollama-qwen3`). If it's revived
later without a fresh rating computed first, the 30-day staleness
fail-open (shipped today, `0730aec`) means the BENCH gate will not catch
it — it lands back trading unrestricted on its 49-day-and-growing-stale D
verdict, or rather, on no verdict at all once it's past 30 days.

**`qwen3-4b-audition`: connects differently — checked, and it's not the
same mechanism.** It has **zero rows in `agent_ratings`, ever** — it's an
audition seat, suspended before it ever accumulated enough clean trades
to be rated at all. `_bench_block_reason()`'s existing "never-rated
players fail open" behavior — which predates today's fix entirely, not
something the 30-day change introduced — already meant this agent would
land unrestricted on revival, with or without today's staleness fix. Real
gap, but not the *same* gap, and not new today.

**Whether the ledger being bypassable makes either of these worse: checked
`scripts/fleet_lifecycle.py`'s `revive` path directly — it never touches
`agent_ratings` at all** (grepped the whole file for
`agent_ratings`/`calculate_rating`/`rating` — zero hits outside a
docstring comment). This means the BENCH-staleness gap and the
ledger-bypass risk are **independent, not compounding**: even a fully
doctrine-compliant `scripts/fleet_lifecycle.py revive ollama-qwen3
--reason "..."` — properly recorded, no bypass, ledger and order doc both
correct — would land exactly as unrestricted, because the tool was never
wired to force a rating recompute on revive in the first place. Fixing
the ledger-bypass problem (if it recurs) would not fix the BENCH gap, and
the BENCH gap existing doesn't make a ledger bypass any more or less
dangerous than it already is on its own bookkeeping terms. Two separate
gaps that both happen to matter at the same "revive" moment, not one
causing the other.

---

## 2. Two verifications

**Premarket Reveille:** fired today, on schedule, clean. `trader.log`:
`[2026-09-01 05:55:42] Reveille: 🌅 Reveille 2026-09-01 · regime ...` —
headline correctly dated for today, not a stale card. Zero `"Reveille
error"` lines in today's log.

**`regime_history` / `recall_refresh`:** `regime_history` has exactly 1
row for `date='2026-09-01'` (`regime=BEAR_CROSS`, `spy_close=761.63`) —
matches its own `UNIQUE` constraint and the observed 1-row-per-trading-day
cadence in the 5 most recent dates. `recall_refresh.log`'s
`2026-09-01T15:00:00-0700` run: `exit=0`, `"new": 0, "embedded": 0,
"corpus_total": 255` — exactly the baseline figure, confirmed also via a
direct `SELECT COUNT(*) FROM recall_corpus` = 255. Both clean, both at
expected values, nothing to chase.

---

## Not re-chased, per instruction (already closed)

- plutus confidence=0.85 flag — not stuck; today's `decision_audit` shows
  1,074 decisions with 15 distinct confidence values (0.25–0.82).
- 05:10–06:46 flapping — quiet ~24h since the `5fba6e7` mutex fix.
- `mlx_qwen3` — healthy today, heartbeat 0.0–3.6 min.

None of these were touched or re-verified this pass, per instruction.
