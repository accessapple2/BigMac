# HM-D — Pre-halt watchlist_signals Decision
*2026-05-04 evening, Scotty investigation, no fixes applied*

## Inventory

- **Total rows:** 1,230
- **Halted-player rows:** 165 (13.4% of table)
  - `ollama-llama` (exit_only since 2026-04-25): **62** rows (49 with `halted_emit=0`, 13 with `halted_emit=1` from HM-C backfill)
  - `dayblade-sulu` (exit_only since 2026-03-31): **41** rows (all `halted_emit=0`)
  - `gemini-2.5-pro` (exit_only since 2026-04-30): **35** rows (all `halted_emit=0`)
  - `grok-3` (exit_only since 2026-04-25): **27** rows (all `halted_emit=0`)

### Status breakdown of halted-player rows

| Status | Count | Operational meaning |
|---|---|---|
| `active` | **34** | still being scored as live tracking signals |
| `watching` | 62 | post-resolution observation period |
| `hit_target` | 44 | resolved win |
| `expired` | 25 | resolved loss/timeout |

### Active readers — 16 SELECT sites across 2 files

- `engine/signal_tracker.py` (15 sites)
- `engine/crew_scanner.py:3965` (1 site, fleet consensus pick)

### Reader filter awareness

| Site | Filter applied | Risk |
|---|---|---|
| `signal_tracker.py:47-48,57-58,208,280,331,367,372,375,411` (10 sites) | `HALTED_EMIT_FILTER` (= `halted_emit = 0`) | ✅ correctly excludes post-halt leaks |
| `signal_tracker.py:104,179,191,243,311` (5 sites) | none | ⚠️ includes pre-halt rows from retired players |
| `crew_scanner.py:3965` (fleet consensus pick) | `HALTED_EMIT_FILTER` | ⚠️ pre-halt rows from retired players still pass through (`halted_emit=0` for those rows) |

---

## Question

What should happen to the 165 pre-halt rows from retired players?

### Sub-question revealed by inventory

**HM-C's `halted_emit=0` filter does NOT exclude these rows.** At emission time those players WERE active, so `halted_emit` was correctly 0. HM-C only catches signals emitted **while halted** (the 13-row ollama-llama leak). The 152 pre-halt rows from retired players still flow through `HALTED_EMIT_FILTER`-aware readers.

So the operational concern is specifically: **34 active-status rows from retired players** are still being included in fleet leaderboards and the fleet consensus pick query.

---

## Three options analyzed

### Option α — Retain
- **Pros:** Historical record preserved (165 rows of valuable pre-halt signal data). Zero risk. The 34 active rows are bounded and self-resolving — `signal_tracker.py:124,133` will transition them to `hit_target`/`expired` over time as price action plays out, and retired players cannot emit new `active` rows (`halt_gate.can_emit_signal` blocks at `signal_tracker.py:32-37`).
- **Cons:** Until the 34 actives age out, fleet consensus query at `crew_scanner.py:3965` could count a retired player toward the 3-agent threshold. Active-leaderboard panels show retired players. Effect bounded; not bug-level.
- **Effort:** 0 hr.

### Option β — Add a halt-aware filter (JOIN ai_players halt_mode='active')
- **Pros:** Surgical. Adds one JOIN line to the 6 currently-unaware readers (5 in signal_tracker + 1 in crew_scanner). Doesn't touch data. Doesn't change schema. Doesn't lose history.
- **Cons:** 6 read sites to update + verify. Hand-coded JOIN at every site, or a new helper in `halt_gate.py` (`with_active_player_filter()`).
- **Effort:** ~30-45 min in a future session.

### Option γ — Archive (move halted-player rows to `watchlist_signals_archive`)
- **Pros:** Clean break. Active table contains only active-player rows.
- **Cons:** Schema change + migration. Any reader needing historical data has to UNION. Conflicts with HM-C philosophy (HM-C kept rows and added a flag instead of moving them).
- **Effort:** ~2 hr in a future session.

---

## Recommended option: **α (Retain) + minor follow-up**

The operational impact (34 active rows in fleet calculations) is bounded and self-resolving as price action ages them into resolved status. Historical record value of the 165 rows is real. HM-C's pattern (flag-not-archive) is the established convention; γ would break that pattern. β is a defensible follow-up if the 34-active-row issue is judged to need fixing now rather than letting it age out.

**If a follow-up is queued, prefer β over γ.** Specifically:
1. Add a helper in `engine/halt_gate.py`:
   ```python
   ACTIVE_PLAYER_JOIN = "JOIN ai_players ap ON ap.id = w.player_id AND ap.halt_mode = 'active'"
   ```
2. Apply at the 6 readers currently-unaware (5 signal_tracker + 1 crew_scanner).
3. Tag with `# HM-D-fix:` markers.

Total effort: ~30-45 min, single session. No DB change.

---

## Reader-by-reader impact under each option

| Site | α (Retain) | β (add JOIN) | γ (archive) |
|---|---|---|---|
| `signal_tracker.py:47-48,57-58` (de-dup checks) | no change | + JOIN | no change (rows in archive, dedup still works on active rows only) |
| `signal_tracker.py:104,179` (active leaderboards) | shows retired players | + JOIN; cleaner | clean |
| `signal_tracker.py:191` (recent-signals listing) | shows retired players | + JOIN or leave for diagnostic value | UNION needed |
| `signal_tracker.py:208,280,331,367,372,375,411` (HALTED_EMIT_FILTER readers) | leaks pre-halt-retired rows | + JOIN; clean | UNION needed |
| `signal_tracker.py:243,311` (watching-status JOINs) | minor | + JOIN | UNION needed |
| `crew_scanner.py:3965` (fleet consensus) | retired-player can vote | + JOIN; clean | clean |

---

## Open questions for the Admiral

1. **Is the 34-active-row impact on fleet consensus calculations material today?** The picks gate to a 3-agent fleet threshold; a single retired-player vote could push a 2-agent emerging consensus to the 3-agent threshold. Worth a sanity-check query: how often does fleet consensus include a retired-player row?
2. **Do you want β as a follow-up session?** Low-risk, bounded scope, no DB change.
3. **Would a new column `halted_emit_mode` (TEXT: `active`/`exit_only`/`full`) replace the boolean `halted_emit`?** Captures the granularity HM-Q's open question #3 raised. If the answer is "yes someday," β can be deferred until then since β's JOIN-based approach captures the same information dynamically.
