# Worf wiring gap — traced 2026-09-01

**STATUS: Worf's gap is closed/traced — root cause identified below, do not
re-trace it.** The live open thread is the Aug 27 gate-rejection pattern
(`BENCH: rating D (40/100)` against `ollama-plutus`) — pick up there, not here.

**Branch:** exec-pipeline, HEAD 8431548 (unchanged — investigation only, no code edits)

## Question posed
Worf (`qwen3-8b-flash`, B-rated 78.3/100) participated in War Room 389x today but
never reaches signal_emit/gate. Not a gate rejection. Find where his trade-decision
loop is supposed to fire and why it doesn't.

## Fork question answered: does Worf emit anywhere?

Checked every candidate downstream table for player_id/agent-column matches on
`qwen3-8b-flash`:

| Table | Worf rows | Notes |
|---|---|---|
| `war_room` | 4,627, latest today 16:10:23 | Debate/take layer only — free-text `take` column, no direction/confidence/action |
| `decision_audit` | **0** | Today's 1,140 rows are 100% `ollama-plutus` |
| `quorum_votes` | 0 (table is empty for everyone, 0 rows total) | Looks structurally dead, not a Worf-specific gap |
| `bridge_votes` | 0 | |
| `crew_decisions` | 74, but **stale — last row 2026-05-29T19:43**, all `action='PASS'` | Separate/older pipeline, not the live one |
| `signals_v2` / `signals` | not directly checked by player but implied 0 (decision_audit signal_emit gate is upstream of both) | |

**Verdict: he never emits past the debate layer.** This is the "different fix" branch
from the original question — not a consumer-side gap, a producer-side one.

## Arena trace — where the trade-decision loop actually lives

- `engine/ai_brain.py:356` — the trading engine class is `Arena` (not `AIBrain`).
  `Arena.__init__(providers: list, ...)` builds `self.providers = {p.player_id: p
  for p in providers}` (`ai_brain.py:358`).
- `Arena.run_scan(symbols, force=False, player_ids: set | None = None)`
  (`ai_brain.py:380`) — `player_ids` is an **allowlist**; only players named in it get
  `_run_player()` called this cycle.
- `Arena._run_player()` (`ai_brain.py:1008`) → for each symbol calls
  `provider.analyze_chain(...)` (line 1284) → `save_signal(player_id, ...)`
  (line 1300, `engine/paper_trader.py:3780`). `save_signal()` is the single
  chokepoint that writes `signals` + `decision_audit(event_type='signal_emit')` +
  `signals_v2`/events bus. This is the ONLY code path that gets a player into
  `decision_audit`.
- **`Arena()` is instantiated** in `main.py:137` inside `initialize_arena()`
  (`main.py:102`), called lazily from `run_scanner()` (`main.py:419-420`:
  `if arena is None: arena = initialize_arena()`), held in the module-level
  global `arena`.

### Provider list (who CAN trade) vs player_ids (who DOES this cycle)

- **Provider list** — `initialize_arena()` calls
  `build_all_providers(default_url=OLLIE_URL, default_timeout=180,
  skip_ids={"ollama-llama"})` (`main.py:124`, `engine/agent_routing.py:164`).
  This is DB-iterated: every `ai_players` row with `halt_mode != 'full'` and not in
  `skip_ids` gets a provider built. Worf's row —
  `halt_mode='active', is_active=1, provider='ollama'` — passes this filter cleanly.
  **A provider object for Worf exists in `Arena.providers`.**
- **player_ids allowlist** — built per-cycle in `run_scanner()` (`main.py:437-453`)
  as the union of three hardcoded tier sets:
  - `_SCAN_TIER1` (`main.py:245`, 30min) — 1 entry (`mlx-qwen3`, itself `halt_mode='full'`)
  - `_SCAN_TIER2` (`main.py:250`, 2hr) — `ollama-plutus`, `ollama-qwen3` only
  - `_SCAN_TIER3` (`main.py:266`, open/close only) — 10 cadet entries
  - **`qwen3-8b-flash` is in none of the three.**

The comment directly above `_SCAN_TIER2` (`main.py:253-262`) states the cause
explicitly:

> `qwen3-8b-flash (Worf) REMOVED 2026-05-29 (HM-WORF-DRIFT-RECONCILE): benched
> S6.1 → ADVISORY_CREW (bridge-vote only), non-emitting since 2026-05-07.`

This is corroborated in `engine/crew_specialization.py:76`:
`ADVISORY_CREW` list includes `"qwen3-8b-flash", # Worf — benched S6.1 (-0.36%)`.

**Root cause:** on 2026-05-29 a performance review (S6.1, -0.36%) benched Worf,
removed him from `_SCAN_TIER2`, and added him to the hardcoded `ADVISORY_CREW`
list — both of which gate the trading loop (`player_ids` allowlist and, presumably,
downstream mandate checks keyed off `ADVISORY_CREW`). **Nobody re-added him to a
tier since.** Meanwhile `war_room.py`'s debate-eligibility filter
(`engine/war_room.py:1128-1132`) checks only `is_paused` / `is_active` /
`halt_mode` — it has **no knowledge of `ADVISORY_CREW` or scan tiers** — so Worf
keeps debating every cycle while being structurally unreachable from
`Arena.run_scan()`. Two independent eligibility systems, one updated in 2026-05,
the other never touched.

`ai_players.crew_role='active'` / `role='production'` for Worf's DB row is stale/
misleading in the other direction — the DB doesn't reflect the 2026-05-29 bench
decision at all. Whatever produced the current "B-rated 78.3/100, genuinely
tradeable" scorecard read is also disagreeing with the 3-month-old `-0.36%`
verdict baked into `ADVISORY_CREW`. Three sources of truth (DB crew_role, hardcoded
ADVISORY_CREW list, current scorecard) all disagree about Worf's status right now.

## Does this explain the Aug 27 fleet-wide trade_fire cutoff? **No.**

Checked whether Worf's exclusion is the same mechanism behind "no discretionary
agent has fired a trade in 5+ trading days":

- Worf's tier removal is dated **2026-05-29** — three months before the Aug 27
  cutoff. Wrong timing to be the cause.
- Properly-wired, non-benched agents (`ollama-plutus`, `ollama-qwen3` — both live
  in `_SCAN_TIER2`) are **still emitting normally**: `decision_audit
  event_type='signal_emit'` shows `ollama-plutus` at 587 signals today
  (2026-09-01), 509 yesterday — the emit pipeline is healthy and unaffected by
  the tier/roster issue.
- But in the same 3-day window, `ollama-plutus` signals are **100% non-executed**:
  1,025 of 1,028 recent signals are `REJECTED` at the gate. Breakdown of
  `gate_reject` verdicts (last 3 days, `decision_audit`):
  - 655 × `[HM-MARKET-CLOSED]` (before/after hours — expected/correct)
  - **179 × `BENCH: rating D (40/100)`** — a live gate rejecting `ollama-plutus`
    itself on a bench/rating basis
  - 60 × `LOW_CONVICTION` (below 65% threshold)
- `trades` table for the last 10 days shows real executions only from
  `m5-allocator` (scheduled rebalancer), `neo-matrix`, and `ollie-auto` — all
  rule-based/deterministic per `crew_specialization.py`'s own categorization, not
  LLM-discretionary agents. Zero trades from any LLM/discretionary player since
  at least 2026-08-25.

**Conclusion: two unrelated, simultaneously-open problems.**
1. **Worf-specific, old (since 2026-05-29):** roster/tier exclusion keeps him out
   of `Arena.run_scan()` entirely — never reaches `save_signal`/`decision_audit`.
   Confined to Worf (and the rest of `ADVISORY_CREW`'s "benched S6.x" cohort —
   `ollama-llama`, `qwen3-14b-pro`, `dayblade-sulu` carry the same comment pattern).
2. **Fleet-wide, new (visible from ~08-25 on):** the signal-emit path works fine
   for wired agents, but the post-emit gate is rejecting essentially everything
   that isn't a scheduled/rule-based execution, including a `BENCH: rating D`
   verdict actively firing against `ollama-plutus` — the one agent that IS wired
   and IS emitting. This is a gate/execution-layer issue, not a roster-wiring
   issue, and it postdates Worf's exclusion by three months.

Not chased further per scope (no code changes requested) — the `BENCH: rating D`
gate-reject source and why it started firing against `ollama-plutus` specifically
around 08-25..08-27 is the natural next thread if the Aug 27 cutoff itself becomes
the priority.

## Open, not actioned (no code changes made this session)
- Should Worf be re-tiered? Depends on reconciling the disagreement between DB
  `crew_role='active'`, the 2026-05-29 `ADVISORY_CREW` bench verdict, and the
  current B/78.3 scorecard read — that's an Admiral call, not mechanical.
- Same "benched but still debating" split likely affects the rest of
  `ADVISORY_CREW`'s S6.x-benched cohort, not just Worf — not verified per-agent
  here.
- `BENCH: rating D (40/100)` gate against `ollama-plutus` — origin/trigger date
  not traced; flagged as the likely next step for the Aug 27 cutoff specifically.
