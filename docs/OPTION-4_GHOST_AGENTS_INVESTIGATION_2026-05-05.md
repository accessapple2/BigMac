# Option 4 — Ghost Agents Investigation
*2026-05-05 morning, Scotty investigation. **No fixes applied. Retirement decisions deferred to Admiral.***

## Question
HM-T-fleet audit (commit 836fd09 / corrected ee481fa) classified 4 ghost agents in `engine/options_agents.py` as ⚪ "by-design" without verification:
- `ghost-kirk-bc` (GhostKirkBullCall)
- `ghost-kirk-0dte-bc` (GhostKirk0DTEBullCall)
- `ghost-long-call` (GhostLongCall)
- `ghost-naked-put` (GhostNakedPut)

All 4 show zero lifetime signals, zero trades, zero options_trades. Are they intentional placeholders, or orphaned scaffolding (PED-class)? This investigation tests the assumption and presents options without recommending.

## Method
Read-only investigation (~5 min): grep for code references; inspect `options_books`, `ai_players`, `signals`, `trades`, `options_trades` tables; trace the only consumer (`/api/options/scan-preview` endpoint); read class hierarchy + scan logic.

## Architecture intent (the "by-design" claim has substance)

`engine/options_agents.py` defines 8 agents partitioned into 2 explicit books:

```python
ALL_AGENTS: Dict[str, OptionsAgent] = {
    # Production book
    "quark-ic":            QuarkIronCondor(),
    "mccoy-bps":           McCoyBullPut(),
    "anderson-bcs":        AndersonBearCall(),
    "covered-call":        CoveredCallAgent(),
    # Ghost research book
    "ghost-kirk-bc":       GhostKirkBullCall(),
    "ghost-kirk-0dte-bc":  GhostKirk0DTEBullCall(),
    "ghost-long-call":     GhostLongCall(),
    "ghost-naked-put":     GhostNakedPut(),
}
```

The `GhostAgent` base class (line 453) has `book_tag = "ghost"` and a `can_fire()` method that enforces a 20% drawdown gate against the ghost book in `options_books`. Both books exist as DB rows (created 2026-04-21):

| book_tag | starting_capital | notes |
|---|---:|---|
| fleet | $7,500 | Production options book — Quark IC, McCoy BPS, Anderson BCS, Covered Call |
| ghost | $2,500 | Ghost research book — Kirk BC, Kirk 0DTE BC, Ghost Long Call (control), Ghost Naked Put (control) |

The "(control)" annotations next to Long Call and Naked Put indicate **deliberate research design** — paired strategies (Kirk BC vs Long Call as spread-vs-naked, etc.) for A/B comparison.

`role='ghost'` is the discriminator in `ai_players.role`. All 4 ghost rows are `halt_mode='active'`, `is_active=1`, `provider='options-engine'`, created 2026-04-21 16:32:25 (3-min after the production options agents).

## Reality (intent vs activation)

**The only consumer of `run_scan_cycle` is `dashboard/app.py:17731`** (`/api/options/scan-preview`). Comments inside the endpoint explicitly state:
> Dry-run scan of all options agents. Returns what each agent WOULD do given current regime/VIX/convergence — **no trades are created**.

And inside `run_scan_cycle` itself:
> Does NOT execute any trades — pure signal generation. **Execution happens via a separate confirm step after Admiral review.**

That "separate confirm step" does not exist in the codebase. There is no scheduler entry, no production caller, no execution pathway downstream of the preview. Operator must manually hit the dashboard endpoint to trigger any scan cycle, and even then the result is JSON-displayed only.

So both books — production and ghost — have the same status: **scaffolded but never activated for production execution**. This is also why this morning's halt of the 4 production options agents (commit 06b5ce7) had no behavioral impact — they were preview-only too.

## Per-agent classification

All 4 ghosts share identical classification (same dispatch path, same architectural hookup, same activity level).

### ghost-kirk-bc (GhostKirkBullCall, line 486)
- **Class:** 🟡 **Half-wired (research-book-by-design)**
- **Real class with real logic:** scan() filters on regime=BULL, vix<20, requires 3+ fleet BUY convergence on universe ['SPY','QQQ','NVDA','AAPL','MSFT'], returns bull-call-spread proposal with target_delta_long=0.45, spread_width=5, dte 14-30. Not a stub.
- **Consumer:** dashboard `/api/options/scan-preview` only.
- **Lifetime activity:** signals=0, trades=0, options_trades=0.
- **Path to fire:** would need (1) operator hit preview, (2) regime=BULL + vix<20 + 3+ converging fleet BUYs, (3) a "confirm step" wired downstream that doesn't exist today, OR (4) explicit scheduler entry plus execution path.
- **Recommendation:** **NOT PED-class.** Functional research scaffolding waiting for activation, not orphaned. Leave alone unless the broader options-engine subsystem is being retired.

### ghost-kirk-0dte-bc (GhostKirk0DTEBullCall, line 561)
- **Class:** 🟡 **Half-wired (research-book-by-design)**
- **Real class with real logic:** 0DTE variant of the same pattern (different DTE constraints, intraday focus).
- **Consumer:** same dashboard preview.
- **Lifetime activity:** all zeros.
- **Path to fire:** same as ghost-kirk-bc, with 0DTE timing requirements.
- **Recommendation:** **NOT PED-class.** Same as above.

### ghost-long-call (GhostLongCall, line 641)
- **Class:** 🟡 **Half-wired (research-book-by-design)**
- **Role:** "(control)" per options_books.ghost note — naked-call counterpart to GhostKirkBullCall's spread, intended for A/B edge comparison.
- **Real class with real logic.**
- **Lifetime activity:** all zeros.
- **Recommendation:** **NOT PED-class.** Control arm of an A/B research design. Retiring it independently would invalidate the comparison if the experiment ever activates.

### ghost-naked-put (GhostNakedPut, line 712)
- **Class:** 🟡 **Half-wired (research-book-by-design)**
- **Role:** "(control)" per options_books.ghost note — paired with one of the other ghosts (likely Kirk 0DTE) for naked-put-vs-spread or naked-put-vs-CSP comparison.
- **Real class with real logic.**
- **Lifetime activity:** all zeros.
- **Recommendation:** **NOT PED-class.** Same control-arm logic.

## Aggregate verdict

**The HM-T-fleet ⚪ "by-design" classification was directionally correct.**

What the audit got right: ghost agents are not orphans. They have real classes with real scan logic, partitioned into a research book with its own drawdown gate, designed as an A/B research framework alongside the production options agents.

What the audit missed (now corrected here): the ghosts share their dispatch path with the production options agents we halted this morning. Both groups are preview-only — neither has a scheduler entry or a downstream execution step. This makes the morning halt of the 4 production agents (anderson-bcs/mccoy-bps/quark-ic/covered-call) a soft action: the halt prevents any future signal-emission via halt_gate, but those agents weren't producing signals via the preview endpoint either way.

The 4 ghosts and the 4 (now-halted) production options agents are not different in execution status — they're different only in `role` (production vs ghost) and `book_tag` (fleet vs ghost). All 8 are scaffolded research infrastructure waiting for activation that hasn't happened.

## Recommended action — Admiral picks; investigation does NOT pre-commit

### Option A — Leave all 4 ghosts alone (no action)
The cleanest outcome of the investigation: HM-T-fleet's ⚪ classification is upheld. Ghosts stay `halt_mode='active'` so the preview endpoint can still surface them as "what we WOULD do." options_books.ghost row stays. No code change. No DB change.
- **Pros:** Zero work. Preserves the research framework for possible future activation. Symmetric with the 4 production agents we halted this morning (those stay in code, just halted; ghosts stay in code, just inactive).
- **Cons:** Continues the "scaffolded but inactive" pattern indefinitely.
- **Effort:** 0 min.

### Option B — Halt all 4 ghosts (mirror morning's halt-only pattern)
Apply the same halt_mode='full' to the 4 ghost player rows that we applied to the 4 production options agents this morning. Code untouched, dashboard preview keeps working, halt_gate blocks any hypothetical first emission.
- **Pros:** Symmetric closure of the entire `options_agents.py` registry. All 8 agents in same halt state. Cleaner mental model: "options-engine subsystem is fully halted pending separate activation decision."
- **Cons:** Pre-commits to "no ghost activation" without explicit decision. Loses the implicit signal that ghosts were intended for activation later.
- **Effort:** 5 min DB UPDATE, mirrors commit 06b5ce7.

### Option C — Activate the ghosts (opposite direction)
Wire a scheduler entry for `run_scan_cycle` to fire on a cadence (e.g., every 15 min during market hours), build the "separate confirm step" that converts preview output into actual trades, populate `options_trades` for the ghost book to start collecting the 60+ days of statistical evidence the GhostAgent docstring describes.
- **Pros:** Realizes the original research intent.
- **Cons:** Substantial work (~1-2 days). Decision-heavy: which scheduler cadence, what confirm step (auto-execute? operator approval? convergence threshold?), how to integrate with the gate-flipped spread strategies (which already route to Alpaca via a different path).
- **Effort:** Medium-large. Out of scope for this investigation.

### Option D — Retire the entire options-engine subsystem
Halt all 8 agents (already halted: 4 production; would-halt: 4 ghosts), archive `engine/options_agents.py` along the same path the prompt originally proposed for HM-T-fleet (later corrected because of the dashboard import dependency). Disable or stub the `/api/options/scan-preview` endpoint.
- **Pros:** Most aggressive simplification. Subsystem fully retired.
- **Cons:** Loses research framework. Loses dashboard preview capability. Most disruptive.
- **Effort:** 30 min mechanical.

## Open questions for the Admiral

1. **Was ghost-trading activation always the plan?** The options_books.ghost row was created 2026-04-21 with $2,500 starting capital, suggesting "we'll activate this later" intent. Is "later" still on the roadmap, or has the plan changed?
2. **Should ghost-* halt status mirror their production-options siblings?** This morning's halt was Option 1 from a different decision tree. Is symmetric treatment of the entire `options_agents.py` registry desired, or is the production-vs-ghost role distinction load-bearing?
3. **Is the "separate confirm step" promised in the run_scan_cycle docstring a real planned feature, or aspirational comment?** If real, the ghosts are valid pending-activation infrastructure (Option A or C). If aspirational, the entire options_agents subsystem is preview-only by accident (Option B or D).

## What I (Scotty) deliberately did NOT do

- **Did not retire any ghost agents.** Even though Option B is the smallest change available, retirement is a separate decision in a separate session.
- **Did not touch ai_players rows.** No UPDATE to halt_mode for any ghost player.
- **Did not modify engine/options_agents.py.** Code unchanged.
- **Did not change the dashboard endpoint.** `/api/options/scan-preview` still returns ghost results.
- **Did not strongly recommend.** Option A is the no-work outcome and consistent with HM-T-fleet's classification, but B/C/D are valid choices depending on the Admiral's intent for the options-engine subsystem as a whole.
- **Did not investigate the "separate confirm step" question.** That requires either Admiral input or deeper code archaeology beyond this session's scope.

## Side notes (NOT scope, but observed during investigation)

1. **The morning halt of 4 production options agents was effectively cosmetic.** Those 4 agents had no scheduler entry, no path to fire trades or signals beyond the preview endpoint. The halt prevents hypothetical future emissions but doesn't change current behavior. (This is fine — the halt was about marking the agents as not-production, which it does correctly.)

   <!-- HM-Z-cross-ref: 2026-05-05 — clarification appended after HM-Z (commit 306dcf6) -->
   **Footnote (HM-Z cross-ref, added 2026-05-05):** the "cosmetic" claim above applies *only* to `engine/options_agents.py` — the preview-only subsystem reachable via `dashboard/app.py:17731`'s `/api/options/scan-preview` endpoint. There is a **separate real-broker execution path** at `engine/alpaca_options.py` (`submit_vertical_spread`, `submit_iron_condor`, `execute_options_signal`) that is called by `strategies/bull_call_spread_v1.py` and `strategies/bear_put_spread_v1.py` via scheduler entries (`main.py:2622`, `:2671`, `:2716`). HM-Z investigation (commit 306dcf6) confirmed the alpaca_options path is **live** — row 14 of `options_trades` (SPY bull_put_spread @ $1.79, `broker_order_id=88d58691-7ca0-4ef6-8db2-9c6d10999a8d`) was the first real spread fill on Alpaca paper post-gate-flip, executed through alpaca_options.py with no involvement from options_agents.py. The 4 halted production agents (`anderson-bcs` / `mccoy-bps` / `quark-ic` / `covered-call`) live in `options_agents.py` only — their halt status remains correct-but-cosmetic for the preview path, and is unrelated to the working spread-strategy execution path. Cross-references: `docs/HM-I_BRIDGE_SCOPE_INVESTIGATION_2026-05-05.md` (Trade-off #7 names this "third forward path"), and CLAUDE.md § "Architecture: Two-Book Bridge Policy" (notes the spread strategies route via `alpaca_options.py::execute_options_signal`, bypassing the player-keyed `_EXECUTION_PORTFOLIO_BY_PLAYER` table).
2. **The HM-T-fleet doc still says "imported by nothing"** in the per-agent root-cause section. Pre-existing doc-correction follow-up; not a Scotty action this session.
3. **The 4 ghost agents and 4 production options agents share identical dispatch.** Any future activation/retirement decision is more naturally made at the subsystem level (the entire `options_agents.py` file) rather than per-agent.

The Admiral reads this doc and picks A/B/C/D. Implementation lands as a separate session prompt with the chosen option pre-baked.
