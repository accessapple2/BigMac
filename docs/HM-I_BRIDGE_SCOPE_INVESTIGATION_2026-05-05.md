# HM-I — Bridge Scope Investigation
*2026-05-05 morning, Scotty investigation. **No fixes applied. No recommendation.** Admiral picks the path.*

## Question
The internal-book ↔ Alpaca-paper bridge has known divergence. Should we (a) sync internal positions to Alpaca, (b) limit the bridge to spread strategies + already-routed agents only, (c) accept status quo, (d) retire the internal book? Or some other path the Admiral has in mind?

## TL;DR for the Admiral

The investigation surfaced **5 options** (α/β/γ/δ/ε) plus one **active code-level finding** that's distinct from the architectural decision:

> **Active finding:** `engine/paper_trader.py:1300` calls `_forward_to_alpaca("SELL", ...)` **without** the `route_mode == "trading"` gate that the BUY path (line 1015) and full-SELL path (line 1167) both have. This is the source of today's 181 phantom-position SELL skips: legacy fleet players (deepseek-7b-grok4, ollama-qwen3, ollama-plutus, qwen3-8b-flash) emit *partial* SELLs, those forward to Alpaca despite the players being mapped to "Arena Paper" (`route_mode=paper`), Alpaca says qty=0, the SHORT-GUARD skips them, and we get the noise.

The skip noise is **harmless** (SHORT-GUARD prevents any actual short or position drift on Alpaca) but **noisy** (181 entries in today's log, will repeat each morning). It can be silenced with a 1-line gate fix (Option ε) **independent** of which architectural path is chosen.

I am NOT applying that fix this session per the no-code-changes rule. Just flagging it.

---

## Current state — bridge architecture (3 books, 2 flows)

### The 3 books
1. **Internal AI fleet book** (`positions` table, all rows where `player_id != 'webull'`): 40 positions across 9 active players. Grown by `engine/paper_trader.py` writes per agent. Includes shorts and futures (GC=F, SI=F) that Alpaca paper can't accept.
2. **Alpaca paper account** (live broker): 3 positions, $5,734 market value, $94,318 cash, $100,052 equity.
3. **`webull` mirror** (`positions` table, `player_id='webull'`): 3 positions matching Alpaca exactly. Rebuilt from Alpaca on each `run_full_alpaca_sync` cycle.

### The 2 flows
- **Forward (internal → Alpaca):** `engine/paper_trader.py::_forward_to_alpaca` (line 216). Called from BUY (line 1015), full-SELL (line 1167), and partial-SELL (line 1300). BUY and full-SELL gate on `route_mode == "trading"`; **partial-SELL does not** (the active finding above).
- **Reverse (Alpaca → internal):** `shared/alpaca_portfolio_sync.py::run_full_alpaca_sync` (line 84). DELETEs `webull`'s rows then INSERTs from Alpaca's live positions. Also updates `ai_players.cash` for `webull` and `portfolios.current_balance` for portfolio_id=1 ("Alpaca Paper"). Runs on an interval keyed off market session.

### The routing table
`engine/paper_trader.py::_EXECUTION_PORTFOLIO_BY_PLAYER` (line 151) — only **4 mapped players**:

| Player | Mapped portfolio | Resolved `route_mode` | Forwards to Alpaca? |
|---|---|---|---|
| super-agent | Alpaca Paper | trading | yes |
| ollie-auto | Alpaca Paper | trading | yes |
| neo-matrix | Neo Matrix | trading | yes |
| dalio-metals | Enterprise Computer (no DB row exists) | falls through → paper | no |

All other 45 players in the roster default to `portfolio_name="Arena Paper"` (no DB row), `route_mode="paper"`. **They should not forward to Alpaca** — and on the gated paths they don't. The only leak is the ungated partial-SELL path at line 1300.

### The portfolios registry
`portfolios` table has 8 rows. Active ones:

| ID | Name | Type | Execution | Notes |
|---|---|---|---|---|
| 1 | Alpaca Paper | paper | auto | Default Alpaca paper, current $100,051 |
| 2 | Webull | trading | manual | Steve's live (is_active=0; never auto) |
| 3 | TradeStation | trading | auto | Inactive (initial=$0) |
| 4 | IBKR | trading | auto | Inactive (initial=$0) |
| 5 | Dalio Metals | physical | tracking | Display only ($7,247) |
| 6 | Mr. Anderson | paper | auto | Synthetic aggregate |
| 7 | Neo Matrix | independent | auto | $7,050, neo-matrix routes here |
| 8 | Schwab | trading | tracking | MONITOR ONLY ($0) |

## Current state — divergence count

### Internal positions Alpaca doesn't have (Type 1)
Computed: `internal positions where player_id != 'webull'` minus `Alpaca live positions`.

| Player | n | Symbols (qty) |
|---|---:|---|
| ollama-qwen3 | 8 | AAPL, AMD, NOW, NVDA, ORCL, PLTR, QQQ, TSLA |
| qwen3-8b-flash | 8 | AAPL, GOOGL, META, NOW, NVDA, ORCL, SPY, TSLA |
| ollama-plutus | 7 | AAPL, AMD, META, MSFT, NOW, NVDA, TSLA |
| deepseek-7b-grok4 | 6 | AAPL, META, MSFT, NOW, NVDA, TSLA |
| capitol-trades | 3 | AXP, COR, FMAO |
| dalio-metals | 2 | GOOGL, ONDS *(stocks, not metals — looks misclassified)* |
| enterprise-computer | 2 | GC=F, SI=F *(futures — Alpaca paper can't accept)* |
| gemini-2.5-flash | 2 | IREN -13.29, ONDS -135.41 *(both shorts)* |
| energy-arnold | 1 | QQQ |
| **Total Type 1** | **39** | |

### Alpaca positions internal book doesn't have (Type 2)
None. The webull mirror covers exactly the 3 Alpaca positions (KMI, NVDA, WMB).

### Quantity mismatches (Type 3) — symbols held both internal and Alpaca, with different qty
- **NVDA:** webull mirror = 12.34 (matches Alpaca). Other 4 internal holders (qwen3-8b-flash, deepseek-7b-grok4, ollama-qwen3, ollama-plutus) hold a separate 17.50 NVDA the broker doesn't know about. **Not technically a Type 3** — different player_ids; the question is whether to consider these "the same position" or "different books."

### New entries from legacy fleet (Type 4 — ongoing growth)
Last 24h: 5 BUYs from ollama-plutus and qwen3-8b-flash on AAPL, AMD, NVDA, GOOGL. **None gated to forward to Alpaca** (these players aren't in the routing table). So the divergence isn't growing on the BUY side.

### Skip rate — today
- 181 `Alpaca SELL ... skipped: Alpaca qty=0.0` log entries in `logs/trader.log` today.
- All 181 originate at `paper_trader.py:244` (the SHORT-GUARD inside `_forward_to_alpaca`).
- Per investigation above, the entry path is the **ungated partial-SELL at line 1300**. Not a new architectural problem; a code-level inconsistency the line 1167 path doesn't have.
- Pre-gate-flip pattern (commit `df7320c`, 2026-05-04 morning) saw similar skip volume; user reported it dropped to zero by EOD as legacy fleet drained internal SELLs. Today's 181 are the morning-window resurgence as the legacy fleet starts a new emit cycle.

### Trend
- BUY-side: stable (gated, not growing).
- SELL-side: noisy each market open, drains across the session, then quiets. Not pathological — divergence isn't widening, just oscillating.
- Reverse-flow: stable (Alpaca → webull mirror works fine).

---

## Architectural options

Five options. Each presented with effort, risk, pros, cons, reversibility, and open questions.

### Option α — Full sync (legacy positions → Alpaca)
**One-liner:** Reconcile all 39 internal positions onto Alpaca at session start; every internal trade also routes to Alpaca going forward.
- **Effort:** 1-2 days. Initial reconciliation script ($50K+ of paper transactions on Alpaca), removal of `route_mode` gates, handling for shorts (Alpaca paper supports), handling for futures (Alpaca paper does NOT support GC=F / SI=F — special-case carve-out needed).
- **Risk:** **High.** Pollutes the Alpaca paper account state with legacy positions that the spread strategies (gate-flipped 2026-05-04) are operating against. Could distort spread-strategy backtest baselines. Once Alpaca state is changed, reverting requires more paper transactions.
- **Pros:** Single source of truth. Eliminates the divergence concept entirely. Internal `positions` table becomes a (player-keyed) view of the broker book.
- **Cons:** Conflates "test environment" with "broker reality." Loses the legacy-fleet calibration data that the per-player paper books represent. Doesn't naturally accommodate shorts and futures uniformly. Aggressive change against a freshly gate-flipped soak.
- **Reversibility:** Hard. Once Alpaca state changes, undoing requires accurate replay of every legacy fleet entry/exit. Possible but expensive.
- **Open questions for the Admiral:**
  1. Should pre-gate-flip legacy positions be reconciled at cost-basis or at current price?
  2. How do shorts and futures get represented? (Alpaca paper supports stock shorts but not futures.)
  3. If a legacy agent later under-performs and gets halted, does the position persist on Alpaca or get unwound?

### Option β — Bridge limited to spread strategies + already-routed agents (firm separation)
**One-liner:** Internal book and Alpaca book stay separate ledgers, by explicit design. Only super-agent, ollie-auto, neo-matrix, and the spread strategies route to Alpaca. Everyone else's internal book is paper-only, never reaches the broker.
- **Effort:** Small. The architecture already mostly works this way. Required work:
  - Fix the partial-SELL leak at line 1300 (apply the same `route_mode == "trading"` gate as line 1167) — this is **Option ε**, see below.
  - Dashboard updates to visibly distinguish "internal book" panels from "Alpaca paper" panels.
  - Reconciliation tool that explains the two-book picture for any audit.
  - Document the policy in CLAUDE.md.
- **Risk:** Low. Aligns with the post-gate-flip mental model (spread strategies = real-on-broker, legacy fleet = paper-only).
- **Pros:** Minimal change. Preserves legacy-fleet test isolation. Keeps shorts and futures handling natural (they live in the internal book; broker never sees them). The spread strategies, which are the agents whose performance actually matters per CLAUDE.md, route to Alpaca cleanly.
- **Cons:** Two books exist forever. Anyone reading dashboards must understand which book they're looking at. The naming "Arena Paper" vs "Alpaca Paper" needs better discipline (today they're easy to confuse).
- **Reversibility:** Easy. Could move to α later by promoting more players into the routing table.
- **Open questions:**
  1. Is the dashboard's existing partition between "Arena Paper" and "Alpaca Paper" sufficient or does it need a redesign?
  2. Should `webull`'s dual role (human Webull + Alpaca mirror) be split into two separate player_ids for clarity?
  3. Should new agents default to internal-only, or should we audit each new agent's intended Alpaca routing during creation?

### Option γ — Status quo + skip-noise suppression
**One-liner:** Accept the divergence; just silence the SHORT-GUARD skip log.
- **Effort:** Trivial (~5 min): change one `console.log` line to `console.log(level=DEBUG)` or similar.
- **Risk:** Medium. The skip-noise is a useful canary — it surfaced this entire investigation. Silencing it makes future divergence drift invisible until something else breaks.
- **Pros:** Zero engineering effort. Doesn't pre-commit to any architectural decision.
- **Cons:** Divergence becomes invisible. The 181 daily skips that prompted HM-I would never have surfaced if we'd done this earlier. The line 1300 leak (Option ε territory) stays unfixed.
- **Reversibility:** Easy.
- **Open questions:**
  1. If we lose the canary, what replaces it? A daily reconciliation report?

### Option δ — Retire internal book entirely
**One-liner:** Stop the legacy fleet from emitting trades. Halt all 9 active legacy-fleet players. Only spread strategies (already gate-flipped) and the 3 currently-routed agents (super-agent, ollie-auto, neo-matrix) survive.
- **Effort:** Medium (~1 day): halt 9 players via `halt_mode='full'` per CLAUDE.md halt runbook, drain their internal positions, document in CLAUDE.md retirement section.
- **Risk:** **High strategic risk.** Loses every legacy agent's signal contribution. The qwen3-8b-flash, deepseek-7b-grok4, ollama-plutus, ollama-qwen3 quartet IS the current live "🟢 Active" cohort per HM-T-fleet — they're the agents producing daily signals and trades. Halting them empties the soak.
- **Pros:** Ultimate simplification. Single book. No divergence. Bridge becomes a non-question.
- **Cons:** Calibration goes stale. The Active 4 fleet (McCoy / Neo / Dax / Capitol per CLAUDE.md) currently runs through the legacy-fleet write paths — δ effectively means halting most of those too unless they're added to `_EXECUTION_PORTFOLIO_BY_PLAYER` first (which is just Option α with a different framing).
- **Reversibility:** Medium. Halt is reversible per the runbook (set `halt_mode='active'`, leave history intact), but the calibration window gap is permanent.
- **Open questions:**
  1. Is the legacy fleet still producing useful research output that'd be lost? (Per CLAUDE.md OOS validation, yes — McCoy at +11.1 Sharpe.)
  2. Would halting clear the divergence count to zero immediately, or do legacy positions sit until manually drained?

### Option ε — Plug the partial-SELL leak only (deferred-decision option)
**One-liner:** Fix `paper_trader.py:1300` to add the `route_mode == "trading"` gate matching line 1167. Don't pick α/β/γ/δ — buy time for the architectural decision.
- **Effort:** Tiny (~15 min including verification): wrap line 1300 in `if route["route_mode"] == "trading":`, restart service, confirm skip count drops to ~0.
- **Risk:** Very low. Same gate already used at line 1167 (full-SELL) — copy-paste plus context check.
- **Pros:**
  - Eliminates today's 181-skip noise immediately.
  - Restores the gating symmetry that BUY and full-SELL already have.
  - Doesn't pre-commit to any architectural option — α/β/γ/δ remain on the table.
  - Removes the "skip noise canary" benefit, but only because the canary was firing on a *bug*, not on actual divergence.
- **Cons:** Doesn't answer the bigger architectural question. The 39 internal Type-1 positions still exist; they just stop trying to forward.
- **Reversibility:** Trivial — one-line revert.
- **Open questions:**
  1. Once ε is applied, do we still need to make an α/β/γ/δ decision, or is the divergence acceptable indefinitely?
  2. After ε, what *would* a future canary look like to detect new architectural drift? (Daily reconciliation cron job comparing internal vs Alpaca? End-of-day report?)

---

## Trade-offs the Admiral should consider

1. **Option ε is decision-orthogonal.** It can be applied today without committing to α/β/γ/δ. The architectural decision becomes "do we want one book or two?" without the noise distraction.

2. **β and ε together** are arguably the cheapest path that ends the noise AND ends the divergence-drift question. β codifies "two books by design," ε plugs the one ungated path that the current architecture already implies should be gated.

3. **α and δ are mirror-image aggressive options.** α merges the two books up onto Alpaca; δ shrinks the fleet so only one book exists. Both make the divergence concept disappear, but at different costs (paper-account state pollution vs lost calibration data).

4. **γ alone is the worst option** — it silences the canary without addressing the underlying cause, and would have prevented this investigation if applied earlier.

5. **The dashboard naming overlap is its own problem.** "Arena Paper" (default unmapped routing) vs "Alpaca Paper" (portfolio id 1) sound like the same thing but route differently. β requires a naming-discipline pass; α and δ don't (because one of the two books goes away).

6. **`webull` player's dual role is technical debt regardless of option.** Today it's both "the human Webull benchmark" AND "the Alpaca mirror" because `alpaca_portfolio_sync.py` uses `player_id='webull'` for the Alpaca-positions destination. Splitting this into `webull` (human) and `alpaca-mirror` (broker) is a small refactor that any of α/β/δ would benefit from.

7. **Spread strategies (post-gate-flip) need clarity.** They route to Alpaca via `engine/alpaca_options.py::execute_options_signal`, NOT through `_forward_to_alpaca`. This is a third forward path that bypasses the player_id routing table entirely. Worth making sure whatever option is picked accounts for this third path.

---

## Open questions for the Admiral

1. **Do you want one book or two as the long-term truth model?** (α/δ = one book; β = two books by design; γ/ε = silent on this question.)
2. **Is the current 4-player routing table (`_EXECUTION_PORTFOLIO_BY_PLAYER`) the intended "Alpaca-routed" set?** I observed super-agent, ollie-auto, neo-matrix actively forwarding; is that by design, or are some of those vestigial?
3. **What's the post-gate-flip mental model for spread strategies?** They're "real-on-broker" via a third forward path. Should that mental model extend to the legacy fleet (α direction) or stay separated (β direction)?
4. **Should I apply Option ε now as a decision-independent line-1300 patch?** It's a 15-minute change that ends the daily skip noise and doesn't pre-commit to an architectural choice. (I am NOT applying it this session per the no-code-changes rule of this prompt — but it's the smallest unit of work that could land independently.)

---

## What I (Scotty) deliberately did NOT do

- **Did not pick an option.** All 5 are presented neutrally; the Admiral picks.
- **Did not implement any code or schema change.** The line 1300 leak is *flagged*, not patched.
- **Did not strongly recommend.** The TL;DR notes that ε is decision-orthogonal and could be applied independently, but it's still presented as one option among five.
- **Did not silence the skip-noise.** That's Option γ; the Admiral may prefer it.
- **Did not investigate every consumer of `engine.alpaca_bridge`.** Skipped: dashboard read paths, cash_manager, tax_harvester, alpaca_sync.py — all readers, not writers, so they don't affect divergence.

The Admiral reads this doc, asks follow-up questions if needed, and picks the path. Implementation lands as a separate session prompt with the chosen option pre-baked.
