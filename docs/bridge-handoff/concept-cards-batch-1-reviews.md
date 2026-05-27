# OllieTrades Bridge — Concept Cards Batch 1: XO + Scotty Reviews

Companion to `concept-cards-batch-1.md`. Each concept card gets two reviews appended:

- **XO review** — operational discipline, dashboard-display considerations, risk surface, cross-link suggestions.
- **Scotty review** — trader-engineering perspective. What this means for the codebase that has to produce, gate, log, and execute against this concept. Where the rubber meets `paper_trader.py`.

Both reviews assume the existing OllieTrades stack: FastAPI on bigmac:8080, Signal Center on :9000, vanilla HTML/CSS/JS monolith dashboard, paper_trader.buy/sell pipeline with multi-tier gates (SCANNER_FILTER → QUALITY_GATE → halt_mode → MAX_TRADES_PER_DAY).

---

## 📊 MACD

**XO review:**
The card correctly frames MACD as a confirmation tool, not a primary trigger. The "10% weight in composite trend score" line for Spock is the right disclosure — Captain reads that and knows MACD alone won't fire a trade. Worth cross-linking to Convergence in the gotchas: "MACD vote shares momentum DNA with RSI vote — count once in convergence math." Currently sits at a 3-touch reading: histogram divergence (the most useful insight) is buried in numbers and gotchas. Consider promoting "histogram shrinking = early warning before line cross" to its own concept primer down the road. Color-blind note: any MACD chart rendering on the dashboard must use shape + position differentiation between MACD line, signal line, and histogram bars — not three colored lines.

**Scotty review:**
Backend implementation lives in `engine/momentum/` if anywhere — would need to verify. The card prescribes 12/26/9 defaults; existing scanner pipeline computes momentum scores but I haven't audited whether MACD specifically is wired or whether we lean on RSI+price-action+volume convergence instead. If we ship the three-depth Explain layer (handoff section 3), every trade payload claiming an `rsi_overbought` trigger should probably also surface MACD state in `concepts_referenced` even when MACD wasn't decisive — Captain learns more from seeing all signal-family inputs than just the firing one. Data plumbing concern: MACD histogram values aren't currently persisted per-cycle in `trade_signals` table. Adding them is a schema migration worth scoping as `HM-CONCEPT-PERSIST-MACD` for tomorrow's queue. Lag warning in gotchas is real — paper_trader's SCANNER_FILTER currently has no MACD-cross-only filter and shouldn't add one without testing on 60 days of historical signals first.

---

## 📉 Bear Call Spread

**XO review:**
Worf is the canonical caller. Strong card — the asymmetric risk-reward callout in gotchas is the single most important honesty surface for a learning audience. Captain has live `bear_put_spread_v1.py` and `bull_spread_v1.py` agents (per OPS_LOG context); this card describes the *bear call* structure specifically, which is a separate strategy file path that doesn't exist yet in the codebase. Flag the naming carefully — the Bridge dashboard should not display "Worf opened a Bear Call Spread on QQQ" if the actual executing agent is `bear_put_spread_v1`. Cross-link gotcha: earnings inside DTE is the same time bomb across credit spreads — consider a shared sub-card "Earnings inside DTE" referenced from both bear-call and bull-put cards.

**Scotty review:**
Three things land hard in the code:
1. The hard-stop rule ("underlying reclaims short strike A on volume ≥ 1.5× average") needs to translate to a paper_trader exit gate. Currently exits route through `exit_manager` (saw `[exit_manager] 1 open positions for bull_spread_v1` in earlier logs), but I'd want to verify the trigger condition is encoded — not just an alert.
2. The 7-DTE time stop matches Worf's mandatory rule in the Time Stop card — good consistency, but the executor enforcement needs an audit: does Scotty (me) actually fire close orders at 7 DTE automatically, or is it currently captain-manual?
3. The "Probability of profit ≈ 1 minus delta" math is correct for European-style options. American options (most equity options) have early-assignment risk that this card silently ignores. Worth a one-line gotcha addition: "American options can be assigned early on dividends or deep ITM — don't ride a winning short call into ex-div on a dividend payer." This came up in the dividend-payer code path Dax owns.

---

## 📈 Bull Put Spread

**XO review:**
McCoy + Dax own this structure. The card correctly disclaims that "support breaks" — Captain has been burned by selling premium near apparent support in the past (referenced in old session memories). The Dax IVR > 40 threshold is more conservative than McCoy's, which is correct given dividend-payer behavior around ex-div dates. Cross-link suggestion: the "selling into a falling-knife" gotcha should reference Stop Loss card's "moving stops mid-trade" — same psychological failure mode, different surface. Display consideration: when both a Bear Call Spread card AND a Bull Put Spread card render side-by-side in the Concept Drawer (the docs page), keep their visual treatments distinct beyond just the headline — the structural diagrams look near-identical when flipped, and a color-blind reader can lose the mirror symmetry.

**Scotty review:**
Mirrors the Bear Call Spread review almost exactly with one addition: bull put spreads on dividend payers are the trap McCoy + Dax should NOT walk into without dividend calendar awareness. The ex-dividend date can pin a short put deeper in-the-money through assignment risk, and `engine/exit_manager.py` doesn't currently check dividend dates pre-entry. Filing this as `HM-DIVIDEND-AWARE-EXIT` — a real ticket. The card's "skew note: put premium is structurally inflated" is true but means the credit math looks better than the realized profit math after slippage and assignment risk are baked in. Production reality: paper Alpaca executes both spreads with theoretical fills at mid; real Schwab will see worse fills and earlier assignments on dividend payers. The teaching surface should not promise mid-price math will materialize on real broker accounts.

---

## 🌡️ IV Rank

**XO review:**
Cleanest card in the batch. The 0–20 → 90+ scale with explicit "what to do" guidance per band is exactly the pedagogical shape Captain wanted. The "different providers compute the window differently" gotcha is real and consequential — OllieTrades should pick one source (Polygon options chain, likely) and stick with it. Currently I don't know if IVR is computed locally or pulled from a third party — worth a one-paragraph "where this number comes from" addendum in the production card. Cross-link: IV Crush should be a separate concept card (referenced but not yet written). When the Bridge dashboard shows IVR readings, color cue alone won't survive color-blindness — pair the number with a shape (filled circle for HIGH, empty for LOW) and a text label ("RICH" / "MID" / "CHEAP").

**Scotty review:**
Two ground-truth issues:
1. **IVR source.** Polygon Developer plan ($79/mo) gives us options chain data — IV per contract — but **does not directly publish IVR**. We'd be computing it locally from historical IV30 readings. That requires storing the IV30 series in a table (`iv_history` doesn't exist yet) and back-filling 252 trading days. Real work, file as `HM-IVR-COMPUTE` for next session. Without it, every card that references IVR thresholds is referencing a number we don't actually have access to in real-time.
2. **The thresholds.** The card says IVR < 25 blocks Spock's short-premium trades. Quality Gate (`engine/quality_gate.py`) currently has no IVR check anywhere. Wiring it in means a new gate layer between SCANNER_FILTER and QUALITY_GATE, with the IVR computation as a precondition. Sequencing: HM-IVR-COMPUTE first, then HM-QG-IVR-INTEGRATION. Today's QG Patch 1 (ETF fast-path) doesn't conflict — additive layer.

The gotcha about IVR staying elevated for weeks during macro stress matches today's actual market: VIX 18.7, IVR sector-wide is likely 60–80 right now on tech names. If Bridge surfaces real IVR readings tomorrow, expect Captain to see "HIGH IVR" warnings widely — that's correct, not a bug.

---

## ⏰ Time Stop

**XO review:**
Underappreciated discipline tool. The card's framing — "exit by the clock, not by price" — is the right mental model for an audience that's already absorbed Stop Loss as a price-based concept. The cardinal sin callout in Stop Loss should appear in this card too: time stops are also a psychological trap when the trade is "almost there." Cross-link to Stop Loss tight pair. Scotty's note in the "how the crew uses" section ("enforces time stop at the executor level — fires the close order automatically") is the right architectural commitment — surface that prominently in the dashboard whenever a time stop fires so Captain sees the engineering trail.

**Scotty review:**
The card *prescribes* automatic enforcement and *attributes* it to Scotty. That's a contract the codebase must honor. Audit needed:
1. Where in `paper_trader.py` or `exit_manager.py` is the 7-DTE auto-close coded for bear/bull spreads? Saw `[exit_manager]` lines in this morning's log but didn't verify the DTE math.
2. Is the 0DTE 1pm-ET hard close wired? Trader runs in AZ time (UTC-7), so 1pm ET = 10am AZ. Cron schedule check needed.
3. The "swing trade 5–10 bar no-progress" rule is more squishy — "no progress" needs a concrete definition (closed within 0.5× ATR of entry?) before automation.

These are three separate enforcement-layer audits, scoping as `HM-TIME-STOP-ENFORCEMENT-AUDIT`. The output: a report covering which time stops are programmatic vs documented-but-manual. If any are documented-only, the card's claim that "Scotty enforces every time stop programmatically" is overpromise — fix the gap or fix the card.

Today's HM-WAR-ROOM-LATENCY Layer 1 ships cycle-duration logging which is adjacent: time-stop firings should also emit a `[TIME-STOP]` log + NTFY so Captain has visibility when they execute autonomously. Cheap addition to a future Layer 1.5.

---

## 🔄 Gamma Flip

**XO review:**
Most conceptually advanced card in the batch. The "regime line" framing is excellent — Captain can absorb the abstraction in one read. Risk: this is the card most likely to be misused by a learning audience who treats the flip as a binary tradeable signal rather than context. Strongly recommend the production card includes a "for context, not a trigger" callout box above the numbers section. The mention of SqueezeMetrics and SpotGamma as data providers is honest about where the number comes from — but neither is currently subscribed (verify), which raises the next review point.

**Scotty review:**
This is the card the codebase isn't ready for.
1. **No subscription.** OllieTrades does not currently subscribe to SqueezeMetrics or SpotGamma. Both are paid feeds with paid APIs ($50–$200/mo each).
2. **No local compute.** Computing gamma flip from raw options chains requires GEX (Gamma Exposure) modeling assumptions about dealer positioning that are non-trivial. Polygon options chain gives us the raw inputs (open interest × gamma per strike); the rest is a research project, not a configuration toggle.
3. **No persistence layer.** Even if computed, `gamma_state` and `gamma_flip_level` aren't fields on any existing table.

The handoff's trade signal schema (section 6) already includes `regime.gamma_state` as a payload field — which means either (a) we ship the card and the dashboard renders "gamma_state: UNKNOWN" for now, or (b) we defer this card to a later batch and scope `HM-GAMMA-FLIP-RESEARCH` as a real epic. Recommend (b). Shipping the card without backend support sets up a Concept Drawer entry that promises a level the dashboard can't display, which erodes the teaching promise of the whole three-depth Explain layer.

The card itself is solid teaching material. It just doesn't match production readiness.

---

## 🎯 Convergence

**XO review:**
Chekov's namesake. The "4 votes from 4 momentum indicators is one vote dressed up as four" gotcha is the single best line in the whole batch — that's a 10-year-trader insight handed to a novice in one sentence. Lead with that visually in the production card. The number scale (1 vote noise → 6+ strong) is the right pedagogical ladder. Convergence is also the concept that ties most other concepts together; when the Bridge dashboard renders a Convergence card, every other concept that voted should be visible as inline links — turning the card into a hub for navigating to underlying primers.

**Scotty review:**
Convergence is actually implemented in the codebase — `engine.momentum.bridge.fetch_signal_center_active_signals` filters by `confidence >= 70`, which is the system's current proxy for "enough signals converged." Today's morning fire of HM-AN2.C executed 4 BUY trades that passed this filter. The card's "4 of N" threshold language is consistent with the runtime behavior.

Three gaps between card and code:
1. The card describes vote *families* (momentum, volume, breadth, options, macro). The actual confidence score isn't decomposed by family — it's a single number from each Signal Center agent. So when Bridge renders "4 of N strategies agree" on a trade card, the underlying truth is closer to "4 agents independently scored above 70" without family-level diversity tracking.
2. Independence claim ("Strategies that share underlying inputs aren't independent") is enforced by which agents Captain enables, not by the convergence math. If Captain enables 4 RSI-based agents, the system happily reports 4-of-4 convergence on what's really 1 signal in 4 wrappers. File as `HM-SIGNAL-FAMILY-INDEPENDENCE-METRIC` for future work.
3. The lag warning ("by the time 4 signals line up, the easy part of the move has often already happened") is observable in trade outcomes — could be quantified by measuring entry-to-first-target velocity on HM-AN2.C trades vs single-source trades. Future research ticket.

Card is honest. Code is consistent with the card at a high level, less so at the family-decomposition level. Production-ready as written.

---

## 🛑 Stop Loss

**XO review:**
The cardinal-sin paragraph ("Moving a stop further away mid-trade is the cardinal sin") is the right emotional weight for a teaching card. This is also the card most likely to save real money in Captain's actual broker accounts (Schwab primary), not just paper. The position-sizing-derived-from-stop framing is exactly correct and underappreciated — most retail education has it backwards. The "gap risk and slippage are real" gotcha should reference Stop Loss orders vs Stop Limit orders explicitly; the difference is critical and most learners conflate them.

**Scotty review:**
Two things tight:
1. The card says **"Scotty: executor enforces every stop programmatically. No mid-session overrides allowed."** That's a promise. Audit needed: does `paper_trader.py` honor a stop_loss field set at entry, automatically firing the close when crossed? If the answer is "stop is logged but enforcement is `exit_manager`'s job which runs every 5 min" — that's not "programmatic" in the way a retail trader would understand it. A 5-min poll cycle means a 5-min worst-case window where the stop is missed. Fast-moving names can trade through a 5-min gap routinely. File as `HM-STOP-ENFORCEMENT-LATENCY-AUDIT`.
2. Slippage modeling. Paper Alpaca fills at theoretical mid; real Schwab fills somewhere worse, sometimes much worse on stop orders triggered into low-liquidity windows. The teaching card should not promise that paper performance translates 1:1 to real-account results. This isn't the card's job to fix, but adjacent: the Bridge dashboard should somewhere display "this is paper math, real broker fills will differ" when showing P&L from paper trades.

The card is the most production-impactful in the batch. Worth a follow-up review pass after the enforcement audit lands.

---

## 🏁 Take Profit

**XO review:**
Honest card. The "let winners run sounds wise and breaks portfolios" line is the right pushback against the most cliché bad advice in retail trading. The structural distinction in gotchas ("defined-risk credit spreads → exit early; directional debit positions → scale out") is the right calibration — most education collapses these into one rule, which is wrong. Production rendering note: when Bridge shows a take-profit target as a price level on a chart, the structural tier (1R/2R/runner) should be visually distinguishable — not just three points on a line.

**Scotty review:**
The card describes three-tier scaling ("1/3 at 1R, 1/3 at 2R, hold 1/3 with trailing stop"). Paper Alpaca + `paper_trader.py` currently sells in whole-contract or whole-share blocks. Verify: does the codebase support fractional-position partial exits? If not, three-tier scaling on a 1-contract spread is mathematically impossible (you can't sell 1/3 of 1 contract). The card should either:
- Acknowledge that three-tier requires 3+ contracts/shares per position, OR
- Drop the three-tier prescription for the standard OllieTrades position size

Most likely fix: card is fine, dashboard sizing logic should warn Captain when a chosen position size precludes the three-tier exit plan ("with 1 contract, only single-target exit available").

Two more pieces:
1. The "0DTE: 50% by 1pm ET" rule needs Scotty enforcement (same as Time Stop's 1pm rule). If 0DTE exit-at-50% is programmatic, surface it in trade logs. If manual, the card overpromises.
2. "Closing at 100% of max profit only happens at expiration — and you eat assignment / pin risk getting there." This is the exact reason auto-exit logic should ALWAYS close credit spreads before expiration, not pin them. Audit the close-out automation — does `exit_manager` force-close credit spreads at, say, 2 DTE regardless of P&L state? If not, Captain has assignment risk hiding in the queue every Friday afternoon.

---

## 📏 ATR

**XO review:**
Foundational card. Every other concept in the batch depends on ATR for sizing (Stop Loss, Time Stop indirectly, Bear Call Spread "within 1 ATR of resistance"). The normalize-by-price gotcha is critical and underappreciated. The TSLA vs MSFT example concrete-vs-abstract pairing is exactly the pedagogical shape that works for a learning audience. Strong cross-link candidate: every other card should link back to ATR when sizing math is mentioned. Production note: when Bridge shows an ATR reading, also show the ATR/price ratio next to it (the actually-useful normalized number).

**Scotty review:**
ATR is the one concept in the batch that's genuinely fully implemented. The scanner uses ATR. The Quality Gate uses ATR (verify). The exit_manager uses ATR for stop placement (verify). The trade payload schema (handoff section 6) has `atr_14` as an explicit field. Good production maturity.

Three deeper observations:
1. **ATR period.** Card uses ATR(14) as default. Verify the scanner is using 14, not 20. If different periods are used in different code paths, the dashboard's displayed ATR may not match the ATR the scanner gated on. Audit and standardize.
2. **Earnings caveat.** Card correctly notes "around earnings or scheduled news catalysts, the expected move is more useful than ATR." OllieTrades has earnings-blackout logic in `options_selector.py` (per memory) but I don't know if the *expected move* is computed from the options chain anywhere. Polygon options data has the inputs. File as `HM-EXPECTED-MOVE-COMPUTE`.
3. **Compression-precedes-expansion gotcha.** Quantifiable: a "low ATR triggering vol-expansion alert" feature could be a useful tile on the Bridge dashboard. Not a fix to the card — a future feature surfaced by the gotcha.

ATR is the card with the smallest gap between teaching content and code reality. Use this as the standard for production-readiness of future concept cards: if the gap is bigger than ATR's, the card needs a "where this comes from in the system" addendum before it can be ground-truth.

---

## Cross-cutting observations (XO + Scotty)

**XO — three patterns across the batch:**

1. **"Scotty enforces this programmatically" appears as a teaching trope in three cards (Stop Loss, Time Stop, Take Profit).** That's a promise. Either the codebase honors it for every case mentioned, or those claims need adjustment. The XO recommends a single audit ticket: `HM-EXECUTOR-CLAIMS-AUDIT` that grounds every "Scotty enforces" statement against a specific paper_trader/exit_manager code path. Outputs: either greenlight per-card OR list of gaps to fill OR card revisions.

2. **Convergence is the hub.** Multiple cards reference it; it references many. When the Concept Drawer renders, Convergence should be the most visually prominent cross-link target. Consider making Convergence the default landing card if no specific concept is open.

3. **Gamma Flip is the outlier.** Every other concept maps to something live or near-live in the codebase. Gamma Flip would require subscription, compute, or research before the dashboard can render real values. Recommend explicit "Coming Soon" treatment on this card until backend lands.

**Scotty — three patterns across the batch:**

1. **The trade payload schema (handoff section 6) names fields that don't exist yet.** Specifically: `iv_rank`, `gamma_state`, `gamma_flip_level`, `relative_volume`. Each maps to a card that prescribes behavior. The schema is aspirational without backend producing these fields. Recommend a `HM-CONCEPT-PAYLOAD-GAPS` ticket inventorying which schema fields are live, which are placeholders, which require new infrastructure.

2. **Real-broker vs paper-broker discrepancies are uniform across cards.** Every card describing exit rules, slippage, and assignment was written for a clean execution model that paper Alpaca approximates and real Schwab does not. The Bridge dashboard's teaching surface should somewhere display: "this guidance assumes ideal execution; real broker behavior varies." One short concept card titled "Paper vs Real Execution" would carry the disclaimer for the whole batch.

3. **Concept Card writing is the easy part. Production-ready backing is the hard part.** Of these 10 cards, 4–5 (ATR, Convergence, Bear/Bull Spreads, Stop Loss in principle) are fully or mostly backed by current code. 3–4 (IV Rank, Time Stop, Take Profit, MACD persistence) need real engineering tickets to surface their numbers. 1 (Gamma Flip) requires external subscription or substantial research. The dashboard rollout should sequence card visibility to match backend readiness — don't ship a card whose numbers the system can't compute.

---

_End of XO + Scotty reviews, batch 1._

---
---

# Part 2 — Icon Set Reviews (CharacterIcons.tsx + ConceptGlyphs.tsx)

The Chrome session also shipped two TSX files with full SVG source. These are reviewed as a pair.

## CharacterIcons.tsx — 12 character portraits

**XO review:**

This is the right file. Twelve avatars at 64×64 source viewBox, multi-color filled illustration style, accessible via inline `<title>` and `role="img"`. The header comment commits to the right behaviors: degrade to colored circle + initial below 20px (which is what the current dashboard's leaderboard already does — good continuity), and theme rotation maps `strategy_id → persona`. The component pattern is mature: `IconProps` interface, `SVG_DEFAULTS` shared constant, named exports plus a `characterIcons` registry plus a `CharacterIconById` lookup component.

The author resolved one subtle design tension: avatars are *evocative* of Trek crew but original characters, not direct portraits. Two reasons that matters — (a) the IP question goes away (no copyrighted Trek likenesses on a public dashboard), (b) theme-rotation to MASH or Dallas mid-session still feels like the same component, just different roster.

Production concern: the `--char-*` CSS variables referenced in the comment exist in the design handoff section 7. But Bridge's production index.html uses its own established color set for character circles (gold-S, teal-C, blue-L, orange-G, silver-W, yellow-T, purple-T, green-D, red-K — per March 2026 session memory). Two palettes need to reconcile before deployment. Either:
- Adopt section-7 `--char-*` values and update the existing leaderboard circles to match, OR
- Map the existing leaderboard colors into the `--char-*` slots so the icon file matches what's already on screen

The second is lower risk (no visible regression to Captain's daily view).

Accessibility note: the `<title>` element inside each `<svg>` is what screen readers announce. With `role="img"` set on the wrapper, this is the standard pattern. Verified the author got it right; not all SVG icon libraries do.

**Scotty review:**

This is React/TypeScript. Bridge is vanilla HTML/JS in a single 35,530-line monolith served via FileResponse. The TSX file does NOT drop into `dashboard/static/icons/` and immediately work.

Three porting paths, in increasing order of effort:

1. **Inline the SVG bodies directly into index.html.** The JSX inside each component is essentially raw SVG — `<svg viewBox="0 0 64 64">...children...</svg>`. Extract those bodies (without React wrapping), embed as `<svg>` tags inline wherever an avatar renders. Add a small JS helper `renderCharacterIcon(charId, size)` that returns the right SVG markup string for a given character ID. Bundle size impact: ~22 KB of SVG inline once per page load, no React overhead. Estimated work: 2-3 hours including the registry helper.

2. **Generate `.svg` files from the TSX source, load via `<img>` or `<use>` from sprite.** Most accessible to vanilla. Each character becomes one `static/icons/character-{id}.svg`. The leaderboard renders `<img src="/static/icons/character-worf.svg" width="32" height="32" alt="Worf">`. Trade-off: loses inline color theming (can't pass `currentColor` via image src), so character color becomes baked into the SVG file rather than CSS-controlled. Estimated work: 3-4 hours including build script + spritesheet generation.

3. **Adopt a build pipeline and React.** Set up Vite + React, migrate the entire dashboard. Multi-week refactor. Not on the table for this batch.

Recommended path: **Option 1** (inline SVG bodies). Preserves the `currentColor` and CSS-variable theming, drops cleanly into existing inline-template patterns, doesn't introduce build tooling.

Practical ticket: `HM-CHAR-ICONS-VANILLA-PORT` — extract SVG bodies from `CharacterIcons.tsx`, write `dashboard/static/character-icons.js` registry helper, replace existing emoji or colored-circle avatars in index.html at the leaderboard + War Room post avatars + Crew Read regions of the Ticker Command Card (once that ships). Scope: ~3 hours including verification at 24/32/48 px renders.

Audit needed: which existing dashboard sections currently use which avatar pattern? Some leaderboard rows use letter circles; some War Room posts use emojis. The vanilla port should unify these.

## ConceptGlyphs.tsx — 14 concept glyphs

**XO review:**

Companion file. 24×24 source viewBox, line-art (stroke-only with `currentColor`), inherits surrounding text color. Render size 16 px is the spec — inline glyphs within paragraph text, not standalone badges. The cardinal-rule callout in the header comment is important: glyphs identify concepts (shape only), state shapes (▲ ▼ ◆ ● ✦) carry stance. Separation of concerns enforced at the component level.

Coverage of the 14 glyphs lines up with the trade-signal payload schema in handoff section 6: bear-call, bull-put, iron-condor, covered-call, cash-secured-put, rsi, relative-volume, atr, ema-ribbon, iv-rank, gamma-flip, time-stop, stop-loss, take-profit. That's the working concept_id vocabulary. If batch-2 concept cards add (e.g.) MACD, breadth, fear-greed, expected-move, the glyph registry needs corresponding additions — and the convention is to extend, not modify, so existing usage stays stable.

Same palette-reconciliation concern as the character file: glyphs inherit `currentColor` so theming is automatic, but the surrounding text color must match the Bridge's existing CSS variables. Verify the dashboard's `--text-primary` and the design handoff's `--text-primary` resolve to the same hex value.

Accessibility: same pattern as characters — inline `<title>` + `role="img"` + override via prop. Standard, correct.

One missing piece: there's no MACD glyph in this Tier 2 set, but the concept-cards batch 1 includes a full MACD card. Either:
- Add a MACD glyph in batch 2 of icons, OR
- The MACD card renders without a glyph (header emoji placeholder only)

Not a blocker — flag it for the next icon batch.

**Scotty review:**

Same porting path as CharacterIcons. Option 1 (inline SVG) is even cleaner here because the glyphs are stroke-only line art — the SVG bodies are smaller (~9 KB total file vs 22 KB for characters), and they have zero color baked in. Inline drops fluidly into existing CSS.

Practical implementation:
```javascript
// dashboard/static/concept-glyphs.js
const CONCEPT_GLYPHS = {
  'rsi': '<path d="..." stroke="currentColor" .../>',
  'macd': '<path d="..." .../>',  // when added
  // ... 14 entries
};

function renderConceptGlyph(conceptId, size = 16) {
  const inner = CONCEPT_GLYPHS[conceptId];
  if (!inner) return '';
  return `<svg width="${size}" height="${size}" viewBox="0 0 24 24" 
          fill="none" role="img" aria-label="${conceptId}">${inner}</svg>`;
}
```

Estimated work: ~1.5 hours (smaller than character port due to line-art simplicity + no color logic).

Where these get used in production:
1. **Concept Cards rendering** (when Bridge dashboard adds the Concept Drawer per handoff section 3) — glyph appears at top of each card as the visual anchor
2. **Walk-back narration inline** — glyph appears inline next to italicized concept terms in the section 3 Walk-back layer
3. **Trade card payloads** — when a trade displays `concepts_referenced: ["rsi", "relative_volume"]`, render the corresponding glyphs as compact tags

None of (1)/(2)/(3) currently exist on the dashboard. They're future surfaces. The glyph file is ready ahead of demand — appropriate sequencing.

Database concern: nothing currently stores `concept_id` references per trade. The trade_signals table records reasoning text but not a structured concepts_referenced array. Adding it is a schema migration — file as `HM-TRADE-CONCEPTS-SCHEMA`. Until that lands, glyph rendering on existing trade cards has to parse free-text reasoning to detect concept mentions, which is brittle. Recommend shipping glyph file + helper now (cost ~1.5 hours, no risk), then doing the schema migration before connecting them to live trade data.

---

# Part 3 — Ticker Command Card Spec Review

The Chrome session went beyond section 14's kickoff prompt and produced an implementation contract for the Ticker Command Card. 418 lines, 18 sections. This is a real engineering contract, not a sketch.

## XO review

**On the spec quality itself:**

This is the strongest design document in the batch. Sections 4 (props), 5 (data flow), 12 (empty states), 13 (accessibility), 14 (keyboard map), 17 (out of scope) — those six sections collectively form a tight, complete engineering contract. The empty-states enumeration in section 12 is the kind of pre-thought-through completeness that prevents the "we forgot the loading state" reviews three sprints in. The keyboard map is unusually thorough for a single component — `/` as the search focus shortcut, `←`/`→` for tab cycling, `↑`/`↓` for Crew Read navigation, `?` for help overlay. That's professional-grade accessibility thinking.

**On the strategic priorities:**

Section 17 ("Out of scope v1") is the smartest section in the doc. Read it carefully:
- Order submission is deferred to Phase 2 (read-only preview in v1)
- Custom multi-leg options builder deferred (only crew-endorsed structures in v1)
- Embedded chart deferred (link to TradingView in v1)
- Mobile layout deferred

This is correctly sequenced. Captain ships v1 read-only, validates the consolidated-view UX, then phases in order submission against Scotty's executor with proper risk gates. The alternative — building order submission into v1 — would create a paper-account button on a dashboard component that hasn't proven the UX yet. Wrong order.

**On the regions:**

Region 4 (Crew Read) is the pedagogical centerpiece. The stance vocabulary table in section 9 — `BUY`, `SELL`, `ADD`, `TRIM`, `HOLD`, `WATCH`, `CAUTION`, `STAND_DOWN`, `NOT_APPLICABLE`, `NEUTRAL` — is precise and properly differentiated. The sort order rules (conviction first, then watch, then hold, then neutral-fallback) ensure the most actionable rows surface first. The empty-state fallback ("No active signals... + re-scan button") is a small but important UX courtesy.

Region 6 (Learn drawer) closes the teaching loop. The two-sentence explanation pulled from a `character.id × strategy_id` registry is the elegant implementation of "every trade teaches." The cycling behavior (`<` / `>` to walk through multiple characters' takes on the same ticker) is the missing piece in most retail platforms — they show what one analyst thinks, not the full debate.

**Open questions in section 18 that deserve attention before build:**
- "Sizing calc fleet cash ceiling source" — the calculator UI depends on this; until it's wired, the calculator is detached from reality
- "Tab state in URL?" — small but matters for Slack pastes. Recommend yes, with a fallback (state lives in URL hash, defaults if absent)
- "Real-position broker resolution" — aggregate-with-breakdown is the right answer; Captain has positions across Schwab + Webull + Alpaca paper, and conflating them silently would hurt

**One missing topic:**

Telemetry. When the Ticker Command Card ships, which interactions get logged for product analysis? Most important to track: tab switches (does anyone use the Alerts tab?), Crew Read row clicks (which characters get attention?), drawer expansions (does Learn matter?). Without this, v2 priorities will be guesses. Recommend adding a section 19 — "Telemetry events emitted by this card."

## Scotty review

**On the backend reality — section 5 data flow:**

The spec lists six hooks: `useQuote`, `useKeyLevels`, `usePosition`, `useCrewStances`, `useOptionsChain`, `useAlerts`. Each maps to backend service. Let me ground-truth each one against current OllieTrades state (today, 2026-05-15, post-all-PRs):

| Hook | Backend reality | Status |
|------|-----------------|--------|
| `useQuote` | Polygon Developer ($79/mo) — primary. Alpaca SDK fallback. yfinance second fallback. The 5s/60s polling cadence is achievable on Polygon. | **READY** |
| `useKeyLevels` | Computed locally from candles. MAs, ATR, 52w range. We have candle data via Polygon + DB. | **READY** |
| `usePosition` | Schwab CSV pipeline (HM-AT-β shipped). `real_holdings.json` reads. Alpaca SDK for paper. Webull/IBKR monitor-only (memory). | **READY** for Schwab + Alpaca paper. Webull integration is monitor-only (per memory) so positions might not roll up cleanly. |
| `useCrewStances` | `signals.db` direct read — 617MB. 30s cadence. Reads scorecard + outcome tables. | **READY** in principle, but spec says "all 12 characters' current evaluation" — that requires every character to evaluate every ticker on a 30s cycle. Reality: characters evaluate the watchlist, not arbitrary tickers. **GAP** when user types in an off-watchlist ticker. |
| `useOptionsChain` | Polygon Options Starter — but per recent memory we're actually on the **Stocks + Options Starter bundle**. Lazy-load on tab activate is correct architecturally. | **READY** (verify the bundle includes the options endpoints needed) |
| `useAlerts` | Internal alerts service at port 8080 — same FastAPI app as the dashboard. Persistence layer for alerts not currently audited. Existing ntfy fanout via Dr. Crusher healthcheck. | **PARTIAL** — alerts persistence schema may need work. File as `HM-ALERTS-SCHEMA-AUDIT`. |

The `useCrewStances` gap is the largest. The spec implicitly assumes characters can answer "what do you think of TSLA right now?" on demand — but the architecture is event-driven scanning, not on-demand evaluation. Off-watchlist tickers won't have current stances. Two ways to handle:

1. **Honest empty state.** When ticker is off-watchlist, the Crew Read region renders "No characters have evaluated this name. Last scan included $watchlist_size tickers." with a "Run on-demand evaluation" button that triggers a one-off cycle.
2. **Background fetch on card open.** Card mount triggers an async crew evaluation against the typed ticker. Heavyweight (12 LLM calls), would cost user-perceptible latency.

Recommend (1). Captain learns the system's actual boundaries. Filed as `HM-TICKER-CARD-OFF-WATCHLIST-UX`.

**On performance (section 15):**

The performance claims are achievable but assume infrastructure that needs verification:

- **5-second cache on quotes.** Polygon allows this if request volume stays inside the plan limits. Need to model worst case: if 12 dashboard users have the card open on different tickers, that's 12 × every-5-seconds = 144 quote requests per minute. Polygon Developer is ample for that. But if user opens 12 tabs simultaneously, gets worse.
- **30-second cache on crew stances.** `signals.db` is 617 MB. SELECT on indexed columns should be sub-100ms, well within budget. But signals.db is also the write target for the Signal Center service. Heavy concurrent read traffic from many dashboard sessions + writes from the trader could create contention. File as `HM-SIGNALS-DB-READ-CONCURRENCY-MODEL`.
- **150 ms debounce on symbol input.** Sensible. Standard practice.
- **Memoization per region.** The spec is correct that a price update shouldn't re-render the Crew Read. In React this is trivial; in vanilla HTML/JS it requires careful state-update routing. The vanilla port needs to maintain this separation manually.

**On services touched (section 16):**

This section explicitly names internal services and external APIs. Worth a careful re-read:

```
useQuote — Polygon primary, Alpaca fallback, yfinance second fallback
```

Today's HM-SLOW-FUNDAMENTALS work (PR #8) addressed yfinance ETF skipping. So this hook would benefit from that fix. The Ticker Card is exactly the kind of caller that would hit the slow path otherwise.

```
useCrewStances — signals.db direct read
```

This is the same direct-SQLite pattern that bypassed our Signal Center auth gate today (HM-AN bridge auth blocker discussion). Dashboard → 127.0.0.1:9000 wasn't actually blocked. The Ticker Card can either:
- Bypass Signal Center, read signals.db directly (faster, no auth, but creates two write/read paths)
- Use the HM-AN Phase 1 Tier-1 proxies once those ship (slower by ~10ms, but unifies access)

Recommend the latter. The Tier-1 proxy list includes `/api/signals/scorecard` and `/api/signals/outcomes` — exactly what the Crew Read needs. Sequence: HM-AN Phase 1 ships → Ticker Card consumes proxies → no direct DB access from dashboard.

**On vanilla port effort:**

This is the biggest port of the four files. The spec describes 6 sub-components, 3 tabs in section 10, complex sort logic in section 9, drawer state, debounce, memoization. In vanilla:

- 6 regions = 6 functions returning HTML strings (or DOM nodes)
- 3 tabs = standard tab pattern with show/hide
- Sort logic in section 9 = client-side JS sort with the stance ordering enum hardcoded
- Drawer = `<details>`/`<summary>` (built-in HTML), or click-toggle on a div
- Debounce = `setTimeout` clear-pattern
- Memoization = "track last rendered values per region, skip update if unchanged"

None of it is hard. All of it is more code than React would generate. Estimated effort for full vanilla port: ~12-16 hours of focused work, split across:
- 3 hours: Region 1, 2, 3 (Facts, Position, Key Levels) — mostly display
- 3 hours: Region 4 (Crew Read) — sort logic + CharacterChip rendering
- 4 hours: Region 5 (Action tabs) — sizing calculator math + options chain rendering
- 2 hours: Region 6 (Learn drawer) + registry
- 4 hours: Integration, debounce, memoization, accessibility QA

That's 2-3 sessions. Worth doing in stages: ship Regions 1+3+4 first as a read-only ticker view, validate, then phase in Action tabs.

Filed as the implementation epic: `HM-TICKER-CARD-V1`.

---

# Part 4 — Strategic Summary

## What the Chrome session shipped (cumulative)

```
1. ollietrades-bridge-handoff.md     14-section design system spec
2. concept-cards-batch-1.md          10 Concept Cards (≈350 words each)
3. CharacterIcons.tsx                12 character avatars
4. ConceptGlyphs.tsx                 14 concept glyphs
5. ticker-command-card-spec.md       Implementation contract (18 sections)

Total: ~80 KB of design artifacts. Production-quality.
```

## The decision that frames everything else

The handoff and TSX files assume React/TypeScript/Tailwind. Bridge is vanilla HTML/CSS/JS in a single monolith.

**Three paths Captain has to pick from:**

| Path | Cost | Risk | Outcome |
|------|------|------|---------|
| A. Vanilla port (recommended) | ~25 hours across 4-5 sessions | LOW — drop-in to existing patterns | Design system lands in production-ready form; no build tooling change |
| B. React migration | Multi-week | HIGH — touches 35,530 LOC | Modern stack, easier future development, big disruption |
| C. Hybrid (Bridge stays vanilla, new components in React) | Indeterminate | MED — two stacks to maintain | Worst of both worlds; not recommended |

Path A is the right call. The handoff's React framing was the Chrome session author's natural mental model, not a hard requirement. The actual design (CSS variables, SVG inline, ARIA patterns, three-channel state encoding) is framework-agnostic.

## Recommended sequencing for Bridge dashboard rollout

```
SESSION 1 — Foundation (~6 hours)
  → Reconcile color palettes (existing dashboard ↔ handoff section 7)
  → Inline character icons into static/character-icons.js (~3 hours)
  → Inline concept glyphs into static/concept-glyphs.js (~1.5 hours)
  → Replace emoji avatars in War Room + leaderboard with inlined SVG (~1.5 hours)
  → Single PR, low risk, immediate visible upgrade

SESSION 2 — Concept Drawer (~5 hours)
  → Build static/concept-cards/ directory with one .md per concept
  → Markdown → HTML rendering (use marked.js or similar lightweight lib)
  → Concept Drawer modal that opens beside trade cards
  → Glyph-prefixed concept links in trade reasoning text
  → Single PR, medium risk

SESSION 3 — Ticker Card v1 read-only (~16 hours, multi-day)
  → Regions 1, 2, 3 (Facts, Position, Key Levels) — Session 3a
  → Region 4 (Crew Read) wired to HM-AN Phase 1 proxies — Session 3b  
  → Regions 5, 6 (Action tabs read-only, Learn drawer) — Session 3c
  → Three PRs across multiple days, high attention required

SESSION 4+ — Phase 2
  → Order submission against Scotty's executor (with the right risk gates)
  → Embedded chart
  → Mobile layout
  → Theme rotation (Wars/MASH/Dallas)
  → Telemetry
```

## Dependencies clarified

Before Session 3 (Ticker Card) can ship cleanly:
- HM-AN Phase 1 Tier-1 proxies need to be live (decision shipped today, code TODO)
- HM-IVR-COMPUTE — IV Rank computation backend (filed in Concept Cards review)
- HM-TRADE-CONCEPTS-SCHEMA — structured `concepts_referenced` field per trade

Session 1 and 2 have no backend dependencies — pure frontend work, can start any session.

## What I recommend right now

```
Tonight: stand down per current plan. Watch NTFY arrives, log it, move on.
Monday morning: validate tomorrow's HM-WAR-ROOM-INIT-FIX worked at open.
Then ship-track:
  Session-1 Bridge work can run parallel to backend ticket queue 
  (icons + glyphs are fully independent of any trader-side work).
```

The Bridge work and the trader work don't compete for the same files or services. The character-icons port doesn't touch any of the 7 PRs we shipped today. Two parallel tracks possible.

---

_End of consolidated reviews — Concept Cards Batch 1, Icon Set, Ticker Command Card spec._
