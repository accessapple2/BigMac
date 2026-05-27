# OllieTrades Bridge — Design & Trading Handoff
_Compiled from a Claude browser-extension session. Drop into Claude Desktop to continue the work._

---

## 0. Context

The Bridge dashboard at `bridge.ollietrades.com` is a multi-agent trading cockpit. Each AI agent is themed as a Star Trek character whose persona corresponds to a trading strategy (Worf = Bear Spreads, Dr. McCoy = Crisis/oversold rescue, Spock = Pure Quant, Troi = Risk/Sentiment Read, Kirk = Captain's Plan, Chekov = Convergence Scanner, Dax = Dividend Value, Scotty = Engineering/Execution, Riker = Daily Briefing, Data = Pure Logic, Uhura = Comms, Ollie = brand/fleet leader). Theme is intentionally pedagogical: the character is a **mnemonic** for the strategy. Theme is rotatable (Star Wars, MASH, Dallas) via a theme-pack map; characters change, strategies do not.

The user is color-blind and wants:
- Clear, easy-to-read, easy-to-execute UI
- Less Enterprise set-dressing, more functional surface
- Realistic / higher-fidelity character icons (replacing emojis)
- Color-blind-safe palette
- A ticker-centric workflow ("take a ticker and know what to do from there")
- Every trade should teach: a three-depth Explain layer (headline → walk-back → concept primers)

---

## 1. The Character Chip pattern (used everywhere a character is named)

Four-part bundle, same order every time:
1. **Persona handle** (Worf · Dr. McCoy · Spock)
2. **One-line strategy claim** (e.g., "Bear Call Spreads in down-trending names")
3. **When-to-use trigger** (e.g., VIX > 20, sector below 50MA, RSI > 70)
4. **Current track record** (last 20 trades: 12-8, +2.4R, 60% win)

Implementation: standardize one `<CharacterChip />` component used in the Comms feed, Leaderboard, Ticker Command Card, walk-backs, tooltips. Same look, every surface. Repetition is the teacher.

### Theme rotation
JSON map: `strategy_id → { persona_name, icon, color, one_liner }`. Swap Trek → Wars → MASH → Dallas without changing strategy logic. Setting in Ready Room: Trek / Wars / MASH / Dallas / "No theme — strategies only."

---

## 2. The Ticker Command Card (search a symbol → know what to do)

Single consolidated view. Layout, top to bottom:

**Facts (5-second scan):** Symbol · last · day % · day $ · volume vs avg · earnings date · sector. Position (if any): qty, avg cost, unrealized P&L, % of portfolio, days held. Key levels: 20/50/200 MA, ATR, 52w range.

**Crew read (the mnemonic payoff):** one row per character that has a stance on this name today, each rendered as a Character Chip with stance + reason:

```
SPOCK   · Quant       · ADD       · Score 80 · grade A · prob 80%
WORF    · Bear        · HOLD      · No bearish setup
McCOY   · Crisis      · —         · Not oversold (RSI 58)
CHEKOV  · Convergence · BUY       · 4 strategies agree
TROI    · Risk        · CAUTION   · IV elevated, mixed breadth
KIRK    · Captain     · HOLD      · per current plan
```

**Action layer (three tabs, side-by-side):**
- **Stock** — sizing calculator respecting fleet cash ceiling, ATR-based stop, character-playbook target
- **Options** — chain pre-filtered to structures the crew currently endorses, each labeled by the endorsing character
- **Alerts** — one-click price / IV / news alerts at the levels the crew is watching

**Learn (collapsible "Why?"):** two-sentence explanation of the active character's strategy and why it fired (or didn't) on this ticker.

---

## 3. The three-depth "Explain This Trade" layer

Every trade card exposes three depths:

### Depth 1 — Headline (always visible, one sentence)
Auto-assembled from the signal payload. Example:
> Worf opened a Bear Call Spread on QQQ at $612/$615 — RSI overbought into resistance, volume confirming the rejection.

### Depth 2 — Walk-back (first click, "Why this trade?")
Five fixed sections, same order every time:
1. **The Setup** — preconditions (regime, structural levels)
2. **The Triggers** — the catalysts that crossed thresholds (each linked to a Concept Card)
3. **The Structure** — why this specific construction (DTE, strikes, credit vs debit)
4. **The Risk** — max loss, hard stop, time stop, profit target
5. **The Crew's Disagreement** — who else evaluated this, what they said, why

### Depth 3 — Concept drawer (deeper click)
Every technical term in Depth 2 is a clickable Concept Card. Drawer opens beside the trade so concrete + abstract sit side by side.

---

## 4. Concept Card shape (~350 words each)

Five sections, repeated for every concept (RSI, MACD, Relative Volume, ATR, EMA Ribbon, Bollinger Bands, Gap & Go, Pullback to SMA, Volume Dry-Up, Trend Resumption, Breakout Volume, Convergence, IV Rank, IV Crush, Delta, Gamma, Gamma Flip, Put Wall / Call Wall, GEX, Bear Call Spread, Bull Put Spread, Iron Condor, Covered Call, Cash-Secured Put, 0DTE, VIX, Fear & Greed, Market Breadth, King Node, Time Stop, Trail Stop, Take-Profit Tiers, Stop-Loss, Position Sizing, Win Rate, Profit Factor, Expectancy):

1. **The one-liner** — what it is in a sentence
2. **How it's computed** — mechanics, briefly
3. **What the numbers mean** — thresholds and zones
4. **How the crew uses it** — named to characters who rely on it
5. **The gotchas** — when it lies

### Reference: full RSI Concept Card

**📈 RSI — Relative Strength Index**
*A momentum gauge that asks: how stretched is recent buying or selling?*

**One-liner:** A 0-to-100 number summarizing whether recent moves have been up-dominated or down-dominated. High = buyers crowded; low = sellers crowded.

**Computed:** Over a 14-bar lookback, the indicator compares average up-close size to average down-close size: `RSI = 100 − (100 / (1 + avg_gain / avg_loss))`. Default 14 bars ≈ three trading weeks on a daily chart.

**Numbers:**
- > 70 = overbought (pause/pullback often follows)
- 50 = neutral
- < 30 = oversold (bounce often follows)
- Sustained > 80 or < 20 = trending (do NOT fade)

**How the crew uses RSI:**
- **Worf (Bear Spreads):** > 70 + resistance + rising relative volume → bear call spread.
- **McCoy (Crisis Doctor):** < 30 + capitulation volume → cash-secured put on quality names.
- **Spock (Pure Quant):** ~15% weight in composite momentum score; never standalone.
- **Chekov (Convergence):** RSI extreme counts as one vote; needs ≥ 4 votes to fire.

**Gotchas:** RSI lies in strong trends. Always pair with structure (level, volume, trend filter). Timeframe matters — 5-min RSI 78 ≠ daily RSI 78.

---

## 5. Reference walk-back — Worf shorts QQQ

> **WORF · BEAR CALL SPREAD · QQQ $612 / $615 · 14 DTE**
> *Worf shorted QQQ into resistance — RSI overbought, volume confirming the rejection.*
> Credit $0.87 · Max loss $213 · Filled 10:47 ET

**The setup** — Regime NEUTRAL. QQQ has tagged $618 resistance three times in five sessions. ATR(14) = $4.20. Price at $613.80 is inside Worf's strike zone.

**The triggers** —
- RSI(14) = **78** (threshold 70) ✓ [Concept: RSI]
- Relative volume = **1.8×** (threshold 1.5×) ✓ [Concept: Relative Volume]
- IV rank = **42** (Worf prefers 30–60 band) ✓ [Concept: IV Rank]

**The structure** — Defined-risk bear call spread, 14 DTE. Selling premium because IV is elevated but not extreme; long $615 caps loss. [Concept: Bear Call Spread, Theta]

**The risk** —
- Max loss $213 above $615
- Hard stop: QQQ reclaims $614 on rising volume
- Time stop: close at 7 DTE if unresolved [Concept: Time Stop, Gamma]
- Target: $0.20 debit-to-close (~75% credit captured)

**The crew's read** —
- Spock NEUTRAL (momentum score +12; needs ≤ -20 for bearish lean)
- Troi CAUTION (Fear & Greed 84 = crowded short risk)
- McCoy NOT_APPLICABLE (oversold rescues only)
- Chekov STAND_DOWN (2 of 4 strategies bearish; needs 4)

Worf is the only character firing → single-conviction trade → sized at 50% of standard allocation.

---

## 6. Trade signal payload schema (auto-renders all three depths)

```json
{
  "trade_id": "t_2026_05_14_10_47_worf_qqq_001",
  "timestamp_utc": "2026-05-14T14:47:12Z",
  "timestamp_market": "2026-05-14T10:47:12-04:00",
  "character": {
    "id": "worf",
    "display_name": "Worf",
    "role_label": "Bear Spreads",
    "model": "ollama/qwen3:8b",
    "recent_record": { "trades": 20, "wins": 12, "losses": 8, "win_rate": 0.60, "avg_r": 2.4 }
  },
  "strategy": {
    "id": "bear_call_overbought_rejection",
    "display_name": "Overbought Rejection — Bear Call",
    "family": "defined_risk_credit_spread",
    "thesis": "Short premium into a confirmed rejection at resistance when IV is elevated but not extreme."
  },
  "instrument": {
    "ticker": "QQQ",
    "underlying_price": 613.80,
    "structure": "bear_call_spread",
    "legs": [
      { "side": "short", "type": "call", "strike": 612, "expiration": "2026-05-28", "qty": 1 },
      { "side": "long",  "type": "call", "strike": 615, "expiration": "2026-05-28", "qty": 1 }
    ],
    "credit_debit": "credit",
    "net_premium": 0.87,
    "max_profit": 87,
    "max_loss": 213,
    "dte": 14,
    "breakeven": 612.87
  },
  "regime": {
    "market": "NEUTRAL",
    "vix": 17.3,
    "fear_greed": 84,
    "gamma_state": "STABLE",
    "size_factor": 0.5,
    "size_factor_reason": "single_conviction"
  },
  "setup_conditions": [
    { "id": "resistance_within_1_atr",
      "description": "Underlying within 1 ATR of multi-touch resistance",
      "evaluated": { "resistance_level": 618.0, "atr_14": 4.20, "distance_atrs": 1.00 },
      "passed": true },
    { "id": "iv_rank_band",
      "description": "IV rank inside 30-60 preference band",
      "evaluated": { "iv_rank": 42 },
      "passed": true }
  ],
  "triggers": [
    { "id": "rsi_overbought", "concept_id": "rsi",
      "indicator": "RSI(14)", "value": 78, "threshold": 70, "comparator": ">",
      "status": "crossed_at_10_47_ET", "passed": true },
    { "id": "relative_volume_confirmation", "concept_id": "relative_volume",
      "indicator": "rel_vol", "value": 1.8, "threshold": 1.5, "comparator": ">",
      "status": "crossed_at_10_47_ET", "passed": true }
  ],
  "risk_plan": {
    "max_loss_dollars": 213,
    "hard_stop": { "rule": "underlying_reclaims", "level": 614.0, "qualifier": "on_rising_volume" },
    "time_stop": { "rule": "close_at_dte", "value": 7 },
    "profit_target": { "rule": "close_at_pct_credit_captured", "value": 0.75, "debit_to_close": 0.20 }
  },
  "crew_dissent": [
    { "character_id": "spock",  "stance": "NEUTRAL",
      "reason": "Composite momentum score +12; needs ≤ -20 for bearish lean." },
    { "character_id": "troi",   "stance": "CAUTION",
      "reason": "Fear & Greed 84 suggests crowded-short squeeze risk." },
    { "character_id": "mccoy",  "stance": "NOT_APPLICABLE",
      "reason": "Crisis Doctor only acts on oversold rescues." },
    { "character_id": "chekov", "stance": "STAND_DOWN",
      "reason": "2 of 4 convergence strategies bearish; needs ≥ 4." }
  ],
  "concepts_referenced": [
    "rsi","relative_volume","iv_rank","bear_call_spread",
    "theta","time_stop","gamma","atr","resistance"
  ],
  "execution": {
    "status": "filled", "venue": "paper_alpaca",
    "fill_time_market": "2026-05-14T10:47:12-04:00",
    "fill_price": 0.87, "commissions": 0
  }
}
```

Strategy authors author *once*. Concept Cards author *once*. Trades narrate themselves forever.

---

## 7. Color-blind-safe palette (CSS variables, drop-in)

```css
:root {
  --state-positive:  #3FA7D6;  /* cyan — up / aligned */
  --state-negative:  #E5573F;  /* vermilion — down / at-risk */
  --state-caution:   #F4B400;  /* amber — watch */
  --state-neutral:   #8A94A6;  /* slate — standby */
  --state-live:      #C6428E;  /* magenta — firing now */

  --state-positive-on-dark: #6BC4E8;
  --state-negative-on-dark: #FF7A63;
  --state-caution-on-dark:  #FFCB47;
  --state-neutral-on-dark:  #B0B8C6;
  --state-live-on-dark:     #E26AAC;

  --surface-base:    #0F1419;
  --surface-raised:  #1A2028;
  --surface-overlay: #232B36;
  --surface-border:  #2E3744;
  --text-primary:    #E8ECF1;
  --text-secondary:  #A6B0BD;
  --text-muted:      #6E7785;

  /* Character identity — NEVER used to convey state */
  --char-worf:    #9B5188;
  --char-mccoy:   #3FA796;
  --char-spock:   #5778AB;
  --char-troi:    #C685D1;
  --char-kirk:    #E5B85C;
  --char-chekov:  #82B05B;
  --char-dax:     #E0897C;
  --char-scotty:  #B08A5F;
  --char-riker:   #6B8BAC;
  --char-data:    #C2C7CE;
  --char-uhura:   #DC72A8;
  --char-ollie:   #F0AA5C;
}

[data-display-mode="high-contrast"] {
  --state-positive:  #00E0FF;
  --state-negative:  #FF6040;
  --state-caution:   #FFD700;
  --state-neutral:   #FFFFFF;
  --state-live:      #FF40A0;
  --surface-base:    #000000;
  --surface-raised:  #0A0A0A;
  --text-primary:    #FFFFFF;
}

[data-display-mode="text-only"] .character-icon,
[data-display-mode="text-only"] .concept-glyph {
  display: none;
}
```

**Cardinal rule:** every state is conveyed by **three independent channels** — color + shape + text label. Remove any one and meaning survives.

State shape pairings:
- positive → ▲ filled triangle
- negative → ▼ filled triangle
- caution  → ◆ diamond
- neutral  → ● circle
- live     → ✦ four-point star

---

## 8. Custom SVG icon set (replaces emojis)

Two tiers, all line-art SVG, all inherit color via `currentColor`. Silhouette alone identifies — color is reinforcement.

**Tier 1 — Character portraits (12 icons, 64px source, render 24/32/48):**
Worf (helmeted ridged-brow bust), McCoy (caduceus), Spock (Vulcan-salute hand), Troi (sound-wave in oval), Kirk (three chevrons), Chekov (four converging arrows), Dax (coin with leaf), Scotty (wrench-and-gear), Riker (open scroll), Data (chip with center dot), Uhura (headset arc), Ollie (O with up-arrow inside).

**Tier 2 — Concept glyphs (14 icons, 24px source, render 16px):**
Bear Call Spread (descending staircase + cap), Bull Put Spread (ascending staircase + floor), Iron Condor (inward arrows + band), Covered Call (circle + ceiling), Cash-Secured Put (floor + down-arrow), RSI (scale 0-100 + needle), Relative Volume (tall bar vs short bar), ATR (vertical range bar), EMA Ribbon (three flowing lines), Gamma Flip (pivot bar + arrows), IV Rank (thermometer), Time Stop (clock), Stop Loss (line + break), Take Profit (line + target ring).

Full JSX source for all 26 icons is in the section-2 and section-3 listings of the full handoff (see "OllieTrades Bridge — Accessibility Handoff" artifact, written separately).

---

## 9. Component specs

**`<CharacterChip />`** — props: character, size (sm/md/lg), state, showRecord, roleLabel, record, onClick. ARIA: `"Worf, Bear Spreads, 60% win rate, currently Watch"`. Min height 44px (WCAG tap target).

**`<StateIndicator />`** — always renders SVG shape + color + text label. Three channels mandatory.

**`<PnLPill />`** — single component for every positive/negative dollar/percent rendering. Guarantees cyan-up / vermilion-down + triangle consistency app-wide.

**`<ConceptGlyph />`** — inline 16px glyph used in walk-backs and Concept Cards.

**`<ConsensusRow />`** — replaces the 500-ticker wall of squares. Renders only tickers with active stances; "Show all 500 monitored" button reveals the rest.

---

## 10. Display Settings panel

Controls (persist to `localStorage` key `bridge.display`, apply via `data-*` attributes on `<html>`):
- **Mode:** Default · High Contrast · Text Only
- **Icon size:** Small (24) · Medium (32) · Large (48)
- **Show state labels:** on/off
- **Reduce motion:** on/off (also respects OS `prefers-reduced-motion`)

---

## 11. Verification checklist

After each implementation step:

**Color/contrast** — WCAG AA (4.5:1 text, 3:1 graphics). Chrome DevTools → Rendering → Emulate vision deficiencies. Walk deuteranopia, protanopia, tritanopia, achromatopsia. Meaning must survive each.

**Icons** — every CharacterIcon identifiable by silhouette in greyscale. Renders cleanly at 24/32/48. No emoji characters remain (`grep '[\u{1F300}-\u{1FAFF}]|[\u{2600}-\u{27BF}]'` returns zero).

**Keyboard / SR** — tab order matches visual order; visible focus rings; ARIA labels on every chip; state shapes marked `aria-hidden` with sibling text node OR `aria-label` on wrapper.

**Touch** — every clickable target ≥ 44×44px.

**Settings** — toggling each preference applies without layout shift; persists across reload; honors OS-level reduce-motion.

**Zoom** — readable at 200% browser zoom on 1280px viewport.

---

## 12. Implementation order + Claude Code prompt

In your repo, run Claude Code with this prompt:

> "Implement the design system in the attached handoff document against this codebase. Work in this order, committing after each step:
> 1. Add the CSS variables + Tailwind tokens from section 7. Migrate hardcoded colors in components to these tokens.
> 2. Create `src/components/icons/CharacterIcons.tsx` and `src/components/icons/ConceptGlyphs.tsx` from section 8.
> 3. Build `CharacterChip`, `StateIndicator`, `PnLPill`, `ConsensusRow` from section 9.
> 4. Replace every emoji character with the equivalent SVG. Grep migration: `▲|▼|⚠️|🔴|🟢|🟡|⚪|🟠`.
> 5. Build `DisplaySettingsPanel` + `useDisplayPrefs` hook from section 10; mount in Ready Room.
> 6. Lock the trade-signal payload from section 6 as a TypeScript type or Pydantic model; validate every signal before render.
> 7. Build the three-depth Explain component (Headline → WalkBack → ConceptCard drawer) from sections 3-5, driven entirely by the payload.
> 8. Build the Ticker Command Card from section 2.
> 9. Run the section 11 checklist after each step. Stop and report if any check fails.
> Do not introduce colors, icons, shapes, or strategies beyond what is specified."

---

## 13. Where we left off

Open threads for the next session:
- Full JSX source for all 26 SVG icons (Tier 1 + Tier 2)
- 30+ remaining Concept Cards (MACD, Bear Call Spread, IV Rank, Time Stop, Gamma Flip, Convergence, etc.) written in the same 5-section shape
- Crew-dissent evaluation pipeline (every character has to score every signal, not just the firing one)
- Ticker Command Card detailed component spec with the three-tab (Stock/Options/Alerts) action layer
- Star Wars / MASH / Dallas theme-pack mappings
- Re-organization of the left navigation rail by time-of-day workflow (Pre-Market / Live / Research / Review)

---

## 14. Suggested kickoff prompt for Claude Desktop

> "I'm continuing work on the OllieTrades Bridge dashboard. The attached document is the design + trading handoff from my previous session. Pick up at section 13 — start by producing the full JSX source for all 26 SVG icons specified in section 8, then write Concept Cards (5-section shape per section 4) for: MACD, Bear Call Spread, Bull Put Spread, IV Rank, Time Stop, Gamma Flip, Convergence, Stop Loss, Take Profit, ATR. After that, draft the Ticker Command Card component spec from section 2."

---

_End of handoff._
