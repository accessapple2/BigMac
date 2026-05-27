# Ticker Command Card — Component Spec

Implementation contract for the section-2 component of the OllieTrades Bridge design handoff. Hand to Scotty (Claude Code) as the source of truth.

---

## 1. Purpose

Type or paste a ticker, get a single screen that answers three questions in order:

1. **What is this name doing today?** (Facts)
2. **What does the crew think?** (Crew Read)
3. **What do I do about it?** (Action tabs)

Replaces the multi-page workflow of "look at chart → check positions → look up options chain → check news → set alert" with one consolidated surface. The product brief from section 0 of the handoff: *"take a ticker and know what to do from there."*

---

## 2. When and where it renders

Three entry points, all converging on the same component:

- **Primary:** the persistent ticker search box in the dashboard header
- **Secondary:** clicking any ticker symbol in the Comms feed, Leaderboard, Captain's Plan, or Trade history
- **Tertiary:** URL-addressable deep link `/ticker/{symbol}` (so Slack and email pastes work)

Three render variants share the component:

- `embedded` — fills its parent column in the dashboard
- `modal` — overlay over the current page, dismissible
- `full-page` — when reached via deep link, fills the viewport

Variant is a prop; layout adapts but content is identical.

---

## 3. Anatomy

Six regions, top to bottom, all owned by `<TickerCommandCard />`. Each is a discrete sub-component for testability and to let regions render independently as data arrives.

```
┌─────────────────────────────────────────────────────┐
│ Region 1 — Header / Facts                           │
│   Symbol · last · day % · day $ · vol · sector ·    │
│   earnings date                                     │
├─────────────────────────────────────────────────────┤
│ Region 2 — Position panel (only if position > 0)    │
│   Qty · avg cost · unrealized P&L · % portfolio ·   │
│   days held                                         │
├─────────────────────────────────────────────────────┤
│ Region 3 — Key levels                               │
│   20/50/200 MA · ATR(14) · 52w range                │
├─────────────────────────────────────────────────────┤
│ Region 4 — Crew Read                                │
│   One CharacterChip row per character with a stance │
├─────────────────────────────────────────────────────┤
│ Region 5 — Action tabs                              │
│   [ Stock ] [ Options ] [ Alerts ]                  │
│   (tabpanel content)                                │
├─────────────────────────────────────────────────────┤
│ Region 6 — Learn drawer (collapsed by default)      │
│   Two-sentence "Why?" for the active character      │
└─────────────────────────────────────────────────────┘
```

---

## 4. Props interface

```typescript
interface TickerCommandCardProps {
  /** Ticker symbol — uppercase, no $ prefix. Component normalizes. */
  symbol: string;

  /** Called when user dismisses the card (modal variant only). */
  onClose?: () => void;

  /** Which action tab is active on mount. Default: 'stock'. */
  initialTab?: 'stock' | 'options' | 'alerts';

  /** Render variant. Default: 'embedded'. */
  variant?: 'embedded' | 'modal' | 'full-page';

  /** Optional override for the data source (testing / Storybook). */
  dataSource?: TickerDataSource;
}
```

Data is fetched, not passed in. The component owns its own data lifecycle keyed by symbol. Parent passes the symbol and gets out of the way.

---

## 5. Data flow

Six independent data hooks fire when `symbol` changes (debounced 150ms to avoid mid-typing thrash):

| Hook | Purpose | Polling cadence |
|------|---------|-----------------|
| `useQuote(symbol)` | last, day %, day $, volume, sector, earnings date | 5s market hours / 60s after hours |
| `useKeyLevels(symbol)` | 20/50/200 MA, ATR(14), 52w high/low | 60s |
| `usePosition(symbol)` | qty, avg cost, unrealized P&L, % port, days held | 15s |
| `useCrewStances(symbol)` | all 12 characters' current evaluation | 30s |
| `useOptionsChain(symbol)` | crew-endorsed structures only | lazy — fires only when Options tab activates |
| `useAlerts(symbol)` | existing alerts on this symbol | 60s |

Each region renders progressively as its data arrives. Skeleton state per region while loading. A slow Options chain never blocks Facts or Crew Read.

The hooks layer is the seam where Polygon (primary), Alpaca (fallback), and the internal `signals.db` are wired. The component itself doesn't know which source served any given field.

---

## 6. Region 1 — Header / Facts

Always rendered. Five-second-scan layout: ticker is the largest type, everything else is supporting.

**Required fields:**
- Symbol (large, bold, monospace)
- Last price (large)
- Day change (`$` value and `%`, both with `<PnLPill />`)
- Day volume vs 20-day average (e.g. "1.8× avg")
- Sector tag (chip)
- Next earnings date (or "—" if none scheduled)

**Behavior:**
- Last price ticks with `aria-live="polite"` — screen readers announce changes without interrupting
- Volume comparison: round to one decimal place
- Earnings date: relative format when ≤ 7 days ("in 3 days"); absolute otherwise ("May 28")
- Sector chip uses neutral palette — sector is not a state, just a tag

---

## 7. Region 2 — Position panel

**Renders only if** the user holds a position in this symbol (qty ≠ 0). When position is zero, this region is omitted entirely (not just hidden) so layout collapses cleanly.

Fields:
- Quantity (with sign — negative for short)
- Average cost
- Unrealized P&L (dollar and %, `<PnLPill />`)
- Percentage of total portfolio
- Days held
- Source broker (Alpaca paper / Schwab / Webull / IBKR — tag chip)

A position from the autonomous Alpaca paper account is labeled differently from a Schwab real position. The label color matches the broker's chip in the existing dashboard.

---

## 8. Region 3 — Key levels

Always rendered. Six fields in two rows:

Row 1: 20-day MA · 50-day MA · 200-day MA
Row 2: ATR(14) · 52w high · 52w low

Each level shows the value plus the underlying's relationship to it ("above," "below," "at" — with `<StateIndicator />` shape). The ATR field also shows "x.x ATR from last" — i.e. how many ATRs the current price is from the most recent close — useful for sizing context.

---

## 9. Region 4 — Crew Read

The mnemonic payoff. One row per character with an active stance on this ticker today. Each row is a `<CharacterChip />` from section 9 of the handoff, sized `lg`.

**Stance vocabulary** (matches the section-6 payload `crew_dissent[].stance` plus active-conviction values):

| Stance | Meaning | Shape |
|--------|---------|-------|
| `BUY` | Open new long | ▲ |
| `SELL` | Open new short | ▼ |
| `ADD` | Add to existing long | ▲ |
| `TRIM` | Reduce existing long | ▼ |
| `HOLD` | Maintain current | ● |
| `WATCH` | Monitoring, no action | ● |
| `CAUTION` | Active concern | ◆ |
| `STAND_DOWN` | Considered but rejected | ● |
| `NOT_APPLICABLE` | Out of scope for this character | (row omitted) |
| `NEUTRAL` | No opinion | (row omitted unless zero conviction rows) |

**Sort order:**
1. Conviction rows (`BUY`, `SELL`, `ADD`, `TRIM`) — by character's score descending
2. `WATCH` / `CAUTION` — by recency
3. `HOLD` / `STAND_DOWN` — by character's display order from the roster
4. `NEUTRAL` rows only appear when (1)–(3) produce zero rows; in that case, show top-3 neutrals so the section is never empty

**Empty-state fallback:** if no character has evaluated this name in the last hour, show `"No active signals. Last evaluation: HH:MM ET."` with a re-scan button.

**Each row's content** (left to right):
- Character avatar (32px, from `CharacterIcons.tsx`)
- Character name · role label
- Stance pill with shape + color + text
- One-line reason (truncated to single line on narrow viewports, full on wide)
- Score badge (optional, only when character provides one)

**Click behavior:** clicking a row expands the Learn drawer (region 6) with that character active.

---

## 10. Region 5 — Action tabs

Three tabs, side-by-side. Tab state is local to this card instance (not URL-persisted in v1).

### 10.1 Stock tab

A two-column layout:

**Left column — Sizing calculator**
- Inputs (with sensible defaults):
  - ATR multiplier — default 1.5× (swing) or 2.5× (position)
  - Max risk % — default 0.5% of fleet equity
  - Stop method — radio: ATR-based / Percent-based / Custom price
- Outputs (computed live as inputs change):
  - Suggested share count
  - Stop price (and stop distance in ATRs)
  - Target 1 (1R) · Target 2 (2R) · Target 3 (3R)
  - Total dollar risk
  - Effective position size as % of fleet
- A warning chip appears if the suggested size would breach the fleet cash ceiling (`FLEET_CASH_CEILING` config). The chip explains: "Caps at $X to respect ceiling."

**Right column — Order preview**
- Side (BUY / SELL / SELL_SHORT / BUY_TO_COVER)
- Order type (LIMIT / STOP / STOP_LIMIT / MARKET)
- Time-in-force (DAY / GTC)
- Price field (pre-filled from calc)
- Quantity (pre-filled from calc)
- Pre-filled with calc output; user can override
- v1: read-only preview. Phase 2: submit-to-Alpaca-paper button.

**Character playbook override:** if a character has fired a BUY/SELL signal for this name today, their stop and target rules override the calculator defaults. A small "Using Worf's playbook" chip appears at the top of the calculator with a button to revert to manual defaults.

### 10.2 Options tab

**Lazy-loaded** — the options chain query fires only when this tab is first activated.

**Filtered view (default):** show only structures currently endorsed by the crew. Each row labeled by the endorsing character:

```
WORF · Bear Call $612 / $615 · 14 DTE · Cr $0.87 · POP 70%
SPOCK · Iron Condor $605/$610/$620/$625 · 14 DTE · Cr $1.20 · POP 65%
McCOY · CSP $580 · 14 DTE · Cr $1.40 · POP 75%
```

Grouped by structure family. Each row expandable to show:
- Full leg detail
- The triggering signal payload (link)
- The character's playbook reasoning (one paragraph)
- "Send to order builder" button (phase 2)

**Toggle: "Show full chain"** — bypasses the crew filter and renders the standard chain. Sort by IV rank descending by default; secondary sort by DTE ascending.

**Empty state:** "No crew-endorsed structures for this name today" with a one-line explanation of *why* (e.g. "IVR 18 — below the 25 threshold for short-premium").

### 10.3 Alerts tab

One-click alerts pre-populated based on the levels the crew is already watching.

**Pre-populated alerts (defaults):**
- Price touches each visible key level (resistance, support, MAs) — one alert per level
- IV rank crosses 60 (above) and 25 (below)
- Earnings date approaches (T-1 day reminder)
- Existing alerts get a delete icon

**Manual alert builder:**
- Type (Price / IV / News)
- Condition (>= / <= / crosses)
- Value (price, IV rank value)
- Channel (in-app / Slack / email / ntfy — matching the Dr. Crusher healthcheck fanout)
- Save button

Alerts persist across sessions, keyed by user + symbol. Maximum 20 active alerts per symbol (UI shows count and prevents over-add).

---

## 11. Region 6 — Learn drawer

Collapsible. Default collapsed. Click header to expand. Also expands when user clicks a Crew Read row.

**Content shape:** for the active character, render two-sentence explanation pulled from a static registry keyed by `character.id × strategy_id`:

```
WORF · BEAR CALL SPREAD
"Worf shorts premium into confirmed resistance rejections when IV is
elevated but not extreme. Today he fired on QQQ because RSI hit 78,
volume confirmed at 1.8× average, and IVR is at 42 — inside his
30–60 sweet spot."
```

Below that, a single-row "Concept references" line linking each italicized term back to its Concept Card via the `<ConceptGlyphById />` registry.

**Cycling:** if multiple characters have stances on this ticker, the drawer shows a `<` / `>` cursor allowing the user to cycle through them. Active character defaults to highest-conviction (BUY > ADD > WATCH > etc.).

**No firing today:** if a character is in the Crew Read with `STAND_DOWN`, the drawer shows *why they passed*:

```
CHEKOV · CONVERGENCE
"Chekov waits for 4-of-N strategies to agree before firing. Today only
2 are bearish on QQQ (RSI extreme + volume) — momentum and breadth
both still positive. Stand down."
```

---

## 12. Empty states (full enumeration)

| Condition | Behavior |
|-----------|----------|
| No symbol entered | Show search prompt with recent searches and watchlist shortcuts. No card body. |
| Invalid symbol | Clear error message + "did you mean" suggestions from fuzzy match. |
| Market closed | Banner across top: `"After hours · last close 4:00pm ET"`. All regions still render with most recent values. |
| Premarket | Banner: `"Premarket · open in HH:MM"`. Quote shows premarket print where available. |
| No position | Region 2 omitted. Sizing calc shows "no current position" with no override needed. |
| No crew stances | Region 4 shows "No active signals on this name. Last evaluation: HH:MM ET" + re-scan button. |
| Options chain unavailable | Tab still selectable but body shows error + retry. Stock and Alerts tabs unaffected. |
| Alerts service down | Alerts tab body shows error; existing alerts list still readable from cache. |
| Quote stale (> 60s during market hours) | Yellow staleness indicator next to last price. |

---

## 13. Accessibility

The card is a focus-managed surface; section 11 verification rules from the handoff apply throughout.

**Roles and semantics:**
- Search input: `role="combobox"`, `aria-autocomplete="list"`, `aria-controls` pointing at the results listbox
- Action tabs: `role="tablist"` with `role="tab"` children, `aria-selected` toggling, `aria-controls` pointing at panels
- Tab panels: `role="tabpanel"`, `aria-labelledby` referencing the active tab
- Crew Read rows: each is a `<button>` with full text label baked in (`"Worf, Bear Spreads, BUY signal, RSI overbought into resistance, score 80"`) — one SR read gives the whole row
- Learn drawer: `<details>` / `<summary>` pattern, or ARIA disclosure if custom

**Live regions:**
- Last price: `aria-live="polite"` — announces ticks without interrupting
- Stance changes when on the Crew Read region: `aria-live="assertive"` — interrupts because action may be required
- Alerts firing: `aria-live="assertive"` — interrupts

**Focus management:**
- On mount: focus moves to the search input (full-page variant) or the first interactive control in Region 1 (embedded/modal)
- On tab change: focus moves to the first interactive element in the new tabpanel
- On modal dismiss: focus returns to the element that opened the modal
- All clickable targets ≥ 44×44 px (WCAG)

**Cardinal rule (section 7):** every state conveyed by **color + shape + text label**. Remove any one and the meaning still gets through. The card is the primary place this rule gets tested — verify with Chrome DevTools → Rendering → Emulate vision deficiencies (deuteranopia, protanopia, tritanopia, achromatopsia).

---

## 14. Keyboard map

| Key | Effect |
|-----|--------|
| `/` | Focus the search input from anywhere on the dashboard |
| `Tab` / `Shift+Tab` | Standard focus order |
| `←` / `→` while on tablist | Cycle action tabs |
| `↑` / `↓` while on Crew Read | Move between rows |
| `Enter` on Crew Read row | Expand Learn drawer with that character active |
| `Esc` | Collapse Learn drawer if open; else close card (modal variant) |
| `Enter` in search | Jump to first matching result |
| `?` | Toggle keyboard help overlay |

---

## 15. Performance

- Symbol change → 150ms debounce before firing data queries
- All hooks run in parallel; no serial dependency between regions
- Component memoized at the top level; each region individually memoized to prevent cascading re-renders on quote ticks (a price update shouldn't re-render the Crew Read)
- Options chain lazy-loaded — never fetched unless tab activates
- Learn drawer registry is static JSON imported at module load; no runtime fetch
- Cached responses honored for 5 seconds (quote), 60 seconds (key levels, alerts), 30 seconds (crew stances)
- Stale-while-revalidate pattern — show last-known value with staleness indicator rather than blocking on refetch

---

## 16. Integration points

Components consumed (existing, from section 9 of the handoff):

- `<CharacterChip />` — every Crew Read row, every character reference in Learn drawer
- `<StateIndicator />` — stance pills, level-above/below indicators
- `<PnLPill />` — day change, position P&L, every dollar value with sign
- `<ConceptGlyph />` (and `<ConceptGlyphById />` registry from `ConceptGlyphs.tsx`) — inline in Learn drawer text

Payload consumed (existing, from section 6 of the handoff):

- The trade signal payload schema is one source feeding Crew Read. The other source is the latest-evaluation snapshot per character regardless of whether a signal fired, so STAND_DOWN and NOT_APPLICABLE rows are first-class citizens.

Services touched:

- `useQuote` — Polygon primary, Alpaca fallback, yfinance second fallback
- `useKeyLevels` — local computation from candles
- `usePosition` — `real_holdings.json` (Schwab CSV pipeline, HM-AT-β) for real positions; Alpaca SDK for paper
- `useCrewStances` — `signals.db` direct read; the same database every other dashboard region uses
- `useOptionsChain` — Polygon Options Starter ($29/mo) primary, Alpaca options fallback
- `useAlerts` — internal alerts service (port 8080)

---

## 17. Out of scope (v1)

Explicitly deferred:

- **Order submission.** v1 is read-only preview. Phase 2 wires submit-to-Alpaca-paper through Scotty's executor.
- **Custom multi-leg options builder.** v1 only shows crew-endorsed structures and standard chain. Phase 2 may add custom builder.
- **Embedded chart.** Defer to Phase 2 — for now, link out to TradingView with symbol pre-filled.
- **Cross-instrument types.** Stock + options + alerts only. Futures, crypto, FX later.
- **Save-to-watchlist from this card.** Use the existing watchlist UI.
- **Comparison view (this ticker vs that ticker).** A real feature request, but not for v1.
- **Mobile-specific layout.** v1 is desktop-first. Mobile (narrow viewport) gets a stacked single-column variant in Phase 2.

---

## 18. Open questions for the next session

- **Sizing calc fleet cash ceiling source.** Where does the ceiling come from — config file, env var, or a database row? Currently TBD; calc shows the cap behavior but the source needs to be wired before phase 2.
- **Real-position broker resolution.** When both Schwab and Webull show a position in the same symbol, which one drives the Position panel? Probably aggregate with broker breakdown on hover.
- **Conflict resolution between character playbook stops and user-overridden stops.** Right now the playbook chip is dismissible. Is there an audit trail when a user dismisses it? Affects Captain's Plan accountability.
- **Learn drawer registry storage.** Static JSON imported at module load is fine for ~150 entries (12 characters × ~12 strategies). At higher scale, move to lazy-loaded by character.
- **Tab state in URL?** Right now tab state is local. Deep-linking to "this ticker, options tab" could matter for Slack pastes.

---

_End of spec._
