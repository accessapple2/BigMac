# NIGHT CREW REPORT — 2026-06-29
**Branch:** exec-pipeline · **Reported by:** Scotty · **Admiral-review required before any further alpha work**

---

## RESUME STATE
_Written 2026-06-29 ~22:15 UTC. HOLD FOR ADMIRAL._

### What shipped this session

| Unit | Commit | Status |
|---|---|---|
| Rung 1 — CONTACT tab (onclick fix + empty-state fix + parseFocus loop fix) | `21e8279` | ✅ LIVE |
| Rung 2 — CONTEXT-ON-ARRIVAL (`carrier_rung2_context.js`, 5 slots, contamination-guarded) | `e44ec48` | ✅ LIVE |
| Rung 3 spec — debate/review in place (design-only) | filed `drafts/RUNG3_DEBATE_IN_PLACE_SPEC.md` | ✅ FILED |
| Realized-return rewire spec — P1 gate (design-only) | filed `drafts/REALIZED_RETURN_REWIRE_SPEC.md` | ✅ FILED |

**Rung 2 self-verify (Chrome, 2026-06-29 ~22:11 UTC):**
- `?focus=NVDA&src=uhura` auto-acquired ✓
- Uhura emphasis order correct: LIVE TAPE leads (real headline rendered in blue) ✓
- LAST/MARK: `$191.88 (-2.05%)` in **amber** (≥2% move = notable; number shown) ✓
- OPTIONS FLOW / CONGRESS / GAMMA: "no data" fail-soft ✓
- FLEET READ: "pending realized-return rewire (P1)" — stub held ✓
- **NO fwd_return / NO edge / NO alpha number anywhere** ✓
- Colorblind: amber + blue only, numbers always alongside ✓

**Three bugs caught and fixed during Rung 1 integration (same commit):**
1. `JSON.stringify` in onclick attribute (double-quote attribute break) → index lookup
2. `.ct-empty.hidden` CSS rule missing → added
3. `parseFocus → showTab → loadContactTab → parseFocus` infinite loop → guard on tab visibility

**Rung 2 field adaptations (real JSON shapes):**
- `/api/price/{sym}` → `d.price`, `d.change_pct` (no `ts` field; no `last` alias)
- `/api/market/flow` → 404 on this host → fail-soft immediately
- `/api/congress/top-buys` → `{top_buys:[{ticker,buy_count,politicians,signal_strength}]}` → filter by ticker
- `/api/gex-snapshot` → `{data:{SYM:{spot,total_gex,gamma_flip,regime,asof,...}}}` → lookup by `d.data[sym]`
- `/api/news/{sym}` → `[{headline,source,...}]` (no `ts` field)

### Staged / blocked
- **Options Flow slot** always "no data" — `/api/market/flow` returns 404. If a flow endpoint exists under a different path, adapt `slotFlow()` in `carrier_rung2_context.js` (marked with "ADAPT" comment).
- **Congress/Insider slot** shows "no data" for tickers not in today's top-buys list (e.g. NVDA). This is correct behavior — slot fills when congress activity exists for the ticker.
- **Gamma/GEX slot** shows "no data" for any ticker that isn't SPY — the `/api/gex-snapshot` only contains SPY data. If per-ticker GEX is added later, `slotGEX()` is already written to handle `d.data[sym]` for any sym.
- **No restart was performed** — Rung 1 and Rung 2 are pure static file changes served directly; no Python restart needed or triggered.

### THE NEXT MOVE FOR ADMIRAL

**Review `drafts/REALIZED_RETURN_REWIRE_SPEC.md` — this is the gate on everything.**

Until the P1 realized-return rewire lands:
- No source has deployment authority (alpha read is scanner-projected, not realized)
- Fleet Read slot stays stubbed
- No Rung 4 sortie path can be built
- Rung 3 stays design-only

The spec is complete and ready for Admiral review. The build is ~4–5h. After P1:
1. Run `/api/observations/summary` → this is the real alpha read
2. Review realized `avg_fwd_1d_realized` per source
3. Sources with positive, consistent, statistically meaningful realized return → Rung 4 consideration
4. Emit-time acted tagging (sequenced behind P1) → solves the acted_by_fleet dead-end

### Open questions for Admiral
1. **P1 build greenlight** — ready to build the realized-return rewire? It needs a restart to activate the evaluator change (plan a maintenance window).
2. **Options Flow slot** — is there a live flow endpoint on this host? Current path `/api/market/flow` returns 404. If so, what's the correct path?
3. **Rung 3 review** — ready to review `drafts/RUNG3_DEBATE_IN_PLACE_SPEC.md`? Active mode (Convene War Room) requires explicit Admiral approval before build.

---

## ⚠ HEADLINE: FWD_RETURN_1D IS NOT REALIZED RETURN — ALPHA READ INVALID

`fwd_return_1d` is computed in `engine/signal_evaluator.py` as:

```python
fwd_return_1d = round((target_price - entry_price) / entry_price, 6)
```

where `entry_price` and `target_price` come from `deep_scan_results` — the scanner's **own projected target at signal time**, not what the market actually delivered.

**Every per-source `avg_fwd_*` in `/api/observations/summary` is a measure of scanner optimism, not measured edge.** The numbers are high and suspiciously consistent across sources because `bk_avwap`, `deep_scan`, and `bk_orb` all tend to fire on the same tickers the deep scanner projected bullishly on the same day.

**VERDICTS:**
- The alpha read is NOT trustworthy.
- NO source earns Rung 4 deployment authority from these numbers.
- Deployment authority is NOT established.
- This is an Admiral-review item before any further alpha work proceeds.

---

## PROXY NUMBERS (LABELED — scanner-projected, NOT realized, do NOT bank)

Snapshot captured 2026-06-29 ~04:27 UTC. Drain mechanically complete for all scoreable rows.

```
total obs:       11,057
evaluated:       10,628  (415 in-window pending — score when expiry closes)
fwd_return fill: 22.9%   (2,531 rows have fwd_return_1d; remainder evaluated,
                          no deep_scan match for that ticker/date)
```

| Source | N (evaluated) | avg_fwd_1d_not_acted | Note |
|---|---|---|---|
| `bk_avwap` | 8,103 | +9.08% | scanner-projected, NOT realized |
| `deep_scan` | 1,904 | +9.17% | scanner-projected, NOT realized |
| `bk_orb` | 451 | +10.12% | scanner-projected, NOT realized |
| `uhura` | 34 | +3.06% | scanner-projected, NOT realized |
| `bk_box` | 117 | +0.44% | scanner-projected, NOT realized |
| `fred_bankrate` | 5 pending | — | — |
| `grok_kirk_scan` | 22 pending | — | — |

**One fleet-acted observation** (`bk_orb`): acted avg_fwd_1d = +5.04% (scanner-projected).
`acted_by_fleet` structurally ~0 across all sources — retrospective join dead-end confirmed.
`by_grade`: 11,017 null (pre-fix rows), 4 grade-A, 22 grade-B (forward-only; populating correctly).

**`/api/measurement-health` at snapshot:**
```
evaluator status: OK  (fill_rate 22.9% > 5% threshold)
filled_1d:  2,531 / 11,057
last_run:   2026-06-28 21:26:55
```

---

## P1 OPEN ITEM: REWIRE fwd_return_1d TO REALIZED RETURN

**Design — do not build without Admiral greenlight.**

### What it needs to do
For each evaluated observation: compute actual market price change from signal `ts` to `expiry` (or nearest close), using a real price feed. Replace the `deep_scan_results.target_price` proxy entirely.

### Price source
**Alpaca historical bars** — already integrated, authenticated, used by the fleet. Endpoint: `GET /v2/stocks/{symbol}/bars` with `start`, `end`, `timeframe=1Day`. Returns OHLCV. Use `close` at signal date as entry proxy; `close` at expiry date (or last available) as exit.

No Schwab reads (RULE #1). No new external APIs needed.

### Schema change
`fwd_return_1d` already exists as a nullable REAL column. The backfill would SET it where currently NULL or overwrite the current scanner-projected values. **Overwriting existing values is a schema mutation decision** — requires Admiral call on whether to:
- (A) Overwrite `fwd_return_1d` with realized return (cleanest; single source of truth)
- (B) Add `fwd_return_1d_realized REAL` column alongside the existing proxy (preserves history; two fields to track)

Recommendation: Option A — the proxy values are not meaningful; keeping them creates confusion. But Admiral decides.

### Backfill plan
1. For each `signal_observations` row where `fwd_return_1d IS NOT NULL` (2,531 rows) OR `evaluated_at IS NOT NULL` (all evaluated rows): fetch Alpaca daily bars for `(ticker, ts_date, expiry_date)`.
2. Compute `realized = (close_at_expiry - close_at_signal) / close_at_signal`.
3. Write to `fwd_return_1d` (or new column, per Admiral decision).
4. Rate-limit against Alpaca: batch by ticker, cache per-ticker bars, ~2,531 unique (ticker, date) pairs max. Estimate: a few minutes at conservative rate.
5. Forward: wire `signal_evaluator.py` to fetch realized return at eval time instead of `deep_scan_results` join.

### Effort
~3–4h total: Alpaca bars fetch helper, backfill runner (same pattern as `nightcrew_fwd_return_backfill.py`), evaluator wiring, dry-run verification. No trading logic. No restart risk for backfill; evaluator change needs restart.

---

## TASK STATUS

| Task | Status | Notes |
|---|---|---|
| Rung 1 HTML | **BUILT, not yet integrated** | `_nightcrew/carrier_rung1_contact.html` — 5 integration markers; awaits Task 1 integration work |
| Rung 2 spec | **FILED** | `drafts/RUNG2_CONTEXT_ON_ARRIVAL_SPEC.md` |
| Emit-time acted tagging spec | **FILED** | `drafts/EMIT_TIME_ACTED_TAGGING_SPEC.md` — Admiral-review gate, touches fire path |
| Backfill drain | **Mechanically complete** | 10,628/11,057 evaluated; 415 in-window pending; drained the wrong metric (scanner-projected, not realized) |
| Alpha read | **INVALID** | See headline — no deployment authority established |
| Realized-return rewire | **P1 OPEN** | Design above; do not build without Admiral greenlight |

---

## OPEN QUESTIONS FOR ADMIRAL

1. **Realized return column:** Option A (overwrite `fwd_return_1d`) or Option B (add `fwd_return_1d_realized`)? This decision gates the backfill build.
2. **Rung 1 integration:** greenlight to integrate `carrier_rung1_contact.html` into bridge-v2? Zero execution risk; pure display.
3. **Emit-time acted tagging:** ready for Admiral review of `drafts/EMIT_TIME_ACTED_TAGGING_SPEC.md`? Touches fire path — requires explicit go.
4. **Measurement loop:** once realized-return rewire is built and backfilled, re-run `/api/observations/summary` for the real alpha read. That is the deployment authority gate.
