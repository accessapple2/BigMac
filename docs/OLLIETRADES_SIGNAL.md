# OllieTrades Signal — Design Doc

**Status:** Phase 1 (ghost book, no execution) — spec'd and implemented 2026-07-09/10.
**Owner decision points:** flagged inline as **[ADMIRAL]** — nothing below is auto-promoted.

## 1. Vision (as specified)

The owner's phone/Chrome gets exactly two kinds of push, ever:
1. **SYSTEM-CRITICAL health alerts** (ops/infra — `hm_ops_sentinel.py`, DB locks, heartbeat staleness). Unaffected by this feature.
2. **OllieTrades Signal** — a top actionable trade where every currently-winning model agrees.

Everything else (individual `dyn_*` alerts, `bk_*` scanner alerts, per-agent trade fills) stays in-dashboard, not pushed. Tap the push → deep link → scorecard (chart, who approved and why, track record) → Trade button (paper only, existing confirm-guard).

Silence is a feature: if nothing qualifies, nothing pushes.

## 2. What already exists vs. what's new (research summary)

Grounded by direct code reads (file:line) before writing a line of new code — see below. Nothing in this feature duplicates working infrastructure; three places the obvious reuse candidate turned out to be the wrong shape, noted explicitly.

| Building block | Existing? | Verdict |
|---|---|---|
| Directional per-model signal log | `signals` table (`setup_db.py:80-91`, written by `paper_trader.py:3842-3849`) | **Reuse directly.** Columns: `player_id, symbol, signal, confidence, reasoning, created_at`. `signal` ∈ {BUY, BUY_CALL, BUY_PUT, SHORT, HOLD}. |
| "Consensus" signal table | `watchlist_signals` / `get_consensus_signals()` (`engine/signal_tracker.py:198-213`) | **Not reusable as-is** — BUY-only, no direction column, `HAVING model_count>=2` hardcoded. The gate needs true directional agreement; built fresh off the raw `signals` table instead. |
| Winning-model grading | `fleet_report_card()` (`engine/agent_ratings.py:308-322`) → `rating` (A-E), `total_trades`, `win_rate` | **Reuse** for grade/trade-count legs of the winning-model filter. |
| Season P&L | Arena leaderboard (`dashboard/app.py:2921`, `/api/arena/leaderboard`) → `return_pct`, `total_pnl`, `halt_mode` | **Reuse** for the P&L leg — NOT the same source as `fleet_report_card`'s `total_pnl` (that one is a quality-filtered alltime subset, explicitly documented as distinct via `scope_note`). Two sources, joined on `player_id`. |
| Model→confidence-for-symbol lookup | `TradeDecision.action/confidence` (`engine/providers/base.py:71-81`), persisted via the `signals` insert above | **Reuse.** |
| Scanner convergence tiers | `/api/scanner/convergence` (`dashboard/app.py:2046`), `_scanner_tier_for()` (`app.py:2033-2039`) — T1≥5, T2=4, T3=3 strategies in 90min, computed live off `strategy_signals` | **Reuse as a context input**, not a gate leg. |
| GEX regime / congress | `/api/market/gex`, `/api/congress/top-buys` | **Reuse as context inputs.** |
| Trade scorecard precedent | `trade_explanations` + `_build_trade_explain()` (`dashboard/app.py:21525`) — votes, risk, timeline, sources, **no chart, no entry/stop/target** | **Pattern reused, table not.** Post-trade only (keyed by `trade_id`). New signal scorecard needs pre-trade entry/stop/target + chart, so a new table (§4). |
| Deep-link contact-card route | `?focus=SYMBOL` reader exists in bridge-v2.html (`parseFocus()`, `bridge-v2.html:2789`) | **Half-wired, confirmed by an existing code comment** (`HM-NOTIF-DEEPLINK-WAR-ROOM 2026-05-29: deeplink half (inert until producer exists)`). No notification payload carries a URL today (`notifications` table has no URL column). Building a purpose-built `/signal/<id>` route rather than forcing into `?focus=`. |
| Phone push channel | `engine.alert_channels.send_alert()` → ntfy.sh (`_send_ntfy`, `AlertLevel.RED_ALERT` → `priority=urgent`, `tags=rotating_light`) | **Reuse directly.** Browser `Notification()` API is a *different*, weaker mechanism (requires an open tab) — not the real phone channel; not used here. |
| Market-hours gate | `engine.market_calendar.is_within_alert_hours()` (shipped tonight, item 8 of the same session) | **Reuse directly** — literally the same function, unmodified. |
| Ghost/shadow book pattern | Three independent, non-shared implementations found: shadow-CSP (`strategies/validation.py` DSR/PBO gate), `ollie_machine_p4_gate.py` (tier-floors), audition (`crew_role` flag). No common module. | **Pattern reused (statistical rigor, manual-flip-only promotion, never auto-promote), no code reused** — none of the three fit a consensus-alert-pipeline; a lighter Phase1→2 criterion is defined fresh in §7. |
| Outcome/win-loss tracking | Checked `signals_v2` (`status` is dispatch lifecycle only: pending/executed/expired, no outcome column) | **Entirely new** — confirmed no existing table anywhere in the codebase resolves a signal against forward price action for win/loss. This is genuinely net-new territory, built carefully (§5). |

## 3. The Gate

`engine/ollietrades_signal.py::evaluate_gate(symbol=None) -> list[SignalCandidate]`, called on a scheduled cadence (main.py, market-hours only — reuses `is_within_alert_hours()`, no separate cadence logic needed).

**Step 1 — Winning models (dynamic, never hardcoded names).**
```python
def get_winning_models(min_rating="B", min_trades=20, min_return_pct=0.0) -> set[str]:
```
Join `fleet_report_card()` (rating, total_trades) with the Arena leaderboard (return_pct, halt_mode). A player qualifies iff: `halt_mode == "active"` AND `rating` is A or B (or better than `min_rating` per the config) AND `total_trades >= min_trades` AND `return_pct > min_return_pct`. Recomputed every gate cycle — the roster can change (an agent can enter/leave "winning" status day to day) without a code change.

**Step 2 — Directional agreement.**
For each symbol with a `signals` row from a winning model in the last N minutes (config, default 60): group by symbol, require **every currently-winning model that has an opinion on that symbol today agrees on direction** (BUY/BUY_CALL vs SHORT — HOLD doesn't count either way, doesn't break unanimity), AND at least 2 winning models have actually voted (a single model "agreeing with itself" is not consensus — configurable `min_agreeing_models`).

**Step 3 — Playbook match.**
`PLAYBOOK_REGISTRY` (extensible dict, not hardcoded logic per strategy) maps a symbol's signal shape (option_type, direction, existing strategy tags from `signals_v2.strategy_tag` / scanner tier) to a named playbook: `bull_put_spread`, `leveraged_put`, `bear_play`, `ollie_live_swing`. Unmatched-but-otherwise-qualifying candidates are logged to the ledger as `SHOWN-ONLY` (visible in-dashboard, never pushed) rather than silently dropped — new playbooks get added to the registry, not hand-rolled per-symbol.

**Step 4 — Confidence threshold + market hours.**
Composite conviction = weighted avg of agreeing models' confidences, weighted by each model's `rating_score`. Must clear `MIN_COMPOSITE_CONVICTION` (config). `is_within_alert_hours()` gates the whole cycle — outside the window, the gate still *evaluates* and *logs* (ledger status `EXPIRED` if unactioned) but never reaches the push step, exactly mirroring how `dynamic_alerts._notify()` was fixed tonight.

**Step 5 — Daily cap, ranked.**
All candidates clearing steps 1-4 today are ranked by composite conviction; only the top `MAX_PUSHES_PER_DAY` (default 2-3, config) actually push. The rest log as `SHOWN-ONLY`. If zero candidates clear the gate: push nothing. Silence is the expected common case, not an error state.

**[ADMIRAL]** Exact starting values for `min_rating`, `min_trades`, `min_return_pct`, `min_agreeing_models`, `MIN_COMPOSITE_CONVICTION`, `MAX_PUSHES_PER_DAY` are set conservatively in Phase 1 (see `config.py` block, §8) and are meant to be tuned once ghost data accumulates — not treated as final.

## 4. Signal Ledger (immutable, required Phase 1)

Table `signal_ledger` (new, `data/trader.db`):

```sql
CREATE TABLE signal_ledger (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    symbol              TEXT NOT NULL,
    direction           TEXT NOT NULL,
    strategy            TEXT NOT NULL,            -- playbook registry key
    entry_price         REAL,
    stop_price          REAL,
    target_price         REAL,
    composite_conviction REAL NOT NULL,
    approving_models_json TEXT NOT NULL,          -- frozen: [{player_id, display_name, action, confidence, rating, rating_score}]
    dissents_json       TEXT,                     -- frozen: models with an opinion that did NOT agree, if any (HOLD excluded)
    context_json        TEXT,                     -- frozen: scanner tier, GEX regime, congress overlap at call time
    gate_config_json     TEXT NOT NULL,            -- frozen: thresholds in effect at call time (min_rating etc.)
    status              TEXT NOT NULL DEFAULT 'SHOWN-ONLY',  -- PUSHED | SHOWN-ONLY | SKIPPED-BY-OWNER | TRADED | EXPIRED
    pushed_at           TEXT,
    trade_id            INTEGER,                   -- FK into trades, set only if status=TRADED (paper)
    outcome             TEXT,                       -- NULL until resolved: WIN | LOSS | EXPIRED_UNRESOLVED
    outcome_r_multiple  REAL,
    outcome_resolved_at TEXT,
    outcome_detail_json TEXT                        -- resolution trail: which of target/stop hit first, when, at what price
);
CREATE INDEX idx_signal_ledger_created ON signal_ledger(created_at);
CREATE INDEX idx_signal_ledger_status ON signal_ledger(status);
CREATE INDEX idx_signal_ledger_symbol ON signal_ledger(symbol);
```

**No-repaint rule, structurally enforced, not just documented:** `entry_price`/`stop_price`/`target_price`/`approving_models_json`/`dissents_json`/`context_json`/`gate_config_json` are written once at INSERT and **never UPDATEd** by application code (enforced by convention + a code-review checklist item, since SQLite has no column-level immutability — the outcome-resolution engine (§5) only ever touches the `outcome*` and `status`/`pushed_at`/`trade_id` columns, verified by a dedicated test that asserts the frozen columns are byte-identical before/after a resolution pass). The scorecard page (§6) always renders directly from this row, never re-queries live model state for a past signal.

**Ghost-phase logging (ties to §7):** in Phase 1, every candidate that clears the FULL gate (steps 1-4) but where real pushing is disabled logs as `SHOWN-ONLY` even though a real deployment would have pushed it — this is exactly the ghost record the promotion decision in §7 is measured against. `SKIPPED-BY-OWNER` is reserved for Phase 2+ (an owner explicitly dismissing a push without trading it) and unused while Phase 1 never truly pushes.

## 5. Outcome Resolution Engine

`engine/ollietrades_signal.py::resolve_outcomes()`, scheduled (e.g. every 15 min, market hours). For every `signal_ledger` row with `outcome IS NULL` and `entry_price` set:
1. Pull forward price bars from `entry_price`'s timestamp to now (reuses `engine.market_data.get_intraday_candles`, same infra as the VWAP fix from earlier tonight).
2. Walk forward: did price hit `target_price` or `stop_price` first? Record whichever came first, the timestamp, and the actual fill-equivalent price.
3. If neither hit within a configurable resolution window (default 5 trading days) → `outcome='EXPIRED_UNRESOLVED'`.
4. Write `outcome`, `outcome_r_multiple` (= realized move / (entry-stop) distance), `outcome_resolved_at`, `outcome_detail_json`. Never touches the frozen call-time columns.

This runs identically regardless of `status` (PUSHED/SHOWN-ONLY/SKIPPED-BY-OWNER/TRADED) — **every signal that cleared the gate gets a verdict**, which is the entire point of the "regret meter" in §6: a `SHOWN-ONLY` (or, once Phase 2 ships, `SKIPPED-BY-OWNER`) call that would have won is exactly as informative as one that was traded and won.

## 6. `/signals/history` — Ledger Page

New route, `dashboard/static/index.html` (or a dedicated static page — Phase 1 ships it inside `/classic` as a new `registerSectionInit('signals-history', ...)` section, consistent with every other section in that file).

- Table: every `signal_ledger` row — timestamp, symbol, strategy, direction, entry/stop/target, approving models (name + confidence chip), composite conviction, status, outcome.
- Rollups (computed server-side, `/api/signals/ledger/rollup`): overall WR, WR by strategy, WR by approving-model combination (grouped by the sorted tuple of `approving_models_json` player_ids), avg R multiple, WR traded vs. WR skipped (the "regret meter" — literally `WR(status IN (SHOWN-ONLY,SKIPPED-BY-OWNER)) - WR(status=TRADED)`, signed so a positive number means the owner is leaving winners on the table), pushes/day, current streak.
- Filters: date range, strategy, traded/skipped, model — plain query params on `/api/signals/ledger?from=&to=&strategy=&status=&model=`.
- Each row links to `/signal/<id>` (§ below) — the frozen snapshot, not a live re-render.

## 7. `/signal/<id>` — Scorecard Page

New route + endpoint `/api/signals/ledger/<id>`. Renders directly from the frozen `signal_ledger` row (no live lookups for historical rows — the no-repaint rule from §4 applies to the render layer too):
- Chart with entry/stop/target drawn (reuses the existing lightweight-charts wiring already in `index.html` — `createLiveChart()` — extended to accept static price-line overlays, which `loadGex`-adjacent code already does for GEX walls).
- Strategy name, R:R (computed from frozen entry/stop/target).
- Every approving model: name, confidence at call time, AND its track record (`rating`, W/L, streak) — the track record fields are ALSO frozen at call time (`approving_models_json` already carries `rating`/`rating_score`), not re-fetched live, so a model's later grade changes don't retroactively repaint an old scorecard.
- Data sources used (from `context_json`), dissents (from `dissents_json`).
- Trade button: wired to the existing confirm-guarded PAPER execution path (same pattern as bridge-v2's `advisoryExecute()`/Battle-tab flow) — present in Phase 1's UI for completeness, but see §9: real execution stays behind `can_trade_live` gates untouched by this feature, and Phase 1 ships with pushing itself disabled (ghost mode), so in practice the button has nothing live to act on yet.

## 8. `/signals/compare` — Performance Comparison View

New route + endpoint `/api/signals/compare?window=`. Five series, same window, same outcome-resolution rules (§5 applied uniformly so the comparison is apples-to-apples):
1. **OllieTrades Signal** — WR/avg R/total return from `signal_ledger` (all statuses, or traded-only via toggle).
2. **Each winning model's solo calls** — same window, pulled from the raw `signals` table for that `player_id`, resolved with the identical §5 engine (a parallel, lighter-weight resolution pass keyed by `(player_id, symbol, created_at)` instead of `signal_ledger.id`).
3. **Fleet average** — existing `fleet_report_card()` data, no new resolution needed (it already carries realized WR).
4. **Ollie Live scanner tiers alone** — T1/T2/T3 picks from `strategy_signals`, same §5 resolution.
5. **Buy-and-hold SPY** — trivial baseline, entry = window start, no stop/target, pure return.

Sortable table + equity-curve overlay chart (cumulative R or cumulative $ over the window, one line per series). Traded-only vs. all-calls toggle. Per-strategy breakdown (facet by `signal_ledger.strategy`). Auto-updates as `resolve_outcomes()` (§5) fills in verdicts — no separate cache-invalidation logic needed since it queries `signal_ledger`/`signals` live each request (rollups are cheap at ghost-phase volume; revisit caching if/when Phase 2 volume grows).

**The question this page must answer:** does unanimity produce better calls, or just fewer calls? Concretely: is `signal_ledger`'s WR materially above the best individual winning model's solo WR over the same window? If yes and pushes stayed rare, that's the Phase 2 promotion signal (§9). If the consensus WR is roughly the same as (or worse than) the best solo model, unanimity is filtering for agreement, not for quality — a real, useful, non-flattering finding this page is explicitly designed to be able to surface.

## 9. Phasing

- **Phase 1 (now, ghost):** Gate + ledger + resolution engine + all three pages ship. `TRADING_ALERT_HOURS_ET`-gated, but the actual push step is hard-disabled (`OLLIETRADES_SIGNAL_PUSH_ENABLED = False` in config, checked immediately before the `send_alert()` call — same fail-safe-off pattern as `ALERT_DEFS_ENABLED`). Every candidate that would have pushed logs `SHOWN-ONLY` instead. No execution path is reachable — the Trade button exists in the UI for Phase-1 completeness but there is nothing live to click through to yet.
- **Phase 2 [ADMIRAL — explicit go, not automatic]:** once `/signals/compare` shows the ghost record is selective (pushes stayed rare under the cap) and the WR is materially above fleet/best-solo-model average over a meaningful sample (no fixed N prescribed here deliberately — this is a judgment call for whoever reviews the comparison view, the same "Admiral reviews, never auto-promotes" pattern as every other gate in this codebase) — flip `OLLIETRADES_SIGNAL_PUSH_ENABLED = True`. Real ntfy pushes begin. Trade button becomes live-clickable (still paper-only, still confirm-guarded, still the existing ceilings).
- **Phase 3 [ADMIRAL, explicit, later]:** live-money routing. Out of scope for this doc entirely — RULE #1 (Schwab hands-off) and the existing `can_trade_live` gates are untouched by everything above; this feature has no path to live execution without a separate, explicit future decision.

## 10. Config (`config.py`)

```python
# OllieTrades Signal (Phase 1 — ghost book, see docs/OLLIETRADES_SIGNAL.md)
OLLIETRADES_SIGNAL_PUSH_ENABLED = False   # [ADMIRAL] flip only after reviewing /signals/compare
OLLIETRADES_SIGNAL_MIN_RATING = "B"
OLLIETRADES_SIGNAL_MIN_TRADES = 20
OLLIETRADES_SIGNAL_MIN_RETURN_PCT = 0.0
OLLIETRADES_SIGNAL_MIN_AGREEING_MODELS = 2
OLLIETRADES_SIGNAL_MIN_CONVICTION = 0.75
OLLIETRADES_SIGNAL_MAX_PUSHES_PER_DAY = 3
OLLIETRADES_SIGNAL_LOOKBACK_MINUTES = 60   # how fresh a model's signals() row must be to count
OLLIETRADES_SIGNAL_RESOLUTION_WINDOW_DAYS = 5
```

## 11. What Phase 1 does NOT include (explicit, not an oversight)

- Real pushes (hard-disabled, §9).
- Live execution of any kind (Trade button has nothing to act on while pushing is disabled).
- Chart price-line overlay extension to `createLiveChart()` is scoped as a small follow-on if the base scorecard page ships first without it (render entry/stop/target as a table instead, upgrade later) — noted here rather than silently dropped.
- Auto-promotion Phase 1→2 — always an explicit Admiral action, never code-driven, matching every other gate in this codebase.
