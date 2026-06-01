# Overnight Queue — 2026-06-01 (Scotty, autonomous)

Read-only / safe-to-fail. No execution, no order path, no agent/producer turned on,
shadow feed + W0 scoring left to accrue untouched, no bare launchd, sacred-data
(archive/rename only), frontend = diagnose/scope only. Decisions/restarts HELD for
Admiral morning review. Worked in phase order; committed incrementally.

## PHASE 1 — Validation readiness (read-only)

### DSR/PBO readiness vs graduation gate (DSR≥0.95 ∧ PBO≤0.30)
Computed from existing W0 `scored_predictions` (IN-SAMPLE historical) for the two proven
edges the shadow bridge re-emits:

| setup | n | DSR 1d | DSR 3d | DSR 5d | DSR 10d |
|---|--|--|--|--|--|
| **relative_strength** | 444 | 1.00 ✓ | 1.00 ✓ | 1.00 ✓ | 1.00 ✓ |
| bull_flag | 38 | 0.49 ✗ | 1.00 ✓ | 0.997 ✓ | 0.95 ✗(marginal) |

expectancy_R: relative_strength +0.41/+0.71/+0.52/+0.34 (1/3/5/10d); bull_flag +0.08/+0.73/+0.56/+0.33.

**relative_strength clears the DSR leg at every horizon (n=444).** bull_flag clears at 3d/5d
(thin n=38; fails 1d, marginal 10d).

### ⚠️ PBO leg is NOT honestly evaluable yet
CSCV PBO across only the 2 setups = **0.55 (flagged "fragile")** — but that's a **degenerate-N
artifact**: with n_strategies=2 the IS-best generalizes to OOS at ~coin-flip → PBO ~0.5 by
construction (median_logit −0.69). PBO is only meaningful over a LARGE config universe (cf. the
345-sweep's 36 strategies). **The PBO gate cannot be cleared/failed on a 2-setup matrix.**
GAP to surface: a meaningful PBO needs either (a) parameter-variant configs per setup, or
(b) per-symbol sub-strategies, accrued forward. Until then, DSR is the operative read and PBO
is "not yet computable."

### Forward shadow accrual
Shadow signals emitted **2026-05-31 20:09** (22 rows). **0 closed outcomes yet** — they need
forward trading-day bars; the 1d horizon first matures ~2026-06-02 (06-01 is Sunday). The
graduation gate's *forward* (true OOS) confirmation will populate over the coming sessions;
today's DSR is in-sample only.

### Distance to clearing (the gate I'm waiting on)
- **relative_strength:** in-sample DSR ✓ at all horizons. Remaining: (1) FORWARD shadow sample
  to confirm OOS (accruing), (2) a non-degenerate PBO. **Closest to graduation.**
- **bull_flag:** in-sample DSR ✓ at 3d/5d only; thin (n=38). Needs more sample + forward.
- **unusual_oi:** no closed history (new aggregate-flow setup); accruing from zero.

### W0 forward-scoring health
- All **22 shadow signals tracked** in `signal_outcomes` (0 untracked / stuck). Outcome-tracker
  daemon healthy; last_update = emit time (market closed Sunday → no price re-poll yet; will
  update tracked_high/low/current on the next RTH cycle). No stuck-untracked signals.
- Shadow boundary intact: 0 shadow-era trades; executor chokepoint + consumer skip both live.

**HOLD items for Admiral:** none blocking — relative_strength is the lead graduation candidate
(in-sample DSR ✓); awaiting forward shadow accrual + a non-degenerate PBO before any go.

## PHASE 2 — Bug diagnosis (read-only; cause + proposed fix; implemented nothing)

**3. Morpheus binding "—" (NOT a timing race — payload shape-drift).**
`/api/morpheus/awareness` top-level keys are advisory/cockpit/commits/daily_snapshot/…/signals/
sources_loaded/ts — there is **NO top-level `regime` and NO `total_records`** (both return None).
But the JS binds `#h-regime ← regime.label` (signal-center/index.html:1756) and `#db-records ←
s.total_records` (:2546). The payload was restructured; regime now lives under `daily_snapshot`
(master_grade) / predictions, and record counts under `/api/stats`. → bindings read a stale shape
→ render "—". **Fix (additive backend, signal-center):** add top-level `regime` + `total_records`
to `/api/morpheus/awareness` so the existing bindings resolve (cleanest); OR re-point the JS to
the current paths. HOLD (signal-center restart + browser smoke).

**4. Scanner contradiction (phase vs market_open) — two clocks.**
`dashboard/app.py::scanner_status` (14058) computes `phase` from raw time-bins (14068-70:
"pre-market"/"market-open") but `market_open = RiskManager.is_market_hours()` (14075) separately
→ on a holiday/weekend, time-bin says "market-open" while is_market_hours()=false. **Fix:** derive
BOTH from `engine/market_calendar.py` — `phase` from `get_market_status()`, `market_open` from
`is_us_market_open()`. One source of truth (the documented reuse). HOLD (verify + restart).

**5. Movers excludes 553/553 when closed — no session-aware fallback.**
Movers/scanner nuke the full universe off-hours (same class flagged in W1 spec §2.2 "scanner nukes
553/553 on weekends"). **Fix:** session-aware guard via market_calendar — when closed, serve the
LAST SESSION's movers (cached) labeled "as of <last close>", instead of excluding all. HOLD.

**6. Master-Score 100 vs 61 — two metrics, one label.**
The Matrix reads `daily_snapshot.master_score` (=100 today) which was stored as
`int(predictions[0].master_score)` at snapshot time — i.e. the TOP single pick's score (caps at
100). The header shows a composite/aggregate (~61, live-computed). Two different metrics both
labeled "Master Score." **Fix:** standardize both on one definition (composite recommended), OR
relabel (Matrix = "top-pick score", header = "composite"). HOLD (design decision).

**7. /api/health nulls — mapped.** (dashboard/app.py)
- `last_ollama_success` (3897-3905): from the Ollama queue's last-success ts; null when
  `queue.total_requests=0` (noted unreliable at 3510). Populate from a per-call success stamp.
- `scan_health` (3913-3916): `_gw().get_scan_health()` — null if the gateway/watchdog
  (`ai_brain.record_scan_health`, engine/ai_brain.py:780) hasn't recorded yet.
- `websocket_status` (3921-3934): "connected"/"polling"/"unknown" from the WS/tape state — "unknown"
  when neither branch matches.
- `dayblade_last_scan` (3950-3964): parsed from a log marker; null if the marker absent today.
**Fix (Phase 5c candidate):** wire each to its real source with a sane default; feeds the W1 health grid.

**8. Crew roster empty-E — 6 active/0-trade agents.**
active=21; **0-trade active agents:** enterprise-computer (metals tracker), holly-scanner (shadow A/B),
ollama-coder (code review), quark-ic (rules/IC), qwen3-14b-pro, qwen3-8b-sonnet. Several are
non-trading roles by design. **Fix (frontend, design-only):** a collapsible "Dormant / non-trading"
drawer in the crew roster so the active list shows real traders; 0-trade or grade-E → drawer. HOLD
(frontend; needs browser smoke).

**9. LiveChart MutationObserver console error.**
The throwing observer is the HM-BJ ticker-linkification observer (`new MutationObserver(schedule)`
at index.html:2628, watching .ls-sym/.at-risk-sym etc.). LiveChart (9799+) mutates DOM at high
frequency (tick updates) → the `schedule`→linkify handler throws on an unexpected/removed node
mid-mutation. **Fix:** wrap the observer callback in try/catch AND exclude the live-chart subtree
(`#liveChart*`) from observation (it has no linkifiable tickers). Diagnose-only (exact throw line
needs the console text). HOLD.

**10. DOM/JS bloat — measured.**
`index.html` = **2405 KB**, **168 `<script>` blocks**, ~**11,276 elements**, 52 lazy `registerSectionInit`
sections. The 52 sections lazy-INIT their JS but all section HTML+scripts ship in the one 2.4MB file
(full DOM parsed at load). **Lazy-load scope (analysis only):** (a) defer/consolidate the 168 inline
scripts; (b) move per-section HTML to templates injected on `showSection` (biggest win, big refactor);
(c) split the single file. Est. ~11k elements → a few k visible-at-load. Design-only.

**11. Security surface — REPORT (changed nothing). ⚠️ HIGHEST SEVERITY.**
- **Mutation endpoints have NO app-level auth guard:** `/api/alpaca/buy`, `/api/alpaca/sell`,
  `/api/alpaca/close/{sym}`, `/api/alpaca/close-all`, `/api/autopilot/toggle`, `/api/kill-switch`,
  `/api/gateway/kill-switch/{id}` — no Depends/session/token check in their signatures. Confirmed
  `/api/*` is OPEN on loopback (e.g. `/api/kill-switch/history` → 200 unauth). **Protection rests
  ENTIRELY on the network layer** (127.0.0.1 bind + whatever the Cloudflare tunnel enforces). IF the
  tunnel forwards `/api/*` without 2FA (the HTML `/` IS gated, but API paths returned data unauth),
  these order/kill/autopilot endpoints are internet-reachable. **Recommend:** (1) verify the tunnel
  gates `/api/*` not just `/`; (2) add app-level auth (session/token) to ALL mutation endpoints —
  defense-in-depth, don't rely solely on the network. **VERIFY-then-fix; held for Admiral.**
- **/api/stats path leak CONFIRMED:** signal-center `/api/stats` (server.py:1774) returns
  `"/Users/bigmac/autonomous-trader/signal-center/signals.db"` in its body — filesystem path
  disclosure. **Fix (Phase 5b):** strip the path from the response. (Trader has no /api/stats → 404.)

## PHASE 3 — State/hygiene sweeps (read-only)

**12. Doc-drift sweep (CLAUDE.md + docstrings vs runtime).**
- ✅ already fixed this session: neo-matrix docstring ("exit_only"→active), super-agent two-book
  routing (listed live→annotated halted).
- **Fleet-count line (CLAUDE.md:531) is stale:** claims "20 active … as of 2026-05-28"; runtime =
  **21 active** (holly-scanner added for the Holly A/B, never halted). 3-day drift. Doc-only fix.
- Port/bind claims (8080 / 9000 / 127.0.0.1 loopback) — **match runtime ✓.**
- Pattern: doc-vs-runtime drift is RECURRING (neo, super-agent, fleet count, prior navigator/
  scanner "NOT dead" corrections). **Recommend** a periodic doc-reconcile (or a tiny script that
  diffs `ai_players` halt states + counts against the roster) so this stops accreting. Design-only.

**13. Halt-mode-vs-intent reconcile (all agents).**
Runtime: **21 active / 6 exit_only / 45 full.** Reconciled against the documented roster:
- No dangerous "documented-active-but-silently-halted" found beyond super-agent (already handled).
- `mccoy-bps`=full is the retired BPS variant (NOT the McCoy voter = `ollama-plutus`, active) — OK.
- Worf (`qwen3-8b-flash`) active is INTENTIONAL (kept for WR bridge-vote, per the Worf reconcile) — OK.
- Only drift = the count (20→21, holly-scanner) — doc lag, not a runtime problem.
**Verdict:** halt states are consistent with intent; the one true drift (super-agent) is fixed.

**14. Source-freshness sweep (W1 /api/sources/health).**
Summary now: 11 GREEN · 0 AMBER · 6 RED/UNKNOWN · 2 RETIRED · 1 quarantined.
- **Both live_decision sources GREEN:** bridge_consensus (2h), riker_synthesis (0s — recovered;
  was UNKNOWN earlier, its ts now resolves). **No silently-stale live feed.** ✓
- Stale but context-only (non-gating): cto_briefing 11d (known), execution_log 2d (minor —
  Morpheus exec log idle), macro/metals/schwab UNKNOWN (no ts source / manual). webull archive.
- No NEW silently-stale feed beyond the known ones.

**15. Daemon reboot-survival inventory — SYSTEMIC GAP confirmed at scale.**
- **SURVIVE SSH-only reboot:** 4 `@reboot` cron wrappers (trader, signal-center, cloudflared,
  swingdesk) + **28 time-based cron** jobs. These are the resilient set.
- **DIE on SSH-only reboot:** ~**24 `com.ollietrades.*` / `com.trademinds.*` LaunchAgents** present
  in ~/Library/LaunchAgents — and **`launchctl list` shows ZERO of them loaded right now.** Same
  failure mode that killed etfregime/optionsflow: gui/501 domain isn't bootstrapped on a headless
  reboot. Dead jobs include: danelfin-update, iv-backfill, model-watcher, ti-email-poller,
  ti-picks-watcher, uhura-watch, movers-poller, nightly-backtest/regression, enrichment-poller,
  archer-briefing, premarket, scanner, mcp, stale-trim-obs, universe-refresh, ollama-keepalive …
- Some are redundant (signal-center/tunnel ALSO have @reboot crons — the cron is the live path,
  the plist is dead-but-harmless). Others have NO cron equivalent → genuinely dead since the last
  reboot. This is the broader confirmation of the `DAEMON-GRAVEYARD-REHOME-PLAN-2026-05-30` (16
  cataloged); the real count of unloaded LaunchAgents is ~24.
- **Rule (load-bearing):** on this box, ANY bare LaunchAgent is non-reboot-survivable. Re-home the
  ones with real function to `@reboot`/time cron (the graveyard plan, Admiral-gated). HOLD.

## PHASE 4 — Design specs → drafts/ (design-only, no build)
Five specs written (each grounded in this session's W0 edges / canonical GEX / validation gate /
shadow-bridge pattern; all observation-first, execution gated on graduation + Admiral go):
- **SPEC_W2_BRACKET_SIZING.md** — fixed-fractional 0.5–1%/trade → ≤0.25× Kelly post-graduation;
  correlation/exposure + earnings/IV blackout + P95-DD sizing; observation-only hook logs size on
  shadow signals for W0 ("sized R" vs "raw R"), no buy().
- **SPEC_W3_GAMMA_STRATEGY_MAPPER.md** — canonical GEX → structure: pos-gamma between walls=iron
  condor at walls; approaching call wall=fade/short-call-spread; neg-gamma=directional/debit;
  strikes anchored to flip/walls. Emits shadow-gex signals.
- **SPEC_W3_UNUSUAL_OI_SMART_MONEY.md** — ranked unusual-OI from the flow_gex aggregate (vol/OI,
  notional), tier-compliant; print-level (sweep/block/aggressor) flagged as a Polygon tier DECISION.
- **SPEC_W4_REGIME_CONDITIONAL_ROUTING.md** — expectancy sliced by gamma-sign × VIX-term × time-of-day;
  router surfaces a setup only where it has graduated edge in the live regime. Start LOGGING the
  regime vector on shadow signals now so buckets accrue.
- **SPEC_W1_FRONTEND.md** — health grid (replaces "13/13 loaded"), per-tile as-of stamps, NTFY
  auto-quarantine (>3 RED cadence periods). Frontend → Admiral browser smoke before ship.

## PHASE 5 — Guard-railed additive fixes (Phases 1–4 were clean → proceeded)
- **(a) /api/trades ?limit= — HELD (already bounded).** The endpoint ALREADY has `limit: int = 500`
  with all filters pushed to SQL before LIMIT — it is NOT an unbounded fetch. Lowering the default
  to 100 would truncate existing consumers (dashboard trades view expects up to 500). No safe
  additive fix needed; lowering the default is a product DECISION → held for Admiral.
- **(b) /api/stats path leak — SHIPPED ✅.** Removed `"db_path": DB_PATH` from the signal-center
  `/api/stats` response (server.py:1789). Verified: response keys now {daily_snapshots, newest_record,
  oldest_record, total_records, unique_signals} — **db_path gone**, total_records intact (90192).
  py_compile OK; signal-center restarted (PID 59916); trader untouched.
- **(c) /api/health nulls — HELD.** Item-7 showed the fields ARE wired (last_ollama_success,
  scan_health, websocket_status, dayblade_last_scan all read real sources). The nulls reflect
  genuinely-IDLE sources (dayblade halted → no scan; Ollama queue total_requests=0; WS in polling/
  unknown), NOT unwired fields. "Wiring" empty sources to fake values would MASK the real idle
  state. Correct fix = render them as "idle/stale" in the W1 health grid (SPEC_W1_FRONTEND), not
  fabricate. Held — needs the W1 frontend + an Admiral call on default semantics.

### Phase 5 restart accounting
Only signal-center changed (b) → one signal-center restart (low-risk read aggregator, same as W0/
GEX ships). **NO trader restart** (a & c held → zero trader code change). No broken code left;
nothing reverted (the one shipped change verified clean).

---

# CONSOLIDATED REPORT — overnight 2026-06-01

**Shipped (verified, safe):** /api/stats path-leak strip (b). That's the only code change overnight;
everything else was read-only diagnosis/specs per the queue rules.

**Shadow feed / W0 (left to accrue, untouched):** 22 shadow signals tracked in signal_outcomes, 0
executed, boundary intact. **relative_strength clears the DSR leg at all horizons (1.0, n=444)** —
lead graduation candidate; awaiting forward shadow sample + a NON-degenerate PBO (2-setup PBO is a
coin-flip artifact, not a real read). bull_flag clears DSR at 3d/5d only.

**HELD for Admiral (decisions / restart-to-verify / frontend):**
1. ⚠️ **Security (highest):** mutation endpoints (alpaca buy/sell/close-all, autopilot/toggle,
   kill-switch) have NO app-level auth — protection is network-only. Verify the Cloudflare tunnel
   gates /api/* (not just /); add app-level auth as defense-in-depth.
2. Morpheus binding "—" → add regime/total_records to /api/morpheus/awareness (SC restart + browser smoke).
3. Scanner phase-vs-market_open + movers-when-closed → single source of truth = market_calendar (restart-verify).
4. Master-Score 100-vs-61 → standardize on one definition (design decision).
5. /api/trades default 500→100 (decision); /api/health nulls → render idle, not fake (W1 frontend).
6. Crew empty-E "dormant drawer", LiveChart MutationObserver guard, DOM-bloat lazy-load (frontend → browser smoke).
7. ~24 LaunchAgents dead (0 loaded) — reboot-survival re-home (DAEMON-GRAVEYARD plan, Admiral-gated).
8. Fleet-count doc 20→21 stale; recommend a periodic doc-vs-runtime reconcile script.
9. Wave 2/3/4 + W1-frontend specs in drafts/ — review before any build.

**All execution remains OFF; all frontend HELD for your browser smoke; no bare launchd touched;
no .db deleted; nothing left broken.**

## GROUP A SHIPPED — 2026-06-01 (Admiral approved; relabel Master-Score)
All py_compile clean; signal-center + trader restarted (canonical), single-writer verified.
- **(3) Morpheus header — SHIPPED.** Added `regime{regime,label}` + `vix{current:{vix}}` + `total_records`
  to `/api/morpheus/awareness`. Verified: regime=BULL, vix=15.78, total_records=90192 → header
  `scoreRegime(data.regime)`/`scoreVix(data.vix)` now resolve. (db-records is a SEPARATE frontend
  path — loadStats()→/api/stats, already works; not an awareness gap.)
- **(4) Scanner clock — SHIPPED.** phase + market_open both from `engine/market_calendar`
  (get_market_status/is_us_market_open). Verified: phase=pre-market, market_open=False, AGREE=True
  (no more holiday/weekend contradiction).
- **(5) Movers session-guard — SHIPPED.** `get_market_movers` returns last-session disk cache when
  `not is_us_market_open()`. Correct + additive; live benefit is RTH-dependent (SWR cold right after
  restart; disk-cache source warms during RTH). Not broken.
- **(6) Master-Score RELABEL — SHIPPED.** Header "Score:"→"Composite:" (the weighted indicator
  composite, ~61); Matrix "Master Score"/"Score:"→"Top Pick Score"/"Top Pick:" (the top single
  pick, =100). Two distinct metrics now distinctly labeled.
- **(7) /api/health idle-sentinels — SHIPPED.** null→"idle" for last_ollama_success/dayblade_last_scan,
  "unknown" for websocket_status, {"status":"idle"} for scan_health. Verified.
Restarts: 1 signal-center (3,6) + 1 trader via trader_restart.sh (4,5,7). Nothing reverted; nothing broken.
