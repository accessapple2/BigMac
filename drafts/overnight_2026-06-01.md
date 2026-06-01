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
