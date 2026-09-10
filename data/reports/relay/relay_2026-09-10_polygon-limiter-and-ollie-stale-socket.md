# Relay — Polygon limiter cap raise + Ollie stale-socket fix, 2026-09-10

## What this covers
A live mid-morning session (10:52–11:30 local) pulled the Polygon limiter
cap raise forward from the after-close queue, then branched into two more
threads the Captain flagged from live 11:00 numbers: a post-deploy
invalidation-fill/stop-distance read, and a root-cause chase on McCoy's
confidence drift that led to a real, diagnosed, and fixed Ollama
connection bug. Read-heavy investigation with four staged code/doc
changes, none deployed yet — all four are folded into the 13:00 bundle
below. Nothing on this list has been committed or restarted.

## Staged, not yet deployed (13:00 bundle)

Final order, confirmed by the Captain:

1. Phase 1.1 acceptance read (pass rate + stop-distance distribution)
2. **This session's Ollama connection fix** (below)
3. **This session's Polygon limiter cap raise** (below)
4. signals.db backfill
5. Polygon key rotation
6. bk_orb direct-path pagination re-scope
7. Bridge cosmetics

GPU power-limit raise on olliemax (200W→250W) is the Captain's own
after-restart action, not part of this bundle's code changes.

### 1. Polygon rate limiter cap raise (4/min → 100/min)
`engine/polygon_rate_limiter.py`: `CAP_PER_MIN` 4→100, `LIVE_RESERVED_PER_MIN`
2→50 (same 50% live-reserved ratio preserved). This was already queued in
`docs/XO_BACKLOG.md` ("5/min → ~100/min... Massive Stocks Starter went
live 9/9 night") for after-close — pulled forward because the shadow
report was showing high simulated-exhaustion volume during market hours.

**Correction made during triage:** `POLYGON_LIMITER_MODE` is currently
`shadow`, confirmed live from `data/polygon_limiter_cache_shadow_report.json`
— shadow mode never actually throttles a real call (the module's own
comment in `market_data.py` is explicit: fetch always runs for real in
shadow; only `enforce`, not active, would change behavior). So today's
~17,530–17,818-and-climbing "budget exhausted" events were simulated
telemetry against the stale 4/min cap, not real throttling. Real live
impact today was 151 genuine Polygon HTTP 429s — a separate code path
(`market_data.py`'s `_set_polygon_limited()` 60s global cooldown),
unrelated to this cap.

Both `CAP_PER_MIN`/`LIVE_RESERVED_PER_MIN` are baked into a module-level
`TieredRateLimiter` singleton at import time — **requires a restart** to
take effect (unlike `POLYGON_LIMITER_MODE`, which is read fresh per call).
`py_compile` clean.

**Regression metric:** tomorrow's `would_fail_loud`/"budget exhausted"
count against the new 100/min cap, same shadow-report/log source as
today's baseline.

### 2. McCoy Ollama connection fix — root cause diagnosed by the Captain
**Diagnosis (Captain, live-verified from bigmac):** not the network, not
Ollama itself. Direct `/api/generate` over the tailnet returned in 1.9s,
tailscale ping 1ms direct (no DERP), olliemax load 0.00, GPUs 0%, Ollama
serving at 85 tok/s with idle slots. The failure is a **dead pooled
keep-alive socket**: the server had already closed it, the client reused
it from the connection pool, the write succeeded with no immediate error,
and the call then blocked in `recv()` for the full read-timeout budget
with nothing ever coming back
(`HTTPConnectionPool(host='100.95.195.20', port=11434): Read timed out`).
Onset 09:35 local, ~3 min after an olliemax runner reschedule at 09:32
dropped existing sockets.

**Two corrections to the initial framing, found during implementation:**
- The client read-timeout budget was actually **180s live**, not the 85s
  suggested by `engine/providers/ollama_provider.py`'s own
  `_HM_WR_CANCEL_BUDGET_S` constant/docstring. That constant is only the
  `__init__` fallback for callers that don't pass an explicit `timeout=`
  — the real scan-path Arena wiring (`main.py:126`,
  `initialize_arena()` → `build_all_providers(default_timeout=180)`, the
  single source of provider routing per its own HM-CN Phase 2 comment)
  passed 180 explicitly. Confirmed against data: all `[OLLAMA-CANCEL]`
  log lines today show `wall≈180.0s`, not 85s.
- The `[OLLAMA-CANCEL]` warning log line **already reaches
  `trader_error.log`** (stdlib logger → Logging Sink Split doctrine) — 52
  of them today as of 11:23:27 local. The original ask ("these exist only
  as decision_audit rows, no log-based monitor can see them") was half
  right: the log line exists, but (a) it only recorded
  `type(e).__name__` ("ReadTimeout"), not the actual exception text, and
  (b) no monitor currently reads it — a sentinel-check gap, not a
  logging gap (see Proposed, not applied below).

**Fix shipped (`engine/providers/ollama_provider.py`, `main.py`):**
1. Shared `requests.Session` + `HTTPAdapter` with
   `Retry(total=1, connect=1, read=0, status=0, allowed_methods=frozenset({"POST"}))`,
   replacing a bare `requests.post()` against the module-level default
   pool. Lets urllib3 proactively detect+discard a cleanly-closed pooled
   connection before writing to it. **`allowed_methods` is a required
   addition, not in the original spec** — urllib3 2.x's `Retry` excludes
   POST from its default allowlist (non-idempotent); without it,
   `connect=1` would silently never fire for this module's only verb and
   the whole mount would be a no-op. Confirmed via `.venv` import test.
2. Read timeout cut 180s→30s at the real live call site (`main.py:126`),
   plus the `_HM_WR_CANCEL_BUDGET_S` fallback 85→30 for consistency (no
   other caller of that fallback — `engine/shadow_csp.py` — needs more
   than 30s; p99 real generate today is ~13s). One true number now
   instead of a documented-85/live-180 split.
3. On a caught `ReadTimeout`, `_session.close()` (discard pooled
   connections) then re-raise — **not** an app-level retry-and-resend of
   the same failed call. Deliberate: `read=0` in the Retry config was
   explicitly to avoid silently resending a genuinely slow generate
   (`/api/generate` has no idempotency key — a read-timeout is
   indistinguishable client-side from "dead socket" vs. "still
   generating," so auto-resending risks firing a second real generation
   on top of one that may still be in flight). Confirmed with the
   Captain: this reading (discard-not-resend) is correct and matches the
   `read=0` intent.
4. `[OLLAMA-CANCEL]` log line enriched with the actual exception text
   (was `type(e).__name__` only), so a log-only reader doesn't need to
   cross-reference `decision_audit` to see the underlying detail.

All changes `py_compile`-clean and import-tested live under `.venv`
(Python 3.14, the actual trader interpreter) — session/retry/adapter
construction confirmed to not error at import time.

**Real scope wider than first estimated:** `decision_audit`'s 31–34 rows
today only capture McCoy's scan-path decisions (via the
`paper_trader.py`/`ic_squadron.py`/`regime_router.py` INSERT path). The
log-based count — 52 `[OLLAMA-CANCEL]` events today — is the complete
picture and is meaningfully higher, meaning the **witness/War Room
provider paths were affected too, not just McCoy's scan cycle.** The fix
therefore helps more broadly than the original decision_audit-only
estimate suggested.

**Baseline / regression metric (Captain-confirmed to use log-based, not
decision_audit-based, counts — same signal both days):**
- **Today's baseline: 52** `[OLLAMA-CANCEL]` events (`grep -c
  "OLLAMA-CANCEL" logs/trader_error.log`), last at 11:23:27 local, all
  `wall≈180s reason=ReadTimeout`.
- **Tomorrow's regression metric:** same grep, post-fix (30s timeout +
  discard-on-timeout + retry-with-allowlisted-POST). Expect materially
  fewer, and any that do occur should show `wall≈30s` not `≈180s`.

### 3. MAX_LOADED_MODELS hypothesis — ruled out, not actioned
Original working hypothesis (before the Captain's direct diagnosis
above): `MAX_LOADED_MODELS=2` with three models wanting residency
(qwen3:8b, gemma3:4b, bge-m3) evicting mid-call. **Killed by the
Captain's own olliemax data: 5 model loads all day, no eviction
pressure.** No `MAX_LOADED_MODELS` change going into the 13:00 bundle.
Superseded by the dead-pooled-socket diagnosis above, which is the real
cause and is fixed independently of this.

## Proposed, not applied — sentinel visibility gap
A `check_ollama_decision_health()` design was drafted for
`scripts/hm_ops_sentinel.py`, matching that file's existing conventions
(`AlertTuple`, `check_lock_errors()`'s "any occurrence → alert" posture,
`check_collector_freshness()`'s market-hours gating). Two RED_ALERT
conditions: any `decision_audit` row with a read-timeout in a 15-min
trailing window, and any player with ≥3 decisions in the prior hour
dropping to zero in the trailing window (structurally-safe floor, no
tuned per-player baseline exists yet — same caveat this file's own FD
thresholds carry). **Not wired into `main()` or deployed** — held per
this project's Workflow doctrine (propose, get approval, then apply) for
a live 5-10min cron script that routes to pushover/email. Given the fix
above should sharply cut real occurrences, this may be worth revisiting
in scope once tomorrow's baseline comes in — not blocking the 13:00
bundle either way.

Given the `decision_audit`-vs-log undercounting finding above, if/when
this check gets built it should key off `trader_error.log`'s
`[OLLAMA-CANCEL]` (the complete signal) rather than `decision_audit`
alone (which misses witness/War Room path cancellations) — noted for
whoever picks this up.

## Filed to `docs/XO_BACKLOG.md` — roster reconciliation (separate from the
## Ollama connection fix, not folded in)
Found while triaging the timeout cluster: `qwen3-8b-flash` (Worf) is
`halt_mode='active'` and *is* in `config.AI_PLAYERS`, but produced zero
`decision_audit`/`crew_decisions` rows today — cause not diagnosed.
`options-sosnoff` (Troi) is `halt_mode='active'` in `ai_players` but
**absent from `config.AI_PLAYERS` entirely**, so it cannot produce a live
decision regardless of halt state. Distinct from `HM-ORPHAN-SEATS`
(2026-07-01 — `model_id` pointing at a model no longer served); this is
upstream of that — a seat can be active with a resolvable model and still
never fire because it isn't in the routing config at all. Filed as a new
row in the `docs/XO_BACKLOG.md` consolidated table, owner Scotty,
unslotted. Needs: audit all active `ai_players` rows against
`config.AI_PLAYERS` membership, diagnose Worf's silence, decide per seat
(re-wire or formally retire).

## Also verified this session, no action needed
- **Invalidation fill, post-deploy window (McCoy, restart boundary
  2026-09-10 03:43:25 UTC):** 841/873 = 96.3%, vs. 68.7% pre-deploy and
  86.6% blended whole-day — the blended figure was masking materially
  better post-deploy performance, not diluting it.
- **Stop-distance distribution, post-deploy (n=825 directional
  signals):** not clustered — mean 3.05%, median 2.71%, stdev 2.00%,
  smooth single-peak histogram 2.25–3.5%. McCoy varies stop distance
  per-signal.
- **Rejection reasons today:** LOW_CONVICTION (60-62% band) 433,
  confirmed exact. Regime block is two gates summing to 299 (vs. quoted
  296, consistent with ~8min of elapsed growth) —
  `regime_mismatch` (161) + `BEAR_CROSS avoid-list` (138). Ranking
  confirmed: LOW_CONVICTION > regime block. LOW_CONVICTION across all
  confidence sub-bands (479) is already today's single largest
  non-market-hours rejection category — worth knowing before anyone
  proposes moving the 65% floor.
- **McCoy confidence drift (0.80→0.74-0.77→0.20 through the day) —
  resolved as a measurement artifact, not a real signal-quality drop.**
  The 0.20 reading came from 21 rows in the last hour with
  confidence=0.0 tagged with an Ollama read-timeout error string (the
  same connection bug fixed above) dragging the naive average down.
  Excluding those, real confidence in that hour was 0.787 — matches
  baseline. Explicitly ruled out: model swap (zero swaps today), stale
  Polygon/GEX cache link (timeouts are against the Ollama host, not
  Polygon — mechanically unrelated). **Do not move the 65% LOW_CONVICTION
  threshold on today's data** — the underlying issue was McCoy's Ollama
  backend, not signal quality, and that's now fixed pending the 13:00
  restart.

## Status
Four changes staged this session, all `py_compile`/import-test clean,
none committed, none restarted:
- `engine/polygon_rate_limiter.py`
- `engine/providers/ollama_provider.py`
- `main.py`
- `docs/XO_BACKLOG.md` (docs-only, roster reconciliation item)

Holding for the 13:00 bundle in the order above.
