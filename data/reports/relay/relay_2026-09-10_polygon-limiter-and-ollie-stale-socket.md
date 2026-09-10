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

**Fix shipped, round 1 (`engine/providers/ollama_provider.py`, `main.py`),
committed `ef321ff`:**
1. Shared `requests.Session` + `HTTPAdapter` with
   `Retry(total=1, connect=1, read=0, status=0, allowed_methods=frozenset({"POST"}))`,
   replacing a bare `requests.post()`. **`allowed_methods` was a required
   addition, not in the original spec** — urllib3 2.x's `Retry` excludes
   POST from its default allowlist (non-idempotent); without it,
   `connect=1` would silently never fire for this module's only verb and
   the whole mount would be a no-op.
2. Read timeout cut 180s→30s at the real live call site (`main.py:126`),
   plus the `_HM_WR_CANCEL_BUDGET_S` fallback 85→30. One true number
   instead of a documented-85/live-180 split.
3. On a caught `ReadTimeout`, discard the pooled connection then re-raise
   — **not** an app-level retry-and-resend (`/api/generate` has no
   idempotency key; a read-timeout can't be told apart client-side from
   "dead socket" vs. "still generating," so resending risks firing a
   second real generation on top of one that may still be in flight).
4. `[OLLAMA-CANCEL]` log line enriched with the actual exception text
   (was `type(e).__name__` only).

**Round 2 — second-opinion review, same day, three more accepted for the
13:00 bundle (not yet committed):**
1. **Don't pool at all for `/api/generate`.** The round-1 fix mounted a
   *shared, persistent* `Session` — which, on reflection, reintroduces
   exactly the class of bug this incident exposed (a keep-alive
   connection sitting idle across calls, vulnerable to being silently
   killed server-side between uses). Replaced with a fresh
   `requests.Session()` + `HTTPAdapter` constructed and closed inside
   `_do_request()` for every single call, plus an explicit
   `Connection: close` header. At this call volume (a handful of
   calls/min fleet-wide) a fresh TCP handshake costs nothing next to
   multi-second generation time, and this removes the failure class
   structurally rather than mitigating it. The `Retry(connect=1, ...)`
   mount stays — for genuine connect-phase failures *within* one call
   (DNS, refused, connect timeout) — but there's no longer a persistent
   pool for it to protect across calls. The discard-on-`ReadTimeout` step
   from round 1 is now moot (nothing persists to discard) and was
   removed.
2. **Timeout, retry count, and the Polygon cap moved to `config.py`**
   (env-backed, same `os.environ.get(...)` pattern the rest of that file
   already uses): `OLLAMA_GENERATE_TIMEOUT_S` (30), `OLLAMA_CONNECT_RETRY_TOTAL`
   (1), `POLYGON_LIMITER_CAP_PER_MIN` (100), `POLYGON_LIMITER_LIVE_RESERVED_PER_MIN`
   (50). `main.py` and `ollama_provider.py` both now import
   `OLLAMA_GENERATE_TIMEOUT_S` from the same place — the 85-vs-180 split
   was two independent literals with no single source; that can't recur
   structurally now.
3. **Effective values logged at startup**, one line each, confirmed
   firing on import under `.venv`:
   `[OLLAMA-PROVIDER-CONFIG] generate_timeout=30s connect_timeout=5s connect_retry_total=1 pooling=disabled (fresh connection per call)`
   and
   `[POLYGON-LIMITER-CONFIG] cap_per_min=100 live_reserved_per_min=50 mode=shadow (env POLYGON_LIMITER_MODE=shadow)`.
   "What's live?" is now a log line, which is the whole thing today's
   85-vs-180 confusion cost.

All changes (both rounds) `py_compile`-clean and import-tested live under
`.venv` (Python 3.14, the actual trader interpreter) — session/retry/
adapter construction and both startup log lines confirmed firing at
import time, no errors.

**`tests/test_ollama_cancel_on_timeout.py` — fixed properly, not just the
reported symptom.** The Captain's diagnosis (`_StubQueue.submit()` missing
the `timing=` kwarg, added by HM-OLLIE-QUEUE-CONTENTION-2026-09-09 after
this stub was last updated) was correct but not sufficient: applying only
that fix took 4 failures → 3, with the 4th now passing *by accident* (a
real `ConnectionError` from `localhost:11434` refusing the connection
happened to match the expected exception type). Root cause of the other
3: round 2's no-pooling rewrite replaced the module-level `requests.post()`
call with a fresh `Session().post()` per call, so
`patch("...requests.post")` no longer intercepts anything and the tests
fell through to real (unmocked) network calls. Fixed by moving the patch
target to `requests.Session.post` in all four tests, which applies to
every ad-hoc `Session` `_do_request()` constructs. Also added an
assertion pinning the new `Connection: close` header. **All 7 tests in
the file pass now, deterministically** — confirmed with a second run.

**Live smoke-test finding, verified just now — an unresolved, ongoing,
server-side condition, separate from and not fixed by today's client-side
change:** the first smoke call used a literal `192.168.1.168` (not
sourced from config, and moot regardless — `config.OLLIE_URL`/
`OLLAMA_URL` are both already `http://100.95.195.20:11434` live, no LAN
address in the loop at all). The second call used the real Tailscale
address, `100.95.195.20`, and also failed — not a routing/sandbox
artifact. Isolated with three follow-up checks:
- `GET /api/ps` on `100.95.195.20:11434` → 200 OK in 0.01s, `gemma3:4b`
  and `qwen3:8b` both shown resident.
- `POST /api/generate` with `gemma3:4b` → succeeded in 0.19s.
- `POST /api/generate` with `qwen3:8b` (raw, no pooling, no queue,
  brand-new connection) → hung the full 60s given, zero response.

Cross-checked against live production: `decision_audit` shows the same
`Read timed out (read timeout=180)` pattern still firing at 18:48:53 and
18:51:59 UTC (minutes before this check) — **the incident is still
actively happening**, not resolved on its own since this morning. Every
one of today's 67 `[OLLAMA-CANCEL]` events is `model=plutus-v1` — zero on
any other model — and `plutus-v1`/`qwen3:8b` are the same underlying
weights (HM-OLLAMA-ALIAS-2026-08-27). Sampled across the full day
(09:58→11:51 local): continuous, unbroken, still ongoing.

**Conclusion: this is not a network/routing/pooling problem at all — the
host and every other model respond instantly; something specific to
`qwen3:8b`/`plutus-v1`'s serving path on olliemax has been stuck since
09:35 this morning and still is.** Today's client-side fix (fresh
connection per call, 30s timeout, proper logging) reduces the cost of
each hit from ~180s to ~30s and makes it observable — it does not address
whatever is wedged server-side. McCoy may keep hitting this after the
13:00 restart, just cheaper per-hit. Needs attention on olliemax itself
(Ollama's own logs, that model's worker/slot state) — flagged to the
Captain live, not deferred to a backlog item, given the time-sensitivity.

**CLOSED 11:58 local — root cause confirmed, cleared, no trader-side
change involved.** Captain's diagnosis: a stuck runner slot on olliemax,
fixed with `systemctl restart ollama`. Verified from bigmac's `.venv`
over the tailnet: `qwen3:8b` returned 200 OK in 11.7s (cold load) where
it had been hanging the full 180s minutes earlier.

**Post-recovery check:** one `[OLLAMA-CANCEL]` at exactly 11:58:25
(`wall=142.78s reason=ConnectionError` — shorter than the usual 180.01s
and `ConnectionError` rather than `ReadTimeout`, consistent with being
the in-flight call caught mid-request by the `systemctl restart` itself,
not a new/recurring wedge). **Zero events in the ~4 minutes since**
(11:58:25 → 12:02:28, when this check ran) — clean so far, though that's
a short window; the real confirmation is continued silence over the next
30-60 minutes, not asserted here as fully proven.

**Final incident cost, corrected from the earlier running estimate:**
**70** `[OLLAMA-CANCEL]` events total today (09:35:50 → 11:58:25, the
one boundary event included), summed `wall=` time **12,563.52s ≈ 3.49
cumulative hours** (computed precisely from each event's logged wall
time, not assumed-uniform 180s × count — the sum exceeds the ~2h22m
wall-clock incident duration because `OLLAMA_QUEUE_WORKERS=2` allows two
concurrent inferences, so two separate hung calls could and did overlap
in time). Reason breakdown: 69 `ReadTimeout`, 1 `ConnectionError` (the
11:58:25 boundary event above).

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

**Generalized by the second-opinion review** into its own backlog item
(below) — a reusable dead-man's-switch monitor class, not just this one
check.

## Filed to `docs/XO_BACKLOG.md` — Ollama connection-reuse hardening,
## round 2 (after-close, not today)
Four follow-ups from the second-opinion review, explicitly kept out of
the 13:00 bundle:
1. **Streaming with an idle-timeout** instead of wall-clock on
   `/api/generate` — a wall-clock budget kills a call that's genuinely
   still producing tokens slowly (cold model load); an idle-timeout (no
   bytes for N seconds) only kills calls actually stuck. Needs
   `stream=True` + incremental NDJSON reading — a bigger structural
   change than this incident's fix.
2. **A cheap `/api/ps` health gate before each scan cycle** — confirm the
   target model is resident/responsive before committing a full cycle to
   it, so a hang degrades to "skip this cycle" instead of eating a full
   timeout per affected call.
3. **An integration test that hangs a stub server** (accepts the
   connection, never responds) and asserts the pool doesn't hand back the
   dead socket for a later call — real regression coverage for the
   failure class itself, not just the cancel-and-log behavior already
   covered.
4. **Highest value: a reusable dead-man's-switch monitor class** for
   `scripts/hm_ops_sentinel.py` — alert when expected work does NOT
   happen, not just when something errors loudly. Three of this week's
   real findings were silence, not errors (McCoy's decision rate
   silently dropping — this incident; Worf producing zero decisions
   despite active+configured — roster item above; the general shape of
   "a periodic job that just... doesn't run, no exception anywhere").
   The `check_ollama_decision_health()` draft above is a first instance;
   this item generalizes it into a configurable class (table/column,
   "was active" floor, window) instead of a one-off.

Owner Scotty, target "after close." Full detail in `docs/XO_BACKLOG.md`'s
consolidated table.

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

**Possible connection, not confirmed:** Worf's model (`qwen3-8b-flash` →
`qwen3:8b`) is the *same underlying weights* the live smoke-test found
stuck on olliemax (`plutus-v1` is an alias of `qwen3:8b`, HM-OLLAMA-ALIAS
2026-08-27). If that server-side wedge is per-model rather than
per-alias-name, Worf's calls would hang the same way McCoy's do. Doesn't
fully explain Worf's silence on its own — today's 67 `[OLLAMA-CANCEL]`
events are all logged as `model=plutus-v1`, none as `model=qwen3:8b`,
suggesting Worf isn't even attempting calls rather than attempting and
timing out — but worth checking together rather than as two unrelated
mysteries.

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
  backend, not signal quality. **Update, later same session:** the
  client-side fix reduces the cost of each hit (30s vs. 180s) but a live
  smoke-test found the server-side condition itself still active as of
  this update (see the live-verified finding above) — don't treat this as
  fully resolved until confirmed post-restart.

## Status
**Round 1 committed and pushed** — `ef321ff` on `exec-pipeline`.
**Round 2 + test fix committed and pushed** — `c9c1731`:
- `config.py` (new `OLLAMA_GENERATE_TIMEOUT_S`, `OLLAMA_CONNECT_RETRY_TOTAL`,
  `POLYGON_LIMITER_CAP_PER_MIN`, `POLYGON_LIMITER_LIVE_RESERVED_PER_MIN`)
- `engine/polygon_rate_limiter.py` (reads from `config`, startup log line)
- `engine/providers/ollama_provider.py` (fresh-connection-per-call, reads
  from `config`, startup log line)
- `main.py` (reads `OLLAMA_GENERATE_TIMEOUT_S` from `config`)
- `docs/XO_BACKLOG.md` (round-2 backlog item, corrected post-incident —
  see the `/api/ps`/`/api/tags` correction above — plus this session's
  final closure note)
- `tests/test_ollama_cancel_on_timeout.py` (mock target fixed for the
  no-pooling rewrite, `timing=` kwarg added to the queue stub — all 7
  tests pass deterministically)

**Incident status: CLOSED 11:58 local**, root cause confirmed
server-side (stuck olliemax runner slot, fixed by `systemctl restart
ollama`, no trader-side change involved) — see the closure note above.
Client-side fix (this relay doc, both rounds) ships in the 13:00 bundle
regardless — it's correct defensive engineering independent of today's
specific root cause, and materially reduces the cost of any future
recurrence (30s vs. 180s per hit, proper logging, no pooling to go
stale). Final cost: 70 events, 3.49 cumulative hours — see above.

## Deploy-day closing numbers — tomorrow's baselines

Restart landed 13:08:20 local (PID 40893). Checked ~13:20:51 local
(~12.5 min post-restart, after-hours):

- **`[OLLAMA-CANCEL]` total: 70** — unchanged since the restart, zero
  new events. No recurrence. (Full-day final; the incident closed at
  11:58, nothing since.)
- **Polygon budget-exhausted (`would_fail_loud`), post-restart window
  only: 0** (of 638 total gated calls, 327 `would_throttle`, all 327
  served from cache as `would_serve_stale` — none needed to fail loud).
  `process_started_at` in the shadow report confirms this is scoped
  cleanly to post-restart (counters reset on process start, no
  pre-restart contamination).

**Important caveat, not a clean receipt yet:** `RiskManager.is_market_hours()`
returned `post_market` for this entire window. The shadow limiter's
fail-loud branch requires `in_market` to be true
(`tiered_rate_limiter.py`'s `gated_call`) — it **cannot** increment
after hours regardless of cap size. So today's 0 is confounded by
market being closed, not yet proof the 100/min cap holds under real
intraday load. **Today's 17,530 (pre-raise, mid-market) and today's 0
(post-raise, after-hours) are not a like-for-like comparison** — the
real test is tomorrow's first live market-hours reading against the new
cap. Use tomorrow's `would_fail_loud` count (market hours only) as the
actual regression metric, not today's post-restart number.

**Second confound found, and corrected same day PM before it could bite
tomorrow:** Polygon key rotation (Steve) landed after the 13:08:20
restart above, so the then-running process was still holding the OLD
key. Initial read was "stale until next restart" — **corrected: the old
key was already revoked at the provider**, so every Polygon call on that
process was actively failing auth, not just running on a soon-to-expire
credential. Market was closed, so restarted immediately rather than
waiting for tomorrow:

- Fresh backup: `data/backups/trader_pre-key-rotation-restart_20260910_134530.db`,
  `integrity_check=ok`.
- Restart `13:45:47`, PID **45372**, clean (single writer, orphan-free).
- Both `[CONFIG]` lines confirmed live on the new process:
  `[OLLAMA-PROVIDER-CONFIG] ...` at `13:45:48`,
  `[POLYGON-LIMITER-CONFIG] ...` at `13:46:49`.
- One direct Polygon fetch on the new key (`GET /v2/aggs/ticker/SPY/prev`):
  **HTTP 200**, `status: OK`, real data returned (`SPY prev close $762.40`)
  — not just a 200 with an empty/error body.

**Net effect: tomorrow's premarket cycle is no longer the first live call
on the new key — it already happened, verified clean, this afternoon.**
The market-hours confound from the section above still stands (today's
`would_fail_loud=0` is after-hours and not yet a cap-raise proof either
way), but the key-auth confound is now closed. Tomorrow's first
market-hours `would_fail_loud` reading can be attributed to cap behavior
alone.

## Offhost backup duration — check pending, not yet available

`scripts/offhost_backup.sh` runs on a `20:30 MST` cron entry — has not
run yet as of this update (13:2x local). Last night's run: **9,638s**
(vs. ~1,065s a week prior), with `signal-center/signals.db`'s pre-
retention 2.19 GB flagged as the likely cause (largest single DB in the
backup set, full-rsync'd). Post-retention (this session, step 4 of the
13:00 bundle): `signals.db` is now **180.2 MB**, a ~92% reduction.
Expect a materially shorter run tonight if that diagnosis was right —
not asserting a number ahead of the actual run. Will check once the
cron has fired and update this doc with the real duration.
