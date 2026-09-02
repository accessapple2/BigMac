# GEX collector re-check + signal cadence verdict — closing the July audit and weekend performance-fix threads

**Investigate/report, one live diagnostic run of an existing collector
script (no code changes from that run — it correctly wrote nothing).**

---

## GEX collector — NOT a regression, NOT a live bug. Confirmed live, tonight.

**Short answer: the "fix failed its first live test" framing doesn't
match what's actually happening. The 08-29/08-30 fixes were correct and
complete — they fixed pacing/scheduling and, in doing so, correctly
diagnosed a real, code-unfixable blocker (a cancelled Polygon
subscription), and the collector was deliberately re-disabled as a
result. The system has behaved exactly as intended since 08-30. Verified
fresh tonight, not just re-read from old logs.**

### Step 1 — is the Saturday fix loaded?

Yes, trivially — the process has been restarted twice today (13:00 and
16:44 MST), both long after the relevant commits:

| commit | date | what |
|---|---|---|
| `85b5f7e` | 2026-08-29 06:53 | "restore GEX collector + build collector-freshness sentinel" |
| `e55f330` | 2026-08-30 15:27 | "pace Polygon paging, stop treating 429 as end-of-chain" |

But "loaded" isn't the right question — **these two commits were never
meant to make `as_of` move forward on their own.** Two more commits the
same evening (`631e576`, `174a593`, both 08-30 15:36–15:46) patch
downstream consumers (Ready Room, 0DTE rules) to degrade gracefully when
GEX is stale/unavailable — the team's own paper trail shows they hit a
harder wall within ~20 minutes of the pacing fix and pivoted to
acceptance, not a silent stall.

### Step 2 — why didn't the staleness sentinel fire today?

Not a detector bug — **intentionally, explicitly suppressed.**
`data/.hm_ops_sentinel_acks.json` (already surfaced earlier today, this
session):

```json
"sentinel_collector_stale_gex_qqq": {
  "acked_at": "2026-08-31T14:45:45...", "acked_by": "Admiral",
  "ceiling": 999999.0,
  "note": "GEX collector RETIRED 8/30 (HM-GEX-RETIRED): Polygon options
           403 on free tier. Snapshot is permanently frozen at
           2026-07-21; staleness will only grow. Unack when the CBOE
           repoint lands."
}
```
(identical entry for `sentinel_collector_stale_gex_spy`). Ceiling
999999.0 is an effectively-infinite staleness tolerance — the sentinel
would fire if unacked; it's correctly silent because someone already
told it not to, pending a real data-source change.

### Step 3 — root cause, verified live tonight, not just read from logs

**Ran `scripts/hm_gex_daily_collect.py` directly, right now, to see
current behavior first-hand rather than trust old log lines or the
crontab comment alone:**

```
[options_flow_gex] SPY: chain TRUNCATED at page 0 (0 contracts, HTTP 403) -- discarding partial chain rather than scoring incomplete gamma
[options_flow_gex] QQQ: chain TRUNCATED at page 0 (0 contracts, HTTP 403) -- discarding partial chain rather than scoring incomplete gamma
[hm_gex_daily_collect] SPY: GEX error -- no chain/spot
[hm_gex_daily_collect] QQQ: GEX error -- no chain/spot
```

**Live, current, HTTP 403 on page 0 of the options-chain request, for
both tickers, right now.** Matches the crontab's own retirement comment
exactly:

> RETIRED 2026-08-30 (HM-GEX-RETIRED): /v3/snapshot/options 403
> NOT_AUTHORIZED -- options chain requires a paid Polygon/Massive plan,
> cancelled 7/22. Verified 8/30. Data preserved in data/flow_gex.db;
> see XO_BACKLOG.

This is a **subscription/billing-level block**, not a code defect —
no pacing, retry, or backoff fixes this; the account genuinely doesn't
have access to the endpoint anymore. `data/flow_gex.db`'s file mtime
(`Jul 21 13:05`) matches the API's reported `as_of` exactly — it's the
durable last-good snapshot, correctly never overwritten with a failed
fetch (`options_flow_gex.persist()`'s own collapsed-wall/partial-chain
guard discarded tonight's failed attempt too, as designed — confirmed
`flow_gex.db`'s mtime unchanged after running the script just now).

**One wrinkle worth naming plainly:** the collector script itself
(`scripts/hm_gex_daily_collect.py`) is present on disk (restored 08-29,
mtime `Aug 29 06:50`) but its crontab line is commented out — retired,
not removed. This is exactly correct process (matches "Archive Convention"
doctrine — code kept, not deleted) but it does mean nobody will discover
if Polygon's subscription is ever restored, since nothing re-attempts it
automatically. Resolution path is already on record in the ack:
"unack when the CBOE repoint lands" — a data-source change (Polygon →
CBOE for options chains), not a retry/pacing fix. Not actioned tonight.

### Step 4 — the safeguard, added to doctrine

Added to `docs/FLEET_LIFECYCLE.md`'s "What this doesn't cover (yet)"
section: a fix to a cron/launchd-scheduled job isn't "done" when it's
committed and the process restarted — it's done when the job has
produced one real artifact under its normal schedule. Full text of the
new bullet references this exact incident by name so it isn't abstract.
Not a code gate (nothing currently enforces this automatically), a
process rule.

### Step 5 — VWAP: still open, and it's a *different*, more precise gap than "market conditions"

**Not closed.** Checked whether VWAP was non-null at any point during
today's session and traced it further than that: **`/api/market/gex`'s
`vwap` field is structurally always `None` — nothing in
`engine/canonical_gex.py` or `engine/options_flow_gex.py` (the two files
that build the dict this endpoint serves) ever sets a `"vwap"` key at
all.** `dashboard/app.py:6707` reads `c.get("vwap")` from a dict that
never had one. This isn't "null today because the market's closed" or
"null because of the 403" — it would read `None` at 10am on a Tuesday
with a fully live options chain too, because the code path never
computes it for this endpoint.

The capability isn't missing from the codebase, just unwired here:
`dashboard/app.py:18476-18479` (a different endpoint) does populate a
real `vwap` via `engine.market_data.get_session_vwap()` — the same
`get_intraday_candles` cascade this session already patched today for
the 429-backoff fix. Fix path is obvious (call `get_session_vwap` inside
`_canonical_gex`/`canonical_gex()` and add it to the returned dict) but
**not done tonight** — report only, per instruction. This closes the
July audit item's *investigation*, not the item itself — it's still
open, now with a precise, code-level cause instead of an open question.

---

## Signal cadence vs. `ollama_model_swap_log` — eviction hypothesis REFUTED by today's data

**Verdict: the eviction hypothesis does not explain today's slow
cadence. `MAX_LOADED_MODELS` 1→2 (Saturday) produced a real but partial
improvement (~2x faster than the 86-91s regression), landing at a median
~46-47s — still ~6x slower than the 6-8s target — and the remaining gap
does not correlate with model-swap events at all.**

### Cadence, measured two independent ways, cross-validated

1. `decision_audit` (`event_type='signal_emit'`, `player_id='ollama-plutus'`,
   today): 950 signals, 949 gaps, **median 47.0s** (mean 90.8s, skewed by
   overnight/between-cycle pauses — p10/p25/p50/p75/p90 = 41/43/47/68/105s).
2. `trader_error.log`'s `ollama_call model=plutus-v1 agent=ollama-plutus
   wall=Xs` lines, today: 1,566 calls, **median wall-time 34.5s** (min
   3.3s, max 148.1s); inter-call-start gaps: 1,564 gaps, **median 46.0s**
   — matches method 1 almost exactly.

Compared against the 08-29 post-mortem's own baselines (median 6-8s
pre-regression, 86-91s post-regression, confirmed via a clean before/after
split at the plist-edit timestamp): today sits roughly halfway — real
improvement from the regression, real shortfall from the target.

### Correlation with `ollama_model_swap_log` — the actual test

For every one of today's 1,566 `ollama-plutus` calls, checked whether a
swap-log event (any model, either direction) landed in the preceding 15
seconds:

| | n | median wall-time |
|---|---|---|
| Call preceded by a swap event (≤15s) | 70 | **34.4s** |
| Call with no swap event nearby | 1,496 | **34.5s** |

**Statistically indistinguishable.** Only 4.5% of calls even have a swap
event nearby at all, and even those show zero measurable slowdown
relative to the other 95.5%. If eviction/reload were driving the slow
cadence, calls following a swap should show a clear latency spike — they
don't. Every call is uniformly ~34s regardless of eviction proximity.

### What this actually points to — and a complicating new fact

The 08-29 post-mortem's *other* hypothesis (`OLLAMA_FLASH_ATTENTION`/
`OLLAMA_KV_CACHE_TYPE` slowing raw inference, not `MAX_LOADED_MODELS`)
was never confirmed with an A/B revert at the time (blocked on sudo).
Cannot confirm it here either — but a new fact from today's own
investigation complicates the story further: **the process actually
serving requests today (`/opt/homebrew/bin/ollama serve`, PID 300, the
Homebrew process, not `com.ollama.serve`) has *neither*
`OLLAMA_FLASH_ATTENTION` nor `OLLAMA_KV_CACHE_TYPE` set at all** — those
vars were only ever added to `com.ollama.serve.plist`, which isn't the
process handling traffic (see the earlier same-day correction in
`relay_2026-09-01_four-item-followup.md` section 7). So today's ~34s
median can't be attributed to those specific vars either, since they're
absent from the process that's actually slow. It also has
`OLLAMA_CONTEXT_LENGTH=16384` set, which — without flash attention to
offset it — is a plausible contributor to per-call latency, but this is
not confirmed, only flagged as the next thing to check if this gets
picked up again.

**Bottom line: eviction is ruled out with real data. The 08-27
FLASH_ATTENTION/KV_CACHE_TYPE hypothesis can neither be confirmed nor
ruled out against today's actual serving process, because that process
never had those vars in the first place — a different, still-open
question than the one the 08-29 doc left blocked on sudo.**

---

## Quick check: S7 season tile

Confirmed healthy — `GET /api/season` returns `season=7`,
`name="Season 7"`, `day_number=52`, populated `config` (8 active agents
listed) — matches today's earlier `season_config` fix
(`relay_2026-09-01_four-item-followup.md` section 8). Nothing further
needed here.
