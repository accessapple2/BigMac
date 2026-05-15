# Scotty: HM-WAR-ROOM-LATENCY Scope Doc — 2026-05-15

**Status:** scope-ready. No code edited. Captain to prioritize + pick ship window.

**Trigger:** today's 107-minute War Room cycle (05:43 → 07:30 AZ), which
delayed the live trader's first signal by ~62 min past market open.

## Problem statement

`run_war_room` (in `main.py:1114`, wrapping `engine.war_room.run_war_room`)
iterates **synchronously** over all active LLM providers (~49 today) calling
`provider.call_model(prompt)` per provider. Each call has its own timeout
but no per-cycle wall-clock cap. If any provider stalls near its timeout,
the WHOLE cycle waits.

```
main.py:1114  run_war_room()
              └── threading.Thread(_war_room_thread)
                  └── get_bulk_prices(get_active_universe())   # bulk endpoint, ~25× faster
                  └── engine.war_room.run_war_room(providers, prices)
                      └── for pid, provider in providers.items():     # ← serial loop
                              generate_hot_take(provider, pid, symbol, ...)
                                  └── provider.call_model(prompt)     # ← can stall ≤ timeout
```

**Provider timeouts (per file inspection):**

| Provider | timeout | comment |
|---|---:|---|
| OllamaProvider | 180s | local Ollie box; default in constructor |
| MLXProvider | 180s | local mlx-community models |
| Grok | 90s | xAI API |
| Groq | 90s | groq cloud |
| Dalio | 60s | rule-based with HTTP fallback |
| Polygon | 10s | data only |
| OpenAI / base | (library default) | no explicit timeout — can be unbounded |

Worst-case latency = sum of all per-provider timeouts: `49 × 180s = 147min`
on Ollama-heavy fleet. The 107-min observation today fits the "several
Ollama providers stalled to their 180s timeout, others responded quickly"
pattern.

The `_war_room_running` guard at `main.py:1139` correctly suppresses
scheduler ticks while a cycle is in flight — but that means the next cycle
also can't start until the stuck one releases. During the 107-min window,
the 3-minute scheduler tick fired ~35× and skipped silently each time.
Trader was alive, market open, agents responsive — but no new debate
rounds, hence no new HM-AN2 signal consumption, hence no trades.

## What's already in place (don't re-build)

- **HM-BQ instrumentation decorator** (`main.py:43-51`) wraps many scheduled
  jobs with wall-clock timing. **It also wraps `run_war_room` at main.py:1113**,
  but only the outer dispatcher — which spawns the thread and returns
  immediately. The actual cycle wall-clock inside the thread is NOT
  captured by HM-BQ today.
- **NTFY infrastructure** (`engine.alert_channels.send_alert`) with
  AlertLevel.WARNING / RED_ALERT, topic `ollietrades-admin`. Ready to
  receive a "cycle exceeded threshold" alert.
- **War Room round-complete log** (`engine/war_room.py:937`) emits
  `"War Room round complete: N responses on SYMBOL"` — useful but
  records only count + symbol, not wall-clock.

## Proposed defense layers

Three layers, additive. Layer 1 is cheap diagnostics; Layer 2 prevents
the 107-min stall; Layer 3 reduces the per-provider drag.

### Layer 1 — Cycle duration logging + NTFY threshold (low risk)

Add a `time.perf_counter()` bracket inside `_war_room_thread` in
`main.py:1144-1163` (the daemon-thread body). Log wall-clock at the
finally-block. NTFY if wall exceeds a configurable threshold.

```python
def _war_room_thread():
    _war_room_running.set()
    _wr_t0 = time.perf_counter()
    _wr_symbol = "?"  # captured from _run_wr if possible
    try:
        prices = get_bulk_prices(get_active_universe())
        if prices:
            _run_wr(arena.providers, prices)
        else:
            console.log("[yellow]War Room: no prices available, skipping")
    except Exception as e:
        console.log(f"[red]War Room error: {e}")
    finally:
        _wr_wall = time.perf_counter() - _wr_t0
        console.log(f"[WR-DUR] cycle wall={_wr_wall:.1f}s")
        if _wr_wall > 600:  # 10-minute threshold
            try:
                from engine.alert_channels import send_alert, AlertLevel
                send_alert(
                    f"War Room cycle wall={_wr_wall/60:.1f}min "
                    f"(threshold: 10min). Scheduler will skip ticks "
                    f"until cycle releases.",
                    level=AlertLevel.WARNING,
                    alert_type="war_room_slow_cycle",
                )
            except Exception:
                pass
        _war_room_running.clear()
```

**Properties:**
- Zero behavioral change to in-flight cycles
- Immediate visibility into latency via log + NTFY
- NTFY rate-limit semantics (see CLAUDE.md "Alert rate-limit semantics —
  in-memory only") mean one alert per error class per process lifetime;
  for an alert_type='war_room_slow_cycle' this means up to N fires per
  N restarts, which is acceptable for our use
- Reveals the latency-vs-symbol distribution post-deploy (which symbols /
  which providers correlate with slow cycles)

### Layer 2 — Per-cycle wall-clock cap (medium risk)

Inside `engine/war_room.py:run_war_room` (line 797+), add a deadline at
the start of the function and check it inside the providers loop. When
deadline passes, finish the cycle with whatever takes were already
collected.

```python
def run_war_room(providers: dict, prices: dict):
    ...
    _wr_deadline = time.time() + _WR_CYCLE_BUDGET_S  # default 300s (5 min)
    ...
    for pid, provider in providers.items():
        if time.time() > _wr_deadline:
            console.log(
                f"[yellow][WR-CAP] cycle budget exceeded; finishing with "
                f"{len(round_takes)}/{len(_debate_expected)} responses "
                f"on {symbol}"
            )
            break
        ...
        try:
            take = generate_hot_take(provider, pid, symbol, ...)
            ...
```

**Properties:**
- Hard cap of 5 min per cycle (configurable via `WAR_ROOM_CYCLE_BUDGET_S`
  env var)
- Schedule's 3-min tick can advance even if a previous cycle was slow
- Late-arriving providers get skipped this cycle but participate next
  cycle
- Honest framing: "this cycle ran out of time" vs "this cycle hung"

**Risk:** if a single provider takes 4 min and we deadline at 5 min, only
that provider responds. Mitigate by combining with Layer 3 (per-provider
timeouts tightened so 4-min stalls don't happen) and by logging which
provider was active when the deadline hit.

### Layer 3 — Tighten per-provider timeouts (higher risk)

Reduce `OllamaProvider` and `MLXProvider` default timeout from 180s to
something like 30s for War Room calls (a 2–4 sentence take should not
take 30s+ on a healthy Ollie box).

Approach:
1. Add `timeout_war_room: int = 30` constructor arg to OllamaProvider /
   MLXProvider (and any others used by War Room).
2. Pass it explicitly when constructing the Arena's provider dict.
3. Document the trade-off: faster failures, but a model under temporary
   load may be excluded from a debate round.

**Risk:** legitimate slow models (e.g. qwen3:14b cold-start) get cut off
and stop participating. Mitigate by:
- Per-provider override (some models can keep 90s; only the chronic
  offenders get 30s)
- Pair with Layer 1 — observe which models actually take how long
  before tightening

**Recommended deploy order:** Layer 1 ships first (zero risk, gives us
data), Layer 2 next (after a week of Layer 1 data), Layer 3 last (only
after the deadline cap has been holding cycle wall under 5 min for several
sessions).

## Test plan

`tests/test_war_room_latency.py` (new):

1. `test_cycle_wall_logged_on_completion` — invoke `_war_room_thread`
   via mock; assert `[WR-DUR]` log line emitted with finite wall-clock
2. `test_ntfy_fires_above_threshold` — mock a synthetic 11-min cycle;
   assert `send_alert` called with `alert_type='war_room_slow_cycle'`
3. `test_ntfy_does_not_fire_below_threshold` — 1-min cycle; assert
   `send_alert` not called
4. (Layer 2) `test_cycle_aborts_at_deadline` — mock providers list of
   10 entries where each takes 60s; set deadline to 90s; assert loop
   exits after ~2 providers respond
5. (Layer 2) `test_deadline_break_logs_warning` — assert `[WR-CAP]`
   log line emitted with collected-vs-expected counts
6. (Layer 3) `test_provider_timeout_override` — assert OllamaProvider
   instantiated with `timeout_war_room=30` uses 30s on requests.post

Mocks needed:
- `engine.alert_channels.send_alert`
- `engine.market_data.get_bulk_prices` (return fake price dict)
- `engine.war_room.run_war_room` (replaced with sleeping stubs)
- `engine.providers.ollama_provider.OllamaProvider.call_model`

## Implementation order

| Phase | Risk | Time | What |
|---|---|---|---|
| 1.1 | LOW | 1h | Layer 1 — `_war_room_thread` instrumentation + NTFY |
| 1.2 | LOW | 30min | Tests 1-3 (TDD red→green) |
| 1.3 | LOW | (Captain merge + 13:00 restart) | Deploy + observe for 1 session |
| 2.1 | MED | 1.5h | Layer 2 — deadline cap in `engine/war_room.py:run_war_room` |
| 2.2 | MED | 1h | Tests 4-5 (TDD red→green) |
| 2.3 | MED | (Captain merge + restart) | Deploy + observe |
| 3.1 | MED-HIGH | 1h | Layer 3 — per-provider timeout override |
| 3.2 | LOW | 30min | Test 6 |
| 3.3 | (post-soak only) | | Deploy after Layer 1+2 stable |

**Total time estimate** (all 3 phases): ~5-6 hours over multiple ship
windows. Layer 1 alone is ~1.5h and unblocks Captain visibility — that's
the urgent piece.

## Risk profile

### Layer 1 (instrumentation + NTFY)
- **Risk:** essentially zero — adds logging + a try/except'd NTFY call
- **Failure mode:** instrumentation throws → caught by enclosing
  `try/finally`, `_war_room_running.clear()` still fires → no impact
  on in-flight or future cycles
- **Rollback:** revert the commit; the inner cycle logic is untouched

### Layer 2 (deadline cap)
- **Risk:** medium — changes cycle behavior. A cycle that previously
  ran to completion (slowly) now truncates
- **Failure mode 1:** truncated cycle misses some providers' takes
  → those takes appear next cycle instead. Acceptable.
- **Failure mode 2:** `_debate_completed` list shrinks → debate
  completion stats may flag "incomplete debate" for the round. Need
  to verify `_finish_debate` handles partial completion gracefully
  (looking at line 173: `_finish_debate(debate_id, expected, completed)`
  — it appears to handle a `completed` shorter than `expected` as the
  normal partial-success case).
- **Rollback:** revert the deadline check; existing loop logic
  unchanged

### Layer 3 (provider timeout tightening)
- **Risk:** medium-high — could cut off slow-but-functional models
- **Failure mode:** a model that takes 35s on cold-start now times out
  every cycle until its weights are loaded. Some Ollie-box models do
  show 30-60s cold-start latency.
- **Mitigation:** Layer 1's per-model latency log informs which
  timeouts are actually safe to tighten before Layer 3 ships
- **Rollback:** revert the constructor change; restart trader

## What this scope does NOT cover (deferred)

- **Asynchronous provider calls.** Converting the serial loop in
  `run_war_room` to `asyncio.gather()` would let cycles complete in
  max(provider_times) instead of sum(provider_times). This is the
  "right" long-term fix but adds substantial complexity and risk
  (debate-state ordering, save_hot_take race conditions, provider
  client thread-safety). Scoped as separate **HM-WAR-ROOM-ASYNC**
  ticket post-Layer-3.
- **Provider pre-warming.** Keeping models hot via lightweight ping
  before the cycle starts — separate optimization.
- **Reduce the 49-provider fleet.** Halt-mode dormant providers are
  filtered out (line 866: `if pid in paused_ids or inactive_ids or
  halted_ids: continue`). The active ~21 is the real iteration count.
  Reducing further is a fleet-roster decision, not a latency fix.

## Captain action

- [ ] Review this doc
- [ ] Prioritize Layer 1 vs higher-priority work (suggested: ship
  Layer 1 next session — 1.5h, unblocks visibility into the problem
  before any cycle-changing logic ships)
- [ ] Pick a ship window for Layer 1 (suggested: any pre-market window
  in the next 2-3 sessions; trader restart already bundles)
- [ ] On approval: Scotty opens `hm-war-room-latency-layer1` branch
  with instrumentation + tests

## Files

- This doc: `data/scotty_hm_war_room_latency_scope_2026-05-15.md`
- Memory: `project_hm_war_room_cycle_latency.md` (auto-memory)
- Source under proposed change: `main.py:1114` (outer dispatcher) +
  `engine/war_room.py:797 run_war_room` (inner cycle)
- Source unchanged: `engine/providers/*_provider.py` (Layer 3 only)
- Existing infrastructure to reuse: `engine.alert_channels.send_alert`,
  `_hm_bq_instr` decorator pattern (`main.py:43`)
