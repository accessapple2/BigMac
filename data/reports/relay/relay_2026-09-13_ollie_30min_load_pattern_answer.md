# Relay — 2026-09-13. Answer to Trip's Priority 2 (the other half of HM-OLLIE-SEAT-OVERSIZE).

## The ask

Trip fixed the destructive half of HM-OLLIE-SEAT-OVERSIZE-2026-09-12
(`ebcd94b`): `phi3:mini`'s 12288 override was evicting `plutus-v1`'s live
seat, dropped to 4096. Left open: **what calls a model on a ~30-minute
cadence to answer a small (~180-token) prompt, why that cadence, and does
it need to be a model call at all.**

## What I checked (read-only, RULE #1 untouched throughout)

1. **`agents/janeway.py`** — the only confirmed real `phi3:mini` caller
   per `ebcd94b`'s own investigation (182-token prompt, measured live via
   `prompt_eval_count`). Cadence: **daily**, not 30-min
   (`main.py:6146` `schedule.every().day.at("05:35")` for the DCA,
   `:6148` `05:45` for the brief-cache refresh). `get_janeway_brief()`'s
   cache is a 7-day in-memory TTL (`agents/janeway.py:43`) with no other
   caller in the codebase (`grep` found exactly the two `main.py` call
   sites). Ruled out as the 30-min source.

2. **`mlx-qwen3` (Ensign Ro)** — historically the other `phi3:mini`-tagged
   agent (`main.py`'s `_SCAN_TIER1` comment). Confirmed **dead on both
   paths that could fire it**: `ai_players.halt_mode='full'` (verified
   live in `trader.db`; its real `model_id` is actually `ministral-3:3b`,
   not `phi3:mini` — the `_SCAN_TIER1` comment is stale), and
   `engine.agent_routing.build_all_providers()` explicitly skips
   `halt_mode=='full'` at the DB-iteration step (`engine/
   agent_routing.py:188`) — so even though `_SCAN_TIER1` still lists it,
   `Arena.run_scan()`'s `player_ids` intersection against `self.providers`
   (which never contains it) means the call never actually goes out.
   Separately, `engine/crew_scanner.py`'s own `ALPHA_SQUAD`/`SCAN_PAIRS`
   rotation (a different, older scanning system) does **not** include
   `mlx-qwen3` at all — its three real pairs run `qwen3:14b`, `deepseek-
   r1:14b`+`phi4`, and `llama3.3:8b`+`plutus`, none of them `phi3:mini`.
   Ruled out on both fronts.

3. **Every literal `schedule.every(30).minutes.do(...)` line in `main.py`**
   (16 of them) — checked each for an unconditional Ollama call:
   `run_daily_summary`/`run_daily_rating_update`/`run_journal`/
   `run_ah_scanner`/`run_ready_room`/`run_team_advisor`/
   `run_daily_enrichment`/`run_fingerprint_capture`/`run_weekly_picks` are
   all internally date/time-gated to fire once (or a handful of times) a
   day, not every tick. `_run_signal_evaluator`/`_run_realized_evaluator`
   touch no Ollama code at all (`grep` clean). `_bg_autopilot` fires
   unconditionally every 30 min but is pure price-rebalancing, no LLM
   call. `_run_prime_directive_safe` polls USTR/Commerce RSS, no LLM.
   `_run_ic_manager_safe` → `engine.ic_squadron.run_manager()` does call
   `qwen3:8b` (`num_predict=120`, ~same shape as the 180-token ask) but
   **only as an event-driven post-mortem fired on an actual IC position
   close**, not on every 30-min tick — IC positions are multi-week
   holds, so this does not fire every 30 min in practice.
   **None of the sixteen is an unconditional every-tick model call.**

4. **Live ground truth on olliemax** — pulled 24h of `journalctl -u ollama`
   (GIN request lines: timestamp + duration, no model name in that log
   format) and diffed against the known schedulers. The dominant pattern
   is the sentinel's staggered 5-min probe (two sub-50ms calls per tick,
   already fixed/expected). The one recurring *slower* pattern (1.4-1.9s,
   occasionally 3.3-3.6s or a 16-18s cold reload during contention) lines
   up with **`engine/riker_xo.py::generate_riker_synthesis()`
   (`main.py:4707`, `schedule.every(10).minutes`)** — not 30 min, but the
   closest real, currently-open match: no market-hours or weekend gate
   (ticks 24/7), no cache short-circuit (regenerates every single fire,
   the in-memory cache is read-only for the API getter), and its live
   output measured at 891–949 characters (`relay_2026-09-12_ollie_silent_
   seat_fix.md`) — roughly 150-190 tokens, i.e. the same order of
   magnitude as "~180 tokens." This exact gap was already named as **open,
   not fixed** in that same relay doc: *"Riker's synthesis scheduler
   still has no weekend/market-hours gate — it will keep ticking every 10
   minutes regardless... worth a future ticket if 24/7 ticking against a
   paper-only crew-intelligence synthesis isn't wanted."*
   No bigmac-local Ollama activity in the same window (`com.ollama.serve`
   `/api/ps` currently empty, `com.ollama.serve` isn't home to any of
   these callers) and no matching crontab line at a 30-min cadence either.

## Honest verdict

**I could not find a literal 30-minute-interval caller anywhere in the
code, cron, or live olliemax traffic that matches "~180 tokens."** The
single real, still-open, ungated periodic small-output model call in the
live system is Riker's synthesis job — but its coded and observed cadence
is **10 minutes, not 30**. I'm flagging this discrepancy rather than
rounding it away. Two possibilities: (a) the ~30-min figure was an
approximate/visual read (e.g. of olliemax's dashboard or `/api/ps` at
spaced check-ins, which would alias a 10-min cadence to something that
*looks* like ~30 min if only sampled every third fire), or (b) there's a
caller I haven't found — everything above is what I checked and ruled
out, not a claim of completeness.

## Does it need to be a model call at all — answer either way

Whether the real cadence is 10 or 30 minutes, the underlying question
holds for `generate_riker_synthesis()`: **no, not on a blind interval.**
Per its own `HM-RIKER-XO-SCHEDULE-2026-07-09` commit note, this was wired
onto a plain interval purely to stop the dashboard widget from showing
"awaiting synthesis" forever after a restart — a caching/UX problem, not
a decision-path need (Riker's output "never reaches any execution path,"
confirmed by the Bridge Correctness Phase 1 trace). Two cheaper fixes,
either of which removes the 24/7 background cost:
1. **Gate it to market hours + weekdays** (same one-line pattern already
   used by `run_crew_scanner_job`/`_run_ic_manager_safe` — `if not
   RiskManager.is_market_hours(): return`), cutting ~2/3 of today's fires
   (nights + weekends) for zero loss of relevance.
2. **Make it lazy-on-view instead of interval-polled**: generate on the
   first `/api/riker/recommendation` GET after the existing cache
   (`_TTL` in `riker_xo.py`) expires, rather than a scheduler pushing a
   fresh one every 10 min whether or not anyone's looking at the widget.
   Given this only feeds a display widget (RULE #1: no execution path),
   lazy generation is strictly cheaper and no less correct.

Not actioned this pass — the Admiral asked for the "what/why/does it need
to be" answer, not a fix; recommend a follow-up ticket if either option
above is wanted.
