# Relay — RECALL_OLLAMA_URL root cause, repo-wide host sweep (Tier 3), 0xroyce trace, 2026-09-10 (close of session)

Final piece of today's session. Earlier relay docs today:
`relay_2026-09-10_bakeoff-minimax-provider-and-arms.md`,
`relay_2026-09-10_four-item-sweep-and-doc-revisit-expiry-check.md`,
`relay_2026-09-10_phase2-bakeoff-and-phase13-spec.md`. This one covers
the tail: fixing the actual `RECALL_OLLAMA_URL` bug, the repo-wide sweep
it led to, and an unrelated live-Ollama-log finding traced to a
harmless-but-stale healthcheck.

## RECALL_OLLAMA_URL — root cause found and fixed

Earlier tonight I mischaracterized `com.ollama.serve` being down on bigmac
as an outage needing a restart. **Corrected by the Captain**: it was
deliberately booted out + disabled 2026-09-09 when all inference and
embeddings moved to olliemax — restarting it would stand up a second
Ollama server competing with the one actually serving. Never attempted.

**Actual bug**: `RECALL_OLLAMA_URL` was never added to `.env` in that
migration (confirmed: not in `.env`, no launchd plist, no shell profile)
despite `OLLAMA_URL`/`ADVISORY_OLLAMA_URL` both being correctly
repointed. `engine/setup_similarity_signal.py`'s
`os.environ.get("RECALL_OLLAMA_URL", "http://127.0.0.1:11434/api/embed")`
silently fell through to its stale default every call. **Fixed**: now
derives from `config.OLLAMA_URL` (the one source of truth that's actually
migrated) instead of maintaining a second, independently-configurable
fallback pair — the exact pattern that let this drift. Live-verified
against olliemax's real `/api/embed` endpoint (payload shape confirmed
first: `{"model":"bge-m3","input":...}`, distinct from the older
`/api/embeddings` singular-prompt endpoint).

**Recall-in-prompt latency — root-caused and fixed (by the Captain).**
Initial live measurement: 10-17s per call, wildly inconsistent. Root
cause: olliemax's `OLLAMA_MAX_LOADED_MODELS=2` with 3 models wanting
residency — bge-m3 kept losing the slot. Raised to 3. Re-measured
post-fix: **0.035-0.171s**. Recall-in-prompt is viable as originally
specced at these numbers; a precomputed-embeddings table remains the
better long-term architecture (same ticker embedded hundreds of times a
day for an identical result) but is no longer mandatory for latency
alone — noted as a future improvement, not built tonight.

## Repo-wide hardcoded-Ollama-host sweep

Per the Captain: "that's the third partial migration this week caused by
a hardcoded local address." Grepped both host families — `127.0.0.1`/
`localhost:11434` (bigmac-local) and `192.168.1.166`/`192.168.1.168`
(pre-migration Ollie Box / Ollie Max, found while checking `war_room.py`)
— across the whole repo. ~30 live-code hits (excluding tests, comments,
docs, and already-archived code).

**Tier 3 (offline/manual tools, no restart risk) — fixed and pushed
tonight** (commit `fcf1f6c`): `engine/setup_similarity_signal.py`,
`scripts/hm_forge_bench.py`, `recall_bakeoff.py`, `plutus_v6/bakeoff_judge.py`,
`plutus_v6/bakeoff_gen.py`, `witness_ab_scorer.py`, `uhura/parse_signals.py`,
`model_sweep_2026_04_20.py`, `model_sweep_v2.py`, `hm_perf_queue_loadtest.py`,
`plutus_v7_eval/common.py`, `backtest_baseline.py`, `ollama_bulk_backtest.py`,
`ollie_backtest_v6.py`, `engine/weekend_backtest.py`, `resume_course.sh`,
`dr_crusher.sh`, `premarket-scan.sh`, `launch-trademinds.sh`. Each now
reads `OLLAMA_URL`/`OLLIE_URL` from env with no fallback — fails loud
(Python `KeyError`, or an explicit shell check-and-exit) instead of
silently reaching for a dead host. `load_dotenv()` added where a script
didn't already call it. All compile/syntax-checked; confirmed via a
follow-up grep that no live hardcoded hits remain in these files (only
explanatory comments mentioning the old addresses).

Two incidental findings while sweeping: `premarket-scan.sh`'s own header
claims it's "Managed by: com.trademinds.premarket launchd agent" — that
agent doesn't exist (`launchctl list`, no plist anywhere) — the script
was already orphaned independent of the host bug. `scripts/
ollama_model_swap_probe.py` (in the original inventory) was separately
retired 2026-09-09, moot either way.

**Tier 2 (fleet-critical, needs coordinated fix + restart) — held for
2026-09-11 after close, per the Captain**: `main.py`, `engine/war_room.py`,
`engine/crew_scanner.py`, `engine/paper_trader.py`, `engine/ic_squadron.py`,
`engine/wb_advisory_team.py`, `engine/portfolio_optimizer.py`,
`engine/rebalancer.py`, `engine/fleet_status.py`, `engine/reveille.py`,
`engine/phaser_lock.py`, `engine/archer/brain.py`, `watchdog.py`,
`dashboard/app.py`, `config.py`, `agents/sarek.py`/`surak.py`/`janeway.py`.
Not touched tonight — bundled with the restart already scheduled for the
S8 rotation and premium restatement, not done piecemeal at 18:30 on a
Thursday.

## 0xroyce/plutus live-log finding — traced, not a live decision path

Captain observed real `0xroyce/plutus:latest` calls on olliemax tonight
(19:33/19:52/20:07/20:22), model resident at 10GB/24,576 ctx, interleaved
with `plutus-v1` calls. Traced:

- **The only seat configured to use it**: `dayblade-0dte` ("T'Pol"),
  hardcoded directly in `main.py:161`'s `initialize_dayblade()`, bypassing
  `config.AI_PLAYERS`. **Halted since 2026-07-13** (`halt_mode='full'`,
  explicit reason on file: systematic options mispricing ~3x rich causing
  -71% stops, reasoning/action inversion, fractional qty bugs). Zero
  `decision_audit` rows, zero trades today — not live.
- **Actual source of the observed calls**: `main.py`'s `dr_crusher_check()`
  healthcheck, running every 5 minutes, pinging `0xroyce/plutus` with a
  trivial 3-token "ok" completion purely to check "is the scan model
  responsive." Never retargeted when `dayblade-0dte` was halted two months
  ago — has been keeping a model no live seat uses resident this whole
  time, plausibly contributing to the GPU-residency contention that was
  also starving bge-m3 before tonight's `MAX_LOADED_MODELS` fix. Already
  flagged once in a 2026-09-08 relay doc, not actioned then either.
- Filed to `docs/XO_BACKLOG.md`. Not fixed tonight — needs a decision
  (retarget the healthcheck, or drop it until `dayblade-0dte` is revived).

## Tomorrow (2026-09-11) after-close sequence, full and final

1. Any remaining RULE #1 delete-layer work, if open items remain
2. S8 season rotation (`rotate_season(caller="s8-manual")`, six-step
   sequence re-verified tonight, dry-run clean)
3. Phase 1.1 acceptance read
4. Tier 2 hardcoded-host fixes (above), bundled with the restart this
   batch requires anyway
5. Options premium restatement
6. Dataset exporter (depends on #5)

## Session close

Context budget and session length flagged by the Captain — stopping here
per explicit instruction. Nothing further attempted tonight beyond what's
recorded above and in the three earlier relay docs.
