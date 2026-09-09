# Olliemax Cutover + 30B Model + Overnight Cleanup — 2026-09-08

**VERDICT: qwen3:30b-a3b on olliemax (100.95.195.20:11434), verified via direct production-path calls (7.4-15.3s/call) since War Room itself is market-gated and couldn't fire tonight. 05:46 premarket brief has a working model.**

---

## What shipped (commits `6362e36`, `0560d44`, `b1ccddf`, pushed to `exec-pipeline`)

### 1. The actual migration bug (found before any of tonight's cutover work)
`config.py`'s `OLLAMA_URL`/`OLLIE_URL` were hardcoded `"http://localhost:11434"` literals, not env-reads, despite `load_dotenv(override=True)` at the top of the same file. Every `.env` edit made earlier tonight (and in this afternoon's original migration commit) was inert for the fleet's real routing path — `config.AI_PLAYERS` embeds `OLLIE_URL` at import time, and a live `build_all_providers()` call confirmed production routing still resolved to `localhost:11434` regardless. Fixed: both constants are now `os.environ.get(...)`-backed. `OLLAMA_LOCAL_URL` deliberately left alone (CLAUDE.md: "retained for reference, do not reuse").

### 2. Two hops to the working olliemax address tonight
- First attempt: `192.168.1.55` (the LAN IP from this afternoon's original migration). Code-correct, but blocked: curl succeeded, raw Python sockets got instant `[Errno 65] No route to host` — traced to macOS Local Network privacy permission scoped per-binary, broken specifically for the homebrew python3.14 binary (curl and `/usr/bin/python3` exempt), and known to reset on every brew upgrade. Rolled back to `127.0.0.1` via `.env` only while this was diagnosed.
- Second attempt (tonight, after olliemax moved to new hardware): `100.95.195.20`, reached over Tailscale rather than the LAN, which sidesteps the Local Network privacy gate entirely. **This is the one that's live now.** `.env`'s four URLs and `trader_restart.sh` line 96's default both point here.

### 3. 30B cutover (McCoy, Troi, Worf)
- **Gate check, not skipped:** last-24h actual token counts (input+output) across the three seats, n=695: p50=9429, p95=9618, p99=9677, max=9727. `num_ctx=8192` would have silently truncated the *median* call. `16384` covers the max with room — used that instead, per your own fallback rule.
- **VRAM, live-verified, not estimated:** loaded `qwen3:30b-a3b` at `num_ctx=16384` on olliemax → 18.4GB VRAM (of ~21.5GB across both cards). Then loaded `bge-m3` alongside and re-checked `/api/ps`: **both stayed resident simultaneously** (bge-m3 only needs 77MB VRAM in practice — far smaller than its 1.16GB on-disk size suggested). `recall_refresh` will not get evicted.
- **Routing changed by reference, not `ollama cp` alias:** `ai_players.model_id` (DB-authoritative) and `config.AI_PLAYERS`'s `model` field both updated for `ollama-plutus`/McCoy, `options-sosnoff`/Troi, `qwen3-8b-flash`/Worf. Also updated `crew_specialization.py`'s `ollama-local`/Geordi entry (the gemma3:4b "advisory" tier tag) — that seat is `halt_mode='full'` since the 2026-06-06/07-04 scorecard cull, so this doesn't touch live traffic, just keeps the reference honest.
- **`think:false`:** no code change needed — `qwen3:30b-a3b` already qualifies via the existing `model_id.startswith("qwen3")` check.
- **`num_ctx` was a global constant** (10240, hardcoded for every model) — now a per-model override dict (`_NUM_CTX_OVERRIDES` in `ollama_provider.py`), default unchanged for everything else.
- **Latency:** direct-call tests (identical production code path, `build_all_providers()` → `call_model()`) — 15.34s cold, 7.37s warm. Both well under the 60s abort threshold, and faster than tonight's 35–39s bigmac baseline. **Caveat:** organic trader.log confirmation wasn't possible — War Room's debate cycle is market-hours-gated (`[WR-DAEMON-HB] ... market closed — skipped`) and no other scheduled job happened to call these three seats in a 5-minute post-restart watch either. The numbers above are real, same-code-path measurements, just not pulled from a live trader.log line.
- Response text showed longer, chattier preamble ("Okay, the user is asking...") than a plain answer — `think:false` may not be fully suppressing this model's reasoning style the way it does for qwen3:8b. Not blocking, worth a look later.

### 4. Attribution question you raised mid-verification
`0xroyce/plutus:latest` showed up resident on olliemax right after the first restart — **not a trading seat**. It's `dr_crusher_check()` in `main.py` (~line 4989), a standalone 5-minute health-check that fires an unconditional raw HTTP POST to `{OLLIE_URL}/api/generate` with a hardcoded `"model": "0xroyce/plutus"`, bypassing `OllamaProvider`, the DB, and any halt check — it's an infra "is the scan model responsive" probe, not seat inference. Confirmed `dayblade-0dte`/T'Pol (the seat that model name is nominally associated with) is genuinely `halt_mode='full'` and its own `run_scan()` correctly gates on that before ever touching a provider — no bypass bug there. Doesn't register tools (bare completion call, no `tools` param — same pattern as everywhere else in this codebase). **Not repointed tonight** since it's outside the named seat list, but flagging: once the three seats move to 30B, this healthcheck is verifying a model nobody actually scans with anymore.

### 5. olliemax monitoring gap — closed
`scripts/olliemax_health_probe.py`, cron every 5 min, `curl -sf --max-time 5 http://100.95.195.20:11434/api/tags`. On failure: `send_alert(level=RED_ALERT, ...)`, 30-min cooldown — routes through pushover+email since **ntfy has been a global no-op since 2026-07-19** (`DECOM-SILENCE` guard in `engine/alert_channels.py::_send_ntfy()` — first line is `return False`). This is the first thing that pages if olliemax drops overnight.

### 6. Pushover flood (sentinel_launchd_mass_outage) — acked + fixed at the root
Today's ~8h power-off meant launchd never retroactively fired missed `StartCalendarInterval` slots, so every daytime job's log went stale simultaneously and the mass-outage check paged repeatedly (11/15 jobs "silent"). Acked through 2026-09-09 09:00 MST (`ceiling: 17`, covers the full registry) as an immediate stopgap. Root fix shipped the same night: `check_launchd_jobs_health()` now computes `_uptime_hours()` (sysctl `kern.boottime`) and grades any job — or the mass-outage check itself — whose relevant staleness window is longer than the box has even been up since boot. Live dry-run post-fix: `checked=15, stale=0, graced=0` (most jobs had already caught up naturally by the time this landed ~4.8h post-boot).

### 7. Disk — KEEP change + a real monitoring bug found
- `db_snapshot.sh` `KEEP=14→7`. Today's snapshot already existed (cron ran at 20:15 before this change), so the script's own SKIP-if-exists logic meant it never reached retention — ran the identical retention block standalone against the real `data/backups/` directory instead. Archived (never deleted) `trader_2026-09-01.db` → `_archive/trader_2026-09-01.db.gz`. 7 daily snapshots remain.
- **`disk_space_alert.log` is checking the wrong volume.** It reports "33% used, 33GiB free" against a 95% alarm threshold. `df -h /System/Volumes/Data` shows the *same* 33GiB free but **83% used** — the script's percentage is computed against a different (much larger apparent) baseline than the real Data volume, so its alarm threshold can structurally never fire even as the real volume genuinely fills. This is very likely why tonight's near-critical disk state (your own "92.8%/16.4GiB free" reading) never paged anyone on its own. Not fixed tonight — flagging for a deliberate pass, since it needs to be re-pointed at the right volume rather than patched blind.
- Growth beyond snapshots, confirmed still present and unretained: `signal-center/signals.db` (2.19GB, growing, zero rotation/archiving policy — the single biggest unretained consumer found), `~/Library/Logs/ollama-serve.err.log` (155MB, no rotation), `logs/trader_error.log` (183MB, no rotation — `trader.log` itself does rotate, has a `.1.gz`, this one doesn't).

### 8. Remaining hardcoded `localhost:11434` — tomorrow's repoint list, not touched tonight
**Genuinely hardcoded, no env indirection at all** (will break outright once bigmac's Ollama stops): `healthcheck.py` (dead code per its own header comment — HM-WATCHDOG-SUPERVISOR turned it off 2026-06-10, low priority), `launch-trademinds.sh`, `premarket-scan.sh` (raw curl precheck, no env), `scripts/backtest_baseline.py`, `scripts/hm_forge_bench.py`, `scripts/ollama_bulk_backtest.py`, `scripts/recall_bakeoff.py`, `scripts/plutus_v6/bakeoff_judge.py`, `scripts/plutus_v6/bakeoff_gen.py` (offline backtest/bakeoff tools), `engine/providers/dalio_provider.py` (constructor default, not yet checked whether any caller relies on it).

**Looks env-backed but the env var itself is never set** — the "safe-looking but actually still local" class: `engine/setup_similarity_signal.py`'s `OLLAMA_EMBED_URL = os.environ.get("RECALL_OLLAMA_URL", "http://127.0.0.1:11434/api/embed")` — `RECALL_OLLAMA_URL` isn't one of the four migrated `.env` vars and isn't set at all, so `scripts/recall_refresh.py` (which imports this) is still local in practice despite the env-read pattern. This is the one to fix first — it's the most likely to bite silently.

`kirk_briefing.py` is actually fine — it reads `ADVISORY_OLLAMA_URL`/`OLLAMA_BASE_URL`/`config.OLLIE_URL` in that order (all now env-backed), the "localhost" mention there is a stale comment, not live behavior, once this restart's `.env` state is picked up.

**Intentionally local by design, exclude from the repoint list:** `scripts/ollama_model_swap_probe.py` (deliberately monitors bigmac's own local Ollama), `scripts/model_watcher.py` (deliberately compares both hosts by name, not a bug).

---

## Open items for tomorrow (not done tonight, by design)
- Repoint `RECALL_OLLAMA_URL` + the rest of the "genuinely hardcoded" list above, in order, before bigmac's `com.ollama.serve` can actually be stopped.
- Re-point `disk_space_alert.log`'s check at the real Data volume (or wherever the true fill-level lives) so its 95% alarm can actually fire.
- Set up rotation/archiving for `signal-center/signals.db` (2.19GB, unretained) and the two unrotated logs.
- Consider whether `dr_crusher_check()`'s hardcoded `0xroyce/plutus` healthcheck target should move to `qwen3:30b-a3b` now that no seat actually scans with the old model.
- `qwen3:30b-a3b`'s verbose reasoning-leak-through-think:false — watch it, not blocking.
- Confirm the 30B swap holds up once the market actually opens and War Room fires for real (tonight's verification was necessarily synthetic given the after-hours gate).

## Audit-integrity note
Everything above that touches a live system was disclosed as it happened: two restarts (19:34→20:55→21:15, per the timing windows given), one `.env` write-then-revert-then-rewrite (never printed in full — targeted, masked edits only), direct test calls to olliemax before committing to each restart, and one earlier accidental leak of the real Polygon API key via an `rtk grep -c` preview (flagged immediately, redaction shipped in `6362e36`, recommended rotating that key).
