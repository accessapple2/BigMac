# Relay: HM-FULL-AUDIT-2026Q3 (on-box half)

**Date:** 2026-07-18 (Saturday), directive issued by XO, Priority HIGH.
**Scope:** read-only audit except where noted (one restore-test copy to `/tmp`, one docs/XO_BACKLOG.md append for the archer-portrait add-on). No service restarts, no sacred-DB writes, no speculative refactors performed — findings and recommendations only.
**Method:** five parallel research passes (Sections A-E), each independently live-verified against command output/file paths/DB queries — no claim taken on faith from `docs/XO_BACKLOG.md` or `CLAUDE.md` without a fresh check. Several of the most severe findings (tour_api.py exposure, MAX_ACTIVE_AGENTS breach, AG/SPY reconciliation drift root cause) were independently re-verified by the lead session on top of the research-pass evidence.

---

## EXECUTIVE SUMMARY — the one narrative that ties this together

The single biggest finding this pass is **not** any individual bug — it's a causal chain spanning three sections. `config.py:375` declares `MAX_ACTIVE_AGENTS = 8  # hard ceiling on concurrently halt_mode='active' agents`, but that constant is only checked once, at DB setup time (`setup_db.py`), never in any live runtime path. The fleet is currently at **75 active agents** (vs. a documented 2026-07-01 waypoint of 15 active/55 halted eighteen days ago) — a ~9x breach of the design ceiling, with no ticket anywhere documenting when or why this happened, most plausibly an undocumented Season 6→7 transition side effect (Season 6 `end_date=2026-07-10` in `season_config`, no Season 7 row exists). This is very likely the root cause of Section A's confirmed finding that the single-threaded `schedule.run_pending()` queue is now at/beyond saturation (war-room cycle p95=777.7s, max=3245.6s vs. a nominal 300s cadence, markedly worse in the last 4 trading days specifically) — which is *itself* the confirmed root cause of two silent full-day misses this week: `bridge_vote` (already reported, HM-BRIDGE-CONSENSUS-STALE) and, newly found this pass, `run_daily_rating_update` on 2026-07-16. **This needs an explicit Admiral ruling: was the mass reactivation intentional?** If not, re-halting to design capacity would likely relieve the scheduler pressure and prevent further silent misses without any code change.

Second-highest-severity finding: a genuine **RED security gap**, independently confirmed live — `tour_api.py` (port 8088) exposes an unauthenticated `POST /api/paper/order` reachable from the LAN and the box's Tailscale interface, completely bypassing Cloudflare Access. Blast radius is bounded (hard-asserted to `route='alpaca_paper'`, cannot touch Schwab/live capital per RULE #1), but it's a real, live, zero-auth order-submission path that the two sibling services (`dashboard/app.py`, `swingdesk/backend.py`) already solved correctly — this one just never got the same treatment.

Third: `HM-SIGNALS-V2-STARVATION-RECURRENCE`, filed 2026-07-12 as a structural risk, is now **confirmed and getting worse** — the ticket's own automated Monday check already proved "CONFIRMED OUTRANKED" on 07-13, that result was never written back into the backlog doc, and the pending queue has grown from the 140-row sample that triggered the check to **8,290 rows today**, oldest row now 8+ days stale.

Beyond those three: one real (small-dollar) trading-book drift bug was root-caused (reconciliation ADD-ON, see below), one 43-day-stale real-cash data feed with zero alarm coverage was found (Schwab), and roughly a dozen smaller RED/AMBER findings round out Sections A-E below. Secrets hygiene, sacred-DB integrity, and the backup/restore spine all came back clean.

---

## A. SERVICES & SCHEDULING

*(Current time at audit start: Sat Jul 18 14:29:29 MST 2026)*

**[INFO] A1.0** bigmac: 26 loaded LaunchAgents + 8 LaunchDaemons + ~18 correctly-inactive retired plists. `crontab -l` = 85 raw lines but only **46** are live entries (rest are comments/blanks) — do not cite "85" as the job count going forward.

**[RED] A1.1** `com.ollietrades.crusher` is registered as **both** a LaunchAgent and a LaunchDaemon (same script, both `StartInterval=360`, both active) — confirmed firing twice per 6-min tick with identical timestamps in `logs/crusher.log`. Recommend: unload one copy.

**[RED] A1.2** `com.trademinds.premarket` has failed **5 consecutive trading days** (07-13 through 07-17): Ollama not reachable on port 11434 despite `ollama serve`'s LaunchDaemon showing as running since Monday — process alive, port not bound (wedged). Not covered by `ollama_prewarm.sh` (06:45, after this 04:00 job). Recommend: restart the daemon, add a port-liveness precheck ahead of the scan.

**[RED] A1.3** `com.ollietrades.ti-email-poller` — `last exit code = 2`, live cause confirmed in fresh logs: `imaplib.IMAP4.error: [AUTHENTICATIONFAILED] Invalid credentials`, every 5-min cycle. TI email intel ingestion down.

**[RED] A1.4** `scripts/morning_cd_instr.sh` reads `trader_error.log` for `HM-CD-instr` markers that are only ever written to `trader.log` (0 vs 2,667 matches) — has been firing a false-positive high-priority ntfy alert every weekday for ~2 months, own stdout log frozen since May 22. Trivial one-line path fix.

**[RED] A1.5** `engine/riker_synthesis.py` running on two overlapping schedules simultaneously (stale-commented-out cron `*/10 * * * *` that's actually still active + LaunchAgent `StartInterval=600`) — interleaved log timestamps confirm effective ~5-min cadence instead of intended 10. Recommend: disable one.

**[AMBER] A1.6** Three LaunchAgents (archer-briefing, danelfin-update, nightly-backtest) log to a sibling `~/ollietrades/logs/` directory, not `autonomous-trader/logs/` — not orphaned (logs are fresh), but invisible to any log-sweep tooling scoped to this repo's tree.

**[GREEN] A1.7 / A1.8 / A1.9** Every active plist's referenced script confirmed to exist — zero truly orphaned jobs. ~18 retired plists correctly absent from `launchctl list` — good hygiene. Known one-shot jobs (`hm-signals-v2-monday-check{,-verify}`, `hm-bridge-consensus-monday-check`) all in expected state, not stale.

**[INFO] A1.10** olliemax runs zero OllieTrades scheduling (Ubuntu, no launchd, empty crontab) — it's an on-demand SSH-triggered GPU box, not an independent scheduling node.

**A2 — window-gated scheduler jobs (the bridge_vote queue-jam class), full table:**

| Function | File:Line | Window | Criticality | Logs on skip? |
|---|---|---|---|---|
| `run_bridge_vote_job` | `engine/bridge_vote.py:685` | 9:00-9:10 AM ET (10min) | Advisory dashboard consensus | **No** |
| `run_daily_rating_update` | `main.py:3273` | 16:30-17:00 ET (30min) | Fleet report card feed. **Confirmed missed 2026-07-16** (system verified fully active/busy that exact window; log shows the marker for every weekday except 07-16) | **No** |
| `run_oi_morning_snapshot` | `main.py:2922` | 6:30-6:40 AM AZ (10min) | SPY OI baseline, unrecoverable same-day if missed | **No** (not exhaustively checked for actual misses) |
| `run_carts_persist` | `main.py:1116` | 06:00-06:30 AZ (30min) | Low — observability only | No, but wide window makes a miss unlikely |
| `run_crew_scanner_job` | `main.py:3828` | 5×~10-11min slots | Low-moderate — has a fallback interval path | Partial |
| `run_gap_scan`/`run_premarket_gaps` | `main.py:1935`/`3573` | 60-min windows | Low — wide relative to observed delays | No, low practical risk |
| `run_bk_orb_scan` | `main.py:2257` | 09:46-12:00 ET (134min) | Low — feature flag default OFF anyway | No, moot while disabled |

**Takeaway: not a one-off.** Two jobs (bridge_vote, daily_rating_update) now **directly confirmed** to have silently skipped entire days from scheduler-queue delay; `run_oi_morning_snapshot` shares the identical fragile shape. Recommend a shared helper that logs a WARNING when a narrow-window job's tick lands outside its window with no "done today" flag set, and widen windows to ≥20-30min wherever tolerable.

**[RED] A3 — wall-time health verdict: queue is at/beyond saturation, and worsening.** `[WR-DUR]` cycle wall= (war-room debate cycle), last 500 entries (last 4 trading days): **p50=298.6s, p95=777.7s, p99=3245.0s, max=3245.6s (54min)**, 48.6% of recent cycles ≥300s (the nominal 5-min tick). Full-history comparison (p50=37.7s, p95=528.5s) proves this has gotten markedly worse *recently*, not a steady-state condition. Two other frequent handlers (`run_guardian_sweep` p95=217.5s/max=775.8s, `run_autopilot` p95=229.9s/max=333.2s) compound additively in the same single-threaded queue. **This is the leading suspect root cause behind A2's confirmed misses, and per the Executive Summary, very plausibly downstream of the fleet-size breach in Section C/E.**

---

## B. DATA & SOURCES

**[INFO] B4.0** Full `source_registry` (21 rows) cross-checked against live `engine.source_gate.all_health()`: `{green: 14, amber: 0, red: 0, unknown: 2, retired: 4, dormant: 1}`. **No live_decision source is currently RED or stale** — `signals`, `bridge_consensus`, `riker_synthesis` all GREEN at query time.

**[AMBER] B4.1** `signals` (live_decision) producer `engine/signal_bridge.py::_post_signal()` swallows POST failures with a bare `except Exception: return 0`, **no log call**. The caller only logs when `emitted>0` — a run where every single post fails produces zero log trace, not even "0 emitted." Recommend: log on the except branch; gate the caller's logging on `failed>0`, not just `emitted>0`.

**[AMBER] B4.2** `gex_snapshot`'s registry `ts_format` points at `data.SPY.asof` but the live API field is actually `data.SPY._asof` (underscore-prefixed) — permanently forces UNKNOWN state, masking otherwise-fresh data (verified live: `_asof: "2026-07-17 20:05:19"`, real and recent). Feeds a W3 gamma-strategy-mapper freshness gate despite being tagged context-only. One-line registry fix.

**[GREEN] B4.3 / B4.4** `bridge_consensus` and `riker_synthesis` producers both log failures explicitly — not silent-failure-capable.

**[GREEN] B5.1-B5.3 — sacred DB integrity, all clean.** `trader.db` (~914MB, WAL active, `PRAGMA integrity_check` → `ok`). `arena.db` (0 bytes, confirmed still deprecated/unchanged since 2026-07-06 → `ok`). `tractor.db` located at `/Users/bigmac/ollietrades_archived_2026-07-06/tractor_beam/tractor.db` (matches documented disposition, last real write Apr 14 → `ok`).

**[GREEN] B6.1-B6.4 — backup spine fully verified, including a real restore test.** `db_snapshot.sh` (nightly 20:15 MST) confirmed actually running through 07-17. `backup_freshness_check.sh` confirmed running, dual-checks local + off-host independently. Off-host replication to olliemax verified live via SSH — byte-identical file sizes confirmed for `trader_2026-07-17.db` on both sides, 5 consecutive nights of `SUCCESS` in the log. **Restore test:** copied `data/backups/trader_2026-07-17.db` to `/tmp`, ran `PRAGMA integrity_check` (`ok`) and real queries (`SELECT COUNT(*) FROM ai_players` → 82, `SELECT COUNT(*),MAX(executed_at) FROM trades` → 2733/2026-07-16) — **the backup is genuinely restorable, not just present.**

---

## C. FLEET & TRADING LOGIC

**[INFO] C7.1 — current roster: active=75, exit_only=1, full=6 (82 total).** Versus the 2026-07-01 documented waypoint (15/9/55/79): active **+60**, full **-49** — a much larger, opposite-direction swing than any prior documented drift. No ticket tracks this. **See Executive Summary — this cross-references directly with E13.6's `MAX_ACTIVE_AGENTS=8` breach and A3's queue saturation.**

**[AMBER] C7.2** Season 6 `end_date=2026-07-10` (8 days before this audit), no Season 7 row exists in `season_config` — plausible but **unconfirmed** trigger for the mass reactivation.

**[AMBER] C7.4** `picard`/`riker` still `halt_mode='active'` despite CLAUDE.md documenting both as retired 2026-06-24 (`crew_role='benched'` but halt_mode never flipped) — fields disagree.

**[AMBER] C7.5** Zombie check (active + zero trades/signals in 30d) flags 64/75 active agents, but the methodology has a confirmed false-positive mode (`ollie-machine` flagged but has 18 recent rows in its own dedicated ledger table, not `trades`/`signals`). Full candidate list captured in the source agent output — needs per-agent re-verification against each subsystem's own table before any halt action, not a blind kill list.

**[RED] C7.6** 22 `navigator_bm_*`/`navigator_bn1_*` benchmark-bakeoff seats are active with **zero rows ever** in their dedicated `bakeoff_trades` table (all-time, not just 30d) — genuine cleanup candidate, not a false positive.

**[GREEN] C7.7** Decision Desk (`DESK_EXECUTE_ENABLED`) confirmed OFF, correctly gated (`dashboard/app.py:5412-5433`, unset in `.env`, HTTP 403 when off, test coverage exists).

**[GREEN] C8.1/C8.2** All 4 canonical execution gates (`bull_call_spread_v1`, `bear_put_spread_v1`, `bull_spread_v1`, `executor`) confirmed `True`/armed. No kill-switch active (`KILL_SWITCH` file absent, `kill_switch_log` has never fired).

**[AMBER] C8.3** "Dead ticker" filtering is two separate, narrower mechanisms (money-market filter vs. `DELISTED_BLACKLIST`+tradability gate), not one unified gate as framed. The noisy "possibly delisted" log tickers (MMAN, AABT, OO, etc.) pass through **neither** — they die at the data-fetch layer before ever reaching a gate decision (zero rows in `signals`/`signals_v2` for any of them, ever). Harmless but wastes API calls; root cause of the malformed symbols (doubled-first-letter pattern) UNVERIFIED.

**[GREEN] C8.4** Sufficiency-style gates exist in two forms (not literally "n≥400/20 trading days" as originally framed — that exact figure wasn't found in code, marked UNVERIFIED): `adaptive_tuner.py` (`MIN_DAYS=15`, currently satisfied at 19 days for all 5 buckets) and `ollie_machine_p4_gate.py` (`FLOOR_MIN_TRADES=30`, currently at 18/30, **not yet satisfied**).

**[GREEN] C9.1/C9.3** Shadow-witness A/B family (incl. plutus v7d) — already investigated, killed (commit `2a59e0c`), confirmed holding live: `SHADOW_WITNESS_ENABLED='false'`, zero new writes to `witness_ab`/`plutus_shadow_critiques` in 7-8 days, cron line removed. No action needed.

**[INFO] C9.2** "grade-B reversal relax" — not found anywhere in `docs/XO_BACKLOG.md` or `CLAUDE.md` under that name. UNVERIFIED, possibly mis-transcribed; recommend supplying an HM-ticket ID if this needs following up.

**[AMBER] C9.4** gemini/IREN: the documented remediation step (flip `exit_only`→`full` once IREN closes) was bypassed — IREN position is closed (benign outcome) but the agent landed on `active` instead, consistent with getting swept into the same mass-reactivation event as C7.1.

---

## D. SECURITY & ACCESS

**[RED] D10.4 — independently re-verified live by the lead session.** `tour_api.py` (port 8088, `uvicorn.run(host="0.0.0.0")`) has zero auth wiring anywhere in the file and exposes `POST /api/paper/order`. Confirmed via direct `curl` to the box's real LAN IP (`192.168.1.248:8088`, not loopback) with the harmless `__noop__` payload — got a real response, proving live LAN reachability. Box also has an active Tailscale interface (`100.103.190.24`), extending reachability to the Tailscale mesh too. Blast radius bounded by a hard `route='alpaca_paper'` assert (cannot touch Schwab/live capital). Recommend: apply the same `AuthMiddleware`/`try_cf_access` pattern already proven in `dashboard/app.py` and `swingdesk/backend.py`, or simplest — bind `127.0.0.1` since tunnel ingress for `tour.ollietrades.com` already covers external access.

**[GREEN] D10.2/D10.3** Bridge (8080) and SwingDesk (8889) both bind `0.0.0.0` but this is confirmed *deliberate and correctly implemented* — both have real `AuthMiddleware` enforcing loopback-or-authenticated, verified live in the registered middleware stack. Not oversights.

**[AMBER] D10.5** `status_page.py` (port 8090) binds `0.0.0.0`, no auth, **not** in the cloudflared ingress list at all — pure direct-LAN exposure, no tunnel path, no CF Access. Read-only (GET only) so risk is information disclosure, not write/execute. Recommend bind to `127.0.0.1` unless LAN-wide access is intentional.

**[AMBER] D10.6** `ib_server.py` (port 5001) — separate project outside this repo (`~/ib_chart`), same box, `0.0.0.0` bind, no auth found on its GET routes. Flagged for awareness, out of this repo's direct scope.

**[AMBER] D10.8** macOS Application Firewall is globally **disabled** — app-layer auth (D10.2/D10.3) is the only thing standing between the LAN and bridge/swingdesk; no OS-level network ACL as a second layer. Recommend enabling as defense-in-depth.

**[GREEN] D11.1-D11.3 — secrets hygiene clean.** Zero hardcoded credential-shaped strings found outside `.env` across 8 distinct regex patterns (API keys, Bearer tokens, AWS keys, GitHub/Slack/Anthropic-style tokens). `.env` permissions: `600` (owner-only). `.gitignore` correctly excludes `.env` and variants; confirmed not tracked in git.

**[GREEN] D12.2 — git history secrets scan clean (time-capped, high-confidence not proof-of-absence).** `-S"API_KEY="` and `-S"AKIA"` each returned a small number of hits, all individually verified as placeholder templates or coincidental substring matches inside a vendored JS blob — no real secret found. Recommend a full `gitleaks`/`trufflehog` sweep as a follow-up if stronger-than-high-confidence assurance is wanted (out of scope for this pass's time budget).

**[INFO] D12.1** Uncommitted-changes inventory: 2 modified tracked files (`dashboard/app.py`, `drafts/daily_ledger.csv`), 24 untracked (backup-ID files, doc/report artifacts, 3 scripts) — routine operational accumulation, no secrets observed in any filename, no urgency.

---

## E. KNOWN-ISSUE LEDGER

*(66 items total covered across the full `docs/XO_BACKLOG.md`, 9,787 lines. Full detail preserved in the research pass; highest-severity and named items below — see the "remaining open items" catalog in the underlying research output for the complete lower-priority tail.)*

**Named items requested:**
- **[GREEN] HM-CRONTAB-EINTR** — not reproducing on the read path today (5x back-to-back `crontab -l`, all clean). Write-path retest blocked by this session's own mutation classifier (read-only audit constraint) — genuinely unverified for the write path, which is what actually failed historically. Close as stale for now, note the caveat.
- **[RED] HM-SIGNALS-V2-STARVATION-RECURRENCE** — confirmed and worsening (see Executive Summary). 8,290 pending rows now vs. 140 at filing, oldest 8+ days. The ticket's own automated Monday check already proved "CONFIRMED OUTRANKED" but that result never made it back into the backlog doc. **Fix now.**
- **[GREEN] HM-SHELLY-WATCHDOG** — confirmed still exactly as documented, design-only, deliberately deferred (DB-corruption risk on power-cut is sound reasoning). No drift. Defer.
- **[AMBER] HM-DAX** — `ollama-qwen3`/Lt. Dax confirmed still 100% stock scalps, zero options, unchanged since filing, no Admiral decision recorded. The counterpart seat the ticket assumed exists (`shadow-qwen35-csp`) appears to have vanished — re-verify before deciding. Cheap decision, fix now.
- **[GREEN] HM-AM** — all four phases confirmed shipped and live in code, no discrepancy found. No action.

**Other high-severity items found (RED/AMBER, live-reverified):**
- **[RED] HM-SCHWAB real-cash data 43 days stale** — OAuth refresh token invalid/expired, silently falling back to a 2026-06-05 CSV snapshot, **zero alarm coverage** (neither `hm_ops_sentinel.py` nor `source_health_watcher.py` watches Schwab). Real cash, per RULE #1's own read-only-tracking mandate — the data being wrong defeats the purpose. Fix now.
- **[AMBER] HM-GATE-REJECT-LOG garbage prices** — 3.7% of recent gate-reject rows (576/15,527, 30 days) have sub-$1 prices for normal stocks, actively still writing bad rows today, corrupts `counterfactual_report.py` stats. Contained root cause (upstream price pass-through). Fix now.
- **[RED] HM-ORPHAN-SEATS** — 13 active seats (worse than the 11 originally filed) attempt inference against Ollama models confirmed absent from olliemax's live model list, every cycle. Fix now (repoint-or-retire pass).
- **[RED] HM-LESSON-SHADOW** — `lesson_validation_shadow` confirmed silent for 42+ days (last row 2026-06-06), no scheduler entry anywhere. Fix or formally retire — ambiguous status keeps resurfacing on every audit.
- **[RED] HM-PERF-FLEET-THROUGHPUT** — an explicit "DO NOT DECLARE SUCCESS" verdict with a defined 07-08 decision-tree was never executed; system has run 11 days on an unvalidated setting. Fix now (close the decision).
- **[AMBER] HM-SCAN-LIVENESS-WATCHDOG fast-lane decoupling** — the ticket's own stated top-priority follow-up (decouple user alert-defs from the LLM scan lock) never built; already caused a confirmed missed-alert incident (a real FOMC-minutes window missed during a 68-min scan cycle). Fix now.
- **[AMBER] HM-EXTERNAL-FETCH-DISCIPLINE-AUDIT** — HIGH-priority unbounded-fetch hang risk confirmed still live and unaddressed in `market_data.py`/`whisper_network.py`.
- **[AMBER] HM-WAL-BUSY-TIMEOUT-HYGIENE wave 2** — scoped 07-07, never executed; raw `sqlite3.connect()` site count trending *up*, not down, across the codebase.
- **[AMBER] Meta-label gate permanently stalled** — its own precondition (a clean week of forward-generated `signal_labels`) can never be satisfied because the producer (`label_signals.py`) isn't scheduled anywhere — zero new labels in 11 days. Needs an explicit Admiral flag; will silently block forever otherwise.
- **[AMBER] HM-DRAWDOWN-BLIND-TO-OPTIONS-PNL** — filed as "zero current impact"; live query shows the concerned agent's real closed-options P&L is actually +$29,868.74 (gross, needs era-cutoff cross-check) — the "no impact" premise no longer holds at face value.
- **[AMBER] HM-ARMED-DORMANT-SPREAD-STRATEGIES residual** — the SPY-only leg self-healed as predicted after the 07-10 fix (26 rows recovered), but the two 10-ticker sibling strategies are still at zero rows 8 days later — the ticket declared success prematurely for those.

**Confirmed already resolved despite an open-looking doc header (queue rot, doc-cleanup only, no code action needed):** XO-DEPARTURE-HARDENING Phase 1&2, HM-SWEEP-CADENCE, HM-ADVISORY-CREW-DRIFT-SWEEP, HM-TROI-MAXPOS-CAP-DEAD, HM-SCORECAP-REVISIT, HM-STOP-COVERAGE-GAP, HM-SILENT-CATCH-SWEEP, HM-CF-502-BLIP, HM-UNEXPLAINED-ARTIFACTS, HM-FINVIZ-ELITE-AUTH, HM-T (PED), and roughly a dozen more — full list in the underlying research output. **Recommend a documentation pass to correct these doc statuses so they stop contradicting live reality on future audits.**

Remaining ~35 lower-severity/deferred items (dashboard UX polish, sequencing-blocked features, Admiral-decision-pending items with no live urgency) are cataloged in full in the source research pass but omitted here for report length — available on request, nothing in that tail changes this report's priority ranking.

---

## RECONCILIATION-DRIFT ADD-ON (2026-07-18, delivered mid-session, folded in for completeness)

Cross-referenced `data/reconciliation/2026-07-18.json` against `engine/reconciliation.py`'s actual comparison logic and live DB state:

- **11 equity symbols flagged "in_alpaca_not_internal" → false positive**, a reconciliation-script design artifact (all 11 are genuinely held internally under `alpaca-mirror`, which is deliberately excluded from the drift comparison to avoid circular self-comparison — the exclusion has the side effect of making its real holdings invisible to the other routed players' diff). Not a bug in the books, but a misleading report as currently written.
- **AG qty mismatch → real drift, root-caused.** `neo-matrix` trade id=3048 (SELL 9.2665 AG, 2026-07-15) has `alpaca_order_id=NULL` — recorded internally, never confirmed executed at the broker.
- **2 SPY put legs "in_internal_not_alpaca" → real drift, same root-cause class.** `options_trades` id=140 (`strategy:bull_spread_v1`, entered 07-13): `broker_order_id=NULL, venue=NULL` despite `status='open'`.
- **Broader pattern found:** 6 unconfirmed SELLs (null `alpaca_order_id`) across the 3 actively-routed players in the last 2 weeks — AG is the one still visibly drifting today; the other 5 likely self-resolved via later real trades but represent the same underlying gap. **Severity: AMBER** — small dollar impact, but a real forward-path/confirmation-writeback bug worth an engineering look.
- Added `HM-ARCHER-PORTRAIT-REPEAT-FETCH` to `docs/XO_BACKLOG.md` (low priority, not investigated further — 22 references in `index.html`, 16 inside per-chat-message avatar template strings, plausible explanation for "18 requests," root cause of whether they're real re-fetches vs. cache hits undetermined).

---

## F. TOP 10 RECOMMENDATIONS (ranked by risk reduced vs. effort — best return first)

1. **[TRIVIAL effort, HIGH risk reduced] Fix `tour_api.py`'s unauthenticated `/api/paper/order` (D10.4).** Bind `127.0.0.1` (tunnel ingress already covers external access) or port the existing `AuthMiddleware` pattern from `dashboard/app.py`/`swingdesk/backend.py`. Closes the one live, exploitable, zero-auth gap found in this audit.

2. **[TRIVIAL effort, HIGH risk reduced] Un-jam the duplicate schedulers (A1.1, A1.5).** `com.ollietrades.crusher` and `engine/riker_synthesis.py` are each running on two overlapping schedules simultaneously — unload one copy of each. Zero-risk, immediate reduction in wasted cycles/duplicate writes.

3. **[TRIVIAL effort, HIGH risk reduced] Restart the wedged Ollama daemon (A1.2) and fix `morning_cd_instr.sh`'s wrong log path (A1.4).** Unblocks 5 consecutive days of broken premarket scanning and kills a 2-month-running false-positive alert that's been training the Admiral to ignore that alert channel.

4. **[MODERATE effort — mostly a decision, HIGH risk reduced] Resolve the fleet-size / MAX_ACTIVE_AGENTS breach (Exec Summary, C7.1, E13.6).** Get an explicit Admiral ruling on whether the 15→75 active-agent swing was intentional. If not, re-halting to design capacity is the single highest-leverage fix in this whole audit — it's the leading suspect behind the scheduler saturation (A3) that's directly causing silent job misses (A2). If it *was* intentional, the scheduler needs capacity-planning for 75 concurrent agents instead of 8, which is a much larger effort — worth knowing which branch you're on before investing in either.

5. **[LOW-MODERATE effort, HIGH risk reduced] Fix the Schwab real-cash data feed (E13.7).** OAuth token refresh, plus add Schwab to `source_health_watcher.py`'s coverage so this class of failure can't go 43 days unnoticed again. Concerns real cash, not paper.

6. **[MODERATE effort, HIGH risk reduced] Implement HM-SIGNALS-V2-STARVATION-RECURRENCE's own proposed fix (E13.2).** Candidate (a) TTL/age-cap auto-expiry is already scoped in the ticket. 8,290 pending rows and growing; also fix the process gap where the ticket's own verified Monday-check result never got written back to the backlog doc.

7. **[LOW effort, MODERATE risk reduced] Fix the `HM-GATE-REJECT-LOG` sub-$1 price bug (E13.9).** Contained root cause in `paper_trader.py`'s price pass-through; currently corrupting 3.7% of gate-reject rows and every downstream `counterfactual_report.py` statistic.

8. **[LOW effort, MODERATE risk reduced — remove/simplify] Clean up 13 `HM-ORPHAN-SEATS` (E13.10) + 22 zombie `navigator_bm_*`/`navigator_bn1_*` bakeoff seats (C7.6) + fix the `picard`/`riker` halt_mode/crew_role mismatch (C7.4).** All three are mechanical repoint-or-halt passes, no design work needed, meaningfully reduces both wasted-cycle load (ties back to #4's scheduler pressure) and roster-audit noise on future passes.

9. **[LOW effort, MODERATE risk reduced] Fix `HM-ORPHAN-SEATS`-adjacent silent-failure logging in `engine/signal_bridge.py::_post_signal` (B4.1) and the `gex_snapshot` registry dotpath typo (B4.2).** Both are one-function/one-line fixes that close real silent-failure gaps on live_decision-adjacent data paths.

10. **[LOW effort, LOW-MODERATE risk reduced — remove/simplify] Documentation-hygiene pass on `docs/XO_BACKLOG.md`.** ~15 items across Section E are confirmed already resolved but still carry an open-looking header (queue rot) — correcting these prevents future audits from re-spending time re-verifying already-closed work, and prevents the doc from contradicting `CLAUDE.md` (e.g. `HM-GIT-PUSH-HEALTH-MONITOR` is marked OPEN in the backlog but SHIPPED in `CLAUDE.md`, both currently true statements about different files, actively confusing).

*(Runners-up just outside the top 10, in case appetite exists for more: `status_page.py`/`ib_server.py` LAN exposure (D10.5/D10.6), `HM-EXTERNAL-FETCH-DISCIPLINE-AUDIT`'s unbounded-fetch hang risk, `HM-WAL-BUSY-TIMEOUT-HYGIENE` wave 2, and the reconciliation ADD-ON's 6-incident unconfirmed-SELL pattern.)*

---

## NEEDS ADMIRAL (consolidated across all sections)

- **A1** — `/Library/LaunchDaemons/com.nordvpn.macos.helper.plist`: permission denied without sudo. Unrelated to OllieTrades, low priority, noted for completeness only.
- **C** — The Season 6→7 causal link for the mass fleet reactivation (C7.2) could not be confirmed from any doc/ticket — needs the Admiral to state directly whether this was deliberate. Root crontab (vs. user crontab) was not checked — confirm it's also clean if that's a plausible deployment pattern here.
- **D** — `pfctl -s info`: permission denied without sudo; could not verify whether a packet-filter ruleset (separate from the confirmed-disabled Application Firewall) provides any LAN-blocking for ports 8088/8090/5001. ARDAgent (port 3283) intentionality is a System Settings toggle, not CLI-verifiable. Full-repo secrets-in-git-history scan was time-capped (45s/pattern) — high-confidence, not proof-of-absence; a full `gitleaks`/`trufflehog` sweep would need dedicated runtime budget.
- **E** — `signal.ollietrades.com`'s Cloudflare Access gate status (the CLAUDE.md:527 TODO) genuinely can't be verified read-only from this box — no Cloudflare API token available in `.env` (consistent with the earlier HM-SWING-HOSTNAME-ROUTING investigation this session, which hit the same limitation). Needs Cloudflare dashboard or a scoped read-only API token. The crontab EINTR write-path retest (E13.1) was blocked by this session's own mutation-action classifier — read path confirmed clean, write path genuinely unverified. Item 4/E13.6/E13.63's fleet-roster and `MAX_ACTIVE_AGENTS` policy questions are Admiral-decision items, not technical blockers, but no code fix should proceed on them without an explicit ruling given the scale of drift found.

---

*Full unabridged Section E research output (all 66 backlog items with individual evidence) is preserved in this session's agent transcripts if a complete copy is wanted — this report includes the highest-severity ~20 plus a summary of the rest to keep the relay document a reasonable read.*
