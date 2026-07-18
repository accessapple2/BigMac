# Relay: HM-SEASON-ROTATION-HALT-RESET-CORRECTION

**Date:** 2026-07-18, follow-on to HM-FULL-AUDIT-2026Q3.
**Trigger:** Admiral confirmed the 65-agent reactivation found in the audit was **not intentional** and directed a root-cause + restore.

## Root cause — confirmed with exact timestamp and mechanism

**2026-07-12 23:59:02 MST** (Sunday night), `main.py`'s scheduled job `run_season_rotation()` (`main.py:5521-5537`, `schedule.every(30).minutes.do(...)`, fires when `weekday()==Sunday and hour==23 and minute>=55`) fired automatically and called `engine.season_manager.rotate_season()` (`engine/season_manager.py:122-191`).

**This is not a bug in the sense of broken code — it's the documented, working-as-designed weekly season-rotation feature**, whose docstring literally says "Seasons rotate automatically every Sunday at 11:59 PM MST." The design flaw is that `rotate_season()` unconditionally resets **every** `ai_players` row except `webull`/`alpaca-mirror`/`neo-matrix`:

```python
# engine/season_manager.py:161-165
conn.execute(
    "UPDATE ai_players SET halt_mode='active', halt_reason=NULL, halted_at=NULL "
    "WHERE id NOT IN ('webull','alpaca-mirror') AND id != ?",
    (NEO_PLAYER_ID,)
)
```

with zero carve-out for permanently-retired personas (picard/riker), zombie benchmark seats (22 `navigator_bm_*`/`navigator_bn1_*`), MAX_ACTIVE_AGENTS-cap-enforced seats (archer/cto-grok42/holly-scanner/q-witness/quark-ic/sell-the-news/energy-arnold — all carry a `[2026-07-05] HM-ROSTER-CAP: roster reconciliation to MAX_ACTIVE_AGENTS=8` halt_reason), or any other deliberately-halted agent. It also wipes cash to $7000 and `DELETE FROM positions` for every affected agent.

**Live confirmation:** `logs/trader.log` — `Season auto-rotation complete → Season 7` at `[2026-07-12 23:59:02]`; `settings.current_season='7'`, `settings.season_7_start='2026-07-12T23:59:02.688095'`; `season_history` has 81 rows dated `2026-07-12T23:59:02.679129` (Season 6 close-out). This exactly brackets the 07-12→07-13 snapshot diff that first surfaced the discrepancy.

## Restoration performed

Source of truth: `data/backups/trader_2026-07-12.db` (20:15 MST nightly snapshot, the last clean pre-rotation state). Restored `halt_mode`, `halt_reason`, `halted_at` for all 65 affected agent IDs to their exact pre-rotation values via individual parameterized `UPDATE`s against the live `data/trader.db`. Scope was deliberately limited to **status fields only**, per the Admiral's request — cash and deleted positions were NOT restored (see "Not restored" below).

**One correction made mid-restoration:** the naive restore would have overwritten `ollie-auto`'s halt_reason with the stale 07-12 value (`door1-cut: has open positions, exit_only for wind-down`, dated 2026-06-19), clobbering a **legitimate, more recent** halt decision (`[2026-07-17] HM-PG-ESCALATION: Proving Ground kill_warning unacknowledged for 10 days`, dated 2026-07-17 — i.e. made *after* the rotation bug, independently of it). Caught this by diffing every restored row's live pre-write state against dates ≥2026-07-13 before finalizing, found this one case, and corrected it back to the 2026-07-17 value. `halt_mode` itself was `exit_only` in both cases so no functional change was at risk — only the audit-trail fields.

### Full before/after ledger (65 rows)

```
id | pre-restore (live, wrongly active) | restored to (pre-rotation truth, or corrected post-rotation truth for ollie-auto)
anderson-bcs | active/None/None -> full/'2026-05-05 retired via HM-T-fleet bundle (Option 1 halt-only; code preserved per sacred-data rule, halt prevents activity)'/'2026-05-05 14:09:20'
archer | active/None/None -> full/'[2026-07-05] HM-ROSTER-CAP: roster reconciliation to MAX_ACTIVE_AGENTS=8 -- not in the measured/auditioning 8, requires a passing audition (AUDITION_CRITERIA) to reactivate'/'2026-07-05 14:22:19'
chekov | active/None/None -> full/'orphan row — real Chekov is navigator (id=navigator). Hard-halted 2026-05-11. (halted_at backfilled HM-HALTED-AT-BACKFILL 2026-05-19)'/'2026-05-11 00:00:00'
claude-haiku | active/None/None -> full/'HM-AK 2026-05-07 dormant cleanup (no activity 7d; see OPS_LOG) | HM-FLEET-REBASELINE-2026-07-04: sweep confirms retirement, guarded return 0.68% <9%, spam_rate 75.1%>48%'/'2026-05-07 17:18:36'
claude-sonnet | active/None/None -> full/'HM-AK 2026-05-07 dormant cleanup (no activity 7d; see OPS_LOG) | HM-FLEET-REBASELINE-2026-07-04: sweep confirms retirement, guarded return 2.67% <9%, spam_rate 83.8%>48%'/'2026-05-07 17:18:36'
covered-call | active/None/None -> full/'2026-05-05 retired via HM-T-fleet bundle (Option 1 halt-only; code preserved per sacred-data rule, halt prevents activity)'/'2026-05-05 14:09:20'
cto-grok42 | active/None/None -> full/'[2026-07-05] HM-ROSTER-CAP: roster reconciliation to MAX_ACTIVE_AGENTS=8 -- not in the measured/auditioning 8, requires a passing audition (AUDITION_CRITERIA) to reactivate'/'2026-07-05 14:22:19'
dalio-metals | active/None/None -> full/'[2026-06-19] door1-cut: -$164 realized, negative-expectancy'/'2026-06-20 02:11:43'
deepseek-7b-grok4 | active/None/None -> full/'[2026-06-19] door1-cut: -$471 realized, negative-expectancy'/'2026-06-20 02:11:43'
energy-arnold | active/None/None -> full/'[2026-07-05] HM-ROSTER-CAP: roster reconciliation to MAX_ACTIVE_AGENTS=8 -- not in the measured/auditioning 8, requires a passing audition (AUDITION_CRITERIA) to reactivate'/'2026-07-05 14:22:19'
gemini-2.5-flash | active/None/None -> full/'2026-07-07 HM-FLEET-REBASELINE-2026-07-04 retirement complete: guarded return 8.93%<9%, spam 54.5%>48%; IREN position closed, exit_only->full flip executed by scripts/iren_flip_watch.py'/'2026-07-07 14:15:00'
gemini-2.5-pro | active/None/None -> full/'Retired S6.3 — qwen3:14b too heavy for bigmac, no active role. Halted 2026-04-30 (routingleak fix). | HM-FLEET-REBASELINE-2026-07-04: guarded honest return 7.06% <9%, spam_rate 61.7%>48% (fleet_realism_sweep_20260704_073227.json)'/'2026-04-30 00:00:00'
ghost-kirk-0dte-bc | active/None/None -> full/'2026-05-05 retired via Option-4 ghost bundle (Option B halt-only; preview-only subsystem; same pattern as 06b5ce7 production halt)'/'2026-05-05 15:57:18'
ghost-kirk-bc | active/None/None -> full/'2026-05-05 retired via Option-4 ghost bundle (Option B halt-only; preview-only subsystem; same pattern as 06b5ce7 production halt)'/'2026-05-05 15:57:18'
ghost-long-call | active/None/None -> full/'2026-05-05 retired via Option-4 ghost bundle (Option B halt-only; preview-only subsystem; same pattern as 06b5ce7 production halt)'/'2026-05-05 15:57:18'
ghost-naked-put | active/None/None -> full/'2026-05-05 retired via Option-4 ghost bundle (Option B halt-only; preview-only subsystem; same pattern as 06b5ce7 production halt)'/'2026-05-05 15:57:18'
gpt-4o | active/None/None -> full/'HM-AK 2026-05-07 dormant cleanup (no activity 7d; see OPS_LOG) | HM-FLEET-REBASELINE-2026-07-04: sweep confirms retirement, guarded return 3.73% <9%, spam_rate 55.5%>48%'/'2026-05-07 17:18:36'
gpt-o3 | active/None/None -> full/'HM-AK 2026-05-07 dormant cleanup (no activity 7d; see OPS_LOG)'/'2026-05-07 17:18:36'
grok-3 | active/None/None -> full/'S6 review: routing zombie, retired 2026-04-25 | HM-FLEET-REBASELINE-2026-07-04: guarded honest return 2.28% <9%, spam_rate 62.6%>48% (fleet_realism_sweep_20260704_073227.json)'/'2026-04-25 00:00:00'
grok-4 | active/None/None -> full/'HM-AK 2026-05-07 dormant cleanup (no activity 7d; see OPS_LOG)'/'2026-05-07 17:18:36'
guardian-of-forever | active/None/None -> exit_only/'HM-GUARDIAN-ADOPTION 2026-06-12: exit-only stop guardian for orphan Alpaca positions; may NEVER buy. Flat 12% stop, routes exits to Alpaca.'/'2026-06-12 17:21:18'
holly-scanner | active/None/None -> full/'[2026-07-05] HM-ROSTER-CAP: roster reconciliation to MAX_ACTIVE_AGENTS=8 -- not in the measured/auditioning 8, requires a passing audition (AUDITION_CRITERIA) to reactivate'/'2026-07-05 14:22:19'
mccoy-bps | active/None/None -> full/'2026-05-05 retired via HM-T-fleet bundle (Option 1 halt-only; code preserved per sacred-data rule, halt prevents activity)'/'2026-05-05 14:09:20'
mlx-qwen3 | active/None/None -> full/'[2026-05-20] HM-FLEET-COUNT-CLEANUP: season-1 carryover halted to match Season 5/6 operational fleet'/'2026-05-20 18:39:47'
navigator_bm_devstral-s2_new | active/None/None -> full/'HM-BM bakeoff clone — audit trail only'/'2026-05-16 15:52:31'
navigator_bm_devstral-s2_old | active/None/None -> full/'HM-BM bakeoff clone — audit trail only'/'2026-05-16 15:52:31'
navigator_bm_gemma4-26b_new | active/None/None -> full/'HM-BM bakeoff clone — audit trail only'/'2026-05-16 15:52:31'
navigator_bm_gemma4-26b_old | active/None/None -> full/'HM-BM bakeoff clone — audit trail only'/'2026-05-16 15:52:31'
navigator_bm_gemma4-31b_new | active/None/None -> full/'HM-BM bakeoff clone — audit trail only'/'2026-05-16 15:52:31'
navigator_bm_gemma4-31b_old | active/None/None -> full/'HM-BM bakeoff clone — audit trail only'/'2026-05-16 15:52:31'
navigator_bm_llama4-scout_new | active/None/None -> full/'HM-BM bakeoff clone — 67GB on 32GB ceiling, expected INCONCLUSIVE'/'2026-05-16 15:52:31'
navigator_bm_llama4-scout_old | active/None/None -> full/'HM-BM bakeoff clone — audit trail only'/'2026-05-16 15:52:31'
navigator_bm_ministral3-3b_new | active/None/None -> full/'HM-BM bakeoff clone — audit trail only'/'2026-05-16 15:52:31'
navigator_bm_ministral3-3b_old | active/None/None -> full/'HM-BM bakeoff clone — audit trail only'/'2026-05-16 15:52:31'
navigator_bm_qwen36-27b_new | active/None/None -> full/'HM-BM bakeoff clone — audit trail only'/'2026-05-16 15:52:31'
navigator_bm_qwen36-27b_old | active/None/None -> full/'HM-BM bakeoff clone — audit trail only'/'2026-05-16 15:52:31'
navigator_bm_qwen36-35b-a3b_new | active/None/None -> full/'HM-BM bakeoff clone — audit trail only'/'2026-05-16 15:52:31'
navigator_bm_qwen36-35b-a3b_old | active/None/None -> full/'HM-BM bakeoff clone — audit trail only'/'2026-05-16 15:52:31'
navigator_bn1_baseline | active/None/None -> full/'HM-BN.1 bakeoff clone — audit trail only'/'2026-05-17 02:28:45'
navigator_bn1_gemma3 | active/None/None -> full/'HM-BN.1 bakeoff clone — audit trail only'/'2026-05-17 02:28:45'
navigator_bn1_gemma4e4b | active/None/None -> full/'HM-BN.1 bakeoff clone — audit trail only'/'2026-05-17 02:28:45'
navigator_bn1_llama31 | active/None/None -> full/'HM-BN.1 bakeoff clone — audit trail only'/'2026-05-17 02:28:45'
navigator_bn1_ministral | active/None/None -> full/'HM-BN.1 bakeoff clone — audit trail only'/'2026-05-17 02:28:45'
navigator_bn1_plutus | active/None/None -> full/'HM-BN.1 bakeoff clone — audit trail only'/'2026-05-17 02:28:45'
ollama-coder | active/None/None -> full/'[2026-06-19] door1-cut: 0 trades, never produced, dead-weight'/'2026-06-20 02:11:43'
ollama-deepseek | active/None/None -> full/'scorecard-driven cull 2026-06-06 (losing track record)'/'2026-06-07 03:38:49'
ollama-gemma27b | active/None/None -> full/'HM-AK 2026-05-07 dormant cleanup (no activity 7d; see OPS_LOG)'/'2026-05-07 17:18:36'
ollama-glm4 | active/None/None -> full/'HM-AK 2026-05-07 dormant cleanup (no activity 7d; see OPS_LOG)'/'2026-05-07 17:18:36'
ollama-kimi | active/None/None -> full/'[2026-06-19] door1-cut: -$1368 realized, negative-expectancy bleeder'/'2026-06-20 02:11:43'
ollama-llama | active/None/None -> exit_only/'S6 review: routing zombie, retired 2026-04-25'/'2026-04-25 00:00:00'
ollama-local | active/None/None -> full/'scorecard-driven cull 2026-06-06 (losing track record) | HM-FLEET-REBASELINE-2026-07-04: sweep confirms retirement, guarded return 0.94% <9%, spam_rate 85.1%>48%'/'2026-06-07 03:38:49'
ollie-auto | exit_only (already correct, reason CORRECTED to preserve 2026-07-17 post-rotation truth, NOT the 07-12 pre-rotation snapshot) -> exit_only/'[2026-07-17] HM-PG-ESCALATION: Proving Ground kill_warning unacknowledged for 10 days -- auto-halted new entries pending manual ship/kill decision'/'2026-07-17 20:18:14'
picard | active/None/None -> full/'briefing generator retired 2026-06-24; agent benched on-deck slot 1'/'2026-06-24 03:07:17'
q-witness | active/None/None -> full/'[2026-07-05] HM-ROSTER-CAP: roster reconciliation to MAX_ACTIVE_AGENTS=8 -- not in the measured/auditioning 8, requires a passing audition (AUDITION_CRITERIA) to reactivate'/'2026-07-05 14:22:19'
quark-ic | active/None/None -> full/'[2026-07-05] HM-ROSTER-CAP: roster reconciliation to MAX_ACTIVE_AGENTS=8 -- not in the measured/auditioning 8, requires a passing audition (AUDITION_CRITERIA) to reactivate'/'2026-07-05 14:22:19'
qwen-coder-haiku | active/None/None -> full/'HM-AK 2026-05-07 dormant cleanup (no activity 7d; see OPS_LOG)'/'2026-05-07 17:18:36'
qwen3-14b-grok3 | active/None/None -> full/'HM-AK 2026-05-07 dormant cleanup (no activity 7d; see OPS_LOG)'/'2026-05-07 17:18:36'
qwen3-14b-pro | active/None/None -> full/'[2026-06-19] door1-cut: 0 trades, never produced, dead-weight'/'2026-06-20 02:11:43'
qwen3-8b-4o | active/None/None -> full/'HM-AK 2026-05-07 dormant cleanup (no activity 7d; see OPS_LOG)'/'2026-05-07 17:18:36'
qwen3-8b-o3 | active/None/None -> full/'HM-AK 2026-05-07 dormant cleanup (no activity 7d; see OPS_LOG)'/'2026-05-07 17:18:36'
qwen3-8b-sonnet | active/None/None -> full/'[2026-06-19] door1-cut: 0 trades, never produced, dead-weight'/'2026-06-20 02:11:43'
red-alert | active/None/None -> full/'[2026-05-20] HM-FLEET-COUNT-CLEANUP: season-1 carryover halted to match Season 5/6 operational fleet'/'2026-05-20 18:39:47'
riker | active/None/None -> full/'synthesis job stood down 2026-06-24 with Picard; benched on-deck slot 2'/'2026-06-24 03:07:17'
sell-the-news | active/None/None -> full/'[2026-07-05] HM-ROSTER-CAP: roster reconciliation to MAX_ACTIVE_AGENTS=8 -- not in the measured/auditioning 8, requires a passing audition (AUDITION_CRITERIA) to reactivate'/'2026-07-05 14:22:19'
super-agent | active/None/None -> full/'is_paused=1 reconcile 2026-05-11 (halted_at backfilled HM-HALTED-AT-BACKFILL 2026-05-19)'/'2026-05-11 00:00:00'
```

## Verification

```
Fleet-wide halt_mode, post-restoration:
  active: 11   (was 75)
  exit_only: 3 (unchanged)
  full: 68     (was 6)
Total: 82 (unchanged, no rows added/removed)
```

Excluded-from-rotation IDs confirmed untouched throughout (`neo-matrix`, `alpaca-mirror`, `webull`, `dayblade-0dte`, `dayblade-sulu`, `navigator` all still `full`, consistent with pre-existing, separate, legitimate halts unrelated to this bug).

Current 11 active agents (post-restore): `capitol-trades`, `desk-manual`, `enterprise-computer`, `m5-allocator`, `ollama-plutus` (McCoy), `ollama-qwen3` (Dax), `ollie-machine`, `options-sosnoff` (Troi), `qwen3-4b-audition` (Worf cadet), `qwen3-8b-flash` (Worf), `trade-desk` — a coherent, focused production roster, roughly in line with the `MAX_ACTIVE_AGENTS=8` design intent (11 vs 8 is a small, plausibly-legitimate overage worth a separate look, not the 75 we had).

**Immediate effect confirmed live, no restart required** (halt_mode is read fresh every cycle per existing code comments): `[HM-EQ] snapshot pass: 12 fired across 12` at `2026-07-18 15:35:36`, down from `77 fired across 77` observed repeatedly throughout the prior day's audit (2026-07-17) — an ~85% drop in this specific per-cycle workload metric, immediately following the restore.

**Wall-time (`[WR-DUR]` war-room cycle) recovery — NOT YET VERIFIABLE.** `run_war_room()` (`main.py:1577`) explicitly early-returns on weekends ("Fully closed (weekends, overnight)") — confirmed via direct code read, and no `[WR-DUR]` log line has been produced since `2026-07-17 20:03:40` (before market close Friday). This is expected, not a new problem. **A true before/after wall-time comparison requires Monday 2026-07-20's market session.** The `[HM-EQ]` 77→12 figure above is the best currently-available proxy, since it's a direct measure of per-cycle workload that scales with active-agent count and isn't market-hours-gated — but it's not the same metric the original finding was based on. Recommend checking `[WR-DUR] cycle wall=` p50/p95 again Monday afternoon and comparing against this audit's baseline (p50=298.6s, p95=777.7s, last-4-day sample as of 07-18).

## Not restored (scope explicitly limited to "statuses" per the request)

- **Cash**: `rotate_season()` reset all 65 agents' cash to $7000 default. Not restored — would need a separate decision, since correct values may not simply be "whatever it was on 07-12" if any legitimate trading occurred in between.
- **Positions**: `rotate_season()` executed `DELETE FROM positions` for all affected agents. **10 real positions were silently deleted**: `energy-arnold` (QQQ short, -0.8967), `guardian-of-forever` (AVB/F/GM/IWP/KMI/SPGI/WMB, 7 positions), `ollie-auto` (BLK/PM, 2 positions). Not restored — `ollie-auto` is one of the 5 `ROUTED_PLAYERS` that forwards to real Alpaca paper trades, so blindly re-inserting stale position rows risks creating *new* reconciliation drift rather than fixing anything. Recommend a separate, deliberate decision on whether/how to reconcile these against Alpaca's actual current state before touching `positions`.

## ⚠️ URGENT — this will recur automatically in ~32 hours unless addressed

Today (2026-07-18) is Saturday. Tomorrow, **2026-07-19, is a Sunday** — `run_season_rotation()` has no minimum-interval guard, only a `weekday()==Sunday and hour==23 and minute>=55` check. **Unless `rotate_season()` is fixed or the automatic trigger is disabled first, this exact same blanket reset will fire again at 2026-07-19 23:59 MST (~32 hours from this report) and silently undo this entire restoration.**

## Recommendation (not implemented — code change, out of this correction's scope)

`engine/season_manager.py::rotate_season()` (and its sibling `start_season()`) should not do a blanket `halt_mode='active'` reset. Candidate fix: only unhalt agents whose `halt_reason` indicates a *season-scoped* halt (e.g. performance-based cuts meant to reset each season), while explicitly preserving `halt_mode` for anything tagged as a permanent retirement, a roster-cap exclusion (`HM-ROSTER-CAP`), or a bakeoff/audit-trail-only clone (`HM-BM`/`HM-BN.1`). Given the ~32-hour window above, this needs an Admiral decision now: ship a scoped code fix before Sunday night, or temporarily disable/no-op `run_season_rotation()`'s scheduler registration (`main.py:5537`) until a proper fix lands.
