# Scotty Pre-Flight Summary — 2026-05-10

**Diagnostic log:** `/tmp/scotty_preflight_20260510_1626.log`
**Method:** read-only single-batch pre-flight (Phase 0 of loose-ends sweep). All findings cross-checked against `git log`, code grep, DB `SELECT`, and `docs/XO_BACKLOG.md`.

## TL;DR

Most "May 4 caught up" items have **already shipped** in commits between 2026-05-04 and 2026-05-10. The directive's premise (May 9 options-flow alerter firing on Saturday with `0.0x baseline`) does **not** match the current repo — `0.0x baseline` pattern is absent from `engine/` / `signals/` / `agents/`, and weekend gates are already present in 30+ sites. Net: very few Apply targets remain valid; most loose ends are closed or out of scope.

## Status table

| Item | Status | Evidence | Action |
|---|---|---|---|
| Tier-2 landmine (M-1) | ❓ UNKNOWN | No "M-1" or "landmine" grep hits in `XO_BACKLOG.md`. Tier-2 gating refs are intentional (tractor-beam gate, `engine/risk_radar.py:170`). | Question file → skip |
| Weekend gate (B27 / "May 9 bug") | ✅ DONE | `healthcheck.py` has `_is_market_hours()` at lines 121-123, 316, 375, 548, 591-593 — already weekend-gated. `engine/options_agents.py:53,84` also weekend-gated. **No `0.0x` or `/ baseline` patterns found anywhere.** | Skip Task 1 — preconditions unmet |
| 0.0x baseline math | ❌ NOT FOUND | `grep -rn "0\.0x\|baseline\s*=\s*0\|/ baseline"` returns 0 hits in `engine/` / `signals/` / `agents/`. The only baseline file is `engine/volume_baselines.py` (rolling 20-day avg, no zero-division surface). | Skip — bug pattern does not exist |
| 12 commits pushed (May 4) | 🔧 OPEN | `git log origin/main..HEAD` shows **2 commits ahead** of `origin/main` (a18487e, b0260e6). Reference to "12 commits" appears to be dated. | Closure report: Admiral pauses VPN, pushes |
| accessapple sprint (B16) | ✅ MOSTLY DONE | `healthcheck.py:25` already uses `bridge.ollietrades.com`. **Only drift:** stale comment string at `healthcheck.py:476` still references `bridge.accessapple.com`. Functional code uses `TUNNEL_URL`. | Optional cosmetic in Task 4 (CLAUDE.md scope), or skip |
| B24 log rotation | 🔧 OPEN (preventive) | `logs/` total = 78 MB. **No log > 50 MB.** Largest: `crusher.log` 717 KB, `aladdin.log` 404 KB. Backlog marks B24 as "Phase 3 investigation report" — proposal only. | Task 2: ship `scripts/rotate_logs.sh` + plist proposal doc; do NOT truncate |
| CLAUDE.md updates | 🔧 OPEN (mild drift) | 528 lines, last touched 2026-05-08 (`ae425fb` dashboard canonical path). Known stale: `energy-arnold` model in some comment contexts; tractor.db SACRED-DATA reference (B23). | Task 4: surgical diff sync |
| NEW-1 second gate | ❓ UNKNOWN | No `NEW-1`, `gate_2`, `secondary_gate`, or `second gate` hits in code or backlog. | Question file → skip |
| energy-arnold parser | ❓ UNKNOWN | No parser failures in `logs/trader.log`. Only log lines are governance ("MANDATE BLOCKED: energy-arnold is a Bridge Voter") which is intentional. DB row `ai_players` = qwen3:8b; `main.py:104` instantiates qwen3:8b. **One stale comment** at `main.py:225` says `# Trip Tucker (phi3:mini)` — actual model is qwen3:8b. | Task 3: fix the stale Tier-2 comment block in `main.py:225` only; file question on "parser fix" if anything else needed |
| Short-squeeze scanner | ✅ SHIPPED | `engine/squeeze_scanner.py` exists (filter Float Short > 20%, score 1-10, persists to `squeeze_watch`). Dashboard panel shipped HM-AO-β-2 (`143a94a`). Scheduler wired (`57f5043`) + HM-AS-β.2 `_bg()` wrapper (`a18487e`). | Investigation 5: file retrospective, not new-build proposal |
| HM-AM (Total Portfolio Unification) | ✅ ALL 4 PHASES SHIPPED | Backlog: "ALL PHASES SHIPPED 2026-05-07." Phase 1 `4f0bcff` data layer, Phase 2 `d338605` Kirk envelope, Phase 3 `d6c9647` Advisory Team prompts, Phase 4 `52d7298` dalio-metals. | Investigation 6: file retrospective; no scope work needed |
| HM-AN scoping | ⏭ DEFERRED (already scoped) | `docs/SCOTTY_AUDIT_2.md` Section I (line 389) already filed the recommendation: file as "HM-AN — Signal Center → Dashboard read bridge" (P3, 4h). Not yet ticketed in `XO_BACKLOG.md`. | Investigation 7: ticket-it doc only, do NOT re-scope |
| 8-strategy SPRINT | ⏭ DEFERRED | Epic. Out of session scope. | None |
| Legacy scanner migration | ❓ UNKNOWN | No "legacy.scanner" or "scanner.migration" hits in backlog or code. | Skip |

## Live state snapshot

- **Service:** `com.trademinds.trader` PID 15010 (launchd-managed). Two `python.*main.py` processes (36698, 36701) appear to be parent + worker.
- **Branch:** `main`, 2 commits ahead of `origin/main`.
- **Working tree:** dirty (5 modified files, 5 untracked paths under `archive/stubs/`, `backups/`, `data/model_watch_log.jsonl`, `docs/model_watch/MODEL_WATCH_2026-05-10.md`, `reports/`). These are pre-existing — **not** part of this session. Will leave untouched.
- **Backups:** daily backups OK through 2026-05-10 (268 MB).
- **Execution gates:** `engine/risk_radar.py`, `engine/autopilot.py`, `engine/ai_brain.py`, `engine/ai_journal.py` all wired through `halt_mode` filter (HM-AK-β/γ shipped 2026-05-07).
- **Tests:** 2,635 test files inventoried (likely includes archived suites — high signal nonetheless).
- **Anti-patterns:** 3 files with bare `except:` (2 in `_archive/`, 1 in `scripts/snapshot_real_portfolio.py`). Acceptable surface; no urgent action.

## Re-plan for Phase 1 (Apply)

- **Task 1 (Weekend gate + baseline math):** SKIP. Preconditions don't match repo state. Already gated.
- **Task 2 (B24 log rotation):** PROCEED (preventive). Add `scripts/rotate_logs.sh` + plist proposal doc.
- **Task 3 (energy-arnold parser):** REDUCE SCOPE. Fix `main.py:225` Tier-2 comment block (stale `phi3:mini` → `qwen3:8b`). File question if any "parser fix" beyond this was intended.
- **Task 4 (CLAUDE.md):** PROCEED. Surgical sync against current state.

## Investigations

- **5 (Squeeze):** retrospective scope doc — what shipped, residual work.
- **6 (HM-AM):** retrospective doc — what's live, consumer-side touch points.
- **7 (HM-AN):** ticket-it doc — convert SCOTTY_AUDIT_2.md Section I recommendation into a BACKLOG-ready entry.

## Questions for Admiral (will append to `data/scotty_questions_2026-05-10.md`)

1. **"energy-arnold parser fix"** — was the intent the Tier-2 comment drift in `main.py:225`, or was there a separate parser/output bug I missed? `logs/trader.log` shows energy-arnold operating normally.
2. **"May 9 options-flow alert with 0.0x baseline"** — code does not contain this pattern. Was the alerter retired, or is this a memory of an old behavior I should leave alone?
3. **"NEW-1 second gate"** — no code or backlog reference. Was this folded into HM-AK halt_mode work?
4. **"Tier-2 landmine (M-1)"** — no "M-1" or "landmine" identifier found. Reference target unclear.
