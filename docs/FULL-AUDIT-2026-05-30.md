# FULL AUDIT — 2026-05-30 (USS TradeMinds)

Built from live files + DB + this weekend's investigations. Every item carries a **concrete blocker** and a
**falsifiable trigger** (the observable event that unblocks/confirms it). **Repeat-offender bug-classes** are
flagged ⟳. Split: **🟢 ship-today (off-market)** vs **⛔ genuinely market-locked** vs **🔴 go-gated**.

═══════════════════════════════════════════════════════════════════════════════════════════
## ⬛ ONE-SCREEN EXEC SUMMARY
═══════════════════════════════════════════════════════════════════════════════════════════
**Health:** trader single-writer (PID 61501), HTTP 200, regressions holding, market-gated-idle (normal weekend).
**This weekend SHIPPED:** §C deepseek+coder removal · orphan-prevention (proven) · Worf-reconcile complete ·
model-config comment · save_signal rules-scanner hook · 4-agent skip-set · dalio row-fix · trades_clean→alpaca boundary.
**The ONE open scan-path item:** §C **floor** (McCoy+Dax analyze-all-307, ~85min, starves TIER1) → **Lever A
bounded-rotation**, design done, BUILD Monday (needs live per-symbol cost). Everything else §C is CLOSED.
**Biggest latent risk:** Phase-2 external-fetch batch (7 unbounded leaves) — market-hours-confirm-gated.
**Repeat-offenders (act on the CLASS, not instances):** ⟳ unbounded-external-fetch (6+) · ⟳ role-conversion-stale-paths (6) ·
⟳ measurement-contamination (3, escalating) · ⟳ manual-SQL-cleanup-pollution (dalio + $237K) · ⟳ config/DB model drift (10 agents).
**Honest walls:** Monday market-open (floor build, live verification) · human-browser-smoke (Wave 7 frontend) · Admiral go (save_signal scope, real-money).

═══════════════════════════════════════════════════════════════════════════════════════════
## §1 — §C SCAN-WATCHDOG (the multi-day arc)
═══════════════════════════════════════════════════════════════════════════════════════════
| Item | State | Blocker | Falsifiable trigger |
|---|---|---|---|
| Indicators hang (Loop 3) | ✅ CLOSED | — | `indicators wall<5s` (holds 0.8-1.0s) |
| Catalyst/trending/quote_summary spikes (5B-5D) | ✅ CLOSED | — | 0 `ctx:catalyst:trending` HELD samples |
| deepseek + ollama-coder redundant arena paths | ✅ CLOSED | — | static: arena=={McCoy,Dax}; Monday: `deepseek/coder:infer`==0 |
| **§C FLOOR (McCoy+Dax analyze-all-307)** | 🟡 DESIGN DONE | **⛔ Monday: needs live per-symbol cost to size N** | a scan COMPLETES (`post_processing>0`) + TIER1 starvation 0-1 slots |

**Fix = Lever A bounded-rotation (Shape B)** — bound N/cycle, rotate offset, full coverage over ~6 scans, zero alpha
loss. Offset → `_ALPHA_PAIR_IDX` pattern + persist to `settings` (VBC done). ⟳ This is where **spikes-mask-floors**
bit: the loud hangs masked a quiet legitimately-long floor — distinguish hang (bound the call) from slow (reduce work).

═══════════════════════════════════════════════════════════════════════════════════════════
## §2 — DATA INTEGRITY ⟳ (manual-SQL-cleanup-pollution class)
═══════════════════════════════════════════════════════════════════════════════════════════
| Item | State | Blocker | Falsifiable trigger |
|---|---|---|---|
| **Gate 0** — no perf-assessment pre-2026-05-14 | 🚨 STANDING CONSTRAINT | clean window too short (~2wk, only ollie-auto N=38 + neo-matrix N=18 have broker-real) | clean window grows past ~30 trades/agent |
| contaminated-flag | ✅ DIAGNOSED → 🔴 deprecate | flag has NO detection logic; missed $237K | `trades_clean` now uses `alpaca_order_id` (✅ SHIPPED); aggregators still read flag → migrate |
| dalio row 2539 | ✅ ROW-FIX SHIPPED | — | row 2539 PnL=0 (✅) |
| **dalio residual −255.08** | 🔴 OPEN | **multi-site aggregator, verified session** | tracking-aware aggregator → dalio realized PnL == 0 |

**The durable fix for BOTH dalio + the $237K = stop trusting `known_contaminated`, key everything off
`alpaca_order_id IS NOT NULL` + route_mode='tracking' exclusion.** Row-by-row whack-a-mole is wrong (ONDS
siblings prove it). ⟳ Manual-SQL-cleanup is a recurring pollution source — guard at the aggregator, not the row.

═══════════════════════════════════════════════════════════════════════════════════════════
## §3 — AGENT ROSTER & DUAL-PATH ⟳ (role-conversion-stale-paths class, bitten 6×)
═══════════════════════════════════════════════════════════════════════════════════════════
| Item | State | Blocker | Falsifiable trigger |
|---|---|---|---|
| Arena LLM set | ✅ static-confirmed == {McCoy, Dax} | — | Monday: arena `infer:*` shows only plutus/qwen3 |
| 6 redundant arena paths (deepseek/coder + 4 Tier3) | ✅ SHIPPED (skip-set) | — | the 6 absent from arena registration |
| Worf/Seven/navigator "still scan" | ✅ REFUTED (tier-gated) | orphan-tally was contaminated | static: not in any `_SCAN_TIER` |
| **Rules-scanner `signals` blind spot** | ✅ HOOK SHIPPED | **⛔ Monday: weekend=all-PASS, no actionable to record** | a rules-scanner BUY/SELL writes a `sources='rules'` signal row |
| save_signal scope (every-eval vs acted-on) | acted-on chosen | Admiral can widen | — |

⟳ **Role-conversion-stale-paths**: every time an agent is converted (LLM→rules, scanner→bench), the OLD path is
left wired — deepseek/coder (arena leftover), Worf (3 lists), navigator (dead emitter). **Doctrine banked: enumerate
+ remove ALL old paths on conversion.** Open candidate sweep: when any agent is next re-homed, audit all N paths.

═══════════════════════════════════════════════════════════════════════════════════════════
## §4 — EXTERNAL-FETCH DISCIPLINE ⟳ (unbounded-external-fetch class, 6+ instances)
═══════════════════════════════════════════════════════════════════════════════════════════
| Leaf | State | Blocker | Falsifiable trigger |
|---|---|---|---|
| _fh_get, yahoo_quote_summary, trending, earnings_calendar | ✅ BOUNDED (this arc) | — | deadline/breaker logs fire |
| **7 unbounded leaves (Phase 2)** | 🔴 INVENTORIED | **⛔ market-hours confirm before batch-fix** | each gets deadline/cache/breaker; cold scan ≤Ns |

Phase-2 targets (HM-EXTERNAL-FETCH-DISCIPLINE-AUDIT): get_earnings_surprises (AV per-sym), get_stock_price
(5-source cascade), get_alpaca_bars per-sym fallback, get_all_prices (no pool timeout), fetch_news, sec_edgar,
openbb-SDK. ⟳ **The class fix = a fetch-discipline wrapper** (total-deadline + cache + circuit-breaker), applied
fleet-wide, so the NEXT new fetch is bounded by construction — not whack-a-mole per leaf. Ship before WAVE 7 weekend.

═══════════════════════════════════════════════════════════════════════════════════════════
## §5 — OPERATIONAL HAZARDS ⟳ (measurement-contamination + config-drift classes)
═══════════════════════════════════════════════════════════════════════════════════════════
| Item | State | Blocker | Falsifiable trigger |
|---|---|---|---|
| Orphan-prevention | ✅ SHIPPED + PROVEN | — | `trader_restart.sh` single-writer gate=1 (proof test passed) |
| ⟳ measurement-contamination (3 instances) | ✅ doctrine banked | — | every measurement states its boundary method; `lsof` single-writer pre-trust |
| ⟳ model-config-staleness (10 agents) | ✅ documented | DB has garbage placeholders → comment not sync | read DB not config.py for live model |
| 3 startup tracebacks (run_dashboard/realtime-monitor) | 🟢 OPEN-LOW | pre-existing, non-fatal | root-cause the 2 thread exceptions |
| battle_station daemon 0 weekend fires | 🟢 likely market-gated | ⛔ confirm Monday | `[HM-BS-DAEMON]` fires post-open |

⟳ **Measurement-contamination escalated 3×** this arc (date-less-logs → rich-console-wrapping → orphan-double-write)
and each time **reverted/misled a correct fix**. The defense (single-writer verify + stated-boundary) is now doctrine + tooling.

═══════════════════════════════════════════════════════════════════════════════════════════
## §6 — STANDING BACKLOG & FRONTEND
═══════════════════════════════════════════════════════════════════════════════════════════
| Item | State | Blocker / Falsifiable trigger |
|---|---|---|
| WAVE 7 frontend (inline-sweep B6-9, LCARS T2, palette, AN-Bridge, ollie-ai S7) | ⛔ BLOCKED | **human-browser-smoke** (HM-BJ.E2; un-runnable from CLI) → human verifies |
| signals_v2 stale-sweep (pending 3,076) | ⛔ Monday | run AFTER floor closes (don't drain a filling pond) → pending drops + stays low |
| 6 standing items (RISK-MGR-CONVICTION-STOP, SCHWAB-ALARM, TRADES-MIRROR-GAP, ALPACA-BRIDGE-LIMIT, QG-FLOAT, CONVICTION-TIER) | 🔴 mixed | Admiral-gated / maintenance-window; **re-verify current state first** (verify-before-fix) |
| LOW (LOG-VOLUME-ROTATION, ADJUSTED-OHLCV-VERIFY, MEMORY-Q2, BS-DAEMON-HEARTBEAT) | 🟢 anytime | small; BS-DAEMON-HEARTBEAT bundles next restart |
| deepseek → HM-BM bakeoff (~June 15) | DEFERRED | operational-cost convergence banked |
| NOTIF-WAR-ROOM-PRODUCER | 🟡 BLOCKED | Captain trigger definition |

═══════════════════════════════════════════════════════════════════════════════════════════
## SHIP-TODAY vs MARKET-LOCKED (the actionable split)
═══════════════════════════════════════════════════════════════════════════════════════════
**🟢 SHIP-TODAY (off-market, done or doable):** all of §3 rules-hook + skip-set (DONE) · §2 row-fix + trades_clean (DONE) ·
§5 orphan-prevention (DONE) · the LOW items · the 3 startup tracebacks (diagnose).
**🔴 GO-GATED (build-able, await Captain):** dalio tracking-aware aggregator · contaminated-flag aggregator migration ·
6 standing items (re-verify first).
**⛔ GENUINELY MARKET-LOCKED (Monday+):** §C floor build (live cost) · Phase-2 fetch batch (market-hours confirm) ·
signals_v2 sweep (post-floor) · all live behavioral verification (rules-signal firing, arena absence, battle_station, nightly scanners).
**⛔ HUMAN-GATED:** WAVE 7 frontend (browser smoke).
