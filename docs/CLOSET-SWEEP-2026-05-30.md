# CLOSET SWEEP — 2026-05-30 (honest remaining-work inventory)

Read-only reconciliation from 4 live sources (XO_BACKLOG, ALL-OUT-AUDIT items vs live DB/code, standing tickets re-verified, orphan-marker grep). After a heavy ship day (safety arc, daemon graveyard, $237K aggregator). Stakes: 🔴 live-decision/real-money · 🟠 fleet-data · 🟡 cosmetic/minor.

## ✅ STRIKE — closed/resolved/misread (do NOT carry as open)
- **HM-ALPACA-BRIDGE-LIMIT-FIX** — SHIPPED: `_build_order_request` honors order_type + tif (`_tif_map`); no longer downgrades limit→market. (was 🔴)
- **HM-QG-FLOAT-TRUNCATION** — SHIPPED: `quality_gate.py:137` `round(score,1)` + float pass; 0.5 partials preserved.
- **HM-NAVIGATOR-SIGNAL-PATH-DEAD** — save_signal hook shipped today (`5457271`).
- **navigator gemma4:26b drift** — MISREAD (bakeoff log noise; DB=qwen3:8b, no live navigator gemma4 call).
- **ollama-plutus ministral-3:3b "36%"** — BY DESIGN (HM-BN.1 canonical triage model, main.py:237/3343), not drift.
- **3 startup tracebacks** — one-off 2026-05-27 wrong-interpreter boot; 0 recurrence in 3 days.
- **schwab "no such table: schwab_holdings"** — RESOLVED (table exists, cadence-check exits 0).
- **HM-DALIO-GOOGL aggregator follow-up + HM-CONTAMINATED-FLAG-INCOMPLETE** — verify-closeable: aggregator shipped (`eb2886e`), dalio trades-PnL=0 confirmed live. Close after confirming (done in ship).

## 🟢 SHIP-TODAY (off-market, no live-scan dependency)
| item | what | stakes | blocker | trigger |
|---|---|---|---|---|
| **prompt_version + strategy_id NULL-at-INSERT** | learning-loop cols 100% NULL on newest rows (41/41) — INSERT drops the passed params | 🔴 | single-file paper_trader INSERT fix | a 2026-05-29+ row has non-NULL values |
| **HM-COVERED-CALL-RECORDING** | covered-call writes BUY/+qty instead of SELL/−qty → PnL direction inverted | 🔴 | fix `execute_covered_calls` action + backfill | regression asserts SELL/qty<0 |
| **holly-scanner roster ghost** | in RULES_SCANNERS, 0 `ai_players` rows → slips governance | 🔴 | add ai_players row OR remove from RULES_SCANNERS | row exists or entry removed |
| **exit_manager Task-7b chain lookup** | live exit mark-to-market returns 0.0 unrealized → exit rules blind to PnL; Polygon now active (unblocks) | 🔴 | wire Alpaca/Polygon chain lookup | live exit-eval returns real PnL |
| **HM-RISK-MGR conviction backfill** | conviction-stop flags now LIVE-ON; 58% (36/62) positions NULL conviction → fall back to default stop | 🔴 | backfill positions.conviction from originating signals (RED DB write) | NULL count → 0; tiered stops apply |
| **HM-BENCHMARK-DB-MISMATCH** (filed today) | benchmark writes `autonomous_trader.db`, reads `data/trader.db` → Sharpe-vs-SPY silently wrong | 🟠 | fix DB constant or document split | benchmark numbers reconcile |
| **_BSM_RISK_FREE hardcoded** | options_selector live path, 0.045 frozen; "move when Polygon active" — now met | 🟠 | config/live rate | rate sourced from config |
| **options_exec.py stale "zero callers" docstring** | module IS production-wired (wheel/autopilot/paper_trader); docstring lies → wrongful-retire risk | 🟠 | correct docstring | docstring matches callers |
| **tick_recorder unbounded** | records every tick (live); only 4h-retention DELETE mitigates | 🟠 | tier-aware sampling | sampling active |
| **bull_spread_v1 SPY-only width ladder** | [15,10,5] hardcoded for ~$700; mis-sizes non-SPY | 🟠 | scale by price (formula drafted in-comment) | widths scale by ticker price |
| **HM-ADVISORY-CREW-DRIFT-SWEEP** | ADVISORY_CREW members still in `_SCAN_TIER2` | 🟠 | remove from list (bundle w/ next restart) | intersection empty |
| **HM-SCHWAB-CROSS-MECHANISM-ALARM** | watcher+cadence share cron (shared-fate); no cross-check | 🟠 | independent dead-man check | kill watcher → alarm fires |
| **HM-MODEL-CONFIG-STALENESS** (residual) | config `model` values still unsynced (comment-only shipped) | 🟡 | sync config to DB or accept comment | config==DB or doc'd |
| **dead-artifact retirement** | `known_contaminated` col + `trades_clean` view linger, 0 readers (aggregator superseded them) | 🟡 | archive-rename per sacred-db | view/col removed-or-doc'd |
| **neo-matrix garbage model_id** | `'8000 / Independent'` placeholder | 🟡 | normalize sentinel | real model/sentinel value |
| **stray 0-byte DBs** | root `trader.db`, `backtest_results.db`, `deep_scan.db` empty | 🟡 | archive-rename | files gone |
| **20 navigator_bm bakeoff shadows** | full-halt shadow rows w/ exotic model_ids | 🟡 | archive or doc intentional | rows archived |
| HM-SIGNALS-RECENT-ACTED-ON-FIELD, HM-FUNDAMENTALS-COMPANY-NAME, HM-DEEPSEEK-CONCENTRATION-CAP-V2, HM-BS-DAEMON-HEARTBEAT, HM-ADJUSTED-OHLCV-VERIFY | assorted MED/LOW | 🟠/🟡 | small, off-market | — |

## ⛔ MONDAY-MARKET-OPEN (hard physical reason — needs a live scan/cost/broker call)
| item | stakes | physical reason |
|---|---|---|
| **HM-RUN-SCAN-WATCHDOG §C floor** (bounded-rotation build) | 🔴 | no weekend arena scans → can't confirm arena={McCoy,Dax} or size N without live per-symbol cost |
| **HM-EXTERNAL-FETCH-DISCIPLINE Phase 2** (7 leaves) | 🔴 | needs market-hours load to confirm the unbounded paths before bounding |
| **HM-SIGNALS-V2-STALE-SWEEP** (pending 3,076, worsening) | 🟠 | run AFTER floor fix ("don't drain a filling pond") |
| **HM-TRADES-MIRROR-GAP** | 🟠 | trades-side clean (0/118 missing oid); gap only quantifiable via live Alpaca `get_orders` pull |
| nightly rs_rank/minervini confirm, battle_station daemon Monday-fire | 🟡 | needs a market-day run |

## 🧍 HUMAN-GATED (needs Admiral/Captain decision or browser)
| item | stakes | the human action |
|---|---|---|
| **executor iron-condor non-atomic close** (HM-AC-extension) | 🔴 | design decision: atomic MLEG close + broker support |
| **admin-auth half-wired** (service token + recovery hash, app.py:21426) | 🔴 | Admiral runs `docs/AUTH_SETUP.md` §2/§3 |
| **HM-RISK-MGR conviction-stop** (options-stop flag) | 🔴 | Admiral flips the options-stop flag (equities+trail already on) |
| **HM-CONVICTION-TIER-BOUNDARY calibration** | 🟡 | Admiral approves new tier boundaries post-shadow |
| **HM-EXIT-TRAILING-STOP-TIER-DOCTRINE** | 🔴 | 4 Admiral decisions (Q1-Q4) |
| **HM-DOCTRINE-SHORT-INTEREST-READING** | 🔴 | Admiral timing on agent-prompt change |
| **HM-NOTIF-WAR-ROOM-PRODUCER** | 🟠 | Captain defines the trigger event |
| **HM-EQUITY-CURVE-ORPHAN** (filed today) | 🟡 | revive (wire panel) vs retire (delete endpoint) |
| **WAVE-7 frontend** (inline-sweep, LCARS, palette, AN-Bridge, ollie-ai) | 🟡 | browser smoke (HM-BJ.E2, un-runnable from CLI) |
| Accessapple rebrand sprint | 🟠 | Admiral + weekday browser window |

## 📅 CALENDAR-GATED (June)
- **review-2026-06-04** — Worf bench re-eval (only during a genuine BEAR cycle; today=BULL).
- **HM-DEEPSEEK-30D-RECHECK** — 2026-06-07.
- **deepseek → HM-BM bakeoff** — ~June 15.
- theme/CSS refactors (HM-THEME-*) — post-v4.4 soak ≥1wk.

## Long tail (parked, low-stakes — not itemized)
Early-May residue (B12/13/17-21/27, AI-1/2/4/5, H1/H3, X3/X4/X5, options_agents.py orphans, HM-AU/AS/AW/AK-β cleanups) + ~15 LOW frontend/scanner ergonomics (HM-OAI-*, HM-TRENDSPIDER cluster, HM-SC-ATR, HM-CHART-DATA-*). Mostly off-market 🟡; H1/H3 are 🔴 pre-spread-live (tiered_exits never called, /api/wheel 500).

## 🎯 Single highest-value off-market item
**prompt_version + strategy_id NULL-at-INSERT** — confirmed 100% NULL on the newest rows; the learning-loop columns the system was *designed* to capture are silently non-functional. Single-file additive INSERT fix (the params are already passed, just dropped), zero live-trade risk, fully verifiable off-market, and it's the same data-capture-at-the-write-path class as today's aggregator win. Close runner-up (higher stakes, higher risk): the conviction backfill (completes an already-LIVE feature for 58% of positions) and HM-COVERED-CALL-RECORDING (inverted PnL accounting).
