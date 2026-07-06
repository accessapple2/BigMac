# Agent Rules Review — pre-reopening audit (2026-07-03)

Scope: verification and extraction of every agent-facing rule in the OllieTrades codebase
(extracted at `/tmp/bot`), as the working document for deciding which of the 55+
`halt_mode='full'` benched agents to reopen with updated instructions.

Fleet state per CLAUDE.md dated waypoint (2026-07-01, HM-FULL-AUDIT): **15 active / 9 exit_only /
55 full / 79 total** — drift from prior 22/6/45/73 baseline RESOLVED as legitimate season churn
(CLAUDE.md:293-315). Note `docs/FLEET-ROSTER.md:148-151` still carries the stale 2026-06-01
count (21/6/45) — see Inconsistency #13.

---

## 1. Rule locations map

| File | Lines | What lives there |
|---|---|---|
| `trading_rules.txt` | 1-62 | Core long-only doctrine: 8-position cap, conviction sizing, cash floors, 2-of-4 entry signals, stop ladder, macro overlay, RSI profit-taking, buy-the-blood, hype-trap ban. Plus an appended Strategy Lab block (see #7.4). |
| `config.py` | 253-298 | Trading constants (MAX_POSITIONS, STOP_LOSS_PCT, take-profit tiers, options limits, scan intervals, API budgets). |
| `config.py` | 6-101, 148-166 | Feature flags (exec router, spread guard, auto-spreads, grade-B relax, dedup) with reversal instructions. |
| `config.py` | 300-337 | `AI_PLAYERS` static roster — **INFORMATIONAL ONLY**; runtime model = `ai_players.model_id` in DB (staleness warning at config.py:302-312). |
| `main.py` | 224-296 | `_SCAN_TIER1/2/3` rosters, tier intervals, tier-3 open/close windows, benching comments with dates. |
| `main.py` | 105-137 | `initialize_arena()` — providers built from `ai_players` DB; `halt_mode='full'` agents skipped. |
| `engine/providers/base.py` | 62-101 | `CREW_ROSTER` block + RECOVERY OVERRIDE (F&G < 35 relaxes VIX rules) — injected into every persona. |
| `engine/providers/base.py` | 103-440 | `MODEL_PERSONALITIES` — per-agent embedded rule text (Spock, Geordi, Worf, Troi, Sulu, etc.). |
| `engine/providers/base.py` | 521-1390 | `build_prompt()` — full per-scan prompt assembly incl. the 15 embedded RULES, conviction scoring table, survival mindset. |
| `engine/ai_brain.py` | 288, 962, 1018 | Active-roster query (`halt_mode='active'`), drawdown check call, `is_auto_tradeable` guard. |
| `engine/risk_manager.py` | 108-296 | Universal guardrails: per-model daily trade limits, cash floors, conviction floors, bear overrides, position caps, min-hold days, per-model MODEL_GUARDRAILS. |
| `engine/risk_manager.py` | 979-1015, 1121-1131 | Fleet trailing stop (3% flat / conviction-scaled 3-5%) + dynamic trailing ladder. |
| `engine/stops.py` | 16-103 | Canonical conviction-scaled tier tables: entry stop 12/15/18%, trail 3/4/5%, options stop 30/40/50%. |
| `engine/quality_gate.py` | 1-138 | Quality Gate V3 — 3-of-5 checks before any BUY; GATE_EXEMPT set; ETF fast-path. |
| `engine/halt_gate.py` | 1-116 | halt_mode helpers (`can_emit_signal` / `can_open_position` / `can_close_position`), `is_auto_tradeable`, `HALTED_EMIT_FILTER`. |
| `engine/paper_trader.py` | 740-756, 1735-1746 | Buy-side and sell-side HALT GATEs; plus grade-B fleet gate (900-965), max-positions (1147-1155), quality gate call (1165-1172), min-hold (1677-1711). |
| `engine/backtester.py` | 144-175, 254 | Backtest-side MIN_HOLD_DAYS + (divergent) position limits. |
| `engine/events_bus_consumer.py` | 35-37, 190-215 | 60-min re-entry cooldown + open-position rail for signals_v2 dispatch. |
| `engine/crew_specialization.py` | 46-141, 143-633 | ALPHA_SQUAD, ADVISORY_CREW, ENABLED_STRATEGIES, AGENT_STRATEGIES, EXECUTION_RULES_6_3, full CREW_MANIFEST (per-agent mandates/conditions/universes). |
| `engine/crew_specialization.py` | 651-809 | `should_agent_trade()` — the mandate-condition gate engine. |
| `CLAUDE.md` | 7-20, 48-74, 289-322 | RULE #1 (Schwab hands-off), manual-halt SQL pattern + halt_mode doctrine, fleet roster waypoint. |
| `docs/FLEET-ROSTER.md` | all | Roster detail: active 4 voters, bench, sniper squad, backtest pool (deliberate-OFF), zombie candidates, elder council, metals command. |
| `engine/guardian_sweep.py` | 1-40 | Dedicated exit-only stop sweep for `guardian-of-forever` (exit_only agents are never scanned/stop-checked by the main loop). |

---

## 2. Core doctrine (trading_rules.txt digest)

Persona: "ruthless, high-conviction, **long-only** equity trader. No shorting. No options. No
leverage." (lines 1-2 — contradicted by the live fleet; see Inconsistency #1.)

1. **Max 8 positions** at any time; ideal 4-7. "Concentration = alpha."
2. **Conviction-tiered sizing:** 9-10/10 → 20-35% max; 7-8/10 → 10-20%; 5-6/10 → 5-10%;
   absolute cap 35% of account.
3. **Cash is a position:** ≥15% floor normal market (deploy up to 85%); VIX >25 or macro
   uncertainty → ≥30-50% cash.
4. **Entry requires ≥2 of 4 signal families:** Technical (RSI <35 / MA reclaim / high-vol
   reversal), Fundamental (beat+raise / backlog / margin expansion), Sentiment (capitulation /
   F&G <20), Macro tailwind (sector rotation).
5. **Stop-loss & exit ladder (NON-NEGOTIABLE):**
   a. Initial hard stop −12% to −18% from entry;
   b. Trailing stop −8% to −12% below post-entry high;
   c. Thesis-break stop (guidance cut, lost customer, debt crisis) → sell immediately;
   d. Technical break (lose 50-day MA on volume OR RSI >75 stalling) → exit;
   e. Better-opportunity rotation (new 9-10/10 replaces 5-6/10 names);
   f. Portfolio stop: drawdown >15-20% → go 50%+ cash.
6. **Macro overlay:** VIX >30 → 40-60% cash; oil >$100 sustained → overweight energy; Fed
   cutting → growth/AI; recession signals → defensive rotation.
7. **Never average down** on losers absent new material bullish info.
8. Never FOMO into strength without pullback or new catalyst.
9. Always show: allocation %, conviction 1-10, entry, stop, target, risk amount.
10. **RSI profit-taking (NON-NEGOTIABLE):** RSI >70 → trim 50%; RSI >80 → trim another 25%.
11. **Buy the blood:** RSI <20 on quality stock (mcap > $50B) = automatic BUY at 0.75
    conviction.
12. **Hype-trap ban:** no zero-revenue / negative-EPS names without a specific catalyst ≤7 days
    out; beaten-down quality = opportunity, no-business stocks = traps.

Coda: fewer, higher-conviction trades; "one great trade beats ten average ones"; size up when
momentum + catalyst align.

**Appended machine-written block (lines 57-62):** "STRATEGY LAB OPTIMIZED — RSI MEAN REVERSION"
(rsi_buy=20, rsi_sell=60, stop −5%, position 10%), deployed 2026-03-17, with impossible stats
(WR 100.0%, PF 50.0, **Return 0%**). The auto-deploy write path was since removed
(`engine/strategy_lab.py:1075`) after it corrupted STOP_LOSS_PCT on 2026-05-31
(`strategy_lab.py:1026`). This block is stale residue — see Inconsistency #4.

---

## 3. Guardrail constants (every value with file:line)

### config.py (Trading Rules block, lines 253-298)
| Constant | Value | Line |
|---|---|---|
| `PAPER_TRADING` | True | config.py:6 |
| `STARTING_CASH` | $7,000 | config.py:254 |
| `POSITION_SIZE_PCT` | 0.10 | config.py:255 |
| `MAX_POSITIONS` | 8 | config.py:256 |
| `STOP_LOSS_PCT` | 0.05 | config.py:257 |
| `TAKE_PROFIT_TIERS` | +5%→sell 50%, +10%→25%, +15%→50%, +25%→50%, +50%→100% | config.py:258-264 |
| `MAX_POSITION_PCT` | 0.30 (high-conviction 0.85+) | config.py:265 |
| `MAX_DRAWDOWN_PCT` | 0.20 | config.py:266 |
| `MIN_CASH_RESERVE_PCT` | 0.15 | config.py:267 |
| `MAX_DAILY_TRADES` | 30 | config.py:268 |
| `OPTIONS_MAX_PCT` | 0.05 per position | config.py:269 |
| `OPTIONS_TOTAL_MAX_PCT` | 0.10 (reduced from 0.20) | config.py:270 |
| `OPTIONS_DEFAULT_DTE` / `MIN_DTE` / `AUTO_CLOSE_DTE` | 30 / 7 / 1 | config.py:271-273 |
| `OPTIONS_STOP_LOSS_PCT` | 0.50 | config.py:274 |
| `AUTO_SPREADS_ENABLED` | False (master gate, config-edit + restart only) | config.py:154 |
| `AUTO_SPREAD_MAX_DEBIT_PER_TRADE` / `MAX_OPEN` / `MAX_NEW_PER_DAY` / `MAX_TOTAL_DEBIT` / `MIN_CONVICTION` | $500 / 5 / 3 / $2,500 / 8.0 | config.py:162-166 |
| `EXEC_ROUTER_ENABLED` | False (dry-run only; RULE #1: never Schwab) | config.py:27 |
| `TRADE_DESK_BYPASS_GATES` | **True** (trade-desk bypasses check_trade gates) | config.py:34 |
| `ZERO_DTE_EXECUTION_ENABLED` | False (darked 2026-06-19: 0/2 lifetime, −$1,670) | config.py:20 |
| `SOURCE_DEDUP_ENABLED` / `WINDOW_MIN` | True / 60 min | config.py:51-52 |
| `GRADE_B_REVERSAL_RELAX_ENABLED` / `MIN_MA8_MARGIN_PCT` | False / 0.3% | config.py:93-94 |
| `DAILY_API_BUDGET` / `MONTHLY_API_BUDGET` | $5.00 / $35.00 | config.py:295-297 |

### engine/risk_manager.py (RiskManager class)
| Constant | Value | Line |
|---|---|---|
| `MAX_TRADES_PER_DAY` | default 3; geordi 2, worf 2, **spock 8**, dax 3, mccoy 3, data 3, uhura 2, trip 2, troi 3, chekov 8, neo 5, ollie-auto 15 | risk_manager.py:113-127 |
| `UNIVERSAL_MIN_CASH_PCT` | 0.20 | risk_manager.py:131 |
| `UNIVERSAL_MIN_CONVICTION` | 0.65 | risk_manager.py:132 |
| `BEAR_MAX_TRADES_PER_DAY` | 1 | risk_manager.py:135 |
| `BEAR_MIN_CASH_PCT` | 0.35 | risk_manager.py:136 |
| `BEAR_MIN_CONVICTION` | 0.80 | risk_manager.py:137 |
| `BEAR_MAX_POSITIONS` / `NORMAL_MAX_POSITIONS` | 8 / 8 ("user override — sequential Ollama") | risk_manager.py:138-139 |
| `CORRELATION_THRESHOLD` / `CORRELATED_GROUP_MAX_PCT` | 0.70 / 0.40 | risk_manager.py:141-142 |
| `PER_SYMBOL_MAX_PCT` / `BEAR_PER_SYMBOL_MAX_PCT` | 0.18 / 0.15 | risk_manager.py:143-144 |
| `WARNING_ONLY_PLAYERS` | neo-matrix, enterprise-computer, alpaca-mirror, super-agent | risk_manager.py:148 |
| `FLEET_TRAILING_STOP_OPT_OUT` | ollama-local (−8% hard), dayblade-sulu (−3% intraday), energy-arnold (−7% sector) | risk_manager.py:153 |
| `AI_SIGNAL_PLAYERS` (conviction-scaled-stop eligible) | 19 ids | risk_manager.py:172-178 |
| `MAX_POSITIONS_PER_MODEL` | default 8 (all 8 except troi 5, chekov 5) | risk_manager.py:190-200 |
| `MIN_HOLD_DAYS` | default 5; spock 7, worf 5, geordi 5, scotty 10, mccoy 7 | risk_manager.py:203-210 |
| `get_max_position_size` (conviction sizing) | normal: ≥0.90→33%, ≥0.80→25%, ≥0.70→20%, else 15%; bear: 25/15/10% | risk_manager.py:210-228 |
| Legacy `get_stop_loss_pct` (stale dup) | ≥0.90→18%, ≥0.80→15%, ≥0.70→12%, **else 8%** | risk_manager.py:231-241 |
| MODEL_GUARDRAILS geordi | 2 trades/day, VIX>30 = 0 trades, 8% stop, 15% max position, 30% min cash, 3 losers → cash | risk_manager.py:245-255 |
| MODEL_GUARDRAILS spock | 3/day, thesis ≥50 chars citing data, conviction 0.75 (0.85 @ VIX>30), 50% cash in bear, 24h revenge cooldown, **$150 max-loss dollar cap** | risk_manager.py:256-273 |
| MODEL_GUARDRAILS worf | 2/day, VIX>30 blocks all buys, SPY<200MA blocks buys, conviction 0.70 | risk_manager.py:274-279 |
| Drawdown auto-halt | `check_drawdown()` ≥ `max_drawdown_pct` 0.20; transient (recomputed per cycle, no flag) | risk_manager.py:1146-1170; called from ai_brain.py:962; doctrine CLAUDE.md:66-71 |
| Fleet trailing stop | flat 3% below high-water mark, never below avg_price; opt-out set applies | risk_manager.py:979-1015 |
| Dynamic trailing ladder `_get_trailing_stop_pct` | gain ≥20% → 10% trail; ≥10% → 12%; ≥5% → 15%; small gain → tight 5% | risk_manager.py:1121-1131 |

### engine/stops.py (canonical conviction tier tables, Admiral-locked 2026-05-25)
| Function | Tiers | Line |
|---|---|---|
| `get_stop_loss_pct` | ≥0.90→0.18, ≥0.80→0.15, <0.80→**0.12 floor invariant** (0.08 regression removed — the risk_manager copy still has it) | stops.py:77-103 |
| `get_trail_pct` | ≥0.90→0.05, ≥0.80→0.04, <0.80→0.03 | stops.py:51-74 |
| `get_options_stop_pct` | ≥0.90→0.50, ≥0.80→0.40, <0.80→0.30 (intentional Rule-5 deviation: theta/IV-crush) | stops.py:16-48 |
| Activation flags (env, **default OFF**) | `CONVICTION_SCALED_STOPS_ENABLED`, `CONVICTION_SCALED_TRAIL_ENABLED`, `CONVICTION_SCALED_OPTIONS_STOP_ENABLED`; shadow logger `CONVICTION_SCALED_STOPS_SHADOW` default **ON** → `ghost_conviction_stops` table | risk_manager.py:9-48 |

### engine/quality_gate.py
- BUY requires **3 of 5** checks (int(score) ≥ 3): earnings growth >0, revenue growth >0,
  analyst buy (hold = 0.5 partial), RSI <70, smart-money (3+ models bought). Missing data =
  0.5 partial credit each (quality_gate.py:63-138).
- `GATE_EXEMPT`: SPY/QQQ/XLE/XOP/IWM/DIA/GLD/SLV/USO + Dalio bonds/commodities/gold
  (quality_gate.py:13-19).
- ETF-shape fast-path: all three fundamentals None → pass on technicals unless RSI ≥70
  (quality_gate.py:50-61).
- Enforced at buy: paper_trader.py:1165-1172.

### engine/paper_trader.py gates (buy path, in order)
- Human/mirror guard → paper_trader.py:736-739; HALT GATE (blocks `exit_only` AND `full`) →
  740-751; stale-signal gate → ~756+; regime-router gate → 856+; **grade-B fleet gate**
  (0.60 ≤ conf < 0.75 stock BUYs blocked in BEAR_CROSS / CAUTIOUS_BEAR / SPY < −0.1% intraday)
  → 900-965; per-model max positions → 1147-1155; quality gate → 1165-1172.
- Sell path: HALT GATE blocks only `full` (exit_only may sell) → 1735-1746; min-hold →
  1771, 2021.
- `_check_min_hold` (1677-1711): 24h universal minimum, 72h for SWING timeframes;
  stop/target/expired reasons bypass.

### Re-entry / dedup rails
- `events_bus_consumer.REENTRY_COOLDOWN_MIN = 60` (events_bus_consumer.py:37); rail 1 = open
  position in (source,symbol), rail 2 = BUY within 60 min; skipped signals are soft-voided
  (events_bus_consumer.py:190-215; guard applied at 112-124). Fail-OPEN on DB error.
- `SOURCE_DEDUP_WINDOW_MIN = 60` fallback, auto-tuned per timeframe (config.py:36-52) —
  deliberately symmetric with the re-entry cooldown.
- Backtest `MIN_HOLD_DAYS` (backtester.py:152-159): default 5, spock 7, worf 5, geordi 5,
  scotty 10, mccoy 7; used at backtester.py:254. **Backtester position limits diverge:**
  `BEAR_MAX_POSITIONS = 3`, `NORMAL_MAX_POSITIONS = 5`, per-model 3-5 (backtester.py:162-175)
  vs. risk_manager's all-8.

### crew_specialization.py EXECUTION_RULES_6_3 (line 131-141)
Trade window 10:30-15:00 ET; min option OI 500; max bid/ask spread 10%; VIX >35 pause all
spreads; max 3 trades/day; target DTE 45, min DTE 21; exit model "model_f"
(50%@50% profit / 30%@75% / 20%@90% or 21 DTE; stop = 2× credit received —
crew_specialization.py:168-171).

---

## 4. Scan tiers & cadences

Defined in main.py:224-296. Tier membership is a *scheduling* roster — the halt gate and
`build_all_providers` (skips `halt_mode='full'`, main.py:113-127) are the real enforcement, so
stale roster entries are harmless-but-misleading (see Inconsistency #8).

### Tier 1 — Bridge Crew, every 30 min during market hours (`_SCAN_TIER1`, main.py:228-235; `_TIER1_INTERVAL` main.py:266)
| player_id | Comment identity | Status vs DB reality |
|---|---|---|
| `dayblade-sulu` | Sulu — "S6.3 primary options trader, PRIORITY 1" | Benched S6.3 (XO coaching: R:R 0.10, META losses — crew_specialization.py:79); FLEET-ROSTER: TOGGLE-OFF since 2026-03-31, `exit_only`, is_paused=1 |
| `super-agent` | Anderson (crewai) | SHELVED — "unrestricted aggression conflicts with alpha gate" (crew_specialization.py:72, 485) |
| `deepseek-7b-grok4` | Spock | → `full` in 2026-06-19/20 Door-1 kill-gate cut (CLAUDE.md:300-303) |
| `ollama-coder` | Data | → `full` in same kill-gate cut (CLAUDE.md:302) |
| `mlx-qwen3` | Chekov (roster comment; CREW_MANIFEST says Ensign Ro) | ADVISORY_CREW; FLEET-ROSTER:148-150 lists as `full` season-1 carryover |

### Tier 2 — Department Heads, every 2 hours (`_SCAN_TIER2`, main.py:237-251; `_TIER2_INTERVAL` main.py:267)
Current members: `ollama-plutus` (McCoy) and `ollama-qwen3` (Scotty label; Dax elsewhere).
Note `ollama-qwen3` went `exit_only` in the 06-19/20 kill-gate cut (CLAUDE.md:303).

Benching comments preserved in the roster (main.py:240-250):
- **Worf (`qwen3-8b-flash`) REMOVED 2026-05-29** (HM-WORF-DRIFT-RECONCILE): benched S6.1 →
  ADVISORY_CREW bridge-vote only; non-emitting since 2026-05-07.
- **HM-ADVISORY-CREW-DRIFT-SWEEP 2026-05-29** removed 4 more ADVISORY_CREW agents that "lied to
  the scanner roster": `options-sosnoff` (Troi), `energy-arnold` (Trip), `ollama-local`
  (Geordi), `ollama-llama` (Uhura). All benched S6.x, last signals 2026-05-02..05-07; kept
  `ai_players` active *at that time* for War Room bridge-voting.

### Tier 3 — Cadets, market open + close only (`_SCAN_TIER3`, main.py:253-264; window main.py:289-296)
Members: `qwen3-8b-4o`, `qwen3-8b-sonnet`, `qwen3-14b-grok3`, `ollama-gemma27b`, `ollama-glm4`,
`ollama-kimi`, `qwen3-8b-o3`, `ollama-deepseek`, `qwen-coder-haiku`, `cto-grok42`.
Windows: 6:30-7:00 AM MST (open) and 12:45-1:30 PM MST (pre-close); `_TIER3_INTERVAL` = 4h
minimum gap (main.py:268). Most Tier-3 ids are `full` in the DB (zombie-candidate list,
FLEET-ROSTER.md:110-117) — they are skipped at provider build.

### Benching / cull events with dates (CLAUDE.md:293-315)
- **2026-06-07** scorecard cull → `full`: ollama-local, dayblade-0dte, ollama-deepseek.
- **2026-06-19/20** Door-1 kill-gate (G1-G4) → `full`: qwen3-8b-sonnet, qwen3-14b-pro,
  deepseek-7b-grok4, ollama-kimi, dalio-metals, ollama-coder; → `exit_only`: navigator,
  ollie-auto, ollama-qwen3.
- **2026-06-24** Picard/Riker retirement — inserted as new `full` benched rows (on-deck slots
  1-2; CLAUDE.md:424-437).
- **New active seats:** sell-the-news (06-06), archer (06-06), q-witness (06-07); new
  exit_only: guardian-of-forever (06-12, inserted exit_only by design).
- **HM-ORPHAN-SEATS** still open: 11 `ai_players` seats reference Ollama models absent from
  olliemax; all 11 within the 55 `full` count (CLAUDE.md:312-315). Must be resolved before
  reopening any of those 11.
- **Backtest Pool — deliberate OFF, do NOT retire:** grok-4, claude-haiku, claude-sonnet,
  gpt-4o, gpt-o3 kept `full` for cost doctrine, wired with fallback_model
  (FLEET-ROSTER.md:78-90).

### Base scan cadence (Dilithium Crystal Protocol)
`_get_scan_interval()` (main.py:298-346): pre-market 5 min; market hours 3-5 min (config says
300s, docstring says 180s — see Inconsistency #19); lunch lull 10 min; power hour 90s per
docstring but `SCAN_INTERVAL_POWER_HOUR=300` (config.py:288); after-hours 10-15 min; overnight
30 min; weekend 1h.

---

## 5. Per-agent mandates (crew_specialization.py CREW_MANIFEST + embedded prompt personas)

Squad structures (crew_specialization.py:46-80): `ALPHA_SQUAD` = ollama-coder / ollama-qwen3 +
ollama-plutus (scan pairs); `ADVISORY_CREW` (bridge-vote only, 20 ids) includes all shelved +
benched-S6.1 agents. `AGENT_STRATEGIES` whitelist (115-128): sulu = iron_condor/bear_call/
bull_put; mccoy = csp/bull_put_spread/long_put (covered_call DISABLED 2026-04-17 OOS verdict,
line 103-104); spock = rsi_bounce/mean_reversion; data = long_equity/momentum/short_equity/
inverse_etf; dax = swing_trade/ema_pullback/momentum; chekov = ema_pullback/momentum/
bull_momentum_breakout; capitol = congress_copy; ollie-auto = gate_commander.

Every CREW_MANIFEST entry (mandate · conditions · sizing · universe):

| player_id | Name / Role | Mandate & conditions | max_pos / size_factor | Universe |
|---|---|---|---|---|
| `deepseek-7b-grok4` | Spock — Mean Reversion (tier 1) | RSI extremes only; blocked in TRENDING_BULL/BEAR sessions; requires RSI <30 or >70 | 2 / 0.8 | any |
| `dayblade-sulu` | Sulu — Iron Condor King [S6.3] (tier 1) | IC primary (+572% 180d backtest claim); Model F exits 50/30/20; stop 2× credit; window 10:30-15:00 ET; OI ≥500; VIX 14-35 band; DTE 21-45 | 4 / 1.2 | any |
| `energy-arnold` | Trip Tucker — Contrarian [ADVISORY] | Fade crowd: P/C >1.5 buy, <0.6 short, dead zone 0.9-1.1; F&G <25 buy / >75 short | 2 / 0.6 | any |
| `qwen3-8b-flash` | Worf — Bear Specialist (tier 1) | Bearish only; blocked TRENDING_BULL; VIX ≥16; breadth <55% | 2 / 0.7 | SH, SQQQ, UVXY, SPXS, PSQ, DOG, RWM, SDOW |
| `options-sosnoff` | Troi — Sentiment Reader [ADVISORY] | Sentiment divergences; F&G <20 buy / >80 short; 1.0× size on divergence else 0.5× | 2 / 1.0 | any |
| `ollama-coder` | Data — Pure Quant (active) | Deep Scan signal_strength ≥0.6 only; no sentiment | 4 / 0.9 | any |
| `mlx-qwen3` | Ensign Ro — Breakout Hunter [ADVISORY] | 20-day highs on 2×+ volume, TRENDING_BULL only, max 2-day hold | 3 / 0.9 | any |
| `ollama-local` | Geordi — Sector Rotation [ADVISORY] | Buy leading sector ETF, short lagging, weekly via sector_heatmap | 2 / 1.0 | 11 SPDR sector ETFs |
| `ollama-plutus` | McCoy — Crisis Doctor (tier 2) | Buys the blood: VIX ≥15, F&G ≤55, breadth ≤50%; hold until VIX <12; fully inactive below VIX 12 | 2 / 1.2 | TQQQ, NVDA, AMD, META, SPY, QQQ, IWM |
| `ollama-qwen3` | Dax — Swing Breakout (active) | Breakout >20MA on 1.5×+ volume; sits out TRENDING_BEAR | 3 / 0.9 | any |
| `ollama-llama` | Uhura — Options Flow (tier 1) | Trades when 2+ options-flow signals align | 2 / 1.0 | any |
| `qwen-coder-haiku` | Reed — Tactical/Defense [ADVISORY] | Defensive bridge voter, no individual trades | 0 / 0.0 | — |
| `qwen3-8b-sonnet` | Sisko — Decisive Strategist [ADVISORY] | Bridge voter only | 0 / 0.0 | — |
| `qwen3-14b-pro` | Seven of Nine — Pure Data (tier 1) | Highest signal_strength from Deep Scan; no conditions | 2 / 0.8 | any |
| `qwen3-8b-4o` | Janeway — All-Conditions [ADVISORY] | `unrestricted: True` — only Event Shield CRITICAL / Troi STAND_DOWN stop her | 3 / 1.0 | any |
| `qwen3-8b-o3` | Tuvok — Risk Assessment [ADVISORY] | Bridge voter only | 0 / 0.0 | — |
| `qwen3-14b-grok3` | Hoshi — Signal Interceptor [ADVISORY] | Bridge voter only | 0 / 0.0 | — |
| `ollama-glm4` | Q — Wildcard [ADVISORY] | `unrestricted: True`; "ignores most gates" | 3 / 1.2 | any |
| `ollama-kimi` | Bashir — Diagnostics [ADVISORY] | Bridge voter only | 0 / 0.0 | — |
| `ollama-deepseek` | Odo — Contrarian [ADVISORY] | Requires signal divergence | 2 / 0.7 | any |
| `ollama-gemma27b` | (also "Jadzia Dax") — Patient Swing [ADVISORY] | swing_only 2-5 day, MA-pullback entry required | 3 / 0.9 | any |
| `super-agent` | Mr. Anderson [ADVISORY] | SHELVED (alpha-gate conflict); `unrestricted`, min_confidence 0.45, small-cap momentum focus | 5 / 2.0 | any |
| `neo-matrix` | Neo (special) | "Independent — no mandate restrictions"; unrestricted; WARNING_ONLY in risk radar | 3 / 1.0 | any |
| `dalio-metals` | Mr. Dalio [ADVISORY] | SHELVED (metals not in strategy whitelist) | 4 / 1.0 | any |
| `capitol-trades` | Capitol Trades — Congress Scout [S6.2] (tier 1) | Copies congressional disclosures; bypasses alpha gate (regime-agnostic) | 2 / 0.8 | any |
| `navigator` | Chekov — EMA Scout [S6.2] (tier 1) | EMA pullback + bull momentum breakout w/ volume/RSI/ADX confirm; bypasses alpha gate | 2 / 0.8 | any |
| `holly-scanner` | Holly — Pattern Scout [S6.2] (tier 1) | 6-pattern detector (volume spike, gap up, RSI bounce, SMA20 breakout, SMA50 pullback, sector momentum); rules-based, no LLM | 2 / 0.8 | any |
| `enterprise-computer` | Dilithium Reserve (special) | Capital reserve/rebalancing | 2 / 0.5 | — |
| `webull` | Captain Kirk (special) | Captain's discretion — **human**; blocked by is_auto_tradeable | 10 / 1.0 | — |
| `dayblade-0dte` | T'Pol [ADVISORY] | SHELVED: "0DTE execution delay = guaranteed loss"; still bridge-votes | 3 / 1.0 | any |
| `cto-grok42` | CTO Grok 4.2 (special) | Tech-sector advisor, unrestricted. NOTE crew_specialization.py:613: DB model_id still `devstral-small-2` (uninstalled) — WR/debate path dead until DB fix | 3 / 1.0 | any |
| `red-alert` | Red Alert System (special) | Automated risk monitor, never trades | 0 / 0.0 | — |

`should_agent_trade()` (crew_specialization.py:651-809) enforces the conditions mechanically:
advisory tier → always blocked from trading; special tier → always allowed; tier-1/2 checked
against session type, VIX, P/C, F&G, breadth, momentum, deep-scan strength.

### Embedded prompt rules (engine/providers/base.py)
Every LLM scan prompt = persona (MODEL_PERSONALITIES) + CREW_ROSTER + trade memory +
leaderboard standing + ~30 data blocks + the fixed RULES section (base.py:1286-1390):
- "You are a profit-driven equity trader… ONLY US stocks and US stock options" (1288-1290).
- **RULES 1-15** (1297-1311): max **5** positions; sizing 0.85+→20-30%, 0.7-0.84→12-20%,
  0.55-0.69→8-12%, conviction-multiplier 0.90+ w/ flow+catalyst → up to **40%**; options 5%
  per trade / **20% total** cap; cash floor 15% (don't hoard >40%); momentum-is-money (+3% on
  2× vol → BUY ≥0.60); contrarian −5% no-news bounce ≥0.55; news trumps technicals ≥0.50;
  sector rotation; **−12% hard stop**; hold-penalty (3 consecutive HOLDs → act); no adding to
  held stock; RSI >70 trim 50% / >80 trim 25% ("autopilot enforces this automatically");
  buy-the-blood RSI <20 + mcap >$50B → auto BUY ≥0.75; min hold 1 full trading day; hype-trap
  ban (verbatim from trading_rules.txt §12).
- Thesis requirement (no thesis = HOLD), extended-hours half-size rule, conviction scoring
  table 0.5-1.0, output format Decision/Timeframe/Confidence/Reasoning (1313-1390).
- Parser floors: BUY <0.50 discarded, BUY_CALL/BUY_PUT <0.80 discarded (base.py:1545-1551
  region, `parse_decision`).
- `CREW_ROSTER` RECOVERY OVERRIDE (base.py:92-101): F&G <35 → VIX hold threshold 30→35,
  min conviction −0.10, quality mega-caps only, stops still mandatory.
- Notable per-persona hard rules: Spock 3/day + 0.75 conviction (0.85 @ VIX>25) + 30% cash +
  24h revenge cooldown (116-135); Geordi 2/day, −8% stop, 15% cap, 30% cash, VIX>25 hold mode,
  RSI 35-65 no-trade band, 3 losers → cash (150-170); Worf CAN SLIM all-7, VIX>25 or SPY<200MA
  → defensive posture, inverse-ETF doctrine (184-213); Troi wheel 3/5/30 rule, options only,
  max 20% portfolio options (215-238); Trip energy-domain whitelist, crude regime bands
  $75/$90, stop −7% (240-271); **Sulu intraday DayBlade, −3% hard stop, close all by 3:45 ET,
  no overnight** (273-301 — conflicts with S6.3 IC mandate, see #7.6); Chekov thesis-level,
  0.70 min conviction, 3/day (303-322); Uhura swing-only with mandatory [STOP:]/[TARGET:] tags
  else rejected (373-386); Capitol 2+ Congress members in 30 days, equal weight 10%, hold
  30-60 days, max 10 positions, 20% cash (398-413). Nine agents' personas are literally the
  string "Paused." (387-397).
- **Expired intel block:** `_INTEL_EXPIRES = "2026-06-18"` FOMC macro frame now self-destructs
  (fails closed) — doctrine after the 12-week stale-Powell poisoning incident (base.py:868-897).

---

## 6. halt_mode state machine

**Values:** `active` | `exit_only` | `full` — CHECK-constraint enforced (CLAUDE.md:61-64).
Semantics (halt_gate.py:4-8): active = normal; exit_only = no signals, no new entries, exits
permitted (default for halts — "halts must never trap capital"); full = nothing (reserved for
runaway agents).

**Storage:** `data/trader.db` → `ai_players.halt_mode`, plus `halted_at` and `halt_reason`
columns. `is_halted` column DROPPED 2026-05-04 (HM-B, commit 9256890; CLAUDE.md:61-63).
Manual halt SQL pattern (CLAUDE.md:53-58): `UPDATE ai_players SET halt_mode='exit_only',
halted_at=CURRENT_TIMESTAMP, halt_reason='[YYYY-MM-DD] [reason]' WHERE id='X';` —
`tests/test_halted_at_enforcement_trigger.py` suggests a DB trigger enforces halted_at.
No programmatic halt path exists; flips are SQL + restart.

**Every code checkpoint:**

| Checkpoint | Behavior | Location |
|---|---|---|
| `halt_gate.halt_mode()` / `can_emit_signal` / `can_open_position` / `can_close_position` | active-only for emit/open; active+exit_only for close; unknown player defaults 'active' | halt_gate.py:27-50 |
| `halt_gate.is_auto_tradeable()` | blocks humans (is_human=1), passive mirrors (`alpaca-mirror`), and unknown ids | halt_gate.py:61-93; used in ai_brain.py:1018 |
| paper_trader **buy** HALT GATE | rejects if halt_mode != 'active' (exit_only AND full); logs gate-reject | paper_trader.py:740-751 |
| paper_trader **sell** HALT GATE | rejects only 'full'; exit_only may sell | paper_trader.py:1735-1746 |
| Arena provider build | `build_all_providers` skips `halt_mode='full'` rows | main.py:113-127; agent_routing.py:7, 42-83 |
| Scan/price loop roster | `WHERE is_active=1 AND halt_mode='active'` (excl. webull/steve-webull) | ai_brain.py:288 |
| War Room bridge vote | excludes `COALESCE(halt_mode,'active') != 'active'` — **non-active agents do NOT bridge-vote** | war_room.py:1132-1133 |
| Autopilot roster | `is_active=1 AND halt_mode='active'` | autopilot.py:123-125; same query main.py:3267-3269 |
| Guardian sweep | dedicated 10-min stop/TP sweep for exit_only `guardian-of-forever` (main scan loop never stop-checks non-active players — structural gap for ALL exit_only agents holding positions) | guardian_sweep.py:1-40; main.py:3357-3368 |
| neo-matrix signal-center consumer | relies on paper_trader.buy HALT GATE as executor chokepoint (neo is ACTIVE and does execute — corrected 2026-05-31) | crew_scanner.py:2405-2438, 3303-3312 |
| Read-side scoring filter | `HALTED_EMIT_FILTER = "halted_emit = 0"` for scoring/calibration reads only | halt_gate.py:96-116 |
| Drawdown auto-halt (distinct mechanism) | transient ≥20% drawdown halt, recomputed each cycle, no flag; unhalt = recover to new peak | risk_manager.py:1146-1170; ai_brain.py:962; CLAUDE.md:66-71 |
| Season/aux consumers | season_manager, morning_briefing, fleet_status, risk_radar, dayblade, chekov_autotrade, signal_bridge, ai_journal, proving_ground, ollie_machine_p2a all read halt_mode | grep: 169 references repo-wide |

---

## 7. Inconsistencies & stale rules (each with evidence)

1. **Core doctrine contradicts the live fleet.** trading_rules.txt:1-2 mandates "No shorting.
   No options… Long stocks only," yet ENABLED_STRATEGIES includes short_equity, inverse_etf,
   and 8 option strategies (crew_specialization.py:83-109), S6.3's PRIMARY strategy is iron
   condors (crew_specialization.py:32-39), and the prompt offers BUY_CALL/BUY_PUT/SHORT
   (base.py:1290, parse_decision). The doctrine file was never updated for the options era.
2. **Max positions: 8 vs 5 vs 3-5.** trading_rules.txt:7 and config.py:256 say 8;
   risk_manager.py:138-139 says 8/8; the prompt RULES say **5** (base.py:1298 "Maximum 5
   positions… ({len(stock_positions)}/5 max)" at 1345); backtester.py:162-175 says
   normal 5 / bear 3 / per-model 3-5. Agents are told 5 while the engine enforces 8 — the
   backtests that justify reopening used yet another set.
3. **Cash floor: 15% vs 20% vs 30%.** config.py:267 = 0.15; prompt Rule 4 = 15% (base.py:1300);
   risk_manager.py:131 UNIVERSAL_MIN_CASH_PCT = 0.20; Spock/Geordi guardrails = 0.30
   (risk_manager.py:255, 264). trading_rules.txt:3 says ≥15%. Three different floors can
   apply to the same trade.
4. **Stop-loss values conflict, and trading_rules.txt carries corrupt residue.**
   config.py:257 STOP_LOSS_PCT=0.05; trading_rules.txt §5a says −12/−18%; prompt Rule 9 says
   −12% (base.py:1305); stops.py tiers 12/15/18%. The appended Strategy Lab block
   (trading_rules.txt:57-62) prescribes stop −5% with fabricated stats (100% WR, PF 50,
   Return 0%) and the auto-deploy path that wrote it was removed after it flipped
   STOP_LOSS_PCT 0.05→0.20 on 2026-05-31 (strategy_lab.py:1026, 1075). Delete the block.
5. **Duplicate, divergent conviction-stop tables.** risk_manager.py:231-241 still returns
   **0.08** for conviction <0.70 — exactly the regression the canonical stops.py:82-93 floor
   invariant removed (documented: it hurt ollama-kimi PF 9.89→4.02). Any caller hitting the
   RiskManager staticmethod instead of engine.stops gets the outlawed tight stop. Also note
   all three conviction-scaling env flags remain default-OFF (risk_manager.py:15-38), so live
   behavior is flat stops/3% trail with shadow logging only.
6. **Sulu has two irreconcilable identities.** Persona: intraday DayBlade, −3% hard stop, no
   overnight holds, close all 3:45 ET (base.py:273-301). Mandate: S6.3 Iron Condor King,
   21-45 DTE multi-week spreads (crew_specialization.py:163-184). If reopened, whichever
   surface loads last wins the agent's behavior.
7. **Min-hold rules exist in three incompatible forms.** risk_manager.py:203-210 /
   backtester.py:152-159 = 5-10 *days* per model; paper_trader.py:1677-1711 = 24h universal /
   72h swing; prompt Rule 14 = "at least 1 full trading day" (base.py:1310). The prompt
   promise and the enforcement do not match the doctrine.
8. **Scan tier rosters are stale against the DB.** _SCAN_TIER1 (main.py:228-235) still lists
   deepseek-7b-grok4 and ollama-coder (both `full` since 06-19/20, CLAUDE.md:300-302),
   dayblade-sulu (benched/exit_only since 2026-03-31, FLEET-ROSTER.md:105-107) and super-agent
   (shelved). The May-29 drift sweeps (main.py:240-250) fixed exactly this disease in Tier 2 —
   Tier 1 now has it.
9. **ADVISORY_CREW "kept active for bridge voting" is no longer true.** The design keeps
   benched agents `active` so war_room counts their votes (FLEET-ROSTER.md:56-62), but the
   06-19/20 kill-gate moved qwen3-8b-sonnet, qwen3-14b-pro, deepseek-7b-grok4, ollama-kimi,
   dalio-metals, ollama-coder to `full` (CLAUDE.md:301-302) and war_room.py:1133 excludes
   non-active — those bridge voters are silently gone from the vote.
10. **halt_gate.py docstring is stale.** halt_gate.py:10-13 claims "the `is_halted` column
    remains for backwards-compat with ~22 read sites" — HM-B dropped the column on 2026-05-04
    (CLAUDE.md:62-63). Harmless but misleading for anyone auditing halt logic.
11. **config.AI_PLAYERS models are knowingly wrong for ~10 agents.** Documented at
    config.py:302-312 (ollama-plutus, ollama-qwen3, ollama-llama, ollama-local,
    ollama-gemma27b, ollama-kimi, qwen3-14b-grok3, qwen3-8b-4o, qwen3-8b-o3). DB is runtime
    truth; some DB model_ids are themselves garbage placeholders (neo-matrix
    `'8000 / Independent'`). CREW_MANIFEST `model` fields are a third, also-divergent source
    (e.g. crew_specialization.py:294 says McCoy = `0xroyce/plutus:latest` vs config plutus-v1).
12. **cto-grok42 War-Room path is dead.** crew_specialization.py:613: `ai_players.model_id`
    still `devstral-small-2` (uninstalled MSI-migration orphan) — debate/WR calls 404 until
    the DB row is fixed. It sits in _SCAN_TIER3 (main.py:263) regardless.
13. **Roster documents disagree on counts and identities.** FLEET-ROSTER.md:148-151 says
    21/6/45 (2026-06-01); CLAUDE.md:294-296 says 15/9/55/79 (2026-07-01). FLEET-ROSTER's
    "Active 4 voters" table predates the June churn (McCoy/Neo/Dax/Capitol). Two different
    agents both display as "Lt. Jadzia Dax" (crew_specialization.py:310, 465); main.py:239
    labels ollama-qwen3 "Scotty" while CREW_MANIFEST calls it Dax; main.py:234 labels
    mlx-qwen3 "Chekov" while CREW_MANIFEST calls it Ensign Ro. Reopening decisions made off
    display names will hit the wrong player_id.
14. **Spock's trade limit is internally inconsistent.** Persona: "Maximum 3 trades per day.
    Period." (base.py:126); MODEL_GUARDRAILS max_daily_trades=3 (risk_manager.py:257); but
    MAX_TRADES_PER_DAY["deepseek-7b-grok4"]=8 (risk_manager.py:117, "100% WR, 15 trades
    proven"). Whichever table the check reads wins.
15. **Take-profit tiers diverge.** config.py:258-264 includes a +5%→sell-50% first tier;
    RiskManager.__init__ default (risk_manager.py:284-289) starts at +10%. Agents constructed
    without explicit tiers keep more of early winners than config doctrine intends.
16. **Prompt sizing exceeds doctrine caps.** Prompt Rule 2's CONVICTION MULTIPLIER authorizes
    **40%** of capital (base.py:1299); trading_rules.txt:9 caps any position at 35%;
    config.py:265 MAX_POSITION_PCT=0.30; risk_manager get_max_position_size tops out at 33%.
    Agents are being told they may take a position the risk layer will refuse (or worse,
    won't, depending on path).
17. **Prompt options cap (20%) vs config options cap (10%).** Prompt Rule 3 says options
    limited to 20% of portfolio (base.py:1300); config.py:270 OPTIONS_TOTAL_MAX_PCT=0.10
    ("reduced from 20% to limit losses") — prompt text never updated. Troi's persona also
    still says 20% (base.py:222).
18. **"Paused" personas vs full mandates.** Nine ids have placeholder personas ("Paused.
    Former quant specialist." base.py:387-397) while CREW_MANIFEST simultaneously defines
    real mandates for them (Sisko, Tuvok, Janeway, Q, Bashir, Hoshi, Seven, Reed, Odo). If
    reopened without persona restoration, they scan with no identity/rules beyond the generic
    RULES block.
19. **Scan-interval docstring vs constants.** main.py:301-311 docstring promises market=3 min /
    power hour=90s; config.py:287-288 sets both to 300s (v3 cost-cut, 2026-03-23). Cosmetic,
    but the in-code schedule table (`_sectionIntervalDefs`, main.py:271-284) is the one to
    trust.
20. **exit_only agents holding positions are not stop-checked by the main loop.** The scan
    loop only stop-checks `halt_mode='active'` players (guardian_sweep.py:3-7); only
    guardian-of-forever got a dedicated sweep. navigator / ollie-auto / ollama-qwen3 and the
    other 2026-06 exit_only seats (9 total) have no equivalent — verify they hold no open
    positions, or extend the sweep, before/while reopening.
21. **TRADE_DESK_BYPASS_GATES=True** (config.py:34): trade-desk orders bypass daily limits,
    MAX_POSITION_VALUE, kill switch and Uhura veto. Fine for a manual desk; flag it in any
    audit of "what can trade without rules."

---

## 8. Reopening checklist (per agent, in order)

**A. Decide & document**
1. Pull the seat's record: `SELECT id, display_name, halt_mode, halted_at, halt_reason,
   model_id, is_active, is_paused FROM ai_players WHERE id='X';` — confirm the original
   benching reason (cross-ref §4 dates: 06-07 scorecard cull, 06-19/20 kill gate, S6.1
   performance bench, shelved-by-design).
2. Check exclusion classes first: HM-ORPHAN-SEATS (11 seats, model absent on olliemax —
   CLAUDE.md:312-315), Backtest Pool deliberate-OFF (5 paid seats — FLEET-ROSTER.md:78-90,
   "NOT zombies. Do NOT retire" — reopening these costs money and needs Admiral spend
   approval per Free-Models-First, CLAUDE.md:94-97), zombie candidates
   (FLEET-ROSTER.md:110-117), and human/mirror rows (never reopen: webull, alpaca-mirror).

**B. Fix the rule surfaces BEFORE the SQL flip**
3. Resolve the agent's entries in every rule location: persona (base.py MODEL_PERSONALITIES —
   restore if currently "Paused."), CREW_MANIFEST tier/conditions (advisory → tier change if
   it should actually trade), AGENT_STRATEGIES whitelist, ADVISORY_CREW membership
   (crew_specialization.py:57-80), MAX_TRADES_PER_DAY + MODEL_GUARDRAILS + MIN_HOLD_DAYS
   entries (risk_manager.py), and FLEET_TRAILING_STOP_OPT_OUT if it has its own stop.
4. Verify `ai_players.model_id` names a model actually installed on Ollie Box
   (192.168.1.168:11434) — the HM-TIER3 2026-05-28 404 sweep shows what happens otherwise;
   fix known-bad rows (cto-grok42 devstral-small-2). DB is runtime truth, not config.AI_PLAYERS
   (config.py:302-312).
5. Add the id to the correct scan tier (main.py:228-264) — and remove stale Tier-1 entries
   (Inconsistency #8) in the same commit so the roster matches DB reality. Respect RAM
   discipline / model co-residency (CLAUDE.md:91-92).
6. Reconcile the contradictions that touch this agent from §7 (esp. #2 position cap, #6 Sulu
   dual identity, #14 Spock limits, #16/#17 sizing caps) — reopening without fixing these
   re-imports the bug that got the agent benched.

**C. The flip (SQL — canonical pattern, CLAUDE.md:53-58)**
```sql
UPDATE ai_players
   SET halt_mode  = 'exit_only',          -- stage 1: exits/visibility only
       halted_at  = CURRENT_TIMESTAMP,
       halt_reason = '[2026-07-XX] staged reopen — exit_only burn-in'
 WHERE id = 'X';
-- after clean burn-in:
UPDATE ai_players
   SET halt_mode  = 'active',
       halt_reason = '[2026-07-XX] reopened: <justification + scorecard ref>'
 WHERE id = 'X';
```
Also confirm `is_active=1` and `is_paused=0` — war_room.py:1133, autopilot.py:125 and
ai_brain.py:288 filter on these too. Note the buy-side HALT GATE blocks `exit_only`
(paper_trader.py:745), so stage 1 is genuinely no-new-entries.

7. Canonical restart (required — providers are built from the DB at startup, main.py:113-127):
   `./scripts/trader_restart.sh` (zsh only, never bash — CLAUDE.md:126).

**D. Monitoring & rails**
8. Smoke-verify per Restart-then-verify doctrine (CLAUDE.md:210-215): grep trader.log for the
   provider-build count line ("HM-CN Phase 2 routing: N providers", main.py:128) and the
   agent's first scan; confirm no 404 model errors.
9. If reopening into `exit_only` with open positions, extend guardian-style stop coverage
   (guardian_sweep.py pattern) — the main loop will not stop-check it (Inconsistency #20).
10. Watch the standard rails for 1-2 weeks: gate_rejects for the id (paper_trader
    `_log_gate_reject` HALT/GRADE_B/MAX_POSITIONS/quality-gate reasons), MAX_TRADES_PER_DAY
    hits, drawdown check (auto-halts at −20%, transient), agent scorecard, and the proving
    ground SHIP/KILL criteria if routing through Sniper Mode (FLEET-ROSTER.md:66-76:
    SHIP = go≥5/6 AND maxDD ≤ −15% for 10 consecutive days).
11. Reopen in small batches (≤3 seats per restart) so scorecard attribution stays clean, and
    log each reopen + rationale in CLAUDE.md per the Archive Convention (CLAUDE.md:381-385).
12. Absolute rails that survive every reopen: **RULE #1 — nothing touches Schwab** (CLAUDE.md:
    7-20; Schwab is read-only tracking; promotion to real money requires ≥3 months live-Alpaca
    OOS Sharpe ≥ backtest + explicit Admiral approval, CLAUDE.md:31-40). Paper (Alpaca) only;
    EXEC_ROUTER stays dry-run; KILL_SWITCH file overrides everything (config.py:26).
