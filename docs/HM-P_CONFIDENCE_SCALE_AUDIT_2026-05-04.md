# HM-P — Confidence-Scale Audit
*2026-05-04, Scotty investigation, no fixes applied*

## Background

- `data/trader.db::signals.confidence` is **REAL, scale 0.0–1.0**
- `signal-center/signals.db::trade_signals.confidence` is **INTEGER, scale 0–100**
- `data/trader.db::deep_scan_results.confidence` is **REAL 0.0–1.0**
- `data/trader.db::ghost_trades.confidence` is **REAL 0.0–1.0**
- `data/trader.db::watchlist_signals.confidence` is **REAL 0.0–1.0**

Same field name, different scales across DB boundaries. Footgun risk on any code path that reads from one DB and compares against a literal sized for the other.

## Findings

### Total comparisons audited: **42 production sites** (+10 alternate-named `conf` sites)
*Excluded: `test_*`, `*.bak`, `archive/`, `.venv/`, prompt-template strings inside `engine/providers/base.py` that document scale to LLMs.*

### Classification summary

| Class | Count | % |
|---|---|---|
| ✅ CORRECT | 49 | 96% |
| ⚠️ AMBIGUOUS | 2 | 4% |
| 🚨 WRONG | **0** | 0% |

### **NO URGENT FLAG.** Gate-flipped strategy code is CORRECT.

All four gate-flipped files use `TB_CONF_THRESHOLD = 85` against `trade_signals.confidence` (INT 0–100). Verified:
- `strategies/bull_call_spread_v1.py:11,131-135` — `TB_CONF_THRESHOLD = 85` → `WHERE agent_name='tractor-beam' AND symbol=? AND confidence >= ?`
- `strategies/bear_put_spread_v1.py:9,148-152` — same pattern
- `strategies/executor.py` — no numeric confidence comparisons
- `strategies/exit_manager.py` — no numeric confidence comparisons

The Captain may continue the gate-flip soak; this audit reveals no spread-strategy bugs.

---

## Detailed classification

### ✅ CORRECT — INT 0–100 scale (reading from `trade_signals` or local INT vars)

| File:line | Comparison | Source |
|---|---|---|
| `strategies/bull_call_spread_v1.py:131-135` | `confidence >= 85` | `trade_signals` (TB_CONF_THRESHOLD=85) |
| `strategies/bear_put_spread_v1.py:148-152` | `confidence >= 85` | `trade_signals` (TB_CONF_THRESHOLD=85) |
| `engine/strategies.py:662,746,819` | `confidence >= 70` / `TB_DIRECT_THRESHOLD=85` | `trade_signals` |
| `engine/strategies.py:885` | `tb_conf >= 85` | `trade_signals` |
| `engine/ghost_trader.py:123` | `confidence >= 70` | `trade_signals` |
| `engine/ghost_trader.py:141` | `conf >= 90 / 85` | `trade_signals` (line 140 casts) |
| `engine/archer_morning_synthesis.py:116` | `confidence >= 80` | `trade_signals` (file comment confirms DB) |
| `engine/crew_scanner.py:189` | `SNIPER_MIN_CONFIDENCE = 55` | local INT var |
| `engine/crew_scanner.py:3085,3190,3192,3194` | `confidence >= 90/80/70` | local INT (3180: `conf_normalized = confidence / 100.0`) |
| `engine/crew_scanner.py:1648,1650` | `_tb_conf >= 80 / > 0` | trade_signals |
| `engine/dayblade_scanner.py:78` | `confidence > 80` | param `confidence: int = 0` (signature documents scale) |
| `engine/ollie_commander.py:274` | `confidence >= 90` | param documented `0-100 LLM confidence` (line 201) |
| `engine/ollie_commander.py:271` (comment only, no code) | — | — |
| `signal-center/server.py:1918,1940` | `confidence >= 80 / >= 85` | trade_signals writer-side |
| `signal-center/server.py:2759,2761` | `conf >= 75 / 60` | local INT (`int(row['confidence'])`) |

### ✅ CORRECT — REAL 0.0–1.0 scale (reading from `signals` / `ghost_trades` / `watchlist_signals` / `deep_scan_results` / player decisions)

| File:line | Comparison | Source |
|---|---|---|
| `engine/signal_tracker.py:30` | `confidence < 0.65` | `watchlist_signals` (REAL) |
| `engine/ghost_trades.py:21` | `confidence < 0.60` | player decision REAL |
| `engine/ai_brain.py:969,980,989,995,1066,1169,1171,1183,1307` | `>= 0.65/0.60/0.85/0.80/0.50/0.70/0.90` | `decision.confidence` (player REAL) and `ghost_trades.confidence` (REAL) |
| `engine/paper_trader.py:716,724,1522` | `< 0.70/0.90` | player decision REAL |
| `engine/dayblade.py:873` | `confidence < 0.70` | player decision REAL |
| `engine/capitol_fund.py:140` | `confidence < 0.81` | player decision REAL |
| `engine/risk_manager.py:268` | `confidence < 0.80` | player decision REAL |
| `engine/historical_backtest.py:368,505` | `>= 0.65 / < 0.65` | LLM prompt + decision REAL |
| `engine/morning_briefing.py:715` | `confidence >= 0.5` | `deep_scan_results` (REAL) |
| `engine/model_dna.py:221,223` | `avg_conf >= 0.80 / 0.70` | aggregated player REAL |
| `engine/providers/base.py:1442` | `confidence < 0.50` | LLM decision return REAL |
| `engine/providers/base.py:1293,1302,1332` | (text in prompt strings — document 0–1 scale to LLMs) | n/a |
| `engine/learning_engine.py:214` | (string `> 90%` for LLM context) | n/a |

### ⚠️ AMBIGUOUS (2)

**1. `engine/strategies.py:659-660` (comment-only)** — comment says "TB signals with confidence >= 90% skip convergence". The `%` suggests INT scale, and the actual code at `:746` uses INT (TB_DIRECT_THRESHOLD=85). Comment is consistent but reads ambiguously. **Recommendation:** annotate comment as `>= 90 (INT 0-100 from trade_signals)`.

**2. `engine/strategies.py:803-804` (comment-only)** — similar pattern. Same recommendation.

Neither is a runtime bug — flagged for future doc-cleanliness only.

### 🚨 WRONG (0)

**No scale-mismatch bugs found in production code.**

---

## Cross-cutting observations

### Convention is implicit, not enforced

The 42 production sites follow a clean convention:
- Code that touches `signal-center/signals.db` or `trade_signals` → **INT 0–100**
- Code that touches `data/trader.db::signals` or player-decision objects → **REAL 0–1**

But this convention is **nowhere documented** outside of inline comments in `ollie_commander.py:201` and `dayblade_scanner.py:78` (signature). A future refactor that, say, moves a function from `engine/ai_brain.py` (REAL world) to `engine/crew_scanner.py` (INT world) without renaming variables would silently break.

### Highest-blast-radius mismatch surfaces

If a future bug is going to slip in, it'll happen at one of these crossings:
1. `engine/crew_scanner.py` (INT world) calls into `engine/ai_brain.py` decision evaluation (REAL world) — currently safe because crew_scanner divides by 100 first (`conf_normalized = confidence / 100.0`).
2. `engine/ollie_commander.approve_or_reject` (INT 0-100 per signature) is called with `float(confidence)` from crew_scanner — works because the float just preserves the INT magnitude. But an unwary refactor could pass `decision.confidence` (REAL 0-1) here, which would silently fail (everything < 90).
3. Any new strategy file that copies the `WHERE confidence >= 0.85` pattern from ai_brain into a query against `trade_signals` would silently return zero rows.

### Footgun rating: medium

This is not a fire. There are zero bugs today. But the convention is one careless paste away from a silent failure. Worth one of:
- Naming convention: rename `confidence` → `confidence_pct` in INT world, leave `confidence` for REAL world.
- Type-hint convention: annotate `confidence: int` in INT functions, `confidence: float` in REAL.
- Boundary helpers: a `_to_pct(x: float) -> int` and `_to_unit(x: int) -> float` at the DB-read sites.

---

## Recommended fix shape (deferred to future session)

**Don't unify scales** — both DBs have their own ecosystems and ~50 call sites each. The migration cost outweighs the bug rate (zero today).

**Do annotate.** Add a `# scale: 0-100 INT (from trade_signals)` or `# scale: 0-1 REAL (from signals/decision)` comment at every confidence-comparison site. Cheap, prevents future bugs, no runtime change. Estimated effort: 60-90 min one-shot pass.

**Optionally rename param** in `engine/ollie_commander.approve_or_reject` from `confidence` → `confidence_pct` since that function is the most likely cross-boundary surface (it's called from crew_scanner with INT, but anyone wiring it into a new caller from ai_brain world might pass REAL).

## Open questions for the Admiral

1. **Is unifying the scales worth a half-day session sometime?** Picking one canonical scale (probably INT 0-100, since that's what the LLM prompts emit naturally) and writing migrations for both DBs would close this footgun for good. Not urgent — just wanted on the radar.
2. **Should I queue the annotation pass as HM-P-fix?** It's a low-risk, high-value cleanup that could ride alongside the next doc session.
3. **Does the `trade_signals.confidence INTEGER` column ever store fractional values via implicit cast?** I didn't probe the SQLite-level coercion behavior; if some writer accidentally inserts `0.85` into the INT column, SQLite may silently store `0` (or `0.85` — SQLite's type system is famously loose). Worth a one-shot data-quality check: `SELECT COUNT(*) FROM trade_signals WHERE confidence < 1`.
