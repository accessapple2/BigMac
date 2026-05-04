# Audit #6 Investigation — May 4, 2026
*Conducted by Scotty (Claude Code Opus 4.7), read-only investigation. ~2 hr.*
*Investigates `XO_AUDIT_2026-05-03.md` Top-10 #6 — gate-flip calibration prerequisite.*

---

## Executive Summary

1. **Audit #6 mis-bundled three independent problems.** `signal_scorecard` (table in `data/trader.db`), Kirk's silent tables, and Janeway/Surak/ghost agents share no code, no data, and no dependency. They can ship in any order.
2. **Several audit-#6 claims are factually wrong** when checked against current DB state. `kirk_advisory_log` is actively written (272 rows, last write 2026-05-01), not silent 33 days. `janeway_signals` has 32 rows (not 0), `surak_signals` has 55 rows (not 0). The Elder Council signal-writer pipelines are **working as designed** — they're long-horizon agents whose `paper_trades` tables are intentionally sparse (Janeway = next fire 2026-07-01, Surak = next fire 2027-01-01).
3. **`signal_scorecard` writer has a real wiring gap.** The module at `engine/signal_scorecard.py` defines `log_signal()` (line 58) fully, but it has **zero callers** anywhere in the codebase. `score_signals()` (line 92) runs hourly via `main.py:2906` but only does UPDATE — it requires existing rows to score, and there are none. The only consumer of any consequence is `dashboard/app.py:3710` (`/api/v1/signal-scorecard` endpoint).
4. **`signal_scorecard` is NOT actually a gate-flip prerequisite.** The "ghost scorecard calibration" required for gate-flip per `XO_BACKLOG.md:187` and `DRYDOCK_2026-04-25.md:135` lives at **`signal-center/server.py:2121`** — a Flask route over `signals.db::trade_signals` + `signal_outcomes`. **Different scorecard, different DB, different system.** The audit conflated the two. Fixing `signal_scorecard` (this audit's #6) does not unblock gate-flip.
5. **Kirk has a split personality.** Three modules, three tables, two distinct purposes:
   - `engine/kirk_advisory.py` + `engine/kirk_grok_advisor.py` write to `kirk_advisory_log` — **active**, the Webull Holly-style daily advisor.
   - `agents/kirk.py` (`propose_swing()`) writes to `kirk_signals` / `kirk_swing_trades` — **never wired**, ghost-trade swing pipeline that has no caller.
   The "Kirk silent" audit claim was about the latter (correct) but framed as if Kirk overall was silent (wrong).

**Recommendation summary:** Pick scorecard fix only if Admiral wants the Alpha Engine indicator-scoring loop closed. Don't ship it expecting gate-flip unlock. Kirk Swing Desk needs a build/retire decision. Elder Council needs no work — it's behaving exactly as designed.

---

## The Three Problems

### Problem A — `signal_scorecard` writer

**Schema (verbatim from `sqlite3 .schema signal_scorecard`):**

```sql
CREATE TABLE signal_scorecard (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    ticker          TEXT    NOT NULL,
    direction       TEXT    NOT NULL,    -- PUT / CALL
    indicator       TEXT    NOT NULL,    -- e.g. RSI_OVERSOLD, GEX_NEG, GAP_FILL
    strategy        TEXT,                -- e.g. BEAR_PUT_SPREAD, MARKET_PUT
    confidence      REAL    DEFAULT 0,
    entry_price     REAL,
    strike          REAL,
    expiry          TEXT,
    vix_at_entry    REAL,
    gex_at_entry    REAL,
    session         TEXT,
    -- Outcome fields (filled in by score_signals)
    exit_price      REAL,
    outcome_pct     REAL,
    win             INTEGER,             -- 1=win 0=loss NULL=pending
    scored_at       TEXT,
    notes           TEXT
);
CREATE INDEX ix_sc_ticker ON signal_scorecard(ticker);
CREATE INDEX ix_sc_indicator ON signal_scorecard(indicator);
CREATE INDEX ix_sc_created ON signal_scorecard(created_at);
```

**Current state: writer-defined-but-never-called.**

Module `engine/signal_scorecard.py` (179 lines, 6 functions) is fully implemented:

- `log_signal(signal_data: dict) -> int` (line 58) — INSERT path. **Has zero callers anywhere in the codebase.** Verified via `grep -rn "log_signal" --include="*.py"` filtered for non-archive/non-bak: every match is either the function definition itself, a docstring reference, or `_log_signal_only` in `paper_trader.py` (a different function).
- `score_signals()` (line 92) — UPDATE-only outcome scorer. Wired to scheduler at `main.py:2906-2907` inside `run_signal_scorecard()`, fires hourly via `schedule.every(1).hours.do(run_signal_scorecard)`. **Currently runs to completion as a no-op** because the SELECT at line 100-107 returns 0 rows (early-return at line 110-111 when `pending` is empty).
- `get_scorecard(limit)` (line 146), `get_indicator_stats()` (line 162) — readers.
- `ensure_tables()` (line 26) — DDL bootstrap.

`SELECT COUNT(*) FROM signal_scorecard` → **0**.

**Reader call sites (3 in production code):**

1. `dashboard/app.py:3710` — `@app.get("/api/v1/signal-scorecard")` endpoint:
   ```python
   from engine.signal_scorecard import get_scorecard, ensure_tables
   ensure_tables()
   return {"signals": get_scorecard(limit)}
   ```
2. `engine/brain_context.py:154` — `_source_signal_scorecard(symbol)` for AI brain context. **NOTE: contains a latent bug — line 156 filters by `s.get("symbol")` but the schema column and `get_scorecard()` return key is `ticker`. Reader silently produces 0 results even with data.** Documented; not fixed.
3. `engine/indicator_bench.py:92,98` — daily indicator benchmark, fires once daily ~4:30 PM ET, computes per-indicator win rates. Currently logs `"No scored signals yet — skipping benchmark run"` every day (line 105 of indicator_bench).

**Source tables it appears to want to read from:** `signal_scorecard` itself only (the writer takes a dict from a caller, not a SELECT). The natural caller would be **wherever indicator-level signals are emitted** — `RSI_OVERSOLD`, `GEX_NEG`, `GAP_FILL` are indicator-bench-style outputs, NOT AI-player BUY/SELL signals. Likely candidate caller files: `strategies/bull_call_spread_v1.py`, `strategies/bear_put_spread_v1.py` (RSI/GEX-driven), `engine/screener_engine.py`, `engine/dayblade.py`. None of them currently call `log_signal()`.

**Key clarification:** `signal-center/server.py:2121` is a different `/api/signals/scorecard` route entirely — it queries `trade_signals` + `signal_outcomes` in `signal-center/signals.db`, not our `signal_scorecard` table in `data/trader.db`. The audit's reference to "Reader exists at `signal-center/server.py:~2104`" pointed at the wrong file. **This other scorecard is a separate, working system** — it's the one referenced by `XO_BACKLOG.md:187` for gate-flip calibration.

**Proposed contracts (Admiral picks):**

- **Option 1 — Indicator-Lab:** Wire `log_signal()` from `engine/screener_engine.py` whenever a single-indicator setup fires (RSI_OVERSOLD, GEX_NEG, GAP_FILL). Outcome window: `score_signals()` already uses 1-hour-old + first-touch price. Hit = ≥1% directional move (already coded at line 132-135). Effort: ~2 hours, ~30 LOC. Adds a new feature to the Alpha-Engine sub-system; doesn't change any existing behavior.
- **Option 2 — Strategy-Lab:** Wire `log_signal()` from `strategies/bull_call_spread_v1.py:fire()` and `strategies/bear_put_spread_v1.py:fire()` so every spread entry gets a per-leg outcome row. Outcome window: extend `score_signals()` to 24h or 48h to give spreads time to mature. Effort: ~4 hours (need to extend `score_signals()` window logic + add caller side). Higher value because it tracks *strategy-level* not *indicator-level* outcomes.
- **Option 3 — Retire:** Drop the table, remove the scheduler entry at `main.py:2902-2910`, remove the `/api/v1/signal-scorecard` endpoint, remove `_source_signal_scorecard` from brain_context. Effort: ~30 minutes. Reasonable if the Alpha-Engine indicator-scoring concept has been superseded by the ghost-trader scorecard or `signal-center/server.py` scorecard.

**Effort estimate:** Option 1 = 2 hr, Option 2 = 4 hr, Option 3 = 30 min.

**Recommendation:** **Not gate-flip blocking.** Defer to Admiral's Alpha-Engine roadmap decision. Option 3 (retire) is acceptable if the calibration column promised in `UX_SPRINT_2026-04-28.md` Priority 1 is read from the *ghost*-scorecard at `signal-center/server.py:2121`, not from `engine/signal_scorecard.py`. Option 1 is the lowest-risk way to "ship" if the Alpha-Engine concept is to be preserved.

---

### Problem B — Kirk silent

**Audit claim:** `kirk_advisory_log` last write `2026-03-31 18:18:23`. **WRONG.**

**Verified DB state:**

```
sqlite3 data/trader.db "SELECT MAX(created_at) FROM kirk_advisory_log;"
→ 2026-05-01 00:01:25

sqlite3 data/trader.db "SELECT COUNT(*) FROM kirk_advisory_log;"
→ 272

# Daily writes Apr 1 → May 1 (last 30d):
2026-04-01: 1, 2026-04-08: 1, 2026-04-09: 1, 2026-04-10: 5,
2026-04-11: 28, ..., 2026-04-29: 3, 2026-05-01: 4
```

The audit's "33 days silent" claim does not match observed reality. The table has been writing daily. The last entry timestamp `2026-03-31 18:18:23` referenced by the audit corresponds to `id=1` (first row), not the last row.

**However**, two related Kirk tables ARE empty and have no writers:

```
kirk_signals      → 0 rows  (designed for swing-trade BUY/SELL with pike_vote)
kirk_swing_trades → 0 rows  (designed for ghost-traded swing P&L)
```

`grep "INSERT INTO kirk_signals"` and `INSERT INTO kirk_swing_trades` return zero matches in production code.

**Code locations (3 modules under "Kirk"):**

1. `engine/kirk_advisory.py` (14.8 KB, mtime 2026-04-25) — `generate_kirk_advisory()` at line 170. Writes to `kirk_advisory_log`. **ACTIVE.** Schema check + dedup query at line 327; INSERT at line 333.
2. `engine/kirk_grok_advisor.py` (16.1 KB, mtime 2026-04-26, has `.bak.20260426` sibling) — `run_grok_advisory()` at line 302. Hooked to scheduler via `main.py:1718` (`run_grok_advisor`) — fires 9:30 AM and 1:30 PM ET on weekdays. **ACTIVE.**
3. `agents/kirk.py` (9.1 KB, mtime 2026-04-23) — `propose_swing(ticker, context)` at line 132, `get_kirk_brief()` at line 225. Designed to write `kirk_signals` and `kirk_swing_trades` via internal `_ensure_tables()` at line 61. **NEVER WIRED.** No external caller of `propose_swing` exists. No scheduler entry. No `from agents.kirk import propose_swing` anywhere except `agents/kirk.py:255` (its own `if __name__ == "__main__"` block).

**Failure mode:** Not crash, not regression — **never-built**. The Kirk Swing Desk pipeline (`agents/kirk.py` + `agents/pike.py::second_opinion`) was scaffolded but never connected to the scheduler. Per `CLAUDE.md:128-129` Pending TODOs:
> "Build Swing Desk agents (Kirk = qwen3:8b, Pike backup = mistral:7b) — ghost-trade swing setups for 30 days before promoting"

This TODO is still pending. Kirk-as-Webull-advisor (engine/kirk_*) was built and works; Kirk-as-Swing-Desk (agents/kirk.py) was scaffolded but the scheduler integration was never written.

**Pike's emptiness is downstream:** `pike_votes = 0 rows`. `agents/pike.py::second_opinion(ticker, kirk_proposal)` is called only from `agents/kirk.py:173` (`propose_swing` lazy-imports it). If Kirk's `propose_swing` never fires, Pike never votes. Pike is not independently broken — it's gated behind the never-wired Kirk caller.

**Working sibling for comparison — Sarek:**

```
sarek_signals       → 30 rows, last write 2026-05-04T05:47:14
sarek_paper_trades  → 3 rows, last write 2026-05-01T05:31:22 (monthly DCA on 1st)
```

`agents/sarek.py` differs from `agents/kirk.py` in two ways that matter:
1. Sarek has scheduler hookup at `main.py:3552-3565` (`run_sarek_monthly`) and `main.py:3608-3617` (`run_elder_briefs` calls `get_sarek_brief()`).
2. Sarek's `get_sarek_brief()` (line 133, 24h cache) writes to `sarek_signals` daily, and `run_monthly_dca()` (line 204) writes to `sarek_paper_trades` on calendar-cadence.
3. Kirk has `get_kirk_brief()` and `propose_swing()` but **no equivalent to `run_<x>_dca`** in the scheduler. Kirk was supposed to be event-driven (per ticker, per opportunity), not calendar-driven, so the scheduler integration would look different — needs a "scan watchlist for swing setups" wrapper that doesn't exist yet.

**Recommendation: BUILD or RETIRE — Admiral picks.** Either:
- **BUILD:** ~6-8 hours. Add `run_kirk_swing_scan()` to `main.py` that iterates the watchlist, calls `propose_swing()` per ticker on a 30-min or hourly cadence during market hours, threads through Pike's `second_opinion()` for the ambiguous cases. Per `CLAUDE.md` doctrine: ghost-trade for 30 days before promoting. Net effect: Kirk Swing Desk goes from scaffolded to live-ghost-trading.
- **RETIRE:** ~30 minutes. Drop `kirk_signals` and `kirk_swing_trades` tables. Update `CLAUDE.md:84-85` and `:128-129` to remove the Swing Desk concept. Move `agents/kirk.py` and `agents/pike.py` to `agents/_archive/2026-05-04/`. Document retirement reason: "Webull-side Kirk advisor (engine/kirk_advisory.py) covered the gap; standalone swing desk never built".

**Effort estimates:** BUILD = 6-8 hr, RETIRE = 30 min + CLAUDE.md edit.

**If RETIRE — CLAUDE.md cleanup needed:**
- Lines 80-85 (Swing Desk section): remove Kirk + Pike rows
- Line 106: keep (historical Grok-4 retirement note still accurate)
- Lines 128-129 (Pending TODOs): delete Swing Desk TODO entry
- Line 159 (Drydock note): keep (it documents Ollie URL routing fix that still applies)

---

### Problem C — Janeway / Surak / ghost agents

**Per-agent table inventory (verbatim from DB):**

| Table | Rows | Last write (col=value) | Module | Scheduler | Status |
|---|---:|---|---|---|---|
| `kirk_advisory_log` | 272 | created_at=2026-05-01 | engine/kirk_advisory.py + engine/kirk_grok_advisor.py | main.py:1718 | ✅ ACTIVE |
| `kirk_signals` | 0 | — | agents/kirk.py | none | 🚫 NEVER WIRED |
| `kirk_swing_trades` | 0 | — | agents/kirk.py | none | 🚫 NEVER WIRED |
| `pike_votes` | 0 | — | agents/pike.py | none | 🚫 downstream of Kirk |
| `sarek_signals` | 30 | timestamp=2026-05-04T05:47 | agents/sarek.py | main.py:3552, 3608 | ✅ ACTIVE |
| `sarek_paper_trades` | 3 | timestamp=2026-05-01T05:31 | agents/sarek.py | main.py:3552 (monthly) | ✅ AS DESIGNED (next: 2026-06-01) |
| `janeway_signals` | 32 | timestamp=2026-05-02T05:45 | agents/janeway.py | main.py:3568, 3608 | ✅ ACTIVE |
| `janeway_paper_trades` | 0 | — | agents/janeway.py | main.py:3568 (quarterly) | ✅ AS DESIGNED (next: 2026-07-01) |
| `surak_signals` | 55 | timestamp=2026-05-04T05:47 | agents/surak.py | main.py:3585, 3608 | ✅ ACTIVE |
| `surak_paper_trades` | 0 | — | agents/surak.py | main.py:3585 (annual) | ✅ AS DESIGNED (next: 2027-01-01) |
| `ghost_trades` | 9 | ts=2026-05-01T11:50 | engine/ghost_trader.py + engine/ghost_trades.py | engine/ghost_trader.py:run_daemon | ✅ ACTIVE-SPARSE |
| `ghost_options_watch` | 1 | ts=2026-05-01T13:26 | (no INSERT writer found) | — | ⚠️ static or stale |
| `ghost_portfolio` | 14 | opened_at=2026-05-01T11:50 | scripts/ghost_advisor.py | — | ✅ ACTIVE |
| `ghost_ticker_cache` | 18 | updated_at=2026-05-04T14:16 | scripts/ghost_advisor.py | scripts/ghost_advisor.py | ✅ ACTIVE |
| `ghost_seed` | 14 | — | scripts/ghost_seed.py | seed-once | static |
| `ghost_advisor_state` | 4 | — | scripts/ghost_advisor.py | — | ✅ ACTIVE |
| `ghost_cooldowns` | 4 | — | scripts/ghost_advisor.py | — | ✅ ACTIVE |
| `ghost_equity_history` | 2 | — | scripts/ghost_advisor.py | — | sparse |
| `ghost_cash` | 1 | — | scripts/ghost_advisor.py | — | static |
| `picard_briefings` | 3 | — | (separate flow) | weekly | ✅ ACTIVE-SPARSE |
| `holly_deepdives` | 10 | created_at=2026-04-21 | (separate flow) | — | ⏸ stale 13d (worth flagging) |
| `rikers_log` | 2495 | created_at=2026-05-04 | engine/riker_xo.py | main.py | ✅ VERY ACTIVE |

**Audit claims fact-checked:**

| Audit claim | Reality | Verdict |
|---|---|---|
| `signal_scorecard` 0 rows, writer never wired | 0 rows confirmed; writer `log_signal()` defined but uncalled | ✅ correct |
| `kirk_advisory_log` last write 2026-03-31 (33 days silent) | Last write 2026-05-01, 272 rows, daily activity | ❌ wrong |
| `kirk_signals` 0 rows | 0 rows confirmed | ✅ correct |
| `janeway_paper_trades` 0 | 0 confirmed BUT by design (quarterly cadence; next 2026-07-01) | ✅ technically correct, ❌ framing |
| `surak_paper_trades` 0 | 0 confirmed BUT by design (annual cadence; next 2027-01-01) | ✅ technically correct, ❌ framing |
| `ghost_options_watch` 1 row | Confirmed; last write 2026-05-01 | ✅ correct |
| `ghost_trades` 9 rows | Confirmed; last write 2026-05-01 | ✅ correct |

**Shared root cause with Kirk?** **No.**

Kirk Swing Desk's emptiness is "scaffolded but never wired to scheduler." Janeway/Surak's `paper_trades` emptiness is "wired correctly, fires on calendar cadence, hasn't fired yet because today isn't 1st-of-quarter / 1st-of-year." `ghost_options_watch` having 1 row is a separate question (no INSERT writer found in production code — possibly seeded once via REPL or migration).

**Recommendations per agent:**

- **Janeway** = NO ACTION. Working as designed. Quarterly DCA next fires 2026-07-01.
- **Surak** = NO ACTION. Working as designed. Annual DCA next fires 2027-01-01.
- **Sarek** = NO ACTION (already working — 3 trades since 2026-04-01 monthly cadence).
- **Pike** = depends on Kirk Swing Desk decision (REPAIR/RETIRE).
- **Ghost trader** = NO ACTION. Sparse but active. `ghost_options_watch` 1-row anomaly is worth a 15-min investigation in a separate session if the Admiral cares — not in scope for #6.
- **Holly deepdives stale 13 days** (last write 2026-04-21) = flag for separate investigation. Out of audit-#6 scope.

---

## Dependency Graph

```
signal_scorecard writer  ─── NO DEPENDENCIES on Kirk/Janeway/Surak ────  (independent)
                          │
                          └── reader at dashboard/app.py:3710 (independent)
                              reader at engine/brain_context.py:154 (broken: ticker/symbol bug)
                              reader at engine/indicator_bench.py:92 (waits silently)

Kirk Swing Desk ─── depends on: nothing. Self-contained build/retire decision.
                  └── pike_votes empty AS A CONSEQUENCE (downstream)

Elder Council ─── working as designed. No work needed. Flag if calendar misses.

Ghost trader ─── working. ghost_options_watch 1-row anomaly is separate.

Gate-flip prerequisite ─── lives at signal-center/server.py:2121 (DIFFERENT scorecard,
                            DIFFERENT DB, DIFFERENT system from audit-#6's signal_scorecard)
```

**Hidden coupling that would surprise the Admiral:**

1. **Audit #6 framed signal_scorecard as gate-flip-blocking. It isn't.** The "ghost scorecard calibration" referenced in `XO_BACKLOG.md:187` and `DRYDOCK_2026-04-25.md:135` queries `signals.db::trade_signals + signal_outcomes` via the Flask route at `signal-center/server.py:2121`. Our `signal_scorecard` table in `data/trader.db` is a completely separate Alpha-Engine concept. Wiring its writer does not unblock gate-flip.
2. **`brain_context._source_signal_scorecard` has a column-name bug** (line 156: `s.get("symbol")` vs schema `ticker`) that would silently produce 0 results even if the writer were wired. Documented; not fixed per investigation discipline.
3. **CLAUDE.md still lists the Swing Desk Kirk + Pike as a Pending TODO** (line 128-129). The Admiral's `CLAUDE.md` reflects intent that hasn't been built. Either build needs to happen, or `CLAUDE.md` needs to retire the entry. There is no third option that leaves the docs honest.

---

## Recommended Ship Order

1. **Audit #6B = Kirk Swing Desk decision (BUILD or RETIRE)** — independent of everything, smallest blast radius if RETIRE, most explicit Pending TODO in `CLAUDE.md`. Decision should land before further fleet expansion.
2. **Audit #6C = `signal_scorecard` writer (Option 1, 2, or 3)** — independent of #6B. Defer if Alpha-Engine roadmap is unclear; ship Option 1 if a simple "indicator-lab" wins-and-losses panel is wanted; ship Option 3 (retire) if the concept has been superseded.
3. **No Audit #6D needed** — Janeway/Surak/Sarek/ghost are all working as designed or working sparsely. Defer indefinitely.

**Why this order:** Kirk's CLAUDE.md TODO is a documentation lie or pending build — fixing the doc-vs-reality drift first is cheap insurance. `signal_scorecard` is purely additive: shipping it never breaks anything, but skipping it costs nothing immediate either.

**Justification for de-prioritizing #6C:** The audit's framing made `signal_scorecard` look gate-flip-blocking. Investigation shows it isn't. The actual gate-flip prerequisite (signal-center scorecard) is a separate system that may be working — out of scope for this investigation but flagged for next-pass.

---

## Open Questions for the Admiral

1. **Scorecard scoring window — what counts as a "hit"?** Current code (line 132-135) hits on ≥1% directional move within 1 hour of entry. Is that the intended contract, or should it be 24h / first-touch / max-favorable-excursion?
2. **Kirk Swing Desk verdict — BUILD (~6-8 hr) or RETIRE (~30 min + CLAUDE.md edit)?** Per `CLAUDE.md:128-129`, this was scoped to "ghost-trade for 30 days before promoting". Is that 30-day clock something we want to start, or has the Webull-side Kirk advisor (engine/kirk_advisory.py) made it redundant?
3. **`signal_scorecard` table — Option 1 (indicator-lab), Option 2 (strategy-lab), or Option 3 (retire)?** Depends on Alpha-Engine roadmap. If Option 3, also retire the scheduler entry at `main.py:2902-2910`, the API endpoint at `dashboard/app.py:3707-3712`, and the `_source_signal_scorecard` import at `brain_context.py:154`.
4. **`signal-center/server.py:2121` scorecard — separately verify it's actually working before next gate-flip review?** This is the actual gate-flip dependency. Worth a 30-min separate audit pass to confirm `signals.db::trade_signals + signal_outcomes` are populated and the SELECTs return real numbers, not empty.
5. **`brain_context._source_signal_scorecard` `symbol` vs `ticker` bug** — fix in #6C, separately, or never? Currently silently broken; would only manifest if signal_scorecard ever has rows.
6. **Holly deepdives stale 13 days** — flag for separate investigation? Last write 2026-04-21 is suspicious given other agent writes are all current.
7. **`CLAUDE.md` updates required regardless of #6B verdict** — the doc says "Active 4 voters" + "Bench 4 ghost-traded" + "Swing Desk Kirk+Pike". The Swing Desk row claims an active state that isn't backed by code. Update this Sunday whether we BUILD or RETIRE.

---

## Coverage Gaps

This investigation deliberately did not look at:

- **`signal-center/server.py:2121` scorecard** — beyond confirming it's a different system. Whether it's actually populated and returning sensible numbers is the real gate-flip prerequisite, but it's a separate audit (call it #6X).
- **`ghost_options_watch` 1-row mystery** — nobody appears to INSERT into it; the row exists. Could be migration-seeded, REPL-seeded, or there's a writer hidden behind dynamic SQL. 15-min separate investigation, not in scope.
- **Holly deepdives stale 13 days** — flagged but not pursued.
- **The 7+ ghost_* tables not detailed above** — most have ts/timestamp data showing recent writes; sampled enough to confirm "ghost trader works", not exhaustive.
- **Whether the existing Pending TODO at `CLAUDE.md:128-129` for Swing Desk is the only stale TODO** — investigation found it; didn't audit the rest of the TODO list.
- **Latent bugs found but not fixed:** column-name `symbol`-vs-`ticker` mismatch in `brain_context.py:156`. Investigation discipline says document, don't fix.

---

## Citations

**Code references (file:line):**
- `engine/signal_scorecard.py:58` — `def log_signal(signal_data: dict) -> int:`
- `engine/signal_scorecard.py:92` — `def score_signals():`
- `engine/signal_scorecard.py:100-107` — pending SELECT (early-return empty path)
- `engine/signal_scorecard.py:132-135` — hit definition (≥1% directional)
- `engine/signal_scorecard.py:146` — `def get_scorecard(limit: int = 50)`
- `main.py:2902-2910` — `run_signal_scorecard` scheduler block
- `main.py:1718` — `run_grok_advisor` (Kirk Webull-style advisor scheduler)
- `main.py:3543-3620` — Elder Council scheduler block (Sarek/Janeway/Surak)
- `dashboard/app.py:3707-3712` — `/api/v1/signal-scorecard` endpoint
- `engine/brain_context.py:148-170` — `_source_signal_scorecard` (with line 156 ticker/symbol bug)
- `engine/indicator_bench.py:62-110` — `run_indicator_bench` consumer
- `signal-center/server.py:2121` — `@app.route('/api/signals/scorecard')` Flask route — **DIFFERENT SYSTEM**
- `engine/kirk_advisory.py:170` — `generate_kirk_advisory()` writer (active)
- `engine/kirk_advisory.py:333` — `INSERT INTO kirk_advisory_log`
- `engine/kirk_grok_advisor.py:302` — `run_grok_advisory()` (Webull side, active)
- `agents/kirk.py:132` — `propose_swing()` (orphaned)
- `agents/kirk.py:173` — lazy import of `agents.pike.second_opinion`
- `agents/kirk.py:255` — only caller of `propose_swing` (`__main__` block)
- `agents/pike.py:104` — `second_opinion(ticker, kirk_proposal)`
- `agents/pike.py:143` — `INSERT INTO pike_votes`
- `agents/sarek.py:178,227` — `INSERT INTO sarek_signals`, `INSERT INTO sarek_paper_trades`
- `agents/janeway.py:173,217` — `INSERT INTO janeway_signals`, `INSERT INTO janeway_paper_trades`
- `agents/surak.py:180,226` — `INSERT INTO surak_signals`, `INSERT INTO surak_paper_trades`
- `engine/ghost_trader.py:148` — `INSERT INTO ghost_trades`

**DB queries + results:**
```
SELECT COUNT(*) FROM signal_scorecard;                   → 0
SELECT MAX(created_at) FROM kirk_advisory_log;           → 2026-05-01 00:01:25
SELECT COUNT(*) FROM kirk_advisory_log;                  → 272
SELECT COUNT(*) FROM kirk_signals;                       → 0
SELECT COUNT(*) FROM kirk_swing_trades;                  → 0
SELECT COUNT(*), MAX(timestamp) FROM janeway_signals;    → 32, 2026-05-02T05:45:24
SELECT COUNT(*) FROM janeway_paper_trades;               → 0
SELECT COUNT(*), MAX(timestamp) FROM surak_signals;      → 55, 2026-05-04T05:47:32
SELECT COUNT(*) FROM surak_paper_trades;                 → 0
SELECT COUNT(*), MAX(timestamp) FROM sarek_signals;      → 30, 2026-05-04T05:47:14
SELECT COUNT(*) FROM sarek_paper_trades;                 → 3
SELECT COUNT(*) FROM pike_votes;                         → 0
SELECT COUNT(*) FROM ghost_options_watch;                → 1
SELECT COUNT(*) FROM ghost_trades;                       → 9
SELECT COUNT(*), MAX(updated_at) FROM ghost_ticker_cache;→ 18, 2026-05-04T14:16:30
SELECT COUNT(*) FROM rikers_log;                         → 2495
```

**Doc references:**
- `docs/UX_SPRINT_2026-04-28.md` Priority 1 — describes calibration column requirements
- `docs/XO_BACKLOG.md:70` — AI-1 entry (signal_scorecard writer)
- `docs/XO_BACKLOG.md:187` — gate-flip dependency on `signal-center/server.py:2104`
- `docs/DRYDOCK_2026-04-25.md:135` — "Ghost scorecard calibration before gate-flip"
- `docs/MONDAY_CHECKLIST_2026-04-27.md` — Tuesday gate-flip per-agent checklist
- `docs/XO_AUDIT_2026-05-03.md:11` — original audit-#6 framing (incorrect)
- `CLAUDE.md:84-85` — Kirk + Pike active-state claim
- `CLAUDE.md:128-129` — Pending TODO for Swing Desk build

— Lt. Cmdr. M. Scott, 2026-05-04 08:30 MST
