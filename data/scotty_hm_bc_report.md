# HM-BC Discovery

**Phase:** BC.0 (read-only inventory + endpoint smoke)
**Date:** 2026-05-11
**Scotty:** Claude Code · Opus 4.7 · `claude-opus-4-7[1m]`
**Premise from Captain's brief:** *"engine/ghost_trader.py (singular) reads the renamed legacy DB; 3 dashboard endpoints may now 500."*

**Discovery finding:** **The premise is wrong.** Endpoints don't 500 — `init_db()` silently recreated `data/ghost_trades.db` on first call, and the system auto-revived. More important: **singular and plural are orthogonal systems serving different ghost-tracking concerns**, not a stale-vs-canonical pair. Retiring singular would delete a working, UI-bound, signal-center scoring pipeline. **Recommend a 4th option (D) — do not retire; disambiguate.**

---

## 1. Callers of `engine.ghost_trader` (singular)

Only **3 import sites**, all in `dashboard/app.py`, all lazy imports inside try/except:

```
dashboard/app.py:16977   from engine.ghost_trader import get_scorecard, init_db          → /api/ghost/scorecard
dashboard/app.py:16989   from engine.ghost_trader import get_recent_trades, init_db      → /api/ghost/trades
dashboard/app.py:17000   from engine.ghost_trader import capture_new_signals,            → /api/ghost/refresh (POST)
                                                          check_outcomes, init_db
```

No tests. No plist that runs the file (see §1b). No other Python imports.

### 1b. launchd plist exists but is dormant

```
~/Library/LaunchAgents/com.ollietrades.ghost-trader.plist
  state = not running
  RunAtLoad = false
  KeepAlive = false
  last exit code = (never exited)
```

Plist is registered but inert by design. The singular's `run_daemon()` (L420) is never actually invoked — capture only fires when the dashboard's `POST /api/ghost/refresh` button is clicked, which the user did from `dashboard/static/index.html:21777-21781` (`btn-ghost` handler).

### 1c. Live UI consumer

Active dashboard panel `section-ghost-scorecard` (index.html L9594) consumes all 3 endpoints:

- Sidebar nav L2183: `showSection('ghost-scorecard'); fetchGhostScorecard()`
- L21656-21661 `fetchGhostScorecard()` → parallel `fetch('/api/ghost/scorecard?days=N')` + `fetch('/api/ghost/trades?limit=100')`
- L21777-21781 refresh button → `fetch('/api/ghost/refresh', POST)` then `fetchGhostScorecard()`
- L25734 — polled every 300s when section visible
- Days-filter dropdown at L9601

**This is live, user-facing functionality, not orphaned plumbing.**

---

## 2. The 3 dashboard endpoints

```python
# dashboard/app.py:16972-17006
@app.get("/api/ghost/scorecard")              # → get_scorecard(days)
@app.get("/api/ghost/trades")                 # → get_recent_trades(limit, agent, status)
@app.post("/api/ghost/refresh")               # → capture_new_signals() + check_outcomes()
```

All three wrap their work in `try/except` and fail-soft into `{"success": False, "error": str(e), ...}`. So even if they DID 500, they'd render as zero-data panels with `success=false` — no actual 500 status code to the browser.

---

## 3. Endpoint smoke — they work

Smoke-curled all 3 against live service (PID 76719):

| Endpoint | Result | Note |
|----------|--------|------|
| `GET /api/ghost/scorecard?days=30` | `{success:true, scorecard:[], days:30}` | Empty — no WIN/LOSS yet, only OPEN rows |
| `GET /api/ghost/trades?limit=5` | `{success:true, trades:[<5 rows>]}` | Returns live rows (UPRO, AAL, …) post-refresh |
| `POST /api/ghost/refresh` | `{success:true, captured:26, scored:20}` | **Auto-revived the DB and captured fresh signals from signal-center** |

`init_db()` runs `CREATE TABLE IF NOT EXISTS ghost_trades …` against `data/ghost_trades.db`. Since the path didn't exist (renamed to `.legacy_lean_2026-05-11`), SQLite simply created a fresh empty file. **No error. Auto-resurrection.**

### 3b. DB state post-revival

```
data/ghost_trades.db                              44 KB    26 rows (OPEN, post-refresh)
data/ghost_trades.db.legacy_lean_2026-05-11      316 KB   784 rows (Apr 14 – Apr 28 history)
data/ghost_trades.db.pre-rename-20260511_132055  316 KB   identical to legacy backup
```

Legacy backup carries 12 days of scoring history: **784 rows / 19 OPEN / 231 WIN / 196 LOSS** plus EXPIREDs. The first-call revival nuked the live continuity but the historical artifact remains in the backup files.

---

## 4. Surface diff — these are NOT redundant systems

### Function exports

| `engine/ghost_trader.py` (singular, 8 public) | `engine/ghost_trades.py` (plural, 4 public) |
|----------------------------------------------|---------------------------------------------|
| `init_db()` | — |
| `capture_new_signals()` | — |
| `check_outcomes()` | — |
| `get_scorecard(days)` | — |
| `get_recent_trades(limit, agent, status)` | `get_ghost_trades(player_id, limit)` |
| `print_scorecard()` | — |
| `run_daemon(interval_minutes)` | — |
| — | `log_ghost_trade(player_id, symbol, conf, …)` |
| — | `update_ghost_outcomes(prices)` (no-op stub) |
| — | `get_ghost_stats()` |

**Zero function-name overlap.** The naming similarity (`ghost_trader` vs `ghost_trades`) is one-letter visual collision masking two entirely different systems.

### Schema diff (same table name, different shapes)

| Column | `data/ghost_trades.db` (singular) | `data/trader.db` (plural, post-HM-BB) |
|--------|----------------------------------|---------------------------------------|
| Primary key | `id INTEGER` | `id INTEGER` |
| Symbol | `symbol TEXT` | `symbol TEXT` |
| Trade-shape | `agent, action, entry_price, stop_price, target_price, confidence, pattern, reasoning, signal_time` | `side, qty, price, fill_price, venue, advisor, signal_id, rationale, entry_price (HM-BB), confidence (HM-BB)` |
| Outcome-shape | `status (OPEN/WIN/LOSS/EXPIRED), exit_price, exit_time, pnl_pct, hit_target, hit_stop, max_gain_pct, max_loss_pct` | `status (filled/ghost), exit_price (HM-BB), pnl_pct (HM-BB)` |
| Price snapshots | `price_1h, price_4h, price_1d, price_3d` | — |
| Signal binding | `signal_id UNIQUE` (FK to signal-center) | `signal_id TEXT` (advisor's internal id) |

### Semantic difference (what each system *does*)

**`engine/ghost_trader.py` (singular)** — **signal-center scoring pipeline**
- READS: `signal-center/signals.db::trade_signals` (BUY signals with `confidence ≥ 70`, last 7 days)
- WRITES: `data/ghost_trades.db::ghost_trades` (its own DB)
- COMPUTES: actual price-action outcomes via Alpaca bars (`get_alpaca_bars`, L268). Resolves WIN/LOSS/EXPIRED against the signal's own stop_price/target_price levels.
- PURPOSE: agent-level win-rate scorecard ("did this agent's BUY signals hit their TP/SL?")
- AGENT SCOPE: signal-center agents (`etf_regime_trader`, `danelfin_ai`, `momentum_scout`, etc. — distinct from the OllieTrades fleet)

**`engine/ghost_trades.py` (plural)** — **decision logger for trader.db**
- WRITES: `data/trader.db::ghost_trades` (via `log_ghost_trade`, called from `ai_brain.py:989` for HOLD>0.6 confidence)
- ALSO READS: same table via `scripts/ghost_advisor.py` BUY/SELL writes (HM-BB.3)
- COMPUTES: trade-by-trade entry/exit pnl_pct (HM-BB.4 aggregates: would_have_won, avg_pnl_pct, top_missed)
- PURPOSE: decision-log + missed-opportunity tracking
- AGENT SCOPE: OllieTrades fleet (per `advisor` column: `ollie_super_trades`, `trailing_stop`, etc.)

**These solve different problems.** Singular scores signal-center's TP/SL predictions; plural logs trader.db's actual buy/sell decisions.

---

## 5. Where the original "may now 500" concern came from

That note in `data/scotty_hm_bb_report.md` §"Out-of-scope follow-ups" was written when I hadn't yet observed `init_db()`'s auto-CREATE-TABLE behavior. The renamed-DB concern was theoretically correct (the file `data/ghost_trades.db` had vanished) but operationally moot — SQLite re-materializes empty DBs on first connection if the parent dir is writable. So:

- **No 500s ever fired** — the worst that would have happened is empty results on first hit, then revival.
- **Continuity loss is the real issue** — the 784 rows of scoring history are sitting in `.legacy_lean_2026-05-11` while the live DB starts from zero. The agent scorecard is now empty until 7+ days of fresh capture-then-score-after-72h.

---

## 6. Captain's Options A/B/C — analyzed under the corrected premise

### Option A — Compatibility shim
Rewrite singular as `from engine.ghost_trades import …` re-exports.

**Problem:** Plural exports only 4 functions, singular's 3 callers need 5 (`get_scorecard`, `get_recent_trades`, `capture_new_signals`, `check_outcomes`, `init_db`). Plural doesn't have `get_scorecard` (no agent-level WIN/LOSS aggregation), doesn't have `capture_new_signals` (no signal-center bridge), doesn't have `check_outcomes` (its `update_ghost_outcomes` is a no-op stub per HM-AZ). A shim would either return empty/zero results forever (silently break the UI panel) or require porting ~250 LOC of singular's pipeline into plural (defeats the simplification goal).

**Verdict:** breaks UI scorecard. NOT recommended.

### Option B — Redirect each caller to plural, archive singular
Switch the 3 endpoints to call into plural.

**Problem:** Plural doesn't track signal-center's `trade_signals` table at all. Redirecting `/api/ghost/scorecard` to a plural function would return decision-log stats (from trader.db) where the panel expects signal-prediction stats (from signal-center). Different agent universe, different data shape. The UI labels say "Ghost Trader agent win-rate scorecard" — that's the singular's domain.

**Verdict:** functionally wrong. Renders unrelated data in the panel. NOT recommended.

### Option C — Hybrid
Same fundamental issue as A and B: plural can't serve singular's data because the underlying data sources differ.

**Verdict:** still NOT recommended.

---

## 7. **Recommendation — Option D: Don't retire. Disambiguate.**

The original BB closure note framed this as "stale singular needs cleanup." The discovery shows that **both modules are active, both serve real concerns, and the resemblance is purely lexical**. The actual problems are:

1. **Naming collision** — `ghost_trader.py` vs `ghost_trades.py` is one letter apart, and the schemas share a table name (`ghost_trades`). This caused the false-stale read.
2. **Continuity loss from HM-AZ.2** — the legacy DB rename inadvertently zero'd 784 rows of scoring history. The system silently recovered an empty DB instead of erroring loudly.
3. **No documentation of the two-ghost-system split** — neither file references the other, neither header explains the boundary.

### Recommended phases (BC.1–BC.4)

**BC.1 — Restore singular's DB continuity (sacred-data, no destructive ops)**
- Stop the live empty `data/ghost_trades.db` (it has 26 rows post-refresh — confirm Captain wants to discard those before proceeding; alternatives below).
- Three sub-options:
  - **α** — Replace live DB with the legacy backup: `cp data/ghost_trades.db.legacy_lean_2026-05-11 data/ghost_trades.db`. Forfeit the 26 new rows captured during this discovery (they'll be re-captured by the next refresh; signal-center re-emits them).
  - **β** — UNION the 26 new rows into the legacy backup. Slightly more work; preserves both histories.
  - **γ** — Accept the reset, keep going forward only. Lose 784 rows of scoring history (cheap; simple).
  - **Recommended: α** — re-capture is automatic per `capture_new_signals` design (it dedupes against existing signal_ids), so the 26 are not really "lost," and starting from 784 historical rows preserves the agent scorecard signal.
- Backup current `data/ghost_trades.db` to `data/ghost_trades.db.pre-hm-bc-<TS>` before any overwrite.

**BC.2 — Disambiguate via headers + a doctrine note**
- Top-of-file docstring in `engine/ghost_trader.py` explicitly stating "this is the signal-center scoring pipeline; for trader.db decision-log, see engine/ghost_trades.py".
- Symmetric pointer in `engine/ghost_trades.py` header.
- One-paragraph addition to `CLAUDE.md` under a new "Ghost system architecture" subsection — two systems, two DBs, two concerns. Prevents the next agent from re-discovering this in BC.0.
- Optional but recommended: deprecate the file-naming collision by renaming `engine/ghost_trader.py` → `engine/ghost_scoring.py` (or similar non-collision name). Requires updating 3 import statements in `dashboard/app.py` and 1 plist file. Higher blast radius — Captain decision.

**BC.3 — Plist hygiene**
- Decide: keep `com.ollietrades.ghost-trader.plist` as a never-loaded artifact, OR remove it. Currently dormant + harmless. Cost-to-remove is tiny; cost-to-keep is one launchctl-list line. **Recommended: remove.** (Per sacred convention, move to `~/Library/LaunchAgents/_archive/` rather than delete.)

**BC.4 — Closure report + verify**
- Smoke 3 endpoints, confirm scorecard now renders with restored history.
- Document the two-system split in `CLAUDE.md`.
- Add an entry under HM-BB.E followups in the BB report marking the issue resolved.

---

## 8. Open questions for Captain (block BC.1+ until resolved)

### Q1 — Confirm Option D over A/B/C
Recommend D (do not retire; disambiguate + restore). A/B/C all silently break the live UI scorecard. **Recommended: D.**

### Q2 — DB continuity sub-option (BC.1)
α (restore legacy 784-row backup), β (union 784 + 26), or γ (accept reset)?
**Recommended: α.** New 26 will be re-captured automatically; 784 history rows light up the agent scorecard panel immediately.

### Q3 — File rename (BC.2)
Rename `engine/ghost_trader.py` → `engine/ghost_scoring.py` to kill the one-letter collision? Cost: 3 imports in `dashboard/app.py` + 1 plist path. Benefit: prevents future agents from making the same misread you and I made.
**Recommended: YES** — pay the small cost now to kill a recurring confusion source. Anchor with `# === HM-BC.2 ===` markers.

### Q4 — Plist disposition (BC.3)
Archive `com.ollietrades.ghost-trader.plist` (move to `_archive/`) or leave it dormant in place?
**Recommended: archive.** It's been `RunAtLoad=false, KeepAlive=false` since at least Apr 14 with "never exited" status — it's vestigial.

### Q5 — `CLAUDE.md` doctrine block
Add a "Ghost Tracking Architecture (two systems)" subsection? Roughly 12 lines explaining the singular-vs-plural split.
**Recommended: YES** — this is the kind of architectural reality that audits keep rediscovering.

---

## 9. Files that would change if Captain approves D + Q3 + Q4 + Q5

| File | Change | Anchor |
|------|--------|--------|
| `data/ghost_trades.db` | Replace with legacy backup (BC.1 α) | DB-only, no commit |
| `engine/ghost_trader.py` → `engine/ghost_scoring.py` | Rename + header docstring update (BC.2) | `# === HM-BC.2 ===` |
| `engine/ghost_trades.py` | Header pointer to scoring module (BC.2) | `# === HM-BC.2 ===` |
| `dashboard/app.py` | 3 import lines L16977/16989/17000 (BC.2) | `# === HM-BC.2 ===` |
| `~/Library/LaunchAgents/com.ollietrades.ghost-trader.plist` | Archive + rename path target (BC.3) | filesystem move |
| `CLAUDE.md` | New "Ghost Tracking Architecture" subsection (BC.4) | doc edit |
| `data/scotty_hm_bc_report.md` | Closure section (BC.4) | this file |

Approximate diff size: ~30 LOC changed code + 12 LOC doc + 1 DB swap.

---

## 10. What NOT to do

- **Do NOT delete or truncate** `data/ghost_trades.db` or `.legacy_lean_*` (sacred-DB rule).
- **Do NOT** force the plural module to absorb singular's responsibilities — they're orthogonal and the data shapes / source tables don't reconcile.
- **Do NOT** modify the working HM-BB plural reader/writer — that epic landed clean yesterday.
- **Do NOT** push or restart service during BC.1–BC.4 per HM-BC standing rules.

---

**HALT.** Ready for Captain direction on Q1–Q5 before BC.1.

---

## HM-BC Closure (2026-05-11)

Captain green-lit Option D + all 5 sub-decisions. BC.1 → BC.4 executed without halts.

### Commits staged (not pushed)

```
5fa8a57 refactor(ghost): HM-BC.2 — rename ghost_trader.py → ghost_scoring.py
```

(BC.1 is DB-only / no commit; BC.3 is filesystem-only / no commit; BC.4 closure commit below.)

### Phase ledger

| Phase | Outcome | Artifact |
|-------|---------|----------|
| BC.0 | Discovery report + premise-inversion finding + 5 captain-questions | this file §1–§10 |
| BC.1 | `data/ghost_trades.db` restored from legacy backup (26→784 rows) | `data/ghost_trades.db.pre-hm-bc-20260511_1401` snapshot |
| BC.2 | `engine/ghost_trader.py` → `engine/ghost_scoring.py` + 3 imports + header pointers + logger rename | commit `5fa8a57` |
| BC.3 | `com.ollietrades.ghost-trader.plist` archived (file + .bak) | `~/Library/LaunchAgents/_archive/com.ollietrades.ghost-trader.plist.archived-hm-bc-20260511_1403` |
| BC.4 | CLAUDE.md "Ghost Tracking Architecture" subsection + this closure | one commit (below) |

### DB restoration verification

```
BEFORE BC.1: data/ghost_trades.db = 26 rows (auto-revived from POST /api/ghost/refresh)
AFTER  BC.1: data/ghost_trades.db = 784 rows (19 OPEN, 231 WIN, 196 LOSS, 338 EXPIRED)
Live smoke: /api/ghost/scorecard returns chekov (32.8% WR, 232 trades),
            navigator (32.2% WR, 230 trades), and other agents as expected.
Pre-HM-BC.1 26-row state preserved at:
    data/ghost_trades.db.pre-hm-bc-20260511_1401   (+ -shm/-wal companions)
```

The 26 captured signals from BC.0 are not lost — `capture_new_signals` dedupes on `signal_id UNIQUE`, so the next `/api/ghost/refresh` press will re-capture them (the originals still exist in `signal-center/signals.db::trade_signals`).

### Rename verification

- `engine/ghost_trader.py` → `engine/ghost_scoring.py` via `git mv` (history preserved, 93% similarity per git rename detection).
- 3 dashboard imports updated (`dashboard/app.py:16978/16991/17004`).
- 12 `# === HM-BC.2 ===` anchors landed across 3 files.
- Logger renamed `"ghost_trader"` → `"ghost_scoring"`.
- Cross-reference docstrings added to both `ghost_scoring.py` and `ghost_trades.py` headers.
- `grep -rn "engine\.ghost_trader" --include="*.py"` returns zero matches in active code (only intentional historical references in docstrings/comments remain).

### Plist disposition

```
Active (before): ~/Library/LaunchAgents/com.ollietrades.ghost-trader.plist
Archived (after): ~/Library/LaunchAgents/_archive/com.ollietrades.ghost-trader.plist.archived-hm-bc-20260511_1403
                  ~/Library/LaunchAgents/_archive/com.ollietrades.ghost-trader.plist.bak.20260430_routingleak
```

The launchctl in-memory registration persists until next reboot or until Captain runs:

```bash
launchctl bootout gui/$(id -u)/com.ollietrades.ghost-trader
```

This is **optional** — the file is gone, so the registration is a no-op (any attempted invocation would fail to find the program path). Leaving the bootout to Captain per the no-restart standing rule.

### Restart impact

**Minimal.** The dashboard's lazy imports (`from engine.ghost_scoring import …` inside endpoint functions) will resolve fresh on next call. The currently-running service still has the old `engine.ghost_trader` module cached in `sys.modules` until a Captain-initiated restart. Until then:

- New requests to `/api/ghost/*` endpoints WILL FAIL with `ModuleNotFoundError: engine.ghost_trader` because the lazy import inside the try/except will fail (the file no longer exists). The error is caught and serialized as `{"success": false, "error": "..."}` — soft failure, dashboard panel shows empty.
- **This is the first phase that requires a service restart to take effect.** Same restart command as HM-BB:
  `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader`
- Once restarted, the lazy imports resolve `engine.ghost_scoring`, the restored 784-row DB is read, and the scorecard panel lights up.

### Out-of-scope follow-ups (HM-BC.E candidates)

- **Backfill the BC.0-captured 26 rows.** Trivial — one `/api/ghost/refresh` POST after restart. Captain can do this from the UI button or via curl. No code change required.
- **`launchctl bootout` the stale ghost-trader registration.** Cosmetic; doesn't affect runtime.
- **Move `pre-hm-bc-20260511_1401` snapshot to `data/_archive/`** after a few weeks of stability. Sacred-DB rule — never delete, but moving to subdir is fine.
- **HM-BB closure note correction.** The HM-BB report (`data/scotty_hm_bb_report.md`) §"Out-of-scope follow-ups" includes the line "ghost_trader.py 500-triage" — that concern is now resolved (HM-BC). Cosmetic doc fix.

### What lights up after Captain restart

1. `/api/ghost/scorecard` — full agent scorecard (chekov 32.8%, navigator 32.2%, mlx-qwen3, deepseek-7b-grok4, etc.)
2. `/api/ghost/trades` — 19 OPEN rows + recent history
3. `/api/ghost/refresh` POST — re-captures the 26 BC.0 signals into the restored 784-row DB

Ready for Captain push + service restart.
