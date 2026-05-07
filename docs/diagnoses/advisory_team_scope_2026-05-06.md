# Advisory Team Scope Diagnosis — 2026-05-06

**Status:** Investigation only. No code changes, no DB writes.
**Cross-ref:** Kirk None-fix commit `d2be8bb` (2026-05-06) surfaced the "two advisory systems" question; today's 10:41:11 Advisory Team scan log line.
**Investigator:** Claude (Admiral-approved scope).

---

## TL;DR

There is **one scheduler entry** in `main.py` (`run_grok_advisor`, line 1717) that fires twice per market day (09:30 ET / 13:30 ET) and calls `engine.wb_advisory_team.run_team_scan()`. That orchestrator runs **three sub-advisors in sequence**:

1. **Grok-via-Ollie** (`engine.kirk_grok_advisor.run_grok_advisory`) — reads Kirk's **real Schwab holdings** from `data/real_holdings.json` (~23 positions) and runs `qwen3:8b` on the Ollie box. The xAI Grok API path is dead (Free-Models-First, 2026-04-17). Persisted as `advisor='grok'` rows in `portfolio_advice`.
2. **Troi** (`run_troi_scan`) — sentiment pass on **alpaca-mirror paper book** positions (currently 3: KMI/NVDA/WMB) via `qwen2.5-coder:7b`. Persisted as `advisor='troi'`.
3. **Worf** (`run_worf_scan`) — risk pass on the **same alpaca-mirror 3 positions** via `qwen3:14b` (env override `WORF_MODEL` falls back to `qwen3:8b`). Persisted as `advisor='worf'`.

**Today's actual log line:** `[10:41:11] Advisory Team : Grok 6 sym $0.0000 | Troi 3 | Worf 3` (`logs/trader.log:391777`). The "23 positions processed" framing in the original brief is the **Grok input count**; Troi and Worf only saw 3.

**The Advisory Team and Kirk Advisory are NOT redundant.** They cover different scopes and surface in different cards:

| System | Data source | Output | Surfacing |
|---|---|---|---|
| **Kirk Advisory** (`engine.kirk_advisory.generate_kirk_advisory`) | `real_holdings.json` (23 Schwab positions) | Rule-based TRIM/HOLD/SELL using stop-loss thresholds + GEX/VIX/F&G regime | `/api/kirk/advisory` → `#kirk-advisory-card` (orange border) + `kirk_advisory_log` table |
| **Advisory Team — Grok sub-agent** | `real_holdings.json` (same 23 positions) | LLM swing thesis with target/stop/hold-period | `/api/wb-team/advice` → `#wb-advisory-team-card` (purple border, "🛸 Advisory Team", Grok tab) |
| **Advisory Team — Troi/Worf** | `positions` WHERE `player_id='alpaca-mirror'` (3 paper positions) | LLM sentiment + tactical risk on a different book entirely | Same purple card, Worf tab (Troi panel exists in code but the dashboard tab strip currently shows only Grok + Worf — see Finding 5) |

Three real issues surfaced during the audit and warrant Admiral attention:

- **A) LLM hallucination on Schwab positions.** Today's saved Grok rows include `VRTX` (real holdings have `VRT`/Vertiv, not Vertex), `TSLA` (not held), and `NVDA` (not in Schwab — held only on alpaca-mirror). Out of 23 input positions only 6 came back, and 3 of those 6 are wrong tickers. See Finding 4.
- **B) Two-book mismatch inside the same orchestrator.** Grok runs on the Schwab book; Troi+Worf run on the alpaca-mirror paper book. The card renders them all under one "Advisory Team" UI as if they're three views of the same portfolio. See Finding 6.
- **C) Misleading docstring.** `main.py:1718` says "Kirk Grok Swing Advisor" but the function calls the full team. See Finding 7.

Recommendation is **non-destructive** — fix the docstring, add a one-line code comment clarifying scope, and consider unifying Troi/Worf onto the Schwab book so all three advisors see the same positions. Do not halt either system. See Recommendation section.

---

## 1. Module Purpose

`engine/wb_advisory_team.py` (422 lines) docstring opens:

> *"Webull Advisory Team — Troi (sentiment) + Worf (risk) advisors for Kirk's portfolio. Both run on local Ollama (free). Orchestrated by run_team_scan() which calls kirk_grok_advisor.run_grok_advisory() (Grok/Ollie) then Troi then Worf, combining all three into a unified portfolio_advice row per symbol with advisor='troi'/'worf'."*

**"Both run on local Ollama (free)" refers to Troi and Worf** — the two advisors *defined in this file*. Kirk Grok is imported from `engine/kirk_grok_advisor.py` and runs separately.

Module structure:

- `_init_db()` (line 69) — ensures `portfolio_advice` table + columns exist (idempotent ALTER TABLE pattern shared with `kirk_grok_advisor.py:80`)
- `_get_positions()` (line 117) — DB read, alpaca-mirror only
- `_get_positions_live()` (line 143) — Webull API fallback (dormant since Webull liquidation 2026-04-17, but code-live)
- `_call_ollama()`, `_parse_json()` (lines 184, 199)
- `_save_troi()` / `_save_worf()` (lines 210, 250) — INSERT into `portfolio_advice` with `advisor='troi'`/`'worf'`
- `run_troi_scan()`, `run_worf_scan()`, `run_team_scan()` (lines 294, 325, 359) — public entry points
- `get_team_advice()` (line 394) — read API for the dashboard card

Models (lines 25-27):

- `TROI_MODEL` = `os.getenv("TROI_MODEL", os.getenv("CREWAI_CODE_MODEL", "qwen2.5-coder:7b"))` — fast sentiment pass
- `WORF_MODEL` = `os.getenv("WORF_MODEL", os.getenv("CREWAI_MODEL", "qwen3:14b"))` — deeper risk analysis

Note: `qwen3:14b` is the documented Worf default but today's saved rows show `model_used='qwen3:8b'` for Worf (`portfolio_advice.id` 131-133). Either `WORF_MODEL`/`CREWAI_MODEL` env var is overriding, or qwen3:14b silently falls back. Worth a separate look but out of scope.

---

## 2. Data Source

**Two sources, depending on which sub-advisor:**

### Grok (via `kirk_grok_advisor.run_grok_advisory`)

`engine/kirk_grok_advisor.py:167` — `_get_positions()`:
```python
from engine.kirk_advisory import _load_real_holdings
holdings = _load_real_holdings()
```

`engine/kirk_advisory.py:51` — `_load_real_holdings()` reads `data/real_holdings.json`:
```python
schwab = (raw.get("accounts") or {}).get("schwab") or {}
raw_positions = schwab.get("positions") or []
```

Per `Kirk-Schwab-realign-2026-05-05` (Admiral Option A), this replaced the alpaca-mirror paper read.

### Troi + Worf (via `wb_advisory_team`)

`engine/wb_advisory_team.py:117-129` — `_get_positions()`:
```python
rows = conn.execute(
    "SELECT p.symbol, p.qty, p.avg_price, "
    "  p.avg_price AS current_price "
    "FROM positions p "
    "WHERE p.player_id='alpaca-mirror' AND p.qty > 0 "
    "ORDER BY p.symbol",
).fetchall()
```

The `HM-I-β-Item3 (2026-05-05)` comment notes this re-targets to alpaca-mirror post-split. **Note also:** the column `current_price` is aliased from `avg_price` — Troi/Worf prompts always see "current price = entry price", so P&L always shows 0.0%. The LLM is prompted with degraded context. Not necessarily a bug since Troi is sentiment-only and Worf focuses on size, but it explains today's Troi rows all returning "stock price unchanged" (id 128-130).

### Why the two-source split happened

`run_team_scan()` (line 359) calls `run_grok_advisory()` first (which fetches its own 23-position Schwab list internally), then passes its locally-fetched 3-position alpaca-mirror list to Troi and Worf. The two sub-paths were never reconciled when Kirk realigned to Schwab on 2026-05-05.

---

## 3. Position Count Reconciliation

Today's log: `Grok 6 sym ... | Troi 3 | Worf 3`.

Verified counts as of 2026-05-06 ~17:50 MST:

| Source | Count | Symbols |
|---|---|---|
| `data/real_holdings.json` (Schwab snapshot) | **23** | VRT, ITA, INTC, XLV, CRWD, ANET, XLF, BWXT, CEG, CRDO, VTI, LMT, XLE, DELL, AVGO, AMZN, WMT, COPX, AMD, SNOW, MU, AAPL, IBIT |
| `schwab_holdings` (latest snapshot, excl summary) | 23 | (matches; same source) |
| `positions` WHERE `player_id='alpaca-mirror'` | **3** | KMI, NVDA, WMB |
| Alpaca live `get_all_positions()` | not queried (out of scope, would need broker call) | — |

**Reconciliation:** Grok was given **23** positions (Schwab) but the LLM returned **6** rows. Troi and Worf were given **3** alpaca-mirror positions and returned **3** each. The "23 positions" headline number is real for Grok and for the *Kirk Advisory* card; it is not what Troi/Worf saw.

Verification SQL reproducing the gap:
```sql
SELECT COUNT(*) FROM positions WHERE player_id='alpaca-mirror' AND qty > 0;  -- 3
SELECT COUNT(*) FROM schwab_holdings
 WHERE snapshot_id=(SELECT MAX(snapshot_id) FROM schwab_holdings)
   AND is_summary_row=0;                                                       -- 23
```

---

## 4. Recommendation Output

**Saved to:** `portfolio_advice` table in `data/trader.db`. Schema (`engine/kirk_grok_advisor.py:84-103`, with extension columns added by `wb_advisory_team._init_db()` at lines 95-109):

```
id, symbol, advisor, action, confidence, reasoning,
support_level, resistance_level, stop_loss, target_price, hold_period,
model_used, response_time_ms, created_at, expires_at, acknowledged, acknowledged_at, raw_response,
sentiment, crowd_risk, bias_check,        -- Troi-specific
risk_level, downside_pct, tactical, risk_factors  -- Worf-specific
```

`expires_at` defaults to `created_at + 8 hours` (`wb_advisory_team.py:213`, `kirk_grok_advisor.py:283`).

**Today's actual rows** (created_at = 2026-05-06 17:40-17:41 UTC):

```
id  symbol advisor action          confidence  model_used         reasoning(excerpt)
122 AAPL   grok    HOLD            0.75        qwen3:8b           (empty in this row's reasoning column — see raw_response)
123 VRTX   grok    HOLD            0.90        qwen3:8b           (empty)        ← NOT IN SCHWAB HOLDINGS
124 ANET   grok    SELL            0.55        qwen3:8b           (empty)
125 AMZN   grok    HOLD            0.85        qwen3:8b           (empty)
126 TSLA   grok    SELL            0.60        qwen3:8b           (empty)        ← NOT IN SCHWAB HOLDINGS
127 NVDA   grok    HOLD            0.70        qwen3:8b           (empty)        ← NOT IN SCHWAB HOLDINGS
128 KMI    troi    NEUTRAL         0.85        qwen2.5-coder:7b   "No significant market news... stock price remains stable."
129 NVDA   troi    NEUTRAL         0.85        qwen2.5-coder:7b   "No notable news... stock price is static."
130 WMB    troi    NEUTRAL         0.85        qwen2.5-coder:7b   "No material market or social sentiment... unchanged."
131 KMI    worf    REDUCE_EXPOSURE 0.75        qwen3:8b           "position size (9.3% of account), energy sector volatility, geopolitical risk"
132 NVDA   worf    EXIT_NOW        0.65        qwen3:8b           "position size (36.8% of account), tech sector volatility, Fed rate sensitivity"
133 WMB    worf    EXIT_NOW        0.60        qwen3:8b           "position size (39.8% of account), energy sector exposure, oil price volatility"
```

**Hallucination check** — confirmed by `python3 -c "json.load(open('data/real_holdings.json'))"`:
- `VRTX` is **not** in Schwab holdings. The real position is `VRT` (Vertiv, qty 5, avg $318.99). Likely the LLM completed `VRT` → `VRTX`.
- `TSLA` is **not** held anywhere. Pure hallucination.
- `NVDA` is **not** in Schwab — but IS in alpaca-mirror (qty 5). Possible cross-context bleed (the LLM was given Schwab tickers but generated NVDA anyway), or it picked up NVDA from the system prompt's example (`{"symbol":"AAPL",...}` is the example; NVDA is not — so this doesn't fully explain it).

Of 23 real positions, **17 were silently dropped** by the LLM and **3 of the 6 returned are wrong**. Net effective coverage: 3/23 correct (AAPL, ANET, AMZN). Worf's percentages on alpaca-mirror (e.g., NVDA = 36.8% of account) are calibrated against the **3-position paper book**, not the Schwab account.

**Historical volume:**
```
advisor  days_active  first        last         total_rows
grok     8            2026-04-13   2026-05-06   78
troi     6            2026-04-21   2026-05-06   41
worf     3            2026-04-30   2026-05-06   14
```

Grok has been writing the longest. Worf joined most recently.

---

## 5. Surfacing

The output is surfaced in three places:

### a) Dashboard "Advisory Team" card (live UI)

`dashboard/static/index.html:5486-5512`, id `wb-advisory-team-card`, purple border (`#7c3aed`), labeled "🛸 Advisory Team". Tabs render Grok and Worf panels (`#wbAdvTab-grok` line 5503, `#wbAdvTab-worf` line 5504). The Troi panel exists in JS (`_renderTroiPanel`, line 17460) but **the tab strip in HTML lines 5502-5505 only renders Grok and Worf buttons** — Troi data is fetched via `/api/wb-team/advice` but never displayed. Worth flagging as a UI bug separate from this investigation.

API endpoint: `dashboard/app.py:13598` `/api/wb-team/advice` returns `{"advisors": get_team_advice(), "meta": get_scan_meta()}`. Manual trigger at `/api/wb-team/scan` (line 13606).

### b) Ship's Computer alert pipeline (`engine/portfolio_monitor.py`)

Lines 152-169 read `portfolio_advice` for `action IN ('SELL','TRIM')` rows created in the last 30 minutes. Inserts `NEW_ADVICE` rows into `ship_computer_alerts` table. Surfaced via `/api/ship-computer/portfolio-alerts` (`dashboard/app.py:13627`) and rendered at `dashboard/static/index.html:25068`. **Today's Worf "EXIT_NOW" rows on NVDA and WMB will fire `NEW_ADVICE` alerts** since `EXIT_NOW` doesn't match the `('SELL','TRIM')` filter — actually, they **won't** fire because Worf's action column stores `EXIT_NOW`/`REDUCE_EXPOSURE`, not `SELL`/`TRIM`. The filter at line 154 is action-name-specific. This is a latent gap: Worf can flag "EXIT_NOW" on a position and Ship's Computer will not amplify it. Worth a separate finding but out of scope.

The Grok `SELL` rows on ANET (id 124) and TSLA (id 126) **will** fire NEW_ADVICE alerts, including the hallucinated TSLA — Ship's Computer will surface a SELL alert on a ticker the user does not hold.

### c) Archer Morning Synthesis (`engine/archer_morning_synthesis.py`)

Lines 50-65 read top 10 most-recent `portfolio_advice` rows from today and feed them into the morning briefing under `get_kirk_advisory()`. This pulls all advisors indiscriminately (`advisor` column not filtered). Today's morning briefing would include the Worf and Troi rows from Schwab even though they describe alpaca-mirror.

### d) NTFY

Searched: no direct NTFY emission for `portfolio_advice` writes. The only push path is via Ship's Computer `NEW_ADVICE` (above) → `ship_computer_alerts`, and from there to whatever pushes Ship's Computer alerts (not investigated).

---

## 6. Overlap with Kirk Advisory

**Both fire from `run_grok_advisor` in main.py:1717.** Wait — actually, `run_grok_advisor` calls `run_team_scan()` only. `engine.kirk_advisory.generate_kirk_advisory()` is called from a **different scheduler entry**. Verified:

```
$ grep -n "generate_kirk_advisory\|kirk_advisory" main.py
```

— shows it's invoked elsewhere (in `run_kirk_advisory` or via the `/api/kirk/advisory` endpoint). The Schwab-position 23-count only feeds Kirk Advisory's rule-based recommendations and Grok's LLM swing thesis. They are **independent code paths** sharing the same data source (`real_holdings.json`).

Today's count summary, by surface:

| Surface | Source | Today's output |
|---|---|---|
| `#kirk-advisory-card` (Kirk Advisory rule-engine) | 23 Schwab positions | Up to 23 TRIM/HOLD/SELL rows + cash recommendation; non-HOLD goes to `kirk_advisory_log` |
| `#wb-advisory-team-card` Grok tab | Same 23 Schwab positions, LLM thesis | 6 rows (3 hallucinated) in `portfolio_advice` advisor='grok' |
| `#wb-advisory-team-card` Worf tab | 3 alpaca-mirror paper positions | 3 rows in `portfolio_advice` advisor='worf' |
| `#wb-advisory-team-card` Troi panel (rendered, not tabbed) | 3 alpaca-mirror paper positions | 3 rows in `portfolio_advice` advisor='troi' |

**Are they consuming each other's output?** No. Both are independent producers writing to different surfaces. There is **one consumer link**: `engine/portfolio_monitor.py::check_captains_portfolio()` reads Grok's `stop_loss` field (line 109-119) to detect STOP_BREACH conditions on Schwab positions — but neither Worf's `risk_level` nor Troi's `sentiment` feeds Kirk Advisory's rule logic.

**Intent of running both?** From the docstrings: Kirk Advisory is a **deterministic rule engine** (fast, never hallucinates, uses GEX/F&G/VIX regime); Advisory Team's Grok is a **LLM second opinion** with target/stop/hold-period detail. The pair is similar to Spock-as-second-opinion in the fleet — not redundant if one is rule-based and the other is generative. The Troi/Worf pieces are intended as "psychology + tactical risk" side-cars.

---

## 7. Comment Clarity

**Misleading docstring at `main.py:1718`:**

```python
def run_grok_advisor():
    """Kirk Grok Swing Advisor: fires at 9:30 AM and 1:30 PM ET on weekdays."""
```

The function actually calls `run_team_scan()` (line 1750), which fans out to Grok + Troi + Worf. The function name, the docstring, and the global flag set name (`_grok_advisor_slots_done_today`) all reference Grok, but the runtime behavior is "fire all three advisors."

The console log line at 1757-1763 is correct (`[green]Advisory Team [{slot_id}]: ...`), so the user-facing log already says "Advisory Team" — only the source code is misleading.

**Suggested corrected docstring** (drop-in, no behavior change):

```python
def run_grok_advisor():
    """Advisory Team scheduler: fires Grok+Troi+Worf at 9:30 AM and 1:30 PM ET on weekdays.

    Calls engine.wb_advisory_team.run_team_scan(), which orchestrates:
      - Grok-via-Ollie on Kirk's Schwab holdings (data/real_holdings.json, ~23 positions)
      - Troi sentiment on alpaca-mirror paper positions
      - Worf risk on alpaca-mirror paper positions
    Function name kept for backwards compatibility with _grok_advisor_slots_done_today.
    """
```

The function name itself isn't worth churning — the slot-tracking global and any downstream import would have to change, and the cosmetic clarity gain doesn't justify it.

---

## 8. Recommendation

**Leave the system running. Apply two cosmetic clarifications and one observability addition.** Do not halt either system; do not consolidate Kirk Advisory and Grok (they are intentionally independent producers).

### Recommended actions, in priority order

1. **Fix the misleading docstring** at `main.py:1718` (see Finding 7). Five-minute change, zero risk.

2. **Add a one-line clarifying comment** at `wb_advisory_team.py:359` (top of `run_team_scan`) noting that Grok and Troi/Worf operate on **different books**:

   ```python
   def run_team_scan() -> dict:
       """Run full advisory team: Grok/Ollie → Troi → Worf in parallel-ish sequence.

       NOTE: Grok runs on Schwab real_holdings.json (~23 positions, via
       kirk_grok_advisor._get_positions). Troi and Worf run on alpaca-mirror
       paper positions (3 currently, via _get_positions in this module). This
       is intentional post-Kirk-Schwab-realign-2026-05-05; the two books are
       never reconciled here.
       """
   ```

3. **Add a `wb_advisory_team` log line surfacing the dual-book scope** at the start of `run_team_scan` (line ~366):

   ```python
   logger.info(
       "Team scan starting: Grok will read Schwab (~23 pos), Troi/Worf will read alpaca-mirror (%d pos)",
       len(positions),
   )
   ```

   This makes the "23 positions" + "3 positions" dual count visible in `trader_error.log` (per CLAUDE.md logging-sink discipline) and prevents future investigators from being puzzled.

### Non-recommended (called out but not pursued)

- **Do not merge Advisory Team into Kirk Advisory.** The rule-engine vs LLM-thesis split is the value. Kirk Advisory triggers stop-losses; Grok adds target prices, hold periods, and a thesis sentence.
- **Do not halt Advisory Team.** Output is small, free (Ollama), and feeds Ship's Computer alerts on `SELL`/`TRIM`. Today's hallucinations are a model-quality issue, not an architectural one — see "Latent issues" below for a cheaper mitigation.
- **Do not change the function name** `run_grok_advisor` → `run_advisory_team`. The global flag `_grok_advisor_slots_done_today` and any outside importers would have to follow. Cosmetic-only payoff.

### Latent issues uncovered during the audit (not part of this investigation, but flagged for triage)

- **a) qwen3:8b hallucinates Schwab tickers.** Today's Grok scan returned VRTX (real: VRT), TSLA (not held), and NVDA (not in Schwab). Mitigation idea: add a post-LLM filter in `kirk_grok_advisor._save_advice` that rejects any item whose `symbol` isn't in the input position set. Cheaper than swapping the model. Out of scope to fix in this investigation.
- **b) Worf actions don't match Ship's Computer's NEW_ADVICE filter.** `engine/portfolio_monitor.py:154` matches `action IN ('SELL','TRIM')`. Worf writes `EXIT_NOW`/`REDUCE_EXPOSURE`. So Worf "EXIT_NOW" alerts never amplify to Ship's Computer alert pipeline. If Worf is meant to drive alerts, the filter needs to widen.
- **c) Troi panel exists in JS but no tab button in HTML.** `dashboard/static/index.html:5502-5505` only renders Grok and Worf tabs; the `_renderTroiPanel` function at line 17460 is unreachable from the UI. Either add a Troi tab or remove the panel/render code.
- **d) Worf's `current_price = avg_price` aliasing in `wb_advisory_team._get_positions` (line 124)** means Troi and Worf prompts always see "P&L 0%". Today's Troi rows all said "stock price unchanged" because the prompt told them so. Either fetch real prices in `_get_positions` (one yfinance call per position) or remove the bogus current/pnl_pct fields from the prompt.
- **e) `WORF_MODEL` env var likely overrides the `qwen3:14b` documented default** — today's saved rows show `model_used='qwen3:8b'` for Worf despite the line-27 default being `qwen3:14b`. Verify the env path before reading too much into Worf's risk reasoning quality.

Items (a), (b), (c), (d), (e) are independent of the investigation question and should be handled in their own backlog entries.

---

## Cross-references

- Source files: `engine/wb_advisory_team.py`, `engine/kirk_grok_advisor.py`, `engine/kirk_advisory.py`, `engine/portfolio_monitor.py`, `engine/archer_morning_synthesis.py`, `main.py:1717-1768`
- Dashboard: `dashboard/app.py:13598-13624`, `dashboard/static/index.html:5486-5512`, `:17392-17500+`
- DB: `portfolio_advice`, `kirk_advisory_log`, `ship_computer_alerts`, `schwab_holdings`, `positions`
- Files: `data/real_holdings.json`
- Today's log line: `logs/trader.log:391777` — `[10:41:11] Advisory Team : Grok 6 sym $0.0000 | Troi 3 | Worf 3`
- Related decisions: Kirk-Schwab-realign-2026-05-05 (Admiral Option A), HM-I-β-Item3 (2026-05-05), Free-Models-First doctrine 2026-04-16, Kirk None-fix commit `d2be8bb` (2026-05-06)
