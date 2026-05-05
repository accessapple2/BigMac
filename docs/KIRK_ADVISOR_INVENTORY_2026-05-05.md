# Kirk Advisor Inventory — 2026-05-05

*Read-only inventory to inform Admiral's identity decision. No fixes applied.*

---

## Stated mission (Admiral, 2026-05-05)

> "We want Kirk to comment, recommend, advise on real holdings, as a contrast to Grok."

---

## Where Kirk lives

| Path | Role | Status |
|---|---|---|
| `engine/kirk_advisory.py` | Deterministic rules advisor (GEX × VIX × F&G × P/L) | **ACTIVE** |
| `engine/kirk_grok_advisor.py` | LLM swing advisor (Grok-mode → falls back to Ollama qwen3:8b) | **ACTIVE** |
| `logs/kirk.log` | 0 bytes since 2026-04-23 — orphan file | dormant |
| `archive/retired/2026-05-04-kirk-swing-desk/agents__kirk.py` | Old swing-desk Kirk (different agent) | RETIRED 2026-05-04 |
| `engine/providers/grok_provider.py` | Grok API provider (xAI) | unrelated to advisor |
| `scripts/import_schwab_csv.py:309` | Importer comment claims `real_holdings.json` is "Kirk Advisory data source" | **stale comment / mismatch** |

The currently active "Kirk" is the pair `engine/kirk_advisory.py` + `engine/kirk_grok_advisor.py`. The agent-level Kirk (`agents/kirk.py` Holly Swing Advisor) was archived yesterday per CLAUDE.md and is unrelated.

---

## What Kirk reads today

### `engine/kirk_advisory.py` (deterministic)

```python
# line 24
PLAYER_ID = "alpaca-mirror"
```

Sources, in `generate_kirk_advisory()`:

1. **Positions** — `paper_trader.get_portfolio('alpaca-mirror')`, optionally pre-empted by `settings.webull_positions_cache` (legacy live-Webull JSON cache; likely stale since 2026-04-17 Webull liquidation).
2. **Cash** — priority order: env `KIRK_PORTFOLIO_CASH` → `settings.webull_synced_value` → `ai_players.cash WHERE id='alpaca-mirror'`.
3. **Trade history** — `trades WHERE player_id='webull'` (Steve's 127 historical Webull trades). Used only as a "your win rate is X%" footer line — context anchor.
4. **Market data** — `gex_calculator.get_latest_snapshot('SPY')`, `engine.fear_greed`, `engine.vix_monitor`.
5. **Backtest context** — `backtest_results` table.
6. **Rebalance overlay** — `rebalance_recs` table (TRIM/SELL only, last 20).

### `engine/kirk_grok_advisor.py` (LLM-mode)

Lines 165–171:
```sql
SELECT p.symbol, p.qty, p.avg_price, p.avg_price AS current_price
FROM positions p
WHERE p.player_id='alpaca-mirror' AND p.qty > 0
ORDER BY p.symbol
```

`PLAYER_ID = "kirk-grok-advisor"` is the api-cost ledger key (separate). Position source is the same `alpaca-mirror` row set.

### What Kirk **does not** read

- `data/real_holdings.json` — the manually-maintained Schwab snapshot (23 positions, $22k notional, cash $2,220.77). Zero references in either Kirk module.
- Schwab CSVs in `data/schwab_csv_archive/`, `data/schwab_imports/`.
- Any `schwab` table or scratch payload.

---

## What Kirk produces

### Deterministic mode (`kirk_advisory.py`)

- Returns dict: `{positions: [recommendations], cash, cash_recommendation, market_context, trade_history, rebalance_conflicts, generated_at}`.
- Per-position fields: `symbol, qty, entry, current, pnl_pct, value, action ∈ {HOLD,TRIM,SELL,ADD}, reasoning, urgency ∈ {low,medium,high,critical}`.
- **Persists** non-HOLD recommendations to `kirk_advisory_log` table with 30-min dedup.
- **No NTFY** in this module; surfaces consumed via dashboard.

### LLM mode (`kirk_grok_advisor.py`)

- Calls xAI API or Ollama fallback (`qwen3:8b`); per Free-Models-First (commit `a1b1fef`, 2026-04-17), **`use_ollama = True` unconditionally** — Grok API is dead code.
- Persists to `portfolio_advice` table (8h TTL, `advisor='grok'`).
- Per-position fields: `action, confidence, reasoning, support_level, resistance_level, stop_loss, target_price, hold_period`.
- Cost log: `api_costs` row written even on Ollama path (cost=0).
- **No NTFY** in this module either.

---

## How Kirk is invoked

`main.py:2592`:
```python
schedule.every(30).minutes.do(run_grok_advisor)
```

Wrapper at `main.py:1717`:
```python
def run_grok_advisor():
    """Kirk Grok Swing Advisor: fires at 9:30 AM and 1:30 PM ET on weekdays."""
```

Despite the 30-min cadence, internal slot-id gating (`_grok_advisor_slots_done_today`) restricts to 09:30 ET and 13:30 ET. Calls into `wb_advisory_team` (Grok-mode + Troi + Worf trio) which itself reads alpaca-mirror positions.

`engine/kirk_advisory.py::generate_kirk_advisory()` is invoked from the dashboard endpoint path (not a separate scheduler); rendered into the dashboard advisory pane and writes log rows on demand.

---

## Today's re-target — what changed

Commit `5186408` (HM-I-β-Item3, 2026-05-05 08:06 MST), per the message:

> Kirk re-targets to its actual data source — was advising on Alpaca paper data for ~6 weeks under the wrong label.

Key context: pre-split, the `webull` player_id was double-duty — both Steve's human-Webull import benchmark AND Alpaca-paper-sync target. `shared/alpaca_portfolio_sync.py` was overwriting the human-imported positions every cycle. So the `webull` rows Kirk read for ~6 weeks were *already* alpaca-paper-sourced. Today's commit re-labelled what was always there; it did **not** change the paper-vs-real character of Kirk's data.

Kirk has been advising on paper data since at least 2026-04-17 (Webull liquidation date). The Webull-language docstring at line 1 of `kirk_advisory.py` and the prompt header `"Kirk's Webull swing portfolio (~$6,500 account)"` at `kirk_grok_advisor.py:330` are stale — the $6,500 figure is the original real-Webull starting balance, not what alpaca-mirror reflects today (~$94k cash + 4 positions per `alpaca_portfolio_sync` 08:04:44 forced run).

---

## How Grok (the contrast) compares

The Admiral's "Grok" referent is ambiguous — there is no standalone external Grok module producing real-holdings advice. Three candidates:

| Candidate | What it is | Reads | Distinct from Kirk? |
|---|---|---|---|
| `engine/kirk_grok_advisor.py` | Kirk's LLM mode (Ollama qwen3:8b, ex-Grok) | alpaca-mirror | No — same module pair as Kirk |
| `qwen3-14b-grok3` / `deepseek-7b-grok4` (main.py:115) | War-room conversational players (free Ollama) | war_room context | Yes — chatter, not advisory |
| `cto-grok42` advisory (main.py:3863, "CTO Advisory armed (Grok 4.2 — 4x daily)") | CTO advisor module | reads alpaca-mirror per `engine/cto_advisor.py` (today's commit also re-targeted it) | Yes — but ALSO reads alpaca-mirror |

**Net:** every "Grok" in the live code now reads alpaca-mirror after today's split. None reads `real_holdings.json`. There is no real-vs-paper contrast surface today; the contrast the Admiral named does not exist in the code.

---

## Recent activity

### `kirk_advisory_log` (deterministic non-HOLD writes, last 7 days)

```
2026-05-01  4 rows  BWXT,CRWD,MU,PLTR
2026-04-29  3 rows  KMI
```

No log rows since 2026-05-01. Either Kirk emitted only HOLDs (logged as nothing) or the run path errored.

### `portfolio_advice` (LLM mode output, last 7 days)

```
2026-05-01  qwen3:8b         CSCO,KMI,NVDA,ORCL,WMB     (10 rows)
2026-05-01  qwen2.5-coder:7b CSCO,KMI,NVDA,ORCL,WMB     (5 rows)
2026-04-30  qwen2.5-coder:7b CSCO,KMI,NVDA,ORCL,TSLA,WMB (12)
2026-04-30  qwen3:8b         CSCO,KMI,NVDA,ORCL,TSLA,WMB (12)
2026-04-28  qwen3:14b        KMI,NVDA,ORCL,QQQ,TSLA,WMB  (6)
2026-04-28  qwen2.5-coder:7b KMI,NVDA,ORCL,QQQ,TSLA,WMB  (6)
```

No `portfolio_advice` rows since 2026-05-01. The 09:30 / 13:30 ET slots may have failed today and yesterday — gate-flip soak distractions, or quiet Ollama 5xx.

### `trader_error.log` — `[LRS] Kirk advisory error`

```
07:29:27  '<' not supported between instances of 'NoneType' and 'int'
10:51:20  '<' not supported between instances of 'NoneType' and 'int'
10:53:12  '<' not supported between instances of 'NoneType' and 'int'
09:03:27  '<' not supported between instances of 'NoneType' and 'int'
```

Recurring `NoneType < int` blowup in `kirk_advisory.py`. Bare `except Exception as e: logger.error(f"Kirk advisory error: {e}")` at line 382 — pre-HM-U posture, no `type(e).__name__` enrichment, no NTFY. Likely cause: a position with `current_price=None` (option contract — see "Surprising finding" below) compared against a numeric threshold (`STOP_LOSS_PCT=-8.0` or similar). Guard at line 232–243 only fires when `entry <= 0`, not when `current` is `None`.

---

## Mission alignment verdict — **MISMATCH**

| Dimension | Stated mission | Kirk reality (post-5186408) |
|---|---|---|
| Account type | "real holdings" | Alpaca paper |
| Data source | `data/real_holdings.json` (Schwab) | `positions WHERE player_id='alpaca-mirror'` |
| Position count | 23 (Schwab) | 4 (alpaca-mirror: KMI 18, NVDA 12.34, WMB 35, SPY260515P00719000 3) |
| Notional | ~$22k Schwab + $2.2k cash | $94k Alpaca paper cash + ~$8k positions |
| Symbol overlap with Schwab | by mission, all 23 | KMI, NVDA shared (paper-sync coincidentally tracks them); WMB paper-only; SPY put paper-only |
| Contrast vs "Grok" | by mission, real-vs-LLM | both Kirk and the candidate Grok modules read the same alpaca-mirror book |

Today's commit `5186408` was technically correct — it relabelled an already-paper data source from `webull` to `alpaca-mirror`. The commit was not a misdirection; it was a relabel. **However, it surfaces the deeper mismatch:** Kirk has been a paper-portfolio advisor under "Webull commentary" branding since the 2026-04-17 Webull liquidation, and the Admiral's stated mission was never reconciled to that drift. The named contrast against Grok also does not currently exist in code.

---

## Most surprising finding

**Kirk's 4th alpaca-mirror "position" is an option contract mislabeled as `asset_type='stock'`:**

```
SPY260515P00719000  qty=3  avg_price=4.4488  asset_type=stock
```

This is the OCC symbol for a SPY 2026-05-15 $719 PUT — 3 contracts, $4.4488 premium. The deterministic Kirk applies stock heuristics (`qty * current_price`, `(current - entry)/entry × 100`) to this row, which produces nonsensical "DOWN 80% — hit stop loss" advice the moment the put decays toward zero. This row is plausibly the source of the recurring `'NoneType' < int` errors in `trader_error.log` if the option's spot lookup via `get_stock_price(symbol)` returns `None`.

Two-bug stack: (a) `alpaca_portfolio_sync` writing options into the `positions` table with `asset_type='stock'`, and (b) Kirk's per-position loop has no asset-type guard. Neither is in scope for this inventory; both surface as natural follow-ups whichever option the Admiral picks below.

---

## Options for the Admiral

### Option A — Re-target Kirk to `data/real_holdings.json` (Schwab, real)

- **Effort:** ~30–45 min Scotty session — replace `_get_positions()` and `generate_kirk_advisory()` data source; update docstrings; decide cash source.
- **Effect:** Kirk advises on the 23 Schwab holdings. Mission alignment ✅.
- **Contrast vs Grok:** weak today (no Grok-on-real-holdings exists). Would need a paired decision: does Grok-mode (`kirk_grok_advisor.py`) also re-target to Schwab, or stay paper, creating an explicit real-vs-paper contrast inside the Kirk pair?
- **Risks:** `real_holdings.json` is manually maintained — Kirk would advise on data as fresh as the last manual sync (2026-05-04 12:15 snapshot today). Stale advice is a real failure mode. Today's HM-I-β Item 3 work would need a followup hunk for both Kirk modules; minimal but the commit-marker pattern needs to extend.

### Option B — Keep Kirk on `alpaca-mirror`, redefine the mission

- **Effort:** zero code; documentation only — update Kirk's docstrings to "Alpaca paper book swing advisor"; remove "$6,500 Webull" prompt language; CLAUDE.md note.
- **Effect:** Kirk continues as a paper-portfolio advisor. Mission alignment ❌ vs Admiral statement; ✅ vs current behavior.
- **Contrast vs Grok:** still the deterministic-vs-LLM pair inside Kirk modules (kirk_advisory.py vs kirk_grok_advisor.py), which is real but small.
- **Risks:** drifts further from the Admiral's stated intent; the SPY-put mislabel bug still bites.

### Option C — Make Kirk read both (Schwab real + Alpaca paper, cross-portfolio synthesis)

- **Effort:** ~1–1.5 hr — add Schwab loader, restructure output to two sections, consider per-section cash recommendations.
- **Effect:** Kirk produces a real-money column and a paper column with cross-commentary ("your real Schwab is heavy in defense; the paper book is testing thematic XLF — overlap risk if you scale up").
- **Contrast vs Grok:** Grok could remain paper-only, giving an explicit two-axis contrast (Kirk: real+paper synthesis vs Grok: paper-only LLM).
- **Risks:** scope creep; harder Grok-contrast to explain to humans; Schwab data freshness still bounded by manual sync.

### Option D — Defer; do nothing today

- **Effort:** zero.
- **Effect:** today's split holds; Kirk continues advising on alpaca-mirror; the SPY-put bug persists; the dashboard label says "BROKER BOOK (Alpaca paper)" which is at least truthful.
- **Risks:** Kirk's docstring still claims "Webull"; the trader_error.log error is recurring; the Admiral's stated mission and the code stay out of sync indefinitely.

---

## What I (Scotty) deliberately did NOT do

- Did not change Kirk's data source.
- Did not patch the `current_price=None` guard in `kirk_advisory.py` — even though the recurring trader_error.log error has an obvious 1-line fix.
- Did not address the OCC-symbol-stored-with-`asset_type=stock` upstream defect in `alpaca_portfolio_sync` — this is the deeper bug; out of scope for this inventory.
- Did not modify any Kirk module, docstring, or CLAUDE.md.
- Did not pick A/B/C/D.

The Admiral reads this doc, picks an option (or hybrid), implementation lands as a separate session prompt.

---

## References

- Today's split: commit `5186408` ("HM-I-β-Item3: webull dual-role split — webull (human) + alpaca-mirror (broker)")
- HM-I-β architecture decision: commit `086a123`, CLAUDE.md "Architecture: Two-Book Bridge Policy"
- Free-Models-First Grok retirement: commit `a1b1fef` (2026-04-17), CLAUDE.md "Free Models First"
- Kirk swing-desk archive: 2026-05-04 retirement at `archive/retired/2026-05-04-kirk-swing-desk/`
- Real holdings format: `data/real_holdings.json` v2026-05-04
