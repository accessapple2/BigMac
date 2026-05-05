# Reconciliation Drift Investigation — 2026-05-05
*Read-only investigation following Item 5 canary's first fire at 13:30 MST. Admiral picks remediations.*

## Question
Item 5 reconciliation canary surfaced 4 findings at 13:30 MST. Are they real drift, expected coincidence, or canary bugs?

## Headline findings

| Thread | Finding | Classification |
|---|---|---|
| **A — ONDS** | dalio-metals (route_mode=`tracking`) holds ONDS short — by-design never forwards to Alpaca | 🟡 **Canary semantics issue** — routed_drift treats all ROUTED_PLAYERS as if they trade-route; doesn't distinguish tracking-mode |
| **B — SPY 719 leg** | alpaca-mirror's `positions` table is stale from 12:56:31 sync (~33 min before reconciliation); live Alpaca at 13:45 had only 3 positions, no 719 leg | 🟡 **Sync staleness, not architectural drift** — alpaca_portfolio_sync didn't refresh between 12:56 and 13:30 |
| **C — 4 unrouted overlap** | All 4 active-fleet players hold NVDA in their own paper books; Alpaca holds NVDA via webull/alpaca-mirror sync; canary counts shared-symbol matches | 🟡 **Shared-symbol coincidence, not architectural leak** — canary `compute_unrouted_drift` counts SYMBOL match, not order-forward |
| **D — HM-V NTFY missing from trader.log** | Reconciliation drift NTFY DID fire at 13:30:02 (HTTP 200 to ollietrades-admin); logger output goes to `trader_error.log`, not `trader.log` | 🟢 **False alarm — NTFY plumbing works perfectly** — search-the-wrong-file mistake |

**Net:** zero real architectural drift. 3 canary-semantics findings + 1 logging-sink-discovery. The canary is too strict in 2 of 3 drift categories; that's adjustable.

---

## Thread A — ONDS drift

### Finding
Two players hold ONDS short:

| Player | qty | avg_price | opened |
|---|---:|---:|---|
| **dalio-metals** | -76.514 | 7.88 | 2026-03-30 12:16 |
| gemini-2.5-flash | -58.8948 | 8.80 | 2026-03-30 08:24 |

dalio-metals is the one that triggered routed_drift. Its route resolution: `portfolio='Enterprise Computer' route_mode=tracking`. Per HM-I-β policy (CLAUDE.md), tracking-mode players **never forward to Alpaca by design** — they log-only via `_log_signal_only()`. So Alpaca correctly doesn't have ONDS.

### Classification
🟡 **Canary semantics issue.** The canary's `ROUTED_PLAYERS` frozenset includes all 5 routed players (super-agent, ollie-auto, neo-matrix, dalio-metals, alpaca-mirror) but doesn't sub-classify by `route_mode`:
- `super-agent`, `ollie-auto`, `neo-matrix` → `route_mode=trading` (forward to Alpaca; drift here = real)
- `dalio-metals` → `route_mode=tracking` (log-only; drift here = expected by design)
- `alpaca-mirror` → broker mirror (drift here = sync staleness, see Thread B)

The canary should either (a) exclude dalio-metals from routed_drift, or (b) split ROUTED_PLAYERS into ROUTED_TRADING vs ROUTED_TRACKING vs ROUTED_MIRROR with separate drift logic per category.

### Recommended next step
Filter routed_drift to `route_mode=trading` players only. ~5-line fix in `engine/reconciliation.py::compute_routed_drift`. dalio-metals' ONDS short stops triggering false-positive drift.

---

## Thread B — SPY260515P00719000 leg drift

### Finding
The 719 leg was held on **alpaca-mirror** with `qty=3.0, asset_type='stock', opened_at=2026-05-05 12:56:31` — that's the most recent alpaca_portfolio_sync run (per `settings.last_alpaca_sync_label = '12:56 PM AZ'`).

At canary time (13:30), live Alpaca had **only 3 positions** (KMI, NVDA, WMB). The 719 leg disappeared between 12:56 and 13:30. **alpaca_portfolio_sync didn't fire again to refresh alpaca-mirror's snapshot.** Per `settings.last_alpaca_full_sync = 2026-05-05T19:56:31` (UTC = 12:56 MST), the last sync was ~50 min before reconciliation.

The 719 strike appears in 9+ rows of `options_trades` as the LONG leg of bull_put_spread entries (rows 17/18/19/21/22/23/24/26 etc., all status='open' with broker_order_id populated). At 12:56 the broker was netting these into 3 contracts of long-719P; by 13:30 some action (close fills, exercise, internal Alpaca rebalance) reduced it to zero net.

### Classification
🟡 **Sync staleness, not architectural drift.** alpaca-mirror's positions table is a snapshot, not live. The canary correctly identifies the snapshot-vs-live divergence but the root cause is the sync not running every 5 min as the scheduler intends.

### Why didn't sync run?
`shared/alpaca_portfolio_sync.py::run_full_alpaca_sync` has a `_required_interval()` gate keyed off `_market_session()`. The sync probably gates more heavily during market hours than the 5-min schedule cadence implies. Likely the gate held it off after 12:56 due to session detection.

Worth a separate session to confirm — this could affect alpaca-mirror staleness across many downstream consumers (Kirk advisor, first_officer, dashboard panels), not just the canary.

### Recommended next step
**Two options:**
- (1) Force `alpaca_portfolio_sync` to run within ~1 min of reconciliation (chained schedule, or reconciliation invokes sync first then reads).
- (2) Have reconciliation read directly from live Alpaca (`alpaca.positions()`) instead of from the alpaca-mirror snapshot. Reduces dependency on sync cadence.

Both are small fixes. Option 2 is architecturally cleaner since reconciliation's job is comparing internal vs broker reality.

Worth noting: HM-V will catch close-side fills via `hm-v-spread-close-{strategy}` once HM-V loads (next restart) — that surfaces close events at the source rather than depending on sync-then-canary detection.

---

## Thread C — 4 unrouted player Alpaca overlap

### Finding
The canary reports `unrouted_drift = {ollama-qwen3: 1, qwen3-8b-flash: 1, deepseek-7b-grok4: 1, ollama-plutus: 1}`.

Each player's open positions:
| Player | Positions |
|---|---|
| ollama-qwen3 | AAPL, NOW, **NVDA**, PLTR, TSLA |
| qwen3-8b-flash | AAPL, GOOGL, META, NOW, **NVDA**, ORCL, TSLA |
| deepseek-7b-grok4 | META, NOW, **NVDA** |
| ollama-plutus | AAPL, AMD, META, MSFT, NOW, **NVDA** |

Live Alpaca holds: KMI, **NVDA**, WMB.

The single overlap is **NVDA in all 4 cases**. Each player independently bought NVDA in their own paper book (popular ticker; alpha signals frequently flag it). Alpaca holds NVDA because of Steve's historical Webull NVDA holding + alpaca-mirror sync target.

### Classification
🟡 **Shared-symbol coincidence, NOT architectural leak.** Per `compute_unrouted_drift` source:

```python
overlap = sum(
    1 for p in positions
    if p.get("asset_type") == "stock" and p["symbol"] in alpaca_syms
)
```

This counts SYMBOL matches, not orders forwarded. The two-book policy (CLAUDE.md) forbids forwarding, not shared symbols. None of these 4 players' NVDA holdings reached Alpaca via forwarding — verified by the routing-table check (`_EXECUTION_PORTFOLIO_BY_PLAYER` only has 4 entries: super-agent, ollie-auto, neo-matrix, dalio-metals).

### Recommended next step
Reframe `compute_unrouted_drift` to detect **forwarded orders**, not symbol overlaps. One way:
```python
# Look for trades with broker_order_id populated under non-routed player_ids
SELECT player_id, COUNT(*) FROM trades
 WHERE alpaca_order_id IS NOT NULL AND alpaca_order_id != ''
   AND player_id NOT IN (routed players)
   AND player_id != 'webull'
 GROUP BY player_id;
```

Today this query returns zero (no tracked Alpaca order IDs in `trades` table — the field exists but isn't populated by any caller). So either the existing logic is correct in spirit but the docstring overpromises, OR alpaca_order_id needs to be populated to enable proper drift detection.

Lower-effort fix: rewrite the docstring to clarify "overlap" means "shared symbol", not drift, and either drop the metric or rename it.

---

## Thread D — HM-V NTFY verification

### Finding
**NTFY plumbing works perfectly.** The "missing from trader.log" observation was a search-the-wrong-file mistake.

Evidence from `logs/trader_error.log`:

```
11:45:14 [LRS] ntfy sent [200]: ?? TradeMinds Warning
11:45:14 [LRS] Alert dispatched [warning/hm-u-submit_single-APIError]: submit_single_option APIError: APIError('{"code":40310000,...
12:13:17 [LRS] ntfy sent [200]: ?? TradeMinds Warning
12:13:17 [LRS] Alert dispatched [warning/hm-u-submit_single-APIError]: ...
12:40:14 [LRS] ntfy sent [200]: ?? TradeMinds Warning
12:40:14 [LRS] Alert dispatched [warning/hm-u-submit_single-APIError]: ...
12:48:22 [LRS] ntfy sent [200]: ?? TradeMinds Warning
12:48:22 [LRS] Alert dispatched [warning/hm-u-close_position-APIError]: close_options_position SPY260515P00718000 APIError: ...
13:30:02 [LRS] ntfy sent [200]: ?? TradeMinds Warning
13:30:02 [LRS] Alert dispatched [warning/hm-i-b-item5-drift-2026-05-05]: Daily reconciliation drift detected (2026-05-05): 2 routed positions in internal book not on Alpaca; 4 non-routed players with Alpaca overlap: [...]
```

5 NTFY events fired today, all returning HTTP 200 from ntfy.sh:
1. **11:45:14** — HM-U fired post-Item-3-restart for submit_single APIError flood
2. **12:13:17** — HM-U after second restart (HM-AB ground)
3. **12:40:14** — HM-U after HM-AC Option B restart
4. **12:48:22** — HM-U fired for close_options_position SPY260515P00718000 APIError (separate code path; same root cause as HM-AC)
5. **13:30:02** — **Reconciliation drift NTFY** ✅ — first occurrence per day per `alert_type`, succeeded

### Why no entries in trader.log
- `console.log(...)` (rich Console) → stdout → captured to `logs/trader.log`
- `logger.info(...)` (Python logging) → stderr (default for unconfigured handlers) → captured to `logs/trader_error.log`

`engine/alert_channels.py:32` does `logger = logging.getLogger(__name__)` and uses `logger.info` for NTFY success/failure (lines 163, 166, 187, 190, 269). All those lines write to `trader_error.log`, not `trader.log`.

### Classification
🟢 **No bug. NTFY works. Logging sink is split between the two log files; investigation needs to grep both.**

### Cross-cutting observation
The **dedup state (`_rate_state` dict in `engine/alert_channels.py`) is in-memory, NOT persisted to DB.** Each restart resets it. That's why HM-U submit_single APIError fired 3 times (11:45, 12:13, 12:40) — once per restart. Within a single PID's lifetime, rate_limit_secs=86400 holds; across restarts, dedup resets. For drift-class alerts (Item 5 fires once per day at 13:30), this is fine. For HM-U error-class alerts on a heavy-restart day like today, expect N fires per error_type per day where N = number of restarts.

Worth flagging if Admiral wants persistent dedup across restarts (small fix: persist `_rate_state` to `data/trader.db::settings`).

---

## Cross-cutting observations

1. **alpaca-mirror sync staleness affects multiple downstream consumers** — Thread B's root cause (alpaca_portfolio_sync gated by `_required_interval()` during market hours) is broader than just the canary. Kirk advisor, first_officer, Q entity, dashboard panels all read alpaca-mirror's positions table per HM-I-β-Item3 (commit 5186408). Stale sync = stale advisor context.

2. **Canary's "drift" definition mixes 3 different things** — true architectural drift, sync staleness, and shared-symbol coincidence. Each is a different fix shape. The current canary surfaces all 3 categories under one alert; consider splitting into routed_drift / sync_staleness / shared_symbol_overlap.

3. **Logging sink split is itself a documentation gap.** Future-Scotty greps trader.log first; investigations like this one need both files. Worth adding to CLAUDE.md or HM-W (working-tree hygiene): "alert_channels logger output goes to trader_error.log, not trader.log."

4. **HM-AA/HM-U enrichment is doing exactly what it was designed to do** — surfacing previously invisible bugs (today's 200+ submit_single APIErrors in cumulative log; 13:30 drift findings). Even the "false positive" findings here are valuable signal: they expose semantic ambiguities in the canary that wouldn't have surfaced otherwise.

---

## Options for the Admiral (do not pre-commit)

### Thread A
- **A1:** Filter routed_drift to `route_mode=trading` only (~5-line fix in `compute_routed_drift`)
- **A2:** Split `ROUTED_PLAYERS` into 3 categories with separate drift logic each

### Thread B
- **B1:** Investigate `_required_interval()` gating — is sync supposed to run every 5 min during market hours or longer?
- **B2:** Have reconciliation read live Alpaca directly (skip alpaca-mirror snapshot dependency)
- **B3:** Chain reconciliation to invoke a fresh sync first

### Thread C
- **C1:** Reframe `compute_unrouted_drift` to query `trades.alpaca_order_id` (forwarded orders) instead of symbol overlap
- **C2:** Drop or rename the metric to clarify "overlap" ≠ "drift"
- **C3:** Populate `trades.alpaca_order_id` in `_forward_to_alpaca` (this is the proper architectural fix; the field exists but no caller writes to it)

### Thread D
- **D1:** Document the trader.log vs trader_error.log split somewhere greppable (CLAUDE.md addition)
- **D2:** Persist alert_channels `_rate_state` to DB so dedup survives restarts (small fix; protects against multi-restart days)

### General
- **G1:** Investigate the HM-AA-newly-surfaced close_options_position SPY260515P00718000 APIError at 12:48:22 (likely dalio-metals stale-CALL pattern from HM-AC — separate from the 19 spread-close path, single-leg path)

---

## What I (Scotty) deliberately did NOT do
- Did not patch any drift findings
- Did not modify `engine/reconciliation.py`
- Did not run alpaca_portfolio_sync manually
- Did not pick remediation options
- Did not ship any code or schema changes
- Did not investigate the close_options_position 12:48 APIError beyond noting it (G1)

The Admiral picks remediation per thread separately. Each option is small (~5-30 line code change in known files); none requires architectural decision.
